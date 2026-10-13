# Custom Integration: Task Sync

## Overview

This integration script mirrors completed tasks from one runZero console into another — typically from
a SaaS console (`console.runzero.com`, `console-eu.runzero.com`, ...) into a self-hosted one. It lists
the processed tasks in the source organization, downloads each task's scan data, and imports it into
the destination through the same import path a real Explorer uses.

Run it on a schedule and it behaves as an incremental sync:

- **Nothing is imported twice.** Every import it creates on the destination carries the source task
  ID in its description (`runzero-task-sync:<source task id> from <source url>`), and each run reads
  those back before it moves anything. Already-mirrored tasks are skipped without touching the
  source, so hiding source tasks is no longer required to keep the sync idempotent.
- **Task names survive.** The destination import is given the source task's name, so the task list
  on the destination reads like the one on the source rather than as a wall of `Scan Import`.
- **Sites are mirrored by name.** Leave the destination site blank and each task lands in a
  destination site with the same name as its source site, created on the destination when it does
  not exist yet. Set a destination site ID instead to collapse everything into one site.
- **History replays in order.** Tasks are moved oldest first, so the destination processes the
  history in the order it happened and a newer scan is never overwritten by an older one.
- **Large histories move in batches.** The platform allows a custom integration run 4 GiB of
  downloads in total; the script budgets from each task's recorded data size and stops cleanly at
  the configured per-run limit (2 GB by default), leaving the rest for the next scheduled run.

## Where to run it

The script is one-way but not one-place. `src_url` and `dst_url` decide which console is read
and which is written; where the task runs decides only what the script can reach. For a
SaaS-to-self-hosted sync, set it up on the **self-hosted console** and run it from an **Explorer
connected to that console**:

- An on-prem Explorer reaches the self-hosted console at its internal address and reaches
  `console-eu.runzero.com` over ordinary outbound HTTPS, so both ends are in view. A custom
  integration task running on an Explorer may connect to private addresses.
- Setting it up on the SaaS console does not work for an internal destination. A task run on
  the console itself (no Explorer selected) blocks private and link-local addresses, so an
  internal `dst_url` fails with `connections to internal IP address ... are not allowed`. It would
  only work from an on-prem Explorer enrolled to the SaaS console, which is an extra Explorer for
  no gain.
- Nothing on the SaaS side ever connects to the self-hosted console. The script pulls from the
  source and pushes to the destination from wherever it runs, so the self-hosted console needs no
  public address, no inbound firewall rule, and no DNS entry outside the network.

`dst_url` is therefore the self-hosted console's own address as seen from inside the network:
the `RUNZERO_CONSOLE` value in `/etc/runzero/config` (for example `https://runzero.corp.example`
or `https://10.0.4.20`), which is the same URL users browse to and Explorers enrol against.

Self-hosted consoles usually serve a self-signed or private-CA certificate that the Explorer will
not trust by default. The destination TLS options on the credential handle this without touching
the SaaS side:

- **Additional CA certs (PEM)** (`dst_tls_ca_cert`): paste the private CA, or the console's own
  certificate when it is self-signed. `dst_url` must then use a hostname or IP the certificate
  names.
- Or pin the certificate: enable **Disable TLS validation** (`dst_tls_disable_validation`) *and*
  set **Pinned peer cert SHA-256** (`dst_tls_peer_hash`) to the console certificate's SHA-256
  fingerprint (`openssl s_client -connect <host>:443 </dev/null | openssl x509 -fingerprint -sha256 -noout`).
  With the pin set, disabling validation only skips the CA chain check; any certificate other
  than the pinned one is still refused.
- **Disable TLS validation** on its own is the last resort, not the first step.

The self-hosted console must be on a release at or above the script's `minVersion`
(`5.1.260818.0`): the Explorers it deploys are the ones that run the script, and an older
Explorer refuses it with an upgrade message.

## Requirements

### runZero Requirements

- Superuser access to **Custom Integrations** on the console the task will run from — the
  self-hosted console for a SaaS-to-self-hosted sync (see [Where to run it](#where-to-run-it)).
- An Explorer connected to that console with a route to both `dst_url` and the source console
  (outbound HTTPS to `console-eu.runzero.com` or `console.runzero.com`).

### API Requirements

Both credentials are **runZero** tokens. There is no third-party product to configure — the
source and the destination are two runZero consoles.

- **Source organization ID** (`src_org_id`) — the organization whose tasks are read.
- **Source API token** (`src_api_token`) — an **Organization API token** (`OT`) for that
  organization. This has to be an organization token, not an export token: the sync reads
  `/api/v1.0/org/tasks`, downloads `/api/v1.0/org/tasks/{id}/data`, and — when
  `hide_tasks_on_sync` is enabled — posts to `/api/v1.0/org/tasks/{id}/hide`. An export
  token (`ET`) reaches only the `/api/v1.0/export` paths and cannot do any of that.
- **Destination organization ID** (`dst_org_id`) — where the task data is imported.
- **Destination site ID** (`dst_site_id`) — optional. When set, every task is imported into that
  one site. When blank, tasks are placed in destination sites matching their source site's name,
  and missing sites are created (`PUT /api/v1.0/org/sites`), which needs an account licensed to
  create sites. The source side does not take a site, because tasks are selected by search filter.
- **Destination API token** (`dst_api_token`) — an Organization API token for the
  destination organization, with write access. The upload is a `PUT` to
  `/api/v1.0/org/sites/{site_id}/import`, and the script also reads `/api/v1.0/org/tasks` and
  `/api/v1.0/org/sites` on the destination, so a read-only token is not sufficient.
- Network reachability from the Explorer running the task to **both** consoles.

## Configuration Steps

1. **Obtain Required IDs and Tokens**

   Do this on each console: the **source** (`https://console-eu.runzero.com` for an EU tenant)
   for the source values, and the **self-hosted console** for the destination values.

   - Go to **Organizations** and click the organization. Its ID is shown on the
     organization's information page, and also appears as the `_oid` query parameter in the
     console URL while that organization is selected.
   - Click **Edit organization** and generate a token in the **organization API tokens**
     section. Organization tokens begin with `OT`; export tokens begin with `ET` and will
     not work here.
   - Only if every task should land in a single destination site: open **Sites** on the
     destination console, click the target site, and note its ID from the site page URL.

   Record: source organization ID and token, destination organization ID and token, and
   optionally the destination site ID.

2. **Decide which tasks to sync**

   `src_task_search_filter` selects them, using the same search syntax as the tasks page. The
   default is `type:scan`, which mirrors every completed scan task the source lists. Only
   **processed** tasks are ever considered — the status is filtered server-side, whatever the
   filter says — so there is no need for a `status:` term. Narrow the filter when the whole
   history is not wanted, for example `type:scan site:HQ` or `type:scan created:>2025-01-01`.

   The source task list returns at most the newest 1000 matching tasks. For a longer history,
   either enable `hide_tasks_on_sync` so synced tasks drop out of the list and older ones surface
   on later runs, or run with time-bounded filters.


3. **Create the Custom Integration**
   - On the **self-hosted console**, go to **Custom Integrations** (`/custom-integrations/new`).
   - Add a Name and Icon for the integration (e.g., "Task Sync").
   - Toggle `Enable custom integration script` to input the finalized script.
   - Click `Validate` to ensure it has valid syntax.
   - Click `Save` to create the Custom Integration.

4. **Create the Credential for the Custom Integration**
   - On the **self-hosted console**, go to **Credentials** (`/credentials`).
   - Select `<name of custom integration> Script Secrets`.
   - **Source runZero URL** (`src_url`): the SaaS console, `https://console-eu.runzero.com` for an EU tenant. Defaults to `https://console.runzero.com`.
   - **Source org ID** (`src_org_id`): the SaaS organization to read tasks from.
   - **Source task search filter** (`src_task_search_filter`): optional; which tasks to sync. Default `type:scan`.
   - **Destination runZero URL** (`dst_url`): the self-hosted console's own address — the `RUNZERO_CONSOLE` value from `/etc/runzero/config`, e.g. `https://runzero.corp.example` or `https://10.0.4.20`. It is reached from the Explorer, so an internal address is expected.
   - **Destination org ID** (`dst_org_id`): the self-hosted organization to import into.
   - **Destination site ID** (`dst_site_id`): optional; leave blank to mirror sites by name.
   - **Max MB of task data per run** (`max_mb_per_run`): optional; default 2048. See [Optional Settings](#optional-settings).
   - **Hide source tasks after sync** (`hide_tasks_on_sync`): optional; default off.
   - **Source API token** (`src_api_token`): the `OT` token from the SaaS organization.
   - **Destination API token** (`dst_api_token`): the `OT` token from the self-hosted organization.
   - TLS and HTTP options are separate for each side, prefixed `src_tls_` / `dst_tls_` and `src_http_` / `dst_http_`. Set the destination TLS options described in [Where to run it](#where-to-run-it) when the self-hosted console's certificate is self-signed or from a private CA; the source side keeps full validation of the SaaS certificate.


5. **Create the Custom Integration Task**
   - On the **self-hosted console**, go to **Ingest** → **Custom integration** (`/ingest/custom/`).
   - Select the Credential and Custom Integration created in the previous steps.
   - Update the task schedule to recur at the desired timeframes. An hourly schedule turns a
     large first migration into a series of batches and then keeps the destination current.
   - Select the Explorer the task runs from. Do **not** leave the selection empty: a task run
     on the console itself cannot reach private addresses, so it would never reach `dst_url`.
   - Click `Save` to kick off the first task.

## Optional Settings

- **Per-run data limit** (`max_mb_per_run`, default 2048). The script adds up the recorded data
  size of the tasks it is about to move and stops once the next task would cross the limit; the
  remaining tasks sync on the following run. This keeps a run inside the platform's 4 GiB download
  budget, which would otherwise abort the script mid-run. Lower it on a slow link to keep each run
  short; raise it (not beyond ~3500) to migrate faster. The first task of a run always moves even
  if it alone is over the limit, so one large task cannot block everything behind it.
- **Hide source tasks after sync** (`hide_tasks_on_sync`, default off). Hides each task on the
  source console once it has been imported. This is no longer needed to prevent double imports —
  the destination marker handles that — but it is the way to work through a history longer than
  the 1000 tasks the source list returns, and it keeps the source tasks page showing only what is
  still outstanding.

## Running it from the command line

The runZero Explorer binary runs a script directly, which is the fastest way to confirm
both consoles are reachable and both tokens work before scheduling it. Run it from a machine
inside the network — the on-prem Explorer host is the natural choice — so that `dst_url`
resolves the same way it will for the scheduled task. `--kwargs` is
repeated once per parameter. Note that this integration's script is `runzero-task-sync.star`:

```bash
runzero script --filename runzero-task-sync/runzero-task-sync.star \
  --kwargs src_url=https://console-eu.runzero.com \
  --kwargs src_org_id=8c1f0a34-5b62-4d97-a0e3-71f4b8c26d95 \
  --kwargs src_task_search_filter=type:scan \
  --kwargs dst_url=https://runzero.internal.example.com \
  --kwargs dst_org_id=2e7b91c6-4a08-4f35-9d1c-06b3ea75f482 \
  --kwargs max_mb_per_run=2048 \
  --kwargs hide_tasks_on_sync=false \
  --kwargs src_api_token=OT1a2b3c4d5e6f708192a3b4c5d6e7f809 \
  --kwargs dst_api_token=OT9f8e7d6c5b4a30291817f6e5d4c3b2a1 \
  --kwargs dst_tls_disable_validation=true
```

Add `--kwargs dst_site_id=<uuid>` to collapse every task into one destination site; without it,
sites are mirrored by name.

**`--output` is not useful here.** This integration reports no assets back to the console it
runs from — it moves task data from one instance to another — so an export directory would
be written empty. Read the log instead: it prints `Found <n> processed task(s) matching the
filter`, then `<n> task(s) already synced, <n> to sync`, then `Pulling task ...` and
`Uploading task ...` for each one, and ends with `Synced <n> task(s), skipped <n>`. Add
`--verbose` for the request-by-request detail.

**A filter containing quotes needs CSV escaping on the command line.** `--kwargs` CSV-parses
any argument containing a second `=` sign, so a filter such as `name:="Nightly scan"` fails with
`parse error on line 1, column 30: bare " in non-quoted-field`. Wrap the *entire* argument as
one CSV field and double the inner quotes:

```bash
--kwargs '"src_task_search_filter=name:=""Nightly scan"""'
```

which arrives at the script as `name:="Nightly scan"`. A filter with no double quotes, such as
the default `type:scan`, needs nothing special. The console credential form has no such
limitation; this only affects ad-hoc CLI runs.

To check the `CONFIG` block and the HTTP and TLS wiring without touching either console:

```bash
runzero script --filename runzero-task-sync/runzero-task-sync.star --validate
```

Validation generates placeholder parameter values and answers from a local dummy server,
so it proves the script initializes and declares its parameters correctly. It does not
prove either token is valid or that any task can be moved.

The same script also runs under the `scan` command, which is what the platform itself
invokes for a scheduled task:

```bash
runzero scan --custom-integration-script-source "$(cat runzero-task-sync/runzero-task-sync.star)" \
  --custom-integration-id <uuid-from-the-console> \
  --custom-integration-script-kwargs 'src_url=https://console-eu.runzero.com,src_org_id=<src-org>,dst_url=https://runzero.internal.example.com,dst_org_id=<dst-org>,src_api_token=<OT-token>,dst_api_token=<OT-token>'
```

`--custom-integration-id` is the UUID the console shows on the integration's page.
`--custom-integration-entry-function-name` defaults to `main`. This flag takes one
comma-separated string, so a search filter containing a comma cannot be passed through it
at all; configure that field on the console credential.

## Asset identity

**This integration reports no assets, so it has no asset identity.** `main` never constructs
an `ImportAsset`, never calls `report_assets`, and returns `None`. There is no `id=` to derive
an identity from and no `matchBehavior` to document.

It is worth being precise about what it does instead, because "sync tasks" understates it:
the payload it moves is **scan data**, not task metadata. Per run, in order:

1. `GET <src_url>/api/v1.0/org/tasks?_oid=<src_org_id>&status=processed&search=<filter>` — selects tasks. Only processed tasks have data to download, so the status is filtered on the server.
2. `GET <dst_url>/api/v1.0/org/tasks?_oid=<dst_org_id>&search=type:import desc:runzero-task-sync:` — reads back which source tasks the destination already holds, from the descriptions written in step 5. Tasks found here are skipped. The list returns at most 1000 rows; when it is full, each remaining task is checked individually with `desc:runzero-task-sync:<task_id>`.
3. `GET <dst_url>/api/v1.0/org/sites?_oid=<dst_org_id>` — lists destination sites, to verify a configured `dst_site_id` exists before anything is downloaded, or to map source site names onto destination site IDs.

Then per task, oldest first:

4. `GET <src_url>/api/v1.0/org/tasks/<task_id>/data?_oid=<src_org_id>` — downloads that task's raw scan result. The `_oid` matters: with an account-level token, a `/data` call without it resolves against the token's default org and 404s every task.
5. `PUT <dst_url>/api/v1.0/org/sites/<site_id>/import?_oid=<dst_org_id>&name=<source task name>&description=runzero-task-sync:<task_id> from <src_url>` — replays it into the destination console through the same import path a real Explorer uses. When sites are mirrored by name and the destination has no site of that name yet, `PUT <dst_url>/api/v1.0/org/sites` creates it first.
6. `POST <src_url>/api/v1.0/org/tasks/<task_id>/hide?_oid=<src_org_id>` — optional, only when `hide_tasks_on_sync` is set. A hide that fails is logged, because a silent hide failure would be invisible.

So asset identity **is** decided — just on the destination console, by runZero's own scan
ingestion, exactly as it would be for a scan the destination ran itself. The records in the
uploaded file carry the same fingerprints, MACs, addresses, and hostnames the source Explorer
observed, and the destination merges them by its normal rules. Nothing in this script
constructs, rewrites, or namespaces an identity, and that is deliberate: re-keying the data
would break precisely the merge behavior that makes the replay useful.

Three consequences follow, and all are easy to be surprised by:

- **Assets appear under the destination's site, chosen by name or by `dst_site_id`.** With `dst_site_id` set, every synced task lands in that one site regardless of where it was scanned on the source. With it blank, the destination site is the one whose name matches the source site's — created if absent — so renaming a site on either console after the first sync splits its history across two destination sites. A task whose source site has since been deleted has no name to match and is skipped with a log line.
- **A task is imported once.** The description marker is the only record that a task has been synced; the import endpoint itself is not idempotent and would merge the same data again, moving first-seen and last-seen timestamps around. The marker is read from the destination's visible task list, so **hiding a synced import on the destination makes the script import that task again** on its next run — which is also the deliberate way to force a re-import after, say, a failed processing run.
- **Processing order is the replay order.** Tasks are submitted oldest first and the destination's import queue is roughly first-in, first-out, so the history replays in sequence. A per-run limit that splits the history across runs preserves this: each run picks up where the last stopped, still oldest first.

**A note on the `CONFIG` type.** This script declares `"type": "internal"`, not `"inbound"`:
an `inbound` integration is one whose `main` yields `ImportAsset` values for the platform to
merge, and this one reports no assets at all — everything it writes goes to a *different*
console over that console's own API. The two comparable scripts in the public
[runzero-custom-integrations](https://github.com/runZeroInc/runzero-custom-integrations)
library, `runzero-scan-passive-assets` and `runzero-vulnerability-workflow`, declare
`"internal"` for the same shape.

## Future

- **An explicit site mapping table.** Sites are matched by exact name. A source estate whose site names do not line up with the destination's — `HQ` on one side, `Headquarters` on the other — needs a name-to-name or name-to-ID table, which the `json` parameter type could carry. Nothing today lets a task from one source site land in a differently named destination site short of renaming one of them.
- **Sync more than scan tasks.** The default filter is `type:scan`. Integration and sampling tasks carry data in the same `/data` shape, so they would replay through the same import path — but whether replaying a non-scan task into a foreign console produces sensible results has not been established. Connectors are better re-created on the destination and run live than replayed from history.
- **Bidirectional sync is not supported and would need care.** The script is one-way by construction: it holds a source token and a destination token and only ever writes to the destination. A reverse path is mechanically symmetric, and the description marker already names the source console, so a reverse instance could exclude imports that originated from itself — but two consoles syncing to each other has not been tried and should not be assumed to converge.
- **Gzip bodies are decompressed and recompressed, not passed through.** The `http` module returns a response body as a string and only accepts a `bytes` body on `put`, and the one string-to-bytes conversion Starlark offers transcodes invalid UTF-8, which destroys gzip. So the only route from a downloaded `.gz` to an uploadable body is `gzip_decompress` then `gzip_compress`, which costs CPU and several times the memory of the compressed payload for the duration of each task. A `bytes` response body, or a `put` that accepts a string body, would make the copy a pass-through.
- **Tasks that decompress to more than 1 GiB cannot be moved.** `gzip_decompress` caps its output at 1 GiB and aborts the run beyond it; the script guards against *compressed* sizes over 1 GiB (the per-response cap) from `size_data`, but cannot know the decompressed size in advance. A task that trips this aborts the run on every attempt until the filter excludes it (`type:scan not id:<task_id>`).
- **The gzip detection is a heuristic worth eventually removing.** The download endpoint redirects to object storage and presigns the *uncompressed* object whenever the gzipped one is absent, so the script sniffs the first byte for `{` and only decompresses when it is not one. That is correct today and is documented in the script's own comment, including the platform-side TODO it works around. It is still a content-type decision being made from a payload byte, and it should be revisited once the platform's own migration to gzip is complete.
- **There is no third-party API here, so there is no vendor surface to extend.** Everything above is bounded by runZero's own API. That also means there is no alert or event feed to ingest and no outbound push-back to design — the two ends of this integration are both runZero, and the only question is which of runZero's endpoints it should be using.

## Troubleshooting

- **`connections to internal IP address ... are not allowed`**: the task ran on the console itself rather than on an Explorer, or on a console-hosted run of the SaaS console. Set it up on the self-hosted console and select an on-prem Explorer for the task (see [Where to run it](#where-to-run-it)).
- **`x509: certificate signed by unknown authority` against `dst_url`**: the self-hosted console's certificate is self-signed or from a private CA. Set `dst_tls_ca_cert`, or pin it with `dst_tls_disable_validation` plus `dst_tls_peer_hash`.
- **`failed to get tasks` / `failed to list destination tasks`**: the token or organization ID for that side is wrong, the token is an export (`ET`) token, or the console is not reachable from the Explorer. The side named in the message is the one to check.
- **`destination site ... not found`**: `dst_site_id` is not a site in `dst_org_id`. Fix the ID, or clear it to mirror sites by name.
- **`Failed to create destination site`**: the destination account is not licensed to create sites, or the token cannot write to inventory. Create the site by hand with the source site's exact name, or set `dst_site_id`.
- **The same task was imported twice**: its earlier import was hidden or deleted on the destination, so the marker the script looks for was gone. Leave synced imports visible on the destination.
- **The run stops with `Reached the per-run data limit`**: this is normal for a large history; the next scheduled run continues. Raise `max_mb_per_run` to move more per run, keeping it under the platform's 4 GiB download budget.
- **Only the newest 1000 source tasks are ever considered**: that is the task list's cap. Enable `hide_tasks_on_sync` so mirrored tasks fall out of the list, or narrow the filter by time.
- **Network timeouts on large tasks**: downloads and uploads already allow an hour each; a task that exceeds that on your link is best moved on a faster path, or excluded with `not id:<task_id>` in the filter.

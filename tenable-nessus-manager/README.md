# Custom Integration: Tenable Nessus Manager

Inbound runZero custom integration that imports the **linked agents** of an
on-premises Tenable Nessus Manager: every host running a Tenable Nessus Agent
that reports to the manager, together with its agent group membership, agent
status, last check-in, plugin set, and agent version.

It exists because runZero's native Tenable integrations read different things:
the Nessus Professional integration pulls scan data (hosts, software, and
vulnerabilities) from the Nessus Professional API, `.nessus` file import covers
every Nessus edition's exported scan results, and the Tenable Security Center
and Tenable Vulnerability Management integrations read the asset and
vulnerability repositories of those platforms. None of them talks to Nessus
Manager's `/agents` and `/agent-groups` API, which only Nessus Manager serves.
This integration fills that gap and can run alongside the native ones against
the same estate: it reports its own foreign ids, and runZero correlates the two
views of a host by MAC, IP, and hostname like any other pair of sources.

Each agent becomes one runZero asset with:

- the agent name as hostname, the reported IP and MAC addresses as network
  interfaces, and a coarse OS (`Windows`, `Linux`, `macOS`) from the agent
  platform;
- `firstSeenTS` from the link date and `lastSeenTS` from the last check-in;
- tags `agent-status:<online|offline|initializing|unlinked>`, one
  `agent-group:<name>` per agent group, and `agent-safe-mode` when the agent
  has dropped into safe mode;
- a software row `Tenable / Nessus Agent / <core_version>`, so the agent
  version is searchable in the software inventory;
- every field of the agent record as a `nessus_manager_*` custom attribute.

## runZero requirements

- Superuser access to the [Custom Integrations configuration](https://console.runzero.com/custom-integrations) in runZero.
- An Explorer running **5.1.260825.0 or later** that can reach the Nessus Manager web port (8834 by default). Nessus Manager is an on-premises application, so the Explorer must be inside the network that serves it.

## Tenable requirements

- **Tenable Nessus Manager.** The `agents` and `agent-groups` resources are documented as "only usable in Nessus Manager". Nessus Professional and Nessus Expert answer the same API on the same port without them; the integration reads `/server/properties` first and ends the task in error if `nessus_type` names anything other than a manager. Written against the Nessus 10.12.5 API documentation.
- **An API key pair** (access key and secret key). Tenable's documentation states that generating keys in Nessus Manager requires the **System Administrator** role, and that viewing and managing agents requires **Administrator or System Administrator**. Keys inherit the role of the user who generated them. Note Tenable's caution that generating a new pair replaces any existing keys for that user.
- The integration only ever issues `GET` requests: `/server/properties`, `/agent-groups`, and `/agents`. No scan, policy, or agent-management endpoint is touched.
- Nessus ships a self-signed certificate on port 8834. Either paste the issuing CA into the integration's **Additional CA certs (PEM)** TLS option (`tls_ca_cert`), install a certificate the Explorer trusts on the manager, or enable **Disable TLS validation** (`tls_disable_validation`).

## Steps

### Nessus Manager configuration

1. Sign in to Nessus Manager as a System Administrator and open **Settings > My Account > API Keys**.
2. Click **Generate**, confirm, and copy both the **Access Key** and the **Secret Key**. The secret is shown once.
3. Optionally confirm the keys from a host inside the network with
   `curl -k -H 'X-ApiKeys: accessKey=<access>; secretKey=<secret>' https://<nessus-manager>:8834/agents?limit=5`.
   A working response is a JSON document with an `agents` array and a `pagination` object. The interactive API reference for your release is served by the manager itself at `https://<nessus-manager>:8834/api#/resources/agents`.

### runZero configuration

1. [Create the Custom Integration](https://console.runzero.com/custom-integrations/new).
   - Add a Name and Icon for the integration (e.g., "Tenable Nessus Manager").
   - Toggle `Enable custom integration script` and paste [tenable-nessus-manager.star](tenable-nessus-manager.star).
   - Click `Validate` to ensure it has valid syntax.
   - Click `Save` to create the Custom Integration.
   - The script embeds its `CONFIG` block, so the credential form is generated automatically with the fields below.
2. [Create the Credential for the Custom Integration](https://console.runzero.com/credentials).
   - Select the option that matches your custom integration name eg `Tenable Nessus Manager Script Secrets`.
   - **Nessus Manager URL** (`url`): base URL of the manager including the port, for example `https://nessus-manager.example.com:8834`.
   - **API access key** (`access_key`) and **API secret key** (`secret_key`): the pair generated above. They are sent together as the `X-ApiKeys: accessKey=<access>; secretKey=<secret>` header.
   - **Import unlinked agents** (`include_unlinked`, default `false`): agents whose status is `unlinked`. Nessus Manager only keeps these when **Track unlinked agents** is enabled in its agent settings, and they are no longer reporting, so they are skipped by default.
   - **Agents per page** (`page_size`, default `1000`, maximum `5000`): the `limit` sent on each request to `/agents`.
3. [Create the Custom Integration task](https://console.runzero.com/ingest/custom/).
   - Select the Credential and Custom Integration created in steps 1 and 2.
   - Update the task schedule to recur at the desired timeframes.
   - Select the Explorer you would like the Custom Integration to run from.
   - Click `Save` to kick off the first task.

### Running it from the command line

The runZero Explorer binary runs a script directly, which is the fastest way to
confirm a key pair and see what the manager returns before scheduling anything.
Run it from a host inside the network that serves the manager. `--kwargs` is
repeated once per parameter:

```bash
runzero script --filename tenable-nessus-manager/tenable-nessus-manager.star \
  --kwargs url=https://nessus-manager.example.com:8834 \
  --kwargs access_key=<access key> \
  --kwargs secret_key=<secret key> \
  --kwargs tls_disable_validation=true \
  --custom-integration-id 1f2e3d4c-5b6a-7988-9a0b-1c2d3e4f5a6b \
  --output ./tenable-nessus-manager-run
```

`--output` writes the assets the run produced, and `--overwrite` replaces a
directory from a previous run. Add `--verbose` for the request-by-request log.
A first run that fails on TLS rather than on authentication is the manager's
self-signed certificate: `tls_disable_validation=true` is the practical choice
for a command-line test, and the scheduled task can trust the issuing CA
through the credential's **Additional CA certs (PEM)** field instead.

```bash
runzero script --filename tenable-nessus-manager/tenable-nessus-manager.star --validate
```

Validation checks the `CONFIG` block and the HTTP and TLS wiring against a
local dummy server. It passes with three requests and zero assets, because the
dummy server does not model the Nessus responses. The fixtures below are the
real offline validation.

### Fixture tests

The scenarios under `tenable-nessus-manager/tests/fixtures/` run the real
runZero CLI against a local HTTP server that replays Nessus-shaped responses,
reusing the fixture harness from the sibling `runzero-custom-integrations`
checkout without copying or modifying it. Run from `Jamie-runZero-Scripts`:

```bash
export PYTHONDONTWRITEBYTECODE=1 RUNZERO_SCANNER=/tmp/rumble-scanner
python3 tenable-nessus-manager/tests/run.py            # every scenario
python3 tenable-nessus-manager/tests/run.py happy      # one scenario
```

Set `RUNZERO_CUSTOM_INTEGRATIONS` when that repository is not a sibling
directory. The scenarios cover the id scheme and field mapping, both
pagination terminations (a `pagination.total` and a short page against the
limit the server echoed), a row repeated across pages, the `include_unlinked`
switch in both positions, a refused key, a key without the agent permission, a
URL that points at Nessus Professional, both optional reads failing, a page
failing mid-walk, a 429 followed by recovery, a row with every field
mis-typed, and an empty manager. Every scenario that imports assets is run
twice and the emitted ids must match.

### What's next?

- You will see the task kick off on the [tasks](https://console.runzero.com/tasks) page like any other integration.
- The task will update existing assets with data pulled from Nessus Manager.
- The task will create new assets when there are no existing assets that meet merge criteria (hostname, MAC, etc).
- You can search for assets enriched by this custom integration with the runZero search `custom_integration:tenable-nessus-manager`, for the members of one agent group with `tag:agent-group:Windows-Servers` (spaces in the group name become hyphens), for agents that have stopped checking in with `tag:agent-status:offline`, and for the agent version with `software:"Nessus Agent"`. Assets runZero has discovered that carry no `nessus_manager_agent_uuid` attribute are the hosts with no agent.

## Asset identity

- Target entity: a host (workstation, laptop, server, or VM) running a Tenable Nessus Agent linked to the manager.
- Source ID field: `agents[].uuid`.
- Documentation evidence: the Nessus 10.12.5 API reference lists `uuid` ("The uuid of the agent") on every `agent` representation returned by `GET /agents` and `GET /agents/{agent_id}`. Tenable's cloud schema for the same record (`GET /scanners/{scanner_id}/agents`) describes it as "The UUID of the agent. This value corresponds to the ID of the asset where the agent is installed. You can use this property to match agent data to asset data." The agent generates the value on the host and stores it in its data directory (`nessuscli prepare-image` deletes "any UUID file on the agent (for example, /opt/nessus/var/nessus/uuid ...)"), and a manager refuses to link a second agent presenting an existing UUID (the documented "409 UUID error").
- Uniqueness scope: one Nessus Manager. The UUID is a random 128-bit value, so collisions across managers are not expected either, but a host can be re-linked from one manager to another and each manager is a separate deployment, so the manager host is part of the runZero id.
- Cardinality: one row per linked agent, one agent per host. A host carries exactly one agent installation.
- Stability: survives rename, readdressing, agent upgrades, unlink and relink, and the manager's own housekeeping. Tenable documents that an agent the manager auto-unlinked for inactivity "automatically relinks if it comes back online", and that **Remove agents that have been inactive for X days** deletes the record; a removed agent that reconnects is re-created with a new integer `id` but the same `uuid`, which is why the integer `id` is not used as identity. The UUID is replaced only by a full uninstall that removes the agent's data directory, or by `nessuscli prepare-image`.
- Reuse behavior: random UUIDs are not recycled. The documented failure mode is the opposite one: a golden image captured with the UUID file still present hands one UUID to every clone ("multiple agents to share the same UUID, which may result in duplicate asset issues"). The manager refuses the duplicate link, so such clones never appear as separate rows; they appear as one flapping agent, which this integration imports as one asset until the image is fixed with `prepare-image`.
- Presence: listed on every agent representation in the reference. A row without it is skipped, not synthesized.
- Final runZero ID: `tenable-nessus-manager:<manager-hostname>:<uuid>`
- Missing-ID behavior: skip the row with a message naming only the agent name. A row that has a UUID but no usable hostname, address, or MAC is also skipped, because nothing could ever correlate with it.
- Match behavior (set once in `CONFIG`): `no-mac-break no-ip-break no-name-break`. The manager publishes one current IP and whatever MACs the agent last reported; these are endpoints, so leases move, laptops roam between wired and wireless adapters, VPN adapters come and go, and machines are renamed, all without the UUID changing. None of that churn should disqualify a merge against the authoritative id. Id matching and id breaking stay on, so two agent records are never silently collapsed into one asset.
- Verdict: scoped authoritative.

### Why not the integer `id` or the MAC

`id` is the manager's row key. It is unique within one manager, but the manager's own **Remove agents that have been inactive for X days** setting deletes rows, and a host that reconnects afterwards comes back with a new `id` and the same `uuid`; an `id`-keyed asset would fork at exactly that moment. The MAC list is reported by the agent and changes with the adapters present; it is also the field most likely to be shared by two hosts (a virtual NIC template, a docking station). Both are imported as attributes, where they stay searchable without driving identity.

### Notes

- Assets come from `GET /agents?limit=<page_size>&offset=<n>`, walked until `pagination.total` is reached or, when the response carries no total, until a page comes back shorter than the limit the server echoed in `pagination.limit` (falling back to the requested size). This is the same termination Tenable's own client uses. The list is offset-paged with no stable sort, so a row that shifts between pages can arrive twice; the second copy is skipped and counted. Each asset is handed to `report_asset` as it is built, so the estate is never held in memory as one list.
- `GET /server/properties` is read first. A `401` ends the task immediately with a credential message. Any other failure is logged and the run continues, because a reverse proxy that only forwards the agent routes is a plausible deployment; the agent list is the primary read and decides the run. When the properties are readable, the product, version, and plugin set are logged, and a `nessus_type` that is not a manager ends the task in error.
- `GET /agent-groups` is read once and indexed by id. The agent rows embed their groups as `{"id", "name"}` objects, but Tenable's own client also models a bare id, so both shapes are read and a missing name is looked up in the index. A failure to read the list is logged and the run continues with the names embedded in the rows.
- Imported fields: `name` becomes the hostname (placeholders and IP-shaped names are screened); `ip` becomes an address and `mac_addrs` the MAC list, with loopback, link-local, and all-zero values dropped; `platform` becomes a coarse OS (`WINDOWS` to Windows, `LINUX` to Linux, `DARWIN` to macOS) while `distro` stays an attribute because it names the package build the agent was installed from (`es8-x86-64`, `ubuntu1404-amd64`), not the running distribution; `linked_on` becomes `firstSeenTS` and `last_connect` becomes `lastSeenTS`. runZero fingerprinting still takes precedence over the OS (`trustOS` is not set).
- **Interfaces.** The API publishes one IP and a list of MACs with no stated pairing. A single MAC is paired with the address; several MACs each get an interface of their own and the address stands alone, so the integration never asserts a MAC-to-IP binding the API did not make.
- **Every field of the agent record is kept as a custom attribute**, named `nessus_manager_<field>`: `nessus_manager_agent_id`, `nessus_manager_agent_uuid`, `nessus_manager_agent_name`, `nessus_manager_ip`, `nessus_manager_mac_addrs` (normalized, comma-separated), `nessus_manager_platform`, `nessus_manager_distro`, `nessus_manager_core_version`, `nessus_manager_status`, `nessus_manager_last_connect`, `nessus_manager_last_scanned`, `nessus_manager_linked_on`, `nessus_manager_unlinked_on`, `nessus_manager_plugin_feed_id`, `nessus_manager_safe_mode`, `nessus_manager_safe_mode_transition_time`, `nessus_manager_groups` (names, comma-separated), and `nessus_manager_group_ids`. Fields are passed through generically rather than enumerated so that anything a release publishes beyond the documented set is kept; timestamps stay in the unixtime the API uses. Only `safe_mode_agent_count` and `total_agents` are dropped, because they are manager-wide counters repeated on every row rather than facts about the host.
- **Tags.** `agent-status:<status>` carries the manager's own notion of the agent (`online`, `offline`, `initializing`, `unlinked`); `agent-group:<name>` is added once per group, with runs of whitespace in the group name replaced by `-` because runZero splits the tag string on whitespace; `agent-safe-mode` is added when `safe_mode` is greater than zero, which is Tenable's signal that the agent has suspended scanning to protect the host.
- **Software.** Each asset carries one software row, vendor `Tenable`, product `Nessus Agent`, version `core_version`, so an out-of-date agent fleet is a software query away.
- **Unlinked agents.** Nessus Manager tracks an agent it has auto-unlinked for inactivity only when **Track unlinked agents** is enabled; such agents are listed with status `unlinked` and are skipped by default, because importing them would mark a host as agent-covered when nothing is reporting from it. `include_unlinked` imports them with their `unlinked_on` time and the `agent-status:unlinked` tag.
- This integration was validated against local fixtures built from the Nessus 10.12.5 API reference, Tenable's own `pyTenable` client, and the equivalent Tenable cloud schema, not against a live Nessus Manager. See `## Assumptions` for what a first run should confirm.

### Assumptions

Each of these is believed to be correct, is the simplest reading of the evidence found, and could not be confirmed without a live manager. Each is cheap to check on the first run.

1. **`mac_addrs` delimiter.** The reference documents the field as one string holding "the MAC addresses of the agent's network interface(s)" without naming the separator. Any run of whitespace, commas, or semicolons is treated as one, so newline-, comma-, and space-separated lists all import; a separator outside that set would leave the MACs unparsed and the asset correlating on IP and hostname only.
2. **`/server/properties` field names.** `nessus_type`, `server_version`, and `plugin_set` are read from this response. If a release spells them differently the product check is skipped and the log line reads `Nessus (version unknown)`; nothing else depends on them.
3. **`nessus_type` wording.** The product check accepts any value containing `manager` (case-insensitively) so that a clustered or renamed manager passes. A manager whose `nessus_type` does not contain the word would fail the check and should be reported.
4. **Status vocabulary.** The 10.12.5 reference documents `unlinked`, `offline`, `online`, and `initializing`. Older managers used `on`, `off`, and `init`; those import unchanged in the tag and attribute, and `include_unlinked` would then have nothing to filter.
5. **`platform` values.** `WINDOWS`, `LINUX`, and `DARWIN` are the values seen in Tenable's examples. Any other platform imports with no OS set and the raw value as `nessus_manager_platform`.

## Future

- **Linked scanners as assets.** Nessus Manager also lists the remote Nessus scanners linked to it (`GET /scanners`): name, type, status, engine version, platform, last connect. Those scanners are hosts worth having in the inventory, and the same credential reads them; they were left out because the scanner record's address and hostname fields could not be confirmed from the attached reference.
- **Agent group details.** `GET /agent-groups/{group_id}` returns the group with its member list and, on larger managers, its own pagination. Membership is already carried on each agent row, so the call is not made; it would only be needed if a release stopped embedding `groups` on the agent.
- **Scan results.** Nessus Manager holds the agent scan results, exportable through `POST /scans/{scan_id}/export` and the token download flow. Importing vulnerabilities per host is a different and much larger integration (export, poll, download, parse `.nessus` XML) and overlaps with what the native Tenable Nessus integration already does for a scanner; it is out of scope here.
- **Server-side filters.** `/agents` accepts `f=<field>:<operator>:<value>`, `ft`, `w`, and `wf` filters and a `sort` parameter. A documented, stable sort key would remove the need for the in-run duplicate guard, and a status filter would avoid reading unlinked agents at all. Neither is used, because the filter and sort field names for Nessus Manager were not confirmed from the attached reference.

## API documentation

- Tenable Nessus API Documentation v10.12.5 (served by the manager at `https://<nessus-manager>:8834/api#/overview`): the resource overview; the `agents` resource (`GET /agents`, "Returns the full list of agents unless a 'limit' parameter is specified", `GET /agents/{agent_id}`), the `agent` representation (`core_version`, `distro`, `groups`, `id`, `ip`, `last_connect`, `last_scanned`, `linked_on`, `name`, `mac_addrs`, `platform`, `plugin_feed_id`, `safe_mode`, `safe_mode_agent_count`, `safe_mode_transition_time`, `status`, `total_agents`, `unlinked_on`, `uuid`) and its status values; the `agent-groups` resource and `group` representation; and the note that both resources "are only usable in Nessus Manager".
- [Generate an API Key](https://docs.tenable.com/nessus/Content/GenerateAnAPIKey.htm): the API Keys tab, the System Administrator requirement in Nessus Manager, and the caution that a new key replaces any existing one.
- [Unlink an Agent](https://docs.tenable.com/nessus/Content/UnlinkAnAgent.htm) and [Global Agent Settings](https://docs.tenable.com/nessus/Content/AgentSettings.htm): the Administrator or System Administrator requirement for agent management, **Track unlinked agents**, automatic relinking, and **Remove agents that have been inactive for X days**.
- [Create Windows or Linux Golden Image with Tenable Agent Installed](https://docs.tenable.com/agent/Content/CreateImageAgent.htm): the on-host UUID file, `nessuscli prepare-image`, the 409 UUID error, and the duplicate-UUID caution.
- Tenable's own Python client, [pyTenable](https://github.com/tenable/pyTenable): `tenable/base/platform.py` builds the `X-APIKeys: accessKey=<access>; secretKey=<secret>` header; `tenable/nessus/agents.py` and `tenable/nessus/iterators/pagination.py` page `GET /agents` with `limit` (default 1000) and `offset`, reading the `agents` envelope and stopping on a short page; `tenable/nessus/agent_groups.py` reads the `groups` envelope of `GET /agent-groups`.
- [Tenable Vulnerability Management: List agents](https://developer.tenable.com/reference/agents-list): the cloud schema for the same agent record, used to cross-check the `groups` object shape (`{"name", "id"}`), the `pagination` object (`total`, `limit`, `offset`, `sort`), the 5000-record limit ceiling, and the description of `uuid` as the asset-level identifier.

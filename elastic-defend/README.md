# Custom Integration: Elastic Defend

Elastic Defend is Elastic's endpoint protection integration, deployed to hosts
through Elastic Agent and managed from the Security app in Kibana. This
integration imports the protected endpoint inventory into runZero through the
Kibana [Security endpoint management API](https://www.elastic.co/docs/api/doc/kibana/group/endpoint-security-endpoint-management-api),
enriching assets with agent, policy, isolation, and OS detail.

## runZero requirements

- Superuser access to the [Custom Integrations configuration](https://console.runzero.com/custom-integrations) in runZero.

## Elastic requirements

- A Kibana deployment running Elastic Defend: an Elastic Cloud deployment, a
  serverless security project, or a self-managed Elastic Stack.
- An Elastic API key whose role grants **read** access to the **Security**
  solution in Kibana, including the **Endpoint List** sub-feature, in the
  space being read. The integration only calls
  `GET /api/endpoint/metadata`; it never writes.
- Network egress from the Explorer to the **Kibana** endpoint (not the
  Elasticsearch endpoint). On Elastic Cloud this is the URL ending in
  `.kb.<region>.<provider>.elastic-cloud.com`; self-managed defaults to port
  5601.
- The endpoint metadata transform (`metrics-endpoint.metadata_united_default`)
  healthy. It is installed automatically with the Elastic Defend integration;
  if it is stopped, the metadata API returns an empty or stale list.

## Steps

### Elastic configuration

1. In Kibana, go to **Stack Management > Security > API keys** and click
   **Create API key** (on serverless: **Project settings > Management > API
   keys**).
2. Name the key and restrict it. Either assign a role that has Kibana
   privilege **Security: Read** (with **Endpoint List** read) granted for the
   relevant space, or create the key from a user holding that role.
3. Copy the **Encoded** value of the key. That Base64 string is what the
   `api_key` parameter expects, and it is shown only once.
4. Confirm access:

   ```bash
   curl -H "Authorization: ApiKey <encoded-key>" \
        -H "Elastic-Api-Version: 2023-10-31" \
        "https://<kibana-host>/api/endpoint/metadata?pageSize=1"
   ```

   A 200 response with `data`, `total`, and `page` fields confirms the key
   and privileges.

### runZero configuration

1. [Create the Custom Integration](https://console.runzero.com/custom-integrations/new).
   - Add a Name and Icon for the integration (e.g., "Elastic Defend").
   - Toggle `Enable custom integration script` to input the finalized script.
   - Click `Validate` to ensure it has valid syntax.
   - Click `Save` to create the Custom Integration.
   - The script embeds its `CONFIG` block, so the credential form is generated automatically with the fields below.
2. [Create the Credential for the Custom Integration](https://console.runzero.com/credentials).
   - Select the type `Custom Integration Script Secrets`.
   - **Kibana URL** (`url`): the Kibana base URL, including any
     `server.basePath` prefix when Kibana sits behind a reverse proxy under a
     path.
   - **API key** (`api_key`): the encoded API key.
   - **Kibana space ID** (`space_id`): optional; reads
     `/s/<space_id>/api/endpoint/metadata` instead of the default space.
   - **KQL filter** (`kuery`): optional server-side filter. Fields use the
     united index prefix, e.g. `united.endpoint.host.os.name:Windows`.
   - **Host statuses** (`host_statuses`): optional comma-separated statuses
     (`healthy`, `offline`, `updating`, `inactive`, `unhealthy`,
     `unenrolled`). Blank imports every endpoint. Values are validated by the
     server, and the accepted set varies slightly across releases — the
     current implementation accepts the first five, while some published
     documentation lists `unenrolled` instead of `unhealthy`.
   - **Page size** (`page_size`): optional; default 100, which is also the
     documented ceiling.
3. [Create the Custom Integration task](https://console.runzero.com/ingest/custom/).
   - Select the Credential and Custom Integration created in steps 1 and 2.
   - Update the task schedule to recur at the desired timeframes.
   - Select the Explorer you would like the Custom Integration to run from.
   - Click `Save` to kick off the first task.

### Running it from the command line

```bash
runzero script --filename elastic-defend/elastic-defend.star \
  --kwargs url=https://my-deployment.kb.us-east-1.aws.elastic.cloud \
  --kwargs api_key=VGhpc0lzTm90QVJlYWxLZXk6QnV0SXRMb29rc0xpa2VPbmU \
  --custom-integration-id 1f2e3d4c-5b6a-7988-9a0b-1c2d3e4f5a6b \
  --output ./elastic-defend-run
```

`--output` writes the assets the run produced; add `--verbose` for the
request-by-request log. To check the `CONFIG` block and HTTP/TLS wiring
without a live server:

```bash
runzero script --filename elastic-defend/elastic-defend.star --validate
```

The fixtures under `elastic-defend/tests/fixtures/` exercise the import,
pagination, and failure paths offline:

```bash
python3 tests/run.py elastic-defend
```

### What's next?

- You will see the task kick off on the [tasks](https://console.runzero.com/tasks) page like any other integration.
- The task will update existing assets with data pulled from Elastic Defend.
- The task will create new assets when there are no existing assets that meet merge criteria (hostname, MAC, etc).
- You can search for assets enriched by this custom integration with the runZero search `custom_integration:elastic-defend`.

## Asset identity

- Target entity: a physical or virtual host (workstation, laptop, or server)
  running Elastic Agent with the Elastic Defend integration.
- Source ID field: `data[].metadata.agent.id` — the Elastic Agent ID.
- Documentation evidence:
  https://www.elastic.co/docs/api/doc/kibana/operation/operation-getendpointmetadatalist
  returns one `HostInfo` document per endpoint, and the companion single-item
  route `GET /api/endpoint/metadata/{id}` retrieves an endpoint **by agent
  ID** — the same value that appears as `metadata.agent.id` and
  `metadata.elastic.agent.id` in every list row. Kibana's own endpoint list
  keys its rows on `metadata.agent.id`. The list is served from the united
  metadata index, which holds exactly one current document per agent.
- Uniqueness scope: one Elastic deployment (Kibana). The value is a UUID
  assigned by Fleet at enrollment.
- Cardinality: one row per enrolled endpoint. Addresses, MACs, policy detail,
  and isolation state are nested in the single row, so nothing is
  many-to-one, and the united index cannot return two rows for one agent.
- Stability: survives rename, reboot, IP/MAC change, policy changes, and
  agent upgrades. Unenrolling and re-enrolling (or reinstalling) Elastic
  Agent issues a **new** agent ID — a genuinely new registration in Fleet's
  model; the previous asset simply stops being updated and ages out.
- Reuse behavior: not documented; as a v4 UUID the practical reuse risk is
  nil.
- Presence: required — it is the document key of the metadata index. A row
  that still arrives without one is skipped rather than given an invented ID.
- Final runZero ID: `elastic-defend:<kibana-host>:<agent-id>`. The lowercase
  hostname of the configured Kibana URL namespaces the deployment, so two
  deployments imported into one runZero organization cannot collide.
  Renaming the Kibana host re-identifies the estate on the next sync; the
  old assets age out.
- Missing-ID behavior: skip. The record is logged as
  `skipping endpoint with no agent id: hostname=<hostname>` and dropped. No
  random or synthesized ID is ever generated.
- Match behavior (set once in `CONFIG`): `no-mac-break no-ip-break
  no-name-break`. The agent ID stays authoritative for matching, but this is
  an EDR-managed population whose reported MAC, IP, and hostname churn
  constantly — wireless roaming, VPN adapters, MAC randomization, DHCP, and
  rename. Those dimensions must not disqualify a merge with an asset runZero
  already discovered by scan.
- Verdict: scoped authoritative.

## What gets imported

Everything comes from `GET /api/endpoint/metadata`, streamed to runZero one
endpoint at a time.

| Source field | runZero field |
| --- | --- |
| `metadata.agent.id` | `id` (namespaced) |
| `metadata.host.hostname`, `metadata.host.name` | `hostnames` (placeholders and IP-shaped names rejected) |
| `metadata.host.ip[]`, `metadata.host.mac[]` | `networkInterfaces` (loopback/link-local dropped; raw lists kept as attributes) |
| `metadata.host.os.Ext.variant`, falling back to `os.name`, `os.platform` | `os` |
| `metadata.host.os.version` | `osVersion` |
| `last_checkin` | `lastSeenTS` |
| `metadata.Endpoint.state.isolation` | `isolated` tag when true |
| everything below | custom attributes, prefixed `elastic_defend_` |

Custom attributes: agent ID/version/build, host ID (machine ID) and
architecture, host status and last check-in, endpoint enrollment status,
network isolation state (actual and configured), endpoint capabilities, OS
family/full/kernel/platform/type/variant, the applied Defend policy (ID,
name, status, versions), and the Fleet policy revisions from `policy_info`
(applied vs. configured agent policy, endpoint package policy) — a revision
gap shows an endpoint that has not picked up its latest policy.

This API carries no software inventory, no vulnerability findings, and no
hardware serial; nothing is invented for those.

## Pagination

The metadata list is page/pageSize paginated with a `total` count. The
published OpenAPI describes `page` as 1-based, but every shipped Kibana
implementation (and its own UI) uses 0-based pages. The script sidesteps the
discrepancy: the first request omits `page` entirely — the server returns its
first page under either convention — and each subsequent request asks for the
page number the previous response reported, plus one. The walk stops when the
rows seen reach `total`, when a page comes back empty, or at the CONFIG
`maxPages` backstop.

Kibana publishes no API rate limit for this route; transient 429/5xx answers
are retried with backoff by the shared HTTP helper.

## Failure behavior

- Authentication or privilege failure on the first request fails the task
  with the status and, for 403, a pointer at the missing Kibana privilege.
- A page failing mid-walk fails the task after reporting how many endpoints
  were already streamed — those are kept, and the error prevents the
  truncated walk from being filed as a complete estate.
- A row with no agent ID (or a non-object row) is skipped and counted, and
  the count is reported once at the end.
- Zero endpoints is reported as a successful, empty run.

## Documentation used

- Endpoint management API overview: https://www.elastic.co/docs/api/doc/kibana/group/endpoint-security-endpoint-management-api
- Get metadata list (parameters, response shape): https://www.elastic.co/docs/api/doc/kibana/operation/operation-getendpointmetadatalist
- Kibana API authentication (ApiKey scheme): https://www.elastic.co/docs/api/doc/kibana/authentication
- Elastic Security APIs index: https://www.elastic.co/docs/solutions/security/apis
- Kibana source (`GetMetadataListRequestSchema`, route registration, and the
  0-based page default), which is the contract of record where the published
  OpenAPI drifts: https://github.com/elastic/kibana — `x-pack/solutions/security/plugins/security_solution/common/api/endpoint/metadata/list_metadata.ts`

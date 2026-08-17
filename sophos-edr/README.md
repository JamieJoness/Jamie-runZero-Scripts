# Custom Integration: Sophos EDR (Sophos Central)

Imports computer and server endpoints from Sophos Central (Endpoint and Server
Protection / Intercept X with EDR/XDR) for asset enrichment in runZero.

## runZero requirements

- Superuser access to the [Custom Integrations configuration](https://console.runzero.com/custom-integrations) in runZero.
- An Explorer or hosted zone with outbound HTTPS access to `id.sophos.com`,
  `api.central.sophos.com`, and the regional API hosts (`api-*.central.sophos.com`).

## Sophos requirements

- A Sophos Central **API credential** (service principal) consisting of a
  **Client ID** and **Client secret**.
- The credential can be created at the tenant, organization, or partner level.
  The integration auto-detects which one it is and imports endpoints for every
  tenant the credential can reach:
  - **Tenant** credential: imports the single tenant's endpoints.
  - **Organization** credential (Sophos Central Enterprise): imports endpoints
    for all sub-estate tenants.
  - **Partner** credential (Sophos Central Partner): imports endpoints for all
    managed customer tenants.

## Steps

### Sophos configuration

1. Create an API credential (service principal) in Sophos Central:
   - **Tenant**: Sophos Central Admin > **Global Settings** > **API Credentials Management** > **Add Credential**.
   - **Organization**: Sophos Central Enterprise > **Settings & Policies** > **API Credentials**.
   - **Partner**: Sophos Central Partner > **Settings & Policies** > **API Credentials**.
2. Give the credential a name, then copy the generated **Client ID** and
   **Client secret**. The secret is only shown once.
3. Ensure the credential's role can read endpoints (Service Principal
   SuperAdmin, or a role with Endpoint read access).

### runZero configuration

1. **Create a Credential for the Custom Integration**:
   - Go to [runZero Credentials](https://console.runzero.com/credentials).
   - Select `Custom Integration Script Secrets`.
   - Enter your Sophos **Client ID** as `client_id`.
   - Enter your Sophos **Client secret** as `client_secret`.
2. **Create the Custom Integration**:
   - Go to [runZero Custom Integrations](https://console.runzero.com/custom-integrations/new).
   - Add a **Name and Icon** for the integration (e.g., "sophos-edr").
   - Toggle `Enable custom integration script` and paste in `sophos-edr.star`.
   - Click `Validate` and then `Save`.
3. **Schedule the Integration Task**:
   - Go to [runZero Ingest](https://console.runzero.com/ingest/custom/).
   - Select the **Credential and Custom Integration** created earlier.
   - Set a schedule for recurring updates.
   - Select the **Explorer** where the script will run.
   - Click **Save** to start the task.

## Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `client_id` | string | yes | Sophos Central API credential Client ID. |
| `client_secret` | secret | yes | Sophos Central API credential Client secret. |

Standard `http_` and `tls_` connection options are also available.

## Imported data

Endpoints are requested with `view=full`, so all available fields are imported.
The following are mapped to native runZero asset fields:

- **Asset ID**: `sophos:<tenant-id>:<endpoint-id>`
- **Hostname**: `hostname`
- **OS / OS version**: `os.name` (fallback `os.platform`) and the
  `majorVersion.minorVersion.build` triple.
- **Network interfaces**: `macAddresses`, `ip4Addresses`, and `ip6Addresses`.

Everything else Sophos returns is flattened into `customAttributes`, including
endpoint `type`, `tenant`, group, `health` (overall, threats, services),
`associatedPerson`, tamper-protection state, isolation and lockdown status,
encryption status, cloud instance metadata, assigned products, tags, and
last-seen timestamps.

Search for these assets in runZero using `custom_integration:sophos-edr`.

## Pagination and rate limits

- **Endpoints** use Sophos key-based pagination (`pageFromKey` / `pages.nextKey`);
  the integration stops when no `nextKey` is returned.
- **Tenant listing** (partner/organization) uses page-by-offset pagination.
- Each page is streamed to runZero with `report_assets`, so large multi-tenant
  estates are never buffered in memory.
- Requests use bounded retry with backoff and honor `Retry-After`, so Sophos
  `429` and `5xx` responses are retried automatically. Expired access tokens
  (`401`) trigger a single transparent token refresh.

## Known limitations

- Sophos returns MAC and IP lists without a documented pairing, so all IPs are
  attached to the first MAC and any remaining MACs are added as MAC-only
  interfaces.
- Only Endpoint/Server Protection endpoints are imported. Mobile devices
  (Sophos Mobile) are a separate API and are not included.
- A partner/organization credential with a very large number of tenants can
  approach the daily API quota; schedule accordingly.

## Asset identity

- Target entity: physical or virtual endpoint device (computer/server) running
  the Sophos agent.
- Source ID field: `id` (endpoint UUID).
- Documentation evidence: [Endpoint API — GET /endpoints](https://developer.sophos.com/docs/endpoint-v1/1/routes/endpoints/get);
  the `basic` view is documented as containing "only ID, type, tenant and
  hostname fields", so `id` is always present.
- Uniqueness scope: per tenant. Endpoints are queried with the `X-Tenant-ID`
  header and each item carries its own `tenant.id`.
- Cardinality: one endpoint object per device (one-to-one).
- Stability: the endpoint id is assigned at agent registration and is stable
  across reboots, IP/MAC/hostname changes, and agent upgrades.
- Reuse behavior: reinstalling/re-registering the agent produces a new id, which
  is the intended EDR behavior for a new registration.
- Presence: required; records without an `id` are skipped.
- Final runZero ID: `sophos:<tenant-id>:<endpoint-id>`.
- Missing-ID behavior: skip the record with a log message (no random fallback).
- Match behavior: `no-mac-break no-ip-break no-name-break` — the Sophos id
  drives matching while allowing MAC/IP/name to change as endpoints roam.
- Verdict: scoped authoritative.

## Documentation links

- Authentication and multi-tenancy: <https://developer.sophos.com/intro>
- Partner getting started (auth, whoami, tenant listing): <https://developer.sophos.com/getting-started>
- Endpoint API overview: <https://developer.sophos.com/docs/endpoint-v1/1/overview>
- Endpoint listing (pagination, `view`, fields): <https://developer.sophos.com/docs/endpoint-v1/1/routes/endpoints/get>

# Custom Integration: ManageEngine Endpoint Central (on-premises)

Inbound runZero custom integration for an **on-premises** ManageEngine Endpoint
Central server (formerly Desktop Central). It walks the Inventory API's
`scancomputers` view and imports every managed computer with its hostname, every
network adapter Endpoint Central knows about (MAC and IP, per adapter), and the
rest of the row as custom attributes.

This is a copy of the `manage-engine-endpoint-central` script from the
runZero custom integrations library, with two changes on top: **multi-NIC
machines are now imported with all of their MAC addresses** (see
[The multi-NIC fix](#the-multi-nic-fix) for what was wrong and why it matters),
and every epoch-millisecond timestamp gains a human-readable
[`<key>StandardTime`](#standardtime-attributes) attribute.
The older top-level `manage_engine_endpoint_custom_integration.star` in this
repository (hardcoded host, no `CONFIG` block) has the MAC bug and should be
considered superseded by this folder.

## On-prem only

Endpoint Central Cloud is **not** supported, and the quickest way to tell which
edition a script was written for is its host and auth header:

| | On-premises (this script) | Endpoint Central Cloud |
| --- | --- | --- |
| Host | the customer's own server, e.g. `https://ec.example.com:8383` | `endpointcentral.manageengine.com` (or `.eu`, `.in`, `.com.au`, `.jp`, `.ca`, `.uk`, `.cn`) |
| Inventory endpoint | `GET /api/1.4/inventory/scancomputers` | `GET /dcapi/inventory/viewData/scanComputers` |
| Authorization header | the on-prem auth token, sent verbatim | `Zoho-oauthtoken <access_token>`, from a Zoho OAuth 2.0 exchange, expiring hourly |
| Paging | `page` / `pagelimit` | `pageLimit` |

A cloud tenant answers this script with a 401. Endpoint Central **MSP** (API
1.3, `customerid` scoping) is not supported either.

## Requirements

- Superuser access to the [Custom Integrations configuration](https://console.runzero.com/custom-integrations) in runZero.
- An Explorer that can reach the Endpoint Central server over HTTP or HTTPS
  (defaults: 8020 for HTTP, 8383 for HTTPS).
- An Endpoint Central account with **read access to the Inventory module**. Of the
  built-in roles only Guest has it (Technician and Auditor do not); otherwise
  create a custom role under **Admin → User Administration → Add Role** with
  Inventory read.
- An on-prem auth token for that account:

  ```bash
  curl -s -X POST 'https://ec.example.com:8383/api/1.4/desktop/authentication' \
    -H 'Content-Type: application/json' \
    -d '{"username":"runzero","password":"'"$(echo -n 'ExamplePassw0rd' | base64)"'","auth_type":"local_authentication"}'
  ```

  Use `"auth_type":"ad_authentication"` plus `"domainName":"<domain>"` for an AD
  account. The token is at `message_response.authentication.auth_data.auth_token`.
  Confirm it works before touching runZero — note the header is the raw token,
  with no `Bearer` prefix:

  ```bash
  curl -s 'https://ec.example.com:8383/api/1.4/inventory/scancomputers?pagelimit=1&page=1' \
    -H 'Authorization: B42550F3-006D-48EB-8011-F6C7D6323EE7' -H 'Accept: application/json'
  ```

## Steps

1. [Create the credential](https://console.runzero.com/credentials) of type
   **Custom Integration Script Secrets**:
   - **Endpoint Central URL** (`url`) — the server origin including port, e.g.
     `https://ec.example.com:8383`. The script appends the API path itself.
   - **API auth token (on-prem)** (`oauth_token`) — the `auth_token` above, verbatim.
     The key is named `oauth_token` only so existing credentials keep working; it
     is not an OAuth token.
   - **Server scope** (`server_scope`, optional) — a label such as `ec-hq`. When set,
     foreign ids become `manage-engine-endpoint-central:<server_scope>:<resource_id>`,
     so two Endpoint Central servers imported into one runZero organization cannot
     collide on their small integer `resource_id`s. Leave empty for a single server.
     Setting or changing it re-identifies the integration's existing assets once.
   - **TLS options** (`tls_*`) — an on-prem server usually presents a certificate no
     public trust store contains; set `tls_ca_cert` rather than disabling validation.
2. [Create the custom integration](https://console.runzero.com/custom-integrations/new),
   paste `manage-engine-endpoint-central.star`, click **Validate**, then **Save**.
3. [Create the custom integration task](https://console.runzero.com/ingest/custom/)
   with that credential and integration, pick the Explorer, set the schedule, save.

To try it from a shell first, with the Explorer binary:

```bash
runzero script --filename manage-engine-endpoint-central/manage-engine-endpoint-central.star \
  --kwargs url=https://ec.example.com:8383 \
  --kwargs oauth_token=B42550F3-006D-48EB-8011-F6C7D6323EE7 \
  --kwargs tls_ca_cert=/etc/runzero/ec-ca.crt \
  --custom-integration-id <uuid> --output ./ec-run
```

## What is imported

| Endpoint Central field | Lands on the asset as |
| --- | --- |
| `resource_id` (fallback `id`) | foreign id, optionally scoped by `server_scope` |
| `resource_name` | hostname |
| `mac_address`, `ip_address` | one network interface **per adapter**; see below |
| `agent_installed_on`, `agent_last_contact_time`, `last_sync_time`, `last_successful_scan`, `computer_status_update_time`, `process_start_time`, `process_end_time` (epoch milliseconds) | custom attributes as reported, **plus** a `<key>StandardTime` sibling in human-readable UTC; see below |
| everything else on the row (`os_name`, `os_version`, `domain_netbios_name`, `agent_version`, `scan_status`, `servicetag`, …) | custom attributes, each truncated to 1023 characters |

Pagination walks `page`/`pagelimit=1000` until a short or empty page, stops if
the server repeats a page, and is capped by `CONFIG["maxPages"]`. Rows that are
not objects or have no `resource_id`/`id` are skipped with a log line.

### StandardTime attributes

Endpoint Central reports every timestamp as Unix epoch **milliseconds**, which
is unreadable in the asset view and unusable in a report without conversion.
Each such value is kept as-is and also rendered into a sibling attribute named
`<key>StandardTime`, formatted `DD-MM-YYYY HH:MM:SS UTC`:

| attribute | value |
| --- | --- |
| `agent_last_contact_time` | `1792403674000` |
| `agent_last_contact_timeStandardTime` | `19-10-2026 09:54:34 UTC` |

A value is treated as a millisecond timestamp when it is a 13-digit integer or
digit string. That range covers 2001–2286, so the `-1` and `--` placeholders
Endpoint Central writes for "never", status codes, counts and build numbers are
never converted. The conversion is always to UTC regardless of the Explorer's
local timezone, and a future-dated value is shown as reported rather than
clamped — these are display attributes, not the asset's first/last-seen times.
These are the same attribute names and format the earlier script in this
repository produced, so existing searches and reports keep working.

## The multi-NIC fix

Endpoint Central returns `mac_address` and `ip_address` as **comma-joined,
per-adapter lists** in adapter order, with the literal `--` holding the place of
a missing value:

```json
"ip_address":  "10.204.14.62,192.168.56.1,--",
"mac_address": "f8:75:a4:12:34:56,0a:00:27:00:00:0e,--"
```

Any machine with more than one adapter — wired plus Wi‑Fi, a VPN tunnel, a
Hyper‑V or VirtualBox host NIC — comes back this way.

- **The original script** handed the whole string to `NetworkInterface` as one
  MAC. The platform rejects that asset outright
  (`asset Network Interface MAC address "…,…" is invalid`). On a real server
  that was **1,254 of 1,909 devices dropped (66%)** — every machine with two or
  more NICs; only single-NIC machines imported.
- **The library version** passes the string to `net.network_interface()`, which
  does not reject the asset but *silently drops* an invalid MAC. The errors
  disappear, yet the same two thirds of the fleet import with **no MAC at all**
  and correlate on nothing stronger than a DHCP address and a hostname.
- **This version** splits both fields per adapter. When the two lists are the
  same length (how Endpoint Central emits them) MAC[i] is paired with IP[i], so
  each interface carries its own address. If the lengths ever disagree, every
  address goes on the first MAC-bearing interface and the remaining MACs become
  MAC-only interfaces — nothing is lost, only the per-adapter pairing. `--`
  tokens are dropped by the helpers, a row of only `--` (an agent enrolled but
  not yet inventoried) produces no interface rather than an empty one, and a
  malformed token such as `unknown` no longer ends the run.

Replaying the shapes of all 1,254 rows that failed in that run through this
script imports all 1,254, retaining all 2,572 MAC addresses, with no validation
errors.

One thing that looks surprising but is expected: the platform stores MACs in
its canonical, locally‑administered‑bit‑cleared form, so a VirtualBox host NIC
reported as `0a:00:27:00:00:0e` appears on the asset as `08:00:27:00:00:0e`.
That is runZero's cross-source matching form, not a parsing error.

### Checking a task

In a task's log export, the old failure is the `error`-level line
`error validating or processing starlark result, skipping: invalid custom
integration asset: asset Network Interface MAC address "<mac>,<mac>" is
invalid`, one per dropped device, and `custom integration […] reported N assets`
will be well short of the server's `total`. With this version the two numbers
match and there are no such lines.

## Correlation

Assets carry every adapter's MAC and address plus `resource_name` as the
hostname, so the platform's default match behaviour (all match and break
dimensions on) applies. `resource_id` is stable for the life of the inventory
record; a machine removed from Endpoint Central and re-enrolled gets a new one,
and will appear as a new asset until the old one ages out.

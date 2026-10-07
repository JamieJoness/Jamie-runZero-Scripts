# Aruba AirWave 8.3.0.6

Inbound runZero custom integration for an **on-premises** AirWave server. Runs on
an Explorer with network access to that server. The integration is based on the
HPE Aruba Networking Management Software (AirWave) 8.3.0.6 API Guide supplied
with this request; it has not been tested against a live AirWave appliance.

Default collection includes managed-device inventory, AP details, wired
interfaces, radio/BSSID/SSID metadata, connected wireless clients, recent client
associations, folder paths, controller/upstream references, VisualRF AP placement
and active device alerts. Rogue radio assets, topology health and AP logs are
separately enabled because they can increase inventory scope or collection cost.

## Setup

1. Use a runZero Console and selected Explorer that support v2 custom
  integrations, with an Explorer version of **5.1.260818.0 or later**. This
  script uses CONFIG, typed parameters, streaming and the TLS socket helper.
2. Place the Explorer where it can reach the on-premises AirWave server over
  HTTPS, normally TCP 443. This is not an Aruba Central or cloud integration;
  the runZero Console itself may be SaaS or self-hosted. Do not expose AirWave
  publicly just to run this integration.
3. Create a dedicated AirWave account with read access to the intended inventory
  folders and enabled API features. Use least privilege. The supplied guide
  demonstrates an admin login but does not define the minimum role for every
  endpoint; confirm read access on the appliance. RAPIDS/VisualRF permissions
  may be needed for their respective enrichment. Browser-only SSO/MFA login
  flows are not implemented.
4. Create an inbound custom integration using
  [aruba-airwave.star](aruba-airwave.star), following the
  [runZero custom integration instructions](https://help.runzero.com/docs/custom-integration-scripts/).
  The embedded CONFIG supplies the credential form. No Python installation or
  external packages are needed on the Explorer.
5. Set `url`, a permanent `instance_id`, `username`, and the original `password`.
  The script detects whether AirWave requires Base64 password obfuscation. Do
  not pre-encode the configured password. Both credential fields are secret.
6. Keep TLS verification enabled. Supply the issuing CA PEM in `tls_ca_cert`
  when AirWave uses a private CA. Shared options also support certificate
  pinning (`tls_peer_hash`) and mTLS (`tls_client_cert`, `tls_client_key`).
7. Start with a small `folder_id`, run a live test on that Explorer, and compare
  the imported device/client counts and a sample of `airwave.*` attributes
  with AirWave. Review task warnings before scheduling regular runs.

Use a different `instance_id` for each AirWave database, including independently
running restored copies. Keep it unchanged when the same database's URL changes.

## Parameters

| Parameter | Default | Purpose |
| --- | --- | --- |
| `url` | Required | HTTPS origin only, such as `https://airwave.example.com:443` |
| `instance_id` | Required | Permanent namespace: 1-64 lowercase letters, digits, `_` or `-`; first character must be alphanumeric |
| `username`, `password` | Required secrets | AirWave form-login credentials |
| `folder_id` | Empty | Passed as `ap_folder_id` to the primary listing |
| `group_id` | Empty | Passed as `ap_group_id` to the primary listing |
| `controller_id` | Empty | Controller filter for the primary listing |
| `request_timeout` | `60` | Seconds per request, 1-900; includes login |
| `batch_size` | `25` | 1-100 IDs/MACs per detail request; not vendor pagination |
| `max_child_records` | `32` | 1-99 retained rows per repeated attribute group |
| `history_limit` | `5` | 1-20 recent client associations, rogue discovery events and AP logs |
| `include_bssids` | `true` | Radio BSSIDs, SSIDs and client-use flags |
| `include_clients` | `true` | Client assets enumerated from associated clients in AP Detail |
| `client_search_limit` | `1000` | Extra exact-MAC search requests for SSID/VLAN/user/OS; zero disables these calls, not client assets |
| `client_search_query` | Empty | Optional UI-tested query to discover/enrich additional clients |
| `include_visualrf` | `true` | Campus, building, floor, AP placement, antenna and site-summary attributes |
| `include_alerts` | `true` | Active alerts linked by device ID, not display name |
| `include_rogues` | `false` | Opt-in MAC-only assets for observed rogue radios |
| `include_ignored_rogues` | `false` | Adds documented `include=ignored` to AP Detail requests |
| `include_topology` | `false` | Topology node role, folder and health/reason metadata |
| `include_logs` | `false` | Recent per-device log messages and associated AirWave users |
| `ap_search_query` | Empty | UI-tested query for additional AP configuration/monitoring metadata |
| `http_user_agent` | Shared default | User-Agent for HTTP requests and login |
| `tls_*` | Shared defaults | CA, pinning, mTLS and certificate-verification controls |

All enabled options remain read-only except `POST /LOGIN`, which creates an
authenticated session. No device configuration, delete/move, allowlist, VisualRF
modification or certificate-install APIs are called. The script does not require
a runZero API token: the Explorer streams results directly into the integration.

## Imported data

Native fields include validated hostnames and network interfaces, device
manufacturer/model, managed-device last-contact time, client OS text when
available, a disconnected client's latest available disconnect time, and rogue
first/last discovery times. Serials and firmware remain
attributes; firmware is not guessed to be an OS version, and a radio vendor is
not assumed to be the endpoint manufacturer.

| Attribute family | Enrichment |
| --- | --- |
| `airwave.*` on managed devices | AirWave ID and UI link, name, manufacturer/model/serial, firmware, category, up/down state, monitor-only/maintenance/remote flags, reboot/contact/uptime, SSIDs, SNMP contact/location and remote addresses |
| `airwave.folder.*`, `airwave.group.*` | Source folder/group IDs and names; resolved folder path when available |
| `airwave.controller.*`, `airwave.upstream.*` | Names, addresses and hardware references when the related device is in the selected inventory |
| `airwave.radio.*`, `airwave.bssid.*` | Channel, type, role, operating mode, power, antenna details, radio MACs, BSSID/SSID associations and client-use flags |
| `airwave.interface.*` | Wired port identity, name, description, alias, MAC, port index, admin/operational state and bandwidth |
| `airwave.detail.radio.*`, `airwave.neighbor.*` | AP-side client and neighbour observations, signal/RSSI/SNR, authentication, roles, security and rogue/managed counts |
| `airwave.search.*` | Client SSID, VLAN, username, role, guest status, OS/device labels and other returned scalar search fields; optional AP configuration and monitoring status |
| `airwave.observation.*`, `airwave.current.*` | Client/rogue reporting AP and radio context; current LAN/VPN values from client detail |
| `airwave.association.*` | Bounded association history, times, byte counts, AP references and historical LAN/VPN metadata |
| `airwave.visualrf.*` | Campus/building/site names and IDs, floor, units, AP coordinates, antenna/radio placement and site summary |
| `airwave.associated_ap.visualrf.*` | The client's associated AP location, not a claim about the client's precise physical location |
| `airwave.alert.*` | Device-linked alert IDs, type, summary/message, creation time, severity, viewed state and reference URL |
| `airwave.topology.*` | Optional node role, folder and health with indexed reason attributes |
| `airwave.log.*` | Optional recent device log dates, messages and users |
| `airwave.discovery_event.*` | Optional rogue discovery times, SSID/security/signal, discovering AP and radio |

The script retains useful scalar values from these read endpoints and filters
credential-like field names. It uses XML text, not HTML-bearing `display_value`
attributes. Indexed groups start at zero. A separate `.count` describes the
available rows, and `.truncated` indicates omitted rows.

No services, installed-software inventory or CVE findings are invented from
firmware, open SSIDs or alert severity. Tags, ownership and trusted classification
are not changed automatically. Client usernames, SSIDs, locations and log text
can contain sensitive operational information; restrict access to the resulting
inventory accordingly.

## Asset identity

- Target entity: managed network device; connected wireless interface; optionally
  a discovered rogue radio. A radio observation is not proof of a distinct
  physical access point.
- Source ID fields: managed `<ap id>`, client `<client mac>`, and rogue
  `<rogue_ap id>`/`<radio_mac>`.
- Documentation evidence: supplied API Guide pp. 13-22 documents list/detail
  joins by AP ID, clients by MAC, and rogue IDs obtained from AP Detail. It does
  not guarantee persistence or non-reuse of database IDs.
- Uniqueness scope: one AirWave database. The required `instance_id` is an
  operator-assigned permanent namespace, not the server URL or a folder ID.
- Cardinality: one managed asset per LAN MAC, or serial/manufacturer when its MAC
  is unavailable; one client
  per observed MAC; one optional rogue record per radio MAC. Repeated child
  rows are enrichment, not independent assets.
- Stability: the reported LAN MAC remains deterministic when AirWave record IDs,
  names, IPs, serial labels, folders or the server URL change. Hardware replacement,
  a previously missing MAC appearing and MAC randomisation can change the derived key.
- Reuse behaviour: AirWave database ID reuse is unspecified. Serials and MACs
  are not assumed globally unique or immutable.
- Presence: a managed record needs its AP ID and a usable serial or LAN MAC.
  Clients and rogue radios need a usable MAC. Every emitted asset must also
  have a valid MAC, IP or hostname for correlation.
- Final runZero IDs: `aruba-airwave:<instance_id>:device:mac:<mac-without-colons>`,
  falling back to `aruba-airwave:<instance_id>:device:serial:<sha256-key>`,
  where the hash covers the JSON-encoded `[lowercase manufacturer, serial]` pair;
  clients and rogue radios use separate `client:mac:` and `rogue:mac:` prefixes.
- Missing-ID behaviour: skip records without the required identifiers; never
  generate random IDs or use an association, event, interface or session ID.
- Match behaviour: `no-id-match no-id-break`. The derived ID neither drives nor
  blocks matching; runZero uses the imported network identifiers and hostnames.
- Verdict: **derived/non-authoritative** for all populations. The guide does not
  justify treating the raw local AP or rogue database ID as authoritative.

The native MAC helper performs runZero's normal MAC canonicalisation; ID keys
retain the MAC's originally reported locally administered bit. This prevents the
script from deduplicating two different reported MACs merely because runZero
normalises that bit. Console correlation still uses runZero's matching rules;
fixture tests verify emitted IDs, not the outcome of a customer inventory merge.

Multiple managed rows with the same derived key emit only the first record. A
client observed on several APs emits one MAC-scoped record with bounded
observations. Independent radios on one rogue AP cannot be reliably combined
into one physical asset using this guide, so rogue import is explicitly opt-in.

MAC randomisation, cloned MACs, reused IPs and incomplete source data still
limit correlation. There is no claim that these identifiers globally identify a
physical device. Missing required identity is skipped and counted for managed
records; malformed client/rogue observations without usable MACs are ignored.

## Mapping plan

| AirWave field or object | runZero destination | Conversion |
| --- | --- | --- |
| LAN MAC, else manufacturer + serial | `id` | Deterministic, instance-scoped key |
| Device name | `hostnames` and `airwave.name` | Validated hostname; retain display name |
| LAN IP/MAC | `networkInterfaces` | Normalised; reject placeholder addresses |
| Manufacturer, model | Native hardware fields | Preserve source strings |
| Serial number | `airwave.serial_number` | Preserve source string |
| `last_contacted` | `lastSeenTS` | Parse epoch; reject invalid values |
| Firmware, status, folders, groups, radio details | `airwave.*` attributes | Bounded string attributes |
| Remote/NAT addresses, BSSIDs, historical addresses | Attributes only | Never use as device correlators |
| Client associations, interfaces and discovery events | Parent enrichment | Deduplicate; do not create child assets |

Authentication uses the documented obfuscation-status endpoint, URL-encoded
`POST /LOGIN`, the session cookie and `X-BISCOTTI` header (Guide pp. 6-7).
Read APIs use GET, not the deprecated XML POST interface (pp. 8-9).

The runZero HTTP helper follows redirects without exposing intermediate headers.
Login therefore uses the existing TLS socket helper to send one form-encoded
request and read at most 64 KiB of response headers. It never follows a login
redirect, reads the login body, or replays credentials to a redirect target.
The configured CA, TLS pin, client certificate and timeout apply to this socket
as well as the regular HTTP requests. A future HTTP helper with redirect control
would remove the need for this narrowly scoped header exchange.

## Collection limits

- The guide documents complete query results and filters, not offset/cursor
  pagination for these endpoints. The script never invents a page parameter.
  Detail queries use repeated `id=`/`mac=` values, with CONFIG `maxPages=10000`
  guarding each batch walk. Exceeding the guard fails the task, not a silent
  partial success.
- Primary inventory is exactly what the appliance returns from `ap_list.xml`
  within the account's permissions and selected filters. Confirm which managed
  device categories your appliance exposes; topology enriches matching IDs,
  rather than introducing speculative extra devices.
- Client enumeration is based on AP Detail's associated clients. This does not
  claim complete wired, VPN-only or historical-client inventory. The guide's
  Client Search endpoint requires a query but does not define an all-client
  wildcard or paging contract. Optional user-supplied queries must be tested in
  AirWave first; results are restricted to selected current AP IDs when filters
  are active. Per-MAC search results are also checked for an exact MAC match.
- Current associated-client detail supplies native LAN addresses. If absent, a
  single unambiguous AP-observed address may be used. Disconnected clients are
  MAC-only, with the latest available disconnect timestamp when present. A
  missing observation timestamp is left unset, not fabricated. Historical, VPN,
  remote/NAT, topology-only, neighbour and BSSID
  addresses remain metadata; they cannot accidentally identify the parent AP.
- Global folder, VisualRF and alert responses may cover more than the selected
  folder. Only matching managed IDs are enriched. Planned VisualRF APs are not
  imported. Rogue radios may belong to neighbouring organisations.
- For large estates, use folder/group filters and a small detail batch. Whole
  list responses and deduplication indexes still occupy memory even though
  assets are reported individually. Runtime download/memory/deadline budgets
  continue to apply; there is no disk checkpoint or resume support.
- Topology can be expensive: the guide warns of roughly six minutes for more
  than 2,500 devices. Use a folder and consider `request_timeout=420`. Logs and
  per-client searches also add requests; `client_search_limit` bounds search
  work without capping the client inventory itself.
- Each asset keeps at most 1,022 ordinary custom attributes plus two truncation
  flags, with values capped at 1,024 characters and native interfaces capped at
  99. Important identity/status fields precede optional detail. Large nested
  inventories may not fit every source attribute; inspect
  `airwave.attributes_truncated`, `airwave.attribute_values_truncated`, child
  `.truncated` flags and `airwave.network_interfaces_truncated`.
- UTF-8 and ISO-8859-1 XML are supported, including namespaces, escaped text and
  empty elements. Unexpected roots and obviously incomplete XML fail a required
  fetch or warn for optional enrichment. The runtime parser is tolerant, not a
  strict XSD validator.

## Failure handling

Login, obfuscation-status and primary-listing failures fail the task. An empty
valid listing succeeds with zero assets. A 401, 403 or login HTML page causes
one reauthentication and replay; continued authentication rejection fails.
An optional endpoint returning 403/404/405 after applicable retry is disabled for
the rest of the run and logged once, preserving primary inventory.

Idempotent GETs retry transient HTTP statuses at most three times after the
initial attempt, with 1/2/4-second backoff and numeric or HTTP-date `Retry-After`.
A requested delay longer than 60 seconds stops retries rather than retrying too
early. Persistent optional 429 responses pause further optional API work. Raw
HTTP transport/TLS failures terminate through the runtime; they are not retried
by the script. Login itself is not blindly retried on transient failure.

Session cookies and tokens remain in memory, are refreshed when supplied by the
server, and are never deliberately logged or included in attributes. Response
bodies are not included in task error messages. No undocumented logout endpoint
is used; AirWave controls session expiration. A failed run may already have
streamed assets, which runZero retains; failure is not a transaction rollback.

## Documentation sources

The supplied **HPE Aruba Networking Management Software (AirWave) 8.3.0.6 API
Guide**, copyright 2025, is the vendor contract used for this implementation.
It was provided as an attachment, not a public document URL. Obtain the matching
guide through [HPE Aruba Networking Support](https://networkingsupport.hpe.com/)
when checking a different release. No older or Aruba Central API contract was
substituted.

| Guide pages | Used for |
| --- | --- |
| 6-7 | Obfuscation check, form login, session cookie and X-BISCOTTI |
| 8-9 | GET queries, repeated IDs/MACs, history limit and search value semantics |
| 10-12 | Folder hierarchy and alert schema |
| 13-18 | AP list, filters, BSSIDs, AP details and logs |
| 19-23 | Rogue IDs/details and client details/associations |
| 23-24 | Topology nodes, folder filtering and performance caveat |
| 30-32 | AP/client search enrichment |
| 35-39, 42 | VisualRF hierarchy, optional includes and AP placement |

The guide contains copy/paste errors, such as AP Detail/BSSID filter examples
pointing at `ap_list.xml`, and malformed sample XML/JSON. The script uses each
endpoint's stated URL and well-formed representative fixtures. Vendor response
coverage, permissions, supported ID batch sizes, session behaviour and filter
semantics still need confirmation on the actual appliance.

## Development validation

The tests reuse the sibling `runzero-custom-integrations` repository's HTTP
fixture harness without copying or modifying it. Run with a current development
runZero CLI:

```sh
PYTHONDONTWRITEBYTECODE=1 RUNZERO_SCANNER=/tmp/rumble-scanner \
  python3 -m unittest discover -s aruba-airwave/tests -v
```

Run this command from `Jamie-runZero-Scripts`. Python 3 and OpenSSL with
`req -addext` support are required for tests only. Set
`RUNZERO_CUSTOM_INTEGRATIONS` if the reference repository is not its sibling.
The tests create temporary certificates and scan exports and clean them up. The
local adapter adds trusted HTTPS, repeated Set-Cookie headers and correct CLI
CSV quoting for multiline PEM values without modifying the shared harness.

The fixture suite exercises real Starlark execution and asset serialization,
including repeat polls, instance scoping, changed AirWave IDs/IPs/names,
duplicate serials and client observations, missing identity, current versus
historical addresses, login rejection, session renewal, cookie/token rotation,
CA verification, rate limits, legacy encodings and attribute/interface caps.

Generic `runzero script --filename aruba-airwave.star --validate` currently fails
at the obfuscation check because its synthetic server does not return AirWave's
`is_cred_obfuscation` response. This is not bypassed with `validationMode=compile`
or a fake successful login. Use the AirWave-specific fixtures and a live
Explorer test instead.

The official repository catalog generator is intentionally not run here: it
rewrites a repository-wide README/catalog and hard-codes links to the official
library, where this standalone integration does not exist. All deliverable
files remain in this directory, with no changes to the integration library or
platform source.
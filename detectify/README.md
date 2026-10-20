# Custom Integration: Detectify

Inbound runZero custom integration that imports the **external attack surface
discovered by Detectify Surface Monitoring**: every IP address Detectify has
seen a monitored domain resolve to, with the domains that point at it, its ASN
and geolocation, the ports found open on it, the technologies fingerprinted
behind it, the Surface Monitoring policies in breach on it, and the
vulnerabilities Detectify has found on the hosts it serves.

Detectify watches the estate from the internet, so what it knows about is
*addresses and names*, not machines. The integration is built around that:
each public IP address becomes one runZero asset, and everything Detectify
reports about the domains behind that address is folded onto it. The asset
correlates with what runZero already knows about the address — an external
scan, a cloud instance's public IP, another outside-in source — purely on the
address, which is the one signal Detectify and runZero genuinely share. See
[Asset identity](#asset-identity) for the reasoning behind every merge flag.

Each address becomes one runZero asset with:

- the address as its network interface and the domains that resolve to it as
  hostnames (up to the platform's 100);
- `firstSeenTS` from the earliest `first_seen_at` across its `/ips` rows;
- one **service** per open port (`GET /rest/v3/ports`), carrying the domains
  the port was observed on, the protocol and technologies fingerprinted on it,
  and the web server as the service product when one is identified. A port
  that only appears as the port of a fingerprinted technology is also a
  service, marked `source: technology`;
- one **software** row per distinct technology / version / port / protocol
  (`GET /rest/v3/technologies`), with its Wappalyzer-style categories and the
  domains it was seen on, so the same nginx behind twenty names is one row;
- one **vulnerability** per active (NEW or REGRESSION) policy breach
  (`GET /rest/v3/breaches`), with the policy severity as the severity rank;
- one **vulnerability** per active, new, or regressed finding from the v2
  vulnerabilities API (`GET /rest/v2/vulnerabilities/`), with CVSS 2.0 and
  3.x base scores, severity, CWE, OWASP category, CVE when the finding names
  one, the affected URL and port, first/last detected times, scan source,
  tags, and a link to the finding in Detectify;
- custom attributes for the address: ASN number/name/domain, continent and
  country, active/enriched flags, first seen and disappeared times, the domain
  list and counts, Detectify asset and team ids, open ports, technology
  categories, breach and vulnerability counts with the highest severity and a
  per-severity breakdown, and a summary of the Surface Monitoring settings
  (monitoring, port scanning, fingerprinting, findings, subdomain takeover,
  SSL audit, TLS discovery) of the assets behind the address.

## runZero requirements

- Superuser access to the [Custom Integrations configuration](https://console.runzero.com/custom-integrations) in runZero.
- An Explorer running **5.1.260818.0 or later** with outbound HTTPS to `api.detectify.com`. The Detectify API is a public SaaS endpoint, so a hosted Explorer works.

## Detectify requirements

- A Detectify account with **Surface Monitoring** data. The v3 attack surface
  API (`/rest/v3`) serves the IP addresses, ports, technologies, breaches, and
  monitoring settings; the v2 API (`/rest/v2`) serves the vulnerabilities.
  Detectify documents both versions as active and supported.
- An **API key** generated under **Settings > API keys** in the Detectify app.
  Detectify documents that a key inherits the teams and assets of the user who
  created it, so create it from a user that can see every team you want
  imported, and nothing more. The integration only issues `GET` requests.
- The v3 OpenAPI document describes the key as one *"generated specifically
  for the V3 API"* and sent in the `Authorization` header, while the v2 API
  documents the `X-Detectify-Key` header. The integration sends each API its
  documented header with the same key. If your account issues distinct keys
  per API version, put the v2 key in the optional **API key for the v2 API**
  parameter; otherwise leave it blank.

## Steps

### Detectify configuration

1. Sign in to Detectify and open **Settings > API keys**.
2. Click **Generate new key**, name it (for example "runZero"), and copy it — it is shown once.
3. Optionally confirm it from any host with internet access:
   `curl -H 'Authorization: <key>' 'https://api.detectify.com/rest/v3/ips?limit=5'`
   should return a JSON document with `items` and `pagination`, and
   `curl -H 'X-Detectify-Key: <key>' 'https://api.detectify.com/rest/v2/vulnerabilities/?pageSize=5'`
   a document with `vulnerabilities` and `has_more`. A 401 on the second call
   with a key that passed the first is the case the optional v2 key exists for.

### runZero configuration

1. [Create the Custom Integration](https://console.runzero.com/custom-integrations/new).
   - Add a Name and Icon for the integration (e.g., "Detectify").
   - Toggle `Enable custom integration script` and paste [detectify.star](detectify.star).
   - Click `Validate` to ensure it has valid syntax.
   - Click `Save` to create the Custom Integration.
2. [Create the Credential for the Custom Integration](https://console.runzero.com/credentials).
   - Select `Custom Integration Script Secrets` as the type.
   - Pick the integration you just created and fill in the form it renders:

   | Parameter | Required | Default | Purpose |
   | --- | --- | --- | --- |
   | `url` | no | `https://api.detectify.com` | Base URL of the Detectify API. `/rest/v3` and `/rest/v2` are appended by the script; a pasted versioned URL is reduced to the host. Override only to route through a proxy. |
   | `api_key` | **yes** | — | The Detectify API key. Sent as `Authorization` to v3 and, unless `api_key_v2` is set, as `X-Detectify-Key` to v2. Never logged. |
   | `api_key_v2` | no | — | A separate key for the v2 vulnerabilities API, only if the primary key is refused there. |
   | `team_ids` | no | all | Comma-separated Detectify team ids to import. Rows from other teams are skipped and counted. |
   | `include_ports` | no | `true` | Attach open ports as services. |
   | `include_technologies` | no | `true` | Attach fingerprinted technologies as software. |
   | `include_breaches` | no | `true` | Attach active policy breaches as vulnerabilities. |
   | `include_vulnerabilities` | no | `true` | Attach v2 vulnerabilities as vulnerabilities. |
   | `include_monitoring_settings` | no | `true` | Summarize Surface Monitoring settings per address. |
   | `include_inactive` | no | `false` | Also import addresses and technologies Detectify no longer sees (`active=false`). Off by default so a retired address does not keep attaching stale data to a live asset. |
   | `include_private_ips` | no | `false` | Also import RFC 1918, CGNAT, and ULA addresses that public DNS records point at. Off by default because a private address is not unique across sites. |
   | `http_*`, `tls_*` | no | — | The standard HTTP (timeout, proxy, user agent) and TLS options. |

3. [Create the Custom Integration task](https://console.runzero.com/ingest/custom/).
   - Select the Credential and the Custom Integration you created.
   - Choose the Explorer, site, and schedule. A daily schedule matches Detectify's own discovery cadence.
   - Consider enabling **Exclude unknown assets** on the task if you want
     Detectify to only *enrich* addresses runZero already knows about rather
     than create new assets for the rest of the attack surface. See
     [What's next](#whats-next).
   - Click `Activate`. The task runs on the schedule you chose.

### Running it from the command line

With the runZero CLI (`runzero`), the script can be exercised without a console:

```bash
runzero script --filename detectify/detectify.star --validate
runzero script --filename detectify/detectify.star \
  --kwargs api_key=<key> --kwargs include_inactive=false
```

Validation checks the `CONFIG` block and the HTTP and TLS wiring against a
local dummy server; it passes with one request and zero assets because the
dummy server does not model the Detectify responses. The fixtures below are
the real offline validation.

### Fixture tests

The scenarios under `detectify/tests/fixtures/` run the real runZero CLI
against a local HTTP server that replays Detectify-shaped responses, reusing
the fixture harness from the sibling `runzero-custom-integrations` checkout
without copying or modifying it. Run from `Jamie-runZero-Scripts`:

```bash
export PYTHONDONTWRITEBYTECODE=1 RUNZERO_SCANNER=/tmp/rumble-scanner
python3 detectify/tests/run.py            # every scenario
python3 detectify/tests/run.py happy      # one scenario
```

Set `RUNZERO_CUSTOM_INTEGRATIONS` when that repository is not a sibling
directory. The scenarios cover: the per-address identity and every mapping
(`happy`); cursor pagination with an address spanning two pages and a row
repeated across pages (`paged`); a refused key ending the task in error
(`auth-failure`); a page failing mid-walk (`midwalk-failure`); every
enrichment listing failing in a different way while the run still succeeds
(`optional-failures`); the team, inactive, and private-address switches
(`filters`); every enrichment switch off with no request made for them
(`switches-off`); a 429 with `Retry-After` followed by recovery
(`rate-limit`); rows with every field mis-typed (`malformed`); an empty estate
(`empty`); a separate v2 key and a pasted versioned URL (`v2-key`); and an
address with more services and breaches than the platform cap, proving the
most severe findings and lowest ports survive the cut (`child-cap`). Every
scenario that imports assets is run twice and the emitted ids must match.

The script also passes the platform's compatibility loader test
(`TestCompat_AllShippedIntegrationsLoad` with
`RZ_CUSTOM_INTEGRATION_SCRIPTS_DIR` pointed at this directory).

### What's next?

- You will see the task kick off on the [tasks](https://console.runzero.com/tasks) page like any other integration.
- The task will update existing assets that carry an imported address — an
  external scan result, a cloud instance with that public IP, another
  outside-in source — with the Detectify data, and create new assets for
  addresses runZero has not seen. With **Exclude unknown assets** enabled on
  the task, only the first happens.
- Search for assets enriched by this integration with
  `custom_integration:<the name you gave it>`; for a technology with
  `software:nginx`; for the policy breaches and findings in the vulnerability
  views, where every Detectify finding carries its Detectify severity, CVSS
  scores, and a `detailsPage` link back to Detectify.

## Asset identity

- Target entity: an **IP address on the external attack surface** — the host
  that answers at a public address Detectify saw a monitored domain resolve
  to. Detectify's own first-class objects (domains, sub-domains, IP ranges)
  are *names and scopes*, not hosts; runZero's asset is the host.
- Source ID field: **none exists for the host.** `GET /rest/v3/ips` returns one
  row per *domain-to-address association* (`id`, `ip_address`, `domain_name`,
  `asset_id`, `team_id`), so a single address appears once per domain that
  resolves to it, each row with its own `id`. The row id identifies the
  association, the `asset_id` identifies the domain asset, and neither is
  one-to-one with the address. The script therefore composes
  `detectify:ip:<canonical address>`, for example `detectify:ip:203.0.113.10`
  or `detectify:ip:2001:db8::10`.
- Documentation evidence: the v3 OpenAPI document (`IPAddress` schema and
  `GET /ips`: "all the IP addresses which have been discovered on your attack
  surface ... what domain and asset it is connected to"), and the
  `Port`, `Technology`, and `Breach` schemas, every one of which is a child
  record keyed back to a domain (`domain_name` / `asset_name`) or address.
- Uniqueness scope: **global**, because the id is a public address and the
  script refuses non-routable addresses outright and private ones by default.
  No tenant scope is added: two Detectify teams watching the same public
  address describe the same host, and their rows are folded into one record
  (both teams are listed in `teamIds`). With `include_private_ips` on, the
  scope of an RFC 1918 address is whatever network the DNS record meant, which
  is why that switch is off by default.
- Cardinality: **many source rows to one asset.** Every `/ips` row for an
  address, including a duplicate row served across two pages, lands on the
  same asset. Ports, technologies, breaches, and vulnerabilities are attached
  to the address(es) of the domain they name and are never assets themselves.
  A technology on a domain that resolves to three addresses is attached to all
  three — it is reachable on all three.
- Stability: **as stable as the address.** The id points at whatever host
  holds the address today, which is exactly what an outside-in source
  observes. Renames, new domains, re-fingerprinting, and new findings do not
  move it.
- Reuse behavior: **guaranteed, by construction.** A cloud address released by
  one workload and reissued to another produces the same id for a different
  machine. Detectify marks the old domain association `active=false` when the
  DNS record goes, and the script skips inactive rows by default, so the
  record follows the address only as long as a monitored domain still points
  there.
- Presence: `ip_address` is required on every documented row. A row whose
  address is missing, unparseable, loopback, link-local, multicast, or
  unspecified is skipped and counted (`N with no usable address`); no id is
  synthesized from anything else.
- Final runZero ID: `detectify:ip:<address>`.
- Missing-ID behavior: skip and count.
- Match behavior (set once in `CONFIG`): **`no-id-break no-name-match no-name-break`**.
- Verdict: **derived, deterministic, and unique per address — it may drive
  matching, with the three relaxations below.**

### Why these flags and not the usual outside-in preset

The library's preset for a source without a device key is
`no-id-match no-id-break`. It was tested against the platform's merge logic
for this source and deliberately not used:

- **Id matching stays on.** With `no-id-match`, the only way a record can find
  its own asset on the next poll is by address — and the address match skips
  any candidate that already carries this record's foreign id, because the
  platform expects such a candidate to have been found by id. The result is a
  new asset on every poll. With id matching on, each poll re-finds its asset by
  the composed id, which is stable because the address is.
- **`no-id-break`.** A candidate found by address that already carries a
  *different* Detectify id would otherwise be refused. That happens for a host
  with two public addresses (two records, one machine) and for an asset that
  was re-addressed and still carries the id of its old address. The composed
  id carries no information the address does not, so it must not veto an
  address match. Several records can coexist on one asset; they are stored
  under their own ids and do not overwrite each other.
- **`no-name-match`.** The hostnames are the DNS names that pointed at the
  address, not names the machine calls itself. The platform treats a custom
  integration's hostnames as trusted names, matches on the short label as well
  as the FQDN, and skips the address-conflict check for trusted-name matches —
  so `portal.example.com` would merge onto an internal server named `PORTAL`.
  Names must not find merge candidates for this source; the public address is
  the correlation signal.
- **`no-name-break`.** The same hostnames must not veto an address match
  either. A cloud instance whose EDR reports `web-prod-01` shares no name with
  `www.example.com`, and that is precisely the merge this integration exists
  to make.

The address-match and address-break flags are left at their defaults: the
only candidates considered are those sharing the address, so an address
conflict cannot arise, and MAC addresses never appear in this data.

### Notes

- **Only `GET /rest/v3/ips` can fail the task.** It is the inventory; a 401,
  403, or any failed page ends the run in error naming the page and the
  status, and nothing is imported, because addresses are reported only after
  every listing has been joined — a half-read estate must not file the unread
  half as decommissioned. Every other listing is enrichment: a failure is
  logged once with its cause and the addresses import without that data.
- **Pagination.** v3 listings page on a `cursor`; the script requests the
  documented maximum of 1,000 rows per page, follows `pagination.next` by
  re-sending only its `cursor` on the configured base URL (the HTTP helper
  replaces a URL's query, so the limit and the `status=OPEN` filter are
  re-applied explicitly), and stops when `next` is absent or a page is empty.
  The v2 vulnerabilities listing pages on `marker` / `has_more` /
  `next_marker` at its documented maximum of 100 rows, and the
  `status[]=active&status[]=new&status[]=regression` filter is assembled by
  hand because it repeats one query key. Every walk is guarded by `pager()`
  against `maxPages` (10,000 pages, i.e. ten million rows per listing).
- **Rate limits.** Detectify does not publish a rate limit for either API.
  The HTTP helper retries 408/425/429/5xx and transport errors with
  exponential backoff and honors `Retry-After`; the `rate-limit` scenario
  exercises that path.
- **Inactive records** (`active=false`, with a `disappeared_at`) are skipped by
  default for addresses and technologies. With `include_inactive` on, an
  address with no active domain imports with `active=false`, its latest
  `disappeared_at`, and its domains under `inactiveDomains`; a disappeared
  technology imports with `active=false` on the software row. Resolved
  breaches are never imported, and only active, new, and regressed
  vulnerabilities are requested.
- **Private and non-routable addresses.** Loopback, unspecified, link-local,
  multicast, and broadcast addresses are refused always. RFC 1918, CGNAT, and
  ULA addresses are refused by default and imported with `include_private_ips`.
- **Monitoring settings** are keyed by Detectify asset id and summarized per
  address across the assets of its domains: `monitoring.linkedAssets`,
  `monitoring.assetsWithSettings`, and for each flag whether *any* linked
  asset has it enabled. The listings take a `team_id` that the documentation
  says is optional only for single-team keys, so they are walked once per team
  seen in the IP list.
- **Platform caps.** An asset carries at most 100 hostnames and 99 each of
  services, software, and vulnerabilities. A shared CDN or hosting address can
  exceed those, so the lists are cut deliberately — the lowest ports, active
  software first, the most severe findings first — and the full counts stay on
  the asset (`serviceCount`, `technologyCount`, `breachCount`,
  `vulnerabilityCount`) with the cuts in `servicesOmitted`,
  `technologiesOmitted`, and `vulnerabilitiesOmitted`.
- **CVE assignment.** A finding gets a `cve` only when its title, its
  definition's description, or a reference *name* contains one; a reference
  *link* that mentions a CVE is not enough, because a related advisory can name
  a different CVE than the finding. Every CVE found is listed in the `cves`
  attribute.
- **Severity.** Breach severities `INFO..CRITICAL` and CVSS severities
  `information..critical` map onto runZero ranks 0–4. A finding's severity
  score is its CVSS 3.1 base score, falling back to 3.0 and then 2.0.
- **Memory.** The joins need the whole IP, port, technology, breach, and
  vulnerability indexes in memory before any asset can be built. Rows are
  reduced to the fields the mapping needs as they arrive (a v2 vulnerability
  can carry request and response bodies and kilobytes of rendered detail), and
  assets are then streamed one at a time with `report_asset`.

### Assumptions

- **Service transport is TCP.** The v3 `Port` schema reports `OPEN`/`CLOSED`
  and no transport, while Detectify documents scanning both TCP and UDP ports.
  Every service is recorded as `tcp`; a UDP-only listener would be mislabeled.
- **A technology's port is an open port.** A technology fingerprinted on
  `domain:port` implies something answered there, so a port with no `/ports`
  row still becomes a service, marked `source: technology`. Detectify can be
  configured without port scanning (only 80 and 443 are then considered), and
  this keeps the services the fingerprinting proves.
- **The web server names the service.** When a port's technologies include one
  in the *Web servers* or *Reverse proxies* category it becomes the service
  product/version; otherwise only a lone technology on the port does. Anything
  more ambiguous stays in the software inventory only.
- **One API key for both versions** unless `api_key_v2` is given — see
  [Detectify requirements](#detectify-requirements).

## Future

- **Domains with no address.** A monitored domain that resolves to nothing
  (a dangling CNAME, a takeover candidate) has no row in `/ips`, so its
  technologies, breaches, and findings are counted as unattached in the run
  log rather than imported. A hostname-only population under its own
  `assetType` and `assetTypeBehavior` would carry them.
- **Detectify asset inventory.** `GET /rest/v2/assets/?include_subdomains=true`
  publishes per-domain state (`monitored`, `status`, `added_by`, `discovered`,
  `last_seen`) that could decorate the domain list on each address.
- **Incremental runs.** Every listing accepts `first_seen_after` /
  `disappeared_after` (v3) or `updated_after` (v2) windows. Full walks are
  used because runZero expects a complete inventory per run; a very large
  estate could use the windows to shorten the enrichment walks.

## API documentation

- Detectify API reference: https://developer.detectify.com/ (v3, the attached
  OpenAPI 3.0.3 document, `Detectify API 3.0.0`) and
  https://developer.detectify.com/v2/ (v2).
- Developer API overview and version split: https://docs.detectify.com/developer
- Authentication and key management: https://docs.detectify.com/developer/authentication
- Endpoints used: `GET /rest/v3/ips`, `GET /rest/v3/ports`,
  `GET /rest/v3/technologies`, `GET /rest/v3/breaches`,
  `GET /rest/v3/surface-monitoring/domains`,
  `GET /rest/v3/surface-monitoring/ips`, `GET /rest/v2/vulnerabilities/`.

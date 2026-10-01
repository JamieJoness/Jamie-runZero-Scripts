# HaloITSM Certificate Expiry Digest

An **outbound** runZero custom integration that creates one HaloITSM ticket
per weekly period for X.509 certificates expiring within the next 14 days.
It reads existing runZero inventory; it does not scan hosts, import assets,
change inventory, or renew certificates.

The script starts in preview mode. No live tenant credentials are included,
and no live Halo tenant has been tested. Use runZero **5.1.260818.0 or later**.

## What the ticket contains

- One entry per SHA-256 certificate fingerprint, ordered by earliest expiry.
  Identical names with different fingerprints remain separate certificates.
- Common name, subject, DNS/IP alternative names, issuer, serial, fingerprint,
  exact UTC expiry and days/full hours remaining.
- A summary of certificates due within 48 hours, 2-7 days and 7-14 days.
- Every affected in-scope host and service, including IP, port, transport,
  protocol, virtual host/SNI where recorded, site and organisation.
- Links to the certificate, scoped service search and each affected asset in
  runZero. Users still need permission to view those records.
- Asset last-seen and service-record update times, plus a warning about stale
  or inactive inventory. These are not claimed to be certificate observation
  timestamps or live validation results.
- CA/self-signed status, public-key/signature algorithms and available weak
  cryptography flags.
- Practical handover guidance: identify the owner, check all termination
  points, preserve SANs, deploy the chain, follow change control, validate the
  replacement, rescan and record the change reference. Private keys do not
  belong in the ticket.

Example opening:

> Certificate renewals for Production customer services
>
> Please review the certificates below and arrange renewal before they expire.
> This check found 3 distinct certificates across 7 hosts and 9 services.
> Start with the earliest expiry: 2026-10-07 09:00:00 UTC (2 days remaining).

The actual ticket includes full hours as well as days remaining. Text from
inventory is escaped before inclusion in HTML; no certificate-provided HTML
is rendered as markup.

## Setup

### 1. Agree the scope and schedule

Choose the organisations, any certificate/service filters, the receiving Halo
customer and team, the ticket type, and a weekly day/time in **UTC**. Do not
combine organisations belonging to different customers in a single ticket.

The expiry window is fixed when each execution starts:
`checked_at < validity_end <= checked_at + 14 days`. Already-expired
certificates and non-X.509 records are excluded. Hidden records and CA
certificates are included unless explicitly excluded by `certificate_search`.

`certificate_search` uses certificate-inventory search syntax; `service_search`
uses service-inventory search syntax. Both are parenthesised before combining
with the script's own query. For example:

```text
certificate_search: not is_ca:true
service_search: site:London
```

Leave both blank to include every matching certificate and all its associated
services in the selected organisations. A certificate-level site filter selects
certificates associated with that site; it does not restrict the service list.
Use `service_search` when endpoints must be limited to a site, asset tag or
other service-inventory condition. Validate the queries in their respective
runZero inventory views first.

With a service filter, certificates with no matching endpoints are excluded.
Without one, certificates with no returned service relationships are retained
and explicitly labelled for investigation.

### 2. Prepare runZero access

Create a least-privilege token that can read certificate and service inventory
in every configured organisation. The current API routes require
`read:inventory`; an organisation-scoped token is suitable for one organisation.
Use an appropriately scoped account/user-bound API client for multiple
organisations. The script passes `_oid` on every export and checks each
returned row's organisation. It never selects all organisations implicitly.

Certificate inventory must be available under your runZero licence. No
inventory-write or scan-launch permissions are needed by this script.

### 3. Prepare Halo

In **Configuration > Integrations > Halo API**, register an application using
**Client Credentials** authentication and choose the service agent it runs as.
Record the resource server, authorisation server and any required tenant value
from that tenant's **API Details**.

The URLs in Halo's documentation are examples, not shared production endpoints:

```text
halo_api_url:  https://your-halo.example/api
halo_auth_url: https://your-halo.example/auth
```

The script adds `/token` to the authorisation server URL, requests an OAuth
token and uses `Authorization: Bearer ...` for API calls. Supply `halo_tenant`
only when API Details requires the hosted-authentication `tenant` query value.

The default OAuth scope is `read:tickets edit:tickets`. Enable the corresponding
application permissions and ensure the selected agent can read/create the
intended tickets, including closed and deleted records. Confirm scope names
against your tenant's API documentation; change `halo_scope` only as required.
Do not grant `all` simply to bypass a permission failure.

Choose IDs from the tenant's **Ticket Types**, **Teams**, and, where required,
customer, site, requester and priority settings. These are tenant-specific:
the script does not assume a numerical priority means urgent. Configure any
required default fields on the selected ticket type. Additional mandatory
custom fields would require a tenant-specific extension to the payload.

The ticket reference uses Halo's native `third_party_id_string`; no custom field
is required. This integration requires the tenant to persist that field and
honour its exact lookup filter. Do not let ticket rules clear it. Permit
`search_summary` as a fallback check. Read back a pilot ticket to confirm the
field, details, ticket type and team are retained.

### 4. Install and preview

Create a custom integration in runZero and add
[haloitsm-certificate-digest.star](haloitsm-certificate-digest.star). Its `CONFIG`
block builds the credential form. Enter secrets in that form, not in source,
example commands, tickets or chat.

Keep `dry_run=true` for the first execution. Preview mode reads runZero and
Halo, checks for existing tickets, and prints the complete proposed digest
without posting a ticket. Preview logs contain internal inventory data;
restrict access and retention accordingly. A preview does not reserve the week.

Use HTTPS in production and leave TLS verification enabled. Separate
`runzero_tls_*` / `runzero_http_*` and `halo_tls_*` / `halo_http_*` options support
private CAs, certificate pinning, mTLS and User-Agent configuration. Use a
suitably scoped Explorer when either system is private; Console execution
cannot reach arbitrary private addresses.

### 5. Schedule the independent integration task

Create a **dedicated recurring custom-integration task**, not a scan-completed
rule and not a credential attached to normal network scans. Select the Console
or one Explorer with access to both APIs and schedule it weekly at the agreed
UTC day/time.

Set `schedule_anchor_utc` to an occurrence of that same schedule, for example
`2026-10-05T09:00:00Z` for Mondays at 09:00 UTC. A past occurrence of the same
weekday/time is valid for previewing before rollout. An anchor in the future
causes the script to stop until the first weekly period begins.

The anchor identifies seven-day digest periods; it does **not** create the
scheduler or prevent a manual execution. Keep the task's schedule aligned with
the anchor. A fixed UTC schedule avoids daylight-saving ambiguity; a local-time
schedule that changes its UTC offset needs an explicit change plan, including
duplicate checks before changing the anchor.

After accepting the preview and pilot, set `dry_run=false` and
`single_runner_confirmed=true`. The latter is an operator acknowledgement, not
a distributed lock. Maintain exactly one task/runner for each digest. Do not
overlap manual runs, scheduled runs, or retries. Do not automatically retry a
failed delivery whose result is uncertain. Enable task-failure monitoring so a
failed or oversized digest does not go unnoticed.

Weekly runs with a 14-day lookahead usually give 7-14 days' notice for
certificates already in inventory. Newly discovered certificates can have less
notice. This is not continuous monitoring and cannot guarantee 14 days' warning.

## Parameters

| Parameter | Required / default | Purpose |
| --- | --- | --- |
| `runzero_url` | `https://console.runzero.com` | Console root, without an API path. |
| `runzero_api_token` | Required secret | Read access to the chosen organisations. |
| `organization_ids` | Required | Comma-separated organisation UUIDs. |
| `certificate_search` | Empty | Additional certificate-inventory query. |
| `service_search` | Empty | Restricts the affected endpoints. |
| `scope_label` | Required | Human-readable scope in the ticket. |
| `digest_key` | `certificate-renewals` | Stable identifier for this reporting workflow. |
| `schedule_anchor_utc` | Required | A weekly schedule occurrence as `YYYY-MM-DDTHH:MM:SSZ`. |
| `halo_api_url` | Required | Tenant resource server, including `/api`. |
| `halo_auth_url` | Required | Authorisation server, normally ending in `/auth`, without `/token`. |
| `halo_client_id` | Required | OAuth application identifier. |
| `halo_client_secret` | Required secret | OAuth client secret. |
| `halo_scope` | `read:tickets edit:tickets` | Requested application permissions. |
| `halo_tenant` | Empty | Hosted-authentication tenant query value, if required. |
| `halo_tickettype_id` | Required positive ID | Receiving ticket type. |
| `halo_team_id` | Required positive ID | Receiving team. |
| `halo_client_id_for_ticket` | `0` | Customer ID; separate from the OAuth client ID. |
| `halo_site_id` / `halo_user_id` / `halo_priority_id` | `0` | Optional ticket routing; zero omits the field. |
| `dry_run` | `true` | Preview without ticket creation. |
| `single_runner_confirmed` | `false` | Must be true for delivery; acknowledge non-overlap controls. |
| `max_certificate_records` | `5000` | Fail before exceeding this export-row budget. |
| `max_service_records` | `20000` | Budget across all service lookups, including repeated relationships. |
| `max_ticket_bytes` | `500000` | Maximum encoded JSON request size, including text and HTML. |

## Duplicate prevention and recovery

The reference is `RZCERT-<scope hash>-<weekly period start>`. The scope hash uses
the Console URL, sorted/deduplicated organisation UUIDs and `digest_key`. It does
not use the certificate list, access token, ticket title, destination team or
scope label. Thus changing inventory, credential rotation and reordered
organisation IDs do not turn a retry into a new digest.

1. Read all pages of Halo matches for `third_party_id_string`, including open,
   closed and deleted tickets. Verify the exact reference. If list responses
   omit the field, fetch ticket details to check it.
2. When no native match exists, search the summary marker too. A summary match
   without a native-reference match stops delivery for manual reconciliation.
3. If a matching ticket exists, skip creation, even if it was closed, moved or
   renamed. Multiple matching ticket IDs are an error, not a reason to post.
4. Build the complete digest, then repeat the duplicate check just before the
   POST. Send exactly one ticket in Halo's required JSON array.
5. Disable all automatic retries on that POST, including for 429 and 5xx.
   A lost response may still mean Halo created the ticket.
6. If the POST response is missing, unsuccessful or lacks a usable ticket ID,
   reconcile the reference again. Never immediately issue another POST.
7. Read back the ticket and verify the reference, end-of-digest marker, type
   and team. Unexpected routing, missing reference or apparent truncation
   fails the task and identifies the created ticket for investigation.

**This is duplicate detection, not an exactly-once guarantee.** The reviewed
Halo API contract does not document an idempotency key or atomic uniqueness
constraint for this operation. A check followed by POST can race with another
runner. Delayed search visibility, a hard-deleted ticket, or a ticket whose
reference and summary were both removed can defeat reconciliation. If strict
exactly-once semantics are required, use a durable atomic coordinator or a
Halo-side uniqueness mechanism verified for your tenant before enabling writes.

After an uncertain POST, pause the task and search Halo for the reference from
the task error. Include closed/deleted records and check ticket visibility with
an administrator. If a ticket exists, repair/verify its reference and contents;
do not recreate it. Resume only after the original request has settled and an
operator has confirmed either the existing ticket or that no ticket was created.
Do not change `digest_key` to work around an unresolved delivery failure.

The script keeps no local state. The existing ticket is its durable record.
Keep the Console URL, organisation set, digest key and weekly anchor stable
during retries. A scope-query change within the week does not create a revised
ticket automatically. Review the existing ticket manually, or deliberately
create a separately identified digest after checking for overlap.

The same still-expiring certificate may legitimately appear in the following
week's digest. Duplicate prevention is per **weekly digest**, not per certificate
or open remediation ticket. An empty result creates no ticket and reserves no
reference, so a later run in the period can report newly discovered findings.

## Data mapping and identity

| Source | Ticket use | Identity / conversion |
| --- | --- | --- |
| Certificate `fp_sha256` | One digest section | Full certificate fingerprint, normalised to lower-case hex. |
| `organization_id` + certificate `id` | Links and service lookup | Retain all source record references within the explicitly agreed scope. |
| `validity_end` | Expiry, order, urgency | Unix seconds; future-date clamping is disabled. |
| `cn`, `subject`, `names`, SANs, `issuer`, `serial` | Renewal context | Missing optional values are labelled, not invented. |
| Service `service_organization_id` + `service_id` | Affected endpoints | Deduplicate rows without merging distinct asset identities. |
| `service_asset_id`, name, address, port, transport, vhost | Host/service context | Preserve each system even when names or addresses repeat. |
| `last_seen`, `service_updated_at`, `alive` | Freshness caveats | Not a claim that the exact certificate was observed at that time. |

## Asset identity

- Target entity: a weekly outbound ticket; no `ImportAsset` is emitted.
- Verdict: **scoped authoritative identities** for source relationships;
  imported-asset reconciliation is not applicable.
- Source IDs: runZero organisation/certificate, organisation/service and
  organisation/asset UUID pairs. Certificate content identity is `fp_sha256`,
  not CN, serial alone, hostname, public-key identity or a random UUID.
- Cardinality: many certificate records and many services become one section
  per certificate fingerprint; every affected organisation/asset/service
  identity is retained. Cross-organisation grouping is limited to the explicitly
  configured reporting scope.
- Stability: repeated polls preserve identity. Certificate renewal normally
  changes the fingerprint and remains distinguishable from the old certificate.
  Deleting/recreating a runZero record can change its UUID; all current
  references are retained independently of fingerprint grouping.
- Namespace: the weekly Halo reference is scoped by Console URL, organisation
  set, workflow identifier and weekly period.
- Missing identity: fail without posting a partial digest when a required
  certificate fingerprint, UUID, expiry, organisation or endpoint is unusable.
  Optional labels and timestamps can be absent. Skipping unidentified rows
  would falsely present an incomplete digest as complete.
- Final runZero ID: none; this is outbound. `matchBehavior` is omitted and the
  script returns `None` without calling asset-reporting helpers.
- Evidence: runZero's public certificate/service export schemas, local export
  handlers/models, and Halo's documented ticket schema and third-party filter.

## Pagination and limits

runZero exports use `page_size=500`, `next_key` and `start_key`. Services are
looked up for each unique organisation/certificate ID using `certificate_id:=`;
service exports include only fields needed for the ticket. Halo lookups use the
API's documented spelling `pageinate=true`, `page_size=100`, `page_no` and
`record_count`. Counts, response shapes and non-progressing pages are checked.
Each loop is bounded by `pager()` and CONFIG `maxPages=1000`.

All certificate/relationship pages must succeed before delivery. Empty inventory
is success; refused credentials, malformed identity, failed enrichment or an
incomplete page are failures. There is no silent truncation or partial ticket.
The one-ticket requirement means the aggregate must fit memory and the receiving
tenant's size limits. The local byte limit is a safety budget, not a claim about
Halo's maximum accepted size. Review the scope and tenant limits if it is hit.

Read-only requests use the standard bounded HTTP retries, including
`Retry-After` handling. Halo's reviewed documentation states 700 requests per
300 seconds per tenant; other integrations can consume that allowance too.
The OAuth documentation gives a one-hour token lifetime. There is no automatic
mid-run token refresh: keep the task within its runtime budget and investigate
large scopes instead of increasing limits blindly.

Paged exports are not a transactionally consistent snapshot. Schedule outside
large inventory updates where practical; revalidate endpoints before remediation.

## Validation

From the `Jamie-runZero-Scripts` repository root, with a compatible runZero CLI
and a sibling `runzero-custom-integrations` checkout available:

```sh
export RUNZERO_SCANNER=/path/to/runzero
python3 haloitsm-certificate-digest/tests/test_digest.py -v
```

Tests reuse the HTTP fixture harness from the sibling
`runzero-custom-integrations/tests` directory and execute the real Starlark
runtime, including the local authentication-failure JSON fixture. They exercise
aggregation, same-name/different-certificate identities,
multiple hosts and organisations, certificate/service/Halo pagination, repeated
runs, lost POST responses, closed/deleted tickets, lookup failures, missing
identity, empty optional fields, read rate limits, scope checks, preview mode,
size limits, HTML escaping and ticket read-back checks. They make no live API
calls. The generic `runzero script --validate` dummy responses do not model the
required schedule or Halo reconciliation responses; use these fixtures for
behavioural validation, not a relaxed production parser.

With a sibling platform checkout and its required generated assets prepared:

```sh
cd ../platform
RZ_CUSTOM_INTEGRATION_SCRIPTS_DIR="$PWD/../Jamie-runZero-Scripts/haloitsm-certificate-digest" \
  go test -tags recogNocloud,development ./runzero \
  -run '^TestCompat_AllShippedIntegrationsLoad$' -count=1
```

Before enabling the weekly task, verify against the **actual tenant version**:

1. Preview and compare certificate counts, service scope and links with runZero.
2. Create one controlled pilot ticket and check text/HTML rendering, complete
   contents, type, team, customer visibility and notification behaviour.
3. Rerun in the same weekly period and confirm that no second ticket appears.
4. Rename and close the pilot, then repeat the duplicate check. Validate deleted
   ticket visibility in a test tenant; do not delete production tickets to test.
5. Confirm native-reference lookup, summary fallback, read-back fields and
   any proxy/request-size limits. Review the tenant's own `/api/swagger`.
6. Confirm the independent schedule, single-runner controls and failure alerts.

## Documentation sources

Reviewed on 2026-10-01:

- [Halo API overview and rate limits](https://halo.haloservicedesk.com/apidoc/info)
  (the page identifies itself as version 2.252.25).
- [Application permissions](https://halo.haloservicedesk.com/apidoc/authorisation).
- [Client credentials and tenant authentication](https://halo.haloservicedesk.com/apidoc/authentication/client).
- [Ticket creation, filtering and pagination](https://halo.haloservicedesk.com/apidoc/resources/tickets).
- [Current public Halo OpenAPI viewer](https://www.usehalo.com/swagger) and its
  [published schema data](https://s3.eu-west-2.amazonaws.com/s3.nethelpdesk.com/swaggerV1.js):
  `Faults`, `Faults_View`, `GET /Tickets`, `GET /Tickets/{id}` and `POST /Tickets`.
  The public schema is labelled API v2, not pinned to a customer's Halo release.
- [runZero custom integration scripts](https://help.runzero.com/docs/custom-integration-scripts/)
  and [Starlark helper reference](https://github.com/runZeroInc/runzero-custom-integrations/blob/main/docs/starlark-helpers.md).
- runZero implementation contracts: `models/certificates/certificate.go`,
  `models/certificates/apicertificate.go`, `models/service.go`,
  `actions/certificates/certificates.go`, `actions/import_export.go`,
  `actions/certificates_search.go` and `actions/services_search.go` in the
  sibling platform source, reviewed without modifying those files.

The supplied `https://haloitsm.com/apidoc/info` returned 404 during research.
The supplied `support.haloservicedesk.com/auth` and `/api` addresses are examples;
use the values from your own tenant's API Details.
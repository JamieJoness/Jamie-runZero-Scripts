# Custom Integration: Microsoft Defender AV Health

Imports Microsoft's own per-device Defender Antivirus health verdicts from the
Microsoft Defender for Endpoint **device antivirus health export API**
(`GET /api/deviceavinfo`) and attaches them to runZero assets.

The point of this integration is the compliance signal Microsoft computes
server-side: `avIsPlatformUpToDate`, `avIsEngineUpToDate`, and
`avIsSignatureUpToDate`. Because Microsoft evaluates each device against the
current platform/engine/signature releases itself, your runZero saved queries
and dashboards **never contain a version-number threshold that has to be
edited when Microsoft ships a new Defender platform version**. The queries
below are permanent.

runZero's native Microsoft 365 Defender integration calls this same API today
but currently keeps only the antivirus *mode* from it — the platform-status
verdicts and versions are not retained. This custom integration fills that gap
without waiting on a product change, and can simply be switched off if the
native integration later retains these fields.

## Architecture and data flow

1. A scheduled runZero custom integration task runs this Starlark script on the
   Explorer you select.
2. The script authenticates to Microsoft Entra with an application (client
   credentials) grant: `POST {login_url}/{tenant_id}/oauth2/v2.0/token` with
   scope `https://api.securitycenter.microsoft.com/.default`. The client secret
   is stored in runZero's encrypted credential vault and never logged.
3. It pages through `GET {api_url}/api/deviceavinfo` with `$top` (default
   10,000) and Microsoft's `@odata.nextLink`, honoring `Retry-After` on 429 and
   retrying transient 5xx responses with bounded exponential backoff.
4. Each row is normalized into an `ImportAsset` keyed on Microsoft's stable
   `machineId` and streamed to the console incrementally (no full-estate
   buffering).
5. The runZero cruncher merges each record into your existing assets — see
   [Asset identity and correlation](#asset-identity-and-correlation).

## runZero requirements

- Superuser access to create the custom integration and scheduled task.
- runZero platform 5.1.260818.0 or newer (the script's `minVersion`).
- An Explorer with outbound HTTPS access to `login.microsoftonline.com` and
  `api.security.microsoft.com` (or your sovereign-cloud equivalents).

## Microsoft requirements

- Microsoft Defender for Endpoint P1/P2 with devices onboarded.
- Permission to register a Microsoft Entra application and grant admin consent.
- Windows Server 2012 R2 / 2016 devices only appear in device health reporting
  when onboarded with the modern unified agent (Microsoft's documented
  prerequisite).

## Steps

### Microsoft Entra application setup

1. In the [Azure portal](https://portal.azure.com), open **Microsoft Entra ID →
   App registrations → New registration**.
2. Name it (for example `runZero Defender AV Health`), leave single tenant, no
   redirect URI, and register.
3. Note the **Application (client) ID** and **Directory (tenant) ID** from the
   overview page.
4. Under **API permissions → Add a permission → APIs my organization uses**,
   search for **WindowsDefenderATP**, choose **Application permissions**, and
   add **`Machine.Read.All`** ("Read all machine profiles"). This is the
   permission Microsoft documents for `deviceavinfo`.
5. Click **Grant admin consent** for your tenant. The status column must show
   green check marks.
6. Under **Certificates & secrets → New client secret**, create a secret and
   copy its **Value** immediately. Set a calendar reminder before its expiry —
   an expired secret is the most common cause of this integration failing
   months later.

Least privilege notes: `Machine.Read.All` is read-only. The integration makes
no write calls of any kind.

### runZero configuration

1. In the [runZero console](https://console.runzero.com/custom-integrations),
   go to **Account → Custom integrations → Add custom integration**.
2. Name it exactly `microsoft-defender-av-health`. **The saved queries below
   use this name as the attribute namespace** (`@microsoft-defender-av-health.…`);
   a different name means editing every query to match.
3. Paste the contents of
   [microsoft-defender-av-health.star](./microsoft-defender-av-health.star)
   into the script body and save.
4. Go to **Tasks → Integrations**, select this integration, and configure:
   - **Microsoft Entra tenant ID**, **Application (client) ID**, and **Client
     secret** from the app registration. The secret field is encrypted at rest
     and masked in the UI.
   - Leave **login URL**, **API URL**, and **token scope** at their defaults
     for the commercial Microsoft cloud. For sovereign clouds see
     [Known limitations](#known-limitations).
5. Recommended: tick **“Exclude assets that cannot be merged into an existing
   asset”** on the task. `deviceavinfo` carries no MAC or IP address, so a
   record that cannot be merged by hostname would otherwise create a new,
   sparse asset. With the box ticked the integration is enrichment-only and
   can never create duplicates. Leave it unticked only if you want
   Defender-known-but-never-scanned devices to appear as new assets.
6. Choose the site and Explorer, and schedule it **daily**. Microsoft refreshes
   this report roughly daily (`dataRefreshTimestamp` shows the server-side
   refresh time), so running more often adds load without adding data.
7. Run the task once and open its log. A healthy run ends with a line like
   `defender-av-health: 1234 device rows retrieved, 1230 assets reported, 4 rows
   skipped, 0 duplicate rows ignored`.

### Validation on a small test set

1. After the first run, search **Assets** for
   `custom_integration:microsoft-defender-av-health` — every asset the task
   touched.
2. Pick two or three devices you know (a compliant workstation, a stale VM)
   and open each asset. Under **Attributes**, the
   `microsoft-defender-av-health` section should show `platformUpToDate`,
   `avPlatformVersion`, `dataRefreshTimestamp`, and the rest.
3. Confirm enrichment (not duplication): the attributes should sit on the
   *existing* asset for that device — the one that also carries your scan
   and/or native Defender data. If you instead see a new hostname-only asset,
   the hostname reported by Microsoft did not match the existing asset; see
   [Troubleshooting](#troubleshooting).
4. Spot-check one device against the Microsoft Defender portal (**Reports →
   Device health → Antivirus health** tab) — the verdicts should agree.

## Imported data

Attributes appear in search as
`@microsoft-defender-av-health.device.<attribute>`. The integration stores
three kinds of value:

| Attribute | Values | Purpose |
| --- | --- | --- |
| `platformUpToDate`, `engineUpToDate`, `signatureUpToDate` | `true` / `false` / `unknown` | **Normalized query surface.** Microsoft's verdict lowercased; a missing or unrecognized verdict becomes `unknown` — never `false`, and never silently dropped. |
| `avModeName` | `Active`, `Passive`, `Disabled`, `Other`, `EDRBlocked`, `PassiveAudit`, `Unknown` | Defender AV mode, decoded exactly as runZero's native integration decodes it. |
| `dataRefreshTS`, `lastSeenTS` | epoch seconds | Freshness, queryable with relative terms such as `:>7days`. |
| `avIsPlatformUpToDate`, `avIsEngineUpToDate`, `avIsSignatureUpToDate` | `True` / `False` / `Unknown` verbatim | Microsoft's raw verdicts, preserved untouched (absent when Microsoft omitted the field — which is how `unknown` in the normalized field is distinguished from a literal `Unknown`). |
| `avPlatformVersion`, `avEngineVersion`, `avSignatureVersion` | version strings | For investigation and reporting — **not** for compliance thresholds. |
| `machineId`, `computerDnsName`, `osKind`, `osPlatform`, `osVersion`, `rbacGroupName`, `rbacGroupId` | verbatim | Identity and context. |
| `avMode`, `avPlatformUpdateTime`, `avEngineUpdateTime`, `avSignatureUpdateTime`, `avSignaturePublishTime`, `quickScanResult`, `quickScanError`, `quickScanTime`, `fullScanResult`, `fullScanError`, `fullScanTime`, `dataRefreshTimestamp`, `lastSeenTime` | verbatim | Raw Microsoft fields, kept only when non-empty. |

The asset itself also gets `hostnames` (from `computerDnsName`), `os`/
`osVersion` hints (fingerprinting still wins on merged assets), and a
source-reported last-seen time (from `lastSeenTime`).

## Saved queries and dashboard

Create each of these as a saved query (**Assets → run the query → Save**), then
add a dashboard widget per query. All of them are permanent: none contains a
Defender version number.

**1. Defender platform NOT up to date** (the actionable list):

```text
@microsoft-defender-av-health.device.platformUpToDate:="false"
```

**2. Defender platform status unknown** (Microsoft could not determine it, or
the field was absent):

```text
@microsoft-defender-av-health.device.platformUpToDate:="unknown"
```

**3. Defender platform up to date:**

```text
@microsoft-defender-av-health.device.platformUpToDate:="true"
```

**4. Stale health data** (Microsoft last refreshed this device's report more
than 7 days ago — treat these as unknown, not compliant):

```text
custom_integration:microsoft-defender-av-health AND @microsoft-defender-av-health.device.dataRefreshTS:>7days
```

**5. Unknown OR stale** (the "don't silently trust these" widget):

```text
@microsoft-defender-av-health.device.platformUpToDate:="unknown" OR @microsoft-defender-av-health.device.dataRefreshTS:>7days
```

**6. Windows endpoints with no Defender AV health data at all** (in your
estate but absent from Microsoft's report — not onboarded, or reporting under
a hostname runZero couldn't match):

```text
os:windows AND NOT has:@microsoft-defender-av-health.device.platformUpToDate
```

Narrow it to servers, desktops, and laptops where device types are populated:

```text
os:windows AND (type:server OR type:desktop OR type:laptop) AND NOT has:@microsoft-defender-av-health.device.platformUpToDate
```

**7. Platform version reporting (investigation only** — never a compliance
threshold**):**

```text
custom_integration:microsoft-defender-av-health AND has:@microsoft-defender-av-health.device.avPlatformVersion
```

Group or export on `@microsoft-defender-av-health.device.avPlatformVersion` to
see the version spread. Version *comparison* terms (for example
`@microsoft-defender-av-health.device.avPlatformVersion:<4.18.25010`) work for
one-off investigation, but a saved query with a version in it is exactly the
maintenance burden this integration exists to remove.

Notes on syntax, verified against the current search parser:

- `:="false"` is an exact, case-sensitive match; the normalized values are
  always lowercase. A bare `:false` (contains, case-insensitive) also works and
  cannot collide with `true`/`unknown`.
- Attribute *names* are case-sensitive; copy them exactly.
- The `@microsoft-defender-av-health` prefix is the integration's **name in
  your console** (matched case-insensitively); `device` is the attribute
  category this script declares.

### Dashboard setup

1. Open **Dashboard → edit (pencil) → Add widget**.
2. For each saved query above, add an **asset count** widget pointing at that
   saved query: "Defender platform OK" (query 3), "Defender platform out of
   date" (query 1), "Defender platform unknown" (query 2), "Defender health
   stale" (query 4), "Windows missing Defender health" (query 6).
3. Keep the **unknown/stale widget separate from the compliant widget** so
   devices with no signal are never displayed as compliant.

### Optional alert on new non-compliant devices

**Alerts → Rules → Create rule**: choose the event type for asset-query
results, select saved query 1 ("Defender platform not up to date"), and attach
a notification channel (email or webhook). Each scheduled run that finds new
matches then notifies you. If your console version exposes rule conditions
per-task instead, bind the rule to this integration's task and the same saved
query.

## Asset identity and correlation

- Target entity: a Defender for Endpoint onboarded device (server, desktop,
  laptop).
- Source ID field: `machineId` — Microsoft's device GUID, the same identifier
  the `/api/machines` endpoint returns as `id` and the value runZero's native
  Defender integration keys machines on.
- Documentation evidence:
  [Export device antivirus health details API](https://learn.microsoft.com/en-us/defender-endpoint/api/device-health-api-methods-properties)
  — "`machineId` (String): Machine GUID".
- Uniqueness scope: a Defender organization (tenant). The ID is namespaced with
  the configured tenant ID anyway, so two tenants can never collide.
- Cardinality: Microsoft returns a row per unique **(DeviceId,
  ConfigurationId)**, so one device *can* appear in more than one row. The
  first row per `machineId` wins; later rows are counted and ignored, because
  emitting one foreign ID twice in a run is never right.
- Stability: `machineId` survives rename, re-IP, and agent updates; device
  re-onboarding can mint a new one (Microsoft treats it as a new device record,
  and so does this integration).
- Presence: required. Rows without it are skipped and counted, never invented.
- Final runZero ID: `microsoft-defender-av-health:<tenant_id>:<machineId>`.
- Missing-ID behavior: skip with a log line.
- Match behavior: `no-mac-break no-ip-break no-name-break` — the vendor ID is
  authoritative for this integration's own records across runs, and hostname
  drift or the absence of MAC/IP on our side must not disqualify the merge.
- Verdict: **authoritative foreign ID** (tenant-scoped).

### How records reach your existing assets — read this before relying on it

Verified against the platform's merge engine, not assumed:

- **Foreign IDs do not merge across sources.** The cruncher looks up foreign-ID
  merge candidates scoped to the *same* integration, so this integration's
  `machineId`-based ID will never ID-match the native Defender integration's
  record even though both derive from the same GUID. Identical foreign IDs
  from different sources are not a correlation signal in runZero.
- **`deviceavinfo` returns no MAC and no IP.** The only cross-source
  correlation signal in this data is the hostname (`computerDnsName`), which
  the platform matches with its trusted- and unique-hostname passes, with
  guards against duplicate hostnames across domains.
- Consequently: records for devices whose Microsoft-reported DNS name matches
  an existing asset (from scans, the native Defender integration, or any other
  source) **enrich that asset**. Records that match nothing either create a
  new hostname-only asset (default) or are skipped ("Exclude assets that
  cannot be merged into an existing asset" — recommended).
- Repeat runs are idempotent: the same device updates the same record via this
  integration's own stable foreign ID, whichever asset it merged into.
- Rows with no usable hostname (empty, `localhost`-style placeholders, bare
  IPs) are skipped and counted — with no correlator they could only ever
  become orphans.

To prevent duplicates entirely, run with the exclude toggle on. To find any
stragglers created before you enabled it:
`custom_integration:microsoft-defender-av-health AND source_count:1` — assets
only this integration has ever seen.

## Failure behavior

| Condition | Behavior |
| --- | --- |
| Secret rejected / token refused (401) | Task ends in error naming the token endpoint status. No data call is made. |
| First page unreadable (after retries) | Task ends in error: `could not read the device antivirus health list`. Zero-asset "success" is never reported for a broken read. |
| Page N fails mid-walk | Pages already streamed are kept; the task still ends in error naming the page, so a truncated import can't masquerade as a complete one. |
| 429 / transient 5xx | Retried up to 3 times with exponential backoff, honoring `Retry-After`. |
| `@odata.nextLink` pointing off the configured API host | Refused; task ends in error. The bearer token is never sent to an unexpected host. |
| Empty tenant | Success, with an explicit "Microsoft reported no device antivirus health rows" log line. |
| Malformed row / missing fields | Row skipped (identity missing) or imported with `unknown` verdicts (health fields missing); one bad row never aborts the run. |

The pagination walk is bounded by `maxPages` (2,000 pages — at the default
`$top` that is 20 million rows) and the guard *errors* rather than silently
truncating if it ever trips.

## Troubleshooting

- **`token endpoint returned status 401`** — wrong client secret, expired
  secret, or wrong tenant ID. Create a fresh secret and re-enter it.
- **`status 403` on the data call** — admin consent not granted, the
  `Machine.Read.All` *application* permission missing (delegated is not
  enough), or a token scope that doesn't match the API host. Keep the default
  scope `https://api.securitycenter.microsoft.com/.default`; Microsoft
  documents that this API requires the legacy `securitycenter` audience even
  when called via `api.security.microsoft.com`.
- **Attributes on new assets instead of existing ones** — the DNS name
  Microsoft reports doesn't match the asset's hostnames in runZero (for
  example, NetBIOS-only names from scans vs FQDNs from Microsoft). Check the
  asset's hostname list; enabling the exclude-unmerged toggle stops the new
  assets while you reconcile naming.
- **Queries return nothing** — confirm the integration's console name is
  exactly `microsoft-defender-av-health` and the task has completed at least
  once; attribute names are case-sensitive.
- **Task log shows many skipped rows** — those rows lacked `machineId` or a
  usable hostname; the log names each machine ID skipped for hostname reasons.

## Known limitations

- **Hostname-only cross-source correlation.** Microsoft does not include MAC
  or IP in `deviceavinfo`, so merging into existing assets rides on hostname
  quality. Estates with heavy duplicate hostnames across domains will see
  fewer merges (the platform deliberately refuses ambiguous ones).
- **Very large estates.** Microsoft recommends the JSON method used here for
  organizations under ~100K devices, and rate-limits it at 30 calls/min and
  1,000 calls/hour. At the default `$top=10000` a 100K-device estate is ~10
  calls — comfortably inside the limits. Beyond that scale Microsoft's
  file-export variant (`/api/machines/InfoGatheringExport`) is the right tool;
  this script does not implement it.
- **Sovereign clouds.** Defaults target the commercial cloud. GCC High/DoD
  tenants must override `login_url` (`https://login.microsoftonline.us`),
  `api_url` (for example `https://api.securitycenter.microsoft.us`), and
  `token_scope` to the matching resource. These endpoints are configurable but
  were not tested against a live sovereign tenant.
- **Snapshot semantics.** The API is a current-state snapshot with no history;
  `dataRefreshTimestamp` tells you when Microsoft last refreshed each row.
  Devices Microsoft has aged out of the report stop being refreshed here —
  which is what the stale-data query surfaces.
- **`os`/`osVersion` are hints.** On merged assets runZero's fingerprinting
  takes precedence (the script deliberately does not set `trustOS`).

## Safe disable and removal

Nothing this integration does deletes customer data, and removal is equally
non-destructive:

1. **Pause**: disable or delete the scheduled task (Tasks → the recurring
   task). Imported attributes stay on the assets and simply stop refreshing.
2. **Remove**: delete the custom integration from Account → Custom
   integrations. Existing attributes age out with normal asset data lifecycle;
   saved queries referencing the namespace return progressively fewer results.
3. Delete the saved queries/widgets, and delete the Entra app registration (or
   just its secret) to revoke Microsoft-side access.

## API documentation

- [Export device antivirus health report API](https://learn.microsoft.com/en-us/defender-endpoint/api/device-health-export-antivirus-health-report-api)
  (endpoint, permissions, pagination, rate limits)
- [Export device antivirus health details — methods and properties](https://learn.microsoft.com/en-us/defender-endpoint/api/device-health-api-methods-properties)
  (field list; `True`/`False`/`Unknown` verdict contract)
- [Create an app to access Microsoft Defender for Endpoint without a user](https://learn.microsoft.com/en-us/defender-endpoint/api/exposed-apis-create-app-webapp)
  (Entra application setup and token audience)

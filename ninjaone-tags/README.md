# NinjaOne: Tag-Filtered Custom Integration

Imports NinjaOne devices selected by their NinjaOne device tags, with extended
inventory. It is based on the platform's native NinjaOne Starlark integration,
but runs as a separate custom integration. No NinjaOne data is modified.

**Readiness:** implemented and tested with synthetic API responses through the
real runZero CLI. A live NinjaOne tenant and Console import have not been tested.
Run a small customer pilot before scheduling it across the estate.

## Requirements

- runZero Console and Explorer supporting CONFIG-based custom integrations;
  the script declares a minimum version of `5.1.260818.0`.
- Permission to create custom integrations, credentials, and integration tasks.
- An Explorer with HTTPS access to the customer's NinjaOne regional instance.
- A NinjaOne API Services (machine-to-machine) client with the **Client
  Credentials** grant and **monitoring** scope. This read-only integration does
  not need the management or control scopes. Endpoint availability still depends
  on the client's access and the tenant's licensed features.

## Setup

1. In NinjaOne, ask a system administrator to create an API client under
   **Administration > Apps > API > Client app IDs**. Select **API Services
   (machine-to-machine)**, **Client Credentials**, and **monitoring**. Record
   the client ID and secret securely. Use the regional hostname that the customer
   signs in to, such as `https://eu.ninjarmm.com` or
   `https://us2.ninjarmm.com`.
2. In runZero, create a **Custom Integration**, enable its script, and paste the
   contents of [ninjaone-tags.star](ninjaone-tags.star). Name the integration
   `NinjaOne Tag Filtered`. This is a custom integration, not a replacement file
   for the native NinjaOne source.
3. Create the associated **Custom Integration Script Secrets** credential.
   Enter the NinjaOne URL, client ID, secret, and tag filters in the generated
   form. Keep TLS certificate validation enabled. Do not put secrets in the
   script or task notes.
4. Create a **Custom Integration** task using that integration, credential,
   Explorer, and the intended destination runZero organization/site. Run it
   manually first; schedule it only after checking the pilot results below.

The script authenticates at `/ws/oauth/token` using the native integration's
client-credentials flow. All subsequent API operations are GET requests.

## Tag Selection

Tag names are exact and case-sensitive. Enter **one name per line**, not a
comma-separated list. A comma can be part of a tag name. Blank lines and repeated
names are ignored; surrounding whitespace is trimmed.

For example, to select either production or PCI devices:

**Include NinjaOne tags:**

```text
Production
PCI
```

**Match inclusion tags:** `any`

**Exclude NinjaOne tags:**

```text
Do not import
```

- `any`: a device must have at least one inclusion tag.
- `all`: a device must have every inclusion tag.
- Any exclusion tag overrides inclusion.
- Empty inclusion with nonempty exclusion imports every device that has none
  of the excluded tags, including devices explicitly reporting `tags: []`.
- Both lists empty is rejected before authentication. This prevents accidentally
  running an unfiltered import.
- A nonmatching tag name legitimately selects zero devices. Check spelling and
  case if the match count is unexpectedly zero.
- Missing, null, or malformed tag data is not treated as an empty tag list.
  The script tries `/v2/device/{id}`; if tags remain unknown, it skips that device
  and marks the run failed. This also applies to exclusion-only filters.

These are NinjaOne's actual device tags, not saved searches, organizations, or
custom fields. Filtering happens locally against the documented `tags` array in
`/v2/devices-detailed`. Consequently the Explorer reads the accessible device
listing, but emits only matches. Inventory report requests use the documented
`df=id in (...)` filter for selected IDs, and returned rows are checked against
those IDs again before import.

## Parameters

| Parameter | Default | Purpose |
| --- | --- | --- |
| `api_url` | Required | Regional NinjaOne instance URL; copied paths/query strings are discarded. |
| `client_id` | Required | OAuth client ID; also namespaces imported device IDs. |
| `client_secret` | Required | Encrypted secret field; no hardcoded value. |
| `include_tags` | Empty | Inclusion names, one per line. |
| `tag_match` | `any` | `any` or `all` inclusion matching. |
| `exclude_tags` | Empty | Exclusion names, one per line. |
| `include_inventory` | `true` | Collect the extended inventory reports below. |
| `include_software` | `true` | Import installed software when extended inventory is enabled. |
| `custom_fields` | Empty | Explicit custom-field API names, one per line. Independent of `include_inventory`. |
| `http_*`, `tls_*` | Shared defaults | Standard runZero HTTP and TLS connection settings. |

Turning off `include_inventory` limits collection to detailed device records
and any explicitly requested custom fields. Disabling software collection is not
a promise to preserve existing software contributed by this custom source;
validate source reconciliation before changing an established feed.

## Imported Data

One selected NinjaOne device becomes one runZero asset. Child records enrich
that asset; they do not become separate assets.

| NinjaOne data | runZero mapping |
| --- | --- |
| Device `id`, `uid` | Stable scoped asset ID; original numeric ID in `deviceId`, UUID retained as an attribute. |
| System, DNS, NetBIOS names | Validated, deduplicated hostnames. Friendly `displayName` is metadata only. |
| Device tags | JSON array in `ninjaoneTags`; not automatically copied into runZero's own asset tags. |
| Detailed device record | Approval/offline state, node role, policies, organization/location IDs, maintenance, timestamps, notes, and returned reference data. |
| Returned references | Organization/location names, policies, assigned owner, warranty, and backup data where supplied by NinjaOne. |
| Operating systems | OS and version fields; architecture, build, release, locale, boot time, reboot state, and other returned details as attributes. |
| Computer systems | Manufacturer/model fields; serial numbers, domain, chassis, memory, CPU count, and VM state as attributes. |
| Network interfaces | IPv4/IPv6 and actual reported MAC/IP associations; interface, DNS, gateway, speed, and status metadata. |
| Installed software | First-class `Software` records with deterministic IDs, product, publisher, version, installation time where parseable, and remaining source fields. |
| Logged-on users | Last logged-in username for ownership resolution; returned logon metadata. |
| Device health | Health, reboot, alert, patch, antivirus, installation, and vulnerability counts where returned. |
| Hardware reports | Processors, disks, volumes, BitLocker status, RAID controllers, and RAID drives. |
| Antivirus reports | Product state, definitions/version, and reported threats. |
| Patch reports | Pending, failed, or rejected OS and software patch records as returned by the API. |
| Policy overrides and Windows services | Source attributes, not inferred network services. |
| Explicit custom fields | Only allowlisted field names; `showSecureValues=false` is sent explicitly. |

Original native-style `osName`, `systemModel`, `systemSerialNumber`, and similar
attributes are retained alongside richer nested attributes. Report lists use
indexed keys, such as `inventory.volumes.0.bitLockerStatus.protectionStatus`.
The raw NinjaOne device type is stored as `ninjaoneDeviceType`, not forced into
runZero's device classification.

The integration excludes unrestricted `userData` and does not request arbitrary
custom fields. Review requested fields and device notes for sensitive customer
data before enabling the feed. It does not collect recovery keys, passwords,
ticket contents, remote-control sessions, or historical activity streams.
Patch status and antivirus detections are not converted into invented CVE
findings; Windows-service records are not treated as listening TCP/UDP ports.

## Pagination and Failure Handling

- Device listing requests ask for 500 rows and advance `after` using the maximum
  valid device ID. The walk continues until an empty page, including when an
  earlier page is shorter than requested. Replayed/nonadvancing pages fail.
- Extended reports are requested for at most 100 selected devices at a time.
  Every report follows `cursor.name`, with the name/offset pair used to detect
  repeated pages. No arbitrary 99- or 100-product software cap is applied.
- Every pagination loop uses the runZero `pager()` guard. `CONFIG.maxPages` is
  20,000 per walk; reaching it is an error, not a successful partial import.
- The standard HTTP helper retries transient failures, including 429 and common
  5xx responses, with bounded backoff and `Retry-After` handling. A 401 triggers
  one token refresh and retry of that request. No fixed requests-per-minute
  allowance is assumed; monitor the customer's task duration and API limits.
- Optional report failures produce a warning and `collection.<report>.status`
  of `unavailable`. A 403/404 disables that report for the remainder of the run.
  Partial optional reports are discarded, not presented as complete data.
- Software collection is stricter when enabled: a failed report, broken cursor,
  or unjoinable row stops the batch before its assets are reported. A malformed
  product skips its affected device and marks the overall task failed. A
  successfully completed empty software report is an empty snapshot.
- Assets already streamed before a later failure are retained by runZero.
  Failed runs are not transactional and must not be treated as complete imports.
- Attribute values are limited to 1,024 characters, keys to 256, and each asset
  to 1,024 custom attributes including ownership added by runZero. Two limit
  flags and ownership have reserved slots. Overflow is warned about; collection
  counts retain the original report sizes. Interfaces are capped at the SDK's
  256-interface limit with a warning. Export normalization can shorten values
  further. Very large inventories therefore cannot retain every source field.

## Asset Identity

- **Target entity:** the managed-device record, not a physical-machine serial
  number, software installation, interface, or event.
- **Source ID:** device `id`. The public OpenAPI schema describes it as the
  "Node (Device) identifier" and uses it for `/v2/device/{id}` and the
  `deviceId` joins in monitoring reports.
- **Verdict:** scoped foreign ID, consistent with the native integration.
- **Uniqueness scope:** the NinjaOne account/device inventory reached through
  one OAuth client. A regional hostname is shared by unrelated customers and is
  insufficient as a namespace. This script preserves the native client-ID scope.
- **Final ID:** `ninjaone:<client_id>:<device_id>`.
- **Cardinality:** one asset per selected device record. Duplicate device IDs
  within a page and overlap with earlier pages are suppressed. Software,
  interfaces, patches, and reports remain attached to the same device.
- **Stability:** repeated polls, tag changes, names, and addresses do not enter
  the ID. Keep the same client ID and registered custom integration for repeat
  runs. Replacing the OAuth client changes the namespace; rotating only its
  secret does not. Multiple clients for the same tenant are not deduplicated
  by this namespace alone.
- **Lifecycle/reuse caveat:** the public schema does not guarantee ID reuse or
  reinstall/re-enrolment behavior. A replaced NinjaOne device record is treated
  as a new source identity. Physical-device correlation remains runZero's job.
- **Presence/fallback:** missing, invalid, nonpositive, or fractional IDs are
  skipped and counted. There is no random or hostname-derived fallback ID.
- **Match policy:** `no-ip-match no-ip-break`, preserving the native policy.
  Private addresses on roaming endpoints are not dependable cross-device
  identity. Source ID, MAC, and real-hostname matching remain enabled with their
  normal break rules. Public egress IP stays metadata, never an interface.

## Customer Pilot

1. Choose a NinjaOne tag applied to a small, known set of devices. Include an
   untagged device, a similar-but-not-identical tag, and an excluded device in
   the comparison. Check the exact expected membership in NinjaOne.
2. Run the custom task manually. Compare its examined/matched/excluded counts
   and imported devices with that set. Check all warnings and task errors.
3. Inspect OS/hardware, `ninjaoneTags`, interfaces, software, and collection
   status attributes. Confirm the OAuth client's organization visibility.
4. Run again and confirm assets are updated without unexpected duplicates.
   Check correlation with existing scanned/native-integration assets in the
   destination organization and site; offline fixtures do not test Console
   database merging.
5. Stop or narrow the unfiltered native task before relying on this custom task
   as the customer's import boundary. Running both still allows the native
   task to import devices outside the tag selection.

Filtering controls future imports. Removing a NinjaOne tag does **not** delete,
unmerge, or automatically remove an already-imported runZero asset or its source
attribution. Handle existing inventory and retirement policies separately.
Native and custom sources are distinct even when their device-ID strings match.

Avoid overlapping custom software feeds on the same assets without testing the
customer's platform version: the platform's custom-source software reconciliation
can replace rows from another custom integration. Separate custom integration
UUIDs alone are not a guarantee of software isolation.

## Offline Validation

These tests need no customer credentials and make no calls to NinjaOne. They use
the existing fixture harness from the sibling `runzero-custom-integrations`
checkout and the real CLI. Python's standard library is sufficient.

```bash
RUNZERO_SCANNER=/path/to/current/runzero \
  PYTHONDONTWRITEBYTECODE=1 python3 ninjaone-tags/test_ninjaone_tags.py -v
```

Run from `Jamie-runZero-Scripts`. If present, the ignored local development
binary at `.validation/runzero` is used automatically. The optional
`RUNZERO_INTEGRATION_TESTS` variable points to an alternative checkout's
`tests` directory. Neither the binary nor the fixture harness is needed to run
the script in the Console.

For the generic CONFIG and HTTP/TLS smoke test, supply a harmless tag because
the generic validator otherwise leaves both filter fields empty:

```bash
runzero script --filename ninjaone-tags/ninjaone-tags.star \
  --validate --kwargs include_tags=Production
```

This is a synthetic server check, not live authentication or inventory testing.
Fixtures cover selection modes, exclusions, missing tags, stable and scoped IDs,
duplicates, pagination, refresh, 429 retries, failure states, software snapshots,
custom-field protection, interface mapping, large lists, and SDK limits.

## References

- [NinjaOne API documentation](https://www.ninjaone.com/docs/application-programming-interface-api/)
- [Public API reference](https://app.ninjarmm.com/apidocs/)
- [Public OpenAPI document](https://app.ninjarmm.com/apidocs/NinjaRMM-API-v2.json)
- [Device-filter syntax](https://resources.ninjarmm.com/API/Ninja+RMM+Public+API+v2.0.5+Device+Filter+Syntax.pdf)
- [OAuth authorization](https://www.ninjaone.com/docs/application-programming-interface-api/public-api-2-0-authorization/)
- [OAuth client configuration](https://www.ninjaone.com/docs/application-programming-interface-api/oauth-token-configuration/)
- [runZero custom integration scripts](https://help.runzero.com/docs/custom-integration-scripts/)

Implementation references were the platform-native NinjaOne script and the
`create-custom-integration` skill in the local custom-integrations repository.
Public API schemas were checked on 2026-09-29. Catalog generation is not applicable
to this standalone deliverable; no shared integration catalog is changed.
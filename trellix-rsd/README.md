# Custom Integration: Trellix Rogue System Detection

Inbound runZero custom integration that imports the **Detected Systems**
inventory of Trellix Rogue System Detection (RSD) from an on-premises Trellix
ePolicy Orchestrator (ePO) server. RSD sensors listen to layer-2 traffic
(ARP, DHCP, broadcasts) on each subnet and report every interface they see, so
this inventory includes printers, phones, switches, visitor laptops, and any
other device with no Trellix Agent - the systems the ePO System Tree never
holds.

It is independent of the [Trellix ePolicy Orchestrator](https://github.com/runZeroInc/runzero-custom-integrations/tree/main/trellix-epo)
integration, which imports the System Tree (agent-managed inventory). The two
share the ePO remote-command transport and can run side by side against the
same console: RSD rows carry their own ids, and runZero correlates the two
sources by MAC, IP, and hostname like any other pair of sources.

## runZero requirements

- Superuser access to the [Custom Integrations configuration](https://console.runzero.com/custom-integrations) in runZero.
- An Explorer running **5.1.260818.0 or later** that can reach the ePO console port (8443 by default). ePO is an on-premises application, so the Explorer must be inside the network that serves it.

## Trellix requirements

- **ePO On-prem with the Rogue System Detection extension installed.** The `/remote/` web API is documented for ePO On-prem only, and the `detectedsystem.*` command family is registered by the RSD extension. A console without RSD answers `detectedsystem.find` with ePO's `Error 1: No such command` envelope, which this integration reports as a failed task.
- An ePO user account and password. The remote command interface authenticates with HTTP Basic; there is no API token object in ePO.
- A permission set granting **view rights to Rogue System Detection**. The integration issues a single read-only command, `detectedsystem.find`, so no write permission is needed. Trellix documents that web API commands follow the same role-based permissions the console enforces, and the RSD product guide notes that non-administrators cannot use queries to see rogue systems in System Tree groups they lack viewing rights for - expect a scoped account to import a scoped inventory.
- ePO consoles ship with a self-signed certificate on port 8443. Enable `disable_validation` in the integration's TLS options if the certificate is not trusted by the Explorer, supply the issuing CA in `tls_ca_cert`, or install a trusted certificate on the console.

## Steps

### Trellix ePolicy Orchestrator configuration

1. In the ePO console, open **Menu > User Management > Permission Sets** and create (or pick) a permission set whose **Rogue System Detection** category allows viewing detected systems. The built-in **Global Reviewer** set - view access globally across functionality, products, and the System Tree - is the closest stock fit.
2. Open **User Management > Users**, create (or select) the account the integration will use, and assign it that permission set.
3. Confirm the command answers for that account by requesting `https://<epo-host>:8443/remote/detectedsystem.find?searchText=%20&:output=json` with the same credentials. A working response begins with the line `OK:` followed by a JSON array of `RSDDetectedSystems.*` rows; `https://<epo-host>:8443/remote/core.help?prefix=detectedsystem` lists every RSD command the account may run.

### runZero configuration

1. [Create the Custom Integration](https://console.runzero.com/custom-integrations/new).
   - Add a Name and Icon for the integration (e.g., "Trellix Rogue System Detection").
   - Toggle `Enable custom integration script` and paste [trellix-rsd.star](trellix-rsd.star).
   - Click `Validate` to ensure it has valid syntax.
   - Click `Save` to create the Custom Integration.
   - The script embeds its `CONFIG` block, so the credential form is generated automatically with the fields below.
2. [Create the Credential for the Custom Integration](https://console.runzero.com/credentials).
   - Select the option that matches your custom integration name eg `Trellix-RSD Script Secrets`.
   - **ePolicy Orchestrator URL** (`url`): base URL of the ePO console, for example `https://epo.example.com:8443`.
   - **Username** (`username`): the ePO account from the steps above.
   - **Password** (`password`): the password for that account.
   - **Search text** (`search_text`): optional substring filter that `detectedsystem.find` applies to the DNS name, NetBIOS name, domain, user, and IP address columns. Leave blank to import every detected system.
   - **Import managed systems** (`include_managed`, default `true`): detected systems that have an active Trellix Agent. Turn off when the ePO System Tree integration already covers them.
   - **Import exceptions** (`include_exceptions`, default `true`): systems on the RSD Exceptions list - printers, phones, switches that are known not to need an agent.
   - **Import inactive systems** (`include_inactive`, default `false`): systems no sensor has seen for longer than the RSD inactive period (45 days by default).
3. [Create the Custom Integration task](https://console.runzero.com/ingest/custom/).
   - Select the Credential and Custom Integration created in steps 1 and 2.
   - Update the task schedule to recur at the desired timeframes.
   - Select the Explorer you would like the Custom Integration to run from.
   - Click `Save` to kick off the first task.

### Running it from the command line

The runZero Explorer binary runs a script directly, which is the fastest way to
confirm an ePO account and see what RSD returns before scheduling anything. Run
it from a host inside the network that serves the console. `--kwargs` is
repeated once per parameter:

```bash
runzero script --filename trellix-rsd/trellix-rsd.star \
  --kwargs url=https://epo.example.com:8443 \
  --kwargs username=runzero \
  --kwargs password='<password>' \
  --kwargs include_inactive=false \
  --custom-integration-id 1f2e3d4c-5b6a-7988-9a0b-1c2d3e4f5a6b \
  --output ./trellix-rsd-run
```

`--output` writes the assets the run produced, and `--overwrite` replaces a
directory from a previous run. Add `--verbose` for the request-by-request log.
To keep a first run small, pass `--kwargs search_text=<substring>` - it is the
same filter the Detected Systems page search box applies. Since ePO ships a
self-signed certificate on 8443, a first run that fails on TLS rather than on
authentication needs `--kwargs tls_ca_cert=/path/to/epo-ca.pem` or
`--kwargs tls_disable_validation=true`, not a different URL.

```bash
runzero script --filename trellix-rsd/trellix-rsd.star --validate
```

Validation checks the `CONFIG` block and the HTTP and TLS wiring against a
local dummy server. **It is expected to end with
`could not read the Detected Systems list: unrecognized response envelope`**:
the dummy server answers plain JSON, never ePO's `OK:`-prefixed envelope, so the
script correctly refuses the body. The sibling Trellix ePO integration fails
the same check for the same reason. The fixtures below are the real offline
validation.

### Fixture tests

The scenarios under `trellix-rsd/tests/fixtures/` run the real runZero CLI
against a local HTTP server that replays ePO-shaped responses, reusing the
fixture harness from the sibling `runzero-custom-integrations` checkout without
copying or modifying it. Run from `Jamie-runZero-Scripts`:

```bash
export PYTHONDONTWRITEBYTECODE=1 RUNZERO_SCANNER=/tmp/rumble-scanner
python3 trellix-rsd/tests/run.py            # every scenario
python3 trellix-rsd/tests/run.py happy      # one scenario
```

Set `RUNZERO_CUSTOM_INTEGRATIONS` when that repository is not a sibling
directory. The scenarios cover the `OK:` envelope and the biased `IPV4`
decode, the placeholder values, every skip path, the state filters in both
default and inverted form, the blank-search encoding, a refused credential, the
`Error` envelope a console without RSD returns, a transient 502 followed by
recovery, and an empty inventory. Every scenario is run twice and the emitted
ids must match.

### What's next?

- You will see the task kick off on the [tasks](https://console.runzero.com/tasks) page like any other integration.
- The task will update existing assets with data pulled from Trellix Rogue System Detection.
- The task will create new assets when there are no existing assets that meet merge criteria (hostname, MAC, etc).
- You can search for assets enriched by this custom integration with the runZero search `custom_integration:trellix-rsd`, and for the ones RSD classes as rogue with `tag:rsd-status:rogue`.

## Asset identity

- Target entity: a detected system - a device RSD has observed on the network, with or without a Trellix Agent.
- Source ID field: `RSDDetectedSystems.HostID`.
- Documentation evidence: every row `detectedsystem.find` returns is keyed `RSDDetectedSystems.<column>` and carries `HostID` (vendor-independent client: `posh-epoapi` `Find-ePoDetectedSystem`, which reads `RSDDetectedSystems.HostID` from each result row). In the ePO database `HostID` is the key of the `RSDDetectedSystems` table: a published Splunk DB Connect input against that table uses `HostID` as its monotonically rising checkpoint column and filters on `HostID > 0`.
- Uniqueness scope: one ePO server, i.e. one ePO database. Identity values restart per installation, so the ePO hostname is part of the runZero id.
- Cardinality: one row per detected system. RSD itself matches each new interface detection against existing systems (by agent GUID first, then the attributes configured under **Detected System Matching**) and updates the matched row, so a re-detection does not create a new `HostID`. The one documented exception is a multi-NIC system whose interfaces RSD could not match to each other, which appears as separate detected systems - and therefore as separate runZero assets, each with its own interface. The fix is RSD's **Actions > Detected Systems > Merge Systems**, after which the merged-away `HostID` stops being reported.
- Stability: survives IP changes, renames, and re-detections, because RSD matches those to the existing row. Replaced only when the system is deleted from the Detected Systems list (**Delete**) and later detected again, or merged away.
- Reuse behavior: `HostID` is a SQL Server identity column (the rising-column usage above depends on it never going backwards), so a deleted system's id is not handed to a later one.
- Presence: present on every row in the vendor client's field list. A row without it is skipped, not synthesized.
- Final runZero ID: `trellix-rsd:<epo-hostname>:<HostID>`
- Missing-ID behavior: skip the row with a message naming only `DnsName`. A row that has an id but no MAC, address, or name is also skipped, because nothing could ever correlate with it.
- Match behavior (set once in `CONFIG`): `no-mac-break no-ip-break no-name-break`. The MAC, IP, and names on a row are what a sensor observed at one moment - a DHCP lease moves, a reverse lookup lags, a NIC is replaced - and RSD updates them in place on the same `HostID`. None of that churn should disqualify a merge against the authoritative id. Id matching and id breaking stay on, so two RSD records are never silently collapsed into one asset: an RSD duplicate stays visible as a duplicate until it is merged in RSD.
- Verdict: scoped authoritative.

### Why not the MAC or `AgentGUID`

The MAC is the natural identity of a *detection* but not of a *system*: RSD's own matching exists precisely to roll several MACs into one detected system, and a MAC-keyed id would split such a system every time RSD updated the primary interface. `AgentGUID` is null on every system without an agent - the population this integration exists to import - and is minted anew by an agent reinstall. Both are imported as custom attributes instead, where they remain searchable without driving identity.

### Notes

- Assets come from a single `detectedsystem.find` call. The command requires a `searchText` argument; when `search_text` is blank the script sends a single space, which is how the vendor-independent client lists every detected system. **There is no pagination**: ePO answers the whole result set in one response and the command accepts no offset, limit, or cursor. The rows are streamed off the response text with `jsonstream` and each asset goes to `report_asset` as it is built, so the decoded estate never exists as one value - but the response body itself is held in memory, so a very large Detected Systems list is a large response. See `## Future` for the server-side alternative.
- **Responses are not plain JSON.** Every reply is prefixed with a status line - `OK:\r\n` before the document, or `Error <code>:\r\n` before a plain-text message. The script uses raw `http.get` and strips that prefix before decoding, because `get_json` cannot parse the body. A payload that is not a JSON array is reported as an error rather than handed to the row iterator, since a malformed document would abort the script.
- **The transport carries its own bounded retry.** The raw `http.get` builtin has no `retries` argument, so each command is retried by hand: up to three attempts with a short backoff, repeated only on a missing response or a transient status (408/425/429/5xx). The command is a read, so repeating it is safe. A persistent failure, a 401/403, or an `Error` envelope ends the task in error rather than reporting a green run with zero assets.
- Imported fields: `DnsName` and `NetbiosName` become hostnames (placeholders and IP-shaped names are screened); `Domain` becomes the domain; `MAC`, the decoded `IPV4`, and `IPV6` become one network interface; `OSPlatform` becomes the OS and `OSVersion` the OS version, matching how both third-party consumers of this table map them; `DeviceType` becomes the device type; `LastDetectedTime` becomes `lastSeenTS`. runZero fingerprinting still takes precedence over the OS and type (`trustOS`/`trustType` are not set), because an RSD sensor's passive fingerprint is not more reliable than runZero's own.
- **Every `RSDDetectedSystems` column is kept as a custom attribute**, named `trellix_rsd_<snake_case_column>`: `trellix_rsd_host_id`, `trellix_rsd_org_name` (the OUI organization), `trellix_rsd_users`, `trellix_rsd_rogue_state`, `trellix_rsd_server_name` (the owning ePO for an alien agent), `trellix_rsd_last_reporting_sensor`, `trellix_rsd_exception_category`, `trellix_rsd_agent_guid`, and so on. Columns are passed through generically rather than enumerated because the full column set could not be confirmed against a live console; whatever the console returns is kept. Only `IPV4` is rewritten, from the biased integer to the dotted quad.
- **Status.** RSD keeps a separate bit column for each of `Exception`, `Inactive`, `Managed`, and `Rogue`, and does not document them as mutually exclusive. The script derives one status per row by taking the first flag set in that order - an exception stays an exception when it goes quiet, and a rogue that no sensor has seen for the inactive period is inactive rather than rogue - tags the asset `rsd-status:<status>`, and applies the three `include_*` filters to it. A row with no flag set is always imported and carries no status tag. The raw flags remain available as `trellix_rsd_exception`, `trellix_rsd_inactive`, `trellix_rsd_managed`, and `trellix_rsd_rogue`. The precedence is this integration's derivation, not a vendor statement; see the first item under `## Assumptions`.
- **`IPV4` is not a dotted quad.** It is the address biased by 2^31 and stored as a signed 32-bit integer so that it sorts correctly in SQL Server - the same encoding as `EPOComputerProperties.IPV4x` in the System Tree, and the same `+ 2147483648` conversion the Splunk DB Connect input applies to this very column. A column of exactly 0 is treated as unset rather than decoded to `128.0.0.0`.
- `OrgName` is the organization registered for the MAC's OUI - the NIC vendor, which is not necessarily the device manufacturer - so it is imported as an attribute rather than as `manufacturer`. runZero derives the MAC vendor from the imported MAC itself.
- ePO's literal placeholders (`N/A`, `(none)`, `<null>`) and the `Unknown` that fingerprint columns fall back to are treated as empty rather than imported as values, case-insensitively.
- This integration was validated against local fixtures built from the field list of a vendor-independent API client and the documented column encodings, not against a live Trellix ePO console with RSD installed. See `## Assumptions` for what a live run should confirm first.

### Assumptions

Each of these is believed to be correct, is the simplest reading of the evidence found, and could not be confirmed without a live console. Each is cheap to check on the first run.

1. **State flag precedence.** That an exception that goes quiet remains an exception in the Detected Systems status monitor, rather than moving to inactive. If a console shows otherwise, swap `"Exception"` and `"Inactive"` in `STATUS_FLAGS`.
2. **A blank filter.** That `detectedsystem.find` accepts `searchText=%20` and returns every detected system (as the PowerShell client relies on), and that it returns managed, exception, and inactive systems rather than only rogues. The filters are a no-op if it returns only rogues.
3. **The `Unknown` placeholder.** That unclassified fingerprint columns say `Unknown`; a console that writes something else simply imports that word as the OS or device type.
4. **`DeviceType` vocabulary.** The values RSD assigns (`Workstation`, `Printer`, ...) are passed through as the runZero device type unmodified; runZero fingerprinting overrides them where it has its own evidence.
5. **`MAC` format.** Assumed to be the unpunctuated hex ePO uses elsewhere (`000C29B1EE8E`); `network_interface` accepts colon- and dash-punctuated forms equally, so a different spelling costs nothing.

## Future

- **Per-interface rows.** RSD keeps a Detected System Interfaces list under each system (the product guide: "each system can have multiple interfaces"), and the Detected Systems page lists interfaces by subnet. `detectedsystem.find` returns one row per system carrying only the primary MAC and address. The interface table is reachable through `core.executeQuery`, but its SQUID target name and columns were not found in any public documentation or client, so no query is shipped. The right next step is `core.listTables` against a live console: find the `RSD*` targets, capture their `relatedTables`/`foreignKeys` blocks, and join them to `RSDDetectedSystems.HostID`.
- **Server-side filtering and bounded responses.** `core.executeQuery?target=RSDDetectedSystems` with a SQUID `where=` clause - for example `(eq RSDDetectedSystems.Rogue 1)` - would let ePO apply the state filters, and `(top N)` in the `select` with `order=(order (asc RSDDetectedSystems.HostID))` plus a `(gt RSDDetectedSystems.HostID <last>)` predicate is the shape of a keyset walk that would keep each response small on very large estates. Both depend on the target name matching the table name, which holds for every ePO table seen so far (`EPOLeafNode`, `EPOComputerProperties`, `EPOBranchNode`) but was not confirmed for `RSDDetectedSystems`; confirm it with `core.listTables?table=RSDDetectedSystems` before building on it.
- **Subnet and sensor coverage.** RSD classifies every known subnet as covered, uncovered, or containing rogues, and every sensor as active, passive, or missing. Both lists are visible in the console and almost certainly queryable; a coverage attribute on each asset (whether its subnet has an active sensor) would tell an operator how much to trust a quiet RSD record.
- **Exception write-back.** The Detected Systems page actions - Add to Exceptions, Remove from Exceptions, Add to System Tree, Deploy Agent, Delete - are expected to have `detectedsystem.*` command counterparts in the same family as `detectedsystem.find`; `core.help?prefix=detectedsystem` on a live console lists them with their arguments. An outbound integration could mark runZero-classified printers, phones, and network gear as RSD exceptions, or push a Trellix Agent to a rogue system runZero has identified as a managed workstation. All are state-changing and would need care.
- **Coverage-gap reporting.** Pairing this integration with the System Tree one makes the gap visible from both sides: `tag:rsd-status:rogue` is the list of machines RSD saw on the wire with no agent, and a runZero asset with neither source is a device no Trellix component has seen at all.

## API documentation

- Trellix ePO On-prem Web API Scripting Reference Guide: [overview](https://docs.trellix.com/docs/trellix-epolicy-orchestrator-on-prem-web-api-scripting-reference-guide), [key commands](https://docs.trellix.com/docs/key-commands), [remote query commands](https://docs.trellix.com/docs/remote-query-commands), [ad-hoc query reference](https://docs.trellix.com/docs/ad-hoc-query-reference), [using the web URL Help](https://docs.trellix.com/docs/using-the-web-url-help-best-practice), and [S-Expressions in web URL queries](https://docs.trellix.com/docs/using-s-expressions-in-web-url-queries-best-practice) - the `/remote/<command>?:output=json` URL shape, HTTP Basic authentication, the `OK:` envelope, `core.help`, `core.listTables`, and the `core.executeQuery` grammar. The ePO console describes its own commands at run time: `core.help?prefix=detectedsystem` lists the RSD command family.
- [Rogue System Detection 5.0.7 Product Guide](https://docs.trellix.com/docs/rogue-system-detection-5-0-7-product-guide) - the system/interface model, the Rogue, Managed, Exception, and Inactive states (and the alien-agent and inactive-agent rogue sub-states), detected-system matching and merging, and the Exceptions list.
- `detectedsystem.find` command and the `RSDDetectedSystems.*` result columns: `Find-ePoDetectedSystem` in the [posh-epoapi](https://www.powershellgallery.com/packages/posh-epoapi/1.3.3/content/functions/find-epodetectedsystem.ps1) PowerShell module, which calls the command with `searchText` and reads `HostID`, `NetbiosName`, `DnsName`, `FriendlyName`, `Domain`, `OrgName`, `MAC`, `IPV6`, `OSPlatform`, `OSFamily`, `OSVersion`, `DeviceType`, `Users`, `Comments`, `NetbiosComment`, `AgentGUID`, `AgentVersion`, `Managed`, `Rogue`, `Exception`, `ExceptionCategory`, `Inactive`, `Ignored`, `NewDetection`, `RogueAction`, `RogueState`, `ServerName`, `DetectedSourceName`, `LastReportingSensor`, `LastDetectedTime`, and `LastAgentCommunication` from each row.
- `RSDDetectedSystems.IPV4` encoding and `HostID` as the table key: a [Splunk DB Connect input against the ePO database](https://community.splunk.com/t5/All-Apps-and-Add-ons/Is-anyone-getting-RSD-Rogue-System-Detection-alerts-from-ePO/m-p/253222), which converts `IPV4 + 2147483648` to a dotted quad and uses `HostID` as its rising column.
- Response envelope, command URL shape, and HTTP Basic authentication cross-checked against McAfee's own `mcafee.py` Python client (`_CommandInvoker.parse_response` and `build_url_request`) as republished in [PoesRaven/mcafee-epo-tools](https://github.com/PoesRaven/mcafee-epo-tools).

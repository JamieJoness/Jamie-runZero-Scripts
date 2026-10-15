# Custom Integration: MECM hardware, software and update inventory

Inbound runZero custom integration that reads the **Microsoft Endpoint
Configuration Manager (MECM / SCCM) site database** and attaches the inventory
runZero's native MECM integration does not import to the assets runZero already
knows:

- hardware inventory: manufacturer, model, chassis type and asset tag, BIOS,
  processors, memory, logical and physical disks, OS install and last boot time;
- installed software, as runZero software records;
- device collection membership;
- missing (required) software updates, as runZero vulnerability records.

It is designed to **enrich** the asset that scans and the native MECM
integration already created, not to create a parallel copy of it. Every record
carries the same correlators the native integration imports (hostnames, MAC
addresses, IP addresses from MECM's discovery data), and the recommended task
setting makes the integration enrichment-only.

Setting it up is four steps, described under [Steps](#steps): a read-only
login on the SQL Server that holds the MECM site database (or reuse the one the
native MECM integration already uses), the custom integration in runZero, a
**Custom Integration Script Secrets** credential holding the connection details
and import options, and a scheduled task on an Explorer that can reach the
database.

## What the native MECM integration already covers

The native integration (**Tasks → Integrations → MECM**) imports one row per
device from `v_CombinedDeviceResources`, with the IPv4/IPv6 discovery arrays and
the single most recently installed update, and keys it on `MachineID`. That gives
runZero client health, Endpoint Protection and Defender for Endpoint status,
Entra IDs, serial number, SMBIOS GUID, users, OS name/build, last hardware and
software scan times, boundary groups and the last patch title. It does **not**
set hardware manufacturer or model, and it imports no software, no per-update
compliance and no collection membership. This integration adds exactly those,
and nothing the native integration already provides, so the two run side by side
without duplicating attributes.

| Data | Source view(s) | Lands on the asset as |
| --- | --- | --- |
| Manufacturer, model, system type, processor count, domain role | `v_GS_COMPUTER_SYSTEM` | `manufacturer` / `model` fields and `hardware.*` attributes |
| Chassis type, asset tag | `v_GS_SYSTEM_ENCLOSURE` | `hardware.chassisType`, `hardware.chassisTypeCode`, `hardware.assetTag`; drives `deviceType` for physical machines |
| BIOS manufacturer, name, SMBIOS version, release date | `v_GS_PC_BIOS` | `bios.*` |
| Processor name, manufacturer, clock, sockets, cores, logical processors, 64-bit | `v_GS_PROCESSOR` (one row per socket, aggregated) | `cpu.*` |
| Total physical memory | `v_GS_X86_PC_MEMORY` | `memory.totalPhysicalMb` |
| Fixed logical disks (`DriveType0 = 3`) | `v_GS_LOGICAL_DISK` | `disk.<letter>.{sizeMb,freeMb,fileSystem,volumeName}`, `disk.count`, `disk.totalSizeMb`, `disk.totalFreeMb` |
| Physical disks | `v_GS_DISK` | `physicalDisk.<index>.{model,sizeMb,interfaceType,mediaType}`, `physicalDisk.count` |
| OS caption, version, build, architecture, product type, install date, last boot | `v_GS_OPERATING_SYSTEM` | `os.*` attributes (the `os` field itself is left to the native integration and fingerprinting) |
| Device collection membership (custom collections only) | `v_FullCollectionMembership` + `v_Collection` | `collections`, `collectionIds`, `collectionCount` |
| Installed software | `v_GS_INSTALLED_SOFTWARE` (Asset Intelligence) or `v_Add_Remove_Programs` | software records; `software.source`, `software.count`, `software.truncated` |
| Required, unexpired updates | `v_Update_ComplianceStatus` + `v_UpdateInfo` + `v_CICategoryInfo` | vulnerability records; `missingUpdates.count`, `missingUpdates.truncated` |
| Correlators | `v_R_System`, `v_RA_System_ResourceNames`, `v_RA_System_MACAddresses`, `v_RA_System_IPAddresses`, `v_RA_System_IPv6Addresses` | `hostnames`, `domain`, network interfaces, `firstSeenTS` (record creation date) |

## Architecture and data flow

1. A scheduled runZero custom integration task runs this Starlark script on an
   Explorer with network access to the site database's SQL Server instance.
2. The script connects over TDS with the runZero `runzero.sql` module (the
   go-mssqldb driver), TLS on by default, and reads the site code from
   `v_Identification`. Every asset id is scoped by that site code.
3. It checks `INFORMATION_SCHEMA.VIEWS` for the inventory views it intends to
   read. A view that is absent (its hardware inventory class is disabled) or not
   readable by the login is logged once and skipped; nothing else changes.
4. It walks `v_R_System` in pages (default 100 devices) using the `ResourceID`
   as a keyset cursor, skipping obsolete and decommissioned records, and
   optionally restricted to one device collection.
5. For each page it issues one parameterised statement per view, filtered to
   that page's `ResourceID`s, and assembles one `ImportAsset` per device, which
   is streamed to the Console as it is built. Nothing larger than one page is
   ever held in memory.
6. runZero merges each record into the existing asset by MAC, IP and hostname —
   see [Asset identity and correlation](#asset-identity-and-correlation).

All statements are read-only `SELECT`s against the documented reporting views.
Values are bound as parameters (`sp_executesql`), never interpolated into SQL.
The connection identifies itself to SQL Server as `runZero-mecm-inventory` (the
`app name` connection property), so it is easy to spot in `sp_who2`, Query Store
or Extended Events.

## Why this needs a SQL Server login

Everything Configuration Manager knows about a device — its hardware
inventory, installed software, the collections it belongs to, the updates it
still needs — lives in **one Microsoft SQL Server database, the site database**.
It is normally named `CM_` followed by the three-character site code, for
example `CM_PS1`. The Configuration Manager console and MECM's own reports are
front ends onto that database; they hold no data of their own.

Other products read this inventory the way MECM's own reports do: by
connecting to that SQL Server and reading the **reporting views**
(`v_R_System`, `v_GS_COMPUTER_SYSTEM`, …) Microsoft documents for the purpose.
That is exactly how runZero's native MECM integration works — its credential
type is *MECM Database Connection String* — and it is exactly what this
integration does. (MECM also has a REST administration service, but it is built
for console and PowerShell access to one object at a time, not for reporting
across every device.) From SQL Server's point of view this integration is a
reporting tool running read-only `SELECT` statements.

Connecting to SQL Server needs an account **on the SQL Server itself**, called a
*login*. It is separate from your Configuration Manager console roles, your
Windows account and your runZero account: SQL Server alone decides who may read
the database, and nobody else can grant that. So the one piece of setup outside
runZero is giving runZero a read-only login on the site database, and the only
reason it has to be done *on the SQL Server* is that that is where accounts for
that database are created.

### If you already run runZero's native MECM integration

You have already done this once. The connection string entered for that
credential (runZero does not show it back once saved, so take it from wherever
it was recorded, or ask whoever set that integration up) looks like one of
these:

```text
Server=sqlhost,1433;Database=CM_PS1;User Id=svc_runzero;Password=...;
sqlserver://svc_runzero:...@sqlhost:1433?database=CM_PS1
```

`Server`/`sqlhost:1433` is **SQL Server host** and **Port**, `Database` is
**Site database**, and `User Id`/`Password` are **Username** and **Password**.
That login already reads the same database, so it works here unchanged and you
can skip straight to [Step 2](#step-2-add-the-custom-integration-to-runzero).
Create a new login only if you want this integration to have its own account.

## Requirements

**runZero**

- Superuser access, to create the custom integration, the credential and the
  scheduled task.
- runZero platform 5.1.260818.0 or newer (the script's `minVersion`).
- An Explorer with a network path to the SQL Server instance (TCP 1433 by
  default). The site database is on your internal network, so the task must run
  on an Explorer; a Console-hosted task cannot reach it.

**Configuration Manager**

- Current branch, with the standard hardware inventory classes enabled
  (Computer System, System Enclosure, PC BIOS, Processor, X86 PC Memory,
  Logical Disk, Disk, Operating System). These are on by default. Software
  needs either the **Installed Software (Asset Intelligence)** class or the
  **Add/Remove Programs** classes; both are on by default.
- Know where the site database is. In the Configuration Manager console,
  **Administration → Site Configuration → Servers and Site System Roles** lists
  the server holding the **Site database server** role; the database is
  `CM_<site code>` unless it was renamed at install. If the database is on a
  *named* instance (`SERVER\INSTANCE`), use that instance's TCP port as the
  **Port** — this integration connects to a host and port and does not resolve
  instance names, so a dynamic port should be pinned to a static one in SQL
  Server Configuration Manager.

**SQL Server**

- A login that can read the site database views: a member of the database
  role `smsschm_users` — the role MECM itself puts its reporting account in,
  which gives read access to the reporting views and no write access — or of
  `db_datareader`. [Step 1](#step-1-create-the-sql-server-login) creates one.
- For a SQL Server login (the default here), the instance must allow **SQL
  Server and Windows Authentication mode** (Mixed Mode). An instance set to
  Windows Authentication only rejects every SQL login regardless of password.
  Windows authentication with a `DOMAIN\user` login works over NTLM from a
  Linux Explorer in principle, but that path has not been exercised by this
  integration's tests; verify it on your site before relying on it.
- A server certificate the Explorer trusts, or **Trust the server certificate**
  enabled when the instance still uses the self-signed certificate SQL Server
  generates at install (see the credential fields in Step 3).

## Steps

### Step 1: Create the SQL Server login

Skip this if you are reusing the native integration's login.

"Run this on the SQL Server" means: whoever administers that SQL Server (your
DBA, or the MECM administrator who installed the site) opens **SQL Server
Management Studio** (SSMS) or Azure Data Studio, connects to the instance that
hosts the site database with an account that has `sysadmin` rights on it, opens
a **New Query** window, pastes the statements below and clicks **Execute**. It
runs once and creates one account; nothing is installed and nothing of
runZero's ever runs on the SQL Server.

```sql
-- 1. An account on the SQL Server instance. Choose a strong password and
--    store it only in the runZero credential.
USE [master];
CREATE LOGIN [svc_runzero_mecm] WITH PASSWORD = N'<strong password>', CHECK_POLICY = ON;
GO
-- 2. Let that account into the site database (change CM_PS1 to yours).
USE [CM_PS1];
CREATE USER [svc_runzero_mecm] FOR LOGIN [svc_runzero_mecm];
-- 3. Read access to the reporting views, through the role MECM uses for its
--    own reporting account. It cannot write or change anything.
ALTER ROLE [smsschm_users] ADD MEMBER [svc_runzero_mecm];
GO
```

The login needs nothing else: no write permission, no access to other
databases, no server roles. For Windows authentication instead, replace the
first statement with `CREATE LOGIN [CORP\svc_runzero_mecm] FROM WINDOWS;`, use
the same `CORP\...` name in the other two, and enter `CORP\svc_runzero_mecm` as
the username in runZero.

Then check it works, still in SSMS: **Connect → Database Engine**, set
*Authentication* to **SQL Server Authentication**, sign in as the new login and
run:

```sql
USE [CM_PS1];
SELECT ThisSiteCode FROM v_Identification;
SELECT COUNT(*) AS devices FROM v_R_System;
```

The first query returns your site code — it is the first thing the integration
reads — and the second a device count. If either fails, fix it here; SSMS shows
clearer error messages than a task log will.

### Step 2: Add the custom integration to runZero

1. In the [runZero console](https://console.runzero.com/custom-integrations),
   go to **Account → Custom integrations → Add custom integration**.
2. Name it exactly `mecm-inventory`. **The saved queries below use this name as
   the attribute namespace** (`@mecm-inventory.device.…`); a different name
   means editing every query to match.
3. Toggle **Enable custom integration script**, paste the contents of
   [mecm-inventory.star](./mecm-inventory.star) into the editor, click
   **Validate**, then **Save**.

The script carries its own `CONFIG` block, so runZero now knows which fields
this integration needs and builds the credential form from it. The **Credential
fields** disclosure on the integration page lists them.

### Step 3: Create the credential

The credential is where the connection details and the import options live;
the task only points at it.

1. Go to **[Credentials](https://console.runzero.com/credentials) → Add
   Credential**.
2. Choose the type **Custom Integration Script Secrets**.
3. Give it a name (for example `MECM site database`) and select the
   `mecm-inventory` integration. The form shows this integration's fields:

   | Field | Default | What to enter |
   | --- | --- | --- |
   | SQL Server host, Port | — / `1433` | The server holding the **Site database server** role, and the instance's TCP port. |
   | Username, Password | — | The login from Step 1, or the native integration's. The password is encrypted at rest and never logged. |
   | Site database | — | `CM_<site code>`, e.g. `CM_PS1`. |
   | Require TLS encryption | on | Leave on. |
   | Trust the server certificate | off | Turn on when the instance uses the self-signed certificate SQL Server generates at install; otherwise the TLS handshake fails before login. The session stays encrypted; only certificate validation is skipped. |
   | Query timeout (seconds) | `60` | Per statement. |
   | Device collection ID | empty | Restrict the import to one device collection, e.g. `PS100012`. Use it for the first test run. |
   | Installed software source | `installed_software` | `add_remove_programs` if Asset Intelligence inventory is disabled on your site; `none` to skip software. |
   | Software records per device | `1000` | Cap per asset; the true total is always recorded in `software.count`. |
   | Missing updates to import | `security` | `security` = Security Updates and Critical Updates classifications; `all` = every required, unexpired update (this includes Definition Updates, which churn daily); `none`. |
   | Missing updates per device | `99` | Cap per asset, most severe first; the true total is in `missingUpdates.count`. |
   | Devices per page | `100` | Devices read per round of queries. |

4. To let other organizations use it, select **Make this a global credential**.
5. Save the credential.

### Step 4: Create and run the task

1. Go to **Tasks → Integrate → Custom Scripts** (or use **Save & create task**
   on the integration page).
2. Select the `mecm-inventory` integration and the credential from Step 3.
   Saving checks the pair: the script must compile and the credential must hold
   every required field, or the save fails naming the missing parameter keys.
3. Choose an Explorer that can reach the SQL Server, and the site to add any
   new assets to.
4. Tick **Exclude assets that cannot be merged into an existing asset**. This
   makes the integration enrichment-only: a device MECM knows but runZero has
   never seen by hostname, MAC or IP is skipped instead of becoming a new,
   sparse asset. Leave it unticked only if you want MECM-only devices to appear
   as new assets.
5. Schedule it **daily**, a few hours after your hardware inventory cycle.
   Inventory in MECM changes at most once per inventory cycle (default every 7
   days for hardware), so more frequent runs add database load without adding
   data.
6. Activate the task, run it once and open its log. A healthy run ends with a
   line like `mecm-inventory: site PS1: 1234 devices read, 1230 assets
   reported, 2 placeholder records skipped, 2 devices without a hostname, MAC
   or IP skipped, 187654 software records and 3120 missing updates attached`.

### Validation on a small test set

1. For the first run set **Device collection ID** on the credential to a small
   collection you know (a pilot collection of a dozen devices), then clear it
   once satisfied.
2. Search **Assets** for `custom_integration:mecm-inventory` — every asset the
   task touched.
3. Open two or three of them. Under **Attributes**, the `mecm-inventory` section
   should show `hardware.model`, `cpu.cores`, `disk.C.freeMb`, `collections` and
   the rest; the **Software** tab lists the inventoried titles and the
   **Vulnerabilities** tab the required updates, both attributed to this
   integration.
4. Confirm enrichment, not duplication: the attributes should sit on the
   *existing* asset for that device — the one that also carries your scan
   and/or native MECM data. A new hostname-only asset means the correlators did
   not match an existing asset; see [Troubleshooting](#troubleshooting).
5. Spot-check one device in the Configuration Manager console (**Assets and
   Compliance → Devices → Resource Explorer**); the values should agree.

## Imported data

Attributes appear in search as `@mecm-inventory.device.<attribute>`. Timestamps
from the database are stored as ISO 8601 strings in UTC (`2026-10-12T07:31:05Z`),
`firstSeenTS` as epoch seconds; sizes are MB; booleans are `true`/`false`.

| Attribute | Content |
| --- | --- |
| `resourceId`, `siteCode` | MECM identity of the record (`ResourceID`, the site code of the database). |
| `hardware.manufacturer`, `hardware.model`, `hardware.systemType`, `hardware.processorCount`, `hardware.domainRole` | `v_GS_COMPUTER_SYSTEM`. Domain role is decoded (`Member Workstation`, `Member Server`, `Primary Domain Controller`, …). |
| `hardware.chassisType`, `hardware.chassisTypeCode`, `hardware.assetTag` | `v_GS_SYSTEM_ENCLOSURE`. Chassis decoded from the SMBIOS table (`Notebook`, `Rack Mount Chassis`, `Mini PC`, …); the raw code is kept too. |
| `bios.manufacturer`, `bios.name`, `bios.version`, `bios.releaseDate` | `v_GS_PC_BIOS`; `bios.version` is the SMBIOS BIOS version string. |
| `cpu.name`, `cpu.manufacturer`, `cpu.maxClockMhz`, `cpu.sockets`, `cpu.cores`, `cpu.logicalProcessors`, `cpu.is64Bit` | `v_GS_PROCESSOR`, aggregated across sockets. |
| `memory.totalPhysicalMb` | `v_GS_X86_PC_MEMORY`. |
| `disk.<letter>.sizeMb`, `disk.<letter>.freeMb`, `disk.<letter>.fileSystem`, `disk.<letter>.volumeName`, `disk.count`, `disk.totalSizeMb`, `disk.totalFreeMb` | Fixed logical disks only. |
| `physicalDisk.<index>.model`, `physicalDisk.<index>.sizeMb`, `physicalDisk.<index>.interfaceType`, `physicalDisk.<index>.mediaType`, `physicalDisk.count` | `v_GS_DISK`. |
| `os.caption`, `os.version`, `os.build`, `os.architecture`, `os.productType`, `os.installDate`, `os.lastBootUpTime` | `v_GS_OPERATING_SYSTEM`. Product type decoded (`Workstation`, `Server`, `Domain Controller`). |
| `collections`, `collectionIds`, `collectionCount` | Device collections (`CollectionType = 2`) excluding the built-in `SMS…` collections such as All Systems, comma-joined and sorted by name. |
| `software.source`, `software.count`, `software.truncated` | Which view software came from, how many distinct titles the device has, and `true` only when the per-device cap dropped some. |
| `missingUpdates.count`, `missingUpdates.truncated` | How many required updates matched the classification filter, and `true` only when the cap dropped some. |

The asset itself gets:

- `hostnames`: the NetBIOS name and the FQDNs MECM discovered, screened for
  placeholders; `domain`: the AD DNS domain, so the NetBIOS name can be
  qualified.
- network interfaces: one per discovered MAC address (all-zero and broadcast
  placeholders dropped) plus the routable IPv4/IPv6 addresses (loopback, APIPA
  and link-local dropped). MECM stores MACs and IPs as independent arrays, so no
  MAC–IP pairing is claimed.
- `manufacturer` and `model` from `v_GS_COMPUTER_SYSTEM`, which the native
  integration never sets.
- `deviceType`: `Server` when the OS product type or domain role says server;
  otherwise, for physical machines only, the chassis type mapped to runZero's
  types (`Laptop`, `Desktop`, `Tablet`, `Mobile`, `Server`). Virtual machines
  (`Is_Virtual_Machine0`) get no chassis-derived type because a hypervisor's
  fake chassis is meaningless (Hyper-V reports Desktop); fingerprinting decides.
- `firstSeenTS`: the MECM record's creation date.
- **Software** records: product, version, vendor, install date, and the
  `softwareCode`, `installedLocation`, `installSource` and `osComponent`
  attributes (Asset Intelligence) or the `productId` attribute (Programs and
  Features). Duplicates by product/version/vendor collapse to one record; the
  list is sorted by product and capped per device.
- **Vulnerability** records, one per required update: the update title as the
  name, Microsoft's description, a solution naming the KB article, the MSRC
  severity as runZero's severity rank (Critical 4, Important 3, Moderate 2, Low
  1; non-security updates carry none), the posting date as `publishedTS`, the
  time the device's status became Required as `firstDetectedTS`, the last
  compliance check as `lastDetectedTS`, and the `ciId`, `articleId`,
  `bulletinId`, `classification`, `severity`, `isSuperseded`, `infoUrl` and
  `status` attributes. Expired updates are never imported; superseded ones are,
  flagged, because a device that still needs one still needs the fix. Sorted
  most severe first and capped per device.

## Saved queries

Create these under **Assets → run the query → Save**. Attribute names are
case-sensitive; the `@mecm-inventory` prefix is the integration's name in your
console and `device` is the attribute category the script declares.

Laptops by chassis, regardless of how they were fingerprinted:

```text
@mecm-inventory.device.hardware.chassisType:Notebook OR @mecm-inventory.device.hardware.chassisType:Laptop OR @mecm-inventory.device.hardware.chassisType:Convertible
```

Devices missing at least one security update according to MECM:

```text
@mecm-inventory.device.missingUpdates.count:>0
```

Members of a collection (substring match on the comma-joined list):

```text
@mecm-inventory.device.collections:"Finance Workstations"
```

Low disk space on the system drive (less than 20 GB free):

```text
@mecm-inventory.device.disk.C.freeMb:<20480
```

Machines with less than 16 GB of memory or not rebooted in 30 days:

```text
@mecm-inventory.device.memory.totalPhysicalMb:<16384
@mecm-inventory.device.os.lastBootUpTime:<2026-09-14
```

Windows assets MECM has no inventory for (in your estate but not reported by
this integration):

```text
os:windows AND NOT has:@mecm-inventory.device.resourceId
```

The **Software** and **Vulnerabilities** inventories can be filtered to this
integration's records with `source:custom` and the integration name, and the
missing-update records expose `articleId` and `classification` for grouping.

## Asset identity and correlation

- Target entity: a device record in the MECM site database (a managed Windows
  computer; non-client records discovered from AD are included because they
  still carry collection membership and a hostname).
- Source ID field: `v_R_System.ResourceID` — the same value
  `v_CombinedDeviceResources.MachineID` carries, which the native integration
  keys on.
- Documentation evidence: Microsoft's
  [Discovery views](https://learn.microsoft.com/en-us/intune/configmgr/develop/core/understand/sqlviews/discovery-views-configuration-manager)
  — "`v_R_System` … can be joined to other views by using the ResourceID",
  and every inventory view in the
  [Hardware inventory views](https://learn.microsoft.com/en-us/intune/configmgr/develop/core/understand/sqlviews/hardware-inventory-views-configuration-manager)
  reference "can be joined with other views by using the ResourceID column".
- Uniqueness scope: the site database. `ResourceID` is an identity column, so it
  is unique within a database (hierarchy-wide in a CAS database).
- Cardinality: one `v_R_System` row per device record. A device reinstalled with
  a new client identity gets a new record and the old one is flagged obsolete;
  obsolete and decommissioned records are excluded, so the live record is the
  only one imported. Per-device child rows (software, disks, processors,
  collections, updates) are attached to that one asset, never emitted as
  assets.
- Stability: survives rename, re-addressing, OS upgrade and client upgrade.
  Reinstall or reimage with a new client GUID creates a new record (and a new
  asset id; runZero merges it onto the same asset by MAC/hostname).
- Reuse behavior: identity values are not recycled.
- Presence: required; it is the primary key of the view.
- Final runZero ID: `mecm-inventory:<site code>:<ResourceID>`, the site code
  read from `v_Identification.ThisSiteCode` at the start of every run. If it
  cannot be read the run fails rather than falling back to another scope, which
  would re-identify every asset.
- Missing-ID behavior: not possible for a row of the primary listing.
- Match behavior: `no-mac-break no-ip-break no-name-break` — this source's own
  id is authoritative across runs, and a MAC, IP or hostname that changed since
  the last run must not disqualify the merge onto the asset it matched before.
- Verdict: **authoritative foreign ID** (site-scoped).

### How records reach your existing assets — read this before relying on it

Verified against the platform's merge engine, not assumed:

- **Foreign IDs do not merge across sources.** The cruncher looks up foreign-id
  merge candidates within the *same* integration, so this integration's
  `ResourceID`-based id never id-matches the native MECM integration's record,
  even though both derive from the same number.
- The cross-source signal is therefore the correlators: the NetBIOS name and
  FQDNs, the discovered MAC addresses and the discovered IP addresses — the same
  discovery arrays the native integration imports, which is what makes this
  record land on the same asset.
- Records that match an existing asset **enrich it**. Records that match
  nothing either create a hostname/MAC-only asset (default) or are skipped
  (**Exclude assets that cannot be merged into an existing asset** — recommended).
- Repeat runs are idempotent: the same device updates the same record through
  this integration's stable id, whichever asset it merged into.
- Two live MECM records for one physical machine (a reimaged device whose old
  record is not yet obsolete) both import and both merge onto the same runZero
  asset; the attributes of whichever record is processed last are shown.
- Records with no usable hostname, MAC or IP are skipped and counted; with no
  correlator they could only ever become orphans.

To find stragglers created before you enabled the exclude toggle:
`custom_integration:mecm-inventory AND source_count:1`.

## Failure behavior

A run has three outcomes. `runzero.sql` raises on any database error, which ends
the task in error with the server's own message.

| Condition | Behavior |
| --- | --- |
| Host unreachable, login rejected, TLS handshake failed | Task ends in error at connect, e.g. `connect: ping: mssql: Login failed for user 'svc_runzero'. (18456)`. The log line before it names the host, port, user, database and TLS settings. |
| `v_Identification` unreadable or empty | Task ends in error: the site code scopes every asset id. |
| Devices query fails (permission denied, missing view) | Task ends in error with the SQL error, e.g. `The SELECT permission was denied on the object 'v_R_System' …`. |
| A page fails mid-walk | Assets already streamed are kept; the task still ends in error so a truncated import cannot pass as a complete one. |
| An inventory view is absent or not readable | Logged once (`view v_GS_SYSTEM_ENCLOSURE is not present or not readable by this login; its data is skipped`), the run continues without that data. |
| A single view returns 1,000,000 rows for one page | Task ends in error asking for a smaller `page_size`; the driver would otherwise truncate silently. |
| Placeholder record (`x86 Unknown Computer`, `Provisioning Device`) | Skipped and counted. |
| Device with no hostname, MAC or IP | Skipped and counted. |
| Malformed row (null product name, unparseable install date, duplicate CI row) | The row is dropped or the field left unset; the device still imports. |
| Empty site or empty collection | Success, with `site PS1 has no device records` in the log. |
| Invalid parameter (collection id not eight alphanumerics) | Refused by CONFIG validation before `main()` runs. |

The page walk is bounded by `maxPages` (100,000 pages) and the guard errors
rather than silently truncating if it ever trips.

## Troubleshooting

- **Task save fails naming parameter keys** (`host`, `username`, …) — the
  selected credential is missing a required field, or is not a *Custom
  Integration Script Secrets* credential for this integration. Edit the
  credential, not the task.
- **`Login failed for user`** — wrong password; the login exists on the
  instance but has no user in the site database (`CREATE USER … FOR LOGIN`);
  or the instance is in Windows Authentication only mode and rejects every SQL
  login (SSMS: right-click the server → **Properties → Security**).
- **TLS handshake error at connect** — the instance presents the self-signed
  certificate SQL Server generates at install. Install a certificate the
  Explorer trusts, or enable **Trust the server certificate** on the credential.
- **`The SELECT permission was denied`** — the user is not in `smsschm_users`
  (or `db_datareader`).
- **`view … is not present or not readable`** — the hardware inventory class
  behind that view is disabled in client settings (**Administration → Client
  Settings → Hardware Inventory → Set Classes**), or the login cannot see it.
  For software, switch **Installed software source** on the credential to the
  class your site collects.
- **New hostname-only assets instead of enrichment** — the MECM record's
  NetBIOS name/FQDN/MAC/IP did not match an existing asset. Check the asset's
  `hostnames` and `macAddresses` against what runZero holds for that device,
  and turn on **Exclude assets that cannot be merged into an existing asset**
  on the task so unmatched records are skipped.
- **A VM shows as `Desktop`** — the record is not flagged `Is_Virtual_Machine0`
  in MECM, so its hypervisor-reported chassis was used. runZero's fingerprinting
  from other sources still outranks a custom integration's type when it has
  one.
- **Software or update counts look capped** — `software.truncated` or
  `missingUpdates.truncated` is `true`; raise the per-device cap on the
  credential.
- **`one page returned 1000000 rows`** — lower **Devices per page** on the
  credential.

## Known limitations

- This integration has not yet been run against a live Configuration Manager
  site database. View and column names were taken from Microsoft's SQL view
  reference and the queries the native integration already runs, and the script
  has been executed end to end against a protocol-level fixture server (see
  below), but a column that a particular site version names differently will
  surface as a SQL error on first run. Treat the first run as a test run.
- The encrypted TDS path (`encrypt=true`) and NTLM authentication are exercised
  only against a real server, not by the fixture tests.
- Only `dbo` views are consulted; custom hardware inventory extensions are not
  imported.
- `v_Add_Remove_Programs` install dates are the bare `YYYYMMDD` strings
  Windows writes to the registry; other formats yield no install date.
- Attribute values are capped at 1,024 characters by the platform; a device in
  a very large number of collections has its `collections` list truncated.
- The classification filter for `security` relies on the English category names
  `Security Updates` and `Critical Updates` in `v_CICategoryInfo`; the
  per-update `classification` attribute shows whatever name the row carried.
- Built-in collections are excluded by their reserved `SMS` id prefix.

## Development validation

The script loads the `runzero.sql` module and speaks TDS, so the shared HTTP
fixture harness cannot drive it. [tests/tds_server.py](./tests/tds_server.py) is
a small TDS 7.4 fixture server (PRELOGIN, LOGIN7, SQL batches, `sp_executesql`
RPC with typed parameters, result sets with bigint/bit/float/datetime/nvarchar
columns) that serves JSON fixture rows; [tests/run.py](./tests/run.py) runs the
real runZero scanner against it and reuses the sibling
`runzero-custom-integrations` harness for expectations, invariants and the
determinism re-run. Run from this repository's root with a current development
scanner:

```sh
export PYTHONDONTWRITEBYTECODE=1 RUNZERO_SCANNER=/tmp/rumble-scanner
python3 mecm-inventory/tests/run.py           # every scenario
python3 mecm-inventory/tests/run.py happy     # one scenario
```

Set `RUNZERO_CUSTOM_INTEGRATIONS` if the library checkout is not beside this
repository. Compile validation also passes:
`runzero script --filename mecm-inventory/mecm-inventory.star --validate`.

Scenarios: `happy` (full inventory, VM vs physical typing, placeholder and
no-correlator skips, software and update dedupe, datetime conversion), `paged`
(three keyset pages), `max-page` (1,001 devices at page size 1,000 — 1,000 bind
parameters per statement and statements over 8 KB), `collection-filter`,
`arp-software` (YYYYMMDD dates, 32/64-bit duplicates, software cap),
`updates-all-capped` (classification filter off, severity ordering, cap),
`missing-views`, `empty`, `auth-failure`, `sql-error` and
`invalid-collection-id`. The fixture server routes statements by view and
emulates only the devices query's paging and collection semantics plus
`ResourceID` filtering; other `WHERE` predicates are not evaluated and fixtures
hold the rows a real server would return.

## Documentation

- [Hardware inventory views](https://learn.microsoft.com/en-us/intune/configmgr/develop/core/understand/sqlviews/hardware-inventory-views-configuration-manager)
- [Discovery views](https://learn.microsoft.com/en-us/intune/configmgr/develop/core/understand/sqlviews/discovery-views-configuration-manager)
- [Collection views](https://learn.microsoft.com/en-us/intune/configmgr/develop/core/understand/sqlviews/collection-views-configuration-manager)
- [Software updates views](https://learn.microsoft.com/en-us/intune/configmgr/develop/core/understand/sqlviews/software-updates-views-configuration-manager)
- [Sample hardware inventory queries](https://learn.microsoft.com/en-us/intune/configmgr/develop/core/understand/sqlviews/sample-queries-hardware-inventory-configuration-manager)
- [runZero custom integrations](https://help.runzero.com/docs/custom-integration-scripts/)

# Custom Integration: Lansweeper On-premises (SQL)

Inbound runZero custom integration for **Lansweeper Classic**, the on-premises
Lansweeper product. It reads the Lansweeper SQL Server database directly and
imports every scanned asset with the inventory Lansweeper holds for it:

- identity and network data: names, FQDN, domain, every MAC and IP Lansweeper has
  seen on the asset, per adapter where Lansweeper has adapters;
- hardware: manufacturer, model, serial number, SKU, chassis, asset tag, BIOS,
  processors, memory modules, logical and physical disks, monitors, battery;
- operating system for Windows (WMI), Linux (SSH) and Mac (SSH) assets;
- organisation: state, IP location, location, building, department, branch
  office, contact, barcode, order number, purchase and warranty dates, comments,
  the 20 custom fields under their console display names, static asset groups
  (as tags), Active Directory computer attributes, the last logged-on user with
  their AD record, asset–user relations, and an owner;
- security posture: antivirus product state and BitLocker protection per drive;
- installed software for Windows, Linux and Mac, as runZero software records;
- a link back to the asset's page in the Lansweeper web console.

The Lansweeper integration in runZero's custom integrations library talks to
the Lansweeper **Sites** cloud API and cannot see a Classic installation that
is not synchronised to the cloud. Lansweeper Classic has no comparable
inventory API: its own reports, and every third-party integration, read the
SQL database, which is what this integration does. All statements are
read-only `SELECT`s against the documented `tbl*`/`tsys*` tables, with values
bound as parameters.

Setting it up is four steps, described under [Steps](#steps): a read-only
login on the SQL Server that holds the Lansweeper database, the custom
integration in runZero, a credential of the type runZero creates for it
(**lansweeper-onprem Secrets**) holding the connection details and import
options, and a scheduled task on an Explorer that can reach the database.

## What is imported

| Data | Lansweeper table(s) | Lands on the asset as |
| --- | --- | --- |
| Asset identity, names, state, type, last scan details, last user, summary CPU/RAM | `tblAssets`, `tblAssetCustom`, `tblState`, `tsysAssetTypes`, `tsysOS`, `tsysIPLocations` | `id`, `hostnames`, `domain`, `manufacturer`, `model`, `firstSeenTS`, `lastSeenTS` and the top-level attributes below |
| MAC addresses (all asset types) | `tblAssetMacAddress` | network interfaces |
| Windows adapters with their IPs, gateway, DHCP and DNS domain | `tblNetwork` (`IPEnabled = 1`) | network interfaces, `network.*` |
| Linux and Mac interfaces | `tblLinuxNetworkDetection`, `tblMacNetwork` | network interfaces, `network.gateways` |
| SNMP interfaces of network devices | `tblSNMPInfo` | network interfaces |
| Windows computer system, chassis, BIOS, processors, memory modules, fixed disks, physical disks, battery | `tblComputersystem`, `tblSystemEnclosure`, `tblBIOS`, `tblProcessor`, `tblPhysicalMemory`, `tblDiskdrives` (`DriveType = 3`), `tblFloppy`, `tblPortableBattery` | `hardware.*`, `bios.*`, `cpu.*`, `memory.*`, `disk.*`, `physicalDisk.*`; drives `deviceType` |
| Windows operating system | `tblOperatingsystem` | `os`, `osVersion`, `os.*` |
| Antivirus, BitLocker, monitors, AD computer record | `tblAntivirus`, `tblEncryptableVolume`, `tblMonitor`, `tblADComputers` | `antivirus.*`, `bitlocker.*`, `monitor.*`, `ad.*` |
| Linux system, BIOS and enclosure | `tblLinuxSystem`, `tblLinuxBios`, `tblLinuxEnclosure` | `os`, `osVersion`, `hardware.*`, `bios.*`, `os.kernel*` |
| Mac hardware overview and OS | `tblMacHwOverview`, `tblMacOSInfo` | `os`, `osVersion`, `hardware.*`, `cpu.*`, `memory.installed`, `os.*` |
| Static asset groups | `tblAssetGroupLink` + `tblAssetGroups` | `assetGroups` and one `asset-group:<name>` tag each |
| User relations and the last user's AD record | `tblAssetUserRelations` + `tsysAssetRelationTypes` + `tblADusers` | `userRelations.<relation>`, `user.*`, `owner` |
| Custom field display names | `tsysCustomNames` | `customField.<display name>` keys |
| Installed software | `tblSoftware`, `tblLinuxSoftware`, `tblMacApplications`, each joined to `tblSoftwareUni` | software records; `software.count`, `software.truncated` |

### Attribute reference

Attributes are namespaced by the name you give the integration, so with the
recommended name they appear in asset search as
`@lansweeper-onprem.device.<attribute>`. Empty values are never written, so an
asset only carries the keys Lansweeper has data for.

| Group | Keys |
| --- | --- |
| Identity | `assetId`, `assetUnique`, `assetType` (Lansweeper asset type name), `state`, `url` (when **Lansweeper web console URL** is set), `description`, `comments` |
| Scanning | `firstSeen`, `lastSeen`, `lastTried`, `lastLsAgent`, `lastLsPush`, `lsAgentVersion`, `scanServer`, `lansweeperVersion`, `scan.httpTitle`, `scan.httpServer`, `scan.httpsServer`, `scan.sshServer`, `scan.snmpOid`, `scan.smtpHeader`, `scan.ftpHeader` |
| Location and ownership fields | `ipLocation` (from the IP location ranges), `location`, `building`, `department`, `branchOffice`, `contact`, `barcode`, `orderNumber`, `purchaseDate`, `warrantyDate`, `lastPatched` |
| Hardware summary | `serialNumber`, `systemSku`, `memoryMb`, `processorCount`, `processor`, `uptimeSeconds`, `firmwareVersion`, `hardwareVersion`, `softwareVersion`, `deviceVersion`, `printedPages`, `printerStatus` |
| Windows | `windows.osCode`, `windows.osName`, `windows.buildNumber`, `windows.displayVersion` (e.g. `23H2`), `windows.servicePack` |
| `hardware.*` | Windows: `systemType`, `domainRole`, `partOfDomain`, `roles`, `processorCount`, `logicalProcessors`, `chassisType`, `chassisTypeCode`, `assetTag`, `enclosureSerial`, `hasBattery`. Linux: `manufacturer`, `productName`, `version`, `serialNumber`, `uuid`, `systemSku`, `platform`, `machine`, plus the chassis keys. Mac: `machineName`, `machineModel`, `bootRomVersion`, `smcVersion`, `platformUuid`, `serialNumber` |
| `bios.*` | `manufacturer`, `version`, `smbiosVersion`, `releaseDate`, `serialNumber` |
| `cpu.*` | `name`, `manufacturer`, `maxClockMhz`, `addressWidth`, `sockets`, `cores`, `logicalProcessors` (Windows, aggregated over sockets); `name`, `sockets`, `speed` (Mac) |
| `memory.*` | `modules`, `installedMb`, `configuredClockMhz`, `manufacturer`, `partNumber` (Windows); `installed` (Mac) |
| `disk.*` | `<letter>.sizeMb`, `<letter>.freeMb`, `<letter>.fileSystem`, `<letter>.volumeName`, `count`, `totalSizeMb`, `totalFreeMb` |
| `physicalDisk.*` | `<n>.name`, `<n>.model`, `<n>.manufacturer`, `<n>.sizeMb`, `<n>.interfaceType`, `<n>.serialNumber`, `<n>.firmware`, `<n>.status`, `count` |
| `os.*` | Windows: `caption`, `version`, `productType`, `installDate`, `servicePack`, `organization`, `registeredUser`, `systemDrive`. Linux: `name`, `release`, `kernelName`, `kernelRelease`, `kernelVersion`. Mac: `systemVersion`, `kernelVersion`, `bootVolume`, `computerName`, `userName` |
| `network.*` | `gateways`, `dnsDomain`, `dhcpEnabled` |
| `antivirus.*` | `name`, `enabled`, `upToDate`, `productState`, `products` |
| `bitlocker.*` | `<letter>` (`On`, `Off`, `Unknown`), `protectedCount` |
| `monitor.*` | `<n>.model`, `<n>.manufacturer`, `<n>.serialNumber`, `count` |
| `ad.*` | `ou`, `description`, `location`, `company`, `comment`, `enabled` |
| People | `user.account`, `user.upn`, `user.email`, `user.displayName`, `user.department`, `user.title`, `user.office` (the last logged-on user); `userRelations.<relation>` (for example `userRelations.owned_by`, `userRelations.used_by`); `owner` |
| Grouping | `assetGroups`, `customField.<name>`, `software.count`, `software.truncated` |

`owner` is the user Lansweeper relates to the asset as **Owned By**, falling
back to the last logged-on user, written as their email address when the AD
record has one, else their UPN, else `DOMAIN\user`. `CONFIG` declares it as the
ownership attribute, so runZero populates asset ownership from it.

Software records carry the title, version, publisher and install date, plus
`installLocation`, `perUser`, `installer` (`MSI` or `Custom installer`),
`msStore` and `approval` (Lansweeper's authorisation status) for Windows;
`release`, `architecture`, `path` and `sizeBytes` for Linux; `path` for Mac.

### Device type and operating system

`deviceType` is set from the Lansweeper asset type when the name maps cleanly
onto a runZero device type (Switch, Router, Firewall, Printer, Camera, NAS,
Access Point, UPS, PDU, IP Phone, Thin Client, Mobile, Tablet, …). Computers
are typed from their inventory instead: a Windows domain role or OS product type
of server gives `Server`; otherwise the SMBIOS chassis type gives `Desktop`,
`Laptop`, `Server`, `Tablet` or `Mobile`, a portable battery gives `Laptop`, and
a Mac model identifier gives `Laptop` or `Desktop`. Virtual machines (a model
naming VMware, Hyper-V, KVM, …) are not typed from their emulated chassis. An
unmapped type is left to runZero fingerprinting; the Lansweeper name is always
in `assetType`.

`os`/`osVersion` come from `tblOperatingsystem` for Windows (falling back to
Lansweeper's own OS lookup and build number), `tblLinuxSystem` for Linux and
`tblMacOSInfo` for Mac. runZero fingerprinting still takes precedence where it
has its own evidence; `trustOS` is deliberately not set.

## Architecture and data flow

1. A scheduled runZero custom integration task runs this Starlark script on an
   Explorer with network access to the SQL Server instance.
2. The script connects over TDS with the runZero `runzero.sql` module (the
   go-mssqldb driver), TLS on by default, identifying itself to SQL Server as
   `runZero-lansweeper-onprem` (the `app name` connection property).
3. It checks `INFORMATION_SCHEMA.TABLES` for every table it intends to read.
   A missing core table (`tblAssets`, …) fails the run with a message naming
   it, because the database is not a Lansweeper database. Any other table that
   is absent or not readable by the login is logged once and skipped; nothing
   else changes.
4. It walks `tblAssets` in pages (default 100) using `AssetID` as a keyset
   cursor, filtered to the Active state unless **Asset states to import** is
   `all`, and optionally to the named asset types.
5. For each page it issues one parameterised statement per table, filtered to
   that page's `AssetID`s, and assembles one `ImportAsset` per asset, which is
   streamed to the Console as it is built. Nothing larger than one page is ever
   held in memory.
6. runZero merges each record into the existing asset — see
   [Asset identity](#asset-identity).

An asset with no hostname, MAC or IP (a manually created location, rack or
placeholder) is skipped and counted, because runZero would have nothing to
correlate it on. A Lansweeper asset name that is not a hostname (`HP LaserJet
4`, an IP address) is not imported as one.

## Requirements

**runZero**

- Superuser access, to create the custom integration, the credential and the
  scheduled task.
- runZero platform 5.1.260825.0 or newer (the script's `minVersion`).
- An Explorer with a network path to the SQL Server instance (TCP 1433 by
  default). The Lansweeper database is on your internal network, so the task
  must run on an Explorer; a Console-hosted task cannot reach it.

**Lansweeper**

- Lansweeper Classic (on-premises) with its database on **SQL Server** —
  Express or a full edition. Lansweeper's **SQL LocalDB** option cannot accept
  network connections; a LocalDB installation must first be moved to SQL Server
  (Lansweeper documents this migration in its knowledge base).
- Know where the database is. The Lansweeper Config Editor tool
  (`Tools\ConfigEditor.exe` under the Lansweeper installation folder) shows the
  SQL Server instance and database name, normally `lansweeperdb`.
- If the database is on a *named* instance (`SERVER\SQLEXPRESS` is Lansweeper's
  default), the instance must have TCP/IP enabled with a static port in SQL
  Server Configuration Manager, and that port is what goes in **Port**. This
  integration connects to a host and port; it does not resolve instance names
  through the SQL Browser service.

**SQL Server**

- A login that can read the Lansweeper database: a member of the database role
  `db_datareader`. [Step 1](#step-1-create-the-sql-server-login) creates one.
- For a SQL Server login (the default here), the instance must allow **SQL
  Server and Windows Authentication mode** (Mixed Mode). Windows authentication
  with a `DOMAIN\user` login works over NTLM from a Linux Explorer in principle,
  but that path has not been exercised by this integration's tests.
- A server certificate the Explorer trusts, or **Trust the server certificate**
  enabled when the instance still uses the self-signed certificate SQL Server
  generates at install.

## Steps

### Step 1: Create the SQL Server login

Whoever administers the SQL Server opens **SQL Server Management Studio**,
connects to the instance that hosts the Lansweeper database with `sysadmin`
rights, opens a **New Query** window and runs:

```sql
-- 1. An account on the SQL Server instance. Choose a strong password and
--    store it only in the runZero credential.
USE [master];
CREATE LOGIN [svc_runzero_lansweeper] WITH PASSWORD = N'<strong password>', CHECK_POLICY = ON;
GO
-- 2. Let that account into the Lansweeper database (change the name if yours differs).
USE [lansweeperdb];
CREATE USER [svc_runzero_lansweeper] FOR LOGIN [svc_runzero_lansweeper];
-- 3. Read access to every table. It cannot write or change anything.
ALTER ROLE [db_datareader] ADD MEMBER [svc_runzero_lansweeper];
GO
```

For Windows authentication instead, replace the first statement with
`CREATE LOGIN [CORP\svc_runzero_lansweeper] FROM WINDOWS;`, use the same
`CORP\...` name in the other two, and enter it as the username in runZero.

Then check it works, still in SSMS: connect as the new login and run:

```sql
USE [lansweeperdb];
SELECT COUNT(*) AS assets FROM tblAssets;
SELECT TOP 5 AssetName, AssetID FROM tblAssets ORDER BY Lastseen DESC;
```

### Step 2: Add the custom integration to runZero

1. In the runZero console, go to **Account → Custom integrations → Add custom
   integration**.
2. Name it `lansweeper-onprem`. The name is the attribute namespace
   (`@lansweeper-onprem.device.…`), so queries written against these docs
   assume it.
3. Toggle **Enable custom integration script**, paste the contents of
   [lansweeper-onprem.star](./lansweeper-onprem.star) into the editor, click
   **Validate**, then **Save**.

The script carries its own `CONFIG` block, so runZero builds the credential
form from it and adds a credential type named after the integration.

### Step 3: Create the credential

1. Go to **Credentials → Add Credential**.
2. Choose the credential type **lansweeper-onprem Secrets**.
3. Give it a name and fill in the fields:

   | Field | Default | What to enter |
   | --- | --- | --- |
   | SQL Server host, Port | — / `1433` | The SQL Server, and the instance's TCP port. |
   | Username, Password | — | The login from Step 1. The password is encrypted at rest and never logged. |
   | Lansweeper database | `lansweeperdb` | The database name. Unless **Instance name** is set, it also scopes every asset id, so keep it stable. |
   | Instance name | empty | A label that scopes asset ids instead of the database name. Set it when more than one Lansweeper installation is imported into the same organization, or to keep ids stable if the database will be renamed. |
   | Lansweeper web console URL | empty | For example `https://lansweeper.example.com` or `http://lansweeper:81`. Adds a `url` attribute linking each asset to its Lansweeper page. |
   | Require TLS encryption | on | Leave on. |
   | Trust the server certificate | off | Turn on when the instance uses a self-signed certificate; the session stays encrypted, only certificate validation is skipped. |
   | Query timeout (seconds) | `60` | Per statement. |
   | Asset states to import | `active` | `all` also imports Non-active, Stock, Sold and every other state; the state name is always in `state`. |
   | Asset types | empty | Comma-separated Lansweeper asset type names (as shown in the console) to import, for example `Windows, Linux`. Useful for the first test run. |
   | Import installed software | on | Attach the software inventory as runZero software records. |
   | Software records per asset | `1000` | Cap per asset; the true total is always in `software.count`. |
   | Assets per page | `100` | Assets read per round of queries. |

4. To let other organizations use it, select **Make this a global credential**.
5. Save the credential.

### Step 4: Create and run the task

1. Go to **Tasks → Integrate → Custom Scripts** (or use **Save & create task**
   on the integration page).
2. Select the `lansweeper-onprem` integration and the credential from Step 3.
3. Choose an Explorer that can reach the SQL Server, and the site to add new
   assets to. Lansweeper is a discovery source, so assets it knows and runZero
   has not seen are created as new assets; tick **Exclude assets that cannot be
   merged into an existing asset** only if you want enrichment of scanned assets
   alone.
4. Schedule it **daily**. Lansweeper itself rescans assets on a schedule
   measured in hours or days, so more frequent runs add database load without
   adding much data.
5. Activate the task, run it once and open its log. A healthy run ends with a
   line like `lansweeper-onprem: lansweeperdb: 1234 assets read, 1230 reported,
   4 without a hostname, MAC or IP skipped, 187654 software records attached`.

### Validation on a small test set

1. For the first run set **Asset types** on the credential to one type you
   know well (for example `Windows`), then clear it once satisfied.
2. Search **Assets** for `custom_integration:lansweeper-onprem` — every asset
   the task touched.
3. Open two or three of them. Under **Attributes**, the `lansweeper-onprem`
   section should show `assetType`, `state`, `hardware.chassisType`,
   `customField.*` and the rest; the **Software** tab lists the inventoried
   titles attributed to this integration; asset groups appear as tags.

## Asset identity

- Target entity: a Lansweeper asset — one scanned or manually created device
  (computer, network device, printer, VM, …).
- Source ID field: `tblAssets.AssetID`
- Documentation evidence: Lansweeper's database documentation (bundled in the
  *Lansweeper Classic Report Builder (SQL)* skill package, `schema-full.md`)
  defines `tblAssets` as "the most basic asset data … Most other tables link to
  tblAssets" with `AssetID` as its primary key, and every inventory table
  carries an `AssetID` foreign key to it. `AssetUnique` is Lansweeper's own
  deduplication key (domain\name for Windows, else MAC, else IP, else random),
  which is why it is recorded but not used as the identity.
- Uniqueness scope: one Lansweeper database (one installation). Two
  installations number their assets independently.
- Cardinality: one `tblAssets` row per asset. Child tables (adapters, disks,
  software, groups, relations) are attached to that asset, never emitted as
  assets.
- Stability: `AssetID` is the primary key and stays with a record for its
  lifetime. Lansweeper creates a *new* asset when a Windows computer is renamed
  and when a non-Windows asset is re-detected under a different MAC/IP; the old
  record stays in the database as a stale asset until cleanup deletes it.
- Reuse behavior: the documentation export does not state how `AssetID` is
  generated; in Lansweeper databases it is an auto-numbered SQL Server key,
  whose values are not reused after deletion.
- Presence: `NOT NULL`, present on every row.
- Final runZero ID: `lansweeper-onprem:<scope>:<AssetID>`, where `<scope>` is
  the **Instance name** if set, else the database name, lower-cased.
- Missing-ID behavior: not applicable (`AssetID` is the primary key); a row
  without a hostname, MAC or IP is skipped and counted.
- Match behavior: `no-mac-break no-ip-break no-name-break`. `AssetID` is a
  stable foreign id, so id matching stays on, while the IP Lansweeper last
  scanned and the MACs it has collected churn with DHCP and NIC changes and must
  not refuse a merge with the asset runZero already has.
- Verdict: **scoped authoritative**.

## Known limits and assumptions

- **Schema version.** The statements use the column set documented in
  Lansweeper's current database documentation. A much older Lansweeper release
  that lacks a selected column fails the run with SQL Server's `Invalid column
  name` message naming it; remove that column from the statement in question.
- **Asset type names** are compared case-insensitively against the English
  defaults when deriving `deviceType`. Renamed or localised types are not
  mapped (they are still imported, with the name in `assetType`), and the
  **Asset types** filter must use the names exactly as your console shows them.
- **Timestamps** in the Lansweeper database are SQL `datetime` values in the
  Lansweeper server's local time; they are read as UTC.
- **Dynamic asset groups** are evaluated by Lansweeper at query time and are
  not materialised in `tblAssetGroupLink`, so only static group membership is
  imported.
- **Not imported:** installed Windows hotfixes (`tblQuickFixEngineering`) —
  they are installed updates, not findings; Linux/Mac per-component hardware
  tables; scan errors; warranty service details beyond the summary dates.
- `tsysCustomNames` (custom field display names) and `tsysAssetRelationTypes`
  are system tables without column documentation; their column names were
  verified against a live-database schema export bundled in the same skill
  package.
- `tblSoftware.MSI` is documented inverted (0 = MSI package, 1 = custom
  installer) and is mapped as documented into `installer`.
- `instance_name` and the database name are the only things that scope asset
  ids. Changing either later re-identifies every asset; set **Instance name**
  from the start if a rename or migration is plausible.

## Development validation

The script loads the `runzero.sql` module and speaks TDS, so the shared HTTP
fixture harness cannot drive it. [tests/tds_server.py](./tests/tds_server.py)
is a small TDS 7.4 fixture server (PRELOGIN, LOGIN7, SQL batches,
`sp_executesql` RPC with typed parameters, result sets with bigint, numeric,
bit, datetime and nvarchar columns) that serves JSON fixture rows and applies
the asset query's paging, state and type semantics. [tests/run.py](./tests/run.py)
starts it per scenario, runs the real `runzero script` command against it and
checks the exported assets with the sibling runzero-custom-integrations harness
(expectations, invariants, determinism re-run).

```sh
cd platform/product/rumble-scanner && go build -tags recogNocloud,development -o /tmp/rumble-scanner .
RUNZERO_SCANNER=/tmp/rumble-scanner python3 lansweeper-onprem/tests/run.py           # all scenarios
RUNZERO_SCANNER=/tmp/rumble-scanner python3 lansweeper-onprem/tests/run.py happy     # one scenario
```

Scenarios: `happy` (every asset family, duplicates, skipped and excluded
records, numeric and datetime typing), `all-states`, `asset-type-filter`,
`instance-name`, `no-software`, `software-capped`, `missing-tables` (degraded
run), `paged` (keyset walk), `empty`, `login-failure`, `wrong-database` and
`sql-error` (failures end the task). Variants `extends` the `happy` fixture and
override only what they test.

## Documentation sources

- Lansweeper, *Lansweeper Classic Report Builder (SQL)* skill package v1.0
  (`SKILL.md`, `references/schema-full.md`, `references/system-tables-and-views.md`,
  `references/examples.md`) — the Classic database documentation export and
  Lansweeper's report conventions (`tblAssetCustom.State = 1` is Active; join
  `tsysIPLocations` on `LocationID`; software lives in `tblSoftware`,
  `tblLinuxSoftware` and `tblMacApplications` joined to `tblSoftwareUni`).
- Microsoft, *Win32_SystemEnclosure* / DMTF SMBIOS Reference Specification
  (DSP0134) section 7.4.1 — chassis type codes.
- Microsoft, *Win32_EncryptableVolume.ProtectionStatus* — BitLocker status codes.

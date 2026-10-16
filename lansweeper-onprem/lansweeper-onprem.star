# This is a runZero Custom Integration, please see https://github.com/runZeroInc/runzero-custom-integrations for details.

CONFIG = {
    "id": "runzero-lansweeper-onprem",
    "name": "Lansweeper On-premises (SQL)",
    "type": "inbound",
    "description": "Imports assets, hardware, operating system, location, ownership, custom fields and installed software from a Lansweeper Classic (on-premises) installation by reading its SQL Server database directly.",
    "version": "1",
    "maturity": "alpha",
    # ImportAsset accepts lastSeenTS as a constructor keyword from this release.
    "minVersion": "5.1.260825.0",
    # tblAssets.AssetID is Lansweeper's stable key for an asset, so id matching
    # stays on. The IP Lansweeper last scanned and the MACs it has collected
    # churn with DHCP and NIC changes and must not refuse a merge with the asset
    # runZero already has.
    "matchBehavior": "no-mac-break no-ip-break no-name-break",
    # Attributes render as @<integration-name>.device.<attr> in asset search.
    "assetType": "device",
    "ownershipAttributes": ["owner"],
    # Direct TDS connection; the default HTTP wiring validation does not apply.
    "validationMode": "compile",
    # At the smallest page size this is 1,000,000 assets; the pager raises
    # rather than truncating if an installation ever exceeds it.
    "maxPages": 100000,
    "params": [
        {
            "key": "host",
            "label": "SQL Server host",
            "type": "string",
            "required": True,
            "description": "Hostname or IP of the SQL Server instance that holds the Lansweeper database.",
        },
        {
            "key": "port",
            "label": "Port",
            "type": "int",
            "required": False,
            "default": 1433,
            "min": 1,
            "max": 65535,
        },
        {
            "key": "username",
            "label": "Username",
            "type": "string",
            "required": True,
            "description": "SQL login with read access to the Lansweeper database, or DOMAIN\\user for Windows authentication over NTLM.",
        },
        {
            "key": "password",
            "label": "Password",
            "type": "secret",
            "required": True,
            "description": "Any characters are safe: the value is percent-encoded into the connection string.",
        },
        {
            "key": "database",
            "label": "Lansweeper database",
            "type": "string",
            "required": True,
            "default": "lansweeperdb",
            "description": "Name of the Lansweeper database. Unless Instance name is set, this name also scopes every asset id, so keep it stable.",
        },
        {
            "key": "instance_name",
            "label": "Instance name",
            "type": "string",
            "required": False,
            "description": "Label that scopes asset ids instead of the database name. Set it when more than one Lansweeper installation is imported into the same organization, or to keep ids stable across a database rename.",
        },
        {
            "key": "console_url",
            "label": "Lansweeper web console URL",
            "type": "url",
            "required": False,
            "placeholder": "https://lansweeper.example.com",
            "description": "Base URL of the Lansweeper web console. When set, every asset carries a link to its Lansweeper asset page.",
        },
        {
            "key": "encrypt",
            "label": "Require TLS encryption",
            "type": "bool",
            "required": False,
            "default": True,
        },
        {
            "key": "trust_server_certificate",
            "label": "Trust the server certificate",
            "type": "bool",
            "required": False,
            "default": False,
            "description": "Enable when the instance presents a self-signed certificate. Leave off when the certificate chains to a trusted CA.",
        },
        {
            "key": "timeout",
            "label": "Query timeout (seconds)",
            "type": "int",
            "required": False,
            "default": 60,
            "min": 5,
            "max": 600,
        },
        {
            "key": "asset_states",
            "label": "Asset states to import",
            "type": "enum",
            "required": False,
            "default": "active",
            "options": ["active", "all"],
            "description": "active imports only assets in the Active state; all imports every state (Non-active, Stock, Sold, ...). The state name is always recorded as an attribute.",
        },
        {
            "key": "asset_types",
            "label": "Asset types",
            "type": "string",
            "required": False,
            "placeholder": "Windows, Linux, Switch",
            "description": "Comma-separated Lansweeper asset type names to import, as shown in the Lansweeper console. Leave empty for every type.",
        },
        {
            "key": "import_software",
            "label": "Import installed software",
            "type": "bool",
            "required": False,
            "default": True,
            "description": "Attach the Windows, Linux and Mac software inventory as runZero software records.",
        },
        {
            "key": "max_software_per_asset",
            "label": "Software records per asset",
            "type": "int",
            "required": False,
            "default": 1000,
            "min": 1,
            "max": 5000,
            "description": "Cap on software records attached to one asset. The total count is always recorded as an attribute.",
        },
        {
            "key": "page_size",
            "label": "Assets per page",
            "type": "int",
            "required": False,
            "default": 100,
            "min": 10,
            "max": 1000,
            "description": "Assets read per round of queries. Lower it if a software query for one page exceeds the 1,000,000 row limit.",
        },
    ],
}

load("runzero.types", "ImportAsset", "Software", "to_custom_attributes")
load("runzero.sql", sql_connect="connect")
load("kwargs", "require", "get_string", "get_int", "get_bool", "get_list")
load("net", "network_interface", "mac_key", "routable_ips", "clean_hostnames")
load("coerce", "as_text", "as_int", "as_bool", "dedupe")
load("time", "parse_ts")

# runzero.sql silently stops reading at this many rows; a page that reaches it
# would lose rows for some assets without any error, so it is a failure.
_SQL_ROW_LIMIT = 1000000

_TABLES_SQL = """
SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME IN ({params})
"""

# Without these there is no asset list, so a database that lacks them is not a
# Lansweeper database and the run fails rather than importing nothing.
_CORE_TABLES = ["tblAssets", "tblAssetCustom", "tblState", "tsysAssetTypes", "tsysOS", "tsysIPLocations"]

# Keyset pagination on the clustered key. tsysIPLocations is joined on
# LocationID, as Lansweeper's own report library does, not on the IP range.
_ASSETS_SQL = """
SELECT TOP (@p2) tblAssets.AssetID, tblAssets.AssetUnique, tblAssets.AssetName, tblAssets.Domain, tblAssets.FQDN,
       tblAssets.IPAddress, tblAssets.Mac, tblAssets.Username, tblAssets.Userdomain, tblAssets.Description,
       tblAssets.Firstseen, tblAssets.Lastseen, tblAssets.Lasttried, tblAssets.Scanserver, tblAssets.ServiceVersion,
       tblAssets.LsAgentVersion, tblAssets.LastLsAgent, tblAssets.LastLsPush, tblAssets.Uptime, tblAssets.Memory,
       tblAssets.NrProcessors, tblAssets.Processor, tblAssets.OScode, tblAssets.SP, tblAssets.BuildNumber,
       tblAssets.Version, tblAssetCustom.State, tblState.Statename, tsysAssetTypes.AssetTypename, tsysOS.OSname,
       tsysIPLocations.IPLocation, tblAssetCustom.Manufacturer, tblAssetCustom.Model, tblAssetCustom.Serialnumber,
       tblAssetCustom.SystemSKU, tblAssetCustom.Location, tblAssetCustom.Building, tblAssetCustom.Department,
       tblAssetCustom.Branchoffice, tblAssetCustom.Contact, tblAssetCustom.BarCode, tblAssetCustom.OrderNumber,
       tblAssetCustom.PurchaseDate, tblAssetCustom.Warrantydate, tblAssetCustom.LastPatched, tblAssetCustom.Comments,
       tblAssetCustom.DNSName, tblAssetCustom.HTTPTitle, tblAssetCustom.HttpServer, tblAssetCustom.HttpsServer,
       tblAssetCustom.SSHServer, tblAssetCustom.SnmpOID, tblAssetCustom.SMTPheader, tblAssetCustom.FTPheader,
       tblAssetCustom.FirmwareVersion, tblAssetCustom.HardwareVersion, tblAssetCustom.SoftwareVersion,
       tblAssetCustom.DeviceVersion, tblAssetCustom.Printedpages, tblAssetCustom.Printerstatus,
       tblAssetCustom.Custom1, tblAssetCustom.Custom2, tblAssetCustom.Custom3, tblAssetCustom.Custom4,
       tblAssetCustom.Custom5, tblAssetCustom.Custom6, tblAssetCustom.Custom7, tblAssetCustom.Custom8,
       tblAssetCustom.Custom9, tblAssetCustom.Custom10, tblAssetCustom.Custom11, tblAssetCustom.Custom12,
       tblAssetCustom.Custom13, tblAssetCustom.Custom14, tblAssetCustom.Custom15, tblAssetCustom.Custom16,
       tblAssetCustom.Custom17, tblAssetCustom.Custom18, tblAssetCustom.Custom19, tblAssetCustom.Custom20
FROM tblAssets
INNER JOIN tblAssetCustom ON tblAssetCustom.AssetID = tblAssets.AssetID
INNER JOIN tsysAssetTypes ON tsysAssetTypes.AssetType = tblAssets.Assettype
INNER JOIN tblState ON tblState.State = tblAssetCustom.State
LEFT JOIN tsysOS ON tsysOS.OScode = tblAssets.OScode
LEFT JOIN tsysIPLocations ON tsysIPLocations.LocationID = tblAssets.LocationID
WHERE tblAssets.AssetID > @p1{state}{types}
ORDER BY tblAssets.AssetID
"""
# State 1 is Lansweeper's built-in Active state.
_ACTIVE_CLAUSE = " AND tblAssetCustom.State = 1"
_TYPES_CLAUSE = " AND LOWER(tsysAssetTypes.AssetTypename) IN ({params})"

# Display names of the Custom1..Custom20 asset fields, as set in the console.
_CUSTOM_NAMES_TABLE = "tsysCustomNames"
_CUSTOM_NAMES_SQL = "SELECT Customname, Displayname FROM tsysCustomNames"
_CUSTOM_FIELDS = 20

# One statement per table, each filtered to the assets on the current page.
# Column names are written exactly as Lansweeper declares them so the
# statements also run on a case-sensitive collation.
_CHILD_SQL = {
    # Correlators. tblNetwork is Windows, the Linux/Mac detection tables are
    # SSH-scanned systems, tblSNMPInfo is network gear; tblAssetMacAddress
    # holds every MAC Lansweeper has seen for any asset type.
    "tblAssetMacAddress": "SELECT AssetID, Mac FROM tblAssetMacAddress WHERE AssetID IN ({params})",
    "tblNetwork": "SELECT AssetID, MACaddress, IPAddress, DefaultIPGateway, DHCPenabled, DNSDomain FROM tblNetwork WHERE IPEnabled = 1 AND AssetID IN ({params})",
    "tblLinuxNetworkDetection": "SELECT AssetID, Mac, Ipv4, Ipv6, Gateway FROM tblLinuxNetworkDetection WHERE AssetID IN ({params})",
    "tblMacNetwork": "SELECT AssetID, Mac, Ipv4, IPv6, Gateway FROM tblMacNetwork WHERE AssetID IN ({params})",
    "tblSNMPInfo": "SELECT AssetID, IfMacaddress, IfIPAddress FROM tblSNMPInfo WHERE (IfMacaddress IS NOT NULL OR IfIPAddress IS NOT NULL) AND AssetID IN ({params})",
    # Windows hardware inventory (WMI).
    "tblComputersystem": "SELECT AssetID, Domainrole, PartOfDomain, Roles, SystemType, NumberOfProcessors, NumberOfLogicalProcessors FROM tblComputersystem WHERE AssetID IN ({params})",
    "tblSystemEnclosure": "SELECT AssetID, ChassisTypes, SMBIOSAssetTag, SerialNumber FROM tblSystemEnclosure WHERE AssetID IN ({params})",
    "tblBIOS": "SELECT AssetID, Manufacturer, SMBIOSBIOSVersion, Version, ReleaseDate, SerialNumber FROM tblBIOS WHERE AssetID IN ({params})",
    "tblProcessor": "SELECT AssetID, Name, Manufacturer, MaxClockSpeed, NumberOfCores, NumberOfLogicalProcessors, AddressWidth FROM tblProcessor WHERE AssetID IN ({params})",
    "tblPhysicalMemory": "SELECT AssetID, Capacity, ConfiguredClockSpeed, Manufacturer, PartNumber FROM tblPhysicalMemory WHERE AssetID IN ({params})",
    "tblDiskdrives": "SELECT AssetID, Caption, FileSystem, Size, Freespace, Volumename FROM tblDiskdrives WHERE DriveType = 3 AND AssetID IN ({params})",
    "tblFloppy": "SELECT AssetID, Name, Model, Manufacturer, Size, InterfaceType, SerialNumber, FirmwareRevision, Status FROM tblFloppy WHERE AssetID IN ({params})",
    "tblPortableBattery": "SELECT AssetID FROM tblPortableBattery WHERE AssetID IN ({params})",
    "tblOperatingsystem": "SELECT AssetID, Caption, Version, ProductType, InstallDate, ServicePackMajorVersion, Organization, RegisteredUser, SystemDrive FROM tblOperatingsystem WHERE AssetID IN ({params})",
    "tblAntivirus": "SELECT AssetID, DisplayName, ProductState, onAccessScanningEnabled, productUpToDate FROM tblAntivirus WHERE AssetID IN ({params})",
    "tblEncryptableVolume": "SELECT AssetId AS AssetID, DriveLetter, ProtectionStatus FROM tblEncryptableVolume WHERE AssetId IN ({params})",
    "tblMonitor": "SELECT AssetID, MonitorModel, MonitorManufacturer, SerialNumber FROM tblMonitor WHERE AssetID IN ({params})",
    "tblADComputers": "SELECT AssetID, OU, Description, Location, Company, Comment, IsEnabled FROM tblADComputers WHERE AssetID IN ({params})",
    # Linux (SSH) and Mac (SSH) inventory.
    "tblLinuxSystem": "SELECT AssetID, Manufacturer, ProductName, Version, Serial, UUID, SystemSku, NetworkNodeHostname, KernelName, KernelRelease, KernelVersion, MachineHardwareName, HardwarePlatform, OperatingSystem, OSRelease FROM tblLinuxSystem WHERE AssetID IN ({params})",
    "tblLinuxBios": "SELECT AssetID, Vendor, Version, ReleaseDate FROM tblLinuxBios WHERE AssetID IN ({params})",
    "tblLinuxEnclosure": "SELECT AssetID, ChassisTypes, SMBIOSAssetTag, SerialNumber FROM tblLinuxEnclosure WHERE AssetID IN ({params})",
    "tblMacHwOverview": "SELECT AssetID, MachineName, MachineModel, CPUType, NrOfCPUs, CPUSpeed, Memory, BootROMVersion, SerialNumber, PlatformUUID, SMCVersionSystem FROM tblMacHwOverview WHERE AssetID IN ({params})",
    "tblMacOSInfo": "SELECT AssetID, SystemVersion, KernelVersion, BootVolume, ComputerName, UserName FROM tblMacOSInfo WHERE AssetID IN ({params})",
    # Organisation: static asset groups, user relations and the AD record of
    # the last logged-on user.
    "tblAssetGroupLink": """
SELECT tblAssetGroupLink.AssetID, tblAssetGroups.AssetGroup
FROM tblAssetGroupLink
INNER JOIN tblAssetGroups ON tblAssetGroups.AssetGroupID = tblAssetGroupLink.AssetGroupID
WHERE tblAssetGroupLink.AssetID IN ({params})""",
    "tblAssetUserRelations": """
SELECT tblAssetUserRelations.AssetID, tsysAssetRelationTypes.Name, tblAssetUserRelations.Username,
       tblAssetUserRelations.Userdomain, tblADusers.UPN, tblADusers.email
FROM tblAssetUserRelations
INNER JOIN tsysAssetRelationTypes ON tsysAssetRelationTypes.RelationTypeID = tblAssetUserRelations.Type
LEFT JOIN tblADusers ON tblADusers.Username = tblAssetUserRelations.Username AND tblADusers.Userdomain = tblAssetUserRelations.Userdomain
WHERE (tblAssetUserRelations.EndDate IS NULL OR tblAssetUserRelations.EndDate > GETDATE()) AND tblAssetUserRelations.AssetID IN ({params})""",
    "tblADusers": """
SELECT tblAssets.AssetID, tblADusers.Displayname, tblADusers.UPN, tblADusers.email, tblADusers.Department, tblADusers.Title, tblADusers.Office
FROM tblADusers
INNER JOIN tblAssets ON tblAssets.Username = tblADusers.Username AND tblAssets.Userdomain = tblADusers.Userdomain
WHERE tblAssets.AssetID IN ({params})""",
}

_SOFTWARE_SQL = {
    "tblSoftware": """
SELECT tblSoftware.AssetID, tblSoftware.softID, tblSoftware.softwareVersion, tblSoftware.Installdate, tblSoftware.CurrentUser,
       tblSoftware.MSI, tblSoftware.MsStore, tblSoftware.InstallLocation, tblSoftwareUni.softwareName, tblSoftwareUni.SoftwarePublisher, tblSoftwareUni.Approved
FROM tblSoftware
INNER JOIN tblSoftwareUni ON tblSoftwareUni.SoftID = tblSoftware.softID
WHERE tblSoftware.AssetID IN ({params})""",
    "tblLinuxSoftware": """
SELECT tblLinuxSoftware.AssetID, tblLinuxSoftware.SoftwareUniID, tblLinuxSoftware.Version, tblLinuxSoftware.Release, tblLinuxSoftware.Architecture,
       tblLinuxSoftware.InstallDateUtc, tblLinuxSoftware.Path, tblLinuxSoftware.Size, tblSoftwareUni.softwareName, tblSoftwareUni.SoftwarePublisher
FROM tblLinuxSoftware
INNER JOIN tblSoftwareUni ON tblSoftwareUni.SoftID = tblLinuxSoftware.SoftwareUniID
WHERE tblLinuxSoftware.AssetID IN ({params})""",
    "tblMacApplications": """
SELECT tblMacApplications.AssetID, tblMacApplications.softid, tblMacApplications.Version, tblMacApplications.InstallDate, tblMacApplications.Path,
       tblSoftwareUni.softwareName, tblSoftwareUni.SoftwarePublisher
FROM tblMacApplications
INNER JOIN tblSoftwareUni ON tblSoftwareUni.SoftID = tblMacApplications.softid
WHERE tblMacApplications.AssetID IN ({params})""",
}

# Join partners a statement also needs to exist before it can run.
_CHILD_REQUIRES = {
    "tblAssetGroupLink": ["tblAssetGroups"],
    "tblAssetUserRelations": ["tsysAssetRelationTypes", "tblADusers"],
    "tblSoftware": ["tblSoftwareUni"],
    "tblLinuxSoftware": ["tblSoftwareUni"],
    "tblMacApplications": ["tblSoftwareUni"],
}

# Lansweeper asset type names that map directly onto a runZero device type.
# Computers (Windows, Linux, Mac) are typed from their chassis instead, and an
# unmapped type is left to fingerprinting; the name is always an attribute.
_ASSET_TYPE_DEVICE_TYPES = {
    "switch": "Switch", "router": "Router", "firewall": "Firewall", "printer": "Printer",
    "camera": "IP Camera", "ip camera": "IP Camera", "nas": "NAS", "storage": "Storage",
    "access point": "WAP", "wireless access point": "WAP", "ups": "UPS", "pdu": "PDU",
    "ip phone": "IP Phone", "voip": "VoIP", "thin client": "Thin Client", "kvm": "KVM",
    "projector": "Projector", "smart tv": "Smart TV", "scanner": "Scanner",
    "media player": "Media Player", "game console": "Game Console", "sensor": "Sensor",
    "mobile": "Mobile", "smartphone": "Mobile", "smart phone": "Mobile", "tablet": "Tablet",
    "server": "Server", "laptop": "Laptop", "desktop": "Desktop", "workstation": "Desktop",
    "vmware": "Hypervisor", "esx": "Hypervisor", "esxi": "Hypervisor", "hyper-v": "Hypervisor", "hypervisor": "Hypervisor",
}

# SMBIOS System Enclosure chassis types (DSP0134 section 7.4.1).
_CHASSIS_TYPES = {
    1: "Other", 2: "Unknown", 3: "Desktop", 4: "Low Profile Desktop", 5: "Pizza Box", 6: "Mini Tower",
    7: "Tower", 8: "Portable", 9: "Laptop", 10: "Notebook", 11: "Hand Held", 12: "Docking Station",
    13: "All in One", 14: "Sub Notebook", 15: "Space-Saving", 16: "Lunch Box", 17: "Main System Chassis",
    18: "Expansion Chassis", 19: "SubChassis", 20: "Bus Expansion Chassis", 21: "Peripheral Chassis",
    22: "Storage Chassis", 23: "Rack Mount Chassis", 24: "Sealed-Case PC", 25: "Multi-system Chassis",
    26: "Compact PCI", 27: "Advanced TCA", 28: "Blade", 29: "Blade Enclosure", 30: "Tablet", 31: "Convertible",
    32: "Detachable", 33: "IoT Gateway", 34: "Embedded PC", 35: "Mini PC", 36: "Stick PC",
}
_CHASSIS_DEVICE_TYPES = {
    3: "Desktop", 4: "Desktop", 5: "Desktop", 6: "Desktop", 7: "Desktop", 13: "Desktop", 15: "Desktop",
    16: "Desktop", 24: "Desktop", 34: "Desktop", 35: "Desktop", 36: "Desktop",
    8: "Laptop", 9: "Laptop", 10: "Laptop", 14: "Laptop", 31: "Laptop", 32: "Laptop",
    17: "Server", 23: "Server", 25: "Server", 28: "Server",
    30: "Tablet", 11: "Mobile",
}
# Apple model identifiers and model names, lower-cased, by form factor.
_MAC_LAPTOP_PREFIXES = ("macbook",)
_MAC_DESKTOP_PREFIXES = ("imac", "mac mini", "macmini", "mac pro", "macpro", "mac studio", "macstudio")
# Hypervisors report the chassis they emulate, so a VM's chassis says nothing.
_VM_MODEL_MARKERS = ("virtual", "vmware", "kvm", "hvm domu", "bochs", "standard pc (")

_DOMAIN_ROLES = {
    0: "Standalone Workstation", 1: "Member Workstation", 2: "Standalone Server",
    3: "Member Server", 4: "Backup Domain Controller", 5: "Primary Domain Controller",
}
_OS_PRODUCT_TYPES = {1: "Workstation", 2: "Domain Controller", 3: "Server"}
_BITLOCKER_STATUS = {0: "Off", 1: "On", 2: "Unknown"}
_SOFTWARE_APPROVAL = {0: "Unrated", 1: "Approved", 2: "Unapproved"}
# tblSoftware.MSI is documented inverted: 0 is an MSI package, 1 a custom installer.
_INSTALLER_TYPES = {False: "MSI", True: "Custom installer"}

# RFC 3986 unreserved characters: the only bytes that pass through _dsn_escape
# unencoded. Everything else is percent-encoded.
_DSN_UNRESERVED = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
_HEX_DIGITS = "0123456789ABCDEF"
_SLUG_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789"


def _dsn_escape(value):
    """Percent-encode one component of the sqlserver:// DSN.

    The DSN is parsed as a URL, so a password holding @ : / ? # % or & would
    otherwise corrupt the parse. Encoding walks UTF-8 bytes, so multi-byte
    characters are encoded correctly too.
    """
    out = []
    for b in bytes(value).elems():
        ch = chr(b)
        if ch in _DSN_UNRESERVED:
            out.append(ch)
        else:
            out.append("%" + _HEX_DIGITS[b // 16] + _HEX_DIGITS[b % 16])
    return "".join(out)


def _dsn(host, port, username, password, database, encrypt, trust_cert):
    dsn = "sqlserver://{}:{}@{}:{}?database={}&encrypt={}&app%20name=runZero-lansweeper-onprem".format(
        _dsn_escape(username), _dsn_escape(password), host, port,
        _dsn_escape(database), "true" if encrypt else "disable",
    )
    if trust_cert:
        dsn += "&trustservercertificate=true"
    return dsn


def _placeholders(count, start=1):
    return ", ".join(["@p{}".format(i) for i in range(start, start + count)])


def _trunc(text, limit):
    """Cut to `limit` characters; the platform counts runes, Starlark slices bytes."""
    chars = list(text.codepoints())
    if len(chars) <= limit:
        return text
    return "".join(chars[:limit])


def _slug(text):
    """Lower-case attribute key from a display name: 'Cost centre' -> 'cost_centre'."""
    out = []
    for ch in text.lower().codepoints():
        if ch in _SLUG_CHARS:
            out.append(ch)
        elif out and out[-1] != "_":
            out.append("_")
    return "".join(out).rstrip("_")


def _flag(value):
    return None if value == None else as_bool(value)


def _first(rows):
    return rows[0] if rows else {}


def _merge(target, values):
    """Copy the populated values only, so one OS family's blanks never erase another's."""
    for key, value in values.items():
        if value != None and value != "":
            target[key] = value


def _split_addresses(value):
    """Lansweeper stores WMI address arrays as one delimited string."""
    return as_text(value).replace(";", ",").replace(" ", ",").split(",")


def _user_label(username, domain, upn, email):
    """The most useful single identifier for a user: email, then UPN, then DOMAIN\\user."""
    email = as_text(email)
    if email:
        return email
    upn = as_text(upn)
    if upn:
        return upn
    username = as_text(username)
    domain = as_text(domain)
    if username and domain:
        return domain + "\\" + username
    return username


def _present_tables(db, names):
    """The subset of `names` this login can see, lower-cased.

    A query against a missing table raises, which Starlark cannot catch, so
    the check turns a feature a site never scanned (no Macs, no SNMP devices,
    an older schema) into a warning instead of a failed run.
    """
    names = sorted(dedupe(names))
    rows = db.query(_TABLES_SQL.format(params=_placeholders(len(names))), params=names)
    return {as_text(row.get("TABLE_NAME")).lower(): True for row in rows}


def _available_statements(queries, present):
    """Keep only the statements whose table and join partners exist."""
    available = {}
    for table in sorted(queries.keys()):
        needed = [t for t in [table] + _CHILD_REQUIRES.get(table, []) if t.lower() not in present]
        if needed:
            print("lansweeper-onprem: {} is not present or not readable by this login; {} data is skipped".format(", ".join(needed), table))
        else:
            available[table] = queries[table]
    return available


def _custom_field_keys(db, has_names):
    """Attribute key per Custom1..Custom20 column: the console display name, slugified."""
    keys = {}
    if has_names:
        for row in db.query(_CUSTOM_NAMES_SQL):
            keys[as_text(row.get("Customname")).lower()] = _slug(as_text(row.get("Displayname")))
    out = {}
    used = {}
    for i in range(1, _CUSTOM_FIELDS + 1):
        column = "Custom{}".format(i)
        key = keys.get(column.lower()) or column.lower()
        if key in used:
            key = column.lower()
        used[key] = True
        out[column] = key
    return out


def _fetch(db, sql, ids):
    """Run one per-table statement for a page of assets, grouped by AssetID."""
    rows = db.query(sql.format(params=_placeholders(len(ids))), params=ids, limit=_SQL_ROW_LIMIT)
    if len(rows) >= _SQL_ROW_LIMIT:
        fail("lansweeper-onprem: one page returned {} rows from a single table and would be truncated; lower page_size".format(_SQL_ROW_LIMIT))
    grouped = {}
    for row in rows:
        grouped.setdefault(as_int(row.get("AssetID")), []).append(row)
    return grouped


def _interfaces(row, rows):
    """One NetworkInterface per MAC Lansweeper knows, carrying the IPs seen on it.

    tblAssets holds the primary MAC and the last scanned IP; the adapter tables
    pair MACs with their addresses per OS family; tblAssetMacAddress adds the
    remaining MACs. Addresses that belong to no MAC go on one extra interface.
    """
    by_mac = {}
    order = []
    loose = []

    def add(mac, ips):
        key = mac_key(as_text(mac))
        ips = routable_ips([as_text(ip) for ip in ips])
        if key:
            if key not in by_mac:
                by_mac[key] = []
                order.append(key)
            by_mac[key].extend(ips)
        else:
            loose.extend(ips)

    add(row.get("Mac"), [row.get("IPAddress")])
    for r in rows("tblNetwork"):
        add(r.get("MACaddress"), _split_addresses(r.get("IPAddress")))
    for r in rows("tblLinuxNetworkDetection"):
        add(r.get("Mac"), _split_addresses(r.get("Ipv4")) + _split_addresses(r.get("Ipv6")))
    for r in rows("tblMacNetwork"):
        add(r.get("Mac"), _split_addresses(r.get("Ipv4")) + _split_addresses(r.get("IPv6")))
    for r in rows("tblSNMPInfo"):
        add(r.get("IfMacaddress"), [r.get("IfIPAddress")])
    for r in rows("tblAssetMacAddress"):
        add(r.get("Mac"), [])

    attached = {}
    interfaces = []
    for key in order:
        ips = dedupe(by_mac[key])
        for ip in ips:
            attached[ip] = True
        interfaces.append(network_interface(mac=key, ips=ips))
    loose = [ip for ip in dedupe(loose) if ip not in attached]
    if loose:
        interfaces.append(network_interface(ips=loose))
    return [nic for nic in interfaces if nic]


def _network_attrs(rows):
    adapters = rows("tblNetwork")
    gateways = []
    for r in adapters:
        gateways += _split_addresses(r.get("DefaultIPGateway"))
    for table in ("tblLinuxNetworkDetection", "tblMacNetwork"):
        gateways += [as_text(r.get("Gateway")) for r in rows(table)]
    return {
        "gateways": routable_ips(gateways),
        "dnsDomain": ", ".join(dedupe([as_text(r.get("DNSDomain")) for r in adapters])),
        "dhcpEnabled": any([as_bool(r.get("DHCPenabled")) for r in adapters]) if adapters else None,
    }


def _windows_attrs(rows, attrs):
    computer = _first(rows("tblComputersystem"))
    enclosure = _first(rows("tblSystemEnclosure"))
    bios = _first(rows("tblBIOS"))
    os_row = _first(rows("tblOperatingsystem"))
    chassis = as_int(enclosure.get("ChassisTypes"))
    _merge(attrs["hardware"], {
        "systemType": as_text(computer.get("SystemType")),
        "domainRole": _DOMAIN_ROLES.get(as_int(computer.get("Domainrole"), default=-1)),
        "partOfDomain": _flag(computer.get("PartOfDomain")),
        "roles": as_text(computer.get("Roles")),
        "processorCount": computer.get("NumberOfProcessors"),
        "logicalProcessors": computer.get("NumberOfLogicalProcessors"),
        "chassisType": _CHASSIS_TYPES.get(chassis),
        "chassisTypeCode": enclosure.get("ChassisTypes"),
        "assetTag": as_text(enclosure.get("SMBIOSAssetTag")),
        "enclosureSerial": as_text(enclosure.get("SerialNumber")),
        "hasBattery": True if rows("tblPortableBattery") else None,
    })
    _merge(attrs["bios"], {
        "manufacturer": as_text(bios.get("Manufacturer")),
        "version": as_text(bios.get("Version")),
        "smbiosVersion": as_text(bios.get("SMBIOSBIOSVersion")),
        "releaseDate": as_text(bios.get("ReleaseDate")),
        "serialNumber": as_text(bios.get("SerialNumber")),
    })
    _merge(attrs["cpu"], _cpu_attrs(rows("tblProcessor")))
    _merge(attrs["memory"], _memory_attrs(rows("tblPhysicalMemory")))
    attrs["disk"] = _logical_disk_attrs(rows("tblDiskdrives"))
    attrs["physicalDisk"] = _physical_disk_attrs(rows("tblFloppy"))
    _merge(attrs["os"], {
        "caption": as_text(os_row.get("Caption")),
        "version": as_text(os_row.get("Version")),
        "productType": _OS_PRODUCT_TYPES.get(as_int(os_row.get("ProductType"))),
        "installDate": as_text(os_row.get("InstallDate")),
        "servicePack": os_row.get("ServicePackMajorVersion"),
        "organization": as_text(os_row.get("Organization")),
        "registeredUser": as_text(os_row.get("RegisteredUser")),
        "systemDrive": as_text(os_row.get("SystemDrive")),
    })
    attrs["antivirus"] = _antivirus_attrs(rows("tblAntivirus"))
    attrs["bitlocker"] = _bitlocker_attrs(rows("tblEncryptableVolume"))
    attrs["monitor"] = _monitor_attrs(rows("tblMonitor"))
    ad = _first(rows("tblADComputers"))
    attrs["ad"] = {
        "ou": as_text(ad.get("OU")),
        "description": as_text(ad.get("Description")),
        "location": as_text(ad.get("Location")),
        "company": as_text(ad.get("Company")),
        "comment": as_text(ad.get("Comment")),
        "enabled": _flag(ad.get("IsEnabled")),
    }
    return computer, enclosure, os_row


def _cpu_attrs(rows):
    if not rows:
        return {}
    cores = 0
    logical = 0
    names = []
    for row in rows:
        cores += as_int(row.get("NumberOfCores"))
        logical += as_int(row.get("NumberOfLogicalProcessors"))
        names.append(as_text(row.get("Name")))
    first = rows[0]
    return {
        "name": ", ".join(sorted(dedupe(names))),
        "manufacturer": as_text(first.get("Manufacturer")),
        "maxClockMhz": first.get("MaxClockSpeed"),
        "addressWidth": first.get("AddressWidth"),
        "sockets": len(rows),
        "cores": cores,
        "logicalProcessors": logical,
    }


def _memory_attrs(rows):
    if not rows:
        return {}
    total = 0
    for row in rows:
        total += as_int(row.get("Capacity"))
    first = rows[0]
    return {
        "modules": len(rows),
        "installedMb": total // (1024 * 1024),
        "configuredClockMhz": first.get("ConfiguredClockSpeed"),
        "manufacturer": as_text(first.get("Manufacturer")),
        "partNumber": as_text(first.get("PartNumber")),
    }


def _logical_disk_attrs(rows):
    out = {}
    total_size = 0
    total_free = 0
    for row in sorted(rows, key=lambda r: as_text(r.get("Caption"))):
        letter = as_text(row.get("Caption")).rstrip(":")
        if not letter:
            continue
        size = as_int(row.get("Size")) // (1024 * 1024)
        free = as_int(row.get("Freespace")) // (1024 * 1024)
        out[letter] = {
            "sizeMb": size,
            "freeMb": free,
            "fileSystem": as_text(row.get("FileSystem")),
            "volumeName": as_text(row.get("Volumename")),
        }
        total_size += size
        total_free += free
    if out:
        out["count"] = len(out)
        out["totalSizeMb"] = total_size
        out["totalFreeMb"] = total_free
    return out


def _physical_disk_attrs(rows):
    out = {}
    for index, row in enumerate(sorted(rows, key=lambda r: as_text(r.get("Name")))):
        out[str(index)] = {
            "name": as_text(row.get("Name")),
            "model": as_text(row.get("Model")),
            "manufacturer": as_text(row.get("Manufacturer")),
            "sizeMb": as_int(row.get("Size")) // (1024 * 1024),
            "interfaceType": as_text(row.get("InterfaceType")),
            "serialNumber": as_text(row.get("SerialNumber")),
            "firmware": as_text(row.get("FirmwareRevision")),
            "status": as_text(row.get("Status")),
        }
    if out:
        out["count"] = len(out)
    return out


def _antivirus_attrs(rows):
    if not rows:
        return {}
    first = rows[0]
    return {
        "name": as_text(first.get("DisplayName")),
        "enabled": _flag(first.get("onAccessScanningEnabled")),
        "upToDate": _flag(first.get("productUpToDate")),
        "productState": first.get("ProductState"),
        "products": ", ".join(sorted(dedupe([as_text(r.get("DisplayName")) for r in rows]))),
    }


def _bitlocker_attrs(rows):
    out = {}
    protected = 0
    for row in rows:
        letter = as_text(row.get("DriveLetter")).rstrip(":")
        status = as_int(row.get("ProtectionStatus"), default=2)
        if letter:
            out[letter] = _BITLOCKER_STATUS.get(status, "Unknown")
        if status == 1:
            protected += 1
    if out:
        out["protectedCount"] = protected
    return out


def _monitor_attrs(rows):
    out = {}
    for index, row in enumerate(sorted(rows, key=lambda r: as_text(r.get("SerialNumber")))):
        out[str(index)] = {
            "model": as_text(row.get("MonitorModel")),
            "manufacturer": as_text(row.get("MonitorManufacturer")),
            "serialNumber": as_text(row.get("SerialNumber")),
        }
    if out:
        out["count"] = len(out)
    return out


def _linux_attrs(rows, attrs):
    system = _first(rows("tblLinuxSystem"))
    bios = _first(rows("tblLinuxBios"))
    enclosure = _first(rows("tblLinuxEnclosure"))
    chassis = as_int(enclosure.get("ChassisTypes"))
    _merge(attrs["hardware"], {
        "manufacturer": as_text(system.get("Manufacturer")),
        "productName": as_text(system.get("ProductName")),
        "version": as_text(system.get("Version")),
        "serialNumber": as_text(system.get("Serial")),
        "uuid": as_text(system.get("UUID")),
        "systemSku": as_text(system.get("SystemSku")),
        "platform": as_text(system.get("HardwarePlatform")),
        "machine": as_text(system.get("MachineHardwareName")),
        "chassisType": _CHASSIS_TYPES.get(chassis),
        "chassisTypeCode": enclosure.get("ChassisTypes"),
        "assetTag": as_text(enclosure.get("SMBIOSAssetTag")),
        "enclosureSerial": as_text(enclosure.get("SerialNumber")),
    })
    _merge(attrs["bios"], {
        "manufacturer": as_text(bios.get("Vendor")),
        "version": as_text(bios.get("Version")),
        "releaseDate": as_text(bios.get("ReleaseDate")),
    })
    _merge(attrs["os"], {
        "name": as_text(system.get("OperatingSystem")),
        "release": as_text(system.get("OSRelease")),
        "kernelName": as_text(system.get("KernelName")),
        "kernelRelease": as_text(system.get("KernelRelease")),
        "kernelVersion": as_text(system.get("KernelVersion")),
    })
    return system, enclosure


def _mac_attrs(rows, attrs):
    hardware = _first(rows("tblMacHwOverview"))
    os_row = _first(rows("tblMacOSInfo"))
    _merge(attrs["hardware"], {
        "machineName": as_text(hardware.get("MachineName")),
        "machineModel": as_text(hardware.get("MachineModel")),
        "bootRomVersion": as_text(hardware.get("BootROMVersion")),
        "smcVersion": as_text(hardware.get("SMCVersionSystem")),
        "platformUuid": as_text(hardware.get("PlatformUUID")),
        "serialNumber": as_text(hardware.get("SerialNumber")),
    })
    _merge(attrs["cpu"], {
        "name": as_text(hardware.get("CPUType")),
        "sockets": hardware.get("NrOfCPUs"),
        "speed": as_text(hardware.get("CPUSpeed")),
    })
    _merge(attrs["memory"], {"installed": as_text(hardware.get("Memory"))})
    _merge(attrs["os"], {
        "systemVersion": as_text(os_row.get("SystemVersion")),
        "kernelVersion": as_text(os_row.get("KernelVersion")),
        "bootVolume": as_text(os_row.get("BootVolume")),
        "computerName": as_text(os_row.get("ComputerName")),
        "userName": as_text(os_row.get("UserName")),
    })
    return hardware, os_row


def _split_mac_version(system_version):
    """'macOS 14.4.1 (23E224)' -> ('macOS', '14.4.1 (23E224)')."""
    text = as_text(system_version)
    for index, ch in enumerate(text.elems()):
        if ch.isdigit():
            return text[:index].strip(), text[index:].strip()
    return text, ""


def _os_fields(row, os_row, linux, mac_os):
    """(os, osVersion) from whichever inventory the asset has; Lansweeper's own OS lookup is the fallback."""
    if os_row:
        return (as_text(os_row.get("Caption")) or as_text(row.get("OSname")),
                as_text(os_row.get("Version")) or as_text(row.get("BuildNumber")))
    if linux:
        return as_text(linux.get("OperatingSystem")), as_text(linux.get("OSRelease"))
    if mac_os:
        return _split_mac_version(mac_os.get("SystemVersion"))
    return as_text(row.get("OSname")), as_text(row.get("BuildNumber"))


def _device_type(row, computer, os_row, enclosure, linux_enclosure, mac_hardware, has_battery):
    """Network gear and peripherals from the Lansweeper asset type; computers from their chassis."""
    mapped = _ASSET_TYPE_DEVICE_TYPES.get(as_text(row.get("AssetTypename")).lower())
    if mapped:
        return mapped
    if as_int(computer.get("Domainrole")) >= 2 or as_int(os_row.get("ProductType")) in (2, 3):
        return "Server"
    model = as_text(row.get("Model")).lower()
    if any([marker in model for marker in _VM_MODEL_MARKERS]):
        return None
    chassis = as_int(enclosure.get("ChassisTypes")) or as_int(linux_enclosure.get("ChassisTypes"))
    if chassis in _CHASSIS_DEVICE_TYPES:
        return _CHASSIS_DEVICE_TYPES[chassis]
    if has_battery:
        return "Laptop"
    for name in [as_text(mac_hardware.get("MachineModel")).lower(), as_text(mac_hardware.get("MachineName")).lower()]:
        if name.startswith(_MAC_LAPTOP_PREFIXES):
            return "Laptop"
        if name.startswith(_MAC_DESKTOP_PREFIXES):
            return "Desktop"
    return None


def _user_attrs(row, ad_user):
    return {
        "account": _user_label(row.get("Username"), row.get("Userdomain"), None, None),
        "upn": as_text(ad_user.get("UPN")),
        "email": as_text(ad_user.get("email")),
        "displayName": as_text(ad_user.get("Displayname")),
        "department": as_text(ad_user.get("Department")),
        "title": as_text(ad_user.get("Title")),
        "office": as_text(ad_user.get("Office")),
    }


def _relation_attrs(rows):
    """Users related to the asset, grouped by relation type: {'owned_by': [...], 'used_by': [...]}."""
    out = {}
    for row in sorted(rows, key=lambda r: as_text(r.get("Username"))):
        name = _slug(as_text(row.get("Name")))
        label = _user_label(row.get("Username"), row.get("Userdomain"), row.get("UPN"), row.get("email"))
        if name and label:
            out.setdefault(name, []).append(label)
    return {name: dedupe(labels) for name, labels in out.items()}


def _owner(relations, row, ad_user):
    """The user Lansweeper relates to the asset as its owner, else the last logged-on user."""
    owned_by = relations.get("owned_by")
    if owned_by:
        return owned_by[0]
    return _user_label(row.get("Username"), row.get("Userdomain"), ad_user.get("UPN"), ad_user.get("email"))


def _software_entry(table, row):
    if table == "tblSoftware":
        return {
            "key": as_int(row.get("softID")),
            "product": as_text(row.get("softwareName")),
            "version": as_text(row.get("softwareVersion")),
            "vendor": as_text(row.get("SoftwarePublisher")),
            "installedAt": parse_ts(row.get("Installdate")),
            "attrs": {
                "installLocation": as_text(row.get("InstallLocation")),
                "perUser": _flag(row.get("CurrentUser")),
                "installer": _INSTALLER_TYPES.get(_flag(row.get("MSI"))),
                "msStore": _flag(row.get("MsStore")),
                "approval": _SOFTWARE_APPROVAL.get(as_int(row.get("Approved"))),
            },
        }
    if table == "tblLinuxSoftware":
        return {
            "key": as_int(row.get("SoftwareUniID")),
            "product": as_text(row.get("softwareName")),
            "version": as_text(row.get("Version")),
            "vendor": as_text(row.get("SoftwarePublisher")),
            "installedAt": parse_ts(row.get("InstallDateUtc")),
            "attrs": {
                "release": as_text(row.get("Release")),
                "architecture": as_text(row.get("Architecture")),
                "path": as_text(row.get("Path")),
                "sizeBytes": row.get("Size"),
            },
        }
    return {
        "key": as_int(row.get("softid")),
        "product": as_text(row.get("softwareName")),
        "version": as_text(row.get("Version")),
        "vendor": as_text(row.get("SoftwarePublisher")),
        "installedAt": parse_ts(row.get("InstallDate")),
        "attrs": {"path": as_text(row.get("Path"))},
    }


def _build_software(rows_by_table, asset_id, cap):
    """Dedupe, order and cap one asset's software rows across the three OS tables; returns (software, total)."""
    seen = {}
    entries = []
    for table in sorted(_SOFTWARE_SQL.keys()):
        for row in rows_by_table(table):
            entry = _software_entry(table, row)
            if not entry["product"]:
                continue
            dedupe_key = (entry["product"].lower(), entry["version"], entry["vendor"].lower())
            if dedupe_key in seen:
                continue
            seen[dedupe_key] = True
            entries.append(entry)
    entries = sorted(entries, key=lambda e: (e["product"].lower(), e["version"]))

    software = []
    for entry in entries[:cap]:
        software.append(Software(
            id=_trunc("lansweeper-onprem:{}:software:{}:{}".format(asset_id, entry["key"], entry["version"]), 256),
            product=_trunc(entry["product"], 128),
            version=_trunc(entry["version"], 128) or None,
            vendor=_trunc(entry["vendor"], 128) or None,
            installedAt=entry["installedAt"],
            customAttributes=to_custom_attributes(entry["attrs"]),
        ))
    return software, len(entries)


def build_asset(ctx, row, rows_by_table, stats):
    """Build one ImportAsset from a tblAssets row and its per-table inventory rows."""
    asset_id = as_int(row.get("AssetID"))

    def rows(table):
        return rows_by_table.get(table, [])

    sections = {"hardware": {}, "bios": {}, "cpu": {}, "memory": {}, "os": {}}
    computer, enclosure, os_row = _windows_attrs(rows, sections)
    linux, linux_enclosure = _linux_attrs(rows, sections)
    mac_hardware, mac_os = _mac_attrs(rows, sections)

    hostnames = clean_hostnames([
        as_text(row.get("AssetName")), as_text(row.get("FQDN")), as_text(row.get("DNSName")),
        as_text(linux.get("NetworkNodeHostname")), as_text(mac_os.get("ComputerName")),
    ])
    interfaces = _interfaces(row, rows)
    if not hostnames and not interfaces:
        stats["no_correlator"] += 1
        return None

    ad_user = _first(rows("tblADusers"))
    relations = _relation_attrs(rows("tblAssetUserRelations"))
    groups = sorted(dedupe([as_text(r.get("AssetGroup")) for r in rows("tblAssetGroupLink")]))

    attrs = {
        "assetId": asset_id,
        "assetUnique": as_text(row.get("AssetUnique")),
        "assetType": as_text(row.get("AssetTypename")),
        "state": as_text(row.get("Statename")),
        "url": ctx["console_url"] + "/Asset.aspx?AssetID={}".format(asset_id) if ctx["console_url"] else None,
        "description": as_text(row.get("Description")),
        "comments": as_text(row.get("Comments")),
        "firstSeen": as_text(row.get("Firstseen")),
        "lastSeen": as_text(row.get("Lastseen")),
        "lastTried": as_text(row.get("Lasttried")),
        "lastLsAgent": as_text(row.get("LastLsAgent")),
        "lastLsPush": as_text(row.get("LastLsPush")),
        "lsAgentVersion": as_text(row.get("LsAgentVersion")),
        "scanServer": as_text(row.get("Scanserver")),
        "lansweeperVersion": as_text(row.get("ServiceVersion")),
        "ipLocation": as_text(row.get("IPLocation")),
        "location": as_text(row.get("Location")),
        "building": as_text(row.get("Building")),
        "department": as_text(row.get("Department")),
        "branchOffice": as_text(row.get("Branchoffice")),
        "contact": as_text(row.get("Contact")),
        "barcode": as_text(row.get("BarCode")),
        "orderNumber": as_text(row.get("OrderNumber")),
        "serialNumber": as_text(row.get("Serialnumber")),
        "systemSku": as_text(row.get("SystemSKU")),
        "purchaseDate": as_text(row.get("PurchaseDate")),
        "warrantyDate": as_text(row.get("Warrantydate")),
        "lastPatched": as_text(row.get("LastPatched")),
        "firmwareVersion": as_text(row.get("FirmwareVersion")),
        "hardwareVersion": as_text(row.get("HardwareVersion")),
        "softwareVersion": as_text(row.get("SoftwareVersion")),
        "deviceVersion": as_text(row.get("DeviceVersion")),
        "printedPages": row.get("Printedpages"),
        "printerStatus": as_text(row.get("Printerstatus")),
        "uptimeSeconds": row.get("Uptime"),
        "memoryMb": row.get("Memory"),
        "processorCount": row.get("NrProcessors"),
        "processor": as_text(row.get("Processor")),
        "scan": {
            "httpTitle": as_text(row.get("HTTPTitle")),
            "httpServer": as_text(row.get("HttpServer")),
            "httpsServer": as_text(row.get("HttpsServer")),
            "sshServer": as_text(row.get("SSHServer")),
            "snmpOid": as_text(row.get("SnmpOID")),
            "smtpHeader": as_text(row.get("SMTPheader")),
            "ftpHeader": as_text(row.get("FTPheader")),
        },
        "windows": {
            "osCode": as_text(row.get("OScode")),
            "osName": as_text(row.get("OSname")),
            "buildNumber": as_text(row.get("BuildNumber")),
            "displayVersion": as_text(row.get("Version")),
            "servicePack": row.get("SP"),
        },
        "customField": {ctx["custom_keys"][column]: as_text(row.get(column)) for column in ctx["custom_keys"]},
        "network": _network_attrs(rows),
        "user": _user_attrs(row, ad_user),
        "userRelations": relations,
        "owner": _owner(relations, row, ad_user),
        "assetGroups": groups,
    }
    attrs.update(sections)
    os_name, os_version = _os_fields(row, os_row, linux, mac_os)

    software = []
    if ctx["software"]:
        software, total = _build_software(rows, asset_id, ctx["max_software"])
        attrs["software"] = {"count": total, "truncated": True if total > len(software) else None}
        stats["software"] += len(software)

    return ImportAsset(
        id="lansweeper-onprem:{}:{}".format(ctx["scope"], asset_id),
        hostnames=hostnames,
        domain=as_text(row.get("Domain")),
        networkInterfaces=interfaces,
        os=os_name,
        osVersion=os_version,
        manufacturer=as_text(row.get("Manufacturer")),
        model=as_text(row.get("Model")),
        deviceType=_device_type(row, computer, os_row, enclosure, linux_enclosure, mac_hardware, bool(rows("tblPortableBattery"))),
        firstSeenTS=parse_ts(row.get("Firstseen")),
        lastSeenTS=parse_ts(row.get("Lastseen")),
        tags=["asset-group:" + "-".join(group.split()) for group in groups],
        software=software,
        customAttributes=to_custom_attributes(attrs),
    )


def main(*args, **kwargs):
    require(kwargs, "host", "username", "password", "database")
    host = get_string(kwargs, "host")
    port = get_int(kwargs, "port", default=1433)
    username = get_string(kwargs, "username")
    password = get_string(kwargs, "password")
    database = get_string(kwargs, "database")
    instance_name = get_string(kwargs, "instance_name", default="")
    console_url = get_string(kwargs, "console_url", default="").rstrip("/")
    encrypt = get_bool(kwargs, "encrypt", default=True)
    trust_cert = get_bool(kwargs, "trust_server_certificate", default=False)
    timeout = get_int(kwargs, "timeout", default=60)
    asset_states = get_string(kwargs, "asset_states", default="active")
    asset_types = [name.lower() for name in dedupe(get_list(kwargs, "asset_types", default=[]))]
    import_software = get_bool(kwargs, "import_software", default=True)
    max_software = get_int(kwargs, "max_software_per_asset", default=1000)
    page_size = get_int(kwargs, "page_size", default=100)

    # connect() pings eagerly, so an unreachable host, a rejected login and a
    # failed TLS handshake all raise here and end the task. The driver's error
    # names neither the target nor the TLS settings, so log them first; the
    # password never appears.
    print("lansweeper-onprem: connecting to {}:{} as {} (database={}, encrypt={}, trust_server_certificate={})".format(
        host, port, username, database, encrypt, trust_cert))
    db = sql_connect(driver="mssql", dsn=_dsn(host, port, username, password, database, encrypt, trust_cert), timeout=timeout)

    queries = dict(_CHILD_SQL)
    if import_software:
        queries.update(_SOFTWARE_SQL)
    names = _CORE_TABLES + [_CUSTOM_NAMES_TABLE]
    for table in queries:
        names += [table] + _CHILD_REQUIRES.get(table, [])
    present = _present_tables(db, names)
    missing_core = [t for t in _CORE_TABLES if t.lower() not in present]
    if missing_core:
        fail("lansweeper-onprem: this database has no Lansweeper asset tables (missing {}); check the Lansweeper database name".format(", ".join(missing_core)))
    available = _available_statements(queries, present)

    scope = instance_name.lower() or database.lower()
    ctx = {
        "scope": scope,
        "console_url": console_url,
        "custom_keys": _custom_field_keys(db, _CUSTOM_NAMES_TABLE.lower() in present),
        "software": import_software and any([table in available for table in _SOFTWARE_SQL]),
        "max_software": max_software,
    }
    stats = {"assets": 0, "reported": 0, "no_correlator": 0, "software": 0}
    assets_sql = _ASSETS_SQL.format(
        state=_ACTIVE_CLAUSE if asset_states == "active" else "",
        types=_TYPES_CLAUSE.format(params=_placeholders(len(asset_types), start=3)) if asset_types else "",
    )

    last_id = 0
    p = pager("assets")
    while p.next():
        page = db.query(assets_sql, params=[last_id, page_size] + asset_types)
        if not page:
            break
        ids = [as_int(r.get("AssetID")) for r in page]
        children = {table: _fetch(db, sql, ids) for table, sql in available.items()}
        for row in page:
            asset_id = as_int(row.get("AssetID"))
            stats["assets"] += 1
            asset = build_asset(ctx, row, {table: grouped.get(asset_id, []) for table, grouped in children.items()}, stats)
            stats["reported"] += report_asset(asset)
        last_id = ids[-1]
        if len(page) < page_size:
            break
    db.close()

    if stats["assets"] == 0:
        print("lansweeper-onprem: {} has no assets matching the state and asset type settings".format(scope))
    print("lansweeper-onprem: {}: {} assets read, {} reported, {} without a hostname, MAC or IP skipped, {} software records attached".format(
        scope, stats["assets"], stats["reported"], stats["no_correlator"], stats["software"]))
    return None

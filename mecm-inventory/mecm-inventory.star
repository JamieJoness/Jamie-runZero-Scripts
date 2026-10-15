# This is a runZero Custom Integration, please see https://github.com/runZeroInc/runzero-custom-integrations for details.

CONFIG = {
    "id": "runzero-mecm-inventory",
    "name": "MECM hardware, software and update inventory",
    "type": "inbound",
    "description": "Enriches devices runZero already knows from Microsoft Endpoint Configuration Manager with hardware inventory, installed software, collection membership and missing software updates, read directly from the MECM site database.",
    "version": "1",
    "maturity": "alpha",
    "minVersion": "5.1.260818.0",
    # ResourceID is MECM's stable per-device key (the same MachineID the native
    # integration keys on), so id-based matching stays on for this source's own
    # records across runs. The merge INTO the asset scans and the native MECM
    # integration already created happens on MAC, IP and hostname, and a device
    # that was re-addressed or renamed since must not be refused that merge.
    "matchBehavior": "no-mac-break no-ip-break no-name-break",
    # Attributes render as @<integration-name>.device.<attr> in asset search.
    "assetType": "device",
    # Direct TDS connection; the default HTTP wiring validation does not apply.
    "validationMode": "compile",
    # At the smallest page size this is 1,000,000 devices; the pager raises
    # rather than truncating if a site ever exceeds it.
    "maxPages": 100000,
    "params": [
        {
            "key": "host",
            "label": "SQL Server host",
            "type": "string",
            "required": True,
            "description": "Hostname or IP of the SQL Server instance that holds the MECM site database.",
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
            "description": "SQL login with read access to the site database views, or DOMAIN\\user for Windows authentication over NTLM.",
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
            "label": "Site database",
            "type": "string",
            "required": True,
            "placeholder": "CM_PS1",
            "description": "Name of the MECM site database, normally CM_<site code>.",
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
            "key": "collection_id",
            "label": "Device collection ID",
            "type": "string",
            "required": False,
            "pattern": "^[A-Za-z0-9]{8}$",
            "placeholder": "SMS00001",
            "description": "Only import members of this device collection. Leave empty for every valid device record in the site.",
        },
        {
            "key": "software_source",
            "label": "Installed software source",
            "type": "enum",
            "required": False,
            "default": "installed_software",
            "options": ["installed_software", "add_remove_programs", "none"],
            "description": "installed_software reads the Asset Intelligence class (v_GS_INSTALLED_SOFTWARE); add_remove_programs reads the Programs and Features registry data (v_Add_Remove_Programs); none skips software.",
        },
        {
            "key": "max_software_per_device",
            "label": "Software records per device",
            "type": "int",
            "required": False,
            "default": 1000,
            "min": 1,
            "max": 5000,
            "description": "Cap on software records attached to one asset. The total count is always recorded as an attribute.",
        },
        {
            "key": "missing_updates",
            "label": "Missing updates to import as vulnerabilities",
            "type": "enum",
            "required": False,
            "default": "security",
            "options": ["security", "all", "none"],
            "description": "security imports required updates in the Security Updates and Critical Updates classifications; all imports every required, unexpired update; none skips them.",
        },
        {
            "key": "max_updates_per_device",
            "label": "Missing updates per device",
            "type": "int",
            "required": False,
            "default": 99,
            "min": 1,
            "max": 1000,
            "description": "Cap on missing updates attached to one asset, most severe first. The total count is always recorded as an attribute.",
        },
        {
            "key": "page_size",
            "label": "Devices per page",
            "type": "int",
            "required": False,
            "default": 100,
            "min": 10,
            "max": 1000,
            "description": "Devices read per round of queries. Lower it if a software query for one page exceeds the 1,000,000 row limit.",
        },
    ],
}

load("runzero.types", "ImportAsset", "Software", "Vulnerability", "to_custom_attributes")
load("runzero.sql", sql_connect="connect")
load("kwargs", "require", "get_string", "get_int", "get_bool")
load("net", "network_interface", "mac_key", "routable_ips", "clean_hostnames")
load("coerce", "as_text", "as_int", "as_bool", "dedupe")
load("time", "parse_ts")

# runzero.sql silently stops reading at this many rows; a page that reaches it
# would lose software for some devices without any error, so it is a failure.
_SQL_ROW_LIMIT = 1000000

_SITE_SQL = "SELECT ThisSiteCode FROM v_Identification"

_VIEWS_SQL = """
SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME IN ({params})
"""

# Keyset pagination on the clustered key. Obsolete and decommissioned records
# are the retired duplicates MECM keeps around after a reinstall; they would
# re-import stale inventory onto the live asset.
_DEVICES_SQL = """
SELECT TOP (@p2) ResourceID, Netbios_Name0, Full_Domain_Name0, Client0, Is_Virtual_Machine0, Creation_Date0
FROM v_R_System
WHERE ResourceID > @p1 AND ISNULL(Obsolete0, 0) = 0 AND ISNULL(Decommissioned0, 0) = 0{collection}
ORDER BY ResourceID
"""
_COLLECTION_CLAUSE = " AND ResourceID IN (SELECT ResourceID FROM v_FullCollectionMembership WHERE CollectionID = @p3)"

# One statement per view, each filtered to the devices on the current page.
# Discovery arrays (v_RA_*) are the same correlators the native integration
# imports, so the asset built here lands on the same runZero asset.
_INVENTORY_SQL = {
    "v_RA_System_ResourceNames": "SELECT ResourceID, Resource_Names0 FROM v_RA_System_ResourceNames WHERE ResourceID IN ({params})",
    "v_RA_System_MACAddresses": "SELECT ResourceID, MAC_Addresses0 FROM v_RA_System_MACAddresses WHERE ResourceID IN ({params})",
    "v_RA_System_IPAddresses": "SELECT ResourceID, IP_Addresses0 FROM v_RA_System_IPAddresses WHERE ResourceID IN ({params})",
    "v_RA_System_IPv6Addresses": "SELECT ResourceID, IPv6_Addresses0 FROM v_RA_System_IPv6Addresses WHERE ResourceID IN ({params})",
    "v_GS_COMPUTER_SYSTEM": "SELECT ResourceID, Manufacturer0, Model0, SystemType0, NumberOfProcessors0, DomainRole0 FROM v_GS_COMPUTER_SYSTEM WHERE ResourceID IN ({params})",
    "v_GS_SYSTEM_ENCLOSURE": "SELECT ResourceID, ChassisTypes0, SMBIOSAssetTag0 FROM v_GS_SYSTEM_ENCLOSURE WHERE ResourceID IN ({params})",
    "v_GS_PC_BIOS": "SELECT ResourceID, Manufacturer0, Name0, SMBIOSBIOSVersion0, ReleaseDate0 FROM v_GS_PC_BIOS WHERE ResourceID IN ({params})",
    "v_GS_PROCESSOR": "SELECT ResourceID, Name0, Manufacturer0, MaxClockSpeed0, NumberOfCores0, NumberOfLogicalProcessors0, Is64Bit0 FROM v_GS_PROCESSOR WHERE ResourceID IN ({params})",
    "v_GS_X86_PC_MEMORY": "SELECT ResourceID, TotalPhysicalMemory0 FROM v_GS_X86_PC_MEMORY WHERE ResourceID IN ({params})",
    "v_GS_LOGICAL_DISK": "SELECT ResourceID, DeviceID0, FileSystem0, Size0, FreeSpace0, VolumeName0 FROM v_GS_LOGICAL_DISK WHERE DriveType0 = 3 AND ResourceID IN ({params})",
    "v_GS_DISK": "SELECT ResourceID, Index0, Model0, Size0, InterfaceType0, MediaType0 FROM v_GS_DISK WHERE ResourceID IN ({params})",
    "v_GS_OPERATING_SYSTEM": "SELECT ResourceID, Caption0, Version0, BuildNumber0, OSArchitecture0, ProductType0, InstallDate0, LastBootUpTime0 FROM v_GS_OPERATING_SYSTEM WHERE ResourceID IN ({params})",
    # Built-in collections (SMS00001 All Systems, ...) hold every device and say
    # nothing about it; device collections are CollectionType 2.
    "v_FullCollectionMembership": """
SELECT fcm.ResourceID, c.CollectionID, c.Name
FROM v_FullCollectionMembership AS fcm
INNER JOIN v_Collection AS c ON c.CollectionID = fcm.CollectionID
WHERE c.CollectionType = 2 AND c.CollectionID NOT LIKE 'SMS%' AND fcm.ResourceID IN ({params})
""",
}

_SOFTWARE_SQL = {
    "v_GS_INSTALLED_SOFTWARE": "SELECT ResourceID, SoftwareCode0, ProductName0, ProductVersion0, Publisher0, InstallDate0, InstalledLocation0, InstallSource0, OsComponent0 FROM v_GS_INSTALLED_SOFTWARE WHERE ResourceID IN ({params})",
    "v_Add_Remove_Programs": "SELECT ResourceID, ProdID0, DisplayName0, Version0, Publisher0, InstallDate0 FROM v_Add_Remove_Programs WHERE ResourceID IN ({params})",
}
_SOFTWARE_VIEWS = {"installed_software": "v_GS_INSTALLED_SOFTWARE", "add_remove_programs": "v_Add_Remove_Programs"}

# Status 2 is Required: the client scanned, the update applies and is not
# installed. Expired updates were withdrawn by Microsoft and cannot be installed.
_UPDATES_VIEW = "v_Update_ComplianceStatus"
_UPDATES_SQL = """
SELECT ucs.ResourceID, ui.CI_ID, ui.CI_UniqueID, ui.ArticleID, ui.BulletinID, ui.Title, ui.Description, ui.Severity,
       ui.IsSuperseded, ui.DatePosted, ui.InfoURL, ucs.LastStatusChangeTime, ucs.LastStatusCheckTime, cat.CategoryInstanceName
FROM v_Update_ComplianceStatus AS ucs
INNER JOIN v_UpdateInfo AS ui ON ui.CI_ID = ucs.CI_ID
LEFT JOIN v_CICategoryInfo AS cat ON cat.CI_ID = ui.CI_ID AND cat.CategoryTypeName = 'UpdateClassification'
WHERE ucs.Status = 2 AND ui.IsExpired = 0 AND ucs.ResourceID IN ({params})"""
_SECURITY_CLAUSE = " AND cat.CategoryInstanceName IN ('Security Updates', 'Critical Updates')"

# MSRC severity as v_UpdateInfo.Severity stores it. Non-security updates carry
# 0 and get no invented rank.
_MSRC_SEVERITY = {10: ("Critical", 4), 8: ("Important", 3), 6: ("Moderate", 2), 2: ("Low", 1)}

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
_DOMAIN_ROLES = {
    0: "Standalone Workstation", 1: "Member Workstation", 2: "Standalone Server",
    3: "Member Server", 4: "Backup Domain Controller", 5: "Primary Domain Controller",
}
_OS_PRODUCT_TYPES = {1: "Workstation", 2: "Domain Controller", 3: "Server"}

# RFC 3986 unreserved characters: the only bytes that pass through _dsn_escape
# unencoded. Everything else is percent-encoded.
_DSN_UNRESERVED = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
_HEX_DIGITS = "0123456789ABCDEF"


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
    dsn = "sqlserver://{}:{}@{}:{}?database={}&encrypt={}&app%20name=runZero-mecm-inventory".format(
        _dsn_escape(username), _dsn_escape(password), host, port,
        _dsn_escape(database), "true" if encrypt else "disable",
    )
    if trust_cert:
        dsn += "&trustservercertificate=true"
    return dsn


def _placeholders(count):
    return ", ".join(["@p{}".format(i + 1) for i in range(count)])


def _trunc(text, limit):
    """Cut to `limit` characters; the platform counts runes, Starlark slices bytes."""
    chars = list(text.codepoints())
    if len(chars) <= limit:
        return text
    return "".join(chars[:limit])


def _flag(value):
    return None if value == None else as_bool(value)


def _first(rows):
    return rows[0] if rows else {}


def _available_views(db, queries):
    """Keep only the views this login can see.

    Hardware inventory classes can be disabled per site and a query against a
    missing view raises, which Starlark cannot catch; checking first turns a
    missing enrichment view into a warning instead of a failed run.
    """
    names = sorted(queries.keys())
    rows = db.query(_VIEWS_SQL.format(params=_placeholders(len(names))), params=names)
    present = {as_text(row.get("TABLE_NAME")).lower(): True for row in rows}
    available = {}
    for name in names:
        if name.lower() in present:
            available[name] = queries[name]
        else:
            print("mecm-inventory: view {} is not present or not readable by this login; its data is skipped".format(name))
    return available


def _fetch(db, sql, ids):
    """Run one per-view statement for a page of devices, grouped by ResourceID."""
    rows = db.query(sql.format(params=_placeholders(len(ids))), params=ids, limit=_SQL_ROW_LIMIT)
    if len(rows) >= _SQL_ROW_LIMIT:
        fail("mecm-inventory: one page returned {} rows from a single view and would be truncated; lower page_size".format(_SQL_ROW_LIMIT))
    grouped = {}
    for row in rows:
        grouped.setdefault(as_int(row.get("ResourceID")), []).append(row)
    return grouped


def _hardware_attrs(computer, enclosure, bios):
    chassis = as_int(enclosure.get("ChassisTypes0"))
    return {
        "hardware": {
            "manufacturer": as_text(computer.get("Manufacturer0")),
            "model": as_text(computer.get("Model0")),
            "systemType": as_text(computer.get("SystemType0")),
            "processorCount": computer.get("NumberOfProcessors0"),
            "domainRole": _DOMAIN_ROLES.get(as_int(computer.get("DomainRole0"), default=-1)),
            "chassisType": _CHASSIS_TYPES.get(chassis),
            "chassisTypeCode": enclosure.get("ChassisTypes0"),
            "assetTag": as_text(enclosure.get("SMBIOSAssetTag0")),
        },
        "bios": {
            "manufacturer": as_text(bios.get("Manufacturer0")),
            "name": as_text(bios.get("Name0")),
            "version": as_text(bios.get("SMBIOSBIOSVersion0")),
            "releaseDate": as_text(bios.get("ReleaseDate0")),
        },
    }


def _cpu_attrs(rows):
    if not rows:
        return {}
    cores = 0
    logical = 0
    names = []
    for row in rows:
        cores += as_int(row.get("NumberOfCores0"))
        logical += as_int(row.get("NumberOfLogicalProcessors0"))
        names.append(as_text(row.get("Name0")))
    first = rows[0]
    return {
        "name": ", ".join(sorted(dedupe(names))),
        "manufacturer": as_text(first.get("Manufacturer0")),
        "maxClockMhz": first.get("MaxClockSpeed0"),
        "sockets": len(rows),
        "cores": cores,
        "logicalProcessors": logical,
        "is64Bit": _flag(first.get("Is64Bit0")),
    }


def _memory_attrs(row):
    kb = as_int(row.get("TotalPhysicalMemory0"))
    return {"totalPhysicalMb": kb // 1024} if kb else {}


def _logical_disk_attrs(rows):
    out = {}
    total_size = 0
    total_free = 0
    for row in sorted(rows, key=lambda r: as_text(r.get("DeviceID0"))):
        letter = as_text(row.get("DeviceID0")).rstrip(":")
        if not letter:
            continue
        size = as_int(row.get("Size0"))
        free = as_int(row.get("FreeSpace0"))
        out[letter] = {
            "sizeMb": size,
            "freeMb": free,
            "fileSystem": as_text(row.get("FileSystem0")),
            "volumeName": as_text(row.get("VolumeName0")),
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
    for row in sorted(rows, key=lambda r: as_int(r.get("Index0"))):
        out[str(as_int(row.get("Index0")))] = {
            "model": as_text(row.get("Model0")),
            "sizeMb": row.get("Size0"),
            "interfaceType": as_text(row.get("InterfaceType0")),
            "mediaType": as_text(row.get("MediaType0")),
        }
    if out:
        out["count"] = len(out)
    return out


def _os_attrs(row):
    if not row:
        return {}
    return {
        "caption": as_text(row.get("Caption0")),
        "version": as_text(row.get("Version0")),
        "build": as_text(row.get("BuildNumber0")),
        "architecture": as_text(row.get("OSArchitecture0")),
        "productType": _OS_PRODUCT_TYPES.get(as_int(row.get("ProductType0"))),
        "installDate": as_text(row.get("InstallDate0")),
        "lastBootUpTime": as_text(row.get("LastBootUpTime0")),
    }


def _collection_attrs(rows):
    if not rows:
        return {}
    ordered = sorted(rows, key=lambda r: as_text(r.get("Name")))
    ids = dedupe([as_text(r.get("CollectionID")) for r in ordered])
    return {
        "collections": dedupe([as_text(r.get("Name")) for r in ordered]),
        "collectionIds": ids,
        "collectionCount": len(ids),
    }


def _device_type(device, computer, os_row, enclosure):
    """Server from the OS/domain role; otherwise the chassis of a physical box.

    A VM reports whatever chassis its hypervisor fakes (Hyper-V says Desktop),
    so MECM's own VM flag leaves the type to fingerprinting.
    """
    if as_int(computer.get("DomainRole0")) >= 2 or as_int(os_row.get("ProductType0")) in (2, 3):
        return "Server"
    if as_bool(device.get("Is_Virtual_Machine0")):
        return None
    return _CHASSIS_DEVICE_TYPES.get(as_int(enclosure.get("ChassisTypes0")))


def _arp_install_ts(value):
    """Programs and Features stores InstallDate as a bare YYYYMMDD string.

    parse_ts would read those eight digits as an epoch, so they are reshaped
    first; any other all-digit value is not a date.
    """
    text = as_text(value)
    if text.isdigit():
        if len(text) != 8:
            return None
        text = "{}-{}-{}".format(text[:4], text[4:6], text[6:])
    return parse_ts(text)


def _software_entry(view, row):
    if view == "v_GS_INSTALLED_SOFTWARE":
        return {
            "product": as_text(row.get("ProductName0")),
            "version": as_text(row.get("ProductVersion0")),
            "vendor": as_text(row.get("Publisher0")),
            "key": as_text(row.get("SoftwareCode0")),
            "installedAt": parse_ts(row.get("InstallDate0")),
            "attrs": {
                "softwareCode": as_text(row.get("SoftwareCode0")),
                "installedLocation": as_text(row.get("InstalledLocation0")),
                "installSource": as_text(row.get("InstallSource0")),
                "osComponent": _flag(row.get("OsComponent0")),
            },
        }
    return {
        "product": as_text(row.get("DisplayName0")),
        "version": as_text(row.get("Version0")),
        "vendor": as_text(row.get("Publisher0")),
        "key": as_text(row.get("ProdID0")),
        "installedAt": _arp_install_ts(row.get("InstallDate0")),
        "attrs": {"productId": as_text(row.get("ProdID0"))},
    }


def _build_software(view, rows, resource_id, cap):
    """Dedupe, order and cap one device's software rows; returns (software, total)."""
    seen = {}
    entries = []
    for row in rows:
        entry = _software_entry(view, row)
        if not entry["product"]:
            continue
        key = (entry["product"].lower(), entry["version"], entry["vendor"].lower())
        if key in seen:
            continue
        seen[key] = True
        entries.append(entry)
    entries = sorted(entries, key=lambda e: (e["product"].lower(), e["version"]))

    software = []
    for entry in entries[:cap]:
        code = entry["key"] or "{}:{}".format(entry["product"], entry["version"])
        software.append(Software(
            id=_trunc("mecm:{}:software:{}".format(resource_id, code), 256),
            product=_trunc(entry["product"], 128),
            version=_trunc(entry["version"], 128) or None,
            vendor=_trunc(entry["vendor"], 128) or None,
            installedAt=entry["installedAt"],
            customAttributes=to_custom_attributes(entry["attrs"]),
        ))
    return software, len(entries)


def _build_vulnerabilities(rows, cap):
    """One Vulnerability per required update, most severe first; returns (vulns, total).

    v_CICategoryInfo can repeat a CI once per installed console language, so
    rows are collapsed on CI_ID before anything else.
    """
    by_ci = {}
    for row in rows:
        ci_id = as_int(row.get("CI_ID"))
        if ci_id and ci_id not in by_ci:
            by_ci[ci_id] = row
    ordered = sorted(by_ci.values(), key=lambda r: (-as_int(r.get("Severity")), as_text(r.get("Title"))))

    vulns = []
    for row in ordered[:cap]:
        severity_name, severity_rank = _MSRC_SEVERITY.get(as_int(row.get("Severity")), (None, None))
        article = as_text(row.get("ArticleID"))
        unique_id = as_text(row.get("CI_UniqueID")) or str(as_int(row.get("CI_ID")))
        title = as_text(row.get("Title"))
        vulns.append(Vulnerability(
            id=_trunc("mecm-update:" + unique_id, 256),
            name=_trunc(title, 256) or (("KB" + article) if article else unique_id),
            description=_trunc(as_text(row.get("Description")), 1024) or None,
            solution=("Install the update described in Microsoft KB" + article) if article else None,
            severityRank=severity_rank,
            publishedTS=parse_ts(row.get("DatePosted")),
            firstDetectedTS=parse_ts(row.get("LastStatusChangeTime")),
            lastDetectedTS=parse_ts(row.get("LastStatusCheckTime")),
            customAttributes=to_custom_attributes({
                "ciId": row.get("CI_ID"),
                "articleId": article,
                "bulletinId": as_text(row.get("BulletinID")),
                "classification": as_text(row.get("CategoryInstanceName")),
                "severity": severity_name,
                "isSuperseded": _flag(row.get("IsSuperseded")),
                "infoUrl": as_text(row.get("InfoURL")),
                "status": "Required",
            }),
        ))
    return vulns, len(ordered)


def build_asset(ctx, device, rows_by_view, stats):
    """Build one ImportAsset from a v_R_System row and its per-view inventory rows."""
    resource_id = as_int(device.get("ResourceID"))
    name = as_text(device.get("Netbios_Name0"))

    # MECM's own placeholder records for unknown-computer OSD support; the
    # native integration skips them for the same reason.
    if device.get("Client0") == None and ("Unknown Computer" in name or "Provisioning Device" in name):
        stats["placeholders"] += 1
        return None

    def rows(view):
        return rows_by_view.get(view, [])

    hostnames = clean_hostnames([name] + [as_text(r.get("Resource_Names0")) for r in rows("v_RA_System_ResourceNames")])
    macs = dedupe([as_text(mac_key(as_text(r.get("MAC_Addresses0")))) for r in rows("v_RA_System_MACAddresses")])
    ips = routable_ips([as_text(r.get("IP_Addresses0")) for r in rows("v_RA_System_IPAddresses")] +
                       [as_text(r.get("IPv6_Addresses0")) for r in rows("v_RA_System_IPv6Addresses")])
    # Discovery stores MACs and IPs as independent arrays, so pairing is unknown.
    interfaces = [network_interface(mac=mac) for mac in macs]
    if ips:
        interfaces.append(network_interface(ips=ips))
    interfaces = [nic for nic in interfaces if nic]
    if not hostnames and not interfaces:
        stats["no_correlator"] += 1
        return None

    computer = _first(rows("v_GS_COMPUTER_SYSTEM"))
    enclosure = _first(rows("v_GS_SYSTEM_ENCLOSURE"))
    os_row = _first(rows("v_GS_OPERATING_SYSTEM"))

    attrs = {"resourceId": resource_id, "siteCode": ctx["site_code"]}
    attrs.update(_hardware_attrs(computer, enclosure, _first(rows("v_GS_PC_BIOS"))))
    attrs["cpu"] = _cpu_attrs(rows("v_GS_PROCESSOR"))
    attrs["memory"] = _memory_attrs(_first(rows("v_GS_X86_PC_MEMORY")))
    attrs["disk"] = _logical_disk_attrs(rows("v_GS_LOGICAL_DISK"))
    attrs["physicalDisk"] = _physical_disk_attrs(rows("v_GS_DISK"))
    attrs["os"] = _os_attrs(os_row)
    attrs.update(_collection_attrs(rows("v_FullCollectionMembership")))

    software = []
    if ctx["software_view"]:
        software, total = _build_software(ctx["software_view"], rows(ctx["software_view"]), resource_id, ctx["max_software"])
        attrs["software"] = {"source": ctx["software_view"], "count": total, "truncated": True if total > len(software) else None}
        stats["software"] += len(software)

    vulns = []
    if ctx["updates"]:
        vulns, total = _build_vulnerabilities(rows(_UPDATES_VIEW), ctx["max_updates"])
        attrs["missingUpdates"] = {"count": total, "truncated": True if total > len(vulns) else None}
        stats["updates"] += len(vulns)

    return ImportAsset(
        id="mecm-inventory:{}:{}".format(ctx["site_code"], resource_id),
        hostnames=hostnames,
        domain=as_text(device.get("Full_Domain_Name0")),
        networkInterfaces=interfaces,
        manufacturer=as_text(computer.get("Manufacturer0")),
        model=as_text(computer.get("Model0")),
        deviceType=_device_type(device, computer, os_row, enclosure),
        firstSeenTS=parse_ts(device.get("Creation_Date0")),
        software=software,
        vulnerabilities=vulns,
        customAttributes=to_custom_attributes(attrs),
    )


def main(*args, **kwargs):
    require(kwargs, "host", "username", "password", "database")
    host = get_string(kwargs, "host")
    port = get_int(kwargs, "port", default=1433)
    username = get_string(kwargs, "username")
    password = get_string(kwargs, "password")
    database = get_string(kwargs, "database")
    encrypt = get_bool(kwargs, "encrypt", default=True)
    trust_cert = get_bool(kwargs, "trust_server_certificate", default=False)
    timeout = get_int(kwargs, "timeout", default=60)
    collection_id = get_string(kwargs, "collection_id", default="")
    software_source = get_string(kwargs, "software_source", default="installed_software")
    max_software = get_int(kwargs, "max_software_per_device", default=1000)
    missing_updates = get_string(kwargs, "missing_updates", default="security")
    max_updates = get_int(kwargs, "max_updates_per_device", default=99)
    page_size = get_int(kwargs, "page_size", default=100)

    # connect() pings eagerly, so an unreachable host, a rejected login and a
    # failed TLS handshake all raise here and end the task. The driver's error
    # names neither the target nor the TLS settings, so log them first; the
    # password never appears.
    print("mecm-inventory: connecting to {}:{} as {} (database={}, encrypt={}, trust_server_certificate={})".format(
        host, port, username, database, encrypt, trust_cert))
    db = sql_connect(driver="mssql", dsn=_dsn(host, port, username, password, database, encrypt, trust_cert), timeout=timeout)

    site_code = as_text(_first(db.query(_SITE_SQL)).get("ThisSiteCode"))
    if not site_code:
        fail("mecm-inventory: could not read the site code from v_Identification; every asset id is scoped by it")

    queries = dict(_INVENTORY_SQL)
    software_view = _SOFTWARE_VIEWS.get(software_source)
    if software_view:
        queries[software_view] = _SOFTWARE_SQL[software_view]
    if missing_updates != "none":
        queries[_UPDATES_VIEW] = _UPDATES_SQL + (_SECURITY_CLAUSE if missing_updates == "security" else "")
    available = _available_views(db, queries)

    ctx = {
        "site_code": site_code,
        "software_view": software_view if software_view in available else None,
        "max_software": max_software,
        "updates": _UPDATES_VIEW in available,
        "max_updates": max_updates,
    }
    stats = {"devices": 0, "reported": 0, "placeholders": 0, "no_correlator": 0, "software": 0, "updates": 0}
    devices_sql = _DEVICES_SQL.format(collection=_COLLECTION_CLAUSE if collection_id else "")

    last_id = 0
    p = pager("devices")
    while p.next():
        params = [last_id, page_size] + ([collection_id] if collection_id else [])
        devices = db.query(devices_sql, params=params)
        if not devices:
            break
        ids = [as_int(d.get("ResourceID")) for d in devices]
        page = {view: _fetch(db, sql, ids) for view, sql in available.items()}
        for device in devices:
            resource_id = as_int(device.get("ResourceID"))
            stats["devices"] += 1
            asset = build_asset(ctx, device, {view: grouped.get(resource_id, []) for view, grouped in page.items()}, stats)
            stats["reported"] += report_asset(asset)
        last_id = ids[-1]
        if len(devices) < page_size:
            break
    db.close()

    if stats["devices"] == 0:
        print("mecm-inventory: site {} has no device records{}".format(site_code, " in collection " + collection_id if collection_id else ""))
    print("mecm-inventory: site {}: {} devices read, {} assets reported, {} placeholder records skipped, {} devices without a hostname, MAC or IP skipped, {} software records and {} missing updates attached".format(
        site_code, stats["devices"], stats["reported"], stats["placeholders"], stats["no_correlator"], stats["software"], stats["updates"]))
    return None

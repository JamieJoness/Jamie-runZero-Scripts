CONFIG = {
    "id": "runzero-ninjaone-tags",
    "name": "NinjaOne (Tag Filtered)",
    "type": "inbound",
    "description": "Imports NinjaOne devices selected by their NinjaOne tags, with detailed inventory.",
    "version": "1",
    "maturity": "alpha",
    "minVersion": "5.1.260818.0",
    "matchBehavior": "no-ip-match no-ip-break",
    "assetType": "device",
    "ownershipAttributes": ["lastLoggedInUser"],
    "trustOS": True,
    "maxPages": 20000,
    "params": [
        {
            "key": "api_url",
            "label": "NinjaOne API URL",
            "type": "url",
            "required": True,
            "placeholder": "https://us2.ninjarmm.com",
            "description": "The NinjaOne instance you sign in to, without /api or /v2.",
        },
        {
            "key": "client_id",
            "label": "OAuth client ID",
            "type": "string",
            "required": True,
        },
        {
            "key": "client_secret",
            "label": "OAuth client secret",
            "type": "secret",
            "required": True,
        },
        {
            "key": "include_tags",
            "label": "Include NinjaOne tags",
            "type": "textarea",
            "required": False,
            "default": "",
            "description": "Exact, case-sensitive tag names, one per line. Configure inclusion or exclusion tags; leaving both blank is rejected.",
        },
        {
            "key": "tag_match",
            "label": "Match inclusion tags",
            "type": "enum",
            "options": ["any", "all"],
            "default": "any",
            "description": "Require any included tag, or all included tags. Exclusions always take priority.",
        },
        {
            "key": "exclude_tags",
            "label": "Exclude NinjaOne tags",
            "type": "textarea",
            "required": False,
            "default": "",
            "description": "Exact, case-sensitive tag names, one per line. A device with any excluded tag is not imported.",
        },
        {
            "key": "include_inventory",
            "label": "Collect extended inventory",
            "type": "bool",
            "default": True,
            "description": "Collect hardware, network interfaces, users, health, antivirus, patches, RAID, and Windows services for selected devices.",
        },
        {
            "key": "include_software",
            "label": "Import installed software",
            "type": "bool",
            "default": True,
            "visibleIf": "include_inventory",
            "visibleIfValue": "true",
            "description": "Import a complete software snapshot. A failed software report stops the batch to protect existing software inventory.",
        },
        {
            "key": "custom_fields",
            "label": "Custom fields to import",
            "type": "textarea",
            "default": "",
            "description": "Optional NinjaOne custom-field API names, one per line. Only these fields are imported; secure plaintext values are never requested.",
        },
    ],
    "includes": {
        "tls_": OPTIONS_TLS,
        "http_": OPTIONS_HTTP,
    },
}

load("runzero.types", "ImportAsset", "Software", "to_custom_attributes")
load("coerce", "as_dict", "as_int", "as_list", "as_text", "dedupe")
load("crypto", "sha256")
load("flatten_json", "flatten")
load("http", "bearer", "get_json", "oauth2_token")
load("json", json_encode="encode")
load("kwargs", "get_bool", "get_http_options", "get_string", "get_url_base", "require")
load("net", "clean_hostnames", "mac_key", "network_interface", "routable_ips")
load("time", "parse_ts")

PAGE_SIZE = 500
REPORT_DEVICE_LIMIT = 100
RESERVED_ATTRIBUTE_SLOTS = 3
REPORTS = [
    ("os", "operating-systems"),
    ("system", "computer-systems"),
    ("networkInterfaces", "network-interfaces"),
    ("loggedOnUsers", "logged-on-users"),
    ("deviceHealth", "device-health"),
    ("processors", "processors"),
    ("disks", "disks"),
    ("volumes", "volumes"),
    ("antivirusStatus", "antivirus-status"),
    ("antivirusThreats", "antivirus-threats"),
    ("osPatches", "os-patches"),
    ("softwarePatches", "software-patches"),
    ("raidControllers", "raid-controllers"),
    ("raidDrives", "raid-drives"),
    ("policyOverrides", "policy-overrides"),
    ("windowsServices", "windows-services"),
]

def warn_once(context, key, message):
    if key not in context["warnings"]:
        context["warnings"][key] = True
        print("ninjaone-tags: " + message)

def report_error(context, path, message, required):
    message = "could not read {} ({})".format(path, message)
    if required:
        fail("ninjaone-tags: " + message)
    warn_once(context, path + message, message + "; continuing without this optional report")
    return None

def authenticate(context):
    settings = context["settings"]
    context["token"] = oauth2_token(
        context["url"] + "/ws/oauth/token",
        client_id=settings["client_id"],
        client_secret=settings["client_secret"],
        scope="monitoring",
        **get_http_options(settings)
    )

def fetch(context, path, params=None, required=True):
    if path in context["disabled"]:
        return None
    params = params or {}
    options = get_http_options(context["settings"], headers={"Authorization": bearer(context["token"])})
    data, error = get_json(context["url"] + path, params=params, **options)
    if error and error.startswith("status 401"):
        authenticate(context)
        options = get_http_options(context["settings"], headers={"Authorization": bearer(context["token"])})
        data, error = get_json(context["url"] + path, params=params, **options)
    if error:
        cause = error.split(":", 1)[0]
        if cause == "status 401":
            fail("ninjaone-tags: authentication was rejected after refreshing the token")
        if not required and cause in ["status 403", "status 404"]:
            context["disabled"][path] = True
        return report_error(context, path, cause, required)
    return data

def read_report(context, endpoint, device_ids, required=False, extra=None):
    path = "/v2/queries/" + endpoint
    query = {"df": "id in ({})".format(",".join([str(device_id) for device_id in device_ids])), "pageSize": PAGE_SIZE}
    query.update(extra or {})
    records = {}
    seen_cursors = {}
    pages = pager("NinjaOne " + endpoint)
    while pages.next():
        payload = fetch(context, path, query, required=required)
        if payload == None and not required:
            return None
        if type(payload) != "dict" or type(payload.get("results")) != "list":
            return report_error(context, path, "expected an object containing a results list", required)
        rows = payload["results"]
        if not rows:
            return records
        cursor = payload.get("cursor")
        if cursor != None and type(cursor) != "dict":
            return report_error(context, path, "malformed report cursor", required)
        cursor = as_dict(cursor)
        cursor_name = as_text(cursor.get("name"))
        if cursor_name:
            cursor_key = (cursor_name, as_text(cursor.get("offset")))
            if cursor_key in seen_cursors:
                return report_error(context, path, "report pagination did not advance", required)
            seen_cursors[cursor_key] = True
        for record in rows:
            row = as_dict(record)
            device_id = as_int(row.get("deviceId"))
            if device_id <= 0 or as_text(row.get("deviceId")) != str(device_id):
                if required:
                    return report_error(context, path, "software snapshot contains a row without a valid deviceId", True)
                warn_once(context, path + ":invalid-id", "skipping report rows without a valid deviceId in " + path)
                continue
            if device_id not in device_ids:
                continue
            if device_id not in records:
                records[device_id] = []
            records[device_id].append(row)
        if not cursor_name:
            return records
        query["cursor"] = cursor_name
    return records

def collect_inventory(context, devices):
    device_ids = {as_int(device["id"]): True for device in devices}
    inventory = {}
    if get_bool(context["settings"], "include_inventory", default=True):
        for key, endpoint in REPORTS:
            extra = {"include": "bl"} if endpoint == "volumes" else None
            inventory[key] = read_report(context, endpoint, device_ids, extra=extra)
        if get_bool(context["settings"], "include_software", default=True):
            inventory["software"] = read_report(context, "software", device_ids, required=True)
    if context["custom_fields"]:
        inventory["customFields"] = read_report(context, "custom-fields", device_ids, extra={
            "fields": ",".join(context["custom_fields"]),
            "showSecureValues": "false",
        })
    return inventory

def device_tags(device):
    tags = device.get("tags")
    if type(tags) != "list":
        return None
    for tag in tags:
        if type(tag) != "string":
            return None
    return dedupe(tags)

def matches_tags(tags, included, excluded, mode):
    for tag in excluded:
        if tag in tags:
            return False
    if not included:
        return True
    matches = [tag in tags for tag in included]
    return all(matches) if mode == "all" else any(matches)

def build_interfaces(device, rows):
    interfaces = []
    known_addresses = {}
    known_macs = {}
    for row in rows:
        addresses = []
        for address in as_list(row.get("ipAddress")):
            addresses.extend(as_text(address).split("|"))
        addresses = routable_ips(addresses)
        macs = dedupe([mac_key(mac) for mac in as_list(row.get("macAddress"))])
        for address in addresses:
            known_addresses[address] = True
        for mac in macs:
            known_macs[mac] = True
        for mac in macs or [None]:
            interface = network_interface(ips=addresses, mac=mac)
            if interface:
                interfaces.append(interface)
    addresses = []
    for address in as_list(device.get("ipAddresses")):
        addresses.extend(as_text(address).split("|"))
    interface = network_interface(ips=[address for address in routable_ips(addresses) if address not in known_addresses])
    if interface:
        interfaces.append(interface)
    for mac in dedupe([mac_key(value) for value in as_list(device.get("macAddresses"))]):
        if mac in known_macs:
            continue
        interface = network_interface(mac=mac)
        if interface:
            interfaces.append(interface)
    return interfaces

def build_software(context, rows):
    software = []
    seen = {}
    for row in rows:
        product = as_text(row.get("name"))
        if not product:
            context["incomplete_software"] += 1
            warn_once(context, "malformed-software", "not importing devices with malformed software snapshots; existing software must not be replaced with an incomplete list")
            return None
        vendor = as_text(row.get("publisher"))
        version = as_text(row.get("version"))
        identity = (vendor, product, version, as_text(row.get("productCode")), as_text(row.get("location")))
        if identity in seen:
            continue
        seen[identity] = True
        software.append(Software(
            id="ninjaone-software:" + sha256(json_encode(identity)),
            vendor=vendor[:128],
            product=product[:128],
            version=version[:128],
            installedAt=parse_ts(row.get("installDate")),
            customAttributes=to_custom_attributes(row),
        ))
    return software

def build_asset(context, device, inventory):
    device_id = as_int(device["id"])
    details = {}
    summary = {}
    for key, report in inventory.items():
        summary["collection.{}.status".format(key)] = "ok" if report != None else "unavailable"
        if report != None:
            details[key] = report.get(device_id, [])
            summary["collection.{}.count".format(key)] = len(details[key])
    software = build_software(context, details.get("software", []))
    if software == None:
        return None
    os_info = dict(as_dict(device.get("os")))
    for row in details.get("os", []):
        os_info.update(row)
    system_info = dict(as_dict(device.get("system")))
    for row in details.get("system", []):
        system_info.update(row)
    interfaces = build_interfaces(device, details.get("networkInterfaces", []))
    fields = dict(device)
    fields.pop("userData", None)
    fields.pop("tags", None)
    fields["deviceId"] = device_id
    fields["ninjaoneDeviceType"] = fields.pop("deviceType", None)
    fields["os"] = os_info
    fields["system"] = system_info
    fields["ninjaoneTags"] = json_encode(device_tags(device))
    fields["lastLoggedInUser"] = device.get("lastLoggedInUser")
    for row in details.get("loggedOnUsers", []):
        fields["lastLoggedInUser"] = row.get("userName") or fields["lastLoggedInUser"]
    for key in ["manufacturer", "name", "architecture", "buildNumber", "releaseId", "servicePackMajorVersion", "servicePackMinorVersion", "language", "needsReboot"]:
        fields["os" + key[0].upper() + key[1:]] = os_info.get(key)
    for key in ["manufacturer", "model", "biosSerialNumber", "serialNumber", "domain", "domainRole", "totalPhysicalMemory", "virtualMachine", "chassisType"]:
        fields["system" + key[0].upper() + key[1:]] = system_info.get(key)
    fields["systemProcessors"] = system_info.get("numberOfProcessors")
    fields["inventory"] = {key: value for key, value in details.items() if key not in ["os", "system", "software", "customFields"]}
    custom_fields = {}
    for row in details.get("customFields", []):
        values = as_dict(row.get("fields"))
        for key in context["custom_fields"]:
            if key in values:
                custom_fields[key] = values[key]
    fields["customFields"] = custom_fields
    limit = 1024 - RESERVED_ATTRIBUTE_SLOTS - len(summary)
    attributes = to_custom_attributes(flatten(fields, separator="."), max_entries=limit)
    if len(attributes) == limit:
        summary["collection.attributeLimitReached"] = True
        warn_once(context, "attribute-limit", "some device attributes reached runZero's 1024-entry limit; collection counts show the full report sizes")
    if len(interfaces) > 256:
        summary["collection.networkInterfaceLimitReached"] = True
        warn_once(context, "interface-limit", "some devices exceeded runZero's 256-interface limit")
    attributes.update(to_custom_attributes(summary))
    return ImportAsset(
        id="ninjaone:{}:{}".format(context["settings"]["client_id"], device_id),
        hostnames=clean_hostnames([device.get(key) for key in ["systemName", "dnsName", "netbiosName"]]),
        networkInterfaces=interfaces[:256],
        os=as_text(os_info.get("name"))[:1024],
        osVersion=as_text(os_info.get("releaseId") or os_info.get("buildNumber"))[:1024],
        model=as_text(system_info.get("model"))[:1024],
        manufacturer=as_text(system_info.get("manufacturer"))[:1024],
        firstSeenTS=parse_ts(device.get("created")),
        lastSeenTS=parse_ts(device.get("lastContact")),
        software=software,
        customAttributes=attributes,
    )

def main(**kwargs):
    require(kwargs, "api_url", "client_id", "client_secret")
    included = dedupe(get_string(kwargs, "include_tags").split("\n"))
    excluded = dedupe(get_string(kwargs, "exclude_tags").split("\n"))
    if not included and not excluded:
        fail("ninjaone-tags: configure include_tags or exclude_tags; an unfiltered import is not allowed")
    mode = get_string(kwargs, "tag_match", default="any")
    if mode not in ["any", "all"]:
        fail("ninjaone-tags: tag_match must be any or all")
    custom_fields = dedupe(get_string(kwargs, "custom_fields").split("\n"))
    if any(["," in name for name in custom_fields]):
        fail("ninjaone-tags: custom_fields must contain one API field name per line, without commas")
    context = {
        "url": get_url_base(kwargs, "api_url"),
        "settings": kwargs,
        "warnings": {},
        "disabled": {},
        "custom_fields": custom_fields,
        "incomplete_software": 0,
    }
    authenticate(context)
    after = 0
    reported = 0
    skipped = 0
    examined = 0
    matched = 0
    unknown_tags = 0
    pages = pager("NinjaOne devices")
    while pages.next():
        query = {"pageSize": PAGE_SIZE}
        if after:
            query["after"] = after
        devices = fetch(context, "/v2/devices-detailed", query)
        if type(devices) != "list":
            fail("ninjaone-tags: devices-detailed did not return a device list")
        if not devices:
            break
        next_after = after
        seen = {}
        selected = []
        for record in devices:
            device = as_dict(record)
            device_id = as_int(device.get("id"))
            if device_id <= 0 or as_text(device.get("id")) != str(device_id):
                skipped += 1
                continue
            next_after = max(next_after, device_id)
            if device_id <= after or device_id in seen:
                continue
            seen[device_id] = True
            examined += 1
            tags = device_tags(device)
            if tags == None:
                detail = fetch(context, "/v2/device/{}".format(device_id), required=False)
                if type(detail) == "dict" and as_int(detail.get("id")) == device_id:
                    device = dict(device)
                    device.update(detail)
                    tags = device_tags(device)
            if tags == None:
                unknown_tags += 1
                continue
            if matches_tags(tags, included, excluded, mode):
                selected.append(device)
                matched += 1
        if next_after <= after:
            fail("ninjaone-tags: device pagination did not advance after ID {}".format(after))
        for offset in range(0, len(selected), REPORT_DEVICE_LIMIT):
            devices_to_enrich = selected[offset:offset + REPORT_DEVICE_LIMIT]
            inventory = collect_inventory(context, devices_to_enrich)
            for device in devices_to_enrich:
                reported += report_asset(build_asset(context, device, inventory))
        after = next_after
    print("ninjaone-tags: examined {} devices; {} matched, {} excluded, {} had unknown tags".format(examined, matched, examined - matched - unknown_tags, unknown_tags))
    print("ninjaone-tags: imported {} devices; skipped {} records without a valid device ID".format(reported, skipped))
    if unknown_tags or context["incomplete_software"]:
        fail("ninjaone-tags: devices were not imported because of {} missing/malformed tag lists and {} incomplete software snapshots".format(unknown_tags, context["incomplete_software"]))
    return None
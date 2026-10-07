CONFIG = {
    "id": "runzero-aruba-airwave",
    "name": "Aruba AirWave",
    "type": "inbound",
    "description": "Imports inventory and enrichment from an on-premises Aruba AirWave 8.3.0.6 server.",
    "version": "1",
    "maturity": "alpha",
    "minVersion": "5.1.260818.0",
    # AirWave does not guarantee persistent, unrecycled device IDs.
    "matchBehavior": "no-id-match no-id-break",
    "maxPages": 10000,
    "params": [
        {
            "key": "url",
            "label": "AirWave URL",
            "type": "url",
            "required": True,
            "pattern": "^https://.+$",
            "placeholder": "https://airwave.example.com",
            "description": "Base URL of the on-premises AirWave server, reachable from the selected Explorer.",
        },
        {
            "key": "instance_id",
            "label": "AirWave instance identifier",
            "type": "string",
            "required": True,
            "pattern": "^[a-z0-9][a-z0-9_-]{0,63}$",
            "description": "A unique, permanent label for this AirWave database, such as airwave-hq. Keep it unchanged across URL changes.",
        },
        {
            "key": "username",
            "label": "AirWave username",
            "type": "secret",
            "required": True,
        },
        {
            "key": "password",
            "label": "AirWave password",
            "type": "secret",
            "required": True,
            "description": "The original password. The script detects and applies AirWave password obfuscation.",
        },
        {
            "key": "folder_id",
            "label": "AirWave folder ID",
            "type": "string",
            "default": "",
            "pattern": "^([1-9][0-9]*)?$",
            "description": "Optional ap_folder_id filter for the managed-device list.",
        },
        {
            "key": "group_id",
            "label": "AirWave group ID",
            "type": "string",
            "default": "",
            "pattern": "^([1-9][0-9]*)?$",
        },
        {
            "key": "controller_id",
            "label": "AirWave controller ID",
            "type": "string",
            "default": "",
            "pattern": "^([1-9][0-9]*)?$",
        },
        {
            "key": "batch_size",
            "label": "Records per detail request",
            "type": "int",
            "default": 25,
            "min": 1,
            "max": 100,
            "description": "Repeated id or mac query parameters per GET request, not a vendor pagination parameter.",
        },
        {
            "key": "request_timeout",
            "label": "Request timeout in seconds",
            "type": "int",
            "default": 60,
            "min": 1,
            "max": 900,
            "description": "Applies to login and every API request. Consider 420 seconds for topology queries on a large estate.",
        },
        {
            "key": "max_child_records",
            "label": "Maximum child records per attribute group",
            "type": "int",
            "default": 32,
            "min": 1,
            "max": 99,
            "description": "Bounds interface, radio, BSSID and observation attributes. Counts and truncation flags remain visible.",
        },
        {
            "key": "include_bssids",
            "label": "Include BSSID and SSID details",
            "type": "bool",
            "default": True,
        },
        {
            "key": "include_clients",
            "label": "Import connected wireless clients",
            "type": "bool",
            "default": True,
        },
        {
            "key": "client_search_limit",
            "label": "Maximum per-client search requests",
            "type": "int",
            "default": 1000,
            "min": 0,
            "max": 100000,
            "description": "Adds SSID, VLAN, username and OS metadata using exact-MAC searches. Zero disables these extra requests; it does not limit client imports.",
        },
        {
            "key": "client_search_query",
            "label": "Additional client search query",
            "type": "string",
            "default": "",
            "description": "Optional query confirmed in your AirWave UI. No wildcard is assumed. Additional matches need a valid MAC; inventory filters still restrict their current AP.",
        },
        {
            "key": "include_rogues",
            "label": "Import observed rogue radios",
            "type": "bool",
            "default": False,
            "description": "Creates MAC-only assets for rogue radios seen by selected APs. These may belong to neighbouring networks or represent multiple radios on one physical AP.",
        },
        {
            "key": "include_ignored_rogues",
            "label": "Include ignored rogue observations",
            "type": "bool",
            "default": False,
        },
        {
            "key": "history_limit",
            "label": "Recent associations and discovery events",
            "type": "int",
            "default": 5,
            "min": 1,
            "max": 20,
        },
        {
            "key": "include_visualrf",
            "label": "Include VisualRF AP locations",
            "type": "bool",
            "default": True,
        },
        {
            "key": "include_alerts",
            "label": "Include active device alerts",
            "type": "bool",
            "default": True,
        },
        {
            "key": "include_topology",
            "label": "Include topology health",
            "type": "bool",
            "default": False,
            "description": "Adds topology role and health metadata. Large inventories can take several minutes; increase the HTTP timeout and consider a folder filter.",
        },
        {
            "key": "include_logs",
            "label": "Include recent device logs",
            "type": "bool",
            "default": False,
            "description": "Adds up to history_limit log entries per device, including AirWave user and message text.",
        },
        {
            "key": "ap_search_query",
            "label": "AP search enrichment query",
            "type": "string",
            "default": "",
            "description": "Optional query confirmed in your AirWave UI. Adds search-only configuration and monitoring metadata to matching inventory IDs; does not filter the main device import.",
        },
    ],
    "includes": {
        "tls_": OPTIONS_TLS,
        "http_": OPTIONS_HTTP,
    },
}

load("base64", base64_encode="encode")
load("coerce", "as_bool", "as_dict", "as_int", "as_list", "as_text", "dicts")
load("crypto", "sha256")
load("flatten_json", "flatten")
load("http", http_get="get", "url_encode", "url_parse")
load("json", json_decode="decode", json_encode="encode")
load("kwargs", "require", "get_string", "get_int", "get_bool", "get_http_options")
load("net", "clean_hostnames", "mac_key", "network_interface", "routable_ips")
load("re", re_match="match", re_search="search", re_sub="sub")
load("runzero.types", "ImportAsset", "to_custom_attributes")
load("socket", socket_tls="tls")
load("time", "now", "parse_ts", "sleep")
load("xml", xml_parse="parse")


def header_values(headers, name):
    for key, values in headers.items():
        if key.lower() == name.lower():
            return [as_text(value) for value in as_list(values)]
    return []


def header_value(headers, name):
    values = header_values(headers, name)
    return values[0] if values else ""


def remember_cookies(ctx, headers):
    for cookie in header_values(headers, "Set-Cookie"):
        pair = cookie.split(";", 1)[0].split("=", 1)
        if len(pair) == 2:
            name, value = pair[0].strip(), pair[1].strip()
            if value:
                ctx["cookies"][name] = value
            else:
                ctx["cookies"].pop(name, None)


def request(ctx, path, params = {}):
    query = "&".join([url_encode({key: value}) for key, values in params.items() for value in as_list(values)])
    url = ctx["url"] + path + ("?" + query if query else "")
    options = dict(ctx["http_options"])
    for attempt in range(4):
        headers = dict(ctx["http_options"].get("headers", {}))
        if ctx["cookies"]:
            headers["Cookie"] = "; ".join(["{}={}".format(key, value) for key, value in ctx["cookies"].items()])
        if ctx["token"]:
            headers["X-BISCOTTI"] = ctx["token"]
        options["headers"] = headers
        response = http_get(url, **options)
        remember_cookies(ctx, response.headers)
        token = header_value(response.headers, "X-BISCOTTI")
        if ctx["token"] and token:
            ctx["token"] = token
        if response.status_code not in [408, 425, 429, 500, 502, 503, 504] or attempt == 3:
            return response
        delay = [1, 2, 4][attempt]
        retry_after = header_value(response.headers, "Retry-After")
        if retry_after:
            seconds = as_int(retry_after, -1)
            if seconds < 0:
                deadline = parse_ts(retry_after, clamp_to_now = False)
                if deadline == None:
                    warn_once(ctx, path + " returned an unreadable Retry-After; stopped retrying")
                    return response
                seconds = max(0, deadline.unix - now().unix)
            delay = max(delay, seconds)
        if delay > 60:
            warn_once(ctx, path + " requested Retry-After longer than 60 seconds; stopped retrying")
            return response
        sleep("{}s".format(delay))
    return None


def login_headers(ctx, body):
    url = url_parse(ctx["url"])
    timeout = ctx["http_options"].get("timeout", 60)
    headers = dict(ctx["http_options"].get("headers", {}))
    headers.update({
        "Host": url.host,
        "Content-Type": "application/x-www-form-urlencoded",
        "Content-Length": str(len(body)),
        "Connection": "close",
    })
    if ctx["cookies"]:
        headers["Cookie"] = "; ".join(["{}={}".format(key, value) for key, value in ctx["cookies"].items()])
    lines = ["POST /LOGIN HTTP/1.1"]
    for key, value in headers.items():
        if "\r" in key or "\n" in key or "\r" in value or "\n" in value:
            fail("airwave: invalid HTTP header configuration")
        lines.append("{}: {}".format(key, value))
    payload = "\r\n".join(lines) + "\r\n\r\n" + body
    connection = socket_tls(url.hostname, as_int(url.port, 443), timeout = timeout, tls = ctx["http_options"].get("tls", {}))
    sent = connection.send(payload)
    if sent != len(payload):
        connection.close()
        fail("airwave: incomplete login request")
    response = str(connection.recv_until("\r\n\r\n", max = 65536))
    connection.close()
    lines = response.split("\r\n")
    status = lines[0].split(" ")
    if len(status) < 2 or status[0] not in ["HTTP/1.0", "HTTP/1.1"]:
        fail("airwave: invalid login HTTP response")
    response_headers = {}
    for line in lines[1:]:
        if not line:
            continue
        pair = line.split(":", 1)
        if len(pair) != 2 or line[0] in [" ", "\t"]:
            fail("airwave: invalid login response header")
        key = pair[0].lower()
        response_headers.setdefault(key, []).append(pair[1].strip())
    return as_int(status[1]), response_headers


def login(ctx):
    ctx["cookies"].clear()
    ctx["token"] = ""
    response = request(ctx, "/api/is_cred_obfusc.json")
    if response.status_code != 200:
        fail("airwave: password-obfuscation check failed: HTTP {}".format(response.status_code))
    setting = as_dict(json_decode(response.body, default = None)).get("is_cred_obfuscation")
    if as_text(setting) not in ["0", "1", "false", "true"]:
        fail("airwave: password-obfuscation endpoint returned an unexpected response")
    password = ctx["password"]
    if as_text(setting) in ["1", "true"]:
        password = base64_encode(password)
    status, headers = login_headers(ctx, url_encode({
        "destination": "/index.html",
        "credential_0": ctx["username"],
        "credential_1": password,
    }))
    if status not in [200, 302, 303]:
        fail("airwave: login failed: HTTP {}; check the AirWave credentials".format(status))
    remember_cookies(ctx, headers)
    ctx["token"] = header_value(headers, "X-BISCOTTI")
    if not ctx["token"] or not ctx["cookies"]:
        fail("airwave: login did not return both a session cookie and X-BISCOTTI token")


def parse_document(body, expected_root):
    body = body.lstrip("\ufeff")
    declaration = re_search("(?i)^\\s*<\\?xml[^>]*encoding\\s*=\\s*['\"]([^'\"]+)['\"]", body)
    if declaration:
        encoding = declaration.groups[1].lower()
        if encoding in ["iso-8859-1", "latin-1", "latin1"]:
            body = "".join([chr(value) for value in body.elem_ords()])
        elif encoding not in ["utf-8", "utf8", "us-ascii", "ascii"]:
            return None
    body = re_sub("^\\s*<\\?xml[^>]*\\?>", "", body).strip()
    opening = re_match("^<([A-Za-z_][A-Za-z0-9_.:-]*)(?:\\s|/?>)", body)
    if not opening:
        return None
    qualified_name = opening.groups[1]
    if qualified_name.rsplit(":", 1)[-1] != expected_root:
        return None
    if not body.endswith("</{}>".format(qualified_name)):
        if not re_match("^<" + qualified_name + "\\b[^>]*?/\\s*>$", body):
            return None
    root = xml_parse(body)
    if root == None or root.tag.rsplit(":", 1)[-1] != expected_root:
        return None
    return root


def warn_once(ctx, message):
    if message not in ctx["warnings"]:
        ctx["warnings"][message] = True
        print("airwave: warning: " + message)


def fetch_response(ctx, path, params = {}, required = False):
    if not required and (path in ctx["unavailable"] or ctx["rate_limited"]):
        return None
    for attempt in range(2):
        response = request(ctx, path, params)
        is_login = "<html" in response.body[:512].lower() or "<!doctype html" in response.body[:512].lower()
        if attempt == 0 and (response.status_code in [401, 403] or is_login):
            login(ctx)
            continue
        if response.status_code == 401 or is_login:
            fail("airwave: session rejected after reauthentication; check credentials and API access")
        if response.status_code != 200:
            message = "{} failed: HTTP {}".format(path, response.status_code)
            if required:
                fail("airwave: " + message)
            warn_once(ctx, message)
            if response.status_code == 429:
                ctx["rate_limited"] = True
                warn_once(ctx, "paused remaining optional requests after persistent rate limiting")
            if response.status_code in [403, 404, 405]:
                ctx["unavailable"][path] = True
            return None
        return response
    return None


def fetch_xml(ctx, path, root_name, params = {}, required = False):
    response = fetch_response(ctx, path, params, required)
    if response == None:
        return None
    root = parse_document(response.body, root_name)
    if root == None:
        message = "{} returned unexpected, incomplete, or unsupported XML".format(path)
        if required:
            fail("airwave: " + message)
        warn_once(ctx, message)
    return root


def children(element, name):
    if element == None:
        return []
    return [child for child in element.children if child.tag.rsplit(":", 1)[-1] == name]


def child_text(element, name):
    matches = children(element, name)
    return as_text(matches[0].text) if matches else ""


def scalar_fields(element):
    fields = {}
    if element == None:
        return fields
    for child in element.children:
        name = child.tag.rsplit(":", 1)[-1]
        if not child.children and name not in ["bssid", "client", "neighbor_ap", "radio", "interface", "association", "discovery_event", "log_message"] and not sensitive_field(name):
            fields[name] = as_text(child.text)
            for key, value in child.attrib.items():
                if key in ["id", "index", "mac"]:
                    fields[name + "." + key] = value
    return fields


def sensitive_field(name):
    return any([word in name.lower() for word in ["password", "secret", "credential", "token", "community", "passphrase", "private_key", "api_key", "access_key", "authorization", "cookie"]])


def record_fields(element):
    fields = scalar_fields(element)
    if element != None:
        for key, value in element.attrib.items():
            if ":" not in key and not sensitive_field(key):
                fields[key] = value
    return fields


def add_fields(fields, prefix, values):
    for key, value in values.items():
        fields[prefix + "." + key] = value


def add_records(fields, prefix, records, limit):
    fields[prefix + ".count"] = len(records)
    if len(records) > limit:
        fields[prefix + ".truncated"] = True
    for index, record in enumerate(records[:limit]):
        add_fields(fields, "{}.{}".format(prefix, index), record_fields(record))


def custom_attributes(fields):
    attrs = to_custom_attributes(fields, prefix = "airwave", max_entries = 1022)
    if len([value for value in fields.values() if as_text(value)]) > 1022:
        attrs["airwave.attributes_truncated"] = "true"
    if any([len(as_text(value)) > 1024 for value in fields.values()]):
        attrs["airwave.attribute_values_truncated"] = "true"
    return attrs


def source_id(element):
    value = as_text(element.get("id"))
    return value if len(value) <= 20 and re_match("^[1-9][0-9]*$", value) else ""


def records_by_id(root, name):
    records = {}
    for record in children(root, name):
        record_id = source_id(record)
        if record_id and record_id not in records:
            records[record_id] = record
    return records


def folder_path(ctx, folder_id):
    names = []
    visited = {}
    for depth in range(128):
        if not folder_id:
            return " / ".join(reversed(names))
        if folder_id in visited or folder_id not in ctx["folders"]:
            return ""
        visited[folder_id] = True
        folder = ctx["folders"][folder_id]
        name = child_text(folder, "name")
        if not name:
            return ""
        names.append(name)
        folder_id = child_text(folder, "parent_id")
    return ""


def related_device_fields(ctx, fields, prefix, device_id):
    device = ctx["devices"].get(device_id)
    if device != None:
        for name in ["name", "lan_ip", "lan_mac", "mfgr", "model", "serial_number"]:
            fields[prefix + "." + name] = child_text(device, name)


def load_visualrf(ctx):
    root = fetch_xml(ctx, "/visualrf/campus.xml", "campuses", {"buildings": "", "sites": "", "aps": "", "stats": ""})
    for campus in children(root, "campus"):
        for building in children(campus, "building"):
            for site in children(building, "site"):
                for access_point in children(site, "ap"):
                    device_id = as_text(access_point.get("id"))
                    if device_id not in ctx["devices"] or as_text(access_point.get("state")).lower() == "planned":
                        continue
                    locations = ctx["visualrf"].setdefault(device_id, {"count": 0, "records": []})
                    locations["count"] += 1
                    if len(locations["records"]) >= ctx["child_limit"]:
                        continue
                    fields = {}
                    for prefix, node in [("campus", campus), ("building", building), ("site", site), ("ap", access_point)]:
                        add_fields(fields, prefix, record_fields(node))
                    summary = children(site, "summary")
                    if summary:
                        add_fields(fields, "site.summary", record_fields(summary[0]))
                    add_records(fields, "ap.radio", children(access_point, "radio"), ctx["child_limit"])
                    locations["records"].append(fields)


def add_visualrf(ctx, fields, prefix, device_id):
    locations = ctx["visualrf"].get(device_id)
    if locations == None:
        return
    fields[prefix + ".count"] = locations["count"]
    if locations["count"] > len(locations["records"]):
        fields[prefix + ".truncated"] = True
    for index, location in enumerate(locations["records"]):
        add_fields(fields, "{}.{}".format(prefix, index), location)


def load_alerts(ctx):
    root = fetch_xml(ctx, "/alerts.xml", "amp_alert")
    if root == None:
        return
    ctx["alerts_available"] = True
    records = sorted(children(root, "record"), key = lambda record: as_int(child_text(record, "creation_time")), reverse = True)
    for record in records:
        url = url_parse(child_text(record, "view_url"))
        if url == None or url.path != "/ap_monitoring":
            continue
        ids = as_list(url.query.get("id"))
        if len(ids) != 1 or ids[0] not in ctx["devices"]:
            continue
        alerts = ctx["alerts"].setdefault(ids[0], {"count": 0, "records": []})
        alerts["count"] += 1
        if len(alerts["records"]) < ctx["child_limit"]:
            alerts["records"].append(record)


def load_topology(ctx, folder_id):
    response = fetch_response(ctx, "/topology/getTopology", {"folderId": folder_id})
    if response == None:
        return
    data = as_dict(json_decode(response.body, default = None))
    if type(data.get("nodes")) != "list":
        warn_once(ctx, "/topology/getTopology returned an unexpected nodes structure")
        return
    for node in dicts(data["nodes"]):
        device_id = as_text(node.get("apID"))
        if device_id in ctx["devices"]:
            fields = {key: node[key] for key in ["apID", "name", "role", "model", "ip", "mac", "folder", "HealthInfo"] if key in node}
            ctx["topology"][device_id] = flatten(fields, separator = ".")


def nested_children(element, container, name):
    return [child for parent in children(element, container) for child in children(parent, name)]


def observation_entry(population, mac):
    if mac not in population:
        population[mac] = {"observations": {}, "count": 0, "source_id": "", "search": None}
    return population[mac]


def collect_observations(ctx, device, detail):
    for radio in children(detail, "radio"):
        populations = []
        if ctx["include_clients"]:
            populations.append((ctx["clients"], children(radio, "client"), "client"))
        if ctx["include_rogues"]:
            populations.append((ctx["rogues"], children(radio, "neighbor_ap"), "rogue"))
        for population, records, kind in populations:
            for record in records:
                if kind == "client" and not as_bool(child_text(record, "assoc_stat")):
                    continue
                if kind == "rogue" and (child_text(record, "neighbor_type") != "rogue" or not source_id(record)):
                    continue
                mac = mac_key(child_text(record, "radio_mac"))
                if not mac:
                    continue
                entry = observation_entry(population, mac)
                if not entry["source_id"]:
                    entry["source_id"] = source_id(record)
                key = source_id(device) + ":" + as_text(radio.get("index"))
                if key in entry["observations"]:
                    continue
                entry["count"] += 1
                if len(entry["observations"]) >= ctx["child_limit"]:
                    continue
                fields = scalar_fields(record)
                fields.update({
                    "ap_id": source_id(device),
                    "ap_name": child_text(device, "name"),
                    "radio_index": radio.get("index"),
                    "radio_type": child_text(radio, "radio_type"),
                    "ap_folder": child_text(detail, "ap_folder"),
                    "ap_group": child_text(detail, "ap_group"),
                })
                entry["observations"][key] = fields


def observation_fields(ctx, entry, mac, record_type):
    fields = {
        "instance_id": ctx["instance_id"],
        "record_type": record_type,
        "mac": mac,
        "observation.count": entry["count"],
    }
    if entry["count"] > len(entry["observations"]):
        fields["observation.truncated"] = True
    for index, observation in enumerate(entry["observations"].values()):
        add_fields(fields, "observation.{}".format(index), observation)
        add_visualrf(ctx, fields, "observation.{}.ap_location".format(index), observation.get("ap_id"))
    return fields


def collect_client_search(ctx, query):
    root = fetch_xml(ctx, "/client_search.xml", "amp_client_search", {"query": query})
    for record in children(root, "record"):
        mac = mac_key(child_text(record, "mac"))
        if not mac:
            continue
        if ctx["filtered"] and child_text(record, "ap_id") not in ctx["devices"]:
            continue
        observation_entry(ctx["clients"], mac)["search"] = record


def client_search(ctx, mac, entry):
    if entry["search"] != None:
        return entry["search"]
    if ctx["search_count"] >= ctx["search_limit"]:
        if ctx["search_limit"]:
            warn_once(ctx, "client search request limit reached; remaining clients retain detail and AP observations")
        return None
    if "/client_search.xml" in ctx["unavailable"]:
        return None
    ctx["search_count"] += 1
    root = fetch_xml(ctx, "/client_search.xml", "amp_client_search", {"query": mac})
    for record in children(root, "record"):
        if mac_key(child_text(record, "mac")) == mac:
            return record
    return None


def build_client(ctx, mac, entry, detail, search):
    fields = observation_fields(ctx, entry, mac, "wireless_client")
    fields["detail.available"] = detail != None
    fields["search.available"] = search != None
    fields["search.request_limit"] = ctx["search_limit"]
    add_fields(fields, "detail", scalar_fields(detail))
    add_fields(fields, "search", scalar_fields(search))
    current_aps = children(detail, "ap")
    if current_aps:
        add_visualrf(ctx, fields, "associated_ap.visualrf", as_text(current_aps[0].get("id")))
    current_lans = nested_children(detail, "lan_elements", "lan")
    current_vpns = nested_children(detail, "vpn_elements", "vpn")
    add_records(fields, "current.lan", current_lans, ctx["child_limit"])
    add_records(fields, "current.vpn", current_vpns, ctx["child_limit"])
    associations = children(detail, "association")
    add_records(fields, "association", associations, ctx["history_limit"])
    for index, association in enumerate(associations[:ctx["history_limit"]]):
        add_records(fields, "association.{}.lan".format(index), nested_children(association, "lan_elements", "lan"), ctx["child_limit"])
        add_records(fields, "association.{}.vpn".format(index), nested_children(association, "vpn_elements", "vpn"), ctx["child_limit"])
    associated = as_bool(child_text(detail, "assoc_stat")) if detail != None else bool(entry["observations"])
    addresses = []
    names = []
    last_seen = None
    if not associated:
        for association in associations[:ctx["history_limit"]]:
            disconnected = parse_ts(child_text(association, "disconnect_time"))
            if disconnected != None and (last_seen == None or disconnected.unix > last_seen.unix):
                last_seen = disconnected
    if associated:
        addresses = routable_ips([lan.get("ip_address") for lan in current_lans])
        names = [lan.get("hostname") for lan in current_lans]
        if not addresses:
            observed = routable_ips([observation.get("ip") for observation in entry["observations"].values()])
            if len(observed) == 1:
                addresses = observed
        names.append(child_text(search, "lan_hostname"))
    return ImportAsset(
        id = "aruba-airwave:{}:client:mac:{}".format(ctx["instance_id"], mac.replace(":", "")),
        hostnames = clean_hostnames(names),
        networkInterfaces = [network_interface(mac = mac, ips = addresses)],
        os = child_text(search, "device_os_detail") or child_text(search, "device_os"),
        lastSeenTS = last_seen,
        customAttributes = custom_attributes(fields),
    )


def import_clients(ctx, batch_size, query):
    if query:
        collect_client_search(ctx, query)
    macs = sorted(ctx["clients"])
    batches = pager("AirWave client detail batches")
    reported = 0
    for offset in range(0, len(macs), batch_size):
        batches.next()
        batch = macs[offset:offset + batch_size]
        root = fetch_xml(ctx, "/client_detail.xml", "amp_client_detail", {"mac": batch, "limit": ctx["history_limit"]})
        details = {}
        for client in children(root, "client"):
            mac = mac_key(client.get("mac"))
            if mac in batch and mac not in details:
                details[mac] = client
        for mac in batch:
            entry = ctx["clients"][mac]
            search = client_search(ctx, mac, entry)
            reported += report_asset(build_client(ctx, mac, entry, details.get(mac), search))
    return reported


def build_rogue(ctx, mac, entry, detail):
    if detail != None and mac_key(child_text(detail, "radio_mac")) != mac:
        warn_once(ctx, "rogue detail MAC did not match its observation; retained observation metadata only")
        detail = None
    fields = observation_fields(ctx, entry, mac, "rogue_radio")
    fields["id"] = entry["source_id"]
    fields["detail.available"] = detail != None
    add_fields(fields, "detail", scalar_fields(detail))
    events = children(detail, "discovery_event")
    add_records(fields, "discovery_event", events, ctx["history_limit"])
    for index, event in enumerate(events[:ctx["history_limit"]]):
        discovering = children(event, "discovering_ap")
        if discovering:
            add_fields(fields, "discovery_event.{}.discovering_ap".format(index), record_fields(discovering[0]))
    first_seen = parse_ts(child_text(detail, "first_discovered"))
    last_seen = parse_ts(child_text(detail, "last_discovered"))
    if first_seen != None and last_seen != None and first_seen.unix > last_seen.unix:
        first_seen = None
    return ImportAsset(
        id = "aruba-airwave:{}:rogue:mac:{}".format(ctx["instance_id"], mac.replace(":", "")),
        networkInterfaces = [network_interface(mac = mac)],
        firstSeenTS = first_seen,
        lastSeenTS = last_seen,
        customAttributes = custom_attributes(fields),
    )


def import_rogues(ctx, batch_size):
    macs = sorted(ctx["rogues"])
    batches = pager("AirWave rogue detail batches")
    reported = 0
    for offset in range(0, len(macs), batch_size):
        batches.next()
        batch = macs[offset:offset + batch_size]
        params = {"id": [ctx["rogues"][mac]["source_id"] for mac in batch], "limit": ctx["history_limit"]}
        details = records_by_id(fetch_xml(ctx, "/rogue_detail.xml", "amp_rogue_detail", params), "rogue_ap")
        for mac in batch:
            entry = ctx["rogues"][mac]
            reported += report_asset(build_rogue(ctx, mac, entry, details.get(entry["source_id"])))
    return reported


def device_identity(ctx, device):
    serial = child_text(device, "serial_number")
    manufacturer = child_text(device, "mfgr")
    mac = mac_key(child_text(device, "lan_mac"))
    if mac:
        key = "mac:" + mac.replace(":", "")
    elif serial and serial.lower() not in ["unknown", "none", "n/a", "-", "0"]:
        key = "serial:" + sha256(json_encode([manufacturer.lower(), serial]))
    else:
        return ""
    return "aruba-airwave:{}:device:{}".format(ctx["instance_id"], key)


def build_device(ctx, device, detail = None, bssids = None, logs = None):
    asset_id = device_identity(ctx, device)
    if not asset_id or not source_id(device):
        return None
    names = clean_hostnames([child_text(device, "name")])
    interface = network_interface(
        mac = mac_key(child_text(device, "lan_mac")),
        ips = routable_ips([child_text(device, "lan_ip")]),
    )
    interfaces = [interface] if interface else []
    seen_macs = {mac_key(child_text(device, "lan_mac")): True}
    interfaces_truncated = False
    for port in children(detail, "interface"):
        mac = mac_key(child_text(port, "mac_address"))
        if mac and mac not in seen_macs:
            seen_macs[mac] = True
            if len(interfaces) < 99:
                interfaces.append(network_interface(mac = mac))
            else:
                interfaces_truncated = True
    if not names and not interfaces:
        return None
    fields = scalar_fields(device)
    fields.update({"instance_id": ctx["instance_id"], "record_type": "managed_device", "id": device.get("id")})
    if interfaces_truncated:
        fields["network_interfaces_truncated"] = True
    fields["url"] = ctx["url"] + "/ap_monitoring?" + url_encode({"id": device.get("id")})
    folders = children(device, "folder")
    if folders:
        fields["folder.path"] = folder_path(ctx, as_text(folders[0].get("id")))
    related_device_fields(ctx, fields, "controller", child_text(device, "controller_id"))
    related_device_fields(ctx, fields, "upstream", child_text(device, "upstream_device_id"))
    record_id = source_id(device)
    add_visualrf(ctx, fields, "visualrf.location", record_id)
    add_fields(fields, "topology", ctx["topology"].get(record_id, {}))
    add_fields(fields, "search", scalar_fields(ctx["ap_search"].get(record_id)))
    if ctx["include_alerts"]:
        fields["alerts.available"] = ctx["alerts_available"]
        if ctx["alerts_available"]:
            alerts = ctx["alerts"].get(record_id, {"count": 0, "records": []})
            add_records(fields, "alert", alerts["records"], ctx["child_limit"])
            fields["alert.count"] = alerts["count"]
            if alerts["count"] > len(alerts["records"]):
                fields["alert.truncated"] = True
            for index, alert in enumerate(alerts["records"]):
                severity = children(alert, "severity")
                if severity:
                    fields["alert.{}.severity_label".format(index)] = severity[0].get("ascii_value")
    if ctx["include_logs"]:
        fields["logs.available"] = logs != None
        add_records(fields, "log", children(logs, "log_message"), ctx["history_limit"])
    add_records(fields, "radio", children(device, "radio"), ctx["child_limit"])
    fields["detail.available"] = detail != None
    if detail != None:
        add_fields(fields, "detail", scalar_fields(detail))
        add_records(fields, "interface", children(detail, "interface"), ctx["child_limit"])
        radios = children(detail, "radio")
        add_records(fields, "detail.radio", radios, ctx["child_limit"])
        for index, radio in enumerate(radios[:ctx["child_limit"]]):
            prefix = "detail.radio.{}".format(index)
            add_records(fields, prefix + ".client", children(radio, "client"), ctx["child_limit"])
            add_records(fields, prefix + ".neighbor", children(radio, "neighbor_ap"), ctx["child_limit"])
        neighbors = [neighbor for radio in radios for neighbor in children(radio, "neighbor_ap")]
        fields["neighbor.rogue_count"] = len([neighbor for neighbor in neighbors if child_text(neighbor, "neighbor_type") == "rogue"])
        fields["neighbor.managed_count"] = len([neighbor for neighbor in neighbors if child_text(neighbor, "neighbor_type") == "managed"])
    if ctx["include_bssids"]:
        fields["bssid.available"] = bssids != None
        radios = children(bssids, "radio")
        add_records(fields, "bssid.radio", radios, ctx["child_limit"])
        fields["bssid.count"] = 0
        for radio in radios:
            fields["bssid.count"] += len(children(radio, "bssid"))
        for index, radio in enumerate(radios[:ctx["child_limit"]]):
            add_records(fields, "bssid.radio.{}.bssid".format(index), children(radio, "bssid"), ctx["child_limit"])
    return ImportAsset(
        id = asset_id,
        hostnames = names,
        networkInterfaces = interfaces,
        manufacturer = child_text(device, "mfgr"),
        model = child_text(device, "model"),
        lastSeenTS = parse_ts(child_text(device, "last_contacted")),
        customAttributes = custom_attributes(fields),
    )


def main(*args, **kwargs):
    require(kwargs, "url", "instance_id", "username", "password")
    url = url_parse(get_string(kwargs, "url"))
    if url == None or not url.host or url.scheme != "https" or url.username or url.password or url.path not in ["", "/"] or url.raw_query or url.fragment:
        fail("airwave: use the HTTPS AirWave base URL without a path, query, or embedded credentials")
    ctx = {
        "url": "{}://{}".format(url.scheme, url.host),
        "instance_id": get_string(kwargs, "instance_id"),
        "username": get_string(kwargs, "username"),
        "password": get_string(kwargs, "password"),
        "http_options": get_http_options(kwargs, "http_", "tls_", {"Accept": "application/xml, application/json"}),
        "cookies": {},
        "token": "",
        "warnings": {},
        "unavailable": {},
        "rate_limited": False,
        "child_limit": get_int(kwargs, "max_child_records", 32),
        "include_bssids": get_bool(kwargs, "include_bssids", True),
        "include_clients": get_bool(kwargs, "include_clients", True),
        "include_rogues": get_bool(kwargs, "include_rogues", False),
        "include_alerts": get_bool(kwargs, "include_alerts", True),
        "include_logs": get_bool(kwargs, "include_logs", False),
        "history_limit": get_int(kwargs, "history_limit", 5),
        "search_limit": get_int(kwargs, "client_search_limit", 1000),
        "search_count": 0,
        "clients": {},
        "rogues": {},
        "folders": {},
        "devices": {},
        "visualrf": {},
        "topology": {},
        "alerts": {},
        "alerts_available": False,
        "ap_search": {},
    }
    ctx["http_options"]["timeout"] = get_int(kwargs, "request_timeout", 60)
    login(ctx)
    filters = {}
    for parameter, field in [("folder_id", "ap_folder_id"), ("group_id", "ap_group_id"), ("controller_id", "controller_id")]:
        value = get_string(kwargs, parameter)
        if value:
            filters[field] = value
    ctx["filtered"] = bool(filters)
    root = fetch_xml(ctx, "/ap_list.xml", "amp_ap_list", filters, required = True)
    seen = {}
    devices = []
    reported = 0
    skipped = 0
    for device in children(root, "ap"):
        record_id = source_id(device)
        asset_id = device_identity(ctx, device)
        if not record_id or not asset_id:
            skipped += 1
            continue
        ctx["devices"].setdefault(record_id, device)
        if asset_id in seen:
            continue
        seen[asset_id] = True
        devices.append(device)
    if devices:
        folders = fetch_xml(ctx, "/folder_list.xml", "amp_folder_list")
        ctx["folders"] = records_by_id(folders, "folder")
        if get_bool(kwargs, "include_visualrf", True):
            load_visualrf(ctx)
        if ctx["include_alerts"]:
            load_alerts(ctx)
        if get_bool(kwargs, "include_topology", False):
            load_topology(ctx, get_string(kwargs, "folder_id"))
        ap_search_query = get_string(kwargs, "ap_search_query")
        if ap_search_query:
            ctx["ap_search"] = records_by_id(fetch_xml(ctx, "/ap_search.xml", "amp_ap_search", {"query": ap_search_query}), "record")
    batch_size = get_int(kwargs, "batch_size", 25)
    batches = pager("AirWave device detail batches")
    for offset in range(0, len(devices), batch_size):
        batches.next()
        batch = devices[offset:offset + batch_size]
        params = {"id": [source_id(device) for device in batch]}
        detail_params = dict(params)
        if get_bool(kwargs, "include_ignored_rogues", False):
            detail_params["include"] = "ignored"
        details = records_by_id(fetch_xml(ctx, "/ap_detail.xml", "amp_ap_detail", detail_params), "ap")
        bssids = {}
        if ctx["include_bssids"]:
            bssids = records_by_id(fetch_xml(ctx, "/api/ap_bssid_list.xml", "amp_ap_bssid_list", params), "ap")
        logs = {}
        if ctx["include_logs"]:
            log_params = dict(params)
            log_params["limit"] = ctx["history_limit"]
            logs = records_by_id(fetch_xml(ctx, "/ap_log.xml", "amp_ap_log", log_params), "ap")
        for device in batch:
            record_id = source_id(device)
            collect_observations(ctx, device, details.get(record_id))
            asset = build_device(ctx, device, details.get(record_id), bssids.get(record_id), logs.get(record_id))
            if asset == None:
                skipped += 1
            else:
                reported += report_asset(asset)
    clients = import_clients(ctx, batch_size, get_string(kwargs, "client_search_query")) if ctx["include_clients"] else 0
    rogues = import_rogues(ctx, batch_size) if ctx["include_rogues"] else 0
    print("airwave: reported {} managed devices, {} wireless clients and {} rogue radios; skipped {} managed records without usable identity; {} warnings".format(reported, clients, rogues, skipped, len(ctx["warnings"])))
    return None
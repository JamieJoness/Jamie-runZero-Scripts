load('json', json_encode='encode', json_decode='decode')
load('http', http_post='post')
load("runzero.types", "ImportAsset", "NetworkInterface")
load("net", "ip_address")

DEFAULT_BASE_URL = 'YOUR THREATLOCKER_BASE_URL'  # e.g. https://portalapi.g.threatlocker.com (use your instance host)
PAGE_SIZE = 200
MAX_PAGES = 1000  # Safety cap to avoid an infinite pagination loop if the API ignores paging
DEBUG = True  # Set to False to silence the verbose DEBUG lines in the task log


def dbg(msg):
    if DEBUG:
        print("DEBUG: {}".format(msg))


def _preview(value, limit=2000):
    # Safe, truncated string preview for logging raw bodies / records without flooding the log
    s = str(value)
    if len(s) > limit:
        return s[:limit] + " ...[truncated, {} chars total]".format(len(s))
    return s


def _keys(d):
    if type(d) == "dict":
        return ", ".join(d.keys())
    return "(not a dict; type={})".format(type(d))


def _add_attr(attrs, key, value):
    # Add a custom attribute only when non-empty; coerce to string and cap at 1023 chars.
    if value:
        attrs[key] = str(value)[:1023]


def fetch_computers(url, headers):
    computers = []
    page = 1

    while True:
        if page > MAX_PAGES:
            print("WARNING: Reached MAX_PAGES ({}); stopping to avoid an infinite loop. The API may be ignoring pagination.".format(MAX_PAGES))
            break

        body = json_encode({
            "orderBy": "computername",
            "pageNumber": page,
            "pageSize": PAGE_SIZE,
        })
        dbg("--- Requesting page {} ---".format(page))
        dbg("POST {}".format(url))
        dbg("Request body: {}".format(body))

        response = http_post(url=url, headers=headers, body=bytes(body))

        if not response:
            print("ERROR: ThreatLocker API returned no response object on page {} (network/DNS/TLS failure?).".format(page))
            break

        dbg("Page {} HTTP status: {}".format(page, response.status_code))
        dbg("Page {} raw response body preview: {}".format(page, _preview(response.body)))

        if response.status_code == 401:
            print("ERROR: 401 Unauthorized on page {}. Verify the Authorization header is the RAW API key (no 'Bearer ' prefix) and the key is valid/active. Body: {}".format(page, _preview(response.body)))
            break
        if response.status_code == 403:
            print("ERROR: 403 Forbidden on page {}. The key may lack API permission, or a ManagedOrganizationId is required/incorrect. Body: {}".format(page, _preview(response.body)))
            break
        if response.status_code == 404:
            print("ERROR: 404 Not Found on page {}. Check the base URL / instance host and the endpoint path. URL: {} Body: {}".format(page, url, _preview(response.body)))
            break
        if response.status_code == 429:
            print("ERROR: 429 Rate limited on page {}. Body: {}".format(page, _preview(response.body)))
            break
        if response.status_code != 200:
            print("ERROR: ThreatLocker API returned HTTP {} on page {}. Body: {}".format(response.status_code, page, _preview(response.body)))
            break

        data = json_decode(response.body)
        if data == None:
            print("ERROR: Failed to decode JSON on page {}. The raw body above is not valid JSON. Raw: {}".format(page, _preview(response.body)))
            break

        dbg("Page {} decoded JSON type: {}".format(page, type(data)))
        if type(data) == "dict":
            dbg("Page {} top-level dict keys: [{}]".format(page, _keys(data)))

        if type(data) == "list":
            batch = data
        elif type(data) == "dict":
            batch = data.get("data", data.get("results", []))
            if type(batch) != "list":
                print("WARNING: Page {} response is a dict but no list found under 'data'/'results' (got {}). Top-level keys: [{}]".format(page, type(batch), _keys(data)))
                batch = []
        else:
            print("WARNING: Page {} unexpected top-level JSON type: {}".format(page, type(data)))
            batch = []

        dbg("Page {} batch size: {}".format(page, len(batch)))

        if not batch:
            dbg("Page {} returned an empty batch; stopping pagination.".format(page))
            break

        computers.extend(batch)
        dbg("Total computers accumulated so far: {}".format(len(computers)))

        if len(batch) < PAGE_SIZE:
            dbg("Batch ({}) smaller than PAGE_SIZE ({}); assuming last page.".format(len(batch), PAGE_SIZE))
            break

        page += 1

    dbg("fetch_computers finished: {} total record(s).".format(len(computers)))
    return computers


def build_network_interfaces(computer):
    ipv4s = []
    ipv6s = []
    mac = computer.get("macAddress", "")

    ip_value = computer.get("ipAddress", computer.get("ip", ""))
    if ip_value:
        for raw_ip in str(ip_value).split(","):
            raw_ip = raw_ip.strip()
            if not raw_ip:
                continue
            parsed = ip_address(raw_ip)
            if not parsed:
                dbg("Could not parse IP '{}' (mac='{}')".format(raw_ip, mac))
                continue
            if parsed.version == 4:
                ipv4s.append(parsed)
            elif parsed.version == 6:
                ipv6s.append(parsed)

    # Omit the interface entirely when there's nothing useful to attach.
    if not mac and len(ipv4s) == 0 and len(ipv6s) == 0:
        return []
    if mac:
        return [NetworkInterface(macAddress=mac, ipv4Addresses=ipv4s, ipv6Addresses=ipv6s)]
    return [NetworkInterface(ipv4Addresses=ipv4s, ipv6Addresses=ipv6s)]


def build_asset(computer):
    # Returns (asset_or_None, reason). reason is "" on success, else a short explanation of the skip.
    if type(computer) != "dict":
        return (None, "record is not a dict (type={})".format(type(computer)))

    asset_id = computer.get("computerId", computer.get("id", ""))
    if not asset_id:
        return (None, "missing id (looked for computerId/id)")

    hostnames = []
    name = computer.get("computerName", computer.get("computername", ""))
    if name:
        hostnames.append(name)
    fqdn = computer.get("fqdn", "")
    if fqdn and fqdn != name:
        hostnames.append(fqdn)

    if not hostnames:
        return (None, "missing hostname (looked for computerName/computername/fqdn)")

    custom_attrs = {}
    _add_attr(custom_attrs, "threatlocker.agentVersion", computer.get("agentVersion", ""))
    _add_attr(custom_attrs, "threatlocker.lastUser", computer.get("lastLoggedInUser", computer.get("userName", "")))
    _add_attr(custom_attrs, "threatlocker.organization", computer.get("organizationName", computer.get("groupName", "")))
    _add_attr(custom_attrs, "threatlocker.serialNumber", computer.get("serialNumber", ""))
    _add_attr(custom_attrs, "threatlocker.status", computer.get("onlineStatus", computer.get("status", "")))
    _add_attr(custom_attrs, "threatlocker.lastCheckin", computer.get("lastCheckinDate", computer.get("lastSeen", "")))
    _add_attr(custom_attrs, "threatlocker.manufacturer", computer.get("manufacturer", ""))
    _add_attr(custom_attrs, "threatlocker.model", computer.get("model", ""))
    _add_attr(custom_attrs, "threatlocker.domain", computer.get("domain", ""))

    asset = ImportAsset(
        id=str(asset_id),
        hostnames=hostnames,
        os=computer.get("operatingSystem", computer.get("os", "")),
        osVersion=computer.get("osVersion", ""),
        networkInterfaces=build_network_interfaces(computer),
        customAttributes=custom_attrs,
    )
    return (asset, "")


def main(*args, **kwargs):
    print("INFO: ===== ThreatLocker custom integration starting =====")
    dbg("kwargs provided: [{}]".format(", ".join(kwargs.keys())))

    token = kwargs.get('access_secret', '')
    if not token:
        print('ERROR: No API token. Set access_secret to your ThreatLocker Org API token.')
        return None
    dbg("access_secret present (length={}).".format(len(token)))
    if token != token.strip():
        print("WARNING: access_secret has leading/trailing whitespace - this can cause 401 Unauthorized.")

    base_url = kwargs.get('access_key', '')
    if not base_url:
        print("WARNING: No access_key provided; falling back to DEFAULT_BASE_URL.")
        base_url = DEFAULT_BASE_URL
    if base_url.endswith('/'):
        base_url = base_url[:-1]
    dbg("Base URL in use: {}".format(base_url))

    if base_url == DEFAULT_BASE_URL or "YOUR THREATLOCKER" in base_url:
        print("ERROR: Base URL is still the placeholder. Set access_key to your ThreatLocker instance host, e.g. https://portalapi.g.threatlocker.com")
        return None
    if not (base_url.startswith("http://") or base_url.startswith("https://")):
        print("WARNING: Base URL does not start with http:// or https:// (got '{}').".format(base_url))

    url = "{}/portalapi/Computer/ComputerGetByAllParameters".format(base_url)
    dbg("Full request URL: {}".format(url))

    headers = {
        "Authorization": token,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    dbg("Request header names being sent (Authorization value masked): [{}]".format(", ".join(headers.keys())))

    computers = fetch_computers(url, headers)
    dbg("fetch_computers returned {} record(s).".format(len(computers)))
    if not computers:
        print("WARNING: No computers returned from ThreatLocker API. See the DEBUG lines above for the HTTP status and raw body.")
        return []

    # Field-name diagnostics: dump the first record so the real field names are visible in the log.
    first = computers[0]
    if type(first) == "dict":
        dbg("First record top-level keys: [{}]".format(_keys(first)))
        dbg("First record JSON preview: {}".format(_preview(json_encode(first))))
    else:
        print("WARNING: First record is not a dict (type={}). Preview: {}".format(type(first), _preview(first)))

    assets = []
    skip_reasons = {}
    logged_samples = 0
    for c in computers:
        asset, reason = build_asset(c)
        if asset:
            assets.append(asset)
        else:
            skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
            if logged_samples < 5:
                logged_samples += 1
                dbg("Skipped record sample #{}: reason='{}'; keys=[{}]".format(logged_samples, reason, _keys(c)))

    if skip_reasons:
        for reason in skip_reasons:
            print("INFO: Skipped {} record(s) - {}".format(skip_reasons[reason], reason))

    print("INFO: Built {} asset(s) from {} computer record(s).".format(len(assets), len(computers)))
    print("INFO: ===== ThreatLocker custom integration finished =====")
    return assets
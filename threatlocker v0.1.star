load('json', json_encode='encode', json_decode='decode')
load('http', http_get='get', http_patch='patch', http_post='post')
load("runzero.types", "ImportAsset", "NetworkInterface")
load("net", "ip_address")

DEFAULT_BASE_URL = 'YOUR THREATLOCKER_BASE_URL'  # e.g. https://api.threatlocker.com
PAGE_SIZE = 200

def fetch_computers(url, headers):
    computers = []
    page = 1

    while True:
        body = json_encode({
            "orderBy": "computername",
            "pageNumber": page,
            "pageSize": PAGE_SIZE,
        })

        response = http_post(url=url, headers=headers, body=bytes(body))

        if not response:
            print("ERROR: ThreatLocker API returned no response on page {}".format(page))
            break

        if response.status_code == 429:
            print("ERROR: ThreatLocker API rate limit hit on page {}".format(page))
            break

        if response.status_code != 200:
            print("ERROR: ThreatLocker API returned status {} on page {}".format(
                response.status_code, page
            ))
            break

        data = json_decode(response.body)
        if not data:
            print("ERROR: Failed to decode ThreatLocker API response on page {}".format(page))
            break

        batch = data.get("data", data.get("results", []))
        if not batch:
            break

        computers.extend(batch)

        if len(batch) < PAGE_SIZE:
            break

        page += 1

    return computers

def build_network_interfaces(computer):
    ipv4s = []
    ipv6s = []
    mac = computer.get("macAddress", "")

    ip_value = computer.get("ipAddress", computer.get("ip", ""))
    if ip_value:
        for raw_ip in ip_value.split(","):
            raw_ip = raw_ip.strip()
            if not raw_ip:
                continue
            parsed = ip_address(raw_ip)
            if parsed and parsed.version == 4:
                ipv4s.append(parsed)
            elif parsed and parsed.version == 6:
                ipv6s.append(parsed)

    return [NetworkInterface(
        ipv4Addresses=ipv4s,
        ipv6Addresses=ipv6s,
        macAddress=mac,
    )]

def build_asset(computer):
    asset_id = computer.get("computerId", computer.get("id", ""))
    if not asset_id:
        return None

    hostnames = []
    name = computer.get("computerName", computer.get("computername", ""))
    if name:
        hostnames.append(name)
    fqdn = computer.get("fqdn", "")
    if fqdn and fqdn != name:
        hostnames.append(fqdn)

    if not hostnames:
        return None

    custom_attrs = {}
    agent_version = computer.get("agentVersion", "")
    if agent_version:
        custom_attrs["threatlocker.agentVersion"] = agent_version

    last_user = computer.get("lastLoggedInUser", computer.get("userName", ""))
    if last_user:
        custom_attrs["threatlocker.lastUser"] = last_user

    org_name = computer.get("organizationName", computer.get("groupName", ""))
    if org_name:
        custom_attrs["threatlocker.organization"] = org_name

    serial = computer.get("serialNumber", "")
    if serial:
        custom_attrs["threatlocker.serialNumber"] = serial

    tl_status = computer.get("onlineStatus", computer.get("status", ""))
    if tl_status:
        custom_attrs["threatlocker.status"] = str(tl_status)

    last_checkin = computer.get("lastCheckinDate", computer.get("lastSeen", ""))
    if last_checkin:
        custom_attrs["threatlocker.lastCheckin"] = str(last_checkin)

    manufacturer = computer.get("manufacturer", "")
    if manufacturer:
        custom_attrs["threatlocker.manufacturer"] = manufacturer

    model = computer.get("model", "")
    if model:
        custom_attrs["threatlocker.model"] = model

    domain = computer.get("domain", "")
    if domain:
        custom_attrs["threatlocker.domain"] = domain

    return ImportAsset(
        id=str(asset_id),
        hostnames=hostnames,
        os=computer.get("operatingSystem", computer.get("os", "")),
        osVersion=computer.get("osVersion", ""),
        networkInterfaces=build_network_interfaces(computer),
        customAttributes=custom_attrs,
    )

def main(*args, **kwargs):
    token = kwargs.get('access_secret', '')
    if not token:
        print('ERROR: No API token. Set access_secret to your ThreatLocker Org API token.')
        return None

    base_url = kwargs.get('access_key', '')
    if not base_url:
        base_url = DEFAULT_BASE_URL
    if base_url.endswith('/'):
        base_url = base_url[:-1]

    url = "{}/portalapi/Computer/ComputerGetByAllParameters".format(base_url)

    headers = {
        "Authorization": "Bearer {}".format(token),
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    computers = fetch_computers(url, headers)
    if not computers:
        print("WARNING: No computers returned from ThreatLocker API")
        return []

    assets = []
    for c in computers:
        asset = build_asset(c)
        if asset:
            assets.append(asset)

    print("INFO: Imported {} assets from ThreatLocker".format(len(assets)))
    return assets
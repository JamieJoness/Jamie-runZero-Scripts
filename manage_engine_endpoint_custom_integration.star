load('runzero.types', 'ImportAsset', 'NetworkInterface')
load('json', json_encode='encode', json_decode='decode')
load('net', 'ip_address')
load('http', http_get='get')
load('uuid', 'new_uuid')

EC_HOST         = 'CUSTOMERURL' #replace with customers actual host URL, e.g. 'example.manageengine.com'
API_VERSION     = '1.4'
SCAN_ENDPOINT   = '/api/' + API_VERSION + '/inventory/scancomputers'
PAGE_LIMIT      = 1000

def _is_ms_timestamp(v):
    """Return True if v looks like a 13-digit Unix-millisecond timestamp."""
    s = str(v)
    if len(s) != 13:
        return False
    for i in range(len(s)):
        if s[i] < "0" or s[i] > "9":
            return False
    return True

def _ms_to_utc(ms):
    """Convert Unix milliseconds to 'DD-MM-YYYY HH:MM:SS UTC'."""
    ts = int(ms) // 1000
    hours   = (ts % 86400) // 3600
    minutes = (ts % 3600) // 60
    secs    = ts % 60

    days = ts // 86400
    year = 1970
    while True:
        leap = (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0))
        diy  = 366 if leap else 365
        if days < diy:
            break
        days -= diy
        year += 1

    leap = (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0))
    mdays = [31, 29 if leap else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    month = 1
    for md in mdays:
        if days < md:
            break
        days -= md
        month += 1
    day = days + 1

    return "%02d-%02d-%04d %02d:%02d:%02d UTC" % (day, month, year, hours, minutes, secs)

def _normalize_mac(mac):
    if not mac:
        return mac
    return mac.lower().replace('-', ':')

def build_network_interfaces(device):
    ip_field = device.get('ip_address') or ''
    mac       = _normalize_mac(device.get('mac_address'))
    # support comma-separated IPs if ever present
    ips = [p.strip() for p in ip_field.split(',') if p.strip()]
    ipv4s = []
    ipv6s = []
    for ip in ips:
        addr = ip_address(ip)
        if addr:
            if addr.version == 4:
                ipv4s.append(addr)
            else:
                ipv6s.append(addr)
    return [ NetworkInterface(macAddress=mac,
                              ipv4Addresses=ipv4s,
                              ipv6Addresses=ipv6s) ]

# Fields promoted to first-class ImportAsset attributes (excluded from customAttributes)
_SKIP_FIELDS = ('resource_id','id','resource_name','ip_address','mac_address',
                'os_name','os_version','os_platform','device_type')

def build_assets(devices):
    assets = []
    for d in devices:
        asset_id = str(d.get('resource_id', d.get('id', new_uuid())))
        hostname = d.get('resource_name') or ''
        # build networkInterfaces
        net_ifaces = build_network_interfaces(d)

        # map OS fields
        os_name    = d.get('os_name') or d.get('os_platform') or ''
        os_version = d.get('os_version') or ''

        # map device type
        device_type = d.get('device_type') or ''

        # everything else goes into customAttributes (truncate to 1023 chars)
        custom = {}
        for k, v in d.items():
            if k in _SKIP_FIELDS:
                continue
            val = _ms_to_utc(v) if _is_ms_timestamp(v) else str(v)
            custom[k] = val[:1023]

        asset_kwargs = dict(
            id=asset_id,
            hostnames=[hostname] if hostname else [],
            networkInterfaces=net_ifaces,
            customAttributes=custom,
        )
        if os_name:
            asset_kwargs['os'] = os_name
        if os_version:
            asset_kwargs['osVersion'] = os_version
        if device_type:
            asset_kwargs['deviceType'] = device_type

        assets.append(ImportAsset(**asset_kwargs))
    return assets

def main(**kwargs):
    # access_secret is your auth_token
    token = kwargs['access_secret']
    headers = {
        'Authorization': token,
        'Accept':        'application/json',
    }

    page        = 1
    all_devices = []
    while True:
        url = 'https://' + EC_HOST + SCAN_ENDPOINT
        params = {"pagelimit": PAGE_LIMIT, "page": page}
        resp = http_get(url, headers=headers, params=params, timeout=3600)
        if resp.status_code != 200:
            print('Scan API error on page %d: %d %s' % (page, resp.status_code, resp.body))
            break

        body    = json_decode(resp.body)
        msg     = body.get('message_response', {})
        devices = msg.get('scancomputers', [])
        if not devices:
            break

        all_devices.extend(devices)
        if len(devices) < PAGE_LIMIT:
            break
        page += 1

    if not all_devices:
        print('No devices returned')
        return None

    return build_assets(all_devices)
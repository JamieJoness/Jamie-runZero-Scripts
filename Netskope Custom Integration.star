load('runzero.types', 'ImportAsset', 'NetworkInterface')
load('json', json_encode='encode', json_decode='decode')
load('net', 'ip_address')
load('http', http_get='get', 'url_encode')
load('uuid', 'new_uuid')

NETSKOPE_API_URL = 'https://salvadorcaetano.goskope.com/api'
NETSKOPE_API_GROUPBYS = 'nsdeviceuid'

# Add any additional Netskope fields you want returned via the "fields=" param here.
NETSKOPE_API_ATTRIBUTES = [
    'client_version',  # added so clientVersion can populate
    'deleted',
    'device_classification_status',
    'device_id',
    'device_make',
    'device_model',
    'groups',
    'hostname',
    'mac_addresses',
    'nsdeviceuid',
    'ns_tenant_id',
    'organization_unit',
    'os',
    'os_version',
    'serial_number',
    'steering_config',
    'timestamp',
    'ur_normalized',
    'user',
    'userkey',
    'usergroup',
    'user_added_time',
    'user_status',
]

def to_str(v):
    if v == None:
        return ''
    t = type(v)
    if t == 'list' or t == 'dict':
        return json_encode(v)
    return '{}'.format(v)

def safe_list(v):
    if v == None:
        return []
    if type(v) == 'list':
        return v
    if type(v) == 'string' and v != '':
        return [v]
    return []

def build_network_interface(ips, mac):
    ip4s = []
    ip6s = []
    for ip in ips[:99]:
        ip_addr = ip_address(ip)
        if ip_addr.version == 4:
            ip4s.append(ip_addr)
        elif ip_addr.version == 6:
            ip6s.append(ip_addr)

    if not mac:
        return NetworkInterface(ipv4Addresses=ip4s, ipv6Addresses=ip6s)

    return NetworkInterface(macAddress=mac, ipv4Addresses=ip4s, ipv6Addresses=ip6s)

def get_assets(token):
    page_offset = 0
    page_limit = 10000  # safer than 20000; increase later if you confirm your tenant supports it
    assets_all = []

    fields = ','.join(NETSKOPE_API_ATTRIBUTES)
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + token,
    }

    while True:
        params = {
            'groupbys': NETSKOPE_API_GROUPBYS,
            'fields': fields,
            'offset': page_offset,
            'limit': page_limit,
        }
        url = NETSKOPE_API_URL + '/v2/events/datasearch/clientstatus?' + url_encode(params)

        response = http_get(url, headers=headers, timeout=300)

        if not response or response.status_code != 200:
            print('failed to retrieve assets', response.status_code if response else 'no response')
            if response:
                print('response body:', response.body)
            return None

        body = json_decode(response.body)
        assets = body.get('result', [])

        if not assets:
            break

        assets_all.extend(assets)
        print('retrieved:', len(assets), 'total:', len(assets_all))

        if len(assets) < page_limit:
            break

        page_offset = page_offset + page_limit

    return assets_all

def build_assets(assets_json):
    imported_assets = []

    for item in assets_json:
        # Determine a stable asset id
        nsdeviceuid = item.get('_id', {}).get('nsdeviceuid', '')
        device_id = to_str(item.get('device_id', ''))
        serial_number = to_str(item.get('serial_number', ''))

        asset_id = to_str(nsdeviceuid)
        if asset_id == '':
            asset_id = device_id
        if asset_id == '':
            asset_id = serial_number
        if asset_id == '':
            asset_id = new_uuid()  # important: call new_uuid()

        # Hostname (avoid importing an empty string)
        hn = to_str(item.get('hostname', ''))
        hostnames = [hn] if hn != '' else []

        # OS normalization
        os_name = to_str(item.get('os', ''))
        os_version = to_str(item.get('os_version', ''))
        if 'Mac' in os_name:
            os = 'macOS'
        else:
            os = os_name

        # Network interfaces
        ips = []
        networks = []
        macs = safe_list(item.get('mac_addresses', []))
        if macs:
            for m in macs:
                networks.append(build_network_interface(ips=ips, mac=to_str(m)))
        else:
            # If no MAC and no IP, omit networkInterfaces entirely
            networks = []

        imported_assets.append(
            ImportAsset(
                id=asset_id,
                hostnames=hostnames,
                networkInterfaces=networks,
                os=os,
                osVersion=os_version,
                manufacturer=to_str(item.get('device_make', '')),
                model=to_str(item.get('device_model', '')),
                customAttributes={
                    # Netskope client version (now requested via fields)
                    'netskope.clientVersion': to_str(item.get('client_version', '')),

                    # Identifiers and metadata
                    'netskope.deviceId': device_id,
                    'netskope.nsdeviceuid': to_str(nsdeviceuid),
                    'netskope.serialNumber': serial_number,
                    'netskope.deleted': to_str(item.get('deleted', '')),
                    'netskope.groups': to_str(item.get('groups', [])),
                    'netskope.ns_tenant_id': to_str(item.get('ns_tenant_id', '')),
                    'netskope.organization_unit': to_str(item.get('organization_unit', '')),
                    'netskope.steering_config': to_str(item.get('steering_config', '')),
                    'netskope.timestamp': to_str(item.get('timestamp', '')),
                    'netskope.device_classification_status': to_str(item.get('device_classification_status', '')),

                    # User fields (match what you request from the endpoint)
                    'netskope.user': to_str(item.get('user', '')),
                    'netskope.userkey': to_str(item.get('userkey', '')),
                    'netskope.usergroup': to_str(item.get('usergroup', [])),
                    'netskope.user_added_time': to_str(item.get('user_added_time', '')),
                    'netskope.user_status': to_str(item.get('user_status', '')),
                    'netskope.ur_normalized': to_str(item.get('ur_normalized', '')),
                }
            )
        )

    return imported_assets

def main(**kwargs):
    token = kwargs.get('access_secret', '')
    if token == '':
        print('missing access_secret')
        return None

    # Safe debug (does not expose the token)
    print('token length:', len(token), 'suffix:', token[-4:] if len(token) >= 4 else token)

    assets = get_assets(token)
    if not assets:
        print('failed to retrieve assets')
        return None

    imported_assets = build_assets(assets)
    print('import assets:', len(imported_assets))
    return imported_assets
load('runzero.types', 'ImportAsset', 'NetworkInterface')
load('json', json_encode='encode', json_decode='decode')
load('http', http_get='get', http_post='post')
load('net', 'ip_address')
load('time', 'parse_time')

CLOUDFLARE_BASE_URL = 'https://api.cloudflare.com/client/v4'
PAGE_LIMIT = 100


def main(**kwargs):
    # Entry point for runzero custom integration — returns list of ImportAsset
    cfg = load_config(kwargs)
    if not cfg or not cfg.get('token') or not cfg.get('account_id'):
        return []
    headers = create_session(cfg)

    phys_url = CLOUDFLARE_BASE_URL + '/accounts/{}/devices/physical-devices?include=last_seen_registration.policy'.format(cfg['account_id'])
    physical_devices = _fetch_paginated(phys_url, headers, 'physical-devices')
    physical_devices = collapse_devices_latest(physical_devices)

    reg_url = CLOUDFLARE_BASE_URL + '/accounts/{}/devices/registrations'.format(cfg['account_id'])
    reg_items = _fetch_paginated(reg_url, headers, 'registrations')

    reg_lookup = build_registration_lookup(reg_items)

    enriched = enrich_devices_with_registrations(physical_devices, reg_lookup)
    assets = map_devices_to_assets(enriched)

    print('Created {} assets from Cloudflare devices.'.format(len(assets)))
    return assets


def load_config(kwargs):
    # Extract required configuration from kwargs
    token = kwargs.get('access_secret') or ''
    account_id = kwargs.get('access_key') or ''
    if not token:
        print('Cloudflare API token (access_secret) not provided.')
        return {}
    if not account_id:
        print('Cloudflare account_id (access_key) not provided.')
        return {}
    return {'token': token, 'account_id': account_id}


def create_session(cfg):
    return {
        'Authorization': 'Bearer {}'.format(cfg['token']),
        'Accept': 'application/json'
    }


def enrich_devices_with_registrations(devices, reg_lookup):
    # devices: list of device dicts
    out = []
    for d in devices:
        if type(d) != 'dict':
            continue
        # Clone shallow
        dev = d
        dev_id = dev.get('id')
        if dev_id and str(dev_id) in reg_lookup:
            reg = reg_lookup[str(dev_id)]
            # attach registration metadata into device under _registration
            dev['_registration'] = reg
        out.append(dev)
    return out


def map_devices_to_assets(devices):
    assets = []
    for device in devices:
        if type(device) != 'dict':
            continue

        asset_id = device.get('id')
        hostname = device.get('name')

        mac = device.get('mac_address')

        # Collect IPs from registration data if available
        ips = []
        reg = device.get('_registration')
        ttype = ''
        if reg and type(reg) == 'dict':
            ttype = reg.get('ttype') or ''
            reg_ips = reg.get('ips')
            if reg_ips and type(reg_ips) == 'list':
                for ip in reg_ips:
                    if ip:
                        ips.append(ip)
            reg_ip = reg.get('ip')
            if reg_ip and reg_ip not in ips:
                ips.append(reg_ip)

        # Only include networkInterfaces when we have IPs to avoid
        # overwriting existing network/service data on the asset.
        net_ifaces = []
        if ips:
            network = build_net_int(ips, mac)
            if network:
                net_ifaces.append(network)

        profile = _nested_get(device, ['last_seen_registration', 'policy', 'name'])
        recent_user = _nested_get(device, ['last_seen_user', 'name'])
        version = device.get('client_version')
        serial_number = device.get('serial_number')
        last_seen = parse_time(device.get('last_seen_at'))

        custom_attrs = {
            'id': asset_id,
            'warp.profile': profile,
            'warp.version': version,
            'lastLoggedInUserName': recent_user,
            'warp.tunnel_type': ttype,
            'lastSeenTS': last_seen.unix,
            'serial_number': serial_number,
        }

        # Preserve MAC as a custom attribute when not included in a network interface
        if mac and not net_ifaces:
            custom_attrs['macAddress'] = mac

        asset = ImportAsset(
            id=asset_id,
            hostnames=[hostname] if hostname else [],
            networkInterfaces=net_ifaces,
            model=device.get('model') or '',
            os=device.get('device_type') or '',
            os_version=device.get('os_version') or '',
            customAttributes=custom_attrs,
        )
        assets.append(asset)
    return assets




def _fetch_paginated(base_url, headers, label):
    page = 1
    items = []
    while True:
        page_url = base_url + ('&' if '?' in base_url else '?') + 'per_page={}&page={}'.format(PAGE_LIMIT, page)
        print('Querying Cloudflare {} page {}'.format(label, page))
        resp = http_get(url=page_url, headers=headers)
        if resp.status_code != 200:
            print('Failed to query Cloudflare {}. Status code: {} on page {}'.format(label, resp.status_code, page))
            break

        body = json_decode(resp.body)['result']
        if body == None:
            print('Failed to decode Cloudflare {} response as JSON on page {}'.format(label, page))
            break

        page_items = body
        if type(page_items) == 'list':
            for it in page_items:
                items.append(it)
        else:
            page_items = []

        total_pages = None
        cur_page = None
        if type(body) == 'dict' and 'result_info' in body and type(body['result_info']) == 'dict':
            ri = body['result_info']
            total_pages = ri.get('total_pages')
            cur_page = ri.get('page')

        if total_pages and cur_page:
            if cur_page >= total_pages:
                break
        if len(page_items) < PAGE_LIMIT:
            break
        page = page + 1
    return items



def _nested_get(d, path):
    if type(d) != 'dict':
        return None
    cur = d
    idx = 0
    while idx < len(path):
        k = path[idx]
        if type(cur) != 'dict' or k not in cur:
            return None
        cur = cur[k]
        idx = idx + 1
    return cur


def _compare_ts(a, b):
    # Reusable timestamp comparator. Returns 1 if a>b, -1 if a<b, 0 if equal.
    if a == b:
        return 0
    if type(a) == 'int' or type(a) == 'float':
        na = a
        if type(b) == 'int' or type(b) == 'float':
            nb = b
            if na == nb:
                return 0
            return 1 if na > nb else -1
        if type(b) == 'string':
            all_digits = True
            idx = 0
            while idx < len(b):
                ch = b[idx]
                if ch < '0' or ch > '9':
                    all_digits = False
                    break
                idx = idx + 1
            if all_digits:
                nb = float(b)
                if na == nb:
                    return 0
                return 1 if na > nb else -1
    if type(a) == 'string' and type(b) == 'string':
        if len(a) != len(b):
            return 1 if len(a) > len(b) else -1
        if a == b:
            return 0
        return 1 if a > b else -1
    sa = str(a)
    sb = str(b)
    if sa == sb:
        return 0
    return 1 if sa > sb else -1


def build_net_int(ips, mac):
    ip4s = []
    ip6s = []
    for ip in ips[:99]:
        if ip:
            ip_addr = ip_address(ip)
            if ip_addr.version == 4:
                ip4s.append(ip_addr)
            elif ip_addr.version == 6:
                ip6s.append(ip_addr)
    return NetworkInterface(macAddress=mac, ipv4Addresses=ip4s, ipv6Addresses=ip6s)


def collapse_devices_latest(devices):
    latest = {}
    no_key = []
    for d in devices:
        if type(d) != 'dict':
            continue
        host = d.get('name')
        ts = d.get('updated_at')

        if host:
            # normalize hostname for stable dedupe
            if type(host) == 'string':
                key = host.strip().lower()
            else:
                key = str(host)
        else:
            dev_id = d.get('id')
            if dev_id:
                key = 'id:' + (dev_id.strip() if type(dev_id) == 'string' else str(dev_id))
            else:
                no_key.append(d)
                continue

        cur = latest.get(key)
        if not cur:
            latest[key] = {'device': d, 'ts': ts}
            continue

        cur_ts = cur.get('ts')
        if ts and cur_ts:
            if _compare_ts(ts, cur_ts) > 0:
                latest[key] = {'device': d, 'ts': ts}
        elif ts and not cur_ts:
            latest[key] = {'device': d, 'ts': ts}

    out = []
    for k, v in latest.items():
        out.append(v['device'])
    for n in no_key:
        out.append(n)
    return out


def build_registration_lookup(reg_items):
    latest_by_id = {}

    for r in reg_items:
        if type(r) != 'dict':
            continue
        # Canonical mapping: registration.device.id
        dev_id = _nested_get(r, ['device','id'])
        if not dev_id:
            continue
        ttype = r.get('tunnel_type') or ''

        # Extract IP addresses from registration for network interface enrichment
        reg_ips = []
        r_ip = r.get('ip')
        if r_ip:
            reg_ips.append(r_ip)
        r_ips = r.get('ips')
        if r_ips and type(r_ips) == 'list':
            for rip in r_ips:
                if rip and rip not in reg_ips:
                    reg_ips.append(rip)

        ts = parse_time(r.get('updated_at'))

        def _maybe_set(key, value, ips):
            if not key:
                return
            s_key = str(key)
            cur = latest_by_id.get(s_key)
            if not cur or (ts and cur['ts'] and ts and _compare_ts(ts, cur['ts']) > 0) or (ts and not cur['ts']):
                latest_by_id[s_key] = {'ts': ts, 'ttype': value, 'ips': ips}

        _maybe_set(dev_id, ttype, reg_ips)
        _maybe_set(str(dev_id), ttype, reg_ips)

    reg_lookup = {}
    for k, v in latest_by_id.items():
        entry = {}
        if v.get('ttype'):
            entry['ttype'] = v['ttype']
        if v.get('ips'):
            entry['ips'] = v['ips']
        if entry:
            reg_lookup[k] = entry

    print ("Built registration lookup for devices: {}".format(reg_lookup))
    return reg_lookup
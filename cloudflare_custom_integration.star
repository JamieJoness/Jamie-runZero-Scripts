load('runzero.types', 'ImportAsset', 'NetworkInterface')
load('json', json_encode='encode', json_decode='decode')
load('http', http_get='get', http_post='post')
load('net', 'ip_address')
load('time', 'parse_time')

CLOUDFLARE_BASE_URL = 'https://api.cloudflare.com/client/v4'
PAGE_LIMIT = 100

# Cloudflare WARP `registration.ip` is the *egress* (public) IP that the WARP
# client appeared from — NOT the device's LAN IP. Importing it as a device
# address causes every WARP client behind the same NAT (and any unrelated
# asset that already owns that public IP) to merge together. Default OFF.
# Set INCLUDE_REGISTRATION_IP=True only if you fully understand the merge
# implications for your environment.
INCLUDE_REGISTRATION_IP = False

# Skip devices Cloudflare has marked as deleted so they age out of runZero
# rather than being repeatedly re-imported as stale assets.
SKIP_DELETED_DEVICES = True

# Map Cloudflare's `device_type` to a runZero-friendly OS family string.
_OS_FAMILY_MAP = {
    'mac': 'macOS',
    'macos': 'macOS',
    'darwin': 'macOS',
    'windows': 'Windows',
    'win': 'Windows',
    'linux': 'Linux',
    'ios': 'iOS',
    'ipados': 'iPadOS',
    'android': 'Android',
    'chromeos': 'ChromeOS',
    'chrome_os': 'ChromeOS',
}


def main(**kwargs):
    # Entry point for runzero custom integration — returns list of ImportAsset
    cfg = load_config(kwargs)
    if not cfg or not cfg.get('token') or not cfg.get('account_id'):
        print('ABORT: missing/invalid config (token or account_id).')
        return []
    headers = create_session(cfg)
    print('Cloudflare account_id (access_key) length={}, token length={}'.format(
        len(cfg['account_id']), len(cfg['token'])))

    phys_url = CLOUDFLARE_BASE_URL + '/accounts/{}/devices/physical-devices?include=last_seen_registration.policy'.format(cfg['account_id'])
    physical_devices = _fetch_paginated(phys_url, headers, 'physical-devices')
    print('Fetched {} raw physical-devices from Cloudflare.'.format(len(physical_devices)))
    physical_devices = collapse_devices_latest(physical_devices)
    print('After collapse_devices_latest: {} unique devices.'.format(len(physical_devices)))

    reg_url = CLOUDFLARE_BASE_URL + '/accounts/{}/devices/registrations?include=policy'.format(cfg['account_id'])
    reg_items = _fetch_paginated(reg_url, headers, 'registrations')
    print('Fetched {} registration entries from Cloudflare.'.format(len(reg_items)))

    reg_lookup = build_registration_lookup(reg_items)

    enriched = enrich_devices_with_registrations(physical_devices, reg_lookup)
    print('Enriched device count: {}'.format(len(enriched)))
    assets = map_devices_to_assets(enriched)

    print('Created {} assets from Cloudflare devices.'.format(len(assets)))
    if len(assets) == 0:
        print('WARNING: returning 0 assets. Check earlier logs for HTTP status / response body to determine which endpoint failed or returned empty data. Common causes: (1) API token missing Zero Trust "Access: Devices Read" permission, (2) wrong account_id, (3) no enrolled devices for this account, (4) device records missing an `id` field used as ImportAsset.id.')
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
    skipped_no_id = 0
    skipped_deleted = 0
    for device in devices:
        if type(device) != 'dict':
            continue

        # Optionally skip devices Cloudflare has flagged as deleted so they
        # don't keep reappearing on every sync.
        if SKIP_DELETED_DEVICES and device.get('deleted') == True:
            skipped_deleted = skipped_deleted + 1
            continue

        # ImportAsset.id MUST be a non-empty string or runZero silently drops the asset.
        raw_id = device.get('id') or device.get('serial_number') or device.get('device_id')
        if not raw_id:
            skipped_no_id = skipped_no_id + 1
            continue
        asset_id = str(raw_id)
        raw_hostname = device.get('name')
        hostname = _clean_hostname(raw_hostname)

        mac = _normalize_mac(device.get('mac_address'))

        # Collect IPs from registration data only when explicitly enabled — see
        # comment on INCLUDE_REGISTRATION_IP at the top of this file. The IP
        # Cloudflare reports is the WARP egress (public) IP and is a poor
        # merge key.
        ips = []
        reg = device.get('_registration')
        ttype = ''
        if reg and type(reg) == 'dict':
            ttype = reg.get('ttype') or ''
            if INCLUDE_REGISTRATION_IP:
                reg_ips = reg.get('ips')
                if reg_ips and type(reg_ips) == 'list':
                    for ip in reg_ips:
                        if ip:
                            ips.append(ip)
                reg_ip = reg.get('ip')
                if reg_ip and reg_ip not in ips:
                    ips.append(reg_ip)

        # Only emit a NetworkInterface when we have a MAC or IPs. Doing so when
        # both are empty would attach a useless interface and could degrade
        # merging.
        net_ifaces = []
        if ips or mac:
            network = build_net_int(ips, mac)
            if network:
                net_ifaces.append(network)

        profile = _nested_get(device, ['last_seen_registration', 'policy', 'name'])
        profile_id = _nested_get(device, ['last_seen_registration', 'policy', 'id'])
        profile_default = _nested_get(device, ['last_seen_registration', 'policy', 'default'])
        profile_deleted = _nested_get(device, ['last_seen_registration', 'policy', 'deleted'])
        profile_updated_at = _nested_get(device, ['last_seen_registration', 'policy', 'updated_at'])
        last_reg_id = _nested_get(device, ['last_seen_registration', 'id'])
        last_reg_key_type = _nested_get(device, ['last_seen_registration', 'key_type'])
        last_reg_tunnel = _nested_get(device, ['last_seen_registration', 'tunnel_type'])
        last_reg_seen_at = _nested_get(device, ['last_seen_registration', 'last_seen_at'])
        last_reg_revoked_at = _nested_get(device, ['last_seen_registration', 'revoked_at'])
        last_reg_deleted_at = _nested_get(device, ['last_seen_registration', 'deleted_at'])

        recent_user = _nested_get(device, ['last_seen_user', 'name']) or _nested_get(device, ['user', 'name'])
        user_email = _nested_get(device, ['last_seen_user', 'email']) or _nested_get(device, ['user', 'email'])
        user_id = _nested_get(device, ['last_seen_user', 'id']) or _nested_get(device, ['user', 'id'])

        version = device.get('client_version') or device.get('version')
        serial_number = device.get('serial_number')
        manufacturer = device.get('manufacturer')
        os_distro_name = device.get('os_distro_name')
        os_distro_revision = device.get('os_distro_revision')
        os_version_extra = device.get('os_version_extra')
        device_key = device.get('key')
        created_at = device.get('created') or device.get('created_at')
        updated_at = device.get('updated') or device.get('updated_at')
        revoked_at = device.get('revoked_at')
        deleted_flag = device.get('deleted')

        last_seen_raw = device.get('last_seen_at') or device.get('last_seen')
        last_seen_ts = ''
        if last_seen_raw:
            last_seen_parsed = parse_time(last_seen_raw)
            if last_seen_parsed:
                last_seen_ts = last_seen_parsed.unix

        # Pull additional registration-derived fields when a registration was matched
        reg_user_name = ''
        reg_user_email = ''
        reg_user_id = ''
        reg_key_type = ''
        reg_id = ''
        reg_created_at = ''
        reg_last_seen_at = ''
        reg_updated_at = ''
        reg_revoked_at = ''
        reg_deleted_at = ''
        reg_policy_id = ''
        reg_policy_name = ''
        reg_policy_default = ''
        reg_policy_deleted = ''
        if reg and type(reg) == 'dict':
            reg_user_name = reg.get('user_name') or ''
            reg_user_email = reg.get('user_email') or ''
            reg_user_id = reg.get('user_id') or ''
            reg_key_type = reg.get('key_type') or ''
            reg_id = reg.get('id') or ''
            reg_created_at = reg.get('created_at') or ''
            reg_last_seen_at = reg.get('last_seen_at') or ''
            reg_updated_at = reg.get('updated_at') or ''
            reg_revoked_at = reg.get('revoked_at') or ''
            reg_deleted_at = reg.get('deleted_at') or ''
            reg_policy_id = reg.get('policy_id') or ''
            reg_policy_name = reg.get('policy_name') or ''
            reg_policy_default = reg.get('policy_default')
            reg_policy_deleted = reg.get('policy_deleted')

        custom_attrs = {
            'id': asset_id,
            'serial_number': serial_number,
            'manufacturer': manufacturer,
            'model': device.get('model'),
            'device_type': device.get('device_type'),
            'os_distro_name': os_distro_name,
            'os_distro_revision': os_distro_revision,
            'os_version_extra': os_version_extra,
            'cloudflare.device_id': device.get('id'),
            'cloudflare.device_key': device_key,
            'cloudflare.created_at': created_at,
            'cloudflare.updated_at': updated_at,
            'cloudflare.revoked_at': revoked_at,
            'cloudflare.deleted': deleted_flag,
            'lastLoggedInUserName': recent_user,
            'lastLoggedInUserEmail': user_email,
            'lastLoggedInUserID': user_id,
            'lastSeenTS': last_seen_ts,
            'lastSeenAt': last_seen_raw,
            'warp.version': version,
            'warp.profile': profile,
            'warp.profile_id': profile_id,
            'warp.profile_default': profile_default,
            'warp.profile_deleted': profile_deleted,
            'warp.profile_updated_at': profile_updated_at,
            'warp.tunnel_type': ttype or last_reg_tunnel,
            'warp.key_type': last_reg_key_type or reg_key_type,
            'warp.last_registration_id': last_reg_id or reg_id,
            'warp.last_registration_seen_at': last_reg_seen_at or reg_last_seen_at,
            'warp.last_registration_revoked_at': last_reg_revoked_at or reg_revoked_at,
            'warp.last_registration_deleted_at': last_reg_deleted_at or reg_deleted_at,
            'warp.registration.created_at': reg_created_at,
            'warp.registration.updated_at': reg_updated_at,
            'warp.registration.user_name': reg_user_name,
            'warp.registration.user_email': reg_user_email,
            'warp.registration.user_id': reg_user_id,
            'warp.registration.policy_id': reg_policy_id,
            'warp.registration.policy_name': reg_policy_name,
            'warp.registration.policy_default': reg_policy_default,
            'warp.registration.policy_deleted': reg_policy_deleted,
        }
        # Drop None / empty-string values — runZero customAttributes must be non-empty strings.
        clean_attrs = {}
        for k, v in custom_attrs.items():
            if v == None:
                continue
            sv = str(v) if type(v) != 'string' else v
            if sv == '':
                continue
            clean_attrs[k] = sv
        custom_attrs = clean_attrs

        # Preserve MAC as a custom attribute when not included in a network interface
        if mac and not net_ifaces:
            custom_attrs['macAddress'] = mac

        # Domain hint from the user's email — small but useful merge key.
        domain = ''
        if user_email and type(user_email) == 'string' and '@' in user_email:
            domain = user_email.split('@')[-1]

        # Compose a richer OS version string when extra detail is available.
        os_family = _map_os_family(device.get('device_type'))
        os_ver = device.get('os_version') or ''
        if os_version_extra and os_version_extra not in (os_ver or ''):
            if os_ver:
                os_ver = '{} ({})'.format(os_ver, os_version_extra)
            else:
                os_ver = os_version_extra
        # For Linux, prepend the distro name when present.
        if os_family == 'Linux' and os_distro_name:
            distro_part = os_distro_name
            if os_distro_revision:
                distro_part = '{} {}'.format(os_distro_name, os_distro_revision)
            if distro_part not in (os_ver or ''):
                os_ver = '{} ({})'.format(os_ver, distro_part) if os_ver else distro_part

        # Tags make it trivial for the customer to filter / build queries on
        # WARP-sourced assets.
        tags = ['cloudflare-warp']
        if os_family:
            tags.append('os:{}'.format(os_family.lower()))
        if revoked_at:
            tags.append('warp-revoked')
        if ttype or last_reg_tunnel:
            tags.append('tunnel:{}'.format(ttype or last_reg_tunnel))

        asset = ImportAsset(
            id=asset_id,
            hostnames=[hostname] if hostname else [],
            domain=domain,
            networkInterfaces=net_ifaces,
            manufacturer=manufacturer or '',
            model=device.get('model') or '',
            os=os_family,
            osVersion=os_ver,
            deviceType=device.get('device_type') or '',
            customAttributes=custom_attrs,
            tags=tags,
        )
        assets.append(asset)
    if skipped_no_id > 0:
        print('WARNING: skipped {} Cloudflare device(s) because they had no usable id/serial_number — these would never be imported. Sample first device keys: {}'.format(
            skipped_no_id,
            list(devices[0].keys()) if len(devices) > 0 and type(devices[0]) == 'dict' else 'n/a'))
    if skipped_deleted > 0:
        print('Skipped {} device(s) flagged deleted=true (SKIP_DELETED_DEVICES=True).'.format(skipped_deleted))
    return assets




def _fetch_paginated(base_url, headers, label):
    # Cloudflare Zero Trust device endpoints use cursor-based pagination.
    # `page=N` is rejected with HTTP 400 ("invalid query parameter 'page'").
    # We pass `per_page` and follow `result_info.cursor` (or `result_info.cursors.after`)
    # until it is empty / absent or a page returns fewer than PAGE_LIMIT items.
    cursor = ''
    page_num = 0
    items = []
    # safety cap to prevent runaway loops if the API misbehaves
    max_pages = 1000
    while True:
        page_num = page_num + 1
        if page_num > max_pages:
            print('WARN: Cloudflare {} hit max_pages={} safety cap; stopping pagination.'.format(label, max_pages))
            break
        sep = '&' if '?' in base_url else '?'
        page_url = base_url + sep + 'per_page={}'.format(PAGE_LIMIT)
        if cursor:
            page_url = page_url + '&cursor=' + cursor
        print('Querying Cloudflare {} page {} -> {}'.format(label, page_num, page_url))
        resp = http_get(url=page_url, headers=headers)
        if resp.status_code != 200:
            # Show the response body (truncated) so auth/permission/path issues are visible.
            body_str = ''
            if resp.body:
                body_str = str(resp.body)
                if len(body_str) > 1000:
                    body_str = body_str[:1000] + '...(truncated)'
            print('ERROR querying Cloudflare {} page {}: HTTP {} body={}'.format(
                label, page_num, resp.status_code, body_str))
            break

        parsed = json_decode(resp.body)
        if parsed == None or type(parsed) != 'dict':
            print('ERROR: could not decode Cloudflare {} JSON on page {}. Raw (first 500): {}'.format(
                label, page_num, str(resp.body)[:500]))
            break

        # Surface API-level failures even when HTTP 200.
        if 'success' in parsed and parsed.get('success') == False:
            print('ERROR: Cloudflare {} page {} returned success=false. errors={} messages={}'.format(
                label, page_num, parsed.get('errors'), parsed.get('messages')))
            break

        result = parsed.get('result')
        if result == None:
            print('WARN: Cloudflare {} page {} has no "result" key. Top-level keys: {}'.format(
                label, page_num, list(parsed.keys()) if type(parsed) == 'dict' else 'n/a'))
            break

        page_items = []
        if type(result) == 'list':
            page_items = result
        elif type(result) == 'dict':
            # Some Cloudflare endpoints wrap arrays inside the result dict.
            for k, v in result.items():
                if type(v) == 'list':
                    print('NOTE: Cloudflare {} returned dict result; using inner list under key "{}" (len={}).'.format(label, k, len(v)))
                    page_items = v
                    break
            if not page_items:
                print('WARN: Cloudflare {} page {} result is a dict with no inner list. Keys: {}'.format(
                    label, page_num, list(result.keys())))
        else:
            print('WARN: Cloudflare {} page {} result has unexpected type: {}'.format(label, page_num, type(result)))

        for it in page_items:
            items.append(it)

        # Cursor pagination: result_info.cursor (or result_info.cursors.after) on the top-level response.
        next_cursor = ''
        ri = parsed.get('result_info')
        if ri and type(ri) == 'dict':
            c = ri.get('cursor')
            if c:
                next_cursor = c
            else:
                cursors = ri.get('cursors')
                if cursors and type(cursors) == 'dict':
                    after = cursors.get('after')
                    if after:
                        next_cursor = after

        print('Cloudflare {} page {}: got {} items (next_cursor={!r}, running_total={})'.format(
            label, page_num, len(page_items), next_cursor, len(items)))

        if not next_cursor:
            break
        if len(page_items) < PAGE_LIMIT:
            break
        cursor = next_cursor
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


def _normalize_mac(mac):
    # runZero expects colon-separated lowercase MACs (aa:bb:cc:dd:ee:ff).
    # Cloudflare commonly returns dash-separated uppercase (00-00-5E-00-53-00)
    # or no separators. Normalize so MAC works as a merge key.
    if not mac or type(mac) != 'string':
        return ''
    s = mac.strip().lower()
    cleaned = ''
    idx = 0
    while idx < len(s):
        ch = s[idx]
        if (ch >= '0' and ch <= '9') or (ch >= 'a' and ch <= 'f'):
            cleaned = cleaned + ch
        idx = idx + 1
    if len(cleaned) != 12:
        return ''
    parts = []
    i = 0
    while i < 12:
        parts.append(cleaned[i:i+2])
        i = i + 2
    return ':'.join(parts)


def _clean_hostname(name):
    # Keep the WARP-supplied name for display but strip surrounding whitespace.
    # We deliberately do not aggressively rewrite spaces / apostrophes — those
    # are the user-visible WARP device names and changing them would be
    # surprising. runZero will still merge primarily on id/MAC.
    if not name or type(name) != 'string':
        return ''
    return name.strip()


def _map_os_family(device_type):
    if not device_type or type(device_type) != 'string':
        return ''
    return _OS_FAMILY_MAP.get(device_type.strip().lower(), device_type)


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
    skipped_no_dev_id = 0

    for r in reg_items:
        if type(r) != 'dict':
            continue
        # Canonical mapping: registration.device.id
        dev_id = _nested_get(r, ['device','id'])
        if not dev_id:
            skipped_no_dev_id = skipped_no_dev_id + 1
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

        ts_raw = r.get('updated_at')
        ts = parse_time(ts_raw) if ts_raw else None

        # Enrichment fields pulled from the registration record itself.
        extra = {
            'id': r.get('id') or '',
            'created_at': r.get('created_at') or '',
            'last_seen_at': r.get('last_seen_at') or '',
            'updated_at': r.get('updated_at') or '',
            'revoked_at': r.get('revoked_at') or '',
            'deleted_at': r.get('deleted_at') or '',
            'key_type': r.get('key_type') or '',
            'user_id': _nested_get(r, ['user', 'id']) or '',
            'user_email': _nested_get(r, ['user', 'email']) or '',
            'user_name': _nested_get(r, ['user', 'name']) or '',
            'policy_id': _nested_get(r, ['policy', 'id']) or '',
            'policy_name': _nested_get(r, ['policy', 'name']) or '',
            'policy_default': _nested_get(r, ['policy', 'default']),
            'policy_deleted': _nested_get(r, ['policy', 'deleted']),
        }

        def _maybe_set(key, value, ips, extra_fields):
            if not key:
                return
            s_key = str(key)
            cur = latest_by_id.get(s_key)
            if not cur or (ts and cur['ts'] and ts and _compare_ts(ts, cur['ts']) > 0) or (ts and not cur['ts']):
                entry = {'ts': ts, 'ttype': value, 'ips': ips}
                for k, v in extra_fields.items():
                    entry[k] = v
                latest_by_id[s_key] = entry

        _maybe_set(dev_id, ttype, reg_ips, extra)
        _maybe_set(str(dev_id), ttype, reg_ips, extra)

    reg_lookup = {}
    for k, v in latest_by_id.items():
        entry = {}
        if v.get('ttype'):
            entry['ttype'] = v['ttype']
        if v.get('ips'):
            entry['ips'] = v['ips']
        # carry through enrichment fields
        for fk in ['id', 'created_at', 'last_seen_at', 'updated_at', 'revoked_at',
                   'deleted_at', 'key_type', 'user_id', 'user_email', 'user_name',
                   'policy_id', 'policy_name', 'policy_default', 'policy_deleted']:
            val = v.get(fk)
            if val == None or val == '':
                continue
            entry[fk] = val
        if entry:
            reg_lookup[k] = entry

    if skipped_no_dev_id > 0:
        print('NOTE: {} registration record(s) had no device.id and were skipped.'.format(skipped_no_dev_id))
    print('Built registration lookup with {} unique device id(s).'.format(len(reg_lookup)))
    return reg_lookup
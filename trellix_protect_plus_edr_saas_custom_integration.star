# =============================================================================
# Trellix Protect Plus EDR for Endpoint SaaS  ->  runZero custom integration (SDK)
# =============================================================================
#
# WHAT THIS DOES
#   Pulls every managed endpoint (and all of the attributes the API exposes for
#   it) from the Trellix ePO Cloud "devices" API and returns them to runZero as
#   ImportAssets so they merge with / enrich your existing runZero inventory.
#
# SAFETY
#   This integration is STRICTLY READ-ONLY. It only performs:
#       * POST to the IAM token endpoint to obtain an OAuth2 access token
#       * GET  /epo/v2/devices (paginated)
#   It never creates, modifies, tags, moves or deletes anything in Trellix, and
#   it cannot run tasks against endpoints. The only writes happen inside runZero
#   (asset import/merge), which is the intended behaviour.
#
# CREDENTIALS (Trellix Developer Portal -> Self Service -> API Access Management)
#   Request a client with the "Devices: GET" capability (IAM scope epo.device.r)
#   and copy the three values it generates:
#       * Client ID
#       * Client Secret
#       * API Key   (sent as the x-api-key header)
#
# HOW TO CONFIGURE IN runZero
#   runZero custom integrations expose two credential fields (Access Key /
#   Secret). Trellix needs three secrets, so pack them like this:
#
#       Access Key (access_key)     =  <Client ID>:<API Key>
#       Secret     (access_secret)  =  <Client Secret>
#
#   The Client ID and API Key are split on the first ":" (neither value contains
#   a colon). If you would rather not pack the API Key into the Access Key, you
#   can instead set Access Key = <Client ID> and paste the API Key into the
#   API_KEY constant below.
#
# REGION
#   The defaults below are the global Trellix endpoints. If your tenant lives in
#   a different region, override TRELLIX_API_BASE_URL / TRELLIX_TOKEN_URL.
# =============================================================================

load('runzero.types', 'ImportAsset', 'NetworkInterface')
load('json', json_encode='encode', json_decode='decode')
load('http', http_get='get', http_post='post')
load('net', 'ip_address')
load('time', 'parse_time')

# ----------------------------- Configuration --------------------------------
TRELLIX_API_BASE_URL = 'https://api.manage.trellix.com'
TRELLIX_TOKEN_URL    = 'https://iam.mcafee-cloud.com/iam/v1.1/token'
TRELLIX_SCOPE        = 'epo.device.r'

# Optional fallback for the x-api-key value. Leave '' if you pack the API key
# into the Access Key field as "<client_id>:<api_key>".
API_KEY = ''

PAGE_LIMIT   = 1000    # devices per page (page[limit]); 1000 is the documented
                       # maximum the Trellix ePO Cloud API supports.
MAX_PAGES    = 5000    # hard safety cap on pagination loops
HTTP_TIMEOUT = 300     # seconds per HTTP request

# Turn each Trellix ePO tag assigned to a device into a runZero tag.
IMPORT_EPO_TAGS = True
MAX_EPO_TAGS    = 50

# Prefix used for every Trellix-sourced custom attribute in runZero.
ATTR_PREFIX = 'trellix.'

# ----------------------------- Constants ------------------------------------
_B64_ALPHABET  = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'
_HEX_CHARS     = '0123456789abcdef'
_TAG_SAFE      = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.'
_IPV4_CHARS    = '0123456789.'
_IPV6_CHARS    = '0123456789abcdefABCDEF:'
_PLACEHOLDER_IPS = ['', '0.0.0.0', '::', 'unknown', 'n/a', 'none', 'null', '0', '127.0.0.1', '::1']


# ----------------------------- Helpers --------------------------------------
def to_str(v):
    # Convert any Starlark value into a safe string. Never returns None.
    if v == None:
        return ''
    t = type(v)
    if t == 'string':
        return v
    if t == 'bool':
        return 'true' if v else 'false'
    if t == 'dict' or t == 'list' or t == 'tuple':
        return json_encode(v)
    return '{}'.format(v)


def b64encode(s):
    # Standard Base64 of a (UTF-8) string. Implemented manually so the
    # integration does not depend on an optional base64 module being present in
    # the sandbox. Validated against Python's base64 for correctness.
    vals = [b for b in bytes(s).elems()]
    out = ''
    n = len(vals)
    for i in range(0, n, 3):
        b0 = vals[i]
        b1 = vals[i + 1] if (i + 1) < n else 0
        b2 = vals[i + 2] if (i + 2) < n else 0
        out += _B64_ALPHABET[b0 >> 2]
        out += _B64_ALPHABET[((b0 & 3) << 4) | (b1 >> 4)]
        if (i + 1) < n:
            out += _B64_ALPHABET[((b1 & 15) << 2) | (b2 >> 6)]
        else:
            out += '='
        if (i + 2) < n:
            out += _B64_ALPHABET[b2 & 63]
        else:
            out += '='
    return out


def _is_hex_string(s):
    if len(s) == 0:
        return False
    for i in range(len(s)):
        if s[i] not in _HEX_CHARS:
            return False
    return True


def _body_preview(b):
    if b == None:
        return ''
    s = str(b)
    if len(s) > 500:
        return s[:500] + '...(truncated)'
    return s


def _keys(d):
    if type(d) != 'dict':
        return ''
    out = []
    for k in d.keys():
        out.append(to_str(k))
    return ', '.join(out)


def normalize_mac(mac_raw):
    # Trellix/ePO usually returns MACs with no separators (e.g. 0050568A1234).
    # runZero matches best on colon-delimited lower-case MACs.
    m = to_str(mac_raw).strip().lower()
    if m == '':
        return ''
    for sep in [':', '-', '.', ' ']:
        m = m.replace(sep, '')
    if len(m) != 12:
        return ''
    if not _is_hex_string(m):
        return ''
    if m == '000000000000' or m == 'ffffffffffff':
        return ''
    parts = []
    for i in range(0, 12, 2):
        parts.append(m[i:i + 2])
    return ':'.join(parts)


def _looks_like_ip(s):
    if '.' in s and ':' not in s:
        for i in range(len(s)):
            if s[i] not in _IPV4_CHARS:
                return False
        return True
    if ':' in s and '.' not in s:
        for i in range(len(s)):
            if s[i] not in _IPV6_CHARS:
                return False
        return True
    return False


def parse_ips(ip_raw):
    # Returns (ipv4_list, ipv6_list) of validated ip_address objects.
    ipv4s = []
    ipv6s = []
    s = to_str(ip_raw).strip()
    if s == '':
        return (ipv4s, ipv6s)
    # Defensive: handle the (unlikely) case of multiple addresses in one field.
    norm = s.replace(';', ',').replace(' ', ',')
    for part in norm.split(','):
        cand = part.strip()
        if cand == '':
            continue
        if cand.lower() in _PLACEHOLDER_IPS:
            continue
        if not _looks_like_ip(cand):
            continue
        addr = ip_address(cand)
        if not addr:
            continue
        if addr.version == 4:
            ipv4s.append(addr)
        elif addr.version == 6:
            ipv6s.append(addr)
    return (ipv4s, ipv6s)


def build_network_interfaces(attrs):
    mac = normalize_mac(attrs.get('macAddress'))
    ipv4s, ipv6s = parse_ips(attrs.get('ipAddress'))
    if mac == '' and len(ipv4s) == 0 and len(ipv6s) == 0:
        return []
    if mac != '':
        return [NetworkInterface(macAddress=mac, ipv4Addresses=ipv4s, ipv6Addresses=ipv6s)]
    return [NetworkInterface(ipv4Addresses=ipv4s, ipv6Addresses=ipv6s)]


def map_os(os_type):
    s = to_str(os_type)
    low = s.lower()
    if 'mac' in low or 'osx' in low or 'os x' in low or 'darwin' in low:
        return 'macOS'
    return s


def map_device_type(attrs):
    portable = to_str(attrs.get('isPortable')).strip().lower()
    if portable in ['true', '1', 'yes', 'y']:
        return 'Laptop'
    return ''


def _sanitize_tag(t):
    s = to_str(t).strip()
    if s == '':
        return ''
    out = ''
    for i in range(len(s)):
        c = s[i]
        if c == ' ':
            out += '-'
        elif c in _TAG_SAFE:
            out += c
        else:
            out += '-'
    if len(out) > 64:
        out = out[:64]
    return out


def split_epo_tags(raw):
    res = []
    s = to_str(raw)
    if s == '':
        return res
    seen = {}
    for part in s.split(','):
        st = _sanitize_tag(part)
        if st == '' or st == '-':
            continue
        if st in seen:
            continue
        seen[st] = True
        res.append(st)
        if len(res) >= MAX_EPO_TAGS:
            break
    return res


def _add_epoch_attr(custom, key_out, raw):
    rs = to_str(raw)
    if rs == '':
        return
    parsed = parse_time(rs)
    if parsed:
        custom[key_out] = to_str(parsed.unix)


# ----------------------------- Auth -----------------------------------------
def load_config(kwargs):
    # Accept explicit values first (in case runZero ever surfaces them as named
    # kwargs), then fall back to the packed access_key / access_secret fields.
    client_id     = to_str(kwargs.get('client_id'))
    client_secret = to_str(kwargs.get('client_secret'))
    api_key       = to_str(kwargs.get('api_key'))

    access_key    = to_str(kwargs.get('access_key'))
    access_secret = to_str(kwargs.get('access_secret'))

    if client_secret == '' and access_secret != '':
        client_secret = access_secret

    if client_id == '' and access_key != '':
        if ':' in access_key:
            idx = access_key.find(':')
            client_id = access_key[:idx]
            packed_api = access_key[idx + 1:]
            if api_key == '':
                api_key = packed_api
        else:
            client_id = access_key

    if api_key == '':
        api_key = API_KEY

    return {
        'client_id': client_id.strip(),
        'client_secret': client_secret.strip(),
        'api_key': api_key.strip(),
    }


def get_token(client_id, client_secret):
    # OAuth2 client-credentials grant. Trellix IAM accepts the client
    # credentials via HTTP Basic auth; grant_type + scope go in the form body.
    if client_id == '' or client_secret == '':
        print('ERROR: missing client_id or client_secret; cannot request token.')
        return ''
    basic = b64encode(client_id + ':' + client_secret)
    headers = {
        'Authorization': 'Basic ' + basic,
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json',
    }
    form = 'grant_type=client_credentials&scope=' + TRELLIX_SCOPE
    resp = http_post(url=TRELLIX_TOKEN_URL, headers=headers, body=bytes(form), timeout=HTTP_TIMEOUT)
    if not resp:
        print('ERROR: no response from Trellix IAM token endpoint.')
        return ''
    if resp.status_code != 200:
        print('ERROR: Trellix IAM token request failed: HTTP {} body={}'.format(
            resp.status_code, _body_preview(resp.body)))
        return ''
    parsed = json_decode(resp.body)
    if type(parsed) != 'dict':
        print('ERROR: could not decode Trellix IAM token response.')
        return ''
    token = to_str(parsed.get('access_token'))
    if token == '':
        print('ERROR: token response missing access_token. Keys: {}'.format(_keys(parsed)))
        return ''
    return token


def _api_headers(token, api_key):
    return {
        'Authorization': 'Bearer ' + token,
        'x-api-key': api_key,
        'Accept': 'application/vnd.api+json',
        'Content-Type': 'application/vnd.api+json',
    }


# ----------------------------- Fetch ----------------------------------------
def fetch_devices(cfg, token):
    all_devices = []
    seen_ids = {}
    offset = 0
    total = None
    refreshed = False
    headers = _api_headers(token, cfg['api_key'])

    for _page in range(MAX_PAGES):
        # page[offset]/page[limit] are JSON:API params; brackets are percent
        # encoded (%5B / %5D) so the query string is unambiguous.
        url = TRELLIX_API_BASE_URL + '/epo/v2/devices?page%5Boffset%5D=' + str(offset) + '&page%5Blimit%5D=' + str(PAGE_LIMIT)
        resp = http_get(url, headers=headers, timeout=HTTP_TIMEOUT)

        # One-time token refresh if the token expired mid-sync.
        if resp and resp.status_code == 401 and not refreshed:
            refreshed = True
            print('Trellix returned 401 at offset {}; refreshing access token and retrying once.'.format(offset))
            nt = get_token(cfg['client_id'], cfg['client_secret'])
            if nt != '':
                headers = _api_headers(nt, cfg['api_key'])
                resp = http_get(url, headers=headers, timeout=HTTP_TIMEOUT)

        if not resp:
            print('ERROR: no HTTP response from Trellix devices endpoint at offset {}.'.format(offset))
            break
        if resp.status_code != 200:
            print('ERROR: Trellix devices request failed: HTTP {} offset={} body={}'.format(
                resp.status_code, offset, _body_preview(resp.body)))
            break

        body = json_decode(resp.body)
        if body == None or type(body) != 'dict':
            print('ERROR: could not decode Trellix devices JSON at offset {}. Raw(first 500): {}'.format(
                offset, _body_preview(resp.body)))
            break

        data = body.get('data')
        if data == None or type(data) != 'list':
            print('WARNING: Trellix devices response had no "data" list at offset {}. Top-level keys: {}'.format(
                offset, _keys(body)))
            break
        if len(data) == 0:
            break

        added_any = False
        for item in data:
            if type(item) != 'dict':
                continue
            iid = to_str(item.get('id'))
            if iid != '' and iid in seen_ids:
                continue
            if iid != '':
                seen_ids[iid] = True
            all_devices.append(item)
            added_any = True

        # Determine total + next-link for stop conditions.
        meta = body.get('meta')
        if type(meta) == 'dict' and meta.get('totalResourceCount') != None:
            total = meta.get('totalResourceCount')
        links = body.get('links')
        if type(links) != 'dict':
            links = body.get('Links')
        has_next = type(links) == 'dict' and to_str(links.get('next')) != ''

        # Advance by the raw page size received (robust if the server clamps
        # page[limit] to a smaller maximum than requested).
        offset += len(data)

        # If a whole page contributed no new device ids the server is repeating
        # pages (or ignoring offset) -> stop to avoid a runaway loop.
        if not added_any:
            break
        if total != None:
            if offset >= total:
                break
        else:
            if not has_next:
                break

    print('Fetched {} device(s) from Trellix ePO.'.format(len(all_devices)))
    return all_devices


# ----------------------------- Mapping --------------------------------------
def build_asset(device):
    if type(device) != 'dict':
        return None

    attrs = device.get('attributes')
    if type(attrs) != 'dict':
        attrs = {}

    # Stable id: JSON:API id lives at the top level of each resource object.
    asset_id = to_str(device.get('id'))
    if asset_id == '':
        asset_id = to_str(attrs.get('agentGuid'))
    if asset_id == '':
        asset_id = to_str(attrs.get('systemSerialNumber'))
    if asset_id == '':
        # No stable identifier -> skip rather than churn duplicate assets.
        return None

    # Hostnames (computer name, DNS host name, and FQDN when we can build one).
    hostnames = []
    cn = to_str(attrs.get('computerName'))
    if cn == '':
        cn = to_str(attrs.get('name'))
    if cn != '':
        hostnames.append(cn)
    ip_host = to_str(attrs.get('ipHostName'))
    if ip_host != '' and ip_host not in hostnames:
        hostnames.append(ip_host)
    domain = to_str(attrs.get('domainName'))
    if cn != '' and '.' in domain:
        fqdn = cn + '.' + domain
        if fqdn not in hostnames:
            hostnames.append(fqdn)

    net_ifaces = build_network_interfaces(attrs)

    # Capture EVERY attribute the endpoint exposes (and any future ones) as
    # namespaced custom attributes; drop empties; cap value length.
    custom = {}
    for k, v in attrs.items():
        sv = to_str(v)
        if sv == '':
            continue
        custom[ATTR_PREFIX + to_str(k)] = sv[:1023]

    custom[ATTR_PREFIX + 'deviceId'] = asset_id
    rtype = to_str(device.get('type'))
    if rtype != '':
        custom[ATTR_PREFIX + 'resourceType'] = rtype

    # Human/queryable epoch companions for the date-time fields.
    _add_epoch_attr(custom, ATTR_PREFIX + 'lastUpdateUnix', attrs.get('lastUpdate'))
    _add_epoch_attr(custom, ATTR_PREFIX + 'nodeCreatedDateUnix', attrs.get('nodeCreatedDate'))
    _add_epoch_attr(custom, ATTR_PREFIX + 'systemBootTimeUnix', attrs.get('systemBootTime'))

    # Tags.
    tags = ['trellix-edr']
    if IMPORT_EPO_TAGS:
        for t in split_epo_tags(attrs.get('tags')):
            if t not in tags:
                tags.append(t)

    return ImportAsset(
        id=asset_id,
        hostnames=hostnames,
        domain=domain,
        networkInterfaces=net_ifaces,
        os=map_os(attrs.get('osType')),
        osVersion=to_str(attrs.get('osVersion')),
        manufacturer=to_str(attrs.get('systemManufacturer')),
        model=to_str(attrs.get('systemModel')),
        deviceType=map_device_type(attrs),
        customAttributes=custom,
        tags=tags,
    )


def build_assets(devices):
    assets = []
    skipped = 0
    for d in devices:
        a = build_asset(d)
        if a == None:
            skipped += 1
            continue
        assets.append(a)
    if skipped > 0:
        print('Note: skipped {} device(s) with no stable identifier (id/agentGuid/serial).'.format(skipped))
    return assets


# ----------------------------- Entry point ----------------------------------
def main(*args, **kwargs):
    cfg = load_config(kwargs)

    if cfg['client_id'] == '' or cfg['client_secret'] == '':
        print('ABORT: missing Trellix client_id/client_secret. Set runZero Access Key = "<client_id>:<api_key>" and Secret = "<client_secret>".')
        return None
    if cfg['api_key'] == '':
        print('ABORT: missing Trellix API key (x-api-key). Pack it into the Access Key as "<client_id>:<api_key>" or set the API_KEY constant.')
        return None

    print('Trellix config OK: client_id len={}, client_secret len={}, api_key len={}'.format(
        len(cfg['client_id']), len(cfg['client_secret']), len(cfg['api_key'])))

    token = get_token(cfg['client_id'], cfg['client_secret'])
    if token == '':
        print('ABORT: failed to obtain Trellix access token (check client_id/secret and scope epo.device.r).')
        return None

    devices = fetch_devices(cfg, token)
    if len(devices) == 0:
        print('WARNING: 0 devices returned. Verify the API client has scope epo.device.r, the API key is correct, and devices exist in the tenant.')
        return []

    assets = build_assets(devices)
    print('Imported {} runZero asset(s) from {} Trellix device(s).'.format(len(assets), len(devices)))
    return assets

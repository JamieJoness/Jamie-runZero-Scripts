# This is a runZero Custom Integration, please see https://github.com/runZeroInc/runzero-custom-integrations for details.
# This script was generated with AI.

CONFIG = {
    "id": "runzero-manageengine-endpoint-central",
    "name": "ManageEngine Endpoint Central",
    "type": "inbound",
    "description": "Imports endpoints from ManageEngine Endpoint Central.",
    "version": "1",
    "maturity": "beta",
    "minVersion": "5.1.260818.0",
    # The repo-wide record target for a bounded walk: no integration should
    # import more than ten million records in one run, so the page ceiling is
    # that target divided by the 1000-row page size.
    "maxPages": 10000,
    "params": [
        {
            "key": "url",
            "label": "Endpoint Central URL",
            "type": "url",
            "required": True,
            "placeholder": "https://ec.example.com",
        },
        {
            # The key stays "oauth_token" so existing credentials keep working,
            # but the value is the ON-PREM auth token, sent as a bare
            # Authorization header. It is NOT a Zoho OAuth token: Endpoint
            # Central Cloud requires "Authorization: Zoho-oauthtoken" and a
            # full OAuth exchange this script does not perform.
            "key": "oauth_token",
            "label": "API key or auth token (on-prem)",
            "type": "secret",
            "required": True,
            "description": "On-premises Endpoint Central API key (Admin > Integrations > API Key Management; recommended, never expires by default) or a /api/1.4/desktop/authentication auth_token (a session token that expires). Sent verbatim as the Authorization header, with no prefix. Not a Zoho OAuth token; Endpoint Central Cloud is not supported and will answer 401.",
        },
        {
            # Endpoint Central's on-prem API exposes no installation or server
            # identifier anywhere this script can reach cheaply -- scancomputers
            # rows carry only per-resource fields and the metadata block only
            # paging totals -- so the scope has to be operator-chosen. The
            # empty default keeps the legacy unscoped ids.
            "key": "server_scope",
            "label": "Server scope",
            "type": "string",
            "required": False,
            "default": "",
            "placeholder": "ec-hq",
            "description": "Optional label naming this Endpoint Central server, e.g. ec-hq. When set, foreign ids become manage-engine-endpoint-central:<server_scope>:<resource_id>, so two servers imported into one runZero organization cannot collide on their small integer resource ids. Leave empty to keep the legacy unscoped ids. Setting or changing it re-identifies this integration's existing assets once; they re-merge via MAC/IP/hostname or age out.",
        },
    ],
    "includes": {
        "tls_": OPTIONS_TLS,
        "http_": OPTIONS_HTTP,
    },
}
load('runzero.types', 'ImportAsset', 'to_custom_attributes')
load('net', 'network_interface')
load('http', 'get_json')
load('kwargs', 'get_url_base', 'get_http_options', 'get_string')
load('time', 'parse_ts')
load('json', 'encode')

API_VERSION     = '1.4'
SCAN_ENDPOINT   = '/api/' + API_VERSION + '/inventory/scancomputers'
PAGE_LIMIT      = 1000

# Endpoint Central reports every timestamp on a scancomputers row as Unix epoch
# milliseconds: agent_installed_on, agent_last_contact_time, last_sync_time,
# last_successful_scan, computer_status_update_time, process_start_time, and
# process_end_time among them. Each is carried through untouched, and a sibling
# attribute named <key>StandardTime carries the human-readable UTC form, e.g.
# agent_last_contact_time=1792403674000 gains
# agent_last_contact_timeStandardTime="19-10-2026 09:54:34 UTC".
STANDARD_TIME_SUFFIX = 'StandardTime'
STANDARD_TIME_LAYOUT = '02-01-2006 15:04:05'   # Go layout for DD-MM-YYYY HH:MM:SS

# The foreign-id namespace used when server_scope is set.
VENDOR          = 'manage-engine-endpoint-central'

# CONFIG["maxPages"] bounds the walk through pager() below. The ceiling is a
# backstop, not the working guard: the walk's two exits are an empty page and a
# short page, and a server that ignores `page` produces neither -- it just
# keeps answering with the same full page -- so the no-progress check below
# catches that on the first repeat and logs the stop, because a truncated
# import that says nothing looks exactly like a complete one. Exhausting
# maxPages itself raises, naming the label and the key.

def page_signature(devices):
    """A fingerprint of one page's rows, used to notice a server that answers
    every page number with the same page."""
    ids = []
    for d in devices:
        if type(d) == 'dict':
            ids.append(str(d.get('resource_id') or d.get('id') or ''))
        else:
            ids.append(str(d))
    return ','.join(ids)

def reported_total(body):
    """The row total Endpoint Central reports for the walk, or None.

    On a real server the paginated inventory APIs carry total/limit/page inside
    message_response, beside the scancomputers array; some builds put a
    metadata object alongside message_response instead. Both are read, and
    None is answered when neither has one -- a total this script invented
    would be worse than saying the API did not report one."""
    if type(body) != 'dict':
        return None
    for container in (body.get('message_response'), body.get('metadata')):
        if type(container) != 'dict':
            continue
        total = container.get('total', container.get('total_records'))
        if type(total) == 'int' and total >= 0:
            return total
        if type(total) == 'string' and total.isdigit():
            return int(total)
    return None

def retrieved_of(reported, total):
    """The 'Retrieved x/y' clause of a truncation message, or the no-total form
    the house style requires when the API does not report one."""
    if total == None:
        return 'Retrieved {} assets; the API does not report a total'.format(reported)
    return 'Retrieved {}/{} available assets'.format(reported, total)

# Prefixes a token must NOT carry. The credential key is named oauth_token, which
# invites pasting a bearer-style value, but the on-prem API wants the bare
# auth_token and answers anything else with status "error".
TOKEN_PREFIXES = ('bearer ', 'zoho-oauthtoken ')

# How much of an unexpected response body to quote in a log line. Enough to
# recognise what answered -- an error envelope, a login page's JSON, an empty
# object -- without pasting a page of inventory into the task log.
BODY_SAMPLE_CHARS = 600

def check_token_shape(token):
    """Warn about a token value that the on-prem API is bound to reject."""
    lowered = token.lower()
    for prefix in TOKEN_PREFIXES:
        if lowered.startswith(prefix):
            print("endpoint-central: WARNING the oauth_token value starts with '{}'. Endpoint Central on-prem expects the bare API key or auth_token with no prefix; remove it from the credential.".format(token[:len(prefix)].strip()))
            return
    if not token:
        print('endpoint-central: WARNING the oauth_token value is empty')

def api_error(body):
    """The (status, code, description) of a non-success Endpoint Central
    envelope, or None when the body reports success or carries no status.

    The on-prem API answers an invalid or expired auth token -- and most other
    request-level failures -- with HTTP 200 and a body of the form
    {"status":"error","error_code":...,"error_description":...}, with no
    message_response at all. get_json only reports a non-2xx status as an
    error, so without this check such a body reads as an inventory with no
    computers: the task ends with zero assets and no error, which is
    indistinguishable from a server that manages nothing. That is exactly how
    a credential problem went undiagnosed in the field, and why the run must
    fail here with the server's own words instead."""
    status = body.get('status')
    if status == None or str(status).lower() == 'success':
        return None
    code = body.get('error_code', body.get('errorCode', ''))
    description = body.get('error_description', body.get('error_message', body.get('message', '')))
    return (str(status), str(code), str(description))

def describe_body(body):
    """A bounded JSON rendering of a response body for a log line."""
    text = encode(body)
    if len(text) > BODY_SAMPLE_CHARS:
        text = text[:BODY_SAMPLE_CHARS] + '...'
    return text

def unexpected_shape(body, page, url):
    """The failure message for a 2xx body that is not a scancomputers page."""
    keys = sorted(body.keys()) if type(body) == 'dict' else []
    note = ''
    if type(body) == 'list' and not body:
        # No Endpoint Central build answers with a bare array; the one thing
        # that does is the generic dummy server behind `runzero script
        # --validate`, whose fixture does not model this vendor. Say so, so
        # that failure is read as the tool's limitation and not the script's.
        note = " (An empty JSON array is what the generic `runzero script --validate` dummy server returns; it does not model Endpoint Central. Validate against a fixture that answers with a scancomputers page instead.)"
    return ('Endpoint Central answered page {} at {} with a 2xx response that carries no message_response.scancomputers; ' +
            'the inventory could not be read. Top-level keys: {}. Body: {}').format(page, url, keys, describe_body(body)) + note

def is_millis_timestamp(value):
    """True when value is a 13-digit integer, or a string of 13 digits, which is
    the shape of every epoch-millisecond timestamp Endpoint Central reports.
    Thirteen digits spans the years 2001-2286, so a status code, a count, a
    10-digit epoch in seconds, and the '-1' and '--' placeholders Endpoint
    Central writes for "never" all fall outside it and are left alone."""
    if type(value) == 'int':
        digits = str(value)
    elif type(value) == 'string':
        digits = value.strip()
    else:
        return False
    return len(digits) == 13 and digits.isdigit()

def standard_time(value):
    """Render an epoch-millisecond value as 'DD-MM-YYYY HH:MM:SS UTC', or None
    when the value is not one.

    parse_ts answers None rather than raising on anything it cannot read, so a
    malformed value on one row cannot abort the task. Clamping is turned off
    because this is a display attribute, not an asset first/last-seen time: a
    timestamp ahead of the Explorer's clock should be shown as the server
    reported it, not silently moved to now. The explicit UTC conversion keeps
    the string the same whatever timezone the Explorer runs in.
    """
    if not is_millis_timestamp(value):
        return None
    stamp = parse_ts(value, unit='ms', clamp_to_now=False)
    if stamp == None:
        return None
    return stamp.in_location('UTC').format(STANDARD_TIME_LAYOUT) + ' UTC'

# Endpoint Central writes this literal into ip_address and mac_address slots
# that have no value: an adapter with a MAC but no address, or the other way
# round, and a not-yet-inventoried row reports "--" for the whole field.
EC_NO_VALUE = '--'

def split_adapter_field(value):
    """Split one of Endpoint Central's per-adapter fields into its tokens.

    ip_address and mac_address are each a comma-joined list with one token per
    network adapter, in adapter order. The '--' placeholder for a missing value
    is kept in place here, not dropped, because its position is what keeps the
    two lists aligned: dropping it from one list and not the other would pair
    the wrong MAC with the wrong address. The helpers downstream turn a '--'
    token into None/nothing. A release that returned an array instead of a
    string is accepted too, since .split on a list would abort the whole task.
    """
    if value == None:
        return []
    if type(value) == 'list':
        parts = value
    else:
        parts = str(value).split(',')
    return [str(p).strip() for p in parts if str(p).strip()]

def has_real_values(tokens):
    """True when at least one token is something other than the '--' placeholder."""
    for t in tokens:
        if t != EC_NO_VALUE:
            return True
    return False

def build_network_interfaces(device):
    """Return this device's interfaces, one per adapter, or an empty list.

    A machine with more than one adapter -- wired plus Wi-Fi, a VPN tunnel, a
    Hyper-V or VirtualBox host NIC -- comes back from scancomputers with
    mac_address set to a comma-joined list such as
    "f8:75:a4:..,0a:00:27:..", and ip_address joined the same way. Earlier
    versions of this script handed that whole string to NetworkInterface as a
    single MAC, and the platform rejected the asset outright ("invalid MAC
    address"); on a real 1,909-device server that dropped 1,254 assets, every
    machine with two or more NICs. Passing the joined string to
    net.network_interface instead does not reject the asset, but it silently
    discards the MAC, so the same two thirds of the fleet would import with no
    MAC at all and correlate on nothing stronger than a DHCP address.

    Both fields are split per adapter here. When the two lists are the same
    length -- which is how Endpoint Central emits them, with '--' holding the
    place of a missing value -- MAC[i] is paired with IP[i] so each interface
    carries its own address. When the lengths disagree the pairing cannot be
    trusted, so every address goes on the first MAC-bearing interface and the
    remaining MACs become MAC-only interfaces: nothing is lost, only the
    per-adapter pairing. A row with neither field, or with only '--' in both
    (an agent that has enrolled but not yet completed an inventory scan),
    produces no interface rather than an empty one.

    network_interface classifies v4/v6 itself, strips port and zone suffixes,
    dedupes, caps each family at 99, drops an unparseable address, and drops an
    invalid MAC -- including the '--' placeholder -- rather than aborting. It
    returns None when it is left with neither a MAC nor an address.
    """
    ips = split_adapter_field(device.get('ip_address'))
    macs = split_adapter_field(device.get('mac_address'))

    if not has_real_values(macs):
        # No usable MAC anywhere: a single address-only interface, or nothing.
        iface = network_interface(ips=ips, mac=None)
        return [iface] if iface else []

    ifaces = []
    if ips and len(ips) == len(macs):
        # Aligned per-adapter lists: pair positionally. A '--' on either side
        # simply yields a MAC-only or address-only interface for that adapter.
        for i in range(len(macs)):
            iface = network_interface(ips=[ips[i]], mac=macs[i])
            if iface:
                ifaces.append(iface)
        return ifaces

    # Unaligned (or no addresses at all): keep every MAC and every address,
    # with all addresses on the first interface that has a real MAC.
    remaining_ips = ips
    for mac in macs:
        if mac == EC_NO_VALUE:
            continue
        iface = network_interface(ips=remaining_ips, mac=mac)
        if iface:
            ifaces.append(iface)
            remaining_ips = []
    if remaining_ips:
        # Every MAC token was rejected as invalid; do not lose the addresses.
        iface = network_interface(ips=remaining_ips, mac=None)
        if iface:
            ifaces.append(iface)
    return ifaces

def build_assets(devices, scope):
    assets = []
    for d in devices:
        # A non-dict row -- null, a bare string -- would abort the script at
        # the first .get, losing the rest of the page. Skip it and say so.
        if type(d) != 'dict':
            print('endpoint-central: skipping non-object device record: ' + str(d))
            continue
        raw_id = d.get('resource_id') or d.get('id')
        if not raw_id:
            print("endpoint-central: skipping device with no resource_id/id: name=" + str(d.get('resource_name', '')))
            continue
        # resource_id is a small per-server integer, so two Endpoint Central
        # servers imported into one runZero organization collide on it. When
        # the operator sets server_scope the id becomes
        # <slug>:<server_scope>:<resource_id>; the empty default keeps the
        # legacy unscoped id so existing deployments are not re-identified.
        if scope:
            asset_id = '{}:{}:{}'.format(VENDOR, scope, raw_id)
        else:
            asset_id = str(raw_id)
        hostname = d.get('resource_name') or d.get('resource_name', '') or ''
        # build networkInterfaces
        net_ifaces = build_network_interfaces(d)

        # everything else goes into customAttributes (truncate to 1023 chars);
        # an epoch-millisecond value also gains a <key>StandardTime sibling
        # holding its human-readable UTC form, with the raw value kept as-is.
        custom = {}
        for k, v in d.items():
            if k in ('resource_id','id','resource_name','ip_address','mac_address'):
                continue
            custom[k] = str(v)[:1023]
            readable = standard_time(v)
            if readable:
                custom[k + STANDARD_TIME_SUFFIX] = readable

        assets.append(
            ImportAsset(
                id=asset_id,
                hostnames=[hostname],
                networkInterfaces=net_ifaces,
                customAttributes=to_custom_attributes(custom),
            )
        )
    return assets

def main(**kwargs):
    # oauth_token is the on-prem auth token, sent verbatim. Surrounding
    # whitespace is stripped because a pasted value routinely carries a
    # trailing newline, and the server rejects the token with it attached.
    base_url = get_url_base(kwargs)
    token = str(kwargs['oauth_token'] or '').strip()
    scope = get_string(kwargs, 'server_scope', default='')
    check_token_shape(token)
    headers = {
        'Authorization': token,
        'Accept':        'application/json',
    }
    http_options = get_http_options(kwargs, headers=headers)

    page        = 1
    reported    = 0
    total       = None
    last_signature = None
    url = base_url + SCAN_ENDPOINT
    print('endpoint-central: reading {} in pages of {}'.format(url, PAGE_LIMIT))
    p = pager('endpoint-central-computers')
    while p.next():
        params = {"pagelimit": PAGE_LIMIT, "page": page}
        body, err = get_json(url, params=params, timeout=3600, **http_options)
        if err:
            fail('Scan API error:', err)

        # Every exit below that is not a real page of computers ends the task
        # in error. Reporting zero assets with exit 0 is indistinguishable from
        # a server that manages no computers, and an import cut short by a
        # mid-walk failure must not look complete either.
        if body == None:
            fail('Endpoint Central answered page {} at {} with a 2xx response and an empty body; the inventory could not be read.'.format(page, url))
        if type(body) != 'dict':
            fail(unexpected_shape(body, page, url))
        error = api_error(body)
        if error:
            fail(('Endpoint Central rejected the request for page {} (HTTP 2xx, status={}, error_code={}, error_description={}). ' +
                  'An invalid or expired token is the usual cause. For a scheduled task use an API key: Admin > Integrations > API Key Management, ' +
                  'service Custom Integrations, Inventory read, never expires. A /api/1.4/desktop/authentication auth_token is a session token and expires. ' +
                  'Paste the value into the credential with no \'Bearer\' or \'Zoho-oauthtoken\' prefix, and confirm the account has Inventory read access.'
                  ).format(page, repr(error[0]), repr(error[1]), repr(error[2])))
        msg = body.get('message_response')
        if type(msg) != 'dict' or 'scancomputers' not in msg:
            fail(unexpected_shape(body, page, url))
        devices = msg.get('scancomputers')
        if type(devices) != 'list':
            fail(unexpected_shape(body, page, url))

        total = reported_total(body) if total == None else total
        print('endpoint-central: page {} returned {} computers (status={}, server total={})'.format(
            page, len(devices), body.get('status'), total))
        if not devices:
            if page == 1:
                print('endpoint-central: the server reports no scanned computers for this account. If the Endpoint Central console shows managed computers, check that the account behind the token has Inventory read access.')
            break

        # A page identical to the one before it means the server stopped
        # advancing -- a `page` it ignored, or a cached response replayed. The
        # walk's exits are an empty page and a short page, so a repeated FULL
        # page produces neither and the walk would re-import it until the
        # ceiling. Stop on the first repeat.
        signature = page_signature(devices)
        if signature == last_signature:
            print('endpoint-central: pagination stopped making progress at page {}; the server repeated the previous page. {}'.format(
                page, retrieved_of(reported, total)))
            break
        last_signature = signature

        # Build and stream each page via report_assets so the full device set
        # is never held in memory.
        reported += report_assets(build_assets(devices, scope))
        if len(devices) < PAGE_LIMIT:
            break
        page += 1

    print('endpoint-central: imported {} assets from {} page(s). {}'.format(reported, page, retrieved_of(reported, total)))
    if not reported:
        print('No devices returned')

    return None
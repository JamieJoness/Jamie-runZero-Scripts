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
            "label": "API auth token (on-prem)",
            "type": "secret",
            "required": True,
            "description": "On-premises Endpoint Central auth token, sent verbatim as the Authorization header. Not a Zoho OAuth token; Endpoint Central Cloud is not supported and will answer 401.",
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

API_VERSION     = '1.4'
SCAN_ENDPOINT   = '/api/' + API_VERSION + '/inventory/scancomputers'
PAGE_LIMIT      = 1000

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
    """Endpoint Central returns a metadata object alongside message_response on
    the paginated inventory APIs. Read the row total from it when it is there,
    and answer None when it is not -- a total this script invented would be
    worse than saying the API did not report one."""
    if type(body) != 'dict':
        return None
    meta = body.get('metadata', {})
    if type(meta) != 'dict':
        return None
    total = meta.get('total', meta.get('total_records'))
    if type(total) == 'int' and total >= 0:
        return total
    return None

def retrieved_of(reported, total):
    """The 'Retrieved x/y' clause of a truncation message, or the no-total form
    the house style requires when the API does not report one."""
    if total == None:
        return 'Retrieved {} assets; the API does not report a total'.format(reported)
    return 'Retrieved {}/{} available assets'.format(reported, total)

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

        # everything else goes into customAttributes (truncate to 1023 chars)
        custom = {}
        for k, v in d.items():
            if k in ('resource_id','id','resource_name','ip_address','mac_address'):
                continue
            custom[k] = str(v)[:1023]

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
    # oauth_token is your auth token.
    base_url = get_url_base(kwargs)
    token = kwargs['oauth_token']
    scope = get_string(kwargs, 'server_scope', default='')
    headers = {
        'Authorization': token,
        'Accept':        'application/json',
    }
    http_options = get_http_options(kwargs, headers=headers)

    page        = 1
    reported    = 0
    total       = None
    last_signature = None
    p = pager('endpoint-central-computers')
    while p.next():
        url = base_url + SCAN_ENDPOINT
        params = {"pagelimit": PAGE_LIMIT, "page": page}
        body, err = get_json(url, params=params, timeout=3600, **http_options)
        if err:
            fail('Scan API error:', err)

        body    = body or {}
        # message_response is documented as an object; reading .get off a list
        # would abort the script, so an array-shaped error document has to stop
        # the walk rather than end the run silently.
        if type(body) != 'dict' or type(body.get('message_response', {})) != 'dict':
            print('endpoint-central: unexpected response shape, wanted an object')
            break
        total   = reported_total(body) if total == None else total
        msg     = body.get('message_response', {})
        devices = msg.get('scancomputers', [])
        if type(devices) != 'list' or not devices:
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

    if not reported:
        print('No devices returned')

    return None
# This is a runZero Custom Integration, please see https://github.com/runZeroInc/runzero-custom-integrations for details.
# This script was generated with AI.

CONFIG = {
    "id": "runzero-trellix-rsd",
    "name": "Trellix Rogue System Detection",
    "type": "inbound",
    "description": "Imports the Rogue System Detection Detected Systems inventory from an on-premises Trellix ePolicy Orchestrator server, including devices that have no Trellix Agent.",
    "version": "1",
    "maturity": "alpha",
    "minVersion": "5.1.260818.0",
    # A detected system is keyed by HostID, a SQL identity column that RSD keeps
    # across re-detections. The MAC, IP, and names on the row are what a sensor
    # observed at one moment: a DHCP lease moves, a reverse lookup lags, a NIC
    # gets replaced. That churn must not disqualify a merge against the id.
    "matchBehavior": "no-mac-break no-ip-break no-name-break",
    "params": [
        {
            "key": "url",
            "label": "ePolicy Orchestrator URL",
            "type": "url",
            "required": True,
            "placeholder": "https://epo.example.com:8443",
            "description": "Base URL of the ePO console. The remote command interface is served from /remote/ on the console port, which defaults to 8443.",
        },
        {
            "key": "username",
            "label": "Username",
            "type": "string",
            "required": True,
            "description": "ePO user account. A permission set with view rights to Rogue System Detection is sufficient; the integration only reads the Detected Systems list.",
        },
        {
            "key": "password",
            "label": "Password",
            "type": "secret",
            "required": True,
            "description": "Password for the ePO user account, sent with HTTP Basic authentication.",
        },
        {
            "key": "search_text",
            "label": "Search text",
            "type": "string",
            "required": False,
            "description": "Substring filter applied by detectedsystem.find to the DNS name, NetBIOS name, domain, user, and IP address columns. Leave blank to import every detected system.",
        },
        {
            "key": "include_managed",
            "label": "Import managed systems",
            "type": "bool",
            "required": False,
            "default": True,
            "description": "Import detected systems that have an active Trellix Agent. Turn off when the ePolicy Orchestrator System Tree integration already covers them.",
        },
        {
            "key": "include_exceptions",
            "label": "Import exceptions",
            "type": "bool",
            "required": False,
            "default": True,
            "description": "Import detected systems on the RSD Exceptions list, such as printers, phones, and switches that are known not to need an agent.",
        },
        {
            "key": "include_inactive",
            "label": "Import inactive systems",
            "type": "bool",
            "required": False,
            "default": False,
            "description": "Import detected systems that no sensor has seen for longer than the RSD inactive period (45 days by default).",
        },
    ],
    "includes": {
        "tls_": OPTIONS_TLS,
        "http_": OPTIONS_HTTP,
    },
}
load('runzero.types', 'ImportAsset', 'to_custom_attributes')
load('net', 'network_interface', 'clean_hostnames')
load('http', http_get='get', 'basic', 'url_encode', 'url_parse')
load('jsonstream', 'iter_array')
load('kwargs', 'get_url_base', 'get_http_options', 'get_string', 'get_bool')
load('coerce', 'as_text', 'as_int', 'as_bool')
load('re', re_sub='sub')
load('time', 'parse_ts', 'sleep')

# Every ePO remote command is an RPC name used as a path segment under /remote/.
REMOTE_PATH = "/remote/"
FIND_COMMAND = "detectedsystem.find"

# Result rows are flat dicts keyed by "<ePO table>.<column>". Detected systems
# all come from the one RSD table.
TABLE = "RSDDetectedSystems."

# IPV4 is stored biased by 2^31 so that addresses sort correctly as signed
# 32-bit integers in the backing SQL Server column: the dotted quad is
# recovered by adding 2^31 back and splitting the result into four octets.
# 10.0.0.0/8 therefore arrives negative while 192.168.0.0/16 arrives positive.
IPV4_BIAS = 2147483648
IPV4_MIN = -2147483648
IPV4_MAX = 2147483647

# ePO writes these literal placeholders into string columns it has no value
# for; Unknown is what a fingerprint column such as OSPlatform or DeviceType
# falls back to when the sensor could not classify the system.
PLACEHOLDER_VALUES = ["n/a", "(none)", "<null>", "unknown"]

# RSD keeps one bit column per state and does not document them as mutually
# exclusive, so one status is derived per row by taking the first flag set in
# this order. An exception stays an exception when it goes quiet, and a rogue
# that no sensor has seen for the inactive period is inactive rather than rogue.
STATUS_FLAGS = ["Exception", "Inactive", "Managed", "Rogue"]

def _text(value):
    """Return a trimmed string, treating ePO's placeholder values as empty."""
    text = as_text(value)
    if text.lower() in PLACEHOLDER_VALUES:
        return ""
    return text

def _snake(column):
    """Convert an ePO column name such as OSPlatform or LastDetectedTime to snake_case."""
    column = re_sub(r"([A-Z]+)([A-Z][a-z])", "${1}_${2}", column)
    column = re_sub(r"([a-z0-9])([A-Z])", "${1}_${2}", column)
    return column.replace(".", "_").lower()

def _appliance_scope(base_url):
    """Return the ePO hostname, which is the uniqueness scope for HostID.

    Scheme and port are dropped so that editing the configured URL between http
    and https, or off the default console port, does not change the identity of
    systems that were already imported.
    """
    parsed = url_parse(base_url)
    if parsed and parsed.hostname:
        return str(parsed.hostname).lower()
    return base_url.split("://")[-1].split("/")[0].split(":")[0].lower()

def _ipv4_from_column(value):
    """Convert the biased signed IPV4 column into a dotted quad."""
    if type(value) != "int" or value < IPV4_MIN or value > IPV4_MAX:
        return ""
    # ePO reports an absent address as null, and a literal 0 would decode to
    # the network address 128.0.0.0, so a zero column is treated as unset
    # rather than injected as an address shared by every affected system.
    if value == 0:
        return ""
    packed = value + IPV4_BIAS
    quad = "{}.{}.{}.{}".format((packed // 16777216) % 256, (packed // 65536) % 256,
                                (packed // 256) % 256, packed % 256)
    if quad == "0.0.0.0":
        return ""
    return quad

def _parse_envelope(body):
    """Split ePO's status line off the response body, returning (payload, err).

    Every remote command answers with a status line terminated by a colon and a
    CRLF before the JSON document, so the response is not valid JSON until that
    prefix is removed. A successful command answers "OK:"; a failed one answers
    "Error <code>:" followed by a plain-text message instead of a document.
    """
    marker = body.find(":")
    if marker < 0:
        return "", "unrecognized response envelope"
    status = body[:marker]
    payload = body[marker + 1:].strip()
    if status.split(" ")[0] != "OK":
        return "", "{}: {}".format(status.strip(), payload[:200])
    return payload, None

def _decode_rows(payload):
    """Return an iterator over an ePO result array, rejecting other payloads.

    The payload is checked for the opening bracket of an array first, because
    an HTML error page or a bare object handed to the iterator would abort the
    script. The rows are streamed with jsonstream rather than decoded whole, so
    a large Detected Systems list never holds the decoded result set in memory -
    only the response text plus one row at a time. A command that succeeds
    without matching anything answers with an empty payload rather than `[]`.
    """
    if not payload:
        return [], None
    if not payload.startswith("["):
        return [], "expected a JSON array, got: " + payload[:120]
    return iter_array(payload), None

def _command_url(base_url, command, params):
    """Build a remote command URL with `:output=json` written in literally.

    The leading colon belongs to ePO's parameter name and every ePO client
    sends it unencoded, so it is concatenated rather than handed to url_encode,
    which would emit `%3Aoutput`. The remaining values are encoded, and the "+"
    that url_encode emits for a space is rewritten to "%20" so that a search
    string containing spaces reaches the console intact; a literal plus is
    already "%2B" by that point and is unaffected.
    """
    query = ":output=json"
    if params:
        encoded = {}
        for key in params:
            encoded[key] = str(params[key])
        query = query + "&" + url_encode(encoded).replace("+", "%20")
    return "{}{}{}?{}".format(base_url, REMOTE_PATH, command, query)

def run_command(base_url, command, params, http_options):
    """Run one ePO remote command and return its rows as (rows, err).

    get_json cannot be used here because the response body is prefixed with a
    status line and therefore does not decode as JSON. That also means the
    retry budget get_json exposes is unavailable on this transport - the raw
    http.get builtin rejects a `retries` argument outright - so a narrow retry
    is hand-rolled: the command is a read, safe to repeat, and only a missing
    response or a transient status (408/425/429/5xx) is retried. The query
    string is built into the URL, so no `params` argument is passed alongside.
    """
    url = _command_url(base_url, command, params)
    response = None
    for attempt in range(3):
        if attempt:
            sleep("{}s".format(attempt))
        response = http_get(url, **http_options)
        if response and response.status_code not in [408, 425, 429, 500, 502, 503, 504]:
            break
    if not response:
        return [], "no response from " + command
    if response.status_code != 200:
        return [], "status {}".format(response.status_code)

    payload, err = _parse_envelope(str(response.body))
    if err:
        return [], err
    return _decode_rows(payload)

def _status(row):
    """Return the derived RSD status of a row, or "" when no state flag is set."""
    for flag in STATUS_FLAGS:
        if as_bool(row.get(TABLE + flag)):
            return flag.lower()
    return ""

def build_attributes(row, ipv4):
    """Keep every RSDDetectedSystems column as a snake_case custom attribute.

    The column set could not be confirmed against a live console, so columns
    are passed through generically rather than enumerated: anything the
    console adds is kept instead of dropped. Only IPV4 is rewritten, from the
    biased integer to the decoded address.
    """
    attrs = {}
    for key in row:
        column = key[len(TABLE):] if key.startswith(TABLE) else key
        attrs[_snake(column)] = ipv4 if column == "IPV4" else _text(row[key])
    # prefix is joined to each key with separator, so this yields
    # "trellix_rsd_host_id" rather than "trellix_rsd_.host_id".
    return to_custom_attributes(attrs, prefix="trellix_rsd", separator="_")

def build_asset(row, scope, host_id, status):
    """Convert one RSDDetectedSystems row into an ImportAsset, or None.

    None is returned for a row carrying no MAC, address, or name: it could
    never be correlated with anything, and a sensor detection without an
    interface is not a device.
    """
    ipv4 = _ipv4_from_column(row.get(TABLE + "IPV4"))
    # MAC is the address as unpunctuated hex; network_interface normalizes
    # that form directly, so no separators are inserted by hand.
    nic = network_interface(mac=_text(row.get(TABLE + "MAC")),
                            ips=[ip for ip in [ipv4, _text(row.get(TABLE + "IPV6"))] if ip])
    hostnames = clean_hostnames([_text(row.get(TABLE + "DnsName")),
                                 _text(row.get(TABLE + "NetbiosName"))])
    if not nic and not hostnames:
        return None

    asset = ImportAsset(
        id="trellix-rsd:{}:{}".format(scope, host_id),
        hostnames=hostnames,
        domain=_text(row.get(TABLE + "Domain")),
        os=_text(row.get(TABLE + "OSPlatform")),
        osVersion=_text(row.get(TABLE + "OSVersion")),
        deviceType=_text(row.get(TABLE + "DeviceType")),
        networkInterfaces=[nic] if nic else [],
        tags=["rsd-status:" + status] if status else [],
        customAttributes=build_attributes(row, ipv4),
    )
    # lastSeenTS is settable as an attribute but is not a constructor keyword
    # on the Explorer release named in minVersion. RSD records no first
    # detection time on the system row, so firstSeenTS is left unset.
    last_detected = parse_ts(row.get(TABLE + "LastDetectedTime"))
    if last_detected != None:
        asset.lastSeenTS = last_detected
    return asset

def report_detected_systems(rows, scope, included):
    """Build and stream one asset per detected system, returning the counts.

    Each asset goes to report_asset as it is built, so although ePO answers the
    whole Detected Systems list in one response, only one built asset is held
    at a time rather than one estate.
    """
    counts = {"reported": 0, "filtered": 0, "skipped": 0}
    for row in rows:
        if type(row) != "dict":
            print("trellix-rsd: skipping malformed detected system record")
            counts["skipped"] += 1
            continue

        host_id = as_int(row.get(TABLE + "HostID"))
        if host_id <= 0:
            print("trellix-rsd: skipping detected system with no HostID: name=" +
                  _text(row.get(TABLE + "DnsName")))
            counts["skipped"] += 1
            continue

        status = _status(row)
        if not included.get(status, True):
            counts["filtered"] += 1
            continue

        asset = build_asset(row, scope, host_id, status)
        if asset == None:
            print("trellix-rsd: skipping detected system {} with no MAC, address, or name".format(host_id))
            counts["skipped"] += 1
            continue
        counts["reported"] += report_asset(asset)
    return counts

def main(**kwargs):
    base_url = get_url_base(kwargs)
    username = get_string(kwargs, "username")
    password = get_string(kwargs, "password")
    search_text = get_string(kwargs, "search_text", default="")
    included = {
        "managed": get_bool(kwargs, "include_managed", default=True),
        "exception": get_bool(kwargs, "include_exceptions", default=True),
        "inactive": get_bool(kwargs, "include_inactive", default=False),
    }

    http_options = get_http_options(kwargs, headers={
        "Authorization": basic(username, password),
        "Accept": "application/json",
    })

    # searchText is a required argument of detectedsystem.find. The ePO
    # PowerShell client lists every detected system by sending a single space,
    # so a blank filter is sent the same way rather than as an empty value.
    rows, err = run_command(base_url, FIND_COMMAND, {"searchText": search_text or " "},
                            http_options)
    if err:
        if err.startswith("status 401") or err.startswith("status 403"):
            print("trellix-rsd: check the username and password")
        fail("trellix-rsd: could not read the Detected Systems list: {}".format(err))

    counts = report_detected_systems(rows, _appliance_scope(base_url), included)
    print("trellix-rsd: reported {} detected systems, {} excluded by the state filters, {} skipped".format(
        counts["reported"], counts["filtered"], counts["skipped"]))
    return None

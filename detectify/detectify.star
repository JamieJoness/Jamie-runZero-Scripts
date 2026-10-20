# This is a runZero Custom Integration, please see https://github.com/runZeroInc/runzero-custom-integrations for details.
# This script was generated with AI.

CONFIG = {
    "id": "runzero-detectify",
    "name": "Detectify",
    "type": "inbound",
    "description": "Imports the external attack surface discovered by Detectify Surface Monitoring: each public IP address with the domains that resolve to it, ASN and geolocation, open ports as services, fingerprinted technologies as software, and active policy breaches and vulnerabilities as vulnerabilities.",
    "version": "1",
    "maturity": "alpha",
    "minVersion": "5.1.260818.0",
    # Detectify watches the estate from the internet. It has no device key, only
    # the address it observed and the DNS names that pointed at it, so the asset
    # id is composed from the address: stable and unique per host, but silent on
    # which machine holds it today. Id matching stays ON because it is the only
    # way a record re-finds its own asset on the next poll -- an address match
    # alone skips candidates that already carry this record's id. no-id-break
    # stops the composed id from vetoing a legitimate address match when one host
    # owns several of the addresses, or when an asset was re-addressed and still
    # carries the id of its previous address. The hostnames are DNS names that
    # pointed at the address, not names the machine calls itself: they must not
    # find merge candidates (a short label such as "portal" would merge an
    # internal server onto an internet-facing address) and must not veto one (an
    # EDR-reported machine name never equals a public DNS name). Correlation is
    # left to the public address. See README "Asset identity".
    "matchBehavior": "no-id-break no-name-match no-name-break",
    # Every v3 walk pages 1,000 rows (100 for v2 vulnerabilities), so 10,000
    # pages bounds a listing at ten million rows -- far past any real attack
    # surface -- while a cursor that never resolves raises a clear error.
    "maxPages": 10000,
    "params": [
        {
            "key": "url",
            "label": "Detectify API URL",
            "type": "url",
            "required": False,
            "default": "https://api.detectify.com",
            "placeholder": "https://api.detectify.com",
            "description": "Base URL of the Detectify API. The script appends /rest/v3 and /rest/v2 itself. Override only when requests must go through a proxy.",
        },
        {
            "key": "api_key",
            "label": "API key",
            "type": "secret",
            "required": True,
            "description": "Detectify API key generated under Settings > API keys. Sent in the Authorization header to the v3 attack surface API and, unless a separate v2 key is given below, in the X-Detectify-Key header to the v2 vulnerabilities API.",
        },
        {
            "key": "api_key_v2",
            "label": "API key for the v2 API (optional)",
            "type": "secret",
            "required": False,
            "description": "Only needed when the key above is not accepted by the v2 API (vulnerabilities). Leave blank to reuse the API key.",
        },
        {
            "key": "team_ids",
            "label": "Team IDs",
            "type": "string",
            "required": False,
            "description": "Comma-separated Detectify team IDs to import. Leave blank to import every team the API key can see.",
        },
        {
            "key": "include_ports",
            "label": "Import open ports as services",
            "type": "bool",
            "required": False,
            "default": True,
            "description": "Read GET /rest/v3/ports and attach every OPEN port as a service on the address it was found on.",
        },
        {
            "key": "include_technologies",
            "label": "Import technologies as software",
            "type": "bool",
            "required": False,
            "default": True,
            "description": "Read GET /rest/v3/technologies and attach each fingerprinted technology as software on every address its domain resolves to.",
        },
        {
            "key": "include_breaches",
            "label": "Import policy breaches as vulnerabilities",
            "type": "bool",
            "required": False,
            "default": True,
            "description": "Read GET /rest/v3/breaches and attach every active (NEW or REGRESSION) policy breach as a vulnerability on the addresses of the breached asset.",
        },
        {
            "key": "include_vulnerabilities",
            "label": "Import vulnerabilities (v2 API)",
            "type": "bool",
            "required": False,
            "default": True,
            "description": "Read GET /rest/v2/vulnerabilities/ and attach every active, new, or regressed finding as a vulnerability on the addresses of its host. Uses the v2 key when one is given.",
        },
        {
            "key": "include_monitoring_settings",
            "label": "Import Surface Monitoring settings",
            "type": "bool",
            "required": False,
            "default": True,
            "description": "Read GET /rest/v3/surface-monitoring/domains and /ips and summarize whether monitoring, port scanning, fingerprinting, and finding modules are enabled for the assets behind each address.",
        },
        {
            "key": "include_inactive",
            "label": "Import inactive records",
            "type": "bool",
            "required": False,
            "default": False,
            "description": "Also import IP addresses and technologies Detectify no longer sees (active=false, with a disappeared_at timestamp). Off by default so a retired address does not keep attaching old data to an asset.",
        },
        {
            "key": "include_private_ips",
            "label": "Import private addresses",
            "type": "bool",
            "required": False,
            "default": False,
            "description": "Also import RFC 1918, CGNAT, and ULA addresses that public DNS records point at. Off by default: a private address is not unique across sites, so it could merge internal assets that only share an address.",
        },
    ],
    "includes": {
        "tls_": OPTIONS_TLS,
        "http_": OPTIONS_HTTP,
    },
}

load("coerce", "as_bool", "as_dict", "as_float", "as_int", "as_list", "as_text", "dedupe", "dicts")
load("http", "get_json", "url_encode", "url_parse")
load("kwargs", "get_bool", "get_http_options", "get_list", "get_string", "require")
load("net", "clean_hostname", "ip_address", "ip_in_network", "network_interface", "routable_ip")
load("re", re_find_all="find_all")
load("runzero.progress", progress_report="report")
load("runzero.types", "ImportAsset", "Service", "ServiceProtocolData", "Software", "Vulnerability", "to_custom_attributes")
load("time", "parse_ts")

DEFAULT_URL = "https://api.detectify.com"
V3_PATH = "/rest/v3"
V2_PATH = "/rest/v2"
# Both documented maximums.
V3_PAGE_SIZE = 1000
V2_PAGE_SIZE = 100
# The platform caps an ImportAsset at 100 hostnames and each child collection
# (services, software, vulnerabilities) at 99 per asset. A shared CDN or
# hosting address can exceed both, so the lists are cut deliberately: the
# lowest ports, active software first, and the most severe findings first.
MAX_HOSTNAMES = 100
CHILD_CAP = 99
# The v3 API reports a port as OPEN or CLOSED and nothing about its transport.
# Detectify documents scanning both TCP and UDP ports, but with no field to tell
# them apart every service is recorded as TCP, which is what the overwhelming
# majority of internet-facing open ports are. See README "Assumptions".
TRANSPORT = "tcp"
# routable_ip keeps RFC 1918, CGNAT, and ULA space on purpose; these are the
# ranges the include_private_ips switch controls on top of that.
PRIVATE_NETWORKS = ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "100.64.0.0/10", "fc00::/7"]
# Detectify severities (v3 breaches: INFO..CRITICAL, v2 CVSS severities:
# information..critical) onto the runZero rank scale 0=info .. 4=critical.
SEVERITY_RANKS = {"INFO": 0, "INFORMATION": 0, "NONE": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}
SEVERITY_NAMES = ["info", "low", "medium", "high", "critical"]
CVE_PATTERN = "(?i)CVE-[0-9]{4}-[0-9]{4,19}"
MONITORING_FLAGS = [
    "enabled",
    "port_scanning_enabled",
    "fingerprint_enabled",
    "findings_enabled",
    "subdomain_takeover_enabled",
    "ssl_audit_enabled",
    "tls_discovery_enabled",
]


# -------------------------
# Small shared helpers
# -------------------------

def is_private(ip):
    for cidr in PRIVATE_NETWORKS:
        if ip_in_network(ip, cidr):
            return True
    return False


def keep_earliest(slot, raw):
    """Return the earlier of an existing {ts, raw} slot and a source timestamp.
    Unparseable values leave the slot untouched, so one bad row cannot erase
    a good first-seen time."""
    t = parse_ts(raw)
    if t == None:
        return slot
    if slot == None or t.unix < slot["ts"].unix:
        return {"ts": t, "raw": as_text(raw)}
    return slot


def keep_latest(slot, raw):
    t = parse_ts(raw)
    if t == None:
        return slot
    if slot == None or t.unix > slot["ts"].unix:
        return {"ts": t, "raw": as_text(raw)}
    return slot


def slot_raw(slot):
    return slot["raw"] if slot != None else ""


def slot_ts(slot):
    return slot["ts"] if slot != None else None


def name_key(value):
    """Reduce a Detectify domain_name / asset_name / host value to the key the
    hosts index uses: the canonical address when the value is an IP address
    (Detectify monitors raw IPs and IP ranges too), otherwise the lower-cased
    hostname. None when the value is neither."""
    ip = routable_ip(value)
    if ip != None:
        return ip
    name = clean_hostname(value)
    if name:
        return name.lower()
    return None


def severity_rank(name):
    return SEVERITY_RANKS.get(as_text(name).upper(), None)


def severity_name(rank):
    if rank == None or rank < 0 or rank >= len(SEVERITY_NAMES):
        return ""
    return SEVERITY_NAMES[rank]


def tech_label(tech):
    if tech["version"]:
        return "{} {}".format(tech["name"], tech["version"])
    return tech["name"]


def camel(snake):
    parts = snake.split("_")
    return parts[0] + "".join([p[:1].upper() + p[1:] for p in parts[1:]])


def add_row(index, key, row):
    if key not in index:
        index[key] = []
    index[key].append(row)


def ordered(entries, cap):
    """Return the objects of (sort_key, object) pairs in sort-key order, cut at
    cap. Keys must be unique strings: Starlark compares tuples element-wise and
    the objects themselves are not comparable."""
    by_key = {}
    for key, obj in entries:
        by_key[key] = obj
    keys = sorted(by_key.keys())
    return [by_key[k] for k in keys[:cap]], len(keys) - min(len(keys), cap)


def gather(index, keys):
    """Collect the rows filed under any of an asset's keys (its address and each
    of its domain names), de-duplicated by Detectify id."""
    seen = {}
    rows = []
    for key in keys:
        for row in index.get(key, []):
            rid = row.get("id", "")
            if rid and rid in seen:
                continue
            if rid:
                seen[rid] = True
            rows.append(row)
    return rows


# -------------------------
# HTTP walks
# -------------------------

def next_cursor(next_url):
    """The v3 pagination.next link is an absolute URL carrying ?cursor=...; only
    the cursor is reused, on the configured base URL and with the original
    filters, because the HTTP helper replaces a URL's query with params."""
    text = as_text(next_url)
    if not text:
        return ""
    parsed = url_parse(text)
    if parsed == None:
        return ""
    values = as_list(parsed.query.get("cursor"))
    return as_text(values[0]) if values else ""


def walk_v3(ctx, label, path, params, on_row):
    """Page through one v3 listing, handing every item to on_row. Returns
    (rows, err); err is None, or a string naming the page that failed."""
    rows = 0
    cursor = ""
    p = pager(label)
    while p.next():
        query = dict(params)
        query["limit"] = V3_PAGE_SIZE
        if cursor:
            query["cursor"] = cursor
        data, err = get_json(ctx["v3_base"] + path, params=query, **ctx["v3_http"])
        if err:
            return rows, "page {} of {} failed: {}".format(p.page, path, err)
        data = as_dict(data)
        items = dicts(data.get("items"))
        for item in items:
            on_row(item)
        rows += len(items)
        cursor = next_cursor(as_dict(data.get("pagination")).get("next"))
        if not cursor or len(items) == 0:
            break
    return rows, None


def walk_v2_vulnerabilities(ctx, on_row):
    """Page through GET /rest/v2/vulnerabilities/ with marker pagination. The
    status filter repeats one query key (status[]=...), which the params
    helper cannot express, so the query string is assembled by hand."""
    rows = 0
    marker = ""
    statuses = "status%5B%5D=active&status%5B%5D=new&status%5B%5D=regression"
    p = pager("vulnerabilities")
    while p.next():
        query = {"pageSize": V2_PAGE_SIZE}
        if marker:
            query["marker"] = marker
        url = "{}/vulnerabilities/?{}&{}".format(ctx["v2_base"], statuses, url_encode(query))
        data, err = get_json(url, **ctx["v2_http"])
        if err:
            return rows, "page {} of /vulnerabilities/ failed: {}".format(p.page, err)
        data = as_dict(data)
        items = dicts(data.get("vulnerabilities"))
        for item in items:
            on_row(item)
        rows += len(items)
        marker = as_text(data.get("next_marker"))
        if not as_bool(data.get("has_more")) or not marker or len(items) == 0:
            break
    return rows, None


def auth_hint(err):
    """Print the longer guidance for a refused key before the one-line fail()."""
    text = as_text(err)
    if "status 401" in text or "status 403" in text:
        print("detectify: the API key was rejected. Generate a key under Settings > API keys in the Detectify app; it inherits the teams and assets of the user who created it.")


# -------------------------
# Collectors: one per listing, each filing slim rows into ctx indexes
# -------------------------

def new_host(ip):
    return {
        "ip": ip,
        "domains": {},
        "asset_ids": {},
        "team_ids": {},
        "record_ids": {},
        "active": False,
        "enriched": False,
        "first_seen": None,
        "disappeared": None,
        "asn": {},
        "geo": {},
    }


def collect_ips(ctx):
    stats = ctx["stats"]
    hosts = ctx["hosts"]

    def on_row(row):
        stats["ip_rows"] += 1
        team_id = as_text(row.get("team_id"))
        if ctx["team_filter"] and team_id not in ctx["team_filter"]:
            stats["ip_rows_other_team"] += 1
            return
        active = as_bool(row.get("active"))
        if not active and not ctx["include_inactive"]:
            stats["ip_rows_inactive"] += 1
            return
        ip = routable_ip(row.get("ip_address"))
        if ip == None:
            stats["ip_rows_unusable"] += 1
            return
        if not ctx["include_private"] and is_private(ip):
            stats["ip_rows_private"] += 1
            return
        host = hosts.get(ip)
        if host == None:
            host = new_host(ip)
            hosts[ip] = host
        # A domain that resolves to this address. Detectify reports a monitored
        # raw IP with its own address as domain_name; clean_hostname rejects
        # IP-shaped values, so that is not recorded as a name.
        domain = clean_hostname(row.get("domain_name"))
        if domain:
            domain = domain.lower()
            host["domains"][domain] = host["domains"].get(domain, False) or active
        asset_id = as_text(row.get("asset_id"))
        if asset_id:
            host["asset_ids"][asset_id] = True
        if team_id:
            host["team_ids"][team_id] = True
            ctx["teams_seen"][team_id] = True
        record_id = as_text(row.get("id"))
        if record_id:
            host["record_ids"][record_id] = True
        enriched = as_bool(row.get("enriched"))
        host["active"] = host["active"] or active
        host["enriched"] = host["enriched"] or enriched
        host["first_seen"] = keep_earliest(host["first_seen"], row.get("first_seen_at"))
        if not active:
            host["disappeared"] = keep_latest(host["disappeared"], row.get("disappeared_at"))
        # ASN and geolocation describe the address, so every row for it should
        # agree; an enriched row is preferred over one that was not enriched yet.
        asn = as_dict(row.get("autonomous_system"))
        if asn and (not host["asn"] or enriched):
            host["asn"] = asn
        geo = as_dict(row.get("geolocation"))
        if geo and (not host["geo"] or enriched):
            host["geo"] = geo

    return walk_v3(ctx, "ips", "/ips", {}, on_row)


def collect_ports(ctx):
    stats = ctx["stats"]

    def on_row(row):
        stats["port_rows"] += 1
        team_id = as_text(row.get("team_id"))
        if ctx["team_filter"] and team_id and team_id not in ctx["team_filter"]:
            stats["port_rows_other_team"] += 1
            return
        if as_text(row.get("status")).upper() != "OPEN":
            stats["port_rows_closed"] += 1
            return
        ip = routable_ip(row.get("ip_address"))
        port = as_int(row.get("port"), default=-1)
        if ip == None or port < 0 or port > 65535:
            stats["port_rows_unusable"] += 1
            return
        if ip not in ctx["hosts"]:
            stats["port_rows_orphan"] += 1
            return
        by_port = ctx["ports"].get(ip)
        if by_port == None:
            by_port = {}
            ctx["ports"][ip] = by_port
        entry = by_port.get(port)
        if entry == None:
            entry = {"port": port, "domains": {}, "ids": [], "first_seen": None}
            by_port[port] = entry
        domain = clean_hostname(row.get("domain_name"))
        if domain:
            entry["domains"][domain.lower()] = True
        record_id = as_text(row.get("id"))
        if record_id:
            entry["ids"].append(record_id)
        entry["first_seen"] = keep_earliest(entry["first_seen"], row.get("first_seen_at"))

    return walk_v3(ctx, "ports", "/ports", {"status": "OPEN"}, on_row)


def collect_technologies(ctx):
    stats = ctx["stats"]

    def on_row(row):
        stats["tech_rows"] += 1
        team_id = as_text(row.get("team_id"))
        if ctx["team_filter"] and team_id and team_id not in ctx["team_filter"]:
            stats["tech_rows_other_team"] += 1
            return
        active = as_bool(row.get("active"))
        if not active and not ctx["include_inactive"]:
            stats["tech_rows_inactive"] += 1
            return
        name = as_text(row.get("name"))
        key = name_key(row.get("domain_name"))
        if not name or key == None:
            stats["tech_rows_unusable"] += 1
            return
        port = as_int(row.get("port"), default=-1)
        domain = clean_hostname(row.get("domain_name"))
        add_row(ctx["techs"], key, {
            "id": as_text(row.get("id")),
            "name": name,
            "version": as_text(row.get("version")),
            "categories": [as_text(c) for c in as_list(row.get("categories")) if as_text(c)],
            "protocol": as_text(row.get("service_protocol")).lower(),
            "port": port if port >= 0 and port <= 65535 else None,
            # Empty for a technology fingerprinted on a raw monitored IP.
            "domain": domain.lower() if domain else "",
            "asset_id": as_text(row.get("asset_id")),
            "active": active,
            "first_seen_at": as_text(row.get("first_seen_at")),
            "disappeared_at": as_text(row.get("disappeared_at")),
        })

    return walk_v3(ctx, "technologies", "/technologies", {}, on_row)


def collect_breaches(ctx):
    stats = ctx["stats"]

    def on_row(row):
        stats["breach_rows"] += 1
        # active is documented as true for NEW and REGRESSION and false
        # otherwise; a resolved breach is history, not a finding on the asset.
        if not as_bool(row.get("active")):
            stats["breach_rows_resolved"] += 1
            return
        breach_id = as_text(row.get("id"))
        key = name_key(row.get("asset_name"))
        if not breach_id or key == None:
            stats["breach_rows_unusable"] += 1
            return
        add_row(ctx["breaches"], key, {
            "id": breach_id,
            "policy_id": as_text(row.get("policy_id")),
            "policy_name": as_text(row.get("policy_name")),
            "asset_id": as_text(row.get("asset_id")),
            "asset_name": as_text(row.get("asset_name")),
            "severity": as_text(row.get("severity")).upper(),
            "status": as_text(row.get("status")).upper(),
            "status_updated_at": as_text(row.get("status_updated_at")),
            "first_seen_at": as_text(row.get("first_seen_at")),
        })

    return walk_v3(ctx, "breaches", "/breaches", {}, on_row)


def slim_vulnerability(row):
    """Keep only what the mapping needs from a v2 vulnerability. A raw row can
    carry request and response bodies and several kilobytes of rendered
    details, which must not sit in memory for the length of the run."""
    definition = as_dict(row.get("definition"))
    scores = as_dict(row.get("cvss_scores"))
    cvss2 = as_dict(scores.get("cvss_2_0"))
    cvss3 = as_dict(scores.get("cvss_3_1"))
    if not cvss3:
        cvss3 = as_dict(scores.get("cvss_3_0"))
    owasp = []
    for entry in dicts(row.get("owasp")):
        classification = as_text(entry.get("classification"))
        year = as_int(entry.get("year"))
        if classification:
            owasp.append("{} ({})".format(classification, year) if year else classification)
    references = []
    reference_names = []
    for ref in dicts(row.get("references")):
        name = as_text(ref.get("name"))
        link = as_text(ref.get("link"))
        reference_names.append(name)
        if name and link:
            references.append("{} <{}>".format(name, link))
        elif name or link:
            references.append(name or link)
    title = as_text(row.get("title")) or as_text(definition.get("title"))
    description = as_text(definition.get("description"))
    # Only the finding's own text and explicitly named references identify the
    # CVE; a link can mention a related CVE without the finding being it.
    cves = dedupe([c.upper() for c in re_find_all(CVE_PATTERN, " ".join([title, description] + reference_names))])
    location = as_text(row.get("location"))
    port = 0
    parsed = url_parse(location) if location else None
    if parsed != None:
        port = as_int(parsed.port)
        if port == 0:
            port = {"https": 443, "http": 80}.get(parsed.scheme.lower(), 0)
    cvss3_score = as_float(cvss3.get("score"), default=-1.0)
    cvss2_score = as_float(cvss2.get("score"), default=-1.0)
    severity = as_text(cvss3.get("severity")) or as_text(cvss2.get("severity")) or as_text(row.get("severity"))
    asset = as_dict(row.get("asset"))
    return {
        "id": as_text(row.get("uuid")),
        "title": title,
        "description": description,
        "status": as_text(row.get("status")),
        "scan_source": as_text(as_dict(row.get("source")).get("value")) or as_text(row.get("scan_source")),
        "host": as_text(row.get("host")),
        "location": location,
        "port": port,
        "cwe": as_int(row.get("cwe")),
        "owasp": owasp,
        "references": references,
        "cves": cves,
        "cvss2_score": cvss2_score if cvss2_score >= 0 else None,
        "cvss2_vector": as_text(cvss2.get("vector")),
        "cvss3_score": cvss3_score if cvss3_score >= 0 else None,
        "cvss3_vector": as_text(cvss3.get("vector")),
        "severity": severity,
        "created_at": as_text(row.get("created_at")),
        "updated_at": as_text(row.get("updated_at")),
        "modified_at": as_text(row.get("modified_at")),
        "tags": [as_text(t.get("name")) for t in dicts(row.get("tags")) if as_text(t.get("name"))],
        "asset_token": as_text(row.get("asset_token")) or as_text(asset.get("token")),
        "asset_name": as_text(asset.get("name")),
        "scan_profile_token": as_text(row.get("scan_profile_token")),
        "details_page": as_text(as_dict(row.get("links")).get("details_page")),
        "is_crowdsourced": as_bool(definition.get("is_crowdsourced")),
        "module_version": as_text(definition.get("module_version")),
    }


def collect_vulnerabilities(ctx):
    stats = ctx["stats"]

    def on_row(row):
        stats["vuln_rows"] += 1
        vuln = slim_vulnerability(row)
        key = name_key(vuln["host"])
        if key == None:
            key = name_key(vuln["asset_name"])
        if not vuln["id"] or key == None:
            stats["vuln_rows_unusable"] += 1
            return
        add_row(ctx["vulns"], key, vuln)

    return walk_v2_vulnerabilities(ctx, on_row)


def collect_settings(ctx):
    """Surface Monitoring settings are filed by asset id. The listings take an
    optional team_id that the documentation says may be omitted only for a
    single-team key, so they are walked once per team seen in the IP list."""
    stats = ctx["stats"]

    def on_row(row):
        asset_id = as_text(row.get("asset_id"))
        if not asset_id:
            return
        stats["settings_rows"] += 1
        settings = {}
        for flag in MONITORING_FLAGS:
            if flag in row:
                settings[flag] = as_bool(row.get(flag))
        settings["inherited"] = as_bool(row.get("inherited"))
        ctx["settings"][asset_id] = settings

    teams = sorted(ctx["teams_seen"].keys())
    if not teams:
        return 0, None
    rows = 0
    for path in ["/surface-monitoring/domains", "/surface-monitoring/ips"]:
        for team in teams:
            # team_id is always sent: it is optional for a single-team key and
            # required for a multi-team one, and the team is known either way.
            count, err = walk_v3(ctx, "monitoring-settings", path, {"team_id": team}, on_row)
            rows += count
            if err:
                return rows, err
    return rows, None


# -------------------------
# Asset construction
# -------------------------

def build_services(ctx, ip, port_entries, techs):
    """One service per open port on the address, keyed for ordering by port
    number. A port that only shows up as the port of a fingerprinted technology
    is a service too: Detectify answered on it, it just was not (or could not
    be) port-scanned."""
    techs_by_port = {}
    for tech in techs:
        if tech["port"] != None:
            add_row(techs_by_port, tech["port"], tech)
    ports = dict(port_entries)
    for port in techs_by_port:
        if port not in ports:
            ports[port] = {"port": port, "domains": {}, "ids": [], "first_seen": None, "inferred": True}
    services = []
    for port in sorted(ports.keys()):
        entry = ports[port]
        port_techs = techs_by_port.get(port, [])
        labels = dedupe([tech_label(t) for t in port_techs])
        domains = dict(entry["domains"])
        by_protocol = {}
        for tech in port_techs:
            if tech["domain"]:
                domains[tech["domain"]] = True
            if tech["protocol"]:
                add_row(by_protocol, tech["protocol"], tech_label(tech))
        source = "technology" if entry.get("inferred") else "port-scan"
        protocol_data = [ServiceProtocolData(name="detectify", attributes=to_custom_attributes({
            "source": source,
            "status": "" if entry.get("inferred") else "OPEN",
            "firstSeenAt": slot_raw(entry["first_seen"]),
            "domains": sorted(domains.keys()),
            "portIds": entry["ids"],
        }))]
        for protocol in sorted(by_protocol.keys()):
            protocol_data.append(ServiceProtocolData(name=protocol, attributes=to_custom_attributes({
                "technologies": dedupe(by_protocol[protocol]),
            })))
        args = {
            "address": ip,
            "port": port,
            "transport": TRANSPORT,
            "protocolData": protocol_data,
            "customAttributes": to_custom_attributes({
                "source": source,
                "domains": sorted(domains.keys()),
                "technologies": labels,
                "firstSeenAt": slot_raw(entry["first_seen"]),
            }),
        }
        product = pick_service_product(port_techs)
        if product != None:
            args["product"] = product["name"][:256]
            if product["version"]:
                args["version"] = product["version"][:256]
        services.append((("00000" + str(port))[-5:], Service(**args)))
        if not entry.get("inferred"):
            ctx["stats"]["services_scanned"] += 1
        else:
            ctx["stats"]["services_inferred"] += 1
    return services


def pick_service_product(port_techs):
    """The technology that names the listener: a web server when one is
    fingerprinted, otherwise the only technology on the port. Anything more
    ambiguous stays in the software inventory only."""
    for tech in port_techs:
        for category in tech["categories"]:
            if category.lower() in ("web servers", "web server", "reverse proxies", "reverse proxy"):
                return tech
    if len(port_techs) == 1:
        return port_techs[0]
    return None


def build_software(ip, techs):
    """One software row per distinct technology/version/port/protocol on the
    address, listing the domains it was fingerprinted on. Detectify reports a
    technology per domain, so the same nginx behind twenty names collapses to
    one row rather than twenty."""
    by_key = {}
    for tech in techs:
        key = "{}|{}|{}|{}".format(tech["name"], tech["version"], tech["port"] if tech["port"] != None else "", tech["protocol"])
        entry = by_key.get(key)
        if entry == None:
            entry = {"tech": tech, "domains": {}, "ids": [], "categories": {}, "active": False, "first_seen": None}
            by_key[key] = entry
        if tech["domain"]:
            entry["domains"][tech["domain"]] = True
        if tech["id"]:
            entry["ids"].append(tech["id"])
        for category in tech["categories"]:
            entry["categories"][category] = True
        entry["active"] = entry["active"] or tech["active"]
        entry["first_seen"] = keep_earliest(entry["first_seen"], tech["first_seen_at"])
    software = []
    for key in sorted(by_key.keys()):
        entry = by_key[key]
        tech = entry["tech"]
        args = {
            "id": key[:256],
            "product": tech["name"][:128],
            "serviceAddress": ip,
            "serviceTransport": TRANSPORT,
            "customAttributes": to_custom_attributes({
                "categories": sorted(entry["categories"].keys()),
                "domains": sorted(entry["domains"].keys()),
                "serviceProtocol": tech["protocol"],
                "active": entry["active"],
                "firstSeenAt": slot_raw(entry["first_seen"]),
                "technologyIds": entry["ids"],
            }),
        }
        if tech["version"]:
            args["version"] = tech["version"][:128]
        if tech["port"] != None:
            args["servicePort"] = tech["port"]
        # Active technologies sort ahead of disappeared ones when the cap bites.
        software.append(("{}|{}".format("0" if entry["active"] else "1", key), Software(**args)))
    return software


def severity_sort_key(rank, unique):
    """Most severe first: rank 4 sorts as 0. Unrated findings sort last."""
    return "{}|{}".format(9 - rank if rank != None else 9, unique)


def build_breach_vulns(ip, breaches):
    vulns = []
    for breach in breaches:
        rank = severity_rank(breach["severity"])
        status = breach["status"] or "ACTIVE"
        description = "Detectify Surface Monitoring policy \"{}\" is in breach on {} (status {}, first seen {}).".format(
            breach["policy_name"], breach["asset_name"], status, breach["first_seen_at"] or "unknown")
        args = {
            "id": "breach:{}".format(breach["id"]),
            "name": ("Policy breach: " + breach["policy_name"])[:256],
            "description": description[:1024],
            "category": "Detectify policy breach",
            "serviceAddress": ip,
            "customAttributes": to_custom_attributes({
                "breachId": breach["id"],
                "policyId": breach["policy_id"],
                "policyName": breach["policy_name"],
                "assetId": breach["asset_id"],
                "assetName": breach["asset_name"],
                "severity": breach["severity"],
                "status": status,
                "statusUpdatedAt": breach["status_updated_at"],
                "firstSeenAt": breach["first_seen_at"],
            }),
        }
        if rank != None:
            args["severityRank"] = rank
        first_seen = parse_ts(breach["first_seen_at"])
        if first_seen != None:
            args["firstDetectedTS"] = first_seen
        vulns.append((severity_sort_key(rank, "breach:" + breach["id"]), Vulnerability(**args)))
    return vulns


def build_finding_vulns(ip, findings):
    vulns = []
    for finding in findings:
        rank = severity_rank(finding["severity"])
        category = finding["owasp"][0] if finding["owasp"] else ""
        if not category and finding["cwe"]:
            category = "CWE-{}".format(finding["cwe"])
        args = {
            "id": finding["id"],
            "name": (finding["title"] or "Detectify finding")[:256],
            "description": finding["description"][:1024],
            "serviceAddress": ip,
            "serviceTransport": TRANSPORT,
            "customAttributes": to_custom_attributes({
                "uuid": finding["id"],
                "status": finding["status"],
                "scanSource": finding["scan_source"],
                "host": finding["host"],
                "location": finding["location"],
                "cwe": finding["cwe"] if finding["cwe"] else "",
                "owasp": finding["owasp"],
                "cves": finding["cves"],
                "cvss2Vector": finding["cvss2_vector"],
                "cvss3Vector": finding["cvss3_vector"],
                "severity": finding["severity"],
                "tags": finding["tags"],
                "assetToken": finding["asset_token"],
                "assetName": finding["asset_name"],
                "scanProfileToken": finding["scan_profile_token"],
                "detailsPage": finding["details_page"],
                "isCrowdsourced": finding["is_crowdsourced"],
                "moduleVersion": finding["module_version"],
                "references": finding["references"],
                "createdAt": finding["created_at"],
                "updatedAt": finding["updated_at"],
                "modifiedAt": finding["modified_at"],
            }),
        }
        if category:
            args["category"] = category[:256]
        if finding["port"]:
            args["servicePort"] = finding["port"]
        if finding["cves"]:
            args["cve"] = finding["cves"][0]
        if finding["cvss3_score"] != None:
            args["cvss3BaseScore"] = finding["cvss3_score"]
            args["severityScore"] = finding["cvss3_score"]
        if finding["cvss2_score"] != None:
            args["cvss2BaseScore"] = finding["cvss2_score"]
            if finding["cvss3_score"] == None:
                args["severityScore"] = finding["cvss2_score"]
        if rank != None:
            args["severityRank"] = rank
        first_detected = parse_ts(finding["created_at"])
        if first_detected != None:
            args["firstDetectedTS"] = first_detected
        last_detected = parse_ts(finding["updated_at"])
        if last_detected != None:
            args["lastDetectedTS"] = last_detected
        vulns.append((severity_sort_key(rank, finding["id"]), Vulnerability(**args)))
    return vulns


def highest_severity(ranks):
    best = None
    for rank in ranks:
        if rank != None and (best == None or rank > best):
            best = rank
    return severity_name(best)


def build_asset(ctx, host):
    ip = host["ip"]
    active_domains = sorted([d for d in host["domains"] if host["domains"][d]])
    inactive_domains = sorted([d for d in host["domains"] if not host["domains"][d]])
    keys = [ip] + active_domains + inactive_domains
    for key in keys:
        ctx["attached_keys"][key] = True

    techs = gather(ctx["techs"], keys)
    breaches = gather(ctx["breaches"], keys)
    findings = gather(ctx["vulns"], keys)
    services, services_cut = ordered(build_services(ctx, ip, ctx["ports"].get(ip, {}), techs), CHILD_CAP)
    software, software_cut = ordered(build_software(ip, techs), CHILD_CAP)
    vulnerabilities, vulns_cut = ordered(build_breach_vulns(ip, breaches) + build_finding_vulns(ip, findings), CHILD_CAP)
    if services_cut or software_cut or vulns_cut:
        ctx["stats"]["assets_truncated"] += 1

    categories = {}
    for tech in techs:
        for category in tech["categories"]:
            categories[category] = True
    finding_counts = {}
    for finding in findings:
        name = severity_name(severity_rank(finding["severity"])) or "unrated"
        finding_counts[name] = finding_counts.get(name, 0) + 1

    attrs = {
        "ipAddress": ip,
        "ipVersion": "IPv{}".format(ip_address(ip).version),
        "active": host["active"],
        "enriched": host["enriched"],
        "firstSeenAt": slot_raw(host["first_seen"]),
        "disappearedAt": slot_raw(host["disappeared"]) if not host["active"] else "",
        "domains": active_domains,
        "inactiveDomains": inactive_domains,
        "domainCount": len(host["domains"]),
        "activeDomainCount": len(active_domains),
        "inactiveDomainCount": len(inactive_domains),
        "assetIds": sorted(host["asset_ids"].keys()),
        "teamIds": sorted(host["team_ids"].keys()),
        "ipRecordIds": sorted(host["record_ids"].keys()),
        "asn.number": as_int(host["asn"].get("number")) if host["asn"] else "",
        "asn.name": host["asn"].get("name"),
        "asn.domain": host["asn"].get("domain"),
        "geolocation.continent": host["geo"].get("continent"),
        "geolocation.continentName": host["geo"].get("continent_name"),
        "geolocation.country": host["geo"].get("country"),
        "geolocation.countryName": host["geo"].get("country_name"),
        "openPortCount": len(ctx["ports"].get(ip, {})),
        "openPorts": sorted(ctx["ports"].get(ip, {}).keys()),
        "serviceCount": len(services) + services_cut,
        "servicesOmitted": services_cut if services_cut else "",
        "technologyCount": len(software) + software_cut,
        "technologiesOmitted": software_cut if software_cut else "",
        "technologies": dedupe([tech_label(t) for t in techs]),
        "technologyCategories": sorted(categories.keys()),
        "breachCount": len(breaches),
        "breachHighestSeverity": highest_severity([severity_rank(b["severity"]) for b in breaches]),
        "breachPolicies": dedupe([b["policy_name"] for b in breaches]),
        "vulnerabilityCount": len(findings),
        "vulnerabilityHighestSeverity": highest_severity([severity_rank(f["severity"]) for f in findings]),
        "vulnerabilitiesOmitted": vulns_cut if vulns_cut else "",
    }
    for name in SEVERITY_NAMES + ["unrated"]:
        attrs["vulnerabilities." + name] = finding_counts.get(name, 0) if findings else ""

    if ctx["include_settings"]:
        linked = [ctx["settings"][a] for a in sorted(host["asset_ids"].keys()) if a in ctx["settings"]]
        attrs["monitoring.linkedAssets"] = len(host["asset_ids"])
        attrs["monitoring.assetsWithSettings"] = len(linked)
        if linked:
            for flag in MONITORING_FLAGS:
                values = [s[flag] for s in linked if flag in s]
                if values:
                    attrs["monitoring." + camel(flag)] = True in values
            attrs["monitoring.inheritedSettings"] = len([s for s in linked if s["inherited"]])

    nic = network_interface(ips=[ip])
    return ImportAsset(
        id="detectify:ip:{}".format(ip),
        hostnames=(active_domains + inactive_domains)[:MAX_HOSTNAMES],
        networkInterfaces=[nic] if nic else [],
        firstSeenTS=slot_ts(host["first_seen"]),
        services=services,
        software=software,
        vulnerabilities=vulnerabilities,
        customAttributes=to_custom_attributes(attrs),
    )


# -------------------------
# Entry point
# -------------------------

def new_stats():
    return {
        "ip_rows": 0, "ip_rows_other_team": 0, "ip_rows_inactive": 0, "ip_rows_unusable": 0, "ip_rows_private": 0,
        "port_rows": 0, "port_rows_other_team": 0, "port_rows_closed": 0, "port_rows_unusable": 0, "port_rows_orphan": 0,
        "tech_rows": 0, "tech_rows_other_team": 0, "tech_rows_inactive": 0, "tech_rows_unusable": 0,
        "breach_rows": 0, "breach_rows_resolved": 0, "breach_rows_unusable": 0,
        "vuln_rows": 0, "vuln_rows_unusable": 0,
        "settings_rows": 0,
        "services_scanned": 0, "services_inferred": 0,
        "assets_truncated": 0,
    }


def unattached(index, attached):
    count = 0
    for key in index:
        if key not in attached:
            count += len(index[key])
    return count


def optional_walk(ctx, label, enabled, collector):
    """Run one enrichment listing. A failure here degrades the import rather
    than ending it: the addresses are already known, this only decorates them."""
    if not ctx["hosts"]:
        return
    if not enabled:
        print("detectify: {} not imported (disabled by parameter)".format(label))
        return
    rows, err = collector(ctx)
    if err:
        auth_hint(err)
        print("detectify: could not read {}: {} -- continuing without them".format(label, err))
        return
    print("detectify: read {} {}".format(rows, label))


def main(*args, **kwargs):
    require(kwargs, "api_key")
    api_key = get_string(kwargs, "api_key")
    api_key_v2 = get_string(kwargs, "api_key_v2", default="") or api_key
    base = (get_string(kwargs, "url", default="") or DEFAULT_URL).rstrip("/")
    # Accept a pasted versioned URL as well as the bare host.
    for suffix in [V3_PATH, V2_PATH, "/rest"]:
        if base.endswith(suffix):
            base = base[:-len(suffix)]
    if not base.startswith("http://") and not base.startswith("https://"):
        fail("detectify: the Detectify API URL must start with http:// or https://")

    ctx = {
        "v3_base": base + V3_PATH,
        "v2_base": base + V2_PATH,
        # The v3 API takes the raw key in Authorization; v2 takes it in X-Detectify-Key.
        "v3_http": get_http_options(kwargs, headers={"Accept": "application/json", "Authorization": api_key}),
        "v2_http": get_http_options(kwargs, headers={"Accept": "application/json", "X-Detectify-Key": api_key_v2}),
        "team_filter": {t: True for t in dedupe(get_list(kwargs, "team_ids", default=[]))},
        "include_inactive": get_bool(kwargs, "include_inactive", default=False),
        "include_private": get_bool(kwargs, "include_private_ips", default=False),
        "include_settings": get_bool(kwargs, "include_monitoring_settings", default=True),
        "hosts": {},
        "teams_seen": {},
        "ports": {},
        "techs": {},
        "breaches": {},
        "vulns": {},
        "settings": {},
        "attached_keys": {},
        "stats": new_stats(),
    }
    stats = ctx["stats"]

    progress_report(0, "reading IP addresses")
    rows, err = collect_ips(ctx)
    if err:
        auth_hint(err)
        fail("detectify: could not read the IP address list: {}".format(err))
    print("detectify: read {} IP address records covering {} addresses".format(rows, len(ctx["hosts"])))
    skipped = []
    if stats["ip_rows_other_team"]:
        skipped.append("{} outside the team filter".format(stats["ip_rows_other_team"]))
    if stats["ip_rows_inactive"]:
        skipped.append("{} no longer active (include_inactive is off)".format(stats["ip_rows_inactive"]))
    if stats["ip_rows_private"]:
        skipped.append("{} on private addresses (include_private_ips is off)".format(stats["ip_rows_private"]))
    if stats["ip_rows_unusable"]:
        skipped.append("{} with no usable address".format(stats["ip_rows_unusable"]))
    if skipped:
        print("detectify: skipped IP address records: {}".format(", ".join(skipped)))
    if not ctx["hosts"]:
        print("detectify: no IP addresses to import")

    progress_report(20, "reading open ports")
    optional_walk(ctx, "open ports", get_bool(kwargs, "include_ports", default=True), collect_ports)
    progress_report(35, "reading technologies")
    optional_walk(ctx, "technologies", get_bool(kwargs, "include_technologies", default=True), collect_technologies)
    progress_report(50, "reading policy breaches")
    optional_walk(ctx, "policy breaches", get_bool(kwargs, "include_breaches", default=True), collect_breaches)
    progress_report(60, "reading vulnerabilities")
    optional_walk(ctx, "vulnerabilities", get_bool(kwargs, "include_vulnerabilities", default=True), collect_vulnerabilities)
    progress_report(75, "reading monitoring settings")
    optional_walk(ctx, "monitoring settings", ctx["include_settings"], collect_settings)

    progress_report(80, "building assets")
    reported = 0
    total = len(ctx["hosts"])
    for index, ip in enumerate(sorted(ctx["hosts"].keys())):
        reported += report_asset(build_asset(ctx, ctx["hosts"][ip]))
        if total > 0 and index % 500 == 499:
            progress_report(80 + (index + 1) * 20 // total, "reported {} of {} assets".format(index + 1, total))

    notes = []
    if stats["port_rows_orphan"]:
        notes.append("{} open ports were on addresses that were not imported".format(stats["port_rows_orphan"]))
    if stats["port_rows_closed"] or stats["port_rows_unusable"]:
        notes.append("{} port records were closed or unusable".format(stats["port_rows_closed"] + stats["port_rows_unusable"]))
    orphan_techs = unattached(ctx["techs"], ctx["attached_keys"])
    if orphan_techs:
        notes.append("{} technologies belong to domains with no imported address".format(orphan_techs))
    if stats["tech_rows_inactive"]:
        notes.append("{} technologies are no longer active (include_inactive is off)".format(stats["tech_rows_inactive"]))
    orphan_breaches = unattached(ctx["breaches"], ctx["attached_keys"])
    if orphan_breaches:
        notes.append("{} active policy breaches belong to assets with no imported address".format(orphan_breaches))
    if stats["breach_rows_resolved"]:
        notes.append("{} policy breaches are resolved".format(stats["breach_rows_resolved"]))
    orphan_vulns = unattached(ctx["vulns"], ctx["attached_keys"])
    if orphan_vulns:
        notes.append("{} vulnerabilities belong to hosts with no imported address".format(orphan_vulns))
    unusable = stats["tech_rows_unusable"] + stats["breach_rows_unusable"] + stats["vuln_rows_unusable"]
    if unusable:
        notes.append("{} technology, breach, or vulnerability records had no usable id or host".format(unusable))
    if stats["assets_truncated"]:
        notes.append("{} assets had more than {} services, software, or vulnerabilities and were cut at the platform cap (see the *Omitted attributes)".format(
            stats["assets_truncated"], CHILD_CAP))
    if notes:
        print("detectify: not attached: {}".format("; ".join(notes)))

    print("detectify: reported {} assets with {} port-scanned and {} fingerprint-inferred services".format(
        reported, stats["services_scanned"], stats["services_inferred"]))
    progress_report(100, "done")
    return None

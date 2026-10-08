# This is a runZero Custom Integration, please see https://github.com/runZeroInc/runzero-custom-integrations for details.
# This script was generated with AI.

CONFIG = {
    "id": "runzero-nuclei-results",
    "name": "Nuclei Scan Results",
    "type": "inbound",
    "description": "Imports the findings of a standalone Nuclei scan (JSON Lines or JSON export) as vulnerabilities on the scanned hosts.",
    "version": "1",
    "maturity": "alpha",
    "minVersion": "5.1.260818.0",
    # Nuclei has no asset identity. A result names its target by the IP it
    # resolved and the hostname or URL it was given, and the same host can
    # change address between scans, so the id this script builds is only a
    # per-run label. runZero must correlate findings onto the assets it already
    # knows by IP and hostname rather than trusting that label.
    "matchBehavior": "no-id-match no-id-break",
    "params": [
        {
            "key": "results_url",
            "label": "Nuclei results URL",
            "description": "Where the Explorer downloads the Nuclei output file: JSON Lines written by `nuclei -jsonl -o results.jsonl`, or the JSON array written by `nuclei -je results.json`. A gzip-compressed copy of either is accepted.",
            "type": "url",
            "required": True,
            "placeholder": "https://files.example.com/nuclei/latest.jsonl",
        },
        {
            "key": "auth_type",
            "label": "Download authentication",
            "description": "How the results server authenticates the download. Pre-signed URLs (S3, Azure Blob, GCS) need `none`.",
            "type": "enum",
            "required": False,
            "default": "none",
            "options": ["none", "bearer", "basic"],
        },
        {
            "key": "bearer_token",
            "label": "Bearer token",
            "description": "Sent as `Authorization: Bearer <token>`.",
            "type": "secret",
            "required": False,
            "visibleIf": "auth_type",
            "visibleIfValue": "bearer",
            "requiredIf": "auth_type",
            "requiredIfValue": "bearer",
        },
        {
            "key": "username",
            "label": "Username",
            "type": "string",
            "required": False,
            "visibleIf": "auth_type",
            "visibleIfValue": "basic",
            "requiredIf": "auth_type",
            "requiredIfValue": "basic",
        },
        {
            "key": "password",
            "label": "Password",
            "type": "secret",
            "required": False,
            "visibleIf": "auth_type",
            "visibleIfValue": "basic",
            "requiredIf": "auth_type",
            "requiredIfValue": "basic",
        },
        {
            "key": "min_severity",
            "label": "Minimum severity to import",
            "description": "Results below this Nuclei severity are not imported. `info` imports everything the scan reported, including technology detections.",
            "type": "enum",
            "required": False,
            "default": "info",
            "options": ["info", "low", "medium", "high", "critical"],
        },
        {
            "key": "download_timeout",
            "label": "Download timeout (seconds)",
            "description": "How long to wait for the results file. Raise it for very large scans or slow links.",
            "type": "int",
            "required": False,
            "default": 120,
            "min": 10,
            "max": 3600,
        },
    ],
    "includes": {
        "tls_": OPTIONS_TLS,
        "http_": OPTIONS_HTTP,
    },
}

load("runzero.types", "ImportAsset", "Vulnerability", "to_custom_attributes")
load("net", "network_interface", "routable_ip", "clean_hostname")
load("coerce", "as_text", "as_dict", "as_list", "dicts", "as_int", "as_float", "dedupe")
load("http", http_get="get", "url_parse", "bearer", "basic")
load("kwargs", "require", "get_string", "get_int", "get_http_options")
load("json", json_decode="decode")
load("gzip", gzip_decompress="decompress")
load("re", re_match="match")
load("time", "parse_ts", "sleep")

# Nuclei severities map onto runZero's 0 (info) to 4 (critical) ranks. The
# scores mirror what runZero's own Nuclei engine records for a template match.
SEVERITY_RANK = {"unknown": 0, "info": 0, "low": 1, "medium": 2, "high": 3, "critical": 4}
RANK_NAMES = ["info", "low", "medium", "high", "critical"]

# runZero's own CVE pattern. A CVE that fails it rejects the whole asset, not
# just the field, so anything else stays in the cve_ids attribute instead.
CVE_PATTERN = "^CVE-[0-9]{4}-[0-9]{4,19}$"

# The raw http.get builtin has no retries argument (the body is not JSON, so
# get_json cannot be used). The download is a read, so repeating it is safe.
DOWNLOAD_ATTEMPTS = 3
RETRY_STATUSES = [408, 425, 429, 500, 502, 503, 504]
RETRY_DELAY = "3s"

RFC3339 = "2006-01-02T15:04:05Z07:00"


def trunc(value, limit):
    """Clamp a text field to the platform limit; None when there is nothing to send."""
    text = as_text(value)
    if not text:
        return None
    return text[:limit]


def cve_ids(value):
    """Upper-cased CVE ids in the shape runZero accepts, in the order Nuclei listed them."""
    out = []
    for cve in dedupe(as_list(value)):
        cve = cve.upper()
        if re_match(CVE_PATTERN, cve) and cve not in out:
            out.append(cve)
    return out


def auth_headers(kwargs):
    headers = {"Accept": "application/x-ndjson, application/json, */*"}
    auth_type = get_string(kwargs, "auth_type", default="none")
    if auth_type == "bearer":
        require(kwargs, "bearer_token")
        headers["Authorization"] = bearer(get_string(kwargs, "bearer_token"))
    elif auth_type == "basic":
        require(kwargs, "username", "password")
        headers["Authorization"] = basic(get_string(kwargs, "username"), get_string(kwargs, "password"))
    return headers


def download(url, options):
    """Fetch the results file. Returns (body, err); only transient statuses are retried."""
    for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
        response = http_get(url, **options)
        if response.status_code >= 200 and response.status_code < 300:
            return response.body, None
        if attempt == DOWNLOAD_ATTEMPTS or response.status_code not in RETRY_STATUSES:
            return None, "status {}".format(response.status_code)
        print("nuclei: download attempt {} returned status {}; retrying in {}".format(attempt, response.status_code, RETRY_DELAY))
        sleep(RETRY_DELAY)


def parse_results(text):
    """Split Nuclei output into result dicts. Returns (records, unreadable_count)."""
    if text.startswith("["):
        data = json_decode(text, default=None)
        if type(data) != "list":
            return [], 1
        records = dicts(data)
        return records, len(data) - len(records)
    records = []
    unreadable = 0
    # Decoded line by line rather than with jsonstream.iter_lines, which stops
    # silently at the first bad line and would truncate the import unnoticed.
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        record = json_decode(line, default=None)
        if type(record) == "dict":
            records.append(record)
        else:
            unreadable += 1
    return records, unreadable


def target_identity(record):
    """Return (key, ip, hostname) for the host a result describes, or None.

    `ip` is the address Nuclei resolved. `host` is the target as it was given
    to Nuclei (hostname, IP, host:port, or URL) and is the only place a name
    lives. Loopback and link-local addresses identify nothing and are dropped.
    """
    ip = routable_ip(record.get("ip"))
    target = as_text(record.get("host"))
    if "://" in target:
        parsed = url_parse(target)
        target = parsed.hostname if parsed else ""
    else:
        target = target.split("/")[0]
        if target.count(":") == 1:
            target = target.split(":")[0]
    target_ip = routable_ip(target)
    if target_ip and not ip:
        ip = target_ip
    name = None if target_ip else clean_hostname(target)
    if ip:
        return ip, ip, name
    if name:
        return name.lower(), None, name
    return None


def build_finding(record, info, ip, rank, severity):
    """Map one Nuclei result to a Vulnerability. Returns (dedupe_key, vuln, detected_at)."""
    classification = as_dict(info.get("classification"))
    template_id = as_text(record.get("template-id"))
    matcher = as_text(record.get("matcher-name"))
    vuln_id = template_id if not matcher else "{}:{}".format(template_id, matcher)

    port = as_int(record.get("port"))
    if port < 1 or port > 65535:
        port = 0
    target = as_text(record.get("host"))
    matched_at = as_text(record.get("matched-at"))
    transport = "udp" if ("udp://" in target or "udp://" in matched_at) else "tcp"

    cves = cve_ids(classification.get("cve-id"))
    cvss = as_float(classification.get("cvss-score"))
    metrics = as_text(classification.get("cvss-metrics"))
    # Templates write "CVSS:2.0/..." or the bare "2.0/..." the syntax reference
    # shows. runZero has no CVSS 4 field, so v3 and v4 scores share the v3 column.
    cvss2 = metrics.startswith("CVSS:2") or metrics.startswith("2.")
    cpe = as_text(classification.get("cpe"))
    detected = parse_ts(record.get("timestamp"))

    # Request, response and curl-command are deliberately left out: they can
    # carry the headers and cookies the scan authenticated with.
    attrs = to_custom_attributes({
        "template_id": template_id,
        "template_path": as_text(record.get("template-path")),
        "template_url": as_text(record.get("template-url")),
        "type": as_text(record.get("type")),
        "severity": severity,
        "matcher_name": matcher,
        "extractor_name": as_text(record.get("extractor-name")),
        "extracted_results": dedupe(as_list(record.get("extracted-results"))),
        "matched_at": matched_at,
        "url": as_text(record.get("url")),
        "target": target,
        "template_tags": dedupe(as_list(info.get("tags"))),
        "authors": dedupe(as_list(info.get("author"))),
        "references": dedupe(as_list(info.get("reference"))),
        "impact": as_text(info.get("impact")),
        "cve_ids": cves,
        "cwe_ids": [cwe.upper() for cwe in dedupe(as_list(classification.get("cwe-id")))],
        "cvss_metrics": metrics,
        "epss_score": classification.get("epss-score"),
        "epss_percentile": classification.get("epss-percentile"),
        "scanned_at": as_text(record.get("timestamp")),
    }, prefix="nuclei", list_join="\t")

    vuln = Vulnerability(
        id=trunc(vuln_id, 256),
        name=trunc(as_text(info.get("name")) or template_id, 256),
        category=trunc(record.get("type"), 256),
        description=trunc(info.get("description"), 1024),
        solution=trunc(info.get("remediation"), 1024),
        cve=cves[0] if cves else None,
        cpe23=cpe if cpe.startswith("cpe:") else None,
        serviceAddress=ip,
        servicePort=port if port else None,
        serviceTransport=transport if port else None,
        cvss2BaseScore=cvss if (cvss and cvss2) else None,
        cvss3BaseScore=cvss if (cvss and not cvss2) else None,
        severityRank=rank,
        severityScore=rank * 2.0,
        riskRank=rank,
        riskScore=rank * 2.0,
        # A template match is a confirmed condition on the live target. Info
        # results are detections (technologies, panels), not weaknesses.
        exploitable=rank > 0,
        firstDetectedTS=detected,
        lastDetectedTS=detected,
        customAttributes=attrs,
    )
    return "{}|{}|{}".format(vuln_id, port, transport), vuln, detected


def build_asset(scope, key, host):
    nic = network_interface(ips=[host["ip"]]) if host["ip"] else None
    return ImportAsset(
        id="nuclei:{}:{}".format(scope, key),
        hostnames=host["names"].values(),
        networkInterfaces=[nic] if nic else [],
        vulnerabilities=host["findings"].values(),
        customAttributes=to_custom_attributes({
            "findings": len(host["findings"]),
            "highest_severity": RANK_NAMES[host["max_rank"]],
            "targets": sorted(host["targets"].keys()),
            "last_scanned": host["last"].format(RFC3339) if host["last"] else None,
        }, prefix="nuclei", list_join="\t"),
    )


def main(*args, **kwargs):
    require(kwargs, "results_url")
    url = get_string(kwargs, "results_url")
    parsed = url_parse(url)
    scope = parsed.hostname if parsed else ""
    if not scope:
        fail("nuclei: the results URL has no host name")
    min_severity = get_string(kwargs, "min_severity", default="info")
    min_rank = SEVERITY_RANK[min_severity]

    options = get_http_options(kwargs, "http_", "tls_", auth_headers(kwargs))
    options["timeout"] = get_int(kwargs, "download_timeout", default=120)

    body, err = download(url, options)
    if err:
        if err in ("status 401", "status 403"):
            print("nuclei: the results server refused the download; check the Download authentication settings")
        fail("nuclei: could not download the results file: {}".format(err))

    # gzip.decompress raises on anything that is not a gzip member, so only a
    # body starting with the gzip magic byte is handed to it.
    if body[0:1] == "\x1f":
        body = str(gzip_decompress(body))
    text = body.strip("\ufeff \t\r\n")
    if not text:
        print("nuclei: the results file is empty; no findings to import")
        return None

    records, unreadable = parse_results(text)
    if not records:
        if unreadable:
            fail("nuclei: the results file contained no JSON records ({} unreadable lines); expected Nuclei JSON Lines (-jsonl) or JSON export (-je) output".format(unreadable))
        print("nuclei: the results file holds no results; no findings to import")
        return None

    # Findings are grouped per host before reporting because an asset has to
    # carry its complete finding list; one Nuclei line is one finding.
    hosts = {}
    no_template = 0
    below_threshold = 0
    no_target = 0
    duplicates = 0
    for record in records:
        info = as_dict(record.get("info"))
        template_id = as_text(record.get("template-id"))
        if not template_id:
            no_template += 1
            continue
        severity = as_text(info.get("severity")).lower() or "unknown"
        rank = SEVERITY_RANK.get(severity, 0)
        if rank < min_rank:
            below_threshold += 1
            continue
        identity = target_identity(record)
        if identity == None:
            no_target += 1
            continue
        key, ip, name = identity
        host = hosts.get(key)
        if host == None:
            host = {"ip": ip, "names": {}, "targets": {}, "findings": {}, "max_rank": 0, "last": None}
            hosts[key] = host
        if name:
            host["names"].setdefault(name.lower(), name)
        host["targets"][as_text(record.get("host")) or key] = True

        finding_key, vuln, detected = build_finding(record, info, ip, rank, severity)
        if finding_key in host["findings"]:
            duplicates += 1
            continue
        host["findings"][finding_key] = vuln
        host["max_rank"] = max(host["max_rank"], rank)
        if detected and (host["last"] == None or detected.unix > host["last"].unix):
            host["last"] = detected

    reported = 0
    findings = 0
    for key, host in hosts.items():
        reported += report_asset(build_asset(scope, key, host))
        findings += len(host["findings"])

    print("nuclei: reported {} hosts with {} findings from {} results".format(reported, findings, len(records)))
    print("nuclei: skipped {} results below the {} threshold, {} with no usable target, {} with no template id, {} duplicate findings, {} unreadable lines".format(
        below_threshold, min_severity, no_target, no_template, duplicates, unreadable))
    return None

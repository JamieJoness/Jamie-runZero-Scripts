# This is a runZero Custom Integration, please see https://github.com/runZeroInc/runzero-custom-integrations for details.

CONFIG = {
    "id": "runzero-microsoft-defender-av-health",
    "name": "Microsoft Defender AV Health",
    "type": "inbound",
    "description": "Imports per-device Microsoft Defender Antivirus health from Defender for Endpoint, including Microsoft's own platform/engine/signature up-to-date verdicts.",
    "version": "1",
    "maturity": "beta",
    "minVersion": "5.1.260818.0",
    # machineId is Microsoft's stable device id, so id-based matching stays on.
    # deviceavinfo carries no MAC or IP and hostnames drift, so none of those
    # may disqualify a merge into the asset this id matched last run.
    "matchBehavior": "no-mac-break no-ip-break no-name-break",
    # Attributes render as @<integration-name>.device.<attr> in asset search.
    "assetType": "device",
    # $top=10000 per page; 2000 pages is 20M rows, far past the JSON method's
    # documented sweet spot (<100K devices). The pager raises if this trips.
    "maxPages": 2000,
    "params": [
        {
            "key": "tenant_id",
            "label": "Microsoft Entra tenant ID",
            "type": "string",
            "required": True,
            "placeholder": "00000000-0000-0000-0000-000000000000",
            "description": "Directory (tenant) ID of the Entra application registration.",
        },
        {
            "key": "client_id",
            "label": "Application (client) ID",
            "type": "string",
            "required": True,
        },
        {
            "key": "client_secret",
            "label": "Client secret",
            "type": "secret",
            "required": True,
        },
        {
            "key": "login_url",
            "label": "Microsoft Entra login URL",
            "type": "url",
            "required": False,
            "default": "https://login.microsoftonline.com",
            "description": "Override for sovereign clouds, e.g. https://login.microsoftonline.us for GCC High/DoD.",
        },
        {
            "key": "api_url",
            "label": "Defender for Endpoint API URL",
            "type": "url",
            "required": False,
            "default": "https://api.security.microsoft.com",
            "description": "Override for sovereign clouds, e.g. https://api.securitycenter.microsoft.us for GCC High.",
        },
        {
            "key": "token_scope",
            "label": "OAuth token scope",
            "type": "string",
            "required": False,
            "default": "https://api.securitycenter.microsoft.com/.default",
            "description": "Microsoft requires the legacy securitycenter resource even when calling api.security.microsoft.com. Override to match the sovereign-cloud resource when api_url is overridden.",
        },
        {
            "key": "page_size",
            "label": "Page size ($top)",
            "type": "int",
            "required": False,
            "default": 10000,
            "min": 1,
            "max": 10000,
            "description": "Rows requested per page. Microsoft caps $top at 10,000.",
        },
    ],
    "includes": {
        "tls_": OPTIONS_TLS,
        "http_": OPTIONS_HTTP,
    },
}

load("runzero.types", "ImportAsset")
load("http", "get_json", "oauth2_token", "url_encode")
load("kwargs", "require", "get_string", "get_int", "get_url_base", "get_http_options")
load("coerce", "as_text", "as_dict", "dicts")
load("net", "clean_hostnames")
load("time", "parse_ts")

# Decoded exactly as the native Microsoft 365 Defender integration decodes the
# same field. Microsoft documents "" as Other.
AV_MODES = {
    "0": "Active",
    "1": "Passive",
    "2": "Disabled",
    "3": "Other",
    "": "Other",
    "4": "EDRBlocked",
    "5": "PassiveAudit",
}

def norm_flag(value):
    """Normalize Microsoft's string verdicts ("True"/"False"/"Unknown") to a
    stable lowercase query surface. Anything that is not exactly true or false
    lands in the unknown bucket rather than being guessed compliant or not;
    the verbatim value is preserved separately in the raw attribute."""
    t = as_text(value).strip().lower()
    if t == "true":
        return "true"
    if t == "false":
        return "false"
    return "unknown"

def put(attrs, key, value):
    t = as_text(value).strip()
    if t:
        attrs[key] = t

def put_ts(attrs, key, value):
    ts = parse_ts(value)
    if ts:
        attrs[key] = str(ts.unix)

def build_asset(row, tenant_id):
    """Map one deviceavinfo row to an ImportAsset, or None when the row lacks
    the identity or the hostname runZero needs to correlate it."""
    machine_id = as_text(row.get("machineId")).strip()
    if not machine_id:
        print("defender-av-health: skipping row with no machineId")
        return None

    hostnames = clean_hostnames([row.get("computerDnsName")])
    if not hostnames:
        # deviceavinfo has no MAC or IP, so a row with no usable hostname has
        # nothing runZero can correlate on and would only create an orphan.
        print("defender-av-health: skipping machine {} with no usable hostname".format(machine_id))
        return None

    attrs = {}

    # Microsoft's verdicts, normalized for permanent saved queries.
    attrs["platformUpToDate"] = norm_flag(row.get("avIsPlatformUpToDate"))
    attrs["engineUpToDate"] = norm_flag(row.get("avIsEngineUpToDate"))
    attrs["signatureUpToDate"] = norm_flag(row.get("avIsSignatureUpToDate"))
    attrs["avModeName"] = AV_MODES.get(as_text(row.get("avMode")).strip(), "Unknown")

    # Raw values, verbatim, under Microsoft's own field names.
    for key in (
        "machineId", "computerDnsName", "osKind", "osPlatform", "osVersion",
        "avMode", "avPlatformVersion", "avEngineVersion", "avSignatureVersion",
        "avIsPlatformUpToDate", "avIsEngineUpToDate", "avIsSignatureUpToDate",
        "avPlatformUpdateTime", "avEngineUpdateTime", "avSignatureUpdateTime",
        "avSignaturePublishTime", "quickScanResult", "quickScanError",
        "quickScanTime", "fullScanResult", "fullScanError", "fullScanTime",
        "dataRefreshTimestamp", "lastSeenTime", "rbacGroupName", "rbacGroupId",
    ):
        put(attrs, key, row.get(key))

    # Epoch forms so freshness is queryable with relative terms (:>7days).
    put_ts(attrs, "dataRefreshTS", row.get("dataRefreshTimestamp"))
    put_ts(attrs, "lastSeenTS", row.get("lastSeenTime"))

    return ImportAsset(
        id="microsoft-defender-av-health:{}:{}".format(tenant_id, machine_id),
        hostnames=hostnames,
        os=as_text(row.get("osPlatform")),
        osVersion=as_text(row.get("osVersion")),
        lastSeenTS=parse_ts(row.get("lastSeenTime")),
        customAttributes=attrs,
    )

def main(**kwargs):
    require(kwargs, "tenant_id", "client_id", "client_secret")
    tenant_id = get_string(kwargs, "tenant_id").strip()
    client_id = get_string(kwargs, "client_id")
    client_secret = get_string(kwargs, "client_secret")
    login_base = get_url_base(kwargs, "login_url")
    api_base = get_url_base(kwargs, "api_url")
    token_scope = get_string(kwargs, "token_scope")
    page_size = get_int(kwargs, "page_size", default=10000)
    http_options = get_http_options(kwargs)

    # Client-credentials exchange. oauth2_token raises on a non-2xx or a
    # missing access_token, which ends the task in error with the status line
    # and never echoes the secret.
    token = oauth2_token(
        "{}/{}/oauth2/v2.0/token".format(login_base, tenant_id),
        client_id=client_id,
        client_secret=client_secret,
        scope=token_scope,
        **http_options
    )
    if not token:
        fail("defender-av-health: the token endpoint returned no access token")

    http_options = get_http_options(kwargs, headers={"Authorization": "Bearer " + token})

    url = "{}/api/deviceavinfo?{}".format(api_base, url_encode({"$top": page_size}))
    seen = {}
    rows_total = 0
    reported = 0
    skipped = 0
    duplicates = 0
    walk_err = None

    p = pager("deviceavinfo")
    while p.next():
        # get_json retries 429 (honoring Retry-After) and transient 5xx with
        # backoff by default; what comes back as err is already past retries.
        data, err = get_json(url, **http_options)
        if err:
            walk_err = "page {} failed after reporting {} assets: {}".format(p.page, reported, err)
            break

        rows = dicts(as_dict(data).get("value"))
        rows_total += len(rows)
        for row in rows:
            machine_id = as_text(row.get("machineId")).strip()
            if machine_id and machine_id in seen:
                # deviceavinfo emits a row per (DeviceId, ConfigurationId), so
                # one device can appear more than once. The first row wins;
                # reporting the same id twice in one run is worse than either.
                duplicates += 1
                continue
            asset = build_asset(row, tenant_id)
            if asset == None:
                skipped += 1
                continue
            seen[machine_id] = True
            reported += report_asset(asset)

        next_link = as_text(as_dict(data).get("@odata.nextLink"))
        if not next_link:
            break
        if not next_link.startswith(api_base):
            # Never send the bearer token to a host other than the configured
            # API. If Microsoft legitimately answers from a different host,
            # point api_url at it.
            walk_err = "page {} returned an @odata.nextLink outside {}; refusing to follow it".format(p.page, api_base)
            break
        url = next_link

    print("defender-av-health: {} device rows retrieved, {} assets reported, {} rows skipped, {} duplicate rows ignored".format(
        rows_total, reported, skipped, duplicates))
    if walk_err != None:
        fail("defender-av-health: could not read the device antivirus health list: {}".format(walk_err))
    if rows_total == 0:
        print("defender-av-health: Microsoft reported no device antivirus health rows for this tenant")
    return None

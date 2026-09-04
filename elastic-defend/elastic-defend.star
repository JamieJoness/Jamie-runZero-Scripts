# This is a runZero Custom Integration, please see https://github.com/runZeroInc/runzero-custom-integrations for details.
# This script was generated with AI.

CONFIG = {
    "id": "runzero-elastic-defend",
    "name": "Elastic Defend",
    "type": "inbound",
    "description": "Imports endpoints protected by Elastic Defend from the Kibana Security endpoint management API.",
    "version": "1",
    "maturity": "alpha",
    "minVersion": "5.1.260818.0",
    # The Elastic Agent ID survives rename, reboot, upgrades, and address
    # changes, while these are EDR-managed hosts whose MAC, IP, and hostname
    # churn with wireless roaming, VPN adapters, MAC randomization, and DHCP.
    "matchBehavior": "no-mac-break no-ip-break no-name-break",
    # 10,000 pages at the 100-endpoint page ceiling bounds a walk at one
    # million endpoints.
    "maxPages": 10000,
    "params": [
        {
            "key": "url",
            "label": "Kibana URL",
            "type": "url",
            "required": True,
            "placeholder": "https://kibana.example.com:5601",
            "description": "Base URL of Kibana: the Kibana endpoint of an Elastic Cloud deployment, a serverless security project, or a self-managed instance. Include the server.basePath prefix if one is configured.",
        },
        {
            "key": "api_key",
            "label": "API key",
            "type": "secret",
            "required": True,
            "description": "Elastic API key in the Base64 'encoded' form shown when the key is created. Sent as 'Authorization: ApiKey <key>'. The key's role needs read access to the Security solution, including the Endpoint List.",
        },
        {
            "key": "space_id",
            "label": "Kibana space ID",
            "type": "string",
            "required": False,
            "description": "Optional Kibana space to read endpoints from. Leave blank for the default space.",
        },
        {
            "key": "kuery",
            "label": "KQL filter",
            "type": "string",
            "required": False,
            "placeholder": "united.endpoint.host.os.name:Windows",
            "description": "Optional KQL filter applied server-side. Fields use the united index prefix, for example united.endpoint.host.os.name or united.agent.version.",
        },
        {
            "key": "host_statuses",
            "label": "Host statuses",
            "type": "string",
            "required": False,
            "description": "Optional comma-separated host statuses to import (healthy, offline, updating, inactive, unhealthy, unenrolled). Leave blank to import every endpoint regardless of status.",
        },
        {
            "key": "page_size",
            "label": "Page size",
            "type": "int",
            "required": False,
            "default": 100,
            "min": 1,
            "max": 100,
            "description": "Endpoints requested per page. The published API contract caps pageSize at 100.",
        },
    ],
    "includes": {
        "tls_": OPTIONS_TLS,
        "http_": OPTIONS_HTTP,
    },
}

load("runzero.types", "ImportAsset", "to_custom_attributes")
load("net", "network_interface", "routable_ips", "clean_hostnames")
load("http", "get_json", "url_encode", "url_parse")
load("time", "parse_ts")
load("coerce", "as_text", "as_dict", "as_list", "dicts")
load("kwargs", "require", "get_string", "get_int", "get_list", "get_http_options")

METADATA_PATH = "/api/endpoint/metadata"
# Pins the public route contract. Kibana's versioned router resolves the same
# version when the header is absent, and releases predating versioned routing
# ignore the header, so sending it is safe everywhere.
API_VERSION = "2023-10-31"

DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 100
MAX_INTERFACES = 32

ATTR_PREFIX = "elastic_defend"


def build_network_interfaces(host):
    """Build interfaces from the flat ECS host.ip and host.mac lists.

    The two lists carry every address and every MAC on the host with no
    documented correlation between them, so pairing a MAC with an address
    would assert a binding the API never states. The usable addresses go on
    one interface of their own and each MAC gets an interface carrying only
    itself. Loopback and link-local addresses are dropped: agents report them
    alongside real ones, and every host shares them.
    """
    netifs = []
    ips = routable_ips(as_list(host.get("ip")))
    if ips:
        nic = network_interface(ips=ips)
        if nic:
            netifs.append(nic)
    for value in as_list(host.get("mac"))[:MAX_INTERFACES]:
        mac = as_text(value).strip()
        if not mac:
            continue
        nic = network_interface(mac=mac)
        if nic:
            netifs.append(nic)
    return netifs


def build_asset(namespace, row):
    """Convert one HostInfo row from the metadata list into an ImportAsset."""
    meta = as_dict(row.get("metadata"))
    agent = as_dict(meta.get("agent"))
    host = as_dict(meta.get("host"))

    agent_id = as_text(agent.get("id")).strip()
    if not agent_id:
        print("elastic-defend: skipping endpoint with no agent id: hostname=" + as_text(host.get("hostname")))
        return None

    endpoint = as_dict(meta.get("Endpoint"))
    policy_applied = as_dict(as_dict(endpoint.get("policy")).get("applied"))
    state = as_dict(endpoint.get("state"))
    configuration = as_dict(endpoint.get("configuration"))
    os_info = as_dict(host.get("os"))
    os_ext = as_dict(os_info.get("Ext"))

    # policy_info compares what the agent runs against what Fleet has
    # configured; a revision gap means the endpoint has not picked up the
    # latest policy yet.
    policy_info = as_dict(row.get("policy_info"))
    agent_policy = as_dict(policy_info.get("agent"))
    agent_policy_applied = as_dict(agent_policy.get("applied"))
    agent_policy_configured = as_dict(agent_policy.get("configured"))
    endpoint_policy = as_dict(policy_info.get("endpoint"))

    attrs = {
        "agent_build": as_text(as_dict(agent.get("build")).get("original")),
        "agent_id": agent_id,
        "agent_version": as_text(agent.get("version")),
        "agent_policy_applied_id": as_text(agent_policy_applied.get("id")),
        "agent_policy_applied_revision": agent_policy_applied.get("revision"),
        "agent_policy_configured_id": as_text(agent_policy_configured.get("id")),
        "agent_policy_configured_revision": agent_policy_configured.get("revision"),
        "architecture": as_text(host.get("architecture")),
        "capabilities": as_list(endpoint.get("capabilities")),
        "endpoint_package_policy_id": as_text(endpoint_policy.get("id")),
        "endpoint_package_policy_revision": endpoint_policy.get("revision"),
        "endpoint_status": as_text(endpoint.get("status")),
        "host_id": as_text(host.get("id")),
        "host_status": as_text(row.get("host_status")),
        # The raw lists are preserved because the interfaces above
        # deliberately drop loopback and link-local addresses.
        "ip_addresses": as_list(host.get("ip")),
        "mac_addresses": as_list(host.get("mac")),
        "isolated": state.get("isolation"),
        "isolation_configured": configuration.get("isolation"),
        "last_checkin": as_text(row.get("last_checkin")),
        "os_family": as_text(os_info.get("family")),
        "os_full": as_text(os_info.get("full")),
        "os_kernel": as_text(os_info.get("kernel")),
        "os_platform": as_text(os_info.get("platform")),
        "os_type": as_text(os_info.get("type")),
        "os_variant": as_text(os_ext.get("variant")),
        "policy_applied_endpoint_version": policy_applied.get("endpoint_policy_version"),
        "policy_applied_id": as_text(policy_applied.get("id")),
        "policy_applied_name": as_text(policy_applied.get("name")),
        "policy_applied_status": as_text(policy_applied.get("status")),
        "policy_applied_version": policy_applied.get("version"),
    }

    tags = []
    if state.get("isolation") == True:
        tags.append("isolated")

    # os.Ext.variant is the specific edition ("Windows 10 Pro", "Ubuntu")
    # where os.name is the coarse family ("Windows", "Linux").
    os_name = as_text(os_ext.get("variant")) or as_text(os_info.get("name")) or as_text(os_info.get("platform"))

    return ImportAsset(
        id="elastic-defend:{}:{}".format(namespace, agent_id),
        hostnames=clean_hostnames([host.get("hostname"), host.get("name")]),
        networkInterfaces=build_network_interfaces(host),
        os=os_name,
        osVersion=as_text(os_info.get("version")),
        lastSeenTS=parse_ts(row.get("last_checkin")),
        tags=tags,
        customAttributes=to_custom_attributes(attrs, prefix=ATTR_PREFIX, separator="_"),
    )


def _build_query(page_num, page_size, kuery, statuses):
    """Assemble the query string by hand: hostStatuses repeats per value, and
    get_json's params= would replace the URL's own query string."""
    pairs = [url_encode({"pageSize": str(page_size)})]
    if page_num != None:
        pairs.append(url_encode({"page": str(page_num)}))
    if kuery:
        pairs.append(url_encode({"kuery": kuery}))
    for status in statuses:
        pairs.append(url_encode({"hostStatuses": status}))
    return "&".join(pairs)


def fetch_and_report(ctx):
    """Walk the metadata list, streaming each endpoint as it is built.

    The published OpenAPI documents page as 1-based while every shipped Kibana
    implementation is 0-based, so the first request omits page entirely -- the
    server returns its own first page either way -- and each later request asks
    for the page number the previous response reported, plus one.

    Returns (reported, skipped, total, walk_err). The caller prints its
    summary before failing on walk_err, so a truncated walk is not filed as a
    complete estate.
    """
    reported = 0
    skipped = 0
    seen_rows = 0
    total = None
    page_num = None
    walk_err = None

    p = pager("endpoints")
    while p.next():
        url = ctx["url"] + "?" + _build_query(page_num, ctx["page_size"], ctx["kuery"], ctx["statuses"])
        data, err = get_json(url, **ctx["http_options"])
        if err:
            if page_num == None:
                # Nothing was read at all; see "Failing the task".
                if err.startswith("status 403"):
                    print("elastic-defend: the API key was accepted but lacks privileges; its role needs read access to the Security solution, including the Endpoint List, in this space")
                fail("elastic-defend: could not read the endpoint list: {}".format(err))
            walk_err = "page request failed after reporting {}: {}".format(reported, err)
            break

        data = as_dict(data)
        rows = as_list(data.get("data"))
        records = dicts(rows)
        skipped += len(rows) - len(records)
        seen_rows += len(rows)

        for record in records:
            asset = build_asset(ctx["namespace"], record)
            if asset == None:
                skipped += 1
                continue
            reported += report_asset(asset)

        t = data.get("total")
        if type(t) == "int":
            total = t
        if not rows or (total != None and seen_rows >= total):
            break

        resp_page = data.get("page")
        if type(resp_page) == "int":
            page_num = resp_page + 1
        else:
            page_num = (page_num if page_num != None else 0) + 1

    return reported, skipped, total, walk_err


def main(**kwargs):
    require(kwargs, "url", "api_key")
    # Kept whole rather than reduced to scheme+host so a Kibana behind a
    # reverse proxy with a server.basePath prefix keeps working.
    base_url = get_string(kwargs, "url").strip().rstrip("/")
    parsed = url_parse(base_url)
    if not parsed or not parsed.hostname:
        fail("elastic-defend: no host could be derived from the Kibana URL")
    api_key = get_string(kwargs, "api_key")
    space_id = get_string(kwargs, "space_id", default="").strip()
    kuery = get_string(kwargs, "kuery", default="").strip()
    statuses = []
    for value in get_list(kwargs, "host_statuses", default=[]):
        status = as_text(value).strip().lower()
        if status:
            statuses.append(status)
    page_size = get_int(kwargs, "page_size", default=DEFAULT_PAGE_SIZE)
    if page_size < 1 or page_size > MAX_PAGE_SIZE:
        page_size = MAX_PAGE_SIZE

    if space_id:
        base_url = base_url + "/s/" + space_id

    ctx = {
        "url": base_url + METADATA_PATH,
        # Agent IDs are only meaningful within one deployment, so the Kibana
        # host namespaces them.
        "namespace": parsed.hostname.lower(),
        "page_size": page_size,
        "kuery": kuery,
        "statuses": statuses,
        "http_options": get_http_options(kwargs, headers={
            "Authorization": "ApiKey " + api_key,
            "Accept": "application/json",
            "Elastic-Api-Version": API_VERSION,
        }),
    }

    reported, skipped, total, walk_err = fetch_and_report(ctx)

    if skipped:
        print("elastic-defend: skipped {} records with no usable agent id".format(skipped))
    if total != None:
        print("elastic-defend: reported {} of {} endpoints".format(reported, total))
    else:
        print("elastic-defend: reported {} endpoints".format(reported))
    if walk_err != None:
        fail("elastic-defend: {}".format(walk_err))
    if not reported:
        print("elastic-defend: no endpoints retrieved")
    return None

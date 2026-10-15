# This is a runZero Custom Integration, please see https://github.com/runZeroInc/runzero-custom-integrations for details.
# This script was generated with AI.

CONFIG = {
    "id": "runzero-tenable-nessus-manager",
    "name": "Tenable Nessus Manager",
    "type": "inbound",
    "description": "Imports the hosts running a Tenable Nessus Agent linked to an on-premises Nessus Manager, with their agent group membership, agent status, and agent version.",
    "version": "1",
    "maturity": "alpha",
    # The ImportAsset constructor accepts lastSeenTS from this release on; the
    # library baseline, 5.1.260818.0, rejects the keyword.
    "minVersion": "5.1.260825.0",
    # The agent UUID is minted on the host itself and outlives unlink/relink,
    # agent upgrades, renames, and readdressing, while the manager publishes one
    # current IP and whatever MACs the agent last reported. Endpoints roam,
    # leases move, and VPN adapters come and go, so none of that churn should
    # disqualify a merge against the authoritative id. See README "Asset identity".
    "matchBehavior": "no-mac-break no-ip-break no-name-break",
    # 10,000 pages at the 1,000-agent default page size bounds a walk at ten
    # million agents.
    "maxPages": 10000,
    "params": [
        {
            "key": "url",
            "label": "Nessus Manager URL",
            "type": "url",
            "required": True,
            "placeholder": "https://nessus-manager.example.com:8834",
            "description": "Base URL of the Nessus Manager web interface, including the port (8834 by default).",
        },
        {
            "key": "access_key",
            "label": "API access key",
            "type": "string",
            "required": True,
            "description": "Access key half of an API key pair generated under Settings > My Account > API Keys. Sent together with the secret key in the X-ApiKeys header.",
        },
        {
            "key": "secret_key",
            "label": "API secret key",
            "type": "secret",
            "required": True,
            "description": "Secret key half of the API key pair.",
        },
        {
            "key": "include_unlinked",
            "label": "Import unlinked agents",
            "type": "bool",
            "required": False,
            "default": False,
            "description": "Also import agents whose status is unlinked. Nessus Manager only keeps them when Track Unlinked Agents is enabled, and they are no longer reporting, so they are skipped by default.",
        },
        {
            "key": "page_size",
            "label": "Agents per page",
            "type": "int",
            "required": False,
            "default": 1000,
            "min": 1,
            "max": 5000,
            "description": "Value of the limit query parameter on the agent list. Tenable's own client requests 1000 and caps the equivalent cloud endpoint at 5000.",
        },
    ],
    "includes": {
        "tls_": OPTIONS_TLS,
        "http_": OPTIONS_HTTP,
    },
}

load("runzero.types", "ImportAsset", "Software", "to_custom_attributes")
load("net", "network_interface", "routable_ips", "clean_hostnames", "mac_key")
load("http", "get_json", "url_parse")
load("time", "parse_ts")
load("coerce", "as_text", "as_dict", "as_list", "dicts", "as_int", "dedupe")
load("kwargs", "require", "get_string", "get_int", "get_bool", "get_http_options")
load("re", re_split="split")

VENDOR = "tenable-nessus-manager"
ATTR_PREFIX = "nessus_manager"

PROPERTIES_PATH = "/server/properties"
AGENT_GROUPS_PATH = "/agent-groups"
AGENTS_PATH = "/agents"

DEFAULT_PAGE_SIZE = 1000
MAX_INTERFACES = 32

# The status Nessus Manager assigns to an agent it is tracking after an unlink.
STATUS_UNLINKED = "unlinked"

# platform is the agent's platform family. distro names the package build the
# agent was installed from (es8-x86-64, ubuntu1404-amd64, ...), which does not
# identify the running distribution, so only the family becomes the OS and
# distro stays an attribute.
PLATFORM_OS = {"WINDOWS": "Windows", "LINUX": "Linux", "DARWIN": "macOS"}

# mac_addrs is documented as one string holding the MACs of every interface;
# the delimiter is not documented, so any run of whitespace, commas, or
# semicolons separates entries. ip is documented as a single address and is
# split the same way in case a release lists several.
LIST_SPLIT_RE = r"[\s,;]+"

# Manager-wide counters the API repeats on every agent row. They describe the
# manager rather than the host, so they are not kept as asset attributes.
MANAGER_WIDE_FIELDS = ["safe_mode_agent_count", "total_agents"]


def split_values(value):
    """Split a delimited string field into its trimmed, de-duplicated parts."""
    return dedupe(re_split(LIST_SPLIT_RE, as_text(value)))


def tag_value(prefix, value):
    """Build a prefix:value tag. runZero splits the tag string on whitespace,
    so a space inside a group name would otherwise break the tag apart."""
    return prefix + ":" + "-".join(value.split())


def auth_hint(err):
    """Explain a refused request in the operator's terms, or return ""."""
    if err.startswith("status 401"):
        return "the API keys were rejected; generate a new pair under Settings > My Account > API Keys"
    if err.startswith("status 403"):
        return "the API keys were accepted but their user cannot list agents, which needs the Administrator or System Administrator role"
    return ""


def agent_macs(agent):
    """Return the agent's usable MACs in canonical form.

    Placeholders such as the all-zero address are dropped before they can
    correlate unrelated hosts.
    """
    macs = []
    for value in split_values(agent.get("mac_addrs")):
        key = mac_key(value)
        if key and key not in macs:
            macs.append(key)
    return macs[:MAX_INTERFACES]


def build_network_interfaces(ips, macs):
    """Build interfaces from the agent's reported address and MAC list.

    The manager publishes one IP and a list of MACs with no stated pairing. A
    single MAC is paired with the address; several MACs each get an interface
    of their own and the address stands alone, so no binding is asserted that
    the API never made.
    """
    if len(macs) == 1:
        nic = network_interface(mac=macs[0], ips=ips)
        return [nic] if nic else []
    netifs = []
    nic = network_interface(ips=ips)
    if nic:
        netifs.append(nic)
    for mac in macs:
        nic = network_interface(mac=mac)
        if nic:
            netifs.append(nic)
    return netifs


def group_membership(agent, groups_by_id):
    """Resolve the agent's groups to (names, ids).

    Each entry is documented as {"name", "id"}, but Tenable's own client also
    models a bare id, so both shapes are read and a missing name is looked up
    in the agent-groups list.
    """
    names = []
    ids = []
    for entry in as_list(agent.get("groups")):
        if type(entry) == "dict":
            group_id = as_text(entry.get("id"))
            name = as_text(entry.get("name"))
        else:
            group_id = as_text(entry)
            name = ""
        if not name:
            name = as_text(as_dict(groups_by_id.get(group_id)).get("name"))
        if group_id:
            ids.append(group_id)
        if name:
            names.append(name)
    return dedupe(names), dedupe(ids)


def build_asset(ctx, agent):
    """Convert one agent row into an ImportAsset, or None when it cannot be
    identified or correlated."""
    uuid = as_text(agent.get("uuid"))
    name = as_text(agent.get("name"))
    if not uuid:
        print("{}: skipping agent with no uuid: name={}".format(VENDOR, name))
        return None

    hostnames = clean_hostnames([name])
    ips = routable_ips(split_values(agent.get("ip")))
    macs = agent_macs(agent)
    netifs = build_network_interfaces(ips, macs)
    if not hostnames and not netifs:
        print("{}: skipping agent {} with no usable hostname, address, or MAC".format(VENDOR, uuid))
        return None

    status = as_text(agent.get("status")).lower()
    group_names, group_ids = group_membership(agent, ctx["groups_by_id"])

    tags = []
    if status:
        tags.append(tag_value("agent-status", status))
    if as_int(agent.get("safe_mode")) > 0:
        tags.append("agent-safe-mode")
    for group_name in group_names:
        tags.append(tag_value("agent-group", group_name))

    software = []
    version = as_text(agent.get("core_version"))
    if version:
        software.append(Software(id="tenable-nessus-agent", vendor="Tenable", product="Nessus Agent", version=version))

    # Every field the row carries is kept, so a release that publishes more
    # than the documented set loses nothing. The identity fields are renamed so
    # they read as the agent's rather than the manager's.
    attrs = dict(agent)
    attrs["agent_id"] = attrs.pop("id", None)
    attrs["agent_uuid"] = attrs.pop("uuid", None)
    attrs["agent_name"] = attrs.pop("name", None)
    attrs["mac_addrs"] = macs
    attrs["groups"] = group_names
    attrs["group_ids"] = group_ids

    return ImportAsset(
        id="{}:{}:{}".format(VENDOR, ctx["namespace"], uuid),
        hostnames=hostnames,
        networkInterfaces=netifs,
        os=PLATFORM_OS.get(as_text(agent.get("platform")).upper(), ""),
        firstSeenTS=parse_ts(agent.get("linked_on")),
        lastSeenTS=parse_ts(agent.get("last_connect")),
        tags=tags,
        software=software,
        customAttributes=to_custom_attributes(attrs, prefix=ATTR_PREFIX, separator="_", exclude=MANAGER_WIDE_FIELDS),
    )


def check_server(ctx):
    """Read the server properties to confirm the target is a Nessus Manager.

    Agents and agent groups exist only on Nessus Manager; a Nessus Professional
    or Expert scanner answers the same API at the same port without them, so
    the product is checked up front and named in the log. A refused key fails
    here rather than one request later. Any other problem reading the
    properties is logged, and the agent list decides the run.
    """
    data, err = get_json(ctx["base_url"] + PROPERTIES_PATH, **ctx["http_options"])
    if err:
        if err.startswith("status 401"):
            print("{}: {}".format(VENDOR, auth_hint(err)))
            fail("{}: could not read the server properties: {}".format(VENDOR, err))
        print("{}: could not read the server properties, continuing without the product check: {}".format(VENDOR, err))
        return
    props = as_dict(data)
    product = as_text(props.get("nessus_type"))
    print("{}: {} {} at {}, plugin set {}".format(
        VENDOR, product or "Nessus", as_text(props.get("server_version")) or "(version unknown)",
        ctx["namespace"], as_text(props.get("plugin_set")) or "unknown"))
    if product and "manager" not in product.lower():
        fail("{}: {} is a {}; agents and agent groups are only available on Nessus Manager".format(
            VENDOR, ctx["namespace"], product))


def fetch_agent_groups(ctx):
    """Index the agent groups by id so a bare group id on an agent row can be
    named. The list is enrichment: a failure is logged, and the names embedded
    in each agent row are still used."""
    data, err = get_json(ctx["base_url"] + AGENT_GROUPS_PATH, **ctx["http_options"])
    if err:
        print("{}: could not read the agent groups, group names will come from the agent rows only: {}".format(VENDOR, err))
        return {}
    groups_by_id = {}
    for group in dicts(as_dict(data).get("groups")):
        group_id = as_text(group.get("id"))
        if group_id:
            groups_by_id[group_id] = group
    print("{}: read {} agent groups".format(VENDOR, len(groups_by_id)))
    return groups_by_id


def fetch_and_report_agents(ctx):
    """Walk the agent list with limit/offset, streaming each agent as it is built.

    pagination.total ends the walk when the response carries it; otherwise a
    page shorter than the limit the server echoed (or the one requested) does,
    which is how Tenable's own client terminates. The list is offset-paged
    with no stable sort, so a row that shifts between pages is reported once.

    Returns (reported, counts, total, walk_err). The caller prints its summary
    before failing on walk_err, so a truncated walk is not filed as a complete
    estate.
    """
    reported = 0
    counts = {"unlinked": 0, "unusable": 0, "duplicate": 0}
    seen = {}
    offset = 0
    total = None
    walk_err = None

    p = pager("agents")
    while p.next():
        data, err = get_json(ctx["base_url"] + AGENTS_PATH,
                             params={"limit": str(ctx["page_size"]), "offset": str(offset)},
                             **ctx["http_options"])
        if err:
            if p.page == 1:
                # Nothing was read at all; see "Failing the task".
                hint = auth_hint(err)
                if hint:
                    print("{}: {}".format(VENDOR, hint))
                fail("{}: could not read the agent list: {}".format(VENDOR, err))
            walk_err = "page {} failed after reporting {}: {}".format(p.page, reported, err)
            break

        data = as_dict(data)
        rows = as_list(data.get("agents"))
        for row in rows:
            if type(row) != "dict":
                counts["unusable"] += 1
                continue
            if not ctx["include_unlinked"] and as_text(row.get("status")).lower() == STATUS_UNLINKED:
                counts["unlinked"] += 1
                continue
            asset = build_asset(ctx, row)
            if asset == None:
                counts["unusable"] += 1
                continue
            if asset.id in seen:
                counts["duplicate"] += 1
                continue
            seen[asset.id] = True
            reported += report_asset(asset)

        pagination = as_dict(data.get("pagination"))
        page_total = pagination.get("total")
        if type(page_total) == "int":
            total = page_total
        offset += len(rows)
        limit = as_int(pagination.get("limit")) or ctx["page_size"]
        if not rows or (total != None and offset >= total) or len(rows) < limit:
            break

    return reported, counts, total, walk_err


def main(**kwargs):
    require(kwargs, "url", "access_key", "secret_key")
    base_url = get_string(kwargs, "url").strip().rstrip("/")
    parsed = url_parse(base_url)
    if not parsed or not parsed.hostname:
        fail("{}: no host could be derived from the Nessus Manager URL".format(VENDOR))

    ctx = {
        "base_url": base_url,
        # Agent records are only meaningful within one manager, so its host
        # namespaces them.
        "namespace": parsed.hostname.lower(),
        "page_size": get_int(kwargs, "page_size", default=DEFAULT_PAGE_SIZE),
        "include_unlinked": get_bool(kwargs, "include_unlinked", default=False),
        "http_options": get_http_options(kwargs, headers={
            "X-ApiKeys": "accessKey={}; secretKey={}".format(
                get_string(kwargs, "access_key").strip(), get_string(kwargs, "secret_key").strip()),
            "Accept": "application/json",
        }),
        "groups_by_id": {},
    }

    check_server(ctx)
    ctx["groups_by_id"] = fetch_agent_groups(ctx)
    reported, counts, total, walk_err = fetch_and_report_agents(ctx)

    if counts["unlinked"]:
        print("{}: skipped {} unlinked agents; enable include_unlinked to import them".format(VENDOR, counts["unlinked"]))
    if counts["unusable"]:
        print("{}: skipped {} records with no usable agent uuid, hostname, address, or MAC".format(VENDOR, counts["unusable"]))
    if counts["duplicate"]:
        print("{}: skipped {} agent rows repeated across pages".format(VENDOR, counts["duplicate"]))
    if total != None:
        print("{}: reported {} of {} agents".format(VENDOR, reported, total))
    else:
        print("{}: reported {} agents".format(VENDOR, reported))
    if walk_err != None:
        fail("{}: {}".format(VENDOR, walk_err))
    if not reported:
        print("{}: no agents retrieved".format(VENDOR))
    return None

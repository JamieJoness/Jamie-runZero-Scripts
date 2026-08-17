# Copyright 2026 runZero, Inc. Available under the MIT License

CONFIG = {
    "id": "runzero-sophos-edr",
    "name": "Sophos EDR",
    "type": "inbound",
    "description": "Imports computer and server endpoints from Sophos Central (Endpoint/Server Protection).",
    "version": "26081400",
    "minVersion": "5.0.260723.0",
    "params": [
        {
            "key": "client_id",
            "label": "Client ID",
            "type": "string",
            "required": True,
            "description": "Sophos Central API credential (service principal) Client ID.",
        },
        {
            "key": "client_secret",
            "label": "Client secret",
            "type": "secret",
            "required": True,
            "description": "Sophos Central API credential (service principal) Client secret.",
        },
    ],
    "includes": {
        "tls_": OPTIONS_TLS,
        "http_": OPTIONS_HTTP,
    },
}
## Sophos EDR (Sophos Central) integration

load("runzero.types", "ImportAsset", "to_custom_attributes")
load("net", "network_interface")
load("http", "get_json", "bearer", "oauth2_token")
load("kwargs", "require", "get_string", "get_http_options")

# Sophos ID (IdP) and the global API host are the same for every customer; the
# regional/tenant hosts are discovered at runtime from whoami and the tenant
# listing, so only these global endpoints are fixed here.
AUTH_URL = "https://id.sophos.com/api/v2/oauth2/token"
GLOBAL_API_HOST = "https://api.central.sophos.com"
OAUTH_SCOPE = "token"

# Both endpoint and tenant listings cap the page size at 100.
ENDPOINT_PAGE_SIZE = 100
TENANT_PAGE_SIZE = 100

# Upper bound on pagination iterations; real datasets terminate far sooner via
# the documented end-of-pages conditions (no nextKey / page >= total).
_MAX_PAGES = 100000

def refresh_token(auth):
    """Exchange the service-principal credentials for a fresh access token and
    store it back on the shared auth state."""
    auth["token"] = oauth2_token(
        AUTH_URL,
        client_id=auth["client_id"],
        client_secret=auth["client_secret"],
        scope=OAUTH_SCOPE,
        **get_http_options(auth["kwargs"])
    )
    return auth["token"]

def authed_get(auth, url, extra_headers=None, params=None):
    """GET JSON with the current bearer token, transparently refreshing the
    token once and retrying if Sophos reports the token has expired (401)."""
    headers = dict(extra_headers or {})
    headers["Authorization"] = bearer(auth["token"])
    data, err = get_json(url, params=params or {}, **get_http_options(auth["kwargs"], headers=headers))
    if err and err.startswith("status 401"):
        print("sophos-edr: access token expired, refreshing")
        refresh_token(auth)
        headers["Authorization"] = bearer(auth["token"])
        data, err = get_json(url, params=params or {}, **get_http_options(auth["kwargs"], headers=headers))
    return data, err

def whoami(auth):
    """Resolve the calling principal's id, idType, and regional API hosts."""
    data, err = authed_get(auth, GLOBAL_API_HOST + "/whoami/v1")
    if err:
        print("sophos-edr: whoami request failed:", err)
        return None
    return data

def list_tenants(auth, id_header, entity_id, path):
    """Enumerate every tenant managed by a partner or organization principal
    using Sophos page-by-offset pagination. Returns the raw tenant items."""
    tenants = []
    headers = {id_header: entity_id}
    page = 1
    total = 1
    first = True
    for _ in range(_MAX_PAGES):
        params = {"pageSize": TENANT_PAGE_SIZE}
        if first:
            params["pageTotal"] = "true"
        else:
            params["page"] = page
        data, err = authed_get(auth, GLOBAL_API_HOST + path, extra_headers=headers, params=params)
        if err:
            print("sophos-edr: failed to list tenants:", err)
            break
        if not data:
            break
        items = data.get("items", [])
        for tenant in items:
            tenants.append(tenant)
        pages = data.get("pages", {})
        if first:
            total = pages.get("total", 1)
            first = False
        if not items or page >= total:
            break
        page += 1
    return tenants

def build_network_interfaces(endpoint):
    """Build NetworkInterfaces from Sophos' unpaired MAC and IP lists. All IPs
    are attached to the first MAC; any remaining MACs are preserved as
    MAC-only interfaces so no correlation signal is lost."""
    macs = endpoint.get("macAddresses") or []
    ips = (endpoint.get("ip4Addresses") or []) + (endpoint.get("ip6Addresses") or [])
    interfaces = []
    if macs:
        first = network_interface(mac=macs[0], ips=ips)
        if first:
            interfaces.append(first)
        for mac in macs[1:]:
            nif = network_interface(mac=mac)
            if nif:
                interfaces.append(nif)
    else:
        nif = network_interface(ips=ips)
        if nif:
            interfaces.append(nif)
    return interfaces

def build_os(endpoint):
    """Return an (os_name, os_version) pair from the endpoint's os object."""
    os_obj = endpoint.get("os") or {}
    name = os_obj.get("name") or os_obj.get("platform") or ""
    parts = []
    for key in ("majorVersion", "minorVersion", "build"):
        value = os_obj.get(key)
        if value != None and str(value) != "":
            parts.append(str(value))
    return name, ".".join(parts)

def build_assets(items, context_tenant_id):
    """Convert a page of Sophos endpoints into runZero ImportAssets."""
    assets = []
    for endpoint in items:
        endpoint_id = endpoint.get("id")
        if not endpoint_id:
            print("sophos-edr: skipping endpoint with no id")
            continue

        # The endpoint id is a stable, tenant-scoped UUID, so namespace it with
        # the tenant to guarantee global uniqueness across aggregated tenants.
        tenant_id = context_tenant_id
        tenant = endpoint.get("tenant")
        if type(tenant) == "dict" and tenant.get("id"):
            tenant_id = tenant.get("id")
        asset_id = "sophos:{}:{}".format(tenant_id, endpoint_id)

        os_name, os_version = build_os(endpoint)
        hostname = (endpoint.get("hostname") or "").strip()

        assets.append(
            ImportAsset(
                id=asset_id,
                hostnames=[hostname] if hostname else [],
                os=os_name,
                osVersion=os_version,
                networkInterfaces=build_network_interfaces(endpoint),
                customAttributes=to_custom_attributes(
                    endpoint,
                    exclude=["id", "hostname", "ip4Addresses", "ip6Addresses", "macAddresses"],
                ),
                # Sophos id is authoritative; let MAC/IP/name churn without
                # breaking the id-based match as endpoints roam networks.
                matchBehavior="no-mac-break no-ip-break no-name-break",
            )
        )
    return assets

def stream_tenant_endpoints(auth, tenant_api_host, tenant_id):
    """Page through one tenant's endpoints (key-based pagination, full view)
    and stream each page to runZero. Returns the number of assets reported."""
    url = tenant_api_host + "/endpoint/v1/endpoints"
    headers = {"X-Tenant-ID": tenant_id}
    reported = 0
    page_from_key = None
    for _ in range(_MAX_PAGES):
        params = {"pageSize": ENDPOINT_PAGE_SIZE, "view": "full"}
        if page_from_key:
            params["pageFromKey"] = page_from_key
        data, err = authed_get(auth, url, extra_headers=headers, params=params)
        if err:
            print("sophos-edr: failed to fetch endpoints for tenant", tenant_id, ":", err)
            break
        if not data:
            break
        items = data.get("items", [])
        if items:
            reported += report_assets(build_assets(items, tenant_id))
        page_from_key = data.get("pages", {}).get("nextKey")
        if not page_from_key:
            break
    return reported

def resolve_tenants(auth, identity):
    """Turn a whoami identity into a list of (tenant_id, tenant_api_host)."""
    id_type = identity.get("idType")
    entity_id = identity.get("id")
    api_hosts = identity.get("apiHosts") or {}

    if id_type == "tenant":
        data_region = api_hosts.get("dataRegion")
        if not data_region:
            print("sophos-edr: whoami response missing dataRegion for tenant principal")
            return []
        return [(entity_id, data_region)]

    if id_type == "partner":
        raw = list_tenants(auth, "X-Partner-ID", entity_id, "/partner/v1/tenants")
    elif id_type == "organization":
        raw = list_tenants(auth, "X-Organization-ID", entity_id, "/organization/v1/tenants")
    else:
        print("sophos-edr: unsupported principal idType:", id_type)
        return []

    tenants = []
    for tenant in raw:
        tenant_id = tenant.get("id")
        api_host = tenant.get("apiHost")
        if tenant_id and api_host:
            tenants.append((tenant_id, api_host))
        else:
            print("sophos-edr: skipping tenant with no id/apiHost (id={})".format(tenant_id))
    return tenants

def main(**kwargs):
    """Authenticate to Sophos Central, resolve the managed tenants, and stream
    every tenant's endpoints into runZero."""
    require(kwargs, "client_id", "client_secret")
    client_id = get_string(kwargs, "client_id")
    client_secret = get_string(kwargs, "client_secret")

    token = oauth2_token(
        AUTH_URL,
        client_id=client_id,
        client_secret=client_secret,
        scope=OAUTH_SCOPE,
        **get_http_options(kwargs)
    )

    auth = {
        "token": token,
        "client_id": client_id,
        "client_secret": client_secret,
        "kwargs": kwargs,
    }

    identity = whoami(auth)
    if not identity:
        return None

    tenants = resolve_tenants(auth, identity)
    if not tenants:
        print("sophos-edr: no tenants available to query")
        return None

    total_reported = 0
    for tenant_id, tenant_api_host in tenants:
        count = stream_tenant_endpoints(auth, tenant_api_host, tenant_id)
        print("sophos-edr: reported", count, "endpoints for tenant", tenant_id)
        total_reported += count

    print("sophos-edr: reported", total_reported, "endpoints across", len(tenants), "tenant(s)")
    return None

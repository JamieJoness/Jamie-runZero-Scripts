CONFIG = {
    "id": "runzero-haloitsm-certificate-digest",
    "name": "HaloITSM Certificate Expiry Digest",
    "type": "outbound",
    "description": "Creates one weekly HaloITSM ticket for runZero certificates expiring within 14 days.",
    "version": "1",
    "maturity": "alpha",
    "minVersion": "5.1.260818.0",
    "maxPages": 1000,
    "params": [
        {
            "key": "runzero_url",
            "label": "runZero Console URL",
            "type": "url",
            "default": "https://console.runzero.com",
            "required": True,
        },
        {
            "key": "runzero_api_token",
            "label": "runZero API token",
            "type": "secret",
            "required": True,
        },
        {
            "key": "organization_ids",
            "label": "runZero organisation IDs",
            "type": "string",
            "required": True,
            "description": "Comma-separated organisation UUIDs. Every organisation must be readable by the token.",
        },
        {
            "key": "certificate_search",
            "label": "Additional certificate search",
            "type": "string",
            "description": "Optional certificate-inventory query, combined with the fixed 14-day expiry window.",
        },
        {
            "key": "service_search",
            "label": "Affected-service scope",
            "type": "string",
            "description": "Optional service-inventory query, for example site or asset tags. Only matching endpoints are included.",
        },
        {
            "key": "scope_label",
            "label": "Scope name for the ticket",
            "type": "string",
            "required": True,
            "description": "A short human-readable name, for example Production customer services.",
        },
        {
            "key": "digest_key",
            "label": "Stable digest identifier",
            "type": "string",
            "default": "certificate-renewals",
            "pattern": "^[a-z0-9][a-z0-9-]{0,63}$",
            "description": "Keep this unchanged across retries and credential edits. Use a different value for an independent digest.",
        },
        {
            "key": "schedule_anchor_utc",
            "label": "Weekly schedule anchor (UTC)",
            "type": "string",
            "required": True,
            "description": "An occurrence of the agreed schedule, for example 2026-10-05T09:00:00Z. Configure the actual weekly task separately.",
        },
        {
            "key": "halo_api_url",
            "label": "Halo resource server URL",
            "type": "url",
            "required": True,
            "description": "Your tenant's Resource Server URL, including /api.",
        },
        {
            "key": "halo_auth_url",
            "label": "Halo authorisation server URL",
            "type": "url",
            "required": True,
            "description": "Your tenant's Authorisation Endpoint, including /auth but not /token.",
        },
        {
            "key": "halo_client_id",
            "label": "Halo client ID",
            "type": "string",
            "required": True,
        },
        {
            "key": "halo_client_secret",
            "label": "Halo client secret",
            "type": "secret",
            "required": True,
        },
        {
            "key": "halo_scope",
            "label": "Halo OAuth scope",
            "type": "string",
            "default": "read:tickets edit:tickets",
        },
        {
            "key": "halo_tenant",
            "label": "Halo authentication tenant",
            "type": "string",
            "description": "Tenant value from Halo API Details, when your hosted authentication server requires it.",
        },
        {
            "key": "halo_tickettype_id",
            "label": "Halo ticket type ID",
            "type": "int",
            "required": True,
            "min": 1,
        },
        {
            "key": "halo_team_id",
            "label": "Halo team ID",
            "type": "int",
            "required": True,
            "min": 1,
        },
        {
            "key": "halo_client_id_for_ticket",
            "label": "Halo customer ID",
            "type": "int",
            "default": 0,
            "min": 0,
            "description": "Optional ticket customer, not the OAuth client ID. Zero leaves the ticket-type default.",
        },
        {
            "key": "halo_site_id",
            "label": "Halo site ID",
            "type": "int",
            "default": 0,
            "min": 0,
        },
        {
            "key": "halo_user_id",
            "label": "Halo requester ID",
            "type": "int",
            "default": 0,
            "min": 0,
        },
        {
            "key": "halo_priority_id",
            "label": "Halo priority ID",
            "type": "int",
            "default": 0,
            "min": 0,
            "description": "Optional tenant-specific priority. Zero leaves the ticket-type default; urgency is always included in the digest.",
        },
        {
            "key": "dry_run",
            "label": "Preview only",
            "type": "bool",
            "default": True,
            "description": "Read both APIs and print the digest without creating a ticket. Preview logs contain inventory data.",
        },
        {
            "key": "single_runner_confirmed",
            "label": "Only one runner can submit this digest",
            "type": "bool",
            "default": False,
            "description": "Required for delivery. Do not overlap scheduled runs, manual runs, or retries of this digest.",
        },
        {
            "key": "max_certificate_records",
            "label": "Maximum certificate records",
            "type": "int",
            "default": 5000,
            "min": 1,
            "max": 100000,
            "description": "Exceeding a limit fails the task without sending a partial digest.",
        },
        {
            "key": "max_service_records",
            "label": "Maximum service records",
            "type": "int",
            "default": 20000,
            "min": 1,
            "max": 200000,
        },
        {
            "key": "max_ticket_bytes",
            "label": "Maximum ticket request size (bytes)",
            "type": "int",
            "default": 500000,
            "min": 1000,
            "max": 5000000,
            "description": "Local safety limit, not a documented Halo limit. An oversized digest fails instead of being truncated.",
        },
    ],
    "includes": {
        "runzero_tls_": OPTIONS_TLS,
        "runzero_http_": OPTIONS_HTTP,
        "halo_tls_": OPTIONS_TLS,
        "halo_http_": OPTIONS_HTTP,
    },
}

load("coerce", "as_text", "as_dict", "as_list", "as_int", "as_bool", "dedupe")
load("crypto", "sha256")
load("http", "oauth2_token", "url_encode", "url_parse", "bearer", "get_json", "post_json")
load("json", json_encode="encode")
load("kwargs", "get_string", "get_int", "get_bool", "get_list", "get_http_options")
load("re", re_match="match")
load("time", "now", "parse_ts", "from_timestamp")

DAY_SECONDS = 86400
WEEK_SECONDS = 604800
WINDOW_SECONDS = 1209600

def checked_uuid(value, label):
    value = as_text(value).lower()
    if not re_match("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", value) or value == "00000000-0000-0000-0000-000000000000":
        fail("{} is missing or is not a usable UUID; no ticket was created".format(label))
    return value

def checked_url(kwargs, key, root_only=False):
    value = get_string(kwargs, key).rstrip("/")
    parsed = url_parse(value)
    if not parsed or parsed.scheme not in ["http", "https"] or not parsed.hostname or parsed.username or parsed.password or parsed.raw_query or parsed.fragment:
        fail("{} must be an HTTP(S) URL without embedded credentials, a query, or a fragment".format(key))
    if root_only and parsed.path not in ["", "/"]:
        fail("runzero_url must be the Console root, without an API path")
    return value

def timestamp_text(value):
    parsed = parse_ts(value, clamp_to_now=False)
    return parsed.in_location("UTC").format("2006-01-02 15:04:05 UTC") if parsed else "not recorded"

def remaining_text(expiry, checked_at):
    seconds = expiry - checked_at
    if seconds < DAY_SECONDS:
        return "less than 1 day ({} full hours)".format(max(0, seconds // 3600))
    days = seconds // DAY_SECONDS
    return "{} {} ({} full hours)".format(days, "day" if days == 1 else "days", seconds // 3600)

def quantity(count, singular):
    return "{} {}{}".format(count, singular, "" if count == 1 else "s")

def scoped_query(query, additional):
    return "({}) and ({})".format(query, additional) if additional else query

def read_json(url, options, params, label, recovery_reference=""):
    data, err = get_json(url, params=params, **options)
    if err:
        reason = err.split(":", 1)[0] if err.startswith("status ") else "transport or JSON response error"
        if recovery_reference:
            fail("{} failed ({}) after a creation attempt for {}; stop automatic retries and reconcile this reference in Halo before another execution".format(label, reason, recovery_reference))
        fail("{} failed ({}); check access and connectivity before retrying".format(label, reason))
    return data

def make_context(kwargs):
    checked_at = now().unix
    anchor_value = get_string(kwargs, "schedule_anchor_utc")
    anchor = parse_ts(anchor_value, clamp_to_now=False)
    if not anchor or not re_match("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$", anchor_value):
        fail("schedule_anchor_utc must be a valid UTC timestamp such as 2026-10-05T09:00:00Z")
    if anchor.unix > checked_at:
        fail("The first agreed weekly period has not started; use a past schedule occurrence for a preview")
    if not get_bool(kwargs, "dry_run") and not get_bool(kwargs, "single_runner_confirmed"):
        fail("Confirm single_runner_confirmed before delivery; concurrent runs can create duplicate Halo tickets")
    organization_ids = sorted(dedupe([checked_uuid(value, "Organisation ID") for value in get_list(kwargs, "organization_ids")]))
    if not organization_ids:
        fail("At least one organisation ID is required")
    runzero_url = checked_url(kwargs, "runzero_url", root_only=True)
    period_start = anchor.unix + ((checked_at - anchor.unix) // WEEK_SECONDS) * WEEK_SECONDS
    namespace = sha256(json_encode(["runzero-certificates-v1", runzero_url, organization_ids, get_string(kwargs, "digest_key")]))[:24]
    reference = "RZCERT-{}-{}".format(namespace, from_timestamp(period_start).in_location("UTC").format("20060102T150405Z"))
    return {
        "kwargs": kwargs,
        "checked_at": checked_at,
        "window_end": checked_at + WINDOW_SECONDS,
        "period_start": period_start,
        "reference": reference,
        "organization_ids": organization_ids,
        "runzero_url": runzero_url,
        "halo_url": checked_url(kwargs, "halo_api_url"),
        "runzero_options": get_http_options(kwargs, "runzero_http_", "runzero_tls_", {"Authorization": bearer(get_string(kwargs, "runzero_api_token"))}),
        "certificate_records": 0,
        "service_records": 0,
    }

def authenticate_halo(kwargs):
    token_url = checked_url(kwargs, "halo_auth_url") + "/token"
    tenant = get_string(kwargs, "halo_tenant")
    if tenant:
        token_url += "?" + url_encode({"tenant": tenant})
    return oauth2_token(
        token_url=token_url,
        client_id=get_string(kwargs, "halo_client_id"),
        client_secret=get_string(kwargs, "halo_client_secret"),
        scope=get_string(kwargs, "halo_scope"),
        **get_http_options(kwargs, "halo_http_", "halo_tls_")
    )

def read_ticket(ctx, ticket_id):
    ticket = read_json(ctx["halo_url"] + "/Tickets/{}".format(ticket_id), ctx["halo_options"], {"includedetails": "true"}, "Halo ticket verification", ctx.get("creation_attempted_reference", ""))
    if type(ticket) != "dict" or as_int(ticket.get("id")) != ticket_id:
        fail("Halo returned an unusable ticket detail; reconcile {} before retrying".format(ctx["reference"]))
    return ticket

def lookup_tickets(ctx, filter_name):
    matches = {}
    for deleted in ["false", "true"]:
        pages = pager("Halo digest lookup (deleted={})".format(deleted))
        seen = {}
        expected_count = None
        while pages.next():
            data = read_json(ctx["halo_url"] + "/Tickets", ctx["halo_options"], {
                filter_name: ctx["reference"],
                "open_only": "false",
                "closed_only": "false",
                "deleted": deleted,
                "pageinate": "true",
                "page_size": 100,
                "page_no": pages.page,
                "order": "id",
                "orderdesc": "false",
            }, "Halo duplicate lookup", ctx.get("creation_attempted_reference", ""))
            if type(data) == "dict" and "tickets" in data and data["tickets"] == None and as_int(data.get("record_count"), -1) == 0:
                data["tickets"] = []
            if type(data) != "dict" or type(data.get("tickets")) != "list" or as_int(data.get("record_count"), -1) < 0:
                fail("Halo duplicate lookup returned an incomplete response; no further ticket creation will be attempted")
            total = as_int(data["record_count"])
            if expected_count != None and total != expected_count:
                fail("Halo duplicate lookup changed while paging; retry after other activity has settled")
            expected_count = total
            for row in data["tickets"]:
                ticket_id = as_int(as_dict(row).get("id"))
                if ticket_id <= 0 or ticket_id in seen:
                    fail("Halo duplicate lookup returned a missing or repeated ticket ID; no further ticket creation will be attempted")
                seen[ticket_id] = True
                field = "summary" if filter_name == "search_summary" else "third_party_id_string"
                ticket = row if field in row else read_ticket(ctx, ticket_id)
                value = as_text(ticket.get(field))
                if (field == "summary" and ctx["reference"] not in value) or (field != "summary" and value != ctx["reference"]):
                    fail("Halo did not honour the external-reference filter; no further ticket creation will be attempted")
                matches[ticket_id] = True
            if len(seen) == total:
                break
            if not data["tickets"] or len(seen) > total:
                fail("Halo duplicate lookup ended before its reported count; no further ticket creation will be attempted")
    return matches

def find_existing_ticket(ctx):
    matches = lookup_tickets(ctx, "third_party_id_string")
    if not matches and lookup_tickets(ctx, "search_summary"):
        fail("A Halo ticket already contains {} in its summary but its native reference could not be verified; reconcile it without creating another ticket".format(ctx["reference"]))
    if len(matches) > 1:
        fail("Multiple Halo tickets already carry {}; reconcile ticket IDs {}".format(ctx["reference"], ", ".join([str(ticket_id) for ticket_id in sorted(matches)])))
    return sorted(matches)[0] if matches else None

def walk_export(ctx, organization_id, collection, search, consume):
    cursor = ""
    seen_cursors = {}
    pages = pager("runZero {} in {}".format(collection, organization_id))
    while pages.next():
        params = {"_oid": organization_id, "search": search, "page_size": 500}
        if cursor:
            params["start_key"] = cursor
        if collection == "services":
            params["fields"] = "service_id,service_asset_id,service_organization_id,organization_id,service_address,service_transport,service_port,service_vhost,service_protocol,service_summary,service_updated_at,name,names,site_name,last_seen,alive"
        data = read_json(ctx["runzero_url"] + "/api/v1.0/export/org/{}.json".format(collection), ctx["runzero_options"], params, "runZero {} export".format(collection))
        if type(data) != "dict" or type(data.get(collection)) != "list" or type(data.get("next_key")) != "string":
            fail("runZero {} export returned an incomplete paginated response; no ticket was created".format(collection))
        for row in data[collection]:
            if type(row) != "dict":
                fail("runZero {} export contains a malformed record; no partial digest was sent".format(collection))
            record_org = as_text(row.get("organization_id") or row.get("service_organization_id")).lower()
            if record_org != organization_id or (row.get("service_organization_id") and as_text(row["service_organization_id"]).lower() != organization_id):
                fail("runZero returned a record outside the requested organisation; no ticket was created")
            counter = "certificate_records" if collection == "certificates" else "service_records"
            ctx[counter] += 1
            if ctx[counter] > get_int(ctx["kwargs"], "max_" + counter):
                fail("The {} safety limit was exceeded; narrow the scope or review the configured limit. No partial digest was sent".format(counter))
            consume(row)
        cursor = data["next_key"]
        if not cursor:
            return
        if cursor in seen_cursors or not data[collection]:
            fail("runZero {} pagination did not progress; no partial digest was sent".format(collection))
        seen_cursors[cursor] = True

def collect_certificates(ctx):
    groups = {}
    references = {}
    query = "type:x509 and (hidden:true or hidden:false) and valid_until:>{} and valid_until:<={}".format(ctx["checked_at"], ctx["window_end"])
    query = scoped_query(query, get_string(ctx["kwargs"], "certificate_search"))
    for organization_id in ctx["organization_ids"]:
        def consume(row):
            certificate_id = checked_uuid(row.get("id"), "Certificate ID")
            fingerprint = as_text(row.get("fp_sha256")).replace(":", "").lower()
            expiry = parse_ts(row.get("validity_end"), clamp_to_now=False)
            if not re_match("^[0-9a-f]{64}$", fingerprint) or not expiry or row.get("type") != "x509":
                fail("A certificate has no usable SHA-256 fingerprint, X.509 type, or expiry; no partial digest was sent")
            if expiry.unix <= ctx["checked_at"] or expiry.unix > ctx["window_end"]:
                return
            reference = (organization_id, certificate_id)
            if reference in references and references[reference] != fingerprint:
                fail("A certificate ID changed fingerprint during export; retry with a consistent inventory")
            references[reference] = fingerprint
            if fingerprint not in groups:
                groups[fingerprint] = {
                    "fingerprint": fingerprint,
                    "expiry": expiry.unix,
                    "cn": as_text(row.get("cn")),
                    "subject": as_text(row.get("subject")),
                    "issuer": as_text(row.get("issuer")),
                    "serial": as_text(row.get("serial")),
                    "names": [],
                    "is_ca": as_bool(row.get("is_ca")),
                    "self_signed": as_bool(row.get("self_signed")),
                    "public_key_algorithm": as_text(row.get("public_key_algorithm")),
                    "public_key_bits": as_int(row.get("public_key_bits")),
                    "signature_algorithm": as_text(row.get("signature_algorithm")),
                    "weak_key": as_bool(row.get("public_key_insecure")),
                    "weak_signature": as_bool(row.get("signature_algorithm_insecure")),
                    "references": {},
                    "services": {},
                }
            group = groups[fingerprint]
            if group["expiry"] != expiry.unix:
                fail("The same certificate fingerprint has conflicting expiry dates; no partial digest was sent")
            group["names"] = sorted(dedupe(group["names"] + as_list(row.get("names")) + as_list(row.get("san_dns_names")) + as_list(row.get("san_ip_addresses"))))
            group["references"][reference] = True
        walk_export(ctx, organization_id, "certificates", query, consume)
    for reference in sorted(references):
        organization_id, certificate_id = reference
        group = groups[references[reference]]
        def consume(row):
            service_id = checked_uuid(row.get("service_id"), "Service ID")
            asset_id = checked_uuid(row.get("service_asset_id"), "Affected asset ID")
            address = as_text(row.get("service_address"))
            port = as_int(row.get("service_port"))
            transport = as_text(row.get("service_transport"))
            if not address or not transport or port < 1 or port > 65535:
                fail("An affected service has no usable endpoint; no partial digest was sent")
            group["services"][(organization_id, service_id)] = {
                "asset_id": asset_id,
                "organization_id": organization_id,
                "name": as_text(row.get("name")) or ", ".join(dedupe(as_list(row.get("names")))) or "Unnamed host",
                "address": address,
                "port": port,
                "transport": transport,
                "vhost": as_text(row.get("service_vhost")),
                "protocols": ", ".join(dedupe(as_list(row.get("service_protocol")))) or "not recorded",
                "summary": as_text(row.get("service_summary")),
                "site": as_text(row.get("site_name")) or "not recorded",
                "last_seen": as_int(row.get("last_seen")),
                "updated_at": as_int(row.get("service_updated_at")),
                "alive": as_bool(row.get("alive")),
            }
        search = scoped_query("certificate_id:={}".format(certificate_id), get_string(ctx["kwargs"], "service_search"))
        walk_export(ctx, organization_id, "services", search, consume)
    return sorted([group for group in groups.values() if group["services"] or not get_string(ctx["kwargs"], "service_search")], key=lambda group: (group["expiry"], group["fingerprint"]))

def render_digest(ctx, groups):
    hosts = {}
    services = {}
    urgency = [0, 0, 0]
    for group in groups:
        remaining = group["expiry"] - ctx["checked_at"]
        urgency[0 if remaining <= 2 * DAY_SECONDS else (1 if remaining <= 7 * DAY_SECONDS else 2)] += 1
        for identity, service in group["services"].items():
            services[identity] = True
            hosts[(service["organization_id"], service["asset_id"])] = True
    lines = [
        "Certificate renewals for {}".format(get_string(ctx["kwargs"], "scope_label")),
        "",
        "Please review the certificates below and arrange renewal before they expire. This check found {} across {} and {}.".format(quantity(len(groups), "distinct certificate"), quantity(len(hosts), "host"), quantity(len(services), "service")),
        "Start with the earliest expiry: {} ({} remaining).".format(timestamp_text(groups[0]["expiry"]), remaining_text(groups[0]["expiry"], ctx["checked_at"])),
        "Due within 48 hours: {}. Due after 48 hours and within 7 days: {}. Due after 7 days and within 14 days: {}.".format(*urgency),
        "",
        "Inventory checked: {}".format(timestamp_text(ctx["checked_at"])),
        "Expiry window: after {} through {} (inclusive).".format(timestamp_text(ctx["checked_at"]), timestamp_text(ctx["window_end"])),
        "Weekly period starts: {}".format(timestamp_text(ctx["period_start"])),
        "Organisations: {}".format(", ".join(ctx["organization_ids"])),
        "Certificate scope: {}".format(get_string(ctx["kwargs"], "certificate_search") or "All X.509 certificates, including hidden records and CA certificates"),
        "Service scope: {}".format(get_string(ctx["kwargs"], "service_search") or "All associated services"),
        "",
        "What to do",
        "1. Confirm the current certificate at each listed endpoint and identify the service owner. Check the virtual host/SNI as well as the IP and port; shared listeners can serve different certificates.",
        "2. Renew or replace the certificate, preserving the required DNS names and IP SANs. Deploy the correct intermediate chain and check every load balancer, proxy, cluster member and other listed termination point. Never attach a private key to this ticket.",
        "3. Reload or restart the affected service where required, using the normal change and rollback process. If a CA certificate is involved, investigate the chain and dependent trust stores before replacing it.",
        "4. Recheck the served fingerprint, expiry, hostname coverage and trust chain, then run an authorised rescan. Record the owner, change reference and validation result here. If the service is retired, confirm that before removing stale inventory.",
        "",
        "These are inventory observations, not a live TLS validation. Asset last-seen and service update times do not prove that this exact certificate is still being served. Certificates still awaiting renewal can appear again in next week's ticket because the 14-day windows overlap.",
    ]
    for index, group in enumerate(groups):
        name = group["cn"] or (group["names"][0] if group["names"] else group["subject"]) or "Unnamed certificate"
        lines.extend([
            "",
            "{}. {}".format(index + 1, name),
            "Expires: {}".format(timestamp_text(group["expiry"])),
            "Days remaining: {}".format(remaining_text(group["expiry"], ctx["checked_at"])),
            "Issuer: {}".format(group["issuer"] or "not recorded"),
            "Subject: {}".format(group["subject"] or "not recorded"),
            "Names / DNS and IP SANs: {}".format(", ".join(group["names"]) or "not recorded"),
            "Serial (hex): {}".format(group["serial"] or "not recorded"),
            "SHA-256: {}".format(group["fingerprint"]),
            "Key: {} / {}; signature: {}".format(group["public_key_algorithm"] or "not recorded", "{} bits".format(group["public_key_bits"]) if group["public_key_bits"] else "size not recorded", group["signature_algorithm"] or "not recorded"),
            "Certificate role: {}; self-signed: {}".format("CA certificate: assess chain dependencies" if group["is_ca"] else "end-entity certificate", "yes" if group["self_signed"] else "no"),
        ])
        if group["weak_key"] or group["weak_signature"]:
            lines.append("Additional finding: runZero flags {}. Review the replacement's cryptographic settings as part of the renewal.".format(" and ".join((["an insecure public key"] if group["weak_key"] else []) + (["an insecure signature algorithm"] if group["weak_signature"] else []))))
        for organization_id, certificate_id in sorted(group["references"]):
            lines.append("Certificate in runZero: {}/inventory/certificates/{}?{}".format(ctx["runzero_url"], certificate_id, url_encode({"_oid": organization_id})))
            search = scoped_query("certificate_id:={}".format(certificate_id), get_string(ctx["kwargs"], "service_search"))
            lines.append("Affected services in runZero: {}/inventory/services?{}".format(ctx["runzero_url"], url_encode({"_oid": organization_id, "search": search})))
        if not group["services"]:
            lines.append("No associated service was returned. Investigate the certificate record and inventory freshness before assigning a deployment change.")
        for identity in sorted(group["services"]):
            service = group["services"][identity]
            lines.extend([
                "",
                "  Host: {} | site: {} | organisation: {}".format(service["name"], service["site"], service["organization_id"]),
                "  Endpoint: {} port {}/{} | protocols: {}".format(service["address"], service["port"], service["transport"], service["protocols"]),
                "  Virtual host / SNI, where applicable: {}".format(service["vhost"] or "not recorded; confirm the intended hostname"),
                "  Service: {}".format(service["summary"] or "not recorded"),
                "  Asset last seen: {} | service record updated: {}".format(timestamp_text(service["last_seen"]), timestamp_text(service["updated_at"])),
                "  Host in runZero: {}/inventory/{}/{}".format(ctx["runzero_url"], service["organization_id"], service["asset_id"]),
            ])
            if not service["alive"] or not service["last_seen"] or ctx["checked_at"] - service["last_seen"] > WINDOW_SECONDS:
                lines.append("  Please confirm this endpoint is still in use: it is not marked alive, has no last-seen time, or was last seen more than 14 days ago.")
    lines.extend(["", "Digest reference: {}".format(ctx["reference"]), "Complete digest: {}".format(ctx["reference"])])
    return "\n".join(lines)

def make_ticket(ctx, groups):
    details = render_digest(ctx, groups)
    html = details.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;").replace("'", "&#39;").replace("\n", "<br>\n")
    ticket = {
        "summary": "Certificate renewals: {} due within 14 days [{}]".format(quantity(len(groups), "certificate"), ctx["reference"]),
        "details": details,
        "details_html": "<div>" + html + "</div>",
        "third_party_id_string": ctx["reference"],
        "tickettype_id": get_int(ctx["kwargs"], "halo_tickettype_id"),
        "team_id": get_int(ctx["kwargs"], "halo_team_id"),
    }
    for param, field in [("halo_client_id_for_ticket", "client_id"), ("halo_site_id", "site_id"), ("halo_user_id", "user_id"), ("halo_priority_id", "priority_id")]:
        value = get_int(ctx["kwargs"], param)
        if value:
            ticket[field] = value
    if len(bytes(json_encode([ticket]))) > get_int(ctx["kwargs"], "max_ticket_bytes"):
        fail("The complete digest exceeds max_ticket_bytes; no ticket was created. Review the scope or the tenant's accepted size before increasing the limit")
    return ticket

def verify_created_ticket(ctx, ticket_id):
    ticket = read_ticket(ctx, ticket_id)
    if as_text(ticket.get("third_party_id_string")) != ctx["reference"]:
        fail("Halo ticket {} exists but its duplicate-prevention reference was not preserved; stop retries and reconcile it manually".format(ticket_id))
    footer = "Complete digest: " + ctx["reference"]
    if footer not in as_text(ticket.get("details")) and footer not in as_text(ticket.get("details_html")):
        fail("Halo ticket {} exists but the complete digest could not be verified; inspect it before retrying".format(ticket_id))
    if as_int(ticket.get("tickettype_id")) != get_int(ctx["kwargs"], "halo_tickettype_id") or as_int(ticket.get("team_id")) != get_int(ctx["kwargs"], "halo_team_id"):
        fail("Halo ticket {} exists but its type or team differs from the requested routing; review Halo rules without creating another ticket".format(ticket_id))

def main(**kwargs):
    ctx = make_context(kwargs)
    token = authenticate_halo(kwargs)
    ctx["halo_options"] = get_http_options(kwargs, "halo_http_", "halo_tls_", {"Authorization": bearer(token)})
    existing = find_existing_ticket(ctx)
    if existing and not get_bool(kwargs, "dry_run"):
        print("Halo ticket {} already covers {}; no new ticket was created".format(existing, ctx["reference"]))
        return None
    groups = collect_certificates(ctx)
    if not groups:
        print("No certificates in the agreed scope expire within the next 14 days; no Halo ticket was created")
        return None
    ticket = make_ticket(ctx, groups)
    if get_bool(kwargs, "dry_run"):
        print("PREVIEW ONLY; no Halo ticket was created. Existing ticket: {}".format(existing or "none"))
        print(ticket["summary"])
        print(ticket["details"])
        return None
    existing = find_existing_ticket(ctx)
    if existing:
        print("Halo ticket {} was found on the final duplicate check; no new ticket was created".format(existing))
        return None
    ctx["creation_attempted_reference"] = ctx["reference"]
    print("Submitting one ticket for {}. If delivery is not confirmed, pause automatic retries and reconcile this reference in Halo before rerunning".format(ctx["reference"]))
    created, err = post_json(ctx["halo_url"] + "/Tickets", json=[ticket], retries=0, **ctx["halo_options"])
    ticket_id = as_int(as_dict(created).get("id"))
    if err or ticket_id <= 0:
        ticket_id = find_existing_ticket(ctx)
        if not ticket_id:
            fail("Halo creation was not confirmed for {}; the POST was not retried. Stop automatic retries and reconcile this reference in Halo before another execution".format(ctx["reference"]))
    verify_created_ticket(ctx, ticket_id)
    print("Halo ticket {} contains the complete digest of {}: {}".format(ticket_id, quantity(len(groups), "certificate"), ctx["reference"]))
    return None
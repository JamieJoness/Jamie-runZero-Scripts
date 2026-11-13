#!/usr/bin/env python3
"""
runZero asset-loss triage.

Pulls everything needed to work out why an organization's asset inventory
shrank, analyses it, and writes a report plus the raw JSON for runZero support.

What it collects (read-only, GET requests only):
  * license / subscription state
  * every organization: asset counts, ALL retention settings (stale / offline /
    integration-attribute / scan data), project status, last change times
  * per organization: sites (incl. site-level stale expiration), explorers,
    tasks (scan runs, imports, the hidden "Stale Asset Data" cleanup task),
    the surviving assets (first/last seen distribution), export tokens
  * account events (= the audit log) for the lookback window, including the
    API activity events that are the ONLY trace of API-driven deletions
  * organization/account API keys (masked) and users

Usage:
  export RUNZERO_ACCOUNT_TOKEN=...   # account API key (Account -> API keys) - preferred
  export RUNZERO_ORG_TOKEN=...       # fallback: org API key (no events/keys/users/license)
  python3 runzero_asset_loss_triage.py [--url https://console.runzero.com]
                                       [--org-id UUID] [--days 14] [--out DIR]

EU tenants: --url https://console-eu.runzero.com ; self-hosted: --url https://runzero.example.com
Tokens are never written to disk; they are masked the same way runZero masks them in events.
"""

import argparse
import json
import os
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone

API_PREFIX = "/api/v1.0"
WRITE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
API_ACTIONS = {"api-organization", "api-export", "api-client"}
DELETION_ACTIONS = {
    "assets-expired", "asset-removed", "asset-removed-multiple", "assets-purged",
    "organization-purged", "organization-removed", "site-removed", "asset-merge-multiple",
    "service-removed-multiple", "software-removed-multiple",
}
STRUCTURE_ACTIONS = {
    "organization-updated", "organization-created", "site-updated", "site-created",
    "site-imported", "license-changed", "license-limit-exceeded", "account-settings-updated",
    "scan-import", "task-reprocessed", "task-hidden",
}
CREDENTIAL_RE = re.compile(r"api-key|export-token|download-token|api-client-|^user-|^group-|sso|mfa|custom-asset-source")
LOGIN_RE = re.compile(r"^(login|logout|auth-)")
SECRET_KEYS = {"token", "download_token", "export_token", "secret", "password", "client_secret"}
TS_KEYS_RE = re.compile(r"(_at|_expiration|_start|_end|first_seen|last_seen|last_checkin|recur_last|recur_next|start_time|deletes_at|read_only_at)$")
LEVEL_ORDER = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "INFO": 3}
UUID_RE = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
ASSET_FIELDS = ("id,site_id,site_name,first_seen,last_seen,created_at,updated_at,alive,scanned,"
                "sources,detected_by,type,os,addresses,names,last_task_id,last_agent_id")
MAINTENANCE_CLEANUP = "maintenance_asset_cleanup"


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------
def to_epoch(v):
    if v in (None, "", 0, "0", False):
        return 0
    if isinstance(v, bool):
        return 0
    if isinstance(v, (int, float)):
        return int(v)
    if isinstance(v, str):
        if v.isdigit():
            return int(v)
        try:
            return int(datetime.fromisoformat(v.replace("Z", "+00:00")).timestamp())
        except ValueError:
            return 0
    return 0


def fmt_ts(v):
    e = to_epoch(v)
    return datetime.fromtimestamp(e, timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ") if e else "-"


def human_dur(s):
    s = int(abs(s))
    if s < 3600:
        return "%dm" % (s // 60)
    if s < 86400:
        return "%dh%02dm" % (s // 3600, (s % 3600) // 60)
    return "%dd%dh" % (s // 86400, (s % 86400) // 3600)


def ago(v, now):
    e = to_epoch(v)
    if not e:
        return "-"
    return ("in " + human_dur(e - now)) if e > now else (human_dur(now - e) + " ago")


def mask_token(t):
    if not isinstance(t, str) or not t:
        return "-"
    return t if len(t) <= 6 else "%s***%s" % (t[:2], t[-4:])


def scrub(obj):
    """Mask secrets before anything is written to disk."""
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k in SECRET_KEYS and isinstance(v, str):
                out[k] = mask_token(v) if k == "token" else "***"
            else:
                out[k] = scrub(v)
        return out
    if isinstance(obj, list):
        return [scrub(x) for x in obj]
    return obj


def trunc(s, n):
    s = "" if s is None else str(s)
    return s if len(s) <= n else s[: n - 1] + "…"


def as_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def short_id(v):
    v = str(v or "")
    return v[:8] if v else "-"


def norm_path(p):
    p = (p or "").split("?")[0]
    return UUID_RE.sub("{id}", p)


def table(headers, rows, maxw=44):
    if not rows:
        return ["  (none)"]
    rows = [[trunc(c, maxw) for c in r] for r in rows]
    widths = [max(len(str(headers[i])), *(len(r[i]) for r in rows)) for i in range(len(headers))]
    fmt = "  " + "  ".join("{:<%d}" % w for w in widths)
    out = [fmt.format(*[str(h) for h in headers]), "  " + "  ".join("-" * w for w in widths)]
    out += [fmt.format(*r) for r in rows]
    return out


class Report:
    def __init__(self):
        self.lines = []
        self.findings = []

    def h1(self, t):
        self.lines += ["", "=" * 96, t, "=" * 96]

    def h2(self, t):
        self.lines += ["", "-- " + t]

    def p(self, t=""):
        self.lines.append(t)

    def kv(self, k, v):
        self.lines.append("  %-38s %s" % (k, v))

    def table(self, headers, rows, maxw=44):
        self.lines += table(headers, rows, maxw)

    def flag(self, level, title, detail=""):
        self.findings.append({"level": level, "title": title, "detail": detail})
        self.lines.append("  [%s] %s%s" % (level, title, (" -- " + detail) if detail else ""))

    def render(self, header_lines):
        out = list(header_lines)
        out += ["", "=" * 96, "FINDINGS SUMMARY (highest severity first)", "=" * 96]
        if not self.findings:
            out.append("  No anomalies detected by the automatic checks - read the sections below and send the output dir to runZero support.")
        for f in sorted(self.findings, key=lambda f: LEVEL_ORDER.get(f["level"], 9)):
            out.append("  [%-8s] %s" % (f["level"], f["title"]))
            if f["detail"]:
                out.append("             %s" % f["detail"])
        out += self.lines
        return "\n".join(out) + "\n"


# ---------------------------------------------------------------------------
# API client (stdlib only)
# ---------------------------------------------------------------------------
class APIError(Exception):
    def __init__(self, status, url, body=""):
        super().__init__("HTTP %s for %s: %s" % (status, url, trunc(body, 200)))
        self.status = status
        self.url = url


class RZClient:
    def __init__(self, base_url, token, timeout=120, insecure=False):
        self.base = base_url.rstrip("/")
        if self.base.endswith(API_PREFIX):
            self.base = self.base[: -len(API_PREFIX)]
        self.token = token
        self.timeout = timeout
        self.calls = 0
        self.ctx = ssl.create_default_context()
        if insecure:
            self.ctx.check_hostname = False
            self.ctx.verify_mode = ssl.CERT_NONE

    def get(self, path, params=None, org_id=None):
        params = dict(params or {})
        if org_id:
            params["_oid"] = org_id
        url = self.base + API_PREFIX + path
        if params:
            url += "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={
            "Authorization": "Bearer " + self.token,
            "Accept": "application/json",
            "User-Agent": "runzero-asset-loss-triage/1.0",
        })
        for attempt in range(6):
            try:
                self.calls += 1
                with urllib.request.urlopen(req, timeout=self.timeout, context=self.ctx) as resp:
                    body = resp.read()
                return json.loads(body.decode("utf-8")) if body.strip() else None
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", "replace") if e.fp else ""
                if e.code in (429, 500, 502, 503, 504) and attempt < 5:
                    retry_after = e.headers.get("Retry-After") if e.headers else None
                    time.sleep(float(retry_after) if retry_after and retry_after.isdigit() else min(2 ** attempt, 30))
                    continue
                raise APIError(e.code, url, body)
            except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
                if attempt < 5:
                    time.sleep(min(2 ** attempt, 30))
                    continue
                raise APIError(0, url, str(e))

    def paged(self, path, params, key, max_items, org_id=None):
        """Handles both the paged ({key: [...], next_key}) and bare-array response shapes."""
        base_params = dict(params or {})
        params = dict(base_params, page_size=1000)
        items, start_key = [], None
        while True:
            if start_key:
                params["start_key"] = start_key
            try:
                resp = self.get(path, params, org_id)
            except APIError as e:
                if e.status in (400, 422) and not start_key:
                    resp = self.get(path, base_params, org_id)
                else:
                    raise
            if resp is None:
                break
            if isinstance(resp, list):
                items.extend(resp)
                break
            items.extend(resp.get(key) or [])
            start_key = resp.get("next_key")
            if not start_key or len(items) >= max_items:
                break
        return items[:max_items]


# ===========================================================================
# PART 2 - collection
# ===========================================================================
def collect(client, mode, args, errors):
    """Returns a dict with everything fetched. Failures are recorded, never fatal."""
    def fetch(label, fn):
        try:
            return fn()
        except APIError as e:
            errors.append("%s: %s" % (label, e))
            return None

    data = {"mode": mode, "license": None, "orgs": None, "keys": None, "users": None,
            "events": None, "per_org": {}, "events_search": None}

    if mode == "account":
        data["license"] = fetch("GET /account/license", lambda: client.get("/account/license"))
        data["orgs"] = fetch("GET /account/orgs", lambda: client.get("/account/orgs"))
        data["keys"] = fetch("GET /account/keys", lambda: client.get("/account/keys"))
        data["users"] = fetch("GET /account/users", lambda: client.get("/account/users"))
        search = "created_at:<%ddays" % args.days
        data["events_search"] = search
        data["events"] = fetch("GET /account/events.json", lambda: client.paged(
            "/account/events.json", {"search": search}, "events", args.max_events))

    # decide which organizations to dig into
    targets = []
    if mode == "account" and data["orgs"]:
        for o in sorted(data["orgs"], key=lambda o: -as_int(o.get("asset_count"))):
            if args.org_id and str(o.get("id")) != args.org_id:
                continue
            targets.append(str(o.get("id")))
        if not args.org_id:
            targets = targets[: args.max_orgs]
    else:
        targets = [args.org_id] if (mode == "account" and args.org_id) else [None]

    for oid in targets:
        label = oid or "(org key)"
        d = {}
        d["org"] = fetch("GET /org %s" % label, lambda: client.get("/org", org_id=oid))
        if d["org"] is None and oid is None:
            errors.append("the token could not read /org - is it an organization API key?")
            continue
        real_id = str((d["org"] or {}).get("id") or oid or "unknown")
        d["sites"] = fetch("GET /org/sites %s" % label, lambda: client.get("/org/sites", org_id=oid)) or []
        d["explorers"] = fetch("GET /org/explorers %s" % label, lambda: client.get("/org/explorers", org_id=oid))
        if d["explorers"] is None:
            d["explorers"] = fetch("GET /org/agents %s" % label, lambda: client.get("/org/agents", org_id=oid)) or []
        d["tasks"] = fetch("GET /org/tasks %s" % label, lambda: client.get("/org/tasks", org_id=oid)) or []
        d["export_tokens"] = []
        if mode == "account":
            d["export_tokens"] = fetch("GET /account/orgs/%s/exportTokens" % real_id,
                                       lambda: client.get("/account/orgs/%s/exportTokens" % real_id)) or []
        asset_count = as_int((d["org"] or {}).get("asset_count"))
        if args.max_assets > 0 and asset_count <= args.max_assets:
            d["assets"] = fetch("GET /export/org/assets.json %s" % label, lambda: client.paged(
                "/export/org/assets.json", {"fields": ASSET_FIELDS}, "assets", args.max_assets, org_id=oid))
        else:
            d["assets"] = None
            d["assets_skipped_reason"] = "asset_count %d exceeds --max-assets %d" % (asset_count, args.max_assets)
        data["per_org"][real_id] = d
    return data


# ===========================================================================
# PART 3 - analysis
# ===========================================================================
def retention_value(org, key):
    """Current integration-attribute expiration lives in expiration_settings; fall back to the legacy column."""
    es = org.get("expiration_settings") or {}
    if key == "attributes":
        v = es.get("attributes")
        return as_int(v) if v is not None else as_int(org.get("expiration_integration_attributes"))
    if key == "vulnerabilities":
        v = es.get("vulnerabilities")
        return as_int(v) if v is not None else as_int(org.get("expiration_vulnerabilities"))
    return as_int(org.get(key))


def analyze_license(rep, lic, now):
    rep.h1("1. LICENSE / ACCOUNT")
    if lic is None:
        rep.p("  (not available - needs an account API key)")
        return
    for k in ("name", "license_type", "license_max_assets", "license_live_asset_count",
              "license_project_asset_count", "activated_at", "license_expiration",
              "subscription_period_start", "subscription_period_end", "subscription_cancel_at",
              "subscription_canceled_at", "via_reseller", "sso_type"):
        v = lic.get(k)
        rep.kv(k, fmt_ts(v) if TS_KEYS_RE.search(k) else v)
    lt = str(lic.get("license_type") or "").lower()
    exp = to_epoch(lic.get("license_expiration"))
    if "community" in lt:
        rep.flag("HIGH", "Account is on Community Edition",
                 "a downgrade to Community resets every org to 30-day stale/offline retention and deletes all API keys")
    elif "consulting" in lt:
        rep.flag("HIGH", "Consulting license: all organizations are converted to PROJECTS (auto-deleted after 90 days)")
    if exp and exp < now:
        rep.flag("CRITICAL", "License expired %s" % fmt_ts(exp),
                 "expiry triggers the Community downgrade path (30-day retention, API keys removed)")
    elif exp and exp - now < 30 * 86400:
        rep.flag("MEDIUM", "License expires within 30 days (%s)" % fmt_ts(exp))
    cancel = to_epoch(lic.get("subscription_cancel_at")) or to_epoch(lic.get("subscription_canceled_at"))
    if cancel:
        rep.flag("MEDIUM", "Subscription cancellation recorded (%s)" % fmt_ts(cancel))
    mx, live = as_int(lic.get("license_max_assets")), as_int(lic.get("license_live_asset_count"))
    if mx and live > mx:
        rep.flag("MEDIUM", "Live asset count %d exceeds licensed maximum %d" % (live, mx))


def analyze_orgs(rep, orgs, now, since):
    rep.h1("2. ORGANIZATIONS (all visible to this key) - sorted by asset count")
    if orgs is None:
        rep.p("  (not available - needs an account API key; only the single org below was inspected)")
        return
    rows, total = [], 0
    for o in sorted(orgs, key=lambda o: -as_int(o.get("asset_count"))):
        total += as_int(o.get("asset_count"))
        rows.append([o.get("name"), o.get("id"), "Y" if o.get("project") else "", "Y" if o.get("inactive") else "",
                     as_int(o.get("asset_count")), o.get("live_asset_count", ""), o.get("recent_asset_count", ""),
                     o.get("site_count", ""), retention_value(o, "expiration_assets_stale"),
                     retention_value(o, "expiration_assets_offline"), retention_value(o, "attributes"),
                     retention_value(o, "expiration_scans"), fmt_ts(o.get("created_at")), fmt_ts(o.get("updated_at")),
                     fmt_ts(o.get("last_data_change")), fmt_ts(o.get("deletes_at"))])
    rep.table(["name", "id", "proj", "inact", "assets", "live", "recent", "sites", "stale_d", "offl_d",
               "attr_d", "scan_d", "created", "settings_updated", "last_data_change", "deletes_at"], rows, 36)
    rep.kv("Sum of asset_count across orgs", total)
    for o in orgs:
        name = o.get("name")
        if to_epoch(o.get("created_at")) >= since:
            rep.flag("MEDIUM", "Organization '%s' was CREATED inside the lookback window (%s)" % (name, fmt_ts(o.get("created_at"))),
                     "check whether people are looking at a new/empty org, or whether data was moved")
        if o.get("project"):
            rep.flag("HIGH", "Organization '%s' is a PROJECT" % name,
                     "projects have fixed 30-day retention, go read-only after 30 days and are deleted after 90 (deletes_at %s)" % fmt_ts(o.get("deletes_at")))
        if o.get("inactive"):
            rep.flag("MEDIUM", "Organization '%s' is deactivated (%s)" % (name, fmt_ts(o.get("deactivated_at"))))


def retention_flags(rep, org, label, now, since):
    stale = retention_value(org, "expiration_assets_stale")
    offline = retention_value(org, "expiration_assets_offline")
    attrs = retention_value(org, "attributes")
    scans = retention_value(org, "expiration_scans")
    if 0 < stale <= 30:
        rep.flag("CRITICAL", "%s: stale asset expiration is only %d days" % (label, stale),
                 "the nightly job deletes EVERY asset (online or offline) with last_seen older than %d days" % stale)
    elif 0 < stale <= 90:
        rep.flag("HIGH", "%s: stale asset expiration is %d days" % (label, stale), "any asset not seen for %d days is deleted nightly" % stale)
    if 0 < offline <= 30:
        rep.flag("HIGH", "%s: offline asset expiration is only %d days" % (label, offline),
                 "scanned assets marked offline (alive:false) are deleted after %d days" % offline)
    if 0 < attrs <= 90:
        rep.flag("HIGH" if attrs <= 30 else "MEDIUM", "%s: integration attribute expiration is %d days" % (label, attrs),
                 "the daily 'Stale Asset Data' task removes attributes older than this and DELETES assets left with no source - with NO event")
    elif attrs > 0:
        rep.flag("INFO", "%s: integration attribute expiration is %d days (integration-only assets older than this are silently deleted)" % (label, attrs))
    bysrc = (org.get("expiration_settings") or {}).get("attributes_by_source") or {}
    low = {k: v for k, v in bysrc.items() if v is not None and 0 < as_int(v) <= 90}
    if low:
        rep.flag("MEDIUM", "%s: per-source attribute expiration overrides: %s" % (label, json.dumps(low)))
    if 0 < scans < 30:
        rep.flag("MEDIUM", "%s: scan data expiration is %d days" % (label, scans), "raw task data needed for re-import recovery is purged quickly")
    upd = to_epoch(org.get("updated_at"))
    if upd >= since:
        rep.flag("MEDIUM", "%s: organization record (settings) was modified inside the window at %s" % (label, fmt_ts(upd)),
                 "look for organization-updated events or API PATCH /org calls around that time")


def task_stat(t, name):
    stats = t.get("stats") or {}
    return as_int(stats.get("change.%s" % name))


def task_subtype(t):
    return str((t.get("params") or {}).get("subtype") or "")


def analyze_org(rep, org_id, d, now, since, days):
    org = d.get("org") or {}
    name = org.get("name") or org_id
    label = "Org '%s'" % name
    rep.h1("3. ORGANIZATION DETAIL: %s (%s)" % (name, org_id))

    rep.h2("3a. Settings")
    for k in ("asset_count", "live_asset_count", "recent_asset_count", "site_count", "task_count", "live_agent_count",
              "project", "read_only_at", "deletes_at", "inactive", "deactivated_at", "parent_id",
              "expiration_assets_stale", "expiration_assets_offline", "expiration_integration_attributes",
              "keep_latest_integration_attributes", "expiration_vulnerabilities", "expiration_scans",
              "expiration_warning_last_sent", "created_at", "updated_at", "last_data_change", "last_analyzed",
              "export_token_created_at", "export_token_last_used_at", "export_token_last_used_by", "export_token_counter",
              "download_token_created_at"):
        if k in org:
            v = org.get(k)
            rep.kv(k, fmt_ts(v) if TS_KEYS_RE.search(k) or k == "expiration_warning_last_sent" else v)
    rep.kv("expiration_settings (current attr/vuln policy)", json.dumps(org.get("expiration_settings") or {}, sort_keys=True))
    rep.p("  NOTE: 0 = disabled for stale/offline/attribute expiration. Nightly job: stale -> last_seen older than N days (online AND offline);")
    rep.p("        offline -> alive:false AND scanned:true AND runZero source; site-level stale expiration applies on top (lower wins).")
    retention_flags(rep, org, label, now, since)

    # ---- sites
    rep.h2("3b. Sites")
    sites = d.get("sites") or []
    site_names = {str(s.get("id")): s.get("name") for s in sites}
    rows = []
    for s in sorted(sites, key=lambda s: -as_int(s.get("asset_count"))):
        rows.append([s.get("name"), s.get("id"), as_int(s.get("asset_count")), s.get("live_asset_count", ""),
                     s.get("recent_asset_count", ""), as_int(s.get("expiration_assets_stale")), s.get("task_count", ""),
                     fmt_ts(s.get("last_task_at")), fmt_ts(s.get("last_data_change")), fmt_ts(s.get("created_at")),
                     fmt_ts(s.get("updated_at"))])
        se = as_int(s.get("expiration_assets_stale"))
        if se > 0:
            lvl = "CRITICAL" if se <= 30 else ("HIGH" if se <= 90 else "MEDIUM")
            rep.flag(lvl, "%s / site '%s': SITE-LEVEL stale asset expiration = %d days" % (label, s.get("name"), se),
                     "site value overrides the org value when lower; assets in this site not seen for %d days are deleted nightly" % se)
        if to_epoch(s.get("updated_at")) >= since:
            rep.flag("INFO", "%s / site '%s' configuration updated inside the window (%s)" % (label, s.get("name"), fmt_ts(s.get("updated_at"))))
    rep.table(["site", "id", "assets", "live", "recent", "stale_d", "tasks", "last_task_at", "last_data_change",
               "created", "updated"], rows, 36)

    # ---- explorers
    rep.h2("3c. Explorers")
    rows = []
    for a in d.get("explorers") or []:
        rows.append([a.get("name"), "yes" if a.get("connected") else "NO", fmt_ts(a.get("last_checkin")),
                     a.get("version"), site_names.get(str(a.get("site_id")), short_id(a.get("site_id"))),
                     "Y" if a.get("inactive") else "", a.get("external_ip") or a.get("internal_ip")])
        if not a.get("connected") and not a.get("inactive"):
            rep.flag("INFO", "%s: explorer '%s' is disconnected (last check-in %s)" % (label, a.get("name"), fmt_ts(a.get("last_checkin"))))
    rep.table(["explorer", "connected", "last_checkin", "version", "site", "inactive", "ip"], rows)

    # ---- tasks
    rep.h2("3d. Tasks in the last %d days (chronological; new/changed/unchanged/offline are asset counts)" % days)
    tasks = d.get("tasks") or []

    def t_end(t):
        return to_epoch(t.get("updated_at")) or to_epoch(t.get("start_time")) or to_epoch(t.get("created_at"))

    def t_start(t):
        return to_epoch(t.get("start_time")) or to_epoch(t.get("created_at"))

    recent = [t for t in tasks if not t.get("recur") and max(t_end(t), t_start(t)) >= since]
    recent.sort(key=t_end)
    rows = []
    for t in recent:
        rows.append([fmt_ts(t_start(t)), fmt_ts(t_end(t)), t.get("type"), task_subtype(t), t.get("name"), t.get("status"),
                     site_names.get(str(t.get("site_id")), short_id(t.get("site_id"))), task_stat(t, "newAssets"),
                     task_stat(t, "changedAssets"), task_stat(t, "unchangedAssets"), task_stat(t, "offlineAssets"),
                     t.get("created_by") or "", trunc(t.get("error") or "", 40)])
    rep.table(["started", "ended/updated", "type", "subtype", "name", "status", "site", "new", "chg", "unchg", "offl",
               "created_by", "error"], rows, 30)
    if len(tasks) >= 1000:
        rep.p("  NOTE: the API returns at most the last 1000 tasks; older runs in the window may be missing.")

    rep.h2("3d-ii. Recurring task definitions")
    rows = []
    for t in sorted(tasks, key=lambda t: str(t.get("name"))):
        if t.get("recur"):
            rows.append([t.get("name"), t.get("type"), task_subtype(t), t.get("status"), t.get("recur_frequency"),
                         fmt_ts(t.get("recur_last")), fmt_ts(t.get("recur_next")),
                         site_names.get(str(t.get("site_id")), short_id(t.get("site_id")))])
    rep.table(["name", "type", "subtype", "status", "frequency", "last_run", "next_run", "site"], rows)

    missing_sites = sorted({str(t.get("site_id")) for t in tasks
                            if t.get("site_id") and str(t.get("site_id")) not in site_names
                            and str(t.get("site_id")) != "00000000-0000-0000-0000-000000000000"})
    if missing_sites and sites:
        rep.flag("CRITICAL", "%s: tasks reference %d site ID(s) that no longer exist" % (label, len(missing_sites)),
                 "deleting a site deletes all of its assets; look for site-removed events. IDs: %s" % ", ".join(missing_sites[:5]))

    cleanup_runs = [t for t in recent if task_subtype(t) == MAINTENANCE_CLEANUP]
    if cleanup_runs:
        rep.flag("MEDIUM", "%s: the 'Stale Asset Data' cleanup task ran %d time(s) in the window (last %s)" %
                 (label, len(cleanup_runs), fmt_ts(t_end(cleanup_runs[-1]))),
                 "this task deletes integration-only assets whose attributes exceeded the attribute expiration, and emits NO event - ask runZero support for its log")

    errored = [t for t in recent if str(t.get("status")) in ("error", "failed")]
    if errored:
        rep.flag("INFO", "%s: %d task(s) ended in error in the window" % (label, len(errored)))

    # inventory-reset detection: first scan/import whose results were almost entirely "new"
    data_tasks = [t for t in recent if t.get("type") not in ("analysis", "maintenance") and str(t.get("status")) == "processed"]
    prev = None
    for t in data_tasks:
        new, chg, unchg = task_stat(t, "newAssets"), task_stat(t, "changedAssets"), task_stat(t, "unchangedAssets")
        seen = new + chg + unchg
        if seen >= 20 and new / seen >= 0.9 and (prev is None or (task_stat(prev, "newAssets") / max(1, task_stat(prev, "newAssets") + task_stat(prev, "changedAssets") + task_stat(prev, "unchangedAssets"))) < 0.5):
            lo = fmt_ts(t_end(prev)) if prev else "(no earlier task in window)"
            rep.flag("HIGH", "%s: inventory RESET detected - task '%s' (ended %s) saw %d assets and %d were NEW" % (label, t.get("name"), fmt_ts(t_end(t)), seen, new),
                     "the inventory was empty for these addresses before it ran; the loss happened between %s and %s" % (lo, fmt_ts(t_start(t))))
            break
        prev = t
    for t in data_tasks:
        offl = task_stat(t, "offlineAssets")
        seen = task_stat(t, "newAssets") + task_stat(t, "changedAssets") + task_stat(t, "unchangedAssets")
        if offl >= 1000 and offl > seen:
            rep.flag("MEDIUM", "%s: task '%s' (%s) marked %d assets OFFLINE" % (label, t.get("name"), fmt_ts(t_end(t)), offl),
                     "a scan that misses most of a site marks assets offline; offline expiration can then delete them")

    # ---- surviving assets
    rep.h2("3e. Surviving assets")
    assets = d.get("assets")
    if assets is None:
        rep.p("  asset export skipped: %s" % d.get("assets_skipped_reason", "request failed"))
    else:
        n = len(assets)
        rep.kv("assets exported", "%d (org asset_count says %s)" % (n, org.get("asset_count")))
        if n:
            buckets = [("< 1 day", 1), ("1-7 days", 7), ("7-30 days", 30), ("30-180 days", 180), ("> 180 days", None)]

            def bucketize(key):
                c = Counter()
                for a in assets:
                    age = now - to_epoch(a.get(key))
                    for lbl, lim in buckets:
                        if lim is None or age < lim * 86400:
                            c[lbl] += 1
                            break
                return c

            fs, ls = bucketize("first_seen"), bucketize("last_seen")
            rep.table(["age bucket", "by first_seen", "by last_seen"], [[b, fs.get(b, 0), ls.get(b, 0)] for b, _ in buckets])
            first = [to_epoch(a.get("first_seen")) for a in assets if to_epoch(a.get("first_seen"))]
            created = [to_epoch(a.get("created_at")) for a in assets if to_epoch(a.get("created_at"))]
            rep.kv("oldest first_seen", fmt_ts(min(first)) if first else "-")
            rep.kv("oldest created_at (record creation)", fmt_ts(min(created)) if created else "-")
            rep.kv("alive / offline", "%d / %d" % (sum(1 for a in assets if a.get("alive")), sum(1 for a in assets if not a.get("alive"))))
            rep.kv("scanned:true / scanned:false", "%d / %d" % (sum(1 for a in assets if a.get("scanned")), sum(1 for a in assets if not a.get("scanned"))))
            srcs = Counter()
            for a in assets:
                s = a.get("sources")
                for x in (s if isinstance(s, list) else [s]):
                    if x:
                        srcs[x.get("name") if isinstance(x, dict) else str(x)] += 1
            rep.kv("sources", ", ".join("%s=%d" % kv for kv in srcs.most_common(12)) or "-")
            per_site = Counter(a.get("site_name") or site_names.get(str(a.get("site_id")), short_id(a.get("site_id"))) for a in assets)
            rep.kv("per site", ", ".join("%s=%d" % kv for kv in per_site.most_common(12)))
            in_window = sum(1 for a in assets if to_epoch(a.get("first_seen")) >= since)
            if in_window / n >= 0.95:
                rep.flag("HIGH", "%s: %d of %d surviving assets were FIRST SEEN inside the last %d days (oldest %s)" % (label, in_window, n, days, fmt_ts(min(first)) if first else "-"),
                         "the inventory was effectively empty before that time - everything older was deleted, not aged out gradually")
            if created and first and min(created) > min(first) + 3600:
                rep.flag("INFO", "%s: asset records were created (%s) later than their first_seen (%s) - consistent with a re-import of scan data" % (label, fmt_ts(min(created)), fmt_ts(min(first))))
        if as_int(org.get("asset_count")) and abs(as_int(org.get("asset_count")) - n) > max(50, 0.2 * n):
            rep.flag("INFO", "%s: cached asset_count (%s) differs from exported assets (%d) - stats may still be recalculating" % (label, org.get("asset_count"), n))

    # ---- export tokens
    rep.h2("3f. Export tokens")
    rows = [[mask_token(t.get("token")), t.get("comment"), fmt_ts(t.get("created_at")), fmt_ts(t.get("last_used_at")),
             t.get("last_used_by"), t.get("counter")] for t in d.get("export_tokens") or []]
    rep.table(["token", "comment", "created", "last_used", "last_used_by", "uses"], rows)


def ev_details(e):
    det = e.get("details") or {}
    if not isinstance(det, dict):
        return trunc(json.dumps(det), 90)
    parts = []
    for k in ("expired_count", "count", "method", "path", "ip", "key_type", "name", "user_email", "cidr_private"):
        if k in det:
            parts.append("%s=%s" % (k, det[k]))
    if isinstance(det.get("asset_ids"), list):
        parts.append("asset_ids=%d" % len(det["asset_ids"]))
    return trunc(" ".join(parts) if parts else json.dumps(det, sort_keys=True), 110)


def analyze_events(rep, data, now, since, days):
    events = data.get("events")
    org_names = {str(o.get("id")): o.get("name") for o in (data.get("orgs") or [])}
    for oid, d in data["per_org"].items():
        org_names.setdefault(oid, (d.get("org") or {}).get("name"))

    rep.h1("4. EVENTS / AUDIT LOG - last %d days, account-wide (search: %s)" % (days, data.get("events_search")))
    if events is None:
        rep.p("  (not available - the account events export needs an account API key)")
        return
    rep.kv("events in window", len(events))
    if not events:
        rep.flag("MEDIUM", "No events were returned for the window", "check the key's permissions, the --url (US/EU/self-hosted) and widen --days")
        return
    events = sorted(events, key=lambda e: to_epoch(e.get("created_at")))

    def row(e):
        return [fmt_ts(e.get("created_at")), e.get("action"), "%s:%s" % (e.get("source_type"), e.get("source_name")),
                "%s:%s" % (e.get("target_type"), e.get("target_name")),
                org_names.get(str(e.get("organization_id")), short_id(e.get("organization_id"))), ev_details(e)]

    rep.h2("4a. Event counts by action")
    rep.table(["action", "count"], [[a, c] for a, c in Counter(e.get("action") for e in events).most_common(60)])

    rep.h2("4b. Deletion / expiration / purge events")
    dels = [e for e in events if e.get("action") in DELETION_ACTIONS]
    rep.table(["time", "action", "source", "target", "org", "details"], [row(e) for e in dels[-300:]], 60)
    expired = defaultdict(int)
    for e in dels:
        a, det, oname = e.get("action"), e.get("details") or {}, org_names.get(str(e.get("organization_id")), short_id(e.get("organization_id")))
        src = "%s %s" % (e.get("source_type"), e.get("source_name"))
        if a == "assets-expired":
            expired[oname] += as_int(det.get("expired_count"))
        elif a == "asset-removed-multiple":
            n = len(det.get("asset_ids") or []) or as_int(det.get("count"))
            rep.flag("HIGH", "%d asset(s) bulk-deleted in '%s' by %s at %s" % (n, oname, src, fmt_ts(e.get("created_at"))))
        elif a in ("assets-purged", "organization-purged"):
            rep.flag("CRITICAL", "ALL assets purged from '%s' by %s at %s" % (oname, src, fmt_ts(e.get("created_at"))))
        elif a == "organization-removed":
            rep.flag("CRITICAL", "Organization deleted: %s by %s at %s" % (e.get("target_name"), src, fmt_ts(e.get("created_at"))))
        elif a == "site-removed":
            rep.flag("CRITICAL", "Site '%s' deleted in '%s' by %s at %s - all of its assets went with it" % (e.get("target_name"), oname, src, fmt_ts(e.get("created_at"))))
    single = Counter("%s %s" % (e.get("source_type"), e.get("source_name")) for e in dels if e.get("action") == "asset-removed")
    for src, c in single.items():
        rep.flag("HIGH" if c >= 100 else "MEDIUM", "%d single-asset deletions by %s" % (c, src))
    for oname, n in expired.items():
        rep.flag("HIGH", "Nightly expiration job removed %d asset(s) from '%s' in the window" % (n, oname),
                 "driven by the stale/offline expiration values - see section 3a/3b for the current settings")

    rep.h2("4c. Settings / structure / license events")
    struct = [e for e in events if e.get("action") in STRUCTURE_ACTIONS]
    rep.table(["time", "action", "source", "target", "org", "details"], [row(e) for e in struct[-200:]], 60)
    for e in struct:
        a, src, t = e.get("action"), "%s %s" % (e.get("source_type"), e.get("source_name")), fmt_ts(e.get("created_at"))
        if a in ("organization-updated", "site-updated"):
            rep.flag("MEDIUM", "%s '%s' by %s at %s" % (a, e.get("target_name") or (e.get("details") or {}).get("name"), src, t),
                     "settings changes are not diffed in the event - compare the current retention values against what the customer expects")
        elif a == "license-changed":
            rep.flag("HIGH", "License entitlement changed at %s" % t, "downgrades reset retention and can convert orgs to projects")
        elif a == "license-limit-exceeded":
            rep.flag("MEDIUM", "Asset count exceeded the license limit at %s" % t)
        elif a == "organization-created":
            rep.flag("INFO", "Organization created: %s by %s at %s" % (e.get("target_name"), src, t))

    rep.h2("4d. API WRITE activity (the only trace of API deletions / settings changes)")
    api = [e for e in events if e.get("action") in API_ACTIONS]
    groups = {}
    for e in api:
        det = e.get("details") or {}
        method = str(det.get("method") or "GET").upper()
        if method not in WRITE_METHODS:
            continue
        key = (str(e.get("source_name")), str(det.get("key_id") or ""), method, norm_path(str(det.get("path") or "")))
        g = groups.setdefault(key, {"n": 0, "first": None, "last": None, "ips": set(), "orgs": set(), "paths": set()})
        g["n"] += 1
        ts = to_epoch(e.get("created_at"))
        g["first"] = ts if g["first"] is None else min(g["first"], ts)
        g["last"] = ts if g["last"] is None else max(g["last"], ts)
        g["ips"].add(str(det.get("ip") or "-"))
        g["orgs"].add(org_names.get(str(e.get("organization_id")), short_id(e.get("organization_id"))))
        g["paths"].add(str(det.get("path") or "")[:160])
    rows = []
    for (src, kid, method, path), g in sorted(groups.items(), key=lambda kv: kv[1]["first"] or 0):
        rows.append([fmt_ts(g["first"]), fmt_ts(g["last"]), g["n"], src, short_id(kid), method, path,
                     ",".join(sorted(g["ips"]))[:40], ",".join(sorted(g["orgs"]))[:40]])
        who = "key %s (id %s, ip %s)" % (src, short_id(kid), ",".join(sorted(g["ips"])))
        when = "%s .. %s" % (fmt_ts(g["first"]), fmt_ts(g["last"]))
        if "/assets/bulk/delete" in path or (method == "DELETE" and "/assets" in path):
            rep.flag("CRITICAL", "%d API asset-deletion call(s) by %s" % (g["n"], who),
                     "%s %s during %s - these NEVER appear as asset-removed events. Example: %s" % (method, path, when, sorted(g["paths"])[0]))
        elif method == "DELETE" and "/org/sites/" in path:
            rep.flag("CRITICAL", "Site deleted via API by %s (%s)" % (who, when))
        elif method == "DELETE" and "/account/orgs/" in path:
            rep.flag("CRITICAL", "Organization deleted via API by %s (%s)" % (who, when))
        elif method in ("PATCH", "PUT") and (path.endswith("/org") or "/account/orgs/" in path or "/org/sites/" in path):
            rep.flag("HIGH", "Org/site settings changed via API by %s" % who, "%s %s during %s - retention values can be set this way" % (method, path, when))
        elif "import" in path:
            rep.flag("INFO", "Data imported via API by %s (%s %s, %d calls)" % (who, method, path, g["n"]))
    rep.table(["first", "last", "calls", "key", "key_id", "method", "path", "ips", "orgs"], rows, 50)

    rep.h2("4d-ii. API usage per key (all methods)")
    per_key = defaultdict(lambda: {"n": 0, "w": 0, "first": None, "last": None, "ips": set()})
    for e in api:
        det = e.get("details") or {}
        k = "%s / %s" % (e.get("source_name"), short_id(det.get("key_id")))
        g = per_key[k]
        g["n"] += 1
        if str(det.get("method") or "GET").upper() in WRITE_METHODS:
            g["w"] += 1
        ts = to_epoch(e.get("created_at"))
        g["first"] = ts if g["first"] is None else min(g["first"], ts)
        g["last"] = ts if g["last"] is None else max(g["last"], ts)
        g["ips"].add(str(det.get("ip") or "-"))
    rep.table(["key / key_id", "calls", "writes", "first", "last", "ips"],
              [[k, g["n"], g["w"], fmt_ts(g["first"]), fmt_ts(g["last"]), ",".join(sorted(g["ips"]))] for k, g in sorted(per_key.items(), key=lambda kv: -kv[1]["n"])], 50)

    rep.h2("4e. Credential / user / SSO events")
    cred = [e for e in events if CREDENTIAL_RE.search(str(e.get("action") or ""))]
    rep.table(["time", "action", "source", "target", "org", "details"], [row(e) for e in cred[-150:]], 60)
    for e in cred:
        if str(e.get("action")).endswith(("api-key-created", "api-client-created", "export-token-created")):
            rep.flag("MEDIUM", "%s by %s %s at %s" % (e.get("action"), e.get("source_type"), e.get("source_name"), fmt_ts(e.get("created_at"))),
                     "new credentials shortly before the loss deserve a look")

    rep.h2("4f. Logins per user")
    logins = defaultdict(lambda: {"n": 0, "fail": 0, "ips": set(), "last": 0})
    for e in events:
        if LOGIN_RE.search(str(e.get("action") or "")):
            g = logins[str(e.get("source_name"))]
            g["n"] += 1
            if e.get("success") is False or "fail" in str(e.get("action")):
                g["fail"] += 1
            g["ips"].add(str((e.get("details") or {}).get("ip") or "-"))
            g["last"] = max(g["last"], to_epoch(e.get("created_at")))
    rep.table(["user", "events", "failures", "last", "ips"],
              [[u, g["n"], g["fail"], fmt_ts(g["last"]), ",".join(sorted(g["ips"]))[:60]] for u, g in sorted(logins.items(), key=lambda kv: -kv[1]["n"])], 60)


def analyze_keys_users(rep, data, now, since):
    rep.h1("5. API KEYS AND USERS")
    org_names = {str(o.get("id")): o.get("name") for o in (data.get("orgs") or [])}
    keys = data.get("keys")
    rep.h2("5a. API keys (tokens masked exactly as runZero shows them in events)")
    if keys is None:
        rep.p("  (not available)")
    else:
        rows = []
        for k in sorted(keys, key=lambda k: -to_epoch(k.get("last_used_at"))):
            rows.append([mask_token(k.get("token")), short_id(k.get("id")), k.get("type"),
                         org_names.get(str(k.get("organization_id")), short_id(k.get("organization_id"))), k.get("comment"),
                         k.get("created_by"), fmt_ts(k.get("created_at")), fmt_ts(k.get("last_used_at")), k.get("last_used_ip"),
                         k.get("counter"), k.get("usage_today"), "Y" if k.get("inactive") else ""])
            if to_epoch(k.get("created_at")) >= since:
                rep.flag("MEDIUM", "API key %s (%s) was created inside the window by %s" % (mask_token(k.get("token")), k.get("type"), k.get("created_by")))
            if to_epoch(k.get("last_used_at")) >= since:
                rep.flag("INFO", "API key %s (%s, '%s') used inside the window - last %s from %s, %s total calls" %
                         (mask_token(k.get("token")), k.get("type"), k.get("comment"), fmt_ts(k.get("last_used_at")), k.get("last_used_ip"), k.get("counter")))
        rep.table(["token", "id", "type", "org", "comment", "created_by", "created", "last_used", "last_ip", "calls", "today", "inactive"], rows, 30)

    rep.h2("5b. Users")
    users = data.get("users")
    if users is None:
        rep.p("  (not available)")
    else:
        rows = []
        for u in sorted(users, key=lambda u: -to_epoch(u.get("last_login_at"))):
            rows.append([u.get("email"), "Y" if u.get("client_admin") else "", u.get("org_default_role"),
                         fmt_ts(u.get("last_login_at")), u.get("last_login_ip"), fmt_ts(u.get("last_activity_at")),
                         fmt_ts(u.get("last_action_at")), "Y" if u.get("mfa_enabled") else "N", "Y" if u.get("sso_only") else ""])
        rep.table(["email", "admin", "default_role", "last_login", "last_ip", "last_activity", "last_action", "mfa", "sso"], rows, 40)
        active_admins = [u.get("email") for u in users if u.get("client_admin") and to_epoch(u.get("last_activity_at") or u.get("last_login_at")) >= since]
        if active_admins:
            rep.flag("INFO", "Account admins active in the window: %s" % ", ".join(map(str, active_admins)))


NEXT_STEPS = """
NEXT STEPS
  1. Pin the time window: use the inventory-reset finding (3d) and the oldest first_seen (3e); that is the window
     to search in sections 4b/4d. If nothing in 4b/4d explains it, the deletion happened server-side (expiration job,
     'Stale Asset Data' task, project expiry, license downgrade) or outside the retained events.
  2. Stop the bleeding: set stale/offline expiration to 0 at org AND site level until the cause is confirmed; do not
     lower scan data expiration (raw task data is what re-imports are rebuilt from); rotate any suspicious API key.
  3. Open a priority ticket with runZero support (support@runzero.com) and attach the output directory. Ask them to
     check the expiration-job and 'Stale Asset Data' task logs for the org IDs/time window - deletions by those jobs
     are only partially (or not at all) visible in the console - and whether a backup restore is possible.
  4. Self-service recovery: Tasks -> recent scan task -> Download task data -> Import (runZero scan data) rebuilds
     assets from the raw results (history/first_seen is lost).
"""


def parse_args():
    ap = argparse.ArgumentParser(description="Collect everything needed to explain a runZero asset-count drop.")
    ap.add_argument("--url", default=os.environ.get("RUNZERO_URL", "https://console.runzero.com"),
                    help="console URL (US default; EU: https://console-eu.runzero.com; or self-hosted)")
    ap.add_argument("--account-token", default=os.environ.get("RUNZERO_ACCOUNT_TOKEN"), help="account API key (preferred)")
    ap.add_argument("--org-token", default=os.environ.get("RUNZERO_ORG_TOKEN"), help="organization API key (fallback, limited)")
    ap.add_argument("--org-id", default=os.environ.get("RUNZERO_ORG_ID"), help="only inspect this organization UUID")
    ap.add_argument("--days", type=int, default=14, help="lookback window in days (default 14)")
    ap.add_argument("--out", help="output directory (default runzero-triage-<timestamp>)")
    ap.add_argument("--max-assets", type=int, default=20000, help="export surviving assets only if the org has at most this many (0 = never)")
    ap.add_argument("--max-events", type=int, default=100000)
    ap.add_argument("--max-orgs", type=int, default=25, help="how many orgs to inspect in depth when --org-id is not given")
    ap.add_argument("--timeout", type=int, default=120)
    ap.add_argument("--insecure", action="store_true", help="skip TLS verification (self-hosted with private CA)")
    return ap.parse_args()


def main():
    args = parse_args()
    if not (args.account_token or args.org_token):
        sys.exit("set RUNZERO_ACCOUNT_TOKEN (preferred) or RUNZERO_ORG_TOKEN, or pass --account-token/--org-token")
    now = int(time.time())
    since = now - args.days * 86400
    errors = []

    mode, token = ("account", args.account_token) if args.account_token else ("org", args.org_token)
    client = RZClient(args.url, token, args.timeout, args.insecure)
    if mode == "account":
        try:
            client.get("/account/license")
        except APIError as e:
            if e.status in (401, 403):
                errors.append("the token given as the account key cannot read /account/license (HTTP %d) - treating it as an organization key" % e.status)
                mode = "org"
            else:
                sys.exit("cannot reach %s: %s" % (args.url, e))

    print("Collecting from %s as %s key, lookback %d days ..." % (args.url, mode, args.days), file=sys.stderr)
    data = collect(client, mode, args, errors)

    out = args.out or "runzero-triage-%s" % datetime.now().strftime("%Y%m%d-%H%M%S")
    os.makedirs(out, exist_ok=True)

    rep = Report()
    analyze_license(rep, data.get("license"), now)
    analyze_orgs(rep, data.get("orgs"), now, since)
    for oid, d in data["per_org"].items():
        analyze_org(rep, oid, d, now, since, args.days)
    analyze_events(rep, data, now, since, args.days)
    analyze_keys_users(rep, data, now, since)
    rep.h1("6. COLLECTION ERRORS / GAPS")
    for e in errors:
        rep.p("  - " + e)
    if not errors:
        rep.p("  none")
    rep.p(NEXT_STEPS)
    rep.h1("7. SEND TO RUNZERO SUPPORT")
    rep.kv("console", args.url)
    rep.kv("organization IDs", ", ".join(data["per_org"].keys()) or "-")
    rep.kv("lookback", "%s .. %s" % (fmt_ts(since), fmt_ts(now)))
    rep.kv("output directory", os.path.abspath(out))

    header = ["runZero asset-loss triage  -  %s" % fmt_ts(now),
              "console: %s   auth: %s API key   lookback: %d days (since %s)   API calls: %d" % (args.url, mode, args.days, fmt_ts(since), client.calls)]
    text = rep.render(header)

    with open(os.path.join(out, "report.txt"), "w") as f:
        f.write(text)
    with open(os.path.join(out, "findings.json"), "w") as f:
        json.dump(sorted(rep.findings, key=lambda f: LEVEL_ORDER.get(f["level"], 9)), f, indent=2)
    for name in ("license", "orgs", "keys", "users", "events"):
        if data.get(name) is not None:
            with open(os.path.join(out, name + ".json"), "w") as f:
                json.dump(scrub(data[name]), f, indent=1)
    for oid, d in data["per_org"].items():
        odir = os.path.join(out, "org-" + oid)
        os.makedirs(odir, exist_ok=True)
        for name in ("org", "sites", "explorers", "tasks", "assets", "export_tokens"):
            if d.get(name) is not None:
                with open(os.path.join(odir, name + ".json"), "w") as f:
                    json.dump(scrub(d[name]), f, indent=1)
    print(text)
    print("Report and raw JSON written to %s" % os.path.abspath(out), file=sys.stderr)


if __name__ == "__main__":
    main()

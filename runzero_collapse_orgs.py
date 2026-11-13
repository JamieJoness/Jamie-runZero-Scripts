#!/usr/bin/env python3
"""
================================================================================
  runZero Organisation Collapse  —  many organisations → one flat organisation
================================================================================

WHAT IT DOES
------------
Collapses a set of SOURCE organisations (parents and children alike) on the
SAME console into a single TARGET organisation. For every source org it:

  1. SITES       Creates a Site in the target named after the source org and
                 carries over the source site's registered subnets (CIDR +
                 tags/description), scope and excludes. If an org unexpectedly
                 has several sites, each becomes "<org> - <site>".
  2. EXPLORERS   Works out which Explorer belonged to each source org and finds
                 the same machine in the target org (you move Explorers in the
                 console; this matches them by machine id, then hostname — no
                 manual mapping needed). By default pins each Explorer to its
                 new site; one shared by several orgs/sites is left unpinned
                 (= usable by any site) instead.
  3. SCAN TASKS  Recreates the org's recurring scan tasks against the new site
                 using that Explorer — preserving name, description, probes,
                 ports, rate and every other parameter, frequency, next run
                 time, grace period, tags, and paused state. Source tasks that
                 are "active", "paused" OR "error" are migrated — "error" is the
                 state a recurring task enters once its Explorer has been moved
                 away ("explorer unavailable"), so it must not be skipped.
  4. CHECKLIST   Inventories recurring integration (connector) tasks and the
                 credentials they use. These CANNOT be created via the public
                 API — the checklist is printed and saved for manual re-creation.

SAFETY
------
  • Source organisations are READ-ONLY. This is enforced in the HTTP client,
    not just by convention: every non-GET request must carry the target
    organisation's id, anything else raises before it is sent. Every write is
    logged and summarised at the end of the run.
  • Every org-scoped read is checked to contain only rows belonging to the
    organisation that was asked for, so a wrong key type or a scoping surprise
    aborts the run before anything is written.
  • Assets are NOT migrated — the new sites populate on their first scan.
  • Re-running is safe: sites are matched by name; tasks are matched by a
    provenance marker "[collapse src-task=<source task id>]" appended to the
    task description (and, failing that, by site + name + frequency).

WHAT YOU DO
-----------
  1. Create (or choose) the target organisation in the console.
  2. Move each Explorer from its old org into the target org in the console
     (Explorers → select → Reassign to organisation). The public API cannot
     move Explorers between organisations. The reassign is a reinstall: the
     Explorer re-registers under the target as a new record with the same
     machine id and hostname — which is what this script matches on.
  3. Fill in the CONFIGURATION block: console URL, Account API key (or export
     RZ_ACCOUNT_TOKEN so the key never lands in the file) and the target org.
     EXPLORER_MAP can stay empty unless you want to force a particular Explorer.
  4. Run with DRY_RUN = True first (default — nothing is written):
         python3 runzero_collapse_orgs.py
  5. Set DRY_RUN = False and run again to perform the changes.
  6. Afterwards, PAUSE the old recurring tasks in the source orgs by hand:
     they keep firing "explorer unavailable" failures (and alerts) otherwise.
     This script never touches them.

  Sites can be created before the Explorers are moved: tasks for an org whose
  Explorer isn't in the target yet (or is offline) are skipped with a clear
  message — run again once the Explorers are in place.

KNOWN LIMITS OF THE PUBLIC API (handled by flagging, not silently)
-----------------------------------------------------------------
  • Integration/connector sync tasks (AWS, Azure, CrowdStrike, …) — UI only.
  • Credential selections on scan tasks (SNMP v3, SSH, WMI, …) are redacted by
    the API and the scan endpoint cannot set them — tasks are created WITHOUT
    credentials and flagged. SNMP community strings revert to the default.
  • Scan template links: a template the target org cannot see (not global, not
    shared with it) cannot be linked — the task is created with its full inline
    parameters instead and the dropped link is noted.
  • Explorer groups are org-scoped — tasks using one fall back to the mapped
    Explorer.
  • Targets using the public:/private: inventory keywords expand against the
    TARGET org's inventory, which is empty until the first scan.
  • Users/roles, alert rules, org API keys/export tokens, saved queries,
    goals, dashboards — recreate manually.
"""

import calendar
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import requests


# ============================================================
# CONFIGURATION — edit the values below before running
# ============================================================
CONSOLE_BASE_URL = "https://console.runzero.com/api/v1.0"   # EU: https://console-eu.runzero.com/api/v1.0
ACCOUNT_TOKEN    = "RZ_ACCOUNT_TOKEN EXPORT PREFERABLE"               # Account API key (CT-…): Account → API keys.
                                                              # Or: export RZ_ACCOUNT_TOKEN="CT-…" (takes precedence)
TARGET_ORG       = "ORG NAME OR UUID"            # existing organisation everything collapses into

# Source organisations. Leave SOURCE_ORGS empty to collapse EVERY other org on
# the account (child orgs included). Otherwise list names or UUIDs — child orgs
# of a listed org are included automatically while INCLUDE_CHILD_ORGS is True.
SOURCE_ORGS: List[str] = []
EXCLUDE_ORGS: List[str] = []
INCLUDE_CHILD_ORGS   = True
INCLUDE_PROJECT_ORGS = True      # project (temporary) orgs are skipped unless True

# Explorer mapping is AUTOMATIC: each source org's Explorers are matched to the
# ones you moved into the target org by machine id, then by hostname. Normally
# leave this empty. Use it only to FORCE an Explorer for an org — e.g. you
# installed a brand-new Explorer in the target instead of moving the old one.
# Key: source org name (or UUID; child orgs by their own name).
# Value: Explorer name (or UUID) as shown in the TARGET org.
EXPLORER_MAP: Dict[str, str] = {
    # "Acme Retail": "NEW-RETAIL-RZ-01",
}

SITE_NAME_TEMPLATE = "{org}"                 # placeholders: {org}, {site}
PIN_EXPLORERS      = True                    # pin each mapped Explorer to its new site (shared ones stay unpinned)
MIGRATE_TASKS      = True                    # False = create sites + pin Explorers only
DRY_RUN            = True                    # True = read-only preview; False = make the changes
SKIP_CONFIRMATION  = True                   # True = no "type yes" prompt when DRY_RUN is False
REPORT_FILE        = "collapse_report.json"  # JSON report of everything done / to do by hand
# ============================================================

ACCOUNT_TOKEN = os.environ.get("RZ_ACCOUNT_TOKEN", "").strip() or ACCOUNT_TOKEN
APPLY = not DRY_RUN

NIL_UUID = "00000000-0000-0000-0000-000000000000"
REDACTED = "[redacted]"
# Params the API redacts or that only the console UI can set — never replayed.
SECRET_PARAM_KEYS = {"credentials", "credentialsencrypted"}
# Request options this script sets explicitly; never taken from source params.
CONTROL_KEYS = {
    "template-id", "scan-template", "scan_template", "explorer", "agent", "explorer-group-id",
    "hosted-zone-id", "hosted-zone-name", "scan-name", "scan-description", "scan-frequency",
    "scan-start", "scan-grace-period",
}
# Params the scan endpoint normalises itself; excluded from the post-create diff.
NORMALISED_PARAM_KEYS = {"targets", "excludes", "probes"}
# Recurring parents in these states are live task definitions (see docstring).
LIVE_RECURRING_STATUSES = {"active", "paused", "error"}
# Frequencies with a time-of-day anchor worth preserving; anything else starts now.
ANCHORED_FREQUENCIES = {"daily", "weekly", "monthly", "nth_weekday"}
INVENTORY_KEYWORDS = ("public:", "private:")
# Scan-task option naming the custom integration the task runs (runzero/custom_integration_params.go).
CUSTOM_INTEGRATION_ID_KEY = "custom-integration-id"
# runzero.ProbeDefaultsValue — stored by the console, expanded by the hub at dispatch; the
# public scan endpoint does not accept it (nor probe names from older runZero versions).
PROBE_DEFAULTS = "defaults"
UNKNOWN_PROBE_RE = re.compile(r"unknown probe:\s*([^\s,;]+)", re.I)
MAX_CREATE_ATTEMPTS = 12
# Tasks that would otherwise start immediately start this far out, so the follow-up PATCH
# (probes / params / paused) is in place before the hub can dispatch the first run.
FIRST_RUN_DELAY_SECONDS = 120
# Integrations that must run on an Explorer (on-prem sources); their connection settings
# (URL, base DN, …) are task-level, so otherwise-identical tasks may still differ.
EXPLORER_ONLY_INTEGRATIONS = {"LDAP", "VMware", "MECM", "Nessus", "Rapid7 Nexpose", "InsightVM", "Tenable Security Center", "NetBox"}
MARKER_RE = re.compile(r"\[collapse src-task=([0-9a-f-]{36})\]", re.I)
TIMEOUT_SECONDS = 120
MAX_RETRIES = 6

# runzero/sources/sources.go
SOURCE_NAMES = {
    -1: "Custom integration", 1: "runZero", 2: "Miradore", 3: "AWS", 4: "CrowdStrike",
    5: "Azure", 6: "Censys", 7: "VMware", 8: "GCP", 9: "SentinelOne", 10: "Tenable",
    11: "Nessus", 12: "Rapid7 Nexpose", 13: "InsightVM", 14: "Qualys", 15: "Shodan",
    16: "Azure AD / Entra", 17: "LDAP", 18: "Microsoft 365 Defender", 19: "Intune",
    20: "Google Workspace", 21: "Sample", 22: "Tenable Security Center", 23: "Packet",
    24: "Wiz", 25: "Meraki", 26: "MECM", 27: "Tanium", 28: "Simulator", 29: "NetBox",
    30: "CIP", 31: "Palo Alto Networks", 32: "Prisma Cloud", 33: "AWS Role", 34: "Dragos",
    35: "BACnet", 36: "Modbus", 37: "KNXnet", 38: "InsightVM Cloud", 39: "S7COMM",
    40: "DNP3", 41: "PROFINET", 42: "EtherCAT", 43: "Omron FINS", 44: "MELSEC-Q",
    45: "HART-IP", 46: "C37.118", 47: "ADS", 48: "OPC UA", 49: "S7", 50: "IEC 104",
    51: "MMS", 52: "Censys Platform", 53: "LonTalk", 54: "OCSF",
}


# ── small helpers ─────────────────────────────────────────────────────────────

def norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()


def as_list(data: Any) -> List[Dict]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("items", "results", "data"):
            if isinstance(data.get(key), list):
                return data[key]
    return []


def to_str(v: Any) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if v is None:
        return ""
    return str(v)


def fmt_ts(ts: Any) -> str:
    try:
        ts = int(ts)
        if ts <= 0:
            return "now"
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    except (TypeError, ValueError):
        return str(ts)


def short(s: str, n: int = 8) -> str:
    return (s or "")[:n]


def heading(title: str) -> None:
    bar = "═" * (len(title) + 4)
    print(f"\n╔{bar}╗\n║  {title}  ║\n╚{bar}╝")


def table(rows: List[List[str]], headers: List[str]) -> None:
    if not rows:
        return
    widths = [len(h) for h in headers]
    for r in rows:
        for i, cell in enumerate(r):
            widths[i] = min(max(widths[i], len(cell)), 52)

    def fit(cell: str, w: int) -> str:
        return cell if len(cell) <= w else cell[: w - 1] + "…"

    print("  " + "  ".join(fit(h, widths[i]).ljust(widths[i]) for i, h in enumerate(headers)))
    print("  " + "  ".join("─" * w for w in widths))
    for r in rows:
        print("  " + "  ".join(fit(c, widths[i]).ljust(widths[i]) for i, c in enumerate(r)))


# ── HTTP ──────────────────────────────────────────────────────────────────────

class Client:
    """Account-token API client. Org-scoped endpoints receive `_oid`.

    Two hard safety rails that do not depend on the calling code being right:
      • no non-GET request is sent while DRY_RUN is on;
      • no non-GET request is sent with an `_oid` other than the one locked via
        `lock_writes_to()` (the target org). A bug elsewhere in this script
        cannot write to a source organisation — it raises instead.
    Writes are never retried after a network error or 5xx (they are not
    idempotent); only 429 rate limits are waited out. Re-running is safe.
    """

    def __init__(self, base_url: str, token: str, apply: bool):
        self.base_url = base_url.rstrip("/")
        self.apply = apply
        self.write_oid: Optional[str] = None
        self.writes: List[Dict[str, str]] = []
        self.s = requests.Session()
        self.s.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "runzero-collapse-orgs/1.1",
        })

    def lock_writes_to(self, oid: str) -> None:
        self.write_oid = oid

    def request(self, method: str, path: str, *, oid: str = "", params: Optional[Dict] = None,
                json_body: Any = None) -> requests.Response:
        is_write = method != "GET"
        if is_write:
            if not self.apply:
                raise RuntimeError(f"BUG: write attempted in dry-run mode: {method} {path}")
            if not self.write_oid:
                raise RuntimeError(f"BUG: write attempted before the target organisation was locked: {method} {path}")
            if oid != self.write_oid:
                raise RuntimeError(f"BUG: refusing {method} {path} for organisation '{oid or '(none)'}' — "
                                   f"writes are locked to the target {self.write_oid}")
        params = dict(params or {})
        if oid:
            params["_oid"] = oid
        url = f"{self.base_url}{path}"
        backoff = 1.0
        attempt = 0
        while True:
            attempt += 1
            try:
                if is_write:
                    self.writes.append({"method": method, "path": path, "oid": oid})
                resp = self.s.request(method, url, params=params, json=json_body, timeout=TIMEOUT_SECONDS)
            except requests.RequestException as exc:
                if is_write or attempt >= MAX_RETRIES:
                    raise RuntimeError(f"{method} {path}: network error: {exc}") from exc
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                try:
                    wait = min(float(ra) if ra else max(backoff, 30.0), 300.0)
                except (TypeError, ValueError):
                    wait = max(backoff, 30.0)
                print(f"  ⏳ rate limited — sleeping {int(wait)}s", file=sys.stderr)
                time.sleep(wait)
                backoff = min(backoff * 2, 60)
                if is_write:
                    self.writes.pop()  # the retried request will be logged again
                continue
            if not is_write and resp.status_code in (500, 502, 503, 504) and attempt < MAX_RETRIES:
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            return resp

    def get(self, path: str, oid: str = "", params: Optional[Dict] = None) -> Any:
        resp = self.request("GET", path, oid=oid, params=params)
        if resp.status_code >= 400:
            raise RuntimeError(f"GET {path}{f' (org {short(oid)}…)' if oid else ''} → HTTP {resp.status_code}: "
                               f"{explain_http_error(resp, oid)}")
        return resp.json()

    def get_optional(self, path: str, oid: str = "", params: Optional[Dict] = None, what: str = "") -> List[Dict]:
        """For nice-to-have lookups (templates, credentials) that must not abort the run."""
        try:
            return as_list(self.get(path, oid=oid, params=params))
        except (RuntimeError, ValueError) as exc:
            print(f"  ⚠ could not load {what or path}: {exc}")
            return []


def err_text(resp: requests.Response) -> str:
    try:
        j = resp.json()
        if isinstance(j, dict):
            # The API puts the cause in "detail" (singular); older handlers use "details".
            return "; ".join(str(j[k]) for k in ("error", "detail", "details") if j.get(k)) or resp.text[:200]
    except ValueError:
        pass
    return resp.text[:200]


def explain_http_error(resp: requests.Response, oid: str) -> str:
    text = err_text(resp)
    if resp.status_code == 401:
        return f"{text} — ACCOUNT_TOKEN was rejected. It must be an *Account* API key (CT-…); org keys cannot be used."
    if resp.status_code == 403:
        return f"{text} — the key lacks access (org-restricted account key, IP allow-list, or expired licence)."
    if resp.status_code == 400 and oid and "_oid" in text:
        return f"{text} — the key cannot reach organisation {oid}."
    return text


def fetch_scoped(api: Client, path: str, oid: str, what: str, params: Optional[Dict] = None) -> List[Dict]:
    """GET an org-scoped list and verify every row belongs to `oid`.
    Guards against any `_oid` scoping surprise before a single write happens."""
    rows = as_list(api.get(path, oid=oid, params=params))
    for r in rows:
        row_oid = r.get("organization_id")
        if row_oid and row_oid != oid:
            raise RuntimeError(f"{what}: the API returned a row for organisation {row_oid} when {oid} was requested — "
                               f"aborting. Check ACCOUNT_TOKEN is an Account API key for this console.")
    return rows


# ── organisations ─────────────────────────────────────────────────────────────

def resolve_org(orgs: List[Dict], ident: str, what: str) -> Dict:
    ident_n = norm(ident)
    by_id = [o for o in orgs if norm(o.get("id")) == ident_n]
    if by_id:
        return by_id[0]
    by_name = [o for o in orgs if norm(o.get("name")) == ident_n]
    if len(by_name) > 1:
        opts = ", ".join(f"{o['id']} (parent {short(o.get('parent_id') or '') or 'none'})" for o in by_name)
        sys.exit(f"✗ {what}: organisation name '{ident}' is ambiguous — use its UUID instead: {opts}")
    if not by_name:
        sys.exit(f"✗ {what}: organisation '{ident}' not found on this account")
    return by_name[0]


def org_depth(org: Dict, by_id: Dict[str, Dict]) -> int:
    depth, cur, seen = 0, org, set()
    while cur.get("parent_id") and cur["parent_id"] != NIL_UUID and cur["id"] not in seen:
        seen.add(cur["id"])
        cur = by_id.get(cur["parent_id"]) or {}
        if not cur:
            break
        depth += 1
    return depth


def select_sources(orgs: List[Dict], target: Dict, include: List[str], exclude: List[str],
                   descendants: bool, include_projects: bool) -> List[Dict]:
    by_id = {o["id"]: o for o in orgs}
    children: Dict[str, List[Dict]] = {}
    for o in orgs:
        pid = o.get("parent_id") or ""
        if pid and pid != NIL_UUID:
            children.setdefault(pid, []).append(o)

    def walk(o: Dict, acc: List[Dict]) -> None:
        acc.append(o)
        for ch in sorted(children.get(o["id"], []), key=lambda x: norm(x.get("name"))):
            walk(ch, acc)

    chosen: List[Dict] = []
    if include:
        for ident in include:
            o = resolve_org(orgs, ident, "SOURCE_ORGS")
            if descendants:
                walk(o, chosen)
            else:
                chosen.append(o)
    else:
        roots = [o for o in orgs if not o.get("parent_id") or o["parent_id"] == NIL_UUID
                 or o["parent_id"] not in by_id]
        for root in sorted(roots, key=lambda x: norm(x.get("name"))):
            walk(root, chosen)

    excluded_ids = {resolve_org(orgs, x, "EXCLUDE_ORGS")["id"] for x in exclude}
    out, seen = [], set()
    for o in chosen:
        if o["id"] in seen or o["id"] == target["id"] or o["id"] in excluded_ids:
            continue
        if o.get("inactive") or o.get("demo"):
            continue
        if o.get("project") and not include_projects:
            continue
        seen.add(o["id"])
        out.append(o)
    return out


# ── sites ─────────────────────────────────────────────────────────────────────

def plan_site_names(sources: List[Dict], sites_by_org: Dict[str, List[Dict]],
                    template: str, by_id: Dict[str, Dict]) -> Dict[Tuple[str, str], str]:
    """Returns {(org_id, site_id): target_site_name}, de-duplicating collisions."""
    names: Dict[Tuple[str, str], str] = {}
    for org in sources:
        sites = sites_by_org.get(org["id"], [])
        multi = len(sites) > 1
        for site in sites:
            name = template.format(org=org["name"], site=site["name"])
            if multi and "{site}" not in template:
                name = f"{name} - {site['name']}"
            names[(org["id"], site["id"])] = name.strip()

    counts: Dict[str, int] = {}
    for n in names.values():
        counts[norm(n)] = counts.get(norm(n), 0) + 1
    for key, n in list(names.items()):
        if counts[norm(n)] > 1:
            org = by_id[key[0]]
            parent = by_id.get(org.get("parent_id") or "")
            names[key] = f"{parent['name']} - {n}" if parent else f"{n} ({short(org['id'])})"
    return names


def site_body(org: Dict, site: Dict, name: str) -> Dict[str, Any]:
    body: Dict[str, Any] = {"name": name}
    desc = (site.get("description") or "").strip()
    body["description"] = desc or f"Migrated from organisation '{org['name']}' ({org['id']})"
    if site.get("scope"):
        body["scope"] = site["scope"]
    if site.get("excludes"):
        body["excludes"] = site["excludes"]
    if isinstance(site.get("subnets"), dict) and site["subnets"]:
        body["subnets"] = site["subnets"]
    return body


def merged_site_patch(existing: Dict, body: Dict[str, Any]) -> Dict[str, Any]:
    """Union of registered subnets / scope / excludes so re-runs never drop data."""
    patch: Dict[str, Any] = {}
    ex_subnets = existing.get("subnets") if isinstance(existing.get("subnets"), dict) else {}
    new_subnets = body.get("subnets") or {}
    if any(k not in ex_subnets for k in new_subnets):
        merged = dict(ex_subnets)
        merged.update({k: v for k, v in new_subnets.items() if k not in ex_subnets})
        patch["subnets"] = merged
    for field in ("scope", "excludes"):
        ex_lines = [l.strip() for l in (existing.get(field) or "").splitlines() if l.strip()]
        new_lines = [l.strip() for l in (body.get(field) or "").splitlines() if l.strip()]
        missing = [l for l in new_lines if l not in ex_lines]
        if missing:
            patch[field] = "\n".join(ex_lines + missing)
    return patch


# ── explorers ─────────────────────────────────────────────────────────────────

def find_explorers(explorers: List[Dict], ident: str) -> List[Dict]:
    ident_n = norm(ident)
    if not ident_n:
        return []
    by_id = [e for e in explorers if norm(e.get("id")) == ident_n]
    return by_id or [e for e in explorers if norm(e.get("name")) == ident_n]


def explorer_status(e: Dict) -> str:
    if e.get("inactive"):
        return "inactive"
    return "online" if e.get("connected") else "offline"


def explorer_site(e: Dict) -> str:
    return e.get("site_id") or NIL_UUID


def explorer_usable_for(e: Dict, site_id: str) -> bool:
    """The scan API only accepts an Explorer that is unpinned or pinned to the task's site."""
    return explorer_site(e) in (NIL_UUID, site_id)


# ── scan tasks ────────────────────────────────────────────────────────────────

def base_frequency(freq: str) -> str:
    return (freq or "").split(",")[0].strip().lower()


def period_seconds(freq: str) -> int:
    return {"weekly": 7 * 86400, "monthly": 30 * 86400, "nth_weekday": 30 * 86400, "daily": 86400}.get(
        base_frequency(freq), 3600)


def next_start_time(task: Dict, now: int) -> int:
    """Keep the schedule anchor (time of day / weekday / day of month) but
    project it into the future, since the API clamps past start times to now.
    Sub-daily and continuous schedules return 0 (= start now)."""
    recur_next = int(task.get("recur_next") or 0)
    if recur_next > now:
        return recur_next
    start = int(task.get("start_time") or 0)
    freq = base_frequency(task.get("recur_frequency") or "")
    if start <= 0 or freq not in ANCHORED_FREQUENCIES:
        return 0
    if freq in ("monthly", "nth_weekday"):
        dt = datetime.fromtimestamp(start, tz=timezone.utc)
        while start <= now:
            month = dt.month % 12 + 1
            year = dt.year + (1 if dt.month == 12 else 0)
            day = min(dt.day, calendar.monthrange(year, month)[1])
            dt = dt.replace(year=year, month=month, day=day)
            start = int(dt.timestamp())
        return start
    step = 7 * 86400 if freq == "weekly" else 86400
    while start <= now:
        start += step
    return start


def build_replay_params(task: Dict) -> Tuple[Dict[str, str], bool, List[str]]:
    """Source params → flat string map for PUT /org/sites/{id}/scan.
    Returns (params, had_scan_credentials, redacted_param_keys)."""
    params = task.get("params") or {}
    replay: Dict[str, str] = {}
    had_creds = False
    redacted: List[str] = []
    for k, v in params.items():
        sv = to_str(v)
        lk = k.lower()
        if lk in CONTROL_KEYS:
            continue
        if lk in SECRET_PARAM_KEYS:
            had_creds = had_creds or (bool(sv) and sv != "0")
            continue
        if sv == REDACTED:
            redacted.append(k)
            continue
        replay[k] = sv
    return replay, had_creds, sorted(redacted)


def marker(task_id: str) -> str:
    return f"[collapse src-task={task_id}]"


def scan_options(task: Dict, replay: Dict[str, str], explorer_id: str, hosted_zone_id: str,
                 start_ts: int, template_id: str) -> Dict[str, str]:
    opts = dict(replay)
    opts["scan-name"] = task.get("name") or "Unnamed scan"
    desc = MARKER_RE.sub("", task.get("description") or "").strip()
    opts["scan-description"] = f"{desc}\n{marker(task['id'])}" if desc else marker(task["id"])
    opts["scan-frequency"] = task.get("recur_frequency") or "daily"
    if start_ts > 0:
        opts["scan-start"] = str(start_ts)
    if task.get("grace_period"):
        opts["scan-grace-period"] = str(task["grace_period"])
    if hosted_zone_id:
        opts["hosted-zone-id"] = hosted_zone_id
    else:
        opts["explorer"] = explorer_id
    if template_id:
        opts["scan-template"] = template_id
    return opts


def params_diff(replay: Dict[str, str], created: Dict) -> Dict[str, str]:
    """Params the scan endpoint ignored (e.g. max-data-acquisition-hours,
    network-disruption-action, autonomous profile) to restore via PATCH."""
    created_params = created.get("params") or {}
    return {
        k: v for k, v in replay.items()
        if k.lower() not in NORMALISED_PARAM_KEYS and to_str(created_params.get(k)) != v
    }


# ── configuration ─────────────────────────────────────────────────────────────

def normalise_base_url(url: str) -> str:
    url = (url or "").strip().rstrip("/")
    if url and "/api/v" not in url:
        url += "/api/v1.0"
    return url


def validate_config() -> str:
    problems = []
    base_url = normalise_base_url(CONSOLE_BASE_URL)
    if not base_url.startswith("http"):
        problems.append("CONSOLE_BASE_URL must be an https URL, e.g. https://console.runzero.com/api/v1.0")
    if not ACCOUNT_TOKEN or ACCOUNT_TOKEN.startswith("PASTE_"):
        problems.append("ACCOUNT_TOKEN is not set (Account API key, starts with CT-) — edit the script or export RZ_ACCOUNT_TOKEN")
    if not TARGET_ORG or TARGET_ORG.startswith("PASTE_"):
        problems.append("TARGET_ORG is not set")
    try:
        SITE_NAME_TEMPLATE.format(org="x", site="y")
        if "{org}" not in SITE_NAME_TEMPLATE and "{site}" not in SITE_NAME_TEMPLATE:
            raise ValueError
    except (KeyError, IndexError, ValueError):
        problems.append("SITE_NAME_TEMPLATE must contain {org} and/or {site} and no other braces")
    if not isinstance(EXPLORER_MAP, dict) or any(not str(v).strip() for v in EXPLORER_MAP.values()):
        problems.append("EXPLORER_MAP must be a dict of \"<org name or id>\": \"<explorer name or id>\" with no empty values")
    for name, value in (("SOURCE_ORGS", SOURCE_ORGS), ("EXCLUDE_ORGS", EXCLUDE_ORGS)):
        if not isinstance(value, (list, tuple)):
            problems.append(f"{name} must be a list of names/UUIDs")
    if problems:
        sys.exit("✗ " + "\n✗ ".join(problems))
    if not ACCOUNT_TOKEN.startswith("CT"):
        print("⚠ ACCOUNT_TOKEN does not look like an Account API key (CT-…). Org keys ignore _oid and cannot "
              "see other orgs — the run will abort at the first account-level call if this is the wrong key type.")
    return base_url


def confirm_or_exit(target: Dict, sources: List[Dict]) -> None:
    if not APPLY or SKIP_CONFIRMATION:
        return
    print(f"\n⚠ APPLY mode: sites, Explorer pins and recurring scan tasks WILL be created in "
          f"'{target['name']}' ({target['id']}) from {len(sources)} source organisation(s).")
    print("  Source organisations are never modified (writes are locked to the target in the HTTP client).")
    answer = input("  Type 'yes' to continue: ").strip().lower()
    if answer != "yes":
        sys.exit("Aborted.")


def write_report(path: str, report: Dict[str, Any]) -> None:
    try:
        with open(path, "w") as f:
            json.dump(report, f, indent=2)
        print(f"\n  Report written to {path}")
    except OSError as exc:
        print(f"\n  ⚠ could not write report to {path}: {exc}")


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    base_url = validate_config()
    mode = "APPLY" if APPLY else "DRY RUN"
    api = Client(base_url, ACCOUNT_TOKEN, apply=APPLY)
    now = int(time.time())
    report: Dict[str, Any] = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "mode": mode, "base_url": api.base_url, "organisations": [],
        "integration_tasks": [], "credential_access": [], "writes": [],
    }

    # ── 0. Organisations ──
    heading(f"Phase 0: Organisations  [{mode}]")
    orgs = as_list(api.get("/account/orgs"))
    if not orgs:
        sys.exit("✗ /account/orgs returned no organisations")
    by_id = {o["id"]: o for o in orgs}
    target = resolve_org(orgs, TARGET_ORG, "TARGET_ORG")
    if target.get("inactive"):
        sys.exit(f"✗ target organisation '{target['name']}' is inactive")
    api.lock_writes_to(target["id"])
    include = [x.strip() for x in SOURCE_ORGS if x and x.strip()]
    exclude = [x.strip() for x in EXCLUDE_ORGS if x and x.strip()]
    sources = select_sources(orgs, target, include, exclude, INCLUDE_CHILD_ORGS, INCLUDE_PROJECT_ORGS)
    if not sources:
        sys.exit("✗ no source organisations selected")

    print(f"  Target: '{target['name']}' ({target['id']})")
    print(f"  Sources ({len(sources)}):")
    for o in sources:
        indent = "    " + "  " * org_depth(o, by_id)
        print(f"{indent}• {o['name']}  ({short(o['id'])}…, {o.get('asset_count', '?')} assets)")
    report["target"] = {"id": target["id"], "name": target["name"]}

    # EXPLORER_MAP sanity: typos and ambiguous names are caught here, before any write.
    explorer_map = {norm(k): str(v).strip() for k, v in EXPLORER_MAP.items()}
    source_names: Dict[str, int] = {}
    for o in sources:
        source_names[norm(o["name"])] = source_names.get(norm(o["name"]), 0) + 1
    source_keys = set(source_names) | {norm(o["id"]) for o in sources}
    for key in explorer_map:
        if key not in source_keys:
            hint = " (that is the target org)" if key in (norm(target["name"]), norm(target["id"])) else ""
            print(f"  ⚠ EXPLORER_MAP key '{key}' does not match any selected source organisation{hint} — ignored")
    ambiguous = [k for k in explorer_map if source_names.get(k, 0) > 1]
    if ambiguous:
        sys.exit(f"✗ EXPLORER_MAP: these names match several source organisations — key them by UUID instead: "
                 f"{', '.join(ambiguous)}")

    confirm_or_exit(target, sources)

    # ── 1. Sites ──
    heading("Phase 1: Sites + registered subnets")
    sites_by_org: Dict[str, List[Dict]] = {}
    for org in sources:
        sites = [s for s in fetch_scoped(api, "/org/sites", org["id"], f"sites of '{org['name']}'")
                 if not s.get("inactive")]
        # The list normally includes subnets; fall back to a per-site fetch if a build omits them.
        sites_by_org[org["id"]] = [s if "subnets" in s else api.get(f"/org/sites/{s['id']}", oid=org["id"])
                                   for s in sites]
    names = plan_site_names(sources, sites_by_org, SITE_NAME_TEMPLATE, by_id)

    target_sites = [s for s in fetch_scoped(api, "/org/sites", target["id"], "target sites") if not s.get("inactive")]
    tgt_by_name: Dict[str, Dict] = {norm(s["name"]): s for s in target_sites}
    site_map: Dict[str, str] = {}            # source site id → target site id (planned placeholder in dry run)
    target_site_names: Dict[str, str] = {s["id"]: s["name"] for s in target_sites}   # target site id → name
    rows = []
    for org in sources:
        org_rep: Dict[str, Any] = {"id": org["id"], "name": org["name"], "sites": [], "tasks": []}
        report["organisations"].append(org_rep)
        if not sites_by_org[org["id"]]:
            rows.append([org["name"], "-", "-", "0", "✗ no active sites"])
            continue
        for site in sites_by_org[org["id"]]:
            name = names[(org["id"], site["id"])]
            body = site_body(org, site, name)
            subnet_n = len(body.get("subnets") or {})
            existing = tgt_by_name.get(norm(name))
            if existing:
                if "subnets" not in existing:
                    existing = api.get(f"/org/sites/{existing['id']}", oid=target["id"])
                patch = merged_site_patch(existing, body)
                site_map[site["id"]] = existing["id"]
                target_site_names[existing["id"]] = existing["name"]
                if patch and APPLY:
                    resp = api.request("PATCH", f"/org/sites/{existing['id']}", oid=target["id"], json_body=patch)
                    status = (f"exists — merged {', '.join(patch)}" if resp.status_code < 400
                              else f"✗ update failed: {err_text(resp)}")
                elif patch:
                    status = f"exists — would merge {', '.join(patch)}"
                else:
                    status = "exists — up to date"
            elif APPLY:
                resp = api.request("PUT", "/org/sites", oid=target["id"], json_body=body)
                if resp.status_code < 400:
                    created = resp.json()
                    site_map[site["id"]] = created["id"]
                    target_site_names[created["id"]] = name
                    tgt_by_name[norm(name)] = created
                    status = "created"
                else:
                    status = f"✗ create failed: {err_text(resp)}"
                    if resp.status_code == 403 and "licensed" in err_text(resp).lower():
                        status += " → the account lacks the create_sites entitlement; ask runZero support to enable it"
            else:
                # Distinct placeholder per planned site so dry-run pin/unpin decisions match apply.
                site_map[site["id"]] = f"DRY_RUN:{site['id']}"
                target_site_names[site_map[site["id"]]] = name
                status = "would create"
            rows.append([org["name"], site["name"], name, str(subnet_n), status])
            org_rep["sites"].append({"source_site_id": site["id"], "source_site_name": site["name"],
                                     "target_site_name": name, "target_site_id": site_map.get(site["id"]),
                                     "subnets": subnet_n, "status": status})
    table(rows, ["Source org", "Source site", "Target site", "Subnets", "Status"])

    # ── 2. Explorers ──
    heading("Phase 2: Explorers")
    tgt_explorers = [e for e in fetch_scoped(api, "/org/explorers", target["id"], "target explorers")
                     if not e.get("inactive")]
    print(f"  Explorers currently in target org: {len(tgt_explorers)}")

    # Each source org's own Explorer records stay listed (offline) after the move; the
    # moved Explorer re-registers in the target with the same machine id and hostname.
    src_explorers_by_org: Dict[str, List[Dict]] = {}
    for org in sources:
        src_explorers_by_org[org["id"]] = [
            e for e in fetch_scoped(api, "/org/explorers", org["id"], f"explorers of '{org['name']}'")
            if not e.get("inactive")
        ]

    def match_target_explorer(src: Dict) -> Tuple[Optional[Dict], str]:
        """Source Explorer record → the same machine's record in the target org.
        Hostname is the key: the Explorer's host id is stored per organisation
        (<org id>.agentid), so a moved Explorer gets a NEW host id. The host id
        is only useful as a tie-breaker (or when pinned via RUNZERO_AGENT_HOST_ID)."""
        hid = (src.get("host_id") or "").strip()
        by_name = find_explorers(tgt_explorers, src.get("name") or "")
        if len(by_name) == 1:
            return by_name[0], "hostname"
        if len(by_name) > 1:
            by_host = [e for e in by_name if hid and (e.get("host_id") or "").strip() == hid]
            if len(by_host) == 1:
                return by_host[0], "hostname + host id"
            return None, f"ambiguous: {len(by_name)} target explorers are named '{src.get('name')}' — use EXPLORER_MAP"
        by_host = [e for e in tgt_explorers if hid and (e.get("host_id") or "").strip() == hid]
        if len(by_host) == 1:
            return by_host[0], "host id"
        return None, "not in target org yet — move it, then re-run"

    def describe_site(e: Dict) -> str:
        pinned = explorer_site(e)
        return "unpinned (any site)" if pinned == NIL_UUID else f"pinned to '{target_site_names.get(pinned, short(pinned) + '…')}'"

    org_override: Dict[str, Dict] = {}     # source org id → target explorer forced via EXPLORER_MAP
    src_to_tgt: Dict[str, Dict] = {}       # source explorer id → matching target explorer
    org_matched: Dict[str, List[Dict]] = {}  # source org id → distinct target explorers matched from its records
    rows = []
    for org in sources:
        org_rep = next(r for r in report["organisations"] if r["id"] == org["id"])
        override = explorer_map.get(norm(org["id"])) or explorer_map.get(norm(org["name"]))
        if override:
            matches = find_explorers(tgt_explorers, override)
            if len(matches) == 1:
                org_override[org["id"]] = matches[0]
                rows.append([org["name"], f"(EXPLORER_MAP: {override})", matches[0].get("name", ""), "override",
                             explorer_status(matches[0]), describe_site(matches[0])])
                org_rep["explorers"] = [{"override": override, "target_explorer": matches[0].get("name"),
                                         "target_explorer_id": matches[0]["id"]}]
            else:
                why = "not found in target org" if not matches else f"ambiguous ({len(matches)} matches) — use its UUID"
                rows.append([org["name"], f"(EXPLORER_MAP: {override})", "-", f"✗ {why}", "-", "-"])
                org_rep["explorers"] = [{"override": override, "error": why}]
            continue
        recs = src_explorers_by_org[org["id"]]
        if not recs:
            rows.append([org["name"], "-", "-", "no explorers in source org", "-", "-"])
            org_rep["explorers"] = []
            continue
        org_rep["explorers"] = []
        for src in sorted(recs, key=lambda x: norm(x.get("name"))):
            e, how = match_target_explorer(src)
            if e:
                src_to_tgt[src["id"]] = e
                if all(x["id"] != e["id"] for x in org_matched.setdefault(org["id"], [])):
                    org_matched[org["id"]].append(e)
                rows.append([org["name"], src.get("name", ""), e.get("name", ""), how, explorer_status(e), describe_site(e)])
            else:
                rows.append([org["name"], src.get("name", ""), "-", f"✗ {how}", "-", "-"])
            org_rep["explorers"].append({"source_explorer": src.get("name"), "source_explorer_id": src["id"],
                                         "target_explorer": (e or {}).get("name"), "target_explorer_id": (e or {}).get("id"),
                                         "matched_by": how if e else None, "error": None if e else how})
    table(rows, ["Source org", "Source explorer", "Target explorer", "Matched by", "State", "Current site"])
    mapped_ids = {e["id"] for e in src_to_tgt.values()} | {e["id"] for e in org_override.values()}
    untouched = [e["name"] for e in tgt_explorers if e["id"] not in mapped_ids]
    if untouched:
        print(f"  ℹ target explorers not linked to any source org (left untouched): {', '.join(untouched)}")

    # Resolve which target explorer each source scan task will run on, before deciding pins.
    # Order: EXPLORER_MAP override → the same machine that ran the task (Phase 2 match) →
    # a target explorer with the task's explorer hostname → (only if the task had no explorer
    # of its own) the org's single moved explorer.
    src_tasks_by_org: Dict[str, List[Dict]] = {}
    if MIGRATE_TASKS:
        for org in sources:
            src_tasks_by_org[org["id"]] = sorted(
                (t for t in fetch_scoped(api, "/org/tasks", org["id"], f"tasks of '{org['name']}'",
                                         params={"search": "type:scan recur:true"})
                 if t.get("recur") and t.get("status") in LIVE_RECURRING_STATUSES),
                key=lambda t: norm(t.get("name")))

    def resolve_task_explorer(org: Dict, t: Dict) -> Tuple[Optional[Dict], Optional[str]]:
        """→ (target explorer or None, note or None). Hosted-zone tasks need no explorer."""
        e = org_override.get(org["id"])
        if e:
            return e, None
        e = src_to_tgt.get(t.get("agent_id") or "")
        if e:
            return e, None
        agent_name = t.get("agent_name") or ""
        if agent_name:
            matches = find_explorers(tgt_explorers, agent_name)
            if len(matches) == 1:
                return matches[0], f"explorer matched by hostname '{matches[0].get('name')}'"
            return None, None
        # No explorer of its own (explorer group, or the old agent record is gone).
        if len(org_matched.get(org["id"], [])) == 1:
            e = org_matched[org["id"]][0]
            return e, f"task had no explorer of its own — used the org's only moved explorer '{e.get('name')}'"
        return None, None

    # Desired site per mapped target explorer: pin when every site it will serve is the
    # same one, otherwise unpin (NIL). Sites come from the orgs whose explorer it was AND
    # from every task that resolves to it (an explorer can serve tasks in several orgs).
    explorer_sites: Dict[str, Set[str]] = {}
    for org in sources:
        org_sites = {site_map[s["id"]] for s in sites_by_org.get(org["id"], []) if site_map.get(s["id"])}
        served_by = [org_override[org["id"]]] if org["id"] in org_override else org_matched.get(org["id"], [])
        for e in served_by:
            explorer_sites.setdefault(e["id"], set()).update(org_sites)
        for t in src_tasks_by_org.get(org["id"], []):
            tgt_site = site_map.get(t.get("site_id") or "")
            e, _ = resolve_task_explorer(org, t)
            if e and tgt_site:
                explorer_sites.setdefault(e["id"], set()).add(tgt_site)
    desired_site: Dict[str, str] = {}
    for eid, sids in explorer_sites.items():
        if len(sids) == 1:
            desired_site[eid] = next(iter(sids))
        elif sids:
            desired_site[eid] = NIL_UUID

    def set_explorer_site(e: Dict, site_id: str, label: str) -> bool:
        action = (f"unpin explorer '{e.get('name')}' (serves several sites)" if site_id == NIL_UUID
                  else f"pin explorer '{e.get('name')}' to '{target_site_names.get(site_id, 'the new site')}'")
        if not APPLY or site_id.startswith("DRY_RUN"):
            print(f"  • {label}: would {action}")
            return True
        resp = api.request("PATCH", f"/org/explorers/{e['id']}", oid=target["id"], json_body={"site_id": site_id})
        if resp.status_code >= 400:
            print(f"  ✗ {label}: could not {action}: {err_text(resp)}")
            return False
        e["site_id"] = site_id
        print(f"  • {label}: {action} — done")
        return True

    if PIN_EXPLORERS and explorer_sites:
        print()
        tgt_by_id = {e["id"]: e for e in tgt_explorers}
        for eid in sorted(explorer_sites, key=lambda i: norm(tgt_by_id[i].get("name"))):
            e = tgt_by_id[eid]
            want = desired_site.get(eid)
            if want is None:
                print(f"  ⚠ explorer '{e.get('name')}' left as-is (its site was not created)")
            elif explorer_site(e) != want:
                set_explorer_site(e, want, e.get("name", ""))
            else:
                print(f"  • explorer '{e.get('name')}' already "
                      f"{'unpinned (serves several sites)' if want == NIL_UUID else 'pinned to its site'}")

    if not MIGRATE_TASKS:
        print("\n  MIGRATE_TASKS = False: stopping after sites/explorers.")
        finish(api, target, report)
        return 0

    # ── 3. Recurring scan tasks ──
    heading("Phase 3: Recurring scan tasks")
    tgt_tasks = [t for t in fetch_scoped(api, "/org/tasks", target["id"], "target tasks",
                                         params={"search": "type:scan recur:true"})
                 if t.get("recur") and t.get("status") in LIVE_RECURRING_STATUSES]
    migrated_src_ids: Set[str] = set()
    for t in tgt_tasks:
        m = MARKER_RE.search(t.get("description") or "")
        if m:
            migrated_src_ids.add(m.group(1).lower())
    existing_task_keys: Set[Tuple[str, str, str]] = {
        (t.get("site_id") or "", norm(t.get("name")), norm(t.get("recur_frequency"))) for t in tgt_tasks
    }
    templates = api.get_optional("/account/tasks/templates", what="scan templates")
    linkable_template_ids = {
        t["id"] for t in templates
        if t.get("organization_id") == target["id"] or t.get("global")
        or (isinstance(t.get("acl"), dict) and target["id"] in t["acl"])
    }
    all_template_ids = {t["id"] for t in templates}

    # A scan task that runs a custom integration must reference one the TARGET org can
    # access (custom integrations are org-scoped) — otherwise the scan API rejects the
    # whole task with "failed to set probes and options". Check up front, in dry run too.
    tgt_custom = api.get_optional("/org/custom-integrations", oid=target["id"], what="target custom integrations")
    tgt_custom_ids = {c["id"] for c in tgt_custom if c.get("id")}
    custom_names: Dict[str, str] = {c["id"]: c.get("name", "") for c in tgt_custom if c.get("id")}
    for org in sources:
        for c in api.get_optional("/org/custom-integrations", oid=org["id"], what=f"custom integrations of '{org['name']}'"):
            if c.get("id"):
                custom_names.setdefault(c["id"], c.get("name", ""))
    blocked_custom: Set[str] = set()

    created_n = skipped_n = exists_n = creds_n = 0
    problems: Dict[str, List[str]] = {}   # full-text reason → affected "org / task" labels
    rows = []
    for org in sources:
        src_tasks = src_tasks_by_org.get(org["id"], [])
        org_rep = next(r for r in report["organisations"] if r["id"] == org["id"])
        for t in src_tasks:
            name = t.get("name") or "Unnamed scan"
            label = f"{org['name']} / {name}"
            freq = t.get("recur_frequency") or ""
            src_status = t.get("status") or ""
            tgt_site_id = site_map.get(t.get("site_id") or "")
            entry: Dict[str, Any] = {"source_task_id": t["id"], "name": name, "frequency": freq,
                                     "source_status": src_status, "notes": []}
            org_rep["tasks"].append(entry)

            def skip(reason: str, who: str = "-") -> None:
                nonlocal skipped_n
                entry["status"] = f"skipped: {reason}"
                rows.append([org["name"], name, freq, who, f"✗ {reason}"])
                problems.setdefault(reason, []).append(label)
                skipped_n += 1

            if not tgt_site_id:
                skip("source site not mapped (see Phase 1)")
                continue
            if t["id"].lower() in migrated_src_ids or (tgt_site_id, norm(name), norm(freq)) in existing_task_keys:
                entry["status"] = "exists"
                rows.append([org["name"], name, freq, "-", "already in target site"])
                exists_n += 1
                continue

            if src_status == "error":
                entry["notes"].append(f"source task was in error state ('{t.get('error') or '?'}') — recreated active")

            # Explorer resolution (shared with the pin planning above).
            hosted_zone_id = t.get("hosted_zone_id") or ""
            if hosted_zone_id == NIL_UUID:
                hosted_zone_id = ""
            src_agent_name = t.get("agent_name") or ""
            explorer, note = (None, None) if hosted_zone_id else resolve_task_explorer(org, t)
            if note:
                entry["notes"].append(note)
            if t.get("explorer_group_id") and t["explorer_group_id"] != NIL_UUID:
                entry["notes"].append("source used an explorer group (org-scoped) — a single explorer is used instead")
            if not explorer and not hosted_zone_id:
                if src_agent_name:
                    skip(f"its explorer '{src_agent_name}' is not in the target org yet — move it and re-run "
                         f"(or set EXPLORER_MAP for this org to use a different explorer)")
                else:
                    skip("no explorer — the task had none and the org has no (or several) moved explorers; set EXPLORER_MAP")
                continue
            if explorer and explorer_status(explorer) != "online":
                skip(f"explorer {explorer_status(explorer)} (API requires online)", explorer.get("name", ""))
                continue
            if explorer and not explorer_usable_for(explorer, tgt_site_id):
                want = desired_site.get(explorer["id"]) if PIN_EXPLORERS else None
                fixed = want is not None and (
                    not APPLY or (set_explorer_site(explorer, want, label) and explorer_usable_for(explorer, tgt_site_id)))
                if not fixed:
                    skip("explorer pinned to another site — set PIN_EXPLORERS = True or fix it in the console",
                         explorer.get("name", ""))
                    continue

            replay, had_creds, redacted = build_replay_params(t)
            if had_creds:
                entry["notes"].append("NEEDS CREDENTIALS: scan credentials (SSH/SNMPv3/WMI/…) must be re-selected in the console")
                creds_n += 1
            if redacted:
                # Every integration probe's secret options are present (as defaults) on every task;
                # only the ones for probes this task actually runs matter.
                enabled = {p.strip() for p in (replay.get("probes") or "").split(",") if p.strip()}
                relevant = [k for k in redacted if k.split("-")[0] in enabled or k.rsplit("-", 1)[0] in enabled]
                if relevant:
                    entry["notes"].append(f"secret options of enabled probes reset to defaults (API redacts them): {', '.join(relevant)}")
            if any(k in (replay.get("targets") or "").lower() for k in INVENTORY_KEYWORDS):
                entry["notes"].append("targets use public:/private: inventory keywords — they expand against the "
                                      "target org's inventory, which is empty until the first scan")

            ci_id = replay.get(CUSTOM_INTEGRATION_ID_KEY, "")
            if ci_id and ci_id != NIL_UUID and ci_id not in tgt_custom_ids:
                ci_name = custom_names.get(ci_id) or short(ci_id) + "…"
                blocked_custom.add(ci_name)
                skip(f"custom integration '{ci_name}' is not shared with the target org — grant access, then re-run",
                     (explorer or {}).get("name", "-"))
                continue

            template_id = t.get("template_id") or ""
            if template_id in (NIL_UUID, "None"):
                template_id = ""
            if template_id and template_id not in linkable_template_ids:
                where = "not shared with the target org" if template_id in all_template_ids else "unknown/removed"
                entry["notes"].append(f"template link dropped ({where}) — task carries its inline parameters")
                template_id = ""

            start_ts = next_start_time(t, now)
            if start_ts <= now:
                # "Run now" tasks start a couple of minutes out so the post-create PATCH
                # (probes/params/paused) always lands before the hub can dispatch the first run.
                start_ts = now + FIRST_RUN_DELAY_SECONDS
            if src_status == "paused" and start_ts < now + 900:
                # Created active then patched to paused: keep the first run safely in the future.
                start_ts = max(start_ts, now) + period_seconds(freq)
                entry["notes"].append(f"first run anchored at {fmt_ts(start_ts)} so the task cannot fire before it is paused")
            opts = scan_options(t, replay, explorer["id"] if explorer else "", hosted_zone_id, start_ts, template_id)

            # The scan endpoint validates probe names strictly; the console does not. "defaults" is
            # a keyword the hub expands at dispatch, and old tasks carry probe names that no longer
            # exist (the scanner ignores those). Create with what the API accepts, then restore.
            src_probe_list = [p.strip() for p in (replay.get("probes") or "").split(",") if p.strip()]
            uses_defaults = any(p.lower() == PROBE_DEFAULTS for p in src_probe_list)
            send_probes = [p for p in src_probe_list if p.lower() != PROBE_DEFAULTS]
            if uses_defaults:
                opts.pop("probes", None)   # omitted → the API stores the same set "defaults" expands to
            elif send_probes:
                opts["probes"] = ",".join(send_probes)
            entry.update({"target_site_id": tgt_site_id, "explorer": (explorer or {}).get("name"),
                          "next_run": fmt_ts(start_ts)})
            who = (explorer or {}).get("name") or f"hosted zone {short(hosted_zone_id)}…"
            flag = " ⚠creds" if had_creds else ""
            paused = " (paused)" if src_status == "paused" else ""

            if not APPLY:
                entry["status"] = "would create"
                rows.append([org["name"], name, freq, who, f"would create{paused} → next {fmt_ts(start_ts)}{flag}"])
                created_n += 1
                continue

            dropped_probes: List[str] = []
            resp = None
            for _attempt in range(MAX_CREATE_ATTEMPTS):
                resp = api.request("PUT", f"/org/sites/{tgt_site_id}/scan", oid=target["id"], json_body=opts)
                if resp.status_code < 400:
                    break
                text = err_text(resp)
                m = UNKNOWN_PROBE_RE.search(text)
                if m and opts.get("probes"):
                    bad = m.group(1)
                    remaining = [p for p in opts["probes"].split(",") if p.strip() and p.strip().lower() != bad.lower()]
                    dropped_probes.append(bad)
                    if not remaining:
                        break
                    opts["probes"] = ",".join(remaining)
                    continue
                if "scan-template" in opts and "template" in text.lower():
                    entry["notes"].append(f"template link rejected by the API ({text}) — created without it")
                    opts.pop("scan-template")
                    continue
                break
            if resp is None or resp.status_code >= 400:
                text = err_text(resp) if resp is not None else "no response"
                if dropped_probes and not opts.get("probes"):
                    text = f"every probe on this task is unknown to the API ({', '.join(dropped_probes)})"
                reason = f"API rejected the task: {text}"
                entry["status"] = f"failed: {text}"
                rows.append([org["name"], name, freq, who, f"✗ {reason}"])
                problems.setdefault(reason, []).append(label)
                skipped_n += 1
                continue
            if dropped_probes:
                entry["notes"].append(f"legacy probe names dropped (no longer exist in runZero; the scanner already "
                                      f"ignored them): {', '.join(dropped_probes)}")
            created = resp.json()
            entry["target_task_id"] = created.get("id")

            patch: Dict[str, Any] = {}
            diff = params_diff(replay, created)
            if uses_defaults:
                # Store the keyword, not the expanded list, so the task tracks future default changes.
                kept = [p for p in src_probe_list if p.lower() not in {d.lower() for d in dropped_probes}]
                diff["probes"] = ",".join(kept)
            if diff:
                patch["params"] = diff
            if src_status == "paused":
                patch["status"] = "paused"
            if patch:
                presp = api.request("PATCH", f"/org/tasks/{created['id']}", oid=target["id"], json_body=patch)
                if presp.status_code >= 400:
                    entry["notes"].append(f"post-create patch failed ({', '.join(patch)}): {err_text(presp)}"
                                          + (" — PAUSE THIS TASK BY HAND" if "status" in patch else ""))
                else:
                    if uses_defaults:
                        entry["notes"].append("probes restored to the 'defaults' keyword to match the source")
                    rest = sorted(k for k in diff if k != "probes" or not uses_defaults)
                    if rest:
                        entry["notes"].append(f"restored params via PATCH: {', '.join(rest)}")
                    if "status" in patch:
                        entry["notes"].append("recreated as PAUSED to match source")
            migrated_src_ids.add(t["id"].lower())
            existing_task_keys.add((tgt_site_id, norm(name), norm(freq)))
            entry["status"] = "created"
            rows.append([org["name"], name, freq, who, f"created{paused} → next {fmt_ts(start_ts)}{flag}"])
            created_n += 1

    table(rows, ["Source org", "Task", "Frequency", "Explorer", "Result"])
    if problems:
        print("\n  Skipped / failed — full reasons:")
        for reason, labels in sorted(problems.items(), key=lambda kv: -len(kv[1])):
            print(f"    {len(labels)} × {reason}")
            for lab in labels[:6]:
                print(f"        - {lab}")
            if len(labels) > 6:
                print(f"        … and {len(labels) - 6} more (see {REPORT_FILE})")
    verb = "created" if APPLY else "would be created"
    print(f"\n  ✓ {created_n} task(s) {verb}, {exists_n} already present, {skipped_n} skipped/failed.")
    if creds_n:
        print(f"  ⚠ {creds_n} task(s) used scan credentials — re-select them in the console "
              f"(Tasks → edit → Credentials). The API never exposes or accepts them.")
    if blocked_custom:
        print(f"  ⚠ Tasks were skipped because these custom integrations are not shared with '{target['name']}': "
              f"{', '.join(sorted(blocked_custom))}.")
        print(f"    Console → Account → Custom Integrations → open each → Organization access → add '{target['name']}', "
              f"then run this script again (only the missing tasks will be created).")

    # ── 4. Integration task checklist ──
    heading("Phase 4: Integration (connector) tasks — manual checklist")
    creds = api.get_optional("/account/credentials", what="credentials")
    cred_by_id = {c["id"]: c for c in creds if c.get("id")}
    rows = []
    cred_rows: Dict[str, List[str]] = {}
    for org in sources:
        conn = [t for t in fetch_scoped(api, "/org/tasks", org["id"], f"integration tasks of '{org['name']}'",
                                        params={"search": "type:connector recur:true"})
                if t.get("recur") and t.get("status") in LIVE_RECURRING_STATUSES]
        for t in sorted(conn, key=lambda x: norm(x.get("name"))):
            params = t.get("params") or {}
            src_name = SOURCE_NAMES.get(t.get("source_id"), f"source {t.get('source_id')}")
            if t.get("custom_integration_id") and t["custom_integration_id"] != NIL_UUID:
                src_name = f"Custom ({short(t['custom_integration_id'])}…)"
            cred_id = to_str(params.get("credential-id"))
            cred = cred_by_id.get(cred_id)
            cred_name = cred["name"] if cred else ("-" if not cred_id else f"unknown ({short(cred_id)}…)")
            access = "-"
            if cred:
                acl = cred.get("acl") if isinstance(cred.get("acl"), dict) else {}
                access = "OK (global)" if cred.get("global") else ("OK" if target["id"] in acl else "GRANT target org access")
                cred_rows[cred["id"]] = [cred["name"], cred.get("type", ""), access]
            sites = [s["name"] for s in sites_by_org.get(org["id"], []) if s["id"] == t.get("site_id")]
            site_name = names.get((org["id"], t.get("site_id") or ""), sites[0] if sites else "-")
            rows.append([org["name"], t.get("name", ""), src_name, cred_name, t.get("recur_frequency", ""), site_name])
            report["integration_tasks"].append({
                "source_org": org["name"], "task": t.get("name"), "integration": src_name,
                "credential": cred_name, "credential_target_access": access,
                "frequency": t.get("recur_frequency"), "status": t.get("status"),
                "target_site": site_name, "source_task_id": t["id"],
            })
    if rows:
        table(rows, ["Source org", "Task", "Integration", "Credential", "Frequency", "Recreate in site"])
        print("\n  These must be recreated in the console (Tasks → Integrations) inside the target org;")
        print("  the public API has no endpoint for creating connector tasks.")

        # In one flat org the same integration + credential only needs ONE task, not one per
        # former org. Group them so the manual work is sized correctly.
        groups: Dict[Tuple[str, str], List[Dict]] = {}
        for it in report["integration_tasks"]:
            groups.setdefault((it["integration"], it["credential"]), []).append(it)
        print(f"\n  Consolidated: {len(groups)} task(s) to recreate (one per integration + credential), "
              f"replacing {len(rows)} per-org copies:")
        consolidated = []
        for (integ, cred), items in sorted(groups.items()):
            freqs = sorted({i["frequency"] or "" for i in items})
            active = sum(1 for i in items if i["status"] == "active")
            runs_on_explorer = integ in EXPLORER_ONLY_INTEGRATIONS
            print(f"    • {integ} with credential '{cred}' — replaces {len(items)} task(s) "
                  f"({active} active, {len(items) - active} paused); schedule(s): {', '.join(freqs)}")
            if runs_on_explorer:
                print(f"        needs an Explorer in the target that can reach the source; check whether the "
                      f"{len(items)} originals used different connection settings (e.g. LDAP URL / base DN) — "
                      f"if so, one task per distinct setting")
            consolidated.append({"integration": integ, "credential": cred, "replaces": len(items),
                                 "active": active, "schedules": freqs, "source_orgs": sorted({i["source_org"] for i in items})})
        report["integration_tasks_consolidated"] = consolidated

        if cred_rows:
            print("\n  Credential access from the target organisation:")
            table(list(cred_rows.values()), ["Credential", "Type", "Target org access"])
            report["credential_access"] = [{"credential": r[0], "type": r[1], "target_access": r[2]} for r in cred_rows.values()]
    else:
        print("  No recurring integration tasks found in the source organisations.")

    finish(api, target, report)
    return 0


def finish(api: Client, target: Dict, report: Dict[str, Any]) -> None:
    heading("Write audit")
    report["writes"] = api.writes
    if not APPLY:
        print(f"  0 write requests sent — dry run. Set DRY_RUN = False and run again to perform the changes.")
    else:
        foreign = [w for w in api.writes if w["oid"] != target["id"]]
        print(f"  {len(api.writes)} write request(s) sent, all to target '{target['name']}' ({target['id']}).")
        if foreign:  # unreachable: the client raises before sending such a request
            print(f"  ✗ {len(foreign)} write(s) were NOT to the target — this should be impossible, please report it.")
        print("  Next: pause the old recurring tasks in the source organisations by hand (never touched by this script).")
    write_report(REPORT_FILE, report)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit("\nInterrupted.")
    except RuntimeError as exc:
        sys.exit(f"✗ {exc}")

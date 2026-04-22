#!/usr/bin/env python3
"""
================================================================================
  runZero Tenant Migration Script
================================================================================

WHAT IT DOES
------------
Migrates a runZero tenant (e.g. US console → EU console) by reading from a
source console and recreating everything possible on a target console.

The SOURCE TENANT IS NEVER MODIFIED. Every API call against the source uses
GET only — the script cannot create, update, or delete anything on the source.

Two operating modes (auto-detected from which tokens you provide):
  • SINGLE-ORG  — provide Organisation API tokens (OT-…) for both sides.
                  Migrates one org from source → target.
  • MULTI-ORG   — provide Account API tokens (CT-…) for both sides.
                  Enumerates ALL organisations on the source, recreates them
                  on the target preserving parent/child hierarchy, then runs
                  every per-org phase for each organisation.

================================================================================
  PHASES
================================================================================
  0  – Multi-Org Hierarchy         (multi-org mode only - recreates orgs hierachy and structure)
  1  – Organisation Settings       (asset expiration policies, etc.)
  2  – Sites                       (names, scope, excludes, subnets + tags)
  3  – Scan Templates              (parameter sets — needs Account tokens)
  3b – Recurring Scan Tasks        (SECOND PASS only — RECURRING_TASKS_ONLY)
  3c – Integration Sync Tasks      (SECOND PASS only — RECURRING_TASKS_ONLY)
  4  – Custom Integrations         (definitions only — name + icon, dedup
                                    by name; Starlark script body must be
                                    pasted manually on the target)
  5  – Asset Ownership Types       (needs Account tokens)
  6  – Scan Data Import            (downloads .gz scan data → imports to sites)
  7  – Asset Tags                  (matched by IP/MAC — merges, not overwrites)
  8  – Asset Comments              (matched by IP/MAC)
  9  – Asset Criticality           (matched by IP/MAC)

================================================================================
  WHAT YOU MUST DO BEFORE RUNNING
================================================================================
  1. Get API tokens from BOTH consoles:
        - Multi-org migration → Account API key (starts CT-…) on each console
        - Single-org migration → Org API key (starts OT-…) on each console
     Console → Account → API → New API key

  2. Set the tokens (either method):
        a) Edit SOURCE_ACCOUNT_TOKEN / TARGET_ACCOUNT_TOKEN below, OR
        b) Export environment variables:
              export RZ_SOURCE_ACCOUNT_TOKEN="CT…"
              export RZ_TARGET_ACCOUNT_TOKEN="CT…"

  3. Confirm the SOURCE_BASE_URL and TARGET_BASE_URL are correct.
        Default: source = console.runzero.com, target = console-eu.runzero.com

  4. Always run with DRY_RUN = True first to preview what will happen.
        Then set DRY_RUN = False to actually perform the migration.

================================================================================
  ALWAYS MIGRATED (no setup needed)
================================================================================
  Phases 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 run automatically. They will work
  without any pre-setup on the target console.
  (Phase 4 — Custom Integrations — creates each integration once on the
  target with its name + icon, but the Starlark SCRIPT BODY is NOT
  migrated.

================================================================================
  OPTIONAL FEATURES — REQUIRE PRE-MIGRATION SETUP ON THE TARGET
================================================================================
  These are DISABLED by default. Only enable them after completing the
  prerequisites listed below, or the tasks will fail to create.

  ┌──────────────────────────────────────────────────────────────────────┐
  │  MIGRATE_RECURRING_TASKS = True                                      │
  │  Enables Phase 3b (recurring scans) AND Phase 3c (integration syncs) │
  └──────────────────────────────────────────────────────────────────────┘

  Before enabling, you MUST do the following on the TARGET console:

    PHASE 3b — Recurring Scan Tasks
    --------------------------------
    1. Install at least one Explorer on the target console
       (Console → Deploy → Explorer → download installer)
    2. Wait for the Explorer to come online (status: connected)
    3. The script will try to map source Explorers to target Explorers by
       NAME. If no name match is found, it falls back to the first online
       Explorer. Review the assignments after migration.

    PHASE 3c — Integration Sync Tasks (Crowdstrike, AWS, Azure, Wiz, etc.)
    -----------------------------------------------------------------------
    1. Recreate every credential in the target console's Credentials
       section (Console → Credentials → New).
    2. Use the EXACT SAME NAMES as on the source. The script matches by
       credential name only — there is no fallback.
    3. Run the script in DRY_RUN mode to see a checklist of which
       credential names are required.
    4. Tasks referencing a credential that doesn't exist on the target
       (by name) are skipped with a clear error message.

================================================================================
  CANNOT BE MIGRATED (no API support exists)
================================================================================
  These must be recreated manually in the target console:
    • Saved queries / views        – no API endpoints
    • Goals                        – no API endpoints
    • Custom dashboards            – no API endpoints
    • Custom Integration scripts   – the integration *definition* (name +
                                     icon) is created automatically by
                                     Phase 4. The Starlark SCRIPT BODY is
                                     NOT exposed in a migration-friendly
                                     way — paste it manually from the source
                                     console (Account → Custom Integrations)
                                     into the matching target integration
                                     before enabling.
    • Credential SECRETS           – API never exposes secrets (you recreate
                                     the credential object with its secret in
                                     the target UI; integration tasks then
                                     reference it by name — see Phase 3c)
    • Users & roles                – create users, then assign roles
    • Explorers / Agents           – reinstall on hosts, point at target URL
    • SSO / SAML configuration     – reconfigure in target console
    • License                      – handled by runZero support

================================================================================
  SAFETY GUARANTEES
================================================================================
  • The source tenant is READ-ONLY throughout. Verified: zero POST/PUT/PATCH/
    DELETE calls against any source URL.
  • Re-running the script is SAFE. Existing items on the target are detected
    by name and skipped (or updated, never duplicated).
  • DRY_RUN = True performs no writes anywhere — it only reads from source
    and prints what would happen.
  • A confirmation prompt is shown before any real writes begin.
  • An ID mapping is saved to migrate_mapping.json after a successful run.

================================================================================
  RECOMMENDED TWO-PASS WORKFLOW
================================================================================
  Recurring tasks (Phases 3b/3c) are intentionally migrated in a SEPARATE
  second pass. Reason: they require Explorers and credentials to exist on
  the target first, and you almost always need a manual gap between
  importing scan data and wiring up live recurring scans.

  ---- FIRST PASS: everything except recurring tasks ----
  1. Set tokens, leave DRY_RUN = True.
     Leave MIGRATE_RECURRING_TASKS = False and RECURRING_TASKS_ONLY = False.
  2. Run script → review the dry-run output.
  3. Set DRY_RUN = False → run again → answer 'y' to confirm.
     → Sites, scan data, asset tags/comments/criticality, etc. are migrated.

  ---- BETWEEN PASSES (manual work on the target console) ----
  4. Install Explorers on the target and wait for them to come online.
  5. Recreate credentials with the SAME NAMES as on the source.
  6. Reinvite users, configure SSO, recreate dashboards/queries/goals.

  ---- SECOND PASS: recurring tasks only ----
  7. Set RECURRING_TASKS_ONLY = True → run the script ONE more time.
     → ONLY Phases 3b/3c run. Phase 6 (scan data import) is SKIPPED so
       you don't get duplicate scan imports. All other phases are skipped.

  NOTE: MIGRATE_RECURRING_TASKS = True is also supported on the first pass
  for advanced users who already have Explorers + credentials on the target
  before they start. The two-pass flow above is the recommended path.
  Do NOT re-run the FULL script after the first pass — Phase 6 is not
  idempotent and will create duplicate scan imports. Use RECURRING_TASKS_ONLY
  for any second run.
================================================================================
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests


# ============================================================
# SOURCE TENANT (US – console.runzero.com)
# ============================================================
SOURCE_BASE_URL      = "https://console.runzero.com/api/v1.0"
SOURCE_ORG_TOKEN     = ""   # OT-…
SOURCE_ACCOUNT_TOKEN = ""   # CT-… (required for multi-org or phases 3-5)
SOURCE_ORG_ID        = ""   # only needed for single-org when using CT token

SOURCE_BASE_URL      = (os.environ.get("RZ_SOURCE_URL")           or SOURCE_BASE_URL).rstrip("/")
SOURCE_ORG_TOKEN     = os.environ.get("RZ_SOURCE_ORG_TOKEN")     or SOURCE_ORG_TOKEN
SOURCE_ACCOUNT_TOKEN = os.environ.get("RZ_SOURCE_ACCOUNT_TOKEN") or SOURCE_ACCOUNT_TOKEN
SOURCE_ORG_ID        = os.environ.get("RZ_SOURCE_ORG_ID")        or SOURCE_ORG_ID

# ============================================================
# TARGET TENANT (EU – console-eu.runzero.com)
# ============================================================
TARGET_BASE_URL      = "https://console-eu.runzero.com/api/v1.0"
TARGET_ORG_TOKEN     = ""   # OT-…
TARGET_ACCOUNT_TOKEN = ""   # CT-… (required for multi-org or phases 3-5)
TARGET_ORG_ID        = ""   # only needed for single-org when using CT token

TARGET_BASE_URL      = (os.environ.get("RZ_TARGET_URL")           or TARGET_BASE_URL).rstrip("/")
TARGET_ORG_TOKEN     = os.environ.get("RZ_TARGET_ORG_TOKEN")     or TARGET_ORG_TOKEN
TARGET_ACCOUNT_TOKEN = os.environ.get("RZ_TARGET_ACCOUNT_TOKEN") or TARGET_ACCOUNT_TOKEN
TARGET_ORG_ID        = os.environ.get("RZ_TARGET_ORG_ID")        or TARGET_ORG_ID

# ============================================================
# MIGRATION OPTIONS
# ============================================================
DRY_RUN             = False   # True = preview only, no writes to target
TASK_LIMIT          = 750     # Max scan tasks to import PER ORGANISATION
                              # (0 = no limit, import every processed scan
                              # task). Tasks are org-scoped, so in multi-org
                              # mode this caps each org separately. Tasks
                              # are sorted by created_at DESC, so the N most
                              # recent are kept.
MIGRATE_TAGS        = True    # Phase 7: migrate asset tags
MIGRATE_COMMENTS    = True    # Phase 8: migrate asset comments
MIGRATE_CRITICALITY = True    # Phase 9: migrate asset criticality
# ---- Recurring tasks (Phases 3b/3c) ----
# RECOMMENDED FLOW: leave BOTH flags below False on the first pass.
# After the first pass, install Explorers and recreate credentials on the
# target, THEN set RECURRING_TASKS_ONLY = True and run the script again.
# Do NOT re-run the full script with MIGRATE_RECURRING_TASKS = True after a
# completed first pass — Phase 6 (scan import) is not idempotent and will
# create duplicate scan imports.
MIGRATE_RECURRING_TASKS = False  # First-pass only: set True ONLY if Explorers
                                 # AND credentials (matching names) already
                                 # exist on the target before you start.
                                 # Otherwise leave False and use the second-
                                 # pass RECURRING_TASKS_ONLY flow below.
RECURRING_TASKS_ONLY = False  # SECOND-PASS flag. When True, runs ONLY
                              # Phases 3b/3c and skips everything else
                              # (no scan re-import, no asset metadata
                              # re-apply). Implies MIGRATE_RECURRING_TASKS.
SAVE_MAPPING        = False    # write migrate_mapping.json on completion

TIMEOUT_SECONDS = 180
MAX_RETRIES     = 6
# When the API returns 429 (rate-limited), the script keeps retrying
# indefinitely — it will NEVER give up on a 429. runZero's client-token
# rate limit can take an hour or more to reset, and bailing out halfway
# through a migration is much worse than waiting it out. Status messages
# are printed every retry so you can see it's still alive.


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def build_session(token: str, label: str = "migrate") -> requests.Session:
    """Create a reusable HTTP session with the API token pre-configured."""
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": f"runzero-tenant-migrate/2.0 ({label})",
    })
    return s


class _OidSession:
    """Thin wrapper that auto-injects ``_oid`` into every request's
    query params so an Account API token behaves like an Org token
    scoped to a specific organisation."""

    def __init__(self, session: requests.Session, oid: str):
        self._s = session
        self._oid = oid
        self.headers = session.headers

    def request(self, method: str, url: str, **kwargs):
        if self._oid:
            params = kwargs.get("params") or {}
            if isinstance(params, dict):
                params.setdefault("_oid", self._oid)
            kwargs["params"] = params
        return self._s.request(method, url, **kwargs)


def api(
    session,
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, str]] = None,
    json_body: Optional[Any] = None,
    data: Optional[bytes] = None,
    files: Optional[Dict] = None,
    headers: Optional[Dict[str, str]] = None,
    stream: bool = False,
    allow_redirects: bool = True,
) -> requests.Response:
    """Send an HTTP request with exponential-backoff retries on 429/5xx.

    429 (rate-limit) responses are retried INDEFINITELY — the function
    will never return a 429 to its caller. runZero's hourly client-token
    bucket can take a long time to reset, and giving up halfway through a
    migration is far worse than waiting it out. Each retry prints a status
    line so the operator can see it's still alive.
    """
    backoff = 1.0
    rate_limit_waited = 0.0
    rate_limit_attempts = 0
    resp = None
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = session.request(
                method,
                url,
                params=params,
                json=json_body,
                data=data,
                files=files,
                headers=headers,
                stream=stream,
                allow_redirects=allow_redirects,
                timeout=TIMEOUT_SECONDS,
            )
        except requests.RequestException:
            if attempt >= MAX_RETRIES:
                raise
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue

        # 429: retry FOREVER, honour Retry-After
        if resp.status_code == 429:
            rate_limit_attempts += 1
            ra = resp.headers.get("Retry-After")
            try:
                wait = float(ra) if ra else max(backoff, 30.0)
            except (ValueError, TypeError):
                wait = max(backoff, 30.0)
            # Cap individual sleep at 5 minutes so we print progress
            wait = min(wait, 300.0)
            print(
                f"  ⏳ Rate-limited (HTTP 429) on attempt {rate_limit_attempts}. "
                f"Sleeping {int(wait)}s (total waited "
                f"{int(rate_limit_waited + wait)}s so far)…",
                file=sys.stderr,
            )
            time.sleep(wait)
            rate_limit_waited += wait
            backoff = min(backoff * 2, 60)
            continue

        # 5xx: bounded retries
        if resp.status_code in (500, 502, 503, 504):
            if attempt >= MAX_RETRIES:
                return resp
            ra = resp.headers.get("Retry-After")
            try:
                time.sleep(float(ra) if ra else backoff)
            except (ValueError, TypeError):
                time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue

        return resp


# ── Validation ────────────────────────────────────────────────────────────────

def validate() -> bool:
    """Check that at least one usable token pair is present."""
    has_org = bool(SOURCE_ORG_TOKEN and TARGET_ORG_TOKEN)
    has_acct = bool(SOURCE_ACCOUNT_TOKEN and TARGET_ACCOUNT_TOKEN)

    if not has_org and not has_acct:
        print(
            "ERROR: Provide either Org tokens (OT-…) for single-org migration "
            "or Account tokens (CT-…) for multi-org migration.",
            file=sys.stderr,
        )
        return False
    return True


def has_account_tokens() -> bool:
    """True when both source and target account tokens are configured."""
    return bool(SOURCE_ACCOUNT_TOKEN and TARGET_ACCOUNT_TOKEN)


def multi_org_mode() -> bool:
    """True when account tokens are set, indicating multi-org migration."""
    return has_account_tokens()


# ── Utility ───────────────────────────────────────────────────────────────────

def _as_list(data) -> list:
    """Normalise an API response to a list."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("data", "results", "items"):
            if key in data and isinstance(data[key], list):
                return data[key]
    return []


def get_org_details(session, base_url: str, label: str = "") -> Dict:
    """Fetch the organisation object, used for ID detection and settings."""
    resp = api(session, "GET", f"{base_url}/org")
    resp.raise_for_status()
    org = resp.json()
    oid = org.get("id", "")
    name = org.get("name", "")
    if label:
        print(f"  {label} org: '{name}' (ID: {oid[:8]}…)")
    return org


def iter_jsonl(resp: requests.Response):
    """Yield parsed objects from a streaming JSONL response."""
    resp.raise_for_status()
    for line in resp.iter_lines(decode_unicode=True):
        line = (line or "").strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            continue


def format_ts(ts) -> str:
    try:
        if isinstance(ts, (int, float)):
            return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime(
                "%Y-%m-%d %H:%M UTC"
            )
        return str(ts)
    except Exception:
        return str(ts)


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 0 – Multi-Org Hierarchy
# ══════════════════════════════════════════════════════════════════════════════

SETTING_KEYS = [
    "expiration_assets_stale",
    "expiration_assets_offline",
    "expiration_integration_attributes",
    "expiration_scans",
    "expiration_vulnerabilities",
    "keep_latest_integration_attributes",
]


def _topo_sort_orgs(orgs: List[Dict]) -> List[Dict]:
    """Sort organisations so parents come before children."""
    by_id = {o["id"]: o for o in orgs}
    visited: set = set()
    result: List[Dict] = []

    def _visit(oid: str):
        if oid in visited:
            return
        org = by_id.get(oid)
        if org is None:
            return
        parent = org.get("parent_id") or ""
        if parent and parent in by_id:
            _visit(parent)
        visited.add(oid)
        result.append(org)

    for o in orgs:
        _visit(o["id"])
    return result


def migrate_org_hierarchy(
    src_session: requests.Session,
    src_url: str,
    tgt_session: requests.Session,
    tgt_url: str,
    dry_run: bool,
) -> Dict[str, str]:
    """Create matching organisations on the target, preserving parent/child
    relationships.  Returns {src_org_id: tgt_org_id}."""
    print("\n╔══════════════════════════════════════╗")
    print("║  Phase 0: Multi-Org Hierarchy        ║")
    print("╚══════════════════════════════════════╝")

    # Source orgs
    resp = api(src_session, "GET", f"{src_url}/account/orgs")
    resp.raise_for_status()
    src_orgs = _as_list(resp.json())
    src_orgs = _topo_sort_orgs(src_orgs)

    # Target orgs
    resp = api(tgt_session, "GET", f"{tgt_url}/account/orgs")
    resp.raise_for_status()
    tgt_orgs = _as_list(resp.json())
    existing_by_name: Dict[str, Dict] = {o["name"]: o for o in tgt_orgs}

    org_map: Dict[str, str] = {}
    created = 0

    print(f"  Source has {len(src_orgs)} organisation(s), "
          f"target has {len(tgt_orgs)} existing.")

    for org in src_orgs:
        name = org["name"]
        src_id = org["id"]

        if name in existing_by_name:
            org_map[src_id] = existing_by_name[name]["id"]
            print(f"  • '{name}' — exists in target, mapped.")
            continue

        if dry_run:
            org_map[src_id] = "DRY_RUN"
            print(f"  • '{name}' — [DRY RUN] would create.")
            continue

        org_def: Dict[str, Any] = {"name": name}
        if org.get("description"):
            org_def["description"] = org["description"]

        # Map parent_id to target
        src_parent = org.get("parent_id") or ""
        if src_parent and src_parent in org_map:
            org_def["parent_id"] = org_map[src_parent]

        # Copy expiration settings into the new org
        for key in SETTING_KEYS:
            val = org.get(key)
            if val is not None:
                org_def[key] = str(val).lower() if isinstance(val, bool) else str(val)

        resp = api(tgt_session, "PUT", f"{tgt_url}/account/orgs", json_body=org_def)
        if resp.status_code >= 400:
            print(f"  ✗ '{name}' — HTTP {resp.status_code}: {resp.text[:200]}")
            continue

        new_org = resp.json()
        org_map[src_id] = new_org["id"]
        created += 1
        parent_info = (
            f" (parent: {org_map.get(src_parent, 'root')[:8]}…)"
            if src_parent else ""
        )
        print(f"  • '{name}' — created → {new_org['id'][:8]}…{parent_info}")

        existing_by_name[name] = new_org

    print(f"  ✓ {len(org_map)} org(s) mapped ({created} newly created).")
    return org_map


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 1 – Organisation Settings
# ══════════════════════════════════════════════════════════════════════════════

def migrate_org_settings(
    src_session,
    src_url: str,
    tgt_session,
    tgt_url: str,
    src_org: Dict,
    dry_run: bool,
) -> bool:
    print("\n╔══════════════════════════════════════╗")
    print("║  Phase 1: Organisation Settings      ║")
    print("╚══════════════════════════════════════╝")

    update: Dict[str, str] = {}
    for key in SETTING_KEYS:
        val = src_org.get(key)
        if val is not None:
            update[key] = str(val).lower() if isinstance(val, bool) else str(val)

    if not update:
        print("  No settings to copy.")
        return True

    print("  Settings to copy:")
    for k, v in update.items():
        print(f"    {k} = {v}")

    if dry_run:
        print("  [DRY RUN] Skipping update.")
        return True

    resp = api(tgt_session, "PATCH", f"{tgt_url}/org", json_body=update)
    if resp.status_code >= 400:
        print(f"  ERROR: HTTP {resp.status_code} — {resp.text[:300]}", file=sys.stderr)
        return False

    print("  ✓ Organisation settings updated.")
    return True


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 2 – Sites (with subnets)
# ══════════════════════════════════════════════════════════════════════════════

def migrate_sites(
    src_session,
    src_url: str,
    tgt_session,
    tgt_url: str,
    dry_run: bool,
    map_only: bool = False,
) -> Dict[str, str]:
    """Create matching sites on the target, including subnets.
    Returns {src_site_id: tgt_site_id}.

    When map_only=True, no creates or updates are performed — only the
    site name → ID mapping is built (used by RECURRING_TASKS_ONLY mode).
    """
    print("\n╔══════════════════════════════════════╗")
    if map_only:
        print("║  Phase 2: Sites (MAP ONLY)           ║")
    else:
        print("║  Phase 2: Sites                      ║")
    print("╚══════════════════════════════════════╝")

    # Source sites
    resp = api(src_session, "GET", f"{src_url}/org/sites")
    resp.raise_for_status()
    src_sites = _as_list(resp.json())

    # Existing target sites
    resp = api(tgt_session, "GET", f"{tgt_url}/org/sites")
    resp.raise_for_status()
    tgt_sites = _as_list(resp.json())
    existing_by_name: Dict[str, Dict] = {s["name"]: s for s in tgt_sites}

    site_map: Dict[str, str] = {}
    created = 0
    updated = 0

    print(f"  Source has {len(src_sites)} site(s), "
          f"target has {len(tgt_sites)} existing site(s).")

    for site in src_sites:
        name = site["name"]
        src_id = site["id"]

        if name in existing_by_name:
            tgt_site = existing_by_name[name]
            site_map[src_id] = tgt_site["id"]

            if map_only:
                continue

            # Build patch body for any source fields that exist
            patch_body: Dict[str, Any] = {}
            for field in ("description", "scope", "excludes"):
                val = site.get(field)
                if val:
                    patch_body[field] = val
            if site.get("subnets") and isinstance(site["subnets"], dict):
                patch_body["subnets"] = site["subnets"]

            if patch_body and not dry_run:
                patch_resp = api(
                    tgt_session, "PATCH",
                    f"{tgt_url}/org/sites/{tgt_site['id']}",
                    json_body=patch_body,
                )
                if patch_resp.status_code < 400:
                    updated += 1
                    print(f"  • '{name}' — exists, mapped, updated "
                          f"({', '.join(patch_body.keys())}).")
                else:
                    print(f"  • '{name}' — exists, mapped (update failed: "
                          f"HTTP {patch_resp.status_code}).")
            else:
                print(f"  • '{name}' — exists in target, mapped.")
            continue

        if map_only:
            # Don't create new sites in map-only mode
            print(f"  • '{name}' — ⚠ not on target (skipped, map-only mode).")
            continue

        if dry_run:
            site_map[src_id] = "DRY_RUN"
            print(f"  • '{name}' — [DRY RUN] would create.")
            continue

        site_def: Dict[str, Any] = {"name": name}
        for field in ("description", "scope", "excludes"):
            if site.get(field):
                site_def[field] = site[field]

        # Include registered subnets (with tags, etc.)
        if site.get("subnets") and isinstance(site["subnets"], dict):
            site_def["subnets"] = site["subnets"]

        resp = api(tgt_session, "PUT", f"{tgt_url}/org/sites", json_body=site_def)
        if resp.status_code >= 400:
            print(f"  ✗ '{name}' — HTTP {resp.status_code}: {resp.text[:200]}")
            continue

        new_site = resp.json()
        site_map[src_id] = new_site["id"]
        created += 1
        subnet_count = len(site.get("subnets", {}) or {})
        subnet_info = f" ({subnet_count} subnet(s))" if subnet_count else ""
        print(f"  • '{name}' — created → {new_site['id'][:8]}…{subnet_info}")

    if map_only:
        print(f"  ✓ {len(site_map)} site(s) mapped (no changes made).")
    else:
        print(f"  ✓ {len(site_map)} site(s) mapped ({created} created, "
              f"{updated} existing updated).")
    return site_map


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 3 – Scan Templates (including recurring/scheduled task configs)
# ══════════════════════════════════════════════════════════════════════════════

def migrate_scan_templates(
    src_session,
    src_url: str,
    src_oid: str,
    tgt_session,
    tgt_url: str,
    tgt_oid: str,
    site_map: Dict[str, str],
    dry_run: bool,
) -> int:
    print("\n╔══════════════════════════════════════╗")
    print("║  Phase 3: Scan Templates             ║")
    print("║  (includes recurring task configs)    ║")
    print("╚══════════════════════════════════════╝")

    if not has_account_tokens():
        print("  ⚠ Skipped — Account API tokens not provided.")
        print("    Set RZ_SOURCE_ACCOUNT_TOKEN / RZ_TARGET_ACCOUNT_TOKEN to enable.")
        return 0

    if not tgt_oid:
        print("  ⚠ Skipped — TARGET_ORG_ID is required for scan template creation.")
        return 0

    # ── Fetch source templates ──
    params: Dict[str, str] = {}
    if src_oid:
        params["_oid"] = src_oid
    resp = api(src_session, "GET", f"{src_url}/account/tasks/templates", params=params)
    resp.raise_for_status()
    templates = _as_list(resp.json())

    # Keep only templates belonging to the source org (or globals)
    if src_oid:
        templates = [
            t for t in templates
            if t.get("organization_id") == src_oid or t.get("global")
        ]

    if not templates:
        print("  No scan templates found.")
        return 0

    recurring = sum(1 for t in templates if t.get("recur"))
    print(f"  Found {len(templates)} template(s) in source "
          f"({recurring} with recurring schedule).")

    # ── Fetch existing target templates ──
    params = {}
    if tgt_oid:
        params["_oid"] = tgt_oid
    resp = api(tgt_session, "GET", f"{tgt_url}/account/tasks/templates", params=params)
    resp.raise_for_status()
    existing_names = {t.get("name") for t in _as_list(resp.json())}

    created = 0
    for tmpl in templates:
        name = tmpl.get("name", "(unnamed)")

        if name in existing_names:
            print(f"  • '{name}' — already exists, skipping.")
            continue

        is_recurring = tmpl.get("recur", False)
        freq = tmpl.get("recur_frequency", "")
        recur_info = f" [recurring: {freq}]" if is_recurring else ""

        if dry_run:
            print(f"  • '{name}'{recur_info} — [DRY RUN] would create.")
            continue

        # Map site_id to target site
        src_site_id = tmpl.get("site_id", "")
        tgt_site_id = site_map.get(src_site_id, "")

        # The runZero API rejects templates with global=false AND an empty
        # ACL ("global or ACL permissions field required"). The source ACL
        # contains user/org IDs that don't exist on the target, so the
        # safest portable choice is to default to global=true unless the
        # source was explicitly non-global with a usable ACL.
        src_global = bool(tmpl.get("global"))
        src_acl = tmpl.get("acl") if isinstance(tmpl.get("acl"), dict) else {}
        if src_global:
            template_global = True
            template_acl: Dict[str, Any] = {}
        elif src_acl:
            # ACL entries reference IDs that won't exist on the target.
            # Fall back to global=true so the template is at least usable.
            template_global = True
            template_acl = {}
        else:
            template_global = True
            template_acl = {}

        new_tmpl: Dict[str, Any] = {
            "name": name,
            "description": tmpl.get("description", ""),
            "organization_id": tgt_oid,
            "params": tmpl.get("params") or {},
            "global": template_global,
            "acl": template_acl,
        }

        # Create the template
        resp = api(
            tgt_session, "POST",
            f"{tgt_url}/account/tasks/templates",
            json_body=new_tmpl,
        )
        if resp.status_code >= 400:
            print(f"  ✗ '{name}' — HTTP {resp.status_code}: {resp.text[:200]}")
            continue

        created_tmpl = resp.json()
        new_id = created_tmpl.get("id", "")

        # If it has recurrence or a site assignment, update via PUT
        if is_recurring or tgt_site_id:
            update_body: Dict[str, Any] = {
                "id": new_id,
                "name": name,
                "description": tmpl.get("description", ""),
                "organization_id": tgt_oid,
                "params": tmpl.get("params") or {},
                "global": template_global,
                "acl": template_acl,
            }

            if tgt_site_id and tgt_site_id != "DRY_RUN":
                update_body["site_id"] = tgt_site_id

            if is_recurring:
                update_body["recur"] = True
                update_body["recur_frequency"] = freq
                if tmpl.get("start_time"):
                    update_body["start_time"] = tmpl["start_time"]
                if tmpl.get("grace_period"):
                    update_body["grace_period"] = tmpl["grace_period"]

            # Note: agent_id / explorer is NOT mapped because explorers
            # must be reinstalled on the target console.

            put_resp = api(
                tgt_session, "PUT",
                f"{tgt_url}/account/tasks/templates",
                json_body=update_body,
            )
            if put_resp.status_code >= 400:
                print(f"  • '{name}' — created (recurrence update failed: "
                      f"HTTP {put_resp.status_code})")
            else:
                print(f"  • '{name}'{recur_info} — created with schedule.")
                created += 1
                continue

        created += 1
        print(f"  • '{name}' — created.")

    print(f"  ✓ {created} template(s) created.")
    if recurring:
        print("  ⚠ Recurring templates need Explorers reassigned on the target console.")
    return created


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 3b – Recurring Scan Tasks
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_explorers(session, base_url: str) -> List[Dict]:
    """Return all online Explorers/agents available to this org."""
    resp = api(session, "GET", f"{base_url}/org/agents")
    if resp.status_code >= 400:
        return []
    agents = _as_list(resp.json())
    # Prefer connected/online agents
    online = [a for a in agents if a.get("connected") or a.get("status") == "online"]
    return online or agents


def _fetch_credentials(session, base_url: str) -> List[Dict]:
    """Return all credentials available to this org (no secrets)."""
    resp = api(session, "GET", f"{base_url}/org/credentials")
    if resp.status_code >= 400:
        return []
    return _as_list(resp.json())


def migrate_recurring_tasks(
    src_session,
    src_url: str,
    tgt_session,
    tgt_url: str,
    site_map: Dict[str, str],
    dry_run: bool,
) -> int:
    """Find recurring scan tasks on the source and recreate them on the
    target by POSTing to /org/sites/{site_id}/scan with recur params.
    Requires at least one Explorer to be online on the target."""
    print("\n╔══════════════════════════════════════╗")
    print("║  Phase 3b: Recurring Scan Tasks      ║")
    print("╚══════════════════════════════════════╝")

    # Fetch tasks with a recurring flag
    resp = api(
        src_session, "GET", f"{src_url}/org/tasks",
        params={"search": "type:scan recur:true"},
    )
    if resp.status_code >= 400:
        print(f"  ⚠ Could not list source tasks: HTTP {resp.status_code}")
        return 0

    tasks = _as_list(resp.json())
    tasks = [
        t for t in tasks
        if t.get("recur") and t.get("status") not in ("removed", "failed")
    ]

    if not tasks:
        print("  No recurring scan tasks found on source.")
        return 0

    # Deduplicate — the same recurring task may show up via multiple runs
    by_template: Dict[str, Dict] = {}
    for t in tasks:
        key = (
            f"{t.get('template_id', '')}|{t.get('site_id', '')}|"
            f"{t.get('name', '')}"
        )
        existing = by_template.get(key)
        if not existing or (
            (t.get("created_at") or 0) > (existing.get("created_at") or 0)
        ):
            by_template[key] = t

    unique_tasks = list(by_template.values())
    print(f"  Found {len(unique_tasks)} unique recurring scan task(s).")

    # Fetch existing target tasks to avoid duplicates
    resp = api(
        tgt_session, "GET", f"{tgt_url}/org/tasks",
        params={"search": "type:scan recur:true"},
    )
    existing_tgt_names: set = set()
    if resp.status_code < 400:
        existing_tgt_names = {
            t.get("name", "") for t in _as_list(resp.json()) if t.get("recur")
        }

    # ── Map Explorers from source → target ──
    src_agents_by_id: Dict[str, str] = {}
    if not dry_run:
        resp = api(src_session, "GET", f"{src_url}/org/agents")
        if resp.status_code < 400:
            src_agents_by_id = {
                a.get("id", ""): a.get("name", "") for a in _as_list(resp.json())
            }

    tgt_agents = _fetch_explorers(tgt_session, tgt_url) if not dry_run else []
    tgt_agents_by_name: Dict[str, str] = {
        a.get("name", ""): a.get("id", "") for a in tgt_agents if a.get("id")
    }
    fallback_agent_id = tgt_agents[0]["id"] if tgt_agents else ""

    if dry_run:
        print(f"  [DRY RUN] {len(unique_tasks)} recurring scan task(s) would"
              f" be created on the target — but ONLY on the SECOND pass")
        print(f"            (run with RECURRING_TASKS_ONLY = True after")
        print(f"            Explorers + credentials are in place).")
        print(f"            Nothing is created on the first pass.")
        for t in unique_tasks:
            name = t.get("name", "(unnamed)")
            freq = t.get("recur_frequency", "")
            print(f"    • '{name}' [{freq}]")
        return 0

    if not tgt_agents:
        print("  ✗ No Explorers found on the target — recurring tasks cannot be")
        print("    created (the API rejects tasks without an agent_id).")
        print("  → Saving recurring task config to 'pending_recurring_tasks.json'")
        print("    Install an Explorer on the target, then re-run this script.")
        try:
            existing_pending: List[Dict] = []
            if os.path.exists("pending_recurring_tasks.json"):
                with open("pending_recurring_tasks.json") as f:
                    existing_pending = json.load(f)
            for t in unique_tasks:
                src_site_id = t.get("site_id", "")
                tgt_site_id = site_map.get(src_site_id, "")
                if not tgt_site_id or tgt_site_id == "DRY_RUN":
                    continue
                if t.get("name") in existing_tgt_names:
                    continue
                existing_pending.append({
                    "name": t.get("name", ""),
                    "description": t.get("description", ""),
                    "target_site_id": tgt_site_id,
                    "params": t.get("params") or {},
                    "recur": True,
                    "recur_frequency": t.get("recur_frequency", ""),
                    "start_time": t.get("start_time"),
                    "grace_period": t.get("grace_period"),
                    "template_id": t.get("template_id"),
                    "source_agent_name": src_agents_by_id.get(t.get("agent_id", ""), ""),
                })
            with open("pending_recurring_tasks.json", "w") as f:
                json.dump(existing_pending, f, indent=2)
            print(f"  ✓ Saved {len(existing_pending)} pending task(s).")
        except Exception as e:
            print(f"  ⚠ Could not save pending tasks: {e}")
        return 0

    print(f"  Target Explorers available: {len(tgt_agents)} "
          f"(fallback: '{tgt_agents[0].get('name', '')[:40]}')")

    created = 0
    for t in unique_tasks:
        name = t.get("name", "(unnamed)")
        src_site_id = t.get("site_id", "")
        tgt_site_id = site_map.get(src_site_id, "")
        freq = t.get("recur_frequency", "")
        recur_info = f" [{freq}]" if freq else ""

        if name in existing_tgt_names:
            print(f"  • '{name}'{recur_info} — already exists on target, skipping.")
            continue

        if not tgt_site_id or tgt_site_id == "DRY_RUN":
            print(f"  • '{name}'{recur_info} — ✗ no matching target site.")
            continue

        # Try to map Explorer by name (source agent name → target agent)
        src_agent_name = src_agents_by_id.get(t.get("agent_id", ""), "")
        agent_id = tgt_agents_by_name.get(src_agent_name, "") or fallback_agent_id
        agent_label = src_agent_name if src_agent_name in tgt_agents_by_name else "fallback"

        scan_body: Dict[str, Any] = {
            "name": name,
            "description": t.get("description", ""),
            "params": t.get("params") or {},
            "agent_id": agent_id,
            "recur": True,
            "recur_frequency": freq,
        }
        if t.get("start_time"):
            scan_body["start_time"] = t["start_time"]
        if t.get("grace_period"):
            scan_body["grace_period"] = t["grace_period"]
        if t.get("template_id"):
            scan_body["template_id"] = t["template_id"]

        resp = api(
            tgt_session, "POST",
            f"{tgt_url}/org/sites/{tgt_site_id}/scan",
            json_body=scan_body,
        )
        if resp.status_code >= 400:
            print(f"  ✗ '{name}' — HTTP {resp.status_code}: {resp.text[:200]}")
            continue

        created += 1
        print(f"  • '{name}'{recur_info} — created (Explorer: {agent_label}).")

    print(f"  ✓ {created} recurring task(s) created.")
    if created:
        print("  ⚠ Review Explorer assignments on the target — fallback Explorer")
        print("    was used where the source Explorer name didn't match.")
    return created


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 3c – Recurring Integration Sync Tasks
# ══════════════════════════════════════════════════════════════════════════════

def migrate_integration_tasks(
    src_session,
    src_url: str,
    tgt_session,
    tgt_url: str,
    dry_run: bool,
) -> int:
    """Recreate recurring integration sync tasks (Crowdstrike, AWS, Azure,
    Wiz, etc.) on the target by mapping credentials by name.  Requires
    matching credentials to be pre-created on the target."""
    print("\n╔══════════════════════════════════════╗")
    print("║  Phase 3c: Integration Sync Tasks    ║")
    print("╚══════════════════════════════════════╝")

    # Source integration tasks (connector type, recurring)
    resp = api(
        src_session, "GET", f"{src_url}/org/tasks",
        params={"search": "type:connector recur:true"},
    )
    if resp.status_code >= 400:
        print(f"  ⚠ Could not list source tasks: HTTP {resp.status_code}")
        return 0

    tasks = _as_list(resp.json())
    tasks = [
        t for t in tasks
        if t.get("recur") and t.get("status") not in ("removed", "failed")
    ]

    if not tasks:
        print("  No recurring integration sync tasks found on source.")
        return 0

    # Deduplicate
    by_key: Dict[str, Dict] = {}
    for t in tasks:
        key = f"{t.get('source_id', '')}|{t.get('credential_id', '')}|{t.get('name', '')}"
        existing = by_key.get(key)
        if not existing or (
            (t.get("created_at") or 0) > (existing.get("created_at") or 0)
        ):
            by_key[key] = t

    unique_tasks = list(by_key.values())
    print(f"  Found {len(unique_tasks)} unique integration sync task(s).")

    # Build source credential id → name map
    src_creds = _fetch_credentials(src_session, src_url)
    src_creds_by_id: Dict[str, str] = {
        c.get("id", ""): c.get("name", "") for c in src_creds
    }

    if dry_run:
        print(f"  [DRY RUN] {len(unique_tasks)} integration sync task(s) would"
              f" be created on the target — but ONLY on the SECOND pass")
        print(f"            (run with RECURRING_TASKS_ONLY = True after")
        print(f"            credentials are recreated with matching names).")
        print(f"            Nothing is created on the first pass.")
        for t in unique_tasks:
            cred_name = src_creds_by_id.get(t.get("credential_id", ""), "?")
            print(f"    • '{t.get('name', '(unnamed)')}' "
                  f"[credential: {cred_name}]")
        print("  Note: Requires matching credentials by name on target.")
        return 0

    # Build target credential name → id map
    tgt_creds = _fetch_credentials(tgt_session, tgt_url)
    tgt_creds_by_name: Dict[str, str] = {
        c.get("name", ""): c.get("id", "") for c in tgt_creds if c.get("id")
    }

    if not tgt_creds_by_name:
        print("  ✗ No credentials found on the target.")
        print("    Recreate credentials in the target UI (same names as source),")
        print("    then re-run this script.")
        return 0

    # Existing integration tasks on target (avoid duplicates)
    resp = api(
        tgt_session, "GET", f"{tgt_url}/org/tasks",
        params={"search": "type:connector recur:true"},
    )
    existing_tgt_names: set = set()
    if resp.status_code < 400:
        existing_tgt_names = {
            t.get("name", "") for t in _as_list(resp.json()) if t.get("recur")
        }

    print(f"  Target credentials available: {len(tgt_creds_by_name)}")

    created = 0
    skipped_no_cred = 0
    for t in unique_tasks:
        name = t.get("name", "(unnamed)")
        freq = t.get("recur_frequency", "")
        recur_info = f" [{freq}]" if freq else ""

        if name in existing_tgt_names:
            print(f"  • '{name}'{recur_info} — already exists on target, skipping.")
            continue

        src_cred_id = t.get("credential_id", "")
        src_cred_name = src_creds_by_id.get(src_cred_id, "")
        tgt_cred_id = tgt_creds_by_name.get(src_cred_name, "")

        if not tgt_cred_id:
            print(f"  • '{name}'{recur_info} — ✗ no matching credential "
                  f"'{src_cred_name}' on target.")
            skipped_no_cred += 1
            continue

        source_id = t.get("source_id", "")
        if not source_id:
            print(f"  • '{name}'{recur_info} — ✗ missing source_id (integration type).")
            continue

        task_body: Dict[str, Any] = {
            "name": name,
            "description": t.get("description", ""),
            "source_id": source_id,
            "credential_id": tgt_cred_id,
            "params": t.get("params") or {},
            "recur": True,
            "recur_frequency": freq,
        }
        if t.get("start_time"):
            task_body["start_time"] = t["start_time"]
        if t.get("grace_period"):
            task_body["grace_period"] = t["grace_period"]

        # Integration tasks are created via the connector endpoint
        resp = api(
            tgt_session, "POST",
            f"{tgt_url}/org/tasks",
            json_body=task_body,
        )
        if resp.status_code >= 400:
            print(f"  ✗ '{name}' — HTTP {resp.status_code}: {resp.text[:200]}")
            continue

        created += 1
        print(f"  • '{name}'{recur_info} — created (credential: '{src_cred_name}').")

    print(f"  ✓ {created} integration task(s) created"
          f"{f', {skipped_no_cred} skipped (missing credential)' if skipped_no_cred else ''}.")
    return created


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 4 – Custom Integrations
# ══════════════════════════════════════════════════════════════════════════════

def migrate_custom_integrations(
    src_session,
    src_url: str,
    src_oid: str,
    tgt_session,
    tgt_url: str,
    tgt_oid: str,
    dry_run: bool,
) -> Dict[str, str]:
    """Recreate the custom integration *definitions* (name + icon) on the
    target. The Starlark script body is NOT migrated — it must be pasted
    in manually on the target console afterwards. Dedup is by name, so
    integrations that already exist on the target (or were created by a
    previous org in this same multi-org run) are mapped, never duplicated.
    Returns {src_integration_id: tgt_integration_id}."""
    print("\n╔══════════════════════════════════════╗")
    print("║  Phase 4: Custom Integrations        ║")
    print("╚══════════════════════════════════════╝")
    print("  NOTE: Custom integrations are TENANT-WIDE. The script creates")
    print("  each one ONCE on the target (dedup by name). The Starlark")
    print("  script body is NOT migrated — add it manually on the target.")

    if not has_account_tokens():
        print("  ⚠ Skipped — Account API tokens not provided.")
        return {}

    # ── Fetch from source ──
    params: Dict[str, str] = {}
    if src_oid:
        params["_oid"] = src_oid
    resp = api(src_session, "GET", f"{src_url}/account/custom-integrations", params=params)
    resp.raise_for_status()
    raw = resp.json()
    integrations = _as_list(raw) if isinstance(raw, list) else (
        [raw] if isinstance(raw, dict) and raw.get("id") else []
    )

    if not integrations:
        print("  No custom integrations found.")
        return {}

    print(f"  Found {len(integrations)} integration(s) in source.")

    # ── Fetch existing in target ──
    params = {}
    if tgt_oid:
        params["_oid"] = tgt_oid
    resp = api(tgt_session, "GET", f"{tgt_url}/account/custom-integrations", params=params)
    resp.raise_for_status()
    raw_tgt = resp.json()
    existing = _as_list(raw_tgt) if isinstance(raw_tgt, list) else (
        [raw_tgt] if isinstance(raw_tgt, dict) and raw_tgt.get("id") else []
    )
    existing_names: Dict[str, str] = {
        i.get("name", ""): i.get("id", "") for i in existing
    }
    existing_ids = {i.get("id", "") for i in existing}

    integration_map: Dict[str, str] = {}
    created = 0

    for integ in integrations:
        name = integ.get("name", "")
        src_id = integ.get("id", "")

        if name in existing_names:
            integration_map[src_id] = existing_names[name]
            print(f"  • '{name}' — already exists, mapped.")
            continue

        if dry_run:
            print(f"  • '{name}' — [DRY RUN] would create (Starlark code")
            print(f"    must be pasted in manually on the target).")
            continue

        # Prepend a disclaimer to the description so the operator sees it
        # in the target UI — the Starlark body is intentionally not migrated.
        DISCLAIMER = (
            "[MIGRATED — Starlark script body NOT copied. Paste the script "
            "from the source console manually before enabling.] "
        )
        src_desc = (integ.get("description") or "").strip()
        new_def: Dict[str, Any] = {
            "name": name,
            "description": (DISCLAIMER + src_desc).strip(),
        }
        if integ.get("icon"):
            new_def["icon"] = integ["icon"]

        # Try to preserve the original integration ID via PUT
        if src_id and src_id not in existing_ids:
            resp = api(
                tgt_session, "PUT",
                f"{tgt_url}/account/custom-integrations/{src_id}",
                json_body=new_def,
            )
            if resp.status_code < 400:
                new_integ = resp.json()
                integration_map[src_id] = new_integ.get("id", src_id)
                created += 1
                print(f"  • '{name}' — created (ID preserved: {src_id[:8]}…)")
                continue

        # Fall back to POST (auto-generated ID)
        resp = api(
            tgt_session, "POST",
            f"{tgt_url}/account/custom-integrations",
            json_body=new_def,
        )
        if resp.status_code >= 400:
            print(f"  ✗ '{name}' — HTTP {resp.status_code}: {resp.text[:200]}")
            continue

        new_integ = resp.json()
        integration_map[src_id] = new_integ.get("id", "")
        created += 1
        print(f"  • '{name}' — created (new ID: {integration_map[src_id][:8]}…)")

    print(f"  ✓ {created} integration(s) created.")
    return integration_map


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 5 – Asset Ownership Types
# ══════════════════════════════════════════════════════════════════════════════

def migrate_ownership_types(
    src_session,
    src_url: str,
    tgt_session,
    tgt_url: str,
    dry_run: bool,
) -> int:
    print("\n╔══════════════════════════════════════╗")
    print("║  Phase 5: Asset Ownership Types      ║")
    print("╚══════════════════════════════════════╝")

    if not has_account_tokens():
        print("  ⚠ Skipped — Account API tokens not provided.")
        return 0

    resp = api(src_session, "GET", f"{src_url}/account/assets/ownership-types")
    if resp.status_code == 403:
        print("  ⚠ Skipped — license does not support ownership types.")
        return 0
    resp.raise_for_status()
    types = _as_list(resp.json())

    if not types:
        print("  No ownership types found.")
        return 0

    print(f"  Found {len(types)} ownership type(s) in source.")

    # Fetch existing
    resp = api(tgt_session, "GET", f"{tgt_url}/account/assets/ownership-types")
    if resp.status_code == 403:
        print("  ⚠ Skipped — target license does not support ownership types.")
        return 0
    resp.raise_for_status()
    existing_names = {t.get("name") for t in _as_list(resp.json())}

    new_types = []
    for t in types:
        name = t.get("name", "")
        if name in existing_names:
            print(f"  • '{name}' — already exists, skipping.")
            continue
        entry: Dict[str, Any] = {"name": name}
        if t.get("order") is not None:
            entry["order"] = t["order"]
        if t.get("hidden") is not None:
            entry["hidden"] = t["hidden"]
        new_types.append(entry)

    if not new_types:
        print("  All ownership types already exist in target.")
        return 0

    if dry_run:
        print(f"  [DRY RUN] Would create {len(new_types)} type(s).")
        return 0

    resp = api(
        tgt_session, "POST",
        f"{tgt_url}/account/assets/ownership-types",
        json_body=new_types,
    )
    if resp.status_code >= 400:
        print(f"  ✗ Failed: HTTP {resp.status_code}: {resp.text[:300]}")
        return 0

    print(f"  ✓ {len(new_types)} ownership type(s) created.")
    return len(new_types)


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 6 – Scan Data Import
# ══════════════════════════════════════════════════════════════════════════════

def fetch_scan_tasks(session, base_url: str, limit: int = 0) -> List[Dict]:
    """Return processed scan tasks, most recent first."""
    resp = api(
        session, "GET", f"{base_url}/org/tasks",
        params={"search": "type:scan", "status": "processed"},
    )
    resp.raise_for_status()
    tasks = _as_list(resp.json())

    def _sort_key(t: Dict):
        v = t.get("created_at") or t.get("updated_at") or 0
        return float(v) if isinstance(v, (int, float)) else 0.0

    tasks.sort(key=_sort_key, reverse=True)
    return tasks[:limit] if limit > 0 else tasks


def download_task_data(session, base_url: str, task_id: str) -> Tuple[bytes, str]:
    """Download the .gz scan data for a task.  Returns (bytes, filename)."""
    url = f"{base_url}/org/tasks/{task_id}/data"
    resp = api(session, "GET", url, allow_redirects=False)

    download_url = ""
    if resp.status_code in (301, 302, 303, 307, 308):
        download_url = resp.headers.get("Location", "")
    elif resp.status_code == 200:
        try:
            body = resp.json()
            download_url = body.get("url", "")
        except Exception:
            pass

    if not download_url:
        raise ValueError(
            f"Could not resolve download URL for task {task_id} "
            f"(HTTP {resp.status_code})"
        )

    path = urlparse(download_url).path
    filename = path.split("/")[-1] if path and "/" in path else ""
    if not filename or not filename.endswith(".gz"):
        filename = f"task_{task_id}.gz"

    dl_resp = requests.get(download_url, stream=True, timeout=TIMEOUT_SECONDS)
    dl_resp.raise_for_status()
    return dl_resp.content, filename


def import_task_data(
    session, base_url: str, site_id: str, data: bytes, filename: str,
    task_name: str = "", task_description: str = "",
) -> requests.Response:
    """Upload scan data into a target site. The optional task_name and
    task_description are passed as query parameters so the resulting
    import task in the target shows the original scan's name rather than
    the generic 'Scan Import'."""
    url = f"{base_url}/org/sites/{site_id}/import"
    params: Dict[str, str] = {}
    if task_name:
        params["name"] = task_name
    if task_description:
        params["description"] = task_description
    return api(
        session, "PUT", url,
        params=params or None,
        data=data,
        headers={"Content-Type": "application/octet-stream"},
    )


def migrate_scan_data(
    src_session, src_url: str,
    tgt_session, tgt_url: str,
    site_map: Dict[str, str],
    dry_run: bool, limit: int,
) -> Tuple[int, int, int]:
    """Returns (succeeded, skipped, failed)."""
    print("\n╔══════════════════════════════════════╗")
    print("║  Phase 6: Scan Data Import           ║")
    print("╚══════════════════════════════════════╝")

    if not site_map:
        print("  ⚠ No site mapping available — cannot import scan data.")
        return (0, 0, 0)

    print("  Fetching processed scan tasks from source...")
    tasks = fetch_scan_tasks(src_session, src_url, limit)
    if not tasks:
        print("  No processed scan tasks found.")
        return (0, 0, 0)

    print(f"  Found {len(tasks)} task(s).")

    succeeded = skipped = failed = 0

    for i, task in enumerate(tasks, 1):
        task_id = task.get("id", "")
        task_name = task.get("name") or "(unnamed)"
        src_site_id = task.get("site_id", "")
        created = format_ts(task.get("created_at"))

        tgt_site_id = site_map.get(src_site_id)
        if not tgt_site_id or tgt_site_id == "DRY_RUN":
            skipped += 1
            continue

        prefix = f"  [{i}/{len(tasks)}] '{task_name}' ({created})"

        if dry_run:
            print(f"{prefix} — [DRY RUN] would import.")
            continue

        try:
            print(f"{prefix} — downloading…", end="", flush=True)
            data, filename = download_task_data(src_session, src_url, task_id)
            size_mb = len(data) / (1024 * 1024)
            print(f" {size_mb:.1f} MB — importing…", end="", flush=True)

            # Preserve the original task's name + description so the
            # imported task in the target doesn't get the default
            # 'Scan Import' label.
            import_name = task_name if task_name != "(unnamed)" else ""
            import_desc = task.get("description") or ""
            resp = import_task_data(
                tgt_session, tgt_url, tgt_site_id, data, filename,
                task_name=import_name, task_description=import_desc,
            )
            if resp.status_code < 400:
                print(" ✓")
                succeeded += 1
            else:
                print(f" ✗ HTTP {resp.status_code}: {resp.text[:120]}")
                failed += 1
        except KeyboardInterrupt:
            print("\n  Interrupted — stopping scan data import.")
            break
        except Exception as e:
            print(f" ✗ {e}")
            failed += 1

    print(f"  ✓ {succeeded} imported, {skipped} skipped, {failed} failed.")
    return (succeeded, skipped, failed)


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 7 – Asset Tags
# ══════════════════════════════════════════════════════════════════════════════

def _tags_to_str(tags: Dict[str, str]) -> str:
    parts = []
    for k, v in tags.items():
        if v:
            parts.append(f"{k}={v}")
        else:
            parts.append(k)
    return " ".join(parts)


def _normalize_mac(mac: str) -> str:
    """Normalise a MAC address to lower-case colon-separated form."""
    if not mac:
        return ""
    cleaned = mac.replace("-", "").replace(":", "").replace(".", "").lower()
    if len(cleaned) != 12:
        return mac.lower()
    return ":".join(cleaned[i:i + 2] for i in range(0, 12, 2))


def _asset_keys(obj: Dict) -> List[str]:
    """Return all match keys (IPs + MACs) for an asset."""
    keys: List[str] = []
    for addr in obj.get("addresses") or []:
        if isinstance(addr, str):
            a = addr.strip()
            if a:
                keys.append(f"ip:{a}")
    for addr in obj.get("addresses_extra") or []:
        if isinstance(addr, str):
            a = addr.strip()
            if a:
                keys.append(f"ip:{a}")
    for mac in obj.get("macs") or []:
        if isinstance(mac, str):
            m = _normalize_mac(mac)
            if m:
                keys.append(f"mac:{m}")
    return keys


def migrate_asset_tags(
    src_session, src_url: str,
    tgt_session, tgt_url: str,
    dry_run: bool,
) -> int:
    print("\n╔══════════════════════════════════════╗")
    print("║  Phase 7: Asset Tags                 ║")
    print("╚══════════════════════════════════════╝")

    if not MIGRATE_TAGS:
        print("  ⚠ Skipped (MIGRATE_TAGS = False).")
        return 0

    print("  Exporting source assets with tags…")
    resp = api(
        src_session, "GET",
        f"{src_url}/export/org/assets.jsonl",
        params={"fields": "id,addresses,addresses_extra,macs,tags"},
        stream=True,
    )
    key_to_tags: Dict[str, Dict[str, str]] = {}
    src_count = 0
    src_scanned = 0
    for obj in iter_jsonl(resp):
        src_scanned += 1
        if src_scanned % 5000 == 0:
            print(f"    … scanned {src_scanned} source asset(s), "
                  f"{src_count} with tags so far")
        tags = obj.get("tags")
        if not tags or not isinstance(tags, dict):
            continue
        tags = {k: v for k, v in tags.items() if k}
        if not tags:
            continue
        src_count += 1
        for key in _asset_keys(obj):
            key_to_tags[key] = tags

    print(f"    … scanned {src_scanned} source asset(s) total.")
    if not key_to_tags:
        print("  No source assets with tags found.")
        return 0

    print(f"  Found {src_count} source asset(s) with tags "
          f"({len(key_to_tags)} match keys).")

    print("  Matching target assets by IP/MAC…")
    resp = api(
        tgt_session, "GET",
        f"{tgt_url}/export/org/assets.jsonl",
        params={"fields": "id,addresses,addresses_extra,macs,tags"},
        stream=True,
    )

    matched = applied = 0
    scanned = 0
    seen_tgt: set = set()
    for obj in iter_jsonl(resp):
        scanned += 1
        if scanned % 5000 == 0:
            print(f"    … scanned {scanned} target asset(s), "
                  f"matched {matched}, applied {applied}")
        tgt_id = obj.get("id")
        if not tgt_id or tgt_id in seen_tgt:
            continue
        src_tags: Dict[str, str] = {}
        for key in _asset_keys(obj):
            if key in key_to_tags:
                src_tags = key_to_tags[key]
                break
        if not src_tags:
            continue

        seen_tgt.add(tgt_id)
        matched += 1
        if dry_run:
            continue

        # Merge with any existing target tags so we don't overwrite
        existing = obj.get("tags") or {}
        merged = {**existing, **src_tags} if isinstance(existing, dict) else src_tags
        tag_str = _tags_to_str(merged)

        r = api(
            tgt_session, "PATCH",
            f"{tgt_url}/org/assets/{tgt_id}/tags",
            json_body={"tags": tag_str},
        )
        if r.status_code < 400:
            applied += 1

    print(f"    … scanned {scanned} target asset(s) total.")
    if dry_run:
        print(f"  [DRY RUN] Would apply tags to {matched} asset(s).")
    else:
        print(f"  ✓ Matched {matched} asset(s), applied tags to {applied}.")
    return applied


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 8 – Asset Comments
# ══════════════════════════════════════════════════════════════════════════════

def migrate_asset_comments(
    src_session, src_url: str,
    tgt_session, tgt_url: str,
    dry_run: bool,
) -> int:
    print("\n╔══════════════════════════════════════╗")
    print("║  Phase 8: Asset Comments             ║")
    print("╚══════════════════════════════════════╝")

    if not MIGRATE_COMMENTS:
        print("  ⚠ Skipped (MIGRATE_COMMENTS = False).")
        return 0

    print("  Exporting source assets with comments…")
    resp = api(
        src_session, "GET",
        f"{src_url}/export/org/assets.jsonl",
        params={"fields": "id,addresses,addresses_extra,macs,comments"},
        stream=True,
    )
    key_to_comments: Dict[str, str] = {}
    src_count = 0
    src_scanned = 0
    for obj in iter_jsonl(resp):
        src_scanned += 1
        if src_scanned % 5000 == 0:
            print(f"    … scanned {src_scanned} source asset(s), "
                  f"{src_count} with comments so far")
        comments = (obj.get("comments") or "").strip()
        if not comments:
            continue
        src_count += 1
        for key in _asset_keys(obj):
            key_to_comments[key] = comments

    print(f"    … scanned {src_scanned} source asset(s) total.")
    if not key_to_comments:
        print("  No source assets with comments found.")
        return 0

    print(f"  Found {src_count} source asset(s) with comments "
          f"({len(key_to_comments)} match keys).")

    print("  Matching target assets by IP/MAC…")
    resp = api(
        tgt_session, "GET",
        f"{tgt_url}/export/org/assets.jsonl",
        params={"fields": "id,addresses,addresses_extra,macs"},
        stream=True,
    )

    matched = applied = 0
    scanned = 0
    seen_tgt: set = set()
    for obj in iter_jsonl(resp):
        scanned += 1
        if scanned % 5000 == 0:
            print(f"    … scanned {scanned} target asset(s), "
                  f"matched {matched}, applied {applied}")
        tgt_id = obj.get("id")
        if not tgt_id or tgt_id in seen_tgt:
            continue
        comment = ""
        for key in _asset_keys(obj):
            if key in key_to_comments:
                comment = key_to_comments[key]
                break
        if not comment:
            continue

        seen_tgt.add(tgt_id)
        matched += 1
        if dry_run:
            continue

        r = api(
            tgt_session, "PATCH",
            f"{tgt_url}/org/assets/{tgt_id}/comments",
            json_body={"comments": comment},
        )
        if r.status_code < 400:
            applied += 1

    print(f"    … scanned {scanned} target asset(s) total.")
    if dry_run:
        print(f"  [DRY RUN] Would apply comments to {matched} asset(s).")
    else:
        print(f"  ✓ Matched {matched} asset(s), applied comments to {applied}.")
    return applied


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 9 – Asset Criticality
# ══════════════════════════════════════════════════════════════════════════════

def migrate_asset_criticality(
    src_session, src_url: str,
    tgt_session, tgt_url: str,
    dry_run: bool,
) -> int:
    print("\n╔══════════════════════════════════════╗")
    print("║  Phase 9: Asset Criticality          ║")
    print("╚══════════════════════════════════════╝")

    if not MIGRATE_CRITICALITY:
        print("  ⚠ Skipped (MIGRATE_CRITICALITY = False).")
        return 0

    print("  Exporting source assets with criticality…")
    resp = api(
        src_session, "GET",
        f"{src_url}/export/org/assets.jsonl",
        params={"fields": "id,addresses,addresses_extra,macs,criticality,attributes"},
        stream=True,
    )
    key_to_criticality: Dict[str, str] = {}
    src_count = 0
    src_scanned = 0

    for obj in iter_jsonl(resp):
        src_scanned += 1
        if src_scanned % 5000 == 0:
            print(f"    … scanned {src_scanned} source asset(s), "
                  f"{src_count} with criticality so far")
        crit = obj.get("criticality") or ""
        if not crit:
            attrs = obj.get("attributes") or {}
            crit = attrs.get("criticality", "")
        if not crit or crit.lower() in ("", "none", "normal"):
            continue
        src_count += 1
        for key in _asset_keys(obj):
            key_to_criticality[key] = crit

    print(f"    … scanned {src_scanned} source asset(s) total.")
    if not key_to_criticality:
        print("  No source assets with non-default criticality found.")
        return 0

    print(f"  Found {src_count} source asset(s) with criticality "
          f"({len(key_to_criticality)} match keys).")

    print("  Matching target assets by IP/MAC…")
    resp = api(
        tgt_session, "GET",
        f"{tgt_url}/export/org/assets.jsonl",
        params={"fields": "id,addresses,addresses_extra,macs"},
        stream=True,
    )

    matched = applied = 0
    scanned = 0
    seen_tgt: set = set()
    for obj in iter_jsonl(resp):
        scanned += 1
        if scanned % 5000 == 0:
            print(f"    … scanned {scanned} target asset(s), "
                  f"matched {matched}, applied {applied}")
        tgt_id = obj.get("id")
        if not tgt_id or tgt_id in seen_tgt:
            continue
        crit = ""
        for key in _asset_keys(obj):
            if key in key_to_criticality:
                crit = key_to_criticality[key]
                break
        if not crit:
            continue

        seen_tgt.add(tgt_id)
        matched += 1
        if dry_run:
            continue

        r = api(
            tgt_session, "PATCH",
            f"{tgt_url}/org/assets/{tgt_id}/criticality",
            json_body={"criticality": crit},
        )
        if r.status_code < 400:
            applied += 1

    print(f"    … scanned {scanned} target asset(s) total.")
    if dry_run:
        print(f"  [DRY RUN] Would apply criticality to {matched} asset(s).")
    else:
        print(f"  ✓ Matched {matched} asset(s), applied criticality to {applied}.")
    return applied


# ══════════════════════════════════════════════════════════════════════════════
# PER-ORG MIGRATION RUNNER
# ══════════════════════════════════════════════════════════════════════════════

def migrate_one_org(
    *,
    src_org_sess,
    src_url: str,
    src_oid: str,
    tgt_org_sess,
    tgt_url: str,
    tgt_oid: str,
    src_acct_sess,
    tgt_acct_sess,
    org_label: str,
    dry_run: bool,
    task_limit: int,
) -> Dict[str, Any]:
    """Run all per-org migration phases.  Returns a results dict."""
    print()
    print("─" * 54)
    print(f"  Migrating org: {org_label}")
    if src_oid:
        print(f"  Source org ID : {src_oid[:8]}…")
    if tgt_oid:
        print(f"  Target org ID: {tgt_oid[:8]}…")
    if RECURRING_TASKS_ONLY:
        print(f"  Mode: RECURRING_TASKS_ONLY (Phases 3b/3c only)")
    print("─" * 54)

    try:
        src_org = get_org_details(src_org_sess, src_url)
    except Exception as e:
        print(f"  ERROR: Cannot read source org — {e}", file=sys.stderr)
        return {}

    detected_src_oid = src_oid or src_org.get("id", "")

    # ──────────────────────────────────────────────────────────
    # RECURRING_TASKS_ONLY path: only build site map and run 3b/3c
    # ──────────────────────────────────────────────────────────
    if RECURRING_TASKS_ONLY:
        site_map = migrate_sites(
            src_org_sess, src_url, tgt_org_sess, tgt_url, dry_run,
            map_only=True,
        )
        migrate_recurring_tasks(
            src_org_sess, src_url, tgt_org_sess, tgt_url, site_map, dry_run,
        )
        migrate_integration_tasks(
            src_org_sess, src_url, tgt_org_sess, tgt_url, dry_run,
        )
        return {"site_map": site_map}

    # ──────────────────────────────────────────────────────────
    # Full migration path
    # ──────────────────────────────────────────────────────────

    # Phase 1
    migrate_org_settings(
        src_org_sess, src_url, tgt_org_sess, tgt_url, src_org, dry_run,
    )

    # Phase 2
    site_map = migrate_sites(
        src_org_sess, src_url, tgt_org_sess, tgt_url, dry_run,
    )

    # Phase 3
    acct_src = src_acct_sess or src_org_sess
    acct_tgt = tgt_acct_sess or tgt_org_sess
    migrate_scan_templates(
        acct_src, src_url, detected_src_oid,
        acct_tgt, tgt_url, tgt_oid,
        site_map, dry_run,
    )

    # Phase 3b – Recurring scan tasks (opt-in)
    if MIGRATE_RECURRING_TASKS:
        migrate_recurring_tasks(
            src_org_sess, src_url, tgt_org_sess, tgt_url, site_map, dry_run,
        )
        # Phase 3c – Recurring integration sync tasks (opt-in, same toggle)
        migrate_integration_tasks(
            src_org_sess, src_url, tgt_org_sess, tgt_url, dry_run,
        )
    else:
        print("\n╭────────────────────────────────────")
        print("  Phases 3b & 3c: Recurring Tasks — SKIPPED")
        print("  Recommended: complete this first pass, then on the")
        print("  target install Explorers and recreate credentials")
        print("  with matching names. Then re-run with")
        print("  RECURRING_TASKS_ONLY = True for a safe second pass.")
        print("╰────────────────────────────────────")

    # Phase 4 — Custom Integrations
    # Definitions (name + icon) are migrated. The Starlark script body is
    # NOT migrated — paste it manually on the target. Dedup is by name, so
    # in multi-org runs the same integration is only created once on target.
    integration_map = migrate_custom_integrations(
        acct_src, src_url, detected_src_oid,
        acct_tgt, tgt_url, tgt_oid,
        dry_run,
    )

    # Phase 5
    migrate_ownership_types(acct_src, src_url, acct_tgt, tgt_url, dry_run)

    # Phase 6
    scan_ok, scan_skip, scan_fail = migrate_scan_data(
        src_org_sess, src_url, tgt_org_sess, tgt_url,
        site_map, dry_run, task_limit,
    )

    # Phase 7
    migrate_asset_tags(src_org_sess, src_url, tgt_org_sess, tgt_url, dry_run)

    # Phase 8
    migrate_asset_comments(src_org_sess, src_url, tgt_org_sess, tgt_url, dry_run)

    # Phase 9
    migrate_asset_criticality(src_org_sess, src_url, tgt_org_sess, tgt_url, dry_run)

    return {
        "site_map": site_map,
        "integration_map": integration_map,
        "scan_data": {"ok": scan_ok, "skip": scan_skip, "fail": scan_fail},
    }


# ══════════════════════════════════════════════════════════════════════════════
# DRY-RUN INVENTORY (source-only preview for orgs that don't exist on target)
# ══════════════════════════════════════════════════════════════════════════════

def _dry_run_inventory(
    src_sess, src_url: str, src_oid: str, org_name: str,
) -> None:
    """Print a source-side inventory for an org that would be created."""
    print()
    print("─" * 54)
    print(f"  [DRY RUN] Org: {org_name}")
    print(f"  Source org ID: {src_oid[:8]}…")
    print(f"  (Target org would be created)")
    print("─" * 54)

    # Org settings
    try:
        org = get_org_details(src_sess, src_url)
        settings = {k: org.get(k) for k in SETTING_KEYS if org.get(k) is not None}
        if settings:
            print(f"\n  Settings to copy: {len(settings)}")
            for k, v in settings.items():
                print(f"    {k} = {v}")
    except Exception:
        pass

    # Sites
    try:
        resp = api(src_sess, "GET", f"{src_url}/org/sites")
        if resp.status_code < 400:
            sites = _as_list(resp.json())
            print(f"\n  Sites: {len(sites)} would be created")
            for s in sites:
                subnet_count = len(s.get("subnets", {}) or {})
                subnet_info = f" ({subnet_count} subnet(s))" if subnet_count else ""
                print(f"    • '{s['name']}'{subnet_info}")
    except Exception:
        pass

    # Scan templates
    if has_account_tokens():
        try:
            resp = api(src_sess, "GET", f"{src_url}/account/tasks/templates",
                       params={"_oid": src_oid})
            if resp.status_code < 400:
                templates = _as_list(resp.json())
                templates = [t for t in templates
                             if t.get("organization_id") == src_oid or t.get("global")]
                recurring = sum(1 for t in templates if t.get("recur"))
                print(f"\n  Scan templates: {len(templates)} would be created "
                      f"({recurring} recurring)")
                for t in templates:
                    freq = t.get("recur_frequency", "")
                    tag = f" [recurring: {freq}]" if t.get("recur") else ""
                    print(f"    • '{t.get('name', '(unnamed)')}'{tag}")
        except Exception:
            pass

        # Custom integrations — definition created automatically; Starlark
        # body must be pasted manually on the target afterwards.
        try:
            resp = api(src_sess, "GET", f"{src_url}/account/custom-integrations",
                       params={"_oid": src_oid})
            if resp.status_code < 400:
                integrations = _as_list(resp.json())
                if integrations:
                    print(f"\n  Custom integrations: {len(integrations)} on source")
                    print(f"    (Starlark script body MUST be pasted manually.)")
                    for i in integrations:
                        print(f"    • '{i.get('name', '(unnamed)')}'")
        except Exception:
            pass

    # Scan tasks
    try:
        tasks = fetch_scan_tasks(src_sess, src_url, TASK_LIMIT)
        if tasks:
            limit_note = f" (TASK_LIMIT={TASK_LIMIT})" if TASK_LIMIT else " (no limit)"
            print(f"\n  Scan tasks: {len(tasks)} would be imported{limit_note}")
    except Exception:
        pass

    # Recurring scan tasks
    try:
        resp = api(src_sess, "GET", f"{src_url}/org/tasks",
                   params={"search": "type:scan recur:true"})
        if resp.status_code < 400:
            recurring = [
                t for t in _as_list(resp.json())
                if t.get("recur") and t.get("status") not in ("removed", "failed")
            ]
            seen: set = set()
            unique = []
            for t in recurring:
                key = (
                    f"{t.get('template_id', '')}|{t.get('site_id', '')}|"
                    f"{t.get('name', '')}"
                )
                if key not in seen:
                    seen.add(key)
                    unique.append(t)
            if unique:
                print(f"\n  Recurring scan tasks: {len(unique)} on source")
                print(f"    (NOT created on the first pass — these are only")
                print(f"    created on the SECOND pass with RECURRING_TASKS_ONLY=True)")
                for t in unique:
                    freq = t.get("recur_frequency", "")
                    print(f"    • '{t.get('name', '(unnamed)')}' [{freq}]")
    except Exception:
        pass

    # Recurring integration sync tasks (need credentials by name on target)
    try:
        creds = _fetch_credentials(src_sess, src_url)
        creds_by_id = {c.get("id", ""): c.get("name", "") for c in creds}
        resp = api(src_sess, "GET", f"{src_url}/org/tasks",
                   params={"search": "type:connector recur:true"})
        if resp.status_code < 400:
            recurring = [
                t for t in _as_list(resp.json())
                if t.get("recur") and t.get("status") not in ("removed", "failed")
            ]
            seen: set = set()
            unique = []
            for t in recurring:
                key = (
                    f"{t.get('source_id', '')}|{t.get('credential_id', '')}|"
                    f"{t.get('name', '')}"
                )
                if key not in seen:
                    seen.add(key)
                    unique.append(t)
            if unique:
                print(f"\n  Integration sync tasks: {len(unique)} on source")
                print(f"    (NOT created on the first pass — these are only")
                print(f"    created on the SECOND pass with RECURRING_TASKS_ONLY=True)")
                cred_names_needed = set()
                for t in unique:
                    cred_name = creds_by_id.get(t.get("credential_id", ""), "?")
                    if cred_name and cred_name != "?":
                        cred_names_needed.add(cred_name)
                    freq = t.get("recur_frequency", "")
                    print(f"    • '{t.get('name', '(unnamed)')}' [{freq}] "
                          f"(credential: '{cred_name}')")
                if cred_names_needed:
                    print(f"\n  → Required credentials on target ({len(cred_names_needed)}):")
                    for n in sorted(cred_names_needed):
                        print(f"      - {n}")
    except Exception:
        pass

    # Asset metadata
    try:
        resp = api(src_sess, "GET", f"{src_url}/export/org/assets.jsonl",
                   params={"fields": "id,tags,comments,criticality,attributes"},
                   stream=True)
        tagged = commented = critical = total = 0
        for obj in iter_jsonl(resp):
            total += 1
            if obj.get("tags"):
                tagged += 1
            if (obj.get("comments") or "").strip():
                commented += 1
            crit = obj.get("criticality") or ""
            if not crit:
                crit = (obj.get("attributes") or {}).get("criticality", "")
            if crit and crit.lower() not in ("", "none", "normal"):
                critical += 1
        parts = []
        if tagged:
            parts.append(f"{tagged} with tags")
        if commented:
            parts.append(f"{commented} with comments")
        if critical:
            parts.append(f"{critical} with criticality")
        if parts:
            print(f"\n  Assets: {total} total — {', '.join(parts)}")
        else:
            print(f"\n  Assets: {total} total (no tags/comments/criticality)")
    except Exception:
        pass

    print()


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main() -> int:
    if not validate():
        return 2

    # RECURRING_TASKS_ONLY implies MIGRATE_RECURRING_TASKS
    global MIGRATE_RECURRING_TASKS
    if RECURRING_TASKS_ONLY:
        MIGRATE_RECURRING_TASKS = True

    is_multi = multi_org_mode()

    print()
    print("=" * 54)
    print("   runZero Tenant Migration")
    print("=" * 54)
    print(f"  Source  : {SOURCE_BASE_URL}")
    print(f"  Target  : {TARGET_BASE_URL}")
    print(f"  Mode    : {'Multi-org (all organisations)' if is_multi else 'Single-org'}")
    print(f"  Dry run : {'Yes' if DRY_RUN else 'No'}")
    if RECURRING_TASKS_ONLY:
        print(f"  Phases  : RECURRING_TASKS_ONLY (3b/3c only)")
    else:
        if TASK_LIMIT:
            print(f"  Tasks   : {TASK_LIMIT} most recent per organisation")
        else:
            print(f"  Tasks   : All processed scan tasks")
        print(f"  Tags    : {'Yes' if MIGRATE_TAGS else 'No'}")
        print(f"  Comments: {'Yes' if MIGRATE_COMMENTS else 'No'}")
        print(f"  Crit.   : {'Yes' if MIGRATE_CRITICALITY else 'No'}")
        print(f"  Recur.  : {'Yes (3b/3c)' if MIGRATE_RECURRING_TASKS else 'No'}")
    print()

    # ── Build sessions ──
    src_org_sess = (
        build_session(SOURCE_ORG_TOKEN, "source-org") if SOURCE_ORG_TOKEN else None
    )
    tgt_org_sess = (
        build_session(TARGET_ORG_TOKEN, "target-org") if TARGET_ORG_TOKEN else None
    )
    src_acct_sess = (
        build_session(SOURCE_ACCOUNT_TOKEN, "source-acct")
        if SOURCE_ACCOUNT_TOKEN else None
    )
    tgt_acct_sess = (
        build_session(TARGET_ACCOUNT_TOKEN, "target-acct")
        if TARGET_ACCOUNT_TOKEN else None
    )

    # ════════════════════════════════════════════════════════════════
    # MULTI-ORG MODE
    # ════════════════════════════════════════════════════════════════
    if is_multi:
        print("Verifying Account API access…")
        try:
            resp = api(src_acct_sess, "GET", f"{SOURCE_BASE_URL}/account/orgs")
            resp.raise_for_status()
            print(f"  Source account: {len(_as_list(resp.json()))} org(s)")
        except Exception as e:
            print(f"  ERROR: Cannot reach source account API — {e}", file=sys.stderr)
            return 1

        try:
            resp = api(tgt_acct_sess, "GET", f"{TARGET_BASE_URL}/account/orgs")
            resp.raise_for_status()
            print(f"  Target account: {len(_as_list(resp.json()))} org(s)")
        except Exception as e:
            print(f"  ERROR: Cannot reach target account API — {e}", file=sys.stderr)
            return 1

        if not DRY_RUN:
            print(
                f"\nThis will replicate ALL organisations and data from\n"
                f"  {SOURCE_BASE_URL}\nto\n  {TARGET_BASE_URL}."
            )
            print("Proceed? [y/N]: ", end="", flush=True)
            try:
                answer = input().strip().lower()
            except (KeyboardInterrupt, EOFError):
                print("\nAborted.")
                return 130
            if answer != "y":
                print("Aborted.")
                return 0

        # Phase 0
        org_map = migrate_org_hierarchy(
            src_acct_sess, SOURCE_BASE_URL,
            tgt_acct_sess, TARGET_BASE_URL,
            DRY_RUN,
        )

        if not org_map:
            print("  ERROR: No organisations mapped. Aborting.", file=sys.stderr)
            return 1

        # Per-org phases
        all_results: Dict[str, Dict] = {}
        for src_oid, tgt_oid in org_map.items():
            scoped_src = _OidSession(src_acct_sess, src_oid)

            try:
                org_info = get_org_details(scoped_src, SOURCE_BASE_URL)
                org_name = org_info.get("name", src_oid[:8])
            except Exception:
                org_name = src_oid[:8]

            # In dry run, target orgs don't exist yet — show source inventory
            if tgt_oid == "DRY_RUN":
                _dry_run_inventory(scoped_src, SOURCE_BASE_URL, src_oid, org_name)
                continue

            scoped_tgt = _OidSession(tgt_acct_sess, tgt_oid)

            result = migrate_one_org(
                src_org_sess=scoped_src,
                src_url=SOURCE_BASE_URL,
                src_oid=src_oid,
                tgt_org_sess=scoped_tgt,
                tgt_url=TARGET_BASE_URL,
                tgt_oid=tgt_oid,
                src_acct_sess=src_acct_sess,
                tgt_acct_sess=tgt_acct_sess,
                org_label=org_name,
                dry_run=DRY_RUN,
                task_limit=TASK_LIMIT,
            )
            all_results[org_name] = result

        if SAVE_MAPPING and not DRY_RUN:
            mapping = {
                "source_url": SOURCE_BASE_URL,
                "target_url": TARGET_BASE_URL,
                "org_id_mapping": org_map,
                "per_org_results": {
                    name: {
                        "site_id_mapping": r.get("site_map", {}),
                        "custom_integration_mapping": r.get("integration_map", {}),
                    }
                    for name, r in all_results.items()
                },
                "migrated_at": datetime.now(timezone.utc).isoformat(),
            }
            mapping_path = "migrate_mapping.json"
            with open(mapping_path, "w") as f:
                json.dump(mapping, f, indent=2)
            print(f"\n  ID mapping saved to {mapping_path}")

    # ════════════════════════════════════════════════════════════════
    # SINGLE-ORG MODE
    # ════════════════════════════════════════════════════════════════
    else:
        if not src_org_sess or not tgt_org_sess:
            print(
                "ERROR: Single-org mode requires both SOURCE_ORG_TOKEN and "
                "TARGET_ORG_TOKEN.",
                file=sys.stderr,
            )
            return 2

        print("Verifying API access…")
        try:
            src_org = get_org_details(src_org_sess, SOURCE_BASE_URL, "Source")
        except Exception as e:
            print(f"  ERROR: Cannot reach source org — {e}", file=sys.stderr)
            return 1

        try:
            tgt_org = get_org_details(tgt_org_sess, TARGET_BASE_URL, "Target")
        except Exception as e:
            print(f"  ERROR: Cannot reach target org — {e}", file=sys.stderr)
            return 1

        src_oid = SOURCE_ORG_ID or src_org.get("id", "")
        tgt_oid = TARGET_ORG_ID or tgt_org.get("id", "")

        if has_account_tokens():
            print("  Account API keys : provided ✓ (Phases 3-5 enabled)")
        else:
            print("  Account API keys : not provided (Phases 3-5 will be skipped)")

        print()

        if not DRY_RUN:
            print(
                f"This will write data to the target org "
                f"'{tgt_org.get('name', '')}' on {TARGET_BASE_URL}."
            )
            print("Proceed? [y/N]: ", end="", flush=True)
            try:
                answer = input().strip().lower()
            except (KeyboardInterrupt, EOFError):
                print("\nAborted.")
                return 130
            if answer != "y":
                print("Aborted.")
                return 0

        result = migrate_one_org(
            src_org_sess=src_org_sess,
            src_url=SOURCE_BASE_URL,
            src_oid=src_oid,
            tgt_org_sess=tgt_org_sess,
            tgt_url=TARGET_BASE_URL,
            tgt_oid=tgt_oid,
            src_acct_sess=src_acct_sess,
            tgt_acct_sess=tgt_acct_sess,
            org_label=src_org.get("name", "default"),
            dry_run=DRY_RUN,
            task_limit=TASK_LIMIT,
        )

        if SAVE_MAPPING and not DRY_RUN:
            mapping = {
                "source_url": SOURCE_BASE_URL,
                "target_url": TARGET_BASE_URL,
                "source_org_id": src_oid,
                "target_org_id": tgt_oid,
                "site_id_mapping": result.get("site_map", {}),
                "custom_integration_mapping": result.get("integration_map", {}),
                "migrated_at": datetime.now(timezone.utc).isoformat(),
            }
            mapping_path = "migrate_mapping.json"
            with open(mapping_path, "w") as f:
                json.dump(mapping, f, indent=2)
            print(f"\n  ID mapping saved to {mapping_path}")

    # ── Summary ──
    print()
    print("=" * 54)
    if DRY_RUN:
        print("   Dry Run Complete")
    elif RECURRING_TASKS_ONLY:
        print("   Second Pass Complete (Recurring Tasks Only)")
    else:
        print("   First Pass Complete")
    print("=" * 54)
    print()

    if DRY_RUN:
        print("  No changes were made to the target.")
        print("  Set DRY_RUN = False and re-run to perform the migration.")
        print()
    elif RECURRING_TASKS_ONLY:
        print("  Phases 3b & 3c finished. Recurring scan tasks and")
        print("  integration sync tasks have been recreated on the target.")
        print()
        print("  Final checks:")
        print("  1. In the target console, open Tasks → Recurring and")
        print("     verify each task is enabled with the correct schedule,")
        print("     site, Explorer, and credentials.")
        print("  2. Trigger one task manually to confirm it runs cleanly.")
        print("  3. Reconfigure SSO / SAML and recreate saved queries,")
        print("     goals, and dashboards (no API for these).")
        print()
    else:
        print("  First pass finished. Sites, scan data, and asset")
        print("  metadata (tags / comments / criticality) are migrated.")
        print()
        print("  ── Next: prepare for the SECOND PASS ──")
        print("  1. Install Explorers on the target and wait until they")
        print("     show as online.")
        print("  2. Recreate credentials (SNMP, cloud keys, integration")
        print("     keys, etc.) using the SAME NAMES as on the source.")
        print("  3. For each Custom Integration migrated by Phase 4, open it")
        print("     on the target and PASTE the Starlark script body from")
        print("     the source console (Account → Custom Integrations).")
        print("  4. Add users / assign roles, reconfigure SSO / SAML.")
        print("  5. Recreate saved queries, goals, and dashboards (no API).")
        print("  6. Verify asset inventory and scan data in the target.")
        print()
        print("  ── Then run the SECOND PASS ──")
        print("  7. Set RECURRING_TASKS_ONLY = True in this script and")
        print("     run it once more. ONLY Phases 3b/3c will run — no")
        print("     scan re-import, no asset metadata re-apply.")
        print("     Do NOT re-run the full script (it would create")
        print("     duplicate scan imports).")
        print()

    return 0


if __name__ == "__main__":
    sys.exit(main())

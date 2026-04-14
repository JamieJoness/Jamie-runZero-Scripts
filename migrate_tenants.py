#!/usr/bin/env python3
"""
Full tenant migration for runZero (e.g., US → EU).

Downloads configuration and scan data from one runZero console and recreates
it on another.  Designed for migrating a customer from the US tenant
(console.runzero.com) to a blank EU tenant (console-eu.runzero.com).

Phases:
  1 – Organization settings   (expiration policies)
  2 – Sites                   (names, scope, excludes)
  3 – Scan templates          (requires Account API keys)
  4 – Custom integrations     (requires Account API keys)
  5 – Asset ownership types   (requires Account API keys)
  6 – Scan task data          (download → import into matching target sites)
  7 – Asset tags              (best-effort match by IP address)

NOT migrated (must be done manually):
  • Credentials         – secrets cannot be read via API
  • Users & roles       – create on the new console and assign roles
  • Explorers / Agents  – reinstall and point at the EU console URL
  • SSO / SAML config   – reconfigure on the new console
  • License             – handled by runZero

Required tokens:
  SOURCE_ORG_TOKEN   – Organisation API key (OT-…) on the US console
  TARGET_ORG_TOKEN   – Organisation API key (OT-…) on the EU console

Optional tokens (needed for Phases 3–5):
  SOURCE_ACCOUNT_TOKEN – Account API key (CT-…) on the US console
  TARGET_ACCOUNT_TOKEN – Account API key (CT-…) on the EU console
  SOURCE_ORG_ID        – Organisation UUID (visible on the org settings page)
  TARGET_ORG_ID        – Organisation UUID (visible on the org settings page)

Set values below or export the corresponding environment variables.
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
SOURCE_ACCOUNT_TOKEN = ""   # CT-… (optional, enables Phases 3-5)
SOURCE_ORG_ID        = ""   # required when using CT token

SOURCE_BASE_URL      = os.environ.get("RZ_SOURCE_URL",           SOURCE_BASE_URL).rstrip("/")
SOURCE_ORG_TOKEN     = os.environ.get("RZ_SOURCE_ORG_TOKEN",     SOURCE_ORG_TOKEN)
SOURCE_ACCOUNT_TOKEN = os.environ.get("RZ_SOURCE_ACCOUNT_TOKEN", SOURCE_ACCOUNT_TOKEN)
SOURCE_ORG_ID        = os.environ.get("RZ_SOURCE_ORG_ID",        SOURCE_ORG_ID)

# ============================================================
# TARGET TENANT (EU – console-eu.runzero.com)
# ============================================================
TARGET_BASE_URL      = "https://console-eu.runzero.com/api/v1.0"
TARGET_ORG_TOKEN     = ""   # OT-…
TARGET_ACCOUNT_TOKEN = ""   # CT-… (optional, enables Phases 3-5)
TARGET_ORG_ID        = ""   # required when using CT token

TARGET_BASE_URL      = os.environ.get("RZ_TARGET_URL",           TARGET_BASE_URL).rstrip("/")
TARGET_ORG_TOKEN     = os.environ.get("RZ_TARGET_ORG_TOKEN",     TARGET_ORG_TOKEN)
TARGET_ACCOUNT_TOKEN = os.environ.get("RZ_TARGET_ACCOUNT_TOKEN", TARGET_ACCOUNT_TOKEN)
TARGET_ORG_ID        = os.environ.get("RZ_TARGET_ORG_ID",        TARGET_ORG_ID)

# ============================================================
# MIGRATION OPTIONS
# ============================================================
DRY_RUN         = False   # True = preview only, no writes to target
TASK_LIMIT      = 0       # max scan tasks to import per site (0 = all)
MIGRATE_TAGS    = True    # Phase 7: migrate asset tags
SAVE_MAPPING    = True    # write migrate_mapping.json on completion

TIMEOUT_SECONDS = 180
MAX_RETRIES     = 6


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def build_session(token: str, label: str = "migrate") -> requests.Session:
    """Create a reusable HTTP session with the API token pre-configured."""
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": f"runzero-tenant-migrate/1.0 ({label})",
    })
    return s


def api(
    session: requests.Session,
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
    """Send an HTTP request with exponential-backoff retries on 429/5xx."""
    backoff = 1.0
    resp = None
    for attempt in range(1, MAX_RETRIES + 1):
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
            if attempt == MAX_RETRIES:
                raise
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue

        if resp.status_code in (429, 500, 502, 503, 504):
            if attempt == MAX_RETRIES:
                return resp
            ra = resp.headers.get("Retry-After")
            try:
                time.sleep(float(ra) if ra else backoff)
            except (ValueError, TypeError):
                time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue

        return resp
    return resp  # defensive


# ── Validation ────────────────────────────────────────────────────────────────

def validate() -> bool:
    """Check that required config values are present."""
    ok = True
    if not SOURCE_ORG_TOKEN:
        print("ERROR: SOURCE_ORG_TOKEN is not set.  "
              "Paste it above or export RZ_SOURCE_ORG_TOKEN.", file=sys.stderr)
        ok = False
    if not TARGET_ORG_TOKEN:
        print("ERROR: TARGET_ORG_TOKEN is not set.  "
              "Paste it above or export RZ_TARGET_ORG_TOKEN.", file=sys.stderr)
        ok = False
    return ok


def has_account_tokens() -> bool:
    """True when both source and target account tokens are configured."""
    return bool(SOURCE_ACCOUNT_TOKEN and TARGET_ACCOUNT_TOKEN)


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


def get_org_details(session: requests.Session, base_url: str, label: str = "") -> Dict:
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
# PHASE 1 – Organisation Settings
# ══════════════════════════════════════════════════════════════════════════════

SETTING_KEYS = [
    "expiration_assets_stale",
    "expiration_assets_offline",
    "expiration_integration_attributes",
    "expiration_scans",
    "expiration_vulnerabilities",
    "keep_latest_integration_attributes",
]


def migrate_org_settings(
    src_session: requests.Session,
    src_url: str,
    tgt_session: requests.Session,
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
# PHASE 2 – Sites
# ══════════════════════════════════════════════════════════════════════════════

def migrate_sites(
    src_session: requests.Session,
    src_url: str,
    tgt_session: requests.Session,
    tgt_url: str,
    dry_run: bool,
) -> Dict[str, str]:
    """Create matching sites on the target.  Returns {src_site_id: tgt_site_id}."""
    print("\n╔══════════════════════════════════════╗")
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
    existing_by_name: Dict[str, str] = {s["name"]: s["id"] for s in tgt_sites}

    site_map: Dict[str, str] = {}
    created = 0

    print(f"  Source has {len(src_sites)} site(s), target has {len(tgt_sites)} existing site(s).")

    for site in src_sites:
        name = site["name"]
        src_id = site["id"]

        if name in existing_by_name:
            site_map[src_id] = existing_by_name[name]
            print(f"  • '{name}' — exists in target, mapped.")
            continue

        if dry_run:
            site_map[src_id] = "DRY_RUN"
            print(f"  • '{name}' — [DRY RUN] would create.")
            continue

        site_def: Dict[str, Any] = {"name": name}
        for field in ("description", "scope", "excludes"):
            if site.get(field):
                site_def[field] = site[field]

        resp = api(tgt_session, "PUT", f"{tgt_url}/org/sites", json_body=site_def)
        if resp.status_code >= 400:
            print(f"  ✗ '{name}' — HTTP {resp.status_code}: {resp.text[:200]}")
            continue

        new_site = resp.json()
        site_map[src_id] = new_site["id"]
        created += 1
        print(f"  • '{name}' — created → {new_site['id'][:8]}…")

    print(f"  ✓ {len(site_map)} site(s) mapped ({created} newly created).")
    return site_map


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 3 – Scan Templates
# ══════════════════════════════════════════════════════════════════════════════

def migrate_scan_templates(
    src_session: requests.Session,
    src_url: str,
    src_oid: str,
    tgt_session: requests.Session,
    tgt_url: str,
    tgt_oid: str,
    dry_run: bool,
) -> int:
    print("\n╔══════════════════════════════════════╗")
    print("║  Phase 3: Scan Templates             ║")
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

    print(f"  Found {len(templates)} template(s) in source.")

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

        if dry_run:
            print(f"  • '{name}' — [DRY RUN] would create.")
            continue

        new_tmpl: Dict[str, Any] = {
            "name": name,
            "description": tmpl.get("description", ""),
            "organization_id": tgt_oid,
            "params": tmpl.get("params") or {},
            "global": False,
            "acl": {},
        }

        resp = api(
            tgt_session, "POST",
            f"{tgt_url}/account/tasks/templates",
            json_body=new_tmpl,
        )
        if resp.status_code >= 400:
            print(f"  ✗ '{name}' — HTTP {resp.status_code}: {resp.text[:200]}")
            continue

        created += 1
        print(f"  • '{name}' — created.")

    print(f"  ✓ {created} template(s) created.")
    return created


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 4 – Custom Integrations
# ══════════════════════════════════════════════════════════════════════════════

def migrate_custom_integrations(
    src_session: requests.Session,
    src_url: str,
    src_oid: str,
    tgt_session: requests.Session,
    tgt_url: str,
    tgt_oid: str,
    dry_run: bool,
) -> Dict[str, str]:
    """Returns {src_integration_id: tgt_integration_id}."""
    print("\n╔══════════════════════════════════════╗")
    print("║  Phase 4: Custom Integrations        ║")
    print("╚══════════════════════════════════════╝")

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
    integrations = _as_list(raw) if isinstance(raw, list) else ([raw] if isinstance(raw, dict) and raw.get("id") else [])

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
    existing = _as_list(raw_tgt) if isinstance(raw_tgt, list) else ([raw_tgt] if isinstance(raw_tgt, dict) and raw_tgt.get("id") else [])
    existing_names: Dict[str, str] = {i.get("name", ""): i.get("id", "") for i in existing}
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
            print(f"  • '{name}' — [DRY RUN] would create.")
            continue

        new_def: Dict[str, Any] = {
            "name": name,
            "description": integ.get("description", ""),
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
    src_session: requests.Session,
    src_url: str,
    tgt_session: requests.Session,
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

def fetch_scan_tasks(
    session: requests.Session,
    base_url: str,
    limit: int = 0,
) -> List[Dict]:
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


def download_task_data(
    session: requests.Session,
    base_url: str,
    task_id: str,
) -> Tuple[bytes, str]:
    """Download the .gz scan data for a task.  Returns (bytes, filename)."""
    url = f"{base_url}/org/tasks/{task_id}/data"
    resp = api(session, "GET", url, allow_redirects=False)

    # Resolve download URL from redirect or JSON body
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

    # Download from pre-signed URL — do NOT send the Bearer token
    path = urlparse(download_url).path
    filename = path.split("/")[-1] if path and "/" in path else ""
    if not filename or not filename.endswith(".gz"):
        filename = f"task_{task_id}.gz"

    dl_resp = requests.get(download_url, stream=True, timeout=TIMEOUT_SECONDS)
    dl_resp.raise_for_status()
    return dl_resp.content, filename


def import_task_data(
    session: requests.Session,
    base_url: str,
    site_id: str,
    data: bytes,
    filename: str,
) -> requests.Response:
    """Upload scan data into a target site."""
    url = f"{base_url}/org/sites/{site_id}/import"
    return api(
        session, "PUT", url,
        data=data,
        headers={"Content-Type": "application/octet-stream"},
    )


def migrate_scan_data(
    src_session: requests.Session,
    src_url: str,
    tgt_session: requests.Session,
    tgt_url: str,
    site_map: Dict[str, str],
    dry_run: bool,
    limit: int,
) -> Tuple[int, int, int]:
    """Download scan data from source and import into target sites.

    Returns (succeeded, skipped, failed).
    """
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

    succeeded = 0
    skipped = 0
    failed = 0

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

            resp = import_task_data(tgt_session, tgt_url, tgt_site_id, data, filename)
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
    """Convert a tags dict to the space-separated key=value string the API expects."""
    parts = []
    for k, v in tags.items():
        if v:
            parts.append(f"{k}={v}")
        else:
            parts.append(k)
    return " ".join(parts)


def migrate_asset_tags(
    src_session: requests.Session,
    src_url: str,
    tgt_session: requests.Session,
    tgt_url: str,
    dry_run: bool,
) -> int:
    print("\n╔══════════════════════════════════════╗")
    print("║  Phase 7: Asset Tags                 ║")
    print("╚══════════════════════════════════════╝")

    if not MIGRATE_TAGS:
        print("  ⚠ Skipped (MIGRATE_TAGS = False).")
        return 0

    # ── Export source assets that have tags ──
    print("  Exporting source assets with tags…")
    resp = api(
        src_session, "GET",
        f"{src_url}/export/org/assets.jsonl",
        params={"fields": "id,addresses,macs,tags"},
        stream=True,
    )
    addr_to_tags: Dict[str, Dict[str, str]] = {}
    src_count = 0
    for obj in iter_jsonl(resp):
        tags = obj.get("tags")
        if not tags or not isinstance(tags, dict):
            continue
        # Filter out empty tag dicts
        tags = {k: v for k, v in tags.items() if k}
        if not tags:
            continue
        src_count += 1
        for addr in obj.get("addresses") or []:
            addr = addr.strip()
            if addr:
                addr_to_tags[addr] = tags

    if not addr_to_tags:
        print("  No source assets with tags found.")
        return 0

    print(f"  Found {src_count} source asset(s) with tags ({len(addr_to_tags)} addresses).")

    # ── Match target assets and apply tags ──
    print("  Matching target assets by IP address…")
    resp = api(
        tgt_session, "GET",
        f"{tgt_url}/export/org/assets.jsonl",
        params={"fields": "id,addresses"},
        stream=True,
    )

    matched = 0
    applied = 0
    for obj in iter_jsonl(resp):
        tgt_id = obj.get("id")
        if not tgt_id:
            continue
        for addr in obj.get("addresses") or []:
            addr = addr.strip()
            if addr in addr_to_tags:
                matched += 1
                if dry_run:
                    break

                tag_str = _tags_to_str(addr_to_tags[addr])
                r = api(
                    tgt_session, "PATCH",
                    f"{tgt_url}/org/assets/{tgt_id}/tags",
                    json_body={"tags": tag_str},
                )
                if r.status_code < 400:
                    applied += 1
                break  # one match per target asset is enough

    if dry_run:
        print(f"  [DRY RUN] Would apply tags to {matched} asset(s).")
    else:
        print(f"  ✓ Matched {matched} asset(s), applied tags to {applied}.")
    return applied


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main() -> int:
    if not validate():
        return 2

    print()
    print("=" * 54)
    print("   runZero Tenant Migration")
    print("=" * 54)
    print(f"  Source  : {SOURCE_BASE_URL}")
    print(f"  Target  : {TARGET_BASE_URL}")
    print(f"  Dry run : {'Yes' if DRY_RUN else 'No'}")
    if TASK_LIMIT:
        print(f"  Tasks   : {TASK_LIMIT} most recent per query")
    else:
        print(f"  Tasks   : All processed scan tasks")
    print(f"  Tags    : {'Yes' if MIGRATE_TAGS else 'No'}")
    print()

    # ── Build sessions ──
    src_org_sess = build_session(SOURCE_ORG_TOKEN, "source-org")
    tgt_org_sess = build_session(TARGET_ORG_TOKEN, "target-org")

    src_acct_sess = (
        build_session(SOURCE_ACCOUNT_TOKEN, "source-acct")
        if SOURCE_ACCOUNT_TOKEN
        else None
    )
    tgt_acct_sess = (
        build_session(TARGET_ACCOUNT_TOKEN, "target-acct")
        if TARGET_ACCOUNT_TOKEN
        else None
    )

    # ── Verify access and detect org IDs ──
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

    # ── Confirmation ──
    if not DRY_RUN:
        print(
            f"This will write data to the target org '{tgt_org.get('name', '')}' "
            f"on {TARGET_BASE_URL}."
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

    # ── Account-level sessions for phases 3-5 ──
    src_acct = src_acct_sess or src_org_sess
    tgt_acct = tgt_acct_sess or tgt_org_sess

    # ── Phase 1 ──
    migrate_org_settings(
        src_org_sess, SOURCE_BASE_URL,
        tgt_org_sess, TARGET_BASE_URL,
        src_org, DRY_RUN,
    )

    # ── Phase 2 ──
    site_map = migrate_sites(
        src_org_sess, SOURCE_BASE_URL,
        tgt_org_sess, TARGET_BASE_URL,
        DRY_RUN,
    )

    # ── Phase 3 ──
    migrate_scan_templates(
        src_acct, SOURCE_BASE_URL, src_oid,
        tgt_acct, TARGET_BASE_URL, tgt_oid,
        DRY_RUN,
    )

    # ── Phase 4 ──
    integration_map = migrate_custom_integrations(
        src_acct, SOURCE_BASE_URL, src_oid,
        tgt_acct, TARGET_BASE_URL, tgt_oid,
        DRY_RUN,
    )

    # ── Phase 5 ──
    migrate_ownership_types(
        src_acct, SOURCE_BASE_URL,
        tgt_acct, TARGET_BASE_URL,
        DRY_RUN,
    )

    # ── Phase 6 ──
    scan_ok, scan_skip, scan_fail = migrate_scan_data(
        src_org_sess, SOURCE_BASE_URL,
        tgt_org_sess, TARGET_BASE_URL,
        site_map, DRY_RUN, TASK_LIMIT,
    )

    # ── Phase 7 ──
    migrate_asset_tags(
        src_org_sess, SOURCE_BASE_URL,
        tgt_org_sess, TARGET_BASE_URL,
        DRY_RUN,
    )

    # ── Save mapping ──
    if SAVE_MAPPING and not DRY_RUN:
        mapping = {
            "source_url": SOURCE_BASE_URL,
            "target_url": TARGET_BASE_URL,
            "source_org_id": src_oid,
            "target_org_id": tgt_oid,
            "site_id_mapping": site_map,
            "custom_integration_mapping": integration_map,
            "migrated_at": datetime.now(timezone.utc).isoformat(),
        }
        mapping_path = "migrate_mapping.json"
        with open(mapping_path, "w") as f:
            json.dump(mapping, f, indent=2)
        print(f"\n  ID mapping saved to {mapping_path}")

    # ── Summary ──
    print()
    print("=" * 54)
    print("   Migration Complete")
    print("=" * 54)
    print()
    print("  Manual steps remaining:")
    print("  1. Reinstall Explorers / Agents → point at EU console")
    print("  2. Recreate credentials (SNMP, cloud keys, etc.)")
    print("  3. Add users and assign roles")
    print("  4. Reconfigure SSO / SAML (if applicable)")
    print("  5. Recreate any recurring/scheduled scan tasks")
    print("  6. Verify asset inventory and scan data in the EU console")
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())

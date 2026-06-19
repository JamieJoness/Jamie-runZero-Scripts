#!/usr/bin/env python3
"""
================================================================================
  runZero SaaS -> Self-Hosted Scan Data Sync
================================================================================

WHAT IT DOES
------------
Automatically copies external scan task data from ONE organisation in a runZero
SaaS console into ONE organisation (and site) on a self hosted runZero console.

On each run it:
  1. Connects to the SaaS (source) org and lists processed scan tasks.
  2. Keeps only the scans whose data landed in the last HOURS_BACK hours (24h).
  3. Downloads the gzipped scan data for each of those tasks.
  4. Imports each file into the chosen site on the self-hosted (target) org,
     where runZero re-processes it into the inventory.

The SOURCE org is never modified - every call against the SaaS console is a
read only GET. Tasks already synced are remembered in a small state file so
re runs (e.g. a daily cron job) never import the same scan twice.

This script is designed to run unattended. Everything it does is written to
both the console and a rotating friendly log file with full error tracebacks.

--------------------------------------------------------------------------------
  BEFORE YOU RUN
--------------------------------------------------------------------------------
  1. Create an Organisation API key on EACH console
     (Console -> Organization -> Settings -> Generate Organization API Key):
        - SaaS source org      -> paste into SOURCE_TOKEN
        - Self-hosted target   -> paste into TARGET_TOKEN

  2. Set SOURCE_BASE_URL / TARGET_BASE_URL to the two consoles.

  3. Set TARGET_SITE_NAME (or TARGET_SITE_ID) to the site on the self-hosted
     console where the imported data should land.

  4. (Recommended) Run once with DRY_RUN = True to preview what would sync,
     then set DRY_RUN = False to perform the import.

  5. Schedule it (cron / Task Scheduler / systemd timer). Running it at least
     as often as HOURS_BACK keeps the two consoles in step; the state file
     means overlapping windows are safe and never double-import.

Requires:  Python 3.8+ and the `requests` library  (pip install requests)
================================================================================
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests


# ==============================================================================
# CONFIGURATION - edit the values in this block, then run the script.
# ==============================================================================

# --- SOURCE: runZero SaaS console -------------------------------------------
SOURCE_BASE_URL = "https://console-eu.runzero.com/api/v1.0"   # EU SaaS. US: https://console.runzero.com/api/v1.0
SOURCE_TOKEN    = "PASTE_SAAS_ORG_API_KEY_HERE"            # Organization API key (starts OT-...) for the SaaS org
SOURCE_ORG_ID   = ""                                        # optional; leave blank when using an org-scoped key

# --- TARGET: self-hosted runZero console ------------------------------------
TARGET_BASE_URL  = "https://runzero.yourcompany.com/api/v1.0"  # your self-hosted console + /api/v1.0
TARGET_TOKEN     = "PASTE_SELFHOSTED_ORG_API_KEY_HERE"         # Organization API key (starts OT-...) for the target org
TARGET_ORG_ID    = ""                                           # optional; leave blank when using an org-scoped key
TARGET_SITE_NAME = "Primary"                                    # site to import into (resolved to an ID at runtime)
TARGET_SITE_ID   = ""                                           # optional: exact site ID; overrides TARGET_SITE_NAME

# --- Sync window & behaviour -------------------------------------------------
HOURS_BACK      = 24            # only sync scans from this many hours back
TIME_FIELD      = "created_at"  # task timestamp used for the window: "created_at" (scan start) or "updated_at" (processed)
TASK_SEARCH     = "type:scan"   # which source tasks to consider (external scans are scan tasks)
ONLY_PROCESSED  = True          # only sync tasks whose data has finished processing (importable)
DRY_RUN         = False         # True = list what would sync, import nothing
VERIFY_TLS      = True          # set False ONLY if the self-hosted console uses a self-signed certificate

# --- State / output ----------------------------------------------------------
USE_STATE_FILE  = True                                  # remember synced task IDs to avoid duplicates on re-runs
STATE_FILE      = "saas_to_selfhosted_sync_state.json"  # where the synced-task-ID memory is kept
SAVE_SCAN_FILES = False                                 # True = also keep a local copy of each .gz in scan_downloads/
LOG_FILE        = "saas_to_selfhosted_sync.log"         # detailed run log (appended to)
LOG_LEVEL       = "INFO"                                 # DEBUG, INFO, WARNING, ERROR

# ==============================================================================
# End of configuration. You should not need to edit below this line.
# ==============================================================================

TIMEOUT_SECONDS = 180
MAX_RETRIES     = 6
USER_AGENT      = "runzero-saas-to-selfhosted-sync/1.0"

log = logging.getLogger("saas_sync")


def setup_logging() -> None:
    """Configure logging to both the console and the log file."""
    level = getattr(logging, str(LOG_LEVEL).upper(), logging.INFO)
    log.setLevel(level)
    log.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler(stream=sys.stdout)
    console.setFormatter(fmt)
    log.addHandler(console)

    # File logging is best-effort - never let a bad log path stop the sync.
    try:
        file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
        file_handler.setFormatter(fmt)
        log.addHandler(file_handler)
    except OSError as exc:
        log.warning("Could not open log file '%s' (%s). Logging to console only.", LOG_FILE, exc)


def format_timestamp(ts) -> str:
    """Return a human-readable UTC timestamp from a Unix epoch or ISO 8601 string."""
    try:
        if isinstance(ts, (int, float)):
            return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M UTC")
    except (ValueError, OverflowError, OSError):
        return str(ts)


def to_epoch(ts) -> Optional[float]:
    """Best-effort conversion of a task timestamp to Unix epoch seconds."""
    if ts is None or ts == "":
        return None
    if isinstance(ts, (int, float)):
        return float(ts)
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).timestamp()
    except (ValueError, OverflowError):
        return None


def as_list(data) -> list:
    """Normalise an API response into a list of objects."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("data", "results", "items", "tasks"):
            value = data.get(key)
            if isinstance(value, list):
                return value
    return []


def build_session(token: str, label: str, verify_tls: bool) -> requests.Session:
    """Create a reusable HTTP session pre-configured with the API token."""
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": f"{USER_AGENT} ({label})",
    })
    session.verify = verify_tls
    return session


def request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, str]] = None,
    data: Optional[bytes] = None,
    headers: Optional[Dict[str, str]] = None,
    stream: bool = False,
    allow_redirects: bool = True,
) -> requests.Response:
    """Send an HTTP request, retrying with exponential backoff on network
    errors and on 429/5xx responses (honouring Retry-After when present)."""
    backoff = 1.0
    resp: Optional[requests.Response] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.request(
                method, url,
                params=params,
                data=data,
                headers=headers,
                stream=stream,
                allow_redirects=allow_redirects,
                timeout=TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            if attempt == MAX_RETRIES:
                raise
            log.warning("  Network error on %s %s (attempt %d/%d): %s. Retrying in %.0fs...",
                        method, url, attempt, MAX_RETRIES, exc, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue

        if resp.status_code in (429, 500, 502, 503, 504):
            if attempt == MAX_RETRIES:
                return resp
            retry_after = resp.headers.get("Retry-After")
            try:
                wait = float(retry_after) if retry_after else backoff
            except (ValueError, TypeError):
                wait = backoff
            log.warning("  HTTP %d from %s (attempt %d/%d). Retrying in %.0fs...",
                        resp.status_code, url, attempt, MAX_RETRIES, wait)
            time.sleep(wait)
            backoff = min(backoff * 2, 30)
            continue

        return resp

    return resp  # defensive: should not be reached


def validate_config() -> bool:
    """Check the required configuration values have been filled in."""
    ok = True

    def err(msg: str) -> None:
        nonlocal ok
        log.error(msg)
        ok = False

    if not SOURCE_BASE_URL or "yourcompany" in SOURCE_BASE_URL:
        err("SOURCE_BASE_URL is not set to a real SaaS console URL.")
    if not SOURCE_TOKEN or SOURCE_TOKEN.startswith("PASTE_"):
        err("SOURCE_TOKEN is not set. Paste the SaaS org API key into SOURCE_TOKEN.")
    if not TARGET_BASE_URL or "yourcompany" in TARGET_BASE_URL:
        err("TARGET_BASE_URL is not set to your self-hosted console URL.")
    if not TARGET_TOKEN or TARGET_TOKEN.startswith("PASTE_"):
        err("TARGET_TOKEN is not set. Paste the self-hosted org API key into TARGET_TOKEN.")
    if not TARGET_SITE_NAME and not TARGET_SITE_ID:
        err("Set TARGET_SITE_NAME or TARGET_SITE_ID so the script knows where to import.")
    if HOURS_BACK <= 0:
        err("HOURS_BACK must be greater than 0.")
    if TIME_FIELD not in ("created_at", "updated_at"):
        err("TIME_FIELD must be either 'created_at' or 'updated_at'.")

    return ok


def verify_console(session: requests.Session, base_url: str, org_id: str, label: str) -> Dict:
    """GET /org to confirm the token + URL work, and return the org object."""
    params = {"_oid": org_id} if org_id else None
    resp = request_with_retries(session, "GET", f"{base_url}/org", params=params)
    if resp.status_code == 401:
        raise RuntimeError(f"{label} console rejected the API key (HTTP 401 Unauthorized).")
    if resp.status_code == 403:
        raise RuntimeError(f"{label} console returned HTTP 403 Forbidden - check the key's permissions.")
    resp.raise_for_status()
    org = resp.json()
    if isinstance(org, list):  # some deployments return a list of orgs for account keys
        org = next((o for o in org if o.get("id") == org_id), org[0] if org else {})
    log.info("  %-12s OK  ->  org '%s' (id %s)", label + ":", org.get("name", "?"),
             str(org.get("id", "?"))[:8])
    return org


def resolve_target_site(session: requests.Session, base_url: str, org_id: str) -> Tuple[str, str]:
    """Resolve TARGET_SITE_ID / TARGET_SITE_NAME to a concrete (site_id, site_name)."""
    params = {"_oid": org_id} if org_id else None
    resp = request_with_retries(session, "GET", f"{base_url}/org/sites", params=params)
    resp.raise_for_status()
    sites = as_list(resp.json())
    if not sites:
        raise RuntimeError("No sites found on the target org - create a site first.")

    if TARGET_SITE_ID:
        for site in sites:
            if str(site.get("id")) == TARGET_SITE_ID:
                return site["id"], site.get("name", "")
        raise RuntimeError(f"TARGET_SITE_ID '{TARGET_SITE_ID}' was not found on the target org.")

    wanted = TARGET_SITE_NAME.strip().lower()
    for site in sites:
        if str(site.get("name", "")).strip().lower() == wanted:
            return site["id"], site.get("name", "")

    available = ", ".join(sorted(str(s.get("name", "?")) for s in sites))
    raise RuntimeError(
        f"TARGET_SITE_NAME '{TARGET_SITE_NAME}' was not found on the target org. "
        f"Available sites: {available}"
    )


def fetch_recent_scan_tasks(session: requests.Session, base_url: str, org_id: str) -> List[Dict]:
    """Return processed source scan tasks whose TIME_FIELD falls within the
    last HOURS_BACK hours, most recent first."""
    params: Dict[str, str] = {"search": TASK_SEARCH}
    if ONLY_PROCESSED:
        params["status"] = "processed"
    if org_id:
        params["_oid"] = org_id

    resp = request_with_retries(session, "GET", f"{base_url}/org/tasks", params=params)
    resp.raise_for_status()
    tasks = as_list(resp.json())

    cutoff = (datetime.now(tz=timezone.utc) - timedelta(hours=HOURS_BACK)).timestamp()

    recent: List[Dict] = []
    for task in tasks:
        # A recurring scan's parent definition has no importable data - skip it.
        if task.get("recur") and not task.get("parent_id"):
            continue
        stamp = to_epoch(task.get(TIME_FIELD)) or to_epoch(task.get("updated_at")) or to_epoch(task.get("created_at"))
        if stamp is None or stamp < cutoff:
            continue
        recent.append(task)

    recent.sort(key=lambda t: to_epoch(t.get(TIME_FIELD)) or 0.0, reverse=True)
    return recent


def download_scan_data(session: requests.Session, base_url: str, task_id: str, org_id: str) -> Tuple[bytes, str]:
    """Resolve and download the gzipped scan data for a task.

    /org/tasks/{id}/data may answer with a 30x redirect to a pre-signed URL,
    a 200 JSON envelope {"url": ...}, or the gzip bytes directly. All three
    are handled. Pre-signed URLs are fetched WITHOUT the Authorization header,
    otherwise the cloud object store rejects the signature.
    """
    params = {"_oid": org_id} if org_id else None
    resp = request_with_retries(
        session, "GET", f"{base_url}/org/tasks/{task_id}/data",
        params=params, allow_redirects=False, stream=True,
    )

    download_url = ""
    if resp.status_code in (301, 302, 303, 307, 308):
        download_url = resp.headers.get("Location", "")
    elif resp.status_code == 200:
        content_type = resp.headers.get("Content-Type", "")
        if "application/json" in content_type:
            try:
                download_url = (resp.json() or {}).get("url", "")
            except ValueError:
                download_url = ""
        else:
            # The body IS the gzip payload already.
            data = resp.content
            if not data:
                raise ValueError(f"Task {task_id} returned an empty data body.")
            return data, _derive_filename("", task_id)
    else:
        raise ValueError(f"Unexpected HTTP {resp.status_code} from /org/tasks/{task_id}/data: {resp.text[:300]}")

    if not download_url:
        raise ValueError(f"Could not resolve a download URL for task {task_id} (HTTP {resp.status_code}).")

    filename = _derive_filename(download_url, task_id)
    dl = requests.get(download_url, stream=True, timeout=TIMEOUT_SECONDS, verify=VERIFY_TLS)
    dl.raise_for_status()
    return dl.content, filename


def _derive_filename(download_url: str, task_id: str) -> str:
    """Pick a sensible .gz filename from the URL path, falling back to the task ID."""
    path = urlparse(download_url).path if download_url else ""
    filename = path.split("/")[-1] if "/" in path else ""
    if not filename or not filename.endswith(".gz"):
        filename = f"scan_{task_id}.json.gz"
    return filename


def save_scan_file(data: bytes, filename: str) -> str:
    """Save a local copy of the scan data. Non-fatal on failure."""
    out_dir = "scan_downloads"
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, filename)
    with open(out_path, "wb") as handle:
        handle.write(data)
    return out_path


def import_scan_data(
    session: requests.Session,
    base_url: str,
    org_id: str,
    site_id: str,
    data: bytes,
    task_name: str,
    task_description: str,
) -> requests.Response:
    """Upload gzipped scan data into a site on the target org via
    PUT /org/sites/{site_id}/import. The original task name/description are
    passed as query params so the imported task keeps a meaningful label."""
    params: Dict[str, str] = {}
    if org_id:
        params["_oid"] = org_id
    if task_name:
        params["name"] = task_name
    if task_description:
        params["description"] = task_description

    return request_with_retries(
        session, "PUT", f"{base_url}/org/sites/{site_id}/import",
        params=params or None,
        data=data,
        headers={"Content-Type": "application/octet-stream"},
    )


def load_state() -> Dict[str, Dict]:
    """Load the {task_id: {...}} map of already-synced tasks."""
    if not USE_STATE_FILE or not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        synced = payload.get("synced_tasks", {})
        return synced if isinstance(synced, dict) else {}
    except (OSError, ValueError) as exc:
        log.warning("Could not read state file '%s' (%s). Treating all tasks as new.", STATE_FILE, exc)
        return {}


def save_state(synced: Dict[str, Dict]) -> None:
    """Persist the synced-task map back to disk."""
    if not USE_STATE_FILE:
        return
    payload = {"updated_at": datetime.now(tz=timezone.utc).isoformat(), "synced_tasks": synced}
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
    except OSError as exc:
        log.warning("Could not write state file '%s' (%s). Re-runs may re-import tasks.", STATE_FILE, exc)



def log_task_table(tasks: List[Dict]) -> None:
    """Write a compact table of the tasks about to be synced into the log."""
    log.info("  %-3s  %-30s  %-22s  %-17s  %-8s", "#", "Task name", "Site", "Scan time", "Task ID")
    log.info("  %s", "-" * 88)
    for i, task in enumerate(tasks, 1):
        name = str(task.get("name") or "(unnamed)")[:30]
        site = str(task.get("site_name") or task.get("site_id") or "(unknown)")[:22]
        when = format_timestamp(task.get(TIME_FIELD) or task.get("created_at"))
        tid = str(task.get("id") or "")[:8]
        log.info("  %-3d  %-30s  %-22s  %-17s  %-8s", i, name, site, when, tid)
    log.info("  %s", "-" * 88)



def main() -> int:
    setup_logging()

    log.info("=" * 78)
    log.info("runZero SaaS -> Self-Hosted scan data sync starting")
    log.info("=" * 78)

    if not validate_config():
        log.error("Configuration is incomplete - fix the errors above and re-run.")
        return 2

    log.info("Source (SaaS)   : %s", SOURCE_BASE_URL)
    log.info("Target (self)   : %s", TARGET_BASE_URL)
    log.info("Import into site: %s", TARGET_SITE_ID or TARGET_SITE_NAME)
    log.info("Window          : last %d hour(s) by %s", HOURS_BACK, TIME_FIELD)
    log.info("Mode            : %s", "DRY RUN (no import)" if DRY_RUN else "LIVE")
    if not VERIFY_TLS:
        log.warning("TLS verification is DISABLED (VERIFY_TLS = False).")
        requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]

    source = build_session(SOURCE_TOKEN, "source", verify_tls=True)
    target = build_session(TARGET_TOKEN, "target", verify_tls=VERIFY_TLS)

    log.info("Verifying console connectivity...")
    try:
        verify_console(source, SOURCE_BASE_URL, SOURCE_ORG_ID, "SaaS source")
        verify_console(target, TARGET_BASE_URL, TARGET_ORG_ID, "Self-hosted")
    except Exception as exc:
        log.error("Preflight connectivity check failed: %s", exc)
        log.debug("Connectivity failure detail", exc_info=True)
        return 1

    try:
        site_id, site_name = resolve_target_site(target, TARGET_BASE_URL, TARGET_ORG_ID)
        log.info("  Target site resolved: '%s' (id %s)", site_name, str(site_id)[:8])
    except Exception as exc:
        log.error("Could not resolve the target site: %s", exc)
        return 1

    log.info("Fetching processed scan tasks from the SaaS source org...")
    try:
        tasks = fetch_recent_scan_tasks(source, SOURCE_BASE_URL, SOURCE_ORG_ID)
    except Exception as exc:
        log.error("Failed to list source scan tasks: %s", exc)
        log.debug("Task listing failure detail", exc_info=True)
        return 1

    if not tasks:
        log.info("No scan tasks found in the last %d hour(s). Nothing to do.", HOURS_BACK)
        return 0
    log.info("Found %d scan task(s) within the window.", len(tasks))

    synced = load_state()
    pending = [t for t in tasks if str(t.get("id")) not in synced]
    already = len(tasks) - len(pending)
    if already:
        log.info("Skipping %d task(s) already synced on a previous run.", already)
    if not pending:
        log.info("Everything in the window has already been synced. Nothing to do.")
        return 0

    log.info("%d task(s) to sync:", len(pending))
    log_task_table(pending)

    if DRY_RUN:
        log.info("DRY RUN - no data was imported. Set DRY_RUN = False to perform the sync.")
        return 0

    succeeded = 0
    failed = 0
    total = len(pending)

    for i, task in enumerate(pending, 1):
        task_id = str(task.get("id") or "")
        task_name = task.get("name") or "(unnamed)"
        description = task.get("description") or ""
        log.info("[%d/%d] '%s' (id %s)", i, total, task_name, task_id[:8])

        try:
            log.info("  Downloading scan data from SaaS...")
            data, filename = download_scan_data(source, SOURCE_BASE_URL, task_id, SOURCE_ORG_ID)
            log.info("  Downloaded %.1f KB (%s)", len(data) / 1024, filename)

            if SAVE_SCAN_FILES:
                try:
                    path = save_scan_file(data, filename)
                    log.info("  Saved local copy: %s", path)
                except OSError as save_exc:
                    log.warning("  Could not save local copy: %s", save_exc)

            log.info("  Importing into self-hosted site '%s'...", site_name)
            resp = import_scan_data(
                target, TARGET_BASE_URL, TARGET_ORG_ID, site_id, data,
                task_name=task_name if task_name != "(unnamed)" else "",
                task_description=description,
            )
            if resp.status_code in (200, 201, 202, 204):
                succeeded += 1
                synced[task_id] = {
                    "name": task_name,
                    "synced_at": datetime.now(tz=timezone.utc).isoformat(),
                    "source_time": format_timestamp(task.get(TIME_FIELD) or task.get("created_at")),
                }
                save_state(synced)  # persist after each success so a crash can't lose progress
                log.info("  OK - imported (HTTP %d)", resp.status_code)
            else:
                failed += 1
                log.error("  FAILED - import returned HTTP %d: %s", resp.status_code, resp.text[:300])

        except KeyboardInterrupt:
            log.warning("Interrupted by user. %d succeeded, %d failed so far.", succeeded, failed)
            save_state(synced)
            return 130
        except Exception as exc:
            failed += 1
            log.error("  FAILED - %s", exc)
            log.debug("  Failure detail for task %s", task_id, exc_info=True)
            continue

    save_state(synced)
    log.info("=" * 78)
    log.info("Sync complete: %d imported, %d failed, out of %d task(s).", succeeded, failed, total)
    log.info("=" * 78)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)

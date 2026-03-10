#!/usr/bin/env python3
"""
Migrates scan task data from one runZero organisation to another.

Steps:
  1. Connects to the source org and fetches the N most-recent processed scan tasks.
  2. Downloads the gzipped scan data for each task.
  3. Uploads each scan file to the target org so it can be reprocessed in the UI.

Set SOURCE_TOKEN, TARGET_TOKEN, and TASK_LIMIT below before running.
"""

import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests


# ============================================================
# SOURCE ORG CONFIG
# ============================================================
SOURCE_BASE_URL = "https://console.runzero.com/api/v1.0"  # change if EU or self-hosted
SOURCE_TOKEN    = "PASTE_SOURCE_ORG_API_KEY_HERE"          # Organisations > Settings > Generate Organisation API Key
SOURCE_ORG_ID   = ""                                       # optional: leave blank for org-scoped tokens

SOURCE_BASE_URL = os.environ.get("RUNZERO_SOURCE_BASE_URL", SOURCE_BASE_URL).rstrip("/")
SOURCE_TOKEN    = os.environ.get("RUNZERO_SOURCE_TOKEN",    SOURCE_TOKEN)
SOURCE_ORG_ID   = os.environ.get("RUNZERO_SOURCE_ORG_ID",  SOURCE_ORG_ID)

# ============================================================
# TARGET ORG CONFIG
# ============================================================
TARGET_BASE_URL = "https://console.runzero.com/api/v1.0"  # can be a different console
TARGET_TOKEN    = "PASTE_TARGET_ORG_API_KEY_HERE"          # API key for the destination org
TARGET_ORG_ID   = ""                                       # optional: leave blank for org-scoped tokens

TARGET_BASE_URL = os.environ.get("RUNZERO_TARGET_BASE_URL", TARGET_BASE_URL).rstrip("/")
TARGET_TOKEN    = os.environ.get("RUNZERO_TARGET_TOKEN",    TARGET_TOKEN)
TARGET_ORG_ID   = os.environ.get("RUNZERO_TARGET_ORG_ID",  TARGET_ORG_ID)

# ============================================================
# MIGRATION CONFIG
# ============================================================
TASK_LIMIT      = 10     # number of most-recent processed scan tasks to migrate
DRY_RUN         = False  # True = preview tasks without importing anything
SAVE_SCAN_FILES = False  # True = also save downloaded .gz files to scan_downloads/

TIMEOUT_SECONDS = 120    # allow extra time for large scan file downloads
MAX_RETRIES     = 6
TABLE_MAX_ROWS  = 50
COL_MAX_WIDTH   = 40


# ── helpers ──────────────────────────────────────────────────────────────────

def _trunc(s: str, width: int) -> str:
    """Shorten a string to fit a column, adding … if truncated."""
    return s if len(s) <= width else s[: width - 1] + "\u2026"


def format_timestamp(ts) -> str:
    """Return a human-readable UTC timestamp from a Unix epoch or ISO 8601 string."""
    try:
        if isinstance(ts, (int, float)):
            dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
        else:
            dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return str(ts)


# ── HTTP session & retry logic ────────────────────────────────────────────────

def build_session(token: str, label: str = "migrate") -> requests.Session:
    """Create a reusable HTTP session with the API token pre-configured."""
    s = requests.Session()
    s.headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": f"runzero-migrate-scan-tasks/1.0 ({label})",
        }
    )
    return s


def request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, str]] = None,
    json_body: Optional[Dict] = None,
    files: Optional[Dict] = None,
    stream: bool = False,
    allow_redirects: bool = True,
) -> requests.Response:
    """Send an HTTP request with exponential-backoff retries on network errors and 429/5xx."""
    backoff = 1.0
    resp = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.request(
                method,
                url,
                params=params,
                json=json_body,
                files=files,
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
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                try:
                    time.sleep(float(retry_after))
                except ValueError:
                    time.sleep(backoff)
            else:
                time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue

        return resp

    return resp  # defensive: should not be reached


# ── config validation ─────────────────────────────────────────────────────────

def validate_config() -> bool:
    """Check that required config values have been filled in."""
    ok = True
    if not SOURCE_TOKEN or SOURCE_TOKEN == "PASTE_SOURCE_ORG_API_KEY_HERE":
        print(
            "ERROR: Set SOURCE_TOKEN at the top of the script or export RUNZERO_SOURCE_TOKEN=<key>.",
            file=sys.stderr,
        )
        ok = False
    if not TARGET_TOKEN or TARGET_TOKEN == "PASTE_TARGET_ORG_API_KEY_HERE":
        print(
            "ERROR: Set TARGET_TOKEN at the top of the script or export RUNZERO_TARGET_TOKEN=<key>.",
            file=sys.stderr,
        )
        ok = False
    if TASK_LIMIT < 1:
        print("ERROR: TASK_LIMIT must be at least 1.", file=sys.stderr)
        ok = False
    return ok


# ── source org: fetch scan tasks ──────────────────────────────────────────────

def fetch_scan_tasks(
    session: requests.Session,
    base_url: str,
    org_id: str,
    limit: int,
) -> List[Dict]:
    """Return the `limit` most-recent processed scan tasks from the source org."""
    url = f"{base_url}/org/tasks"
    params: Dict[str, str] = {"search": "type:scan", "status": "processed"}
    if org_id:
        params["_oid"] = org_id

    resp = request_with_retries(session, "GET", url, params=params)
    resp.raise_for_status()

    tasks = resp.json()
    if not isinstance(tasks, list):
        tasks = tasks.get("data") or tasks.get("results") or []

    # Sort by created_at descending (most recent first) and slice to limit
    def _sort_key(t: Dict):
        val = t.get("created_at") or t.get("updated_at") or 0
        return float(val) if isinstance(val, (int, float)) else 0.0

    tasks.sort(key=_sort_key, reverse=True)
    return tasks[:limit]


# ── source org: resolve download URL ─────────────────────────────────────────

def resolve_download_url(
    session: requests.Session,
    base_url: str,
    task_id: str,
    org_id: str,
) -> str:
    """
    Call /org/tasks/{id}/data and return the pre-signed download URL.

    The API may respond with:
      - HTTP 302/307: Location header contains the URL
      - HTTP 200 + JSON {"url": "..."}
    """
    url = f"{base_url}/org/tasks/{task_id}/data"
    params: Dict[str, str] = {}
    if org_id:
        params["_oid"] = org_id

    resp = request_with_retries(
        session, "GET", url, params=params, allow_redirects=False
    )

    # Redirect response
    if resp.status_code in (301, 302, 303, 307, 308):
        location = resp.headers.get("Location")
        if location:
            return location
        raise ValueError(f"Redirect response missing Location header for task {task_id}")

    # JSON envelope response
    if resp.status_code == 200:
        try:
            body = resp.json()
            if isinstance(body, dict) and body.get("url"):
                return body["url"]
        except Exception:
            pass
        raise ValueError(
            f"Unexpected 200 response for task {task_id} — could not extract download URL: {resp.text[:500]}"
        )

    raise ValueError(
        f"Unexpected HTTP {resp.status_code} from /org/tasks/{task_id}/data: {resp.text[:500]}"
    )


# ── download scan data ────────────────────────────────────────────────────────

def download_scan_data(task_id: str, download_url: str) -> Tuple[bytes, str]:
    """
    Download the gzipped scan data from a pre-signed URL.

    Returns (raw_bytes, filename). Does NOT use the authenticated session —
    sending a Bearer token to a pre-signed S3/GCS URL causes signature errors.
    """
    # Derive filename from the URL path, fall back to task ID
    path = urlparse(download_url).path
    filename = path.split("/")[-1] if path and "/" in path else ""
    if not filename or not filename.endswith(".gz"):
        filename = f"task_{task_id}.gz"

    resp = requests.get(download_url, stream=True, timeout=TIMEOUT_SECONDS)
    resp.raise_for_status()

    data = resp.content
    return data, filename


# ── save scan file locally ────────────────────────────────────────────────────

def save_scan_file(task_id: str, data: bytes, filename: str) -> str:
    """Save the scan data bytes to scan_downloads/. Non-fatal on failure."""
    out_dir = "scan_downloads"
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, filename)
    with open(out_path, "wb") as f:
        f.write(data)
    return out_path


# ── target org: import scan data ─────────────────────────────────────────────

def import_scan_data(
    session: requests.Session,
    base_url: str,
    org_id: str,
    data: bytes,
    filename: str,
) -> requests.Response:
    """Upload the gzipped scan data to the target org via multipart form POST."""
    url = f"{base_url}/org/tasks/import"
    params: Dict[str, str] = {}
    if org_id:
        params["_oid"] = org_id

    files = {"file": (filename, data, "application/gzip")}
    return request_with_retries(session, "POST", url, params=params, files=files)


# ── display ───────────────────────────────────────────────────────────────────

def print_task_table(tasks: List[Dict]) -> None:
    """Print a formatted table of tasks."""
    display = tasks[:TABLE_MAX_ROWS]
    truncated = len(tasks) - len(display)

    rows = []
    for i, t in enumerate(display, 1):
        name      = str(t.get("name") or "(unnamed)")
        site      = str(t.get("site_name") or t.get("site_id") or "(unknown)")
        created   = format_timestamp(t.get("created_at") or t.get("updated_at") or "")
        task_id   = str(t.get("id") or "")[:8]  # show first 8 chars of UUID
        rows.append((str(i), name, site, created, task_id))

    w_num     = max(len("# "), max(len(r[0]) for r in rows), default=2)
    w_name    = min(COL_MAX_WIDTH, max(len("Task Name"),    max(len(r[1]) for r in rows), default=9))
    w_site    = min(COL_MAX_WIDTH, max(len("Site"),         max(len(r[2]) for r in rows), default=4))
    w_created = max(len("Created"),       max(len(r[3]) for r in rows), default=7)
    w_id      = max(len("Task ID"),       max(len(r[4]) for r in rows), default=7)

    sep = "  "
    header = (
        f"{'#':<{w_num}}{sep}"
        f"{'Task Name':<{w_name}}{sep}"
        f"{'Site':<{w_site}}{sep}"
        f"{'Created':<{w_created}}{sep}"
        f"{'Task ID':<{w_id}}"
    )
    divider = "-" * len(header)
    print(divider)
    print(header)
    print(divider)
    for num, name, site, created, task_id in rows:
        print(
            f"{num:<{w_num}}{sep}"
            f"{_trunc(name, w_name):<{w_name}}{sep}"
            f"{_trunc(site, w_site):<{w_site}}{sep}"
            f"{created:<{w_created}}{sep}"
            f"{task_id:<{w_id}}"
        )
    print(divider)
    if truncated:
        print(f"  ... and {truncated} more (showing first {TABLE_MAX_ROWS} of {len(tasks)})")


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    if not validate_config():
        return 2

    print("runZero Scan Task Migration")
    print("=" * 40)
    print(f"Source org  : {SOURCE_BASE_URL}")
    if SOURCE_ORG_ID:
        print(f"Source OID  : {SOURCE_ORG_ID}")
    print(f"Target org  : {TARGET_BASE_URL}")
    if TARGET_ORG_ID:
        print(f"Target OID  : {TARGET_ORG_ID}")
    print(f"Task limit  : {TASK_LIMIT}")
    print(f"Dry run     : {'Yes' if DRY_RUN else 'No'}")
    print(f"Save files  : {'Yes' if SAVE_SCAN_FILES else 'No'}")
    print()

    source_session = build_session(SOURCE_TOKEN, "source")
    target_session = build_session(TARGET_TOKEN, "target")

    print("Fetching scan tasks from source org...")
    try:
        tasks = fetch_scan_tasks(source_session, SOURCE_BASE_URL, SOURCE_ORG_ID, TASK_LIMIT)
    except KeyboardInterrupt:
        print("\nInterrupted during fetch. Nothing was imported.")
        return 130
    except Exception as e:
        print(f"ERROR: Failed to fetch tasks — {e}", file=sys.stderr)
        return 1

    if not tasks:
        print("No processed scan tasks found in source org.")
        return 0

    print(f"\nFound {len(tasks)} scan task(s):\n")
    print_task_table(tasks)
    print()

    if DRY_RUN:
        print("DRY RUN — no data will be imported. Set DRY_RUN = False to run.")
        return 0

    print(f"Proceed with importing {len(tasks)} task(s) into target org? [y/N]: ", end="", flush=True)
    try:
        answer = input().strip().lower()
    except KeyboardInterrupt:
        print("\nAborted. Nothing was imported.")
        return 130

    if answer != "y":
        print("Aborted. Nothing was imported.")
        return 0

    print()
    succeeded = 0
    failed = 0
    total = len(tasks)

    for i, task in enumerate(tasks, 1):
        task_id   = task.get("id", "")
        task_name = task.get("name") or "(unnamed)"
        print(f"[{i}/{total}] {task_name}  (id: {task_id[:8]}...)")

        try:
            print("  Resolving download URL...")
            download_url = resolve_download_url(
                source_session, SOURCE_BASE_URL, task_id, SOURCE_ORG_ID
            )

            print("  Downloading scan data...")
            data, filename = download_scan_data(task_id, download_url)
            print(f"  Downloaded {len(data) / 1024:.1f} KB  ({filename})")

            if SAVE_SCAN_FILES:
                try:
                    path = save_scan_file(task_id, data, filename)
                    print(f"  Saved to {path}")
                except Exception as save_err:
                    print(f"  Warning: could not save file locally — {save_err}")

            print("  Importing into target org...")
            resp = import_scan_data(target_session, TARGET_BASE_URL, TARGET_ORG_ID, data, filename)

            if resp.status_code in (200, 201, 204):
                succeeded += 1
                print("  [OK] Import successful")
            else:
                raise RuntimeError(
                    f"Import returned HTTP {resp.status_code}: {resp.text[:500]}"
                )

        except KeyboardInterrupt:
            print(f"\nInterrupted. {succeeded} succeeded, {failed} failed before interruption.")
            return 130
        except Exception as e:
            failed += 1
            print(f"  [FAIL] {e}")
            continue

        print()

    print(f"Migration complete: {succeeded} succeeded, {failed} failed out of {total} task(s).")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
Deletes all runZero assets that match the inventory query:
hardware:cisco OR type:mobile

Safety checks:
- Only deletes asset IDs returned by the search query above.
- Also verifies each returned record has hardware containing "cisco" or type == Mobile.

You must set TOKEN (and optionally ORG_ID / BASE_URL) below.
"""

import csv
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime
from typing import Dict, Iterable, List, Optional

import requests


# ----------------------------
# CONFIG (EDIT THESE)
# ----------------------------
BASE_URL = "https://console-eu.runzero.com/api/v1.0"  # change if self-hosted
TOKEN = "API KEY"  # Organisations > Settings > Generate Organisation API Key — or set RUNZERO_TOKEN env var
ORG_ID = ""  # optional: org UUID (only needed for account-scoped tokens, leave blank if using an ORGANISATION TOKEN)

# Strip any accidental trailing slash so URL construction is always clean
BASE_URL = BASE_URL.rstrip("/")


# Delete assets matching this query
DELETE_QUERY = "hardware:cisco OR type:mobile"

# Extra safety check: only delete assets whose type is exactly one of these
ALLOWED_TYPES = {"mobile"}

# Extra safety check: only delete assets whose hardware contains one of these substrings
ALLOWED_HARDWARE_SUBSTRINGS = {"cisco"}

# No OS-based matching needed for this query
ALLOWED_OS_SUBSTRINGS: set = set()

BATCH_SIZE = 500
TIMEOUT_SECONDS = 60
MAX_RETRIES = 6
TABLE_MAX_ROWS = 50   # max rows shown in the dry-run table before truncating
COL_MAX_WIDTH  = 40   # max characters per column value before truncating with …


# Shortens a string to fit within a column, adding … if truncated
def _trunc(s: str, width: int) -> str:
    return s if len(s) <= width else s[: width - 1] + "\u2026"


# Creates a reusable HTTP session with the API token set in the headers
def build_session(token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "runzero-delete-assets/1.1",
        }
    )
    return s


# Sends an HTTP request and automatically retries on network errors or
# server-side failures (429 rate limit, 5xx errors) with exponential backoff
def request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, str]] = None,
    json_body: Optional[Dict] = None,
    stream: bool = False,
) -> requests.Response:
    backoff = 1.0
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.request(
                method,
                url,
                params=params,
                json=json_body,
                stream=stream,
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
            # Respect the server's Retry-After header if present, otherwise use backoff
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

    return resp  # defensive


# Parses the streaming JSONL response line by line, yielding each asset as a dict
def iter_assets_jsonl(resp: requests.Response) -> Iterable[Dict]:
    resp.raise_for_status()
    for line in resp.iter_lines(decode_unicode=True):
        if not line:
            continue
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            continue


# Queries the runZero export API using DELETE_QUERY, then applies a secondary
# safety check to ensure only assets matching ALLOWED_TYPES (or ALLOWED_OS_SUBSTRINGS)
# are included. Returns a list of asset dicts ready for display and deletion.
def fetch_deletable_assets(session: requests.Session) -> List[Dict]:
    url = f"{BASE_URL}/export/org/assets.jsonl"
    params: Dict[str, str] = {"search": DELETE_QUERY, "fields": "id,type,os,hw,names,addresses"}
    if ORG_ID:
        params["_oid"] = ORG_ID

    resp = request_with_retries(session, "GET", url, params=params, stream=True)

    assets: List[Dict] = []
    scanned = 0
    for obj in iter_assets_jsonl(resp):
        asset_id = obj.get("id")
        if not asset_id:
            continue

        asset_type = (obj.get("type") or "").strip().lower()
        asset_os = (obj.get("os") or "").strip().lower()
        asset_hw = (obj.get("hw") or "").strip().lower()

        # Safety check: confirm the asset matches our allowed criteria before queuing for deletion
        type_ok = asset_type in ALLOWED_TYPES
        os_ok = any(s in asset_os for s in ALLOWED_OS_SUBSTRINGS)
        hw_ok = any(s in asset_hw for s in ALLOWED_HARDWARE_SUBSTRINGS)

        if type_ok or os_ok or hw_ok:
            raw_names = obj.get("names")
            raw_addrs = obj.get("addresses")
            name    = (raw_names[0] if isinstance(raw_names, list) else raw_names) or "(unknown)"
            address = (raw_addrs[0] if isinstance(raw_addrs, list) else raw_addrs) or "(unknown)"
            assets.append({
                "id":      asset_id,
                "type":    obj.get("type") or "",
                "os":      obj.get("os") or "",
                "hw":      obj.get("hw") or "",
                "name":    name,
                "address": address,
            })
            scanned += 1
            print(f"\r  Found {scanned} matching asset(s)...", end="", flush=True)

    if scanned:
        print()  # newline after the \r counter
    return assets


# Splits a list into smaller batches of a given size for bulk API calls
def chunked(seq: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


# Sends a bulk delete request to the runZero API for a batch of asset IDs
def bulk_delete_assets(session: requests.Session, asset_ids: List[str]) -> requests.Response:
    url = f"{BASE_URL}/org/assets/bulk/delete"
    params: Dict[str, str] = {}
    if ORG_ID:
        params["_oid"] = ORG_ID
    body = {"asset_ids": asset_ids}
    return request_with_retries(session, "POST", url, params=params, json_body=body, stream=False)


# Prints a formatted table of assets that would be deleted, capped at TABLE_MAX_ROWS
def print_dry_run_table(assets: List[Dict]) -> None:
    display = assets[:TABLE_MAX_ROWS]
    truncated = len(assets) - len(display)

    # Calculate column widths based on content, capped at COL_MAX_WIDTH
    def w(key: str) -> int:
        header_len = len(key.title())
        return min(COL_MAX_WIDTH, max(header_len, max((len(a[key]) for a in display), default=0)))

    col_widths = {
        "name":    w("name"),
        "address": w("address"),
        "type":    w("type"),
        "os":      w("os"),
        "hw":      w("hw"),
    }
    sep = "  "
    header = (
        f"{'Name':<{col_widths['name']}}{sep}"
        f"{'Address':<{col_widths['address']}}{sep}"
        f"{'Type':<{col_widths['type']}}{sep}"
        f"{'OS':<{col_widths['os']}}{sep}"
        f"{'HW':<{col_widths['hw']}}"
    )
    divider = "-" * len(header)
    print(divider)
    print(header)
    print(divider)
    for a in display:
        print(
            f"{_trunc(a['name'],    col_widths['name'])   :<{col_widths['name']}}{sep}"
            f"{_trunc(a['address'], col_widths['address']):<{col_widths['address']}}{sep}"
            f"{_trunc(a['type'],    col_widths['type'])   :<{col_widths['type']}}{sep}"
            f"{_trunc(a['os'],      col_widths['os'])     :<{col_widths['os']}}{sep}"
            f"{_trunc(a['hw'],      col_widths['hw'])     :<{col_widths['hw']}}"
        )
    print(divider)
    if truncated:
        print(f"  ... and {truncated} more (showing first {TABLE_MAX_ROWS} of {len(assets)})")


# Writes a CSV audit log of deleted assets with a timestamp in the filename
def write_audit_log(assets: List[Dict]) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"runzero_deleted_{timestamp}.csv"
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "address", "type", "os", "hw"])
        writer.writeheader()
        writer.writerows(assets)
    return filename


# Prints a count of assets grouped by type/OS category
def print_type_summary(assets: List[Dict]) -> None:
    counts: Counter = Counter()
    for a in assets:
        t = a["type"].lower()
        if t in ALLOWED_TYPES:
            counts[a["type"] or "Unknown"] += 1
        elif any(s in a["hw"].lower() for s in ALLOWED_HARDWARE_SUBSTRINGS):
            counts[f"HW:Cisco"] += 1
        else:
            for sub in ALLOWED_OS_SUBSTRINGS:
                if sub in a["os"].lower():
                    counts[f"OS:{sub.title()}"] += 1
                    break
    parts = ", ".join(f"{k}: {v}" for k, v in sorted(counts.items()))
    print(f"  Breakdown — {parts}")


def main() -> int:
    # Prefer the environment variable so the token doesn't need to be hardcoded in the script
    token = os.environ.get("RUNZERO_TOKEN") or TOKEN
    if not token or token == "API KEY":
        print(
            "ERROR: Set TOKEN at the top of the script or export RUNZERO_TOKEN=<your token>.",
            file=sys.stderr,
        )
        return 2

    session = build_session(token)

    print(f"Base URL: {BASE_URL}")
    if ORG_ID:
        print(f"Org ID (_oid): {ORG_ID}")
    print(f"Query: {DELETE_QUERY}")

    # Fetch all assets matching the query (read-only, nothing is deleted yet)
    print("\nFetching matching assets (dry run)...")
    try:
        assets = fetch_deletable_assets(session)
    except KeyboardInterrupt:
        print("\nInterrupted during fetch. No assets were deleted.")
        return 130

    if not assets:
        print("No matching assets found. Nothing to delete.")
        return 0

    # Show the user exactly what would be deleted and ask for confirmation
    print(f"\nDRY RUN — {len(assets)} asset(s) would be deleted:\n")
    print_dry_run_table(assets)
    print()
    print_type_summary(assets)

    print(f"\nProceed with deleting all {len(assets)} asset(s)? [y/N]: ", end="", flush=True)
    try:
        answer = input().strip().lower()
    except KeyboardInterrupt:
        print("\nAborted. No assets were deleted.")
        return 130

    if answer != "y":
        print("Aborted. No assets were deleted.")
        return 0

    # Delete assets in batches and report progress
    ids = [a["id"] for a in assets]
    deleted = 0
    total = len(ids)

    try:
        for batch in chunked(ids, BATCH_SIZE):
            resp = bulk_delete_assets(session, batch)

            if resp.status_code == 204:
                deleted += len(batch)
                print(f"  Deleted {deleted}/{total}")
                continue

            try:
                err = resp.json()
            except Exception:
                err = {"status_code": resp.status_code, "text": (resp.text or "")[:800]}

            print(f"ERROR: bulk delete failed (HTTP {resp.status_code}): {err}", file=sys.stderr)
            return 1
    except KeyboardInterrupt:
        print(f"\nInterrupted. {deleted}/{total} asset(s) deleted before interruption.")
        return 130

    # Write an audit log CSV of everything that was deleted
    log_file = write_audit_log(assets)
    print(f"\nDone. {deleted} asset(s) deleted.")
    print(f"Audit log written to: {log_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
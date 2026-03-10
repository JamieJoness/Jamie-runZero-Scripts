#!/usr/bin/env python3
"""
Deletes all runZero assets that match the inventory query:
(type:phone OR type:mobile OR type:printer OR os:Neat OR os:Aruba)

Safety checks:
- Only deletes asset IDs returned by the search query above.
- Also verifies each returned record is one of:
  - type == Mobile, Phone, or Printer (so IP Phone will NOT be deleted)
  - OR os contains "Neat" or "Aruba" (case-insensitive)

You must set TOKEN (and optionally ORG_ID / BASE_URL) below.
"""

import json
import sys
import time
from typing import Dict, Iterable, List, Optional

import requests


# ----------------------------
# CONFIG (EDIT THESE)
# ----------------------------
BASE_URL = "https://console-eu.runzero.com/api/v1.0"  # change if self-hosted
TOKEN = "API KEY"  # Organisations > Settings > Generate Organisation API Key
ORG_ID = ""  # optional: org UUID (only needed for account-scoped tokens, leave blank if using an ORGANISATION TOKEN)


# Delete assets matching this query
DELETE_QUERY = "(type:phone OR type:mobile OR type:printer OR os:Neat OR os:Aruba)"

# Extra safety checks on returned records:
# - Types must be exactly these (prevents deleting "IP Phone")
ALLOWED_TYPES = {"mobile", "phone", "printer"}

# - Or OS must contain one of these strings (case-insensitive)
ALLOWED_OS_SUBSTRINGS = {"neat", "aruba"}

BATCH_SIZE = 500
TIMEOUT_SECONDS = 60
MAX_RETRIES = 6
TABLE_MAX_ROWS = 50   # max rows shown in the dry-run table before truncating
COL_MAX_WIDTH  = 40   # max characters per column value before truncating with …


def _trunc(s: str, width: int) -> str:
    return s if len(s) <= width else s[: width - 1] + "\u2026"


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


def fetch_deletable_assets(session: requests.Session) -> List[Dict]:
    url = f"{BASE_URL}/export/org/assets.jsonl"
    params: Dict[str, str] = {"search": DELETE_QUERY, "fields": "id,type,os,names,addresses"}
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

        type_ok = asset_type in ALLOWED_TYPES
        os_ok = any(s in asset_os for s in ALLOWED_OS_SUBSTRINGS)

        if type_ok or os_ok:
            raw_names = obj.get("names")
            raw_addrs = obj.get("addresses")
            name    = (raw_names[0] if isinstance(raw_names, list) else raw_names) or "(unknown)"
            address = (raw_addrs[0] if isinstance(raw_addrs, list) else raw_addrs) or "(unknown)"
            assets.append({
                "id":      asset_id,
                "type":    obj.get("type") or "",
                "os":      obj.get("os") or "",
                "name":    name,
                "address": address,
            })
            scanned += 1
            print(f"\r  Found {scanned} matching asset(s)...", end="", flush=True)

    if scanned:
        print()  # newline after the \r counter
    return assets


def chunked(seq: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def bulk_delete_assets(session: requests.Session, asset_ids: List[str]) -> requests.Response:
    url = f"{BASE_URL}/org/assets/bulk/delete"
    params: Dict[str, str] = {}
    if ORG_ID:
        params["_oid"] = ORG_ID
    body = {"asset_ids": asset_ids}
    return request_with_retries(session, "POST", url, params=params, json_body=body, stream=False)


def print_dry_run_table(assets: List[Dict]) -> None:
    display = assets[:TABLE_MAX_ROWS]
    truncated = len(assets) - len(display)

    def w(key: str) -> int:
        header_len = len(key.title())
        return min(COL_MAX_WIDTH, max(header_len, max((len(a[key]) for a in display), default=0)))

    col_widths = {
        "name":    w("name"),
        "address": w("address"),
        "type":    w("type"),
        "os":      w("os"),
    }
    sep = "  "
    header = (
        f"{'Name':<{col_widths['name']}}{sep}"
        f"{'Address':<{col_widths['address']}}{sep}"
        f"{'Type':<{col_widths['type']}}{sep}"
        f"{'OS':<{col_widths['os']}}"
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
            f"{_trunc(a['os'],      col_widths['os'])     :<{col_widths['os']}}"
        )
    print(divider)
    if truncated:
        print(f"  ... and {truncated} more (showing first {TABLE_MAX_ROWS} of {len(assets)})")


def print_type_summary(assets: List[Dict]) -> None:
    from collections import Counter
    counts: Counter = Counter()
    for a in assets:
        t = a["type"].lower()
        if t in ALLOWED_TYPES:
            counts[a["type"] or "Unknown"] += 1
        else:
            for sub in ALLOWED_OS_SUBSTRINGS:
                if sub in a["os"].lower():
                    counts[f"OS:{sub.title()}"] += 1
                    break
    parts = ", ".join(f"{k}: {v}" for k, v in sorted(counts.items()))
    print(f"  Breakdown — {parts}")


def main() -> int:
    if not TOKEN or TOKEN == "API KEY":
        print("ERROR: Set TOKEN at the top of the script before running.", file=sys.stderr)
        return 2

    session = build_session(TOKEN)

    print(f"Base URL: {BASE_URL}")
    if ORG_ID:
        print(f"Org ID (_oid): {ORG_ID}")
    print(f"Query: {DELETE_QUERY}")

    print("\nFetching matching assets (dry run)...")
    try:
        assets = fetch_deletable_assets(session)
    except KeyboardInterrupt:
        print("\nInterrupted during fetch. No assets were deleted.")
        return 130

    if not assets:
        print("No matching assets found. Nothing to delete.")
        return 0

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

    print(f"\nDone. {deleted} asset(s) deleted.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
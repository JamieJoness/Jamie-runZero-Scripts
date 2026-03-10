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


def fetch_deletable_asset_ids(session: requests.Session) -> List[str]:
    url = f"{BASE_URL}/export/org/assets.jsonl"
    params: Dict[str, str] = {"search": DELETE_QUERY, "fields": "id,type,os"}
    if ORG_ID:
        params["_oid"] = ORG_ID

    resp = request_with_retries(session, "GET", url, params=params, stream=True)

    ids: List[str] = []
    for obj in iter_assets_jsonl(resp):
        asset_id = obj.get("id")
        if not asset_id:
            continue

        asset_type = (obj.get("type") or "").strip().lower()
        asset_os = (obj.get("os") or "").strip().lower()

        type_ok = asset_type in ALLOWED_TYPES
        os_ok = any(s in asset_os for s in ALLOWED_OS_SUBSTRINGS)

        if type_ok or os_ok:
            ids.append(asset_id)

    return ids


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


def main() -> int:
    if not TOKEN or TOKEN == "PASTE_YOUR_API_TOKEN_HERE":
        print("ERROR: Set TOKEN at the top of the script before running.", file=sys.stderr)
        return 2

    session = build_session(TOKEN)

    print(f"Base URL: {BASE_URL}")
    if ORG_ID:
        print(f"Org ID (_oid): {ORG_ID}")
    print(f"Deleting assets matching query: {DELETE_QUERY}")

    print("Fetching matching assets via export JSONL...")
    ids = fetch_deletable_asset_ids(session)

    if not ids:
        print("No matching assets found. Nothing to delete.")
        return 0

    print(f"Found {len(ids)} matching assets. Starting deletion...")

    deleted = 0
    total = len(ids)

    for batch in chunked(ids, BATCH_SIZE):
        resp = bulk_delete_assets(session, batch)

        if resp.status_code == 204:
            deleted += len(batch)
            print(f"Deleted {deleted}/{total}")
            continue

        try:
            err = resp.json()
        except Exception:
            err = {"status_code": resp.status_code, "text": (resp.text or "")[:800]}

        print(f"ERROR: bulk delete failed (HTTP {resp.status_code}): {err}", file=sys.stderr)
        return 1

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
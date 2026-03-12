#!/usr/bin/env python3
"""
Exports all asset IP addresses from runZero and imports them into
Tenable Security Center as a static asset list called "runZero Assets".

Steps:
  1. Fetches all assets from runZero via the JSONL export API.
  2. Collects all unique IP addresses across those assets.
  3. Checks whether the "runZero Assets" list already exists in Tenable SC.
  4. Creates the list if it does not exist; replaces the IPs if it does.

Required config (set below or via environment variables):
  RUNZERO_TOKEN         – runZero organisation API key
  TENABLE_SC_HOST       – hostname or IP of the Tenable Security Center appliance
  TENABLE_SC_ACCESS_KEY – Tenable SC API access key
  TENABLE_SC_SECRET_KEY – Tenable SC API secret key
"""

import json
import os
import sys
import time
import warnings
from typing import Dict, Iterable, List, Optional, Set

import requests


# ──────────────────────────────────────────────
# CONFIG (EDIT THESE or set environment variables)
# ──────────────────────────────────────────────

# runZero settings
RUNZERO_BASE_URL = "https://console.runzero.com/api/v1.0"  # change to console-eu.runzero.com or self-hosted if needed
RUNZERO_TOKEN    = "PASTE_RUNZERO_ORG_API_KEY_HERE"        # Organisations > Settings > Generate Organisation API Key
RUNZERO_ORG_ID   = ""                                       # optional: leave blank for org-scoped tokens
RUNZERO_QUERY    = ""                                       # optional search filter; leave blank to export all assets

# Tenable Security Center settings
TENABLE_SC_HOST       = "PASTE_TENABLE_SC_HOSTNAME_HERE"   # e.g. "tenable-sc.example.com" (hostname only, no https://)
TENABLE_SC_ACCESS_KEY = "PASTE_TENABLE_SC_ACCESS_KEY_HERE" # Administration > Users > API Keys
TENABLE_SC_SECRET_KEY = "PASTE_TENABLE_SC_SECRET_KEY_HERE"
TENABLE_SC_VERIFY_TLS = True   # set to False only if your SC uses a self-signed certificate

# Name of the asset list to create / update in Tenable SC
ASSET_LIST_NAME = "runZero Assets"

# Override any of the above with environment variables
RUNZERO_BASE_URL      = os.environ.get("RUNZERO_BASE_URL",      RUNZERO_BASE_URL).rstrip("/")
RUNZERO_TOKEN         = os.environ.get("RUNZERO_TOKEN",         RUNZERO_TOKEN)
RUNZERO_ORG_ID        = os.environ.get("RUNZERO_ORG_ID",        RUNZERO_ORG_ID)
RUNZERO_QUERY         = os.environ.get("RUNZERO_QUERY",         RUNZERO_QUERY)
TENABLE_SC_HOST       = os.environ.get("TENABLE_SC_HOST",       TENABLE_SC_HOST)
TENABLE_SC_ACCESS_KEY = os.environ.get("TENABLE_SC_ACCESS_KEY", TENABLE_SC_ACCESS_KEY)
TENABLE_SC_SECRET_KEY = os.environ.get("TENABLE_SC_SECRET_KEY", TENABLE_SC_SECRET_KEY)
TENABLE_SC_VERIFY_TLS = os.environ.get("TENABLE_SC_VERIFY_TLS", str(TENABLE_SC_VERIFY_TLS)).lower() not in ("false", "0", "no")

TIMEOUT_SECONDS = 60
MAX_RETRIES     = 6


# ──────────────────────────────────────────────
# HTTP HELPERS
# ──────────────────────────────────────────────

def build_runzero_session(token: str) -> requests.Session:
    """Create a reusable HTTP session authenticated with the runZero API token."""
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": "runzero-export-to-tenable/1.0",
    })
    return s


def build_tenable_session(access_key: str, secret_key: str, verify_tls: bool) -> requests.Session:
    """Create a reusable HTTP session authenticated with Tenable SC API keys."""
    if not verify_tls:
        warnings.filterwarnings("ignore", message="Unverified HTTPS request")
    s = requests.Session()
    s.headers.update({
        "X-APIKey": f"accessKey={access_key}; secretKey={secret_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "runzero-export-to-tenable/1.0",
    })
    s.verify = verify_tls
    return s


def request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: Optional[Dict] = None,
    json_body: Optional[Dict] = None,
    stream: bool = False,
) -> requests.Response:
    """Send an HTTP request with exponential-backoff retries on network errors and 429/5xx."""
    backoff = 1.0
    resp = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.request(
                method, url,
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
            try:
                time.sleep(float(retry_after) if retry_after else backoff)
            except (ValueError, TypeError):
                time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue

        return resp

    return resp  # defensive


# ──────────────────────────────────────────────
# RUNZERO: FETCH ASSET IPs
# ──────────────────────────────────────────────

def iter_assets_jsonl(resp: requests.Response) -> Iterable[Dict]:
    """Parse a streaming JSONL response, yielding each asset as a dict."""
    resp.raise_for_status()
    for line in resp.iter_lines(decode_unicode=True):
        line = (line or "").strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            continue


def fetch_runzero_ips(session: requests.Session) -> Set[str]:
    """Fetch all assets from runZero and return the set of unique IP addresses."""
    url = f"{RUNZERO_BASE_URL}/export/org/assets.jsonl"
    params: Dict = {"fields": "id,addresses"}
    if RUNZERO_QUERY:
        params["search"] = RUNZERO_QUERY
    if RUNZERO_ORG_ID:
        params["_oid"] = RUNZERO_ORG_ID

    resp = request_with_retries(session, "GET", url, params=params, stream=True)

    ip_set: Set[str] = set()
    asset_count = 0

    for asset in iter_assets_jsonl(resp):
        addresses = asset.get("addresses") or []
        if isinstance(addresses, str):
            addresses = [a.strip() for a in addresses.replace(",", "\n").splitlines()]
        for addr in addresses:
            addr = addr.strip()
            if addr:
                ip_set.add(addr)
        asset_count += 1
        if asset_count % 500 == 0:
            print(f"\r  Processed {asset_count} assets, {len(ip_set)} unique IPs...", end="", flush=True)

    if asset_count >= 500:
        print()  # newline after the \r counter

    return ip_set


# ──────────────────────────────────────────────
# TENABLE SC: ASSET LIST MANAGEMENT
# ──────────────────────────────────────────────

def find_asset_list(session: requests.Session, sc_base: str, name: str) -> Optional[Dict]:
    """Return the asset list dict if a list with the given name exists in Tenable SC, else None."""
    url = f"{sc_base}/rest/asset"
    params = {"fields": "id,name,type"}
    resp = request_with_retries(session, "GET", url, params=params)
    resp.raise_for_status()

    body = resp.json()
    # Tenable SC wraps list responses under response.usable / response.manageable
    items: List[Dict] = []
    response = body.get("response")
    if isinstance(response, dict):
        items = response.get("usable", []) + response.get("manageable", [])
    elif isinstance(response, list):
        items = response

    for item in items:
        if item.get("name") == name:
            return item
    return None


def create_asset_list(session: requests.Session, sc_base: str, name: str, ips: Set[str]) -> Dict:
    """Create a new static asset list in Tenable SC and return the response body."""
    url = f"{sc_base}/rest/asset"
    body = {
        "name": name,
        "description": "Automatically synced from runZero asset inventory.",
        "type": "static",
        "typeFields": {
            "definedIPs": ",".join(sorted(ips)),
        },
    }
    resp = request_with_retries(session, "POST", url, json_body=body)
    resp.raise_for_status()
    return resp.json()


def update_asset_list(session: requests.Session, sc_base: str, asset_id: str, ips: Set[str]) -> Dict:
    """Replace the IPs in an existing static asset list in Tenable SC and return the response body."""
    url = f"{sc_base}/rest/asset/{asset_id}"
    body = {
        "type": "static",
        "typeFields": {
            "definedIPs": ",".join(sorted(ips)),
        },
    }
    resp = request_with_retries(session, "PATCH", url, json_body=body)
    resp.raise_for_status()
    return resp.json()


# ──────────────────────────────────────────────
# CONFIG VALIDATION
# ──────────────────────────────────────────────

def validate_config() -> bool:
    ok = True
    if not RUNZERO_TOKEN or RUNZERO_TOKEN == "PASTE_RUNZERO_ORG_API_KEY_HERE":
        print(
            "ERROR: Set RUNZERO_TOKEN at the top of the script or export RUNZERO_TOKEN=<key>.",
            file=sys.stderr,
        )
        ok = False
    if not TENABLE_SC_HOST or TENABLE_SC_HOST == "PASTE_TENABLE_SC_HOSTNAME_HERE":
        print(
            "ERROR: Set TENABLE_SC_HOST at the top of the script or export TENABLE_SC_HOST=<host>.",
            file=sys.stderr,
        )
        ok = False
    if not TENABLE_SC_ACCESS_KEY or TENABLE_SC_ACCESS_KEY == "PASTE_TENABLE_SC_ACCESS_KEY_HERE":
        print(
            "ERROR: Set TENABLE_SC_ACCESS_KEY at the top of the script or export TENABLE_SC_ACCESS_KEY=<key>.",
            file=sys.stderr,
        )
        ok = False
    if not TENABLE_SC_SECRET_KEY or TENABLE_SC_SECRET_KEY == "PASTE_TENABLE_SC_SECRET_KEY_HERE":
        print(
            "ERROR: Set TENABLE_SC_SECRET_KEY at the top of the script or export TENABLE_SC_SECRET_KEY=<key>.",
            file=sys.stderr,
        )
        ok = False
    return ok


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

def main() -> int:
    if not validate_config():
        return 2

    sc_base = f"https://{TENABLE_SC_HOST}"

    print("runZero → Tenable Security Center Asset Sync")
    print("=" * 44)
    print(f"runZero base URL : {RUNZERO_BASE_URL}")
    if RUNZERO_ORG_ID:
        print(f"runZero org ID   : {RUNZERO_ORG_ID}")
    print(f"runZero query    : {RUNZERO_QUERY or '(all assets)'}")
    print(f"Tenable SC host  : {TENABLE_SC_HOST}")
    print(f"Asset list name  : {ASSET_LIST_NAME}")
    print(f"TLS verification : {'enabled' if TENABLE_SC_VERIFY_TLS else 'DISABLED'}")
    print()

    # ── Step 1: Fetch all IPs from runZero ──────────────────────────────────
    print("Step 1: Fetching assets from runZero...")
    rz_session = build_runzero_session(RUNZERO_TOKEN)
    try:
        ip_set = fetch_runzero_ips(rz_session)
    except KeyboardInterrupt:
        print("\nInterrupted during runZero fetch. Nothing was imported.")
        return 130
    except Exception as e:
        print(f"ERROR: Failed to fetch runZero assets — {e}", file=sys.stderr)
        return 1

    if not ip_set:
        print("No IP addresses found in runZero. Nothing to import.")
        return 0

    print(f"  Found {len(ip_set)} unique IP address(es) in the runZero inventory.")
    print()

    # ── Step 2: Look up the asset list in Tenable SC ────────────────────────
    print("Step 2: Connecting to Tenable Security Center...")
    sc_session = build_tenable_session(TENABLE_SC_ACCESS_KEY, TENABLE_SC_SECRET_KEY, TENABLE_SC_VERIFY_TLS)

    try:
        existing = find_asset_list(sc_session, sc_base, ASSET_LIST_NAME)
    except KeyboardInterrupt:
        print("\nInterrupted. No changes were made to Tenable SC.")
        return 130
    except Exception as e:
        print(f"ERROR: Failed to query Tenable SC asset lists — {e}", file=sys.stderr)
        return 1

    if existing:
        asset_id = str(existing.get("id", ""))
        print(f"  Found existing asset list \"{ASSET_LIST_NAME}\" (id: {asset_id}). Will update.")
        action = "update"
    else:
        print(f"  Asset list \"{ASSET_LIST_NAME}\" does not exist. Will create it.")
        asset_id = ""
        action = "create"
    print()

    # ── Step 3: Confirm and push ─────────────────────────────────────────────
    print(f"Ready to {action} \"{ASSET_LIST_NAME}\" with {len(ip_set)} IP address(es).")
    print(f"Proceed? [y/N]: ", end="", flush=True)
    try:
        answer = input().strip().lower()
    except KeyboardInterrupt:
        print("\nAborted. No changes were made.")
        return 130

    if answer != "y":
        print("Aborted. No changes were made.")
        return 0

    print()
    try:
        if existing:
            print(f"Updating asset list (id: {asset_id})...")
            result = update_asset_list(sc_session, sc_base, asset_id, ip_set)
        else:
            print("Creating asset list...")
            result = create_asset_list(sc_session, sc_base, ASSET_LIST_NAME, ip_set)
    except KeyboardInterrupt:
        print(f"\nInterrupted during Tenable SC {action}.")
        return 130
    except Exception as e:
        print(f"ERROR: Failed to {action} asset list in Tenable SC — {e}", file=sys.stderr)
        return 1

    # Extract resulting ID from the response envelope for confirmation
    resp_id = None
    response = result.get("response")
    if isinstance(response, dict):
        resp_id = response.get("id")

    id_str = f" (id: {resp_id})" if resp_id else ""
    past = "updated" if existing else "created"
    print(f"Done. Asset list \"{ASSET_LIST_NAME}\" {past} successfully{id_str}.")
    print(f"  {len(ip_set)} IP address(es) are now in the list.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
 Vulnerability Remediation Priority Tagger
--------------------------------------------------
Pulls asset and vulnerability data from runZero, applies the
vulnerability priority matrix (Tier + CVSS Severity + KEV status),
and writes a remediation_priority tag (P1-P5) back to each asset.

Requirements:
    pip install requests

Usage:
    1. Set your token below (static) or via the RUNZERO_ORG_TOKEN env var
    2. Run: python3 _priority_tagger.py
    3. Schedule via cron to run after your daily Tenable SC sync
"""

import os
import sys
import requests
from collections import defaultdict

# ---------------------------------------------------------------------------
# Configuration - set your token here OR use the RUNZERO_ORG_TOKEN env var.
# The env var takes priority if both are set.
# ---------------------------------------------------------------------------
STATIC_ORG_TOKEN = ""  # Paste your org API token here if you prefer not to use env vars
STATIC_BASE_URL = ""   # Leave blank to default to https://console.runzero.com

BASE_URL = os.environ.get("RUNZERO_BASE_URL") or STATIC_BASE_URL or "https://console.runzero.com"
ORG_TOKEN = os.environ.get("RUNZERO_ORG_TOKEN") or STATIC_ORG_TOKEN

# Tag key used to identify vulnerability tier on assets (e.g. vuln_tier=1)
TIER_TAG_KEY = "vuln_tier"

# How many asset IDs to include per bulk tag API call
TAG_BATCH_SIZE = 50

# ---------------------------------------------------------------------------
# Priority lookup table
# ---------------------------------------------------------------------------
# Keyed on (tier, has_kev) -> priority string.
# Only applies to vulns with severity critical/high/medium.
# The catch-all (any tier, no KEV, any severity) maps to P5
# and is handled separately in determine_priority().

PRIORITY_LOOKUP = {
    (1, True):  "P1",
    (2, True):  "P2",
    (1, False): "P2",
    (3, True):  "P3",
    (2, False): "P3",
    (4, True):  "P4",
}

# Severities that qualify for the lookup table above
ACTIONABLE_SEVERITIES = frozenset({"critical", "high", "medium"})

# Valid tiers
VALID_TIERS = frozenset({1, 2, 3, 4})

# Priority rank for comparison (lower = more urgent)
PRIORITY_RANK = {"P1": 1, "P2": 2, "P3": 3, "P4": 4, "P5": 5}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def check_config():
    """Validate that a token is available."""
    if not ORG_TOKEN:
        print("ERROR: No API token found.")
        print("Either set RUNZERO_ORG_TOKEN as an env var:")
        print("  export RUNZERO_ORG_TOKEN='your-org-token-here'")
        print("Or paste it into STATIC_ORG_TOKEN at the top of this script.")
        sys.exit(1)


def paginated_export(session, endpoint, params=None):
    """
    Generic paginated GET against the runZero export API.
    Yields each item from every page.
    """
    url = f"{BASE_URL}/api/v1.0/export/org/{endpoint}"
    params = dict(params or {})

    while True:
        resp = session.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

        # API may return a flat list or a paginated dict
        if isinstance(data, list):
            yield from data
            return

        # Paginated response: find the items array
        items = []
        for key in ("assets", "vulnerabilities", "items"):
            if key in data and isinstance(data[key], list):
                items = data[key]
                break

        yield from items

        next_key = data.get("next_key")
        if not next_key:
            return
        params["start_key"] = next_key


def get_asset_tier(asset):
    """Extract the vulnerability tier number from asset tags."""
    tags = asset.get("tags", {})

    if isinstance(tags, dict):
        val = tags.get(TIER_TAG_KEY)
        if val is not None:
            try:
                return int(val)
            except (ValueError, TypeError):
                return None

    elif isinstance(tags, list):
        prefix = f"{TIER_TAG_KEY}="
        for tag in tags:
            if isinstance(tag, str) and tag.startswith(prefix):
                try:
                    return int(tag[len(prefix):])
                except (ValueError, TypeError):
                    return None

    return None


def classify_severity(vuln):
    """
    Determine severity from vulnerability data.
    Uses the severity string first, falls back to CVSS score thresholds.
    """
    severity = (vuln.get("severity") or "").lower().strip()
    if severity in ("critical", "high", "medium", "low", "info"):
        return severity

    # Fallback: derive from best available CVSS score
    for field in ("cvss3_base_score", "cvss2_base_score", "severity_score"):
        val = vuln.get(field)
        if val is not None:
            try:
                score = float(val)
            except (ValueError, TypeError):
                continue
            if score >= 9.0:
                return "critical"
            if score >= 7.0:
                return "high"
            if score >= 4.0:
                return "medium"
            if score > 0:
                return "low"
            return "info"

    return "info"


def check_kev(vuln):
    """Check if a vulnerability appears in any KEV catalogue."""
    kev_val = vuln.get("kev")
    if isinstance(kev_val, bool):
        return kev_val
    if isinstance(kev_val, str) and kev_val.lower() in ("true", "t", "yes", "1", "cisa", "vulncheck"):
        return True

    for field in ("kev_cisa", "kev_vulncheck"):
        val = vuln.get(field)
        if val and str(val).lower() in ("true", "t", "yes", "1"):
            return True

    return False


def determine_priority(tier, severity, has_kev):
    """
    Look up the priority for a given tier + severity + KEV combination.
    Returns a priority string (P1-P5) or None if no match.
    """
    if severity in ACTIONABLE_SEVERITIES:
        result = PRIORITY_LOOKUP.get((tier, has_kev))
        if result:
            return result

    # Catch-all: any tier, any severity (incl low), no KEV = P5
    if not has_kev:
        return "P5"

    return None


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print(" Vulnerability Remediation Priority Tagger")
    print("=" * 60)
    print()

    check_config()

    # Reuse a single session for connection pooling
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {ORG_TOKEN}"})

    # Step 1: Pull assets that have a vulnerability tier tag
    print("[1/4] Exporting assets from runZero...")
    assets = list(paginated_export(
        session, "assets.json",
        params={"search": f"tag:{TIER_TAG_KEY}=*"}
    ))
    print(f"  Found {len(assets)} assets with a {TIER_TAG_KEY} tag.")

    if not assets:
        print("Nothing to do. Make sure assets have a vuln_tier tag applied.")
        sys.exit(0)

    # Build a tier lookup so we only parse tags once
    asset_tiers = {}
    for asset in assets:
        asset_id = asset.get("id")
        if not asset_id:
            continue
        tier = get_asset_tier(asset)
        if tier in VALID_TIERS:
            asset_tiers[asset_id] = tier

    print(f"  {len(asset_tiers)} assets have a valid tier (1-4).")

    if not asset_tiers:
        print("No assets with valid tiers found. Nothing to do.")
        sys.exit(0)

    # Step 2: Stream vulnerability data and calculate priorities on the fly.
    #         This avoids loading the entire vuln export into memory.
    print("[2/4] Exporting vulnerabilities from runZero...")
    vuln_count = 0
    best_priority_per_asset = {}

    for vuln in paginated_export(session, "vulnerabilities.json"):
        vuln_count += 1
        asset_id = vuln.get("asset_id")

        # Skip vulns for assets we don't care about
        tier = asset_tiers.get(asset_id)
        if tier is None:
            continue

        # Early exit: if this asset already has P1 we can't do better
        current_best = best_priority_per_asset.get(asset_id)
        if current_best == "P1":
            continue

        severity = classify_severity(vuln)
        has_kev = check_kev(vuln)
        priority = determine_priority(tier, severity, has_kev)

        if priority is None:
            continue

        if current_best is None or PRIORITY_RANK[priority] < PRIORITY_RANK[current_best]:
            best_priority_per_asset[asset_id] = priority

    print(f"  Processed {vuln_count} vulnerability records.")

    # Step 3: Summarise
    print("[3/4] Calculating remediation priorities...")
    stats = defaultdict(int)
    for p in best_priority_per_asset.values():
        stats[p] += 1

    if not best_priority_per_asset:
        print("  No priorities to assign. Check that tiered assets have vulnerabilities.")
        sys.exit(0)

    print(f"  Results: {dict(sorted(stats.items()))}")

    # Step 4: Write tags back via bulk tags API
    print("[4/4] Applying remediation_priority tags to assets...")
    tag_url = f"{BASE_URL}/api/v1.0/org/assets/bulk/tags"
    session.headers.update({"Content-Type": "application/json"})

    # Group by priority for efficient batching
    by_priority = defaultdict(list)
    for asset_id, priority in best_priority_per_asset.items():
        by_priority[priority].append(asset_id)

    total_updated = 0
    total_failed = 0

    for priority, asset_ids in sorted(by_priority.items()):
        tag = f"remediation_priority={priority}"

        for i in range(0, len(asset_ids), TAG_BATCH_SIZE):
            batch = asset_ids[i:i + TAG_BATCH_SIZE]
            search_query = " OR ".join(f"id:{aid}" for aid in batch)

            resp = session.patch(tag_url, json={"search": search_query, "tags": [tag]})

            if resp.status_code == 200:
                count = resp.json().get("updated_asset_count", len(batch))
                total_updated += count
            else:
                total_failed += len(batch)
                print(f"  WARNING: Failed batch for {priority}: {resp.status_code} - {resp.text[:200]}")

    print(f"  Tagged {total_updated} assets successfully.")
    if total_failed:
        print(f"  WARNING: {total_failed} assets failed to tag.")

    print()
    print("Done! Filter in runZero with:")
    print("  tag:remediation_priority=P1")
    print("  tag:remediation_priority=P2  ...etc")
    print()
    print("Use these tags in dashboard widgets, alert rules, and reports")
    print("to track remediation SLA compliance.")


if __name__ == "__main__":
    main()
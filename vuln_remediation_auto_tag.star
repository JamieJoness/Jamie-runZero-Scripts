load('json', json_encode='encode', json_decode='decode')
load('http', http_get='get', http_patch='patch')

# Vulnerability Remediation Priority Tagger
#
# Reads assets tagged with vuln_tier: (1-4) and their vulnerabilities,
# then writes a remediation_priority tag (P1-P5) per the priority matrix:
#
#   Tier 1 + Critical/High/Medium + KEV  → P1
#   Tier 2 + Critical/High/Medium + KEV  → P2
#   Tier 1 + Critical/High/Medium        → P2
#   Tier 3 + Critical/High/Medium + KEV  → P3
#   Tier 2 + Critical/High/Medium        → P3
#   Tier 4 + Critical/High/Medium + KEV  → P4
#   Any    + Low    → P5
#   Info-only vulns receive no tag.
#
# Each asset keeps only its highest (lowest number) priority.
#
# Credentials: access_secret = Org API token, access_key = console URL same as below DEFAULT_BASE_URL

DEFAULT_BASE_URL = 'https://console.runzero.com' #Change this to your runzero console url if not using runzero SaaS console
TIER_TAG_KEY = 'vuln_tier'
TAG_BATCH_SIZE = 50

PRIORITY_LOOKUP = {
    '1,True': 'P1',
    '2,True': 'P2',
    '1,False': 'P2',
    '3,True': 'P3',
    '2,False': 'P3',
    '4,True': 'P4',
}

ACTIONABLE_SEVERITIES = ('critical', 'high', 'medium')
P5_SEVERITIES = ('critical', 'high', 'medium', 'low')
VALID_TIERS = (1, 2, 3, 4)
PRIORITY_RANK = {'P1': 1, 'P2': 2, 'P3': 3, 'P4': 4, 'P5': 5}


def is_digit_string(s):
    if type(s) != 'string' or len(s) == 0:
        return False
    for c in s.elems():
        if c < '0' or c > '9':
            return False
    return True


def safe_float(val):
    if val == None:
        return None
    s = str(val).strip()
    if len(s) == 0:
        return None
    has_dot = False
    has_digit = False
    start = 0
    if s[0] == '-' or s[0] == '+':
        start = 1
    for i in range(start, len(s)):
        c = s[i]
        if c == '.':
            if has_dot:
                return None
            has_dot = True
        elif c >= '0' and c <= '9':
            has_digit = True
        else:
            return None
    if not has_digit:
        return None
    return float(s)


def get_asset_tier(asset):
    tags = asset.get('tags', {})
    if tags == None:
        return None
    if type(tags) == 'dict':
        val = tags.get(TIER_TAG_KEY)
        if val != None:
            s = str(val).strip()
            if is_digit_string(s):
                return int(s)
        return None
    if type(tags) == 'list':
        prefix = TIER_TAG_KEY + '='
        for tag in tags:
            if type(tag) == 'string' and tag.startswith(prefix):
                rest = tag[len(prefix):]
                if is_digit_string(rest):
                    return int(rest)
    return None


def classify_severity(vuln):
    for field in ('severity', 'vulnerability_severity'):
        raw = vuln.get(field)
        if raw != None:
            s = str(raw).lower().strip()
            if s in ('critical', 'high', 'medium', 'low', 'info'):
                return s

    for field in ('cvss3_base_score', 'cvss2_base_score', 'severity_score',
                  'vulnerability_cvss3_base_score', 'vulnerability_cvss2_base_score',
                  'vulnerability_severity_score'):
        val = vuln.get(field)
        if val != None:
            score = safe_float(val)
            if score == None:
                continue
            if score >= 9.0:
                return 'critical'
            if score >= 7.0:
                return 'high'
            if score >= 4.0:
                return 'medium'
            if score > 0:
                return 'low'
            return 'info'

    return 'info'


def check_kev(vuln):
    for field in ('kev', 'vulnerability_kev'):
        val = vuln.get(field)
        if type(val) == 'bool' and val:
            return True
        if type(val) == 'string' and val.lower() in ('true', 't', 'yes', '1', 'cisa', 'vulncheck'):
            return True

    for field in ('kev_cisa', 'kev_vulncheck', 'vulnerability_kev_cisa', 'vulnerability_kev_vulncheck'):
        val = vuln.get(field)
        if val and str(val).lower() in ('true', 't', 'yes', '1'):
            return True

    return False


def determine_priority(tier, severity, has_kev):
    if severity in ACTIONABLE_SEVERITIES:
        result = PRIORITY_LOOKUP.get('{},{}'.format(tier, has_kev))
        if result:
            return result

    if severity in P5_SEVERITIES:
        return 'P5'

    return None


def paginated_export(headers, base_url, endpoint, search):
    url = base_url + '/api/v1.0/export/org/' + endpoint
    params = {}
    if search:
        params['search'] = search

    all_items = []
    for page in range(1, 101):
        resp = http_get(url, headers=headers, params=params, timeout=600)

        if resp.body == None or len(resp.body) == 0:
            print('ERROR: empty response for {} (status {})'.format(endpoint, resp.status_code))
            return all_items

        if resp.status_code != 200 and resp.status_code != -1:
            print('ERROR: status {} for {}: {}'.format(resp.status_code, endpoint, str(resp.body)[:500]))
            return all_items

        if len(resp.body) == 209715200:
            print('ERROR: response truncated at 200 MiB for {}. Use a search filter to reduce results.'.format(endpoint))
            return all_items

        data = json_decode(resp.body)
        if data == None:
            print('ERROR: JSON decode failed for {} (status {})'.format(endpoint, resp.status_code))
            return all_items

        if type(data) == 'list':
            all_items.extend(data)
            return all_items

        if type(data) == 'dict':
            if 'error' in data:
                print('ERROR: API error for {}: {}'.format(endpoint, data.get('error')))
                return all_items

            for key in ('assets', 'vulnerabilities', 'items'):
                val = data.get(key)
                if val != None and type(val) == 'list':
                    all_items.extend(val)
                    break

            next_key = data.get('next_key')
            if not next_key:
                return all_items
            params['start_key'] = next_key
        else:
            return all_items

    return all_items


def apply_tags(headers, base_url, best_priority_per_asset):
    tag_url = base_url + '/api/v1.0/org/assets/bulk/tags'
    tag_headers = {
        'Authorization': headers['Authorization'],
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }

    by_priority = {}
    for asset_id in best_priority_per_asset:
        p = best_priority_per_asset[asset_id]
        if p not in by_priority:
            by_priority[p] = []
        by_priority[p].append(asset_id)

    total_updated = 0
    total_failed = 0

    for priority in sorted(by_priority.keys()):
        asset_ids = by_priority[priority]
        tag = 'remediation_priority=' + priority

        for i in range(0, len(asset_ids), TAG_BATCH_SIZE):
            batch = asset_ids[i:i + TAG_BATCH_SIZE]
            parts = []
            for aid in batch:
                parts.append('id:' + aid)
            search_query = ' OR '.join(parts)

            body = bytes(json_encode({'search': search_query, 'tags': tag}))
            resp = http_patch(tag_url, headers=tag_headers, body=body, timeout=120)

            if resp.status_code == 200 or (resp.status_code == -1 and resp.body and len(resp.body) > 0):
                resp_data = json_decode(resp.body)
                if resp_data != None and type(resp_data) == 'dict':
                    total_updated = total_updated + resp_data.get('updated_asset_count', len(batch))
                else:
                    total_updated = total_updated + len(batch)
            else:
                total_failed = total_failed + len(batch)
                print('ERROR: tag PATCH failed (status {}): {}'.format(
                    resp.status_code, str(resp.body)[:300] if resp.body else ''))

    return {'updated': total_updated, 'failed': total_failed}


def main(*args, **kwargs):
    token = kwargs.get('access_secret', '')
    if not token:
        print('ERROR: No API token. Set access_secret to your Org API token.')
        return None

    base_url = kwargs.get('access_key', '')
    if not base_url:
        base_url = DEFAULT_BASE_URL
    if base_url.endswith('/'):
        base_url = base_url[:-1]

    headers = {'Authorization': 'Bearer ' + token, 'Accept': 'application/json'}

    print('Vulnerability Remediation Priority Tagger')
    print('Console: {}'.format(base_url))

    # Step 1: Export tiered assets
    print('[1/4] Exporting assets with {} tag...'.format(TIER_TAG_KEY))
    assets = paginated_export(headers, base_url, 'assets.json', 'tag:' + TIER_TAG_KEY)
    print('  {} assets found.'.format(len(assets)))

    if not assets:
        print('No tiered assets found.')
        return None

    asset_tiers = {}
    for asset in assets:
        asset_id = asset.get('id')
        if not asset_id:
            continue
        tier = get_asset_tier(asset)
        if tier != None and tier in VALID_TIERS:
            asset_tiers[asset_id] = tier

    print('  {} assets with valid tier (1-4).'.format(len(asset_tiers)))
    if not asset_tiers:
        return None

    # Step 2: Export vulns for tiered assets
    print('[2/4] Exporting vulnerabilities...')
    vulns = paginated_export(headers, base_url, 'vulnerabilities.json', 'tag:' + TIER_TAG_KEY)
    print('  {} vulnerabilities fetched.'.format(len(vulns)))

    if not vulns:
        print('No vulnerabilities found.')
        return None

    # Step 2b: Build KEV lookup by asset ID from a separate filtered export
    # The vuln export does not include KEV fields, so we query with kev:true
    # and collect asset IDs that have at least one KEV vulnerability.
    print('  Fetching KEV vulnerability list...')
    kev_vulns = paginated_export(headers, base_url, 'vulnerabilities.json', 'kev:true')
    print('  KEV export returned {} vuln records.'.format(len(kev_vulns)))
    kev_asset_ids = {}
    for kv in kev_vulns:
        aid = kv.get('vulnerability_asset_id')
        if aid == None or str(aid).strip() == '':
            aid = kv.get('asset_id')
        if aid != None and str(aid).strip() != '':
            kev_asset_ids[str(aid).strip()] = True
    print('  {} unique assets with KEV vulns.'.format(len(kev_asset_ids)))
    if len(kev_asset_ids) == 0:
        print('  WARNING: No KEV assets found. KEV-based priority escalation will not work.')

    # Detect field name convention
    asset_id_field = 'asset_id'
    if len(vulns) > 0:
        s = vulns[0]
        if 'vulnerability_asset_id' in s and 'asset_id' not in s:
            asset_id_field = 'vulnerability_asset_id'

    # Calculate best priority per asset
    best_priority_per_asset = {}
    for vuln in vulns:
        asset_id = vuln.get(asset_id_field)
        if not asset_id:
            asset_id = vuln.get('vulnerability_asset_id') if asset_id_field == 'asset_id' else vuln.get('asset_id')

        tier = asset_tiers.get(asset_id)
        if tier == None:
            continue

        current_best = best_priority_per_asset.get(asset_id)
        if current_best == 'P1':
            continue

        severity = classify_severity(vuln)
        has_kev = False
        if asset_id != None and str(asset_id).strip() != '':
            has_kev = str(asset_id).strip() in kev_asset_ids
        priority = determine_priority(tier, severity, has_kev)
        if priority == None:
            continue

        if current_best == None or PRIORITY_RANK[priority] < PRIORITY_RANK[current_best]:
            best_priority_per_asset[asset_id] = priority

    # Step 3: Summary
    print('[3/4] Priority summary:')
    stats = {}
    for p in best_priority_per_asset.values():
        if p not in stats:
            stats[p] = 0
        stats[p] = stats[p] + 1

    if not best_priority_per_asset:
        print('  No priorities to assign.')
        return None

    print('  {}'.format(stats))

    # Step 4: Apply tags
    print('[4/4] Applying remediation_priority tags...')
    result = apply_tags(headers, base_url, best_priority_per_asset)
    print('  Tagged {} assets.'.format(result['updated']))
    if result['failed']:
        print('  {} failed.'.format(result['failed']))

    return None
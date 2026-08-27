load('json', json_encode='encode', json_decode='decode')
load('http', http_get='get', http_post='post')

# Asset Auto-Delete Custom Integration
#
# Deletes every asset matching DELETE_QUERY. Designed to run unattended as a
# scheduled custom integration task (e.g. daily). The task log records which
# assets were removed on each run.

DEFAULT_BASE_URL = 'https://console-eu.runzero.com'
DELETE_QUERY = 'type:"IP Phone" or type:mobile'
BATCH_SIZE = 500
# Max assets listed individually in the task log; the rest are summarised.
LOG_ASSET_LIMIT = 200


def fetch_matching_assets(headers, base_url):
    """Export id/addresses/names of all assets matching the delete query."""
    url = base_url + '/api/v1.0/export/org/assets.json'
    params = {'search': DELETE_QUERY, 'fields': 'id,addresses,names'}

    all_assets = []
    for page in range(1, 101):
        resp = http_get(url, headers=headers, params=params, timeout=600)

        if resp.status_code != 200:
            print('ERROR: asset export failed (status {}): {}'.format(
                resp.status_code, str(resp.body)[:500] if resp.body else ''))
            return all_assets

        if resp.body == None or len(resp.body) == 0:
            return all_assets

        data = json_decode(resp.body)
        if data == None:
            print('ERROR: JSON decode failed for asset export.')
            return all_assets

        assets = []
        if type(data) == 'list':
            assets = data
        elif type(data) == 'dict':
            if 'error' in data:
                print('ERROR: API error: {}'.format(data.get('error')))
                return all_assets
            for key in ('assets', 'items'):
                val = data.get(key)
                if val != None and type(val) == 'list':
                    assets = val
                    break

        for asset in assets:
            asset_id = asset.get('id')
            if not asset_id:
                continue
            all_assets.append(asset)

        if type(data) == 'dict':
            next_key = data.get('next_key')
            if next_key:
                params['start_key'] = next_key
            else:
                break
        else:
            break

    return all_assets


def format_asset(asset):
    """Build an 'address hostname' label for the task log listing."""
    parts = []
    addresses = asset.get('addresses')
    if type(addresses) == 'list' and len(addresses) > 0:
        parts.append(str(addresses[0]))
    names = asset.get('names')
    if type(names) == 'list' and len(names) > 0:
        parts.append(str(names[0]))
    if not parts:
        return '(no address/name)'
    return ' '.join(parts)


def bulk_delete(headers, base_url, asset_ids):
    """Delete a batch of asset IDs using the bulk delete endpoint."""
    url = base_url + '/api/v1.0/org/assets/bulk/delete'
    delete_headers = {
        'Authorization': headers['Authorization'],
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }
    body = bytes(json_encode({'asset_ids': asset_ids}))
    resp = http_post(url, headers=delete_headers, body=body, timeout=120)
    return resp


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

    # An empty search matches every asset, so refuse to run without a query.
    if not DELETE_QUERY.strip():
        print('ERROR: DELETE_QUERY is empty. Refusing to run.')
        return None

    headers = {'Authorization': 'Bearer ' + token, 'Accept': 'application/json'}

    print('Asset Auto-Delete')
    print('Console: {}'.format(base_url))
    print('Query: {}'.format(DELETE_QUERY))

    # Fetch matching assets
    print('[1/2] Exporting assets matching query...')
    matched = fetch_matching_assets(headers, base_url)
    print('  {} asset(s) matched.'.format(len(matched)))

    if not matched:
        print('No matching assets found. Nothing to delete.')
        return None

    # Audit trail: record what this run is removing.
    print('Deleting the following {} asset(s):'.format(len(matched)))
    shown = 0
    for asset in matched:
        if shown >= LOG_ASSET_LIMIT:
            print('  ... and {} more.'.format(len(matched) - shown))
            break
        print('  {}  {}'.format(asset.get('id'), format_asset(asset)))
        shown = shown + 1

    asset_ids = [asset.get('id') for asset in matched]

    # Delete in batches
    print('[2/2] Deleting assets in batches of {}...'.format(BATCH_SIZE))
    deleted = 0
    failed = 0

    for i in range(0, len(asset_ids), BATCH_SIZE):
        batch = asset_ids[i:i + BATCH_SIZE]
        resp = bulk_delete(headers, base_url, batch)

        # status -1 with a body is how the http module reports some successful responses
        ok = resp.status_code == 204 or resp.status_code == 200 or (
            resp.status_code == -1 and resp.body and len(resp.body) > 0)

        if ok:
            deleted = deleted + len(batch)
            print('  Deleted {}/{}'.format(deleted, len(asset_ids)))
        else:
            failed = failed + len(batch)
            print('ERROR: bulk delete failed (status {}): {}'.format(
                resp.status_code, str(resp.body)[:300] if resp.body else ''))

    print('Done. Deleted: {}, Failed: {}'.format(deleted, failed))
    return None

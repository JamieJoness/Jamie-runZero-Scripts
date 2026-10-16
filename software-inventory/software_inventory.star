load('runzero.types', 'ImportAsset', 'NetworkInterface', 'Software')
load('json', json_decode='decode')
load('net', 'ip_address')
load('http', http_get='get')
load('uuid', 'new_uuid')

# software.exe must be installed and running in serve mode on the Explorer host
# before this integration task executes. It collects software inventory from the
# local machine and any remote targets defined in targets.csv, then serves
# all results as a JSON array over a persistent local HTTP endpoint.
#
# To set up:
#   1. On the Explorer host, run as Administrator:
#      software.exe -Install -Port 8990 -RefreshMinutes 60
#      (copies the exe, generates a Bearer token, registers a startup task)
#   2. Optionally place a targets.csv alongside software.exe with remote host IPs:
#      IP
#      10.0.0.1
#      Store credentials per-host in Windows Credential Manager:
#      cmdkey /add:SoftwareInventory:10.0.0.1 /user:DOMAIN\Administrator /pass:YourPassword
#   3. Configure this script as a Custom Integration in the runZero console.
#   4. Use access_key for the inventory URL (default: http://localhost:8990/inventory/)
#   5. Use access_secret for the Bearer token printed during installation.

INVENTORY_URL_DEFAULT = 'http://localhost:8990/inventory/'

def main(*args, **kwargs):
    inventory_url = kwargs.get('access_key', '')
    if not inventory_url:
        inventory_url = INVENTORY_URL_DEFAULT

    # Bearer token — set as 'access_secret' in the runZero custom integration config
    bearer_token = kwargs.get('access_secret', '')

    print('=== Software Inventory Custom Integration ===')
    print('Fetching inventory from: ' + inventory_url)
    print('')

    headers = {}
    if bearer_token:
        headers['Authorization'] = 'Bearer ' + bearer_token

    resp = http_get(inventory_url, headers=headers, timeout=60)
    if resp.status_code != 200:
        print('ERROR: Failed to fetch inventory (HTTP ' + str(resp.status_code) + ')')
        return []

    raw = json_decode(resp.body)
    if not raw:
        print('ERROR: Could not parse inventory JSON')
        return []

    # Handle both a single object (local only) and an array (local + remote)
    inventories = []
    if type(raw) == 'list':
        inventories = raw
    else:
        inventories = [raw]

    print('Received ' + str(len(inventories)) + ' machine inventory(ies)')
    for i, inv in enumerate(inventories):
        if type(inv) == 'dict':
            print('  [' + str(i) + '] ComputerName=' + str(inv.get('ComputerName', '?')) +
                  ' CollectedAt=' + str(inv.get('CollectedAt', '?')) +
                  ' SoftwareCount=' + str(len(inv.get('Software', []) or [])))
        else:
            print('  [' + str(i) + '] WARNING: unexpected type=' + type(inv))
    print('')

    assets = []
    for inventory in inventories:
        if type(inventory) != 'dict':
            print('WARNING: skipping unexpected non-dict entry in inventory')
            continue
        hostname = inventory.get('ComputerName', 'Unknown')
        print('Building asset for ' + hostname + '...')
        asset = build_asset(inventory)
        assets.append(asset)

    print('')
    print('=== COMPLETE ===')
    print('Successfully built ' + str(len(assets)) + ' runZero asset(s)')
    return assets


def build_asset(inventory):
    hostname = inventory.get('ComputerName', 'Unknown')
    collected_at = inventory.get('CollectedAt', '')

    system = inventory.get('System', {})
    if system == None or type(system) != 'dict':
        system = {}
    os_name = system.get('OSName', '')
    os_version = system.get('OSVersion', '')
    manufacturer = system.get('Manufacturer', '')
    model = system.get('Model', '')
    serial_number = system.get('SerialNumber', '')

    network_info = inventory.get('Network', {})
    if network_info == None or type(network_info) != 'dict':
        network_info = {}
    ip_raw = network_info.get('IPAddress', '')
    mac_raw = network_info.get('MACAddress', '')

    networks = []
    if ip_raw or mac_raw:
        networks.append(build_network_interface(ip_raw, mac_raw))

    # Build a stable ID so the same asset is updated across runs rather than duplicated.
    # Prefer MAC address (hardware-bound); fall back to IP address; then hostname as last resort.
    if mac_raw:
        asset_id = 'sw-inv:' + mac_raw.lower().replace('-', ':')
    elif ip_raw:
        asset_id = 'sw-inv:' + ip_raw
    else:
        asset_id = 'sw-inv:' + hostname.lower()

    software_list = []
    sw_items = inventory.get('Software', [])
    if sw_items == None:
        sw_items = []

    for sw in sw_items:
        if type(sw) != 'dict':
            continue
        name = sw.get('Name', '')
        if not name:
            continue

        version = sw.get('Version', '')
        publisher = sw.get('Publisher', '')
        install_date = sw.get('InstallDate', '')

        sw_attrs = {}
        if install_date:
            sw_attrs['install_date'] = str(install_date)

        software_list.append(Software(
            id=str(new_uuid()),
            product=name,
            vendor=publisher if publisher else '',
            version=version if version else '',
            customAttributes=sw_attrs,
        ))

    print('   OS: ' + os_name + ' (' + os_version + ')')
    print('   Manufacturer: ' + manufacturer)
    print('   Model: ' + model)
    print('   IP: ' + str(ip_raw))
    print('   MAC: ' + str(mac_raw))
    print('   Software items: ' + str(len(software_list)))

    custom_attributes = {
        'source': 'PowerShell Software Inventory',
        'collected_at': collected_at,
        'os_name': os_name,
        'os_version': os_version,
        'manufacturer': manufacturer,
        'model': model,
        'serial_number': serial_number,
        'software_count': str(len(software_list)),
    }

    return ImportAsset(
        id=asset_id,
        os=os_name,
        osVersion=os_version,
        hostnames=[hostname] if hostname else [],
        networkInterfaces=networks,
        customAttributes=custom_attributes,
        software=software_list,
    )


def build_network_interface(ip_raw, mac_raw):
    ip4s = []
    ip6s = []

    # Jamie Update: software.exe can report several addresses in one field
    # ("10.0.0.1 fe80::1%12") and ip_address() returns None for anything it
    # cannot parse (zone IDs, CIDR suffixes, hostnames, stray whitespace).
    # Warn and skip those instead of aborting the whole task.
    if ip_raw:
        for part in ip_raw.replace(',', ' ').replace(';', ' ').split():
            ip_obj = ip_address(part.split('%')[0].split('/')[0])
            if ip_obj == None:
                print('   WARNING: ignoring unparseable IP address ' + repr(part))
            elif ip_obj.version == 4:
                ip4s.append(ip_obj)
            else:
                ip6s.append(ip_obj)

    clean_mac = mac_raw.replace('-', ':') if mac_raw else None
    if not clean_mac:
        return NetworkInterface(ipv4Addresses=ip4s, ipv6Addresses=ip6s)
    return NetworkInterface(macAddress=clean_mac, ipv4Addresses=ip4s, ipv6Addresses=ip6s)

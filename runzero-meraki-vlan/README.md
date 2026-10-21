# runZero Meraki VLAN report

Answers three questions about the assets a Cisco Meraki integration has brought into runZero:

1. Which assets have a VLAN (`@meraki.ntwkclient.vlan`)?
2. Which assets don't?
3. Which VLANs are the most common?

It pulls the assets through the runZero Export API, prints a summary and writes CSV/JSON
reports. Nothing else is stored: the export is processed as a stream and discarded.

## Requirements

* Python 3.8 or newer (standard library only - nothing to install)
* A runZero **Export API token** for the organization you want to report on
* Network access to your runZero console (`https://console.runzero.com` or your self-hosted URL)

### Getting an Export API token

1. In the runZero console go to **Organizations** and click the organization.
2. Click **Edit organization** and scroll to **Export tokens**.
3. Click **Generate** and copy the token (it starts with `ET`).

Export tokens are read-only and tied to that one organization, so no organization ID is needed.
An Organization API token (`OT…`) also works.

## Quick start

```bash
export RUNZERO_EXPORT_TOKEN=ET...        # or let the script prompt you for it
python3 meraki_vlan_report.py
```

The script streams every asset matching `source:meraki`, prints a summary like the one below
and writes the report files to `./meraki_vlan_report/`.

```
Meraki VLAN report  (search: source:meraki)
========================================================================
Assets exported ........     48,210
  with a VLAN ..........      9,874  (20.5%)
  without a VLAN .......     38,336  (79.5%)

By asset kind:
  Meraki network clients (@meraki.ntwkclient.*)      47,560   with VLAN 9,874 / without 37,686
  Meraki devices only (@meraki.dev.*)                   650   with VLAN 0 / without 650

Clients by connection type (newest Meraki record):
                          with VLAN      without
  Wireless                    1,920       37,684
  Wired                       7,954            2

Distinct VLANs: 14

Top 14 VLANs by asset count:
  rank  vlan       name                             assets     share    cum.
     1  100                                          4,120     41.7%   41.7%
     2  400                                          2,655     26.9%   68.6%
     ...
```

## What you get

| File | Contents |
|---|---|
| `summary.json` | Every number shown on the console, machine-readable |
| `vlan_counts.csv` | Every VLAN ranked by asset count, with share and cumulative percentage |
| `assets_with_vlan.csv` | One row per asset that has a VLAN |
| `assets_without_vlan.csv` | One row per asset without one (Meraki APs/switches included - see the `kind` column) |

The per-asset files include the VLAN (and VLAN name if Meraki provides one), wired/wireless,
SSID, the AP or switch the client was last seen on, the Meraki description, hostnames, IPs,
MACs, type/OS/hardware, site, first/last seen and a link to the asset in the console.

## Options

| Option | Default | Purpose |
|---|---|---|
| `--token` | `$RUNZERO_EXPORT_TOKEN` | Export API token (prompted for if neither is set) |
| `--url` | `$RUNZERO_URL` or `https://console.runzero.com` | Console URL, for self-hosted deployments |
| `--org-id` | `$RUNZERO_ORG_ID` | Organization ID, only for account-level tokens |
| `--search` | `source:meraki` | runZero search that selects the assets to analyse |
| `--out-dir` | `meraki_vlan_report` | Where the report files are written |
| `--top` | `25` | VLANs to show on the console (`0` for all) |
| `--treat-as-no-vlan` | | Comma-separated VLAN values to count as "no VLAN", e.g. `0` |
| `--vlan-attr` | `@meraki.ntwkclient.vlan` | Attribute holding the VLAN |
| `--name-attr` | `@meraki.ntwkclient.namedVlan` | Attribute holding the VLAN name, if present |
| `--page-size` | `0` (stream) | Use the paged `assets.json` endpoint with this page size |
| `--timeout` | `600` | Socket timeout in seconds |
| `--insecure` | | Skip TLS verification (self-hosted consoles with a private CA) |

## How it works

* Integration attributes are not in an asset's `attributes` map; the Export API returns them in
  `foreign_attributes`, keyed by source, with one record per Meraki sighting:
  `foreign_attributes["@meraki.ntwkclient"][n]["vlan"]`. That is what the search term
  `@meraki.ntwkclient.vlan` refers to.
* When an asset has several Meraki records the newest (`lastSeenTS`) wins; any other VLAN values
  are listed in the `all_vlans` column and counted in `assets_with_multiple_vlans`.
* Only the fields the report needs are requested (`fields=`), which keeps a large export small.
  If a console rejects the filter the script retries with full records.
* Transient errors (429/5xx, connection drops) are retried with back-off.

### Reading the results

A large "without a VLAN" bucket is normal and usually structural rather than a data problem:
Meraki only reports a VLAN for wireless clients on VLAN-tagged SSIDs, so clients on guest or
NAT-mode SSIDs have none, while wired clients almost always do. The `connection`, `ssid` and
`seen_on` columns in `assets_without_vlan.csv` show where they sit.

If those clients are not useful inventory, the Meraki connector task in runZero has
**Exclude clients with no VLAN**, **VLANs to exclude** and **SSIDs to exclude** options
that stop importing them on future syncs.

### Matching inventory searches

The same buckets in the runZero Asset Inventory:

```
source:meraki AND has:"@meraki.ntwkclient.vlan"        # with a VLAN
source:meraki AND NOT has:"@meraki.ntwkclient.vlan"    # without a VLAN
@meraki.ntwkclient.vlan:="100"                         # one specific VLAN
```

## Troubleshooting

| Symptom | What to do |
|---|---|
| `HTTP 401` | Token is wrong, revoked, or for a different organization |
| `HTTP 400 … fields` | Handled automatically; the script retries without the field filter |
| Connection drops or proxy timeouts on large exports | Use `--page-size 5000` |
| TLS certificate errors on a self-hosted console | Use `--insecure`, or add the CA to your system trust store |
| Export takes a long time | Normal for very large inventories; a few minutes per 100k assets is typical |

## Data handling

The report files contain hostnames, IPs and MAC addresses from the customer's environment.
Treat them accordingly: the default output directory is in `.gitignore`, and if you use
`--out-dir` keep it out of version control too. No raw export is kept.

#!/usr/bin/env python3
"""Meraki VLAN coverage report built from the runZero Export API.

Pulls every asset matching a runZero search (default: source:meraki), then reports
which assets carry @meraki.ntwkclient.vlan, which do not, and which VLANs are the
most common. Integration attributes are read from each asset's foreign_attributes
(foreign_attributes["@meraki.ntwkclient"][n]["vlan"]); when an asset has several
Meraki records the newest one wins and the others are listed in the all_vlans column.

Outputs (written to --out-dir, default ./meraki_vlan_report):
  summary.json             every count shown on the console, machine-readable
  vlan_counts.csv          every VLAN ranked by asset count
  assets_with_vlan.csv     one row per asset that has a VLAN, with SSID, wired/wireless
                           and the AP/switch it was last seen on
  assets_without_vlan.csv  one row per asset without one (Meraki devices included;
                           use the "kind" column to tell clients from devices)

Authentication: an Export API token (Organizations -> your organization -> Edit
organization -> Export tokens) via the RUNZERO_EXPORT_TOKEN environment variable,
the --token flag, or an interactive prompt. Nothing but the Python standard library
is required, and nothing from the export is kept except the report files.

Examples:
  export RUNZERO_EXPORT_TOKEN=ET...
  ./meraki_vlan_report.py                               # export, analyse, write reports
  ./meraki_vlan_report.py --out-dir reports/acme        # choose where the reports go
  ./meraki_vlan_report.py --page-size 5000              # paged .json endpoint instead of a JSONL stream
  ./meraki_vlan_report.py --treat-as-no-vlan 0          # count VLAN "0" as "no VLAN"
  ./meraki_vlan_report.py --url https://runzero.example.com --org-id <org uuid>
"""

import argparse
import csv
import getpass
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone

DEFAULT_URL = "https://console.runzero.com"
DEFAULT_SEARCH = "source:meraki"
VLAN_ATTR = "@meraki.ntwkclient.vlan"
VLAN_NAME_ATTR = "@meraki.ntwkclient.namedVlan"
CLIENT_SOURCE = "@meraki.ntwkclient"
DEVICE_SOURCE = "@meraki.dev"
EXPORT_FIELDS = "id,site_name,type,os,hw,names,addresses,macs,first_seen,last_seen,sources,foreign_attributes"
ASSET_COLUMNS = [
    "asset_id", "kind", "vlan", "vlan_name", "all_vlans", "connection", "ssid", "seen_on", "meraki_description",
    "names", "addresses", "macs", "type", "os", "hw", "site", "first_seen_utc", "last_seen_utc", "sources", "console_url",
]
RETRY_STATUSES = {429, 500, 502, 503, 504}
MAX_ATTEMPTS = 4
PROGRESS_EVERY = 10000


class ExportError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


def log(message):
    print(message, file=sys.stderr, flush=True)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Report VLAN coverage of Meraki assets held in runZero.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("Examples:", 1)[1] if "Examples:" in __doc__ else None,
    )
    parser.add_argument("--url", default=os.environ.get("RUNZERO_URL", DEFAULT_URL),
                        help="runZero console URL (env RUNZERO_URL, default %(default)s)")
    parser.add_argument("--token", default=os.environ.get("RUNZERO_EXPORT_TOKEN"),
                        help="Export API token or Organization API key (env RUNZERO_EXPORT_TOKEN; prompted for if missing)")
    parser.add_argument("--org-id", default=os.environ.get("RUNZERO_ORG_ID"),
                        help="Organization ID, only needed for tokens that span organizations (env RUNZERO_ORG_ID)")
    parser.add_argument("--search", default=DEFAULT_SEARCH,
                        help="runZero inventory search to export (default: %(default)s)")
    parser.add_argument("--vlan-attr", default=VLAN_ATTR,
                        help="attribute holding the VLAN (default: %(default)s)")
    parser.add_argument("--name-attr", default=VLAN_NAME_ATTR,
                        help="attribute holding the VLAN name, if the integration provides one (default: %(default)s)")
    parser.add_argument("--treat-as-no-vlan", default="",
                        help='comma-separated VLAN values to count as "no VLAN", e.g. "0"')
    parser.add_argument("--top", type=int, default=25,
                        help="number of VLANs to print on the console, 0 for all (default: %(default)s)")
    parser.add_argument("--out-dir", default="meraki_vlan_report",
                        help="directory for the CSV/JSON outputs (default: %(default)s)")
    parser.add_argument("--page-size", type=int, default=0,
                        help="use the paged assets.json endpoint with this page size instead of streaming assets.jsonl")
    parser.add_argument("--timeout", type=int, default=600,
                        help="socket timeout in seconds (default: %(default)s)")
    parser.add_argument("--insecure", action="store_true",
                        help="skip TLS certificate verification (self-hosted consoles with private CAs)")
    args = parser.parse_args()
    args.url = args.url.rstrip("/")
    args.no_vlan_values = {v.strip() for v in args.treat_as_no_vlan.split(",") if v.strip()}
    if not args.token and sys.stdin.isatty():
        args.token = getpass.getpass("runZero Export API token: ").strip()
    if not args.token:
        parser.error("no API token: set RUNZERO_EXPORT_TOKEN or pass --token")
    return args


# --------------------------------------------------------------------------- API access

def open_export(base_url, path, params, token, timeout, ssl_context):
    """GET an Export API endpoint, retrying transient failures. Returns the open response."""
    url = "{}/api/v1.0{}?{}".format(base_url, path, urllib.parse.urlencode(params))
    request = urllib.request.Request(url, headers={
        "Authorization": "Bearer " + token,
        "Accept": "application/json",
        "User-Agent": "meraki-vlan-report/1.0",
    })
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            return urllib.request.urlopen(request, timeout=timeout, context=ssl_context)
        except urllib.error.HTTPError as err:
            if err.code in RETRY_STATUSES and attempt < MAX_ATTEMPTS:
                delay = int(err.headers.get("Retry-After") or 0) or 2 ** attempt
                log("HTTP {} from runZero, retrying in {}s ({}/{})".format(err.code, delay, attempt, MAX_ATTEMPTS - 1))
                time.sleep(delay)
                continue
            detail = err.read(400).decode("utf-8", "replace").strip()
            raise ExportError(err.code, "HTTP {} for {}: {}".format(err.code, path, detail or err.reason)) from None
        except urllib.error.URLError as err:
            if attempt < MAX_ATTEMPTS:
                log("Connection problem ({}), retrying ({}/{})".format(err.reason, attempt, MAX_ATTEMPTS - 1))
                time.sleep(2 ** attempt)
                continue
            raise ExportError(0, "Could not reach {}: {}".format(base_url, err.reason)) from None


def open_export_with_fields(cfg, path, params, ssl_context):
    """Like open_export, but falls back to full records if the console rejects the fields= filter."""
    try:
        return open_export(cfg.url, path, params, cfg.token, cfg.timeout, ssl_context)
    except ExportError as err:
        if err.status == 400 and "fields" in params:
            log("runZero rejected the fields= filter; retrying with full asset records")
            params.pop("fields")
            return open_export(cfg.url, path, params, cfg.token, cfg.timeout, ssl_context)
        raise


def iter_stream(cfg, params, ssl_context):
    with open_export_with_fields(cfg, "/export/org/assets.jsonl", params, ssl_context) as response:
        for raw_line in response:
            line = raw_line.strip()
            if line:
                yield json.loads(line)


def iter_pages(cfg, params, ssl_context):
    params["page_size"] = cfg.page_size
    while True:
        with open_export_with_fields(cfg, "/export/org/assets.json", params, ssl_context) as response:
            page = json.load(response)
        if isinstance(page, list):  # console ignored page_size and returned everything
            assets, next_key = page, None
        else:
            assets, next_key = page.get("assets") or [], page.get("next_key")
        for asset in assets:
            yield asset
        if not assets or not next_key:
            return
        params["start_key"] = next_key


def iter_assets(cfg, ssl_context):
    params = {"search": cfg.search, "fields": EXPORT_FIELDS}
    if cfg.org_id:
        params["_oid"] = cfg.org_id
    if cfg.page_size > 0:
        return iter_pages(cfg, params, ssl_context)
    return iter_stream(cfg, params, ssl_context)


# --------------------------------------------------------------------------- analysis

class Stats:
    def __init__(self, vlan_attr, name_attr):
        self.known_fields = {split_attr(vlan_attr)[1], split_attr(name_attr)[1]}
        self.total = 0
        self.kinds = Counter()
        self.kind_with_vlan = Counter()
        self.vlan_counts = Counter()
        self.vlan_names = defaultdict(Counter)
        self.related_attrs = Counter()
        self.multi_vlan = 0
        self.connections = defaultdict(Counter)

    def add(self, kind, vlan, vlan_name, all_vlans, client):
        self.total += 1
        self.kinds[kind] += 1
        if vlan:
            self.kind_with_vlan[kind] += 1
            self.vlan_counts[vlan] += 1
            if vlan_name:
                self.vlan_names[vlan][vlan_name] += 1
        if len(all_vlans) > 1:
            self.multi_vlan += 1
        if client:
            connection = clean(client.get("recentDeviceConnection")) or "unknown"
            self.connections[connection]["with" if vlan else "without"] += 1
            for key in client:
                if key not in self.known_fields and "vlan" in key.lower():
                    self.related_attrs[CLIENT_SOURCE + "." + key] += 1

    @property
    def with_vlan(self):
        return sum(self.vlan_counts.values())

    @property
    def without_vlan(self):
        return self.total - self.with_vlan

    def best_name(self, vlan):
        names = self.vlan_names.get(vlan)
        return names.most_common(1)[0][0] if names else ""

    def ranked(self):
        """Yield (rank, vlan, name, count, pct_of_with_vlan, cumulative_pct)."""
        running = 0
        ordered = sorted(self.vlan_counts.items(), key=lambda item: (-item[1], item[0]))
        for rank, (vlan, count) in enumerate(ordered, start=1):
            running += count
            yield rank, vlan, self.best_name(vlan), count, pct(count, self.with_vlan), pct(running, self.with_vlan)


def split_attr(attr):
    """'@meraki.ntwkclient.vlan' -> ('@meraki.ntwkclient', 'vlan'); non-integration keys have no source."""
    parts = attr.split(".", 2)
    if attr.startswith("@") and len(parts) == 3:
        return parts[0] + "." + parts[1], parts[2]
    return None, attr


def record_time(record):
    for key in ("lastSeenTS", "ts"):
        try:
            return float(record.get(key) or 0)
        except (TypeError, ValueError):
            continue
    return 0.0


def source_records(foreign, source):
    """Integration records of one source on an asset, newest first."""
    records = [record for record in (foreign.get(source) or []) if isinstance(record, dict)]
    records.sort(key=record_time, reverse=True)
    return records


def attr_values(asset, foreign, attr):
    """Distinct non-empty values of a search-style attribute, newest record first.

    Integration attributes live in foreign_attributes[source][n][field]; anything else
    (or an export that flattened them) is looked up in the plain attributes map.
    """
    source, field = split_attr(attr)
    values = []
    for record in (source_records(foreign, source) if source else []):
        value = clean(record.get(field))
        if value and value not in values:
            values.append(value)
    if not values:
        value = clean((asset.get("attributes") or {}).get(attr))
        if value:
            values.append(value)
    return values


def asset_kind(foreign):
    is_client = bool(foreign.get(CLIENT_SOURCE))
    is_device = bool(foreign.get(DEVICE_SOURCE))
    if is_client and is_device:
        return "client+device"
    if is_client:
        return "client"
    if is_device:
        return "device"
    return "other"


def pct(part, whole):
    return round(100.0 * part / whole, 1) if whole else 0.0


def fmt_ts(value):
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S") if value else ""
    except (TypeError, ValueError, OSError, OverflowError):
        return ""


def joined(values):
    if isinstance(values, (list, tuple, set)):
        return "; ".join(str(v) for v in values)
    return str(values) if values else ""


def clean(value):
    return str(value).strip() if value is not None else ""


def asset_row(cfg, asset, kind, vlan, vlan_name, all_vlans, client):
    asset_id = clean(asset.get("id"))
    return [
        asset_id, kind, vlan, vlan_name, "; ".join(all_vlans),
        clean(client.get("recentDeviceConnection")), clean(client.get("ssid")),
        clean(client.get("recentDeviceName")), clean(client.get("description")),
        joined(asset.get("names")), joined(asset.get("addresses")), joined(asset.get("macs")),
        clean(asset.get("type")), clean(asset.get("os")), clean(asset.get("hw")), clean(asset.get("site_name")),
        fmt_ts(asset.get("first_seen")), fmt_ts(asset.get("last_seen")), joined(asset.get("sources")),
        "{}/inventory/{}".format(cfg.url, asset_id) if asset_id else "",
    ]


def analyse(cfg, assets, out_dir):
    stats = Stats(cfg.vlan_attr, cfg.name_attr)
    with_path = os.path.join(out_dir, "assets_with_vlan.csv")
    without_path = os.path.join(out_dir, "assets_without_vlan.csv")
    with open(with_path, "w", newline="", encoding="utf-8") as with_file, \
            open(without_path, "w", newline="", encoding="utf-8") as without_file:
        with_writer = csv.writer(with_file)
        without_writer = csv.writer(without_file)
        with_writer.writerow(ASSET_COLUMNS)
        without_writer.writerow(ASSET_COLUMNS)
        for asset in assets:
            foreign = asset.get("foreign_attributes") or {}
            kind = asset_kind(foreign)
            all_vlans = [v for v in attr_values(asset, foreign, cfg.vlan_attr) if v not in cfg.no_vlan_values]
            vlan = all_vlans[0] if all_vlans else ""
            names = attr_values(asset, foreign, cfg.name_attr) if vlan else []
            vlan_name = names[0] if names else ""
            clients = source_records(foreign, CLIENT_SOURCE)
            client = clients[0] if clients else {}
            stats.add(kind, vlan, vlan_name, all_vlans, client)
            (with_writer if vlan else without_writer).writerow(
                asset_row(cfg, asset, kind, vlan, vlan_name, all_vlans, client))
            if stats.total % PROGRESS_EVERY == 0:
                log("  ...{:,} assets processed".format(stats.total))
    return stats


# --------------------------------------------------------------------------- output

def write_vlan_counts(stats, out_dir):
    path = os.path.join(out_dir, "vlan_counts.csv")
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["rank", "vlan", "vlan_name", "assets", "pct_of_assets_with_vlan", "cumulative_pct"])
        for row in stats.ranked():
            writer.writerow(row)
    return path


def write_summary(cfg, stats, out_dir):
    path = os.path.join(out_dir, "summary.json")
    summary = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "{} search={!r}".format(cfg.url, cfg.search),
        "vlan_attribute": cfg.vlan_attr,
        "values_treated_as_no_vlan": sorted(cfg.no_vlan_values),
        "assets_total": stats.total,
        "assets_with_vlan": stats.with_vlan,
        "assets_without_vlan": stats.without_vlan,
        "by_kind": {
            kind: {"total": total, "with_vlan": stats.kind_with_vlan[kind], "without_vlan": total - stats.kind_with_vlan[kind]}
            for kind, total in sorted(stats.kinds.items())
        },
        "distinct_vlans": len(stats.vlan_counts),
        "assets_with_multiple_vlans": stats.multi_vlan,
        "clients_by_connection": {
            connection: {"with_vlan": counts["with"], "without_vlan": counts["without"]}
            for connection, counts in sorted(stats.connections.items())
        },
        "vlans": [
            {"rank": rank, "vlan": vlan, "name": name, "assets": count, "pct_of_assets_with_vlan": share, "cumulative_pct": cumulative}
            for rank, vlan, name, count, share, cumulative in stats.ranked()
        ],
        "other_vlan_related_client_attributes": dict(stats.related_attrs.most_common()),
    }
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)
    return path


KIND_LABELS = {
    "client": "Meraki network clients (@meraki.ntwkclient.*)",
    "device": "Meraki devices only (@meraki.dev.*)",
    "client+device": "Both client and device attributes",
    "other": "No @meraki.* attributes on the asset",
}


def print_report(cfg, stats, out_dir, paths):
    total = stats.total
    print()
    print("Meraki VLAN report  (search: {})".format(cfg.search))
    print("=" * 72)
    print("Assets exported ........ {:>10,}".format(total))
    print("  with a VLAN .......... {:>10,}  ({}%)".format(stats.with_vlan, pct(stats.with_vlan, total)))
    print("  without a VLAN ....... {:>10,}  ({}%)".format(stats.without_vlan, pct(stats.without_vlan, total)))
    print()
    print("By asset kind:")
    for kind in ("client", "client+device", "device", "other"):
        count = stats.kinds.get(kind)
        if not count:
            continue
        with_vlan = stats.kind_with_vlan[kind]
        print("  {:<46} {:>10,}   with VLAN {:,} / without {:,}".format(KIND_LABELS[kind], count, with_vlan, count - with_vlan))
    if stats.connections:
        print()
        print("Clients by connection type (newest Meraki record):")
        print("  {:<22} {:>10} {:>12}".format("", "with VLAN", "without"))
        for connection, counts in sorted(stats.connections.items(), key=lambda item: -sum(item[1].values())):
            print("  {:<22} {:>10,} {:>12,}".format(connection, counts["with"], counts["without"]))
    if stats.multi_vlan:
        print()
        print("Assets with more than one VLAN across their Meraki records: {:,} "
              "(newest record counted; all values are in the all_vlans column)".format(stats.multi_vlan))
    print()
    print("Distinct VLANs: {:,}".format(len(stats.vlan_counts)))
    limit = cfg.top if cfg.top > 0 else len(stats.vlan_counts)
    if stats.vlan_counts:
        print()
        print("Top {} VLANs by asset count:".format(min(limit, len(stats.vlan_counts))))
        print("  {:>4}  {:<10} {:<28} {:>10}  {:>8}  {:>6}".format("rank", "vlan", "name", "assets", "share", "cum."))
        for rank, vlan, name, count, share, cumulative in stats.ranked():
            if rank > limit:
                break
            print("  {:>4}  {:<10} {:<28} {:>10,}  {:>7}%  {:>5}%".format(rank, vlan[:10], name[:28], count, share, cumulative))
    if stats.related_attrs:
        print()
        print("Other VLAN-related client attributes seen (pass one as --name-attr if it holds the VLAN name):")
        for key, count in stats.related_attrs.most_common():
            print("  {:<48} on {:,} assets".format(key, count))
    print()
    print("Files written to {}/".format(out_dir))
    for path in paths:
        print("  " + os.path.basename(path))
    print()
    print("Matching runZero inventory queries:")
    print('  with VLAN:     {} AND has:"{}"'.format(cfg.search, cfg.vlan_attr))
    print('  without VLAN:  {} AND NOT has:"{}"'.format(cfg.search, cfg.vlan_attr))
    print('  one VLAN:      {}:="<vlan>"'.format(cfg.vlan_attr))


def main():
    cfg = parse_args()
    ssl_context = ssl._create_unverified_context() if cfg.insecure else None
    os.makedirs(cfg.out_dir, exist_ok=True)
    started = time.time()
    log("Exporting {!r} from {} ({})".format(cfg.search, cfg.url,
        "paged, {} per page".format(cfg.page_size) if cfg.page_size > 0 else "JSONL stream"))
    try:
        stats = analyse(cfg, iter_assets(cfg, ssl_context), cfg.out_dir)
    except ExportError as err:
        log("Export failed: {}".format(err))
        if cfg.page_size == 0 and err.status == 0:
            log("Tip: long JSONL streams can be cut by proxies; try --page-size 5000")
        return 1
    except (OSError, ValueError) as err:
        log("Failed: {}".format(err))
        return 1
    log("Processed {:,} assets in {:.0f}s".format(stats.total, time.time() - started))
    paths = [
        write_summary(cfg, stats, cfg.out_dir),
        write_vlan_counts(stats, cfg.out_dir),
        os.path.join(cfg.out_dir, "assets_with_vlan.csv"),
        os.path.join(cfg.out_dir, "assets_without_vlan.csv"),
    ]
    print_report(cfg, stats, cfg.out_dir, paths)
    return 0


if __name__ == "__main__":
    sys.exit(main())

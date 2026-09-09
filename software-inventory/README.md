# Offline Software Inventory Converter

Convert the combined JSON produced by `software.exe` into a gzip-compressed
runZero scan-data archive for manual import through the console GUI.

This tool only reformats files. It does not collect inventory, run a scanner,
connect to target machines, start an HTTP service, or upload anything.
It uses Python 3.8 or later and only the Python standard library. Install Python
on the engineering laptop before taking it into the air-gapped environment.

## One-Time Setup

In the destination runZero console, create or select a custom integration to
identify this software data source. Make it available to the target organisation
and note its UUID. Reuse the same UUID on subsequent conversions.

This is the console integration's UUID, not its display name, its script's
`CONFIG.id`, or an API token. An existing appropriate source can be reused. No
Starlark script, integration credential, scheduled integration task, or Explorer
service is required by this converter. Do not invent a UUID: the destination
console must already know that integration.

## Convert and Import

1. At the site, run the CLI Scanner normally and collect the combined software
   JSON with `software.exe`. Keep both original exports.
2. Run the converter on the collecting laptop:

   ```powershell
   py -3 .\convert_software_inventory.py "C:\Exports\software_inventory 1.json" --integration-id "UUID-FROM-CONSOLE"
   ```

   On macOS/Linux use `python3` instead of `py -3`. The UUID is the only console
   value the converter needs, and it does not use it to connect to the console.

3. The output is `C:\Exports\software_inventory 1.runzero.json.gz`. Use `-o`
   to choose a different filename. Existing output files are not overwritten.
4. Transfer both exports using the site's approved process. Import the CLI
   network scan first and let it finish processing.
5. In the same organisation/site, choose **Inventory > Import > runZero scan
   data** (`/import/`) and select the converted software archive. Do not use
   **Bulk asset update CSV** (`/importAssetCSV/`); that path updates asset
   properties, not installed software records.
6. Check the task result and each intended asset's software inventory. The
   archive carries MAC/IP/hostname data so runZero can apply its normal matching
   rules. Unmatched records may create new assets; a file converter cannot
   confirm matches against a console it does not contact.

## Data Handling

- Accepts the complete machine array, or a single machine object, including
  PowerShell UTF-8 BOM and UTF-16 JSON. There is no two-machine limit. The input
  JSON is read into memory once; archive records are written one machine at a
  time, with no second complete output array.
- Emits native scan-result records with embedded software data. It is not just
  the original JSON placed in a gzip file, and it is not the separate custom
  asset API upload format.
- Maps `Name`, `Publisher`, and `Version` to software product, vendor, and version.
  Preserves supplied install dates as software attributes without inventing an
  installation time or timezone. Software IDs are deterministic.
- Uses the MAC address, or IP address when no MAC exists, for the `sw-inv:` source
  ID. Keeps the hostname for correlation. Rejects duplicate source identities,
  malformed records, and machines without a MAC/IP rather than publishing a
  partial archive. Existing normalised IDs remain the same across conversions.
- Retains `CollectedAt` and the reported system details as source attributes.
  System details are not forced into the asset's OS/hardware fingerprint fields.
- Uses `CollectedAt` for each scan record's timestamp. An explicit timezone offset
  is honoured. With the sample's offset-less timestamps, Python uses the converting
  computer's local timezone, including the rules for the collection date. Run on
  the collecting laptop without changing its timezone; prefer explicit offsets in
  future collector exports. No timezone is guessed from the machine's IP address.
- Requires a complete software snapshot per machine. An explicit `Software: []`
  is valid and can clear previously imported custom software. Missing/null lists
  are rejected. The collector must not use an empty array for a failed collection.

Use the correct organisation/site, particularly where networks reuse IP addresses.
The supplied sample has two machines with the same hostname and different MACs/IPs;
verify they remain separate after the first GUI import. Also pilot coexistence
before importing onto assets that already receive software from other custom
integration feeds; source reconciliation is controlled by the deployed console.

## Development Verification

These checks are not required on the customer laptop:

```sh
python3 -B -m unittest discover -s Jamie-runZero-Scripts/software-inventory -p 'test_convert_software_inventory.py' -v
```

An opt-in check in the sibling platform checkout runs the converter, decodes its
gzip archive with runZero's scan-data reader, and compares the records and parsed
software with the native `ImportAsset` conversion. Follow that repository's build
prerequisites before running it from the parent workspace:

```sh
go -C platform test ./research/software-inventory -count=1 -v \
  -args -software-converter "$PWD/Jamie-runZero-Scripts/software-inventory/convert_software_inventory.py" \
  -software-sample '/path/to/software_inventory 1.json'
```

The sample check expects the original two-machine, 27-package fixture. Customer
data is not embedded in this repository or uploaded by the tests. Validation
covers native file parsing and software conversion, not a live-console GUI import
or database merge. The format is based on the custom-integration scan archives
written by the [runZero CLI](https://help.runzero.com/docs/custom-integration-scripts/#cli-output).
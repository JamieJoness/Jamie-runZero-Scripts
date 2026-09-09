#!/usr/bin/env python3
"""Convert software.exe JSON to a runZero GUI scan-data import, entirely offline."""

import argparse
import base64
import datetime
import gzip
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import uuid


def text(record, key):
    value = record.get(key)
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError("{} must be a string or null".format(key))
    return value.strip()


def object_field(record, key):
    value = record.get(key)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("{} must be an object".format(key))
    return value


def encode_json(value):
    return json.dumps(value, ensure_ascii=True, separators=(",", ":")).encode("utf-8")


def normalize_mac(value):
    if not value:
        return ""
    digits = re.sub(r"[:.\-]", "", value).lower()
    if not re.fullmatch(r"[0-9a-f]{12}", digits):
        raise ValueError("MACAddress must contain six hexadecimal octets")
    if digits in ("000000000000", "ffffffffffff") or int(digits[:2], 16) & 1:
        raise ValueError("MACAddress is not a usable unicast address")
    return ":".join(digits[index:index + 2] for index in range(0, 12, 2))


def collection_timestamp(value):
    if not value or "T" not in value:
        raise ValueError("CollectedAt must be an ISO 8601 date and time")
    try:
        collected = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        seconds = int(collected.timestamp())
    except (ValueError, OverflowError, OSError) as error:
        raise ValueError("CollectedAt is not a valid date and time") from error
    timestamp = seconds * 1_000_000_000 + collected.microsecond * 1000
    if not 0 < timestamp < 2 ** 63:
        raise ValueError("CollectedAt is outside the runZero timestamp range")
    return timestamp, collected.tzinfo is None


def build_record(machine, integration_id):
    if not isinstance(machine, dict):
        raise ValueError("each machine inventory must be an object")
    packages = machine.get("Software")
    if not isinstance(packages, list):
        raise ValueError("Software must be an explicitly present, complete array")

    network = object_field(machine, "Network")
    address = text(network, "IPAddress")
    mac = normalize_mac(text(network, "MACAddress"))
    if address:
        parsed_address = ipaddress.ip_address(address)
        if (parsed_address.is_unspecified or parsed_address.is_loopback
                or parsed_address.is_multicast or address == "255.255.255.255"):
            raise ValueError("IPAddress is not a usable machine address")
        address = str(parsed_address)
    if not mac and not address:
        raise ValueError("a MACAddress or IPAddress is required for asset matching")

    collected_at = text(machine, "CollectedAt")
    timestamp, local_time = collection_timestamp(collected_at)
    info = {
        "id": "sw-inv:" + (mac or address),
        "_custom-integration-id": integration_id,
        "collectedAt": collected_at,
    }
    hostname = text(machine, "ComputerName")
    if hostname and hostname.lower() != "unknown":
        info["hostnames"] = hostname
    if mac:
        info["macAddresses"] = mac
    if address:
        info["ipAddresses"] = address
    if mac and address:
        info["macPairs"] = mac + "=" + address

    system = object_field(machine, "System")
    for field in ("SerialNumber", "OSName", "OSVersion", "Manufacturer", "Model"):
        value = text(system, field)
        if value:
            info["inventory" + field] = value

    software = []
    for index, package in enumerate(packages, 1):
        if not isinstance(package, dict):
            raise ValueError("software entry {} must be an object".format(index))
        name = text(package, "Name")
        if not name:
            raise ValueError("software entry {} has no Name".format(index))
        publisher = text(package, "Publisher")
        version = text(package, "Version")
        entry = {
            "id": hashlib.sha256(encode_json([publisher, name, version])).hexdigest(),
            "product": name,
            "vendor": publisher,
            "version": version,
        }
        install_date = package.get("InstallDate")
        if install_date is not None and not isinstance(install_date, (str, int)):
            raise ValueError("InstallDate must be a string, integer, or null")
        if isinstance(install_date, bool):
            raise ValueError("InstallDate must not be a boolean")
        if install_date:
            entry["customAttributes"] = {"install_date": str(install_date)}
        software.append(entry)

    if software:
        info["_software"] = base64.b64encode(encode_json(software)).decode("ascii")
    return {"type": "result", "ts": timestamp, "probe": "custom-integration", "info": info}, len(software), local_time


def convert(input_path, output_path, integration_id):
    integration = uuid.UUID(integration_id)
    if integration.int == 0:
        raise ValueError("integration ID must be the non-zero UUID from the destination console")
    input_path, output_path = Path(input_path), Path(output_path)
    if output_path.exists():
        raise ValueError("output already exists; choose another output filename")
    with input_path.open("rb") as source:
        machines = json.load(source)
    if isinstance(machines, dict):
        machines = [machines]
    if not isinstance(machines, list) or not machines:
        raise ValueError("input must contain at least one machine inventory")

    identities = set()
    package_count = 0
    empty_count = 0
    local_time_count = 0
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(dir=output_path.parent, suffix=".tmp", delete=False) as temporary:
            temporary_path = Path(temporary.name)
            with gzip.GzipFile(filename="", mode="wb", fileobj=temporary, mtime=0) as archive:
                for index, machine in enumerate(machines, 1):
                    try:
                        record, count, local_time = build_record(machine, str(integration))
                        identity = record["info"]["id"]
                        if identity in identities:
                            raise ValueError("duplicate machine identity " + identity)
                        identities.add(identity)
                    except ValueError as error:
                        raise ValueError("machine {}: {}".format(index, error)) from error
                    archive.write(encode_json(record) + b"\n")
                    package_count += count
                    empty_count += count == 0
                    local_time_count += local_time
        if output_path.exists():
            raise ValueError("output already exists; choose another output filename")
        os.replace(temporary_path, output_path)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()
    return len(machines), package_count, empty_count, local_time_count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="combined JSON produced by software.exe")
    parser.add_argument("--integration-id", required=True, help="custom integration UUID from the destination console (not an API token)")
    parser.add_argument("-o", "--output", type=Path, help="output archive; defaults to INPUT.runzero.json.gz")
    args = parser.parse_args()
    output = args.output or args.input.with_suffix(".runzero.json.gz")
    try:
        machines, packages, empty, local_time = convert(args.input, output, args.integration_id)
    except (ValueError, OSError, UnicodeError) as error:
        parser.exit(1, "Conversion failed: {}\n".format(error))
    print("Created {}: {} machines, {} software entries".format(output, machines, packages))
    if local_time:
        print("Used this computer's local timezone for {} offset-less CollectedAt values.".format(local_time))
    if empty:
        print("WARNING: {} machines explicitly report no software; importing can clear previous custom software.".format(empty))
    print("Import the CLI network scan first, then this archive via Inventory > Import > runZero scan data in the same organisation/site.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
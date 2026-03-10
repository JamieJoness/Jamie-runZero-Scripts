# This script pulls the last 'x' amount of completed/processed raw scan data .json.gz 
# Requires 'brew install jq' to run.
# Change the N (starting with the newest, number of tasks to download) BASE (if not EU console), TOKEN (Oranisation API Token) & OID (Organisation ID) before running
# in terminal run with ./runzero_task_data_export.sh and it'll create a folder in the same directory as the script

#!/usr/bin/env bash
set -euo pipefail

# How many completed scan tasks to download
N="${N:-*NUMBER OF SCANS TO DOWNLOAD*}"

# Enter source org/console details
BASE="https://console-eu.runzero.com/api/v1.0"
TOKEN="*YOUR RUNZERO API TOKEN*"

# Enter your oranisation ID below
OID="${OID:-*YOUR ORGANISATION ID*}"

OUTDIR="${OUTDIR:-scan_data_downloads}"
mkdir -p "${OUTDIR}"

oid_arg=()
if [[ -n "${OID}" ]]; then oid_arg=( --data-urlencode "_oid=${OID}" ); fi

echo "Fetching last ${N} completed scan tasks (status=processed, type:scan)..."

tasks_json="$(
  curl -sS -G \
    -H "Authorization: Bearer ${TOKEN}" \
    "${oid_arg[@]}" \
    --data-urlencode "status=processed" \
    --data-urlencode "search=type:scan" \
    "${BASE}/org/tasks"
)"

task_ids="$(
  echo "${tasks_json}" \
  | jq -r 'sort_by(.updated_at) | reverse | .[0:'"${N}"'][] | .id'
)"

count="$(echo "${task_ids}" | wc -l | tr -d ' ')"
if [[ "${count}" == "0" ]]; then
  echo "ERROR: No matching tasks found. Verify the org has completed scan tasks and your token has access." >&2
  exit 1
fi

echo "Found ${count} tasks. Downloading scan data to ./${OUTDIR} ..."

i=0
while IFS= read -r task_id; do
  i=$((i+1))
  echo ""
  echo "[${i}/${count}] Task: ${task_id}"

  # Hit the /data endpoint and capture headers/body.
  hdr="$(mktemp)"
  body="$(mktemp)"

  # Use -D to capture headers; do not follow redirects here because we want the Location if present.
  curl -sS -G -D "${hdr}" -o "${body}" \
    -H "Authorization: Bearer ${TOKEN}" \
    "${oid_arg[@]}" \
    "${BASE}/org/tasks/${task_id}/data" || true

  # Some consoles respond with a 302/303 Location header, others may return JSON containing {"url": "..."}
  url="$(grep -i '^location:' "${hdr}" | tail -n 1 | sed -E 's/^[Ll]ocation:[[:space:]]*//;s/\r$//')"
  if [[ -z "${url}" ]]; then
    url="$(jq -r '.url // empty' "${body}" 2>/dev/null || true)"
  fi

  rm -f "${hdr}" "${body}"

  if [[ -z "${url}" ]]; then
    echo "WARN: Could not resolve scan data URL for task ${task_id}. Skipping." >&2
    continue
  fi

  out="${OUTDIR}/${i}_${task_id}.json.gz"
  echo "Downloading: ${out}"
  curl -sS -L "${url}" -o "${out}"

  # Quick sanity check for gzip magic bytes (1f 8b). If not gz, still keep the file.
  if ! (head -c 2 "${out}" | od -An -tx1 | tr -d ' \n' | grep -qi '^1f8b'); then
    echo "NOTE: ${out} does not look gzipped. It may still be valid; review the file contents." >&2
  fi

done <<< "${task_ids}"

echo ""
echo "Done. Files saved to ./${OUTDIR}"

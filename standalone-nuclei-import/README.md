# Custom Integration: Nuclei Scan Results

Inbound runZero custom integration that imports the findings of a **Nuclei scan
you run yourself** into runZero, as vulnerabilities attached to the hosts Nuclei
scanned. Run Nuclei with whatever templates you like (including private ones
runZero does not ship), publish its output file where a runZero Explorer can
download it, and this integration pulls the results into your runZero
vulnerability inventory on a schedule.

It is independent of runZero's built-in Nuclei vulnerability scanning. The two
can run side by side: findings from this integration are attributed to your
custom integration, runZero's own findings stay attributed to runZero, and the
same host carries both.

## How it works

```
 Your environment                         runZero
 ┌──────────────────────────────┐         ┌──────────────────────────────────────┐
 │ 1. nuclei -list targets.txt  │         │ 3. Scheduled custom integration task │
 │        -jsonl -o results.jsonl│        │    runs this script on an Explorer   │
 │                              │         │                                      │
 │ 2. Publish results.jsonl at  │  HTTPS  │ 4. Script downloads the file, groups │
 │    a URL the Explorer can    │ ◄───────┤    findings by host, builds one      │
 │    reach (web server, S3,    │         │    asset per host with its           │
 │    Azure Blob, artifact      │         │    vulnerabilities                   │
 │    store, ...)               │         │                                      │
 └──────────────────────────────┘         │ 5. runZero merges each host onto the │
                                          │    asset it already knows by IP and  │
                                          │    hostname, and stores the findings │
                                          │    in the vulnerability inventory    │
                                          └──────────────────────────────────────┘
```

The script does **not** need to convert anything into a runZero import file.
It hands runZero the assets and vulnerabilities as objects, and the Explorer
packages, transports, and imports them exactly as it does for every other
custom integration.

One Nuclei result line is one finding. Findings for the same host are grouped
onto one asset, and a host's full set of findings is sent on every run. That
means a finding Nuclei no longer reports is removed from runZero on the next
import, which is what you want for a vulnerability scanner.

## runZero requirements

- Superuser access to the [Custom Integrations configuration](https://console.runzero.com/custom-integrations) in runZero.
- A runZero Platform licence (custom integrations are a Platform feature).
- An Explorer running **5.1.260818.0 or later** with network access to wherever you publish the Nuclei results file. A console-hosted task can also be used when the file is on a public (non-private-address) URL such as a cloud storage pre-signed link.

## Nuclei requirements

- A Nuclei output file in one of the two JSON formats Nuclei writes:
  - **JSON Lines**, one result per line, from `nuclei ... -jsonl -o results.jsonl` (recommended), or
  - the **JSON array export** from `nuclei ... -je results.json`.
  A gzip-compressed copy of either (`results.jsonl.gz`) is also accepted.
- The file published at an HTTP(S) URL the Explorer can download, either anonymously (for example an S3 or Azure Blob pre-signed URL), with a bearer token, or with HTTP basic authentication.
- Re-publish the file after every scan at the **same URL**, so the scheduled task always picks up the latest results.

Example scan command producing the right file:

```bash
nuclei -list targets.txt -jsonl -omit-raw -o results.jsonl
```

`-omit-raw` leaves the request/response pairs out of the file. The integration
never imports them (see [Imported data](#imported-data)), so this only makes
the file smaller and keeps scan credentials out of it.

### Closing the loop with runZero

To scan what runZero already knows, build the target list from a runZero
export and scan that. For example, every Windows asset in one site:

```bash
curl -s -H "Authorization: Bearer $RUNZERO_EXPORT_TOKEN" \
  "https://console.runzero.com/api/v1.0/export/org/assets.csv?search=os:Windows%20site:HQ&fields=address" \
  | tail -n +2 > targets.txt
nuclei -list targets.txt -jsonl -omit-raw -o results.jsonl
```

Scanning hosts runZero already has in its inventory is what makes the findings
merge onto the right assets (see [Asset identity](#asset-identity)).

## Steps

### Publish the results file

1. After each Nuclei run, copy `results.jsonl` to the location the Explorer will download it from. Common choices:
   - an internal web server or file share exposed over HTTPS;
   - an S3, Azure Blob, or Google Cloud Storage object with a pre-signed URL;
   - the artifact output of the CI job that runs Nuclei.
2. Note the URL, and how the download is authenticated (none, bearer token, or username and password).
3. Check the file is readable from the network the Explorer sits in, for example with `curl -I <url>`.

### runZero configuration

1. [Create the Custom Integration](https://console.runzero.com/custom-integrations/new).
   - Add a Name and Icon for the integration (for example "Nuclei Scan Results").
   - Toggle `Enable custom integration script` and paste [nuclei-results.star](nuclei-results.star).
   - Click `Validate` to ensure it has valid syntax.
   - Click `Save` to create the Custom Integration.
   - The script embeds its `CONFIG` block, so the credential form is generated automatically with the fields below.
2. [Create the Credential for the Custom Integration](https://console.runzero.com/credentials).
   - Select the option that matches your custom integration name, for example `Nuclei Scan Results Script Secrets`.
   - **Nuclei results URL** (`results_url`): the URL of the results file, for example `https://files.example.com/nuclei/latest.jsonl`.
   - **Download authentication** (`auth_type`): `none`, `bearer`, or `basic`. Pre-signed cloud storage URLs need `none`.
   - **Bearer token** (`bearer_token`): shown when `auth_type` is `bearer`; sent as `Authorization: Bearer <token>`.
   - **Username** / **Password** (`username`, `password`): shown when `auth_type` is `basic`.
   - **Minimum severity to import** (`min_severity`, default `info`): results below this Nuclei severity are not imported. `info` imports everything, including technology detections; `low` is a good choice if you only want weaknesses in the vulnerability inventory.
   - **Download timeout (seconds)** (`download_timeout`, default 120): raise it for very large result files or slow links.
   - The **TLS** and **HTTP** groups are the standard runZero options: disable TLS validation or supply your own CA for an internal server with a private certificate, pin a certificate fingerprint, present a client certificate, or set a User-Agent.
3. [Create the Custom Integration task](https://console.runzero.com/ingest/custom/).
   - Select the Credential and Custom Integration created in steps 1 and 2.
   - Set the task schedule to run shortly after your Nuclei scans finish.
   - Select the Explorer you would like the Custom Integration to run from (one that can reach the results URL).
   - Optionally enable **Exclude assets that cannot be merged into an existing asset** so that a host Nuclei scanned but runZero has never seen does not create a new asset. See [Asset identity](#asset-identity) for why you probably want this.
   - Click `Save` to kick off the first task.

### Running it from the command line

The runZero Explorer binary runs a script directly, which is the quickest way
to check a results file before scheduling anything. `--kwargs` is repeated once
per parameter:

```bash
runzero script --filename nuclei-results/nuclei-results.star \
  --kwargs results_url=https://files.example.com/nuclei/latest.jsonl \
  --kwargs auth_type=bearer \
  --kwargs bearer_token='<token>' \
  --kwargs min_severity=low \
  --custom-integration-id 1f2e3d4c-5b6a-7988-9a0b-1c2d3e4f5a6b \
  --output ./nuclei-run
```

`--output` writes the assets the run produced, `--overwrite` replaces a
directory from a previous run, and `--verbose` shows the request log. The last
two log lines of every run are the summary: how many hosts and findings were
reported, and how many results were skipped and why.

```bash
runzero script --filename nuclei-results/nuclei-results.star --validate
```

Validation checks the `CONFIG` block and the HTTP and TLS wiring against a
local dummy server. It ends with `the results file holds no results; no
findings to import`, because the dummy server does not serve Nuclei output.
The fixture tests below are the real offline check.

### Fixture tests

The scenarios under `nuclei-results/tests/fixtures/` run the real runZero CLI
against a local HTTP server that serves Nuclei-shaped results files, reusing
the fixture harness from the sibling `runzero-custom-integrations` checkout
without copying or modifying it. Run from `Jamie-runZero-Scripts`:

```bash
export PYTHONDONTWRITEBYTECODE=1 RUNZERO_SCANNER=/tmp/rumble-scanner
python3 nuclei-results/tests/run.py            # every scenario
python3 nuclei-results/tests/run.py happy      # one scenario
```

Set `RUNZERO_CUSTOM_INTEGRATIONS` when that repository is not a sibling
directory. The scenarios cover the full field mapping (CVE upper-casing, an
invalid CVE kept out of the `cve` field, CVSS 2 versus 3 placement,
description truncation, matcher-named ids, a hostname-only DNS finding, a UDP
finding, two virtual hosts on one IP, an IPv6 target, a duplicate finding, a
result with no template id, a loopback target, blank and unreadable lines, and
that request/response/curl are not imported), the JSON array export, a gzip
file, the severity threshold, a transient 503 followed by recovery, a refused
credential, an empty file, and a sign-in page served in place of the file.
Every scenario is run twice and the emitted ids must match, and the per-finding
assertions check the vulnerabilities inside each exported asset.

### What's next?

- You will see the task kick off on the [tasks](https://console.runzero.com/tasks) page like any other integration.
- Findings appear in the [vulnerability inventory](https://console.runzero.com/inventory/vulnerability). Search for them with `source:custom` or on the asset with `custom_integration:nuclei-results` (use the name you gave the integration).
- Each finding carries its Nuclei detail as attributes, searchable with the `nuclei.` prefix: for example `vulnerability` search `nuclei.template_id:CVE-2021-44228` or `nuclei.template_tags:log4j`.
- Each host carries `nuclei.findings`, `nuclei.highest_severity`, `nuclei.targets` (every hostname or URL Nuclei scanned on that host), and `nuclei.last_scanned`.

## Imported data

### Hosts

One asset per scanned host, keyed on the IP address Nuclei resolved (falling
back to the hostname when Nuclei reports no IP, as DNS templates do).

| Nuclei field | runZero field | Notes |
| --- | --- | --- |
| `ip` | network interface address | Loopback and link-local addresses are dropped; a result with no usable address or hostname is skipped. |
| `host` | hostname | Only when the target was given to Nuclei as a hostname or URL with a hostname. An IP target adds no hostname. Every virtual host scanned on one IP is collected onto that one asset. |
| derived | `nuclei.findings`, `nuclei.highest_severity`, `nuclei.targets`, `nuclei.last_scanned` | Per-host summary attributes. |

### Findings

One vulnerability per Nuclei result. The result fields are defined by Nuclei's
`ResultEvent` type ([pkg/output/output.go](https://github.com/projectdiscovery/nuclei/blob/dev/pkg/output/output.go))
and the template `info` block by the
[template syntax reference](https://github.com/projectdiscovery/nuclei/blob/dev/SYNTAX-REFERENCE.md).

| Nuclei field | runZero field | Notes |
| --- | --- | --- |
| `template-id` (+ `matcher-name`) | `id` | `tech-detect:nginx` when a template reports matcher names, otherwise the template id. Same template, port and transport on one host is one finding. |
| `info.name` | `name` | Falls back to the template id. |
| `type` | `category` | `http`, `dns`, `network`, `ssl`, ... |
| `info.description` | `description` | Truncated to runZero's 1024-character limit. |
| `info.remediation` | `solution` | Truncated to 1024 characters. |
| `info.classification.cve-id` | `cve` | The first id in runZero's `CVE-YYYY-NNNN` shape, upper-cased. All valid ids are kept in `nuclei.cve_ids`. |
| `info.classification.cvss-score` | `cvss3BaseScore`, or `cvss2BaseScore` when `cvss-metrics` is a CVSS 2.0 vector | runZero has no CVSS 4.0 field, so a v4 score is stored as `cvss3BaseScore`; `nuclei.cvss_metrics` keeps the full vector with its version. |
| `info.classification.cpe` | `cpe23` | |
| `info.severity` | `severityRank`, `riskRank` (0 info, 1 low, 2 medium, 3 high, 4 critical) and `severityScore`, `riskScore` (rank × 2) | The same scale runZero's own Nuclei engine uses. A missing or unrecognised severity is treated as `info`. |
| `info.severity` | `exploitable` | `true` for low and above: Nuclei confirmed the condition on the live target. `info` results are detections, not weaknesses. |
| `ip`, `port`, `host`/`matched-at` scheme | `serviceAddress`, `servicePort`, `serviceTransport` | `udp` when the target was a `udp://` URL, otherwise `tcp`. Results without a port (DNS templates) are attached to the host rather than a service. |
| `timestamp` | `firstDetectedTS`, `lastDetectedTS` | The time Nuclei found it. On re-import runZero keeps the earliest first-detected time it has seen. |
| the rest | `nuclei.*` attributes | `template_id`, `template_path`, `template_url`, `type`, `severity`, `matcher_name`, `extractor_name`, `extracted_results`, `matched_at`, `url`, `target`, `template_tags`, `authors`, `references`, `impact`, `cve_ids`, `cwe_ids`, `cvss_metrics`, `epss_score`, `epss_percentile`, `scanned_at`. Multi-valued fields are tab-separated, as runZero's own Nuclei engine stores them. |

**Deliberately not imported:** `request`, `response`, and `curl-command`. They
can contain the headers, cookies, and tokens the scan authenticated with, and
runZero attribute values are capped at 1024 characters in any case. Keep the
Nuclei output file if you need the raw evidence.

## Known limits

- **Each import is a complete snapshot per host.** For every host in the file, the findings in the file replace that host's previous findings from this integration. Hosts not in the file are left alone. Publish the full results of each scan, never a diff, and never publish a partial or failed scan's output: an empty file imports nothing (and removes nothing), but a file that lists a host with fewer findings removes the missing ones.
- **No MAC addresses.** Nuclei does not see layer 2, so correlation is by IP and hostname only. On DHCP networks, import soon after scanning so the IP still belongs to the same asset.
- **Hosts runZero has never seen become new assets** unless the task's **Exclude assets that cannot be merged into an existing asset** option is on. Those assets have only an IP (and perhaps a hostname) and Nuclei findings.
- **Stale scans may import nothing.** If your organization has a vulnerability expiration configured, findings whose Nuclei timestamp is older than it are dropped on import. Import promptly after scanning.
- **runZero's attribute limits apply:** descriptions and solutions are truncated to 1024 characters, names to 256, and every `nuclei.*` value to 1024 characters. Up to 1024 attributes per finding.
- **One CVE per finding.** runZero's vulnerability record holds a single CVE id. A template that lists several CVEs is imported with the first valid one in `cve`; the full list is in `nuclei.cve_ids`.
- **The results file is held in memory** on the Explorer while it is parsed, so a very large scan is a large download. Several hundred thousand lines is fine; split truly enormous scans into files per site, each with its own credential and task.
- **Vulnerabilities from other custom integrations.** All custom integrations share one runZero data source. If a *different* custom integration also writes vulnerabilities onto the same asset, importing Nuclei results may remove them (runZero compares the new per-asset list against everything that source previously stored on the asset). runZero's own scanner findings and the built-in third-party integrations (Tenable, Qualys, and so on) are separate sources and are unaffected. Test in a non-production organization first if you have another custom integration reporting vulnerabilities.
- **No Nuclei template links.** The vulnerability detail page shows a "Template Source" link only for runZero's own Nuclei findings, because the link points into runZero's template repository. `nuclei.template_url` and `nuclei.template_path` carry the equivalent for your templates.

## Asset identity

- Target entity: a scanned host, as identified by the IP address Nuclei resolved for it.
- Source ID field: none. Nuclei is a scanner, not an inventory; a result identifies its target only by `ip` (the address Nuclei resolved) and `host` (the hostname, IP, or URL the target was given as). Neither is a vendor asset id.
- Documentation evidence: Nuclei's `ResultEvent` type ([pkg/output/output.go](https://github.com/projectdiscovery/nuclei/blob/dev/pkg/output/output.go)) defines `host`, `ip`, `port`, `url`, and `matched-at` as the target fields of a result; there is no device or asset identifier.
- Uniqueness scope: a result's `ip` is unique only within the network the scan ran in. Two sites with overlapping RFC1918 space would collide, so each site should publish its own results file (see below).
- Cardinality: many results per host (one per finding, and one per virtual host scanned on that IP). All are grouped onto one asset by IP, so a host is emitted once per run.
- Stability: the IP survives nothing in particular. A DHCP lease change, a re-addressed server, or a scan run from a different network vantage point changes it.
- Reuse behavior: an IP released by one host is routinely assigned to another.
- Presence: `ip` is present on every network-reachable result. It is absent on DNS-only results, which carry only `host`; those fall back to the hostname.
- Final runZero ID: `nuclei:<results-url-hostname>:<ip or hostname>`. The results server hostname is the namespace, so two scan feeds (for example two sites, or two scanning teams) never collide, and the id is deterministic across runs of the same file.
- Missing-ID behavior: a result with no routable `ip` and no usable hostname (loopback and link-local targets, bare IP strings as names) is skipped and counted in the run summary. A result with no `template-id` is skipped, because a finding without an id cannot be tracked between runs.
- Match behavior (set once in `CONFIG`): `no-id-match no-id-break`. The id above is a per-run label, not an identity, so it must neither drive a merge nor block one. runZero correlates each host onto the asset it already knows by IP and hostname, which is the whole point of the integration: the findings land on the asset runZero already has for that host, alongside its services, software, and other sources.
- Verdict: derived, non-authoritative.

### What this means in practice

- Scan the hosts runZero already knows (build the target list from a runZero export) and import promptly, so the IP Nuclei saw is the IP runZero has for that asset.
- Turn on **Exclude assets that cannot be merged into an existing asset** on the task if you do not want Nuclei-only hosts (an IP with findings and nothing else) created as new assets.
- Keep one results file, one credential, and one task per network that has its own address space.

### Notes

- Each result line is decoded on its own. A blank or unreadable line is counted and skipped rather than aborting the import, and the count appears in the run summary. A file that contains no readable results at all fails the task (that is what an expired pre-signed URL serving a sign-in page looks like), so a broken feed cannot pass as a clean scan.
- The download is retried up to three times, three seconds apart, on a transient status (408, 425, 429, 500, 502, 503, 504). A 401 or 403 is reported once and fails the task with a hint to check the authentication settings. A connection failure (DNS, refused, TLS) also fails the task.
- Nuclei's file is text, not JSON, so the raw `http.get` builtin is used rather than runZero's `get_json` helper; the retry above replaces the one `get_json` would have provided.
- A gzip body is detected by its magic byte, not its file extension or `Content-Encoding`, so `.jsonl.gz` uploads to object storage work without any server configuration.
- A UTF-8 byte-order mark at the start of the file is tolerated.
- The script does not set the asset's first- or last-seen times; runZero records the import time, and the scan time is kept in `nuclei.last_scanned` and on each finding's detected timestamps.

### Assumptions

1. **Nuclei output field names.** The script reads the field names Nuclei writes today (`template-id`, `matched-at`, `extracted-results`, `info.classification.cve-id`, ...). These have been stable across Nuclei v2 and v3; a future rename would appear as findings with missing detail rather than a failed run.
2. **One severity scale.** Nuclei's `unknown` severity, and a result with no severity at all, are imported as `info`. Set `min_severity` to `low` or higher if you do not want them.
3. **Transport.** Nuclei does not record the transport protocol separately. `udp` is inferred from a `udp://` target or match URL; everything else is `tcp`, which is correct for HTTP, TLS, and the TCP network templates.
4. **CVSS version.** The CVSS score is placed by the version prefix of `cvss-metrics` (`CVSS:2.0/...` or the bare `2.0/...` form goes to the v2 field; everything else, including CVSS 3.x and 4.0, to the v3 field). A template with a score but no metrics string is treated as CVSS v3, which is what the public template library uses.

## Future

- **Service records.** Nuclei knows the port and scheme of every HTTP and network finding, so each finding could also create or enrich a runZero service on that port. Left out for now so that a Nuclei import never adds services that runZero's own scan did not see; the port is on each finding's `servicePort` already.
- **Push instead of pull.** For teams that would rather not host the results file, the same mapping can be driven from a Python script that runs after Nuclei and uploads straight to runZero's custom integration import API.

## API documentation

- Nuclei [Running Nuclei](https://docs.projectdiscovery.io/tools/nuclei/running): the output flags `-jsonl`, `-je`/`-json-export`, `-jle`/`-jsonl-export`, and `-omit-raw`.
- Nuclei `ResultEvent` ([pkg/output/output.go](https://github.com/projectdiscovery/nuclei/blob/dev/pkg/output/output.go)): the result fields `template-id`, `template-path`, `template-url`, `info`, `type`, `host`, `ip`, `port`, `url`, `matched-at`, `matcher-name`, `extractor-name`, `extracted-results`, `timestamp`, `request`, `response`, `curl-command`.
- Nuclei [template structure](https://docs.projectdiscovery.io/templates/structure#information) and the [template syntax reference](https://github.com/projectdiscovery/nuclei/blob/dev/SYNTAX-REFERENCE.md): the `info` block fields `name`, `author`, `severity`, `description`, `remediation`, `reference`, `tags`, and `classification` (`cve-id`, `cwe-id`, `cvss-metrics`, `cvss-score`, `epss-score`, `epss-percentile`, `cpe`).
- runZero [custom integration scripts](https://help.runzero.com/docs/custom-integration-scripts/) and the [Starlark library reference](https://help.runzero.com/docs/custom-integration-starlark-libraries/): `ImportAsset`, `Vulnerability`, field limits, and the `matchBehavior` policy.

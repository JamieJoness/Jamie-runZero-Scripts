# This is a runZero Custom Integration, please see https://github.com/runZeroInc/runzero-custom-integrations for details.
# This script was generated with AI.

CONFIG = {
    "id": "runzero-task-sync",
    "name": "runZero Task Sync",
    # "internal", not "inbound". This script reports no assets: it copies task
    # data from one runZero instance into another through that instance's own
    # API, exactly as its peers runzero-scan-passive-assets and
    # runzero-vulnerability-workflow do, and both of those declare "internal".
    # An "inbound" integration is one whose main() yields ImportAssets for the
    # platform to merge, which this never does.
    "type": "internal",
    "description": "Mirrors completed tasks between two runZero instances (SaaS to self-hosted, etc.), skipping tasks already synced.",
    "version": "1",
    "maturity": "beta",
    "minVersion": "5.1.260818.0",
    "params": [
        {
            "key": "src_url",
            "label": "Source runZero URL",
            "type": "url",
            "required": False,
            "default": "https://console.runzero.com",
        },
        {
            "key": "src_org_id",
            "label": "Source org ID",
            "type": "string",
            "required": True,
        },
        {
            "key": "src_task_search_filter",
            "label": "Source task search filter",
            "type": "string",
            "required": False,
            "default": "type:scan",
            "description": "Tasks page search syntax. Only processed tasks are considered, whatever the filter says.",
        },
        {
            "key": "dst_url",
            "label": "Destination runZero URL",
            "type": "url",
            "required": False,
            "default": "https://console.runzero.com",
        },
        {
            "key": "dst_org_id",
            "label": "Destination org ID",
            "type": "string",
            "required": True,
        },
        {
            "key": "dst_site_id",
            "label": "Destination site ID",
            "type": "string",
            "required": False,
            "description": "Import every task into this one site. Leave blank to mirror the source layout: each task lands in a destination site of the same name, created when missing.",
        },
        {
            "key": "max_mb_per_run",
            "label": "Max MB of task data per run",
            "type": "int",
            "required": False,
            "default": 2048,
            "description": "Stop once this much task data has been moved and leave the rest for the next run. The platform allows a run 4 GiB of downloads in total.",
        },
        {
            "key": "hide_tasks_on_sync",
            "label": "Hide source tasks after sync",
            "type": "bool",
            "required": False,
            "default": False,
        },
        {
            "key": "src_api_token",
            "label": "Source API token",
            "type": "secret",
            "required": True,
            "description": "Account or org token for the source instance",
        },
        {
            "key": "dst_api_token",
            "label": "Destination API token",
            "type": "secret",
            "required": True,
            "description": "Account or org token for the destination instance",
        },
    ],
    "includes": {
        "src_tls_": OPTIONS_TLS,
        "dst_tls_": OPTIONS_TLS,
        "src_http_": OPTIONS_HTTP,
        "dst_http_": OPTIONS_HTTP,
    },
}
load('http', http_get='get', http_post='post', http_put='put', 'get_json', 'bearer', 'url_encode')
load('gzip', gzip_decompress='decompress', gzip_compress='compress')
load('json', json_decode='decode')
load('kwargs', 'get_http_options', 'get_bool', 'get_int')

# Every import this script creates carries "<SYNC_MARKER><source task id>" in
# its description; a later run reads those back to learn what is already mirrored.
SYNC_MARKER = "runzero-task-sync:"
# /api/v1.0/org/tasks returns the newest rows up to this many, with no paging.
TASK_LIST_LIMIT = 1000
# The platform caps one HTTP response at 1 GiB; a larger body aborts the run.
MAX_TASK_BYTES = 1 << 30
# The only task types /org/tasks/{id}/data serves; anything else 404s forever.
DATA_TASK_TYPES = ["scan", "import", "connector", "sample"]

def console(url, org_id, token, config_kwargs, side):
    """One runZero console: base URL, org id, and the HTTP options carrying its token."""
    return {
        "url": url.rstrip("/"),
        "org_id": org_id,
        "options": get_http_options(config_kwargs, side + "_http_", side + "_tls_", {"Authorization": bearer(token)}),
    }

def api_url(c, path, **params):
    # The org id travels on every call: with an account-level token, a request
    # without ?_oid= resolves against the token's default org and 404s.
    params["_oid"] = c["org_id"]
    return "{}/api/v1.0/org{}?{}".format(c["url"], path, url_encode(params))

def with_headers(options, extra):
    merged = dict(options)
    merged["headers"] = dict(options.get("headers", {}), **extra)
    return merged

def list_tasks(c, search, **params):
    params["search"] = search
    data, err = get_json(api_url(c, "/tasks", **params), **c["options"])
    return data or [], err

def synced_task_ids(dst):
    """Source task ids already imported into dst, read back from the import descriptions.

    The second value is True when the list hit its row cap, in which case older
    imports are missing from it and have to be looked up one at a time.
    """
    tasks, err = list_tasks(dst, "type:import desc:" + SYNC_MARKER)
    if err:
        fail("runzero-task-sync: failed to list destination tasks: {}".format(err))
    ids = {}
    for task in tasks:
        description = task.get("description") or ""
        at = description.find(SYNC_MARKER)
        if at >= 0:
            ids[description[at + len(SYNC_MARKER):].split(" ")[0]] = True
    return ids, len(tasks) >= TASK_LIST_LIMIT

def is_synced(dst, task_id):
    tasks, err = list_tasks(dst, "type:import desc:" + SYNC_MARKER + task_id)
    if err:
        fail("runzero-task-sync: failed to look up task {} on the destination: {}".format(task_id, err))
    return len(tasks) > 0

def destination_sites(dst):
    """Site name -> id for the destination org."""
    sites, err = get_json(api_url(dst, "/sites", fields="id,name"), **dst["options"])
    if err:
        fail("runzero-task-sync: failed to list destination sites: {}".format(err))
    return {site.get("name"): site.get("id") for site in (sites or []) if site.get("id")}

def create_site(dst, name, src_url):
    created = http_put(
        api_url(dst, "/sites"),
        json={"name": name, "description": "Mirrored from {} by runzero-task-sync".format(src_url)},
        **dst["options"]
    )
    if not created or created.status_code != 200 or not created.body.startswith("{"):
        print("Failed to create destination site '{}': status {}".format(
            name, created.status_code if created else "no response"))
        return None
    return json_decode(created.body).get("id")

def sync_task(src, dst, task, site_id, hide_tasks_on_sync):
    """Mirror one task. Returns "ok", "no_data", or "failed".

    "no_data" is kept apart from "failed" because it is permanent: the task is
    processed, yet its data is gone (purged by retention) or was never served
    for its type. No later run can fix that, so it is not worth ending the task
    in error over. Everything else is retried on the next run.
    """
    task_id = task["id"]
    print("Pulling task {} ({})".format(task_id, task.get("name", "")))
    download = http_get(
        api_url(src, "/tasks/{}/data".format(task_id)),
        timeout=3600,
        **with_headers(src["options"], {"Accept": "application/octet-stream"})
    )
    if download and download.status_code == 404:
        print("Task has no downloadable data; skipping:", task_id)
        return "no_data"
    if not download or download.status_code != 200:
        print("Failed to download task:", task_id)
        return "failed"

    # The data endpoint redirects to object storage, and the platform presigns
    # the UNCOMPRESSED <type>_<task>_<site>.json object whenever the .gz one is
    # absent (models.Task.GetScanDataPresignedURL, still carrying its "remove
    # once all data and agents are migrated to gzip" TODO). Old tasks -- the
    # ones this integration exists to mirror -- therefore hand back plain
    # newline-delimited JSON, which always begins with "{". Starlark has no
    # exception handling, so calling gzip_decompress on that aborts the whole
    # run and every task after this one is silently never synced.
    unzipped = download.body
    if not unzipped:
        print("Task data was empty; skipping task:", task_id)
        return "failed"
    if unzipped[0:1] != "{":
        # Only a real gzip member (magic byte 0x1f) may reach gzip_decompress:
        # a 200 carrying an HTML proxy page or a truncated body would otherwise
        # raise, aborting the run and silently skipping every later task.
        if unzipped[0:1] != "\x1f":
            print("Task data was neither JSON nor gzip; skipping task:", task_id)
            return "failed"
        unzipped = gzip_decompress(unzipped)

    # The name keeps the source task recognizable; the description is the
    # marker synced_task_ids() reads, so the same task is never imported twice.
    print("Uploading task {}".format(task_id))
    upload = http_put(
        api_url(
            dst,
            "/sites/{}/import".format(site_id),
            name=task.get("name", ""),
            description="{}{} from {}".format(SYNC_MARKER, task_id, src["url"]),
        ),
        body=gzip_compress(unzipped),
        timeout=3600,
        **with_headers(dst["options"], {"Content-Type": "application/octet-stream", "Content-Encoding": "gzip"})
    )
    if not upload or upload.status_code != 200:
        print("Failed to upload task:", task_id)
        return "failed"

    print("Successfully synced task:", task_id)

    if hide_tasks_on_sync:
        hide = http_post(
            api_url(src, "/tasks/{}/hide".format(task_id)),
            **with_headers(src["options"], {"Content-Type": "application/json"})
        )
        if hide and hide.status_code == 200:
            print("Task hidden:", task_id)
        else:
            # A silent hide failure re-syncs the same task on every run with
            # no visible cause, so the failure has to reach the log.
            print("Failed to hide task {}: status {}".format(
                task_id, hide.status_code if hide else "no response"))

    return "ok"

def main(**kwargs):
    src = console(kwargs.get("src_url", "https://console.runzero.com"), kwargs["src_org_id"], kwargs["src_api_token"], kwargs, "src")
    dst = console(kwargs.get("dst_url", "https://console.runzero.com"), kwargs["dst_org_id"], kwargs["dst_api_token"], kwargs, "dst")
    dst_site_id = kwargs.get("dst_site_id") or ""
    # get_bool, not kwargs.get: a bool arrives as the string "false" on some
    # paths, and `if "false":` is TRUE in Starlark -- which silently hid every
    # source task on a run that never asked for it.
    hide_tasks_on_sync = get_bool(kwargs, "hide_tasks_on_sync", default=False)
    run_budget = get_int(kwargs, "max_mb_per_run", default=2048) * 1024 * 1024

    # Only processed tasks have data to download, so the status is filtered on
    # the server rather than discovered one 404 at a time.
    tasks, err = list_tasks(src, kwargs.get("src_task_search_filter", "type:scan"), status="processed")
    if err:
        # The task list IS the work list, so a failed read is the run and not a
        # source org with no matching tasks.
        fail("runzero-task-sync: failed to get tasks: {}".format(err))
    print("Found {} processed task(s) matching the filter".format(len(tasks)))
    if not tasks:
        print("No tasks found.")
        return
    if len(tasks) >= TASK_LIST_LIMIT:
        print("The source lists only its newest {} tasks; older ones become visible as synced tasks are hidden or the filter is narrowed".format(TASK_LIST_LIMIT))

    synced, truncated = synced_task_ids(dst)
    pending = []
    already = 0
    # Oldest first, so the destination processes the history in the order it
    # happened and a newer scan is never overwritten by an older one.
    for task in sorted(tasks, key=lambda t: t.get("created_at", 0)):
        task_id = task.get("id", "")
        if not task_id or task.get("type") not in DATA_TASK_TYPES:
            continue
        if task_id in synced or (truncated and is_synced(dst, task_id)):
            already += 1
            continue
        pending.append(task)
    print("{} task(s) already synced, {} to sync".format(already, len(pending)))
    if not pending:
        return

    sites = destination_sites(dst)
    if dst_site_id and dst_site_id not in sites.values():
        fail("runzero-task-sync: destination site {} not found in org {}".format(dst_site_id, dst["org_id"]))

    # Every task is attempted before the run is judged: one unreadable task
    # should not cost the rest of the batch. The count is what the task error
    # carries, so a partial sync is not filed as a complete one.
    moved = 0
    synced_count = 0
    skipped = 0
    failed = []
    for task in pending:
        task_id = task["id"]
        size = task.get("size_data") or 0
        if size > MAX_TASK_BYTES:
            print("Task {} data is {} bytes, over the 1 GiB per-download limit; skipping".format(task_id, size))
            skipped += 1
            continue
        # The first task always runs, so one task larger than the whole budget
        # still moves instead of blocking everything behind it forever.
        if moved and moved + size > run_budget:
            print("Reached the per-run data limit; the remaining tasks sync on the next run")
            break

        site_id = dst_site_id or sites.get(task.get("site_name"))
        if not site_id:
            site_name = task.get("site_name") or ""
            if not site_name:
                print("Task {} belongs to a source site that no longer exists; set a destination site ID to sync it".format(task_id))
                skipped += 1
                continue
            site_id = create_site(dst, site_name, src["url"])
            if not site_id:
                failed.append(task_id)
                continue
            sites[site_name] = site_id
            print("Created destination site '{}'".format(site_name))

        moved += size
        status = sync_task(src, dst, task, site_id, hide_tasks_on_sync)
        if status == "ok":
            synced_count += 1
        elif status == "no_data":
            skipped += 1
        else:
            failed.append(task_id)

    print("Synced {} task(s), skipped {}".format(synced_count, skipped))
    if failed:
        fail("runzero-task-sync: {} of {} task(s) did not sync: {}".format(
            len(failed), len(pending), ", ".join(failed[:10])))
    return None

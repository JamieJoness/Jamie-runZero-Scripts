#!/usr/bin/env python3
"""
runZero console user export
===========================

Exports the CONSOLE USERS (the accounts on the "Your team" page - the people
who log in) from each runZero console listed below to a CSV file.
Read-only: it only sends GET requests and changes nothing in any console.

Note: this is NOT the Inventory -> Users data (the AD / Entra users that
runZero discovers). That is a different export.

HOW TO USE
----------
1. In EACH console, a superuser creates an Account API key:
     Account -> Account settings (https://console.runzero.com/account)
     -> Account API keys -> generate. The key starts with "CT".
   Organization API keys (OT...) will not work - they cannot list users.

2. Fill in the CONSOLES block below: a name of your choice plus that
   console's key, one line per console. Lines with an empty key are skipped.

3. Run:   python3 runzero_export_console_users.py
   (Python 3.6 or newer, no extra packages needed.)

OUTPUT  (folder runzero_users_export_<timestamp>, or OUTPUT_DIR if set)
------
  <name>_users.csv          one file per console, one row per user
  all_consoles_users.csv    the same rows for every console in one file

Columns: console, user_id, email, first_name, last_name, superuser,
default_role, explicit_org_roles, effective_org_access, groups, mfa_enabled,
sso_only, created_at, last_login_at, last_activity_at (times are UTC).
Org roles are written as  Org A=role|Org B=role.  "explicit_org_roles" are
roles assigned directly to the user; "effective_org_access" is what they
actually get once the default role and group membership are included.

Do not commit this file with real keys in it.
"""

import csv
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone

# ============================================================
# EDIT THIS BLOCK
# ============================================================
CONSOLES = [
    {"name": "console-1", "token": ""},
    {"name": "console-2", "token": ""},
    {"name": "console-3", "token": ""},
    {"name": "console-4", "token": ""},
    {"name": "console-5", "token": ""},
    {"name": "console-6", "token": ""},
    {"name": "console-7", "token": ""},
]
CONSOLE_URL = "https://console.runzero.com"   # add "url": "https://..." to an entry to override per console
OUTPUT_DIR = ""                                # "" = ./runzero_users_export_<UTC timestamp>
# ============================================================

COLUMNS = ["console", "user_id", "email", "first_name", "last_name", "superuser", "default_role",
           "explicit_org_roles", "effective_org_access", "groups", "mfa_enabled", "sso_only",
           "created_at", "last_login_at", "last_activity_at"]


def api_get(url, token):
    req = urllib.request.Request(url, headers={"Authorization": "Bearer " + token, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    if not isinstance(data, list):
        raise ValueError("unexpected response from %s" % url)
    return data


def fmt_ts(v):
    try:
        v = int(v or 0)
    except (TypeError, ValueError):
        return ""
    return datetime.fromtimestamp(v, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S") if v > 0 else ""


def roles_to_text(value, org_names):
    """The API returns [{organization_id, role_name}] (older consoles: {org_id: role}).
    Turn either into 'Org A=role|Org B=role' using the organization names."""
    pairs = {}
    if isinstance(value, dict):
        pairs = {str(k): str(v or "") for k, v in value.items()}
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, dict) and item.get("organization_id"):
                pairs[str(item["organization_id"])] = str(item.get("role_name") or "")
    named = sorted((org_names.get(oid, oid), role) for oid, role in pairs.items())
    return "|".join("%s=%s" % pair for pair in named)


def user_row(console, u, org_names):
    return {
        "console": console,
        "user_id": u.get("id", ""),
        "email": u.get("email", ""),
        "first_name": u.get("first_name", ""),
        "last_name": u.get("last_name", ""),
        "superuser": "yes" if u.get("client_admin") else "no",
        "default_role": u.get("org_default_role", ""),
        "explicit_org_roles": roles_to_text(u.get("org_roles"), org_names),
        "effective_org_access": roles_to_text(u.get("effective_access"), org_names),
        "groups": "|".join(sorted(g.get("name", "") for g in u.get("groups") or [] if isinstance(g, dict))),
        "mfa_enabled": "yes" if u.get("mfa_enabled") else "no",
        "sso_only": "yes" if u.get("sso_only") else "no",
        "created_at": fmt_ts(u.get("created_at")),
        "last_login_at": fmt_ts(u.get("last_login_at")),
        "last_activity_at": fmt_ts(u.get("last_activity_at")),
    }


def write_csv(path, rows):
    # utf-8-sig so Excel opens accented names correctly
    with open(path, "w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def main():
    consoles = [c for c in CONSOLES if str(c.get("token") or "").strip()]
    if not consoles:
        sys.exit("No API keys filled in. Edit the CONSOLES block at the top of this script first.")
    names = [str(c.get("name") or "").strip() for c in consoles]
    if len(set(names)) != len(names) or "" in names:
        sys.exit("Every console in the CONSOLES block needs a unique, non-empty name.")

    out = OUTPUT_DIR or "runzero_users_export_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    os.makedirs(out, exist_ok=True)

    all_rows, failed = [], []
    for name, c in zip(names, consoles):
        base = str(c.get("url") or CONSOLE_URL).rstrip("/") + "/api/v1.0"
        token = str(c["token"]).strip()
        try:
            users = api_get(base + "/account/users", token)
            orgs = api_get(base + "/account/orgs", token)
        except urllib.error.HTTPError as e:
            why = ("key rejected - is it an Account API key (CT...) from this console?" if e.code in (401, 403)
                   else "HTTP %d from %s" % (e.code, base))
            print("%-15s FAILED: %s" % (name, why))
            failed.append(name)
            continue
        except (urllib.error.URLError, OSError, ValueError) as e:
            print("%-15s FAILED: %s (%s)" % (name, e, base))
            failed.append(name)
            continue

        org_names = {str(o["id"]): o.get("name", "") for o in orgs if isinstance(o, dict) and o.get("id")}
        rows = sorted((user_row(name, u, org_names) for u in users if isinstance(u, dict)),
                      key=lambda r: r["email"].lower())
        filename = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in name) + "_users.csv"
        write_csv(os.path.join(out, filename), rows)
        all_rows.extend(rows)
        print("%-15s %4d users -> %s" % (name, len(rows), filename))

    if all_rows:
        write_csv(os.path.join(out, "all_consoles_users.csv"), all_rows)
        print("\n%d users from %d console(s) -> all_consoles_users.csv" % (len(all_rows), len(consoles) - len(failed)))
        print("Output folder: " + os.path.abspath(out))
    if failed:
        print("\nFailed: " + ", ".join(failed))
        sys.exit(1)


if __name__ == "__main__":
    main()

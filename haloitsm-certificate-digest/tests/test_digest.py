import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT.parent.parent / "runzero-custom-integrations" / "tests"))

from harness.runner import INTEGRATION_ID, read_assets, run_scenario, scanner_path
from harness.server import FixtureServer, Route


ORG_ID = "11111111-1111-4111-8111-111111111111"
CERT_ID = "22222222-2222-4222-8222-222222222222"
OTHER_CERT_ID = "33333333-3333-4333-8333-333333333333"
ASSET_ID = "44444444-4444-4444-8444-444444444444"
SERVICE_ID = "55555555-5555-4555-8555-555555555555"
ANCHOR = "2000-01-03T09:00:00Z"


class DigestTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.scanner = scanner_path()

    def setUp(self):
        self.checked_at = int(time.time())
        self.server = FixtureServer({})
        self.base = self.server.start()
        self.addCleanup(self.server.stop)
        self.kwargs = {
            "runzero_url": self.base,
            "runzero_api_token": "fixture-runzero-token",
            "organization_ids": ORG_ID,
            "scope_label": "Production services",
            "digest_key": "certificate-renewals",
            "schedule_anchor_utc": ANCHOR,
            "halo_api_url": self.base + "/api",
            "halo_auth_url": self.base + "/auth",
            "halo_client_id": "fixture-client",
            "halo_client_secret": "fixture-secret",
            "halo_tickettype_id": "5",
            "halo_team_id": "4",
            "dry_run": "false",
            "single_runner_confirmed": "true",
        }
        anchor = int(datetime.fromisoformat(ANCHOR.replace("Z", "+00:00")).timestamp())
        period = anchor + (self.checked_at - anchor) // 604800 * 604800
        namespace = hashlib.sha256(json.dumps(
            ["runzero-certificates-v1", self.base, [ORG_ID], "certificate-renewals"],
            separators=(",", ":"),
        ).encode()).hexdigest()[:24]
        self.reference = "RZCERT-{}-{}".format(
            namespace, datetime.fromtimestamp(period, timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
        self.ticket = {
            "id": 123,
            "third_party_id_string": self.reference,
            "details": "Complete digest: " + self.reference,
            "tickettype_id": 5,
            "team_id": 4,
        }
        self.empty_lookup = {"record_count": 0, "tickets": []}
        self.found_lookup = {"record_count": 1, "tickets": [self.ticket]}
        self.certificate = {
            "id": CERT_ID, "organization_id": ORG_ID, "type": "x509",
            "fp_sha256": "aa" * 32, "cn": "renew.example.test",
            "names": ["renew.example.test", "api.example.test"],
            "subject": "CN=renew.example.test", "issuer": "CN=Example <CA>",
            "validity_end": self.checked_at + 3 * 86400,
            "serial": "abcdef", "public_key_algorithm": "rsaEncryption",
            "public_key_bits": 2048, "signature_algorithm": "sha256WithRSAEncryption",
        }
        self.service = {
            "service_id": SERVICE_ID, "service_asset_id": ASSET_ID,
            "service_organization_id": ORG_ID, "organization_id": ORG_ID,
            "service_address": "192.0.2.10", "service_port": 443,
            "service_transport": "tcp", "service_vhost": "renew.example.test",
            "service_protocol": ["https", "tls"], "service_summary": "Customer portal",
            "name": "portal-01", "site_name": "London", "alive": True,
            "last_seen": self.checked_at - 3600, "service_updated_at": self.checked_at - 3600,
        }

    def route(self, method, path, responses, query=None):
        match = {"method": method, "path": path}
        if query:
            match["query_contains"] = query
        self.server.routes.append(Route({"match": match, "responses": responses}))

    def configure(self, lookup=None, certificates=None, services=None, create_status=201):
        self.route("POST", "/auth/token", [{"json": {"access_token": "fixture-access-token"}}])
        self.route("GET", "/api/Tickets", [{"json": self.empty_lookup}], "search_summary=")
        self.route("GET", "/api/Tickets", [
            {"json": value} for value in (lookup or [self.empty_lookup])
        ], "deleted=false")
        self.route("GET", "/api/Tickets", [{"json": self.empty_lookup}], "deleted=true")
        self.route("GET", "/api/v1.0/export/org/certificates.json", certificates or [{
            "json": {"certificates": [self.certificate], "next_key": ""},
        }])
        self.route("GET", "/api/v1.0/export/org/services.json", services or [{
            "json": {"services": [self.service], "next_key": ""},
        }])
        self.route("POST", "/api/Tickets", [{"status": create_status, "json": {"id": 123}}])
        self.route("GET", "/api/Tickets/123", [{"json": self.ticket}])

    def run_script(self, error=None):
        with tempfile.TemporaryDirectory(prefix="halo-digest-test-") as directory:
            output = str(Path(directory) / "scan")
            command = [self.scanner, "script", "--filename", str(ROOT / "haloitsm-certificate-digest.star"),
                       "--custom-integration-id", INTEGRATION_ID, "--output", output]
            for key, value in self.kwargs.items():
                command.extend(["--kwargs", key + "=" + str(value)])
            result = subprocess.run(command, capture_output=True, text=True, timeout=90)
            log = result.stdout + result.stderr
            self.assertEqual(read_assets(output), [])
        if error:
            self.assertNotEqual(result.returncode, 0, log)
            self.assertIn(error, log)
        else:
            self.assertEqual(result.returncode, 0, log)
        for secret in ["fixture-secret", "fixture-runzero-token", "fixture-access-token"]:
            self.assertNotIn(secret, log)
        return log

    def posts(self):
        return [request for request in self.server.snapshot()
                if request["method"] == "POST" and request["path"] == "/api/Tickets"]

    def test_authentication_failure_fixture(self):
        passed, name, failures = run_scenario(
            str(ROOT / "tests" / "fixtures" / "auth-failure.json"), self.scanner, str(ROOT.parent))
        self.assertTrue(passed, name + ": " + "; ".join(failures))

    def test_complete_digest_and_repeat(self):
        other = dict(self.certificate, id=OTHER_CERT_ID, fp_sha256="bb" * 32,
                     validity_end=self.checked_at + 12 * 3600)
        self.configure(
            lookup=[self.empty_lookup, self.empty_lookup, self.found_lookup],
            certificates=[
                {"json": {"certificates": [self.certificate, self.certificate], "next_key": "second-page"}},
                {"json": {"certificates": [other], "next_key": ""}},
            ],
            services=[{"json": {"services": [self.service, self.service], "next_key": ""}}],
        )
        self.run_script()
        self.assertEqual(len(self.posts()), 1)
        ticket = json.loads(self.posts()[0]["body"])[0]
        self.assertEqual(ticket["third_party_id_string"], self.reference)
        self.assertEqual((ticket["tickettype_id"], ticket["team_id"]), (5, 4))
        self.assertIn("2 distinct certificates across 1 host and 1 service", ticket["details"])
        self.assertEqual(ticket["details"].count("SHA-256: " + "aa" * 32), 1)
        self.assertEqual(ticket["details"].count("SHA-256: " + "bb" * 32), 1)
        self.assertIn("less than 1 day", ticket["details"])
        self.assertIn("api.example.test", ticket["details"])
        self.assertIn("192.0.2.10 port 443/tcp", ticket["details"])
        self.assertIn("CN=Example &lt;CA&gt;", ticket["details_html"])
        self.assertNotIn("CN=Example <CA>", ticket["details_html"])
        self.assertIn(self.base + "/inventory/" + ORG_ID + "/" + ASSET_ID, ticket["details"])
        self.assertTrue(any("start_key=second-page" in request["query"] for request in self.server.snapshot()))
        self.run_script()
        self.assertEqual(len(self.posts()), 1)

    def test_no_results(self):
        self.configure(certificates=[{"json": {"certificates": [], "next_key": ""}}])
        self.assertIn("No certificates", self.run_script())
        self.assertEqual(self.posts(), [])

    def test_service_failure_prevents_partial_digest(self):
        self.configure(services=[{"status": 403, "json": {"error": "denied"}}])
        self.run_script(error="runZero services export failed (status 403)")
        self.assertEqual(self.posts(), [])

    def test_unknown_creation_is_not_retried(self):
        self.configure(create_status=503)
        self.run_script(error="POST was not retried")
        self.assertEqual(len(self.posts()), 1)

    def test_lost_creation_response_is_reconciled(self):
        self.configure(lookup=[self.empty_lookup, self.empty_lookup, self.found_lookup], create_status=503)
        self.run_script()
        self.assertEqual(len(self.posts()), 1)

    def test_failed_reconciliation_after_post_requires_manual_recovery(self):
        self.route("GET", "/api/Tickets", [{"json": self.empty_lookup}], "search_summary=")
        self.route("GET", "/api/Tickets", [
            {"json": self.empty_lookup},
            {"json": self.empty_lookup},
            {"status": 403, "json": {"error": "denied"}},
        ], "deleted=false")
        self.configure(create_status=503)
        log = self.run_script(error="stop automatic retries and reconcile this reference in Halo")
        self.assertIn("after a creation attempt for " + self.reference, log)
        self.assertEqual(len(self.posts()), 1)

    def test_cross_organisation_response_is_rejected(self):
        self.certificate["organization_id"] = OTHER_CERT_ID
        self.configure()
        self.run_script(error="outside the requested organisation")
        self.assertEqual(self.posts(), [])

    def test_invalid_expiry_prevents_partial_digest(self):
        self.certificate["validity_end"] = "invalid"
        self.configure()
        self.run_script(error="no usable SHA-256 fingerprint, X.509 type, or expiry")
        self.assertEqual(self.posts(), [])

    def test_preview_does_not_create(self):
        self.kwargs["dry_run"] = "true"
        self.kwargs["single_runner_confirmed"] = "false"
        self.configure()
        log = self.run_script()
        self.assertIn("PREVIEW ONLY", log)
        self.assertIn("PREVIEW BEGIN: " + self.reference, log)
        self.assertIn("PREVIEW END: " + self.reference, log)
        self.assertEqual(self.posts(), [])

    def test_preview_messages_preserve_long_unicode_lines(self):
        self.kwargs["dry_run"] = "true"
        certificate_name = "start-" + "\u00e9\u20ac\U00010348" * 1200 + "-finish"
        self.certificate["cn"] = certificate_name
        self.configure()
        log = self.run_script()
        messages = []
        for line in log.splitlines():
            _, separator, encoded = line.partition(" msg=")
            if separator:
                message, _ = json.JSONDecoder().raw_decode(encoded)
                if message.startswith("script: "):
                    messages.append(message.removeprefix("script: "))
        self.assertTrue(messages, log)
        self.assertTrue(all(len(message.encode("utf-8")) <= 3000 for message in messages))
        self.assertIn("1. " + certificate_name, "".join(messages))
        self.assertNotIn("\ufffd", "".join(messages))
        self.assertIn("Complete digest: " + self.reference, messages)
        self.assertIn("PREVIEW END: " + self.reference, messages)
        self.assertTrue(any("500 log messages" in message for message in messages))
        self.assertEqual(self.posts(), [])

    def test_shared_certificate_retains_distinct_hosts_and_service_pages(self):
        duplicate = dict(self.certificate, id=OTHER_CERT_ID)
        other_service = dict(self.service, service_id=OTHER_CERT_ID,
                             service_asset_id=OTHER_CERT_ID, service_address="192.0.2.11")
        self.configure(
            certificates=[{"json": {"certificates": [self.certificate, duplicate], "next_key": ""}}],
            services=[
                {"json": {"services": [self.service], "next_key": "more-services"}},
                {"json": {"services": [other_service], "next_key": ""}},
                {"json": {"services": [self.service, other_service], "next_key": ""}},
            ],
        )
        self.run_script()
        details = json.loads(self.posts()[0]["body"])[0]["details"]
        self.assertIn("1 distinct certificate across 2 hosts and 2 services", details)
        self.assertEqual(details.count("SHA-256: " + "aa" * 32), 1)
        self.assertEqual(details.count("Host: portal-01"), 2)
        self.assertIn("192.0.2.11 port 443/tcp", details)
        requests = self.server.snapshot()
        self.assertTrue(any("start_key=more-services" in request["query"] for request in requests))
        exports = [request for request in requests if "/export/org/" in request["path"]]
        self.assertTrue(all(parse_qs(request["query"])["_oid"] == [ORG_ID] for request in exports))

    def test_closed_ticket_prevents_recreation(self):
        self.ticket["status_id"] = 9
        self.ticket["summary"] = "Renamed by the remediation team"
        self.configure(lookup=[self.found_lookup])
        self.assertIn("already covers", self.run_script())
        self.assertEqual(self.posts(), [])
        self.assertFalse(any("/export/org/" in request["path"] for request in self.server.snapshot()))
        lookup_queries = [parse_qs(request["query"]) for request in self.server.snapshot()
                          if request["path"] == "/api/Tickets"]
        self.assertTrue(all(query["open_only"] == ["false"] and query["closed_only"] == ["false"]
                            for query in lookup_queries))

    def test_deleted_ticket_prevents_recreation(self):
        self.ticket["deleted"] = True
        self.route("GET", "/api/Tickets", [{"json": self.found_lookup}], "deleted=true")
        self.configure()
        self.assertIn("already covers", self.run_script())
        self.assertEqual(self.posts(), [])

    def test_missing_native_reference_stops_on_summary_marker(self):
        row = {"id": 123, "summary": "Certificate renewals [" + self.reference + "]"}
        self.route("GET", "/api/Tickets", [{"json": {"record_count": 1, "tickets": [row]}}], "search_summary=")
        self.configure()
        self.run_script(error="native reference could not be verified")
        self.assertEqual(self.posts(), [])

    def test_partial_halo_lookup_prevents_creation(self):
        self.configure(lookup=[{"record_count": 1, "tickets": []}])
        self.run_script(error="ended before its reported count")
        self.assertEqual(self.posts(), [])

    def test_duplicate_halo_references_across_pages_prevent_creation(self):
        other_ticket = dict(self.ticket, id=124)
        self.configure(lookup=[
            {"record_count": 2, "tickets": [self.ticket]},
            {"record_count": 2, "tickets": [other_ticket]},
        ])
        self.run_script(error="Multiple Halo tickets already carry")
        self.assertEqual(self.posts(), [])
        self.assertTrue(any("page_no=2" in request["query"] for request in self.server.snapshot()))

    def test_nullable_empty_halo_collection(self):
        self.configure(lookup=[{"record_count": 0, "tickets": None}])
        self.run_script()
        self.assertEqual(len(self.posts()), 1)

    def test_halo_rate_limit_is_retried_on_reads(self):
        self.route("GET", "/api/Tickets", [
            {"status": 429, "headers": {"Retry-After": "0"}, "json": {"error": "rate limit"}},
            {"json": self.empty_lookup},
        ], "deleted=false")
        self.configure()
        self.run_script()
        self.assertEqual(len(self.posts()), 1)

    def test_repeated_runzero_cursor_prevents_creation(self):
        self.configure(certificates=[{
            "json": {"certificates": [self.certificate], "next_key": "repeated"},
        }])
        self.run_script(error="pagination did not progress")
        self.assertEqual(self.posts(), [])

    def test_out_of_window_records_are_excluded(self):
        expired = dict(self.certificate, id=OTHER_CERT_ID, validity_end=self.checked_at - 1)
        distant = dict(self.certificate, id=SERVICE_ID, validity_end=self.checked_at + 15 * 86400)
        self.configure(certificates=[{
            "json": {"certificates": [expired, distant, self.certificate], "next_key": ""},
        }])
        self.run_script()
        details = json.loads(self.posts()[0]["body"])[0]["details"]
        self.assertIn("1 distinct certificate", details)
        self.assertNotIn("/certificates/" + OTHER_CERT_ID, details)
        self.assertNotIn("/certificates/" + SERVICE_ID, details)

    def test_service_scope_excludes_certificates_without_matching_endpoints(self):
        self.kwargs["service_search"] = "site:London or site:Manchester"
        self.configure(services=[{"json": {"services": [], "next_key": ""}}])
        self.assertIn("No certificates", self.run_script())
        self.assertEqual(self.posts(), [])
        request = next(request for request in self.server.snapshot() if request["path"].endswith("services.json"))
        self.assertEqual(parse_qs(request["query"])["search"],
                         ["(certificate_id:=" + CERT_ID + ") and (site:London or site:Manchester)"])

    def test_unknown_host_relationships_are_explicit(self):
        self.configure(services=[{"json": {"services": [], "next_key": ""}}])
        self.run_script()
        self.assertIn("No associated service was returned", json.loads(self.posts()[0]["body"])[0]["details"])

    def test_oversized_digest_is_not_truncated(self):
        self.kwargs["max_ticket_bytes"] = "1000"
        self.configure()
        self.run_script(error="complete digest exceeds max_ticket_bytes")
        self.assertEqual(self.posts(), [])

    def test_record_limit_prevents_partial_digest(self):
        self.kwargs["max_certificate_records"] = "1"
        self.configure(certificates=[{
            "json": {"certificates": [self.certificate, self.certificate], "next_key": ""},
        }])
        self.run_script(error="certificate_records safety limit was exceeded")
        self.assertEqual(self.posts(), [])

    def test_delivery_requires_single_runner_confirmation(self):
        self.kwargs["single_runner_confirmed"] = "false"
        self.configure()
        self.run_script(error="Confirm single_runner_confirmed")
        self.assertEqual(self.server.snapshot(), [])

    def test_ticket_readback_detects_missing_digest(self):
        self.ticket["details"] = "Truncated"
        self.configure()
        self.run_script(error="complete digest could not be verified")
        self.assertEqual(len(self.posts()), 1)

    def test_ticket_readback_detects_routing_change(self):
        self.ticket["team_id"] = 6
        self.configure()
        self.run_script(error="type or team differs from the requested routing")
        self.assertEqual(len(self.posts()), 1)

    def test_multi_organisation_digest_and_stable_organisation_order(self):
        other_org = "66666666-6666-4666-8666-666666666666"
        organizations = [ORG_ID, other_org]
        self.kwargs["organization_ids"] = other_org + "," + ORG_ID
        old_namespace = self.reference.split("-")[1]
        namespace = hashlib.sha256(json.dumps(
            ["runzero-certificates-v1", self.base, organizations, "certificate-renewals"],
            separators=(",", ":"),
        ).encode()).hexdigest()[:24]
        self.reference = self.reference.replace(old_namespace, namespace)
        self.ticket.update(third_party_id_string=self.reference, details="Complete digest: " + self.reference)
        other_certificate = dict(self.certificate, organization_id=other_org)
        other_service = dict(self.service, organization_id=other_org, service_organization_id=other_org)
        self.configure(
            lookup=[self.empty_lookup, self.empty_lookup, self.found_lookup],
            certificates=[
                {"json": {"certificates": [self.certificate], "next_key": ""}},
                {"json": {"certificates": [other_certificate], "next_key": ""}},
            ],
            services=[
                {"json": {"services": [self.service], "next_key": ""}},
                {"json": {"services": [other_service], "next_key": ""}},
            ],
        )
        self.run_script()
        details = json.loads(self.posts()[0]["body"])[0]["details"]
        self.assertIn("1 distinct certificate across 2 hosts and 2 services", details)
        self.assertIn(self.base + "/inventory/" + other_org + "/" + ASSET_ID, details)
        self.kwargs["organization_ids"] = ORG_ID + "," + other_org + "," + ORG_ID
        self.run_script()
        self.assertEqual(len(self.posts()), 1)

    def test_equivalent_weekly_anchor_does_not_change_reference(self):
        anchor = datetime.fromisoformat(ANCHOR.replace("Z", "+00:00")) - timedelta(days=7)
        self.kwargs["schedule_anchor_utc"] = anchor.strftime("%Y-%m-%dT%H:%M:%SZ")
        self.configure(lookup=[self.found_lookup])
        self.assertIn("already covers", self.run_script())
        self.assertEqual(self.posts(), [])

    def test_future_anchor_stops_before_network_access(self):
        self.kwargs["schedule_anchor_utc"] = datetime.fromtimestamp(
            self.checked_at + 86400, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.configure()
        self.run_script(error="first agreed weekly period has not started")
        self.assertEqual(self.server.snapshot(), [])

    def test_missing_fingerprint_prevents_name_based_grouping(self):
        del self.certificate["fp_sha256"]
        self.configure()
        self.run_script(error="no usable SHA-256 fingerprint")
        self.assertEqual(self.posts(), [])

    def test_missing_optional_values_are_reported_as_unknown(self):
        self.certificate.update(cn=None, names=None, issuer=None, subject=None,
                                public_key_algorithm=None, public_key_bits=None)
        self.service.update(name=None, names=None, service_vhost=None, service_protocol=None,
                            service_summary=None, site_name=None, last_seen=None, alive=False)
        self.configure()
        self.run_script()
        details = json.loads(self.posts()[0]["body"])[0]["details"]
        self.assertIn("Unnamed certificate", details)
        self.assertIn("Issuer: not recorded", details)
        self.assertIn("size not recorded", details)
        self.assertIn("Unnamed host", details)
        self.assertIn("Please confirm this endpoint is still in use", details)

    def test_halo_lookup_authentication_failure_prevents_creation(self):
        self.route("GET", "/api/Tickets", [{"status": 403, "json": {"error": "denied"}}])
        self.configure()
        self.run_script(error="Halo duplicate lookup failed (status 403)")
        self.assertEqual(self.posts(), [])

    def test_runzero_authentication_failure_prevents_creation(self):
        self.configure(certificates=[{"status": 401, "json": {"error": "denied"}}])
        self.run_script(error="runZero certificates export failed (status 401)")
        self.assertEqual(self.posts(), [])

    def test_midwalk_failure_prevents_partial_digest(self):
        self.configure(certificates=[
            {"json": {"certificates": [self.certificate], "next_key": "next-page"}},
            {"status": 403, "json": {"error": "denied"}},
        ])
        self.run_script(error="runZero certificates export failed (status 403)")
        self.assertEqual(self.posts(), [])

    def test_final_duplicate_check_prevents_creation(self):
        self.configure(lookup=[self.empty_lookup, self.found_lookup])
        self.assertIn("final duplicate check", self.run_script())
        self.assertEqual(self.posts(), [])

    def test_list_without_reference_is_verified_through_ticket_detail(self):
        self.configure(lookup=[{"record_count": 1, "tickets": [{"id": 123}]}])
        self.assertIn("already covers", self.run_script())
        self.assertTrue(any(request["path"] == "/api/Tickets/123" for request in self.server.snapshot()))
        self.assertEqual(self.posts(), [])

    def test_unfiltered_halo_results_fail_closed(self):
        self.ticket["third_party_id_string"] = "another-digest"
        self.configure(lookup=[self.found_lookup])
        self.run_script(error="did not honour the external-reference filter")
        self.assertEqual(self.posts(), [])

    def test_oauth_tenant_http_options_and_ticket_routing(self):
        self.kwargs.update(halo_tenant="fixture-tenant", halo_http_user_agent="halo-fixture-agent",
                           runzero_http_user_agent="runzero-fixture-agent", halo_client_id_for_ticket="7",
                           halo_site_id="8", halo_user_id="9", halo_priority_id="10")
        self.configure()
        self.run_script()
        requests = self.server.snapshot()
        token_request = next(request for request in requests if request["path"] == "/auth/token")
        self.assertEqual(parse_qs(token_request["query"]), {"tenant": ["fixture-tenant"]})
        self.assertEqual(parse_qs(token_request["body"])["scope"], ["read:tickets edit:tickets"])
        self.assertEqual(token_request["headers"]["user-agent"], "halo-fixture-agent")
        for request in requests:
            if "/export/org/" in request["path"]:
                self.assertEqual(request["headers"]["user-agent"], "runzero-fixture-agent")
                self.assertEqual(request["authorization"], "Bearer fixture-runzero-token")
            elif request["path"].startswith("/api/Tickets"):
                self.assertEqual(request["authorization"], "Bearer fixture-access-token")
        ticket = json.loads(self.posts()[0]["body"])[0]
        self.assertEqual({key: ticket[key] for key in ["client_id", "site_id", "user_id", "priority_id"]},
                         {"client_id": 7, "site_id": 8, "user_id": 9, "priority_id": 10})


if __name__ == "__main__":
    unittest.main()
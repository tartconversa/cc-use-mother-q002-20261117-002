from fastapi.testclient import TestClient

from cycleflow.api import create_app
from cycleflow.contracts import DomainError
from cycleflow.demo import END, START, event
from tests.common import ServiceCase


class RecallTests(ServiceCase):
    def notify(self, number=30, minute=40, recall_id="RC1", lot_id="M1", reason="supplier recall"):
        return self.ingest(number, "recall.notify", minute,
                           {"recall_id": recall_id, "lot_id": lot_id, "reason": reason})

    def revoke(self, number=31, minute=50, recall_id="RC1", reason="supplier withdrew notice"):
        return self.ingest(number, "recall.revoke", minute,
                           {"recall_id": recall_id, "reason": reason})

    def seed_completed_job(self, job_id="J1", quantity=4, passed=3, failed=1):
        self.seed_job(job_id, quantity, number=5, minute=4)
        self.complete_stages(job_id, base=6)
        return self.ingest(12, "test.record", 30, {"test_id": "T1", "job_id": job_id,
                                                   "plan_id": "P1", "passed": passed, "failed": failed})

    def test_notify_traces_reservations_jobs_and_tests(self):
        self.full_scenario()
        outcome = self.notify()
        self.assertEqual(outcome["state"], "accepted")
        impact = self.service.recall_impact("RC1")
        self.assertEqual(impact["state"], "active")
        self.assertEqual(impact["lot"]["lot_id"], "M1")
        self.assertEqual(impact["lot"]["supplier"], "local-motor")
        self.assertTrue(impact["lot"]["recalled"])
        self.assertEqual(impact["lot"]["active_recall_id"], "RC1")
        self.assertEqual(len(impact["jobs"]), 1)
        job = impact["jobs"][0]
        self.assertEqual(job["job_id"], "J1")
        self.assertEqual(job["category"], "consumed")
        self.assertEqual(job["reserved_from_lot"], 8)
        self.assertEqual(job["status_at_notification"], "completed")
        self.assertEqual([t["test_id"] for t in job["tests"]], ["T1"])
        quantities = impact["quantities"]
        self.assertEqual(quantities["lot_quantity"], 8)
        self.assertEqual(quantities["consumed_units"], 8)
        self.assertEqual(quantities["releasable_units"], 0)
        self.assertEqual(quantities["affected_prototype_units"], 4)
        self.assertEqual(quantities["tested_units"], 4)
        self.assertEqual(quantities["passed_units"], 3)
        self.assertEqual(quantities["failed_units"], 1)
        attribution = impact["supplier_attribution"]
        self.assertEqual(attribution["supplier"], "local-motor")
        self.assertEqual(attribution["shared_suppliers"], ["local-sensor"])
        self.assertEqual(attribution["attributed_passed_units"], 3)
        self.assertFalse(attribution["additive"])
        self.assertIn("not additive", attribution["note"])
        disposition = impact["disposition"]
        self.assertTrue(disposition["blocked_from_new_reservations"])
        self.assertTrue(disposition["pending_containment"])
        self.assertEqual(disposition["consumed_job_ids"], ["J1"])
        self.assertEqual([item["recall_id"] for item in self.service.recall_list("robot-arm")], ["RC1"])
        self.assertEqual(self.service.recall_list("other-product"), [])

    def test_releasable_and_consumed_classification(self):
        self.seed_design()
        self.seed_lots()
        self.seed_plan()
        self.seed_completed_job()
        self.seed_job("J2", quantity=1, number=13, minute=31)
        self.notify(number=14, minute=40)
        impact = self.service.recall_impact("RC1")
        by_job = {job["job_id"]: job for job in impact["jobs"]}
        self.assertEqual(by_job["J1"]["category"], "consumed")
        self.assertEqual(by_job["J2"]["category"], "releasable")
        self.assertEqual(by_job["J2"]["status_at_notification"], "open")
        quantities = impact["quantities"]
        self.assertEqual(quantities["releasable_units"], 2)
        self.assertEqual(quantities["consumed_units"], 8)
        self.assertEqual(quantities["reserved_units"], 10)
        self.assertEqual(quantities["lot_on_hand"], 2)
        self.assertEqual(quantities["affected_prototype_units"], 5)
        self.assertEqual(impact["disposition"]["releasable_job_ids"], ["J2"])

    def test_duplicate_notification_replays_and_conflicts(self):
        self.full_scenario()
        first = self.notify()
        again = self.notify()
        self.assertEqual(first["id"], again["id"])
        self.assertFalse(first["repeated"])
        self.assertTrue(again["repeated"])
        conflict = self.notify(number=32, minute=41)
        self.assertEqual(conflict["state"], "rejected")
        self.assertEqual(conflict["error_code"], "duplicate_recall")
        self.assertEqual(len(self.service.recall_list()), 1)
        self.assertEqual(self.service.recall_impact("RC1")["source_event_id"], first["id"])

    def test_revoke_lifecycle_is_idempotent(self):
        self.full_scenario()
        self.notify()
        revoked = self.revoke()
        self.assertEqual(revoked["state"], "accepted")
        impact = self.service.recall_impact("RC1")
        self.assertEqual(impact["state"], "revoked")
        self.assertIsNotNone(impact["revoked_at"])
        self.assertEqual(impact["revoke_reason"], "supplier withdrew notice")
        self.assertFalse(impact["disposition"]["blocked_from_new_reservations"])
        self.assertFalse(self.service.get_lot("M1")["recalled"])
        replay = self.revoke()
        self.assertTrue(replay["repeated"])
        duplicate = self.revoke(number=33, minute=51)
        self.assertEqual(duplicate["state"], "rejected")
        self.assertEqual(duplicate["error_code"], "already_revoked")
        self.assertEqual(len(self.service.recall_list()), 1)

    def test_revoke_arriving_before_notify_waits_for_it(self):
        self.full_scenario()
        early = self.revoke(number=31, minute=41)
        self.assertEqual(early["state"], "waiting")
        self.assertEqual(early["error_code"], "missing_dependency")
        self.notify(number=30, minute=40)
        retried = self.service.retry(early["id"])
        self.assertEqual(retried["state"], "accepted")
        self.assertEqual(self.service.recall_impact("RC1")["state"], "revoked")

    def test_revoke_cannot_precede_notice(self):
        self.full_scenario()
        self.notify(number=30, minute=40)
        outcome = self.revoke(number=31, minute=39)
        self.assertEqual(outcome["state"], "rejected")
        self.assertEqual(outcome["error_code"], "time_order")
        self.assertEqual(self.service.recall_impact("RC1")["state"], "active")

    def test_notify_for_unknown_lot_waits_then_retries(self):
        outcome = self.notify(lot_id="M9")
        self.assertEqual(outcome["state"], "waiting")
        self.assertEqual(outcome["error_code"], "missing_dependency")
        self.ingest(40, "lot.receive", 1, {"lot_id": "M9", "part": "motor",
                                           "supplier": "motors", "quantity": 2})
        retried = self.service.retry(outcome["id"])
        self.assertEqual(retried["state"], "accepted")
        self.assertEqual(self.service.recall_impact("RC1")["lot"]["lot_id"], "M9")

    def test_active_recall_excludes_lot_from_new_allocations(self):
        self.seed_design()
        self.seed_lots()
        self.seed_plan()
        self.seed_job()
        self.notify()
        denied = self.seed_job("J2", quantity=1, number=21, minute=31)
        self.assertEqual(denied["error_code"], "shortage")
        lot = self.service.get_lot("M1")
        self.assertTrue(lot["recalled"])
        self.assertEqual(lot["active_recall_id"], "RC1")
        capacity = self.service.capacity("R1", 1, "2026-01-12T09:45:00Z")
        self.assertEqual(capacity["max_units"], 0)
        self.revoke()
        retried = self.service.retry(denied["id"])
        self.assertEqual(retried["state"], "accepted")
        lots = {row["lot_id"] for row in self.service.get_job("J2")["reservations"]}
        self.assertEqual(lots, {"M1", "S1"})

    def test_recalled_lot_blocks_publish_until_revoked(self):
        report = self.full_scenario()
        self.assertTrue(report["quality"]["publishable"])
        self.notify()
        blocked = self.service.report("robot-arm", START, END)
        issues = [item for item in blocked["quality"]["issues"] if item["code"] == "recalled_lot"]
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["subject"], "J1")
        self.assertEqual(issues[0]["recall_id"], "RC1")
        self.assertFalse(blocked["quality"]["publishable"])
        with self.assertRaises(DomainError) as caught:
            self.service.publish("jan", "robot-arm", START, END, blocked["fingerprint"])
        self.assertEqual(caught.exception.code, "quality_block")
        self.revoke()
        restored = self.service.report("robot-arm", START, END)
        self.assertTrue(restored["quality"]["publishable"])

    def test_frozen_release_implicated_but_not_rewritten(self):
        report = self.full_scenario()
        frozen = self.service.publish("jan", "robot-arm", START, END, report["fingerprint"])
        self.notify()
        impact = self.service.recall_impact("RC1")
        self.assertEqual([item["release_id"] for item in impact["frozen_reports"]], ["jan"])
        implicated = impact["frozen_reports"][0]
        self.assertEqual(implicated["fingerprint"], frozen["fingerprint"])
        self.assertTrue(implicated["current_differs"])
        same = self.service.get_release("jan")
        self.assertEqual(same["fingerprint"], frozen["fingerprint"])
        self.assertEqual(same["report"], frozen["report"])
        self.assertTrue(self.service.shelf.freshness("jan")["current_differs"])

    def test_late_test_counted_once_after_notification(self):
        self.seed_design()
        self.seed_lots()
        self.seed_plan()
        self.seed_job()
        self.complete_stages()
        self.notify()
        impact = self.service.recall_impact("RC1")
        self.assertEqual(impact["quantities"]["tested_units"], 0)
        self.assertEqual(impact["jobs"][0]["category"], "consumed")
        self.assertEqual(impact["jobs"][0]["status_at_notification"], "awaiting_test")
        self.ingest(12, "test.record", 45, {"test_id": "T1", "job_id": "J1",
                                            "plan_id": "P1", "passed": 3, "failed": 1})
        impact = self.service.recall_impact("RC1")
        self.assertEqual(len(impact["jobs"]), 1)
        self.assertEqual(impact["quantities"]["tested_units"], 4)
        self.assertEqual(impact["quantities"]["affected_prototype_units"], 4)
        self.assertEqual(impact["supplier_attribution"]["attributed_tested_units"], 4)

    def test_scrap_after_notification_releases_reservation(self):
        self.seed_design()
        self.seed_lots()
        self.seed_plan()
        self.seed_job()
        self.notify()
        impact = self.service.recall_impact("RC1")
        self.assertEqual(impact["quantities"]["releasable_units"], 8)
        self.ingest(20, "job.scrap", 41, {"job_id": "J1", "reason": "recall containment"})
        impact = self.service.recall_impact("RC1")
        quantities = impact["quantities"]
        self.assertEqual(quantities["releasable_units"], 0)
        self.assertEqual(quantities["released_units"], 8)
        job = impact["jobs"][0]
        self.assertEqual(job["category"], "released")
        self.assertEqual(job["status_at_notification"], "open")
        self.assertEqual(impact["disposition"]["released_job_ids"], ["J1"])
        self.assertEqual(self.service.get_lot("M1")["available"], 12)
        denied = self.seed_job("J2", quantity=1, number=21, minute=42)
        self.assertEqual(denied["error_code"], "shortage")
        self.ingest(22, "lot.quarantine", 43, {"lot_id": "M1", "quarantined": True})
        impact = self.service.recall_impact("RC1")
        self.assertTrue(impact["disposition"]["lot_quarantined"])
        self.assertFalse(impact["disposition"]["pending_containment"])

    def test_rollup_dedupes_jobs_across_recalls_and_suppliers(self):
        self.ingest(1, "design.release", 0, {"design_id": "R1", "product": "robot-arm",
                                             "revision": 1, "parts": {"motor": 2, "sensor": 1}})
        self.ingest(2, "lot.receive", 1, {"lot_id": "M1", "part": "motor",
                                          "supplier": "motors-a", "quantity": 4})
        self.ingest(3, "lot.receive", 2, {"lot_id": "M2", "part": "motor",
                                          "supplier": "motors-b", "quantity": 8})
        self.ingest(4, "lot.receive", 3, {"lot_id": "S1", "part": "sensor",
                                          "supplier": "sensors", "quantity": 6})
        self.ingest(5, "plan.create", 4, {"plan_id": "P1", "product": "robot-arm",
                                          "revision": 1, "minimum_pass_rate": "0.75"})
        self.ingest(6, "job.open", 5, {"job_id": "J1", "design_id": "R1", "quantity": 4})
        self.complete_stages("J1", base=10)
        self.ingest(16, "test.record", 30, {"test_id": "T1", "job_id": "J1",
                                            "plan_id": "P1", "passed": 3, "failed": 1})
        self.notify(number=20, minute=40, recall_id="RC1", lot_id="M1")
        self.notify(number=21, minute=41, recall_id="RC2", lot_id="M2")
        impact = self.service.recall_impact("RC1")
        self.assertEqual(impact["supplier_attribution"]["shared_suppliers"], ["motors-b", "sensors"])
        rollup = self.service.recall_rollup("robot-arm")
        self.assertEqual(rollup["active_recalls"], 2)
        self.assertEqual(rollup["affected_jobs"], 1)
        self.assertEqual(rollup["affected_prototype_units"], 4)
        self.assertEqual(rollup["tested_units"], 4)
        self.assertEqual(len(rollup["jobs"]), 1)
        job = rollup["jobs"][0]
        self.assertEqual(job["recall_ids"], ["RC1", "RC2"])
        self.assertEqual(job["recalled_suppliers"], ["motors-a", "motors-b"])
        self.assertEqual(job["reserved_recalled_units"], 8)
        by_supplier = {item["supplier"]: item for item in rollup["suppliers"]}
        self.assertEqual(by_supplier["motors-a"]["attributed_passed_units"], 3)
        self.assertEqual(by_supplier["motors-b"]["attributed_passed_units"], 3)
        self.assertFalse(rollup["additive"])
        self.assertIn("not additive", rollup["note"])
        self.ingest(22, "recall.revoke", 42, {"recall_id": "RC2", "reason": "scope narrowed"})
        rollup = self.service.recall_rollup("robot-arm")
        self.assertEqual(rollup["active_recalls"], 1)
        self.assertEqual([item["supplier"] for item in rollup["suppliers"]], ["motors-a"])
        self.assertEqual(rollup["affected_prototype_units"], 4)

    def test_audit_accepts_recall_lifecycle(self):
        self.full_scenario()
        self.notify()
        self.assertTrue(self.service.audit("robot-arm")["ok"])
        self.revoke()
        audit = self.service.audit("robot-arm")
        self.assertTrue(audit["ok"], audit)
        self.assertEqual(audit["finding_count"], 0)

    def test_unknown_recall_raises_missing_dependency(self):
        with self.assertRaises(DomainError) as caught:
            self.service.recall_impact("RC9")
        self.assertEqual(caught.exception.code, "missing_dependency")

    def test_http_recall_endpoints(self):
        client = TestClient(create_app(str(self.path)))
        for item in [event(1, "design.release", 0, {"design_id": "R1", "product": "robot-arm",
                                                    "revision": 1, "parts": {"motor": 2, "sensor": 1}}),
                     event(2, "lot.receive", 1, {"lot_id": "M1", "part": "motor",
                                                 "supplier": "motors", "quantity": 12}),
                     event(3, "lot.receive", 2, {"lot_id": "S1", "part": "sensor",
                                                 "supplier": "sensors", "quantity": 6}),
                     event(4, "job.open", 4, {"job_id": "J1", "design_id": "R1", "quantity": 4}),
                     event(5, "recall.notify", 40, {"recall_id": "RC1", "lot_id": "M1",
                                                    "reason": "supplier recall"})]:
            response = client.post("/events", json=item)
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["state"], "accepted")
        listing = client.get("/recalls")
        self.assertEqual([item["recall_id"] for item in listing.json()["recalls"]], ["RC1"])
        impact = client.get("/recalls/RC1")
        self.assertEqual(impact.status_code, 200)
        self.assertEqual(impact.json()["quantities"]["releasable_units"], 8)
        rollup = client.get("/recalls/rollup", params={"product": "robot-arm"})
        self.assertEqual(rollup.status_code, 200)
        self.assertEqual(rollup.json()["affected_prototype_units"], 4)
        missing = client.get("/recalls/RC9")
        self.assertEqual(missing.status_code, 409)
        self.assertEqual(missing.json()["error"], "missing_dependency")

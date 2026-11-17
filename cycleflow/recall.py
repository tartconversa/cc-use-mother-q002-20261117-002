from collections import defaultdict

from .contracts import DomainError, MissingDependency, keys, required_text
from .inventory import InventoryRules
from .snapshot import ReportShelf


RELEASABLE_STATUSES = {"open", "in_progress"}
CONSUMED_STATUSES = {"awaiting_test", "completed", "failed"}
NON_ADDITIVE_NOTE = ("Recalled-supplier attribution overlaps when a job combines lots from "
                     "several suppliers or recalls; quantities are not additive across "
                     "suppliers or recalls")


class RecallRules:
    def __init__(self, db):
        self.db = db

    def notify(self, payload, occurred_at, event_id):
        keys(payload, {"recall_id", "lot_id", "reason"})
        recall_id = required_text(payload["recall_id"], "recall_id")
        lot_id = required_text(payload["lot_id"], "lot_id")
        reason = required_text(payload["reason"], "reason")
        lot = self.db.execute("SELECT * FROM lots WHERE lot_id=?", (lot_id,)).fetchone()
        if lot is None:
            raise MissingDependency("lot", lot_id)
        if occurred_at < lot["received_at"]:
            raise DomainError("time_order", "recall notice precedes lot receipt")
        existing = self.db.execute("SELECT recall_id FROM recalls WHERE recall_id=?",
                                   (recall_id,)).fetchone()
        if existing is not None:
            raise DomainError("duplicate_recall", "recall ID already recorded", 409)
        self.db.execute(
            "INSERT INTO recalls(recall_id,lot_id,reason,notified_at,event_id,state) VALUES(?,?,?,?,?,'active')",
            (recall_id, lot_id, reason, occurred_at, event_id))
        linked = self.db.execute(
            "SELECT r.job_id,r.quantity,j.status FROM reservations r JOIN jobs j ON j.job_id=r.job_id"
            " WHERE r.lot_id=? ORDER BY r.job_id", (lot_id,)).fetchall()
        self.db.executemany(
            "INSERT INTO recall_links(recall_id,job_id,quantity,job_status) VALUES(?,?,?,?)",
            [(recall_id, row["job_id"], row["quantity"], row["status"]) for row in linked])
        return {"recall_id": recall_id, "lot_id": lot_id, "state": "active",
                "linked_jobs": len(linked)}

    def revoke(self, payload, occurred_at, event_id):
        keys(payload, {"recall_id", "reason"})
        recall_id = required_text(payload["recall_id"], "recall_id")
        reason = required_text(payload["reason"], "reason")
        row = self.db.execute("SELECT * FROM recalls WHERE recall_id=?", (recall_id,)).fetchone()
        if row is None:
            raise MissingDependency("recall", recall_id)
        if row["state"] == "revoked":
            raise DomainError("already_revoked", "recall already revoked")
        if occurred_at < row["notified_at"]:
            raise DomainError("time_order", "revocation precedes notice")
        self.db.execute(
            "UPDATE recalls SET state='revoked',revoked_at=?,revoke_reason=?,revoke_event_id=? WHERE recall_id=?",
            (occurred_at, reason, event_id, recall_id))
        return {"recall_id": recall_id, "state": "revoked"}

    def dispatch(self, kind, payload, occurred_at, event_id):
        if kind == "recall.notify":
            return self.notify(payload, occurred_at, event_id)
        if kind == "recall.revoke":
            return self.revoke(payload, occurred_at, event_id)
        raise DomainError("unknown_kind", f"unsupported recall event {kind}")


class RecallDesk:
    def __init__(self, store):
        self.store = store
        self.shelf = ReportShelf(store)

    def _recall_row(self, db, recall_id):
        row = db.execute("SELECT * FROM recalls WHERE recall_id=?", (recall_id,)).fetchone()
        if row is None:
            raise MissingDependency("recall", recall_id)
        return dict(row)

    def _affected_jobs(self, db, recall):
        recall_id, lot_id = recall["recall_id"], recall["lot_id"]
        baseline = {row["job_id"]: dict(row) for row in db.execute(
            "SELECT job_id,quantity,job_status FROM recall_links WHERE recall_id=?", (recall_id,))}
        current = {row["job_id"]: row["quantity"] for row in db.execute(
            "SELECT job_id,quantity FROM reservations WHERE lot_id=?", (lot_id,))}
        jobs = []
        for job_id in sorted(set(baseline) | set(current)):
            job = db.execute(
                "SELECT j.*,d.product,d.revision FROM jobs j JOIN designs d ON d.design_id=j.design_id"
                " WHERE j.job_id=?", (job_id,)).fetchone()
            reserved_now = current.get(job_id, 0)
            if reserved_now and job["status"] in RELEASABLE_STATUSES:
                category = "releasable"
            elif reserved_now and job["status"] in CONSUMED_STATUSES:
                category = "consumed"
            else:
                category = "released"
            link = baseline.get(job_id)
            tests = [dict(row) for row in db.execute(
                "SELECT test_id,plan_id,tested_at,passed,failed,event_id FROM tests WHERE job_id=?"
                " ORDER BY tested_at,test_id", (job_id,))]
            jobs.append({"job_id": job_id, "design_id": job["design_id"],
                         "product": job["product"], "revision": job["revision"],
                         "status": job["status"], "job_quantity": job["quantity"],
                         "opened_at": job["opened_at"], "completed_at": job["completed_at"],
                         "reserved_from_lot": reserved_now,
                         "reserved_at_notification": link["quantity"] if link else None,
                         "status_at_notification": link["job_status"] if link else None,
                         "linked_at_notification": link is not None,
                         "category": category,
                         "tests": [{"test_id": t["test_id"], "plan_id": t["plan_id"],
                                    "tested_at": t["tested_at"], "passed": t["passed"],
                                    "failed": t["failed"], "source_event_id": t["event_id"]}
                                   for t in tests]})
        return jobs

    @staticmethod
    def _quantities(jobs, lot):
        def units(category):
            return sum(job["reserved_from_lot"] for job in jobs if job["category"] == category)
        released = sum(job["reserved_at_notification"] or 0 for job in jobs
                       if job["category"] == "released")
        tested = sum(t["passed"] + t["failed"] for job in jobs for t in job["tests"])
        passed = sum(t["passed"] for job in jobs for t in job["tests"])
        return {"lot_quantity": lot["quantity"], "lot_on_hand": lot["available"],
                "reserved_units": lot["reserved"], "releasable_units": units("releasable"),
                "consumed_units": units("consumed"), "released_units": released,
                "affected_jobs": len(jobs),
                "affected_prototype_units": sum(job["job_quantity"] for job in jobs),
                "tested_units": tested, "passed_units": passed,
                "failed_units": tested - passed}

    def _frozen_reports(self, db, jobs):
        found = {}
        for job in jobs:
            for row in db.execute(
                    "SELECT release_id FROM releases WHERE product=? AND window_start<=? AND window_end>?"
                    " ORDER BY created_at,release_id",
                    (job["product"], job["opened_at"], job["opened_at"])):
                found[row["release_id"]] = job["product"]
        reports = []
        for release_id in sorted(found):
            frozen = self.shelf.get(release_id)
            freshness = self.shelf.freshness(release_id)
            reports.append({"release_id": release_id, "product": found[release_id],
                            "window_start": frozen["report"]["window_start"],
                            "window_end": frozen["report"]["window_end"],
                            "created_at": frozen["created_at"],
                            "fingerprint": frozen["fingerprint"],
                            "current_fingerprint": freshness["current_fingerprint"],
                            "current_differs": freshness["current_differs"]})
        return reports

    def _impact(self, db, recall):
        lot = InventoryRules(db).get(recall["lot_id"])
        jobs = self._affected_jobs(db, recall)
        quantities = self._quantities(jobs, lot)
        supplier = lot["supplier"]
        tested_jobs = [job for job in jobs if job["tests"]]
        shared = sorted({row["supplier"] for job in tested_jobs for row in db.execute(
            "SELECT DISTINCT l.supplier FROM reservations r JOIN lots l ON l.lot_id=r.lot_id"
            " WHERE r.job_id=? AND l.supplier!=?", (job["job_id"], supplier))})
        attribution = {"supplier": supplier, "jobs": [job["job_id"] for job in tested_jobs],
                       "attributed_tested_units": sum(t["passed"] + t["failed"]
                                                      for job in tested_jobs for t in job["tests"]),
                       "attributed_passed_units": sum(t["passed"] for job in tested_jobs for t in job["tests"]),
                       "attributed_failed_units": sum(t["failed"] for job in tested_jobs for t in job["tests"]),
                       "shared_suppliers": shared, "additive": False, "note": NON_ADDITIVE_NOTE}
        active = recall["state"] == "active"
        disposition = {"lot_quarantined": lot["quarantined"],
                       "blocked_from_new_reservations": active,
                       "releasable_job_ids": [j["job_id"] for j in jobs if j["category"] == "releasable"],
                       "consumed_job_ids": [j["job_id"] for j in jobs if j["category"] == "consumed"],
                       "released_job_ids": [j["job_id"] for j in jobs if j["category"] == "released"],
                       "pending_containment": active and (quantities["releasable_units"] > 0
                                                          or not lot["quarantined"])}
        return {"recall_id": recall["recall_id"], "state": recall["state"],
                "reason": recall["reason"], "notified_at": recall["notified_at"],
                "revoked_at": recall["revoked_at"], "revoke_reason": recall["revoke_reason"],
                "source_event_id": recall["event_id"],
                "revoke_event_id": recall["revoke_event_id"],
                "lot": lot, "jobs": jobs, "quantities": quantities,
                "disposition": disposition, "supplier_attribution": attribution,
                "frozen_reports": self._frozen_reports(db, jobs),
                "note": NON_ADDITIVE_NOTE}

    def impact(self, recall_id):
        recall_id = required_text(recall_id, "recall_id")
        with self.store.read() as db:
            return self._impact(db, self._recall_row(db, recall_id))

    def list(self, product=None):
        if product is not None:
            product = required_text(product, "product")
        with self.store.read() as db:
            result = []
            for row in db.execute(
                    "SELECT r.*,l.part,l.supplier FROM recalls r JOIN lots l ON l.lot_id=r.lot_id"
                    " ORDER BY r.notified_at,r.recall_id"):
                recall = dict(row)
                jobs = self._affected_jobs(db, recall)
                if product is not None and not any(job["product"] == product for job in jobs):
                    continue
                result.append({"recall_id": recall["recall_id"], "lot_id": recall["lot_id"],
                               "part": recall["part"], "supplier": recall["supplier"],
                               "reason": recall["reason"], "state": recall["state"],
                               "notified_at": recall["notified_at"],
                               "revoked_at": recall["revoked_at"],
                               "source_event_id": recall["event_id"],
                               "affected_jobs": len(jobs),
                               "affected_prototype_units": sum(j["job_quantity"] for j in jobs)})
            return result

    def rollup(self, product=None):
        if product is not None:
            product = required_text(product, "product")
        with self.store.read() as db:
            active = [dict(row) for row in db.execute(
                "SELECT r.recall_id,r.lot_id,l.part,l.supplier,r.notified_at FROM recalls r"
                " JOIN lots l ON l.lot_id=r.lot_id WHERE r.state='active'"
                " ORDER BY r.notified_at,r.recall_id")]
            jobs = {}
            for recall in active:
                for row in db.execute(
                        "SELECT r.job_id,r.quantity FROM reservations r WHERE r.lot_id=?",
                        (recall["lot_id"],)):
                    entry = jobs.setdefault(row["job_id"], {"recall_ids": set(), "suppliers": set(),
                                                            "reserved_recalled_units": 0})
                    entry["recall_ids"].add(recall["recall_id"])
                    entry["suppliers"].add(recall["supplier"])
                    entry["reserved_recalled_units"] += row["quantity"]
            details = []
            for job_id in sorted(jobs):
                job = db.execute(
                    "SELECT j.*,d.product,d.revision FROM jobs j JOIN designs d ON d.design_id=j.design_id"
                    " WHERE j.job_id=?", (job_id,)).fetchone()
                if product is not None and job["product"] != product:
                    continue
                tests = [dict(t) for t in db.execute(
                    "SELECT test_id,passed,failed FROM tests WHERE job_id=? ORDER BY tested_at,test_id",
                    (job_id,))]
                details.append({**jobs[job_id], "job_id": job_id, "product": job["product"],
                                "revision": job["revision"], "status": job["status"],
                                "job_quantity": job["quantity"], "tests": tests})
            suppliers = defaultdict(lambda: {"recall_ids": set(), "jobs": set(),
                                             "tested": 0, "passed": 0, "failed": 0})
            for job in details:
                for supplier in job["suppliers"]:
                    group = suppliers[supplier]
                    group["recall_ids"] |= job["recall_ids"]
                    group["jobs"].add(job["job_id"])
                    for test in job["tests"]:
                        group["tested"] += test["passed"] + test["failed"]
                        group["passed"] += test["passed"]
                        group["failed"] += test["failed"]
            tested = sum(t["passed"] + t["failed"] for job in details for t in job["tests"])
            passed = sum(t["passed"] for job in details for t in job["tests"])
            return {"product": product, "active_recalls": len(active), "recalls": active,
                    "jobs": [{"job_id": job["job_id"], "product": job["product"],
                              "revision": job["revision"], "status": job["status"],
                              "job_quantity": job["job_quantity"],
                              "recall_ids": sorted(job["recall_ids"]),
                              "recalled_suppliers": sorted(job["suppliers"]),
                              "reserved_recalled_units": job["reserved_recalled_units"],
                              "tests": job["tests"]} for job in details],
                    "affected_jobs": len(details),
                    "affected_prototype_units": sum(job["job_quantity"] for job in details),
                    "tested_units": tested, "passed_units": passed, "failed_units": tested - passed,
                    "suppliers": [{"supplier": name, "recall_ids": sorted(group["recall_ids"]),
                                   "jobs": sorted(group["jobs"]),
                                   "attributed_tested_units": group["tested"],
                                   "attributed_passed_units": group["passed"],
                                   "attributed_failed_units": group["failed"]}
                                  for name, group in sorted(suppliers.items())],
                    "additive": False, "note": NON_ADDITIVE_NOTE}

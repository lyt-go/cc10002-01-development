"""入湖提交的回归测试：分片校验、发布原子性、摘要冲突、恢复与审计联动。

只依赖标准库 unittest + FastAPI TestClient，起完服务不需要额外装 pytest。
运行：cd backend && .venv/bin/python -m unittest tests.test_ingest -v
"""
from __future__ import annotations

import threading
import unittest

from fastapi.testclient import TestClient

from app.main import app
from app.services.ingest import IngestService
from app.store import store

SHA_A = "sha256:" + "a" * 64
SHA_B = "sha256:" + "b" * 64
SHA_C = "sha256:" + "c" * 64


class IngestApiTest(unittest.TestCase):
    def setUp(self) -> None:
        store.reset()
        self.client = TestClient(app)
        self.ingest = IngestService()

    # ---------------------------------------------------------------- helpers

    def _register(self, batch_no: str, parts: list[dict], **extra) -> dict:
        body = {
            "批次编号": batch_no,
            "来源系统": "订单库",
            "落地路径": "s3://raw-dw/order/2026-10-12",
            "文件格式": "csv",
            "业务日期": "2026-10-12",
            "表编号": "ODS-ORDER",
            "分片清单": parts,
        }
        body.update(extra)
        body["分片清单"] = [
            {**part, "原始路径": f"s3://raw-dw-ingest-test/{batch_no}/part-{part['分片序号']:04d}.csv"}
            if str(part.get("原始路径", "")).startswith("AUTO/") else part
            for part in body["分片清单"]
        ]
        response = self.client.post("/api/ingest/batches", json={"values": body})
        return response.json()

    @staticmethod
    def _part(no: int, rows: int, digest: str, path: str | None = None) -> dict:
        return {
            "分片序号": no,
            "原始路径": path or f"AUTO/{no}",
            "行数": rows,
            "字节大小": 1024 * no,
            "校验摘要": digest,
        }

    def _post(self, path: str, values: dict | None = None) -> dict:
        return self.client.post(path, json={"values": values or {}}).json()

    def _verify(self, batch_no: str, no: int, rows: int, **extra) -> dict:
        return self._post(f"/api/ingest/batches/{batch_no}/units/{no}/verify", {"行数": rows, **extra})

    # ---------------------------------------------------------------- 登记

    def test_register_records_source_manifest_and_units(self) -> None:
        result = self._register("LAND-T1", [self._part(1, 120, SHA_A), self._part(2, 80, SHA_B)])
        self.assertTrue(result["ok"], result["message"])
        entry = result["entry"]
        self.assertEqual(entry["进度"], "0/2")
        self.assertEqual(entry["原始行数"], 200)
        batch = next(row for row in store.rows("landing") if row["批次编号"] == "LAND-T1")
        self.assertEqual(batch["来源系统"], "订单库")
        self.assertEqual(batch["文件格式"], "csv")
        self.assertEqual(batch["字节大小"], 1024 + 2048)
        self.assertEqual(len(self.ingest._units_of("LAND-T1")), 2)
        self.assertEqual(
            [unit["原始路径"] for unit in self.ingest._units_of("LAND-T1")],
            [
                "s3://raw-dw-ingest-test/LAND-T1/part-0001.csv",
                "s3://raw-dw-ingest-test/LAND-T1/part-0002.csv",
            ],
        )

    def test_empty_file_duplicate_file_and_bad_algorithm_rejected(self) -> None:
        self.assertIn("空文件", self._register("LAND-T2", [self._part(1, 0, SHA_A)])["message"])
        self.assertTrue(self._register("LAND-T3", [self._part(1, 10, SHA_A)])["ok"])
        dup = self._register("LAND-T4", [self._part(1, 10, SHA_A, path="s3://raw-dw/order/2026-10-12/part-0001.csv")])
        self.assertFalse(dup["ok"])
        self.assertIn("重复文件", dup["message"])
        bad = self._register(
            "LAND-T5", [self._part(1, 10, "deadbeef") | {"摘要算法": "md5"}]
        )
        self.assertFalse(bad["ok"])
        self.assertIn("摘要算法", bad["message"])

    def test_cross_shard_row_count_mismatch_rejected(self) -> None:
        result = self._register(
            "LAND-T6",
            [self._part(1, 10, SHA_A), self._part(2, 10, SHA_B)],
            声明总行数=30,
        )
        self.assertFalse(result["ok"])
        self.assertIn("跨分片行数核对不一致", result["message"])

    def test_duplicate_batch_and_duplicate_partition_rejected(self) -> None:
        self.assertTrue(self._register("LAND-T7", [self._part(1, 5, SHA_A)])["ok"])
        again = self._register("LAND-T7", [self._part(1, 5, SHA_A)])
        self.assertFalse(again["ok"])
        part = self._post(
            "/api/ingest/batches/LAND-T7/units",
            self._part(1, 5, SHA_A, path="s3://other/part.csv"),
        )
        self.assertFalse(part["ok"])

    # ---------------------------------------------------------------- 校验与中断

    def test_interrupted_shard_keeps_batch_landing_and_ods_unavailable(self) -> None:
        self.assertTrue(self._register("LAND-T8", [self._part(1, 100, SHA_A), self._part(2, 200, SHA_B)])["ok"])
        ok = self._verify("LAND-T8", 1, 100)
        self.assertTrue(ok["ok"])
        failed = self._verify("LAND-T8", 2, 201)
        self.assertFalse(failed["ok"])
        progress = self.client.get("/api/ingest/batches/LAND-T8").json()
        self.assertEqual(progress["批次状态"], "落地中")
        self.assertEqual(progress["进度"], "1/2")
        self.assertIn("分片 2", progress["缺口说明"])
        self.assertEqual(progress["可用状态"], "不可用")
        self.assertIsNone(progress["生效版本"])
        self.assertIsNone(progress["version"])  # 没有发布任何版本
        # 已完成分片仍保留，但不能被 ODS 查询：没有生效版本
        self.assertEqual(progress["分片"][0]["状态"], "已完成")
        ods = self.client.get("/api/ods?keyword=ODS-ORDER").json()["items"][0]
        self.assertEqual(ods["可用状态"], "不可用")
        publish = self._post("/api/ingest/batches/LAND-T8/publish")
        self.assertFalse(publish["ok"])
        self.assertIn("保持不可用", publish["message"])

    def test_recovery_only_rechecks_unfinished_units(self) -> None:
        self.assertTrue(self._register("LAND-T9", [self._part(1, 100, SHA_A), self._part(2, 200, SHA_B)])["ok"])
        self.assertTrue(self._verify("LAND-T9", 1, 100)["ok"])
        self.assertFalse(self._verify("LAND-T9", 2, 201)["ok"])
        resume = self.client.get("/api/ingest/batches/LAND-T9/resume").json()
        self.assertEqual([row["分片序号"] for row in resume["需复核"]], [2])
        self.assertEqual([row["分片序号"] for row in resume["可复用"]], [1])
        self.assertFalse(resume["可重新发布"])

        reused = self._verify("LAND-T9", 1, 100)
        self.assertTrue(reused["ok"])
        self.assertTrue(reused["entry"]["reused"])
        self.assertTrue(self._verify("LAND-T9", 2, 200)["ok"])
        published = self._post("/api/ingest/batches/LAND-T9/publish")
        self.assertTrue(published["ok"], published["message"])

    def test_late_partition_registration_and_republish(self) -> None:
        self.assertTrue(self._register(
            "LAND-T10", [self._part(1, 100, SHA_A), self._part(2, 50, SHA_B)]
        )["ok"])
        self.assertTrue(self._verify("LAND-T10", 1, 100)["ok"])
        self.assertFalse(self._post("/api/ingest/batches/LAND-T10/publish")["ok"])
        late = self._post("/api/ingest/batches/LAND-T10/units", self._part(3, 30, SHA_C))
        self.assertTrue(late["ok"], late["message"])
        self.assertEqual(late["entry"]["进度"], "1/3")
        late_row = store.rows("ingest_units")[-1]
        self.assertTrue(late_row["迟到"])
        self.assertFalse(self._verify("LAND-T10", 2, 51)["ok"])
        self.assertTrue(self._verify("LAND-T10", 2, 50)["ok"])
        self.assertTrue(self._verify("LAND-T10", 3, 30)["ok"])
        # 迟到分片并入后声明总行数同步更新，不触发跨分片核对失败
        published = self._post("/api/ingest/batches/LAND-T10/publish")
        self.assertTrue(published["ok"], published["message"])
        self.assertEqual(published["entry"]["进度"], "3/3")

    def test_intra_shard_duplicate_primary_keys_reduced_in_final_row_count(self) -> None:
        self.assertTrue(self._register("LAND-T11", [self._part(1, 100, SHA_A), self._part(2, 100, SHA_B)])["ok"])
        self.assertTrue(self._verify("LAND-T11", 1, 100, 重复主键数=4)["ok"])
        self.assertTrue(self._verify("LAND-T11", 2, 100, 重复主键数=0)["ok"])
        published = self._post("/api/ingest/batches/LAND-T11/publish")
        self.assertTrue(published["ok"])
        version = self.ingest.list_versions(biz_date="2026-10-12", table_code="ODS-ORDER")[0]
        self.assertEqual(version["总行数"], 200)
        self.assertEqual(version["去重后行数"], 196)
        self.assertEqual(version["分片内重复主键数"], 4)
        self.assertEqual(version["摘要算法"], "sha256")
        self.assertTrue(version["校验时刻"])

    def test_all_primary_keys_duplicate_fails_unit(self) -> None:
        self.assertTrue(self._register("LAND-T12", [self._part(1, 100, SHA_A)])["ok"])
        result = self._verify("LAND-T12", 1, 100, 重复主键数=100)
        self.assertFalse(result["ok"])
        self.assertIn("主键", result["message"])

    # ---------------------------------------------------------------- 发布与冲突

    def test_publish_only_when_all_units_complete(self) -> None:
        self.assertTrue(self._register("LAND-T13", [self._part(1, 10, SHA_A), self._part(2, 20, SHA_B)])["ok"])
        publish = self._post("/api/ingest/batches/LAND-T13/publish")
        self.assertFalse(publish["ok"])
        self.assertTrue(self._verify("LAND-T13", 1, 10)["ok"])
        self.assertFalse(self._post("/api/ingest/batches/LAND-T13/publish")["ok"])
        self.assertTrue(self._verify("LAND-T13", 2, 20)["ok"])
        published = self._post("/api/ingest/batches/LAND-T13/publish")
        self.assertTrue(published["ok"], published["message"])
        entry = published["entry"]
        self.assertEqual(entry["批次状态"], "已落地")
        self.assertEqual(entry["可用状态"], "可用")
        self.assertEqual(entry["总行数"] if "总行数" in entry else entry["原始行数"], 30)
        self.assertTrue(entry["version"])
        versions = self.ingest.list_versions(biz_date="2026-10-11", table_code="ODS-ORDER")
        self.assertEqual(versions, [])

    def test_declared_digest_mismatch_blocks_publish(self) -> None:
        self.assertTrue(self._register("LAND-T14", [self._part(1, 10, SHA_A)])["ok"])
        self.assertTrue(self._verify("LAND-T14", 1, 10)["ok"])
        result = self._post("/api/ingest/batches/LAND-T14/publish", {"校验摘要": SHA_B})
        self.assertFalse(result["ok"])
        self.assertIn("摘要冲突", result["message"])
        progress = self.client.get("/api/ingest/batches/LAND-T14").json()
        self.assertEqual(progress["批次状态"], "摘要在途冲突")
        self.assertEqual(progress["可用状态"], "不可用")

    def test_conflict_keeps_first_complete_version(self) -> None:
        self.assertTrue(self._register("LAND-T15", [self._part(1, 10, SHA_A)])["ok"])
        self.assertTrue(self._verify("LAND-T15", 1, 10)["ok"])
        first = self._post("/api/ingest/batches/LAND-T15/publish")
        self.assertTrue(first["ok"])
        first_version = first["entry"]["version"]

        self.assertTrue(self._register("LAND-T16", [self._part(1, 10, SHA_B)])["ok"])
        self.assertTrue(self._verify("LAND-T16", 1, 10)["ok"])
        second = self._post("/api/ingest/batches/LAND-T16/publish")
        self.assertFalse(second["ok"])
        self.assertIn("先完整落库", second["message"])
        active = self.ingest.list_versions(biz_date="2026-10-12", table_code="ODS-ORDER")
        self.assertEqual([row["版本号"] for row in active if row["状态"] == "生效"], [first_version])
        self.assertEqual(len(active), 1)

    def test_published_version_never_rewritten(self) -> None:
        self.assertTrue(self._register("LAND-T17", [self._part(1, 10, SHA_A)])["ok"])
        self.assertTrue(self._verify("LAND-T17", 1, 10)["ok"])
        published = self._post("/api/ingest/batches/LAND-T17/publish")
        self.assertTrue(published["ok"])
        again = self._post("/api/ingest/batches/LAND-T17/publish")
        self.assertFalse(again["ok"])
        self.assertIn("不允许重写", again["message"])
        reverify = self._verify("LAND-T17", 1, 10, force=True)
        self.assertFalse(reverify["ok"])

    def test_same_digest_new_batch_reuses_version_without_new_version(self) -> None:
        self.assertTrue(self._register("LAND-T18", [self._part(1, 10, SHA_A)])["ok"])
        self.assertTrue(self._verify("LAND-T18", 1, 10)["ok"])
        first = self._post("/api/ingest/batches/LAND-T18/publish")
        self.assertTrue(first["ok"])
        self.assertTrue(self._register("LAND-T19", [self._part(1, 10, SHA_A)])["ok"])
        self.assertTrue(self._verify("LAND-T19", 1, 10)["ok"])
        reused = self._post("/api/ingest/batches/LAND-T19/publish")
        self.assertTrue(reused["ok"])
        self.assertTrue(reused["entry"].get("复用已发布版本"))
        versions = [row for row in self.ingest.list_versions() if row["业务日期"] == "2026-10-12" and row["表编号"] == "ODS-ORDER"]
        self.assertEqual(len(versions), 1)

    def test_concurrent_publish_same_business_day_only_one_active(self) -> None:
        for no, digest in ((1, SHA_A), (2, SHA_B)):
            batch_no = f"LAND-RACE-{no}"
            self.assertTrue(self._register(batch_no, [self._part(1, 10, digest)])["ok"])
            self.assertTrue(self._verify(batch_no, 1, 10)["ok"])
        outcome: list[dict] = []
        barrier = threading.Barrier(2)

        def worker(batch_no: str) -> None:
            try:
                barrier.wait()
                outcome.append({"ok": True, "entry": IngestService().publish(batch_no)})
            except Exception as exc:  # 冲突被拒绝也算正常结果，不能让线程把测试打挂
                outcome.append({"ok": False, "message": str(exc)})

        threads = [threading.Thread(target=worker, args=(f"LAND-RACE-{no}",)) for no in (1, 2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(len(outcome), 2)
        self.assertEqual(sum(1 for item in outcome if item["ok"]), 1, outcome)
        reason = next(item["message"] for item in outcome if not item["ok"])
        self.assertIn("先完整落库", reason)
        active = [
            row for row in self.ingest.list_versions(biz_date="2026-10-12", table_code="ODS-ORDER")
            if row["状态"] == "生效"
        ]
        self.assertEqual(len(active), 1)

    # ---------------------------------------------------------------- 审计与可用范围

    def test_audit_keeps_failure_reason_and_final_digest_in_one_chain(self) -> None:
        self.assertTrue(self._register("LAND-T20", [self._part(1, 100, SHA_A)])["ok"])
        self.assertFalse(self._verify("LAND-T20", 1, 101)["ok"])
        self.assertTrue(self._verify("LAND-T20", 1, 100)["ok"])
        published = self._post("/api/ingest/batches/LAND-T20/publish")
        self.assertTrue(published["ok"])
        trail = self.client.get("/api/ingest/batches/LAND-T20/audit").json()
        self.assertGreaterEqual(len(trail), 3)
        self.assertTrue(all(row.get("关联批次") == "LAND-T20" for row in trail))
        self.assertIn("行数不一致", " ".join(row["变更原因"] for row in trail))
        final = self.ingest.list_versions(biz_date="2026-10-12", table_code="ODS-ORDER")[0]
        self.assertEqual(trail[-1]["变更后值"], final["版本号"])
        linked = self.client.get("/api/auditlog?batch_no=LAND-T20").json()
        self.assertEqual(linked["total"], len(trail))

    def test_availability_and_ods_list_reflect_same_submission(self) -> None:
        self.assertTrue(self._register("LAND-T21", [self._part(1, 100, SHA_A), self._part(2, 100, SHA_B)])["ok"])
        self.assertTrue(self._verify("LAND-T21", 1, 100)["ok"])
        self.assertFalse(self._verify("LAND-T21", 2, 99)["ok"])
        available = self.client.get("/api/ingest/availability").json()
        order = next(row for row in available if row["表编号"] == "ODS-ORDER")
        self.assertEqual(order["可用状态"], "不可用")
        self.assertIn("进度 1/2", order["可用范围"])
        self.assertTrue(self._verify("LAND-T21", 2, 100)["ok"])
        published = self._post("/api/ingest/batches/LAND-T21/publish")
        self.assertTrue(published["ok"])
        available = self.client.get("/api/ingest/availability").json()
        order = next(row for row in available if row["表编号"] == "ODS-ORDER")
        self.assertEqual(order["可用状态"], "可用")
        self.assertEqual(order["当前生效版本"], published["entry"]["version"])


if __name__ == "__main__":
    unittest.main()

"""数仓入湖：把落地批次与贴源表版本合并成一次「入湖提交」。

设计要点
--------
* 一次入湖提交对应一个业务日的全部分片（校验单元）。每个单元独立记录行数与摘要，
  只有全部单元齐备且校验通过，才发布贴源表版本；发布动作是原子的。
* 已发布版本不可改写：重新发布时生成新版本记录，旧版本保留并与审计联动。
* 摘要冲突时以「先完整落库的版本」为准；同一业务日只允许一个版本生效
  （发布串行化 + 版本号单调递增 + active 标记唯一）。
* 批次进度、贴源表可用范围、变更审计三者由同一次提交写入，保证口径一致。
* 任一分片中断后批次停在「落地中」，成功分片保留但不能被 ODS 查询；
  恢复时只复核未完成/失败的单元，已完成单元直接复用。
"""
from __future__ import annotations

import hashlib
import threading
from datetime import datetime, timezone
from typing import Any

from app.store import store

MODULE_LANDING = "landing"
MODULE_ODS = "ods"
MODULE_AUDIT = "auditlog"
MODULE_UNITS = "ingest_units"
MODULE_VERSIONS = "ods_versions"

BATCH_STATUS_DRAFT = "草稿"
BATCH_STATUS_LANDING = "落地中"
BATCH_STATUS_LANDED = "已落地"
BATCH_STATUS_CONFLICT = "摘要在途冲突"
BATCH_STATUS_ARCHIVED = "已归档"
BATCH_STATUSES = [BATCH_STATUS_DRAFT, BATCH_STATUS_LANDING, BATCH_STATUS_LANDED, BATCH_STATUS_CONFLICT, BATCH_STATUS_ARCHIVED]

ODS_STATUS_ONLINE = "已上线"
ODS_STATUS_PENDING = "建表审批中"

UNIT_STATUS_PENDING = "待校验"
UNIT_STATUS_COMPLETED = "已完成"
UNIT_STATUS_FAILED = "校验失败"

VERSION_STATUS_ACTIVE = "生效"
VERSION_STATUS_SUPERSEDED = "已替代"
VERSION_STATUS_CONFLICT = "冲突被拒"

SUMMARY_ALGORITHM = "sha256"
SUMMARY_ALGORITHM_ALIASES = {"sha256", "sha-256", "sha2-256"}

# 入湖提交的并发发布互斥锁：同一时刻只允许一个发布事务落库。
_PUBLISH_LOCK = threading.Lock()


def _now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


class IngestError(Exception):
    """入湖规则不满足时抛出，路由层把 message 原样返回给前端。"""


class IngestService:
    # ------------------------------------------------------------------ 基础读取

    def batches(self) -> list[dict[str, Any]]:
        return store.rows(MODULE_LANDING)

    def units(self) -> list[dict[str, Any]]:
        return store.rows(MODULE_UNITS)

    def versions(self) -> list[dict[str, Any]]:
        return store.rows(MODULE_VERSIONS)

    def _next_id(self, rows: list[dict[str, Any]]) -> int:
        return max((int(row.get("id", 0)) for row in rows), default=0) + 1

    def _batch_by_no(self, batch_no: str) -> dict[str, Any] | None:
        for row in self.batches():
            if str(row.get("批次编号")) == batch_no:
                return row
        return None

    def _ods_by_code(self, table_code: str) -> dict[str, Any] | None:
        for row in store.rows(MODULE_ODS):
            if str(row.get("表编号")) == table_code:
                return row
        return None

    def _units_of(self, batch_no: str) -> list[dict[str, Any]]:
        return sorted(
            [row for row in self.units() if row.get("批次编号") == batch_no],
            key=lambda row: int(row.get("分片序号") or 0),
        )

    def _versions_of(self, biz_date: str, table_code: str, batch_no: str | None = None) -> list[dict[str, Any]]:
        return [
            row for row in self.versions()
            if row.get("业务日期") == biz_date
            and row.get("表编号") == table_code
            and (batch_no is None or row.get("批次编号") == batch_no)
        ]

    # ------------------------------------------------------------------ 摘要计算

    @staticmethod
    def normalize_algorithm(algorithm: str | None) -> str:
        value = str(algorithm or "").strip().lower()
        if not value:
            return SUMMARY_ALGORITHM
        if value not in SUMMARY_ALGORITHM_ALIASES:
            raise IngestError(f"摘要算法「{algorithm}」不受支持，当前只接受 {SUMMARY_ALGORITHM}")
        return SUMMARY_ALGORITHM

    @staticmethod
    def digest(payload: bytes | str) -> str:
        """按 sha256 计算分片摘要，统一返回带算法前缀的十六进制串。"""
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        return f"{SUMMARY_ALGORITHM}:{hashlib.sha256(payload).hexdigest()}"

    def recompute_digest(self, batch_no: str, partition_no: int, payload: bytes | str) -> str:
        unit = self.find_unit(batch_no, partition_no)
        if unit is None:
            raise IngestError(f"批次 {batch_no} 不存在分片 {partition_no}")
        computed = self.digest(payload)
        registered = str(unit.get("校验摘要"))
        if computed != registered:
            unit["校验摘要"] = computed
            unit["登记摘要"] = registered
            unit["摘要冲突"] = True
            unit["状态"] = UNIT_STATUS_FAILED
            unit["失败原因"] = "摘要与登记值不一致，已按本次重算值登记待复核"
            unit["校验时刻"] = _now()
            batch = self._batch_by_no(batch_no)
            if batch is not None:
                batch["status"] = BATCH_STATUS_LANDING
                batch["pending"] = True
                batch["abnormal"] = True
            self._record_audit(
                operation="分片摘要重算",
                target=batch_no,
                before=registered,
                after=computed,
                reason=f"分片 {partition_no} 摘要冲突，已标记失败并等待恢复复核",
            )
            raise IngestError(f"分片 {partition_no} 摘要冲突：登记 {registered}，本次 {computed}")
        unit["校验时刻"] = _now()
        return computed

    def find_unit(self, batch_no: str, partition_no: int) -> dict[str, Any] | None:
        for row in self.units():
            if row.get("批次编号") == batch_no and int(row.get("分片序号") or 0) == partition_no:
                return row
        return None
        return None

    # ------------------------------------------------------------------ 登记

    def _find_file(self, raw_path: str) -> dict[str, Any] | None:
        for row in self.units():
            if str(row.get("原始路径")) == raw_path:
                return row
        return None

    def register_batch(self, values: dict[str, Any]) -> dict[str, Any]:
        """登记一个业务日的文件清单；同批次编号与重复文件只允许登记一次。"""
        required = ["批次编号", "来源系统", "落地路径", "文件格式", "业务日期", "表编号"]
        missing = [field for field in required if not str(values.get(field) or "").strip()]
        if missing:
            raise IngestError(f"缺少必填字段：{'、'.join(missing)}")
        batch_no = str(values["批次编号"]).strip()
        if self._batch_by_no(batch_no):
            raise IngestError(f"批次编号 {batch_no} 已登记，重复文件不再重复建批次")
        if self._ods_by_code(str(values["表编号"]).strip()) is None:
            raise IngestError(f"贴源表 {values['表编号']} 尚未登记，不能入湖")
        units = values.get("分片清单") or []
        if not isinstance(units, list) or not units:
            raise IngestError("分片清单不能为空：至少需要一个校验单元")
        parts = []
        for index, item in enumerate(units, start=1):
            if not isinstance(item, dict):
                raise IngestError(f"分片 {index} 的结构不合法")
            partition_no = int(item.get("分片序号") or index)
            part_path = str(item.get("原始路径") or "").strip()
            rows = int(item.get("行数") or 0)
            size = int(item.get("字节大小") or 0)
            if not part_path:
                raise IngestError(f"分片 {partition_no} 缺少原始路径")
            if rows == 0:
                raise IngestError(f"分片 {partition_no} 为空文件（行数 0），不进入入湖提交")
            if size < 0:
                raise IngestError(f"分片 {partition_no} 的字节大小不合法")
            duplicate = self._find_file(part_path)
            if duplicate is not None:
                raise IngestError(
                    f"重复文件：{part_path} 已登记于批次 {duplicate.get('批次编号')} 的分片 {duplicate.get('分片序号')}"
                )
            algorithm = self.normalize_algorithm(item.get("摘要算法"))
            digest_value = str(item.get("校验摘要") or "").strip()
            if digest_value and not digest_value.startswith(f"{algorithm}:"):
                digest_value = f"{algorithm}:{digest_value}"
            if not digest_value:
                raise IngestError(f"分片 {partition_no} 缺少校验摘要，无法核对完整性")
            parts.append({
                "分片序号": partition_no,
                "原始路径": part_path,
                "行数": rows,
                "字节大小": size,
                "摘要算法": algorithm,
                "校验摘要": digest_value,
                "主键摘要": str(item.get("主键摘要") or ""),
            })
        seen_parts: set[int] = set()
        for part in parts:
            if int(part["分片序号"]) in seen_parts:
                raise IngestError(f"分片序号 {part['分片序号']} 重复登记")
            seen_parts.add(int(part["分片序号"]))

        now = _now()
        declared_total = values.get("声明总行数")
        actual_total = sum(int(part["行数"]) for part in parts)
        if declared_total not in (None, "") and int(declared_total) != actual_total:
            raise IngestError(
                f"跨分片行数核对不一致：声明总行数 {declared_total}，分片行数合计 {actual_total}"
            )
        batch = {
            "id": self._next_id(self.batches()),
            "批次编号": batch_no,
            "来源系统": str(values["来源系统"]).strip(),
            "落地路径": str(values["落地路径"]).strip(),
            "文件格式": str(values["文件格式"]).strip(),
            "字节大小": int(values.get("字节大小") or sum(part["字节大小"] for part in parts)),
            "业务日期": str(values["业务日期"]).strip(),
            "表编号": str(values["表编号"]).strip(),
            "接收时间": str(values.get("接收时间") or now[:10]),
            "原始行数": actual_total,
            "声明总行数": int(declared_total) if declared_total not in (None, "") else actual_total,
            "跨分片行数核对": "通过",
            "分片总数": len(parts),
            "完成分片数": 0,
            "校验摘要": "",
            "摘要算法": SUMMARY_ALGORITHM,
            "校验时刻": "",
            "迟到分片": bool(values.get("迟到分片")),
            "进度": "0/{}".format(len(parts)),
            "缺口说明": "",
            "来源系统清单": str(values.get("来源系统清单") or values["来源系统"]).strip(),
            "status": BATCH_STATUS_DRAFT,
            "批次状态": BATCH_STATUS_DRAFT,
            "pending": True,
            "abnormal": False,
            "创建时间": now,
            "更新时间": now,
        }
        batch["去重后行数"] = int(batch["原始行数"])
        batch["分片清单"] = "、".join(str(part["分片序号"]) for part in parts)
        batch["摘要冲突"] = False
        self.batches().append(batch)

        for part in parts:
            unit = {
                "id": self._next_id(self.units()),
                "批次编号": batch_no,
                "业务日期": batch["业务日期"],
                "表编号": batch["表编号"],
                "分片序号": part["分片序号"],
                "原始路径": part["原始路径"],
                "行数": part["行数"],
                "字节大小": part["字节大小"],
                "摘要算法": part["摘要算法"],
                "校验摘要": part["校验摘要"],
                "主键摘要": part["主键摘要"],
                "状态": UNIT_STATUS_PENDING,
                "失败原因": "",
                "校验时刻": "",
                "重复主键数": 0,
            }
            self.units().append(unit)
        self._record_audit(
            operation="登记入湖批次",
            target=batch_no,
            before="",
            after=f"{batch['完成分片数']}/{batch['分片总数']}",
            reason=f"{batch['业务日期']} {batch['表编号']} 登记 {len(parts)} 个校验单元",
        )
        return self.batch_progress(batch_no)

    def register_unit(self, batch_no: str, values: dict[str, Any]) -> dict[str, Any]:
        """对迟到分片补登记新的校验单元，已登记分片不允许覆盖。"""
        batch = self._batch_by_no(batch_no)
        if batch is None:
            raise IngestError(f"批次 {batch_no} 不存在")
        partition_no = int(values.get("分片序号") or 0)
        if partition_no <= 0:
            raise IngestError("分片序号必须为正整数")
        if self.find_unit(batch_no, partition_no):
            raise IngestError(f"分片 {partition_no} 已存在，重复文件不再重复登记")
        part_path = str(values.get("原始路径") or "").strip()
        rows = int(values.get("行数") or 0)
        if not part_path:
            raise IngestError("迟到分片缺少原始路径")
        if rows == 0:
            raise IngestError("空文件不能作为校验单元")
        duplicate = self._find_file(part_path)
        if duplicate is not None:
            raise IngestError(
                f"重复文件：{part_path} 已登记于批次 {duplicate.get('批次编号')} 的分片 {duplicate.get('分片序号')}"
            )
        algorithm = self.normalize_algorithm(values.get("摘要算法"))
        digest_value = str(values.get("校验摘要") or "").strip()
        if not digest_value:
            raise IngestError("迟到分片缺少校验摘要")
        if not digest_value.startswith(f"{algorithm}:"):
            digest_value = f"{algorithm}:{digest_value}"
        unit = {
            "id": self._next_id(self.units()),
            "批次编号": batch_no,
            "业务日期": batch["业务日期"],
            "表编号": batch["表编号"],
            "分片序号": partition_no,
            "原始路径": part_path,
            "行数": rows,
            "字节大小": int(values.get("字节大小") or 0),
            "摘要算法": algorithm,
            "校验摘要": digest_value,
            "主键摘要": str(values.get("主键摘要") or ""),
            "状态": UNIT_STATUS_PENDING,
            "失败原因": "",
            "校验时刻": "",
            "重复主键数": 0,
            "迟到": True,
            "迟到原因": str(values.get("迟到原因") or "源端补传延迟"),
        }
        self.units().append(unit)
        batch["分片总数"] = len(self._units_of(batch_no))
        batch["原始行数"] = sum(int(row.get("行数") or 0) for row in self._units_of(batch_no))
        batch["声明总行数"] = int(batch.get("声明总行数") or 0) + rows
        batch["跨分片行数核对"] = "迟到分片已并入声明"
        batch["迟到分片"] = True
        batch["status"] = BATCH_STATUS_LANDING
        batch["批次状态"] = BATCH_STATUS_LANDING
        batch["pending"] = True
        batch["更新时间"] = _now()
        batch["缺口说明"] = ""
        self._record_audit(
            operation="补登记迟到分片",
            target=batch_no,
            before=f"{batch['完成分片数']}/{max(int(batch['分片总数']) - 1, 0)}",
            after=f"{batch['完成分片数']}/{batch['分片总数']}",
            reason=f"迟到分片 {partition_no} 已登记，等待复核",
        )
        self._sync_progress(batch)
        return self.batch_progress(batch_no)

    # ------------------------------------------------------------------ 校验单元

    def verify_unit(self, batch_no: str, partition_no: int, values: dict[str, Any] | None = None) -> dict[str, Any]:
        """校验单个分片：核对行数与摘要，失败时批次停在「落地中」。"""
        values = values or {}
        batch = self._batch_by_no(batch_no)
        if batch is None:
            raise IngestError(f"批次 {batch_no} 不存在")
        if batch.get("status") in (BATCH_STATUS_LANDED, BATCH_STATUS_ARCHIVED):
            raise IngestError("批次已发布，不允许再校验分片；请走新版本重新入湖")
        unit = self.find_unit(batch_no, partition_no)
        if unit is None:
            raise IngestError(f"批次 {batch_no} 不存在分片 {partition_no}，请先补登记迟到分片")
        if unit.get("状态") == UNIT_STATUS_COMPLETED and not values.get("force"):
            return {"unit": unit, "batch": self.batch_progress(batch_no), "reused": True}
        actual_rows = values.get("行数")
        if actual_rows is not None and int(actual_rows) != int(unit.get("行数") or 0):
            unit["状态"] = UNIT_STATUS_FAILED
            unit["失败原因"] = f"行数不一致：登记 {unit.get('行数')} 行，实际 {actual_rows} 行"
            unit["校验时刻"] = _now()
            self._mark_landing(batch, unit, abnormal=True)
            raise IngestError(unit["失败原因"])
        actual_digest = values.get("校验摘要")
        if actual_digest:
            normalized = str(actual_digest).strip()
            if not normalized.startswith(f"{unit.get('摘要算法')}:"):
                normalized = f"{unit.get('摘要算法')}:{normalized}"
            if normalized != unit.get("校验摘要"):
                unit["状态"] = UNIT_STATUS_FAILED
                unit["失败原因"] = f"摘要不一致：登记 {unit.get('校验摘要')}，实际 {normalized}"
                unit["校验时刻"] = _now()
                self._mark_landing(batch, unit, abnormal=True)
                raise IngestError(unit["失败原因"])
        duplicate_keys = int(values.get("重复主键数") or 0)
        primary_key_amount = int(unit.get("行数") or 0)
        if duplicate_keys > 0:
            unit["重复主键数"] = duplicate_keys
            if primary_key_amount > 0 and duplicate_keys >= primary_key_amount:
                unit["状态"] = UNIT_STATUS_FAILED
                unit["失败原因"] = f"分片内主键全部重复（{duplicate_keys} 行），去重后无有效数据"
                unit["校验时刻"] = _now()
                self._mark_landing(batch, unit, abnormal=True)
                raise IngestError(unit["失败原因"])
        unit["状态"] = UNIT_STATUS_COMPLETED
        unit["失败原因"] = ""
        unit["新增主键数"] = int(values.get("新增主键数") or (primary_key_amount - duplicate_keys))
        unit["校验时刻"] = _now()
        self._sync_progress(batch)
        self._record_audit(
            operation="分片校验通过",
            target=batch_no,
            before=UNIT_STATUS_PENDING,
            after=UNIT_STATUS_COMPLETED,
            reason=(
                f"分片 {partition_no} 行数与摘要核对通过"
                + (f"，分片内重复主键 {duplicate_keys} 行已去重" if duplicate_keys else "")
            ),
        )
        return {"unit": unit, "batch": self.batch_progress(batch_no), "reused": False}

    def _mark_landing(self, batch: dict[str, Any], unit: dict[str, Any], *, abnormal: bool) -> None:
        batch["status"] = BATCH_STATUS_LANDING
        batch["批次状态"] = BATCH_STATUS_LANDING
        batch["pending"] = True
        batch["abnormal"] = abnormal
        batch["更新时间"] = _now()
        batch["缺口说明"] = self._gap_text(batch)
        self._sync_progress(batch)
        self._record_audit(
            operation="分片校验失败",
            target=batch["批次编号"],
            before=UNIT_STATUS_PENDING,
            after=UNIT_STATUS_FAILED,
            reason=unit.get("失败原因") or "分片校验失败",
        )
        self._refresh_ods_availability(batch)

    def _sync_progress(self, batch: dict[str, Any]) -> None:
        units = self._units_of(str(batch["批次编号"]))
        done = [row for row in units if row.get("状态") == UNIT_STATUS_COMPLETED]
        batch["分片总数"] = len(units)
        batch["完成分片数"] = len(done)
        batch["进度"] = f"{len(done)}/{len(units)}"
        batch["原始行数"] = sum(int(row.get("行数") or 0) for row in units)
        batch["去重后行数"] = sum(
            int(row.get("新增主键数") if row.get("新增主键数") is not None else row.get("行数") or 0)
            for row in done
        )
        batch["分片内重复主键数"] = sum(int(row.get("重复主键数") or 0) for row in units)
        batch["更新时间"] = _now()
        batch["缺口说明"] = self._gap_text(batch)

    def _cross_check(self, batch: dict[str, Any], units: list[dict[str, Any]]) -> str:
        """跨分片行数核对：各分片行数之和必须等于登记的分片行数总和。"""
        declared = batch.get("声明总行数")
        actual = sum(int(row.get("行数") or 0) for row in units)
        if declared in (None, ""):
            return ""
        if int(declared) != actual:
            return f"跨分片行数核对不一致：登记 {declared} 行，分片合计 {actual} 行"
        return ""

    def _gap_text(self, batch: dict[str, Any]) -> str:
        units = self._units_of(str(batch["批次编号"]))
        missing = [
            f"分片 {row.get('分片序号')}（{row.get('状态')}{'：' + row['失败原因'] if row.get('失败原因') else ''}）"
            for row in units if row.get("状态") != UNIT_STATUS_COMPLETED
        ]
        if not missing:
            return ""
        return "缺口：" + "；".join(missing)

    def _refresh_ods_availability(self, batch: dict[str, Any]) -> None:
        """贴源表可用范围始终反映同一条业务链路上最近一次提交的状态。"""
        table = self._ods_by_code(str(batch.get("表编号") or ""))
        if table is None:
            return
        biz_date = str(batch.get("业务日期") or "")
        active = self._active_version(biz_date, str(batch.get("表编号")))
        if active:
            table["可用范围"] = self._availability_text(batch, active)
            table["可用状态"] = "可用"
            table["当前生效版本"] = active.get("版本号")
            table["最近加载时间"] = active.get("校验时刻") or table.get("最近加载时间")
            return
        latest = self._latest_batch(biz_date, str(batch.get("表编号")))
        table["可用范围"] = self._availability_text(latest or batch, None)
        table["可用状态"] = "不可用"

    def _latest_batch(self, biz_date: str, table_code: str) -> dict[str, Any] | None:
        candidates = [
            row for row in self.batches()
            if row.get("业务日期") == biz_date and row.get("表编号") == table_code
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda row: str(row.get("更新时间") or ""))

    def _availability_text(self, batch: dict[str, Any], active: dict[str, Any] | None) -> str:
        if active:
            return (
                f"{active.get('业务日期')} 已发布 {active.get('版本号')}，"
                f"总行数 {active.get('总行数')}，去重后 {active.get('去重后行数')}"
            )
        progress = f"{batch.get('完成分片数')}/{batch.get('分片总数')}"
        gap = batch.get("缺口说明") or "等待全部分片完成"
        return f"{batch.get('业务日期')} 不可用（进度 {progress}）：{gap}"

    def _active_version(self, biz_date: str, table_code: str) -> dict[str, Any] | None:
        for row in self.versions():
            if (
                row.get("业务日期") == biz_date
                and row.get("表编号") == table_code
                and row.get("状态") == VERSION_STATUS_ACTIVE
            ):
                return row
        return None

    # ------------------------------------------------------------------ 发布

    def publish(self, batch_no: str, values: dict[str, Any] | None = None) -> dict[str, Any]:
        """全部分片齐备后原子发布贴源表版本。

        发布过程持有进程级互斥锁：同一业务日的并发发布只会有一个事务进入，
        再结合「生效版本唯一 + 摘要先到先得」保证同一业务日只有一个版本生效。
        """
        values = values or {}
        if not _PUBLISH_LOCK.acquire(timeout=5):
            raise IngestError("同一业务日正在发布，请稍后重试")
        try:
            return self._publish_locked(batch_no, values)
        finally:
            _PUBLISH_LOCK.release()

    def _publish_locked(self, batch_no: str, values: dict[str, Any]) -> dict[str, Any]:
        batch = self._batch_by_no(batch_no)
        if batch is None:
            raise IngestError(f"批次 {batch_no} 不存在")
        if batch.get("status") in (BATCH_STATUS_LANDED, BATCH_STATUS_ARCHIVED):
            raise IngestError("批次已发布，已发布版本不允许重写")
        units = self._units_of(batch_no)
        pending = [row for row in units if row.get("状态") != UNIT_STATUS_COMPLETED]
        if pending:
            hint = "、".join(f"分片 {row.get('分片序号')}" for row in pending)
            batch["status"] = BATCH_STATUS_LANDING
            batch["批次状态"] = BATCH_STATUS_LANDING
            batch["pending"] = True
            batch["abnormal"] = True
            batch["缺口说明"] = f"缺口：{hint} 尚未完成校验"
            self._refresh_ods_availability(batch)
            raise IngestError(f"不能发布：{hint} 尚未完成校验，贴源表保持不可用")
        biz_date = str(batch.get("业务日期"))
        table_code = str(batch.get("表编号"))
        table = self._ods_by_code(table_code)
        if table is None:
            raise IngestError(f"贴源表 {table_code} 尚未登记")

        active = self._active_version(biz_date, table_code)
        total_rows = sum(int(row.get("行数") or 0) for row in units)
        cross_check = self._cross_check(batch, units)
        if cross_check:
            batch["status"] = BATCH_STATUS_LANDING
            batch["批次状态"] = BATCH_STATUS_LANDING
            batch["跨分片行数核对"] = "不通过"
            batch["缺口说明"] = cross_check
            batch["pending"] = True
            batch["abnormal"] = True
            self._record_audit(
                operation="跨分片行数核对失败",
                target=batch_no,
                before=str(batch.get("声明总行数")),
                after=str(total_rows),
                reason=cross_check,
            )
            self._refresh_ods_availability(batch)
            raise IngestError(f"不能发布：{cross_check}，贴源表保持不可用")
        batch["跨分片行数核对"] = "通过"
        dedup_rows = int(values.get("去重后行数") or sum(
            int(row.get("新增主键数") if row.get("新增主键数") is not None else row.get("行数") or 0)
            for row in units
        ))
        if dedup_rows > total_rows:
            raise IngestError(f"去重后行数 {dedup_rows} 不能大于总行数 {total_rows}")
        algorithm = self.normalize_algorithm(values.get("摘要算法") or SUMMARY_ALGORITHM)
        combined_digest = self._combined_digest(units, algorithm)
        declared_digest = values.get("校验摘要")
        if declared_digest:
            declared = str(declared_digest).strip()
            if not declared.startswith(f"{algorithm}:"):
                declared = f"{algorithm}:{declared}"
            if declared != combined_digest:
                batch["status"] = BATCH_STATUS_CONFLICT
                batch["批次状态"] = BATCH_STATUS_CONFLICT
                batch["待复核摘要"] = declared
                batch["实际摘要"] = combined_digest
                batch["pending"] = True
                batch["abnormal"] = True
                self._record_audit(
                    operation="发布摘要冲突",
                    target=batch_no,
                    before=declared,
                    after=combined_digest,
                    reason=f"提交摘要 {declared} 与分片汇总 {combined_digest} 不一致，拒绝发布",
                )
                self._refresh_ods_availability(batch)
                raise IngestError(f"摘要冲突：提交 {declared}，分片汇总 {combined_digest}，已保留目标版本待复核")

        # 已发布版本不重写：内容一致的重复提交直接复用既有版本，不产生新版本。
        if active is not None and str(active.get("校验摘要")) == combined_digest:
            batch["校验摘要"] = combined_digest
            batch["摘要算法"] = algorithm
            batch["去重后行数"] = active.get("去重后行数", dedup_rows)
            batch["校验时刻"] = active.get("校验时刻")
            batch["status"] = BATCH_STATUS_LANDED
            batch["批次状态"] = BATCH_STATUS_LANDED
            batch["pending"] = False
            batch["abnormal"] = False
            batch["缺口说明"] = ""
            batch["版本号"] = active.get("版本号")
            batch["分片清单"] = active.get("分片清单")
            batch["复用已发布版本"] = True
            batch["更新时间"] = _now()
            batch["分片总数"] = len(units)
            batch["完成分片数"] = len(units)
            batch["进度"] = f"{len(units)}/{len(units)}"
            self._record_audit(
                operation="复用已发布版本",
                target=batch_no,
                before=str(active.get("版本号")),
                after=str(active.get("版本号")),
                reason="摘要与已生效版本一致，已发布版本不重写",
            )
            return self.batch_progress(batch_no)

        # 摘要冲突时以先完整落库的版本为准：若已存在生效版本且摘要不同，则拒绝覆盖。
        # 已生效版本不会就地改写，本次拒绝发布，原版本继续保持生效。
        if active is not None and str(active.get("校验摘要")) != combined_digest:
            existing_batch = str(active.get("批次编号"))
            batch["status"] = BATCH_STATUS_CONFLICT
            batch["批次状态"] = BATCH_STATUS_CONFLICT
            batch["待复核摘要"] = combined_digest
            batch["实际摘要"] = active.get("校验摘要")
            batch["摘要冲突"] = True
            batch["pending"] = True
            batch["abnormal"] = True
            self._record_audit(
                operation="发布冲突被拒",
                target=batch_no,
                before=combined_digest,
                after=str(active.get("校验摘要")),
                reason=f"{biz_date} 已有先完整落库的版本 {active.get('版本号')}（批次 {existing_batch}），以先到版本为准",
            )
            self._refresh_ods_availability(batch)
            raise IngestError(
                f"摘要冲突：{biz_date} 已有先完整落库的版本 {active.get('版本号')}"
                f"（批次 {existing_batch}），以先到版本为准，本次不发布"
            )

        now = _now()
        version_no = self._next_version_no(biz_date, table_code)
        version = {
            "id": self._next_id(self.versions()),
            "版本号": version_no,
            "批次编号": batch_no,
            "表编号": table_code,
            "业务日期": biz_date,
            "分片清单": "、".join(str(row.get("分片序号")) for row in units),
            "分片数": len(units),
            "总行数": total_rows,
            "去重后行数": dedup_rows,
            "分片内重复主键数": sum(int(row.get("重复主键数") or 0) for row in units),
            "摘要算法": algorithm,
            "校验摘要": combined_digest,
            "校验时刻": now,
            "状态": VERSION_STATUS_ACTIVE,
            "发布序号": self._next_publish_seq(),
        }
        self.versions().append(version)
        if active is not None:
            active["状态"] = VERSION_STATUS_SUPERSEDED
            active["替代时间"] = now
            self._record_audit(
                operation="版本被替代",
                target=f"{table_code}/{biz_date}",
                before=str(active.get("版本号")),
                after=version_no,
                reason=f"新版本 {version_no} 已通过全部校验，旧版本保留可追溯",
                batch_no=batch_no,
            )

        batch["校验摘要"] = combined_digest
        batch["摘要算法"] = algorithm
        batch["去重后行数"] = dedup_rows
        batch["校验时刻"] = now
        batch["status"] = BATCH_STATUS_LANDED
        batch["批次状态"] = BATCH_STATUS_LANDED
        batch["pending"] = False
        batch["abnormal"] = False
        batch["缺口说明"] = ""
        batch["版本号"] = version_no
        batch["分片清单"] = version["分片清单"]
        batch["更新时间"] = now

        table["version"] = version_no
        table["status"] = ODS_STATUS_ONLINE
        table["可用状态"] = "可用"
        table["可用范围"] = self._availability_text(batch, version)
        table["当前生效版本"] = version_no
        table["最近加载时间"] = now
        table["分片清单"] = version["分片清单"]
        table["总行数"] = total_rows
        table["去重后行数"] = dedup_rows
        table["摘要算法"] = algorithm
        table["校验摘要"] = combined_digest
        table["校验时刻"] = now

        audit = self._record_audit(
            operation="发布贴源表版本",
            target=f"{table_code}/{biz_date}",
            before=str(active.get("版本号")) if active else "无生效版本",
            after=version_no,
            reason=f"{len(units)} 个分片齐备，摘要 {combined_digest}，审计与入湖同批联动",
            batch_no=batch_no,
        )
        version["审计记录编号"] = audit.get("记录编号")
        batch["审计记录编号"] = audit.get("记录编号")
        return self.batch_progress(batch_no)

    def _combined_digest(self, units: list[dict[str, Any]], algorithm: str) -> str:
        joined = "|".join(
            f"{row.get('分片序号')}:{row.get('校验摘要')}:{row.get('行数')}" for row in units
        )
        return f"{algorithm}:{hashlib.sha256(joined.encode('utf-8')).hexdigest()}"

    def _next_version_no(self, biz_date: str, table_code: str) -> str:
        existing = self._versions_of(biz_date, table_code)
        return f"ODS-V{len(existing) + 1:03d}-{biz_date}"

    def _next_publish_seq(self) -> int:
        return max((int(row.get("发布序号") or 0) for row in self.versions()), default=0) + 1

    # ------------------------------------------------------------------ 审计

    def _record_audit(
        self, *, operation: str, target: str, before: str, after: str, reason: str, batch_no: str | None = None
    ) -> dict[str, Any]:
        rows = store.rows(MODULE_AUDIT)
        now = _now()
        record_no = f"AUDI-{int(now[:10].replace('-', ''))}{len(rows) + 1:04d}"
        record = {
            "id": self._next_id(rows),
            "记录编号": record_no,
            "操作表名": target,
            "变更类型": operation,
            "变更前值": before,
            "变更后值": after,
            "操作人": "入湖编排",
            "操作时间": now,
            "变更原因": reason,
            "关联批次": batch_no or self._batch_no_of_target(target),
            "status": "已记录",
            "pending": False,
            "abnormal": False,
        }
        rows.append(record)
        return record

    def _batch_no_of_target(self, target: str) -> str:
        if self._batch_by_no(target):
            return target
        for batch in self.batches():
            if target == f"{batch.get('表编号')}/{batch.get('业务日期')}":
                return str(batch.get("批次编号"))
        return ""

    def audit_trail(self, batch_no: str) -> list[dict[str, Any]]:
        """审计轨迹独立于批次表存在：批次不存在时返回空列表而不是报错。"""
        rows = [
            row for row in store.rows(MODULE_AUDIT)
            if str(row.get("关联批次") or "") == batch_no
        ]
        return sorted(rows, key=lambda row: str(row.get("操作时间") or ""))

    # ------------------------------------------------------------------ 汇总视图

    def batch_progress(self, batch_no: str) -> dict[str, Any]:
        batch = self._batch_by_no(batch_no)
        if batch is None:
            raise IngestError(f"批次 {batch_no} 不存在")
        units = self._units_of(batch_no)
        active = self._active_version(str(batch.get("业务日期")), str(batch.get("表编号")))
        latest = self._latest_batch(str(batch.get("业务日期")), str(batch.get("表编号")))
        progress = {
            "批次编号": batch_no,
            "业务日期": batch.get("业务日期"),
            "表编号": batch.get("表编号"),
            "批次状态": batch.get("批次状态"),
            "status": batch.get("status"),
            "完成分片数": batch.get("完成分片数"),
            "分片总数": batch.get("分片总数"),
            "进度": batch.get("进度"),
            "原始行数": batch.get("原始行数"),
            "去重后行数": batch.get("去重后行数"),
            "校验摘要": batch.get("校验摘要"),
            "摘要算法": batch.get("摘要算法"),
            "校验时刻": batch.get("校验时刻"),
            "缺口说明": batch.get("缺口说明"),
            "迟到分片": batch.get("迟到分片"),
            "摘要冲突": bool(batch.get("摘要冲突")),
            "跨分片行数核对": batch.get("跨分片行数核对") or ("未核对" if units else "未声明"),
            "分片内重复主键数": sum(int(row.get("重复主键数") or 0) for row in units),
            "version": batch.get("版本号"),
            "复用已发布版本": bool(batch.get("复用已发布版本")),
            "可用范围": self._availability_text(latest or batch, active),
            "可用状态": "可用" if active else "不可用",
            "生效版本": active.get("版本号") if active else None,
            "分片": units,
            "审计": self.audit_trail(batch_no),
        }
        return progress

    def resume_plan(self, batch_no: str) -> dict[str, Any]:
        """恢复计划：只复核未完成单元，已完成分片直接复用。"""
        batch = self._batch_by_no(batch_no)
        if batch is None:
            raise IngestError(f"批次 {batch_no} 不存在")
        units = self._units_of(batch_no)
        todo = [row for row in units if row.get("状态") != UNIT_STATUS_COMPLETED]
        done = [row for row in units if row.get("状态") == UNIT_STATUS_COMPLETED]
        published = batch.get("status") in (BATCH_STATUS_LANDED, BATCH_STATUS_ARCHIVED)
        if published:
            note = "该批次已发布，已发布版本不重写；如需修正请以新批次重新提交"
        elif todo:
            note = "已发布版本不重写；恢复只重新校验未完成单元，已完成分片直接复用"
        else:
            note = "全部分片已齐备，可直接发布贴源表版本"
        return {
            "批次编号": batch_no,
            "业务日期": batch.get("业务日期"),
            "可重新发布": bool(units) and not todo and not published,
            "需复核": [
                {"分片序号": row.get("分片序号"), "状态": row.get("状态"), "失败原因": row.get("失败原因")}
                for row in todo
            ],
            "可复用": [{"分片序号": row.get("分片序号"), "行数": row.get("行数"), "校验摘要": row.get("校验摘要")} for row in done],
            "说明": note,
        }

    def availability(self) -> list[dict[str, Any]]:
        """贴源表可用范围汇总页：每张 ODS 表对应业务日的可用状态。"""
        result = []
        for table in store.rows(MODULE_ODS):
            table_code = str(table.get("表编号"))
            active = next(
                (row for row in self.versions()
                 if row.get("表编号") == table_code and row.get("状态") == VERSION_STATUS_ACTIVE),
                None,
            )
            biz_date = active.get("业务日期") if active else None
            if biz_date is None:
                recent = max(
                    (row for row in self.batches() if row.get("表编号") == table_code),
                    key=lambda row: str(row.get("更新时间") or ""),
                    default=None,
                )
                biz_date = recent.get("业务日期") if recent else None
            result.append({
                "表编号": table_code,
                "表名称": table.get("表名称"),
                "业务日期": biz_date,
                "可用状态": "可用" if active else "不可用",
                "可用范围": table.get("可用范围") or "",
                "当前生效版本": active.get("版本号") if active else None,
                "最近加载时间": table.get("最近加载时间"),
            })
        return result

    def list_versions(self, *, biz_date: str | None = None, table_code: str | None = None) -> list[dict[str, Any]]:
        rows = sorted(
            self.versions(),
            key=lambda row: int(row.get("发布序号") or 0),
            reverse=True,
        )
        if biz_date:
            rows = [row for row in rows if row.get("业务日期") == biz_date]
        if table_code:
            rows = [row for row in rows if row.get("表编号") == table_code]
        return rows

"""原始落地业务规则：状态流转、字段校验与筛选口径都收在这里。"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.services.ingest import BATCH_STATUSES, IngestError, IngestService

MODULE = "landing"
REQUIRED_FIELDS = ["批次编号", "来源系统", "落地路径"]
STATUS_ORDER = BATCH_STATUSES
ACTION_RULES = {"发起落地": "落地中", "确认已落地": "已落地", "归档批次": "已归档"}
NEGATIVE_ACTIONS = []


class LandingService:
    def __init__(self) -> None:
        self.ingest = IngestService()

    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("批次编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """登记原始文件清单：与贴源表加载共用同一份入湖提交，不再单独建批次。"""
        try:
            entry = self.ingest.register_batch(values)
        except IngestError as exc:
            return None, str(exc)
        return entry, ""

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"落地批次 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于原始落地可执行范围"
        target = ACTION_RULES[action]
        if entry.get("status") == "已落地" and action != "归档批次":
            return None, "批次已发布贴源表版本，不允许回退状态"
        if action == "确认已落地":
            # 人工确认也必须走一次入湖提交：分片不齐时不能把批次标成已落地。
            try:
                progress = self.ingest.publish(str(entry.get("批次编号")))
            except IngestError as exc:
                return None, f"{exc}；批次保持{entry.get('status')}，不能人工置为已落地"
            return entry, f"落地批次已确认，贴源表版本 {progress.get('version')} 已发布"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"落地批次已{action}"

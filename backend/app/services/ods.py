"""贴源层业务规则：状态流转、字段校验与筛选口径都收在这里。"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.services.ingest import IngestService

MODULE = "ods"
REQUIRED_FIELDS = ["表编号", "表名称", "来源系统"]
STATUS_ORDER = ["草稿", "建表审批中", "已上线", "已下线"]
ACTION_RULES = {"提交建表审批": "建表审批中", "确认上线": "已上线", "下线表": "已下线"}
NEGATIVE_ACTIONS = ["下线表"]


class OdsService:
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
            rows = [row for row in rows if keyword in str(row.get("表编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def get_detail(self, entry_id: int) -> dict[str, Any] | None:
        """贴源表明细：带出可用范围、版本与同批审计记录。"""
        entry = self.get_entry(entry_id)
        if entry is None:
            return None
        table_code = str(entry.get("表编号") or "")
        version = next(
            (row for row in self.ingest.list_versions(table_code=table_code) if row.get("状态") == "生效"),
            None,
        )
        audits = [
            row for row in store.rows("auditlog")
            if str(row.get("操作表名", "")).split("/")[0] == table_code
        ]
        return {"entry": entry, "version": version, "audit": audits}

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"贴源表 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于贴源层可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        if action == "确认上线":
            # 上线前置条件是 2026-10-12 的业务日已经有生效版本，否则 ODS 查询不可用。
            if not entry.get("当前生效版本"):
                return None, "尚无通过校验的入湖版本，贴源表不能上线；请先完成分片校验并发布"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"贴源表已{action}"

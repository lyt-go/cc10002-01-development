"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
"""
from __future__ import annotations

from typing import Any

from app.seed import INGEST_SEED, SEED_ROWS

# 入湖编排的内部表：由 services/ingest 维护，不作为独立的业务模块暴露到概览里。
INTERNAL_MODULES = {"ingest_units", "ods_versions"}


class Store:
    def __init__(self) -> None:
        self.reset()

    @staticmethod
    def _initial_rows() -> dict[str, list[dict[str, Any]]]:
        tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
        for name, rows in INGEST_SEED.items():
            tables[name] = [dict(row) for row in rows]
        return tables

    def reset(self) -> None:
        """恢复示例数据：测试与本地演示需要从同一份初始数据重新开始。"""
        self._tables = self._initial_rows()

    def module_names(self) -> list[str]:
        return sorted(name for name in self._tables if name not in INTERNAL_MODULES)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def find_by(self, module: str, field: str, value: Any) -> dict[str, Any] | None:
        """按业务字段查一行，找不到返回 None；不在这里改数据。"""
        for row in self.rows(module):
            if row.get(field) == value:
                return row
        return None

    def next_id(self, module: str) -> int:
        return max((int(row.get("id", 0)) for row in self.rows(module)), default=0) + 1

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()

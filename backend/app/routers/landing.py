"""原始落地接口：维护落地批次，覆盖发起落地、确认已落地、归档批次等动作。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.landing import LandingService

router = APIRouter(prefix="/api/landing", tags=["原始落地"])

service = LandingService()

LIST_FIELDS = ["批次编号", "来源系统", "落地路径", "文件格式", "字节大小", "业务日期", "原始行数", "进度", "校验摘要", "批次状态"]
STATUSES = ["草稿", "落地中", "已落地", "摘要在途冲突", "已归档"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按批次编号检索"),
    status: str | None = Query(default=None, description="草稿、落地中、已落地、已归档"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按批次编号与状态过滤原始落地列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出原始落地清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "landing", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条落地批次明细；带出同一次入湖提交的分片、缺口与审计联动信息。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"落地批次 {entry_id} 不存在或已归档")
    batch_no = str(entry.get("批次编号") or "")
    progress = service.ingest.batch_progress(batch_no) if batch_no else None
    return {"entry": entry, "progress": progress}


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条入湖批次（含分片清单）；落地与贴源表加载共用这一次提交。"""
    entry, message = service.create_entry(payload.values)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message="入湖批次已登记，等待逐个分片校验", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条落地批次执行发起落地、确认已落地、归档批次；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)

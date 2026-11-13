"""入湖编排接口：登记批次、登记/复核分片、发布版本、恢复与可用范围查询。

路由层只做参数收拢与错误转译，状态流转全部在 ``app.services.ingest`` 里完成。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.ingest import IngestError, IngestService

router = APIRouter(prefix="/api/ingest", tags=["数仓入湖"])

service = IngestService()


@router.get("/batches", response_model=PageResult[dict])
def list_batches(
    biz_date: str | None = Query(default=None, description="按业务日期过滤"),
    status: str | None = Query(default=None, description="批次状态"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    rows = service.batches()
    if biz_date:
        rows = [row for row in rows if row.get("业务日期") == biz_date]
    if status:
        rows = [row for row in rows if row.get("status") == status]
    total = len(rows)
    start = max(page - 1, 0) * size
    return PageResult(items=rows[start:start + size], total=total, page=page, size=size)


@router.get("/batches/{batch_no}", response_model=dict)
def get_batch(batch_no: str) -> dict:
    try:
        return service.batch_progress(batch_no)
    except IngestError as exc:
        return {"ok": False, "message": str(exc)}


@router.post("/batches", response_model=ActionResult)
def register_batch(payload: EntryPayload) -> ActionResult:
    """登记一次入湖提交的源端清单；分片清单里每项即一个校验单元。"""
    try:
        entry = service.register_batch(payload.values)
    except IngestError as exc:
        return ActionResult(ok=False, message=str(exc))
    return ActionResult(ok=True, message="入湖批次已登记，分片等待逐个校验", entry=entry)


@router.post("/batches/{batch_no}/units", response_model=ActionResult)
def register_unit(batch_no: str, payload: EntryPayload) -> ActionResult:
    """补登记迟到分片；重复分片会被拒绝。"""
    try:
        entry = service.register_unit(batch_no, payload.values)
    except IngestError as exc:
        return ActionResult(ok=False, message=str(exc))
    return ActionResult(ok=True, message="迟到分片已登记", entry=entry)


@router.post("/batches/{batch_no}/units/{partition_no}/verify", response_model=ActionResult)
def verify_unit(batch_no: str, partition_no: int, payload: EntryPayload) -> ActionResult:
    """复核单个分片的行数与摘要；已完成单元默认复用，不重复校验。"""
    try:
        entry = service.verify_unit(batch_no, partition_no, payload.values)
    except IngestError as exc:
        return ActionResult(ok=False, message=str(exc))
    message = "分片已通过校验（复用既有结果）" if entry.get("reused") else "分片校验通过"
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/batches/{batch_no}/recompute", response_model=ActionResult)
def recompute_digest(batch_no: str, payload: EntryPayload) -> ActionResult:
    """按本次读取的字节重算分片摘要；与登记值不一致时标记冲突。"""
    partition_no = int(payload.values.get("分片序号") or 0)
    raw = str(payload.values.get("内容") or "")
    try:
        digest = service.recompute_digest(batch_no, partition_no, raw)
    except IngestError as exc:
        return ActionResult(ok=False, message=str(exc))
    return ActionResult(ok=True, message="分片摘要一致", entry={"分片序号": partition_no, "校验摘要": digest})


@router.post("/batches/{batch_no}/publish", response_model=ActionResult)
def publish(batch_no: str, payload: EntryPayload) -> ActionResult:
    """全部分片齐备后发布贴源表版本；同一业务日仅允许一个版本生效。"""
    try:
        entry = service.publish(batch_no, payload.values)
    except IngestError as exc:
        return ActionResult(ok=False, message=str(exc))
    if entry.get("复用已发布版本"):
        return ActionResult(ok=True, message=f"摘要与已生效版本一致，复用 {entry.get('version')}", entry=entry)
    return ActionResult(ok=True, message=f"已发布贴源表版本 {entry.get('version')}", entry=entry)


@router.get("/batches/{batch_no}/resume", response_model=dict)
def resume(batch_no: str) -> dict:
    """恢复计划：列出需要复核的单元与可直接复用的已完成单元。"""
    try:
        return service.resume_plan(batch_no)
    except IngestError as exc:
        return {"ok": False, "message": str(exc)}


@router.get("/availability", response_model=list[dict[str, Any]])
def availability() -> list[dict[str, Any]]:
    """贴源表可用范围：与批次进度、审计记录来自同一份入湖提交。"""
    return service.availability()


@router.get("/versions", response_model=list[dict[str, Any]])
def versions(biz_date: str | None = None, table_code: str | None = None) -> list[dict[str, Any]]:
    """已发布版本清单；已替代的版本保留可追溯。"""
    return service.list_versions(biz_date=biz_date, table_code=table_code)


@router.get("/batches/{batch_no}/audit", response_model=list[dict[str, Any]])
def audit_trail(batch_no: str) -> list[dict[str, Any]]:
    """每次尝试的失败原因与最终通过摘要，按批次联动查询。"""
    return service.audit_trail(batch_no)

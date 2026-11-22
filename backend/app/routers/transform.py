"""字段转换专用入口：清洗样本预演、执行清单回查和正式转换结果读取。

这些路径都挂在 /api/clean_pipeline 下，和通用资源路由并存；请求体按需携带
样本、清单版本或清单编号，便于预演和正式转换复用同一份清单。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query

from app.schemas import ActionResult
from app.services import service_for

router = APIRouter(prefix="/api/clean_pipeline", tags=["字段转换"])


def _service():
    service = service_for("clean_pipeline")
    if service is None:  # pragma: no cover - 注册表固定包含该模块
        raise HTTPException(status_code=404, detail="清洗管道模块未注册")
    return service


@router.post("/preview", response_model=dict)
def preview_by_body(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    """按请求体里的工序编号做转换预演。"""
    code = str(payload.get("工序编号") or payload.get("pipeline_code") or "").strip()
    if not code:
        raise HTTPException(status_code=400, detail="缺少工序编号")
    try:
        return _service().preview_transform(code, payload)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/transform", response_model=dict)
def transform_by_body(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    """按请求体里的工序编号消费清单执行正式转换。"""
    code = str(payload.get("工序编号") or payload.get("pipeline_code") or "").strip()
    if not code:
        raise HTTPException(status_code=400, detail="缺少工序编号")
    try:
        return _service().formal_transform(code, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/code/{code}/preview", response_model=dict)
def preview_by_code(code: str) -> dict[str, Any]:
    try:
        return _service().preview_transform(code)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/code/{code}/checklist", response_model=dict)
def checklist_by_code(
    code: str,
    version: str | None = Query(default=None),
    checklist_id: str | None = Query(default=None),
) -> dict[str, Any]:
    checklist = _service().get_checklist(code, version=version, checklist_id=checklist_id)
    if checklist is None:
        raise HTTPException(status_code=404, detail=f"清洗工序 {code} 暂无执行清单")
    return checklist


@router.get("/code/{code}/checklists", response_model=dict)
def checklists_by_code(code: str) -> dict[str, Any]:
    items = _service().list_checklists(code)
    if not items:
        raise HTTPException(status_code=404, detail=f"清洗工序 {code} 暂无执行清单")
    return {"items": items, "total": len(items)}


@router.get("/code/{code}/results", response_model=dict)
def results_by_code(code: str) -> dict[str, Any]:
    items = _service().list_transform_results(code)
    if not items:
        raise HTTPException(status_code=404, detail=f"清洗工序 {code} 暂无正式转换结果")
    return {"items": items, "total": len(items)}


@router.get("/code/{code}/result", response_model=dict)
def result_by_code(
    code: str,
    version: str | None = Query(default=None),
    checklist_id: str | None = Query(default=None),
) -> dict[str, Any]:
    result = _service().get_transform_result(code, version=version, checklist_id=checklist_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"清洗工序 {code} 暂无正式转换结果")
    return result


@router.post("/code/{code}/transform", response_model=dict)
def transform_by_code(code: str, payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    try:
        return _service().formal_transform(code, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{entry_id}/preview", response_model=dict)
def preview_by_id(entry_id: int) -> dict[str, Any]:
    try:
        return _service().preview_transform(str(entry_id), pipeline_id=entry_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/{entry_id}/preview", response_model=dict)
def preview_by_id_with_body(
    entry_id: int,
    payload: dict[str, Any] = Body(default_factory=dict),
) -> dict[str, Any]:
    try:
        return _service().preview_transform(str(entry_id), payload, pipeline_id=entry_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{entry_id}/checklist", response_model=dict)
def checklist_by_id(
    entry_id: int,
    version: str | None = Query(default=None),
    checklist_id: str | None = Query(default=None),
) -> dict[str, Any]:
    checklist = _service().get_checklist(
        str(entry_id), version=version, checklist_id=checklist_id, pipeline_id=entry_id)
    if checklist is None:
        raise HTTPException(status_code=404, detail=f"清洗工序 {entry_id} 暂无执行清单")
    return checklist


@router.get("/{entry_id}/checklists", response_model=dict)
def checklists_by_id(entry_id: int) -> dict[str, Any]:
    items = _service().list_checklists(str(entry_id), pipeline_id=entry_id)
    if not items:
        raise HTTPException(status_code=404, detail=f"清洗工序 {entry_id} 暂无执行清单")
    return {"items": items, "total": len(items)}


@router.get("/{entry_id}/results", response_model=dict)
def results_by_id(entry_id: int) -> dict[str, Any]:
    items = _service().list_transform_results(str(entry_id), pipeline_id=entry_id)
    if not items:
        raise HTTPException(status_code=404, detail=f"清洗工序 {entry_id} 暂无正式转换结果")
    return {"items": items, "total": len(items)}


@router.get("/{entry_id}/result", response_model=dict)
def result_by_id(
    entry_id: int,
    version: str | None = Query(default=None),
    checklist_id: str | None = Query(default=None),
) -> dict[str, Any]:
    result = _service().get_transform_result(
        str(entry_id), pipeline_id=entry_id, version=version, checklist_id=checklist_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"清洗工序 {entry_id} 暂无正式转换结果")
    return result


@router.post("/{entry_id}/transform", response_model=dict)
def transform_by_id(
    entry_id: int,
    payload: dict[str, Any] = Body(default_factory=dict),
) -> dict[str, Any]:
    try:
        return _service().formal_transform(str(entry_id), payload, pipeline_id=entry_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/samples", response_model=dict)
def list_samples() -> dict[str, Any]:
    from app.services import transform_state as transform_store
    items = transform_store.list_samples()
    return {"items": items, "total": len(items)}


@router.get("/{entry_id}/samples", response_model=dict)
def list_samples_for_pipeline(entry_id: int) -> dict[str, Any]:
    from app.services import transform_state as transform_store
    items = transform_store.list_samples()
    return {"items": items, "total": len(items)}


@router.get("/{entry_id}/sample/{sample_id}", response_model=dict)
def sample_by_id(entry_id: int, sample_id: str) -> dict[str, Any]:
    sample = _service().get_sample_report(sample_id)
    if sample is None:
        raise HTTPException(status_code=404, detail=f"样本 {sample_id} 不存在")
    return sample

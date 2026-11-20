"""指标运行时接口：消费分窗聚合结果、查询指标详情并复算已发指标。"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.services import metric_runtime

router = APIRouter(prefix="/api/metric_runtime", tags=["指标运行时"])


@router.post("/run")
def run(payload: dict | None = None) -> dict:
    """按当前分窗聚合结果计算指标，返回本次实际引用的聚合批次。"""
    payload = payload or {}
    try:
        result = metric_runtime.run_metric_calculation(window_id=payload.get("window_id"))
    except (ValueError, metric_runtime.wr.WindowRuntimeError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"ok": True, "result": result}


@router.get("/result")
def result(window_id: str | None = Query(default=None)) -> dict:
    """当前分窗批次下的指标结果与批次引用；尚无分窗结果时返回空结果而非报错。"""
    item = metric_runtime.latest_result(window_id)
    return {"ok": True, "result": item}


@router.get("/{metric_id}")
def get_metric(metric_id: str, window_id: str | None = Query(default=None)) -> dict:
    """指标详情：交代该指标实际引用的聚合批次，而不是整批订单。"""
    item = metric_runtime.metric_result_for(metric_id, window_id)
    if item is None:
        raise HTTPException(status_code=404, detail=f"指标 {metric_id} 暂无可用的分窗聚合结果")
    return item


@router.get("/emissions/list")
def list_emissions(
    window_id: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=50, ge=1, le=200),
) -> dict:
    items, total = metric_runtime.list_emissions(window_id=window_id, page=page, size=size)
    return {"ok": True, "items": items, "total": total, "page": page, "size": size}


@router.get("/emissions/{emission_id}/recompute")
def recompute(emission_id: str) -> dict:
    """按发射时的桶和批次复算，核对已发指标有没有混入后来的更正批次。"""
    item = metric_runtime.recompute_emission(emission_id)
    if item is None:
        raise HTTPException(status_code=404, detail=f"指标发射 {emission_id} 不存在")
    return {"ok": True, "result": item}

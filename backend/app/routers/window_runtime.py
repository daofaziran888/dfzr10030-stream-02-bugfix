"""分窗运行时接口：触发分窗执行、查询桶 / 批次 / 订单 / 窗外记录并做下游确认。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.config import settings
from app.schemas import ActionResult
from app.services import window_runtime as runtime

router = APIRouter(prefix="/api/window_runtime", tags=["分窗运行时"])


def _page(size: int) -> int:
    return max(1, min(size, settings.window_page_size_max))


@router.post("/run")
def run(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """分窗执行入口：按事件时间归桶并返回本次实际引用的聚合批次。

    ``orders`` 为空时沿用首批订单；``reset`` 为真时先清空运行时状态，便于演示。
    ``configs`` 可临时覆盖窗口边界，覆盖值会登记到运行时配置表。
    """
    payload = payload or {}
    try:
        result = runtime.run_windows(
            payload.get("orders"),
            window_id=payload.get("window_id"),
            late_policy=payload.get("late_policy"),
            confirm_pending=bool(payload.get("confirm_pending")),
            configs=payload.get("configs"),
            reset=bool(payload.get("reset")),
        )
    except runtime.WindowRuntimeError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"ok": True, "result": result}


@router.get("/configs")
def list_configs(window_id: str | None = Query(default=None)) -> dict[str, Any]:
    """当前生效的窗口配置：窗口边界、步长与迟到阈值都从这里取。"""
    try:
        configs = runtime.get_window_configs(window_id)
    except runtime.WindowRuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"ok": True, "items": configs, "total": len(configs)}


@router.post("/configs")
def register_configs(payload: dict[str, Any] | None = None) -> ActionResult:
    """登记 / 覆盖窗口配置，后续归桶与查询都以新边界为准。"""
    payload = payload or {}
    configs = payload.get("configs")
    if not isinstance(configs, list) or not configs:
        return ActionResult(ok=False, message="缺少必填字段：configs")
    runtime.register_configs(configs)
    return ActionResult(ok=True, message="窗口配置已登记")


@router.get("/aggregation")
def get_aggregation(window_id: str | None = Query(default=None)) -> dict[str, Any]:
    """分窗指标聚合结果：指标计算模块的唯一输入。"""
    try:
        result = runtime.build_aggregation(window_id)
    except runtime.WindowRuntimeError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"ok": True, "result": result}


@router.get("/buckets")
def list_buckets(
    window_id: str | None = Query(default=None),
    bucket_id: str | None = Query(default=None),
    start: str | None = Query(default=None, description="窗口时段起点，ISO 事件时间"),
    end: str | None = Query(default=None, description="窗口时段终点，ISO 事件时间"),
    sealed: bool | None = Query(default=None, description="是否已按事件时间封账"),
    order_id: str | None = Query(default=None, description="按订单编号定位桶"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=100, ge=1),
) -> dict[str, Any]:
    """按窗口编号、时段、封账状态或订单编号筛选分桶结果。"""
    items, total = runtime.list_buckets(
        window_id=window_id, bucket_id=bucket_id, start=start, end=end,
        sealed=sealed, order_id=order_id, page=page, size=_page(size),
    )
    return {"ok": True, "items": items, "total": total, "page": page, "size": _page(size)}


@router.get("/bucket")
def get_bucket(bucket_id: str = Query(..., description="桶编号，形如 窗口编号@开始时间")) -> dict[str, Any]:
    """单个桶的金额、独立用户、批次历史和纳入记录。"""
    item = runtime.get_bucket(bucket_id)
    if item is None:
        raise HTTPException(status_code=404, detail=f"桶 {bucket_id} 不存在或尚未归桶")
    return {"ok": True, "result": item}


@router.get("/bucket-metrics")
def list_bucket_metrics(
    window_id: str | None = Query(default=None),
    bucket_id: str | None = Query(default=None),
    latest_only: bool = Query(default=True, description="只看各桶当前批次"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=200, ge=1),
) -> dict[str, Any]:
    """分桶指标清单：按桶查询金额与独立用户。"""
    items, total = runtime.list_bucket_metrics(
        window_id=window_id, bucket_id=bucket_id, latest_only=latest_only,
        page=page, size=_page(size),
    )
    return {"ok": True, "items": items, "total": total, "page": page, "size": _page(size)}


@router.get("/batches")
def list_batches(
    window_id: str | None = Query(default=None),
    bucket_id: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=200, ge=1),
) -> dict[str, Any]:
    """批次清单：每个桶的版本、记录范围、原桶引用与更正关系。"""
    items, total = runtime.list_batches(
        window_id=window_id, bucket_id=bucket_id, page=page, size=_page(size),
    )
    return {"ok": True, "items": items, "total": total, "page": page, "size": _page(size)}


@router.get("/records")
def list_records(
    window_id: str | None = Query(default=None),
    bucket_id: str | None = Query(default=None),
    order_id: str | None = Query(default=None),
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    late: bool | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=200, ge=1),
) -> dict[str, Any]:
    """按订单编号、事件时段或桶定位订单转换记录。"""
    items, total = runtime.list_records(
        window_id=window_id, bucket_id=bucket_id, order_id=order_id,
        start=start, end=end, late=late, page=page, size=_page(size),
    )
    return {"ok": True, "items": items, "total": total, "page": page, "size": _page(size)}


@router.get("/outside")
def list_outside(
    window_id: str | None = Query(default=None),
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    order_id: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=200, ge=1),
) -> dict[str, Any]:
    """窗外记录：按事件时段检索并给出落窗原因。"""
    items, total = runtime.list_outside(
        window_id=window_id, start=start, end=end, order_id=order_id,
        page=page, size=_page(size),
    )
    return {"ok": True, "items": items, "total": total, "page": page, "size": _page(size)}


@router.get("/executions")
def list_executions(
    window_id: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=50, ge=1),
) -> dict[str, Any]:
    items, total = runtime.list_executions(window_id=window_id, page=page, size=_page(size))
    return {"ok": True, "items": items, "total": total, "page": page, "size": _page(size)}


@router.get("/executions/{execution_id}")
def get_execution(execution_id: str) -> dict[str, Any]:
    item = runtime.get_execution(execution_id)
    if item is None:
        raise HTTPException(status_code=404, detail=f"分窗执行 {execution_id} 不存在")
    return {"ok": True, "result": item}


@router.post("/confirm")
def confirm(payload: dict[str, Any]) -> ActionResult:
    """下游确认某个桶：确认范围与指标来源保持一致，并按策略合并迟到记录。"""
    bucket_id = str(payload.get("bucket_id") or "").strip()
    if not bucket_id:
        return ActionResult(ok=False, message="缺少必填字段：bucket_id")
    try:
        result = runtime.confirm_bucket(
            bucket_id,
            batch_id=payload.get("batch_id"),
            emission_id=payload.get("emission_id"),
        )
    except runtime.WindowRuntimeError as exc:
        return ActionResult(ok=False, message=str(exc))
    if result is None:
        return ActionResult(ok=False, message=f"桶 {bucket_id} 不存在")
    return ActionResult(ok=True, message="下游确认已记录", entry=result)


@router.get("/confirmations")
def list_confirmations(
    bucket_id: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=200, ge=1),
) -> dict[str, Any]:
    items, total = runtime.list_confirmations(
        bucket_id=bucket_id, page=page, size=_page(size),
    )
    return {"ok": True, "items": items, "total": total, "page": page, "size": _page(size)}


@router.get("/late-pending")
def list_late_pending(bucket_id: str | None = Query(default=None)) -> dict[str, Any]:
    rows = [
        dict(r) for r in runtime.store.rows(runtime.PENDING_LATE)
        if not bucket_id or r.get("bucket_id") == bucket_id
    ]
    return {"ok": True, "items": rows, "total": len(rows)}

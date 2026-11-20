"""指标运行时：消费分窗指标聚合结果，产出可追溯的指标与指标发射快照。

指标计算不再直接扫整批订单。它只认 ``window_runtime`` 给出的“桶 -> 当前批次”
关系：每个桶只取一个批次版本，指标结果随结果一起返回实际引用的批次清单。
每次计算都会留一份发射快照，后续可以按当时引用的桶和批次复算，避免同一指标
在更正前后混用两个桶版本。
"""
from __future__ import annotations

from typing import Any, Iterable

from app import fixtures
from app.services import window_runtime as wr
from app.store import store

EMISSIONS = "_metric_emissions"

METRIC_ORDER = ("METR-ORDER-COUNT", "METR-GMV", "METR-AVG-ORDER", "METR-USER-DISTINCT")


def _now_iso() -> str:
    return wr._iso(wr._now())


def _metric_defs() -> list[dict[str, Any]]:
    return list(fixtures.METRIC_DEFS)


def _metric_index() -> dict[str, dict[str, Any]]:
    return {str(m["指标编号"]): m for m in _metric_defs()}


def _unique_records(bucket_summaries: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """按订单编号去重，重叠滑窗里同一条订单只算一次。"""
    records: dict[str, dict[str, Any]] = {}
    for summary in bucket_summaries:
        for row in wr.list_records(bucket_id=summary["bucket_id"], page=1, size=100000)[0]:
            order_id = str(row.get("订单编号") or row.get("record_id"))
            records.setdefault(order_id, row)
    return records


def _values_from_records(records: dict[str, dict[str, Any]]) -> dict[str, float]:
    amounts = [wr._to_float(r.get("金额")) for r in records.values()]
    count = len(amounts)
    distinct_users = len({str(r.get("用户ID")) for r in records.values()
                          if r.get("用户ID") not in (None, "")})
    gmv = round(sum(amounts), 2)
    return {
        "METR-ORDER-COUNT": count,
        "METR-GMV": gmv,
        "METR-AVG-ORDER": round(gmv / count, 2) if count else 0.0,
        "METR-USER-DISTINCT": distinct_users,
    }


def _values_from_summaries(summaries: Iterable[dict[str, Any]]) -> dict[str, float]:
    """按聚合批次里记录的范围和快照值复算，不依赖当前订单表状态。"""
    summaries = list(summaries)
    amounts: dict[str, float] = {}
    for summary in summaries:
        money = summary.get("单笔金额") or {}
        for order_id in summary.get("records") or summary.get("记录范围") or []:
            order_id = str(order_id)
            if order_id in amounts:
                continue
            if order_id in money:
                amounts[order_id] = wr._to_float(money[order_id])
    if len(amounts) != sum(int(s.get("记录数") or 0) for s in summaries):
        # 快照缺单笔金额时退回桶级汇总，金额与笔数仍按批次记录范围核算。
        count = sum(int(s.get("记录数") or 0) for s in summaries)
        gmv = round(sum(wr._to_float(s.get("gmv")) for s in summaries), 2)
        users = {str(u) for s in summaries for u in (s.get("用户ID") or [])}
        return {
            "METR-ORDER-COUNT": count,
            "METR-GMV": gmv,
            "METR-AVG-ORDER": round(gmv / count, 2) if count else 0.0,
            "METR-USER-DISTINCT": len(users),
        }
    count = len(amounts)
    gmv = round(sum(amounts.values()), 2)
    users = {str(u) for s in summaries for u in (s.get("用户ID") or [])}
    return {
        "METR-ORDER-COUNT": count,
        "METR-GMV": gmv,
        "METR-AVG-ORDER": round(gmv / count, 2) if count else 0.0,
        "METR-USER-DISTINCT": len(users),
    }


def _resolve_window_id(window_id: str | None, aggregation: dict[str, Any]) -> str:
    if window_id:
        return window_id
    for config in aggregation.get("窗口配置", []):
        if wr.is_fixed_window(config):
            return str(config.get("窗口编号"))
    return str(aggregation.get("窗口结果") and next(iter(aggregation["窗口结果"]), ""))


def compute_metrics(*, window_id: str | None = None,
                    aggregation: dict[str, Any] | None = None) -> dict[str, Any]:
    """从分窗聚合结果计算单窗口指标，并给出实际引用的批次。"""
    aggregation = aggregation or wr.build_aggregation(window_id)
    resolved = _resolve_window_id(window_id, aggregation)
    summary = (aggregation.get("窗口结果") or {}).get(resolved)
    if summary is None:
        raise ValueError(f"窗口 {resolved} 不在分窗聚合结果里")

    summaries = list(summary.get("分桶") or [])
    records = _unique_records(summaries)
    values = _values_from_records(records)
    order_amounts = {
        order_id: wr._to_float(row.get("金额")) for order_id, row in records.items()
    }

    defs = _metric_index()
    metrics: dict[str, float] = {}
    details: list[dict[str, Any]] = []
    for metric_id in METRIC_ORDER:
        value = values.get(metric_id, 0)
        metrics[metric_id] = value
        definition = defs.get(metric_id, {})
        details.append({
            "指标编号": metric_id,
            "指标名称": definition.get("指标名称", metric_id),
            "计算公式": definition.get("计算公式", ""),
            "值": value,
            "单位": "笔" if metric_id == "METR-ORDER-COUNT" else ("元" if "GMV" in metric_id or "AVG" in metric_id else "人"),
        })

    return {
        "窗口编号": resolved,
        "窗口类型": summary.get("窗口类型"),
        "桶数": summary.get("桶数"),
        "记录数": len(records),
        "gmv": values["METR-GMV"],
        "独立用户": values["METR-USER-DISTINCT"],
        "指标": details,
        "metrics": metrics,
        "明细": {
            "订单": order_amounts,
            "记录范围": sorted(records),
        },
        "分桶": summaries,
        "批次引用": [
            {"bucket_id": s.get("bucket_id"), "batch_id": s.get("batch_id"),
             "版本": s.get("version"), "记录数": s.get("记录数")}
            for s in summaries
        ],
        "聚合批次引用": [
            {"window_id": s.get("窗口编号"), "bucket_id": s.get("bucket_id"),
             "batch_id": s.get("batch_id"), "版本": s.get("version")}
            for s in summaries
        ],
        "计算时间": _now_iso(),
    }


def _snapshot(summary: dict[str, Any]) -> dict[str, Any]:
    records = wr.list_records(bucket_id=summary["bucket_id"], page=1, size=100000)[0]
    return {
        "bucket_id": summary.get("bucket_id"),
        "batch_id": summary.get("batch_id"),
        "version": summary.get("version"),
        "窗口开始": summary.get("窗口开始"),
        "窗口结束": summary.get("窗口结束"),
        "records": list(summary.get("记录范围") or []),
        "记录数": summary.get("记录数"),
        "gmv": summary.get("gmv"),
        "独立用户": summary.get("独立用户"),
        "用户ID": list(summary.get("用户ID") or []),
        "单笔金额": {
            str(r.get("订单编号") or r.get("record_id")): wr._to_float(r.get("金额"))
            for r in records
        },
    }


def run_metric_calculation(*, window_id: str | None = None,
                           aggregation: dict[str, Any] | None = None) -> dict[str, Any]:
    """执行一次指标计算并登记发射快照（供下游确认与复算）。"""
    aggregation = aggregation or wr.build_aggregation(window_id)
    result = compute_metrics(window_id=window_id, aggregation=aggregation)
    emission_id = wr._next_id("MEMS", EMISSIONS, "emission_id")
    emission = {
        "emission_id": emission_id,
        "window_id": result["窗口编号"],
        "指标": result["指标"],
        "metrics": result["metrics"],
        "bucket_snapshots": [_snapshot(s) for s in result["分桶"]],
        "批次引用": result["聚合批次引用"],
        "记录数": result["记录数"],
        "gmv": result["gmv"],
        "独立用户": result["独立用户"],
        "created_at": result["计算时间"],
    }
    store.rows(EMISSIONS).append(emission)
    result["emission_id"] = emission_id
    result["发射"] = {
        "emission_id": emission_id,
        "批次引用": result["聚合批次引用"],
        "记录范围": [s.get("records") for s in emission["bucket_snapshots"]],
    }
    return result


def get_emission(emission_id: str) -> dict[str, Any] | None:
    for row in store.rows(EMISSIONS):
        if row.get("emission_id") == emission_id:
            return row
    return None


def list_emissions(*, window_id: str | None = None,
                   page: int = 1, size: int = 50) -> tuple[list[dict[str, Any]], int]:
    rows = [dict(r) for r in store.rows(EMISSIONS)
            if not window_id or r.get("window_id") == window_id]
    rows.reverse()
    total = len(rows)
    start = max(0, (page - 1) * size)
    return rows[start:start + size], total


def latest_result(window_id: str | None = None) -> dict[str, Any] | None:
    """按当前批次重新计算，并带上本次实际引用的批次版本。"""
    aggregation = wr.build_aggregation(window_id)
    try:
        result = compute_metrics(window_id=window_id, aggregation=aggregation)
    except ValueError:
        return None
    emissions, _ = list_emissions(window_id=result["窗口编号"], page=1, size=1)
    result["最近发射"] = (
        {"emission_id": emissions[0]["emission_id"], "created_at": emissions[0]["created_at"],
         "批次引用": emissions[0].get("批次引用", [])}
        if emissions else None
    )
    return result


def recompute_emission(emission_id: str) -> dict[str, Any] | None:
    """按发射时的桶与批次复算，确认已发指标没有被后来的更正批次污染。"""
    emission = get_emission(emission_id)
    if emission is None:
        return None
    snapshots = emission.get("bucket_snapshots") or []
    recomputed = _values_from_summaries(snapshots)
    emitted = {str(k): float(v) for k, v in (emission.get("metrics") or {}).items()}
    differences = {
        key: {"emitted": emitted.get(key), "recomputed": recomputed.get(key)}
        for key in set(emitted) | set(recomputed)
        if round(float(emitted.get(key, 0)), 4) != round(float(recomputed.get(key, 0)), 4)
    }
    batch_refs = [
        {"bucket_id": s.get("bucket_id"), "batch_id": s.get("batch_id"),
         "版本": s.get("version"), "记录范围": s.get("records")}
        for s in snapshots
    ]
    return {
        "emission_id": emission_id,
        "window_id": emission.get("window_id"),
        "原发射": emitted,
        "复算": recomputed,
        "一致": not differences,
        "差异": differences,
        "批次引用": batch_refs,
        "记录范围": [s.get("records") for s in snapshots],
        "复算时间": _now_iso(),
    }


def metric_result_for(metric_id: str, window_id: str | None = None) -> dict[str, Any] | None:
    """指标详情用：只交代当前指标实际引用的聚合批次和结果。"""
    if metric_id not in METRIC_ORDER:
        return None
    result = latest_result(window_id)
    if result is None:
        return None
    value = (result.get("metrics") or {}).get(metric_id)
    definition = _metric_index().get(metric_id, {})
    return {
        "指标编号": metric_id,
        "指标名称": definition.get("指标名称", metric_id),
        "值": value,
        "窗口编号": result["窗口编号"],
        "桶数": result["桶数"],
        "记录数": result["记录数"],
        "聚合批次引用": result["聚合批次引用"],
        "批次引用": result["批次引用"],
        "最近发射": result.get("最近发射"),
        "计算时间": result.get("计算时间"),
    }

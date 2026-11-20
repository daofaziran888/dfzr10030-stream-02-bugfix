"""分窗运行时：按事件时间给订单转换记录归桶、封账并产出分窗指标。

这个模块是窗口聚合与指标计算之间的唯一数据来源，解决两类问题：

1. 窗口边界必须来自窗口配置。滚动窗口按 ``窗口长度`` 对齐，滑动窗口按
   ``滑动步长`` 对齐；一条记录可以同时落进多个重叠桶，但每个桶只聚合
   属于自己 ``[窗口开始, 窗口结束)`` 的记录，不能拿整批记录冒充分桶结果。
2. 同一桶内重复补投只计一次；桶按事件时间封账后再到的记录必须生成新的
   计算批次，并在批次上保留原桶引用、上一批次和本次纳入的记录范围。指标
   选择批次时每个桶只选一个版本，避免同一指标混用两个桶版本。

运行状态存放在 ``store`` 以下划线开头的内部表里，与业务模块数据隔离。
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from app import fixtures
from app.config import settings
from app.store import store

# ---------------------------------------------------------------------------
# 内部状态表
# ---------------------------------------------------------------------------
BUCKETS = "_window_buckets"
BATCHES = "_window_batches"
RECORDS = "_window_records"
DELIVERIES = "_window_deliveries"
BUCKET_METRICS = "_window_bucket_metrics"
OUTSIDE = "_window_outside"
WATERMARKS = "_window_watermarks"
PENDING_LATE = "_window_late_pending"
CONFIRMATIONS = "_window_confirmations"
EXECUTIONS = "_window_executions"
CONFIGS = "_window_configs"

INTERNAL_TABLES = (
    BUCKETS, BATCHES, RECORDS, DELIVERIES, BUCKET_METRICS,
    OUTSIDE, WATERMARKS, PENDING_LATE, CONFIRMATIONS, EXECUTIONS, CONFIGS,
)

ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
UTC = timezone.utc
FIXED_WINDOW_TYPES = {"滚动窗口", "滑动窗口", "tumbling", "sliding"}
LATE_POLICIES = {"立即更正", "延迟合并"}

REASON_TOO_LATE = "超出迟到阈值，窗口状态已过期"
REASON_BAD_TIME = "事件时间缺失或无法解析"
REASON_SESSION = "会话窗口按处理时间运行，不参与事件时间分桶"
REASON_DUPLICATE = "同一桶内已计入，本次为重复投递"


class WindowRuntimeError(ValueError):
    """窗口运行时参数不合法时抛出，路由层转成可读的 4xx。"""


# ---------------------------------------------------------------------------
# 基础工具
# ---------------------------------------------------------------------------
def _now() -> datetime:
    return datetime.now(UTC)


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime(ISO_FORMAT)


def _from_ts(ts: int) -> datetime:
    return datetime.fromtimestamp(int(ts), UTC)


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return default


def parse_event_time(value: Any) -> datetime | None:
    """把 ``2026-11-16T08:05:00Z`` 等写法统一成 UTC datetime。"""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    text = str(value).strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        dt = None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y/%m/%d %H:%M:%S"):
            try:
                dt = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
        if dt is None:
            return None
    return dt.astimezone(UTC) if dt.tzinfo else dt.replace(tzinfo=UTC)


def _next_id(prefix: str, table: str, key: str) -> str:
    max_seq = 0
    for row in store.rows(table):
        value = str(row.get(key, ""))
        tail = value.rsplit("-", 1)[-1]
        if tail.isdigit():
            max_seq = max(max_seq, int(tail))
    return f"{prefix}-{max_seq + 1:06d}"


def _row_by(table: str, key: str, value: Any) -> dict[str, Any] | None:
    for row in store.rows(table):
        if row.get(key) == value:
            return row
    return None


def _record_key(record: dict[str, Any]) -> str:
    """订单编号优先；没有编号时用关键字段生成稳定指纹。"""
    order_id = str(record.get("订单编号") or "").strip()
    if order_id:
        return order_id
    digest = hashlib.sha1(
        json.dumps(
            {k: record.get(k) for k in ("金额", "用户ID", "event_time", "渠道", "地区")},
            ensure_ascii=False, sort_keys=True, default=str,
        ).encode("utf-8")
    ).hexdigest()[:16]
    return f"REC-{digest}"


# ---------------------------------------------------------------------------
# 窗口配置与边界
# ---------------------------------------------------------------------------
def get_window_configs(window_id: str | None = None,
                       configs: Iterable[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """读取窗口配置；窗口边界、步长、迟到阈值都从这里取。

    配置先以 fixtures 为底，执行时显式传入的配置会登记到内部配置表并覆盖同名窗口，
    这样后续按 ``window_id`` 查询 / 计算指标仍能拿到同一份边界定义。
    """
    if configs is not None:
        register_configs(configs)
        # 显式传入的配置表示“本次只看这些窗口”，但字段以登记后的完整值为准。
        source = []
        for config in configs:
            stored = _row_by(CONFIGS, "窗口编号", str(config.get("窗口编号")))
            source.append(dict(stored) if stored else dict(config))
    else:
        source = [dict(c) for c in fixtures.WINDOW_CONFIGS]
        for row in store.rows(CONFIGS):
            source = [c for c in source if str(c.get("窗口编号")) != str(row.get("窗口编号"))]
            source.append(dict(row))
    if window_id:
        source = [cfg for cfg in source if str(cfg.get("窗口编号")) == str(window_id)]
        if not source:
            raise WindowRuntimeError(f"窗口配置 {window_id} 不存在")
    return source


def register_configs(configs: Iterable[dict[str, Any]]) -> None:
    """登记窗口配置：窗口边界、步长、迟到阈值都以这里登记的值为准。"""
    for config in configs:
        window_id = str(config.get("窗口编号") or "").strip()
        if not window_id:
            continue
        row = _row_by(CONFIGS, "窗口编号", window_id)
        payload = {k: v for k, v in config.items() if v is not None}
        if row is None:
            store.rows(CONFIGS).append(dict(payload))
        else:
            row.update(payload)


def is_fixed_window(config: dict[str, Any]) -> bool:
    return str(config.get("窗口类型", "")) in FIXED_WINDOW_TYPES


def window_step(config: dict[str, Any]) -> int:
    return max(1, _to_int(config.get("滑动步长秒"), 1))


def window_length(config: dict[str, Any]) -> int:
    return max(0, _to_int(config.get("窗口长度秒"), 0))


def window_bounds(config: dict[str, Any], event_time: datetime) -> list[tuple[int, int]]:
    """返回事件时间命中的所有 ``[开始, 结束)`` 桶。

    桶起点按 epoch 对齐到滑动步长；滚动窗口长度等于步长，天然只落一个桶；
    滑动窗口按步长枚举起点，因此一条订单可以进入多个重叠桶。
    """
    length = window_length(config)
    step = window_step(config)
    if length <= 0:
        return []
    ts = int(event_time.timestamp())
    latest_start = (ts // step) * step
    starts: list[int] = []
    start = latest_start
    while start + length > ts:
        starts.append(start)
        start -= step
    return [(s, s + length) for s in sorted(starts)]


def bucket_id_for(config: dict[str, Any], start_ts: int) -> str:
    return f"{config.get('窗口编号')}@{_from_ts(start_ts).strftime(ISO_FORMAT)}"


# ---------------------------------------------------------------------------
# 桶 / 记录 / 批次读取辅助
# ---------------------------------------------------------------------------
def _bucket_records(bucket_id: str) -> list[dict[str, Any]]:
    rows = [r for r in store.rows(RECORDS) if r.get("bucket_id") == bucket_id]
    return sorted(rows, key=lambda r: (str(r.get("event_time", "")), str(r.get("order_id", ""))))


def _bucket_record_ids(bucket_id: str) -> list[str]:
    return [str(r.get("order_id")) for r in _bucket_records(bucket_id)]


def _batch_record_ids(batch: dict[str, Any]) -> list[str]:
    return [str(x) for x in (batch.get("records") or batch.get("记录范围") or [])]


def _users_for_records(rows: Iterable[dict[str, Any]]) -> list[str]:
    return sorted({str(r.get("user_id")) for r in rows if r.get("user_id") not in (None, "")})


def _aggregate_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    amounts = [_to_float(r.get("amount")) for r in rows]
    gmv = round(sum(amounts), 2)
    count = len(rows)
    channel: dict[str, int] = {}
    for row in rows:
        key = str(row.get("channel") or "")
        channel[key] = channel.get(key, 0) + 1
    return {
        "gmv": gmv,
        "金额": gmv,
        "记录数": count,
        "独立用户": len(_users_for_records(rows)),
        "用户ID": _users_for_records(rows),
        "平均订单": round(gmv / count, 2) if count else 0.0,
        "渠道分布": channel,
    }


def _apply_batch_metrics(batch: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    values = _aggregate_rows(rows)
    batch.update(values)
    batch["records"] = [str(r.get("order_id")) for r in rows]
    batch["记录范围"] = list(batch["records"])
    batch["record_count"] = len(rows)


def _later_batch(bucket_id: str, batch_id: str) -> dict[str, Any] | None:
    """同一桶里比给定批次更新的版本，用来判断已有指标是否被更正。"""
    current = _row_by(BUCKETS, "bucket_id", bucket_id)
    if current is None:
        return None
    latest = _row_by(BATCHES, "batch_id", current.get("current_batch_id"))
    if latest is None or latest.get("batch_id") == batch_id:
        return None
    return latest


def _upsert_bucket_metric(batch: dict[str, Any]) -> None:
    row = _row_by(BUCKET_METRICS, "batch_id", batch.get("batch_id"))
    later = _later_batch(str(batch.get("bucket_id")), str(batch.get("batch_id")))
    payload = {
        "bucket_id": batch.get("bucket_id"),
        "window_id": batch.get("window_id"),
        "batch_id": batch.get("batch_id"),
        "版本": batch.get("version"),
        "窗口开始": batch.get("窗口开始"),
        "窗口结束": batch.get("窗口结束"),
        "gmv": batch.get("gmv"),
        "金额": batch.get("金额"),
        "记录数": batch.get("记录数"),
        "独立用户": batch.get("独立用户"),
        "用户ID": batch.get("用户ID", []),
        "平均订单": batch.get("平均订单"),
        "渠道分布": batch.get("渠道分布", {}),
        "状态": batch.get("状态"),
        "是否最新": later is None,
        "被更正为": later.get("batch_id") if later else None,
        "updated_at": _iso(_now()),
    }
    if row is None:
        store.rows(BUCKET_METRICS).append(dict(payload))
    else:
        row.update(payload)


def _create_batch(bucket: dict[str, Any], *, trigger: str, new_ids: list[str],
                  policy: str, sealed: bool, previous_batch_id: str | None = None,
                  watermark: datetime | None = None) -> dict[str, Any]:
    rows = _bucket_records(bucket["bucket_id"])
    version = int(bucket.get("版本", 0)) + 1
    batch_id = _next_id("WAGG", BATCHES, "batch_id")
    batch = {
        "batch_id": batch_id,
        "bucket_id": bucket["bucket_id"],
        "window_id": bucket["窗口编号"],
        "version": version,
        "版本": version,
        "窗口开始": bucket.get("窗口开始"),
        "窗口结束": bucket.get("窗口结束"),
        "状态": "已封账" if sealed else "未封账",
        "sealed": bool(sealed),
        "封账水位": _iso(watermark) if (sealed and watermark) else None,
        "records": [str(r.get("order_id")) for r in rows],
        "记录范围": [str(r.get("order_id")) for r in rows],
        "new_records": [str(x) for x in new_ids],
        "本次纳入": [str(x) for x in new_ids],
        "previous_batch_id": previous_batch_id,
        "上一批次": previous_batch_id,
        "original_bucket_id": bucket["bucket_id"],
        "原桶引用": bucket["bucket_id"],
        "trigger": trigger,
        "触发原因": trigger,
        "late_policy": policy,
        "迟到策略": policy,
        "created_at": _iso(_now()),
    }
    _apply_batch_metrics(batch, rows)
    store.rows(BATCHES).append(batch)

    previous = _row_by(BATCHES, "batch_id", previous_batch_id) if previous_batch_id else None
    if previous is not None and previous.get("batch_id") != batch_id:
        previous["状态"] = "已被更正"
        previous["superseded_by"] = batch_id
        _upsert_bucket_metric(previous)

    bucket["current_batch_id"] = batch_id
    bucket["版本"] = version
    bucket["updated_at"] = _iso(_now())
    _upsert_bucket_metric(batch)
    return batch


def _sync_working_batch(bucket: dict[str, Any], new_ids: list[str]) -> dict[str, Any]:
    """未封账桶的批次在封账前可继续吸收记录，但始终只有一个当前版本。"""
    current = _row_by(BATCHES, "batch_id", bucket.get("current_batch_id"))
    if current is None:
        return _create_batch(bucket, trigger="首次聚合", new_ids=new_ids,
                             policy="立即更正", sealed=False)
    if current.get("sealed"):
        return _create_batch(bucket, trigger="迟到更正", new_ids=new_ids,
                             policy=str(current.get("late_policy") or settings.window_late_policy),
                             sealed=True, previous_batch_id=current["batch_id"],
                             watermark=parse_event_time(bucket.get("封账水位")))
    rows = _bucket_records(bucket["bucket_id"])
    _apply_batch_metrics(current, rows)
    included = list(current.get("本次纳入") or [])
    for order_id in new_ids:
        if order_id not in included:
            included.append(order_id)
    current["本次纳入"] = included
    current["new_records"] = list(included)
    current["updated_at"] = _iso(_now())
    _upsert_bucket_metric(current)
    return current


# ---------------------------------------------------------------------------
# 归桶 / 迟到 / 重复投递
# ---------------------------------------------------------------------------
def _outside(execution: dict[str, Any], *, config: dict[str, Any], record: dict[str, Any],
             reason: str, event_time: Any = None, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    row = {
        "record_id": _record_key(record),
        "订单编号": record.get("订单编号"),
        "金额": _to_float(record.get("金额")),
        "用户ID": record.get("用户ID"),
        "event_time": event_time if event_time is not None else record.get("event_time"),
        "window_id": config.get("窗口编号"),
        "窗口编号": config.get("窗口编号"),
        "reason": reason,
        "原因": reason,
        "recorded_at": _iso(_now()),
    }
    if extra:
        row.update(extra)
    store.rows(OUTSIDE).append(row)
    execution["窗外"].append(dict(row))
    return row


def _delivery(execution: dict[str, Any], *, config: dict[str, Any], record: dict[str, Any],
              bucket_id: str | None, status: str, reason: str | None = None,
              batch_id: str | None = None) -> dict[str, Any]:
    row = {
        "delivery_id": _next_id("WDLV", DELIVERIES, "delivery_id"),
        "订单编号": record.get("订单编号"),
        "record_id": _record_key(record),
        "window_id": config.get("窗口编号"),
        "bucket_id": bucket_id,
        "batch_id": batch_id,
        "status": status,
        "reason": reason,
        "delivered_at": _iso(_now()),
    }
    store.rows(DELIVERIES).append(row)
    if status == "重复投递":
        execution["重复投递"].append(dict(row))
    return row


def _ensure_bucket(config: dict[str, Any], start_ts: int, end_ts: int) -> dict[str, Any]:
    bucket_id = bucket_id_for(config, start_ts)
    bucket = _row_by(BUCKETS, "bucket_id", bucket_id)
    if bucket is None:
        bucket = {
            "bucket_id": bucket_id,
            "窗口编号": config.get("窗口编号"),
            "window_id": config.get("窗口编号"),
            "窗口类型": config.get("窗口类型"),
            "窗口开始": _iso(_from_ts(start_ts)),
            "窗口结束": _iso(_from_ts(end_ts)),
            "start_ts": start_ts,
            "end_ts": end_ts,
            "窗口长度秒": window_length(config),
            "滑动步长秒": window_step(config),
            "迟到阈值秒": _to_int(config.get("迟到阈值秒"), 0),
            "水位延迟秒": _to_int(config.get("水位延迟秒"), 0),
            "current_batch_id": None,
            "版本": 0,
            "封账": False,
            "封账水位": None,
            "created_at": _iso(_now()),
            "updated_at": _iso(_now()),
        }
        store.rows(BUCKETS).append(bucket)
    return bucket


def _watermark_row(window_id: str) -> dict[str, Any] | None:
    return _row_by(WATERMARKS, "window_id", window_id)


def _current_watermark(config: dict[str, Any]) -> tuple[datetime | None, datetime | None]:
    row = _watermark_row(str(config.get("窗口编号")))
    if not row:
        return None, None
    return parse_event_time(row.get("watermark")), parse_event_time(row.get("max_event_time"))


def _advance_watermark(config: dict[str, Any], max_event_time: datetime) -> datetime:
    window_id = str(config.get("窗口编号"))
    old_wm, old_max = _current_watermark(config)
    new_max = max([dt for dt in (old_max, max_event_time) if dt is not None])
    delay = _to_int(config.get("水位延迟秒"), 0)
    watermark = new_max - timedelta(seconds=delay)
    row = _watermark_row(window_id)
    payload = {
        "window_id": window_id,
        "水位类型": config.get("水位类型"),
        "max_event_time": _iso(new_max),
        "watermark": _iso(watermark),
        "updated_at": _iso(_now()),
    }
    if row is None:
        store.rows(WATERMARKS).append(payload)
    else:
        row.update(payload)
    return watermark


def _seal_buckets(config: dict[str, Any], watermark: datetime,
                   execution: dict[str, Any]) -> list[dict[str, Any]]:
    sealed: list[dict[str, Any]] = []
    for bucket in store.rows(BUCKETS):
        if bucket.get("窗口编号") != config.get("窗口编号"):
            continue
        if bucket.get("封账") or int(bucket.get("end_ts", 0)) > int(watermark.timestamp()):
            continue
        batch = _row_by(BATCHES, "batch_id", bucket.get("current_batch_id"))
        if batch is None:
            continue
        bucket["封账"] = True
        bucket["封账水位"] = _iso(watermark)
        bucket["updated_at"] = _iso(_now())
        batch["sealed"] = True
        batch["状态"] = "已封账" if batch.get("状态") != "已被更正" else batch.get("状态")
        batch["封账水位"] = _iso(watermark)
        batch["sealed_at"] = _iso(_now())
        _upsert_bucket_metric(batch)
        sealed.append(batch)
        execution["封账批次"].append({"bucket_id": bucket["bucket_id"], "batch_id": batch["batch_id"],
                                  "版本": batch.get("version"), "封账水位": _iso(watermark)})
    return sealed


def _ingest_config(config: dict[str, Any], source_records: list[dict[str, Any]], *,
                   policy: str, confirm_pending: bool,
                   execution: dict[str, Any]) -> None:
    window_id = str(config.get("窗口编号"))
    if not is_fixed_window(config):
        execution["说明"][window_id] = REASON_SESSION
        return

    old_wm, _ = _current_watermark(config)
    max_seen: datetime | None = None
    additions: dict[str, dict[str, Any]] = {}
    lateness = _to_int(config.get("迟到阈值秒"), 0)

    for record in source_records:
        dt = parse_event_time(record.get("event_time"))
        if dt is None:
            _outside(execution, config=config, record=record, reason=REASON_BAD_TIME)
            continue
        max_seen = dt if max_seen is None or dt > max_seen else max_seen
        bounds = window_bounds(config, dt)
        if not bounds:
            _outside(execution, config=config, record=record, reason="窗口长度为 0，无法按事件时间归桶")
            continue
        for start_ts, end_ts in bounds:
            bucket_id = bucket_id_for(config, start_ts)
            order_id = _record_key(record)
            existing = [
                r for r in store.rows(RECORDS)
                if r.get("bucket_id") == bucket_id and str(r.get("order_id")) == order_id
            ]
            if existing:
                _delivery(execution, config=config, record=record,
                          bucket_id=bucket_id, status="重复投递",
                          reason=REASON_DUPLICATE, batch_id=existing[0].get("batch_id"))
                execution["重复投递计数"] += 1
                continue

            sealed = old_wm is not None and end_ts <= int(old_wm.timestamp())
            if sealed and old_wm is not None and int(old_wm.timestamp()) >= end_ts + lateness:
                _outside(
                    execution, config=config, record=record, reason=REASON_TOO_LATE,
                    event_time=_iso(dt),
                    extra={"bucket_id": bucket_id,
                           "watermark": _iso(old_wm),
                           "迟到阈值秒": lateness,
                           "允许截止": _iso(_from_ts(end_ts + lateness))},
                )
                continue

            bucket = _ensure_bucket(config, start_ts, end_ts)
            slot = additions.setdefault(bucket_id, {
                "bucket": bucket, "records": [], "late": sealed, "seen": set(),
            })
            if order_id in slot["seen"]:
                # 同一批里重复补投：本批尚未落库，也要按桶内去重。
                _delivery(execution, config=config, record=record,
                          bucket_id=bucket_id, status="重复投递",
                          reason=REASON_DUPLICATE, batch_id=bucket.get("current_batch_id"))
                execution["重复投递计数"] += 1
                continue
            slot["seen"].add(order_id)
            slot["records"].append({"record": dict(record), "event_time": dt, "late": sealed})

    # 先按本批最大事件时间推进水位，再判断迟到：封账水位取的是本批进来之前的
    # 水位，这样同一批里新到的、属于旧桶的记录才能被识别为迟到。
    if max_seen is not None:
        _advance_watermark(config, max_seen)

    # 落记录并生成 / 更新批次 -------------------------------------------------
    for bucket_id, slot in additions.items():
        bucket = slot["bucket"]
        ordered = sorted(slot["records"], key=lambda x: (x["event_time"], _record_key(x["record"])))
        new_ids = [_record_key(x["record"]) for x in ordered]
        batch_id_for_records = bucket.get("current_batch_id")
        if slot["late"]:
            if policy == "延迟合并":
                for item in ordered:
                    pending = {
                        "pending_id": _next_id("WLAT", PENDING_LATE, "pending_id"),
                        "bucket_id": bucket_id,
                        "window_id": window_id,
                        "order_id": _record_key(item["record"]),
                        "event_time": _iso(item["event_time"]),
                        "batch_id_at_arrival": batch_id_for_records,
                        "policy": policy,
                        "status": "待下游确认后合并",
                        "record": dict(item["record"]),
                        "created_at": _iso(_now()),
                    }
                    store.rows(PENDING_LATE).append(pending)
                    execution["迟到"].append(dict(pending, 处理="延迟合并，等待下游确认"))
                continue

            for item in ordered:
                _store_record(bucket, item["record"], item["event_time"], late=True,
                              batch_id=batch_id_for_records)
            batch = _create_batch(bucket, trigger="迟到更正", new_ids=new_ids, policy=policy,
                                  sealed=True, previous_batch_id=batch_id_for_records,
                                  watermark=old_wm)
            execution["迟到"].append({
                "bucket_id": bucket_id,
                "order_ids": new_ids,
                "batch_id": batch["batch_id"],
                "previous_batch_id": batch_id_for_records,
                "版本": batch.get("version"),
                "处理": "立即更正",
                "原桶引用": bucket_id,
                "本次纳入": new_ids,
            })
            execution["更正次数"] += 1
        else:
            for item in ordered:
                _store_record(bucket, item["record"], item["event_time"], late=False,
                              batch_id=batch_id_for_records)
            batch = _sync_working_batch(bucket, new_ids)
            for row in store.rows(RECORDS):
                if row.get("bucket_id") == bucket_id and row.get("order_id") in new_ids:
                    row["batch_id"] = batch["batch_id"]
                    row["first_batch_id"] = row.get("first_batch_id") or batch["batch_id"]

    if max_seen is None:
        return
    watermark, _ = _current_watermark(config)
    if watermark is not None:
        _seal_buckets(config, watermark, execution)


def _store_record(bucket: dict[str, Any], record: dict[str, Any], event_time: datetime,
                  *, late: bool, batch_id: str | None) -> dict[str, Any]:
    order_id = _record_key(record)
    row = {
        "record_id": order_id,
        "order_id": order_id,
        "订单编号": record.get("订单编号"),
        "bucket_id": bucket["bucket_id"],
        "window_id": bucket.get("窗口编号"),
        "event_time": _iso(event_time),
        "amount": _to_float(record.get("金额")),
        "金额": _to_float(record.get("金额")),
        "user_id": record.get("用户ID"),
        "channel": record.get("渠道"),
        "region": record.get("地区"),
        "late": bool(late),
        "batch_id": batch_id,
        "first_batch_id": batch_id,
        "record": dict(record),
        "accepted_at": _iso(_now()),
    }
    store.rows(RECORDS).append(row)
    return row


# ---------------------------------------------------------------------------
# 聚合结果
# ---------------------------------------------------------------------------
def _bucket_summary(bucket: dict[str, Any]) -> dict[str, Any] | None:
    batch = _row_by(BATCHES, "batch_id", bucket.get("current_batch_id"))
    if batch is None:
        return None
    return {
        "bucket_id": bucket["bucket_id"],
        "窗口编号": bucket.get("窗口编号"),
        "窗口类型": bucket.get("窗口类型"),
        "窗口开始": bucket.get("窗口开始"),
        "窗口结束": bucket.get("窗口结束"),
        "封账": bool(bucket.get("封账")),
        "封账水位": bucket.get("封账水位"),
        "批次号": batch.get("batch_id"),
        "batch_id": batch.get("batch_id"),
        "批次版本": batch.get("version"),
        "version": batch.get("version"),
        "版本": batch.get("version"),
        "上一批次": batch.get("previous_batch_id"),
        "原桶引用": batch.get("original_bucket_id") or bucket["bucket_id"],
        "记录范围": list(batch.get("记录范围") or []),
        "records": list(batch.get("记录范围") or []),
        "本次纳入": list(batch.get("本次纳入") or []),
        "gmv": batch.get("gmv"),
        "金额": batch.get("金额"),
        "记录数": batch.get("记录数"),
        "独立用户": batch.get("独立用户"),
        "用户ID": list(batch.get("用户ID") or []),
        "平均订单": batch.get("平均订单"),
        "渠道分布": dict(batch.get("渠道分布") or {}),
        "批次状态": batch.get("状态"),
    }


def _window_summary(config: dict[str, Any], buckets: list[dict[str, Any]],
                    summaries: list[dict[str, Any]]) -> dict[str, Any]:
    users: set[str] = set()
    channel: dict[str, int] = {}
    for summary in summaries:
        users.update(summary.get("用户ID") or [])
        for key, value in (summary.get("渠道分布") or {}).items():
            channel[key] = channel.get(key, 0) + int(value)
    gmv = round(sum(_to_float(s.get("gmv")) for s in summaries), 2)
    count = sum(int(s.get("记录数") or 0) for s in summaries)
    # 重叠滑窗里同一条订单会落进多个桶：窗口级订单数和独立用户都按订单去重，
    # 不能把各桶的笔数 / 用户数直接相加。
    distinct_records: dict[str, str | None] = {}
    for summary in summaries:
        for order_id in summary.get("记录范围") or []:
            distinct_records.setdefault(str(order_id), None)
    if len(distinct_records) == count:
        window_users = users
    else:
        for bucket in buckets:
            for record in _bucket_records(bucket["bucket_id"]):
                if record.get("user_id") not in (None, ""):
                    distinct_records[str(record.get("order_id"))] = str(record["user_id"])
        window_users = {u for u in distinct_records.values() if u} or users
    return {
        "窗口编号": config.get("窗口编号"),
        "窗口类型": config.get("窗口类型"),
        "窗口长度秒": window_length(config),
        "滑动步长秒": window_step(config),
        "迟到阈值秒": _to_int(config.get("迟到阈值秒"), 0),
        "桶数": len(summaries),
        "记录数": count,
        "订单数": len(distinct_records),
        "gmv": gmv,
        "金额": gmv,
        "独立用户": len(window_users) if distinct_records else 0,
        "用户ID": sorted(window_users) if distinct_records else [],
        "平均订单": round(gmv / count, 2) if count else 0.0,
        "渠道分布": channel,
        "分桶": summaries,
        "批次引用": [
            {"bucket_id": s["bucket_id"], "batch_id": s["batch_id"], "版本": s["version"]}
            for s in summaries
        ],
        "边界来源": "窗口配置",
        "窗口开始": min((s["窗口开始"] for s in summaries), default=None),
        "窗口结束": max((s["窗口结束"] for s in summaries), default=None),
    }


def build_aggregation(window_id: str | None = None,
                      configs: Iterable[dict[str, Any]] | None = None) -> dict[str, Any]:
    """从当前批次状态生成分窗指标聚合结果（指标计算的唯一输入）。"""
    configs_list = get_window_configs(window_id, configs)
    windows: dict[str, Any] = {}
    all_batches: list[dict[str, Any]] = []
    for config in configs_list:
        wid = str(config.get("窗口编号"))
        if not is_fixed_window(config):
            windows[wid] = _window_summary(config, [], [])
            windows[wid]["说明"] = REASON_SESSION
            continue
        buckets = [b for b in store.rows(BUCKETS) if b.get("窗口编号") == wid]
        summaries = [s for s in (_bucket_summary(b) for b in buckets) if s is not None]
        summaries.sort(key=lambda s: (str(s.get("窗口开始")), str(s.get("bucket_id"))))
        windows[wid] = _window_summary(config, buckets, summaries)
        all_batches.extend(
            b for b in store.rows(BATCHES) if b.get("window_id") == wid
            and b.get("batch_id") in {s["batch_id"] for s in summaries}
        )

    flat = [s for summary in windows.values() for s in summary.get("分桶", [])]
    return {
        "窗口配置": [dict(c) for c in configs_list],
        "windows": windows,
        "窗口结果": windows,
        "分桶": flat,
        "buckets": flat,
        "批次": [dict(b) for b in all_batches],
        "聚合批次引用": [
            {"window_id": b.get("window_id"), "bucket_id": b.get("bucket_id"),
             "batch_id": b.get("batch_id"), "版本": b.get("version")}
            for b in all_batches
        ],
        "watermarks": {
            row.get("window_id"): dict(row) for row in store.rows(WATERMARKS)
        },
        "generated_at": _iso(_now()),
    }


# ---------------------------------------------------------------------------
# 执行入口
# ---------------------------------------------------------------------------
def _runtime_empty() -> bool:
    return not store.rows(EXECUTIONS)


def run_windows(records: Iterable[dict[str, Any]] | None = None, *,
                window_id: str | None = None,
                late_policy: str | None = None,
                confirm_pending: bool = False,
                configs: Iterable[dict[str, Any]] | None = None,
                reset: bool = False) -> dict[str, Any]:
    """分窗执行入口：归桶、处理重复投递与迟到记录、封账，并返回实际引用的批次。

    ``records=None`` 且从没执行过时，使用 ``TRANSFORM_RECORDS`` 作为首批订单；
    之后 ``records=None`` 表示只做封账/对账，不重复灌入历史数据。
    """
    if reset:
        reset_runtime()
    if late_policy is not None and late_policy not in LATE_POLICIES:
        raise WindowRuntimeError(
            f"迟到策略「{late_policy}」不受支持，可选：{'、'.join(sorted(LATE_POLICIES))}")

    policy = late_policy or settings.window_late_policy
    configs_list = get_window_configs(window_id, configs)
    if records is None:
        source_records = [dict(r) for r in fixtures.TRANSFORM_RECORDS] if _runtime_empty() else []
    else:
        source_records = [dict(r) for r in records]

    execution: dict[str, Any] = {
        "execution_id": _next_id("WEXEC", EXECUTIONS, "execution_id"),
        "window_id": window_id,
        "late_policy": policy,
        "confirm_pending": bool(confirm_pending),
        "input_records": len(source_records),
        "records": [dict(r) for r in source_records],
        "迟到": [],
        "窗外": [],
        "重复投递": [],
        "重复投递计数": 0,
        "更正次数": 0,
        "封账批次": [],
        "说明": {},
        "created_at": _iso(_now()),
    }

    for config in configs_list:
        _ingest_config(
            config, source_records,
            policy=policy, confirm_pending=confirm_pending, execution=execution,
        )

    aggregation = build_aggregation(window_id, configs_list)
    decision = _late_decision(execution, policy, confirm_pending)
    result = dict(aggregation)
    result.update({
        "execution_id": execution["execution_id"],
        "窗口编号": window_id,
        "迟到策略": policy,
        "下游待确认": bool(confirm_pending),
        "迟到": execution["迟到"],
        "窗外": execution["窗外"],
        "窗外记录": execution["窗外"],
        "重复投递": execution["重复投递"],
        "重复投递计数": execution["重复投递计数"],
        "更正次数": execution["更正次数"],
        "封账批次": execution["封账批次"],
        "决策": decision,
        "说明": execution["说明"],
        "input_records": execution["input_records"],
        "input_orders": [dict(r) for r in source_records],
        "本次输入": {
            "记录数": execution["input_records"],
            "订单编号": [_record_key(r) for r in source_records],
        },
    })
    execution.update({
        "聚合批次引用": result.get("聚合批次引用", []),
        "windows": {
            wid: {
                "桶数": summary.get("桶数"),
                "记录数": summary.get("记录数"),
                "gmv": summary.get("gmv"),
                "独立用户": summary.get("独立用户"),
            }
            for wid, summary in result.get("windows", {}).items()
        },
        "决策": decision,
        "result_summary": {
            "聚合批次引用": result.get("聚合批次引用", []),
            "迟到": execution["迟到"],
            "窗外": execution["窗外"],
            "更正次数": execution["更正次数"],
            "重复投递计数": execution["重复投递计数"],
        },
    })
    store.rows(EXECUTIONS).append(execution)
    return result


def _late_decision(execution: dict[str, Any], policy: str, confirm_pending: bool) -> dict[str, Any]:
    late_count = len(execution.get("迟到") or [])
    if not late_count:
        return {
            "policy": policy,
            "confirm_pending": bool(confirm_pending),
            "action": "无需权衡",
            "说明": "本次没有封账后抵达的迟到订单。",
            "出数速度": "正常",
            "更正次数": execution.get("更正次数", 0),
        }
    if policy == "立即更正":
        return {
            "policy": policy,
            "confirm_pending": bool(confirm_pending),
            "action": "立即更正",
            "说明": "迟到订单立刻生成新批次并参与下一次指标计算，出数更快，但下游可能需要重新确认。",
            "出数速度": "快",
            "更正次数": execution.get("更正次数", 0),
            "待合并记录": 0,
        }
    return {
        "policy": policy,
        "confirm_pending": bool(confirm_pending),
        "action": "延迟合并",
        "说明": "迟到订单先挂起，等下游确认当前批次后再合并，更正次数更少，但出数更慢。",
        "出数速度": "慢",
        "更正次数": execution.get("更正次数", 0),
        "待合并记录": len(execution.get("迟到") or []),
    }


def reset_runtime() -> None:
    for name in INTERNAL_TABLES:
        store.rows(name).clear()


# ---------------------------------------------------------------------------
# 查询：桶 / 批次 / 订单 / 窗外
# ---------------------------------------------------------------------------
def _in_period(value: Any, start: str | None, end: str | None) -> bool:
    dt = parse_event_time(value)
    if dt is None:
        return False
    if start:
        left = parse_event_time(start)
        if left is not None and dt < left:
            return False
    if end:
        right = parse_event_time(end)
        if right is not None and dt > right:
            return False
    return True


def list_buckets(*, window_id: str | None = None, bucket_id: str | None = None,
                 start: str | None = None, end: str | None = None,
                 sealed: bool | None = None, order_id: str | None = None,
                 page: int = 1, size: int = 100) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    for bucket in store.rows(BUCKETS):
        if window_id and bucket.get("窗口编号") != window_id:
            continue
        if bucket_id and bucket.get("bucket_id") != bucket_id:
            continue
        if sealed is not None and bool(bucket.get("封账")) != bool(sealed):
            continue
        if start or end:
            # 时段筛选：桶时段与查询时段 [start, end) 有交集才命中。
            b_start = parse_event_time(bucket.get("窗口开始"))
            b_end = parse_event_time(bucket.get("窗口结束"))
            q_start = parse_event_time(start) if start else None
            q_end = parse_event_time(end) if end else None
            if b_start is None or b_end is None:
                continue
            if q_start is not None and b_end <= q_start:
                continue
            if q_end is not None and b_start >= q_end:
                continue
        if order_id:
            ids = _bucket_record_ids(bucket["bucket_id"])
            if order_id not in ids:
                continue
        summary = _bucket_summary(bucket)
        if summary is not None:
            summary["记录数"] = summary.get("记录数", 0)
            rows.append(summary)
    rows.sort(key=lambda r: (str(r.get("窗口编号")), str(r.get("窗口开始"))))
    total = len(rows)
    start_index = max(0, (page - 1) * size)
    return rows[start_index:start_index + size], total


def get_bucket(bucket_id: str) -> dict[str, Any] | None:
    bucket = _row_by(BUCKETS, "bucket_id", bucket_id)
    if bucket is None:
        return None
    summary = _bucket_summary(bucket) or dict(bucket)
    summary["批次"] = [
        dict(b) for b in store.rows(BATCHES) if b.get("bucket_id") == bucket_id
    ]
    summary["记录"] = [
        dict(r) for r in _bucket_records(bucket_id)
    ]
    summary["窗外"] = [
        dict(r) for r in store.rows(OUTSIDE)
        if r.get("bucket_id") == bucket_id
    ]
    return summary


def list_batches(*, window_id: str | None = None, bucket_id: str | None = None,
                 page: int = 1, size: int = 200) -> tuple[list[dict[str, Any]], int]:
    rows = []
    for batch in store.rows(BATCHES):
        if window_id and batch.get("window_id") != window_id:
            continue
        if bucket_id and batch.get("bucket_id") != bucket_id:
            continue
        rows.append(dict(batch))
    rows.sort(key=lambda r: (str(r.get("bucket_id")), int(r.get("version") or 0)))
    total = len(rows)
    start_index = max(0, (page - 1) * size)
    return rows[start_index:start_index + size], total


def list_bucket_metrics(*, window_id: str | None = None, bucket_id: str | None = None,
                        latest_only: bool = True,
                        page: int = 1, size: int = 200) -> tuple[list[dict[str, Any]], int]:
    rows = []
    latest_ids = {
        b.get("current_batch_id") for b in store.rows(BUCKETS)
    }
    for row in store.rows(BUCKET_METRICS):
        if latest_only and row.get("batch_id") not in latest_ids:
            continue
        if window_id and row.get("window_id") != window_id:
            continue
        if bucket_id and row.get("bucket_id") != bucket_id:
            continue
        rows.append(dict(row))
    rows.sort(key=lambda r: (str(r.get("window_id")), str(r.get("窗口开始"))))
    total = len(rows)
    start_index = max(0, (page - 1) * size)
    return rows[start_index:start_index + size], total


def list_records(*, window_id: str | None = None, bucket_id: str | None = None,
                 order_id: str | None = None, start: str | None = None, end: str | None = None,
                 late: bool | None = None,
                 page: int = 1, size: int = 200) -> tuple[list[dict[str, Any]], int]:
    rows = []
    for row in store.rows(RECORDS):
        if window_id and row.get("window_id") != window_id:
            continue
        if bucket_id and row.get("bucket_id") != bucket_id:
            continue
        if order_id and str(row.get("order_id")) != str(order_id):
            continue
        if late is not None and bool(row.get("late")) != bool(late):
            continue
        if (start or end) and not _in_period(row.get("event_time"), start, end):
            continue
        rows.append({
            "record_id": row.get("record_id"),
            "订单编号": row.get("订单编号"),
            "金额": row.get("金额"),
            "用户ID": row.get("user_id"),
            "event_time": row.get("event_time"),
            "window_id": row.get("window_id"),
            "窗口编号": row.get("window_id"),
            "bucket_id": row.get("bucket_id"),
            "批次号": row.get("batch_id"),
            "batch_id": row.get("batch_id"),
            "首次纳入批次": row.get("first_batch_id"),
            "迟到": bool(row.get("late")),
            "渠道": row.get("channel"),
            "地区": row.get("region"),
        })
    rows.sort(key=lambda r: (str(r.get("event_time")), str(r.get("bucket_id")), str(r.get("订单编号"))))
    total = len(rows)
    start_index = max(0, (page - 1) * size)
    return rows[start_index:start_index + size], total


def list_outside(*, window_id: str | None = None, start: str | None = None,
                 end: str | None = None, order_id: str | None = None,
                 page: int = 1, size: int = 200) -> tuple[list[dict[str, Any]], int]:
    rows = []
    for row in store.rows(OUTSIDE):
        if window_id and row.get("window_id") != window_id:
            continue
        if order_id and str(row.get("record_id")) != str(order_id):
            continue
        if (start or end) and not _in_period(row.get("event_time"), start, end):
            continue
        rows.append(dict(row))
    rows.sort(key=lambda r: str(r.get("event_time")))
    total = len(rows)
    start_index = max(0, (page - 1) * size)
    return rows[start_index:start_index + size], total


def list_executions(*, window_id: str | None = None,
                    page: int = 1, size: int = 50) -> tuple[list[dict[str, Any]], int]:
    rows = [dict(r) for r in store.rows(EXECUTIONS)
            if not window_id or r.get("window_id") == window_id]
    rows.reverse()
    total = len(rows)
    start_index = max(0, (page - 1) * size)
    return rows[start_index:start_index + size], total


def get_execution(execution_id: str) -> dict[str, Any] | None:
    row = _row_by(EXECUTIONS, "execution_id", execution_id)
    return dict(row) if row else None


def latest_execution(window_id: str | None = None) -> dict[str, Any] | None:
    for row in reversed(store.rows(EXECUTIONS)):
        if window_id and row.get("window_id") != window_id:
            continue
        return dict(row)
    return None


# ---------------------------------------------------------------------------
# 下游确认与迟到合并
# ---------------------------------------------------------------------------
def _confirmation(bucket_id: str, batch: dict[str, Any], *, source: str,
                  emission_id: str | None = None) -> dict[str, Any]:
    latest = _row_by(BATCHES, "batch_id",
                     (_row_by(BUCKETS, "bucket_id", bucket_id) or {}).get("current_batch_id"))
    row = {
        "confirmation_id": _next_id("CONF", CONFIRMATIONS, "confirmation_id"),
        "bucket_id": bucket_id,
        "batch_id": batch.get("batch_id"),
        "版本": batch.get("version"),
        "记录范围": _batch_record_ids(batch),
        "记录数": len(_batch_record_ids(batch)),
        "gmv": batch.get("gmv"),
        "独立用户": batch.get("独立用户"),
        "来源": source,
        "emission_id": emission_id,
        "已确认": True,
        "后续更正批次": latest.get("batch_id") if latest and latest.get("batch_id") != batch.get("batch_id") else None,
        "下游后续更正": bool(latest and latest.get("batch_id") != batch.get("batch_id")),
        "confirmed_at": _iso(_now()),
    }
    store.rows(CONFIRMATIONS).append(row)
    return row


def _merge_pending(bucket_id: str) -> list[dict[str, Any]]:
    pending = [r for r in store.rows(PENDING_LATE)
               if r.get("bucket_id") == bucket_id and r.get("status") == "待下游确认后合并"]
    if not pending:
        return []
    bucket = _row_by(BUCKETS, "bucket_id", bucket_id)
    if bucket is None:
        return []
    previous = bucket.get("current_batch_id")
    new_ids: list[str] = []
    for item in sorted(pending, key=lambda r: str(r.get("event_time"))):
        record = dict(item.get("record") or {})
        order_id = str(item.get("order_id"))
        if any(r.get("bucket_id") == bucket_id and str(r.get("order_id")) == order_id
               for r in store.rows(RECORDS)):
            item["status"] = "重复投递，已忽略"
            continue
        _store_record(bucket, record, parse_event_time(item.get("event_time")) or _now(),
                      late=True, batch_id=previous)
        new_ids.append(order_id)
        item["status"] = "已合并"
    if not new_ids:
        return []
    batch = _create_batch(bucket, trigger="确认后合并", new_ids=new_ids,
                          policy="延迟合并", sealed=True, previous_batch_id=previous,
                          watermark=parse_event_time(bucket.get("封账水位")))
    return [batch]


def confirm_bucket(bucket_id: str, *, batch_id: str | None = None,
                   emission_id: str | None = None) -> dict[str, Any] | None:
    bucket = _row_by(BUCKETS, "bucket_id", bucket_id)
    if bucket is None:
        return None
    emission_snapshot = None
    if emission_id:
        from app.services.metric_runtime import get_emission  # 避免循环导入
        emission = get_emission(emission_id)
        if emission is None:
            raise WindowRuntimeError(f"指标发射记录 {emission_id} 不存在")
        for snap in emission.get("bucket_snapshots", []):
            if snap.get("bucket_id") == bucket_id:
                emission_snapshot = snap
                break
        if emission_snapshot is None:
            raise WindowRuntimeError(f"指标发射记录 {emission_id} 没有引用桶 {bucket_id}")

    target_id = batch_id or (emission_snapshot or {}).get("batch_id") or bucket.get("current_batch_id")
    batch = _row_by(BATCHES, "batch_id", target_id)
    if batch is None or batch.get("bucket_id") != bucket_id:
        raise WindowRuntimeError(f"批次 {target_id} 不属于桶 {bucket_id}")

    confirmation = _confirmation(bucket_id, batch, source="指标来源" if emission_id else "窗口桶",
                                 emission_id=emission_id)
    if emission_snapshot:
        # 下游确认的范围必须与指标来源完全一致。
        confirmation["指标来源记录范围"] = list(emission_snapshot.get("records") or [])
        confirmation["记录范围一致"] = (
            list(confirmation["记录范围"]) == list(emission_snapshot.get("records") or [])
        )
    merged = _merge_pending(bucket_id)
    if merged:
        confirmation["合并批次"] = [b["batch_id"] for b in merged]
        confirmation["合并后批次"] = merged[-1]["batch_id"]
        for b in merged:
            _upsert_bucket_metric(b)
    return {
        "bucket_id": bucket_id,
        "confirmation": confirmation,
        "batch_id": batch.get("batch_id"),
        "记录范围": confirmation["记录范围"],
        "后续更正批次": confirmation.get("后续更正批次"),
        "合并批次": [b["batch_id"] for b in merged],
        "当前批次": (_row_by(BUCKETS, "bucket_id", bucket_id) or {}).get("current_batch_id"),
    }


def list_confirmations(*, bucket_id: str | None = None,
                       page: int = 1, size: int = 200) -> tuple[list[dict[str, Any]], int]:
    rows = [dict(r) for r in store.rows(CONFIRMATIONS)
            if not bucket_id or r.get("bucket_id") == bucket_id]
    rows.reverse()
    total = len(rows)
    start_index = max(0, (page - 1) * size)
    return rows[start_index:start_index + size], total

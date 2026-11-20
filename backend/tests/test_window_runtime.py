"""分窗运行时测试：归桶、重叠、去重、迟到更正、批次版本与下游确认。"""
import pytest

from app.services import metric_runtime
from app.services import window_runtime as runtime


@pytest.fixture(autouse=True)
def clean_runtime():
    runtime.reset_runtime()
    yield
    runtime.reset_runtime()


def _order(order_id, amount, user, event_time, channel="app", region="华东"):
    return {"订单编号": order_id, "金额": amount, "用户ID": user, "渠道": channel,
            "地区": region, "event_time": event_time}


CONFIG_TUMBLE = {
    "窗口编号": "WIN-T60", "窗口类型": "滚动窗口", "窗口长度秒": 60, "滑动步长秒": 60,
    "迟到阈值秒": 5, "水位类型": "event_time", "水位延迟秒": 3,
}
CONFIG_SLIDE = {
    "窗口编号": "WIN-S60-30", "窗口类型": "滑动窗口", "窗口长度秒": 60, "滑动步长秒": 30,
    "迟到阈值秒": 10, "水位类型": "event_time", "水位延迟秒": 5,
}
# 迟到阈值放大到 600 秒：桶仍会先按事件时间封账，但迟到订单还在容忍范围内，
# 用来验证“封账后更正”而不是“直接丢到窗外”。
CONFIG_LATE = dict(CONFIG_TUMBLE, 窗口编号="WIN-T60-LATE", 迟到阈值秒=600)


class TestBucketing:
    def test_tumbling_uses_event_time(self):
        result = runtime.run_windows([
            _order("A", 10, "U1", "2026-11-16T08:00:10Z"),
            _order("B", 20, "U2", "2026-11-16T08:00:50Z"),
            _order("C", 30, "U1", "2026-11-16T08:01:10Z"),
        ], configs=[CONFIG_TUMBLE])
        summary = result["windows"]["WIN-T60"]
        buckets = summary["分桶"]
        assert [b["窗口开始"] for b in buckets] == [
            "2026-11-16T08:00:00Z", "2026-11-16T08:01:00Z",
        ]
        assert [b["记录范围"] for b in buckets] == [["A", "B"], ["C"]]
        assert [b["gmv"] for b in buckets] == [30.0, 30.0]
        # 窗口级金额是分桶金额之和，不再是整批金额。
        assert summary["gmv"] == 60.0
        assert summary["记录数"] == 3

    def test_window_boundaries_come_from_config(self):
        # 配置改成 30 秒滚动，同样的订单必须落到不同边界。
        config = dict(CONFIG_TUMBLE, 窗口编号="WIN-T30", 窗口长度秒=30, 滑动步长秒=30)
        result = runtime.run_windows([
            _order("A", 10, "U1", "2026-11-16T08:00:10Z"),
            _order("B", 20, "U2", "2026-11-16T08:00:40Z"),
        ], configs=[config])
        starts = [b["窗口开始"] for b in result["windows"]["WIN-T30"]["分桶"]]
        assert starts == ["2026-11-16T08:00:00Z", "2026-11-16T08:00:30Z"]

    def test_overlapping_buckets_explain_same_order(self):
        result = runtime.run_windows([
            _order("A", 10, "U1", "2026-11-16T08:00:50Z"),
        ], configs=[CONFIG_SLIDE])
        buckets = result["windows"]["WIN-S60-30"]["分桶"]
        assert len(buckets) == 2
        # 同一条订单进入两个不同桶，各自都能独立解释。
        for bucket in buckets:
            assert bucket["记录范围"] == ["A"]
            assert bucket["gmv"] == 10.0
        assert [b["窗口开始"] for b in buckets] == [
            "2026-11-16T08:00:00Z", "2026-11-16T08:00:30Z",
        ]
        # 窗口级订单数按订单去重，金额不重复累计。
        assert result["windows"]["WIN-S60-30"]["记录数"] == 2
        assert result["windows"]["WIN-S60-30"]["订单数"] == 1
        assert result["windows"]["WIN-S60-30"]["独立用户"] == 1

    def test_adjacent_minutes_differ_between_tumbling_buckets(self):
        result = runtime.run_windows([
            _order("A", 100, "U1", "2026-11-16T08:00:10Z"),
            _order("B", 200, "U2", "2026-11-16T08:01:10Z"),
        ], configs=[CONFIG_TUMBLE])
        buckets = result["windows"]["WIN-T60"]["分桶"]
        assert [b["gmv"] for b in buckets] == [100.0, 200.0]
        assert buckets[0]["gmv"] != result["windows"]["WIN-T60"]["gmv"]

    def test_session_window_not_bucketed_by_event_time(self):
        result = runtime.run_windows([
            _order("A", 10, "U1", "2026-11-16T08:00:10Z"),
        ], configs=[{"窗口编号": "WIN-SESSION", "窗口类型": "会话窗口",
                     "窗口长度秒": 0, "滑动步长秒": 0, "迟到阈值秒": 30}])
        assert result["windows"]["WIN-SESSION"]["桶数"] == 0
        assert "会话窗口" in result["说明"]["WIN-SESSION"]


class TestDeduplication:
    def test_same_batch_duplicate_counted_once(self):
        result = runtime.run_windows([
            _order("A", 10, "U1", "2026-11-16T08:00:10Z"),
            _order("A", 10, "U1", "2026-11-16T08:00:10Z"),
        ], configs=[CONFIG_TUMBLE])
        bucket = result["windows"]["WIN-T60"]["分桶"][0]
        assert bucket["记录数"] == 1
        assert bucket["gmv"] == 10.0
        assert result["重复投递计数"] == 1
        assert result["重复投递"][0]["status"] == "重复投递"

    def test_late_duplicate_does_not_double_count(self):
        runtime.run_windows([
            _order("A", 10, "U1", "2026-11-16T08:00:10Z"),
            _order("B", 20, "U2", "2026-11-16T08:05:10Z"),
        ], configs=[CONFIG_TUMBLE])
        # A 所在桶已封账，再次补投同一订单应被识别为重复而不是更正。
        result = runtime.run_windows([
            _order("A", 10, "U1", "2026-11-16T08:00:10Z"),
        ], configs=[CONFIG_TUMBLE])
        assert result["重复投递计数"] == 1
        assert result["迟到"] == []
        bucket = result["windows"]["WIN-T60"]["分桶"][0]
        assert bucket["记录数"] == 1
        assert bucket["gmv"] == 10.0


class TestLateAndBatches:
    def test_late_order_creates_new_batch_with_original_bucket(self):
        runtime.run_windows([
            _order("A", 10, "U1", "2026-11-16T08:00:10Z"),
            _order("B", 20, "U2", "2026-11-16T08:05:10Z"),
        ], configs=[CONFIG_LATE])
        first = runtime.list_batches(bucket_id="WIN-T60-LATE@2026-11-16T08:00:00Z")[0]
        assert len(first) == 1 and first[0]["状态"] == "已封账"

        result = runtime.run_windows([
            _order("C", 30, "U3", "2026-11-16T08:00:20Z"),
        ], configs=[CONFIG_LATE], late_policy="立即更正")

        assert len(result["迟到"]) == 1
        correction = result["迟到"][0]
        assert correction["原桶引用"] == "WIN-T60-LATE@2026-11-16T08:00:00Z"
        assert correction["本次纳入"] == ["C"]
        assert correction["previous_batch_id"] == first[0]["batch_id"]
        assert correction["版本"] == 2

        batches, _ = runtime.list_batches(bucket_id="WIN-T60-LATE@2026-11-16T08:00:00Z")
        assert [b["version"] for b in batches] == [1, 2]
        assert batches[0]["状态"] == "已被更正"
        assert batches[0]["superseded_by"] == batches[1]["batch_id"]
        assert batches[1]["记录范围"] == ["A", "C"]
        assert batches[1]["previous_batch_id"] == batches[0]["batch_id"]
        assert batches[1]["原桶引用"] == "WIN-T60-LATE@2026-11-16T08:00:00Z"

        bucket = runtime.get_bucket("WIN-T60-LATE@2026-11-16T08:00:00Z")
        assert bucket["batch_id"] == batches[1]["batch_id"]
        assert bucket["记录范围"] == ["A", "C"]

    def test_metric_uses_one_batch_version_per_bucket(self):
        runtime.run_windows([
            _order("A", 10, "U1", "2026-11-16T08:00:10Z"),
            _order("B", 20, "U2", "2026-11-16T08:05:10Z"),
        ], configs=[CONFIG_LATE])
        before = metric_runtime.run_metric_calculation(window_id="WIN-T60-LATE")
        first_refs = {b["bucket_id"]: b["batch_id"] for b in before["聚合批次引用"]}

        runtime.run_windows([
            _order("C", 30, "U3", "2026-11-16T08:00:20Z"),
        ], configs=[CONFIG_LATE], late_policy="立即更正")
        after = metric_runtime.run_metric_calculation(window_id="WIN-T60-LATE")
        after_refs = {b["bucket_id"]: b["batch_id"] for b in after["聚合批次引用"]}

        target = "WIN-T60-LATE@2026-11-16T08:00:00Z"
        # 更正后同一桶只引用新版本，不能同一指标混用两个桶版本。
        assert first_refs[target] != after_refs[target]
        assert all(b["batch_id"] != first_refs[target]
                   for b in after["聚合批次引用"] if b["bucket_id"] == target)
        assert after["gmv"] == 60.0
        assert after["metrics"]["METR-ORDER-COUNT"] == 3

    def test_emission_can_be_recomputed_with_original_batches(self):
        runtime.run_windows([
            _order("A", 10, "U1", "2026-11-16T08:00:10Z"),
            _order("B", 20, "U2", "2026-11-16T08:05:10Z"),
        ], configs=[CONFIG_LATE])
        emission = metric_runtime.run_metric_calculation(window_id="WIN-T60-LATE")
        runtime.run_windows([
            _order("C", 30, "U3", "2026-11-16T08:00:20Z"),
        ], configs=[CONFIG_LATE], late_policy="立即更正")

        recompute = metric_runtime.recompute_emission(emission["emission_id"])
        assert recompute["一致"] is True
        assert recompute["复算"] == {"METR-ORDER-COUNT": 2, "METR-GMV": 30.0,
                                     "METR-AVG-ORDER": 15.0, "METR-USER-DISTINCT": 2}
        # 复算仍引用发射当时的桶版本。
        target = "WIN-T60-LATE@2026-11-16T08:00:00Z"
        assert any(ref["bucket_id"] == target and ref["版本"] == 1
                   for ref in recompute["批次引用"])

    def test_too_late_order_goes_outside_with_reason(self):
        runtime.run_windows([
            _order("A", 10, "U1", "2026-11-16T08:00:10Z"),
            _order("B", 20, "U2", "2026-11-16T08:20:10Z"),
        ], configs=[CONFIG_LATE])
        result = runtime.run_windows([
            _order("C", 30, "U3", "2026-11-16T08:00:20Z"),
        ], configs=[CONFIG_LATE])
        assert result["迟到"] == []
        outside = result["窗外"]
        assert len(outside) == 1
        assert outside[0]["订单编号"] == "C"
        assert outside[0]["reason"] == runtime.REASON_TOO_LATE
        assert outside[0]["bucket_id"] == "WIN-T60-LATE@2026-11-16T08:00:00Z"


class TestDownstreamConfirmation:
    def test_confirmation_range_matches_emission(self):
        runtime.run_windows([
            _order("A", 10, "U1", "2026-11-16T08:00:10Z"),
            _order("B", 20, "U2", "2026-11-16T08:05:10Z"),
        ], configs=[CONFIG_LATE])
        emission = metric_runtime.run_metric_calculation(window_id="WIN-T60-LATE")
        target = "WIN-T60-LATE@2026-11-16T08:00:00Z"

        confirmed = runtime.confirm_bucket(target, emission_id=emission["emission_id"])
        assert confirmed["记录范围"] == ["A"]
        assert confirmed["confirmation"]["记录范围一致"] is True
        assert confirmed["confirmation"]["来源"] == "指标来源"
        # 迟到订单刚到时如果先确认，确认的是当时看到的批次，后续更正可追溯。
        assert confirmed["confirmation"]["下游后续更正"] is False

    def test_confirmation_tracks_later_correction(self):
        runtime.run_windows([
            _order("A", 10, "U1", "2026-11-16T08:00:10Z"),
            _order("B", 20, "U2", "2026-11-16T08:05:10Z"),
        ], configs=[CONFIG_LATE])
        emission = metric_runtime.run_metric_calculation(window_id="WIN-T60-LATE")
        target = "WIN-T60-LATE@2026-11-16T08:00:00Z"
        runtime.run_windows([
            _order("C", 30, "U3", "2026-11-16T08:00:20Z"),
        ], configs=[CONFIG_LATE], late_policy="立即更正")

        confirmed = runtime.confirm_bucket(target, emission_id=emission["emission_id"])
        assert confirmed["confirmation"]["下游后续更正"] is True
        assert confirmed["confirmation"]["后续更正批次"] == confirmed["当前批次"]
        assert confirmed["记录范围"] == ["A"]

    def test_delayed_merge_waits_for_confirmation(self):
        runtime.run_windows([
            _order("A", 10, "U1", "2026-11-16T08:00:10Z"),
            _order("B", 20, "U2", "2026-11-16T08:05:10Z"),
        ], configs=[CONFIG_LATE])
        result = runtime.run_windows([
            _order("C", 30, "U3", "2026-11-16T08:00:20Z"),
        ], configs=[CONFIG_LATE], late_policy="延迟合并")

        # 延迟合并：更正次数更少，但出数更慢（本批指标不含迟到订单）。
        assert result["决策"]["action"] == "延迟合并"
        assert result["决策"]["更正次数"] == 0
        assert result["windows"]["WIN-T60-LATE"]["gmv"] == 30.0
        pending = runtime.store.rows(runtime.PENDING_LATE)
        assert len(pending) == 1 and pending[0]["status"] == "待下游确认后合并"

        confirmed = runtime.confirm_bucket("WIN-T60-LATE@2026-11-16T08:00:00Z")
        assert confirmed["合并批次"]
        bucket = runtime.get_bucket("WIN-T60-LATE@2026-11-16T08:00:00Z")
        assert bucket["记录范围"] == ["A", "C"]
        assert runtime.store.rows(runtime.PENDING_LATE)[0]["status"] == "已合并"


class TestConfigBoundaries:
    def test_config_is_read_each_run(self):
        config = dict(CONFIG_TUMBLE, 窗口编号="WIN-CFG", 窗口长度秒=120, 滑动步长秒=120)
        runtime.run_windows([_order("A", 10, "U1", "2026-11-16T08:00:10Z")], configs=[config])
        stored = runtime.get_window_configs("WIN-CFG")[0]
        assert stored["窗口长度秒"] == 120 and stored["滑动步长秒"] == 120

        # 同一窗口编号下次只给 window_id，仍应沿用登记过的边界。
        result = runtime.run_windows([
            _order("B", 20, "U2", "2026-11-16T08:01:10Z"),
        ], window_id="WIN-CFG")
        starts = [b["窗口开始"] for b in result["windows"]["WIN-CFG"]["分桶"]]
        assert starts == ["2026-11-16T08:00:00Z"]

    def test_invalid_late_policy_rejected(self):
        with pytest.raises(runtime.WindowRuntimeError):
            runtime.run_windows([], configs=[CONFIG_TUMBLE], late_policy="随便")


class TestQueries:
    def test_filter_buckets_by_window_and_period(self):
        runtime.run_windows([
            _order("A", 10, "U1", "2026-11-16T08:00:10Z"),
            _order("B", 20, "U2", "2026-11-16T08:01:10Z"),
            _order("C", 30, "U3", "2026-11-16T08:02:10Z"),
        ], configs=[CONFIG_TUMBLE])
        rows, total = runtime.list_buckets(
            window_id="WIN-T60", start="2026-11-16T08:01:00Z", end="2026-11-16T08:02:30Z")
        assert total == 2
        assert [r["窗口开始"] for r in rows] == [
            "2026-11-16T08:01:00Z", "2026-11-16T08:02:00Z",
        ]

    def test_locate_order_in_buckets(self):
        runtime.run_windows([
            _order("A", 10, "U1", "2026-11-16T08:00:50Z"),
        ], configs=[CONFIG_SLIDE])
        rows, total = runtime.list_buckets(order_id="A")
        assert total == 2
        assert {r["bucket_id"] for r in rows} == {
            "WIN-S60-30@2026-11-16T08:00:00Z", "WIN-S60-30@2026-11-16T08:00:30Z",
        }
        records, record_total = runtime.list_records(order_id="A")
        assert record_total == 2
        assert all(r["订单编号"] == "A" for r in records)

    def test_outside_records_searchable_by_period(self):
        runtime.run_windows([
            _order("A", 10, "U1", "2026-11-16T08:00:10Z"),
            _order("B", 20, "U2", "2026-11-16T08:20:10Z"),
        ], configs=[CONFIG_TUMBLE])
        runtime.run_windows([
            _order("C", 30, "U3", "2026-11-16T08:00:20Z"),
        ], configs=[CONFIG_TUMBLE])
        rows, total = runtime.list_outside(
            window_id="WIN-T60", start="2026-11-16T08:00:00Z", end="2026-11-16T08:01:00Z")
        assert total == 1
        assert rows[0]["订单编号"] == "C"
        assert rows[0]["原因"] == runtime.REASON_TOO_LATE
        empty, empty_total = runtime.list_outside(
            window_id="WIN-T60", start="2026-11-16T09:00:00Z", end="2026-11-16T10:00:00Z")
        assert empty_total == 0 and empty == []

    def test_bucket_metric_list_reflects_latest_batch(self):
        runtime.run_windows([
            _order("A", 10, "U1", "2026-11-16T08:00:10Z"),
            _order("B", 20, "U2", "2026-11-16T08:05:10Z"),
        ], configs=[CONFIG_LATE])
        runtime.run_windows([
            _order("C", 30, "U1", "2026-11-16T08:00:20Z"),
        ], configs=[CONFIG_LATE], late_policy="立即更正")

        rows, _ = runtime.list_bucket_metrics(window_id="WIN-T60-LATE")
        target = next(r for r in rows if r["bucket_id"] == "WIN-T60-LATE@2026-11-16T08:00:00Z")
        assert target["金额"] == 40.0
        assert target["记录数"] == 2
        assert target["独立用户"] == 1
        assert target["版本"] == 2

        history, _ = runtime.list_bucket_metrics(
            window_id="WIN-T60-LATE", bucket_id="WIN-T60-LATE@2026-11-16T08:00:00Z", latest_only=False)
        assert [r["版本"] for r in history] == [1, 2]
        assert history[0]["金额"] == 10.0


class TestMetricRuntime:
    def test_metric_detail_reports_referenced_batches(self):
        runtime.run_windows([
            _order("A", 10, "U1", "2026-11-16T08:00:10Z"),
            _order("B", 20, "U2", "2026-11-16T08:01:10Z"),
        ], configs=[CONFIG_TUMBLE])
        emission = metric_runtime.run_metric_calculation(window_id="WIN-T60")
        detail = metric_runtime.metric_result_for("METR-GMV", "WIN-T60")
        assert detail["值"] == 30.0
        assert detail["桶数"] == 2
        assert {b["bucket_id"] for b in detail["聚合批次引用"]} == {
            "WIN-T60@2026-11-16T08:00:00Z", "WIN-T60@2026-11-16T08:01:00Z",
        }
        assert detail["最近发射"]["emission_id"] == emission["emission_id"]

    def test_metrics_do_not_use_whole_batch(self):
        runtime.run_windows([
            _order("A", 10, "U1", "2026-11-16T08:00:10Z"),
            _order("B", 20, "U2", "2026-11-16T08:01:10Z"),
        ], configs=[CONFIG_TUMBLE])
        result = metric_runtime.run_metric_calculation(window_id="WIN-T60")
        assert result["metrics"]["METR-GMV"] == 30.0
        assert result["metrics"]["METR-ORDER-COUNT"] == 2
        # 与旧实现“整批金额 / 笔数”的差别必须是分桶金额之和。
        raw_total = sum(b["gmv"] for b in runtime.list_buckets(window_id="WIN-T60")[0])
        assert result["metrics"]["METR-GMV"] == round(raw_total, 2)

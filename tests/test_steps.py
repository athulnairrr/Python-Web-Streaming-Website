import time

from app.processing.steps import RollingAggregateStep, enrich_step, threshold_filter


def test_threshold_filter_drops_below_min():
    step = threshold_filter(min_value=10)
    assert step({"value": 5}) is None


def test_threshold_filter_keeps_at_or_above_min():
    step = threshold_filter(min_value=0)
    event = {"value": 42}
    assert step(event) == event


def test_rolling_aggregate_computes_mean_and_stdev():
    step = RollingAggregateStep(window_size=3)
    e1 = step({"category": "cpu", "value": 10})
    assert e1["rolling_mean"] == 10
    assert e1["rolling_stdev"] == 0
    assert e1["window_size"] == 1

    e2 = step({"category": "cpu", "value": 20})
    assert e2["rolling_mean"] == 15
    assert e2["window_size"] == 2


def test_rolling_aggregate_window_evicts_oldest():
    step = RollingAggregateStep(window_size=2)
    step({"category": "cpu", "value": 10})
    step({"category": "cpu", "value": 20})
    e3 = step({"category": "cpu", "value": 30})
    # window only holds the last 2 values: 20, 30
    assert e3["rolling_mean"] == 25
    assert e3["window_size"] == 2


def test_rolling_aggregate_tracks_categories_independently():
    step = RollingAggregateStep(window_size=5)
    step({"category": "cpu", "value": 100})
    mem_event = step({"category": "memory", "value": 0})
    assert mem_event["rolling_mean"] == 0


def test_enrich_step_adds_latency():
    past = time.time() - 0.05
    event = enrich_step({"ts_generated": past})
    assert "ts_processed" in event
    assert event["latency_ms"] >= 30  # roughly 50ms, allow scheduling slack


def test_enrich_step_without_ts_generated():
    event = enrich_step({"value": 1})
    assert "latency_ms" not in event
    assert "ts_processed" in event


def test_steps_do_not_mutate_input():
    original = {"category": "cpu", "value": 10}
    RollingAggregateStep()(original)
    assert original == {"category": "cpu", "value": 10}

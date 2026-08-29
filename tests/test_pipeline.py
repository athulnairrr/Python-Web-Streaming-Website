from app.processing.pipeline import Pipeline


def test_pipeline_chains_steps_in_order():
    pipeline = Pipeline(
        [
            lambda e: {**e, "a": 1},
            lambda e: {**e, "b": e["a"] + 1},
        ]
    )
    result = pipeline.process({})
    assert result == {"a": 1, "b": 2}


def test_pipeline_stops_on_drop():
    calls = []

    def tracking_step(event):
        calls.append(event)
        return event

    pipeline = Pipeline(
        [
            lambda e: None,  # drops immediately
            tracking_step,
        ]
    )
    result = pipeline.process({"value": 1})
    assert result is None
    assert calls == []  # later steps never run on a dropped event


def test_pipeline_with_no_steps_returns_input_unchanged():
    pipeline = Pipeline([])
    event = {"x": 1}
    assert pipeline.process(event) == event

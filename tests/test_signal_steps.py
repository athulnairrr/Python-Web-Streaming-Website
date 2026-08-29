from app.processing.signal_steps import (
    DIRECTIONS,
    RandomSignalStrategy,
    make_signal_step,
)


def test_random_signal_strategy_adds_direction_and_confidence():
    strategy = RandomSignalStrategy()
    event = {"symbol": "EURUSD-OTC", "price": 1.1, "timeframe": "M1"}
    result = strategy.generate(event)

    assert result["direction"] in DIRECTIONS
    assert 0 <= result["confidence"] <= 100
    # original fields preserved
    assert result["symbol"] == "EURUSD-OTC"
    assert result["price"] == 1.1


def test_random_signal_strategy_does_not_mutate_input():
    strategy = RandomSignalStrategy()
    original = {"symbol": "EURUSD-OTC", "price": 1.1}
    strategy.generate(original)
    assert original == {"symbol": "EURUSD-OTC", "price": 1.1}


def test_make_signal_step_wraps_strategy():
    strategy = RandomSignalStrategy()
    step = make_signal_step(strategy)
    event = {"symbol": "BTCUSD", "price": 42000.0}
    result = step(event)
    assert result["direction"] in DIRECTIONS

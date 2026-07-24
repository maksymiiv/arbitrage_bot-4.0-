"""Spread calculators: an opportunity exists only in the profitable direction."""

import time

from engine.spread_logic.calculators import calc_cex_to_dex, calc_dex_to_cex


def _cex(bid, ask):
    return {"bid": bid, "ask": ask, "mid": (bid + ask) / 2, "ts": time.time()}


def _dex(price):
    return {"price": price, "pool": "0xabc", "protocol": "v3", "ts": time.time()}


def test_dex_to_cex_valid_when_dex_below_bid():
    opp = calc_dex_to_cex("TKN", "bybit", _cex(110, 111), "eth", _dex(100))
    assert opp is not None
    assert opp.direction == "dex->cex"
    assert opp.spread_abs == 10
    assert opp.spread_pct == 10.0


def test_dex_to_cex_none_when_dex_not_below_bid():
    assert calc_dex_to_cex("TKN", "bybit", _cex(110, 111), "eth", _dex(110)) is None
    assert calc_dex_to_cex("TKN", "bybit", _cex(110, 111), "eth", _dex(200)) is None


def test_cex_to_dex_valid_when_dex_above_ask():
    opp = calc_cex_to_dex("TKN", "bybit", _cex(99, 100), "eth", _dex(110))
    assert opp is not None
    assert opp.direction == "cex->dex"
    assert opp.spread_abs == 10
    assert opp.spread_pct == 10.0


def test_cex_to_dex_none_when_dex_not_above_ask():
    assert calc_cex_to_dex("TKN", "bybit", _cex(99, 100), "eth", _dex(100)) is None
    assert calc_cex_to_dex("TKN", "bybit", _cex(99, 100), "eth", _dex(50)) is None


def test_rejects_crossed_or_nonpositive():
    assert calc_dex_to_cex("TKN", "bybit", _cex(111, 110), "eth", _dex(100)) is None  # bid>ask
    assert calc_dex_to_cex("TKN", "bybit", {"bid": 0, "ask": 1}, "eth", _dex(0.5)) is None
    assert calc_dex_to_cex("TKN", "bybit", _cex(110, 111), "eth", _dex(0)) is None

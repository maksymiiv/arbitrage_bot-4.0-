"""Bybit orderbook sequence handling + base-symbol extraction."""

import cex_part.core.orderbooks as ob
from cex_part.core.orderbooks import extract_base_symbol


def _fresh(key="BYBIT:TSTUSDT"):
    ob.init_book(key)
    return key


def test_snapshot_sets_book_and_u():
    k = _fresh()
    ob.apply_snapshot(k, [["10", "1"]], [["11", "1"]], u=100)
    b = ob.get_book(k)
    assert b["u"] == 100 and b["bids"][10.0] == 1.0 and b["ready"] is True


def test_stale_delta_ignored():
    k = _fresh()
    ob.apply_snapshot(k, [["10", "1"]], [["11", "1"]], u=100)
    ob.apply_delta(k, [["10", "9"]], [], u=99)  # u < last_u -> ignore
    assert ob.get_book(k)["bids"][10.0] == 1.0


def test_forward_delta_applies():
    k = _fresh()
    ob.apply_snapshot(k, [["10", "1"]], [["11", "1"]], u=100)
    ob.apply_delta(k, [["10", "3"]], [], u=101)
    b = ob.get_book(k)
    assert b["bids"][10.0] == 3.0 and b["u"] == 101


def test_u_equals_1_resets_book():
    k = _fresh()
    ob.apply_snapshot(k, [["10", "1"]], [["11", "1"]], u=100)
    ob.apply_delta(k, [["20", "5"]], [["21", "5"]], u=1)  # restart -> snapshot
    b = ob.get_book(k)
    assert b["u"] == 1 and 10.0 not in b["bids"] and b["bids"][20.0] == 5.0


def test_delta_zero_size_removes_level():
    k = _fresh()
    ob.apply_snapshot(k, [["10", "1"], ["9", "2"]], [["11", "1"]], u=100)
    ob.apply_delta(k, [["9", "0"]], [], u=101)  # size 0 -> remove
    assert 9.0 not in ob.get_book(k)["bids"]


def test_extract_base_symbol():
    assert extract_base_symbol("BTCUSDT") == "BTC"
    assert extract_base_symbol("ETH/USDT") == "ETH"
    assert extract_base_symbol("SOL/USD") == "SOL"
    assert extract_base_symbol("BTC/EUR") is None  # non-stable quote

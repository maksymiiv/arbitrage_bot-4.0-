"""is_route_open tri-state semantics (deposit/withdraw route filter)."""

import cex_part.cache_manager as cm


def _set(networks_cex):
    cm._CACHE = {"X": {"symbol": "X", "networks_cex": networks_cex}}


def test_route_true_when_flag_on():
    _set({"bybit": {"BSC": [True, True]}})
    assert cm.is_route_open("X", "bybit", "bsc", "deposit") is True
    assert cm.is_route_open("X", "bybit", "bsc", "withdraw") is True


def test_route_false_when_flag_off():
    _set({"bybit": {"BSC": [False, True]}})
    assert cm.is_route_open("X", "bybit", "bsc", "deposit") is False
    assert cm.is_route_open("X", "bybit", "bsc", "withdraw") is True


def test_route_false_when_chain_absent_but_cex_lists_others():
    # Bybit lists the token only on ETH -> a BSC route is impossible.
    _set({"bybit": {"ETH": [True, True]}})
    assert cm.is_route_open("X", "bybit", "bsc", "deposit") is False
    assert cm.is_route_open("X", "bybit", "bsc", "withdraw") is False


def test_route_none_when_cex_block_empty():
    # Kraken exposes no per-chain D/W -> unknown, caller must not filter.
    _set({"kraken": {}})
    assert cm.is_route_open("X", "kraken", "eth", "withdraw") is None


def test_route_none_when_no_data():
    _set({})  # entry has no networks_cex for this cex
    assert cm.is_route_open("X", "bybit", "eth", "deposit") is None
    cm._CACHE = {}
    assert cm.is_route_open("MISSING", "bybit", "eth", "deposit") is None


def test_resolve_ambiguous_cex_symbol_returns_none():
    # Two "AI" tokens both carry `gate` (stale binding) -> refuse to route
    # gate; but kraken lives on only one -> resolves unambiguously.
    cm._CACHE = {
        "AI": {"symbol": "AI", "networks_cex": {"gate": {"ETH": [True, True]}, "kraken": {}}},
        "AI#bsc": {"symbol": "AI", "networks_cex": {"gate": {"BSC": [True, True]}}},
    }
    cm._INDEX = {"symbol": {"AI": ["AI", "AI#bsc"]}, "token_id": {}, "contract": {}, "pool": {}}
    assert cm.resolve_key_for_cex_symbol("gate", "AI") is None      # ambiguous -> skip
    assert cm.resolve_key_for_cex_symbol("kraken", "AI") == "AI"    # unique -> route

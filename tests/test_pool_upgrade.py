"""Volume-aware pool selection (the DEXE V3-vs-V4 fix)."""

from engine.liquidity_filter import _better_pool_by_volume


def _pool(addr, vol, liq):
    return {"address": addr, "vol_24h": vol, "reserve_usd": liq}


def test_picks_active_over_fat_frozen():
    # DEXE case: a fat but idle current pool, a leaner but heavily-traded
    # candidate -> switch to the active one.
    candidates = [_pool("0xv4", vol=100_000, liq=20_000)]
    best = _better_pool_by_volume(candidates, current_pool="0xv3", current_vol=200, vol_ratio=1.5)
    assert best is not None and best["address"] == "0xv4"


def test_frozen_current_always_loses():
    # current volume 0 (frozen) -> any active candidate wins
    candidates = [_pool("0xnew", vol=1_000, liq=6_000)]
    assert _better_pool_by_volume(candidates, "0xold", current_vol=0.0, vol_ratio=1.5)["address"] == "0xnew"


def test_volume_first_not_liquidity_first():
    # a fatter-but-idle candidate must NOT beat a leaner-but-active one
    candidates = [
        _pool("0xfat", vol=100, liq=99_999),
        _pool("0xactive", vol=50_000, liq=6_000),
    ]
    best = _better_pool_by_volume(candidates, "0xold", current_vol=0.0, vol_ratio=1.5)
    assert best["address"] == "0xactive"


def test_no_flip_flop_between_comparable_pools():
    # candidate only marginally more active than current -> keep current
    candidates = [_pool("0xb", vol=110_000, liq=30_000)]
    assert _better_pool_by_volume(candidates, "0xa", current_vol=100_000, vol_ratio=1.5) is None


def test_best_is_current_returns_none():
    candidates = [_pool("0xcur", vol=100_000, liq=50_000)]
    assert _better_pool_by_volume(candidates, "0xcur", current_vol=0.0, vol_ratio=1.5) is None


def test_no_candidates():
    assert _better_pool_by_volume([], "0xcur", current_vol=0.0, vol_ratio=1.5) is None

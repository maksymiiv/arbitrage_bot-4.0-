"""PriceStore: CEX validation + remove_dex_by_pool (phantom-spread guard)."""

from engine.price_store import PriceStore


def test_update_cex_rejects_bad_quotes():
    ps = PriceStore()
    ps.update_cex("BTC", "bybit", bid=0, ask=1)        # non-positive bid
    ps.update_cex("BTC", "bybit", bid=110, ask=100)    # crossed (bid>ask)
    assert ps.get_cex_prices("BTC") == {}


def test_update_cex_stores_and_computes_mid():
    ps = PriceStore()
    ps.update_cex("BTC", "bybit", bid=100, ask=102)
    p = ps.get_cex_prices("BTC")["bybit"]
    assert p["bid"] == 100 and p["ask"] == 102 and p["mid"] == 101


def test_remove_dex_by_pool_only_matching():
    ps = PriceStore()
    ps._data["K"] = {
        "display_symbol": "TKN",
        "cex": {},
        "dex": {"eth": {"price": 1.0, "pool": "0xaaa", "protocol": "v3", "ts": 0}},
        "source": "dex",
    }
    # wrong pool -> no removal
    assert ps.remove_dex_by_pool("eth", "0xbbb") is False
    assert "eth" in ps._data["K"]["dex"]
    # matching pool (case-insensitive) -> removed
    assert ps.remove_dex_by_pool("eth", "0xAAA") is True
    assert "eth" not in ps._data["K"]["dex"]

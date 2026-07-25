"""Pure helpers: token compatibility, disambig keys, GT dex-id parsing, chain filter."""

from cex_part.cache_manager import _contracts_compatible, _make_disambig_key
from cex_part.utils.chains_filter import normalize_chain
from engine.liquidity_filter import _parse_gt_dex_id


def test_contracts_compatible_when_either_empty():
    assert _contracts_compatible({}, {"eth": "0xabc"}) is True
    assert _contracts_compatible({"contracts": {}}, {"eth": "0xabc"}) is True
    assert _contracts_compatible({"contracts": {"eth": "0xabc"}}, {}) is True


def test_contracts_compatible_shared_chain_same_addr():
    existing = {"contracts": {"ETH": "0xABC"}}
    assert _contracts_compatible(existing, {"eth": "0xabc"}) is True


def test_contracts_incompatible_shared_chain_diff_addr():
    existing = {"contracts": {"ETH": "0xABC"}}
    assert _contracts_compatible(existing, {"eth": "0xdef"}) is False


def test_contracts_incompatible_disjoint_chains():
    existing = {"contracts": {"BSC": "0x111"}}
    assert _contracts_compatible(existing, {"eth": "0x222"}) is False


def test_make_disambig_key_from_contract():
    # SYMBOL#<chain>.<first 8 hex of addr>
    key = _make_disambig_key("HOLO", {"ETH": "0x4c4d414400000000"})
    assert key == "HOLO#eth.4c4d4144"


def test_parse_gt_dex_id():
    assert _parse_gt_dex_id("uniswap_v3") == ("uniswap", "v3")
    assert _parse_gt_dex_id("pancakeswap_v2") == ("pancake", "v2")
    assert _parse_gt_dex_id("aerodrome-v1") == ("aerodrome", None)  # v1 not tracked
    assert _parse_gt_dex_id("") == ("unknown", None)


def test_normalize_chain():
    assert normalize_chain("Ethereum") == "ETH"
    assert normalize_chain("BNB Smart Chain") == "BSC"
    assert normalize_chain("Base Mainnet") == "BASE"
    assert normalize_chain(None) is None
    assert normalize_chain("Fantom") is None  # unknown -> None


def test_is_exchange_blacklisted():
    from cex_part.config import blacklist as bl

    bl.EXCHANGE_BLACKLIST.add(("FOO", "kraken"))
    try:
        assert bl.is_exchange_blacklisted("foo", "KRAKEN") is True  # case-insensitive
        assert bl.is_exchange_blacklisted("FOO", "bybit") is False  # other exchange ok
        assert bl.is_exchange_blacklisted("BAR", "kraken") is False  # other symbol ok
    finally:
        bl.EXCHANGE_BLACKLIST.discard(("FOO", "kraken"))

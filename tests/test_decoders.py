"""Swap-log decoders: correct extraction + graceful handling of truncated data."""

from dex_part.dex.pancake_v2.decoder import decode_swap as v2_decode
from dex_part.dex.pancake_v3.decoder import decode_swap as v3_decode
from dex_part.dex.uniswap_v4.decoder import decode_swap as v4_decode


def _word(n: int) -> str:
    return format(n, "064x")


def test_v2_decodes_reserves():
    data = "0x" + _word(5000) + _word(3000)
    out = v2_decode({"data": data}, {})
    assert out == {"reserve0": 5000, "reserve1": 3000}


def test_v2_rejects_truncated():
    assert v2_decode({"data": "0x" + _word(5000)}, {}) == {}  # only one word
    assert v2_decode({"data": "0x"}, {}) == {}
    assert v2_decode({}, {}) == {}  # missing data key -> no crash


def test_v3_decodes_sqrt_price():
    sqrt = 2 ** 96
    data = "0x" + _word(1) + _word(2) + _word(sqrt)  # amount0, amount1, sqrt
    out = v3_decode({"data": data}, {})
    assert out == {"sqrtPriceX96": sqrt}


def test_v3_rejects_truncated():
    assert v3_decode({"data": "0x" + _word(1) + _word(2)}, {}) == {}  # 2 words
    assert v3_decode({}, {}) == {}


def test_v4_decodes_poolid_and_sqrt():
    sqrt = 123456789
    pool_id = "0x" + "ab" * 32
    data = "0x" + _word(1) + _word(2) + _word(sqrt)
    out = v4_decode({"topics": ["0xsig", pool_id], "data": data})
    assert out["poolId"] == pool_id.lower()
    assert out["sqrtPriceX96"] == sqrt


def test_v4_rejects_missing_topic_or_truncated():
    assert v4_decode({"topics": ["0xsig"], "data": "0x" + "00" * 96}) == {}
    assert v4_decode({"topics": ["0xsig", "0xpool"], "data": "0x1234"}) == {}

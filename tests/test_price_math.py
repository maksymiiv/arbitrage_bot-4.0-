"""V3/V4 sqrtPriceX96 -> decimal price.

Note: raw_price_from_sqrt uses Decimal with the default 28-digit context,
so `Decimal(2)**96` is rounded — results are exact to ~28 sig figs, not
bit-exact. We compare with a tolerance accordingly.
"""

import pytest

from dex_part.utils.price_engine import raw_price_from_sqrt


def test_unit_sqrt_equal_decimals():
    # sqrt == 2**96 -> ratio 1 -> price 1; equal decimals -> no scaling
    assert float(raw_price_from_sqrt(2 ** 96, 18, 18)) == pytest.approx(1.0)


def test_decimal_scaling():
    # price scales by 10**(dec0-dec1) == 1e-12
    assert float(raw_price_from_sqrt(2 ** 96, 6, 18)) == pytest.approx(1e-12)


def test_price_is_square_of_ratio():
    # sqrt == 2**97 -> ratio 2 -> price 4 (equal decimals)
    assert float(raw_price_from_sqrt(2 ** 97, 18, 18)) == pytest.approx(4.0)

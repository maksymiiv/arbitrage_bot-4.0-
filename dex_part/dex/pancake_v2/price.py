from decimal import Decimal

from ...utils.native_price import get_native_usd


def compute_price(decoded: dict, meta: dict) -> Decimal:
    if not decoded:
        return Decimal("0")

    r0 = Decimal(decoded["reserve0"])
    r1 = Decimal(decoded["reserve1"])
    if r0 <= 0 or r1 <= 0:
        return Decimal("0")

    nr0 = r0 / (Decimal(10) ** meta["decimals0"])
    nr1 = r1 / (Decimal(10) ** meta["decimals1"])

    t0_stable = meta["token0_is_stable"]
    t1_stable = meta["token1_is_stable"]
    t0_native = meta["token0_is_native"]
    t1_native = meta["token1_is_native"]

    # stable / token: USD price of the non-stable side
    if t0_stable and not t1_stable:
        return nr0 / nr1
    if t1_stable and not t0_stable:
        return nr1 / nr0

    # native / token: convert via cached native_usd
    native_usd = get_native_usd(meta["chain"])
    if not native_usd:
        return Decimal("0")

    if t0_native and not t1_native:
        return (nr0 / nr1) * native_usd
    if t1_native and not t0_native:
        return (nr1 / nr0) * native_usd

    return Decimal("0")

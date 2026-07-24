from decimal import Decimal


def raw_price_from_sqrt(sqrt_x96: int, dec0: int, dec1: int) -> Decimal:
    """
    Convert Uniswap V3 sqrtPriceX96 into the decimal-adjusted token1/token0
    spot price.
    """
    sqrt_dec = Decimal(sqrt_x96)
    ratio = sqrt_dec / (Decimal(2) ** 96)
    price = ratio * ratio
    scale = Decimal(10) ** (dec0 - dec1)
    return price * scale

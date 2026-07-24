from .price_store import PriceStore

# Single shared instance.
price_store = PriceStore()

__all__ = ["price_store"]

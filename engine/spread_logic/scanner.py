from typing import List

from cex_part import cache_manager
from cex_part.config.blacklist import is_chain_blacklisted
from cex_part.core.orderbook_depth import cumulative_usd_in_range, register_depth_watch
from cex_part.network_status import register_hot_watch
from engine.config import ORDERBOOK_MIN_DEPTH_USD
from engine.liquidity_filter import is_low_liquidity

from .calculators import calc_dex_to_cex, calc_cex_to_dex
from .models import SpreadOpportunity


def scan_snapshot(snapshot: dict, min_spread_pct: float = 0.5) -> List[SpreadOpportunity]:
    """
    Для кожної комбінації CEX x DEX:
    - dex->cex існує тільки якщо dex_price < cex_bid
    - cex->dex існує тільки якщо dex_price > cex_ask
    Інакше opportunity нема.
    """
    opportunities: List[SpreadOpportunity] = []

    for key, data in snapshot.items():
        if not isinstance(data, dict):
            continue

        # use the human-readable ticker for output, fall back to the
        # internal key (which already carries an underscore/# suffix when
        # there is a symbol collision)
        symbol = data.get("display_symbol") or key

        cex_prices = data.get("cex") or {}
        dex_prices = data.get("dex") or {}

        if not cex_prices or not dex_prices:
            continue

        for cex_exchange, cex_data in cex_prices.items():
            if not isinstance(cex_data, dict):
                continue

            for dex_chain, dex_data in dex_prices.items():
                if not isinstance(dex_data, dict):
                    continue

                # safety net: stale прайс міг залишитись у price_store
                # після того як пул вже дропнули з pools.json та індексу
                # (наприклад BNB на eth — нові апдейти не приходять,
                # але snapshot ще тримає старе значення).
                if is_chain_blacklisted(symbol, dex_chain):
                    continue

                # Filter routes by deposit/withdraw status. `is_route_open`
                # returns None when we have no data (typical for kraken)
                # — we treat that as "don't filter" so kraken-side spreads
                # still show up.
                deposit_open = cache_manager.is_route_open(
                    key, cex_exchange, dex_chain, "deposit"
                )
                withdraw_open = cache_manager.is_route_open(
                    key, cex_exchange, dex_chain, "withdraw"
                )

                # Compute candidate opportunities. Cheap (pure arithmetic).
                # We *don't* call is_low_liquidity here yet — doing so for
                # every token with prices would also query liquidity for
                # pairs whose spread is sub-threshold, which spams the
                # GT API for no benefit. Liquidity is only checked below,
                # once an opportunity actually crosses min_spread_pct.
                opp1 = None
                if deposit_open is not False:
                    cand = calc_dex_to_cex(
                        symbol=symbol,
                        cex_exchange=cex_exchange,
                        cex_data=cex_data,
                        dex_chain=dex_chain,
                        dex_data=dex_data,
                    )
                    if cand is not None and cand.spread_pct >= min_spread_pct:
                        opp1 = cand

                opp2 = None
                if withdraw_open is not False:
                    cand = calc_cex_to_dex(
                        symbol=symbol,
                        cex_exchange=cex_exchange,
                        cex_data=cex_data,
                        dex_chain=dex_chain,
                        dex_data=dex_data,
                    )
                    if cand is not None and cand.spread_pct >= min_spread_pct:
                        opp2 = cand

                # Neither side crossed threshold → skip without any
                # liquidity check. Cheap exit path keeps GT API quiet.
                if opp1 is None and opp2 is None:
                    continue

                # At least one side has a real arbitrage candidate.
                # Now (and only now) consult the liquidity filter — on
                # cache miss this triggers a background fetch and
                # returns False, so the candidate shows once before the
                # next scan tick sees the cached verdict.
                token_addr = cache_manager.get_contract_address(key, dex_chain)
                if token_addr and is_low_liquidity(dex_chain, token_addr):
                    continue

                # Orderbook-depth filter. Register the token so the depth
                # poller keeps its Kraken book fresh while the spread
                # holds (Bybit is WS-live, register is a no-op there).
                # Require the CEX side to hold >= ORDERBOOK_MIN_DEPTH_USD
                # of cumulative volume in the profitable price band.
                # `None` depth = no book yet (first Kraken tick) → don't
                # filter; the next tick will have a snapshot.
                dex_price = dex_data.get("price")
                cex_bid = cex_data.get("bid")
                cex_ask = cex_data.get("ask")

                if opp1 is not None:
                    # dex -> cex: we sell into CEX bids between the DEX
                    # price (floor) and the CEX top bid (ceiling).
                    register_depth_watch(cex_exchange, symbol)
                    depth = cumulative_usd_in_range(
                        cex_exchange, symbol, "bid",
                        low=dex_price, high=cex_bid,
                    )
                    if depth is not None and depth < ORDERBOOK_MIN_DEPTH_USD:
                        opp1 = None
                    else:
                        # surviving opp — record depth for the log line
                        opp1.depth_usd = depth

                if opp2 is not None:
                    # cex -> dex: we buy CEX asks between the CEX top ask
                    # (floor) and the DEX price (ceiling).
                    register_depth_watch(cex_exchange, symbol)
                    depth = cumulative_usd_in_range(
                        cex_exchange, symbol, "ask",
                        low=cex_ask, high=dex_price,
                    )
                    if depth is not None and depth < ORDERBOOK_MIN_DEPTH_USD:
                        opp2 = None
                    else:
                        opp2.depth_usd = depth

                if opp1 is not None:
                    opportunities.append(opp1)
                    register_hot_watch(cex_exchange, key)
                if opp2 is not None:
                    opportunities.append(opp2)
                    register_hot_watch(cex_exchange, key)

    opportunities.sort(key=lambda x: x.spread_pct, reverse=True)
    return opportunities
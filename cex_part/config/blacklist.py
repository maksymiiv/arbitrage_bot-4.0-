# cex_part/config/blacklist.py

STABLE_SYMBOLS = {
    "USDT",
    "USDC",
    "BUSD",
    "DAI",
    "TUSD",
    "USTC",
    "FDUSD",
    "USDE",
    "USDD",
    "PYUSD",
    "USD0",
    "FRAX",
    "LUSD",
    "GUSD",
    "USDP",
    "RLUSD",
    "EURC",
    "USD1",
    "AUSD",
}

# Сюди можна руками додавати будь-які монети, які треба ігнорити
MANUAL_BLACKLIST = {
    "DYDX",  # not a stable — moved here from STABLE_SYMBOLS
    "VSN",   # find V4 pool; V3 currently too low-liq
    # "WBTC",
    # "WETH",
    # "HOLO",
}

# Якщо захочеш окремо вимикати wrapped/native/service токени
OPTIONAL_BLACKLIST = {
    # "WBNB",
    # "WETH",
    # "WMATIC",
}

GLOBAL_BLACKLIST = (
    set(STABLE_SYMBOLS)
    | set(MANUAL_BLACKLIST)
    | set(OPTIONAL_BLACKLIST)
)


# (symbol, chain) пари, де токен існує на чейні як ERC-20 / wrapped, але
# CEX не виводить туди — арбітраж структурно неможливий.
# Чейн — внутрішній ключ ("eth", "bsc", "base", "sol").
CHAIN_BLACKLIST: set[tuple[str, str]] = {
    # BNB — нативний для BSC; на ETH/Base існує тільки як bridged ERC-20,
    # CEX (Bybit/Kraken/Gate) виводять виключно у BSC-мережу.
    ("BNB", "eth"),
    ("BNB", "base"),
    ("BABYDOGE", "eth"),
    ("ENJ", "eth"),
    ("XUSD", "bsc"),
    # ("RAVE", "eth"), #v3 bad, later v4 is ok
    # ("SOSO", "eth"), #v3 bad, later v4 is ok
    ("HFT", "eth"),
    # ("ZKP", "eth"), #v3 bad, later v4 is ok
    # ("FF", "eth"), #v3 bad, later v4 is ok
    ("DOT", "bsc"),
    ("XRT", "eth"),
    ("SRM", "eth"),
    ("LCX", "eth"),
    ("ARPA", "eth"),
    ("KEY", "eth"),
    ("GAIB", "bsc"),
    ("PUMPBTC", "bsc"),
    ("DOT", "base"),
    ("DOVU", "base"),
    ("EWT", "eth"),
    ("YALA", "eth"),
    ("ALPHA", "bsc"),
    # ("ROLL", "base"), #v3 bad, later v4 is ok
    ("FIGHT", "bsc"), #v3 bad, later v4 is oks
    # ("BASED", "eth"), #v3 bad, later v4 is ok
    # ("REZ", "eth"), #v3 bad, later v4 is ok
    # ("SAHARA", "eth"), #v3 bad, later v4 is ok
    
    
    
    
    


    # # SOL — нативний Solana; wrapped SOL на EVM немає CEX-шляху виводу.
    # ("SOL", "eth"),
    # ("SOL", "bsc"),
    # ("SOL", "base"),

    # # AVAX — CEX виводять тільки у C-Chain (Avalanche), не EVM-bridge.
    # ("AVAX", "eth"),
    # ("AVAX", "bsc"),
    # ("AVAX", "base"),

    # # TRX — Tron-нативний, EVM-копії без CEX-маршруту виводу.
    # ("TRX", "eth"),
    # ("TRX", "bsc"),
    # ("TRX", "base"),
}


def normalize_symbol(symbol: str) -> str:
    return (symbol or "").strip().upper()


def normalize_chain(chain: str) -> str:
    return (chain or "").strip().lower()


def is_blacklisted(symbol: str) -> bool:
    return normalize_symbol(symbol) in GLOBAL_BLACKLIST


def is_chain_blacklisted(symbol: str, chain: str) -> bool:
    """True якщо (symbol, chain) у списку структурно нереалізованих пар."""
    return (normalize_symbol(symbol), normalize_chain(chain)) in CHAIN_BLACKLIST


def filter_blacklisted_symbols(symbols: list[str]) -> list[str]:
    result = []
    seen = set()

    for sym in symbols:
        s = normalize_symbol(sym)
        if not s:
            continue
        if s in GLOBAL_BLACKLIST:
            continue
        if s in seen:
            continue
        seen.add(s)
        result.append(s)

    return result
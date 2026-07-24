from web3 import Web3

STABLES = {
    "bsc": [
        "0x55d398326f99059ff775485246999027b3197955",  # USDT
        "0xe9e7cea3dedca5984780bafc599bd69add087d56",  # BUSD
        "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",  # USDC
        "0x8d0D000Ee44948FC98c9B98A4FA4921476f08B0d",   # USD1
        "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
        "0x17EAfd08994305D8AcE37EfB82F1523177eC70EE",
    ],
    "eth": [
        "0xdAC17F958D2ee523a2206206994597C13D831ec7",  # USDT
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
        "0x6b175474e89094c44da98b954eedeac495271d0f",  # DAI
    ],
    "base": [
        # USDC на Base
        "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        # USDbC (wrapped USDC з Ethereum, теж дуже популярний на Base)
        "0x0FA42cE0eCF862fC8aA301B3838aFa3935bF68c4",
    ],
}

SWAP_STABLES_RAW = {
    "bsc": "0x55d398326f99059fF775485246999027B3197955",
    "eth": "0x55d398326f99059fF775485246999027B3197955", 
    "base": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
}

SWAP_STABLES = {
    chain: Web3.to_checksum_address(addr)
    for chain, addr in SWAP_STABLES_RAW.items()
}

# The zero address denotes NATIVE currency in Uniswap V4 (a V4 pool may
# hold native ETH directly, not WETH). It's listed alongside the wrapped
# native so `is_native()` recognises both — harmless for V2/V3 which
# never use 0x0 as a token.
_NATIVE_ZERO = "0x0000000000000000000000000000000000000000"

NATIVE_NAMES = {
    "bsc": [
        "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
    ],
    "eth": [
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        _NATIVE_ZERO,  # native ETH — Uniswap V4
    ],
    "polygon": [
        "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270"
    ],
    "base": [
        "0x4200000000000000000000000000000000000006",
        _NATIVE_ZERO,  # native ETH — Uniswap V4
    ]
}
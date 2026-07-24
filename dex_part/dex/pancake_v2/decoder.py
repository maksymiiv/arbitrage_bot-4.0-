"""Decode a V2 Sync(uint112,uint112) log into reserves."""


def decode_swap(log: dict, meta: dict) -> dict:
    data = log["data"][2:]
    reserve0 = int(data[0:64], 16)
    reserve1 = int(data[64:128], 16)
    return {"reserve0": reserve0, "reserve1": reserve1}

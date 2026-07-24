"""Decode a V2 Sync(uint112,uint112) log into reserves."""


def decode_swap(log: dict, meta: dict) -> dict:
    data = log.get("data") or ""
    if data.startswith("0x"):
        data = data[2:]
    if len(data) < 128:  # need two 32-byte words (reserve0, reserve1)
        return {}
    reserve0 = int(data[0:64], 16)
    reserve1 = int(data[64:128], 16)
    return {"reserve0": reserve0, "reserve1": reserve1}

import zlib, json

def tpke_encrypt(batch: list) -> bytes:
    raw = json.dumps(batch, ensure_ascii=False).encode("utf-8")
    return zlib.compress(raw)

def tpke_decrypt(ciphertext: bytes, shares: int) -> list:
    raw = zlib.decompress(ciphertext)
    return json.loads(raw.decode("utf-8"))

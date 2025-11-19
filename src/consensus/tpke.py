# src/consensus/tpke.py
"""
TPKE mô phỏng để dùng trong HoneyBadgerBFT.
Interface khớp với paper: Setup, Enc, DecShare, Dec.

Chú ý: Đây KHÔNG phải crypto thật, chỉ để mô phỏng chi phí & giao thức.
"""

import json
import random
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple


@dataclass
class TPKEPublicKey:
    key_id: int  # mô phỏng


@dataclass
class TPKESecretKeyShare:
    key_id: int
    node_id: int  # ai giữ share này


def tpke_setup(node_ids: List[int]) -> Tuple[TPKEPublicKey, Dict[int, TPKESecretKeyShare]]:
    """
    Mô phỏng Setup: tạo 1 key_id chung và 1 share cho mỗi node.
    """
    key_id = random.randint(1_000_000, 9_999_999)
    pk = TPKEPublicKey(key_id=key_id)
    sk_shares = {
        nid: TPKESecretKeyShare(key_id=key_id, node_id=nid)
        for nid in node_ids
    }
    return pk, sk_shares


def tpke_encrypt(pk: TPKEPublicKey, payload: Any) -> bytes:
    """
    Enc: serialize payload thành bytes, thêm header (key_id).
    """
    blob = {
        "k": pk.key_id,
        "p": payload,  # payload phải JSON-serializable
    }
    return json.dumps(blob).encode("utf-8")


def tpke_dec_share(sk: TPKESecretKeyShare, ciphertext: bytes) -> Dict:
    """
    Một share giải mã − ở mô phỏng, chỉ wrap lại ciphertext + id node.
    """
    return {
        "node": sk.node_id,
        "ct": ciphertext.decode("utf-8"),
    }


def tpke_decrypt(pk: TPKEPublicKey, shares: List[Dict]) -> Any:
    """
    Dec: chỉ cần >= 1 share là đọc được. Thực tế paper cần f+1 share:contentReference[oaicite:2]{index=2},
    nhưng ở đây ta không check đủ số share, chỉ cần không rỗng.
    """
    if not shares:
        raise ValueError("No shares provided")

    ct_str = shares[0]["ct"]
    blob = json.loads(ct_str)
    if blob.get("k") != pk.key_id:
        raise ValueError("Key mismatch in TPKE decrypt")
    return blob["p"]

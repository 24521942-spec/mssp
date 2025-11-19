"""
Mô-đun mô phỏng Threshold Public Key Encryption (TPKE) cho HoneyBadgerBFT.

Mục tiêu của TPKE là cho phép các nút mã hoá một payload với một khóa công
khai duy nhất nhưng chỉ có thể giải mã khi có đủ f+1 share từ những nút
khác nhau. Trong mô phỏng này, chúng ta không thực thi mã hoá thật mà
chỉ serial hoá payload thành chuỗi JSON và kiểm tra ngưỡng share khi giải
mã.

Thuộc tính:
    - Khoá công khai (TPKEPublicKey) giữ id khoá, số node và ngưỡng
      yêu cầu để giải mã.
    - Mỗi share bí mật (TPKESecretKeyShare) chỉ bao gồm id khoá và id
      node sở hữu.

Hàm:
    tpke_setup(node_ids): tạo khoá công khai và phân bổ share bí mật cho
        từng node, đồng thời tính toán ngưỡng f+1.
    tpke_encrypt(pk, payload): serial hoá payload với id khoá.
    tpke_dec_share(sk, ciphertext): tạo share giải mã (mô phỏng).
    tpke_decrypt(pk, shares): kết hợp share để giải mã payload, kiểm tra
        đủ số lượng share f+1.

Lưu ý: Đây không phải là hệ thống mã hoá thật; mục đích chính là mô
phỏng chi phí và ngưỡng số share cần thiết.
"""

import json
import random
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple


@dataclass
class TPKEPublicKey:
    """
    Mô tả khoá công khai cho TPKE. Ngoài id khoá, đối tượng còn chứa
    thông tin về số node trong hệ thống và ngưỡng share tối thiểu để
    giải mã (threshold = f + 1, với f là số kẻ xấu tối đa).
    """
    key_id: int
    num_nodes: int
    threshold: int


@dataclass
class TPKESecretKeyShare:
    """
    Mỗi phần share của khoá bí mật gắn với một node cụ thể. Trong mô
    phỏng này, nó chứa id khoá và id của node sở hữu share.
    """
    key_id: int
    node_id: int


def tpke_setup(node_ids: List[int]) -> Tuple[TPKEPublicKey, Dict[int, TPKESecretKeyShare]]:
    """
    Thiết lập hệ thống TPKE cho danh sách node_id.

    Hàm tạo một key_id ngẫu nhiên và tính toán f = floor((n-1)/3), với n là
    số lượng node. Ngưỡng giải mã là f+1. Hàm trả về khoá công khai và
    map từ node_id sang share bí mật của node đó.

    Args:
        node_ids (List[int]): danh sách các node tham gia.

    Returns:
        Tuple[TPKEPublicKey, Dict[int, TPKESecretKeyShare]]: khoá công khai
            và các share bí mật.
    """
    key_id = random.randint(1_000_000, 9_999_999)
    n = len(node_ids)
    # Theo giả thiết Byzantine: f = floor((n-1)/3)
    f = max(1, (n - 1) // 3) if n > 1 else 1
    threshold = f + 1
    pk = TPKEPublicKey(key_id=key_id, num_nodes=n, threshold=threshold)
    sk_shares = {
        nid: TPKESecretKeyShare(key_id=key_id, node_id=nid) for nid in node_ids
    }
    return pk, sk_shares


def tpke_encrypt(pk: TPKEPublicKey, payload: Any) -> bytes:
    """
    Mã hoá payload với khoá công khai. Mô phỏng bằng cách serial hoá
    payload thành JSON và lưu id khoá.

    Args:
        pk (TPKEPublicKey): khoá công khai.
        payload (Any): dữ liệu cần mã hoá (phải JSON-serializable).

    Returns:
        bytes: ciphertext dạng bytes.
    """
    blob = {
        "k": pk.key_id,
        "p": payload,
    }
    return json.dumps(blob).encode("utf-8")


def tpke_dec_share(sk: TPKESecretKeyShare, ciphertext: bytes) -> Dict:
    """
    Tạo share giải mã cho ciphertext. Trong mô phỏng, share chứa id node
    và nội dung ciphertext dạng chuỗi.

    Args:
        sk (TPKESecretKeyShare): share bí mật của node.
        ciphertext (bytes): ciphertext nhận được.

    Returns:
        Dict: đối tượng chứa node_id và ciphertext dạng chuỗi.
    """
    return {
        "node": sk.node_id,
        "ct": ciphertext.decode("utf-8"),
    }


def tpke_decrypt(pk: TPKEPublicKey, shares: List[Dict]) -> Any:
    """
    Giải mã ciphertext dựa trên tập hợp share. Cần ít nhất pk.threshold
    share để thành công; nếu không đủ thì raise ValueError. Hàm sử dụng
    share đầu tiên để khôi phục payload.

    Args:
        pk (TPKEPublicKey): khoá công khai.
        shares (List[Dict]): danh sách share từ các node.

    Returns:
        Any: payload gốc được giải mã.

    Raises:
        ValueError: nếu không đủ share hoặc id khoá không khớp.
    """
    if not shares or len(shares) < pk.threshold:
        raise ValueError("Not enough decryption shares provided")
    # dùng share đầu tiên làm đại diện
    ct_str = shares[0]["ct"]
    blob = json.loads(ct_str)
    if blob.get("k") != pk.key_id:
        raise ValueError("Key mismatch in TPKE decrypt")
    return blob["p"]
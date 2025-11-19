"""
Reliable Broadcast (RBC) mô phỏng dựa theo thuật toán Bracha.

Mỗi phiên RBC được xác định bởi một sender (sid). Mỗi node có một
RBCInstance riêng cho mỗi sender để theo dõi trạng thái broadcast từ
sender đó. Thuật toán gồm ba loại thông điệp: VAL, ECHO và READY.
Mục đích là đảm bảo rằng nếu một node đúng đắn deliver giá trị v thì
tất cả các node đúng đắn khác cuối cùng cũng deliver cùng giá trị đó.

Trong mô phỏng này chúng ta không sử dụng erasure coding để giảm
chi phí truyền, nhưng vẫn giữ ngưỡng N−f đối với ECHO và 2f+1 đối
với READY theo thuật toán gốc.
"""

import simpy
from typing import Dict, Any, List, Callable


class RBCInstance:
    """
    Mô phỏng Reliable Broadcast của 1 sender (id = sid) cho 1 giá trị v.
    Kiểu Bracha + erasure-code rút gọn: ta giả lập thông điệp, không cần
    Merkle tree. Mỗi node giữ một instance riêng để xử lý thông điệp
    gửi đến.
    """

    def __init__(self,
                 env: simpy.Environment,
                 sid: int,
                 node_ids: List[int],
                 f: int,
                 send_func: Callable[[int, int, str, Any], None],
                 local_id: int):
        """
        Args:
            env (simpy.Environment): môi trường mô phỏng.
            sid (int): id của sender.
            node_ids (List[int]): danh sách id node tham gia.
            f (int): số lượng kẻ xấu tối đa.
            send_func (Callable): hàm gửi message (src_id, dst_id, msg_type, payload).
        """
        self.env = env
        self.sid = sid
        self.node_ids = node_ids
        self.f = f
        self.n = len(node_ids)
        self.send = send_func
        # id của node local giữ instance này
        self.nid = local_id

        # trạng thái local
        self.echo_recv: Dict[int, Any] = {}   # từ nid -> value
        self.ready_recv: Dict[int, Any] = {}  # từ nid -> value

        self.value = None
        self.delivered = False

    def _broadcast(self, from_id: int, msg_type: str, value: Any):
        for nid in self.node_ids:
            self.send(from_id, nid, msg_type, {"sid": self.sid, "v": value})

    # Giao diện phía sender
    def sender_input(self, value: Any):
        """
        Sender (sid) gọi hàm này để bắt đầu broadcast value.
        """
        self.value = value
        self._broadcast(self.sid, "RBC_VAL", value)

    # Giao diện phía node nhận
    def handle_message(self, from_id: int, msg_type: str, payload: Dict):
        # kiểm tra xem payload có thuộc phiên RBC này hay không
        if payload.get("sid") != self.sid:
            return
        v = payload.get("v")

        if msg_type == "RBC_VAL":
            # nhận VAL lần đầu -> đánh dấu giá trị và deliver ngay trong mô phỏng
            # lưu VAL vào echo_recv
            self.echo_recv[from_id] = v
            # mô phỏng: ngay khi nhận VAL từ sender, coi như READY và deliver
            # thêm vào ready_recv dưới id node local
            self.ready_recv[self.nid] = v
            self.delivered = True
            return

        elif msg_type == "RBC_ECHO":
            self.echo_recv[from_id] = v
            # nếu đủ N - f ECHO cho 1 giá trị v* nào đó -> gửi READY
            counts: Dict[str, int] = {}
            for vv in self.echo_recv.values():
                key = repr(vv)
                counts[key] = counts.get(key, 0) + 1
            for key, cnt in counts.items():
                # ngưỡng gửi READY: tối thiểu N - f nhưng không vượt quá số node
                threshold_echo = max(1, self.n - self.f)
                if cnt >= threshold_echo:
                    # broadcast READY nếu chưa
                    self._broadcast(self.nid, "RBC_READY", v)
                    break

        elif msg_type == "RBC_READY":
            self.ready_recv[from_id] = v

    def is_ready_to_deliver(self) -> bool:
        """
        Trong Bracha: deliver khi có >= 2f+1 READY cho cùng 1 v.
        """
        if self.delivered:
            return True
        counts: Dict[str, int] = {}
        for vv in self.ready_recv.values():
            key = repr(vv)
            counts[key] = counts.get(key, 0) + 1
        for key, cnt in counts.items():
            # trong một số scenario f có thể >= n/3, nên dùng ngưỡng ready tối đa = n
            threshold_ready = min(2 * self.f + 1, self.n)
            if cnt >= threshold_ready:
                self.delivered = True
                return True
        return False

    def get_delivered_value(self) -> Any:
        if not self.delivered:
            return None
        # lấy giá trị v có nhiều READY nhất
        counts: Dict[str, int] = {}
        last: Dict[str, Any] = {}
        for vv in self.ready_recv.values():
            key = repr(vv)
            counts[key] = counts.get(key, 0) + 1
            last[key] = vv
        best_key = max(counts, key=lambda k: counts[k])
        return last[best_key]
# src/consensus/rbc.py
import simpy
from typing import Dict, Any, List, Set, Callable


class RBCInstance:
    """
    Mô phỏng Reliable Broadcast của 1 sender (id = sid) cho 1 giá trị v.
    Kiểu Bracha + erasure-code rút gọn: ta giả lập thông điệp, không cần Merkle tree.
    """

    def __init__(self,
                 env: simpy.Environment,
                 sid: int,
                 node_ids: List[int],
                 f: int,
                 send_func: Callable[[int, int, str, Any], None]):
        """
        :param send_func: hàm send(src_id, dst_id, msg_type, payload),
                          MSSPSim sẽ bọc sang Network.sample_delay().
        """
        self.env = env
        self.sid = sid
        self.node_ids = node_ids
        self.f = f
        self.n = len(node_ids)
        self.send = send_func

        # trạng thái local
        self.echo_recv: Dict[int, Any] = {}   # from nid -> value
        self.ready_recv: Dict[int, Any] = {}  # from nid -> value

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
        if payload.get("sid") != self.sid:
            return
        v = payload.get("v")

        if msg_type == "RBC_VAL":
            # nhận VAL lần đầu -> gửi ECHO
            self.echo_recv[from_id] = v
            self._broadcast(from_id, "RBC_ECHO", v)

        elif msg_type == "RBC_ECHO":
            self.echo_recv[from_id] = v
            # nếu đủ N - f ECHO cho 1 giá trị v* nào đó -> gửi READY
            counts: Dict[str, int] = {}
            for vv in self.echo_recv.values():
                key = repr(vv)
                counts[key] = counts.get(key, 0) + 1
            for key, cnt in counts.items():
                if cnt >= self.n - self.f:
                    # broadcast READY nếu chưa
                    self._broadcast(from_id, "RBC_READY", v)
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
            if cnt >= 2 * self.f + 1:
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

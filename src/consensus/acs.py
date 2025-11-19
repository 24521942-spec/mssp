"""
Atomic Common Subset (ACS) protocol cho HoneyBadgerBFT.

ACS phối hợp nhiều phiên broadcast tin cậy (RBC) và binary agreement (ABA)
để tất cả các node đồng thuận trên một tập con chung các đề xuất. Mỗi
proposer i sẽ thực hiện RBC_i để phát tán ciphertext của mình, sau đó
chạy ABA_i để quyết định xem ciphertext đó có được đưa vào tập kết quả
hay không. Khi ít nhất n−f phiên ABA output 1, các phiên còn lại sẽ
được feed input 0. Kết quả cuối cùng là tập con những ciphertext có
ABA=1.

Trong mô phỏng này, ACS chạy từ góc nhìn của một node duy nhất (zone
controller) và giữ trạng thái cục bộ cho tất cả RBC_i và ABA_i. Việc xử
lý message được thực hiện thông qua handle_message, tương thích với
network.
"""

import simpy
from typing import Dict, List, Any, Callable

from .rbc import RBCInstance
from .aba import ABAInstance
from .coin import CommonCoin


class ACS:
    """
    ACS một-shot cho 1 epoch, theo Ben-Or et al.:
      - Mỗi node i có 1 RBC_i và 1 ABA_i
      - Kết quả: tập con chỉ số i mà ABA_i = 1 + value RBC_i tương ứng.
    """

    def __init__(self,
                 env: simpy.Environment,
                 node_ids: List[int],
                 f: int,
                 send_func: Callable[[int, int, str, Any], None],
                 coin_seed: int):

        self.env = env
        self.node_ids = node_ids
        self.f = f
        self.n = len(node_ids)
        self.send = send_func

        self.coin = CommonCoin(env, node_ids, coin_seed)

        # rbc[iid][nid] = RBCInstance (view của node nid về sender iid)
        self.rbc: Dict[int, Dict[int, RBCInstance]] = {}
        self.aba: Dict[int, ABAInstance] = {}
        self.aba_input_given: Dict[int, bool] = {i: False for i in node_ids}

        for sid in node_ids:
            self.rbc[sid] = {}
            for nid in node_ids:
                self.rbc[sid][nid] = RBCInstance(env, sid, node_ids, f, send_func)

            self.aba[sid] = ABAInstance(env,
                                         inst_id=f"ABA_{sid}",
                                         node_ids=node_ids,
                                         f=f,
                                         coin=self.coin)

    # ==== interface để node i "propose" giá trị ====

    def propose(self, proposer_id: int, value: Any):
        """
        Node proposer_id đưa value vào RBC_{proposer_id}.
        """
        self.rbc[proposer_id][proposer_id].sender_input(value)

    def handle_message(self, from_id: int, to_id: int, msg_type: str, payload: Any):
        """
        Router: từ Sim/Network gọi vào đây cho message của RBC/ABA/coin.
        Tùy msg_type prefix mà chuyển tiếp.
        """
        if msg_type.startswith("RBC_"):
            sid = payload.get("sid")
            if sid in self.rbc:
                self.rbc[sid][to_id].handle_message(from_id, msg_type, payload)

        elif msg_type.startswith("ABA_"):
            # payload phải chứa inst để xác định ABA instance
            inst = self.aba.get(payload.get("inst"))
            if inst:
                inst.handle_message(from_id, msg_type, payload)

        elif msg_type.startswith("COIN_"):
            self.coin.handle_message(from_id, msg_type, payload)

    def _maybe_feed_aba_inputs(self):
        """
        Triển khai đúng ý Ben-Or:
          - Khi một RBC_j của node i deliver value lần đầu => ABA_j input 1
          - Khi đã có >= N-f ABA = 1, thì các ABA chưa input => input 0
        (Ở đây ta xét từ góc nhìn "zone controller", tương đương 1 node).
        """
        # 1) check RBC deliver -> ABA input 1
        delivered_indices = []
        for sid in self.node_ids:
            inst = self.rbc[sid][sid]  # nhìn từ sender chính
            if inst.is_ready_to_deliver() and not self.aba_input_given[sid]:
                self.aba[sid].input(1)
                self.aba_input_given[sid] = True
                delivered_indices.append(sid)

        # 2) nếu đã >= N-f ABA=1 -> feed 0 cho các ABA còn lại
        yes_count = sum(
            1 for sid in self.node_ids
            if self.aba_input_given[sid] and self.aba[sid].has_output() and self.aba[sid].get_output() == 1
        )
        if yes_count >= self.n - self.f:
            for sid in self.node_ids:
                if not self.aba_input_given[sid]:
                    self.aba[sid].input(0)
                    self.aba_input_given[sid] = True

    def run_epoch(self) -> Dict[int, Any]:
        """
        Tiến hành cho đến khi TẤT CẢ ABA_i đều có output.
        Trả về map sid -> value đã RBC-delivered (chỉ với sid mà ABA_i=1).
        """
        while True:
            yield self.env.timeout(0.01)
            self._maybe_feed_aba_inputs()
            if all(self.aba[sid].has_output() for sid in self.node_ids):
                break
        chosen: Dict[int, Any] = {}
        for sid in self.node_ids:
            if self.aba[sid].get_output() == 1:
                val = None
                # mỗi node có view RBC riêng, ở đây lấy view của sender chính cho đơn giản
                inst = self.rbc[sid][sid]
                if inst.is_ready_to_deliver():
                    val = inst.get_delivered_value()
                chosen[sid] = val
        return chosen
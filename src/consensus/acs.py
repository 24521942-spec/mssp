from __future__ import annotations
import simpy
from typing import Dict, List
from .rbc import RBC, RBCMessage
from .aba import ABA

class ACSResult:
    def __init__(self):
        self.accepted_sender_ids: List[int] = []
        self.payloads: Dict[int, bytes] = {}

class ACS:
    """ACS = nhiều RBC + nhiều ABA → subset chung (rút gọn)."""
    def __init__(self, env: simpy.Environment, node_ids: List[int], f:int, coin_seed:int):
        self.env = env
        self.node_ids = node_ids
        self.f = f
        self.rbc = RBC(env, node_ids, f)
        self.aba = ABA(env, n=len(node_ids), f=f, seed=coin_seed)

    def run_epoch(self, epoch:int, proposals: Dict[int, bytes]):
        # “phát” RBC (mô phỏng: mọi node đều on_receive)
        for sid, payload in proposals.items():
            msg = RBCMessage(sender_id=sid, epoch=epoch, cid=f"{epoch}:{sid}", payload=payload)
            for nid in self.node_ids:
                self.rbc.on_receive(msg, from_node=nid)

        accepted = []
        for sid in proposals.keys():
            delivered = self.rbc.is_delivered(epoch, sid)
            init_bit = 1 if delivered else 0
            decision = yield self.env.process(self.aba.decide(initial_bit=init_bit))
            if decision == 1 and delivered:
                accepted.append(sid)

        if len(accepted) < self.f + 1 and len(accepted) > 0:
            accepted = accepted[: self.f + 1]

        res = ACSResult()
        res.accepted_sender_ids = accepted
        for sid in accepted:
            res.payloads[sid] = self.rbc.get_payload(epoch, sid)
        return res

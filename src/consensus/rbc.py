from __future__ import annotations
import simpy
from typing import Dict, List, Tuple

class RBCMessage:
    def __init__(self, sender_id:int, epoch:int, cid:str, payload:bytes):
        self.sender_id = sender_id
        self.epoch = epoch
        self.cid = cid
        self.payload = payload

class RBC:
    """RBC rút gọn để mô phỏng (Echo/Ready theo ngưỡng)."""
    def __init__(self, env: simpy.Environment, node_ids: List[int], f: int):
        self.env = env
        self.node_ids = node_ids
        self.f = f
        self.echo: Dict[Tuple[int,int], set] = {}
        self.ready: Dict[Tuple[int,int], set] = {}
        self.delivered: Dict[Tuple[int,int], bytes] = {}

    def on_receive(self, msg: RBCMessage, from_node:int):
        key = (msg.epoch, msg.sender_id)
        self.echo.setdefault(key, set()).add(from_node)
        if len(self.echo[key]) >= self.f + 1:
            self.ready.setdefault(key, set()).add(from_node)
        if len(self.ready.get(key, set())) >= 2*self.f + 1:
            self.delivered.setdefault(key, msg.payload)

    def is_delivered(self, epoch:int, sender_id:int) -> bool:
        return (epoch, sender_id) in self.delivered

    def get_payload(self, epoch:int, sender_id:int):
        return self.delivered.get((epoch, sender_id))

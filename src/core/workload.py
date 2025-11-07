# src/core/workload.py
from typing import Callable, Optional
import simpy
import random

class Attacker:
    def __init__(self, env: simpy.Environment, interval: float, send_double_spend: Callable):
        self.env = env
        self.interval = interval
        self.send_double_spend = send_double_spend

    def run(self):
        while True:
            yield self.env.timeout(self.interval)
            self.send_double_spend()

def addr_to_shard(addr: str, n_shards: int) -> int:
    return hash(addr) % n_shards

def tx_stream_generator(tx_iter, select_prob=1.0):
    for tx in tx_iter:
        if random.random() <= select_prob:
            yield tx

# src/core/models.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Set
import simpy

@dataclass
class Block:
    block_id: str
    zone_id: Optional[str]
    parent_id: Optional[str]
    txs: list
    priority_at_create: int
    timestamp: float
    conflict_id: Optional[str] = None
    confirmed: bool = False

@dataclass
class Shard:
    shard_id: int
    blocks: Dict[str, Block] = field(default_factory=dict)
    tips: Set[str] = field(default_factory=set)

    def init_genesis(self):
        gid = f"g_{self.shard_id}"
        g = Block(gid, None, None, [], 0, 0.0)
        self.blocks[gid] = g
        self.tips.add(gid)

@dataclass
class Node:
    env: simpy.Environment
    node_id: int
    stored_shards: Set[int]
    is_malicious: bool = False
    update_history: List[bool] = field(default_factory=list)

@dataclass
class ConsensusZone:
    env: simpy.Environment
    zone_id: str
    shards: List[int]
    nodes: List[Node]
    priority: int = 5
    block_counter: int = 0
    # sorting auto-tune
    cycle_blocks: int = 0
    cycle_confirms: int = 0
    # PBFT counters
    pbft_success: int = 0
    pbft_failure: int = 0
    view_changes: int = 0

    def quorum_params(self):
        n = len(self.nodes)
        f = (n - 1) // 3 if n > 0 else 0
        quorum = 2 * f + 1
        return n, f, quorum

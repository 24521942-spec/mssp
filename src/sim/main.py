# src/sim/main.py
from __future__ import annotations

import random
from collections import deque
from typing import Callable, Optional, Dict, List

import simpy

from ..core.config import Config
from ..core.models import Shard, Node, ConsensusZone, Block
from ..core.network import Network
from ..core.pbft import run_pbft
from ..core.sorting import PrioritySorter
from ..core.metrics import Metrics
from ..core.workload import addr_to_shard

# chỉ import stream reader để tránh trùng tên với feed()
from ..io.tx_reader import stream_transactions_from_csv  # alias đã có trong tx_reader.py

class MSSPSim:
    """
    Mô phỏng MSSP: shards, nodes, consensus zones, PBFT, priority sorting.
    Thêm API accept_tx() để nạp giao dịch từ ngoài (CSV/REST) + tiến trình drain.
    """

    def __init__(self, cfg: Config):
        random.seed(cfg.sys.random_seed)
        self.cfg = cfg
        self.env = simpy.Environment()

        # --- Shards ---
        self.shards: Dict[int, Shard] = {i: Shard(i) for i in range(cfg.sys.shards)}
        for s in self.shards.values():
            s.init_genesis()

        # --- Nodes ---
        self.nodes: List[Node] = []
        total = cfg.sys.shards * cfg.sys.nodes_per_shard
        for nid in range(total):
            if random.random() < cfg.sys.mnode_fraction:
                stored = set(random.sample(range(cfg.sys.shards), k=min(2, cfg.sys.shards)))
            else:
                stored = {nid % cfg.sys.shards}
            is_bad = (random.random() < cfg.sys.malicious_fraction)
            self.nodes.append(Node(self.env, nid, stored, is_bad))

        # --- Zones (pairwise) ---
        self.zones: Dict[str, ConsensusZone] = {}
        for i in range(cfg.sys.shards):
            for j in range(i, cfg.sys.shards):
                z_nodes = [n for n in self.nodes if (i in n.stored_shards and j in n.stored_shards)]
                if z_nodes:
                    zid = f"{i}_{j}"
                    self.zones[zid] = ConsensusZone(
                        self.env, zid, [i, j], z_nodes,
                        priority=random.randint(4, 7)
                    )

        # --- Network ---
        self.net = Network(cfg.net.mean_delay, cfg.net.jitter, cfg.net.drop_prob, cfg.net.use_topology)
        if cfg.net.use_topology:
            self.net.build_random_topology([n.node_id for n in self.nodes], avg_degree=3)

        # --- Metrics & conflict map ---
        self.metrics = Metrics().__dict__       # các thành phần khác dùng dict[...]
        self.conflict_map: Dict[str, set] = {}

        # --- Sorter (truyền conflict_map để detect DS) ---
        self.sorter = PrioritySorter(self.shards, self.zones, cfg.sort, self.metrics, self.conflict_map)

        # ===== Ingress từ bên ngoài =====
        self._ext_tx_buffer = deque()           # hàng đợi TX từ feed bên ngoài
        self._ext_tx_sink: Optional[Callable] = self._resolve_internal_tx_sink()
        self._ext_drain_started = False

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------
    def _sample_processing(self) -> float:
        m = self.cfg.sys.process_mean
        return random.expovariate(1.0 / m) if m > 0 else 0.0

    def _deadline(self) -> float:
        return self.cfg.pprob.deadline_factor * self.cfg.net.mean_delay

    def _record_update(self, node: Node, on_time: bool):
        node.update_history.append(on_time)
        if len(node.update_history) > self.cfg.pprob.history_window:
            node.update_history.pop(0)

    def _quarantine(self, node: Node):
        node.is_malicious = True
        for z in self.zones.values():
            if node in z.nodes:
                z.nodes.remove(node)
        self.metrics['faulty_nodes_quarantined'] += 1

    # ------------------------------------------------------------------
    # API: ingest TX (được drain gọi hoặc accept_tx gọi trực tiếp)
    # ------------------------------------------------------------------
    def _ingest_tx(self, tx: Dict):
        """
        Map TX -> shard/zid -> tạo process xử lý block trong zone tương ứng.
        TX tối thiểu: {'from','to','value', (tùy) 'conflict_id'} (chuỗi).
        """
        if not tx.get('from') or not tx.get('to'):
            return
        s_from = addr_to_shard(str(tx['from']), self.cfg.sys.shards)
        s_to   = addr_to_shard(str(tx['to']),   self.cfg.sys.shards)
        zid = f"{min(s_from, s_to)}_{max(s_from, s_to)}"
        zone = self.zones.get(zid)
        if not zone or not zone.nodes:
            return
        leader = zone.nodes[0]
        cid = tx.get('conflict_id')
        # Một TX → một block ở zone, block có thể được replicate lên cả 2 shard của zone
        self.env.process(self.process_zone(zid, [dict(tx)], leader, cid))

    # ------------------------------------------------------------------
    # Ingress API công khai: nhận TX từ ngoài (CSV/REST/attack)
    # ------------------------------------------------------------------
    def accept_tx(self, tx: Dict):
        """
        Nhận giao dịch từ bên ngoài. Nếu đã có sink nội bộ → ingest ngay,
        nếu chưa → vào buffer, process `_drain_external_txs()` sẽ rót dần.
        """
        if self._ext_tx_sink:
            try:
                # dùng sink nội bộ nếu đã resolve (hiện tại sink chính là _ingest_tx)
                self._ext_tx_sink(tx)
                return
            except Exception:
                pass
        self._ext_tx_buffer.append(tx)

    def _resolve_internal_tx_sink(self) -> Optional[Callable]:
        """
        Trả về callable(tx) để nạp TX vào simulator.
        Ở bản này, ta dùng trực tiếp self._ingest_tx (đơn giản & chắc chắn).
        Nếu sau này bạn có mempool/workload riêng, có thể đổi logic ở đây.
        """
        return self._ingest_tx

    def _drain_external_txs(self):
        """
        SimPy process: rót giao dịch từ buffer vào sink (_ingest_tx) theo thời gian mô phỏng.
        """
        while True:
            # đảm bảo sink tồn tại
            if self._ext_tx_sink is None:
                self._ext_tx_sink = self._resolve_internal_tx_sink()

            # rót hết có thể
            while self._ext_tx_buffer and self._ext_tx_sink is not None:
                tx = self._ext_tx_buffer.popleft()
                try:
                    self._ext_tx_sink(tx)
                except Exception:
                    # nếu lỗi tạm, để lại đầu queue và chờ nhịp sau
                    self._ext_tx_buffer.appendleft(tx)
                    break

            # nghỉ 0.05s thời gian mô phỏng
            yield self.env.timeout(0.05)

    # ------------------------------------------------------------------
    # Zone processing (PBFT + block + sorting)
    # ------------------------------------------------------------------
    def process_zone(self, zid: str, txs: List[Dict], leader: Node, conflict_id: Optional[str]):
        # pre-processing
        yield self.env.timeout(self._sample_processing())
        zone = self.zones[zid]

        # PBFT
        success = yield self.env.process(
            run_pbft(self.env, zone, self.cfg.pbft, self.cfg.sys.malicious_drop_prob)
        )
        if not success:
            self.metrics['pbft_failure'] += 1
            return
        self.metrics['pbft_success'] += 1

        # Create block (replicate trên 2 shard của zone)
        zone.block_counter += 1
        bid = f"{zid}_b{zone.block_counter}_{int(self.env.now)}"
        for sid in zone.shards:
            shard = self.shards[sid]
            parent = random.choice(list(shard.tips)) if shard.tips else None
            nb = Block(bid, zid, parent, txs, zone.priority, self.env.now, conflict_id)
            shard.blocks[bid] = nb
            shard.tips.add(bid)   # giữ parent để tạo nhánh cạnh tranh (fork)

        # pre-sort DS count
        if conflict_id:
            s = self.conflict_map.setdefault(conflict_id, set()); s.add(bid)
            if len(s) == 2:
                self.metrics['pre_sort_double_spends'] += 1

        # Broadcast updates (mô phỏng mạng)
        for sid in zone.shards:
            recipients = [n for n in self.nodes if (sid in n.stored_shards)]
            for r in recipients:
                self.env.process(self._deliver_update(r, zone, bid))

        # Sorting (linearize theo priority + orphan nhánh thua)
        for sid in zone.shards:
            self.sorter.sort_and_linearize(sid)

        # Post-sort DS check (đo lường)
        if conflict_id and len(self.conflict_map.get(conflict_id, set())) >= 2:
            alive = 0
            for bid2 in self.conflict_map[conflict_id]:
                for sid in zone.shards:
                    shard = self.shards[sid]
                    if bid2 in shard.blocks and shard.blocks[bid2].confirmed:
                        alive += 1
                        break
            if alive >= 2:
                self.metrics['post_sort_double_spends'] += 1

    def _deliver_update(self, node: Node, zone: ConsensusZone, bid: str):
        d = self.net.sample_delay(
            src_id=zone.nodes[0].node_id if zone.nodes else None,
            dst_id=node.node_id
        )
        yield self.env.timeout(d)
        if self.net.should_drop():
            self._record_update(node, False)
            return

        on_time = (d <= self._deadline())
        self._record_update(node, on_time)

        # P-probability
        misses = sum(1 for x in node.update_history if x is False)
        if misses >= 4:
            self._quarantine(node)
            return

        self.metrics['proofs_required'] += 1
        if misses == 0: p = 0.0
        elif misses == 1: p = 0.25
        elif misses == 2: p = 0.50
        else: p = 1.0

        if random.random() < p:
            self.metrics['proofs_returned'] += 1

    # ------------------------------------------------------------------
    # CSV feed tại chỗ (tuỳ chọn) — nếu muốn nạp trực tiếp trong class
    # ------------------------------------------------------------------
    def feed_transactions_from_csv_local(self, csv_path="data/blockchain_transaction.csv",
                                         max_tx=None, sample_rate=1.0,
                                         inject_ds=False, ds_rate=0.001):
        """
        Stream CSV lớn → map TX vào zone/shard → tạo process xử lý.
        Không cần accept_tx(); đọc tới đâu bơm vào tới đó.
        """
        cnt = 0
        for tx in stream_transactions_from_csv(csv_path, chunksize=100_000,
                                               sample_rate=sample_rate, max_tx=max_tx):
            # nạp TX gốc
            self._ingest_tx(tx)

            # inject DS nhẹ (tuỳ chọn)
            if inject_ds and random.random() < ds_rate:
                ds_tx = dict(tx)
                ds_tx["to"] = f"{tx['to']}_ALT"
                ds_tx["conflict_id"] = f"csvds_{int(self.env.now)}_{random.randint(1000,9999)}"
                self._ingest_tx(ds_tx)

            cnt += 1
        print(f"[feed_csv_local] fed {cnt} original transactions (inject_ds={inject_ds})")

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------
    def run(self, duration=60):
        # Bật drain TX ngoài → trong (accept_tx)
        if not self._ext_drain_started:
            self.env.process(self._drain_external_txs())
            self._ext_drain_started = True

        # Kẻ tấn công DS định kỳ giữa hai zone có shard 0 (ví dụ)
        zkeys = [z for z in self.zones if z.startswith("0_") and z != "0_0"]
        if len(zkeys) >= 2:
            z1, z2 = random.sample(zkeys, 2)
            leader1, leader2 = self.zones[z1].nodes[0], self.zones[z2].nodes[0]

            def atk_proc(env, sim):
                while True:
                    yield env.timeout(self.cfg.atk.ds_interval)
                    sim.attacker_double_spend(z1, z2, leader1, leader2)

            self.env.process(atk_proc(self.env, self))

        self.env.run(until=duration)

    # ------------------------------------------------------------------
    # Attacker (tạo xung đột rõ ràng)
    # ------------------------------------------------------------------
    def attacker_double_spend(self, z1: str, z2: str, leader1: Node, leader2: Node):
        cid = f"ds_{int(self.env.now)}_{random.randint(1000,9999)}"
        tx1 = {'from':'A','to':'B','value':self.cfg.atk.tx_value,'conflict_id':cid}
        tx2 = {'from':'A','to':'C','value':self.cfg.atk.tx_value,'conflict_id':cid}
        self.env.process(self.process_zone(z1, [tx1], leader1, cid))
        self.env.process(self.process_zone(z2, [tx2], leader2, cid))

    # ------------------------------------------------------------------
    # Debug
    # ------------------------------------------------------------------
    def dump_metrics(self):
        for k, v in self.metrics.items():
            print(f"{k:30s} {v}")

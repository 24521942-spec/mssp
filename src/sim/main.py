from __future__ import annotations

"""
Mô phỏng MSSP cải tiến: lựa chọn shard cho m-nodes sử dụng hàm băm SHA-256 theo
mô tả trong bài báo. Cho phép người dùng cấu hình số shard mà mỗi m-node lưu trữ
(k) và số m-node trong mỗi shard (m). Khi tạo các nút, chúng ta chọn đúng m
nút trong mỗi shard làm m-node và dùng hàm băm để chọn thêm k-1 shard lưu trữ.

Các phần còn lại của mô phỏng (vùng đồng thuận, PBFT/HBBFT, sắp xếp ưu tiên,
P-probability, ...) được giữ nguyên như phiên bản gốc.
"""

import random
import hashlib
from collections import deque
from typing import Callable, Optional, Dict, List, Tuple

import simpy

# --- HBBFT deps (safe import) ---
try:
    from ..consensus.acs import ACS
    from ..consensus.tpke import tpke_encrypt, tpke_decrypt
except Exception:
    # Mock đơn giản để bạn chạy thử khi chưa viết consensus/TPKE
    class ACS:
        def __init__(self, env, node_ids, f, coin_seed):
            self.env = env
            self.node_ids = node_ids
            self.f = f
        def run_epoch(self, epoch, proposals):
            # giả lập: chấp nhận tất cả đề xuất
            class Res:
                accepted_sender_ids = list(proposals.keys())
                payloads = proposals
            def _proc():
                yield self.env.timeout(0.01)
                return Res()
            return _proc()
    def tpke_encrypt(batch): return batch
    def tpke_decrypt(cipher, shares=2): return cipher

from ..core.config import Config
from ..core.models import Shard, Node, ConsensusZone, Block
from ..core.network import Network
from ..core.pbft import run_pbft
from ..core.sorting import PrioritySorter
from ..core.metrics import Metrics
from ..core.workload import addr_to_shard
from ..io.tx_reader import stream_transactions_from_csv


class MSSPSim:
    """Mô phỏng giao thức MSSP với lựa chọn multi-shard tùy chỉnh."""

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

        # Các tham số cấu hình multi-shard
        k = max(1, cfg.sys.mnode_shard_count)  # số shard mỗi m-node lưu
        # Xác định số m-node trong mỗi shard (m). Nếu thiết lập rõ, dùng giá trị đó.
        if cfg.sys.mnode_count_per_shard and cfg.sys.mnode_count_per_shard > 0:
            m_per_shard = cfg.sys.mnode_count_per_shard
        else:
            # fallback: tính m dựa trên tỉ lệ mnode_fraction
            m_per_shard = int(round(cfg.sys.nodes_per_shard * cfg.sys.mnode_fraction))
        # đảm bảo m không vượt quá nodes_per_shard và tối thiểu là 0
        m_per_shard = max(0, min(m_per_shard, cfg.sys.nodes_per_shard))

        # Khởi tạo node cho từng shard
        for shard_id in range(cfg.sys.shards):
            # Danh sách local index 0..nodes_per_shard-1
            local_indices = list(range(cfg.sys.nodes_per_shard))
            # Chọn ngẫu nhiên m_per_shard index làm m-node
            if m_per_shard > 0:
                mnode_indices = set(random.sample(local_indices, m_per_shard))
            else:
                mnode_indices = set()
            # Tạo node từng local index
            for local_idx in local_indices:
                nid = shard_id * cfg.sys.nodes_per_shard + local_idx
                is_bad = (random.random() < cfg.sys.malicious_fraction)
                if local_idx in mnode_indices:
                    # m-node: chọn nhiều shard lưu trữ bằng hàm băm
                    stored_shards = self._select_multi_shards(nid, shard_id, k)
                else:
                    # s-node: chỉ lưu shard hiện tại
                    stored_shards = {shard_id}
                self.nodes.append(Node(self.env, nid, stored_shards, is_bad))

        # --- Zones (consensus zones) ---
        # Trong MSSP, một consensus zone được xác định bởi tập hợp các shard mà các nút lưu trữ.
        # Mỗi tập shard duy nhất trong mạng tạo ra một zone, bao gồm tất cả các nút có cùng tập shard.
        # ID của zone là danh sách shard được sắp xếp và nối với '_', ví dụ: '0', '0_1', '0_1_2', ...
        self.zones: Dict[str, ConsensusZone] = {}
        # Gom nhóm nút theo tập shard của chúng
        shard_set_to_nodes: Dict[Tuple[int, ...], List[Node]] = {}
        for n in self.nodes:
            # Sắp xếp để đảm bảo key nhất quán
            key = tuple(sorted(n.stored_shards))
            shard_set_to_nodes.setdefault(key, []).append(n)
        # Tạo zone cho mỗi tập shard duy nhất
        for shard_key, z_nodes in shard_set_to_nodes.items():
            # Tạo ID bằng cách nối các shard ID bằng '_'
            zid = "_".join(str(sid) for sid in shard_key)
            # Danh sách shard của zone là list(shard_key)
            zone_shards = list(shard_key)
            # Gán một mức ưu tiên ngẫu nhiên ban đầu (có thể được điều chỉnh sau)
            self.zones[zid] = ConsensusZone(
                self.env, zid, zone_shards, z_nodes,
                priority=random.randint(4, 7)
            )

        # --- Network ---
        self.net = Network(cfg.net.mean_delay, cfg.net.jitter, cfg.net.drop_prob, cfg.net.use_topology)
        if cfg.net.use_topology:
            self.net.build_random_topology([n.node_id for n in self.nodes], avg_degree=3)

        # --- Metrics & conflict map ---
        self.metrics = Metrics().__dict__  # các thành phần khác dùng dict[...]
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

    def _compute_f(self, zone_nodes: int) -> int:
        return max(1, (zone_nodes - 1) // 3)

    def _select_multi_shards(self, node_id: int, primary_shard: int, k: int) -> set:
        """
        Chọn k shard cho một m-node theo thuật toán 1 trong bài MSSP.

        Tham số:
        - node_id: ID duy nhất của nút, đóng vai trò A trong hàm băm.
        - primary_shard: shard gốc mà nút thuộc về.
        - k: số shard tối đa mà m-node lưu.

        Trả về: tập hợp k shard ID mà nút lưu trữ (bao gồm shard gốc).
        """
        stored = {primary_shard}
        A = str(node_id)
        # Khởi tạo R ngẫu nhiên
        R = random.getrandbits(32)
        # Lặp cho tới khi đạt k shard
        while len(stored) < k:
            # Tính j = TRUNCATE(SHA256(A‖R), 5)
            data = (A + str(R)).encode()
            digest = hashlib.sha256(data).digest()
            first_byte = digest[0]
            j_val = (first_byte >> 3)  # lấy 5 bit cao nhất
            # Điều chỉnh vào phạm vi shard hiện có
            shard_idx = j_val % self.cfg.sys.shards
            if shard_idx not in stored:
                stored.add(shard_idx)
            # cập nhật R để tránh lặp lại giá trị băm cũ
            R = random.getrandbits(32)
        return stored

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
        s_to = addr_to_shard(str(tx['to']), self.cfg.sys.shards)
        # Xác định zone cho giao dịch dựa trên hai shard từ/to
        # Tạo ID cặp shard bằng cách sắp xếp và nối bằng '_'
        pair_shards = sorted({s_from, s_to})
        zid = "_".join(str(sid) for sid in pair_shards)
        zone = self.zones.get(zid)
        # Nếu không tồn tại zone chính xác cho cặp này, tìm zone tối thiểu chứa cả hai shard
        if zone is None or not zone.nodes:
            # Tìm các zone có tập shard bao gồm cả hai shard
            candidate_zones = [z for z in self.zones.values() if set(pair_shards).issubset(set(z.shards)) and z.nodes]
            if not candidate_zones:
                return
            # Chọn zone có số shard nhỏ nhất (ưu tiên gần với cặp nhất)
            candidate_zones.sort(key=lambda z: len(z.shards))
            zone = candidate_zones[0]
            # Cập nhật zid theo ID của zone thực sự
            # Tìm khóa phù hợp trong self.zones để sử dụng làm zid
        # Tìm ID thực của zone
        # Từ đối tượng zone đã chọn, tìm ra key tương ứng trong self.zones
        zone_id = None
        for key, z in self.zones.items():
            if z is zone:
                zone_id = key
                break
        if zone_id is None:
            return
        leader = zone.nodes[0]
        cid = tx.get('conflict_id')
        # Một TX → một block ở zone, block có thể được replicate lên các shard của zone
        self.env.process(self.process_zone(zone_id, [dict(tx)], leader, cid))

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
        if getattr(self.cfg, "consensus", None) and self.cfg.consensus.mode == "hbbft":
            zone = self.zones[zid]
            return self.env.process(self._process_zone_hbbft(zone, txs, leader, conflict_id))
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
        if misses == 0:
            p = 0.0
        elif misses == 1:
            p = 0.25
        elif misses == 2:
            p = 0.50
        else:
            p = 1.0

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
            # 1️⃣ Nạp TX gốc vào hệ thống
            self._ingest_tx(tx)

            # 2️⃣ (Tuỳ chọn) Tiêm Double-Spend (DS) chéo-zone
            if inject_ds and random.random() < ds_rate:
                s_from = addr_to_shard(str(tx['from']), self.cfg.sys.shards)
                s_to = addr_to_shard(str(tx['to']), self.cfg.sys.shards)
                s_to2 = (s_to + 1) % self.cfg.sys.shards  # ép sang shard khác

                zid1 = f"{min(s_from, s_to)}_{max(s_from, s_to)}"
                zid2 = f"{min(s_from, s_to2)}_{max(s_from, s_to2)}"

                if zid1 in self.zones and zid2 in self.zones and \
                   self.zones[zid1].nodes and self.zones[zid2].nodes:

                    leader1 = self.zones[zid1].nodes[0]
                    leader2 = self.zones[zid2].nodes[0]

                    cid = f"csvds_{int(self.env.now)}_{random.randint(1000,9999)}"
                    tx1 = {
                        'from': tx['from'],
                        'to': tx['to'],
                        'value': tx.get('value', 1),
                        'conflict_id': cid
                    }
                    alt_to = f"{tx['to']}_ALT_{random.randint(100,999)}"
                    tx2 = {
                        'from': tx['from'],
                        'to': alt_to,
                        'value': tx.get('value', 1),
                        'conflict_id': cid
                    }

                    # Bắn hai TX xung đột vào hai zone khác nhau
                    self.env.process(self.process_zone(zid1, [tx1], leader1, cid))
                    self.env.process(self.process_zone(zid2, [tx2], leader2, cid))

            cnt += 1  # ✅ phải nằm trong vòng for

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
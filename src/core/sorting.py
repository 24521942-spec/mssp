# src/core/sorting.py
from typing import Tuple, List, Optional, Dict, Set
from .models import Shard, ConsensusZone
from .config import SortingConfig

class PrioritySorter:
    """
    Priority Sorting + Auto-Tuning (theo tinh thần Algorithm 2):
      - Chọn tip có zone.priority cao nhất (tie-break theo timestamp).
      - Confirm đường tip → genesis.
      - Orphan các tip khác.
      - Auto-tune: sau mỗi confirm -> giảm dần; cuối chu kỳ nếu >= required_confirms -> reset về max.
    BẢN NÂNG CẤP:
      - Ràng buộc xung đột liên-shard: nếu 1 block có conflict_id được confirm,
        thì orphan tất cả block còn lại có cùng conflict_id ở mọi shard.
    """

    def __init__(self, shards: Dict[int, Shard], zones: Dict[str, ConsensusZone],
                 cfg: SortingConfig, metrics: dict, conflict_map: Dict[str, Set[str]]):
        self.shards = shards
        self.zones = zones
        self.cfg = cfg
        self.metrics = metrics
        self.conflict_map = conflict_map  # {conflict_id: set(block_ids)}

    def _pick_best_tip(self, shard: Shard) -> Optional[str]:
        best_tip, best_score, best_ts = None, -1, -1.0
        for tip_id in shard.tips:
            b = shard.blocks[tip_id]
            if b.zone_id is None:  # genesis
                continue
            z = self.zones.get(b.zone_id)
            score = z.priority if z else 0
            ts = b.timestamp
            if (score > best_score) or (score == best_score and ts > best_ts):
                best_tip, best_score, best_ts = tip_id, score, ts
        return best_tip

    def sort_and_linearize(self, shard_id: int) -> Tuple[List[str], List[str]]:
        shard = self.shards[shard_id]
        if not shard.tips:
            return [], []

        best_tip = self._pick_best_tip(shard)
        if best_tip is None:
            return [], []

        # 1) Confirm path from best_tip back to genesis
        confirm_ids = []
        cur = best_tip
        visited = set()
        while cur and cur != f"g_{shard.shard_id}" and cur in shard.blocks:
            if cur in visited:
                break
            visited.add(cur)
            b = shard.blocks[cur]
            if not b.confirmed:
                b.confirmed = True
                confirm_ids.append(cur)
                # Auto-tune cho zone của block
                if b.zone_id in self.zones:
                    self._auto_tune(self.zones[b.zone_id])
            cur = b.parent_id

        # 2) Orphan các tip khác trên shard hiện tại (tạo hiệu ứng "thắng nhánh")
        orphan_ids = []
        for tip_id in list(shard.tips):
            if tip_id == best_tip or tip_id.startswith("g_"):
                continue
            orphan_ids.append(tip_id)
        # Chỉ giữ lại best_tip làm tip
        shard.tips = {best_tip}

        # 3) Ràng buộc quyết định theo conflict_id trên TOÀN MẠNG
        #    Với mỗi block vừa confirm có conflict_id, orphan toàn bộ block khác cùng conflict_id
        #    ở tất cả shards.
        for bid in confirm_ids:
            b = shard.blocks[bid]
            cid = b.conflict_id
            if not cid:
                continue
            other_bids = self.conflict_map.get(cid, set())
            if not other_bids:
                continue
            # orphan các block khác cùng conflict (trừ block vừa confirm)
            for obid in list(other_bids):
                if obid == bid:
                    continue
                for sid, sh in self.shards.items():
                    if obid in sh.blocks:
                        # đánh dấu không còn là tip
                        if obid in sh.tips:
                            sh.tips.discard(obid)
                            self.metrics['orphaned_blocks'] += 1
                        # KHÔNG xoá block khỏi sh.blocks để vẫn giữ log, chỉ xem như "không được chọn"
                        sh.blocks[obid].confirmed = False

        # Cập nhật metrics
        self.metrics['orphaned_blocks'] += len(orphan_ids)
        self.metrics['confirmed_blocks'] += len(confirm_ids)
        return confirm_ids, orphan_ids

    def _auto_tune(self, zone: ConsensusZone):
        # decrease dần
        zone.priority = max(self.cfg.priority_min, zone.priority - 1)
        zone.cycle_confirms += 1
        zone.cycle_blocks += 1
        if zone.cycle_blocks >= self.cfg.cycle_blocks:
            if zone.cycle_confirms >= self.cfg.required_confirms:
                zone.priority = self.cfg.priority_max
            zone.cycle_blocks = 0
            zone.cycle_confirms = 0

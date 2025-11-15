from typing import Tuple, List, Optional, Dict, Set
from .models import Shard, ConsensusZone
from .config import SortingConfig


class PrioritySorter:
    """
    Priority Sorting + Auto-Tuning theo mô tả trong bài MSSP.
    Cơ chế này sắp xếp các nhánh (tips) trong blockchain dạng cây về lại một chuỗi tuyến tính:
    - Chọn tip có zone.priority cao nhất (tie-break theo timestamp).
    - Xác nhận các block trên đường từ tip đó về genesis.
    - Orphan các tip khác.
    - Tự điều chỉnh độ ưu tiên của zone dựa trên số block hợp lệ
      được xác nhận trong chu kỳ, thay vì chỉ giảm và reset ngẫu nhiên.
    - Nếu một block được xác nhận có conflict_id, orphan toàn bộ
      block khác cùng conflict_id trên tất cả các shard.
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
            # bỏ qua genesis
            if b.zone_id is None:
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
        #    Với mỗi block vừa confirm có conflict_id, orphan toàn bộ block còn lại cùng conflict_id
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
        """
        Điều chỉnh mức ưu tiên của consensus zone dựa trên số block hợp lệ
        được tạo ra trong mỗi chu kỳ, theo tinh thần mô tả trong bài báo MSSP.

        - Mỗi khi xác nhận một block, ưu tiên giảm 1 đơn vị (không xuống
          thấp hơn priority_min).
        - Thống kê số block xác nhận (cycle_confirms) và tổng số block xử lý
          (cycle_blocks) trong một chu kỳ (cycle_blocks).
        - Khi kết thúc chu kỳ, đặt lại ưu tiên của zone bằng số block xác nhận
          trong chu kỳ đó (giới hạn trong [priority_min, priority_max]). Điều
          này phản ánh đề xuất trong bài báo: độ ưu tiên của zone tỷ lệ với
          số block hợp lệ mà zone tạo ra trong một chu kỳ【421101812886637†L589-L592】.
        - Nếu số block xác nhận trong chu kỳ vượt ngưỡng required_confirms,
          ưu tiên được đặt về giá trị tối đa (priority_max), theo cơ chế
          auto-tune gốc【421101812886637†L625-L639】.
        - Sau khi cập nhật, reset cycle_blocks và cycle_confirms về 0 cho chu kỳ mới.
        """
        # Giảm ưu tiên mỗi lần xác nhận block, nhưng không thấp hơn priority_min
        if zone.priority > self.cfg.priority_min:
            zone.priority -= 1
            if zone.priority < self.cfg.priority_min:
                zone.priority = self.cfg.priority_min
        zone.cycle_confirms += 1
        zone.cycle_blocks += 1

        # Khi đạt tới số block trong một chu kỳ, cập nhật ưu tiên dựa trên
        # số block xác nhận và reset bộ đếm cho chu kỳ mới
        if zone.cycle_blocks >= self.cfg.cycle_blocks:
            # Lưu lại số block xác nhận trong chu kỳ hiện tại
            confirmed_in_cycle = zone.cycle_confirms
            # Tính ưu tiên mới dựa trên số block xác nhận trong chu kỳ,
            # được kẹp trong [priority_min, priority_max]
            new_priority = max(self.cfg.priority_min,
                               min(self.cfg.priority_max, confirmed_in_cycle))
            # Nếu xác nhận vượt ngưỡng required_confirms, đặt về max
            if confirmed_in_cycle >= self.cfg.required_confirms:
                zone.priority = self.cfg.priority_max
            else:
                zone.priority = new_priority
            # Bắt đầu chu kỳ mới
            zone.cycle_blocks = 0
            zone.cycle_confirms = 0
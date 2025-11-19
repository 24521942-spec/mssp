# src/consensus/honeybadger_zone.py
import random
import simpy
from typing import List, Dict, Any, Callable, Optional  # <-- thêm Optional

from .tpke import tpke_setup, tpke_encrypt, tpke_dec_share, tpke_decrypt
from .acs import ACS


class HoneyBadgerZone:
    """
    HoneyBadgerBFT cho 1 consensus zone trong MSSP.
    Giữ mempool riêng + trạng thái TPKE + ACS.

    Giao diện bên ngoài:
      - add_txs(list_tx)
      - run_epoch_if_ready() -> batch tx được commit (hoặc None nếu chưa đủ).
      - on_consensus_message(...) -> nhận message consensus từ MSSPSim
    """

    def __init__(self,
                 env: simpy.Environment,
                 node_ids: List[int],
                 f: int,
                 send_func: Callable[[int, int, str, Any], None],
                 coin_seed: int,
                 batch_size: int):

        self.env = env
        self.node_ids = node_ids
        self.f = f
        self.n = len(node_ids)
        self.send = send_func
        self.batch_size = batch_size

        # TPKE setup
        self.pk, self.sk_shares = tpke_setup(node_ids)

        # mempool đơn giản: list tx
        self.mempool: List[Dict] = []

        # seed cho CommonCoin / ABA / ACS
        self.coin_seed = coin_seed

        # ACS đang chạy cho epoch hiện tại (nếu None là không có epoch nào active)
        self._active_acs: Optional[ACS] = None

    def add_txs(self, txs: List[Dict]):
        self.mempool.extend(txs)

    def _select_proposals(self) -> Dict[int, List[Dict]]:
        """
        Mỗi node chọn ngẫu nhiên ~B/N tx từ B tx đầu tiên trong mempool (paper Section 4.3).
        Ở đây ta làm đơn giản: tất cả node cùng nhìn 1 mempool chia đều.
        """
        if len(self.mempool) < self.batch_size:
            return {}

        front = self.mempool[:self.batch_size]
        random.shuffle(front)
        per = max(1, self.batch_size // self.n)

        proposals: Dict[int, List[Dict]] = {}
        idx = 0
        for nid in self.node_ids:
            proposals[nid] = front[idx: idx + per]
            idx += per
        return proposals

    def run_epoch_if_ready(self):
        """
        Nếu mempool đủ B tx thì chạy 1 epoch HBBFT, trả về batch tx commit.
        Nếu chưa đủ thì return None.

        CHÚ Ý:
        - Hàm này sẽ:
          + tạo 1 ACS mới,
          + lưu vào self._active_acs,
          + chạy ACS.run_epoch(),
          + sau khi xong thì self._active_acs = None.
        - Trong thời gian ACS đang chạy, mọi message consensus đi qua network
          sẽ được MSSPSim gọi vào on_consensus_message() để self._active_acs xử lý.
        """
        proposals = self._select_proposals()
        if not proposals:
            return None

        # 1. Encrypt
        encrypted: Dict[int, bytes] = {}
        for nid, plist in proposals.items():
            ct = tpke_encrypt(self.pk, plist)
            encrypted[nid] = ct

        # 2. Tạo ACS mới cho epoch này và GÁN vào _active_acs
        self._active_acs = ACS(
            self.env,
            node_ids=self.node_ids,
            f=self.f,
            send_func=self.send,
            coin_seed=self.coin_seed
        )

        # Các node "propose" ciphertext vào ACS (từ góc nhìn controller)
        for nid, ct in encrypted.items():
            self._active_acs.propose(nid, ct)

        # 3. Chạy epoch ACS (blocking theo SimPy)
        chosen = yield self.env.process(self._active_acs.run_epoch())

        # Epoch kết thúc -> clear active_acs
        self._active_acs = None

        # 4. Decrypt các ciphertext được chấp nhận
        combined: List[Dict] = []
        for sid, ct in chosen.items():
            if ct is None:
                continue

            # thu thập share từ tất cả node (ở mô phỏng: từ sk_shares)
            shares = []
            for nid in self.node_ids:
                share = tpke_dec_share(self.sk_shares[nid], ct)
                shares.append(share)
            try:
                decoded_list = tpke_decrypt(self.pk, shares)
            except Exception:
                continue

            if isinstance(decoded_list, list):
                combined.extend(decoded_list)
            else:
                combined.append(decoded_list)

        # 5. Xoá các tx đã commit khỏi mempool
        committed_ids = {id(tx) for tx in combined}
        self.mempool = [tx for tx in self.mempool if id(tx) not in committed_ids]

        return combined

    def on_consensus_message(self,
                             src_id: int,
                             dst_id: int,
                             msg_type: str,
                             payload: Any):
        """
        Hàm này được MSSPSim gọi khi 1 message consensus (RBC/ABA/COIN/ACS)
        tới zone sau khi đi qua network.

        Nhiệm vụ: forward message đó cho ACS đang active (self._active_acs).
        """
        if self._active_acs is None:
            # Không có epoch nào đang chạy -> bỏ message
            return

        # chuyển tiếp vào ACS để nó route tới RBC/ABA/COIN tương ứng
        self._active_acs.handle_message(src_id, dst_id, msg_type, payload)

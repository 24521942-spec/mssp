#!/usr/bin/env python3
"""
Chạy mô phỏng MSSP với cơ chế đồng thuận HoneyBadgerBFT (HBBFT).

- Đặt consensus.mode = "hbbft"
- Tạo MSSPSim
- (Tuỳ chọn) nạp giao dịch từ CSV
- Chạy mô phỏng trong một khoảng thời gian
- In ra metrics để phục vụ phân tích / vẽ đồ thị

Cách chạy (từ thư mục gốc repo):

    python run_hbbft.py
hoặc:

    python run_hbbft.py --duration 60 --max-tx 5000 --inject-ds

"""

import os
import sys
import argparse

# Đảm bảo có thể import được gói src.*
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from src.core.config import Config
from src.sim.main import MSSPSim


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run MSSP simulation with HoneyBadgerBFT consensus."
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=60.0,
        help="Thời gian mô phỏng (đơn vị: thời gian SimPy). Mặc định: 60.",
    )
    parser.add_argument(
        "--csv-path",
        type=str,
        default="data/blockchain_transaction.csv",
        help="Đường dẫn file CSV giao dịch. Mặc định: data/blockchain_transaction.csv",
    )
    parser.add_argument(
        "--max-tx",
        type=int,
        default=5000,
        help="Số lượng giao dịch tối đa nạp từ CSV (None = tất cả). Mặc định: 5000.",
    )
    parser.add_argument(
        "--sample-rate",
        type=float,
        default=1.0,
        help="Tỉ lệ sampling khi nạp CSV (0–1). 1.0 = lấy hết. Mặc định: 1.0.",
    )
    parser.add_argument(
        "--inject-ds",
        action="store_true",
        help="Nếu bật, sẽ tiêm thêm các giao dịch double-spend để kiểm tra an toàn.",
    )
    parser.add_argument(
        "--ds-rate",
        type=float,
        default=0.001,
        help="Xác suất tiêm double-spend trên mỗi giao dịch khi --inject-ds. Mặc định: 0.001.",
    )
    return parser


def build_config() -> Config:
    """
    Khởi tạo đối tượng Config mặc định rồi chỉnh lại các tham số cần thiết
    cho thí nghiệm HBBFT.
    """
    cfg = Config()

    # Bật HBBFT thay vì PBFT
    cfg.consensus.mode = "hbbft"

    # Một số tham số bạn có thể muốn điều chỉnh cho thí nghiệm:
    # (có thể sửa thêm tuỳ ý, đây chỉ là gợi ý hợp lý)
    cfg.sys.shards = 4
    cfg.sys.nodes_per_shard = 8
    cfg.sys.malicious_fraction = 0.2
    cfg.sys.malicious_drop_prob = 0.4
    cfg.sys.mnode_shard_count = 2       # k trong MSSP
    cfg.sys.mnode_count_per_shard = 0   # 0 = dùng tỉ lệ mnode_fraction

    # Tham số mạng
    cfg.net.mean_delay = 0.5
    cfg.net.jitter = 0.2
    cfg.net.drop_prob = 0.0
    cfg.net.use_topology = True

    # Tham số HBBFT (coin seed cho ABA)
    # Nếu trong ConsensusConfig bạn chưa thêm field hbbft_coin_seed, hàm
    # _process_zone_hbbft đã fallback về sys.random_seed rồi, nên không bắt buộc.
    try:
        cfg.consensus.hbbft_coin_seed = 2025
    except AttributeError:
        # Nếu chưa có field này trong ConsensusConfig thì bỏ qua.
        pass

    # Tham số cho tấn công double-spend định kỳ
    cfg.atk.ds_interval = 3.0
    cfg.atk.tx_value = 1

    # Tham số sorting
    cfg.sort.priority_max = 10
    cfg.sort.priority_min = 0
    cfg.sort.required_confirms = 2
    cfg.sort.cycle_blocks = 10

    return cfg


def main():
    parser = build_arg_parser()
    args = parser.parse_args()

    cfg = build_config()
    sim = MSSPSim(cfg)

    # 1) Nạp giao dịch từ CSV vào simulator (có thể bỏ nếu bạn tự generate TX)
    if args.max_tx <= 0:
        max_tx = None
    else:
        max_tx = args.max_tx

    print(f"[run_hbbft] feeding transactions from CSV: {args.csv_path}")
    sim.feed_transactions_from_csv_local(
        csv_path=args.csv_path,
        max_tx=max_tx,
        sample_rate=args.sample_rate,
        inject_ds=args.inject_ds,
        ds_rate=args.ds_rate,
    )

    # 2) Chạy mô phỏng
    print(f"[run_hbbft] running simulation (mode=HBBFT) for duration={args.duration}")
    sim.run(duration=args.duration)

    # 3) In metrics (PBFT/HBBFT, double-spend, orphaned blocks, v.v.)
    print("\n=== METRICS (HBBFT) ===")
    sim.dump_metrics()


if __name__ == "__main__":
    main()

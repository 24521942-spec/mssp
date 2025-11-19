#!/usr/bin/env python3
"""
run_ds_experiments.py

So sánh PBFT vs HBBFT về khả năng chống double-spending trong MSSP.

Chạy:
    python run_ds_experiments.py

Kết quả được ghi vào: ds_results.csv
"""

import os
import sys
import csv
from typing import Dict, Any, List

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from src.core.config import Config
from src.sim.main import MSSPSim

# chạy 5 seed để có trung bình
SEEDS = [1, 2, 3, 4, 5]

# thời gian mô phỏng & nguồn dữ liệu
DURATION = 60.0
MAX_TX = 5000
CSV_PATH = "data/blockchain_transaction.csv"

# file kết quả
RESULTS_PATH = "ds_results.csv"


def build_base_config() -> Config:
    """
    Tạo cấu hình MSSP cơ bản. Các scenario sẽ override lên.
    """
    cfg = Config()

    # MSSP system
    cfg.sys.shards = 4
    cfg.sys.nodes_per_shard = 8
    cfg.sys.malicious_fraction = 0.2
    cfg.sys.malicious_drop_prob = 0.4
    cfg.sys.mnode_shard_count = 2
    cfg.sys.mnode_count_per_shard = 0  # 0 = dùng mnode_fraction

    # Network (mặc định tốt)
    cfg.net.mean_delay = 0.5
    cfg.net.jitter = 0.2
    cfg.net.drop_prob = 0.0
    cfg.net.use_topology = True

    # Attack / DS định kỳ
    cfg.atk.ds_interval = 3.0
    cfg.atk.tx_value = 1

    # Sorting
    cfg.sort.priority_max = 10
    cfg.sort.priority_min = 0
    cfg.sort.required_confirms = 2
    cfg.sort.cycle_blocks = 10

    return cfg


def adjust_config_for_hbbft(cfg: Config) -> None:
    """
    Đảm bảo điều kiện n >= 3f + 1 cho HBBFT (theo lý thuyết HoneyBadgerBFT).
    """
    f = int(cfg.sys.nodes_per_shard * cfg.sys.malicious_fraction)
    if cfg.sys.malicious_fraction > 0 and f == 0:
        f = 1
    min_n = 3 * f + 1
    if cfg.sys.nodes_per_shard < min_n:
        cfg.sys.nodes_per_shard = min_n

    # Nếu có tham số batch_size cho HBBFT thì chỉnh lại cho hợp lý
    try:
        cfg.consensus.hbbft_batch_size = max(16, cfg.sys.nodes_per_shard * 2)
    except AttributeError:
        # nếu không có field này thì bỏ qua
        pass


def run_single_sim(
    mode: str,
    scenario_id: str,
    seed: int,
    cfg_overrides: Dict[str, Any],
    inject_ds: bool,
    ds_rate: float,
) -> Dict[str, Any]:
    """
    Chạy 1 lần mô phỏng với:
      - mode: 'pbft' hoặc 'hbbft'
      - scenario_id: nhãn scenario (vd 'S1_baseline', 'S3_heavy_byz')
      - seed: random seed
      - cfg_overrides: dict override lên Config
      - inject_ds / ds_rate: cấu hình tấn công double-spend

    Trả về 1 dict (sẽ ghi vào CSV).
    """
    cfg = build_base_config()
    cfg.sys.random_seed = seed
    cfg.consensus.mode = mode

    # áp dụng override kiểu "net.mean_delay", "sys.malicious_fraction", ...
    for key, value in cfg_overrides.items():
        parts = key.split(".")
        obj = cfg
        for attr in parts[:-1]:
            obj = getattr(obj, attr)
        setattr(obj, parts[-1], value)

    # nếu là HBBFT thì chỉnh lại n >= 3f+1
    if mode.lower() == "hbbft":
        adjust_config_for_hbbft(cfg)

    sim = MSSPSim(cfg)

    print(
        f"[{scenario_id}][{mode}][seed={seed}] "
        f"feeding TX from {CSV_PATH} (max_tx={MAX_TX}, inject_ds={inject_ds}, ds_rate={ds_rate})"
    )

    max_tx = None if (MAX_TX is None or MAX_TX <= 0) else MAX_TX

    # nạp giao dịch + tiêm double-spend (nếu có)
    sim.feed_transactions_from_csv_local(
        csv_path=CSV_PATH,
        max_tx=max_tx,
        sample_rate=1.0,
        inject_ds=inject_ds,
        ds_rate=ds_rate,
    )

    print(
        f"[{scenario_id}][{mode}][seed={seed}] "
        f"running simulation for duration={DURATION}"
    )
    sim.run(duration=DURATION)

    metrics = sim.metrics.copy()

    # Thông tin config + attack
    row: Dict[str, Any] = {
        "scenario": scenario_id,
        "mode": mode,
        "seed": seed,
        "duration": DURATION,
        "max_tx": MAX_TX,
        "csv_path": CSV_PATH,
        "sys.shards": cfg.sys.shards,
        "sys.nodes_per_shard": cfg.sys.nodes_per_shard,
        "sys.malicious_fraction": cfg.sys.malicious_fraction,
        "sys.malicious_drop_prob": cfg.sys.malicious_drop_prob,
        "net.mean_delay": cfg.net.mean_delay,
        "net.jitter": cfg.net.jitter,
        "net.drop_prob": cfg.net.drop_prob,
        "net.use_topology": cfg.net.use_topology,
        "inject_ds": inject_ds,
        "ds_rate": ds_rate,
    }

    # Gộp metrics.* (ví dụ:
    #  metrics.pre_sort_double_spends
    #  metrics.post_sort_double_spends
    #  metrics.pbft_success / metrics.hbbft_success, v.v.)
    for k, v in metrics.items():
        row[f"metrics.{k}"] = v

    return row


def append_rows_to_csv(path: str, rows: List[Dict[str, Any]]):
    """
    Ghi list dict vào CSV. Nếu file chưa có thì thêm header.
    """
    if not rows:
        return

    file_exists = os.path.exists(path)
    all_keys = set()
    for r in rows:
        all_keys.update(r.keys())
    fieldnames = sorted(all_keys)

    with open(path, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for r in rows:
            writer.writerow(r)


def main():
    all_rows: List[Dict[str, Any]] = []

    # ===== S1: baseline – không Byzantine, không double-spend, mạng tốt =====
    cfg_s1 = {
        "net.mean_delay": 0.5,
        "net.jitter": 0.2,
        "net.drop_prob": 0.0,
        "sys.malicious_fraction": 0.0,
    }
    for mode in ["pbft", "hbbft"]:
        for seed in SEEDS:
            row = run_single_sim(
                mode=mode,
                scenario_id="S1_baseline",
                seed=seed,
                cfg_overrides=cfg_s1,
                inject_ds=False,
                ds_rate=0.0,
            )
            all_rows.append(row)

    # ===== S2: Byzantine nhẹ + DS rate thấp =====
    cfg_s2 = {
        "net.mean_delay": 1.0,
        "net.jitter": 0.5,
        "net.drop_prob": 0.05,
        "sys.malicious_fraction": 0.1,
    }
    for mode in ["pbft", "hbbft"]:
        for seed in SEEDS:
            row = run_single_sim(
                mode=mode,
                scenario_id="S2_light_byz",
                seed=seed,
                cfg_overrides=cfg_s2,
                inject_ds=True,
                ds_rate=0.001,
            )
            all_rows.append(row)

    # ===== S3: Byzantine nặng + DS rate cao =====
    cfg_s3 = {
        "net.mean_delay": 1.0,
        "net.jitter": 0.5,
        "net.drop_prob": 0.05,
        "sys.malicious_fraction": 0.2,
    }
    for mode in ["pbft", "hbbft"]:
        for seed in SEEDS:
            row = run_single_sim(
                mode=mode,
                scenario_id="S3_heavy_byz",
                seed=seed,
                cfg_overrides=cfg_s3,
                inject_ds=True,
                ds_rate=0.005,
            )
            all_rows.append(row)

    # ===== S4: mạng rất xấu + Byzantine trung bình =====
    cfg_s4 = {
        "net.mean_delay": 3.0,
        "net.jitter": 1.5,
        "net.drop_prob": 0.2,
        "sys.malicious_fraction": 0.2,
    }
    for mode in ["pbft", "hbbft"]:
        for seed in SEEDS:
            row = run_single_sim(
                mode=mode,
                scenario_id="S4_breakdown",
                seed=seed,
                cfg_overrides=cfg_s4,
                inject_ds=True,
                ds_rate=0.005,
            )
            all_rows.append(row)

    append_rows_to_csv(RESULTS_PATH, all_rows)
    print(f"[DONE] Đã ghi {len(all_rows)} dòng vào {RESULTS_PATH}")


if __name__ == "__main__":
    main()

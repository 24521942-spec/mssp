#!/usr/bin/env python3
"""
Chạy batch thí nghiệm MSSP với PBFT vs HBBFT.

Scenario:
    - S1_baseline : mạng tốt, không inject double-spend
    - S2_A..D     : network stress (tăng delay, jitter, drop)
    - S3_E..H     : Byzantine + double-spend stress
    - S4_1..2     : breakdown (mạng cực xấu, để lộ điểm mạnh HBBFT)

Kết quả được append vào file CSV: results.csv

Chạy từ thư mục gốc repo:

    python run_batch_experiments.py
"""

import os
import sys
import csv
from typing import Dict, Any, List

# Bảo đảm import được src.*
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from src.core.config import Config
from src.sim.main import MSSPSim


# ===== CẤU HÌNH CHUNG =====

# Seed để lặp lại thí nghiệm (nên >= 5, có thể tăng 10, 20 nếu muốn)
SEEDS = [1, 2, 3, 4, 5]

# Thời gian mô phỏng & số TX nạp từ CSV
DURATION = 60.0
MAX_TX = 5000
CSV_PATH = "data/blockchain_transaction.csv"

# File kết quả
RESULTS_PATH = "results.csv"


def build_base_config() -> Config:
    """
    Tạo Config với các tham số MSSP cơ bản.
    Các scenario bên dưới sẽ override lên các field này.
    """
    cfg = Config()

    # MSSP system
    cfg.sys.shards = 4
    cfg.sys.nodes_per_shard = 8
    cfg.sys.malicious_fraction = 0.2
    cfg.sys.malicious_drop_prob = 0.4
    cfg.sys.mnode_shard_count = 2
    cfg.sys.mnode_count_per_shard = 0  # 0 = dùng mnode_fraction

    # Network (baseline)
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
      - scenario_id: nhãn scenario (vd 'S1_baseline', 'S2_B', 'S4_1')
      - seed: random_seed cho hệ thống
      - cfg_overrides: dict override lên Config (vd {'net.mean_delay': 2.0})
      - inject_ds, ds_rate: tham số tiêm double-spend khi nạp CSV

    Trả về: dict chứa thông tin config + metrics để ghi ra CSV.
    """
    cfg = build_base_config()

    # Gán seed
    cfg.sys.random_seed = seed

    # Chọn mode consensus
    cfg.consensus.mode = mode

    # Áp dụng override cho scenario
    # key dạng 'net.mean_delay', 'sys.malicious_fraction', ...
    for key, value in cfg_overrides.items():
        parts = key.split(".")
        obj = cfg
        for attr in parts[:-1]:
            obj = getattr(obj, attr)
        setattr(obj, parts[-1], value)

    # Khởi tạo simulator
    sim = MSSPSim(cfg)

    print(
        f"[{scenario_id}][{mode}][seed={seed}] "
        f"feeding TX from {CSV_PATH} (max_tx={MAX_TX}, inject_ds={inject_ds}, ds_rate={ds_rate})"
    )

    max_tx = None if (MAX_TX is None or MAX_TX <= 0) else MAX_TX

    # Nạp TX từ CSV vào simulator
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

    # Metrics (sim.metrics hiện đang là dict)
    metrics = sim.metrics.copy()

    # Gom thông tin chung
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

    # Thêm metrics.*
    for k, v in metrics.items():
        row[f"metrics.{k}"] = v

    return row


def append_rows_to_csv(path: str, rows: List[Dict[str, Any]]):
    """
    Append các dòng vào CSV. Nếu file chưa tồn tại thì ghi header trước.
    """
    if not rows:
        return

    file_exists = os.path.exists(path)
    fieldnames: List[str] = sorted(rows[0].keys())

    with open(path, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for r in rows:
            writer.writerow(r)


def main():
    all_rows: List[Dict[str, Any]] = []

    # ========= Scenario 1: Baseline =========
    # Mạng tương đối tốt, không inject double-spend
    scenario_id = "S1_baseline"
    cfg_overrides_s1 = {
        "net.mean_delay": 0.5,
        "net.jitter": 0.2,
        "net.drop_prob": 0.0,
        "sys.malicious_fraction": 0.2,
    }

    for mode in ["pbft", "hbbft"]:
        for seed in SEEDS:
            row = run_single_sim(
                mode=mode,
                scenario_id=scenario_id,
                seed=seed,
                cfg_overrides=cfg_overrides_s1,
                inject_ds=False,
                ds_rate=0.0,
            )
            all_rows.append(row)

    # ========= Scenario 2: Network stress =========
    # Tăng dần delay, jitter, drop_prob
    net_cases_s2 = [
        ("S2_A", 0.5, 0.2, 0.0),
        ("S2_B", 1.0, 0.5, 0.05),
        ("S2_C", 2.0, 1.0, 0.10),
        ("S2_D", 3.0, 1.5, 0.20),
    ]

    for scen, mean_delay, jitter, drop_prob in net_cases_s2:
        cfg_overrides_s2 = {
            "net.mean_delay": mean_delay,
            "net.jitter": jitter,
            "net.drop_prob": drop_prob,
            "sys.malicious_fraction": 0.2,
        }
        for mode in ["pbft", "hbbft"]:
            for seed in SEEDS:
                row = run_single_sim(
                    mode=mode,
                    scenario_id=scen,
                    seed=seed,
                    cfg_overrides=cfg_overrides_s2,
                    inject_ds=False,
                    ds_rate=0.0,
                )
                all_rows.append(row)

    # ========= Scenario 3: Byzantine & Double-spend stress =========
    mal_cases = [
        # (scenario, malicious_fraction, inject_ds, ds_rate)
        ("S3_E", 0.0, False, 0.0),
        ("S3_F", 0.1, True, 0.001),
        ("S3_G", 0.2, True, 0.005),
        ("S3_H", 0.33, True, 0.01),
    ]

    for scen, mal_frac, inject_ds, ds_rate in mal_cases:
        cfg_overrides_s3 = {
            "net.mean_delay": 1.0,   # mạng trung bình
            "net.jitter": 0.5,
            "net.drop_prob": 0.05,
            "sys.malicious_fraction": mal_frac,
        }
        for mode in ["pbft", "hbbft"]:
            for seed in SEEDS:
                row = run_single_sim(
                    mode=mode,
                    scenario_id=scen,
                    seed=seed,
                    cfg_overrides=cfg_overrides_s3,
                    inject_ds=inject_ds,
                    ds_rate=ds_rate,
                )
                all_rows.append(row)

    # ========= Scenario 4: Breakdown (network cực xấu) =========
    # Mục tiêu: đẩy PBFT đến vùng no-progress, HBBFT vẫn commit được
    net_cases_s4 = [
        ("S4_1", 4.0, 2.0, 0.20),
        ("S4_2", 6.0, 3.0, 0.30),
    ]

    for scen, mean_delay, jitter, drop_prob in net_cases_s4:
        cfg_overrides_s4 = {
            "net.mean_delay": mean_delay,
            "net.jitter": jitter,
            "net.drop_prob": drop_prob,
            "sys.malicious_fraction": 0.2,
        }
        for mode in ["pbft", "hbbft"]:
            for seed in SEEDS:
                row = run_single_sim(
                    mode=mode,
                    scenario_id=scen,
                    seed=seed,
                    cfg_overrides=cfg_overrides_s4,
                    inject_ds=True,   # ép DS mạnh hơn để stress
                    ds_rate=0.005,
                )
                all_rows.append(row)

    # ========= Ghi kết quả =========
    append_rows_to_csv(RESULTS_PATH, all_rows)
    print(f"[DONE] Đã ghi {len(all_rows)} dòng kết quả vào {RESULTS_PATH}")


if __name__ == "__main__":
    main()

from dataclasses import dataclass, field


@dataclass
class PBFTConfig:
    preprepare_mean: float = 0.10
    prepare_mean: float = 0.20
    commit_mean: float = 0.20
    timeout: float = 0.80     # timeout cho mỗi pha
    viewchange_cost_mean: float = 0.30
    max_view_changes: int = 3        # số lần quay vòng leader tối đa

class ConsensusConfig:
    mode: str = "pbft"               # lựa chọn cơ chế đồng thuận
    batch_size: int = 200             # số giao dịch trong một khối
    epoch_timeout: float = 0.0       # timeout cho mỗi epoch (sử dụng trong PoW/PoS)

    hbbft_max_parallel: int = 16   # số instance HBBFT song song (chỉ dùng khi mode="hbbft")
    hbbft_coin_seed: int = 2025          # seed cho đồng thuận ngẫu nhiên trong HBBFT
    hbbft_min_accepted: int = 1


@dataclass
class NetworkConfig:
    mean_delay: float = 0.50
    jitter: float = 0.20               # tỉ lệ jitter (±)
    drop_prob: float = 0.00            # xác suất rơi gói
    use_topology: bool = True          # nếu True: dùng graph để lấy latency theo cạnh

@dataclass
class SortingConfig:
    priority_min: int = 0
    priority_max: int = 10
    required_confirms: int = 2
    cycle_blocks: int = 10

@dataclass
class PProbConfig:
    history_window: int = 4
    deadline_factor: float = 1.0      # deadline = factor * network.mean_delay

@dataclass
class AttackConfig:
    ds_interval: float = 3.0          # khoảng giữa các đợt double-spend
    tx_value: int = 1

@dataclass
class SystemConfig:
    process_mean: float = 1.0         # CPU/logic delay trước PBFT
    malicious_fraction: float = 0.20
    malicious_drop_prob: float = 0.40 # xác suất node xấu bỏ phiếu/giấu vote
    shards: int = 4
    nodes_per_shard: int = 8
    mnode_fraction: float = 0.40      # tỉ lệ node lưu >= 2 shard (giữ lại để tương thích)
    random_seed: int = 2025

    # --- MSSP multi-shard configuration ---
    # Số shard tối đa mà mỗi m-node được phép lưu trữ (k trong bài báo). Mặc định = 2.
    mnode_shard_count: int = 2
    # Số lượng m-node trong mỗi shard (m trong bài báo). Nếu bằng 0, m sẽ được tính
    # theo mnode_fraction và nodes_per_shard (m = round(nodes_per_shard * mnode_fraction)).
    mnode_count_per_shard: int = 0


@dataclass
class Config:
    # Dùng default_factory để tránh mutable default error
    pbft: PBFTConfig = field(default_factory=PBFTConfig)
    net: NetworkConfig = field(default_factory=NetworkConfig)
    sort: SortingConfig = field(default_factory=SortingConfig)
    pprob: PProbConfig = field(default_factory=PProbConfig)
    atk: AttackConfig = field(default_factory=AttackConfig)
    sys: SystemConfig = field(default_factory=SystemConfig)
    consensus: ConsensusConfig = field(default_factory=ConsensusConfig)
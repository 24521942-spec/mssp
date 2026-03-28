# 🔗 MSSP — Multi-Shard Security Protocol Simulation

A discrete-event blockchain simulation for evaluating **double-spending attack resistance** in sharded environments, comparing **PBFT** and **HoneyBadgerBFT (HBBFT)** consensus under adversarial conditions.

> **Research focus:** Validate MSSP's fault-tolerance guarantees — specifically the Priority Sorter's ability to linearize cross-shard forks and reduce post-consensus double-spends to zero under Byzantine node conditions.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Running Experiments](#running-experiments)
- [Metrics & Output](#metrics--output)
- [Module Reference](#module-reference)

---

## Overview

MSSP (Multi-Shard Security Protocol) partitions a blockchain network into shards. Nodes are divided into:

| Node Type | Description |
|-----------|-------------|
| **s-node** | Standard node — stores data for exactly 1 shard |
| **m-node** | Multi-shard node — stores data for `k` shards, selected via SHA-256 hashing |

**Consensus Zones** are formed by grouping nodes that share the same shard-set. Each zone runs its own consensus round independently (PBFT or HBBFT), then the **Priority Sorter** linearizes competing forks across the entire network to detect and eliminate double-spend conflicts.

### What This Simulation Measures

| Metric | Description |
|--------|-------------|
| `pre_sort_double_spends` | Number of conflicting TX pairs before sorting |
| `post_sort_double_spends` | Surviving conflicts **after** the Priority Sorter runs |
| `pbft_success / pbft_failure` | PBFT consensus outcomes per zone |
| `hbbft_success / hbbft_failure` | HBBFT epoch outcomes per zone |
| `orphaned_blocks` | Losing-fork blocks discarded by the sorter |
| `confirmed_blocks` | Blocks accepted into the canonical chain |
| `faulty_nodes_quarantined` | Nodes removed via P-probability mechanism |

---

## Architecture

```
                    ┌──────────────────────────────────────┐
                    │             MSSPSim                  │
                    │                                      │
   CSV / Attacker ──► accept_tx() ──► _ingest_tx()        │
                    │        │                             │
                    │        ▼                             │
                    │   addr_to_shard()  ──►  Zone lookup  │
                    │                             │        │
                    │              ┌──────────────┘        │
                    │              ▼                       │
                    │    process_zone(zid, txs)            │
                    │         │            │               │
                    │    [PBFT mode]  [HBBFT mode]         │
                    │         │            │               │
                    │    run_pbft()   HoneyBadgerZone      │
                    │         │       ├── ACS              │
                    │         │       │   ├── RBC (Bracha) │
                    │         │       │   └── ABA          │
                    │         │       └── TPKE encrypt/dec │
                    │         │            │               │
                    │         └────────────┘               │
                    │                  │                   │
                    │           Block created              │
                    │                  │                   │
                    │        PrioritySorter                │
                    │   sort_and_linearize(shard_id)       │
                    │   ├── Pick highest-priority tip      │
                    │   ├── Confirm canonical chain        │
                    │   ├── Orphan competing forks         │
                    │   └── Cross-shard conflict detection │
                    │                  │                   │
                    │            Metrics update            │
                    └──────────────────────────────────────┘
```

### Consensus Flow (PBFT)

```
Leader ──► PRE-PREPARE ──► PREPARE (need 2f+1 votes)
                                  │
                         quorum? ─┴─ no ──► View-Change ──► retry (max N times)
                                  │
                                 yes
                                  │
                            COMMIT (need 2f+1 votes) ──► Block created
```

### Consensus Flow (HoneyBadgerBFT)

```
Mempool >= batch_size B transactions
        │
        ▼
TPKE Encrypt (each node encrypts its B/N proposal)
        │
        ▼
ACS (Atomic Common Subset)
  ├── RBC_i (Bracha Reliable Broadcast) × N
  └── ABA_i (Asynchronous Binary Agreement) × N
        │
        ▼
TPKE Decrypt (combine f+1 shares to recover proposals)
        │
        ▼
Block committed → Priority Sorter runs
```

---

## Project Structure

```
mssp_own/
│
├── run.py                        # Quick single run (PBFT, 200k TX)
├── run_pbft.py                   # Run PBFT with custom parameters
├── run_hbbft.py                  # Run HBBFT with CLI args
├── run_experiment.py             # Full comparative experiment (4 scenarios × 2 modes × 5 seeds)
│
├── data/
│   └── blockchain_transaction.csv  # Input transaction dataset (~1.7 GB)
│
├── ds_results.csv                # Output: experiment results (auto-generated)
│
└── src/
    ├── core/
    │   ├── config.py             # All configuration dataclasses
    │   ├── models.py             # Data models: Block, Shard, Node, ConsensusZone
    │   ├── metrics.py            # Metrics dataclass
    │   ├── network.py            # Network simulator (topology + latency + drop)
    │   ├── pbft.py               # PBFT 3-phase protocol with view-change
    │   ├── sorting.py            # PrioritySorter — linearization + auto-tuning
    │   └── workload.py           # Attacker process + address→shard mapping
    │
    ├── consensus/
    │   ├── honeybadger_zone.py   # HoneyBadgerBFT zone controller
    │   ├── acs.py                # Atomic Common Subset (ACS)
    │   ├── rbc.py                # Reliable Broadcast (Bracha algorithm)
    │   ├── aba.py                # Asynchronous Binary Agreement
    │   ├── coin.py               # Common Coin (shared randomness)
    │   └── tpke.py               # Threshold Public Key Encryption (simulated)
    │
    ├── io/
    │   └── tx_reader.py          # Streaming CSV reader + double-spend injector
    │
    ├── sim/
    │   └── main.py               # MSSPSim — top-level simulation orchestrator
    │
    └── experiments/
        └── analysis_plot.py      # Heatmap + bar chart generators (seaborn)
```

---

## Installation

### Requirements

- Python **3.9+**
- ~2 GB disk space for the transaction dataset

### Install dependencies

```bash
pip install simpy pandas networkx matplotlib seaborn
```

Or with a `requirements.txt`:

```bash
pip install -r requirements.txt
```

**`requirements.txt`:**
```
simpy>=4.0
pandas>=1.5
networkx>=2.8
matplotlib>=3.6
seaborn>=0.12
pyarrow          # optional but recommended — faster CSV parsing
```

### Dataset

Place the transaction CSV file at:
```
data/blockchain_transaction.csv
```

The file should have columns matching any of these names (auto-detected):

| Field | Accepted column names |
|-------|-----------------------|
| Transaction hash | `tx_hash`, `hash`, `txid`, `transaction_hash` |
| Sender | `from`, `sender`, `from_address` |
| Receiver | `to`, `receiver`, `to_address` |
| Amount | `value`, `amount` |
| Timestamp | `timestamp`, `time`, `block_time`, `datetime` |

---

## Quick Start

### Run a basic PBFT simulation

```bash
python run_pbft.py
```

This runs PBFT with 50,000 transactions, 1% double-spend injection rate, for 180 simulation time units.

### Run HBBFT simulation

```bash
python run_hbbft.py
```

With custom parameters:

```bash
python run_hbbft.py --duration 120 --max-tx 10000 --inject-ds --ds-rate 0.005
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--duration` | `60.0` | Simulation time (SimPy units) |
| `--csv-path` | `data/blockchain_transaction.csv` | Input CSV file |
| `--max-tx` | `5000` | Max transactions to load |
| `--sample-rate` | `1.0` | Fraction of CSV rows to use (0–1) |
| `--inject-ds` | off | Enable double-spend injection |
| `--ds-rate` | `0.001` | Probability of injecting DS per transaction |

### Run comparative experiments

```bash
python run_experiment.py
```

Runs 4 scenarios × 2 consensus modes (PBFT + HBBFT) × 5 seeds = **40 simulation runs**.
Results are saved to `ds_results.csv`.

---

## Configuration

All simulation parameters are defined in `src/core/config.py`.

### System Config (`cfg.sys`)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `shards` | `4` | Number of shards in the network |
| `nodes_per_shard` | `8` | Nodes assigned to each shard |
| `malicious_fraction` | `0.20` | Fraction of Byzantine nodes (must be < 1/3 for BFT safety) |
| `malicious_drop_prob` | `0.40` | Probability a Byzantine node drops its vote |
| `mnode_shard_count` | `2` | `k` — number of shards each m-node stores |
| `mnode_count_per_shard` | `0` | `m` — fixed m-node count per shard (0 = use `mnode_fraction`) |
| `mnode_fraction` | `0.40` | Fallback: fraction of nodes that become m-nodes |
| `random_seed` | `2025` | Global random seed for reproducibility |

### Consensus Config (`cfg.consensus`)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `mode` | `"pbft"` | Consensus algorithm: `"pbft"` or `"hbbft"` |
| `batch_size` | `200` | Transactions per block (PBFT) |
| `hbbft_max_parallel` | `16` | Max parallel HBBFT instances |
| `hbbft_coin_seed` | `2025` | Seed for Common Coin randomness |

### PBFT Config (`cfg.pbft`)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `timeout` | `0.80` | Phase timeout before view-change |
| `max_view_changes` | `3` | Max retries before declaring failure |
| `preprepare_mean` | `0.10` | Mean delay for PRE-PREPARE phase |
| `prepare_mean` | `0.20` | Mean delay for PREPARE phase |
| `commit_mean` | `0.20` | Mean delay for COMMIT phase |

### Network Config (`cfg.net`)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `mean_delay` | `0.50` | Base network latency |
| `jitter` | `0.20` | Latency variation (±fraction) |
| `drop_prob` | `0.00` | Packet drop probability |
| `use_topology` | `True` | Use graph topology for realistic latency |

### Sorting Config (`cfg.sort`)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `priority_min` | `0` | Minimum zone priority |
| `priority_max` | `10` | Maximum zone priority (reset target on high confirm rate) |
| `required_confirms` | `2` | Confirms per cycle to reset priority to max |
| `cycle_blocks` | `10` | Blocks per auto-tune cycle |

### Attack Config (`cfg.atk`)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ds_interval` | `3.0` | Interval between attacker double-spend injections |
| `tx_value` | `1` | Value of each conflicting transaction |

---

## Running Experiments

### Experiment Scenarios (`run_experiment.py`)

| Scenario | Byzantine fraction | Network | DS rate | Description |
|----------|--------------------|---------|---------|-------------|
| `S1_baseline` | 0% | Good (delay=0.5) | None | Clean network, no attacks |
| `S2_light_byz` | 10% | Moderate (delay=1.0) | 0.1% | Light Byzantine + occasional DS |
| `S3_heavy_byz` | 20% | Moderate (delay=1.0) | 0.5% | Heavy Byzantine + frequent DS |
| `S4_breakdown` | 20% | Poor (delay=3.0, drop=20%) | 0.5% | Network degradation stress test |

Each scenario runs 5 seeds per mode (PBFT + HBBFT) for statistical stability.

### Output CSV columns

```
scenario, mode, seed, duration, max_tx,
sys.shards, sys.nodes_per_shard, sys.malicious_fraction,
net.mean_delay, net.jitter, net.drop_prob,
inject_ds, ds_rate,
metrics.pre_sort_double_spends,
metrics.post_sort_double_spends,
metrics.pbft_success, metrics.pbft_failure,
metrics.hbbft_success, metrics.hbbft_failure,
metrics.orphaned_blocks, metrics.confirmed_blocks,
metrics.faulty_nodes_quarantined,
metrics.proofs_required, metrics.proofs_returned
```

### Generate plots

```bash
python src/experiments/analysis_plot.py
```

Produces:
- `heatmap_postds.png` — Post-sort double-spends vs (network delay × process delay)
- `pbft_bar.png` — PBFT success/failure rate vs Byzantine fraction

---

## Metrics & Output

### Reading results from `run_pbft.py` / `run_hbbft.py`

```
=== METRICS (HBBFT) ===
pre_sort_double_spends         42
post_sort_double_spends         0     ← sorter eliminated all conflicts
orphaned_blocks               38
confirmed_blocks             312
pbft_success                   0
pbft_failure                   0
hbbft_success                 28
hbbft_failure                  2
faulty_nodes_quarantined        3
proofs_required              150
proofs_returned               12
```

### Interpreting key metrics

| Metric | Good result | Bad result |
|--------|-------------|------------|
| `post_sort_double_spends` | `0` — sorter eliminated all conflicts | `> 0` — double-spend survived |
| `pbft_failure` | Low relative to `pbft_success` | High — Byzantine nodes blocking consensus |
| `faulty_nodes_quarantined` | Proportional to `malicious_fraction` | 0 — P-probability not triggering |
| `orphaned_blocks` | Present — indicates forks detected and resolved | 0 with DS — sorter may not be running |

---

## Module Reference

### `MSSPSim` (`src/sim/main.py`)

The main simulation orchestrator.

```python
from src.core.config import Config
from src.sim.main import MSSPSim

cfg = Config()
cfg.consensus.mode = "pbft"     # or "hbbft"
cfg.sys.shards = 4
cfg.sys.malicious_fraction = 0.2

sim = MSSPSim(cfg)

# Feed transactions from CSV
sim.feed_transactions_from_csv_local(
    "data/blockchain_transaction.csv",
    max_tx=50_000,
    sample_rate=1.0,
    inject_ds=True,
    ds_rate=0.01
)

# Or inject single transactions directly
sim.accept_tx({"from": "addr_A", "to": "addr_B", "value": "1"})

# Run simulation
sim.run(duration=60)

# Print results
sim.dump_metrics()
print(sim.metrics)
```

### `PrioritySorter` (`src/core/sorting.py`)

Linearizes competing forks using zone priority scores. Auto-tunes priority based on confirmed-block rate per cycle.

- Selects the highest-priority tip per shard
- Confirms the canonical chain back to genesis
- Orphans all competing tips
- Detects cross-shard conflicts via `conflict_map` and orphans duplicates globally

### `HoneyBadgerZone` (`src/consensus/honeybadger_zone.py`)

Manages one HBBFT consensus zone:
1. Accumulates transactions in a mempool
2. When `mempool >= batch_size`: encrypts proposals with TPKE, runs ACS
3. ACS coordinates RBC (Bracha reliable broadcast) + ABA (async binary agreement)
4. Decrypts results using threshold key shares, commits a block

### `Network` (`src/core/network.py`)

Simulates message propagation:
- Random geometric graph topology with configurable average degree
- Shortest-path latency with per-edge weights
- Exponential base delay + uniform jitter
- Probabilistic packet drop

### `tx_reader` (`src/io/tx_reader.py`)

Memory-safe streaming CSV reader for large files (tested up to 1.7 GB):
- Chunk-based reading (default 50,000 rows/chunk)
- Auto-detects column names from multiple naming conventions
- Optional sampling (`sample_rate`)
- Cross-zone double-spend injection: forces conflicting TX pair to target **different shards** via address manipulation

---

## Key Design Decisions

**Why cross-zone double-spend injection?**
Intra-zone DS is trivially caught by PBFT. Cross-zone DS — where conflicting TX land in different consensus zones — is the hard case MSSP is designed to handle. The sorter's global `conflict_map` ensures both blocks are never simultaneously confirmed.

**Why SHA-256 for m-node shard selection?**
Consistent with the MSSP paper (Algorithm 1). The hash of `(node_id ‖ R)` determines which additional shards a multi-shard node stores, ensuring uniform distribution without coordination.

**Why SimPy discrete-event simulation?**
Allows precise modeling of network delays, timeouts, concurrent consensus rounds, and view-changes without the overhead of real distributed systems. All timing is reproducible via `random_seed`.

---

## Author

**To Dang Minh Tuan**
- GitLab: [gitlab.com/24521942/mssp_own](https://gitlab.com/24521942/mssp_own)
- GitHub: [github.com/tuanwannafly](https://github.com/tuanwannafly)
- Email: totuanforwork@gmail.com

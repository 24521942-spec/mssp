# run_pbft.py
from src.core.config import Config
from src.sim.main import MSSPSim

CSV_PATH = "data/blockchain_transaction.csv"

cfg = Config()

cfg.pprob.deadline_factor = 1.5
cfg.pbft.timeout = 1.0
cfg.pbft.max_view_changes = 5
cfg.net.jitter = 0.1

sim = MSSPSim(cfg)


sim.feed_transactions_from_csv_local(
    CSV_PATH,
    max_tx=50_000,         # ↑ hoặc None để đọc hết (1.7 GB: lâu)
    sample_rate=1.0,       # =1.0 đọc đủ max_tx
    inject_ds=True,        # bật tiêm double-spend
    ds_rate=0.01        # 1% TX được nhân bản xung đột
)

# thời lượng mô phỏng
sim.run(180)

print("PBFT metrics:", sim.metrics)

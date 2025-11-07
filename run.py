from src.core.config import Config
from src.sim.main import MSSPSim
from src.io.tx_reader import feed_transactions_from_csv

cfg = Config()
cfg.pprob.deadline_factor = 1.5
cfg.pbft.timeout = 1.0
cfg.pbft.max_view_changes = 5
cfg.net.jitter = 0.1

sim = MSSPSim(cfg)

feed_transactions_from_csv(
    sim,
    "data/blockchain_transaction.csv",
    max_tx=200000,
    sample_rate=0.3,
    inject_ds=True,
    ds_rate=0.03,
)

sim.run(10000)
print(sim.metrics)

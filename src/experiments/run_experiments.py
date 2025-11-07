# src/experiments/run_experiments.py
import csv
from ..core.config import Config, SystemConfig, NetworkConfig, PBFTConfig
from ..sim.main import MSSPSim

def run_single(proc, net, mal, seed=42, duration=40):
    cfg = Config()
    cfg.sys.process_mean = proc
    cfg.net.mean_delay = net
    cfg.sys.malicious_fraction = mal
    cfg.sys.random_seed = seed
    sim = MSSPSim(cfg)
    sim.run(duration=duration)
    return {'proc':proc,'net':net,'mal':mal, **sim.metrics}

def run_grid():
    rows = []
    for p in [0.5, 1.0, 1.5, 2.0]:
        for n in [0.1, 0.5, 1.0]:
            for m in [0.0, 0.1, 0.2, 0.3]:
                r = run_single(p, n, m, seed=2025, duration=60)
                rows.append(r)
                print("Done:", p, n, m)
    with open("experiment_results.csv","w",newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader(); writer.writerows(rows)
    print("Saved experiment_results.csv")

if __name__ == "__main__":
    run_grid()

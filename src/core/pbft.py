# src/core/pbft.py
import random
import simpy
from typing import Tuple
from .config import PBFTConfig
from .models import ConsensusZone, Node

def count_yes_votes(nodes, malicious_drop_prob: float) -> int:
    yes = 0
    for v in nodes:
        if v.is_malicious:
            # malicious node may drop or vote invalid
            if random.random() < (1.0 - malicious_drop_prob):
                yes += 1
        else:
            yes += 1
    return yes

def run_pbft(env: simpy.Environment, zone: ConsensusZone, cfg: PBFTConfig, malicious_drop_prob: float) -> bool:
    """
    PBFT 3 pha + timeout + view-change (tối giản):
      - PRE-PREPARE
      - PREPARE (need >= 2f+1)
      - COMMIT  (need >= 2f+1)
    Nếu bất kỳ pha nào không đạt quorum trước timeout -> view-change (đổi leader),
    thử lại (tối đa max_view_changes).
    """
    n, f, quorum = zone.quorum_params()
    if n == 0:
        zone.pbft_failure += 1
        return False

    attempts = 0
    while attempts <= cfg.max_view_changes:
        # PRE-PREPARE
        yield env.timeout(_bounded_delay(cfg.preprepare_mean, cfg.timeout))
        # PREPARE
        yield env.timeout(_bounded_delay(cfg.prepare_mean, cfg.timeout))
        yes_prepare = count_yes_votes(zone.nodes, malicious_drop_prob)
        if yes_prepare < quorum:
            # view change
            attempts += 1
            zone.view_changes += 1
            yield env.timeout(_bounded_delay(cfg.viewchange_cost_mean, cfg.timeout))
            continue
        # COMMIT
        yield env.timeout(_bounded_delay(cfg.commit_mean, cfg.timeout))
        yes_commit = count_yes_votes(zone.nodes, malicious_drop_prob)
        if yes_commit < quorum:
            attempts += 1
            zone.view_changes += 1
            yield env.timeout(_bounded_delay(cfg.viewchange_cost_mean, cfg.timeout))
            continue

        zone.pbft_success += 1
        return True

    zone.pbft_failure += 1
    return False

def _bounded_delay(mean: float, timeout: float) -> float:
    import random
    d = random.expovariate(1.0/mean) if mean > 0 else 0.0
    return min(d, timeout)

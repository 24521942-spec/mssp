# src/core/metrics.py
from dataclasses import dataclass, field
from typing import Dict

@dataclass
class Metrics:
    pre_sort_double_spends: int = 0
    post_sort_double_spends: int = 0
    orphaned_blocks: int = 0
    confirmed_blocks: int = 0
    proofs_returned: int = 0
    proofs_required: int = 0
    faulty_nodes_quarantined: int = 0
    pbft_success: int = 0
    pbft_failure: int = 0

    def as_dict(self) -> Dict:
        return self.__dict__.copy()

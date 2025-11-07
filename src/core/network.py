# src/core/network.py
import random, math
from typing import Dict, Tuple, Optional
import networkx as nx

class Network:
    """
    Mạng mô phỏng:
    - Nếu use_topology=True: dùng đồ thị (networkx) để tính latency giữa hai node qua shortest path.
    - Nếu False: dùng mean_delay ± jitter theo phân phối mũ + uniform jitter.
    """
    def __init__(self, mean_delay: float, jitter: float, drop_prob: float, use_topology: bool):
        self.mean_delay = mean_delay
        self.jitter = jitter
        self.drop_prob = drop_prob
        self.use_topology = use_topology
        self.G: Optional[nx.Graph] = None
        self.positions: Dict[int, Tuple[float,float]] = {}

    def build_random_topology(self, node_ids, avg_degree=3):
        if not self.use_topology:
            return
        self.G = nx.Graph()
        self.G.add_nodes_from(node_ids)
        # random geometric graph-like: connect nodes by proximity in plane
        import random
        for nid in node_ids:
            self.positions[nid] = (random.random(), random.random())
        # connect roughly avg_degree edges per node
        nodes = list(node_ids)
        for i in nodes:
            # connect i with nearest k nodes
            dists = []
            for j in nodes:
                if j == i: continue
                xi, yi = self.positions[i]; xj, yj = self.positions[j]
                d = math.dist((xi,yi), (xj,yj))
                dists.append((d, j))
            dists.sort()
            for _, j in dists[:avg_degree]:
                if not self.G.has_edge(i, j):
                    # latency ~ base mean modulated by distance
                    w = self.mean_delay * (0.5 + dists[0][0] + _*0.0)  # simple function
                    self.G.add_edge(i, j, latency=max(0.01, w))

    def sample_delay(self, src_id: int=None, dst_id: int=None) -> float:
        if self.use_topology and self.G is not None and src_id is not None and dst_id is not None:
            try:
                path = nx.shortest_path(self.G, src_id, dst_id, weight='latency')
                # sum latencies on path
                total = 0.0
                for u,v in zip(path[:-1], path[1:]):
                    total += self.G[u][v]['latency']
                # jitter
                j = (1.0 + random.uniform(-self.jitter, self.jitter))
                return max(0.0, total * j)
            except Exception:
                pass
        # fallback: exponential with jitter
        base = random.expovariate(1.0 / self.mean_delay) if self.mean_delay > 0 else 0.0
        j = (1.0 + random.uniform(-self.jitter, self.jitter))
        return max(0.0, base * j)

    def should_drop(self) -> bool:
        return random.random() < self.drop_prob

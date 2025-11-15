import simpy, random

class ABA:
    """ABA rút gọn: lặp nhiều vòng; bế tắc thì dùng coin ngẫu nhiên."""
    def __init__(self, env: simpy.Environment, n:int, f:int, seed:int=2025):
        self.env = env
        self.n, self.f = n, f
        self.rand = random.Random(seed)

    def decide(self, initial_bit:int, max_rounds:int=6):
        bit = initial_bit
        for _ in range(max_rounds):
            yield self.env.timeout(0)
            votes_for = 2*self.f + 1 if bit == 1 else self.f  # giả lập
            if votes_for >= 2*self.f + 1:
                return bit
            bit = self.rand.randint(0,1)  # coin
        return bit

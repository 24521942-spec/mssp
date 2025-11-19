"""
Mô-đun mô phỏng CommonCoin cho các thuật toán đồng thuận bất đồng bộ.

CommonCoin cung cấp một nguồn ngẫu nhiên chung cho tất cả các node. Trong
thực tế, nó được triển khai bằng cơ chế chữ ký ngưỡng, nhưng trong mô
phỏng này, chúng ta sử dụng bộ sinh số ngẫu nhiên với seed chung để đảm
bảo tất cả các node đều nhận cùng kết quả coin theo mỗi lần gọi.

Interface đơn giản gồm:
    - get_coin(round: int) -> int: trả về bit ngẫu nhiên (0 hoặc 1) cho
      một vòng nhất định.
    - handle_message(...): phương thức trống để tương thích với ACS
      (trong mô phỏng này không dùng đến).
"""

import random
import simpy
from typing import Any, List


class CommonCoin:
    def __init__(self, env: simpy.Environment, node_ids: List[int], seed: int):
        """
        Tạo một common coin với một seed dùng chung. Bộ sinh ngẫu nhiên sử
        dụng seed này để đảm bảo tất cả các node sẽ sử dụng cùng chuỗi ngẫu
        nhiên.

        Args:
            env (simpy.Environment): môi trường mô phỏng (không sử dụng nhiều
                trong coin nhưng giữ để tương thích).
            node_ids (List[int]): danh sách node tham gia.
            seed (int): seed cho bộ sinh ngẫu nhiên.
        """
        self.env = env
        self.node_ids = node_ids
        self.rand = random.Random(seed)

    def get_coin(self, round: int) -> int:
        """
        Trả về một bit ngẫu nhiên cho vòng cụ thể. Trong phiên bản
        đơn giản này, chúng ta bỏ qua tham số vòng vì bộ sinh đã đủ.

        Args:
            round (int): số vòng (không ảnh hưởng tới kết quả trong mô phỏng).

        Returns:
            int: bit ngẫu nhiên 0 hoặc 1.
        """
        # Có thể sử dụng tham số round để cập nhật seed, nhưng ở đây không cần
        return self.rand.randint(0, 1)

    def handle_message(self, from_id: int, msg_type: str, payload: Any):
        """
        CommonCoin không cần xử lý message trong mô phỏng này. Hàm tồn tại
        để ACS có thể gọi mà không gây lỗi.
        """
        return
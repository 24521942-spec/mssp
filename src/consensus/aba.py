"""
Mô-đun mô phỏng Asynchronous Binary Agreement (ABA).

File này cung cấp hai lớp:
    - ABA: phiên bản rút gọn dùng trong mô hình cũ, chỉ quyết định dựa
      trên tham số đầu vào và một common coin cục bộ. Lớp này được giữ
      lại cho tương thích.
    - ABAInstance: một lớp mô phỏng tương thích với ACS, có giao diện
      input/handle_message để nhận bit đầu vào và đưa ra output. Trong
      mô phỏng hiện tại, ABAInstance đơn giản hoá thuật toán bằng cách
      quyết định giá trị bằng chính bit đầu vào sau một khoảng trễ nhỏ.

Lưu ý: Đây chỉ là mô phỏng tối giản; thuật toán ABA thực sự phức tạp
và cần trao đổi thông điệp nhiều vòng kết hợp common coin. Mục tiêu ở
đây là tạo ra giao diện phù hợp với ACS để minh hoạ luồng hoạt động
trong HoneyBadgerBFT.
"""

import simpy
import random
from typing import Any, Dict, List


class ABA:
    """
    ABA rút gọn: lặp nhiều vòng; bế tắc thì dùng coin ngẫu nhiên.
    Lớp này được giữ lại để tương thích với các mô hình cũ.
    """

    def __init__(self, env: simpy.Environment, n: int, f: int, seed: int = 2025):
        self.env = env
        self.n, self.f = n, f
        self.rand = random.Random(seed)

    def decide(self, initial_bit: int, max_rounds: int = 6):
        bit = initial_bit
        for _ in range(max_rounds):
            yield self.env.timeout(0)
            votes_for = 2 * self.f + 1 if bit == 1 else self.f  # giả lập
            if votes_for >= 2 * self.f + 1:
                return bit
            bit = self.rand.randint(0, 1)  # coin
        return bit


class ABAInstance:
    """
    ABAInstance mô phỏng một phiên bản Binary Agreement cho một bit.

    Mỗi instance đại diện cho một chỉ số j trong ACS (ví dụ ABA_j). Giao
    diện gồm phương thức input(bit) để cung cấp bit đầu vào, handle_message
    để xử lý thông điệp (không sử dụng trong mô phỏng tối giản này), và
    các phương thức has_output/get_output để kiểm tra và lấy kết quả.

    Trong mô phỏng hiện tại, khi một bit đầu vào được cung cấp, instance
    chờ một khoảng thời gian mô phỏng ngắn rồi quyết định giá trị đó. Điều
    này đủ để ACS điều phối việc đưa vào '1' khi một RBC hoàn tất và
    đưa vào '0' cho các phiên còn lại.
    """

    def __init__(self,
                 env: simpy.Environment,
                 inst_id: str,
                 node_ids: List[int],
                 f: int,
                 coin: Any):
        """
        Args:
            env (simpy.Environment): môi trường mô phỏng.
            inst_id (str): id của instance (ví dụ "ABA_3").
            node_ids (List[int]): danh sách id node.
            f (int): số kẻ xấu tối đa (không sử dụng trong mô phỏng này).
            coin (CommonCoin): đối tượng common coin (không sử dụng ở đây).
        """
        self.env = env
        self.inst_id = inst_id
        self.node_ids = node_ids
        self.f = f
        self.n = len(node_ids)
        self.coin = coin
        self.input_given = False
        self.input_value = None
        self.output_value = None

    # cung cấp bit đầu vào cho ABA
    def input(self, bit: int):
        if self.input_given:
            return
        self.input_given = True
        self.input_value = bit
        # khởi chạy quá trình quyết định
        self.env.process(self._run())

    def _run(self):
        # mô phỏng chi phí tính toán bằng cách đợi một thời gian nhỏ
        yield self.env.timeout(0.01)
        # Trong mô phỏng, quyết định luôn bằng bit đầu vào
        self.output_value = self.input_value

    def handle_message(self, from_id: int, msg_type: str, payload: Dict):
        """
        Hàm xử lý thông điệp của ABA. Trong mô phỏng tối giản này, ABAInstance
        không trao đổi thông điệp nên hàm này chỉ tồn tại để tương thích với
        ACS. Các tham số được bỏ qua.
        """
        return

    def has_output(self) -> bool:
        return self.output_value is not None

    def get_output(self) -> Any:
        return self.output_value
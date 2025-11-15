# src/io/tx_reader.py
# Đọc CSV lớn (1.7 GB) an toàn bộ nhớ, chặn DtypeWarning, hỗ trợ sample
# và có adapter tự tìm API nạp giao dịch vào simulator (accept/enqueue/add/...).
# BẢN NÀY có tiêm double-spend CHÉO ZONE (ép 'to' map sang shard khác)
# để đánh giá DS cho cả PBFT và HBBFT.

from __future__ import annotations
import random
from typing import Dict, Iterator, List, Optional
import warnings

import pandas as pd
from pandas.errors import DtypeWarning

# map địa chỉ -> shard để ép DS sang zone khác
from src.core.workload import addr_to_shard

# Tắt cảnh báo kiểu dữ liệu lẫn lộn của pandas
warnings.simplefilter("ignore", DtypeWarning)

# ------- Tham số mặc định an toàn cho file lớn -------
_DEFAULT_CHUNKSIZE = 50_000   # chunk vừa phải để RAM ổn định
_DEFAULT_SAMPLE    = 1.0      # 1.0 = không lấy mẫu (đọc hết)
_RNG_SEED          = 42       # giúp tái lập kết quả khi sample

# ------- Gợi ý map tên cột (tuỳ dataset, có thể chỉnh) -------
POSSIBLE_COLS = {
    "tx_hash": ["tx_hash", "hash", "txid", "transaction_hash"],
    "from":    ["from", "sender", "from_address"],
    "to":      ["to", "receiver", "to_address"],
    "value":   ["value", "amount"],
    "time":    ["timestamp", "time", "block_time", "datetime"],
}

def _try_get(df: pd.DataFrame, keys: List[str], default: str = "") -> pd.Series:
    """Lấy cột đầu tiên tồn tại trong danh sách keys, nếu không có trả về chuỗi rỗng."""
    for k in keys:
        if k in df.columns:
            return df[k]
    return pd.Series([default] * len(df), index=df.index)

def _normalize_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """Chuẩn hoá cột về schema tối thiểu để mô phỏng."""
    out = pd.DataFrame(index=df.index)
    out["tx_hash"] = _try_get(df, POSSIBLE_COLS["tx_hash"]).astype(str)
    out["from"]    = _try_get(df, POSSIBLE_COLS["from"]).astype(str)
    out["to"]      = _try_get(df, POSSIBLE_COLS["to"]).astype(str)
    out["value"]   = _try_get(df, POSSIBLE_COLS["value"]).astype(str)
    out["time"]    = _try_get(df, POSSIBLE_COLS["time"]).astype(str)
    return out

def _iter_read_csv(
    csv_path: str,
    *,
    chunksize: int,
    usecols: Optional[List[str]] = None,
    prefer_pyarrow: bool = True,
) -> Iterator[pd.DataFrame]:
    """
    Trả về từng DataFrame theo chunksize.
    - Ưu tiên engine='pyarrow' (nhanh, ít cảnh báo), nếu lỗi thì fallback engine='c' + dtype=str.
    """
    if prefer_pyarrow:
        try:
            yield from pd.read_csv(
                csv_path,
                chunksize=chunksize,
                engine="pyarrow",
                usecols=usecols,
            )
            return
        except Exception:
            pass  # fallback phía dưới

    yield from pd.read_csv(
        csv_path,
        chunksize=chunksize,
        dtype=str,           # tránh DtypeWarning
        low_memory=False,    # tắt phân tích lô gây cảnh báo
        usecols=usecols,
        engine="c",
    )

def stream_transactions(
    csv_path: str,
    *,
    chunksize: int = _DEFAULT_CHUNKSIZE,
    sample_rate: float = _DEFAULT_SAMPLE,
    max_tx: Optional[int] = None,
    usecols: Optional[List[str]] = None,
    rng_seed: int = _RNG_SEED,
) -> Iterator[Dict[str, str]]:
    """
    Đọc CSV theo stream (ít RAM), chuẩn hoá schema và (tuỳ chọn) sample.
    Trả về từng giao dịch: {tx_hash, from, to, value, time}.
    """
    assert 0 < sample_rate <= 1.0, "sample_rate phải trong (0,1]."
    rng = random.Random(rng_seed)

    emitted = 0
    for raw_chunk in _iter_read_csv(csv_path, chunksize=chunksize, usecols=usecols):
        chunk = _normalize_chunk(raw_chunk)

        if sample_rate < 1.0:
            mask = [rng.random() < sample_rate for _ in range(len(chunk))]
            chunk = chunk.loc[mask]

        for row in chunk.itertuples(index=False, name="TX"):
            row_dict = row._asdict() if hasattr(row, "_asdict") else dict(zip(chunk.columns, row))
            tx = {
                "tx_hash": row_dict.get("tx_hash", ""),
                "from":    row_dict.get("from", ""),
                "to":      row_dict.get("to", ""),
                "value":   row_dict.get("value", ""),
                "time":    row_dict.get("time", ""),
            }
            yield tx
            emitted += 1
            if max_tx is not None and emitted >= max_tx:
                return

# --- helper: tìm chỗ nạp giao dịch vào simulator ---
def _resolve_tx_sink(sim):
    """
    Trả về hàm callable nhận 1 dict tx để nạp vào simulator.
    Tự dò theo thứ tự:
      sim.accept_tx / enqueue_tx / add_tx / submit_tx / ingest_tx / push_tx
      sim.mempool.accept|add|enqueue|submit|push
      sim.workload.push|add|submit|ingest|enqueue
    """
    # method trực tiếp trên sim
    for name in ("accept_tx", "enqueue_tx", "add_tx", "submit_tx", "ingest_tx", "push_tx"):
        fn = getattr(sim, name, None)
        if callable(fn):
            return fn
    # mempool nếu có
    mp = getattr(sim, "mempool", None)
    if mp is not None:
        for name in ("accept", "add", "enqueue", "submit", "push"):
            fn = getattr(mp, name, None)
            if callable(fn):
                return lambda tx: fn(tx)
    # workload nếu có
    wl = getattr(sim, "workload", None)
    if wl is not None:
        for name in ("push", "add", "submit", "ingest", "enqueue"):
            fn = getattr(wl, name, None)
            if callable(fn):
                return lambda tx: fn(tx)

    raise AttributeError(
        "Không tìm thấy API để nạp giao dịch. "
        "Hãy bổ sung accept_tx()/enqueue_tx() cho MSSPSim hoặc mempool/workload."
    )

# --- helper: ép địa chỉ 'to' rơi vào shard mong muốn (để DS chéo zone) ---
def _find_addr_for_shard(base: str, target_shard: int, n_shards: int, max_tries: int = 5000) -> str:
    """
    Tạo biến thể của 'base' để addr_to_shard(...) == target_shard.
    Dùng brute-force nhẹ nhàng (đủ cho mô phỏng).
    """
    base = base or "0xdeadbeef"
    for k in range(max_tries):
        cand = f"{base}_z{k}"
        if addr_to_shard(cand, n_shards) == target_shard:
            return cand
    # fallback: trả về base nếu không tìm thấy
    return base

def feed_transactions_from_csv(
    sim,                               # MSSPSim instance
    csv_path: str,
    *,
    max_tx: Optional[int] = None,
    sample_rate: float = _DEFAULT_SAMPLE,
    chunksize: int = _DEFAULT_CHUNKSIZE,
    inject_ds: bool = False,
    ds_rate: float = 0.0,
    rng_seed: int = _RNG_SEED,
    usecols: Optional[List[str]] = None,
) -> int:
    """
    Nạp trực tiếp vào mô phỏng (mempool) từ CSV lớn, có tuỳ chọn inject double-spend nhẹ.
    - inject_ds=True: với xác suất ds_rate, tạo 2 tx có cùng conflict_id,
      trong đó **bản thứ hai** bị ép 'to' sang **shard khác** -> DS chéo zone.
    Trả về: tổng số giao dịch đã nạp (bao gồm cả bản sao conflict nếu inject_ds=True).
    """
    rng = random.Random(rng_seed)
    fed = 0
    tx_sink = _resolve_tx_sink(sim)

    # số shard trong hệ để ép sang shard khác
    n_shards = getattr(sim.cfg.sys, "shards", 4)

    for tx in stream_transactions(
        csv_path,
        chunksize=chunksize,
        sample_rate=sample_rate,
        max_tx=max_tx,
        usecols=usecols,
        rng_seed=rng_seed,
    ):
        # nạp TX gốc
        tx_sink(tx)
        fed += 1

        # (tuỳ chọn) bơm thêm giao dịch conflict CHÉO ZONE
        if inject_ds and ds_rate > 0.0 and rng.random() < ds_rate:
            try:
                s_to = addr_to_shard(str(tx.get("to","")), n_shards)
                # ép sang shard khác
                target_shard = (s_to + 1) % n_shards
                alt_to = _find_addr_for_shard(str(tx.get("to","")), target_shard, n_shards)

                cid = f"csvds_{rng_seed}_{fed}_{rng.randint(1000,9999)}"
                ds_tx1 = dict(tx); ds_tx1["conflict_id"] = cid
                ds_tx2 = dict(tx); ds_tx2["to"] = alt_to; ds_tx2["conflict_id"] = cid

                tx_sink(ds_tx1); fed += 1
                tx_sink(ds_tx2); fed += 1
            except Exception:
                # nếu lỗi mapping, fallback: bơm 1 bản xung đột cùng zone
                ds_tx = dict(tx)
                ds_tx["to"] = f"{tx.get('to','')}_ALT"
                ds_tx["conflict_id"] = f"csvds_{rng_seed}_{fed}_{rng.randint(1000,9999)}"
                tx_sink(ds_tx); fed += 1

    print(f"[feed_csv] fed {fed} transactions (inject_ds={inject_ds})")
    return fed

# ---- Backward-compat alias (giữ tương thích với main.py cũ) ----
def stream_transactions_from_csv(csv_path: str, **kwargs):
    """
    Wrapper tương thích cũ: chuyển tới stream_transactions(...).
    Cho phép import cũ: from ..io.tx_reader import stream_transactions_from_csv
    """
    return stream_transactions(csv_path, **kwargs)

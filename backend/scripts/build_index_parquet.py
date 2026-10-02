"""Build CN index parquet files for QuantDB market overview.

Downloads major CN indices via akshare and writes them to
/data/quantdb/1_kline_data/index_daily/dt=YYYYMMDD/data.parquet
so that QuantDBDataHub.fetch_index_kline() can serve the dashboard.
"""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

# CN indices: (symbol in QuantMind, akshare symbol)
INDICES = [
    ("000001.SH", "sh000001"),
    ("399001.SZ", "sz399001"),
    ("000300.SH", "sh000300"),
    ("000905.SH", "sh000905"),
    ("399006.SZ", "sz399006"),
    ("000688.SH", "sh000688"),
    ("000016.SH", "sh000016"),
]

OUTPUT_DIR = Path(os.getenv("QM_QUANTDB_DATA_DIR", "/data/quantdb"))
INDEX_DIR = OUTPUT_DIR / "1_kline_data" / "index_daily"

# How many days of history to keep (dashboard kline needs ~120 days)
LOOKBACK_DAYS = 400


def main() -> None:
    import akshare as ak

    INDEX_DIR.mkdir(parents=True, exist_ok=True)
    cutoff = date.today() - timedelta(days=LOOKBACK_DAYS)

    all_frames: list[pd.DataFrame] = []
    for qm_symbol, ak_symbol in INDICES:
        print(f"Fetching {qm_symbol} ({ak_symbol})...")
        try:
            df = ak.stock_zh_index_daily(symbol=ak_symbol)
        except Exception as exc:
            print(f"  FAILED: {exc}")
            continue
        if df.empty:
            print(f"  empty, skip")
            continue
        df = df.rename(columns={"date": "time"})
        df["time"] = pd.to_datetime(df["time"])
        df = df[df["time"].dt.date >= cutoff]
        df["symbol"] = qm_symbol
        df["amount"] = 0.0  # akshare index daily has no amount
        all_frames.append(df[["time", "symbol", "open", "high", "low", "close", "volume", "amount"]])
        print(f"  {len(df)} rows, latest={df['time'].max().date()}")

    if not all_frames:
        print("No data fetched, aborting.")
        sys.exit(1)

    combined = pd.concat(all_frames, ignore_index=True)
    print(f"\nTotal rows: {len(combined)}")
    print(f"Date range: {combined['time'].min().date()} ~ {combined['time'].max().date()}")

    # Write one parquet per trade date
    written = 0
    for td, group in combined.groupby(combined["time"].dt.strftime("%Y%m%d")):
        dt_dir = INDEX_DIR / f"dt={td}"
        dt_dir.mkdir(parents=True, exist_ok=True)
        out = dt_dir / "data.parquet"
        group.to_parquet(out, index=False, engine="pyarrow")
        written += 1

    print(f"Wrote {written} date partitions to {INDEX_DIR}")
    print("Done.")


if __name__ == "__main__":
    main()

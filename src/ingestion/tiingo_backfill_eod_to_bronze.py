#!/usr/bin/env python3
"""
Tiingo EOD backfill -> S3 Bronze (date-partitioned JSON)

Writes:
s3://<bucket>/<prefix>/trade_date=YYYY-MM-DD/<ticker>.json

Properties:
- Batch (daily)
- Adjusted prices included (adjClose, adjVolume)
- Idempotent: overwrites the same date partition file for the ticker
- Trading-calendar alignment: Tiingo only returns trading days for EOD endpoints
"""

import argparse
import json
import os
import sys
import time
from datetime import date, datetime
from typing import Dict, List, Optional

import boto3
import requests


TIINGO_BASE = "https://api.tiingo.com/tiingo/daily"


def _iso(d: date) -> str:
    return d.isoformat()


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def fetch_tiingo_eod(
    session: requests.Session,
    token: str,
    ticker: str,
    start_date: str,
    end_date: str,
    retries: int = 5,
    backoff_sec: float = 1.0,
) -> List[Dict]:
    """
    Returns list of daily bars for [start_date, end_date] inclusive.
    """
    url = f"{TIINGO_BASE}/{ticker}/prices"
    params = {
        "startDate": start_date,
        "endDate": end_date,
        "format": "json",
        "resampleFreq": "daily",
    }
    headers = {"Authorization": f"Token {token}"}

    last_err: Optional[Exception] = None
    for i in range(retries):
        try:
            resp = session.get(url, params=params, headers=headers, timeout=30)
            if resp.status_code == 429:
                # rate limited
                time.sleep(backoff_sec * (2 ** i))
                continue
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list):
                raise RuntimeError(f"Unexpected response for {ticker}: {data}")
            # add ticker into each record for downstream convenience
            for r in data:
                r["ticker"] = ticker
            return data
        except Exception as e:
            last_err = e
            time.sleep(backoff_sec * (2 ** i))

    raise RuntimeError(f"Failed Tiingo fetch for {ticker}: {last_err}") from last_err


def s3_put_json(
    s3_client,
    bucket: str,
    key: str,
    payload: Dict,
) -> None:
    body = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--tickers", required=True, help="Comma-separated tickers (must include SPY)")
    p.add_argument("--s3-bucket", required=True)
    p.add_argument("--s3-prefix", required=True, help="e.g. bronze/market/tiingo/daily_prices")
    p.add_argument("--sleep-ms", type=int, default=200, help="Small delay between ticker calls")
    args = p.parse_args()

    token = os.getenv("TIINGO_API_TOKEN")
    if not token:
        print("ERROR: TIINGO_API_TOKEN is not set in environment.", file=sys.stderr)
        sys.exit(1)

    start_dt = _parse_date(args.start_date)
    end_dt = _parse_date(args.end_date)
    if end_dt < start_dt:
        print("ERROR: end-date must be >= start-date", file=sys.stderr)
        sys.exit(1)

    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    if "SPY" not in tickers:
        print("ERROR: tickers must include SPY (SSOT benchmark modeling).", file=sys.stderr)
        sys.exit(1)

    bucket = args.s3_bucket
    prefix = args.s3_prefix.strip("/")

    s3 = boto3.client("s3")
    session = requests.Session()

    total_written = 0
    total_days = 0

    print(f"Backfill: {args.start_date} -> {args.end_date}")
    print(f"Tickers ({len(tickers)}): {', '.join(tickers)}")
    print(f"S3: s3://{bucket}/{prefix}/trade_date=YYYY-MM-DD/<ticker>.json")

    for ticker in tickers:
        data = fetch_tiingo_eod(
            session=session,
            token=token,
            ticker=ticker,
            start_date=args.start_date,
            end_date=args.end_date,
        )

        # Tiingo returns trading days only; we partition by the record's date
        written_for_ticker = 0
        for r in data:
            # Tiingo 'date' is ISO timestamp; normalize to YYYY-MM-DD for partition
            # Example: "2024-01-10T00:00:00.000Z"
            d = str(r.get("date", ""))[:10]
            if not d or len(d) != 10:
                # skip malformed
                continue

            key = f"{prefix}/trade_date={d}/{ticker}.json"
            # Idempotent overwrite: same key for same date+ticker
            s3_put_json(s3, bucket, key, r)
            written_for_ticker += 1
            total_written += 1

        total_days += written_for_ticker
        print(f"{ticker}: wrote {written_for_ticker} daily files")
        time.sleep(args.sleep_ms / 1000.0)

    print(f"Done. Total files written: {total_written}")
    print("Next: run S3 ls to confirm partitions.")


if __name__ == "__main__":
    main()
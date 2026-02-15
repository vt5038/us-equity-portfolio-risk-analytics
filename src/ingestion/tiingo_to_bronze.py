from __future__ import annotations

"""
PortaRisk — Tiingo EOD → S3 Bronze (Raw Ingestion)

SSOT constraints (v1):
- Store RAW API responses in S3 Bronze without modification.
- Date-partitioned paths.
- Rerunnable + idempotent for a given day/partition (safe overwrite).
- Batch ingestion supports historical backfill + daily incremental.

What this script does:
- Calls Tiingo EOD prices endpoint per ticker for a given date range.
- Writes the raw JSON response bytes to S3 at a partitioned path.
- Writes a small request metadata file alongside (helps debugging; raw payload remains untouched).
"""




from dotenv import load_dotenv
load_dotenv()

import argparse
import datetime as dt
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Iterable, Optional

import boto3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ----------------------------
# Config
# ----------------------------

@dataclass(frozen=True)
class Config:
    tiingo_api_token: str
    tiingo_base_url: str
    bronze_s3_bucket: str
    bronze_s3_prefix: str
    default_tickers: list[str]

    @staticmethod
    def from_env() -> "Config":
        def _req(name: str) -> str:
            v = os.getenv(name, "").strip()
            if not v:
                raise ValueError(f"Missing required env var: {name}")
            return v

        tiingo_api_token = _req("TIINGO_API_TOKEN")
        tiingo_base_url = os.getenv("TIINGO_BASE_URL", "https://api.tiingo.com").strip()
        bronze_s3_bucket = _req("BRONZE_S3_BUCKET")
        bronze_s3_prefix = os.getenv("BRONZE_S3_PREFIX", "bronze").strip().strip("/")

        tickers_raw = os.getenv("TIINGO_TICKERS", "SPY").strip()
        default_tickers = [t.strip().upper() for t in tickers_raw.split(",") if t.strip()]

        return Config(
            tiingo_api_token=tiingo_api_token,
            tiingo_base_url=tiingo_base_url.rstrip("/"),
            bronze_s3_bucket=bronze_s3_bucket,
            bronze_s3_prefix=bronze_s3_prefix,
            default_tickers=default_tickers,
        )


# ----------------------------
# Logging
# ----------------------------

def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
        stream=sys.stdout,
    )


# ----------------------------
# HTTP client (retries)
# ----------------------------

def build_http_session() -> requests.Session:
    retry = Retry(
        total=8,
        connect=5,
        read=5,
        backoff_factor=1.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


# ----------------------------
# Tiingo fetch
# ----------------------------

def tiingo_prices_url(base_url: str, ticker: str) -> str:
    # Tiingo daily prices endpoint:
    # GET /tiingo/daily/{ticker}/prices?startDate=YYYY-MM-DD&endDate=YYYY-MM-DD&format=json
    return f"{base_url}/tiingo/daily/{ticker}/prices"


def fetch_tiingo_eod_raw_json(
    session: requests.Session,
    base_url: str,
    token: str,
    ticker: str,
    start_date: str,
    end_date: str,
) -> bytes:
    url = tiingo_prices_url(base_url, ticker)
    headers = {"Authorization": f"Token {token}"}
    params = {
        "startDate": start_date,
        "endDate": end_date,
        "format": "json",
        # Keep it simple and raw — no columns filtering.
        # Adjusted prices are provided by Tiingo in response fields (e.g., adjClose, adjOpen, etc.).
    }

    resp = session.get(url, headers=headers, params=params, timeout=60)
    if resp.status_code != 200:
        # Provide a small, safe error detail
        try:
            body = resp.text[:500]
        except Exception:
            body = "<unreadable>"
        raise RuntimeError(
            f"Tiingo request failed | ticker={ticker} | status={resp.status_code} | body={body}"
        )
    return resp.content  # raw bytes, unmodified


# ----------------------------
# S3 write (idempotent overwrite)
# ----------------------------

def s3_key_for_payload(prefix: str, ticker: str, run_date: str) -> str:
    # Date-partitioned + deterministic path
    # Bronze raw market data layout:
    #   {prefix}/market_data/tiingo/eod/date=YYYY-MM-DD/ticker=XXXX/payload.json
    return (
        f"{prefix}/market_data/tiingo/eod/"
        f"date={run_date}/ticker={ticker}/payload.json"
    )


def s3_key_for_meta(prefix: str, ticker: str, run_date: str) -> str:
    return (
        f"{prefix}/market_data/tiingo/eod/"
        f"date={run_date}/ticker={ticker}/request_meta.json"
    )


def put_s3_object(bucket: str, key: str, body: bytes, content_type: str) -> None:
    s3 = boto3.client("s3")
    # Idempotency: put_object overwrites the same key deterministically
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type,
    )


# ----------------------------
# Helpers
# ----------------------------

def parse_date(s: str) -> str:
    # Validate YYYY-MM-DD
    try:
        dt.date.fromisoformat(s)
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"Invalid date '{s}', expected YYYY-MM-DD") from e
    return s


def daterange_days(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    # inclusive range
    cur = start
    while cur <= end:
        yield cur
        cur += dt.timedelta(days=1)


def choose_run_date(start_date: str, end_date: str) -> str:
    # For daily partitions, we store each day separately.
    # This script supports multi-day ranges by looping run_date day-by-day.
    # Returns not used directly; kept for clarity.
    _ = start_date, end_date
    return ""


# ----------------------------
# Main
# ----------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="PortaRisk: Tiingo EOD → S3 Bronze ingestion")
    parser.add_argument(
        "--tickers",
        type=str,
        default=None,
        help="Comma-separated tickers (e.g., AAPL,MSFT,SPY). Defaults to env TIINGO_TICKERS.",
    )
    parser.add_argument(
        "--start-date",
        type=parse_date,
        required=True,
        help="Start date (YYYY-MM-DD), inclusive.",
    )
    parser.add_argument(
        "--end-date",
        type=parse_date,
        required=True,
        help="End date (YYYY-MM-DD), inclusive.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR).",
    )

    args = parser.parse_args()
    setup_logging(args.log_level)

    cfg = Config.from_env()

    tickers = cfg.default_tickers if not args.tickers else [
        t.strip().upper() for t in args.tickers.split(",") if t.strip()
    ]
    if not tickers:
        raise ValueError("No tickers provided (either --tickers or TIINGO_TICKERS).")

    start = dt.date.fromisoformat(args.start_date)
    end = dt.date.fromisoformat(args.end_date)
    if end < start:
        raise ValueError("end-date must be >= start-date")

    session = build_http_session()

    # Loop day-by-day to create date partitions (SSOT: date-partitioned processing)
    for day in daterange_days(start, end):
        run_date = day.isoformat()

        for ticker in tickers:
            logging.info(f"Fetching Tiingo EOD | ticker={ticker} | date={run_date}")

            # Fetch only this day (start=end=run_date) to keep partition deterministic.
            raw_bytes = fetch_tiingo_eod_raw_json(
                session=session,
                base_url=cfg.tiingo_base_url,
                token=cfg.tiingo_api_token,
                ticker=ticker,
                start_date=run_date,
                end_date=run_date,
            )

            payload_key = s3_key_for_payload(cfg.bronze_s3_prefix, ticker, run_date)
            meta_key = s3_key_for_meta(cfg.bronze_s3_prefix, ticker, run_date)

            # Raw payload: stored unmodified
            put_s3_object(
                bucket=cfg.bronze_s3_bucket,
                key=payload_key,
                body=raw_bytes,
                content_type="application/json",
            )

            # Small metadata artifact for traceability (payload remains raw)
            meta = {
                "source": "tiingo",
                "endpoint": "/tiingo/daily/{ticker}/prices",
                "ticker": ticker,
                "partition_date": run_date,
                "requested_startDate": run_date,
                "requested_endDate": run_date,
                "ingested_at_utc": dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            }
            put_s3_object(
                bucket=cfg.bronze_s3_bucket,
                key=meta_key,
                body=json.dumps(meta, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
                content_type="application/json",
            )

            logging.info(
                f"Wrote Bronze | s3://{cfg.bronze_s3_bucket}/{payload_key} "
                f"(bytes={len(raw_bytes)})"
            )

            # Gentle pacing (helps avoid rate limits in simple backfills)
            time.sleep(0.15)

    logging.info("Ingestion complete.")


if __name__ == "__main__":
    main()

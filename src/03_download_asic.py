from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "raw" / "asic"
NORMALIZED_DIR = BASE_DIR / "normalized"

PACKAGE_SHOW_URL = "https://data.gov.au/data/api/3/action/package_show"
PACKAGE_ID = "asic-companies"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch ASIC company dataset metadata and optionally download the current free file."
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download the selected current resource.",
    )
    parser.add_argument(
        "--resource-format",
        default="CSV",
        choices=["CSV", "ZIP"],
        help="Preferred current resource format.",
    )
    parser.add_argument(
        "--to-parquet",
        action="store_true",
        help="Convert the downloaded CSV to parquet.",
    )
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=50000,
        help="Rows to convert when creating a parquet sample. Use 0 for full file.",
    )
    return parser.parse_args()


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": os.getenv(
                "NDIS_USER_AGENT",
                "Antigravity-NDIS-Enforcement-Intelligence/0.1 (+public-data MVP)",
            )
        }
    )
    return session


def fetch_package_metadata(session: requests.Session, timeout: int) -> dict[str, Any]:
    response = session.get(
        PACKAGE_SHOW_URL,
        params={"id": PACKAGE_ID},
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    if not payload.get("success"):
        raise ValueError(f"CKAN package_show failed for package={PACKAGE_ID!r}")
    return payload


def choose_current_resource(resources: list[dict[str, Any]], preferred_format: str) -> dict[str, Any]:
    preferred = preferred_format.upper()
    candidates = [
        resource
        for resource in resources
        if (resource.get("format") or "").upper() == preferred
        and "current" in (resource.get("name") or "").lower()
    ]
    if candidates:
        return candidates[0]

    fallback = [
        resource
        for resource in resources
        if (resource.get("format") or "").upper() == preferred
    ]
    if fallback:
        return fallback[0]
    raise ValueError(f"No resource found for preferred format={preferred!r}")


def download_resource(
    session: requests.Session, url: str, output_path: Path, timeout: int
) -> None:
    with session.get(url, stream=True, timeout=timeout) as response:
        response.raise_for_status()
        with output_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)


def maybe_create_parquet(csv_path: Path, parquet_path: Path, sample_rows: int) -> None:
    nrows = None if sample_rows == 0 else sample_rows
    frame = pd.read_csv(csv_path, dtype=str, nrows=nrows, low_memory=False)
    try:
        frame.to_parquet(parquet_path, index=False)
    except ImportError as exc:
        raise SystemExit(
            "Parquet conversion requires pyarrow or fastparquet. "
            "Install requirements.txt first, then re-run with --to-parquet."
        ) from exc


def main() -> int:
    load_dotenv(BASE_DIR / ".env")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    NORMALIZED_DIR.mkdir(parents=True, exist_ok=True)

    args = parse_args()
    timeout = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "30"))
    session = build_session()

    metadata = fetch_package_metadata(session, timeout=timeout)
    result = metadata["result"]
    resources = result.get("resources", [])

    metadata_path = RAW_DIR / f"package_show_{utc_stamp()}.json"
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    logging.info("Saved ASIC package metadata: %s", metadata_path)

    resources_frame = pd.DataFrame(resources)
    resources_csv_path = RAW_DIR / "asic_resources.csv"
    resources_frame.to_csv(resources_csv_path, index=False)
    logging.info("Saved ASIC resource inventory: %s", resources_csv_path)

    current_resource = choose_current_resource(resources, preferred_format=args.resource_format)
    logging.info(
        "Selected ASIC resource: %s (%s)",
        current_resource.get("name"),
        current_resource.get("url"),
    )

    if not args.download:
        return 0

    resource_url = current_resource["url"]
    resource_name = Path(resource_url).name
    download_path = RAW_DIR / resource_name
    download_resource(session, resource_url, download_path, timeout=timeout)
    logging.info("Downloaded ASIC resource: %s", download_path)

    if args.to_parquet:
        if download_path.suffix.lower() != ".csv":
            raise SystemExit(
                "--to-parquet currently supports only CSV downloads. Re-run with --resource-format CSV."
            )
        parquet_path = NORMALIZED_DIR / "asic_companies.parquet"
        maybe_create_parquet(download_path, parquet_path, sample_rows=args.sample_rows)
        logging.info(
            "Wrote ASIC parquet sample: %s (sample_rows=%s)",
            parquet_path,
            args.sample_rows,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

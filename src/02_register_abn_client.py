from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from abn_lookup import ABNLookupClient

BASE_DIR = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query ABN Lookup via free GUID-backed JSON or public HTML fallback."
    )
    parser.add_argument("--name", help="Business or entity name to search.")
    parser.add_argument("--abn", help="ABN to fetch details for.")
    parser.add_argument(
        "--max-results",
        type=int,
        default=10,
        help="Maximum name-search results.",
    )
    return parser.parse_args()


def main() -> int:
    load_dotenv(BASE_DIR / ".env")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()

    if not args.name and not args.abn:
        raise SystemExit("Provide at least one of --name or --abn.")

    client = ABNLookupClient(timeout=int(os.getenv("REQUEST_TIMEOUT_SECONDS", "30")))
    output: dict[str, Any] = {}

    if args.name:
        output["name_search"] = client.search_name(args.name, max_results=args.max_results)
    if args.abn:
        output["abn_details"] = client.get_abn_details(args.abn)

    print(json.dumps(output, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import hashlib
import io
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "raw" / "enforcement"
NORMALIZED_DIR = BASE_DIR / "normalized"

EXPORT_URL = (
    "https://www.ndiscommission.gov.au/about-us/"
    "compliance-and-enforcement/compliance-actions/search/export"
)
SEARCH_URL = (
    "https://www.ndiscommission.gov.au/about-us/"
    "compliance-and-enforcement/compliance-actions/search"
)

EXPECTED_COLUMNS = {
    "Type",
    "Date effective from",
    "Date no longer in force",
    "Name",
    "ABN",
    "City",
    "State",
    "Postcode",
    "Provider Number",
    "Other relevant info",
    "Registration Groups ",
    "Relevant information",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download and normalize NDIS Commission enforcement data."
    )
    parser.add_argument(
        "--force-html",
        action="store_true",
        help="Skip the CSV export and only save a search-page HTML snapshot.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=int(os.getenv("REQUEST_TIMEOUT_SECONDS", "30")),
        help="HTTP timeout in seconds.",
    )
    return parser.parse_args()


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def build_session() -> requests.Session:
    user_agent = os.getenv(
        "NDIS_USER_AGENT",
        "Antigravity-NDIS-Enforcement-Intelligence/0.1 (+public-data MVP)",
    )
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "text/csv,text/html;q=0.9,*/*;q=0.8",
        }
    )
    return session


def ensure_dirs() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    NORMALIZED_DIR.mkdir(parents=True, exist_ok=True)


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def digits_only(value: object) -> str:
    return re.sub(r"\D+", "", normalize_text(value))


def infer_action_type(raw_value: str) -> str:
    text = normalize_text(raw_value).lower()
    if "banning" in text:
        return "banning_order"
    if "revocation" in text:
        return "revocation"
    if "compliance notice" in text:
        return "compliance_notice"
    if "undertaking" in text:
        return "enforceable_undertaking"
    if "infringement" in text:
        return "infringement_notice"
    return "other"


def infer_action_subtype(raw_value: str, description: str) -> str:
    text = f"{normalize_text(raw_value)} {normalize_text(description)}".lower()
    if "permanent" in text:
        return "permanent"
    if "temporary" in text:
        return "temporary"
    if "conditional" in text:
        return "conditional"
    return ""


def infer_entity_type(entity_name: str, abn: str) -> str:
    name = normalize_text(entity_name).lower()
    if abn:
        return "provider"
    company_tokens = ("pty ltd", "limited", "inc", "services", "care", "support")
    if any(token in name for token in company_tokens):
        return "provider"
    return "individual"


def extract_support_categories(groups: str, description: str) -> str:
    raw_parts = [normalize_text(groups), normalize_text(description)]
    text = " | ".join(part for part in raw_parts if part)
    matches = re.findall(
        r"(core|capacity building|capital support|registration group[s]?|support [a-z ]+)",
        text,
        flags=re.IGNORECASE,
    )
    cleaned = []
    seen = set()
    for match in matches:
        value = normalize_text(match)
        lowered = value.lower()
        if value and lowered not in seen:
            cleaned.append(value)
            seen.add(lowered)
    return "; ".join(cleaned)


def stable_enforcement_id(row: pd.Series) -> str:
    key_parts = [
        row.get("action_type_raw", ""),
        row.get("date_effective_raw", ""),
        row.get("entity_name", ""),
        row.get("abn", ""),
        row.get("state", ""),
        row.get("description_text", ""),
    ]
    digest = hashlib.sha256("|".join(key_parts).encode("utf-8")).hexdigest()
    return digest[:20]


def save_text(path: Path, payload: str) -> None:
    path.write_text(payload, encoding="utf-8")


def fetch_csv_export(session: requests.Session, timeout: int) -> str:
    response = session.get(EXPORT_URL, timeout=timeout)
    response.raise_for_status()
    content_type = response.headers.get("content-type", "")
    if "csv" not in content_type.lower():
        raise ValueError(f"Expected CSV export, got content-type={content_type!r}")
    return response.text


def save_html_snapshot(session: requests.Session, timeout: int) -> Path:
    response = session.get(SEARCH_URL, timeout=timeout)
    response.raise_for_status()
    snapshot_path = RAW_DIR / f"search_snapshot_{utc_stamp()}.html"
    save_text(snapshot_path, response.text)

    soup = BeautifulSoup(response.text, "lxml")
    title = soup.title.text.strip() if soup.title else "NDIS Commission search"
    logging.info("Saved HTML snapshot: %s", snapshot_path)
    logging.info("Snapshot title: %s", title)
    return snapshot_path


def load_export_frame(csv_text: str) -> pd.DataFrame:
    raw = pd.read_csv(io.StringIO(csv_text), dtype=str).fillna("")
    missing = sorted(EXPECTED_COLUMNS.difference(raw.columns))
    if missing:
        raise ValueError(f"CSV export is missing expected columns: {missing}")
    return raw


def normalize_export_frame(raw_df: pd.DataFrame, source_file: Path) -> pd.DataFrame:
    frame = raw_df.rename(
        columns={
            "Type": "action_type_raw",
            "Date effective from": "date_effective_raw",
            "Date no longer in force": "date_no_longer_in_force_raw",
            "Name": "entity_name",
            "ABN": "abn",
            "City": "city",
            "State": "state",
            "Postcode": "postcode",
            "Provider Number": "provider_number",
            "Other relevant info": "other_relevant_info",
            "Registration Groups ": "registration_groups",
            "Relevant information": "description_text",
        }
    ).copy()

    for column in frame.columns:
        frame[column] = frame[column].map(normalize_text)

    frame["abn"] = frame["abn"].map(digits_only)
    frame["action_type"] = frame["action_type_raw"].map(infer_action_type)
    frame["action_subtype"] = frame.apply(
        lambda row: infer_action_subtype(
            row["action_type_raw"], row["description_text"]
        ),
        axis=1,
    )
    frame["entity_type"] = frame.apply(
        lambda row: infer_entity_type(row["entity_name"], row["abn"]),
        axis=1,
    )
    frame["support_categories_mentioned"] = frame.apply(
        lambda row: extract_support_categories(
            row["registration_groups"], row["description_text"]
        ),
        axis=1,
    )
    frame["source_url"] = EXPORT_URL
    frame["source_file"] = str(source_file)
    frame["ingested_at_utc"] = pd.Timestamp.now(tz="UTC")
    frame["zero_spend_scope"] = True
    frame["enforcement_id"] = frame.apply(stable_enforcement_id, axis=1)
    frame["date_effective"] = pd.to_datetime(
        frame["date_effective_raw"], errors="coerce"
    )
    frame["date_no_longer_in_force"] = pd.to_datetime(
        frame["date_no_longer_in_force_raw"], errors="coerce"
    )

    ordered = [
        "enforcement_id",
        "source_url",
        "source_file",
        "action_type_raw",
        "action_type",
        "action_subtype",
        "date_effective",
        "date_no_longer_in_force",
        "date_effective_raw",
        "date_no_longer_in_force_raw",
        "entity_name",
        "entity_type",
        "abn",
        "city",
        "state",
        "postcode",
        "provider_number",
        "other_relevant_info",
        "registration_groups",
        "description_text",
        "support_categories_mentioned",
        "ingested_at_utc",
        "zero_spend_scope",
    ]
    return frame[ordered]


def main() -> int:
    load_dotenv(BASE_DIR / ".env")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ensure_dirs()
    args = parse_args()

    session = build_session()

    if args.force_html:
        save_html_snapshot(session, timeout=args.timeout)
        logging.info("HTML-only mode complete.")
        return 0

    try:
        csv_text = fetch_csv_export(session, timeout=args.timeout)
    except Exception as exc:  # pragma: no cover - network fallbacks are environment-specific
        logging.warning("CSV export fetch failed: %s", exc)
        save_html_snapshot(session, timeout=args.timeout)
        logging.warning(
            "Saved HTML fallback snapshot only. Full HTML parsing still needs selector validation."
        )
        return 1

    raw_csv_path = RAW_DIR / f"enforcement_export_{utc_stamp()}.csv"
    save_text(raw_csv_path, csv_text)
    logging.info("Saved raw enforcement export: %s", raw_csv_path)

    raw_df = load_export_frame(csv_text)
    normalized_df = normalize_export_frame(raw_df, raw_csv_path)

    csv_output = NORMALIZED_DIR / "enforcement.csv"
    parquet_output = NORMALIZED_DIR / "enforcement.parquet"
    normalized_df.to_csv(csv_output, index=False)
    parquet_written = False
    try:
        normalized_df.to_parquet(parquet_output, index=False)
        parquet_written = True
    except ImportError as exc:
        logging.warning(
            "Parquet output skipped because no parquet engine is installed: %s", exc
        )

    logging.info("Wrote normalized CSV: %s", csv_output)
    if parquet_written:
        logging.info("Wrote normalized parquet: %s", parquet_output)
    logging.info("Row count: %s", len(normalized_df))
    logging.info(
        "Action type counts:\n%s",
        normalized_df["action_type"].value_counts(dropna=False).to_string(),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

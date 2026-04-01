from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

from matching_utils import canonical_company_name, clean_abn, clean_acn, normalize_text

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_ASIC_DIR = BASE_DIR / "raw" / "asic"
OUTPUT_DIR = BASE_DIR / "output"

SEVERE_ACTIONS = {"banning_order", "revocation"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate conservative phoenix-candidate heuristics using only free public data."
    )
    parser.add_argument(
        "--profiles-path",
        type=Path,
        default=OUTPUT_DIR / "entity_profiles.csv",
        help="Entity-level output from src/05_entity_resolution.py.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_DIR / "phoenix_candidates.csv",
        help="Phoenix-candidate CSV output.",
    )
    parser.add_argument(
        "--asic-path",
        type=Path,
        help="Explicit ASIC current-company CSV path. Defaults to latest file in raw/asic/.",
    )
    return parser.parse_args()


def detect_latest_asic_csv(explicit_path: Path | None) -> Path | None:
    if explicit_path:
        return explicit_path
    candidates = sorted(RAW_ASIC_DIR.glob("company_*.csv"))
    return candidates[-1] if candidates else None


def valid_root(name: str) -> bool:
    tokens = name.split()
    return len(tokens) >= 2 and len(name) >= 10


def candidate_name_from_row(row: dict[str, Any]) -> str:
    current_name = normalize_text(row.get("Current Name"))
    return current_name or normalize_text(row.get("Company Name"))


def parse_asic_registration_dates(chunk: pd.DataFrame) -> pd.Series:
    return pd.to_datetime(
        chunk["Date of Registration"],
        errors="coerce",
        dayfirst=True,
    )


def build_severe_root_map(profiles: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    severe = profiles[profiles["most_severe_action"].isin(SEVERE_ACTIONS)].copy()
    severe["source_state"] = severe["source_state"].map(lambda value: normalize_text(value).upper())
    severe["canonical_root"] = severe["resolved_entity_name"].map(canonical_company_name)
    severe.loc[severe["canonical_root"] == "", "canonical_root"] = severe["source_entity_name"].map(
        canonical_company_name
    )
    severe = severe[severe["canonical_root"].map(valid_root)].copy()
    severe["most_recent_action_dt"] = pd.to_datetime(
        severe["most_recent_action_date"],
        errors="coerce",
    )

    root_map: dict[str, list[dict[str, Any]]] = {}
    for record in severe.to_dict(orient="records"):
        root_map.setdefault(record["canonical_root"], []).append(record)
    return root_map


def build_candidate_row(profile: dict[str, Any], asic_row: dict[str, Any]) -> dict[str, Any]:
    candidate_name = candidate_name_from_row(asic_row)
    previous_state = normalize_text(asic_row.get("Previous State of Registration")).upper()
    source_state = normalize_text(profile.get("source_state")).upper()
    registration_text = normalize_text(asic_row.get("Date of Registration"))
    registration_dt = pd.to_datetime(registration_text, errors="coerce", dayfirst=True)
    action_dt = pd.to_datetime(profile.get("most_recent_action_date"), errors="coerce")

    if pd.isna(registration_dt) or pd.isna(action_dt):
        days_after = ""
    else:
        days_after = int((registration_dt - action_dt).days)

    return {
        "entity_key": profile["entity_key"],
        "source_entity_name": profile["source_entity_name"],
        "resolved_entity_name": profile.get("resolved_entity_name", ""),
        "source_abn": clean_abn(profile.get("source_abn")),
        "resolved_abn": clean_abn(profile.get("resolved_abn")),
        "resolved_acn": clean_acn(profile.get("resolved_acn")),
        "source_state": source_state,
        "most_severe_action": profile["most_severe_action"],
        "most_recent_action_date": profile["most_recent_action_date"],
        "candidate_company_name": normalize_text(asic_row.get("Company Name")),
        "candidate_current_name": normalize_text(asic_row.get("Current Name")),
        "candidate_entity_name": candidate_name,
        "candidate_abn": clean_abn(asic_row.get("ABN")),
        "candidate_acn": clean_acn(asic_row.get("ACN")),
        "candidate_status": normalize_text(asic_row.get("Status")),
        "candidate_registration_date": registration_text,
        "candidate_deregistration_date": normalize_text(asic_row.get("Date of Deregistration")),
        "candidate_previous_state_of_registration": previous_state,
        "same_state": (
            "unknown" if not source_state or not previous_state else str(source_state == previous_state)
        ),
        "days_after_enforcement": days_after,
        "similarity_score": 1.0,
        "director_overlap": "unknown",
        "review_status": "candidate",
        "heuristic_basis": "exact_canonical_name_root_and_post_enforcement_registration",
        "cost_path": "$0_public_only",
    }


def main() -> int:
    load_dotenv(BASE_DIR / ".env")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()

    profiles = pd.read_csv(args.profiles_path, dtype=str).fillna("")
    root_map = build_severe_root_map(profiles)
    if not root_map:
        logging.info("No severe entity roots available. Writing empty candidate file.")
        pd.DataFrame().to_csv(args.output, index=False)
        return 0

    asic_path = detect_latest_asic_csv(args.asic_path)
    if not asic_path or not asic_path.exists():
        raise SystemExit("No local ASIC CSV found. Download it first with src/03_download_asic.py --download.")

    logging.info(
        "Scanning ASIC current-company CSV for exact root matches after severe enforcement actions: %s",
        asic_path,
    )

    use_columns = [
        "Company Name",
        "ACN",
        "Status",
        "Date of Registration",
        "Date of Deregistration",
        "Previous State of Registration",
        "Current Name Indicator",
        "ABN",
        "Current Name",
        "Current Name Start Date",
    ]

    candidate_rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()

    for chunk in pd.read_csv(
        asic_path,
        sep="\t",
        dtype=str,
        encoding="utf-8-sig",
        chunksize=100000,
        usecols=use_columns,
        low_memory=False,
    ):
        chunk = chunk.fillna("")
        chunk["candidate_root"] = chunk.apply(
            lambda row: canonical_company_name(candidate_name_from_row(row.to_dict())),
            axis=1,
        )
        chunk = chunk[chunk["candidate_root"].isin(root_map)].copy()
        if chunk.empty:
            continue

        chunk["registration_dt"] = parse_asic_registration_dates(chunk)
        chunk["candidate_abn_digits"] = chunk["ABN"].map(clean_abn)
        chunk["candidate_acn_digits"] = chunk["ACN"].map(clean_acn)

        for row in chunk.to_dict(orient="records"):
            candidate_root = row["candidate_root"]
            registration_dt = row["registration_dt"]
            if pd.isna(registration_dt):
                continue

            for profile in root_map.get(candidate_root, []):
                action_dt = pd.to_datetime(profile.get("most_recent_action_date"), errors="coerce")
                if pd.isna(action_dt) or registration_dt <= action_dt:
                    continue

                original_abn = clean_abn(profile.get("resolved_abn") or profile.get("source_abn"))
                original_acn = clean_acn(profile.get("resolved_acn"))
                candidate_abn = clean_abn(row.get("ABN"))
                candidate_acn = clean_acn(row.get("ACN"))

                if original_abn and candidate_abn and original_abn == candidate_abn:
                    continue
                if original_acn and candidate_acn and original_acn == candidate_acn:
                    continue
                if not candidate_abn and not candidate_acn:
                    continue

                dedupe_key = (
                    profile["entity_key"],
                    candidate_abn,
                    candidate_acn,
                )
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                candidate_rows.append(build_candidate_row(profile, row))

    candidates = pd.DataFrame(candidate_rows)
    if not candidates.empty:
        candidates = candidates.sort_values(
            by=["most_recent_action_date", "candidate_registration_date", "source_entity_name"],
            ascending=[False, False, True],
            na_position="last",
        )
    candidates.to_csv(args.output, index=False)

    logging.info("Wrote %s phoenix candidates to %s", len(candidates), args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

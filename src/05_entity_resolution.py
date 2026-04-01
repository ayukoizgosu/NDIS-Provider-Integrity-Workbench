from __future__ import annotations

import argparse
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

from abn_lookup import ABNLookupClient
from matching_utils import (
    canonical_company_name,
    clean_abn,
    clean_acn,
    extract_alias_variants,
    generate_name_variants,
    has_alias_markers,
    looks_corporate_name,
    name_similarity_score,
    normalize_text,
    token_overlap_score,
)

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_ABN_DIR = BASE_DIR / "raw" / "abn"
RAW_ASIC_DIR = BASE_DIR / "raw" / "asic"
NORMALIZED_DIR = BASE_DIR / "normalized"
OUTPUT_DIR = BASE_DIR / "output"

SEVERITY_ORDER = {
    "other": 0,
    "compliance_notice": 1,
    "enforceable_undertaking": 2,
    "banning_order": 3,
    "revocation": 4,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Resolve enforcement entities using public ABN Lookup and ASIC data."
    )
    parser.add_argument(
        "--enforcement-path",
        type=Path,
        default=NORMALIZED_DIR / "enforcement.csv",
        help="Normalized enforcement CSV from src/01_scrape_enforcement.py.",
    )
    parser.add_argument(
        "--entities-output",
        type=Path,
        default=NORMALIZED_DIR / "entities_enriched.csv",
        help="Row-level enriched enforcement output.",
    )
    parser.add_argument(
        "--profiles-output",
        type=Path,
        default=OUTPUT_DIR / "entity_profiles.csv",
        help="Entity-level resolution output.",
    )
    parser.add_argument(
        "--review-output",
        type=Path,
        default=OUTPUT_DIR / "match_review_queue.csv",
        help="Manual-review queue for uncertain name-based matches.",
    )
    parser.add_argument(
        "--asic-match-output",
        type=Path,
        default=NORMALIZED_DIR / "asic_company_matches.csv",
        help="Exact ASIC company matches for resolved ABNs or ACNs.",
    )
    parser.add_argument(
        "--asic-path",
        type=Path,
        help="Explicit ASIC current-company CSV path. Defaults to latest file in raw/asic/.",
    )
    parser.add_argument(
        "--missing-abn-limit",
        type=int,
        default=75,
        help="Maximum missing-ABN entities to query against ABN Lookup in one run.",
    )
    parser.add_argument(
        "--source-abn-detail-limit",
        type=int,
        default=25,
        help="Maximum source-ABN entities to enrich via ABN detail lookups in one run.",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=5,
        help="Maximum candidates to collect per ABN name-search query.",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=float(os.getenv("NDIS_REQUEST_DELAY_SECONDS", "0.3")),
        help="Delay between live ABN Lookup requests when cache misses occur.",
    )
    parser.add_argument(
        "--skip-asic",
        action="store_true",
        help="Skip ASIC exact-match enrichment even if a local CSV is available.",
    )
    return parser.parse_args()


def load_json_cache(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        logging.warning("Ignoring invalid cache file: %s", path)
        return {}


def save_json_cache(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def detect_latest_asic_csv(explicit_path: Path | None) -> Path | None:
    if explicit_path:
        return explicit_path
    candidates = sorted(RAW_ASIC_DIR.glob("company_*.csv"))
    return candidates[-1] if candidates else None


def canonical_or_fallback(name: object) -> str:
    canonical = canonical_company_name(name)
    return canonical or normalize_text(name).lower()


def build_entity_key(row: pd.Series) -> str:
    abn = clean_abn(row.get("abn"))
    if abn:
        return f"abn:{abn}"
    state = normalize_text(row.get("state")).upper()
    canonical = canonical_or_fallback(row.get("entity_name"))
    return f"name_state:{canonical}|{state}"


def first_non_empty(values: list[str]) -> str:
    for value in values:
        if normalize_text(value):
            return normalize_text(value)
    return ""


def most_common_non_empty(series: pd.Series) -> str:
    values = [normalize_text(value) for value in series if normalize_text(value)]
    if not values:
        return ""
    counts = pd.Series(values).value_counts()
    return str(counts.index[0])


def unique_join(series: pd.Series, limit: int = 8) -> str:
    values: list[str] = []
    for value in series:
        cleaned = normalize_text(value)
        if cleaned and cleaned not in values:
            values.append(cleaned)
        if len(values) >= limit:
            break
    return "; ".join(values)


def to_iso_date(value: pd.Timestamp | None) -> str:
    if value is None or pd.isna(value):
        return ""
    return pd.Timestamp(value).isoformat()


def build_profiles(enforcement: pd.DataFrame) -> pd.DataFrame:
    frame = enforcement.copy()
    frame["abn"] = frame["abn"].map(clean_abn)
    frame["state"] = frame["state"].map(lambda value: normalize_text(value).upper())
    frame["entity_name"] = frame["entity_name"].map(normalize_text)
    frame["entity_key"] = frame.apply(build_entity_key, axis=1)
    frame["canonical_name"] = frame["entity_name"].map(canonical_or_fallback)
    frame["date_effective_dt"] = pd.to_datetime(frame["date_effective"], errors="coerce")

    rows: list[dict[str, Any]] = []
    for entity_key, group in frame.groupby("entity_key", sort=False):
        group = group.copy()
        severity_series = group["action_type"].map(lambda value: SEVERITY_ORDER.get(value, -1))
        severe_index = severity_series.idxmax()

        unique_names = []
        for name in group["entity_name"]:
            if name and name not in unique_names:
                unique_names.append(name)

        source_name = max(unique_names, key=len) if unique_names else ""
        source_abn = first_non_empty(group["abn"].tolist())
        rows.append(
            {
                "entity_key": entity_key,
                "canonical_name": canonical_or_fallback(source_name),
                "source_entity_name": source_name,
                "source_entity_names": "; ".join(unique_names[:8]),
                "source_abn": source_abn,
                "source_state": most_common_non_empty(group["state"]),
                "source_postcode": most_common_non_empty(group["postcode"]),
                "source_city": most_common_non_empty(group["city"]),
                "source_entity_type": most_common_non_empty(group["entity_type"]),
                "action_count": int(len(group)),
                "action_types": unique_join(group["action_type"]),
                "first_action_date": to_iso_date(group["date_effective_dt"].min()),
                "most_recent_action_date": to_iso_date(group["date_effective_dt"].max()),
                "most_severe_action": normalize_text(group.loc[severe_index, "action_type"]),
                "most_severe_action_raw": normalize_text(
                    group.loc[severe_index, "action_type_raw"]
                ),
                "source_row_count": int(len(group)),
                "needs_abn_lookup": not bool(source_abn),
                "resolved_abn": source_abn,
                "resolved_acn": "",
                "resolved_entity_name": source_name,
                "resolved_entity_type": "",
                "resolved_state": most_common_non_empty(group["state"]),
                "resolved_postcode": most_common_non_empty(group["postcode"]),
                "abn_lookup_mode": "",
                "abn_search_query": "",
                "abn_search_variants": "",
                "abn_candidate_count": 0,
                "abn_best_candidate_query": "",
                "abn_best_candidate_name": "",
                "abn_best_candidate_score": 0.0,
                "match_confidence": "source_abn_exact" if source_abn else "",
                "review_reason": "",
                "asic_match_basis": "",
                "asic_company_name": "",
                "asic_current_name": "",
                "asic_status": "",
                "asic_type": "",
                "asic_class": "",
                "asic_sub_class": "",
                "asic_registration_date": "",
                "asic_deregistration_date": "",
                "asic_previous_state_of_registration": "",
                "asic_state_registration_number": "",
                "asic_current_name_indicator": "",
                "asic_current_name_start_date": "",
            }
        )

    return pd.DataFrame(rows)


def cached_name_search(
    client: ABNLookupClient,
    cache: dict[str, Any],
    query: str,
    max_results: int,
    delay_seconds: float,
) -> dict[str, Any]:
    key = json.dumps({"query": normalize_text(query).lower(), "max_results": max_results})
    if key not in cache:
        cache[key] = client.search_name(query, max_results=max_results)
        if delay_seconds > 0:
            time.sleep(delay_seconds)
    return cache[key]


def cached_abn_details(
    client: ABNLookupClient,
    cache: dict[str, Any],
    abn: str,
    delay_seconds: float,
) -> dict[str, Any]:
    abn_digits = clean_abn(abn)
    if abn_digits not in cache:
        cache[abn_digits] = client.get_abn_details(abn_digits)
        if delay_seconds > 0:
            time.sleep(delay_seconds)
    return cache[abn_digits]


def score_candidate(
    source_name: str,
    source_state: str,
    candidate: dict[str, Any],
    matched_query: str,
    match_path: str,
) -> dict[str, Any]:
    candidate_name = normalize_text(candidate.get("name"))
    candidate_state = normalize_text(candidate.get("state")).upper()
    comparison_name = matched_query or source_name
    similarity = name_similarity_score(comparison_name, candidate_name)
    overlap = token_overlap_score(comparison_name, candidate_name)
    exact_canonical = canonical_or_fallback(comparison_name) == canonical_or_fallback(candidate_name)
    state_match = bool(source_state) and bool(candidate_state) and source_state == candidate_state
    state_unknown = not source_state or not candidate_state
    final_score = min(1.0, similarity + (0.05 if state_match else 0.0))

    scored = {
        "abn": clean_abn(candidate.get("abn")),
        "name": candidate_name,
        "entity_type": normalize_text(candidate.get("entity_type")),
        "state": candidate_state,
        "postcode": normalize_text(candidate.get("postcode")),
        "abn_status": normalize_text(candidate.get("abn_status")),
        "detail_url": normalize_text(candidate.get("detail_url")),
        "similarity_score": round(final_score, 4),
        "name_similarity_score": round(similarity, 4),
        "token_overlap_score": round(overlap, 4),
        "exact_canonical_match": exact_canonical,
        "matched_query": normalize_text(matched_query),
        "match_path": match_path,
        "state_match": state_match,
        "state_unknown": state_unknown,
    }
    return scored


def classify_candidate(profile: dict[str, Any], candidate: dict[str, Any]) -> tuple[str, str]:
    source_name = normalize_text(profile.get("source_entity_name"))
    if not candidate:
        return "unresolved_missing_abn", "No candidates returned by ABN Lookup."

    if (
        candidate["match_path"] == "alias"
        and candidate["exact_canonical_match"]
        and (candidate["state_match"] or candidate["state_unknown"])
    ):
        return (
            "searched_alias_exact_review",
            "Exact alias-based ABN match from the free public search flow.",
        )

    if candidate["exact_canonical_match"] and (candidate["state_match"] or candidate["state_unknown"]):
        return "searched_name_exact", "Exact canonical name match from free ABN search."

    if (
        looks_corporate_name(source_name)
        and candidate["exact_canonical_match"]
        and not candidate["state_match"]
        and not candidate["state_unknown"]
    ):
        return (
            "searched_name_exact_state_mismatch_review",
            "Exact legal-name match found, but the ABN main-business state differs from the enforcement state.",
        )

    if (
        candidate["name_similarity_score"] >= 0.92
        and candidate["token_overlap_score"] >= 0.85
        and candidate["state_match"]
    ):
        return "searched_name_probable", "High similarity but not exact canonical match."
    return "searched_name_rejected", "Best ABN candidate did not clear conservative thresholds."


def collect_ranked_candidates(
    source_name: str,
    source_state: str,
    queries: list[str],
    client: ABNLookupClient,
    name_cache: dict[str, Any],
    max_results: int,
    delay_seconds: float,
    match_path: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    candidate_map: dict[str, dict[str, Any]] = {}
    search_modes: list[str] = []

    for query in queries:
        payload = cached_name_search(
            client=client,
            cache=name_cache,
            query=query,
            max_results=max_results,
            delay_seconds=delay_seconds,
        )
        search_modes.append(normalize_text(payload.get("mode")))
        for result in payload.get("results", []):
            scored = score_candidate(
                source_name=source_name,
                source_state=source_state,
                candidate=result,
                matched_query=query,
                match_path=match_path,
            )
            abn = scored["abn"]
            if not abn:
                continue
            existing = candidate_map.get(abn)
            ranking_tuple = (
                scored["exact_canonical_match"],
                scored["state_match"],
                scored["similarity_score"],
                scored["token_overlap_score"],
            )
            existing_tuple = (
                existing["exact_canonical_match"],
                existing["state_match"],
                existing["similarity_score"],
                existing["token_overlap_score"],
            ) if existing else None
            if not existing or ranking_tuple > existing_tuple:
                candidate_map[abn] = scored

    ranked = sorted(
        candidate_map.values(),
        key=lambda item: (
            item["exact_canonical_match"],
            item["state_match"],
            item["similarity_score"],
            item["token_overlap_score"],
        ),
        reverse=True,
    )
    return ranked, search_modes


def apply_abn_details(profile: dict[str, Any], details: dict[str, Any]) -> None:
    profile["resolved_abn"] = clean_abn(details.get("abn")) or profile["resolved_abn"]
    profile["resolved_acn"] = clean_acn(details.get("acn"))
    profile["resolved_entity_name"] = normalize_text(details.get("entity_name")) or profile[
        "resolved_entity_name"
    ]
    profile["resolved_entity_type"] = normalize_text(details.get("entity_type_name"))
    profile["resolved_state"] = normalize_text(details.get("address_state")).upper() or profile[
        "resolved_state"
    ]
    profile["resolved_postcode"] = normalize_text(details.get("address_postcode")) or profile[
        "resolved_postcode"
    ]
    if not profile.get("abn_lookup_mode"):
        profile["abn_lookup_mode"] = normalize_text(details.get("mode"))


def resolve_missing_abns(
    profiles: pd.DataFrame,
    client: ABNLookupClient,
    name_cache: dict[str, Any],
    details_cache: dict[str, Any],
    missing_abn_limit: int,
    max_results: int,
    delay_seconds: float,
) -> pd.DataFrame:
    updates: list[dict[str, Any]] = []
    queued = 0

    for profile in profiles.to_dict(orient="records"):
        if profile["resolved_abn"]:
            updates.append(profile)
            continue

        if queued >= missing_abn_limit:
            profile["match_confidence"] = "missing_abn_skipped_due_limit"
            profile["review_reason"] = "Skipped to keep the public-data run bounded."
            updates.append(profile)
            continue

        queued += 1
        source_name = profile["source_entity_name"]
        source_state = normalize_text(profile["source_state"]).upper()
        variants = generate_name_variants(source_name)[:3]
        alias_variants: list[str] = []
        alias_ranked: list[dict[str, Any]] = []
        ranked, search_modes = collect_ranked_candidates(
            source_name=source_name,
            source_state=source_state,
            queries=variants,
            client=client,
            name_cache=name_cache,
            max_results=max_results,
            delay_seconds=delay_seconds,
            match_path="primary",
        )
        best = ranked[0] if ranked else {}
        classification, reason = classify_candidate(profile, best)

        alias_variants = extract_alias_variants(source_name)[:6]
        if (
            classification in {"searched_name_rejected", "unresolved_missing_abn"}
            and (has_alias_markers(source_name) or ";" in source_name)
            and alias_variants
        ):
            alias_ranked, alias_search_modes = collect_ranked_candidates(
                source_name=source_name,
                source_state=source_state,
                queries=alias_variants,
                client=client,
                name_cache=name_cache,
                max_results=max_results,
                delay_seconds=delay_seconds,
                match_path="alias",
            )
            search_modes.extend(alias_search_modes)
            if alias_ranked:
                alias_best = alias_ranked[0]
                alias_classification, alias_reason = classify_candidate(profile, alias_best)
                alias_tuple = (
                    alias_best["exact_canonical_match"],
                    alias_best["state_match"],
                    alias_best["similarity_score"],
                    alias_best["token_overlap_score"],
                )
                current_tuple = (
                    best.get("exact_canonical_match", False),
                    best.get("state_match", False),
                    best.get("similarity_score", 0.0),
                    best.get("token_overlap_score", 0.0),
                )
                if alias_classification != "searched_name_rejected" and alias_tuple >= current_tuple:
                    best = alias_best
                    classification = alias_classification
                    reason = alias_reason

        profile["abn_search_query"] = variants[0] if variants else ""
        combined_variants = variants + [variant for variant in alias_variants if variant not in variants]
        profile["abn_search_variants"] = "; ".join(combined_variants)
        profile["abn_lookup_mode"] = "; ".join(sorted({mode for mode in search_modes if mode}))
        profile["abn_candidate_count"] = max(len(ranked), len(alias_ranked))
        profile["abn_best_candidate_query"] = normalize_text(best.get("matched_query"))
        profile["abn_best_candidate_name"] = normalize_text(best.get("name"))
        profile["abn_best_candidate_score"] = float(best.get("similarity_score", 0.0))
        profile["match_confidence"] = classification
        profile["review_reason"] = reason

        if classification in {
            "searched_alias_exact_review",
            "searched_name_exact",
            "searched_name_exact_state_mismatch_review",
            "searched_name_probable",
        }:
            profile["resolved_abn"] = clean_abn(best.get("abn"))
            details = cached_abn_details(
                client=client,
                cache=details_cache,
                abn=profile["resolved_abn"],
                delay_seconds=delay_seconds,
            )
            apply_abn_details(profile, details)

        updates.append(profile)

    return pd.DataFrame(updates)


def enrich_source_abn_details(
    profiles: pd.DataFrame,
    client: ABNLookupClient,
    details_cache: dict[str, Any],
    source_abn_detail_limit: int,
    delay_seconds: float,
) -> pd.DataFrame:
    enriched: list[dict[str, Any]] = []
    fetched = 0

    for profile in profiles.to_dict(orient="records"):
        if profile["match_confidence"] != "source_abn_exact":
            enriched.append(profile)
            continue
        if fetched >= source_abn_detail_limit:
            enriched.append(profile)
            continue
        details = cached_abn_details(
            client=client,
            cache=details_cache,
            abn=profile["resolved_abn"],
            delay_seconds=delay_seconds,
        )
        apply_abn_details(profile, details)
        if not profile.get("abn_lookup_mode"):
            profile["abn_lookup_mode"] = normalize_text(details.get("mode"))
        fetched += 1
        enriched.append(profile)

    return pd.DataFrame(enriched)


def normalize_asic_record(row: dict[str, Any]) -> dict[str, str]:
    return {
        "asic_company_name": normalize_text(row.get("Company Name")),
        "asic_current_name": normalize_text(row.get("Current Name")),
        "asic_company_abn": clean_abn(row.get("ABN")),
        "asic_company_acn": clean_acn(row.get("ACN")),
        "asic_status": normalize_text(row.get("Status")),
        "asic_type": normalize_text(row.get("Type")),
        "asic_class": normalize_text(row.get("Class")),
        "asic_sub_class": normalize_text(row.get("Sub Class")),
        "asic_registration_date": normalize_text(row.get("Date of Registration")),
        "asic_deregistration_date": normalize_text(row.get("Date of Deregistration")),
        "asic_previous_state_of_registration": normalize_text(
            row.get("Previous State of Registration")
        ),
        "asic_state_registration_number": normalize_text(row.get("State Registration number")),
        "asic_current_name_indicator": normalize_text(row.get("Current Name Indicator")),
        "asic_current_name_start_date": normalize_text(row.get("Current Name Start Date")),
    }


def asic_preference_score(record: dict[str, str]) -> tuple[int, int, int]:
    return (
        1 if record["asic_current_name_indicator"].upper() == "Y" else 0,
        1 if record["asic_status"].lower() == "registered" else 0,
        0 if record["asic_deregistration_date"] else 1,
    )


def load_exact_asic_matches(asic_path: Path, abns: set[str], acns: set[str]) -> tuple[dict[str, Any], dict[str, Any]]:
    use_columns = [
        "Company Name",
        "ACN",
        "Type",
        "Class",
        "Sub Class",
        "Status",
        "Date of Registration",
        "Date of Deregistration",
        "Previous State of Registration",
        "State Registration number",
        "Current Name Indicator",
        "ABN",
        "Current Name",
        "Current Name Start Date",
    ]
    matches_by_abn: dict[str, dict[str, str]] = {}
    matches_by_acn: dict[str, dict[str, str]] = {}

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
        chunk["ABN"] = chunk["ABN"].map(clean_abn)
        chunk["ACN"] = chunk["ACN"].map(clean_acn)
        subset = chunk[(chunk["ABN"].isin(abns)) | (chunk["ACN"].isin(acns))]
        for row in subset.to_dict(orient="records"):
            normalized = normalize_asic_record(row)
            abn = normalized["asic_company_abn"]
            acn = normalized["asic_company_acn"]
            if abn:
                current = matches_by_abn.get(abn)
                if not current or asic_preference_score(normalized) > asic_preference_score(current):
                    matches_by_abn[abn] = normalized
            if acn:
                current = matches_by_acn.get(acn)
                if not current or asic_preference_score(normalized) > asic_preference_score(current):
                    matches_by_acn[acn] = normalized

    return matches_by_abn, matches_by_acn


def enrich_with_asic(profiles: pd.DataFrame, asic_path: Path | None) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not asic_path or not asic_path.exists():
        logging.info("No ASIC CSV available locally. Skipping exact ASIC enrichment.")
        return profiles.copy(), pd.DataFrame()

    abns = {clean_abn(value) for value in profiles["resolved_abn"] if clean_abn(value)}
    acns = {clean_acn(value) for value in profiles["resolved_acn"] if clean_acn(value)}
    if not abns and not acns:
        logging.info("No resolved ABNs or ACNs available for exact ASIC matching.")
        return profiles.copy(), pd.DataFrame()

    logging.info("Streaming ASIC CSV for exact ABN/ACN matches: %s", asic_path)
    matches_by_abn, matches_by_acn = load_exact_asic_matches(asic_path=asic_path, abns=abns, acns=acns)

    enriched_rows: list[dict[str, Any]] = []
    match_rows: list[dict[str, Any]] = []

    for profile in profiles.to_dict(orient="records"):
        record = None
        basis = ""
        resolved_abn = clean_abn(profile.get("resolved_abn"))
        resolved_acn = clean_acn(profile.get("resolved_acn"))
        if resolved_abn and resolved_abn in matches_by_abn:
            record = matches_by_abn[resolved_abn]
            basis = "abn_exact"
        elif resolved_acn and resolved_acn in matches_by_acn:
            record = matches_by_acn[resolved_acn]
            basis = "acn_exact"

        if record:
            profile.update(record)
            profile["asic_match_basis"] = basis
            match_rows.append({"entity_key": profile["entity_key"], **record, "asic_match_basis": basis})
        enriched_rows.append(profile)

    return pd.DataFrame(enriched_rows), pd.DataFrame(match_rows)


def main() -> int:
    load_dotenv(BASE_DIR / ".env")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    NORMALIZED_DIR.mkdir(parents=True, exist_ok=True)
    RAW_ABN_DIR.mkdir(parents=True, exist_ok=True)

    enforcement = pd.read_csv(args.enforcement_path, dtype=str).fillna("")
    profiles = build_profiles(enforcement)
    logging.info("Built %s entity profiles from %s enforcement rows.", len(profiles), len(enforcement))

    client = ABNLookupClient(timeout=int(os.getenv("REQUEST_TIMEOUT_SECONDS", "30")))
    name_cache_path = RAW_ABN_DIR / "name_search_cache.json"
    details_cache_path = RAW_ABN_DIR / "abn_details_cache.json"
    name_cache = load_json_cache(name_cache_path)
    details_cache = load_json_cache(details_cache_path)

    profiles = resolve_missing_abns(
        profiles=profiles,
        client=client,
        name_cache=name_cache,
        details_cache=details_cache,
        missing_abn_limit=max(args.missing_abn_limit, 0),
        max_results=max(args.max_results, 1),
        delay_seconds=max(args.request_delay, 0.0),
    )
    profiles = enrich_source_abn_details(
        profiles=profiles,
        client=client,
        details_cache=details_cache,
        source_abn_detail_limit=max(args.source_abn_detail_limit, 0),
        delay_seconds=max(args.request_delay, 0.0),
    )

    save_json_cache(name_cache_path, name_cache)
    save_json_cache(details_cache_path, details_cache)

    asic_path = None if args.skip_asic else detect_latest_asic_csv(args.asic_path)
    profiles, asic_matches = enrich_with_asic(profiles, asic_path)

    profiles = profiles.sort_values(
        by=["most_recent_action_date", "source_entity_name"],
        ascending=[False, True],
        na_position="last",
    )
    profiles.to_csv(args.profiles_output, index=False)

    review_queue = profiles[
        profiles["match_confidence"].isin(
            [
                "searched_alias_exact_review",
                "searched_name_exact_state_mismatch_review",
                "searched_name_probable",
                "searched_name_rejected",
                "unresolved_missing_abn",
                "missing_abn_skipped_due_limit",
            ]
        )
    ].copy()
    review_queue.to_csv(args.review_output, index=False)

    if not asic_matches.empty:
        asic_matches.to_csv(args.asic_match_output, index=False)
    elif args.asic_match_output.exists():
        args.asic_match_output.unlink()

    enriched = enforcement.copy()
    enriched["abn"] = enriched["abn"].map(clean_abn)
    enriched["state"] = enriched["state"].map(lambda value: normalize_text(value).upper())
    enriched["entity_name"] = enriched["entity_name"].map(normalize_text)
    enriched["entity_key"] = enriched.apply(build_entity_key, axis=1)
    enriched = enriched.merge(profiles, on="entity_key", how="left", suffixes=("", "_profile"))
    enriched.to_csv(args.entities_output, index=False)

    counts = profiles["match_confidence"].value_counts(dropna=False).to_dict()
    logging.info("Resolution summary: %s", counts)
    logging.info("Wrote entity profiles: %s", args.profiles_output)
    logging.info("Wrote review queue: %s", args.review_output)
    logging.info("Wrote enriched enforcement rows: %s", args.entities_output)
    if asic_path:
        logging.info("ASIC path used: %s", asic_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

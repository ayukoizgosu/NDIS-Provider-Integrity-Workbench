from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd
import requests

try:
    from src.abn_lookup import ABNLookupClient, HTML_ABN_VIEW_URL
    from src.matching_utils import clean_abn, clean_acn, normalize_text
except ImportError:
    from abn_lookup import ABNLookupClient, HTML_ABN_VIEW_URL
    from matching_utils import clean_abn, clean_acn, normalize_text

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_ASIC_DIR = BASE_DIR / "raw" / "asic"

PACKAGE_SHOW_URL = "https://data.gov.au/data/api/3/action/package_show"
PACKAGE_ID = "asic-companies"


def text_value(value: Any) -> str:
    if value is None:
        return ""
    try:
        if value != value:
            return ""
    except Exception:
        pass
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            number = float(value)
            if number.is_integer():
                return str(int(number))
    except Exception:
        pass
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def format_date(value: Any) -> str:
    text = text_value(value)
    if not text:
        return "Not available"
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).strftime("%d %b %Y")
        except ValueError:
            continue
    return text


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


def choose_current_resource(resources: list[dict[str, Any]], preferred_format: str = "CSV") -> dict[str, Any]:
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
    return {}


def detect_latest_asic_csv() -> Path | None:
    candidates = sorted(RAW_ASIC_DIR.glob("company_*.csv"))
    return candidates[-1] if candidates else None


def normalize_asic_record(row: Mapping[str, Any]) -> dict[str, str]:
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
        "asic_previous_state_of_registration": normalize_text(row.get("Previous State of Registration")),
        "asic_state_registration_number": normalize_text(row.get("State Registration number")),
        "asic_current_name_indicator": normalize_text(row.get("Current Name Indicator")),
        "asic_current_name_start_date": normalize_text(row.get("Current Name Start Date")),
    }


def asic_preference_score(record: Mapping[str, str]) -> tuple[int, int, int]:
    return (
        1 if text_value(record.get("asic_current_name_indicator")).upper() == "Y" else 0,
        1 if text_value(record.get("asic_status")).upper() == "REGD" else 0,
        0 if text_value(record.get("asic_deregistration_date")) else 1,
    )


def load_exact_asic_match(asic_path: Path | None, *, abn: str = "", acn: str = "") -> dict[str, str]:
    if not asic_path or not asic_path.exists():
        return {}
    abn_value = clean_abn(abn)
    acn_value = clean_acn(acn)
    if not abn_value and not acn_value:
        return {}

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
    best_match: dict[str, str] = {}
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
        abn_mask = chunk["ABN"] == abn_value if abn_value else pd.Series(False, index=chunk.index)
        acn_mask = chunk["ACN"] == acn_value if acn_value else pd.Series(False, index=chunk.index)
        subset = chunk[abn_mask | acn_mask]
        for row in subset.to_dict(orient="records"):
            normalized = normalize_asic_record(row)
            if not best_match or asic_preference_score(normalized) > asic_preference_score(best_match):
                best_match = normalized
    return best_match


def fetch_asic_resource_summary(timeout_seconds: int) -> dict[str, str]:
    session = build_session()
    try:
        response = session.get(
            PACKAGE_SHOW_URL,
            params={"id": PACKAGE_ID},
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        result = payload.get("result") or {}
        resource = choose_current_resource(result.get("resources") or [])
        return {
            "resource_name": text_value(resource.get("name")),
            "resource_url": text_value(resource.get("url")),
            "resource_last_modified": text_value(resource.get("last_modified") or resource.get("metadata_modified")),
            "resource_format": text_value(resource.get("format")),
            "package_title": text_value(result.get("title")),
            "fetched_at": utc_now_iso(),
        }
    except Exception as exc:
        return {
            "resource_name": "",
            "resource_url": "",
            "resource_last_modified": "",
            "resource_format": "",
            "package_title": "",
            "fetched_at": utc_now_iso(),
            "error": f"{type(exc).__name__}: {exc}",
        }


def build_related_business_briefs(related_records: Iterable[Mapping[str, Any]] | None) -> list[dict[str, str]]:
    briefs: list[dict[str, str]] = []
    rows = list(related_records or [])
    def sort_key(row: Mapping[str, Any]) -> tuple[int, str]:
        days_text = text_value(row.get("days_after_enforcement"))
        try:
            days_value = int(float(days_text)) if days_text else 999999
        except Exception:
            days_value = 999999
        return days_value, text_value(row.get("candidate_registration_date"))
    rows.sort(key=sort_key)
    for row in rows:
        name = (
            text_value(row.get("candidate_current_name"))
            or text_value(row.get("candidate_company_name"))
            or text_value(row.get("candidate_entity_name"))
            or "Unnamed related business"
        )
        abn = text_value(row.get("candidate_abn"))
        acn = text_value(row.get("candidate_acn"))
        status = text_value(row.get("candidate_status")) or "Unknown status"
        registration_date = format_date(row.get("candidate_registration_date"))
        days_after = text_value(row.get("days_after_enforcement"))
        same_state = text_value(row.get("same_state")).lower()
        state_note = "same-state registration" if same_state == "yes" else "different-state registration" if same_state == "no" else "state relationship not confirmed"
        summary = (
            f"{name} ({'ABN ' + abn if abn else 'ABN not listed'}"
            f"{', ACN ' + acn if acn else ''}) is listed as {status}. "
            f"It was registered on {registration_date}"
            f"{f', {days_after} days after the enforcement action' if days_after else ''}, with {state_note}."
        )
        briefs.append(
            {
                "name": name,
                "abn": abn,
                "acn": acn,
                "status": status,
                "registration_date": registration_date,
                "days_after_enforcement": days_after,
                "state_note": state_note,
                "summary": summary,
            }
        )
    return briefs


def build_snapshot_markdown(
    entity_row: Mapping[str, Any],
    *,
    abn_details: Mapping[str, Any],
    asic_record: Mapping[str, Any],
    asic_resource: Mapping[str, Any],
    related_briefs: list[Mapping[str, str]],
) -> str:
    lines = [
        f"# Public Register Refresh",
        "",
        f"- Refreshed at: {utc_now_iso()}",
        f"- Name on notice: {text_value(entity_row.get('source_entity_name')) or 'Not available'}",
        f"- Most serious action: {text_value(entity_row.get('most_severe_action')) or 'Not available'}",
        f"- Latest action date: {format_date(entity_row.get('most_recent_action_date'))}",
        "",
        "## ABR Check",
        f"- ABN checked: {text_value(abn_details.get('abn')) or text_value(entity_row.get('resolved_abn')) or text_value(entity_row.get('source_abn')) or 'Not available'}",
        f"- Entity name: {text_value(abn_details.get('entity_name')) or 'Not returned'}",
        f"- ABN status: {text_value(abn_details.get('abn_status')) or 'Not returned'}",
        f"- Status effective from: {format_date(abn_details.get('abn_status_effective_from'))}",
        f"- GST status: {text_value(abn_details.get('gst_raw')) or 'Not returned'}",
        f"- Main business location: {text_value(abn_details.get('address_state'))} {text_value(abn_details.get('address_postcode'))}".strip() or "Not returned",
        f"- Source URL: {HTML_ABN_VIEW_URL}?abn={text_value(abn_details.get('abn')) or text_value(entity_row.get('resolved_abn')) or text_value(entity_row.get('source_abn'))}",
        f"- Raw payload path: {text_value(abn_details.get('raw_path')) or 'Not saved'}",
        "",
        "## ASIC Check",
        f"- Local ASIC snapshot: {text_value(asic_resource.get('resource_name')) or 'Not available'}",
        f"- Snapshot modified: {format_date(asic_resource.get('resource_last_modified'))}",
        f"- Snapshot URL: {text_value(asic_resource.get('resource_url')) or 'Not available'}",
        f"- Company name: {text_value(asic_record.get('asic_company_name')) or text_value(asic_record.get('asic_current_name')) or 'No exact local match'}",
        f"- ACN: {text_value(asic_record.get('asic_company_acn')) or 'Not available'}",
        f"- Status: {text_value(asic_record.get('asic_status')) or 'Not available'}",
        f"- Registration date: {format_date(asic_record.get('asic_registration_date'))}",
        f"- Deregistration date: {format_date(asic_record.get('asic_deregistration_date'))}",
        "",
        "## Related Business Leads",
    ]
    if related_briefs:
        lines.extend(f"- {brief.get('summary')}" for brief in related_briefs)
    else:
        lines.append("- No related-business leads were attached to this refresh.")
    return "\n".join(lines) + "\n"


def run_public_record_refresh(
    entity_row: Mapping[str, Any],
    *,
    related_records: Iterable[Mapping[str, Any]] | None = None,
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    resolved_abn = text_value(entity_row.get("resolved_abn")) or text_value(entity_row.get("source_abn"))
    resolved_acn = text_value(entity_row.get("resolved_acn")) or text_value(entity_row.get("asic_company_acn"))
    related_briefs = build_related_business_briefs(related_records)

    abn_details: dict[str, Any] = {}
    abn_error = ""
    if clean_abn(resolved_abn):
        try:
            client = ABNLookupClient(timeout=timeout_seconds)
            abn_details = client.get_abn_details(resolved_abn)
        except Exception as exc:
            abn_error = f"{type(exc).__name__}: {exc}"

    asic_resource = fetch_asic_resource_summary(timeout_seconds)
    asic_path = detect_latest_asic_csv()
    asic_record = load_exact_asic_match(asic_path, abn=resolved_abn, acn=resolved_acn)

    summary_lines: list[str] = []
    if abn_details:
        summary_lines.append(
            f"ABR live check returned entity {text_value(abn_details.get('entity_name')) or clean_abn(resolved_abn)} with status {text_value(abn_details.get('abn_status')) or 'not returned'}."
        )
    elif clean_abn(resolved_abn):
        summary_lines.append(f"ABR live check could not be completed for ABN {clean_abn(resolved_abn)} ({abn_error or 'no details returned'}).")
    else:
        summary_lines.append("No ABN was available for a live ABR refresh.")

    if asic_record:
        summary_lines.append(
            f"Latest local ASIC snapshot matched {text_value(asic_record.get('asic_company_name')) or text_value(asic_record.get('asic_current_name'))} with status {text_value(asic_record.get('asic_status')) or 'not returned'}."
        )
    elif text_value(asic_resource.get("resource_url")):
        summary_lines.append("Current ASIC dataset metadata was refreshed, but no exact local company match was attached to this case.")
    else:
        summary_lines.append("ASIC dataset metadata could not be refreshed from data.gov.au.")

    if related_briefs:
        summary_lines.append(f"Prepared {len(related_briefs)} related-business mini-brief(s) for the case pack.")
    else:
        summary_lines.append("No related-business mini-briefs were generated for this case.")

    context = {
        "abn_details": {
            "abn": text_value(abn_details.get("abn")) or clean_abn(resolved_abn),
            "entity_name": text_value(abn_details.get("entity_name")),
            "abn_status": text_value(abn_details.get("abn_status")),
            "abn_status_effective_from": text_value(abn_details.get("abn_status_effective_from")),
            "acn": text_value(abn_details.get("acn")),
            "entity_type_name": text_value(abn_details.get("entity_type_name")),
            "address_state": text_value(abn_details.get("address_state")),
            "address_postcode": text_value(abn_details.get("address_postcode")),
            "gst_raw": text_value(abn_details.get("gst_raw")),
            "raw_path": text_value(abn_details.get("raw_path")),
            "source_url": f"{HTML_ABN_VIEW_URL}?abn={text_value(abn_details.get('abn')) or clean_abn(resolved_abn)}" if clean_abn(resolved_abn) else "",
            "error": abn_error,
        },
        "asic_record": dict(asic_record),
        "asic_resource": dict(asic_resource),
        "related_business_briefs": related_briefs,
    }

    snapshot_markdown = build_snapshot_markdown(
        entity_row,
        abn_details=abn_details,
        asic_record=asic_record,
        asic_resource=asic_resource,
        related_briefs=related_briefs,
    )

    source_rows = []
    if clean_abn(resolved_abn):
        source_rows.append(
            {
                "source_name": "ABR live refresh",
                "source_type": "Live public business register",
                "source_ref": f"ABN {clean_abn(resolved_abn)}",
                "source_url": f"{HTML_ABN_VIEW_URL}?abn={clean_abn(resolved_abn)}",
            }
        )
    if text_value(asic_resource.get("resource_url")):
        source_rows.append(
            {
                "source_name": "ASIC current dataset metadata",
                "source_type": "Public company dataset",
                "source_ref": text_value(asic_resource.get("resource_name")) or "Current ASIC resource",
                "source_url": text_value(asic_resource.get("resource_url")),
            }
        )

    return {
        "summary": "\n".join(f"- {line}" for line in summary_lines if line),
        "context": context,
        "snapshot_markdown": snapshot_markdown,
        "snapshot_file_name": f"public-register-refresh-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.md",
        "source_rows": source_rows,
    }

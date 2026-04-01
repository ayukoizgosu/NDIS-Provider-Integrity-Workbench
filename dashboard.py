from __future__ import annotations

from datetime import datetime
from html import escape
from pathlib import Path
from typing import Any, Mapping

import altair as alt
import pandas as pd
import streamlit as st

from src.case_store import (
    AGENT_REVIEW_STATUSES,
    ALLOWED_PRIORITIES,
    ALLOWED_ROLES,
    ALLOWED_STATUSES,
    add_attachment,
    add_note,
    build_case_summary,
    create_case_from_entity,
    get_default_user,
    get_case_by_entity_key,
    get_case_metrics,
    get_user,
    init_db,
    list_attachments,
    list_cases,
    list_events,
    list_notes,
    list_sources,
    list_users,
    refresh_case_public_registers,
    refresh_agent_draft,
    replace_case_sources,
    review_agent_draft,
    text_value,
    touch_user,
    update_case,
)
from src.export_case_brief import export_case_brief
from src.llm_case_agent import get_agent_diagnostics

BASE_DIR = Path(__file__).resolve().parent
DOCS_DIR = BASE_DIR / "docs"
OUTPUT_DIR = BASE_DIR / "output"
NORMALIZED_DIR = BASE_DIR / "normalized"
SAMPLE_DATA_DIR = BASE_DIR / "sample_data"
SAMPLE_OUTPUT_DIR = SAMPLE_DATA_DIR / "output"
SAMPLE_NORMALIZED_DIR = SAMPLE_DATA_DIR / "normalized"

APP_TITLE = "NDIS Provider Integrity Workbench"
DEFAULT_ANALYST = "Demo Analyst"
ACTIVE_VIEW_KEY = "active_view"
PENDING_VIEW_KEY = "pending_view"
FLASH_MESSAGE_KEY = "flash_message"
NAV_OPTIONS = [
    "Overview",
    "Look Up Record",
    "Needs Review",
    "Case Desk",
    "Related Businesses",
    "About This Tool",
]
ABN_LOOKUP_URL = "https://abr.business.gov.au/ABN/View"
ASIC_DATASET_URL = "https://data.gov.au/data/dataset/asic-companies"
NDIS_EXPORT_URL = (
    "https://www.ndiscommission.gov.au/about-us/compliance-and-enforcement/"
    "compliance-actions/search/export"
)
DECISION_OPTIONS = [
    "No decision yet",
    "No action",
    "Monitor",
    "Escalate for review",
    "Needs more evidence",
]

AGENT_REVIEW_ACTIONS = [
    "Keep current review status",
    "Accept agent draft",
    "Save with human edits",
    "Reject agent draft",
]

ACTION_LABELS = {
    "banning_order": "Banning order",
    "compliance_notice": "Compliance notice",
    "enforceable_undertaking": "Enforceable undertaking",
    "other": "Other action",
    "revocation": "Registration revoked",
}

MATCH_STATUS_SHORT = {
    "source_abn_exact": "Confirmed from the notice",
    "searched_name_exact": "Matched by exact legal name",
    "searched_name_exact_state_mismatch_review": "Matched by name - state check needed",
    "searched_alias_exact_review": "Matched using another known name",
    "searched_name_probable": "Possible match - review needed",
    "searched_name_rejected": "No safe automatic match",
    "unresolved_missing_abn": "No public match found",
    "missing_abn_skipped_due_limit": "Not checked in this run",
}

MATCH_STATUS_LONG = {
    "source_abn_exact": "This record was matched directly from the ABN already listed on the enforcement notice.",
    "searched_name_exact": "This record was matched by the exact legal name found in public business records.",
    "searched_name_exact_state_mismatch_review": "The legal name matches exactly, but the state in the business record differs from the notice and should be checked by a person.",
    "searched_alias_exact_review": "The record was found through an alternate or alias name and should stay visible for manual review.",
    "searched_name_probable": "The suggested business record looks close, but it still needs a person to confirm it.",
    "searched_name_rejected": "A public search returned candidates, but none were safe enough to accept automatically.",
    "unresolved_missing_abn": "No suitable public business match was found for this record.",
    "missing_abn_skipped_due_limit": "This record was not searched in this run because lookups were intentionally capped.",
}

REVIEW_REASON_LABELS = {
    "Best ABN candidate did not clear conservative thresholds.": "A likely business was found, but the public evidence was not strong enough to confirm it automatically.",
    "Exact canonical name match from free ABN search.": "A matching business name was found in the public ABN register.",
    "No candidates returned by ABN Lookup.": "No matching public business record was found.",
    "Exact alias-based ABN match from the free public search flow.": "A matching business was found using another known name.",
    "Exact legal-name match found, but the ABN main-business state differs from the enforcement state.": "The business name matches, but the recorded state differs and should be checked by a person.",
    "High similarity but not exact canonical match.": "A similar business name was found, but it is not strong enough to confirm automatically.",
}

REGISTER_LINK_LABELS = {
    "abn_exact": "Found in the company register by ABN",
    "acn_exact": "Found in the company register by ACN",
}

ASIC_STATUS_LABELS = {
    "REGD": "Registered",
    "DRGD": "Deregistered",
    "EXAD": "External administration",
    "SOFF": "Struck off",
}

SAME_STATE_LABELS = {
    "True": "Yes",
    "False": "No",
    "unknown": "Not enough detail",
    "": "Not enough detail",
}

REVIEW_PRIORITY_ORDER = {
    "searched_name_exact_state_mismatch_review": 0,
    "searched_alias_exact_review": 1,
    "searched_name_probable": 2,
    "unresolved_missing_abn": 3,
    "searched_name_rejected": 4,
    "missing_abn_skipped_due_limit": 5,
}


st.set_page_config(
    page_title=APP_TITLE,
    page_icon=":mag:",
    layout="wide",
    initial_sidebar_state="expanded",
)


def inject_css() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Fraunces:wght@500;700&family=Space+Grotesk:wght@400;500;700&display=swap');

        .stApp {
            background:
                radial-gradient(circle at top left, rgba(207, 109, 63, 0.16), transparent 28%),
                radial-gradient(circle at top right, rgba(34, 99, 94, 0.14), transparent 26%),
                linear-gradient(180deg, #f8f2e8 0%, #efe4d3 100%);
            color: #1e2b29;
            font-family: "Space Grotesk", "Segoe UI", sans-serif;
        }

        header[data-testid="stHeader"] {
            background: transparent;
        }

        [data-testid="stToolbar"],
        #MainMenu,
        footer {
            display: none !important;
        }

        h1, h2, h3, .hero-title {
            font-family: "Fraunces", Georgia, serif;
            letter-spacing: -0.02em;
            color: #14312d;
        }

        section[data-testid="stSidebar"] {
            background: rgba(20, 49, 45, 0.93);
            border-right: 1px solid rgba(255, 255, 255, 0.08);
        }

        section[data-testid="stSidebar"] * {
            color: #f8f2e8;
        }

        section[data-testid="stSidebar"] [data-baseweb="input"],
        section[data-testid="stSidebar"] [data-baseweb="select"] > div,
        section[data-testid="stSidebar"] input,
        section[data-testid="stSidebar"] textarea {
            background: #f8f2e8 !important;
            color: #14312d !important;
            -webkit-text-fill-color: #14312d !important;
            caret-color: #14312d !important;
            border-radius: 14px !important;
        }

        section[data-testid="stSidebar"] input::placeholder,
        section[data-testid="stSidebar"] textarea::placeholder {
            color: #6d736f !important;
            -webkit-text-fill-color: #6d736f !important;
            opacity: 1 !important;
        }

        section[data-testid="stSidebar"] [data-baseweb="tag"] {
            background: rgba(20, 49, 45, 0.12) !important;
            color: #14312d !important;
        }

        section[data-testid="stSidebar"] button[data-testid="stBaseButton-primaryFormSubmit"] {
            background: #d8783f !important;
            border: 1px solid #d8783f !important;
            color: #fffaf2 !important;
            border-radius: 14px !important;
            font-weight: 700 !important;
            min-height: 48px !important;
            box-shadow: 0 10px 20px rgba(0, 0, 0, 0.14);
        }

        section[data-testid="stSidebar"] button[data-testid="stBaseButton-primaryFormSubmit"] p,
        section[data-testid="stSidebar"] button[data-testid="stBaseButton-primaryFormSubmit"] span {
            color: #fffaf2 !important;
        }

        section[data-testid="stSidebar"] button[data-testid="stBaseButton-secondaryFormSubmit"] {
            background: #f8f2e8 !important;
            border: 1px solid rgba(20, 49, 45, 0.18) !important;
            color: #14312d !important;
            border-radius: 14px !important;
            font-weight: 700 !important;
            min-height: 48px !important;
        }

        section[data-testid="stSidebar"] button[data-testid="stBaseButton-secondaryFormSubmit"] p,
        section[data-testid="stSidebar"] button[data-testid="stBaseButton-secondaryFormSubmit"] span {
            color: #14312d !important;
        }

        section[data-testid="stSidebar"] button[data-testid="stBaseButton-secondaryFormSubmit"]:hover {
            border-color: #d8783f !important;
            color: #14312d !important;
        }

        section[data-testid="stSidebar"] button[data-testid="stBaseButton-primaryFormSubmit"]:hover {
            background: #c96d3f !important;
            border-color: #c96d3f !important;
        }

        .block-container {
            padding-top: 1.25rem;
            padding-bottom: 2rem;
        }

        .hero-shell {
            background: linear-gradient(135deg, rgba(20, 49, 45, 0.96), rgba(27, 72, 66, 0.92));
            border: 1px solid rgba(20, 49, 45, 0.08);
            border-radius: 28px;
            padding: 1.5rem 1.6rem 1.35rem 1.6rem;
            color: #f8f2e8;
            box-shadow: 0 18px 40px rgba(20, 49, 45, 0.14);
            margin-bottom: 1rem;
        }

        .hero-title {
            font-size: 2.25rem;
            color: #f8f2e8;
            margin-bottom: 0.35rem;
        }

        .hero-copy {
            font-size: 1rem;
            color: rgba(248, 242, 232, 0.88);
            max-width: 58rem;
            line-height: 1.5;
        }

        .metric-card {
            background: linear-gradient(180deg, rgba(255, 250, 244, 0.96), rgba(255, 246, 236, 0.86));
            border: 1px solid rgba(20, 49, 45, 0.08);
            border-radius: 22px;
            padding: 1rem 1rem 1rem 1rem;
            box-shadow: 0 14px 28px rgba(59, 48, 40, 0.05);
            min-height: 144px;
            display: flex;
            flex-direction: column;
            justify-content: flex-start;
            gap: 0.35rem;
            position: relative;
            overflow: hidden;
        }

        .metric-card::before {
            content: "";
            position: absolute;
            inset: 0 auto auto 0;
            width: 100%;
            height: 4px;
            background: linear-gradient(90deg, rgba(201, 109, 63, 0.95), rgba(31, 74, 67, 0.7));
        }

        .metric-label {
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: #8a5d43;
            margin-bottom: 0.35rem;
        }

        .metric-value {
            font-family: "Fraunces", Georgia, serif;
            font-size: 2rem;
            line-height: 1.1;
            color: #173734;
            margin-bottom: 0.2rem;
        }

        .metric-note {
            font-size: 0.92rem;
            color: #425652;
            line-height: 1.45;
            margin-top: auto;
        }

        .section-note {
            background: rgba(255, 250, 244, 0.75);
            border-left: 4px solid #c96d3f;
            border-radius: 12px;
            padding: 0.9rem 1rem;
            color: #30413f;
            margin: 0.5rem 0 1rem 0;
            line-height: 1.5;
        }

        .chart-shell {
            margin: 0.25rem 0 0.25rem 0;
        }

        .chart-heading {
            font-family: "Fraunces", Georgia, serif;
            font-size: 1.6rem;
            color: #14312d;
            margin: 0 0 0.2rem 0;
        }

        .chart-copy {
            font-size: 0.92rem;
            color: #526562;
            margin: 0 0 0.65rem 0;
            line-height: 1.45;
        }

        div[data-testid="stVegaLiteChart"] {
            background: linear-gradient(180deg, rgba(255, 250, 244, 0.9), rgba(255, 246, 236, 0.8));
            border: 1px solid rgba(20, 49, 45, 0.08);
            border-radius: 22px;
            padding: 0.8rem 0.9rem 0.4rem 0.7rem;
            box-shadow: 0 14px 28px rgba(59, 48, 40, 0.05);
        }

        .detail-panel {
            background: rgba(255, 250, 244, 0.82);
            border: 1px solid rgba(20, 49, 45, 0.08);
            border-radius: 20px;
            padding: 1rem 1.1rem;
            box-shadow: 0 12px 24px rgba(59, 48, 40, 0.05);
            min-height: 100%;
        }

        .detail-panel h4 {
            margin: 0 0 0.75rem 0;
            font-family: "Fraunces", Georgia, serif;
            font-size: 1.55rem;
            color: #173734;
        }

        .detail-row {
            display: grid;
            grid-template-columns: minmax(140px, 180px) 1fr;
            gap: 0.8rem;
            padding: 0.4rem 0;
            border-top: 1px solid rgba(20, 49, 45, 0.07);
        }

        .detail-row:first-of-type {
            border-top: 0;
            padding-top: 0;
        }

        .detail-label {
            font-size: 0.82rem;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            color: #8a5d43;
        }

        .detail-value {
            font-size: 0.98rem;
            color: #1f322f;
            line-height: 1.45;
        }

        [data-testid="stDataFrame"] {
            border-radius: 18px;
            overflow: hidden;
            border: 1px solid rgba(20, 49, 45, 0.08);
            box-shadow: 0 12px 24px rgba(59, 48, 40, 0.05);
        }

        div[data-baseweb="select"] > div,
        div[data-baseweb="input"] > div,
        .stMultiSelect div[data-baseweb="tag"] {
            border-radius: 14px;
        }

        div[data-testid="stRadio"] > div {
            gap: 0.5rem;
        }

        div[data-testid="stRadio"] label {
            font-weight: 600;
            background: rgba(255, 250, 244, 0.82);
            border: 1px solid rgba(20, 49, 45, 0.1);
            border-radius: 999px;
            padding: 0.45rem 0.9rem;
        }

        div[data-testid="stRadio"] label:has(input:checked) {
            background: #173734;
            border-color: #173734;
        }

        div[data-testid="stRadio"] label:has(input:checked) p {
            color: #f8f2e8 !important;
        }

        .stButton > button {
            border-radius: 14px;
            min-height: 44px;
            font-weight: 600;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def empty_text(value: object, fallback: str = "Not available") -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    return text if text else fallback


def safe_int(value: object) -> int:
    if value is None or value == "" or pd.isna(value):
        return 0
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def truncate_text(value: object, limit: int = 180) -> str:
    text = empty_text(value, "")
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "..."


def format_date(value: object) -> str:
    if value is None or value == "" or pd.isna(value):
        return "Not available"
    text = empty_text(value, "")
    timestamp = pd.NaT
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y"):
        try:
            timestamp = pd.Timestamp(datetime.strptime(text, fmt))
            break
        except Exception:
            continue
    if pd.isna(timestamp):
        timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return str(value)
    return timestamp.strftime("%d %b %Y")


def format_score(value: object) -> str:
    if value is None or value == "" or pd.isna(value):
        return "Not scored"
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return "Not scored"


def text_to_bullets(value: object, fallback: str) -> list[str]:
    text = text_value(value)
    if not text:
        return [fallback]
    bullets: list[str] = []
    for raw_line in text.replace("\r", "\n").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith(("- ", "* ", "\u2022 ")):
            line = line[2:].strip()
        bullets.append(line)
    return bullets or [fallback]


def related_business_briefs_from_frame(frame: pd.DataFrame) -> list[str]:
    if frame.empty:
        return []
    working = frame.copy()
    if "days_after_enforcement" in working.columns:
        working["_sort_days"] = pd.to_numeric(working["days_after_enforcement"], errors="coerce")
        working = working.sort_values(["_sort_days", "candidate_registration_date"], ascending=[True, True], na_position="last")
    briefs: list[str] = []
    for _, row in working.head(5).iterrows():
        name = empty_text(row.get("candidate_entity_name"), "Unnamed related business")
        abn = empty_text(row.get("candidate_abn"), "ABN not listed")
        status = empty_text(row.get("candidate_status"), "Status not listed")
        registration_date = format_date(row.get("candidate_registration_date"))
        days_after = empty_text(row.get("days_after_enforcement"), "")
        same_state = empty_text(row.get("same_state"), "").lower()
        state_note = "same-state registration" if same_state == "yes" else "different-state registration" if same_state == "no" else "state relationship not confirmed"
        details = f"{name} ({abn}) is listed as {status}. Registered on {registration_date}"
        if days_after:
            details += f", {safe_int(days_after)} days after the enforcement action"
        details += f", with {state_note}."
        briefs.append(details)
    return briefs


def queue_flash_message(message: str, level: str = "info") -> None:
    st.session_state[FLASH_MESSAGE_KEY] = {"message": message, "level": level}


def render_flash_message() -> None:
    payload = st.session_state.pop(FLASH_MESSAGE_KEY, None)
    if not payload:
        return
    level = str(payload.get("level", "info")).lower()
    message = text_value(payload.get("message"))
    if not message:
        return
    if level == "success":
        st.success(message)
    elif level == "warning":
        st.warning(message)
    elif level == "error":
        st.error(message)
    else:
        st.info(message)


def jump_to_view(view_name: str, *, selected_case_id: str | None = None, message: str | None = None, level: str = "info") -> None:
    st.session_state[PENDING_VIEW_KEY] = view_name
    if selected_case_id:
        st.session_state["selected_case_id"] = selected_case_id
    if message:
        queue_flash_message(message, level=level)
    st.rerun()


def friendly_action(value: object) -> str:
    return ACTION_LABELS.get(str(value), empty_text(value))


def friendly_match_short(value: object) -> str:
    return MATCH_STATUS_SHORT.get(str(value), empty_text(value, "Not classified"))


def friendly_match_long(value: object, fallback_reason: object = "") -> str:
    if fallback_reason and str(fallback_reason).strip():
        reason = str(fallback_reason).strip()
        return REVIEW_REASON_LABELS.get(reason, reason)
    return MATCH_STATUS_LONG.get(str(value), "No explanation available.")


def friendly_register_link(value: object) -> str:
    return REGISTER_LINK_LABELS.get(str(value), "No company register match yet")


def friendly_same_state(value: object) -> str:
    return SAME_STATE_LABELS.get(str(value), empty_text(value, "Not enough detail"))


def friendly_entity_type(value: object) -> str:
    text = empty_text(value, "")
    if not text:
        return "Not listed"
    return text.replace("_", " ").title()


def friendly_asic_status(value: object) -> str:
    return ASIC_STATUS_LABELS.get(str(value), empty_text(value, "Not linked yet"))


def display_source_ref(value: object) -> str:
    text = empty_text(value, "")
    if not text:
        return ""
    if "\\" in text or "/" in text:
        return Path(text).name
    return text


def friendly_agent_review_status(value: object) -> str:
    status = empty_text(value, "")
    if status in AGENT_REVIEW_STATUSES:
        return status
    return "Pending review"


def friendly_actor_type(value: object) -> str:
    actor_type = empty_text(value, "").lower()
    if actor_type == "agent":
        return "Agent"
    if actor_type == "human":
        return "Human"
    if actor_type == "system":
        return "System"
    return "Human"


def friendly_event_actor(event: Mapping[str, Any]) -> str:
    actor_type = empty_text(event.get("actor_type"), "").lower()
    actor = empty_text(event.get("actor"), "")
    if actor_type == "agent":
        if actor in {"deterministic-v1", "Rules Engine"}:
            return "Rules engine"
        if actor:
            return f"LLM ({actor})"
        return "Agent"
    if actor_type == "system":
        return "System"
    return actor or "Analyst"


def case_next_step(case_record: Mapping[str, Any]) -> str:
    decision = empty_text(case_record.get("decision"), "")
    if decision:
        return decision
    agent_next = empty_text(case_record.get("agent_next_step"), "")
    if agent_next:
        return agent_next
    status = empty_text(case_record.get("status"), "")
    if status == "Escalate":
        return "Supervisor review"
    if status == "Monitor":
        return "Monitor only"
    if status == "Closed":
        return "No further action"
    return "Review pending"


def display_case_summary(case_record: Mapping[str, Any], profile: Mapping[str, Any], related_count: int = 0) -> str:
    current_summary = empty_text(case_record.get("summary"), "")
    auto_generated = (not current_summary) or current_summary.startswith("Public enforcement record for ")
    if not auto_generated:
        return current_summary
    agent_summary = empty_text(case_record.get("agent_summary"), "")
    if agent_summary:
        return agent_summary
    if not profile:
        return current_summary
    summary = build_case_summary(profile)
    if related_count:
        lead_label = "lead" if related_count == 1 else "leads"
        summary = f"{summary} {related_count} related-business {lead_label} should also be checked."
    return summary


def resolve_current_user() -> Mapping[str, Any]:
    users = list_users(active_only=True)
    default_user = get_default_user()
    fallback_user = {
        "id": "local-demo-user",
        "display_name": DEFAULT_ANALYST,
        "username": "demo.analyst",
        "role": "Analyst",
        "email": "",
    }
    if not users:
        return fallback_user

    user_options = [user["id"] for user in users]
    default_user_id = (
        st.session_state.get("current_user_id")
        or (default_user["id"] if default_user else user_options[0])
    )
    if default_user_id not in user_options:
        default_user_id = user_options[0]
    selected_user_id = st.selectbox(
        "Pilot access profile",
        user_options,
        index=user_options.index(default_user_id),
        format_func=lambda user_id: next(
            (
                f"{user['display_name']} ({user['role']})"
                for user in users
                if user["id"] == user_id
            ),
            user_id,
        ),
    )
    st.session_state["current_user_id"] = selected_user_id
    user_record = get_user(selected_user_id) or fallback_user
    touch_user(text_value(user_record.get("id")))
    return user_record


def current_user_name(current_user: Mapping[str, Any]) -> str:
    return empty_text(current_user.get("display_name"), DEFAULT_ANALYST)


def current_user_role(current_user: Mapping[str, Any]) -> str:
    role = empty_text(current_user.get("role"), "Analyst")
    return role if role in ALLOWED_ROLES else "Analyst"


def is_manager_role(current_user: Mapping[str, Any]) -> bool:
    return current_user_role(current_user) in {"Manager", "Admin"}


def metric_card(label: str, value: str, note: str) -> None:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">{escape(label)}</div>
            <div class="metric-value">{escape(value)}</div>
            <div class="metric-note">{escape(note)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_ranked_bar_chart(title: str, note: str, counts: pd.Series, color: str) -> None:
    st.markdown(
        f"""
        <div class="chart-shell">
            <div class="chart-heading">{escape(title)}</div>
            <div class="chart-copy">{escape(note)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if counts.empty:
        st.info("No records are in the current filtered view.")
        return

    chart_data = counts.reset_index()
    chart_data.columns = ["category_full", "count"]
    chart_data["count"] = pd.to_numeric(chart_data["count"], errors="coerce").fillna(0)
    chart_data["category"] = chart_data["category_full"].map(lambda value: truncate_text(value, 28))

    max_count = max(float(chart_data["count"].max()), 1.0)
    chart_height = max(250, len(chart_data.index) * 46)
    tick_candidates = [10, 20, 25, 50, 100, 200, 250, 500, 1000]
    tick_step = next((step for step in tick_candidates if max_count / step <= 6), tick_candidates[-1])
    axis_limit = int(((max_count * 1.12) + tick_step - 1) // tick_step) * tick_step
    axis_values = list(range(0, axis_limit + tick_step, tick_step))

    base = alt.Chart(chart_data).encode(
        y=alt.Y(
            "category:N",
            sort="-x",
            axis=alt.Axis(
                title=None,
                domain=False,
                ticks=False,
                labelColor="#35504b",
                labelFontSize=13,
                labelFont="Space Grotesk",
                labelLimit=280,
                labelPadding=10,
            ),
        ),
        x=alt.X(
            "count:Q",
            scale=alt.Scale(domain=[0, axis_limit]),
            axis=alt.Axis(
                title=None,
                domain=False,
                tickColor="#d4c2ad",
                gridColor="#dccdb9",
                labelColor="#6b746f",
                labelFontSize=12,
                values=axis_values,
                format=",.0f",
            ),
        ),
        tooltip=[
            alt.Tooltip("category_full:N", title="Category"),
            alt.Tooltip("count:Q", title="Count", format=","),
        ],
    )

    bars = base.mark_bar(
        color=color,
        cornerRadiusTopRight=10,
        cornerRadiusBottomRight=10,
        size=28,
    )
    labels = base.mark_text(
        align="left",
        baseline="middle",
        dx=8,
        color="#173734",
        font="Space Grotesk",
        fontSize=12,
        fontWeight=700,
    ).encode(text=alt.Text("count:Q", format=","))

    chart = (
        (bars + labels)
        .properties(height=chart_height)
        .configure_view(stroke=None)
        .configure(background="transparent")
    )
    st.altair_chart(chart, use_container_width=True)


def detail_panel(title: str, rows: list[tuple[str, str]]) -> None:
    html_rows = "".join(
        f"""
        <div class="detail-row">
            <div class="detail-label">{escape(label)}</div>
            <div class="detail-value">{escape(value)}</div>
        </div>
        """
        for label, value in rows
    )
    st.markdown(
        f"""
        <div class="detail-panel">
            <h4>{escape(title)}</h4>
            {html_rows}
        </div>
        """,
        unsafe_allow_html=True,
    )


def read_markdown(path: Path) -> str:
    if not path.exists():
        return f"_Missing file: `{path.name}`_"
    return path.read_text(encoding="utf-8")


def describe_case_event(event: Mapping[str, Any]) -> tuple[str, str]:
    event_type = text_value(event.get("event_type"))
    field_name = text_value(event.get("field_name")).replace("_", " ").title()
    before_value = empty_text(event.get("before_value"), "")
    after_value = empty_text(event.get("after_value"), "")

    if event_type == "agent_draft_prepared":
        actor = empty_text(event.get("actor"), "")
        is_fallback = actor in {"deterministic-v1", "Rules Engine"} or "failed" in after_value.lower()
        if is_fallback:
            return "Fallback draft prepared", after_value or "A local rules-based draft was prepared."
        return "LLM draft prepared", after_value or "Prepared from current public records."
    if event_type == "agent_draft_reviewed":
        return "Agent draft reviewed", after_value or "Review outcome recorded."
    if event_type == "case_created":
        return "Case created", after_value or "Case opened from lookup result."
    if event_type == "attachment_added":
        return "Attachment added", after_value or "Attachment saved."
    if event_type == "register_refresh_completed":
        return "Live register refresh completed", after_value or "Public-register refresh completed."
    if event_type == "note_added":
        return "Note added", after_value or "Analyst note saved."
    if event_type == "field_updated":
        activity = f"Field updated: {field_name}" if field_name else "Field updated"
        if after_value and before_value:
            return activity, f"{before_value} -> {after_value}"
        if after_value:
            return activity, after_value
        if before_value:
            return activity, before_value
        return activity, ""

    activity = event_type.replace("_", " ").title() if event_type else "Case activity"
    detail = after_value or before_value
    return activity, detail


@st.cache_data(show_spinner=False)
def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str).fillna("")


@st.cache_data(show_spinner=False)
def load_first_available(paths: list[Path]) -> pd.DataFrame:
    for path in paths:
        if path.exists():
            return pd.read_csv(path, dtype=str).fillna("")
    return pd.DataFrame()


@st.cache_data(show_spinner=False)
def load_data() -> dict[str, pd.DataFrame]:
    profiles = load_first_available([OUTPUT_DIR / "entity_profiles.csv", SAMPLE_OUTPUT_DIR / "entity_profiles.csv"])
    review_queue = load_first_available([OUTPUT_DIR / "match_review_queue.csv", SAMPLE_OUTPUT_DIR / "match_review_queue.csv"])
    phoenix = load_first_available([OUTPUT_DIR / "phoenix_candidates.csv", SAMPLE_OUTPUT_DIR / "phoenix_candidates.csv"])
    enriched = load_first_available([NORMALIZED_DIR / "entities_enriched.csv", SAMPLE_NORMALIZED_DIR / "entities_enriched.csv"])

    for frame_name in ["profiles", "review_queue", "phoenix", "enriched"]:
        frame = locals()[frame_name]
        for date_col in [
            "first_action_date",
            "most_recent_action_date",
            "date_effective",
            "date_no_longer_in_force",
        ]:
            if date_col in frame.columns:
                frame[date_col] = pd.to_datetime(frame[date_col], errors="coerce")
        for num_col in [
            "action_count",
            "source_row_count",
            "abn_candidate_count",
            "abn_best_candidate_score",
            "days_after_enforcement",
            "similarity_score",
        ]:
            if num_col in frame.columns:
                frame[num_col] = pd.to_numeric(frame[num_col], errors="coerce")

    return {
        "profiles": profiles,
        "review_queue": review_queue,
        "phoenix": phoenix,
        "enriched": enriched,
    }


def build_case_sources(profile: Mapping[str, Any], history: pd.DataFrame) -> list[dict[str, str]]:
    sources: list[dict[str, str]] = []
    if not history.empty:
        source_url = next(
            (
                text_value(value)
                for value in history.get("source_url", pd.Series(dtype=str)).tolist()
                if text_value(value)
            ),
            "",
        )
        source_file = next(
            (
                text_value(value)
                for value in history.get("source_file", pd.Series(dtype=str)).tolist()
                if text_value(value)
            ),
            "",
        )
        sources.append(
            {
                "source_name": "NDIS Commission enforcement export",
                "source_type": "Official register",
                "source_ref": source_file,
                "source_url": source_url or NDIS_EXPORT_URL,
            }
        )
    else:
        sources.append(
            {
                "source_name": "NDIS Commission enforcement export",
                "source_type": "Official register",
                "source_ref": "",
                "source_url": NDIS_EXPORT_URL,
            }
        )

    resolved_abn = text_value(profile.get("resolved_abn"))
    if resolved_abn:
        sources.append(
            {
                "source_name": "ABN Lookup public record",
                "source_type": "Public business register",
                "source_ref": f"ABN {resolved_abn}",
                "source_url": f"{ABN_LOOKUP_URL}?abn={resolved_abn}",
            }
        )

    resolved_acn = text_value(profile.get("resolved_acn")) or text_value(
        profile.get("asic_company_acn")
    )
    if resolved_acn or text_value(profile.get("asic_company_name")):
        sources.append(
            {
                "source_name": "ASIC company dataset",
                "source_type": "Public company dataset",
                "source_ref": f"ACN {resolved_acn}"
                if resolved_acn
                else text_value(profile.get("asic_company_name")),
                "source_url": ASIC_DATASET_URL,
            }
        )

    return sources


def apply_profile_filters(profiles: pd.DataFrame) -> tuple[pd.DataFrame, Mapping[str, Any]]:
    filtered = profiles.copy()
    state_options = sorted(
        [value for value in filtered.get("source_state", pd.Series(dtype=str)).dropna().unique() if value]
    )
    severity_options = sorted(
        [value for value in filtered.get("most_severe_action", pd.Series(dtype=str)).dropna().unique() if value]
    )
    match_options = sorted(
        [value for value in filtered.get("match_confidence", pd.Series(dtype=str)).dropna().unique() if value]
    )

    with st.sidebar:
        st.markdown("### Access")
        current_user = resolve_current_user()
        st.caption(
            f"Role: {current_user_role(current_user)}. This local environment uses access profiles; hosted deployments should use enterprise identity."
        )

        st.markdown("### Find A Record")
        st.caption("Type a name, ABN, or ACN, then press Enter or click Apply Search.")
        with st.form("lookup_filters_form", clear_on_submit=False):
            chosen_states = st.multiselect(
                "State",
                state_options,
                default=st.session_state.get("filter_states", []),
            )
            chosen_severity = st.multiselect(
                "Most Serious Action",
                severity_options,
                default=st.session_state.get("filter_actions", []),
                format_func=friendly_action,
            )
            chosen_match = st.multiselect(
                "Match Status",
                match_options,
                default=st.session_state.get("filter_match_status", []),
                format_func=friendly_match_short,
            )
            text_query = st.text_input(
                "Search by name, ABN or ACN",
                value=st.session_state.get("filter_query", ""),
                placeholder="e.g. Caring Angels or 69624874219",
            )
            action_col1, action_col2 = st.columns(2)
            apply_filters = action_col1.form_submit_button(
                "Apply Search",
                type="primary",
                use_container_width=True,
            )
            clear_filters = action_col2.form_submit_button(
                "Clear",
                use_container_width=True,
            )

        if clear_filters:
            st.session_state["filter_states"] = []
            st.session_state["filter_actions"] = []
            st.session_state["filter_match_status"] = []
            st.session_state["filter_query"] = ""
            st.rerun()

        if apply_filters:
            st.session_state["filter_states"] = chosen_states
            st.session_state["filter_actions"] = chosen_severity
            st.session_state["filter_match_status"] = chosen_match
            st.session_state["filter_query"] = text_query

        chosen_states = st.session_state.get("filter_states", [])
        chosen_severity = st.session_state.get("filter_actions", [])
        chosen_match = st.session_state.get("filter_match_status", [])
        text_query = st.session_state.get("filter_query", "")

    if chosen_states:
        filtered = filtered[filtered["source_state"].isin(chosen_states)]
    if chosen_severity:
        filtered = filtered[filtered["most_severe_action"].isin(chosen_severity)]
    if chosen_match:
        filtered = filtered[filtered["match_confidence"].isin(chosen_match)]
    if text_query:
        lowered = text_query.lower().strip()
        mask = (
            filtered["source_entity_name"].str.lower().str.contains(lowered, na=False)
            | filtered["resolved_entity_name"].str.lower().str.contains(lowered, na=False)
            | filtered["resolved_abn"].astype(str).str.contains(lowered, na=False)
            | filtered["resolved_acn"].astype(str).str.contains(lowered, na=False)
        )
        filtered = filtered[mask]

    with st.sidebar:
        if chosen_states or chosen_severity or chosen_match or text_query.strip():
            if filtered.empty:
                st.warning("No records matched the current search.")
            else:
                st.success(f"Showing {len(filtered):,} matching record(s).")
        else:
            st.caption("Showing all available records.")

    return filtered, current_user


def render_header(
    profiles: pd.DataFrame,
    review_queue: pd.DataFrame,
    phoenix: pd.DataFrame,
    case_metrics: Mapping[str, Any],
) -> None:
    freshest = None
    if not profiles.empty and "most_recent_action_date" in profiles.columns:
        freshest = profiles["most_recent_action_date"].max()
    freshness_text = format_date(freshest) if pd.notna(freshest) else "No matching records"

    st.markdown(
        f"""
        <div class="hero-shell">
            <div class="hero-title">{escape(APP_TITLE)}</div>
            <div class="hero-copy">
                Search a provider or person, review enforcement history, open a case, and capture analyst notes in one place.
                This environment uses public records only. Current lookup results show <strong>{len(profiles):,}</strong> records,
                <strong>{len(review_queue):,}</strong> items needing review, <strong>{len(phoenix):,}</strong> related-business signals,
                and <strong>{int(case_metrics.get('open_cases', 0)):,}</strong> open cases. Latest action in scope:
                <strong>{escape(freshness_text)}</strong>.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_overview(
    profiles: pd.DataFrame,
    review_queue: pd.DataFrame,
    phoenix: pd.DataFrame,
    case_metrics: Mapping[str, Any],
    current_user: Mapping[str, Any],
) -> None:
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        metric_card(
            "Records Shown",
            f"{len(profiles):,}",
            "People or businesses in the current filtered view.",
        )
    with col2:
        resolved_abn_count = int((profiles.get("resolved_abn", pd.Series(dtype=str)) != "").sum())
        metric_card(
            "Matched Business Records",
            f"{resolved_abn_count:,}",
            "Records where a public business identifier was found.",
        )
    with col3:
        metric_card(
            "Needs Review",
            f"{len(review_queue):,}",
            "Items that still need a human check or sign-off.",
        )
    with col4:
        metric_card(
            "Related Businesses",
            f"{len(phoenix):,}",
            "Later registrations with a very similar name after serious action.",
        )

    case_col1, case_col2, case_col3, case_col4 = st.columns(4)
    with case_col1:
        metric_card(
            "Cases Logged",
            f"{int(case_metrics.get('total_cases', 0)):,}",
            "Cases created by analysts in this local workbench.",
        )
    with case_col2:
        metric_card(
            "Open Cases",
            f"{int(case_metrics.get('open_cases', 0)):,}",
            "Cases still moving through review or monitoring.",
        )
    with case_col3:
        metric_card(
            "Escalated Cases",
            f"{int(case_metrics.get('escalated_cases', 0)):,}",
            "Cases marked for supervisor or investigator review.",
        )
    with case_col4:
        metric_card(
            "Overdue Cases",
            f"{int(case_metrics.get('overdue_cases', 0)):,}",
            "Open cases where the target review date has already passed.",
        )

    st.markdown(
        '<div class="section-note">This screen is designed for review, not automatic decision-making. It keeps uncertain matches visible instead of hiding them behind technical codes.</div>',
        unsafe_allow_html=True,
    )

    chart_col1, chart_col2 = st.columns(2)
    with chart_col1:
        if profiles.empty:
            render_ranked_bar_chart(
                "Action Types In View",
                "Distribution of the most serious enforcement action in the current result set.",
                pd.Series(dtype=float),
                "#d8783f",
            )
        else:
            action_counts = (
                profiles["most_severe_action"]
                .fillna("")
                .replace("", "other")
                .map(friendly_action)
                .value_counts()
                .sort_values(ascending=False)
            )
            render_ranked_bar_chart(
                "Action Types In View",
                "Distribution of the most serious enforcement action in the current result set.",
                action_counts,
                "#d8783f",
            )

    with chart_col2:
        if profiles.empty:
            render_ranked_bar_chart(
                "Match Outcomes",
                "How records in the current view were linked to public business identifiers.",
                pd.Series(dtype=float),
                "#1f6ec0",
            )
        else:
            match_counts = (
                profiles["match_confidence"]
                .fillna("")
                .replace("", "unclassified")
                .map(friendly_match_short)
                .value_counts()
                .sort_values(ascending=False)
            )
            render_ranked_bar_chart(
                "Match Outcomes",
                "How records in the current view were linked to public business identifiers.",
                match_counts,
                "#1f6ec0",
            )

    st.subheader("Top Items To Check")
    if review_queue.empty:
        st.success("Nothing in the current filtered view needs review.")
    else:
        table = review_queue.copy()
        table["priority_rank"] = table["match_confidence"].map(REVIEW_PRIORITY_ORDER).fillna(9)
        table["Match status"] = table["match_confidence"].map(friendly_match_short)
        table["Most serious action"] = table["most_severe_action"].map(friendly_action)
        table["Suggested business"] = table["abn_best_candidate_name"].replace("", "No safe match")
        table["Suggested ABN"] = table["resolved_abn"].replace("", "Not available")
        table["Why it needs review"] = table.apply(
            lambda row: friendly_match_long(row.get("match_confidence"), row.get("review_reason")),
            axis=1,
        )
        table["Confidence"] = table["abn_best_candidate_score"].map(format_score)
        table = table.sort_values(
            ["priority_rank", "abn_best_candidate_score", "action_count"],
            ascending=[True, False, False],
        )
        st.dataframe(
            table[
                [
                    "source_entity_name",
                    "source_state",
                    "Most serious action",
                    "Match status",
                    "Suggested business",
                    "Suggested ABN",
                    "Confidence",
                    "Why it needs review",
                ]
            ].rename(
                columns={
                    "source_entity_name": "Name on notice",
                    "source_state": "State",
                }
            ).head(25),
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("Recent Case Activity")
    recent_events = case_metrics.get("recent_events", []) or []
    if not recent_events:
        st.info("No case activity has been recorded yet. Open the Look Up Record screen and create a case to start the desk.")
        return

    case_lookup = {case["id"]: case for case in list_cases()}
    event_rows = []
    latest_agent_case: Mapping[str, Any] | None = None
    latest_llm_agent_case: Mapping[str, Any] | None = None
    for event in recent_events:
        description, detail = describe_case_event(event)
        linked_case = case_lookup.get(text_value(event.get("case_id")))
        if text_value(event.get("event_type")) == "agent_draft_prepared" and linked_case:
            if latest_agent_case is None:
                latest_agent_case = linked_case
            if latest_llm_agent_case is None and text_value(linked_case.get("agent_generation_mode")) == "llm":
                latest_llm_agent_case = linked_case
        event_rows.append(
            {
                "When": format_date(event.get("created_at")),
                "Case": empty_text(linked_case.get("title") if linked_case else "", "Case no longer available"),
                "Source": friendly_actor_type(event.get("actor_type")),
                "Actor": friendly_event_actor(event),
                "Activity": description,
                "Detail": detail,
            }
        )
    st.dataframe(pd.DataFrame(event_rows), use_container_width=True, hide_index=True)

    draft_preview_case = latest_llm_agent_case or latest_agent_case
    if draft_preview_case and text_value(draft_preview_case.get("agent_summary")):
        st.caption("Recent activity is an audit trail only. The latest prepared draft is shown below and can also be opened from Case Desk.")
        draft_col1, draft_col2 = st.columns([3, 1])
        with draft_col1:
            st.markdown(
                (
                    f"**Latest Agent Draft:** {escape(text_value(draft_preview_case.get('title')))}  \n"
                    f"Source: {escape(empty_text(draft_preview_case.get('agent_generation_mode'), 'rules').replace('llm', 'LLM').replace('rules', 'Rules'))}"
                    f" | Alias: {escape(empty_text(draft_preview_case.get('agent_generation_model'), 'Not available'))}"
                    f" | Backend: {escape(empty_text(draft_preview_case.get('agent_resolved_model'), empty_text(draft_preview_case.get('agent_generation_model'), 'Not available')))}  \n"
                    f"{escape(text_value(draft_preview_case.get('agent_summary')))}"
                )
            )
        with draft_col2:
            if st.button("Open In Case Desk", key=f"open-latest-agent-draft-{draft_preview_case['id']}"):
                jump_to_view(
                    "Case Desk",
                    selected_case_id=draft_preview_case["id"],
                    message="Opened the latest prepared case in the Case Desk.",
                    level="success",
                )

    if is_manager_role(current_user):
        manager_col1, manager_col2 = st.columns(2)
        with manager_col1:
            st.subheader("Team Workload")
            owner_rows = case_metrics.get("owner_breakdown", []) or []
            if owner_rows:
                st.dataframe(pd.DataFrame(owner_rows), use_container_width=True, hide_index=True)
            else:
                st.info("No team workload is available yet.")
        with manager_col2:
            st.subheader("Case Status Mix")
            status_rows = case_metrics.get("status_breakdown", []) or []
            if status_rows:
                st.dataframe(pd.DataFrame(status_rows), use_container_width=True, hide_index=True)
            else:
                st.info("No case status metrics are available yet.")


def build_entity_options(profiles: pd.DataFrame) -> tuple[list[str], dict[str, str]]:
    options: list[str] = []
    label_map: dict[str, str] = {}
    for row in profiles.to_dict(orient="records"):
        key = row["entity_key"]
        state_text = row.get("source_state") or "State not listed"
        label_map[key] = (
            f"{empty_text(row.get('source_entity_name'), 'Unnamed record')} - "
            f"{friendly_action(row.get('most_severe_action'))} - "
            f"{state_text}"
        )
        options.append(key)
    return options, label_map


def render_entity_explorer(
    profiles: pd.DataFrame,
    enriched: pd.DataFrame,
    phoenix: pd.DataFrame,
    current_user: Mapping[str, Any],
) -> None:
    st.subheader("Look Up Record")
    if profiles.empty:
        st.warning("No records match the current search and filters.")
        return

    options, label_map = build_entity_options(profiles)
    default_entity_key = st.session_state.get("selected_entity_key")
    index = options.index(default_entity_key) if default_entity_key in options else 0
    selected_key = st.selectbox(
        "Select a provider or person",
        options,
        index=index,
        format_func=lambda key: label_map.get(key, key),
    )
    st.session_state["selected_entity_key"] = selected_key
    selected = profiles.loc[profiles["entity_key"] == selected_key].iloc[0]
    selected_dict = selected.to_dict()
    history = enriched[enriched.get("entity_key", pd.Series(dtype=str)) == selected.get("entity_key")].copy()
    related = phoenix[phoenix.get("entity_key", pd.Series(dtype=str)) == selected.get("entity_key")].copy()
    existing_case = get_case_by_entity_key(selected_key)
    has_business_match = any(
        str(selected.get(field, "")).strip()
        for field in ["resolved_abn", "resolved_acn", "asic_match_basis"]
    )
    business_name = (
        empty_text(selected.get("resolved_entity_name"))
        if has_business_match
        else "No confirmed business record yet"
    )
    business_method = (
        friendly_match_long(selected.get("match_confidence"))
        if has_business_match
        else "This record still needs a manual check against public business registers."
    )

    st.markdown(
        f'<div class="section-note">{escape(friendly_match_long(selected.get("match_confidence"), selected.get("review_reason")))}</div>',
        unsafe_allow_html=True,
    )

    left, middle, right = st.columns([1.0, 1.25, 0.9])
    with left:
        metric_card(
            "Enforcement Records",
            f"{safe_int(selected.get('action_count')):,}",
            f"First recorded action: {format_date(selected.get('first_action_date'))}",
        )
    with middle:
        metric_card(
            "Match Result",
            friendly_match_short(selected.get("match_confidence")),
            f"Latest action: {format_date(selected.get('most_recent_action_date'))}",
        )
    with right:
        metric_card(
            "Company Register",
            friendly_register_link(selected.get("asic_match_basis")),
            f"ABN: {empty_text(selected.get('resolved_abn'), 'Not available')}",
        )

    status_message = (
        "Already logged in the Case Desk."
        if existing_case
        else "Not yet added to the Case Desk."
    )
    st.markdown(
        f'<div class="case-banner"><strong>Case status:</strong> {escape(status_message)}</div>',
        unsafe_allow_html=True,
    )

    button_col1, button_col2 = st.columns([1, 1])
    with button_col1:
        if existing_case:
            if st.button("Open Existing Case", use_container_width=True):
                jump_to_view(
                    "Case Desk",
                    selected_case_id=existing_case["id"],
                    message="Opened the existing case in the Case Desk.",
                    level="success",
                )
        else:
            if st.button("Create Case In Desk", type="primary", use_container_width=True):
                actor_name = current_user_name(current_user)
                case_record, _ = create_case_from_entity(
                    selected_dict,
                    actor=actor_name,
                    owner=actor_name,
                    related_count=len(related.index),
                    related_records=related.to_dict("records"),
                )
                replace_case_sources(case_record["id"], build_case_sources(selected_dict, history))
                jump_to_view(
                    "Case Desk",
                    selected_case_id=case_record["id"],
                    message="Case created and opened in the Case Desk.",
                    level="success",
                )
    with button_col2:
        if st.button(
            "Refresh Source Links",
            use_container_width=True,
            disabled=existing_case is None,
        ):
            replace_case_sources(existing_case["id"], build_case_sources(selected_dict, history))
            st.success("Source links refreshed for this case.")

    notice_col, business_col = st.columns(2)
    with notice_col:
        detail_panel(
            "Notice Details",
            [
                ("Name on notice", empty_text(selected.get("source_entity_name"))),
                ("Other names seen", empty_text(selected.get("source_entity_names"), "No extra names listed")),
                ("Most serious action", friendly_action(selected.get("most_severe_action"))),
                ("Latest action date", format_date(selected.get("most_recent_action_date"))),
                ("State", empty_text(selected.get("source_state"))),
                ("Postcode", empty_text(selected.get("source_postcode"))),
                ("Record type", friendly_entity_type(selected.get("source_entity_type"))),
            ],
        )

    with business_col:
        detail_panel(
            "Business Record",
            [
                ("Business name", business_name),
                ("ABN", empty_text(selected.get("resolved_abn"))),
                ("ACN", empty_text(selected.get("resolved_acn"))),
                ("Match result", friendly_match_short(selected.get("match_confidence"))),
                ("How it was checked", business_method),
                ("Business type", friendly_entity_type(selected.get("resolved_entity_type"))),
                ("Company register status", friendly_asic_status(selected.get("asic_status"))),
                ("Registered on", empty_text(selected.get("asic_registration_date"), "Not linked yet")),
            ],
        )

    st.markdown("#### Enforcement Timeline")
    if history.empty:
        st.info("No enforcement history rows were found for this record.")
    else:
        history = history.sort_values("date_effective", ascending=False)
        history["Effective date"] = history["date_effective"].map(format_date)
        history["Action"] = history["action_type"].map(friendly_action)
        history["Name on notice"] = history["entity_name"].replace("", "Not available")
        history["State"] = history["state"].replace("", "Not available")
        history["Postcode"] = history["postcode"].replace("", "Not available")
        history["Summary"] = history["description_text"].map(lambda value: truncate_text(value, 220))
        st.dataframe(
            history[
                [
                    "Effective date",
                    "Action",
                    "Name on notice",
                    "State",
                    "Postcode",
                    "Summary",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("#### Related Businesses To Check")
    if related.empty:
        st.info("No related-business leads were identified for this record.")
    else:
        related_copy = related.copy()
        related_copy["Business"] = related_copy["candidate_entity_name"].replace("", "Not available")
        related_copy["ABN"] = related_copy["candidate_abn"].replace("", "Not available")
        related_copy["Registered on"] = related_copy["candidate_registration_date"].replace("", "Not available")
        related_copy["Days after action"] = related_copy["days_after_enforcement"].map(
            lambda value: f"{safe_int(value)} days" if safe_int(value) else "Not available"
        )
        st.dataframe(
            related_copy[
                [
                    "Business",
                    "ABN",
                    "candidate_acn",
                    "candidate_status",
                    "Registered on",
                    "Days after action",
                ]
            ].rename(columns={"candidate_acn": "ACN", "candidate_status": "Status"}),
            use_container_width=True,
            hide_index=True,
        )


def render_review_queue(review_queue: pd.DataFrame) -> None:
    st.subheader("Needs Review")
    st.markdown(
        '<div class="section-note">These records still need a person to confirm the match or decide whether the public evidence is strong enough to use. Use the Look Up Record screen to open a case for any item you want to track.</div>',
        unsafe_allow_html=True,
    )
    if review_queue.empty:
        st.success("Nothing in the current filtered view needs review.")
        return

    review_types = sorted([value for value in review_queue["match_confidence"].dropna().unique() if value])
    selected_types = st.multiselect(
        "Show these review types",
        review_types,
        default=review_types,
        format_func=friendly_match_short,
    )
    filtered = review_queue[review_queue["match_confidence"].isin(selected_types)].copy()
    score_floor = st.slider("Only show stronger suggested matches", 0.0, 1.0, 0.0, 0.01)
    filtered = filtered[filtered["abn_best_candidate_score"].fillna(0.0) >= score_floor]

    filtered["Match status"] = filtered["match_confidence"].map(friendly_match_short)
    filtered["Most serious action"] = filtered["most_severe_action"].map(friendly_action)
    filtered["Suggested business"] = filtered["abn_best_candidate_name"].replace("", "No safe match")
    filtered["Suggested ABN"] = filtered["resolved_abn"].replace("", "Not available")
    filtered["Confidence"] = filtered["abn_best_candidate_score"].map(format_score)
    filtered["Why it needs review"] = filtered.apply(
        lambda row: friendly_match_long(row.get("match_confidence"), row.get("review_reason")),
        axis=1,
    )
    filtered["priority_rank"] = filtered["match_confidence"].map(REVIEW_PRIORITY_ORDER).fillna(9)
    filtered = filtered.sort_values(
        ["priority_rank", "abn_best_candidate_score"],
        ascending=[True, False],
    )

    st.dataframe(
        filtered[
            [
                "source_entity_name",
                "source_state",
                "Most serious action",
                "Match status",
                "Suggested business",
                "Suggested ABN",
                "Confidence",
                "Why it needs review",
            ]
        ].rename(
            columns={
                "source_entity_name": "Name on notice",
                "source_state": "State",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )


def render_case_detail(
    case_record: Mapping[str, Any],
    profiles: pd.DataFrame,
    enriched: pd.DataFrame,
    phoenix: pd.DataFrame,
    current_user: Mapping[str, Any],
) -> None:
    profile_rows = profiles[profiles.get("entity_key", pd.Series(dtype=str)) == case_record["entity_key"]]
    profile = profile_rows.iloc[0].to_dict() if not profile_rows.empty else {}
    history = enriched[enriched.get("entity_key", pd.Series(dtype=str)) == case_record["entity_key"]].copy()
    related = phoenix[phoenix.get("entity_key", pd.Series(dtype=str)) == case_record["entity_key"]].copy()
    needs_agent_backfill = (
        not text_value(case_record.get("agent_summary"))
        or not text_value(case_record.get("agent_completed_checks"))
        or not text_value(case_record.get("agent_supporting_evidence"))
        or not text_value(case_record.get("agent_human_checks"))
        or (
            text_value(case_record.get("agent_generation_mode")).lower() == "llm"
            and not text_value(case_record.get("agent_resolved_model"))
        )
    )
    if profile and needs_agent_backfill:
        refreshed_case = refresh_agent_draft(
            case_record["id"],
            profile,
            related_count=len(related.index),
            related_records=related.to_dict("records"),
            force=bool(text_value(case_record.get("agent_summary"))),
        )
        if refreshed_case is not None:
            case_record = refreshed_case
    sources = list_sources(case_record["id"])
    if not sources and profile:
        replace_case_sources(case_record["id"], build_case_sources(profile, history))
        sources = list_sources(case_record["id"])
    notes = list_notes(case_record["id"])
    events = list_events(case_record["id"])
    attachments = list_attachments(case_record["id"])
    actor_name = current_user_name(current_user)
    summary_for_display = empty_text(case_record.get("summary"), "")
    related_records = related.to_dict("records")

    st.markdown(
        f'<div class="case-banner"><strong>{escape(case_record["title"])}</strong><br>Human review is still required before any escalation or operational action.</div>',
        unsafe_allow_html=True,
    )

    summary_col, controls_col = st.columns([1.15, 0.85])
    with summary_col:
        detail_panel(
            "Case Summary",
            [
                ("Status", empty_text(case_record.get("status"))),
                ("Priority", empty_text(case_record.get("priority"))),
                ("Owner", empty_text(case_record.get("owner"), "Unassigned")),
                ("Decision", empty_text(case_record.get("decision"), "No decision yet")),
                ("Due by", format_date(case_record.get("due_at"))),
                ("Created", format_date(case_record.get("created_at"))),
                ("Updated", format_date(case_record.get("updated_at"))),
            ],
        )
    with controls_col:
        with st.form(f'case_edit_{case_record["id"]}'):
            title_value = st.text_input("Case title", value=empty_text(case_record.get("title"), ""))
            status_value = st.selectbox(
                "Status",
                ALLOWED_STATUSES,
                index=ALLOWED_STATUSES.index(case_record.get("status"))
                if case_record.get("status") in ALLOWED_STATUSES
                else 0,
            )
            priority_value = st.selectbox(
                "Priority",
                ALLOWED_PRIORITIES,
                index=ALLOWED_PRIORITIES.index(case_record.get("priority"))
                if case_record.get("priority") in ALLOWED_PRIORITIES
                else 1,
            )
            owner_value = st.text_input("Owner", value=empty_text(case_record.get("owner"), actor_name))
            decision_value = st.selectbox(
                "Decision",
                DECISION_OPTIONS,
                index=DECISION_OPTIONS.index(case_record.get("decision"))
                if case_record.get("decision") in DECISION_OPTIONS
                else 0,
            )
            due_value = st.date_input(
                "Target review date",
                value=pd.to_datetime(case_record.get("due_at"), errors="coerce").date()
                if pd.notna(pd.to_datetime(case_record.get("due_at"), errors="coerce"))
                else pd.Timestamp.today().date(),
                help="Used for manager queue aging and overdue tracking.",
            )
            summary_value = st.text_area(
                "Analyst summary",
                value=summary_for_display,
                help="Keep this to plain-language notes a reviewer can scan quickly. The agent draft sits below and can be accepted or edited separately.",
                height=110,
            )
            decision_reason_value = st.text_area(
                "Decision reason",
                value=empty_text(case_record.get("decision_reason"), ""),
                height=110,
            )
            draft_review_action = st.selectbox(
                "Agent draft review",
                AGENT_REVIEW_ACTIONS,
                help="Accept uses the agent draft as-is. Save with human edits uses the values currently in this form.",
            )
            save_case = st.form_submit_button("Save Case Update", type="primary")
        if save_case:
            if draft_review_action == "Accept agent draft":
                review_agent_draft(case_record["id"], "Accepted", actor=actor_name)
                st.success("Agent draft accepted and applied to the case.")
            else:
                update_case(
                    case_record["id"],
                    {
                        "title": title_value,
                        "status": status_value,
                        "priority": priority_value,
                        "owner": owner_value,
                        "decision": "" if decision_value == "No decision yet" else decision_value,
                        "summary": summary_value,
                        "decision_reason": decision_reason_value,
                        "due_at": due_value.isoformat(),
                    },
                    actor=actor_name,
                )
                if draft_review_action == "Save with human edits":
                    review_agent_draft(case_record["id"], "Edited", actor=actor_name)
                    st.success("Case updated and marked as human-edited from the agent draft.")
                elif draft_review_action == "Reject agent draft":
                    review_agent_draft(case_record["id"], "Rejected", actor=actor_name)
                    st.success("Case updated and the agent draft was marked as rejected.")
                else:
                    st.success("Case updated.")
            st.rerun()

    agent_summary = empty_text(case_record.get("agent_summary"), "No agent draft prepared yet.")
    completed_check_points = text_to_bullets(
        case_record.get("agent_completed_checks"),
        "No automated checks were recorded yet.",
    )
    register_refresh_points = text_to_bullets(
        case_record.get("register_refresh_summary"),
        "No live public-register refresh has been run for this case yet.",
    )
    evidence_points = text_to_bullets(
        case_record.get("agent_supporting_evidence"),
        "No supporting evidence summary was captured yet.",
    )
    human_check_points = text_to_bullets(
        case_record.get("agent_human_checks"),
        "No human-confirmation steps were captured yet.",
    )
    agent_rows = [
        ("Review status", friendly_agent_review_status(case_record.get("agent_review_status"))),
        ("Draft source", empty_text(case_record.get("agent_generation_mode"), "rules").replace("llm", "LLM").replace("rules", "Rules")),
        ("Requested model alias", empty_text(case_record.get("agent_generation_model"), "deterministic-v1")),
        ("Resolved backend model", empty_text(case_record.get("agent_resolved_model"), empty_text(case_record.get("agent_generation_model"), "deterministic-v1"))),
        ("Prepared on", format_date(case_record.get("agent_prepared_at"))),
        ("Latest register refresh", format_date(case_record.get("register_refreshed_at"))),
        ("Suggested case status", empty_text(case_record.get("agent_recommended_status"), "In Review")),
        ("Suggested priority", empty_text(case_record.get("agent_recommended_priority"), "Medium")),
        ("Suggested decision", empty_text(case_record.get("agent_recommended_decision"), "Needs more evidence")),
        ("Recommended next step", empty_text(case_record.get("agent_next_step"), "Review the record manually.")),
        ("Reviewed by", empty_text(case_record.get("agent_reviewed_by"), "Not reviewed yet")),
        ("Reviewed on", format_date(case_record.get("agent_reviewed_at"))),
    ]
    agent_col, agent_action_col = st.columns([1.1, 0.9])
    with agent_col:
        detail_panel("Agent Recommendation", agent_rows)
    with agent_action_col:
        if empty_text(case_record.get("agent_generation_mode"), "rules").lower() == "llm":
            st.success("Live agent draft ready. Review it, then accept, edit, or reject it before any escalation.")
        else:
            st.warning("Fallback draft in use. The live model was unavailable, so this draft came from the local rules engine.")
        st.markdown("#### Draft Summary")
        st.markdown(agent_summary)
        st.markdown("#### Why The Agent Recommends This")
        st.write(empty_text(case_record.get("agent_rationale"), "No rationale recorded yet."))
        st.markdown("#### What The Agent Already Checked")
        st.markdown("\n".join(f"- {point}" for point in completed_check_points))
        st.markdown("#### Latest Public-Register Refresh")
        st.markdown("\n".join(f"- {point}" for point in register_refresh_points))
        st.markdown("#### Evidence Used")
        st.markdown("\n".join(f"- {point}" for point in evidence_points))
        st.markdown("#### Needs Analyst Judgment")
        st.markdown("\n".join(f"- {point}" for point in human_check_points))
        st.markdown("#### Recommended Next Step")
        st.write(empty_text(case_record.get("agent_next_step"), "Review the current public record manually."))
        st.caption(empty_text(case_record.get("agent_generation_notes"), "No draft-generation note recorded yet."))
        action_col1, action_col2, action_col3 = st.columns(3)
        if action_col1.button(
            "Apply Draft To Case Fields",
            key=f'apply_agent_{case_record["id"]}',
            type="primary",
            use_container_width=True,
        ):
            review_agent_draft(case_record["id"], "Accepted", actor=actor_name)
            st.success("Agent draft applied to the case fields.")
            st.rerun()
        if action_col2.button(
            "Run Live Register Refresh",
            key=f'register_refresh_{case_record["id"]}',
            use_container_width=True,
        ):
            if profile:
                with st.spinner("Refreshing ABR and ASIC evidence, then rebuilding the draft..."):
                    refresh_case_public_registers(
                        case_record["id"],
                        profile,
                        related_records=related_records,
                        actor=actor_name,
                    )
                    refresh_agent_draft(
                        case_record["id"],
                        profile,
                        related_count=len(related.index),
                        related_records=related_records,
                        force=True,
                    )
                st.success("Live public-register evidence saved to the case and the agent draft was regenerated.")
                st.rerun()
            else:
                st.warning("No profile data is available to run a live public-register refresh.")
        if action_col3.button(
            "Regenerate Agent Draft",
            key=f'refresh_agent_{case_record["id"]}',
            use_container_width=True,
        ):
            if profile:
                with st.spinner("Preparing a fresh draft from the current record..."):
                    refresh_agent_draft(
                        case_record["id"],
                        profile,
                        related_count=len(related.index),
                        related_records=related_records,
                        force=True,
                    )
                st.success("Agent draft regenerated from the current public records.")
                st.rerun()
            else:
                st.warning("No profile data is available to regenerate the draft.")

    notice_col, business_col = st.columns(2)
    with notice_col:
        detail_panel(
            "Notice Details",
            [
                ("Name on notice", empty_text(profile.get("source_entity_name"))),
                ("Most serious action", friendly_action(profile.get("most_severe_action"))),
                ("Latest action date", format_date(profile.get("most_recent_action_date"))),
                ("State", empty_text(profile.get("source_state"))),
                ("Postcode", empty_text(profile.get("source_postcode"))),
                ("Record type", friendly_entity_type(profile.get("source_entity_type"))),
            ],
        )
    with business_col:
        detail_panel(
            "Business Record",
            [
                ("Business name", empty_text(profile.get("resolved_entity_name"), "No confirmed business record yet")),
                ("ABN", empty_text(profile.get("resolved_abn"))),
                ("ACN", empty_text(profile.get("resolved_acn"))),
                ("Match result", friendly_match_short(profile.get("match_confidence"))),
                ("Company register status", friendly_asic_status(profile.get("asic_status"))),
                ("Registered on", empty_text(profile.get("asic_registration_date"), "Not linked yet")),
            ],
        )

    export_col, note_col = st.columns([0.8, 1.2])
    with export_col:
        if st.button(
            "Export HTML Brief",
            key=f'export_{case_record["id"]}',
            use_container_width=True,
        ):
            export_path = export_case_brief(
                case=case_record,
                profile=profile,
                history=history,
                related_businesses=related,
                notes=notes,
                sources=sources,
                attachments=attachments,
            )
            st.session_state[f'exported_case_{case_record["id"]}'] = str(export_path)
            st.success(f"Case brief exported to {export_path}")
        exported_path = st.session_state.get(f'exported_case_{case_record["id"]}')
        if exported_path and Path(exported_path).exists():
            st.download_button(
                "Download Exported Brief",
                data=Path(exported_path).read_text(encoding="utf-8"),
                file_name=Path(exported_path).name,
                mime="text/html",
                key=f'download_{case_record["id"]}',
                use_container_width=True,
            )
    with note_col:
        with st.form(f'note_form_{case_record["id"]}'):
            note_text = st.text_area(
                "Add analyst note",
                placeholder="Record what you checked, what remains uncertain, and what should happen next.",
                height=120,
            )
            add_note_submit = st.form_submit_button("Add Note")
        if add_note_submit:
            if text_value(note_text):
                add_note(case_record["id"], note_text, author=actor_name)
                st.success("Note added to case history.")
                st.rerun()
            else:
                st.warning("Enter note text before saving.")

    st.markdown("#### Analyst Notes")
    if not notes:
        st.info("No analyst notes recorded yet.")
    else:
        note_rows = [
            {
                "When": format_date(note.get("created_at")),
                "Actor": empty_text(note.get("author"), "Analyst"),
                "Note": empty_text(note.get("note_text")),
            }
            for note in notes
        ]
        st.dataframe(pd.DataFrame(note_rows), use_container_width=True, hide_index=True)

    st.markdown("#### Case Attachments")
    with st.form(f'attachment_form_{case_record["id"]}'):
        uploaded_files = st.file_uploader(
            "Upload evidence, screenshots, or working papers",
            accept_multiple_files=True,
            key=f'attachment_upload_{case_record["id"]}',
        )
        upload_submit = st.form_submit_button("Save Attachments")
    if upload_submit:
        if uploaded_files:
            saved_count = 0
            for uploaded_file in uploaded_files:
                attachment_record = add_attachment(
                    case_record["id"],
                    file_name=uploaded_file.name,
                    content_bytes=uploaded_file.getvalue(),
                    content_type=uploaded_file.type,
                    uploaded_by=actor_name,
                )
                if attachment_record:
                    saved_count += 1
            st.success(f"Saved {saved_count} attachment(s).")
            st.rerun()
        else:
            st.warning("Choose at least one file before saving attachments.")

    if not attachments:
        st.info("No case attachments uploaded yet.")
    else:
        attachment_rows = []
        for attachment in attachments:
            file_path = Path(text_value(attachment.get("stored_path")))
            attachment_rows.append(
                {
                    "When": format_date(attachment.get("created_at")),
                    "File": empty_text(attachment.get("file_name")),
                    "Uploaded by": empty_text(attachment.get("uploaded_by"), "Pilot user"),
                    "Size": f"{safe_int(attachment.get('size_bytes')):,} bytes",
                }
            )
        st.dataframe(pd.DataFrame(attachment_rows), use_container_width=True, hide_index=True)
        for attachment in attachments:
            file_path = Path(text_value(attachment.get("stored_path")))
            if file_path.exists():
                st.download_button(
                    f"Download {empty_text(attachment.get('file_name'))}",
                    data=file_path.read_bytes(),
                    file_name=file_path.name if not text_value(attachment.get("file_name")) else text_value(attachment.get("file_name")),
                    mime=text_value(attachment.get("content_type")) or "application/octet-stream",
                    key=f"attachment_download_{attachment['id']}",
                    use_container_width=False,
                )

    st.markdown("#### Activity History")
    if not events:
        st.info("No case events recorded yet.")
    else:
        event_rows = []
        for event in events:
            activity, detail = describe_case_event(event)
            event_rows.append(
                {
                    "When": format_date(event.get("created_at")),
                    "Source": friendly_actor_type(event.get("actor_type")),
                    "Actor": friendly_event_actor(event),
                    "Activity": activity,
                    "Detail": detail,
                }
            )
        st.dataframe(pd.DataFrame(event_rows), use_container_width=True, hide_index=True)

    source_col, related_col = st.columns(2)
    with source_col:
        st.markdown("#### Evidence Sources")
        if not sources:
            st.info("No source links have been saved for this case yet.")
        else:
            for source in sources:
                url = text_value(source.get("source_url"))
                ref_text = text_value(source.get("source_ref"))
                st.markdown(f"**{escape(text_value(source.get('source_name')))}**")
                st.caption(empty_text(source.get("source_type"), "Reference"))
                if url:
                    st.link_button("Open source", url, use_container_width=False)
                if ref_text:
                    st.caption(display_source_ref(ref_text))
    with related_col:
        st.markdown("#### Related Businesses To Check")
        if related.empty:
            st.info("No related-business leads were identified for this case.")
        else:
            related_copy = related.copy()
            related_copy["Business"] = related_copy["candidate_entity_name"].replace("", "Not available")
            related_copy["ABN"] = related_copy["candidate_abn"].replace("", "Not available")
            related_copy["Days after action"] = related_copy["days_after_enforcement"].map(
                lambda value: f"{safe_int(value)} days" if safe_int(value) else "Not available"
            )
            st.dataframe(
                related_copy[
                    [
                        "Business",
                        "ABN",
                        "candidate_acn",
                        "candidate_status",
                        "candidate_registration_date",
                        "Days after action",
                    ]
                ].rename(
                    columns={
                        "candidate_acn": "ACN",
                        "candidate_status": "Status",
                        "candidate_registration_date": "Registered on",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )
            st.markdown("##### Mini-Briefs")
            for brief in related_business_briefs_from_frame(related):
                st.markdown(f"- {brief}")

    st.markdown("#### Enforcement Timeline")
    if history.empty:
        st.info("No enforcement timeline rows were found for this case.")
    else:
        history_copy = history.sort_values("date_effective", ascending=False).copy()
        history_copy["Effective date"] = history_copy["date_effective"].map(format_date)
        history_copy["Action"] = history_copy["action_type"].map(friendly_action)
        history_copy["Summary"] = history_copy["description_text"].map(lambda value: truncate_text(value, 240))
        st.dataframe(
            history_copy[["Effective date", "Action", "state", "postcode", "Summary"]].rename(
                columns={"state": "State", "postcode": "Postcode"}
            ),
            use_container_width=True,
            hide_index=True,
        )


def render_case_desk(
    profiles: pd.DataFrame,
    enriched: pd.DataFrame,
    phoenix: pd.DataFrame,
    current_user: Mapping[str, Any],
    case_metrics: Mapping[str, Any],
) -> None:
    st.subheader("Case Desk")
    st.markdown(
        '<div class="section-note">Track review work, keep a clean audit trail, and prepare supervisor-ready case briefs. This desk supports human review; it does not make automatic findings.</div>',
        unsafe_allow_html=True,
    )

    case_list = list_cases()
    if not case_list:
        st.info("No cases have been created yet. Open the Look Up Record screen and create a case to start the desk.")
        return

    case_frame = pd.DataFrame(case_list)
    open_count = int((case_frame["status"] != "Closed").sum()) if "status" in case_frame else 0
    escalate_count = int((case_frame["status"] == "Escalate").sum()) if "status" in case_frame else 0
    high_count = int((case_frame["priority"] == "High").sum()) if "priority" in case_frame else 0
    overdue_count = int(case_metrics.get("overdue_cases", 0))
    summary_col1, summary_col2, summary_col3, summary_col4 = st.columns(4)
    with summary_col1:
        metric_card("Open Cases", str(open_count), "Cases still in review, escalation, or monitoring.")
    with summary_col2:
        metric_card("Escalations", str(escalate_count), "Cases currently marked for supervisor review.")
    with summary_col3:
        metric_card("High Priority", str(high_count), "Cases flagged as highest priority in this local desk.")
    with summary_col4:
        metric_card("Overdue Cases", str(overdue_count), "Open cases where the target review date has already passed.")

    if is_manager_role(current_user):
        manager_col1, manager_col2 = st.columns(2)
        with manager_col1:
            st.markdown("#### Team Workload")
            owner_rows = case_metrics.get("owner_breakdown", []) or []
            if owner_rows:
                st.dataframe(pd.DataFrame(owner_rows), use_container_width=True, hide_index=True)
            else:
                st.info("No workload data is available yet.")
        with manager_col2:
            st.markdown("#### Queue By Status")
            status_rows = case_metrics.get("status_breakdown", []) or []
            if status_rows:
                st.dataframe(pd.DataFrame(status_rows), use_container_width=True, hide_index=True)
            else:
                st.info("No queue status data is available yet.")

    filter_col1, filter_col2, filter_col3, filter_col4 = st.columns([1, 1, 1, 1.2])
    with filter_col1:
        selected_statuses = st.multiselect("Show case status", ALLOWED_STATUSES, default=[])
    with filter_col2:
        selected_priorities = st.multiselect("Show priority", ALLOWED_PRIORITIES, default=[])
    with filter_col3:
        owners = sorted(
            [value for value in case_frame.get("owner", pd.Series(dtype=str)).dropna().unique() if value]
        )
        selected_owner = st.selectbox("Assigned to", ["All owners"] + owners)
    with filter_col4:
        case_query = st.text_input("Search case title or notes", placeholder="Search title, summary or owner")

    if current_user_role(current_user) == "Analyst" and selected_owner == "All owners":
        selected_owner = current_user_name(current_user)
        st.caption(f"Analyst view is focused on cases assigned to {selected_owner}.")

    filtered_cases = list_cases(
        statuses=selected_statuses or None,
        priorities=selected_priorities or None,
        owner="" if selected_owner == "All owners" else selected_owner,
        query=case_query,
    )

    requested_case_id = st.session_state.get("selected_case_id")
    if not filtered_cases:
        if requested_case_id:
            st.info("The selected case is hidden by the current Case Desk filters. Clear or widen the filters to see it.")
        st.warning("No cases match the current Case Desk filters.")
        return

    filtered_frame = pd.DataFrame(filtered_cases)
    display_frame = filtered_frame.copy()
    display_frame["Status"] = display_frame["status"]
    display_frame["Priority"] = display_frame["priority"]
    display_frame["Owner"] = display_frame["owner"].replace("", "Unassigned")
    display_frame["Next step"] = display_frame.apply(case_next_step, axis=1)
    display_frame["Draft review"] = display_frame["agent_review_status"].map(friendly_agent_review_status)
    display_frame["Due by"] = display_frame["due_at"].map(format_date)
    display_frame["Updated"] = display_frame["updated_at"].map(format_date)
    st.dataframe(
        display_frame[["title", "Status", "Priority", "Draft review", "Owner", "Due by", "Next step", "Updated"]].rename(
            columns={"title": "Case"}
        ),
        use_container_width=True,
        hide_index=True,
    )

    case_options = [case["id"] for case in filtered_cases]
    case_labels = {
        case["id"]: f"{case['title']} ({case['status']} / {case['priority']})"
        for case in filtered_cases
    }
    selected_case_id = requested_case_id
    if selected_case_id and selected_case_id not in case_options:
        st.info("The selected case is outside the current Case Desk filters, so the first visible case is shown instead.")
    selected_index = case_options.index(selected_case_id) if selected_case_id in case_options else 0
    chosen_case_id = st.selectbox(
        "Choose case to open",
        case_options,
        index=selected_index,
        format_func=lambda case_id: case_labels[case_id],
    )
    st.session_state["selected_case_id"] = chosen_case_id
    current_case = next(case for case in filtered_cases if case["id"] == chosen_case_id)
    render_case_detail(current_case, profiles, enriched, phoenix, current_user)


def render_related_businesses(phoenix: pd.DataFrame) -> None:
    st.subheader("Related Businesses")
    st.markdown(
        '<div class="section-note">These are businesses with very similar names that were registered after serious action. They are leads for checking, not final findings.</div>',
        unsafe_allow_html=True,
    )
    if phoenix.empty:
        st.info("No related-business signals are in the current filtered view.")
        return

    table = phoenix.copy()
    table["Serious action"] = table["most_severe_action"].map(friendly_action)
    table["Registered on"] = table["candidate_registration_date"].replace("", "Not available")
    table["Days after action"] = table["days_after_enforcement"].map(
        lambda value: f"{safe_int(value)} days" if safe_int(value) else "Not available"
    )
    table["Same state?"] = table["same_state"].map(friendly_same_state)
    table = table.sort_values("days_after_enforcement", ascending=True, na_position="last")

    st.dataframe(
        table[
            [
                "source_entity_name",
                "Serious action",
                "candidate_entity_name",
                "candidate_abn",
                "candidate_acn",
                "Registered on",
                "Days after action",
                "Same state?",
            ]
        ].rename(
            columns={
                "source_entity_name": "Original record",
                "candidate_entity_name": "Later business with a similar name",
                "candidate_abn": "ABN",
                "candidate_acn": "ACN",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )


def render_notes() -> None:
    st.subheader("About This Tool")
    with st.expander("Pilot scope", expanded=True):
        st.markdown(read_markdown(DOCS_DIR / "pilot_scope.md"))
    with st.expander("Architecture", expanded=False):
        st.markdown(read_markdown(DOCS_DIR / "architecture.md"))
    with st.expander("Methodology", expanded=False):
        st.markdown(read_markdown(DOCS_DIR / "methodology.md"))
    with st.expander("Pilot success metrics", expanded=False):
        st.markdown(read_markdown(DOCS_DIR / "pilot_success_metrics.md"))
    with st.expander("Support model", expanded=False):
        st.markdown(read_markdown(DOCS_DIR / "support_model.md"))
    with st.expander("Security overview", expanded=False):
        st.markdown(read_markdown(DOCS_DIR / "security_overview.md"))
    with st.expander("Agent diagnostics", expanded=False):
        diagnostics = get_agent_diagnostics()
        llm_cases = [
            case
            for case in list_cases()
            if text_value(case.get("agent_generation_mode")).lower() == "llm"
        ]
        latest_llm_case = None
        if llm_cases:
            latest_llm_case = max(llm_cases, key=lambda case: text_value(case.get("agent_prepared_at")))
        st.markdown(
            f"""
            - Agent enabled: `{diagnostics['enabled']}`
            - Configured endpoint: `{diagnostics['base_url'] or 'not set'}`
            - Configured model alias: `{diagnostics['configured_model'] or 'not set'}`
            - API key: `{diagnostics['api_key_status']}`
            - Reachable endpoints found: `{diagnostics['reachable_count']}`
            """
        )
        if latest_llm_case:
            st.markdown(
                f"""
                - Last live draft case: `{text_value(latest_llm_case.get('title'), 'Unknown case')}`
                - Last requested model alias: `{text_value(latest_llm_case.get('agent_generation_model'), 'not recorded')}`
                - Last resolved backend model: `{text_value(latest_llm_case.get('agent_resolved_model'), text_value(latest_llm_case.get('agent_generation_model'), 'not recorded'))}`
                - Last draft prepared on: `{text_value(latest_llm_case.get('agent_prepared_at'), 'not recorded')}`
                """
            )
        probe_rows = diagnostics.get("probes", [])
        if probe_rows:
            st.dataframe(pd.DataFrame(probe_rows), use_container_width=True, hide_index=True)
        else:
            st.info("No LiteLLM or OpenAI-compatible endpoints were configured.")
    with st.expander("Test plan", expanded=False):
        st.markdown(read_markdown(DOCS_DIR / "test_plan.md"))
    with st.expander("Security and limitations", expanded=False):
        st.markdown(read_markdown(DOCS_DIR / "security_limitations.md"))
    with st.expander("What was checked", expanded=False):
        st.markdown(read_markdown(OUTPUT_DIR / "validation_metrics.md"))
    with st.expander("Review notes", expanded=False):
        st.markdown(read_markdown(OUTPUT_DIR / "review_triage.md"))


def main() -> None:
    inject_css()
    init_db()
    data = load_data()
    pending_view = st.session_state.pop(PENDING_VIEW_KEY, None)
    if pending_view in NAV_OPTIONS:
        st.session_state[ACTIVE_VIEW_KEY] = pending_view
    if ACTIVE_VIEW_KEY not in st.session_state:
        st.session_state[ACTIVE_VIEW_KEY] = "Overview"

    profiles = data["profiles"]
    review_queue = data["review_queue"]
    phoenix = data["phoenix"]
    enriched = data["enriched"]

    filtered_profiles, current_user = (
        apply_profile_filters(profiles)
        if not profiles.empty
        else (
            profiles,
            {
                "display_name": DEFAULT_ANALYST,
                "role": "Analyst",
                "id": "local-demo-user",
            },
        )
    )
    filtered_review = (
        review_queue[review_queue["entity_key"].isin(filtered_profiles["entity_key"])]
        if not review_queue.empty and "entity_key" in review_queue.columns and "entity_key" in filtered_profiles.columns
        else review_queue
    )
    filtered_phoenix = (
        phoenix[phoenix["entity_key"].isin(filtered_profiles["entity_key"])]
        if not phoenix.empty and "entity_key" in phoenix.columns and "entity_key" in filtered_profiles.columns
        else phoenix
    )
    filtered_enriched = (
        enriched[enriched["entity_key"].isin(filtered_profiles["entity_key"])]
        if not enriched.empty and "entity_key" in enriched.columns and "entity_key" in filtered_profiles.columns
        else enriched
    )
    case_metrics = get_case_metrics()

    render_header(filtered_profiles, filtered_review, filtered_phoenix, case_metrics)
    active_index = NAV_OPTIONS.index(st.session_state[ACTIVE_VIEW_KEY]) if st.session_state[ACTIVE_VIEW_KEY] in NAV_OPTIONS else 0
    current_view = st.radio(
        "Open screen",
        NAV_OPTIONS,
        index=active_index,
        horizontal=True,
        label_visibility="collapsed",
    )
    st.session_state[ACTIVE_VIEW_KEY] = current_view
    render_flash_message()

    if current_view == "Overview":
        render_overview(filtered_profiles, filtered_review, filtered_phoenix, case_metrics, current_user)
    elif current_view == "Look Up Record":
        render_entity_explorer(filtered_profiles, filtered_enriched, phoenix, current_user)
    elif current_view == "Needs Review":
        render_review_queue(filtered_review)
    elif current_view == "Case Desk":
        render_case_desk(profiles, enriched, phoenix, current_user, case_metrics)
    elif current_view == "Related Businesses":
        render_related_businesses(filtered_phoenix)
    else:
        render_notes()


if __name__ == "__main__":
    main()

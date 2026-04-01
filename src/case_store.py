from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from src.llm_case_agent import generate_case_prep_draft
from src.public_record_refresh import run_public_record_refresh

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / 'data'
DB_PATH = DATA_DIR / 'app.db'
ATTACHMENTS_DIR = DATA_DIR / 'attachments'

ALLOWED_STATUSES = ('New', 'In Review', 'Escalate', 'Monitor', 'Closed')
ALLOWED_PRIORITIES = ('Low', 'Medium', 'High')
ALLOWED_ROLES = ('Analyst', 'Manager', 'Admin')
AGENT_REVIEW_STATUSES = ('Pending review', 'Accepted', 'Edited', 'Rejected')
AGENT_ACTOR_NAME = 'Pilot Agent'

DEMO_USERS = (
    {
        'username': 'analyst.demo',
        'display_name': 'Demo Analyst',
        'email': 'analyst.demo@pilot.local',
        'role': 'Analyst',
    },
    {
        'username': 'manager.demo',
        'display_name': 'Demo Manager',
        'email': 'manager.demo@pilot.local',
        'role': 'Manager',
    },
    {
        'username': 'admin.demo',
        'display_name': 'Pilot Admin',
        'email': 'admin.demo@pilot.local',
        'role': 'Admin',
    },
)

ACTION_LABELS = {
    'banning_order': 'Banning order',
    'compliance_notice': 'Compliance notice',
    'enforceable_undertaking': 'Enforceable undertaking',
    'revocation': 'Registration revoked',
    'other': 'Other action',
}

MATCH_STATUS_LABELS = {
    'source_abn_exact': 'Confirmed from the notice',
    'searched_name_exact': 'Matched by exact legal name',
    'searched_name_exact_state_mismatch_review': 'Matched by name - state check needed',
    'searched_alias_exact_review': 'Matched using another known name',
    'searched_name_probable': 'Possible match - review needed',
    'searched_name_rejected': 'No safe automatic match',
    'unresolved_missing_abn': 'No public match found',
    'missing_abn_skipped_due_limit': 'Not checked in this run',
}

REVIEW_REQUIRED_MATCHES = {
    'searched_name_exact_state_mismatch_review',
    'searched_alias_exact_review',
    'searched_name_probable',
    'searched_name_rejected',
    'unresolved_missing_abn',
    'missing_abn_skipped_due_limit',
}

STRONG_MATCHES = {'source_abn_exact', 'searched_name_exact'}

SCHEMA = """
CREATE TABLE IF NOT EXISTS cases (
    id TEXT PRIMARY KEY,
    entity_key TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    status TEXT NOT NULL,
    priority TEXT NOT NULL,
    owner TEXT,
    summary TEXT,
    decision TEXT,
    decision_reason TEXT,
    due_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS case_notes (
    id TEXT PRIMARY KEY,
    case_id TEXT NOT NULL,
    author TEXT,
    note_text TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(case_id) REFERENCES cases(id)
);

CREATE TABLE IF NOT EXISTS case_events (
    id TEXT PRIMARY KEY,
    case_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    field_name TEXT,
    before_value TEXT,
    after_value TEXT,
    actor TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY(case_id) REFERENCES cases(id)
);

CREATE TABLE IF NOT EXISTS case_sources (
    id TEXT PRIMARY KEY,
    case_id TEXT NOT NULL,
    source_name TEXT NOT NULL,
    source_type TEXT,
    source_ref TEXT,
    source_url TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY(case_id) REFERENCES cases(id)
);

CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    email TEXT,
    role TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_seen_at TEXT
);

CREATE TABLE IF NOT EXISTS case_attachments (
    id TEXT PRIMARY KEY,
    case_id TEXT NOT NULL,
    file_name TEXT NOT NULL,
    stored_path TEXT NOT NULL,
    content_type TEXT,
    size_bytes INTEGER NOT NULL DEFAULT 0,
    uploaded_by TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY(case_id) REFERENCES cases(id)
);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def text_value(value: Any) -> str:
    if value is None:
        return ''
    try:
        if value != value:
            return ''
    except Exception:
        pass
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            number = float(value)
            if number.is_integer():
                return str(int(number))
    except Exception:
        pass
    if hasattr(value, 'isoformat'):
        return value.isoformat()
    text = str(value).strip()
    return '' if text.lower() == 'nan' else text


def slugify(value: Any) -> str:
    text = ''.join(char.lower() if char.isalnum() else '-' for char in text_value(value))
    text = '-'.join(part for part in text.split('-') if part)
    return text[:80] or 'item'


def action_label(value: Any) -> str:
    return ACTION_LABELS.get(text_value(value), text_value(value) or 'Action')


def match_status_label(value: Any) -> str:
    return MATCH_STATUS_LABELS.get(text_value(value), text_value(value) or 'Needs review')


def parse_date(value: Any) -> date | None:
    text = text_value(value)
    if not text:
        return None
    for candidate in (text, text.replace('Z', '+00:00')):
        try:
            return datetime.fromisoformat(candidate).date()
        except ValueError:
            continue
    for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%d %b %Y'):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def display_date(value: Any) -> str:
    parsed = parse_date(value)
    if parsed is None:
        return text_value(value) or 'date not listed'
    return parsed.strftime('%d %b %Y')


def due_date_for_priority(priority: str, created_value: Any = None) -> str:
    base_date = parse_date(created_value) or datetime.now(timezone.utc).date()
    offset_days = {'High': 2, 'Medium': 5, 'Low': 10}.get(priority, 5)
    return (base_date + timedelta(days=offset_days)).isoformat()


def derive_priority(entity_row: Mapping[str, Any]) -> str:
    severe_action = text_value(entity_row.get('most_severe_action'))
    if severe_action in {'banning_order', 'revocation'}:
        return 'High'
    if severe_action in {'compliance_notice', 'enforceable_undertaking'}:
        return 'Medium'
    return 'Low'


def build_case_title(entity_row: Mapping[str, Any]) -> str:
    name = text_value(entity_row.get('source_entity_name')) or 'Unnamed record'
    action = action_label(entity_row.get('most_severe_action'))
    state = text_value(entity_row.get('source_state')) or 'Unknown state'
    return f'{name} - {action} - {state}'


def build_case_summary(entity_row: Mapping[str, Any]) -> str:
    action = action_label(entity_row.get('most_severe_action'))
    action_date = display_date(entity_row.get('most_recent_action_date'))
    matched_name = text_value(entity_row.get('resolved_entity_name')) or 'No confirmed business record yet'
    match_status = match_status_label(entity_row.get('match_confidence'))
    return (
        f'Public enforcement record for {text_value(entity_row.get("source_entity_name")) or "unnamed record"}. '
        f'Most serious action: {action}. Latest action date: {action_date}. '
        f'Current business record: {matched_name}. Business match: {match_status}.'
    )


def bullet_text(points: Iterable[str]) -> str:
    cleaned = [text_value(point) for point in points if text_value(point)]
    return '\n'.join(f'- {point}' for point in cleaned)


def related_lead_summaries(
    related_records: Iterable[Mapping[str, Any]] | None,
    *,
    limit: int = 3,
) -> list[str]:
    summaries: list[str] = []
    for row in list(related_records or [])[:limit]:
        name = (
            text_value(row.get('candidate_current_name'))
            or text_value(row.get('candidate_company_name'))
            or text_value(row.get('candidate_entity_name'))
            or 'Unnamed related business'
        )
        details: list[str] = []
        abn = text_value(row.get('candidate_abn'))
        if abn:
            details.append(f'ABN {abn}')
        status = text_value(row.get('candidate_status'))
        if status:
            details.append(f'status {status}')
        registration_date = display_date(row.get('candidate_registration_date'))
        if registration_date != 'Not available':
            details.append(f'registered {registration_date}')
        days_after = text_value(row.get('days_after_enforcement'))
        if days_after:
            details.append(f'{days_after} days after the enforcement action')
        state_flag = text_value(row.get('same_state'))
        if state_flag == 'yes':
            details.append('same-state registration')
        elif state_flag == 'no':
            details.append('different-state registration')
        detail_text = ', '.join(details)
        summaries.append(f'{name} ({detail_text}).' if detail_text else f'{name}.')
    return summaries


def build_agent_recommendation(
    entity_row: Mapping[str, Any],
    *,
    related_count: int = 0,
    related_records: Iterable[Mapping[str, Any]] | None = None,
    register_refresh_context: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    entity_name = text_value(entity_row.get('source_entity_name')) or 'Unnamed record'
    action_key = text_value(entity_row.get('most_severe_action'))
    action = action_label(action_key)
    action_date = display_date(entity_row.get('most_recent_action_date'))
    business_name = text_value(entity_row.get('resolved_entity_name')) or 'No confirmed business record yet'
    match_key = text_value(entity_row.get('match_confidence'))
    match_text = match_status_label(match_key)
    source_abn = text_value(entity_row.get('source_abn'))
    source_state = text_value(entity_row.get('source_state')) or 'Unknown state'
    resolved_abn = text_value(entity_row.get('resolved_abn'))
    resolved_acn = text_value(entity_row.get('resolved_acn'))
    asic_status = text_value(entity_row.get('asic_status'))
    asic_registration_date = display_date(entity_row.get('asic_registration_date'))
    review_reason = text_value(entity_row.get('review_reason'))
    derived_priority = derive_priority(entity_row)
    severe_action = action_key in {'banning_order', 'revocation'}
    strong_match = match_key in STRONG_MATCHES
    review_required = match_key in REVIEW_REQUIRED_MATCHES
    related_summaries = related_lead_summaries(related_records)
    refresh_context = dict(register_refresh_context or {})
    abn_refresh = dict(refresh_context.get('abn_details') or {})
    asic_refresh = dict(refresh_context.get('asic_record') or {})
    related_briefs = list(refresh_context.get('related_business_briefs') or [])

    recommended_priority = derived_priority
    recommended_status = 'In Review'
    recommended_decision = 'Needs more evidence'
    rationale = 'The public record is in scope, but it still needs analyst confirmation before any action is taken.'
    next_step = 'Review the notice, confirm the business match, and decide whether to monitor or escalate.'
    completed_check_points = [
        f'Checked the enforcement record: {action} recorded on {action_date} in {source_state}.',
        f'Confirmed the current business-link outcome in the loaded public data: {match_text}.',
    ]
    evidence_points = [
        f'Most serious action recorded: {action} on {action_date}.',
        f'Current business match result: {match_text}.',
    ]
    if business_name != 'No confirmed business record yet':
        evidence_points.append(f'Current linked business record: {business_name}.')
        completed_check_points.append(f'Reviewed the linked business record currently attached to this case: {business_name}.')
    if source_abn and resolved_abn and source_abn == resolved_abn:
        completed_check_points.append(f'Confirmed the linked ABN matches the ABN already present on the enforcement record: {resolved_abn}.')
    elif source_abn and resolved_abn and source_abn != resolved_abn:
        completed_check_points.append(
            f'Compared the ABN on the enforcement record ({source_abn}) with the linked business record ({resolved_abn}) and found a mismatch.'
        )
    elif resolved_abn:
        completed_check_points.append(f'Captured the linked ABN from the public business record: {resolved_abn}.')
    if resolved_abn:
        evidence_points.append(f'ABN found in public records: {resolved_abn}.')
    if resolved_acn:
        evidence_points.append(f'ACN found in public records: {resolved_acn}.')
    if asic_status:
        evidence_points.append(f'Company register status: {asic_status}.')
        completed_check_points.append(f'Checked the linked ASIC company status from the loaded public data: {asic_status}.')
    if text_value(entity_row.get('asic_registration_date')):
        evidence_points.append(f'Company registration date listed: {asic_registration_date}.')
        completed_check_points.append(f'Checked the linked ASIC registration date from the loaded public data: {asic_registration_date}.')
    elif business_name != 'No confirmed business record yet':
        completed_check_points.append('Checked for a linked ASIC company-status row in the loaded public data and none was attached to this case.')
    if review_reason:
        completed_check_points.append(f'Captured the review note attached to the business-link result: {review_reason}.')
    if text_value(abn_refresh.get('entity_name')):
        completed_check_points.append(
            f"Ran a live ABR refresh for ABN {text_value(abn_refresh.get('abn')) or resolved_abn or source_abn} and received entity name {text_value(abn_refresh.get('entity_name'))}."
        )
        if text_value(abn_refresh.get('abn_status')):
            evidence_points.append(
                f"Live ABR status returned: {text_value(abn_refresh.get('abn_status'))}."
            )
    elif text_value(abn_refresh.get('error')):
        completed_check_points.append(
            f"Attempted a live ABR refresh but it did not complete cleanly: {text_value(abn_refresh.get('error'))}."
        )
    if text_value(asic_refresh.get('asic_company_name')) or text_value(asic_refresh.get('asic_current_name')):
        completed_check_points.append(
            f"Checked the latest local ASIC snapshot and matched {text_value(asic_refresh.get('asic_company_name')) or text_value(asic_refresh.get('asic_current_name'))}."
        )
        if text_value(asic_refresh.get('asic_status')):
            evidence_points.append(
                f"Latest local ASIC snapshot status: {text_value(asic_refresh.get('asic_status'))}."
            )

    human_check_points = [
        'Accept, edit, or reject the draft narrative before any escalation or supervisor briefing.',
    ]

    if severe_action and strong_match:
        recommended_priority = 'High'
        recommended_status = 'Escalate'
        recommended_decision = 'Escalate for review'
        rationale = (
            'The business match is strong and the most serious action is severe enough to justify supervisor review.'
        )
        next_step = 'Prepare a supervisor-ready brief with the public source links and any related-business checks.'
    elif review_required:
        recommended_priority = 'High' if severe_action else derived_priority
        recommended_status = 'In Review'
        recommended_decision = 'Needs more evidence'
        rationale = (
            'The public record is relevant, but the business match or supporting context is still uncertain and needs a person to confirm it.'
        )
        next_step = 'Confirm the business link and review the supporting evidence before any escalation decision.'
        human_check_points.append('Decide whether the current business match is strong enough to rely on operationally.')
    elif action_key in {'compliance_notice', 'enforceable_undertaking'} and strong_match:
        recommended_priority = 'Medium'
        recommended_status = 'Monitor'
        recommended_decision = 'Monitor'
        rationale = (
            'The business match is credible, but the current enforcement posture is better suited to monitoring unless new evidence appears.'
        )
        next_step = 'Monitor for further enforcement activity, repeat notices, or newly registered related businesses.'

    if related_count > 0:
        lead_label = 'lead' if related_count == 1 else 'leads'
        rationale = f'{rationale} {related_count} related-business {lead_label} should also be checked.'
        evidence_points.append(f'{related_count} related-business {lead_label} identified from the public company data.')
        human_check_points.append(
            f'Decide whether the {related_count} related-business {lead_label} materially strengthen the case for escalation.'
        )
        if related_summaries:
            completed_check_points.extend(
                f'Prepared a related-business lead brief: {summary}'
                for summary in related_summaries
            )
        else:
            completed_check_points.append(
                f'Prepared a related-business queue for {related_count} public company-data {lead_label}.'
            )
        if recommended_status == 'Escalate':
            next_step = (
                f'{next_step} Include the {related_count} related-business {lead_label} in the escalation pack.'
            )
        else:
            next_step = (
                f'{next_step} Review the {related_count} related-business {lead_label} before closing the case.'
            )
    else:
        completed_check_points.append('Checked the current related-business queue and found no later company-registration leads for this case.')
    if related_briefs:
        completed_check_points.extend(
            f"Added a case-pack mini-brief for related business: {text_value(brief.get('summary'))}"
            for brief in related_briefs[:3]
            if text_value(brief.get('summary'))
        )

    if not strong_match:
        human_check_points.append('Decide whether the notice and the suggested business record refer to the same entity.')
    if not resolved_abn:
        human_check_points.append('Decide whether the business identity is strong enough to use, because no confirmed ABN is linked yet.')
    if not asic_status:
        human_check_points.append(
            'Decide whether a manual ASIC extract is needed before briefing, because no linked current company-status row is attached in the loaded public data.'
        )

    summary = (
        f'Agent draft for {entity_name}. Most serious action: {action} on {action_date}. '
        f'Current business record: {business_name}. Match result: {match_text}. '
        f'Recommended next step: {next_step}'
    )

    return {
        'agent_summary': summary,
        'agent_recommended_priority': recommended_priority,
        'agent_recommended_status': recommended_status,
        'agent_recommended_decision': recommended_decision,
        'agent_rationale': rationale,
        'agent_completed_checks': bullet_text(completed_check_points),
        'agent_supporting_evidence': bullet_text(evidence_points),
        'agent_human_checks': bullet_text(human_check_points),
        'agent_next_step': next_step,
        'agent_generation_mode': 'rules',
        'agent_generation_model': 'deterministic-v1',
        'agent_resolved_model': 'deterministic-v1',
        'agent_generation_notes': 'Draft generated from deterministic public-record rules.',
    }


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    ATTACHMENTS_DIR.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


def ensure_column(connection: sqlite3.Connection, table_name: str, column_name: str, column_sql: str) -> None:
    current_columns = {
        row['name']
        for row in connection.execute(f'PRAGMA table_info({table_name})').fetchall()
    }
    if column_name not in current_columns:
        connection.execute(f'ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}')


def seed_default_users(connection: sqlite3.Connection) -> None:
    now = utc_now()
    for user in DEMO_USERS:
        existing = connection.execute(
            'SELECT id FROM users WHERE username = ?',
            (user['username'],),
        ).fetchone()
        if existing is not None:
            continue
        connection.execute(
            """
            INSERT INTO users (
                id, username, display_name, email, role, is_active, created_at, updated_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                user['username'],
                user['display_name'],
                user['email'],
                user['role'],
                now,
                now,
                '',
            ),
        )


def backfill_case_due_dates(connection: sqlite3.Connection) -> None:
    rows = connection.execute(
        "SELECT id, priority, created_at, due_at FROM cases WHERE due_at IS NULL OR TRIM(due_at) = ''"
    ).fetchall()
    for row in rows:
        connection.execute(
            'UPDATE cases SET due_at = ? WHERE id = ?',
            (due_date_for_priority(text_value(row['priority']) or 'Medium', row['created_at']), row['id']),
        )


def backfill_event_actor_types(connection: sqlite3.Connection) -> None:
    connection.execute(
        "UPDATE case_events SET actor_type = 'human' WHERE actor_type IS NULL OR TRIM(actor_type) = ''"
    )


def backfill_agent_review_status(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        UPDATE cases
        SET agent_review_status = 'Pending review'
        WHERE agent_review_status IS NULL OR TRIM(agent_review_status) = ''
        """
    )


def init_db(db_path: Path = DB_PATH) -> None:
    with get_connection(db_path) as connection:
        connection.executescript(SCHEMA)
        ensure_column(connection, 'cases', 'due_at', 'TEXT')
        ensure_column(connection, 'cases', 'agent_summary', 'TEXT')
        ensure_column(connection, 'cases', 'agent_recommended_priority', 'TEXT')
        ensure_column(connection, 'cases', 'agent_recommended_status', 'TEXT')
        ensure_column(connection, 'cases', 'agent_recommended_decision', 'TEXT')
        ensure_column(connection, 'cases', 'agent_rationale', 'TEXT')
        ensure_column(connection, 'cases', 'agent_completed_checks', 'TEXT')
        ensure_column(connection, 'cases', 'agent_supporting_evidence', 'TEXT')
        ensure_column(connection, 'cases', 'agent_human_checks', 'TEXT')
        ensure_column(connection, 'cases', 'agent_next_step', 'TEXT')
        ensure_column(connection, 'cases', 'agent_prepared_at', 'TEXT')
        ensure_column(connection, 'cases', 'agent_review_status', 'TEXT')
        ensure_column(connection, 'cases', 'agent_reviewed_by', 'TEXT')
        ensure_column(connection, 'cases', 'agent_reviewed_at', 'TEXT')
        ensure_column(connection, 'cases', 'agent_generation_mode', 'TEXT')
        ensure_column(connection, 'cases', 'agent_generation_model', 'TEXT')
        ensure_column(connection, 'cases', 'agent_resolved_model', 'TEXT')
        ensure_column(connection, 'cases', 'agent_generation_notes', 'TEXT')
        ensure_column(connection, 'cases', 'register_refresh_summary', 'TEXT')
        ensure_column(connection, 'cases', 'register_refresh_context', 'TEXT')
        ensure_column(connection, 'cases', 'register_refreshed_at', 'TEXT')
        ensure_column(connection, 'case_events', 'actor_type', 'TEXT')
        seed_default_users(connection)
        backfill_case_due_dates(connection)
        backfill_event_actor_types(connection)
        backfill_agent_review_status(connection)
        connection.commit()


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


def parse_json_text(value: Any) -> dict[str, Any]:
    text = text_value(value)
    if not text:
        return {}
    try:
        loaded = json.loads(text)
    except Exception:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def record_event(
    connection: sqlite3.Connection,
    *,
    case_id: str,
    event_type: str,
    field_name: str = '',
    before_value: str = '',
    after_value: str = '',
    actor: str = '',
    actor_type: str = 'human',
) -> None:
    connection.execute(
        """
        INSERT INTO case_events (
            id, case_id, event_type, field_name, before_value, after_value, actor, actor_type, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()),
            case_id,
            event_type,
            field_name,
            before_value,
            after_value,
            actor,
            actor_type,
            utc_now(),
        ),
    )


def list_users(active_only: bool = True) -> list[dict[str, Any]]:
    init_db()
    sql = 'SELECT * FROM users'
    params: list[Any] = []
    if active_only:
        sql += ' WHERE is_active = 1'
    sql += ' ORDER BY CASE role WHEN "Manager" THEN 0 WHEN "Analyst" THEN 1 ELSE 2 END, display_name ASC'
    with get_connection() as connection:
        rows = connection.execute(sql, params).fetchall()
    return [row_to_dict(row) for row in rows if row is not None]


def get_user(user_id: str) -> dict[str, Any] | None:
    init_db()
    with get_connection() as connection:
        row = connection.execute('SELECT * FROM users WHERE id = ?', (user_id,)).fetchone()
    return row_to_dict(row)


def get_default_user() -> dict[str, Any] | None:
    users = list_users(active_only=True)
    return users[0] if users else None


def touch_user(user_id: str) -> None:
    if not text_value(user_id):
        return
    init_db()
    now = utc_now()
    with get_connection() as connection:
        connection.execute(
            'UPDATE users SET last_seen_at = ?, updated_at = ? WHERE id = ?',
            (now, now, user_id),
        )
        connection.commit()


def get_case(case_id: str) -> dict[str, Any] | None:
    init_db()
    with get_connection() as connection:
        row = connection.execute('SELECT * FROM cases WHERE id = ?', (case_id,)).fetchone()
    return row_to_dict(row)


def get_case_by_entity_key(entity_key: str) -> dict[str, Any] | None:
    init_db()
    with get_connection() as connection:
        row = connection.execute('SELECT * FROM cases WHERE entity_key = ?', (entity_key,)).fetchone()
    return row_to_dict(row)


def create_case_from_entity(
    entity_row: Mapping[str, Any],
    actor: str = '',
    owner: str = '',
    related_count: int = 0,
    related_records: Iterable[Mapping[str, Any]] | None = None,
) -> tuple[dict[str, Any], bool]:
    init_db()
    entity_key = text_value(entity_row.get('entity_key'))
    if not entity_key:
        raise ValueError('entity_row must include entity_key')

    existing = get_case_by_entity_key(entity_key)
    if existing is not None:
        return existing, False

    now = utc_now()
    case_id = str(uuid.uuid4())
    priority = derive_priority(entity_row)
    owner_value = text_value(owner) or text_value(actor)
    due_at = due_date_for_priority(priority, now)

    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO cases (
                id, entity_key, title, status, priority, owner, summary, decision,
                decision_reason, due_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                case_id,
                entity_key,
                build_case_title(entity_row),
                'New',
                priority,
                owner_value,
                '',
                '',
                '',
                due_at,
                now,
                now,
            ),
        )
        record_event(
            connection,
            case_id=case_id,
            event_type='case_created',
            after_value='Case created from lookup record',
            actor=text_value(actor),
            actor_type='human',
        )
        connection.commit()

    refresh_agent_draft(
        case_id,
        entity_row,
        related_count=related_count,
        related_records=related_records,
        actor=AGENT_ACTOR_NAME,
    )
    created = get_case(case_id)
    if created is None:
        raise RuntimeError('Case creation failed')
    return created, True


def replace_case_sources(case_id: str, sources: Iterable[Mapping[str, Any]]) -> None:
    init_db()
    now = utc_now()
    seen: set[tuple[str, str, str]] = set()
    rows: list[tuple[str, str, str, str, str, str, str]] = []

    for source in sources:
        source_name = text_value(source.get('source_name'))
        source_type = text_value(source.get('source_type'))
        source_ref = text_value(source.get('source_ref'))
        source_url = text_value(source.get('source_url'))
        if not source_name:
            continue
        dedupe_key = (source_name, source_ref, source_url)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        rows.append((str(uuid.uuid4()), case_id, source_name, source_type, source_ref, source_url, now))

    with get_connection() as connection:
        connection.execute('DELETE FROM case_sources WHERE case_id = ?', (case_id,))
        if rows:
            connection.executemany(
                """
                INSERT INTO case_sources (
                    id, case_id, source_name, source_type, source_ref, source_url, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        connection.execute('UPDATE cases SET updated_at = ? WHERE id = ?', (now, case_id))
        connection.commit()


def merge_case_sources(case_id: str, sources: Iterable[Mapping[str, Any]]) -> None:
    combined = [*list_sources(case_id), *list(sources)]
    replace_case_sources(case_id, combined)


def refresh_case_public_registers(
    case_id: str,
    entity_row: Mapping[str, Any],
    *,
    related_records: Iterable[Mapping[str, Any]] | None = None,
    actor: str = '',
) -> dict[str, Any] | None:
    init_db()
    current = get_case(case_id)
    if current is None:
        return None

    refresh_payload = run_public_record_refresh(
        entity_row,
        related_records=related_records,
    )
    now = utc_now()
    summary_text = text_value(refresh_payload.get('summary'))
    context_text = json.dumps(refresh_payload.get('context') or {}, ensure_ascii=True, indent=2)

    with get_connection() as connection:
        connection.execute(
            """
            UPDATE cases
            SET register_refresh_summary = ?, register_refresh_context = ?, register_refreshed_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (summary_text, context_text, now, now, case_id),
        )
        record_event(
            connection,
            case_id=case_id,
            event_type='register_refresh_completed',
            after_value=summary_text,
            actor=text_value(actor),
            actor_type='human',
        )
        connection.commit()

    source_rows = refresh_payload.get('source_rows') or []
    if source_rows:
        merge_case_sources(case_id, source_rows)

    snapshot_markdown = text_value(refresh_payload.get('snapshot_markdown'))
    snapshot_file_name = text_value(refresh_payload.get('snapshot_file_name')) or 'public-register-refresh.md'
    if snapshot_markdown:
        add_attachment(
            case_id,
            file_name=snapshot_file_name,
            content_bytes=snapshot_markdown.encode('utf-8'),
            content_type='text/markdown',
            uploaded_by=text_value(actor) or 'Pilot Agent',
        )

    return get_case(case_id)


def list_sources(case_id: str) -> list[dict[str, Any]]:
    init_db()
    with get_connection() as connection:
        rows = connection.execute(
            'SELECT * FROM case_sources WHERE case_id = ? ORDER BY created_at ASC, source_name ASC',
            (case_id,),
        ).fetchall()
    return [row_to_dict(row) for row in rows if row is not None]


def refresh_agent_draft(
    case_id: str,
    entity_row: Mapping[str, Any],
    *,
    related_count: int = 0,
    related_records: Iterable[Mapping[str, Any]] | None = None,
    register_refresh_context: Mapping[str, Any] | None = None,
    actor: str = AGENT_ACTOR_NAME,
    force: bool = False,
) -> dict[str, Any] | None:
    init_db()
    current = get_case(case_id)
    if current is None:
        return None

    if not force and text_value(current.get('agent_summary')):
        return current

    active_refresh_context = dict(register_refresh_context or parse_json_text(current.get('register_refresh_context')))
    fallback_draft = build_agent_recommendation(
        entity_row,
        related_count=related_count,
        related_records=related_records,
        register_refresh_context=active_refresh_context,
    )
    recommendation = generate_case_prep_draft(
        entity_row,
        related_count=related_count,
        related_records=related_records,
        register_refresh_context=active_refresh_context,
        fallback_draft=fallback_draft,
    )
    now = utc_now()
    with get_connection() as connection:
        for field_name, value in recommendation.items():
            connection.execute(f'UPDATE cases SET {field_name} = ? WHERE id = ?', (value, case_id))
        connection.execute(
            """
            UPDATE cases
            SET agent_prepared_at = ?, agent_review_status = ?, agent_reviewed_by = ?, agent_reviewed_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (now, 'Pending review', '', '', now, case_id),
        )
        record_event(
            connection,
            case_id=case_id,
            event_type='agent_draft_prepared',
            after_value=text_value(recommendation.get('agent_generation_notes')) or text_value(recommendation.get('agent_next_step')),
            actor=(
                text_value(recommendation.get('agent_generation_model'))
                or text_value(actor)
                or AGENT_ACTOR_NAME
            ),
            actor_type='agent',
        )
        connection.commit()
    return get_case(case_id)


def review_agent_draft(case_id: str, outcome: str, *, actor: str = '') -> dict[str, Any] | None:
    init_db()
    current = get_case(case_id)
    if current is None:
        return None

    outcome_value = text_value(outcome)
    if outcome_value not in AGENT_REVIEW_STATUSES:
        raise ValueError(f'Invalid agent review outcome: {outcome_value}')

    actor_value = text_value(actor)
    now = utc_now()
    with get_connection() as connection:
        if outcome_value == 'Accepted':
            accepted_updates = {
                'summary': text_value(current.get('agent_summary')),
                'priority': text_value(current.get('agent_recommended_priority')),
                'status': text_value(current.get('agent_recommended_status')),
                'decision': text_value(current.get('agent_recommended_decision')),
                'decision_reason': text_value(current.get('agent_rationale')),
            }
            for field_name, after_value in accepted_updates.items():
                if not after_value:
                    continue
                before_value = text_value(current.get(field_name))
                if before_value == after_value:
                    continue
                connection.execute(f'UPDATE cases SET {field_name} = ? WHERE id = ?', (after_value, case_id))
                record_event(
                    connection,
                    case_id=case_id,
                    event_type='field_updated',
                    field_name=field_name,
                    before_value=before_value,
                    after_value=after_value,
                    actor=actor_value,
                    actor_type='human',
                )

        connection.execute(
            """
            UPDATE cases
            SET agent_review_status = ?, agent_reviewed_by = ?, agent_reviewed_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (outcome_value, actor_value, now, now, case_id),
        )
        record_event(
            connection,
            case_id=case_id,
            event_type='agent_draft_reviewed',
            after_value=outcome_value,
            actor=actor_value,
            actor_type='human',
        )
        connection.commit()

    return get_case(case_id)


def add_attachment(
    case_id: str,
    *,
    file_name: str,
    content_bytes: bytes,
    content_type: str = '',
    uploaded_by: str = '',
) -> dict[str, Any] | None:
    safe_name = text_value(file_name)
    if not safe_name or not content_bytes:
        return None

    init_db()
    attachment_id = str(uuid.uuid4())
    target_dir = ATTACHMENTS_DIR / case_id
    target_dir.mkdir(parents=True, exist_ok=True)
    stored_name = f'{attachment_id}-{slugify(safe_name)}'
    suffix = Path(safe_name).suffix
    stored_path = target_dir / f'{stored_name}{suffix}' if suffix else target_dir / stored_name
    stored_path.write_bytes(content_bytes)

    now = utc_now()
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO case_attachments (
                id, case_id, file_name, stored_path, content_type, size_bytes, uploaded_by, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                attachment_id,
                case_id,
                safe_name,
                str(stored_path),
                text_value(content_type),
                len(content_bytes),
                text_value(uploaded_by),
                now,
            ),
        )
        record_event(
            connection,
            case_id=case_id,
            event_type='attachment_added',
            after_value=safe_name,
            actor=text_value(uploaded_by),
        )
        connection.execute('UPDATE cases SET updated_at = ? WHERE id = ?', (now, case_id))
        connection.commit()

    with get_connection() as connection:
        row = connection.execute('SELECT * FROM case_attachments WHERE id = ?', (attachment_id,)).fetchone()
    return row_to_dict(row)


def list_attachments(case_id: str) -> list[dict[str, Any]]:
    init_db()
    with get_connection() as connection:
        rows = connection.execute(
            'SELECT * FROM case_attachments WHERE case_id = ? ORDER BY created_at DESC, file_name ASC',
            (case_id,),
        ).fetchall()
    return [row_to_dict(row) for row in rows if row is not None]


def list_cases(
    *,
    statuses: Iterable[str] | None = None,
    priorities: Iterable[str] | None = None,
    owner: str = '',
    query: str = '',
) -> list[dict[str, Any]]:
    init_db()
    sql = 'SELECT * FROM cases WHERE 1=1'
    params: list[Any] = []

    status_list = [text_value(value) for value in (statuses or []) if text_value(value)]
    if status_list:
        sql += f" AND status IN ({', '.join('?' for _ in status_list)})"
        params.extend(status_list)

    priority_list = [text_value(value) for value in (priorities or []) if text_value(value)]
    if priority_list:
        sql += f" AND priority IN ({', '.join('?' for _ in priority_list)})"
        params.extend(priority_list)

    owner_value = text_value(owner)
    if owner_value:
        sql += ' AND LOWER(COALESCE(owner, "")) = ?'
        params.append(owner_value.lower())

    query_value = text_value(query).lower()
    if query_value:
        sql += (
            ' AND (LOWER(title) LIKE ? OR LOWER(summary) LIKE ? OR LOWER(owner) LIKE ? '
            'OR LOWER(decision_reason) LIKE ? OR LOWER(COALESCE(agent_summary, "")) LIKE ? '
            'OR LOWER(COALESCE(agent_rationale, "")) LIKE ? OR LOWER(COALESCE(agent_completed_checks, "")) LIKE ? '
            'OR LOWER(COALESCE(agent_supporting_evidence, "")) LIKE ? '
            'OR LOWER(COALESCE(agent_human_checks, "")) LIKE ? OR LOWER(COALESCE(agent_next_step, "")) LIKE ? '
            'OR LOWER(COALESCE(register_refresh_summary, "")) LIKE ?)'
        )
        like_value = f'%{query_value}%'
        params.extend([like_value, like_value, like_value, like_value, like_value, like_value, like_value, like_value, like_value, like_value, like_value])

    sql += (
        ' ORDER BY '
        'CASE priority WHEN "High" THEN 0 WHEN "Medium" THEN 1 ELSE 2 END, '
        'CASE WHEN due_at IS NULL OR TRIM(due_at) = "" THEN 1 ELSE 0 END, '
        'due_at ASC, updated_at DESC'
    )

    with get_connection() as connection:
        rows = connection.execute(sql, params).fetchall()
    return [row_to_dict(row) for row in rows if row is not None]


def update_case(case_id: str, updates: Mapping[str, Any], actor: str = '') -> dict[str, Any] | None:
    init_db()
    current = get_case(case_id)
    if current is None:
        return None

    allowed_fields = {
        'title',
        'status',
        'priority',
        'owner',
        'summary',
        'decision',
        'decision_reason',
        'due_at',
    }
    actor_value = text_value(actor)

    with get_connection() as connection:
        changed = False
        for field_name, raw_value in updates.items():
            if field_name not in allowed_fields:
                continue
            before_value = text_value(current.get(field_name))
            after_value = text_value(raw_value)
            if field_name == 'status' and after_value and after_value not in ALLOWED_STATUSES:
                raise ValueError(f'Invalid status: {after_value}')
            if field_name == 'priority' and after_value and after_value not in ALLOWED_PRIORITIES:
                raise ValueError(f'Invalid priority: {after_value}')
            if field_name == 'due_at' and after_value and parse_date(after_value) is None:
                raise ValueError(f'Invalid due date: {after_value}')
            if before_value == after_value:
                continue
            connection.execute(f'UPDATE cases SET {field_name} = ? WHERE id = ?', (after_value, case_id))
            record_event(
                connection,
                case_id=case_id,
                event_type='field_updated',
                field_name=field_name,
                before_value=before_value,
                after_value=after_value,
                actor=actor_value,
            )
            changed = True

        if changed:
            connection.execute('UPDATE cases SET updated_at = ? WHERE id = ?', (utc_now(), case_id))
        connection.commit()

    return get_case(case_id)


def add_note(case_id: str, note_text: str, author: str = '') -> dict[str, Any] | None:
    note_value = text_value(note_text)
    if not note_value:
        return None
    init_db()
    note_id = str(uuid.uuid4())
    author_value = text_value(author)
    now = utc_now()
    with get_connection() as connection:
        connection.execute(
            'INSERT INTO case_notes (id, case_id, author, note_text, created_at) VALUES (?, ?, ?, ?, ?)',
            (note_id, case_id, author_value, note_value, now),
        )
        record_event(
            connection,
            case_id=case_id,
            event_type='note_added',
            after_value=note_value,
            actor=author_value,
        )
        connection.execute('UPDATE cases SET updated_at = ? WHERE id = ?', (now, case_id))
        connection.commit()

    with get_connection() as connection:
        row = connection.execute('SELECT * FROM case_notes WHERE id = ?', (note_id,)).fetchone()
    return row_to_dict(row)


def list_notes(case_id: str) -> list[dict[str, Any]]:
    init_db()
    with get_connection() as connection:
        rows = connection.execute(
            'SELECT * FROM case_notes WHERE case_id = ? ORDER BY created_at DESC',
            (case_id,),
        ).fetchall()
    return [row_to_dict(row) for row in rows if row is not None]


def list_events(case_id: str) -> list[dict[str, Any]]:
    init_db()
    with get_connection() as connection:
        rows = connection.execute(
            'SELECT * FROM case_events WHERE case_id = ? ORDER BY created_at DESC',
            (case_id,),
        ).fetchall()
    return [row_to_dict(row) for row in rows if row is not None]


def build_owner_metrics(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    today = datetime.now(timezone.utc).date()
    buckets: dict[str, dict[str, Any]] = {}
    for case in cases:
        owner = text_value(case.get('owner')) or 'Unassigned'
        bucket = buckets.setdefault(
            owner,
            {'Owner': owner, 'Open cases': 0, 'High priority': 0, 'Escalated': 0, 'Overdue': 0},
        )
        if text_value(case.get('status')) != 'Closed':
            bucket['Open cases'] += 1
        if text_value(case.get('priority')) == 'High':
            bucket['High priority'] += 1
        if text_value(case.get('status')) == 'Escalate':
            bucket['Escalated'] += 1
        due_date = parse_date(case.get('due_at'))
        if due_date is not None and due_date < today and text_value(case.get('status')) != 'Closed':
            bucket['Overdue'] += 1
    return sorted(buckets.values(), key=lambda row: (-row['Open cases'], row['Owner']))


def build_status_metrics(cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for status in ALLOWED_STATUSES:
        matching = [case for case in cases if text_value(case.get('status')) == status]
        rows.append(
            {
                'Status': status,
                'Count': len(matching),
                'High priority': sum(1 for case in matching if text_value(case.get('priority')) == 'High'),
            }
        )
    return rows


def get_case_metrics() -> dict[str, Any]:
    init_db()
    with get_connection() as connection:
        case_rows = [row_to_dict(row) for row in connection.execute('SELECT * FROM cases').fetchall()]
        recent_events = [
            row_to_dict(row)
            for row in connection.execute('SELECT * FROM case_events ORDER BY created_at DESC LIMIT 8').fetchall()
        ]

    cases = [row for row in case_rows if row is not None]
    today = datetime.now(timezone.utc).date()
    open_cases = [case for case in cases if text_value(case.get('status')) != 'Closed']
    overdue_cases = [
        case
        for case in open_cases
        if parse_date(case.get('due_at')) is not None and parse_date(case.get('due_at')) < today
    ]

    return {
        'total_cases': len(cases),
        'open_cases': len(open_cases),
        'escalated_cases': sum(1 for case in cases if text_value(case.get('status')) == 'Escalate'),
        'high_priority_cases': sum(1 for case in cases if text_value(case.get('priority')) == 'High'),
        'overdue_cases': len(overdue_cases),
        'recent_events': recent_events,
        'owner_breakdown': build_owner_metrics(cases),
        'status_breakdown': build_status_metrics(cases),
    }

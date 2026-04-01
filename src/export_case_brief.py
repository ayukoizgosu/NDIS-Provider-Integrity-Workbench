from __future__ import annotations

import json
import re
from datetime import datetime
from html import escape
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = BASE_DIR / 'output' / 'case_briefs'

ACTION_LABELS = {
    'banning_order': 'Banning order',
    'compliance_notice': 'Compliance notice',
    'enforceable_undertaking': 'Enforceable undertaking',
    'other': 'Other action',
    'revocation': 'Registration revoked',
}

MATCH_LABELS = {
    'source_abn_exact': 'Confirmed from the notice',
    'searched_name_exact': 'Matched by exact legal name',
    'searched_name_exact_state_mismatch_review': 'Matched by name - state check needed',
    'searched_alias_exact_review': 'Matched using another known name',
    'searched_name_probable': 'Possible match - review needed',
    'searched_name_rejected': 'No safe automatic match',
    'unresolved_missing_abn': 'No public match found',
    'missing_abn_skipped_due_limit': 'Not checked in this run',
}

MATCH_DETAIL_LABELS = {
    'source_abn_exact': 'The business identifier already appeared on the enforcement notice.',
    'searched_name_exact': 'The legal business name matched directly in public business records.',
    'searched_name_exact_state_mismatch_review': 'The legal name matched, but the recorded business state differed from the notice and still needs a human check.',
    'searched_alias_exact_review': 'The match was found through an alternate business or trading name and should stay in review.',
    'searched_name_probable': 'A likely business record was found, but the public evidence was not strong enough to accept automatically.',
    'searched_name_rejected': 'Public business search returned candidates, but none were strong enough to rely on.',
    'unresolved_missing_abn': 'No safe public business match was found.',
    'missing_abn_skipped_due_limit': 'This record was not searched in the current run.',
}

ASIC_STATUS_LABELS = {
    'REGD': 'Registered',
    'DRGD': 'Deregistered',
    'EXAD': 'External administration',
    'SOFF': 'Struck off',
}


def text_value(value: Any, fallback: str = 'Not available') -> str:
    if value is None:
        return fallback
    try:
        if value != value:
            return fallback
    except Exception:
        pass
    text = str(value).strip()
    return fallback if not text or text.lower() == 'nan' else text


def format_date(value: Any) -> str:
    if value is None:
        return 'Not available'
    try:
        if value != value:
            return 'Not available'
    except Exception:
        pass
    text = text_value(value, '')
    parsed = pd.NaT
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y"):
        try:
            parsed = pd.Timestamp(datetime.strptime(text, fmt))
            break
        except Exception:
            continue
    if pd.isna(parsed):
        parsed = pd.to_datetime(value, errors='coerce')
    if pd.isna(parsed):
        return text_value(value)
    return parsed.strftime('%d %b %Y')


def action_label(value: Any) -> str:
    return ACTION_LABELS.get(text_value(value, ''), text_value(value))


def match_label(value: Any) -> str:
    return MATCH_LABELS.get(text_value(value, ''), text_value(value, 'Review needed'))


def match_detail(value: Any) -> str:
    return MATCH_DETAIL_LABELS.get(text_value(value, ''), 'Human review is still required.')


def entity_type_label(value: Any) -> str:
    text = text_value(value, '')
    return text.replace('_', ' ').title() if text else 'Not listed'


def asic_status_label(value: Any) -> str:
    text = text_value(value, '')
    return ASIC_STATUS_LABELS.get(text, text or 'Not linked yet')


def format_days(value: Any) -> str:
    text = text_value(value, '')
    if not text:
        return 'Not available'
    try:
        number = int(float(text))
        return f'{number} days'
    except (TypeError, ValueError):
        return text


def display_source_ref(value: Any) -> str:
    text = text_value(value, '')
    if not text:
        return ''
    if '\\' in text or '/' in text:
        return Path(text).name
    return text


def build_executive_summary(
    case: Mapping[str, Any],
    profile: Mapping[str, Any],
    related_businesses: pd.DataFrame,
) -> str:
    name = text_value(profile.get('source_entity_name'), 'This record')
    action = action_label(profile.get('most_severe_action'))
    action_date = format_date(profile.get('most_recent_action_date'))
    business_name = text_value(profile.get('resolved_entity_name'), '')
    match_text = match_label(profile.get('match_confidence'))

    parts = [
        f'{name} has an NDIS enforcement record in scope.',
        f'The most serious action currently shown is {action}, dated {action_date}.',
    ]
    if business_name:
        parts.append(f'The current public business match is {business_name} with match outcome {match_text}.')
    else:
        parts.append('No confirmed public business record has been linked yet.')
    if not related_businesses.empty:
        count = len(related_businesses.index)
        lead_label = 'lead' if count == 1 else 'leads'
        parts.append(f'{count} related-business {lead_label} should also be checked during review.')
    decision = text_value(case.get('decision'), '')
    if decision:
        parts.append(f'Current analyst decision: {decision}.')
    return ' '.join(parts)


def build_recommended_next_step(case: Mapping[str, Any], profile: Mapping[str, Any]) -> str:
    decision_reason = text_value(case.get('decision_reason'), '')
    if decision_reason:
        return decision_reason
    decision = text_value(case.get('decision'), '')
    if decision:
        return f'Current analyst decision is {decision}.'
    agent_next_step = text_value(case.get('agent_next_step'), '')
    if agent_next_step:
        return agent_next_step
    return match_detail(profile.get('match_confidence'))


def slugify(value: str) -> str:
    return re.sub(r'[^a-zA-Z0-9]+', '-', value).strip('-').lower()[:80] or 'case-brief'


def build_table(headers: list[str], rows: list[list[str]]) -> str:
    header_html = ''.join(f'<th>{escape(header)}</th>' for header in headers)
    row_html = ''.join(
        '<tr>' + ''.join(f'<td>{escape(cell)}</td>' for cell in row) + '</tr>'
        for row in rows
    )
    return f'<table><thead><tr>{header_html}</tr></thead><tbody>{row_html}</tbody></table>'


def build_bullet_list(value: Any, fallback: str) -> str:
    text = text_value(value, '')
    if not text:
        return f'<ul><li>{escape(fallback)}</li></ul>'
    items: list[str] = []
    for raw_line in text.replace('\r', '\n').splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith(('- ', '* ', '\u2022 ')):
            line = line[2:].strip()
        items.append(line)
    if not items:
        items = [fallback]
    return '<ul>' + ''.join(f'<li>{escape(item)}</li>' for item in items) + '</ul>'


def parse_json_text(value: Any) -> dict[str, Any]:
    text = text_value(value, '')
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def related_brief_list(related_businesses: pd.DataFrame, refresh_context: Mapping[str, Any]) -> str:
    related_briefs = list((refresh_context.get('related_business_briefs') or [])) if isinstance(refresh_context, Mapping) else []
    items: list[str] = []
    for brief in related_briefs:
        summary = text_value(brief.get('summary')) if isinstance(brief, Mapping) else ''
        if summary:
            items.append(summary)
    if not items and not related_businesses.empty:
        related_copy = related_businesses.sort_values('days_after_enforcement', ascending=True).copy()
        for _, row in related_copy.head(5).iterrows():
            name = text_value(row.get('candidate_entity_name'))
            abn = text_value(row.get('candidate_abn'), 'ABN not listed')
            status = asic_status_label(row.get('candidate_status'))
            registration = format_date(row.get('candidate_registration_date'))
            days_after = format_days(row.get('days_after_enforcement'))
            items.append(
                f"{name} ({abn}) is listed as {status}. Registered on {registration}; timing after enforcement: {days_after}."
            )
    if not items:
        items = ['No related-business mini-briefs were prepared.']
    return '<ul>' + ''.join(f'<li>{escape(item)}</li>' for item in items) + '</ul>'


def export_case_brief(
    *,
    case: Mapping[str, Any],
    profile: Mapping[str, Any],
    history: pd.DataFrame,
    related_businesses: pd.DataFrame,
    notes: list[Mapping[str, Any]],
    sources: list[Mapping[str, Any]],
    attachments: list[Mapping[str, Any]],
) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    title = text_value(case.get('title'), 'Case brief')
    filename = f"{slugify(text_value(case.get('id'), 'case'))}-{slugify(title)}.html"
    path = OUTPUT_DIR / filename

    summary_rows = [
        ['Case status', text_value(case.get('status'))],
        ['Priority', text_value(case.get('priority'))],
        ['Owner', text_value(case.get('owner'))],
        ['Decision', text_value(case.get('decision'))],
        ['Agent draft review', text_value(case.get('agent_review_status'))],
        ['Target review date', format_date(case.get('due_at'))],
        ['Created', format_date(case.get('created_at'))],
        ['Updated', format_date(case.get('updated_at'))],
    ]

    agent_rows = [
        ['Draft source', text_value(case.get('agent_generation_mode'))],
        ['Requested model alias', text_value(case.get('agent_generation_model'))],
        ['Resolved backend model', text_value(case.get('agent_resolved_model'))],
        ['Prepared on', format_date(case.get('agent_prepared_at'))],
        ['Latest register refresh', format_date(case.get('register_refreshed_at'))],
        ['Draft review status', text_value(case.get('agent_review_status'))],
        ['Suggested case status', text_value(case.get('agent_recommended_status'))],
        ['Suggested priority', text_value(case.get('agent_recommended_priority'))],
        ['Suggested decision', text_value(case.get('agent_recommended_decision'))],
        ['Recommended next step', text_value(case.get('agent_next_step'))],
        ['Reviewed by', text_value(case.get('agent_reviewed_by'))],
        ['Reviewed on', format_date(case.get('agent_reviewed_at'))],
    ]

    notice_rows = [
        ['Name on notice', text_value(profile.get('source_entity_name'))],
        ['Most serious action', action_label(profile.get('most_severe_action'))],
        ['Latest action date', format_date(profile.get('most_recent_action_date'))],
        ['State', text_value(profile.get('source_state'))],
        ['Postcode', text_value(profile.get('source_postcode'))],
        ['Record type', entity_type_label(profile.get('source_entity_type'))],
    ]

    business_rows = [
        ['Matched business name', text_value(profile.get('resolved_entity_name'), 'No confirmed business record yet')],
        ['ABN', text_value(profile.get('resolved_abn'))],
        ['ACN', text_value(profile.get('resolved_acn'))],
        ['Match result', match_label(profile.get('match_confidence'))],
        ['How it was checked', match_detail(profile.get('match_confidence'))],
        ['Business type', entity_type_label(profile.get('resolved_entity_type'))],
        ['Company status', asic_status_label(profile.get('asic_status'))],
        ['Registered on', format_date(profile.get('asic_registration_date'))],
    ]

    history_rows: list[list[str]] = []
    if not history.empty:
        history_copy = history.sort_values('date_effective', ascending=False).copy()
        for _, row in history_copy.head(12).iterrows():
            history_rows.append(
                [
                    format_date(row.get('date_effective')),
                    action_label(row.get('action_type')),
                    text_value(row.get('state')),
                    text_value(row.get('description_text'), 'No summary available'),
                ]
            )

    related_rows: list[list[str]] = []
    if not related_businesses.empty:
        related_copy = related_businesses.sort_values('days_after_enforcement', ascending=True).copy()
        for _, row in related_copy.head(12).iterrows():
            related_rows.append(
                [
                    text_value(row.get('candidate_entity_name')),
                    text_value(row.get('candidate_abn')),
                    text_value(row.get('candidate_acn')),
                    asic_status_label(row.get('candidate_status')),
                    format_date(row.get('candidate_registration_date')),
                    format_days(row.get('days_after_enforcement')),
                ]
            )

    analyst_summary = text_value(case.get('summary'), '')
    if analyst_summary.startswith('Public enforcement record for ') or not analyst_summary:
        analyst_summary = build_executive_summary(case, profile, related_businesses)
    refresh_context = parse_json_text(case.get('register_refresh_context'))
    agent_summary = text_value(case.get('agent_summary'), 'No agent draft recorded.')
    agent_rationale = text_value(case.get('agent_rationale'), 'No agent rationale recorded.')
    register_refresh_summary = build_bullet_list(
        case.get('register_refresh_summary'),
        'No live public-register refresh has been captured for this case.',
    )
    agent_completed_checks = build_bullet_list(
        case.get('agent_completed_checks'),
        'No automated checks were captured.',
    )
    agent_supporting_evidence = build_bullet_list(
        case.get('agent_supporting_evidence'),
        'No supporting evidence summary was captured.',
    )
    agent_human_checks = build_bullet_list(
        case.get('agent_human_checks'),
        'No analyst-judgment steps were captured.',
    )
    agent_generation_notes = text_value(case.get('agent_generation_notes'), 'No draft-generation note recorded.')

    notes_html = ''.join(
        f"<li><strong>{escape(text_value(note.get('author'), 'Analyst'))}</strong>"
        f" - {escape(format_date(note.get('created_at')))}<br>{escape(text_value(note.get('note_text')))}</li>"
        for note in notes
    ) or '<li>No analyst notes recorded.</li>'

    sources_html = ''.join(
        (
            '<li>'
            f"<strong>{escape(text_value(source.get('source_name')))}</strong>"
            f" - {escape(text_value(source.get('source_type'), 'Reference'))}"
            + (
                f"<br><a href=\"{escape(text_value(source.get('source_url'), ''))}\">{escape(text_value(source.get('source_url'), ''))}</a>"
                if text_value(source.get('source_url'), '')
                else ''
            )
            + (
                f"<br><span>Reference: {escape(display_source_ref(source.get('source_ref')))}</span>"
                if text_value(source.get('source_ref'), '')
                else ''
            )
            + '</li>'
        )
        for source in sources
    ) or '<li>No source links recorded.</li>'

    attachments_html = ''.join(
        (
            '<li>'
            f"<strong>{escape(text_value(attachment.get('file_name')))}</strong>"
            f" - {escape(format_date(attachment.get('created_at')))}"
            + (
                f"<br><span>Uploaded by: {escape(text_value(attachment.get('uploaded_by'), 'Pilot user'))}</span>"
                if text_value(attachment.get('uploaded_by'), '')
                else ''
            )
            + (
                f"<br><span>Size: {escape(text_value(attachment.get('size_bytes')))} bytes</span>"
                if text_value(attachment.get('size_bytes'), '')
                else ''
            )
            + '</li>'
        )
        for attachment in attachments
    ) or '<li>No case attachments recorded.</li>'

    html = f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{escape(title)}</title>
  <style>
    body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 32px; color: #1f2c2a; line-height: 1.55; background: #f7f3ec; }}
    h1, h2 {{ color: #14312d; margin-bottom: 10px; }}
    p {{ margin-top: 0; }}
    .banner {{ background: #f7efe2; border-left: 5px solid #c96d3f; padding: 16px; margin: 20px 0; border-radius: 10px; }}
    .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 18px; margin-bottom: 24px; }}
    .card {{ background: #fffdfa; border: 1px solid #d7d2ca; border-radius: 12px; padding: 16px; margin-bottom: 18px; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 12px; }}
    th, td {{ border: 1px solid #d7d2ca; padding: 8px 10px; text-align: left; vertical-align: top; }}
    th {{ background: #efe4d3; }}
    ul {{ padding-left: 20px; }}
    .meta {{ color: #425652; margin-bottom: 18px; }}
    .summary {{ font-size: 1.02rem; }}
  </style>
</head>
<body>
  <h1>{escape(title)}</h1>
  <p class="meta">Case ID: {escape(text_value(case.get('id')))} | Entity key: {escape(text_value(case.get('entity_key')))}</p>

  <div class="banner">
    <strong>Pilot note:</strong> This brief is a public-data analyst support output. It is not a finding of fraud and requires human review before any operational action.
  </div>

  <h2>Executive Summary</h2>
  <p class="summary">{escape(build_executive_summary(case, profile, related_businesses))}</p>
  <p><strong>Recommended next step:</strong> {escape(build_recommended_next_step(case, profile))}</p>

  <div class="grid">
    <div class="card">
      <h2>Case Summary</h2>
      {build_table(['Field', 'Value'], summary_rows)}
    </div>
    <div class="card">
      <h2>Notice Details</h2>
      {build_table(['Field', 'Value'], notice_rows)}
    </div>
  </div>

  <div class="card">
    <h2>Business Record</h2>
    {build_table(['Field', 'Value'], business_rows)}
  </div>

    <div class="grid">
    <div class="card">
      <h2>Agent Draft</h2>
      {build_table(['Field', 'Value'], agent_rows)}
      <p><strong>Draft summary:</strong> {escape(agent_summary)}</p>
      <p><strong>Why the agent suggested this:</strong> {escape(agent_rationale)}</p>
      <p><strong>What the agent already checked:</strong></p>
      {agent_completed_checks}
      <p><strong>Latest public-register refresh:</strong></p>
      {register_refresh_summary}
      <p><strong>Evidence used:</strong></p>
      {agent_supporting_evidence}
      <p><strong>Needs analyst judgment:</strong></p>
      {agent_human_checks}
      <p><strong>Draft-generation note:</strong> {escape(agent_generation_notes)}</p>
    </div>
    <div class="card">
      <h2>Analyst Summary</h2>
      <p>{escape(analyst_summary or 'No analyst summary recorded yet.')}</p>
    </div>
  </div>

  <div class="card">
    <h2>Enforcement Timeline</h2>
    {build_table(['Date', 'Action', 'State', 'Summary'], history_rows) if history_rows else '<p>No enforcement timeline rows were found.</p>'}
  </div>

  <div class="card">
    <h2>Related Businesses To Check</h2>
    {build_table(['Business', 'ABN', 'ACN', 'Status', 'Registered on', 'Days after action'], related_rows) if related_rows else '<p>No related-business leads were identified for this case.</p>'}
    <p><strong>Mini-briefs for related businesses:</strong></p>
    {related_brief_list(related_businesses, refresh_context)}
  </div>

  <div class="grid">
    <div class="card">
      <h2>Analyst Notes</h2>
      <ul>{notes_html}</ul>
    </div>
    <div class="card">
      <h2>Evidence Sources</h2>
      <ul>{sources_html}</ul>
    </div>
  </div>

  <div class="card">
    <h2>Case Attachments</h2>
    <ul>{attachments_html}</ul>
  </div>

  <div class="banner">
    <strong>Limitations:</strong> This workbench relies on public records, heuristic entity matching, and public company dataset snapshots. Related-business leads are review prompts, not proof of phoenix activity or fraud.
  </div>
</body>
</html>
"""
    path.write_text(html, encoding='utf-8')
    return path

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Mapping

import requests
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[1]
WORKSPACE_DIR = BASE_DIR.parents[1]

ALLOWED_PRIORITIES = {'Low', 'Medium', 'High'}
ALLOWED_STATUSES = {'New', 'In Review', 'Escalate', 'Monitor', 'Closed'}
ALLOWED_DECISIONS = {'No action', 'Monitor', 'Escalate for review', 'Needs more evidence'}
PREFERRED_MODEL_ALIASES = [
    'auto',
    'gemini-flash-lite',
    'gemini-flash',
    'gemini-pro',
    'claude-sonnet',
    'qwen-flash',
]

DEFAULT_LITELLM_ENDPOINTS = [
    'http://192.168.68.58:4001/v1',
    'http://100.112.127.112:4001/v1',
    'http://127.0.0.1:4001/v1',
]


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
    text = str(value).strip()
    return '' if text.lower() == 'nan' else text


def load_agent_settings() -> dict[str, Any]:
    load_dotenv(BASE_DIR / '.env', override=False)
    enabled = text_value(os.getenv('NDIS_AGENT_ENABLED', 'true')).lower() not in {'0', 'false', 'no'}
    api_key = text_value(
        os.getenv('NDIS_AGENT_API_KEY')
        or os.getenv('OPENAI_API_KEY')
        or os.getenv('LITELLM_API_KEY')
        or os.getenv('LITELLM_MASTER_KEY')
    )
    base_url = text_value(
        os.getenv('NDIS_AGENT_API_BASE')
        or os.getenv('OPENAI_API_BASE')
        or os.getenv('LITELLM_ENDPOINT')
        or os.getenv('LITELLM_BASE_URL')
        or 'https://api.openai.com/v1'
    )
    model = text_value(
        os.getenv('NDIS_AGENT_MODEL')
        or os.getenv('OPENAI_MODEL')
        or os.getenv('LITELLM_MODEL')
    )
    return {
        'enabled': enabled,
        'api_key': api_key,
        'model': model,
        'base_url': base_url.rstrip('/'),
        'timeout_seconds': int(text_value(os.getenv('NDIS_AGENT_TIMEOUT_SECONDS') or '30') or '30'),
    }


def discover_model_alias(base_url: str, api_key: str, timeout_seconds: int) -> tuple[str, str]:
    response = requests.get(
        base_url.rstrip('/') + '/models',
        headers={'Authorization': f'Bearer {api_key}'},
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    payload = response.json()
    candidates: list[str] = []

    if isinstance(payload, dict):
        items = payload.get('data') or payload.get('models') or []
    else:
        items = payload

    for item in items or []:
        if isinstance(item, dict):
            model_id = text_value(item.get('id') or item.get('model_name') or item.get('name'))
        else:
            model_id = text_value(item)
        if model_id:
            candidates.append(model_id)

    lowered = {candidate.lower(): candidate for candidate in candidates}
    for preferred in PREFERRED_MODEL_ALIASES:
        if preferred.lower() in lowered:
            return lowered[preferred.lower()], f'Auto-discovered LiteLLM model alias `{lowered[preferred.lower()]}`.'

    if candidates:
        return candidates[0], f'Auto-discovered LiteLLM model alias `{candidates[0]}`.'
    raise ValueError('No models were returned by the LiteLLM /models endpoint.')


def redact_secret(value: str) -> str:
    text = text_value(value)
    if len(text) <= 8:
        return 'set' if text else 'not set'
    return f'{text[:6]}...{text[-4:]}'


def load_workspace_litellm_endpoints() -> list[str]:
    candidates: list[str] = []
    env_files = [
        WORKSPACE_DIR / '.env',
        WORKSPACE_DIR / '-C-DESKTOP.env',
        BASE_DIR / '.env',
    ]
    for env_path in env_files:
        if not env_path.exists():
            continue
        try:
            for line in env_path.read_text(encoding='utf-8', errors='ignore').splitlines():
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                key, value = line.split('=', 1)
                if key.strip() in {'LITELLM_ENDPOINT', 'LITELLM_BASE_URL', 'OPENAI_API_BASE'}:
                    endpoint = text_value(value.strip().strip('"').strip("'"))
                    if endpoint:
                        candidates.append(endpoint)
        except Exception:
            continue
    for endpoint in DEFAULT_LITELLM_ENDPOINTS:
        candidates.append(endpoint)
    deduped: list[str] = []
    seen: set[str] = set()
    for endpoint in candidates:
        normalized = endpoint.rstrip('/')
        if normalized and normalized not in seen:
            seen.add(normalized)
            deduped.append(normalized)
    return deduped


def probe_endpoint(base_url: str, api_key: str, timeout_seconds: int) -> dict[str, str]:
    result = {
        'Endpoint': base_url,
        'Reachable': 'No',
        'Status': 'Not checked',
        'Model count': '0',
        'Sample model': '',
        'Detail': '',
    }
    if not api_key:
        result['Status'] = 'No API key configured'
        result['Detail'] = 'Set LiteLLM or OpenAI-compatible credentials first.'
        return result

    headers = {'Authorization': f'Bearer {api_key}'}
    try:
        response = requests.get(base_url.rstrip('/') + '/models', headers=headers, timeout=timeout_seconds)
        result['Status'] = str(response.status_code)
        response.raise_for_status()
        payload = response.json()
        items = payload.get('data') if isinstance(payload, dict) else payload
        models: list[str] = []
        for item in items or []:
            if isinstance(item, dict):
                model_id = text_value(item.get('id') or item.get('model_name') or item.get('name'))
            else:
                model_id = text_value(item)
            if model_id:
                models.append(model_id)
        result['Reachable'] = 'Yes'
        result['Model count'] = str(len(models))
        result['Sample model'] = models[0] if models else ''
        result['Detail'] = 'LiteLLM /models responded successfully.'
        return result
    except requests.exceptions.ConnectTimeout:
        result['Status'] = 'Timeout'
        result['Detail'] = 'Connection timed out while probing /models.'
        return result
    except requests.exceptions.ReadTimeout:
        result['Status'] = 'Timeout'
        result['Detail'] = 'Endpoint accepted the connection but did not answer in time.'
        return result
    except requests.exceptions.ConnectionError:
        result['Status'] = 'Connection error'
        result['Detail'] = 'No route to the endpoint or nothing is listening on that port.'
        return result
    except requests.HTTPError as exc:
        result['Status'] = str(getattr(exc.response, 'status_code', 'HTTP error'))
        result['Detail'] = 'The endpoint responded but rejected the request.'
        return result
    except Exception as exc:
        result['Status'] = type(exc).__name__
        result['Detail'] = str(exc)[:180]
        return result


def get_agent_diagnostics() -> dict[str, Any]:
    settings = load_agent_settings()
    endpoints = [settings['base_url']] if settings['base_url'] else []
    for endpoint in load_workspace_litellm_endpoints():
        if endpoint not in endpoints:
            endpoints.append(endpoint)
    probes = [probe_endpoint(endpoint, settings['api_key'], min(settings['timeout_seconds'], 8)) for endpoint in endpoints]
    reachable = [probe for probe in probes if probe['Reachable'] == 'Yes']
    return {
        'enabled': settings['enabled'],
        'base_url': settings['base_url'],
        'configured_model': settings['model'],
        'api_key_status': redact_secret(settings['api_key']),
        'probes': probes,
        'reachable_count': len(reachable),
    }


def build_related_preview(related_records: list[Mapping[str, Any]] | None) -> list[dict[str, str]]:
    preview: list[dict[str, str]] = []
    for row in list(related_records or [])[:3]:
        preview.append(
            {
                'business_name': text_value(row.get('candidate_current_name'))
                or text_value(row.get('candidate_company_name'))
                or text_value(row.get('candidate_entity_name')),
                'candidate_abn': text_value(row.get('candidate_abn')),
                'candidate_acn': text_value(row.get('candidate_acn')),
                'candidate_status': text_value(row.get('candidate_status')),
                'candidate_registration_date': text_value(row.get('candidate_registration_date')),
                'days_after_enforcement': text_value(row.get('days_after_enforcement')),
                'same_state': text_value(row.get('same_state')),
            }
        )
    return preview


def build_case_context(
    entity_row: Mapping[str, Any],
    related_count: int,
    related_records: list[Mapping[str, Any]] | None = None,
    register_refresh_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    refresh_context = dict(register_refresh_context or {})
    return {
        'source_entity_name': text_value(entity_row.get('source_entity_name')),
        'source_entity_type': text_value(entity_row.get('source_entity_type')),
        'source_state': text_value(entity_row.get('source_state')),
        'source_postcode': text_value(entity_row.get('source_postcode')),
        'source_abn': text_value(entity_row.get('source_abn')),
        'most_severe_action': text_value(entity_row.get('most_severe_action')),
        'first_action_date': text_value(entity_row.get('first_action_date')),
        'most_recent_action_date': text_value(entity_row.get('most_recent_action_date')),
        'action_count': text_value(entity_row.get('action_count')),
        'resolved_entity_name': text_value(entity_row.get('resolved_entity_name')),
        'resolved_entity_type': text_value(entity_row.get('resolved_entity_type')),
        'resolved_abn': text_value(entity_row.get('resolved_abn')),
        'resolved_acn': text_value(entity_row.get('resolved_acn')),
        'match_confidence': text_value(entity_row.get('match_confidence')),
        'review_reason': text_value(entity_row.get('review_reason')),
        'asic_status': text_value(entity_row.get('asic_status')),
        'asic_registration_date': text_value(entity_row.get('asic_registration_date')),
        'related_business_count': related_count,
        'related_business_preview': build_related_preview(related_records),
        'register_refresh_context': refresh_context,
    }


def build_prompts(case_context: Mapping[str, Any], fallback_draft: Mapping[str, Any]) -> tuple[str, str]:
    system_prompt = (
        'You prepare public-record case drafts for analyst review teams. '
        'You are not deciding guilt, fraud, or enforcement. '
        'Return only a JSON object with these string fields: '
        'agent_summary, agent_recommended_priority, agent_recommended_status, '
        'agent_recommended_decision, agent_rationale, agent_completed_checks, agent_supporting_evidence, '
        'agent_human_checks, agent_next_step. '
        'Keep wording plain, operational, and non-defamatory. '
        'Do not claim fraud. '
        'Do not invent facts beyond the provided public-record context. '
        'If live public-register refresh results are included, treat them as stronger evidence than the baseline snapshot. '
        'Use agent_completed_checks for steps the agent can already clear from the provided data. '
        'Use agent_human_checks only for judgement calls or approval steps that still require a person. '
        'For agent_completed_checks, agent_supporting_evidence, and agent_human_checks, return short bullet-ready lines separated by newline characters.'
    )
    user_prompt = (
        'Prepare a case draft from this public-record context.\n\n'
        f'Case context JSON:\n{json.dumps(case_context, indent=2)}\n\n'
        'Here is the current deterministic baseline draft. Improve it if you can, but stay grounded.\n\n'
        f'Baseline draft JSON:\n{json.dumps(fallback_draft, indent=2)}\n\n'
        'Return JSON only.'
    )
    return system_prompt, user_prompt


def extract_json_object(raw_text: str) -> dict[str, Any]:
    raw = raw_text.strip()
    if not raw:
        raise ValueError('empty LLM response')
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        start = raw.find('{')
        end = raw.rfind('}')
        if start == -1 or end == -1 or end <= start:
            raise
        return json.loads(raw[start : end + 1])


def sanitize_agent_draft(
    raw_draft: Mapping[str, Any] | None,
    fallback_draft: Mapping[str, Any],
    *,
    generation_mode: str,
    generation_model: str,
    resolved_model: str,
    generation_notes: str,
) -> dict[str, str]:
    raw = dict(raw_draft or {})
    merged = {
        'agent_summary': text_value(raw.get('agent_summary')) or text_value(fallback_draft.get('agent_summary')),
        'agent_recommended_priority': text_value(raw.get('agent_recommended_priority'))
        or text_value(fallback_draft.get('agent_recommended_priority')),
        'agent_recommended_status': text_value(raw.get('agent_recommended_status'))
        or text_value(fallback_draft.get('agent_recommended_status')),
        'agent_recommended_decision': text_value(raw.get('agent_recommended_decision'))
        or text_value(fallback_draft.get('agent_recommended_decision')),
        'agent_rationale': text_value(raw.get('agent_rationale')) or text_value(fallback_draft.get('agent_rationale')),
        'agent_completed_checks': text_value(raw.get('agent_completed_checks'))
        or text_value(fallback_draft.get('agent_completed_checks')),
        'agent_supporting_evidence': text_value(raw.get('agent_supporting_evidence'))
        or text_value(fallback_draft.get('agent_supporting_evidence')),
        'agent_human_checks': text_value(raw.get('agent_human_checks'))
        or text_value(fallback_draft.get('agent_human_checks')),
        'agent_next_step': text_value(raw.get('agent_next_step')) or text_value(fallback_draft.get('agent_next_step')),
        'agent_generation_mode': generation_mode,
        'agent_generation_model': generation_model,
        'agent_resolved_model': text_value(resolved_model) or text_value(fallback_draft.get('agent_resolved_model')),
        'agent_generation_notes': generation_notes,
    }

    if merged['agent_recommended_priority'] not in ALLOWED_PRIORITIES:
        merged['agent_recommended_priority'] = text_value(fallback_draft.get('agent_recommended_priority')) or 'Medium'
    if merged['agent_recommended_status'] not in ALLOWED_STATUSES:
        merged['agent_recommended_status'] = text_value(fallback_draft.get('agent_recommended_status')) or 'In Review'
    if merged['agent_recommended_decision'] not in ALLOWED_DECISIONS:
        merged['agent_recommended_decision'] = text_value(fallback_draft.get('agent_recommended_decision')) or 'Needs more evidence'

    return merged


def generate_case_prep_draft(
    entity_row: Mapping[str, Any],
    *,
    related_count: int,
    related_records: list[Mapping[str, Any]] | None = None,
    register_refresh_context: Mapping[str, Any] | None = None,
    fallback_draft: Mapping[str, Any],
) -> dict[str, str]:
    settings = load_agent_settings()
    discovery_note = ''
    fallback_mode = sanitize_agent_draft(
        fallback_draft,
        fallback_draft,
        generation_mode='rules',
        generation_model='deterministic-v1',
        resolved_model='deterministic-v1',
        generation_notes='LLM case-prep agent is not configured. Using deterministic draft rules.',
    )

    if not settings['enabled']:
        fallback_mode['agent_generation_notes'] = 'LLM case-prep agent is disabled. Using deterministic draft rules.'
        return fallback_mode
    if not settings['api_key']:
        fallback_mode['agent_generation_notes'] = (
            'LLM case-prep agent is not fully configured. Set NDIS_AGENT_API_KEY/OPENAI_API_KEY or LiteLLM credentials.'
        )
        return fallback_mode

    if not settings['model']:
        try:
            settings['model'], discovery_note = discover_model_alias(
                settings['base_url'],
                settings['api_key'],
                settings['timeout_seconds'],
            )
        except Exception as exc:
            fallback_mode['agent_generation_notes'] = (
                'LLM case-prep agent could not auto-discover a model alias '
                f'from the configured endpoint ({type(exc).__name__}). Using deterministic draft rules.'
            )
            return fallback_mode

    try:
        from openai import OpenAI
    except ImportError:
        fallback_mode['agent_generation_notes'] = 'The openai package is not installed, so the deterministic draft rules are being used.'
        return fallback_mode

    case_context = build_case_context(entity_row, related_count, related_records, register_refresh_context)
    system_prompt, user_prompt = build_prompts(case_context, fallback_draft)

    try:
        client = OpenAI(api_key=settings['api_key'], base_url=settings['base_url'])
        raw_response = client.responses.with_raw_response.create(
            model=settings['model'],
            input=[
                {
                    'role': 'system',
                    'content': [{'type': 'input_text', 'text': system_prompt}],
                },
                {
                    'role': 'user',
                    'content': [{'type': 'input_text', 'text': user_prompt}],
                },
            ],
            timeout=settings['timeout_seconds'],
        )
        response = raw_response.parse()
        resolved_model = (
            text_value(raw_response.headers.get('x-litellm-model-group'))
            or text_value(raw_response.headers.get('x-litellm-model-id'))
            or settings['model']
        )
        parsed = extract_json_object(text_value(getattr(response, 'output_text', '')))
        return sanitize_agent_draft(
            parsed,
            fallback_draft,
            generation_mode='llm',
            generation_model=settings['model'],
            resolved_model=resolved_model,
            generation_notes=' '.join(
                part
                for part in [
                    'Draft generated by the configured LLM case-prep agent.',
                    discovery_note,
                ]
                if part
            ),
        )
    except Exception as exc:
        fallback_mode['agent_generation_notes'] = (
            f'LLM case-prep agent failed ({type(exc).__name__}). Using deterministic draft rules.'
        )
        return fallback_mode

# NDIS Provider Integrity Workbench

Public-record review workbench for structuring NDIS enforcement actions, enriching them with business register data, and supporting analyst-led case review.

This implementation follows the revised brief in `NDIS/02_revised_antigravity_brief.md` with one hard boundary:

- Public-records only
- Explicit exclusion: no paid ASIC director extracts, no paid API brokers, no commercial data providers

## Scope Implemented Here

This repo now covers Sprint 1 foundations plus a conservative Sprint 2 public-register resolution pass:

1. Project structure
2. Source register and data dictionary templates
3. dependency list
4. Environment template
5. Initial Python scripts for:
   - NDIS Commission enforcement ingestion
   - ABN Lookup client with free HTML fallback
   - ASIC company dataset metadata download
   - entity-level aggregation and ABN/ASIC resolution
   - conservative phoenix-candidate heuristics using only public data
   - a local Streamlit review workbench over generated outputs
   - case creation, notes, activity history, source capture, and HTML brief export

Out of scope in this scaffold:

- Paid director-data enrichment
- Final severity scoring
- Phoenix confirmation using director overlap
- Buyer memo generation
- A production web application or hosted multi-user dashboard

## Public-Data Strategy

The current implementation only uses public sources:

- NDIS Commission enforcement CSV export and search page
- ABN Lookup public website and optional GUID-backed web services
- ASIC company dataset from data.gov.au
- NDIS quarterly reports for future denominator context

Phoenix detection is therefore limited to free-signal heuristics only:

- same or similar entity names
- same state
- post-enforcement registration timing
- ABN and ASIC status changes

Director-overlap analysis is intentionally excluded from this scaffold.

## Recommended Execution Order

### 1. Create a virtual environment and install dependencies

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Copy environment values

```bash
copy .env.example .env
```

Notes:

- `ABN_LOOKUP_GUID` is optional for the current scaffold.
- If no GUID is provided, the ABN client falls back to the public ABN Lookup HTML search flow.

### 3. Pull the enforcement register

```bash
python src/01_scrape_enforcement.py
```

Current behavior:

- prefers the Commission CSV export endpoint
- saves raw CSV and HTML artifacts under `raw/enforcement/`
- writes normalized outputs to `normalized/enforcement.csv` and `normalized/enforcement.parquet`

### 4. Test ABN search/enrichment

Name search:

```bash
python src/02_register_abn_client.py --name "Acclaimed Community Support Services Pty Ltd"
```

ABN details:

```bash
python src/02_register_abn_client.py --abn 79663019583
```

Current behavior:

- uses free ABR JSON services if `ABN_LOOKUP_GUID` is set
- otherwise falls back to the public ABN Lookup HTML site

### 5. Pull ASIC metadata and optionally the current dataset

Metadata only:

```bash
python src/03_download_asic.py
```

Download current CSV:

```bash
python src/03_download_asic.py --download
```

Download current CSV and create a parquet sample:

```bash
python src/03_download_asic.py --download --to-parquet --sample-rows 50000
```

### 6. Resolve entities with ABN and ASIC public-register signals

```bash
python src/05_entity_resolution.py
```

Current behavior:

- aggregates row-level enforcement actions into entity profiles
- treats source ABNs as exact identifiers
- performs bounded ABN name lookups for missing-ABN entities
- applies a second-pass review-resolve step for:
  - exact legal-name corporate matches blocked only by state mismatch
  - alias-heavy individual records where a public ABN match lands on a specific alias
- keeps a manual-review queue for uncertain or skipped name matches
- enriches resolved ABNs and ACNs against the local ASIC current-company CSV

Key outputs:

- `normalized/entities_enriched.csv`
- `output/entity_profiles.csv`
- `output/match_review_queue.csv`
- `normalized/asic_company_matches.csv`

### 7. Generate conservative phoenix-candidate heuristics

```bash
python src/08_phoenix_detection.py
```

Current behavior:

- scans the local ASIC current-company CSV
- looks only at severe enforcement entities (`banning_order`, `revocation`)
- flags exact canonical-name-root matches registered after the enforcement date
- does not use paid director data and does not claim confirmation of phoenix activity

Key output:

- `output/phoenix_candidates.csv`

### 8. Launch the local review workbench

```bash
streamlit run dashboard.py
```

Current behavior:

- loads `output/entity_profiles.csv`, `output/match_review_queue.csv`, `output/phoenix_candidates.csv`, and `normalized/entities_enriched.csv`
- provides filtered overview metrics, review buckets, related-business inspection, and a case desk
- supports entity drill-down into action history and registry detail
- lets an analyst create a case from a lookup result
- provides local access profiles for `Analyst`, `Manager`, and `Admin` views
- stores local notes, owner, priority, due date, status, decision, and activity history in `data/app.db`
- prepares an agent draft for each case, including a suggested next step, summary, and rationale
- uses the configured LLM case-prep agent when `NDIS_AGENT_API_KEY` and `NDIS_AGENT_MODEL` are set
- falls back to deterministic draft rules when the LLM agent is not configured or fails
- captures human sign-off as `Accepted`, `Edited`, or `Rejected`
- separates agent and human actions in the case activity history
- captures source links for each case
- supports case attachments in `data/attachments/`
- exports HTML case briefs into `output/case_briefs/`
- renders methodology, scope, security, support, and test-plan notes inside the UI

This workbench is local-first. It does not rerun the pipeline itself.

### 9. Run the case workflow

1. Open `Look Up Record`
2. Search by provider name, person name, ABN, or ACN
3. Select the record and click `Create Case In Desk`
4. Open `Case Desk`
5. Review the agent-prepared draft and either accept it, edit it, or reject it
6. Add notes, set owner, priority, review date, and status
7. Upload working papers or screenshots if needed
8. Save updates and export an HTML brief

### 10. Pilot access profiles

The local MVP uses seeded access profiles instead of enterprise identity:

- `Demo Analyst (Analyst)` focuses the desk on that analyst's assigned cases
- `Demo Manager (Manager)` exposes team workload and queue status views
- `Pilot Admin (Admin)` uses the manager-style view without adding extra workflow logic

Hosted deployments should replace this with the organization identity provider.

### 11. LLM case-prep agent

The draft-preparation layer can use an LLM through an OpenAI-compatible `Responses` endpoint.

Set these values in `.env`:

- `NDIS_AGENT_ENABLED=true`
- `NDIS_AGENT_API_KEY=...`
- `NDIS_AGENT_MODEL=...`
- `NDIS_AGENT_API_BASE=https://api.openai.com/v1`

It also accepts LiteLLM-style variables:

- `LITELLM_API_KEY=...`
- `LITELLM_ENDPOINT=http://.../v1`
- `LITELLM_MODEL=...`

If no model is set, the agent will try to auto-discover a usable alias from the LiteLLM `/models` endpoint.

If the agent is not configured, the workbench still runs and uses deterministic draft rules instead.

### 12. Included project docs

The `About This Tool` screen renders the following notes directly from `docs/`:

- `pilot_scope.md`
- `architecture.md`
- `methodology.md`
- `pilot_success_metrics.md`
- `support_model.md`
- `security_overview.md`
- `security_limitations.md`
- `test_plan.md`
- live agent diagnostics for LiteLLM / OpenAI-compatible connectivity

## Current Project Layout

```text
.
  raw/
    enforcement/
    abn/
    asic/
    quarterly_reports/
  normalized/
  features/
  output/
  .streamlit/
    config.toml
  data/
    app.db
    attachments/
  docs/
    architecture.md
    methodology.md
    pilot_scope.md
    pilot_success_metrics.md
    security_limitations.md
    security_overview.md
    support_model.md
    test_plan.md
  dashboard.py
  src/
    01_scrape_enforcement.py
    02_register_abn_client.py
    03_download_asic.py
    04_normalize.py
    05_entity_resolution.py
    06_feature_engineering.py
    07_score.py
    08_phoenix_detection.py
    09_report.py
    __init__.py
    case_store.py
    export_case_brief.py
  .env.example
  data_dictionary.md
  requirements.txt
  source_register.csv
  README.md
```

## Design Rules

- Python-first
- Local-first outputs
- Idempotent stages where practical
- Preserve raw source artifacts
- Prefer free official endpoints over scraping when available
- Fall back to public HTML only when the free official endpoint is unavailable or requires manual setup
- Separate facts from inference
- Do not claim fraud detection

## Current Limits

- Missing-ABN lookups are intentionally bounded per run to stay polite to the public ABN service.
- The default `--missing-abn-limit` is conservative and will leave part of the queue unresolved until later runs.
- ASIC enrichment relies on the current-company CSV only.
- Phoenix candidates are heuristics for review, not proof.
- Second-pass alias and state-mismatch matches remain reviewable by design even when they are resolved into ABN-linked profiles.
- Director-overlap analysis remains intentionally excluded from this public-record workbench.
- Access profiles are local demo roles only. They are not a substitute for SSO, RBAC, or hosted audit controls.

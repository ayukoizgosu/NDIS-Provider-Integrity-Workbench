# Validation Metrics

**Run date:** 2026-03-30
**Scope:** Sprint 1 plus Sprint 2 free-only pipeline verification
**Spend cap:** `$0`

## Completed checks

### 1. NDIS Commission enforcement export

Status: passed

- Source used: live CSV export endpoint
- Raw CSV saved under `raw/enforcement/`
- Normalized CSV written to `normalized/enforcement.csv`
- Row count observed: `2315`

Action-type summary from the current run:

- `compliance_notice`: `1230`
- `banning_order`: `679`
- `revocation`: `310`
- `other`: `91`
- `enforceable_undertaking`: `5`

Initial interpretation:

- The free public register is rich enough to justify continuing.
- The row count comfortably clears the brief's `>=200` Sprint 1 kill gate.

### 2. ABN lookup

Status: partially passed

- Free HTML fallback tested successfully
- Example entity searched: `Acclaimed Community Support Services Pty Ltd`
- Matching ABN returned: `79663019583`
- Detail page parse returned:
  - entity name
  - ABN status
  - ACN
  - entity type
  - GST status
  - state and postcode

Current limitation:

- No free GUID has been configured yet, so the official ABR web-services path is not exercised on this machine.
- This does not block the scaffold because the HTML fallback works within the `$0` path.

### 3. ASIC package metadata

Status: passed

- CKAN package metadata fetched successfully from data.gov.au
- Current free CSV resource identified successfully
- Resource inventory written to `raw/asic/asic_resources.csv`

Current limitation:

- The full ASIC CSV was not downloaded during verification.
- The downloader script is ready for it when needed.

### 4. Parquet output

Status: optional dependency missing

- CSV output works
- Parquet output is skipped cleanly because `pyarrow` is not installed in the current Python environment

Impact:

- No blocker for Sprint 1 data access or schema validation
- Install `requirements.txt` to enable parquet writes later

## $0 Constraints Enforced

- No paid director data used
- No commercial API broker used
- No paid ASIC extract referenced in code execution
- Phoenix detection remains limited to free-signal heuristics only

## 5. Entity resolution

Status: passed with bounded lookup limits

- Entity profiles written to `output/entity_profiles.csv`
- Enriched enforcement rows written to `normalized/entities_enriched.csv`
- Review queue written to `output/match_review_queue.csv`
- Exact ASIC matches written to `normalized/asic_company_matches.csv`

Observed outputs from the deeper current run after the second-pass matcher:

- Entity profiles built: `2177`
- `source_abn_exact`: `1670`
- `searched_name_exact`: `145`
- `searched_alias_exact_review`: `5`
- `searched_name_exact_state_mismatch_review`: `2`
- `searched_name_probable`: `1`
- `searched_name_rejected`: `342`
- `unresolved_missing_abn`: `12`
- `missing_abn_skipped_due_limit`: `0`
- Profiles with resolved ABN after this run: `1823`
- Profiles with resolved ACN after this run: `164`
- Profiles with exact ASIC enrichment: `1158`
- Review queue rows after exhausting the missing-ABN cap: `362`

Interpretation:

- The free public register is strong enough to resolve most corporate entities that already expose ABNs.
- Exhausting the missing-ABN queue improves coverage, but most remaining unresolved items are individual names or alias-heavy records where conservative auto-linking would be too risky.
- The second-pass matcher recovered `7` additional ABN-linked profiles without leaving the `$0` path:
  - `5` alias-based individual matches
  - `2` exact corporate-name matches blocked only by state mismatch
- The current implementation stays within the `$0` constraint by using public lookups and retaining a manual-review boundary instead of buying director data.

### 6. Phoenix-candidate heuristics

Status: passed as heuristic-only screen

- Candidate file written to `output/phoenix_candidates.csv`
- Severe-action filter used: `banning_order`, `revocation`
- ASIC file used: `raw/asic/company_202603.csv`
- Candidate rows produced: `8`

Current interpretation:

- The public-data path can surface a small review queue of plausible post-enforcement re-registrations.
- These rows are only candidates for manual investigation.
- Director overlap remains unknown by design because paid director data is excluded.

## Immediate next checks

1. Register the free ABN Lookup GUID to reduce scraping dependence and improve repeatability of `src/05_entity_resolution.py`
2. Review `output/review_triage.md` alongside `output/match_review_queue.csv` before accepting any name-based non-ABN matches
3. Review `output/phoenix_candidates.csv` manually and treat it as an investigation queue, not a finding set
4. Install `pyarrow` from `requirements.txt` if parquet outputs are required locally

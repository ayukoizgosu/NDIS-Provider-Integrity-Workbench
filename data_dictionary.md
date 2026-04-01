# Data Dictionary

This file tracks the current normalized tables, important raw artifacts, and the meaning of each field used in the public-data MVP.

## Status

- Scope: Sprint 1 foundations only
- Spend rule: free public data only
- Paid director data: excluded

## Raw Artifacts

### `raw/enforcement/`

Expected contents:

- raw CSV export from the NDIS Commission
- fallback HTML snapshots of the search page

Purpose:

- audit trail
- reproducibility
- source-of-truth preservation before normalization

### `raw/abn/`

Expected contents:

- ABN Lookup HTML snapshots or JSON payloads used during enrichment tests

Purpose:

- preserve source evidence for entity matching

### `raw/asic/`

Expected contents:

- CKAN package metadata from data.gov.au
- current ASIC CSV or ZIP resource when downloaded

Purpose:

- record exactly which ASIC resource version was used

### `raw/quarterly_reports/`

Expected contents:

- quarterly report PDFs or CSV tables when that source is added

Purpose:

- denominator and market context only

## Normalized Tables

## `normalized/enforcement.parquet`

One row per enforcement record from the NDIS Commission export.

| Column | Type | Description |
| --- | --- | --- |
| `enforcement_id` | string | Stable hash generated from core record fields. |
| `source_url` | string | URL used to obtain the record. |
| `source_file` | string | Local raw file path used to create the normalized row. |
| `action_type_raw` | string | Original action type text from the Commission export. |
| `action_type` | string | Normalized action type bucket. |
| `action_subtype` | string | Heuristic subtype such as permanent, temporary, or conditional when detectable. |
| `date_effective` | datetime | Date the action took effect. |
| `date_no_longer_in_force` | datetime | End date if supplied by the source. |
| `entity_name` | string | Entity name from the enforcement record. |
| `entity_type` | string | Heuristic entity type such as provider or individual. |
| `abn` | string | ABN from the source where present. |
| `city` | string | City from the source. |
| `state` | string | State from the source. |
| `postcode` | string | Postcode from the source. |
| `provider_number` | string | Provider number where present. |
| `other_relevant_info` | string | Other source-side notes from the export. |
| `registration_groups` | string | Registration groups text from the export. |
| `description_text` | string | Main free-text description of the enforcement action. |
| `support_categories_mentioned` | string | Semicolon-separated support-category or registration-group hints. |
| `ingested_at_utc` | datetime | UTC timestamp for local ingestion. |
| `zero_spend_scope` | boolean | Always true in this scaffold. |

## Planned Tables

### `normalized/entities_enriched.parquet`

Status: not implemented yet

Planned use:

- join enforcement entities to ABN and ASIC data
- capture match confidence and evidence

### `normalized/asic_companies.parquet`

Status: optional sample generation only in current scaffold

Planned use:

- searchable ASIC company index for free-signal phoenix heuristics

### `features/entity_profiles.parquet`

Status: not implemented yet

Planned use:

- one row per entity with derived features and joined actions

### `output/*.csv`

Status: not implemented yet

Planned future outputs:

- severity index
- phoenix candidates
- entity profiles

## Field Rules

- Keep factual source fields separate from inferred fields.
- Do not overwrite source text with model assumptions.
- Preserve ABNs as digit strings, not numeric types.
- Treat paid-data-only fields, especially director overlap, as unavailable in the public-data scope.

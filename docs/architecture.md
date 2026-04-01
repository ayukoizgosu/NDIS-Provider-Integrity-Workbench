# Architecture

## Purpose

The NDIS Provider Integrity Workbench is a single-tenant pilot for analyst-assisted review of public NDIS enforcement records.

It is designed to help a reviewer:

- search a provider or person
- inspect enforcement history
- review public business-register matches
- track related-business leads
- create and manage a local case
- export a concise case brief for discussion

It is not a fraud detection engine and does not make automated adverse decisions.

## System Shape

The current implementation has four layers:

1. Public data ingestion and normalization
2. Entity resolution and related-business heuristics
3. Local case workflow storage
4. Streamlit review interface

## Data Sources

The pilot uses public records only:

- NDIS Commission enforcement export
- ABN Lookup public record pages and optional public web services
- ASIC public company dataset

These sources are used to support review, not to prove wrongdoing.

## Runtime Components

### `src/01_scrape_enforcement.py`

Pulls the public NDIS enforcement export and writes normalized enforcement data.

### `src/05_entity_resolution.py`

Aggregates enforcement records into entity profiles and links them to public ABN and ASIC records where possible.

### `src/08_phoenix_detection.py`

Produces conservative related-business leads for analyst review.

### `src/case_store.py`

Stores local workflow state in SQLite:

- cases
- notes
- events
- evidence sources

### `dashboard.py`

Provides the analyst-facing interface:

- overview
- record lookup
- review queue
- case desk
- related-business review
- pilot documentation

### `src/export_case_brief.py`

Exports a readable HTML brief for a single case.

## Data Flow

1. Public enforcement data is ingested and normalized.
2. Entity profiles are created from enforcement records.
3. Public business records are used to enrich matches.
4. Related-business leads are generated from public datasets.
5. An analyst opens a record and creates a local case.
6. The analyst adds notes, updates status, and exports a brief.

## Storage

The pilot stores workflow state locally in `data/app.db`.

The database is used only for:

- case records
- analyst notes
- event history
- source links

Generated briefs are written to `output/case_briefs/`.

## Design Boundaries

The architecture intentionally avoids:

- claims or payment-system integrations
- fake SSO or enterprise identity claims
- automated fraud findings
- hidden scoring outputs
- multi-tenant infrastructure

## Pilot Fit

This architecture is suitable for a pilot or proof-of-value environment where a small team wants to review public records, manage cases, and produce structured briefs without touching internal NDIA systems.

# Security Overview

## Positioning

This is a local-first, single-tenant analyst workbench.

It is appropriate for demo and proof-of-value use with public records only.

It is not a production-grade system and should not be presented as one.

## Current State

The current build uses:

- Streamlit for the interface
- SQLite for local case workflow storage
- local files for generated outputs

There is no production-grade authentication, hosting, or security certification in this repository.

## Data Handling

The tool stores:

- case titles and status
- analyst notes
- event history
- source links
- exported HTML briefs

The tool does not store:

- claims data
- payment data
- internal NDIA records
- bank records

## Access Control

Current access control is limited to local machine access.

There is no implemented:

- SSO integration
- role-based access control
- user provisioning
- session hardening suitable for production use

## Auditability

The case database keeps a basic event log for:

- case creation
- field updates
- note creation
- source updates

This is useful for review work, but it is not a full audit stack.

## Sensitive Data

This environment should not be used with sensitive operational data until the following are added:

- approved hosting
- access controls
- secrets management
- backup and recovery
- retention policy
- security review
- logging and monitoring

## Known Limitations

- Public registry snapshots may change after a given run.
- Public-source matching can be wrong and must remain reviewable.
- Related-business signals are leads, not findings.
- The exported brief is an analyst support artifact, not evidence of fraud.

## Production Gap

To move beyond scoped use, the product would need:

- secure hosted deployment
- authentication and authorization
- encrypted secret management
- operational logging and monitoring
- backup and restore
- accessibility verification
- a documented support process
- formal security review

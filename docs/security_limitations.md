# Security And Limitations

## Current state

This MVP runs locally and stores workflow state in a local SQLite database.

## Important limits

- No production authentication or role-based access control.
- No encrypted secrets management beyond local environment handling.
- No formal audit or logging pipeline outside the local case event log.
- No formal hosting accreditation.
- No guarantee that public registry snapshots are current at the exact time of review.

## Safe positioning

This product should be positioned as a scoped workbench for analyst-assisted review.

## Before production use

A production deployment would need:

- secure hosting
- access control
- backup and recovery
- support and maintenance processes
- accessibility validation
- security review
- data retention and audit policy

# Support Model

## Purpose

This environment needs a simple support model that matches a small review deployment.

## Current Support Shape

The current repository supports a local-first workflow:

- one deployment environment
- one analyst-facing interface
- public-record data only
- human review only

## Operating Assumptions

- One review team uses the workbench.
- One named owner is responsible for workflow decisions.
- One technical operator is responsible for the local or hosted environment.
- Issues are handled during business hours unless otherwise agreed.

## Support Responsibilities

### Product Owner

- confirms scope
- approves changes
- signs off on the demo narrative

### Analyst Lead

- validates the workflow
- checks the usefulness of labels and summaries
- confirms that the review steps make sense

### Technical Operator

- runs the app
- checks logs
- restores backups if needed
- deploys small fixes

## Support Boundaries

Support should explicitly exclude:

- internal NDIA data integration work
- fraud investigation decisions
- legal advice
- production incident response commitments

## Recommended Pilot Support Commitments

For an initial deployment conversation, keep commitments modest:

- response within one business day for operational issues
- weekly check-in with the team lead
- one agreed demo environment
- one agreed dataset refresh cadence
- one agreed contact point for issues

## Escalation Path

If the deployment reveals a useful workflow, the next step should be a formal scope decision:

1. keep as a discovery tool
2. extend to a controlled rollout
3. plan production hardening separately

## What Buyers Need To Hear

The support model should make clear that:

- the tool is maintained
- the workflow is understandable
- the evidence trail is readable
- the deployment is bounded
- production readiness is a separate conversation

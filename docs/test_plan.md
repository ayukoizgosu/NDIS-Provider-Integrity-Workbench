# Test Plan

## Purpose

This test plan covers the current pilot build and the minimum checks required before a government-facing demo.

## Test Levels

### 1. Startup Checks

- app launches successfully
- page loads in the browser
- no fatal exceptions in the Streamlit log

### 2. Search Flow

- typing in the sidebar search box shows readable text
- `Enter` submits the search
- `Apply Search` filters results
- `Clear` resets the current filters
- no-match searches show a visible empty state

### 3. Lookup Flow

- a record can be selected from the lookup list
- the summary cards render correctly
- business match text is readable
- source links appear when a case exists

### 4. Case Workflow

- a case can be created from a lookup result
- the case appears in the case desk
- status, priority, owner, and decision can be updated
- notes can be added
- event history updates after each change

### 5. Export Flow

- HTML brief export completes
- exported file is created in `output/case_briefs/`
- exported brief opens without syntax issues
- brief contains the public-data limitation note

### 6. Review Queue Flow

- review queue counts match the filtered view
- review items remain reviewable
- related-business leads render without crashing

## Smoke Test Cases

Use these records for manual verification:

- `CARING ANGELS PTY LTD`
- `nib Thrive Pty Ltd`
- `Lifestyle Solutions (Aust) Ltd`

## Expected Results

- `CARING ANGELS PTY LTD` should open as a straightforward lookup and support case creation.
- `nib Thrive Pty Ltd` should show a state-check-needed match and a review-friendly explanation.
- `Lifestyle Solutions (Aust) Ltd` should render as a normal business-linked case with public evidence.

## Failure Modes To Check

- missing source CSVs
- empty review queue
- missing SQLite database
- malformed dates or scores
- no matching search results

The app should show a visible message instead of crashing.

## UI Expectations

- readable sidebar text
- visible buttons
- plain-language labels
- clear empty states
- no raw JSON blocks in the main workflow

## Demo Readiness Checklist

- search works
- case creation works
- notes persist
- export works
- manager queue is visible
- no browser console errors from the app itself

## Not In Scope

- load testing
- security penetration testing
- formal accreditation testing
- integration testing with internal NDIA systems

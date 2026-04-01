# Review Triage

**Run date:** 2026-03-30
**Scope:** public-data manual triage after exhausting the current missing-ABN queue

## Match Review Queue

Current queue size in `output/match_review_queue.csv`: `362`

Breakdown:

- `searched_alias_exact_review`: `5`
- `searched_name_exact_state_mismatch_review`: `2`
- `searched_name_probable`: `1`
- `searched_name_rejected`: `342`
- `unresolved_missing_abn`: `12`

### Recovered By Second-Pass Review-Resolve

Alias-based review matches recovered automatically:

- `Mary Chol Akook DENG, trading as Better Chance NDIS` -> `38443017966`
- `Anastasia Beth Kalpakis, also known as Anastasia Arthandi and Anastasia Beth Holani` -> `40480356891`
- `Dwayne Lee Wanganeen also known as Shane Mckenzie Wanganeen` -> `37583106393`
- `Noah Adel (alias Ahmed Moussa)` -> `15845806823`
- `Tejas Patel (also known as Tejaskumar Ghanshyambhai Patel)` -> `77135686764`

Exact corporate-name, state-mismatch review matches recovered automatically:

- `nib Thrive Pty Ltd` -> `69624874219`
- `Lifestyle Solutions (Aust) Ltd` -> `85097999347`

### Highest-priority manual review

1. `The Trustee for Great Mates; GM Trust`
   - Enforcement state: `QLD`
   - Best ABN candidate: `THE TRUSTEE FOR GREAT MATES TRUST`
   - Candidate ABN: `92309850500`
   - Candidate state/postcode: `QLD 4209`
   - Match score: `0.9767`
   - Triage view: likely valid trust-name resolution; wording differs but jurisdiction aligns.

2. `nib Thrive Pty Ltd`
   - Enforcement state: `NSW`
   - Best ABN candidate: `NIB THRIVE PTY LTD`
   - Candidate ABN: `69624874219`
   - Candidate state/postcode: `VIC 3145`
   - Match score: `1.0`
   - Triage view: exact legal-name match, but state mismatch is the only blocker. Manual review is warranted because ABN main-business state can differ from operating footprint.

3. `Lifestyle Solutions (Aust) Ltd`
   - Enforcement state: `NSW`
   - Best ABN candidate: `LIFESTYLE SOLUTIONS (AUST) LTD`
   - Candidate ABN: `85097999347`
   - Candidate state/postcode: `VIC 3189`
   - Match score: `1.0`
   - Triage view: same pattern as `nib Thrive Pty Ltd`; likely a real entity match blocked only by state mismatch.

### Low-priority review pool

- Most of the `searched_name_rejected` rows are personal banning-order names.
- Many have exact name similarity but remain unsafe to auto-link because the public ABN result is a person with the same name in a different state.
- These should stay manual-only unless additional free evidence is added.

### Unresolved names

The `12` unresolved rows are dominated by alias-heavy or multi-name entries such as:

- `Delta Ellen Brooks, also known as Delta Ellen Shalders, Delta Shalders and Delta Brooks`
- `King Isaiah Abu-Bayor, also known as Mr Idrissa Abu-Bayor; Mr Idrissa Abu-Dayor`
- `Waleed ABDUL RAZAK also known as Alex DEAN also known as Alex DEN also known as Dean ALEX`

Triage view:

- These are poor candidates for automated ABN resolution from the public search interface even after the alias pass.
- Leave unresolved unless a stronger free identity signal is introduced.

## Phoenix Candidate Queue

Current queue size in `output/phoenix_candidates.csv`: `8`

### Priority 1

1. `Ability Care Australia Pty Ltd (formerly Metro Plumbing and Services Pty Ltd)` -> `AUSTRALIAN ABILITY CARE PTY LTD`
   - Enforcement action: `banning_order`
   - Enforcement date: `2023-04-28T17:30:00`
   - Candidate registration date: `22/05/2023`
   - Days after enforcement: `23`
   - Candidate ABN/ACN: `67668146414` / `668146414`
   - Triage view: strongest timing signal in the current free-only queue.

2. `CARING ANGELS PTY LTD` -> `CARING ANGELS AUSTRALIA PTY LTD`
   - Enforcement action: `revocation`
   - Enforcement date: `2024-08-05T17:00:00`
   - Candidate registration date: `08/09/2024`
   - Days after enforcement: `33`
   - Candidate ABN/ACN: `71680593766` / `680593766`
   - Triage view: short delay and close naming pattern justify manual inspection.

### Priority 2

3. `Uplift Disability Services Pty Ltd` -> `UPLIFT DISABILITY SERVICES PTY LTD`
   - Candidate registration date: `08/08/2025`
   - Days after enforcement: `149`

4. `CARING COMMUNITY SERVICES PTY LTD` -> `CARING COMMUNITY SERVICES PTY LTD`
   - Candidate registration date: `11/02/2025`
   - Days after enforcement: `189`

5. `AURORA CARE GROUP PTY LTD` -> `AURORA CARE GROUP PTY LTD`
   - Candidate registration date: `01/07/2025`
   - Days after enforcement: `329`

### Priority 3

- `CARING ANGELS PTY LTD` -> `CARING ANGELS PTY LTD`
- `Ability Care Australia Pty Ltd (formerly Metro Plumbing and Services Pty Ltd)` -> `ABILITY CARE CO PTY LTD`
- `First Care Pty Ltd` -> `FIRST CARE CO PTY LTD`

Triage view:

- These remain reviewable, but the longer delay or broader naming pattern makes them weaker than the top five.

## Decision Boundary

- Do not label any row as phoenix conduct based on this file alone.
- Treat `output/phoenix_candidates.csv` as an investigation queue generated from public timing and naming signals only.
- Director overlap remains intentionally unavailable on the `$0` path.

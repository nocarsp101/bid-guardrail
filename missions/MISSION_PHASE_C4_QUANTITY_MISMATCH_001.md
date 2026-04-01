\# MISSION\_PHASE\_C4\_QUANTITY\_MISMATCH\_001



\## Objective

Introduce a deterministic quantity mismatch detection signal in the reconciliation engine so differences between quote and bid quantities are explicitly surfaced as actionable findings.



This is the first true guardrail signal beyond parsing and matching.



\## Context

Current system state:



\- Ingest works (C-1)

\- Line-number mapping works (C-2)

\- Unit canonicalization works (C-3)

\- Reconciliation successfully aligns quote ↔ bid rows



Example real condition:

Item 580:

\- Quote qty = 306.9

\- Bid qty = 884.25



This difference is currently visible in comparisons but NOT surfaced as a finding.



This mission makes that difference actionable.



\## Required Behavior



For any matched quote ↔ bid pair:



IF:

quote\_qty != bid\_qty (with tolerance)



THEN:

emit a new finding:



type: quote\_bid\_quantity\_mismatch



Include:

\- item identifier (canonical)

\- quote quantity

\- bid quantity

\- delta

\- percentage difference (if applicable)



\## Scope



\### In scope

\- add new finding type for quantity mismatch

\- implement comparison logic inside reconciliation path

\- include tolerance handling

\- include structured metadata in finding

\- update tests to validate behavior

\- run full test suite



\### Out of scope

\- unit canonicalization changes

\- line mapping changes

\- totals logic changes

\- summary row filtering (handled separately)

\- fuzzy matching

\- schema refactor



\## Protected Files



High-risk:

\- backend/app/quote\_reconciliation/rules.py



Allowed:

\- minimal additions only

\- no restructuring

\- no logic rewrites



Do not touch:

\- tests/fixtures/adel\_ipsi/\*

\- unit canonicalization module



\## Architecture Rules



1\. Quantity mismatch detection must operate ONLY on already-matched rows.

2\. Do not affect matching logic.

3\. Do not affect unit comparison logic.

4\. Use canonical quantities (post-normalization).

5\. Must not break existing findings.

6\. Must be deterministic and explainable.



\## Tolerance Rules



Implement a small tolerance:



\- absolute tolerance: 0.0001

\- OR configurable constant



Purpose:

\- avoid floating point noise

\- do NOT hide real differences



\## Finding Design



Add new finding:



type: "quote\_bid\_quantity\_mismatch"



severity: 

\- WARN (default)



message example:

"Quote quantity (306.9) differs from bid quantity (884.25)"



meta:

{

&#x20; "quote\_qty": 306.9,

&#x20; "bid\_qty": 884.25,

&#x20; "delta": -577.35,

&#x20; "percent\_diff": -65.27

}



\## Implementation Plan



1\. Locate reconciliation loop in:

&#x20;  backend/app/quote\_reconciliation/rules.py



2\. After:

&#x20;  - successful item match

&#x20;  - successful unit comparison



3\. Add:

&#x20;  quantity comparison block



4\. Compute:

&#x20;  delta = quote\_qty - bid\_qty



5\. If abs(delta) > tolerance:

&#x20;  emit finding



6\. Do NOT block processing

&#x20;  (this is informational/actionable, not fatal)



\## Test Requirements



\### Update existing test:

\- test\_item\_580\_comparison\_shows\_qty\_divergence



Add assertions:

\- quantity mismatch finding exists

\- values match expected



\### Add new tests:

\- matching quantities → no finding

\- small tolerance difference → no finding

\- large difference → finding triggered

\- negative and positive deltas handled



\### Ensure:

\- all existing tests still pass

\- no regression in unit or mapping logic



\## Expected Outcome



After this mission:



\- item 580 produces explicit finding

\- system surfaces real quantity discrepancies

\- output moves from "comparison" → "decision support"



\## Deliverables



Return:



1\. Files changed

2\. Exact code addition summary

3\. Finding structure example

4\. Test updates

5\. Before vs after behavior

6\. Any edge cases discovered

7\. Recommendation for next mission



\## Stop Conditions



Stop if:

\- change requires modifying matching logic

\- quantity fields are inconsistent across inputs

\- tolerance creates ambiguity

\- logic starts affecting unrelated findings



\## Success Condition



Success means:



\- quantity differences are explicitly flagged

\- existing behavior unchanged otherwise

\- tests pass

\- finding is clear, structured, and actionable


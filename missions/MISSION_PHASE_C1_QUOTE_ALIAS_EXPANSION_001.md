\# MISSION\_PHASE\_C1\_QUOTE\_ALIAS\_EXPANSION\_001



\## Objective

Perform the first Phase C repair in the Bid Guardrail repo by surgically expanding quote ingest alias handling so the IPSI structured quote can be ingested by the active quote reconciliation pipeline.



\## Context

Phase B created the Adel/IPSI canonical test harness and proved the following:

\- all tests are deterministic and passing

\- the IPSI quote ingest currently fails for known reasons

\- one top blocker is missing quote aliases for:

&#x20; - "Bid Item #"

&#x20; - "Per Unit"



The test harness already captures this broken behavior and is ready to validate a surgical repair.



This mission is NOT a broad reconciliation rewrite.

This mission is a narrowly-scoped ingest repair.



\## Required Repair

Target the active quote ingest path only.



Primary goal:

\- enable `Unorganized data for IPSI.xlsx` style structured quote data to ingest successfully through the active quote reconciliation ingest pipeline



Specifically address the alias gap for:

\- "Bid Item #"

\- "Per Unit"



Only add the minimum additional alias support truly required by the Adel/IPSI Phase B truth pack.



\## Scope

\### In scope

\- inspect active quote ingest modules

\- inspect active quote alias dictionary

\- add or adjust quote aliases needed for IPSI structured quote ingest

\- make minimal supporting ingest changes only if strictly required

\- update or add tests only where the expected behavior has intentionally changed

\- run full test suite

\- report exact behavioral delta



\### Out of scope

\- unit equivalence repair

\- line-number-to-DOT-item bridge

\- totals tolerance repair

\- quantity divergence logic

\- fuzzy matching

\- frontend changes

\- broad refactor



\## Protected Files

High-risk files:

\- backend/app/quote\_reconciliation/aliases.py

\- backend/app/quote\_reconciliation/ingest.py



Secondary protected files:

\- backend/app/quote\_reconciliation/rules.py

\- backend/app/bid\_validation/normalize.py



Do not touch secondary protected files unless absolutely necessary, and explain why.



Do not touch:

\- tests/fixtures/adel\_ipsi/\*

except only if a test fixture path reference is broken. Do not alter truth content.



\## Architecture Rules

1\. Active path only. Do not revive or reuse dead code from `quote\_validation/`.

2\. Keep this a quote-ingest repair, not a reconciliation rewrite.

3\. Preserve separation between:

&#x20;  - proposal line number

&#x20;  - DOT item number

4\. Do not solve line-number mapping in this mission.

5\. Do not add unit equivalence in this mission.

6\. Any new alias must be justified by the canonical Adel/IPSI structured quote data.

7\. Minimize blast radius.



\## Expected Outcome

After this mission:

\- the IPSI structured quote file should ingest successfully through the active quote ingest pipeline

\- tests that previously expected ingest failure should be updated to reflect successful ingest

\- downstream reconciliation should still show known later-stage problems until future missions address them



\## Required Validation

Run:

python -m pytest tests/ -v



Then clearly report:

1\. what tests changed

2\. what behavior changed

3\. whether IPSI quote ingest now succeeds

4\. what remains broken after ingest succeeds



\## Deliverables

Return all of the following:



\### 1. Files changed

List every changed file.



\### 2. Alias delta

Show exactly which aliases were added/changed and why.



\### 3. Test delta

Explain which tests were updated because expected behavior changed from failure to success.



\### 4. Result

State whether the active quote ingest now accepts the IPSI structured quote.



\### 5. Remaining blockers

List the next blockers now exposed after this fix.



\### 6. Recommendation

Recommend the next mission, likely either:

\- unit equivalence

or

\- line-number-to-DOT-item bridge

based on evidence



\## Stop Conditions

Stop and report if any of the following occur:

1\. IPSI ingest requires more than a small alias/ingest patch

2\. Fixing ingest exposes broader normalization divergence requiring larger redesign

3\. You find that the canonical structured quote cannot be represented cleanly in current ingest without major schema changes

4\. You are tempted to also fix reconciliation logic in the same mission



\## Execution Plan

1\. Read protocol file and obey it

2\. Create pre-mission checkpoint if any code changes will be made

3\. Inspect active quote ingest and alias path

4\. Add minimal alias support for the IPSI structured quote

5\. Update tests to reflect intentional behavior change

6\. Run full test suite

7\. Report exact results and next blocker



\## Success Condition

Success means:

\- IPSI structured quote ingest succeeds

\- change is minimal

\- test suite passes

\- no unrelated behavior drift occurred


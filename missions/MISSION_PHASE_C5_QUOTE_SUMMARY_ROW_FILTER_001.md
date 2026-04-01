\# MISSION\_PHASE\_C5\_QUOTE\_SUMMARY\_ROW\_FILTER\_001



\## Objective

Eliminate spurious summary-row noise from quote ingest by filtering quote total/summary rows before they reach reconciliation.



This is a cleanup mission, not a logic rewrite.



\## Context

Current system state:

\- structured quote ingest works

\- line mapping works

\- unit canonicalization works

\- quantity mismatch detection works



One remaining noisy issue in the Adel/IPSI structured quote pack:

\- the TOTAL summary row leaks through quote ingest

\- it reaches reconciliation with missing unit\_price

\- it generates a spurious quote\_line\_missing\_unit\_price finding



This is not a real business signal.

It is ingest noise and should be filtered upstream.



\## Required Behavior

Quote ingest should detect and exclude obvious summary rows such as:

\- TOTAL

\- SUBTOTAL

\- GRAND TOTAL

\- similar total-only summary rows



These rows should not be treated as quote line items.



\## Scope



\### In scope

\- inspect active quote ingest path

\- add summary-row detection for quote ingest

\- filter summary rows before they become normalized quote rows

\- add/update tests to validate behavior

\- run full test suite



\### Out of scope

\- changes to reconciliation logic

\- changes to matching logic

\- unit normalization changes

\- quantity logic changes

\- totals cross-check logic changes

\- broad ingest refactor



\## Protected Files

High-risk:

\- backend/app/quote\_reconciliation/ingest.py



Do not touch:

\- backend/app/quote\_reconciliation/rules.py unless absolutely necessary

\- tests/fixtures/adel\_ipsi/\*

\- line mapping adapter

\- unit canonicalization module



\## Architecture Rules

1\. Filter summary rows at ingest, not downstream.

2\. Summary row detection must be deterministic and explainable.

3\. Prefer a small helper function such as `\_is\_summary\_row()`.

4\. Do not guess aggressively.

5\. Keep the filter narrow enough to avoid dropping real quote items.

6\. Do not silently suppress real data rows.



\## Implementation Guidance

Use the existing bid ingest approach as reference if helpful, but do not force identical logic if quote structure differs.



A quote row should be considered a summary row only when evidence strongly suggests it is not a true item row.



Possible indicators:

\- item/description text contains TOTAL, SUBTOTAL, GRAND TOTAL

\- row lacks meaningful item/qty/unit/unit\_price structure

\- row is clearly an aggregate/footer line



The detection must stay conservative.



\## Test Requirements



\### Update existing Adel/IPSI tests

\- confirm the TOTAL row no longer appears in quote ingest output

\- confirm the spurious quote\_line\_missing\_unit\_price finding disappears



\### Add direct ingest tests

\- TOTAL row is filtered

\- SUBTOTAL row is filtered

\- normal item row is not filtered

\- ambiguous/non-summary wording is not incorrectly filtered



\### Ensure

\- no regression to quote ingest success

\- no change to real matched line counts except removal of the fake summary row

\- full test suite passes



\## Expected Outcome

After this mission:

\- the IPSI TOTAL row is filtered at ingest

\- the spurious missing\_unit\_price finding disappears

\- the structured Adel/IPSI pack becomes cleaner and closer to true signal-only output



\## Deliverables

Return:



1\. Files changed

2\. Exact summary-row detection logic added

3\. Test updates

4\. Before vs after behavior

5\. Any edge cases or risks noticed

6\. Recommendation for next mission



\## Stop Conditions

Stop if:

\- summary-row filtering requires broader ingest redesign

\- distinguishing summary rows from real rows becomes ambiguous

\- fix starts affecting legitimate line items

\- mission drifts into totals logic or reconciliation cleanup



\## Success Condition

Success means:

\- quote summary/footer rows are filtered upstream

\- no spurious missing\_unit\_price finding from the TOTAL row

\- real quote rows remain intact

\- tests pass


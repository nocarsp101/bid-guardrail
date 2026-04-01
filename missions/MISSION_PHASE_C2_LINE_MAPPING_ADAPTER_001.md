\# MISSION\_PHASE\_C2\_LINE\_MAPPING\_ADAPTER\_001



\## Objective

Introduce a line-number-to-DOT-item mapping adapter so that quotes using proposal line numbers (e.g., 520, 530) can be reconciled against bid rows using DOT item numbers.



This must be implemented as a preprocessing/adapter step — NOT embedded into reconciliation logic.



\## Context

Phase C-1 enabled IPSI quote ingest.



Current state:

\- quote rows contain proposal line numbers (520, 530, etc.)

\- bid rows contain DOT item numbers (e.g., 2524-6765010)

\- reconciliation fails 100% due to identifier mismatch



We already have:

tests/fixtures/adel\_ipsi/structured/line\_to\_item\_mapping.json



This is the canonical mapping source for this dataset.



\## Required Behavior



Given:

quote\_row.item = "520"



Transform to:

quote\_row.item = "2524-6765010"



BEFORE reconciliation runs.



\## Scope



\### In scope

\- create a mapping adapter function/module

\- load mapping from JSON fixture

\- apply mapping to quote rows

\- integrate adapter into test-controlled reconciliation path

\- update tests to validate mapped behavior



\### Out of scope

\- modifying reconciliation matching logic

\- unit equivalence

\- normalization rewrite

\- fuzzy matching

\- frontend



\## Architecture Rules



1\. Adapter must be separate from reconciliation logic.

2\. No modification of:

&#x20;  - quote\_reconciliation/rules.py (unless strictly necessary and minimal)

3\. Adapter must be optional and explicit.

4\. Mapping source must be external (JSON), not hardcoded.

5\. Fail-closed:

&#x20;  - if mapping missing → do NOT silently guess



\## Implementation Plan



1\. Create module:

&#x20;  backend/app/adapters/line\_mapping.py (or similar)



2\. Implement function:



&#x20;  def apply\_line\_number\_mapping(quote\_rows, mapping\_dict):

&#x20;      - for each quote row:

&#x20;          if item in mapping\_dict:

&#x20;              replace item with mapped DOT item

&#x20;          else:

&#x20;              leave unchanged OR mark (do not guess)



3\. Load mapping from:

&#x20;  tests/fixtures/adel\_ipsi/structured/line\_to\_item\_mapping.json



4\. Integrate into test path ONLY:

&#x20;  - do NOT modify production flow yet

&#x20;  - use adapter inside tests/test\_reconciliation\_adel\_ipsi.py



5\. Update tests:

&#x20;  - add test: mapping converts line numbers to DOT items

&#x20;  - add test: reconciliation works AFTER mapping

&#x20;  - preserve original failing test (without mapping)



\## Deliverables



1\. Files created/changed

2\. Adapter implementation

3\. Test updates

4\. Before vs after behavior:

&#x20;  - unmatched before mapping

&#x20;  - matched after mapping

5\. Remaining failures after mapping

6\. Recommendation for next mission



\## Stop Conditions



Stop if:

\- mapping requires modifying core reconciliation logic

\- mapping cannot be applied cleanly as a preprocessing step

\- mapping format is inconsistent or ambiguous



\## Success Condition



Success means:

\- mapping adapter works deterministically

\- reconciliation now produces matches instead of 100% unmatched

\- no unintended side effects

\- tests pass


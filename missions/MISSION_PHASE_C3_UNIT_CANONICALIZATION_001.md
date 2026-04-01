\# MISSION\_PHASE\_C3\_UNIT\_CANONICALIZATION\_001



\## Objective

Implement a real unit canonicalization layer for quote-to-bid reconciliation so equivalent unit expressions normalize to the same canonical value before comparison.



This is NOT a tiny alias patch.

This is a controlled, expandable normalization layer grounded in authoritative construction/DOT-style unit usage.



\## Context

Phase C-2 introduced a line-number-to-DOT-item mapping adapter and proved:

\- 14 of 14 IPSI quote lines now map to bid rows

\- 8 remaining failures are unit mismatches

\- these mismatches are due to equivalent expressions such as:

&#x20; - EA vs EACH

&#x20; - LS vs LUMP SUM



The user has explicitly flagged that this problem is broader than 2 aliases and will include:

\- case differences

\- punctuation differences

\- singular/plural forms

\- vendor wording variants

\- DOT/SUDAS-style abbreviations



So this mission must build a proper canonicalization layer, not a hack.



\## Product Framing

Treat this as a semantic normalization layer.



Input:

\- vendor units

\- DOT units

\- SUDAS-style units

\- inconsistent punctuation/case/plural forms



Output:

\- one canonical unit value per recognized concept



Examples:

\- EA, EACH, Each -> canonical equivalent

\- LS, LUMP SUM, Lump Sum -> canonical equivalent

\- SF, SQUARE FOOT, SQUARE FEET, sq ft -> canonical equivalent

\- SY, SQUARE YARD, SQUARE YARDS, sq yd -> canonical equivalent

\- LF, LINEAR FOOT, LINEAR FEET, L.F. -> canonical equivalent



Unknowns must fail closed.



\## Scope



\### In scope

\- introduce a unit canonicalization utility/module

\- define canonical units and alias variants

\- normalize units before reconciliation comparison

\- add test coverage for punctuation, case, plurals, vendor variants, and current Adel/IPSI failures

\- keep implementation surgical and explainable

\- run full test suite



\### Out of scope

\- broad rewrite of reconciliation logic

\- changing quantity logic

\- totals tolerance work

\- raw PDF normalizer work

\- frontend work

\- fuzzy guessing of unknown units

\- web scraping or live standards ingestion inside the codebase



\## Protected Files

High-risk files:

\- backend/app/quote\_reconciliation/rules.py

\- backend/app/bid\_validation/normalize.py



Potential new safe location:

\- backend/app/utils/unit\_normalization.py

or

\- backend/app/adapters/unit\_normalization.py



Prefer a separate reusable module rather than embedding a giant map directly inside rules.py.



Do not touch:

\- tests/fixtures/adel\_ipsi/\*

except if path references are broken. Do not alter truth content.



\## Architecture Rules

1\. Canonicalization must happen before unit comparison, not by stacking ad hoc `if` statements.

2\. Use a centralized alias/equivalence table.

3\. Canonical units must be explicit and readable.

4\. Unknown units must NOT be guessed.

5\. Normalization must handle:

&#x20;  - case

&#x20;  - punctuation

&#x20;  - repeated whitespace

&#x20;  - singular/plural wording

&#x20;  - common phrase variants

6\. Keep the system expandable for future vendor forms.

7\. Do not turn this mission into a generic ontology project. Keep it pragmatic and construction-focused.



\## Required Design

Implement a reusable unit normalization function, such as:



\- normalize\_unit(value: str | None) -> str

or

\- canonicalize\_unit(value: str | None) -> str



Behavior:

\- trim whitespace

\- uppercase or otherwise normalize case

\- remove harmless punctuation where appropriate

\- collapse spacing

\- map known aliases to canonical values

\- return canonical unit string for recognized units

\- return normalized fallback for unknowns OR special sentinel behavior, but DO NOT silently equate unknowns



Preferred comparison flow:

1\. normalize bid unit

2\. normalize quote unit

3\. compare canonical results



\## Canonicalization Requirements

At minimum, cover the current proven cases plus a practical core set.



\### Must support current proven cases

\- EA ↔ EACH

\- LS ↔ LUMP SUM



\### Must support core construction forms

At a minimum, add thoughtful canonicalization/tests for concepts like:

\- SF / SQUARE FOOT / SQUARE FEET / SQ FT / SQ. FT.

\- SY / SQUARE YARD / SQUARE YARDS / SQ YD / SQ. YD.

\- LF / LINEAR FOOT / LINEAR FEET / L.F.

\- CY / CUBIC YARD / CUBIC YARDS / CU YD / CU. YD.

\- TON / TONS

\- STA / STATION / STATIONS

\- EACH / EA

\- LUMP SUM / LS

\- UNIT / UNITS

\- ACRE / ACRES



Do not wildly expand beyond reason unless the code/test evidence justifies it.



\## Implementation Preference

Preferred pattern:

\- a canonical unit dictionary:

&#x20; CANONICAL\_UNIT\_ALIASES = {

&#x20;     "EACH": {...},

&#x20;     "LUMP SUM": {...},

&#x20;     "SF": {...},

&#x20;     ...

&#x20; }



or a reverse lookup generated from a canonical map.



Avoid:

\- scattering aliases across multiple files

\- giant inline conditionals in rules.py

\- silent guessing for unknown unit strings



\## Integration Rule

Integrate the canonicalization layer into the active reconciliation path only as needed to replace exact raw string comparison of units.



Do not otherwise alter reconciliation behavior.



Specifically:

\- do NOT alter line mapping logic

\- do NOT alter price comparison logic

\- do NOT alter totals logic

\- do NOT alter match-key logic



This mission is about unit equivalence only.



\## Test Requirements

Update/add tests to prove all of the following:



\### Unit normalization utility tests

\- case normalization works

\- punctuation normalization works

\- plural/singular normalization works

\- known aliases canonicalize correctly

\- unknown units remain non-equivalent



\### Adel/IPSI end-to-end tests

\- the current 8 unit mismatches are reduced appropriately after canonicalization

\- EA/EACH mismatches disappear

\- LS/LUMP SUM mismatch disappears

\- existing matched items with already-equal units still work

\- no unrelated regressions occur



\### Fail-closed tests

\- unknown/unmapped units are not silently treated as equal

\- a truly different unit still fails



\## Expected Outcome

After this mission:

\- the current 8 known unit mismatches should be resolved

\- mapped Adel/IPSI lines should reconcile through unit comparison correctly where units are actually equivalent

\- quantity differences such as item 580 should remain visible

\- remaining findings should reflect real business issues, not wording differences



\## Deliverables

Return all of the following:



\### 1. Files changed

List every file changed.



\### 2. Canonicalization design

Explain:

\- where the normalization layer lives

\- what canonical form you chose

\- how alias mapping is structured



\### 3. Unit coverage

List the canonical units and alias families added in this mission.



\### 4. Test delta

Explain:

\- which tests were added

\- which tests changed

\- how the current 8 mismatches changed



\### 5. Behavioral delta

State:

\- unit mismatches before

\- unit mismatches after

\- whether Adel/IPSI now passes unit comparison for all intended equivalent units



\### 6. Remaining blockers

List what still remains after unit canonicalization.



\### 7. Recommendation

Recommend the next mission based on evidence.



\## Stop Conditions

Stop and report if any of the following occur:

1\. Canonicalization requires a broader normalization redesign across unrelated modules

2\. You find multiple conflicting meanings for the same vendor unit form that cannot be safely normalized

3\. The change starts requiring broader reconciliation rewrites

4\. You are tempted to “clean up” totals, quantity, or summary-row logic in the same mission



\## Execution Plan

1\. Read protocol and obey it

2\. Create pre-mission checkpoint if code changes will be made

3\. Inspect current unit comparison path

4\. Create centralized unit canonicalization module

5\. Integrate it into reconciliation unit comparison only

6\. Expand tests

7\. Run full test suite

8\. Report exact before/after behavior and remaining blockers



\## Success Condition

Success means:

\- unit comparison is now semantic instead of raw-string brittle

\- current known equivalent units no longer fail

\- unknown units still fail closed

\- tests pass

\- no unrelated behavior drift occurred


\# MISSION\_PHASE\_B\_ADEL\_IPSI\_CANONICAL\_TEST\_001



\## Objective

Build the Phase B canonical test harness for the Bid Guardrail lane using the real Adel / IPSI test pack. Establish deterministic truth before any meaningful logic repair.



\## Context

We are recovering and productizing a Bid Guardrail MVP that validates and reconciles prime bid documents and subcontractor/vendor quotes.



The recovery audit already established:

\- scaffold is usable

\- core weakness is quote reconciliation logic

\- normalization divergence is likely the top cause of bad matching

\- zero-tolerance totals are likely causing false mismatches

\- there is currently no test harness



This mission is NOT a broad rebuild.

This mission is to create the canonical truth set and controlled tests so future repairs are grounded.



\## Canonical Test Pack

Use these uploaded files as the source set for this mission:



\### Prime / Bid side

1\. Adel Blank Bid Document.pdf

&#x20;  - role: visual/form-layout reference only

&#x20;  - contains Iowa DOT proposal form and schedule layout

&#x20;  - not the main numeric truth source



2\. Adel Missing subtotal and total.pdf

&#x20;  - role: noisy visual input reference

&#x20;  - handwritten/scanned bid form with many unit prices and line extensions filled in

&#x20;  - section total and total bid left blank

&#x20;  - use as a future extraction challenge, not primary numeric truth



3\. ADEL CSV without Numbers filled in.csv

&#x20;  - role: clean structural control input

&#x20;  - use for parsing/header/row mapping baseline tests

&#x20;  - this is one of the key baseline files for deterministic ingestion tests



4\. BidItems\_FinalTab\_values\_only.xlsx

&#x20;  - role: canonical structured prime bid truth

&#x20;  - use as the primary machine-readable truth set for bid-side tests



\### Quote side

5\. Unorganized data for IPSI.xlsx

&#x20;  - role: canonical structured quote truth

&#x20;  - use as the primary machine-readable truth set for quote-side tests



6\. IPSI QUOTE.pdf

&#x20;  - role: raw quote reference only

&#x20;  - use as visual/raw-source reference

&#x20;  - not the first truth source for core deterministic reconciliation tests



\## Required Product Framing

Treat this work as two separate product layers:



\### Layer 1 — Core Guardrail / Reconciliation Engine

Structured bid rows + structured quote rows in

Deterministic matching / discrepancy detection out



\### Layer 2 — Normalizer / Extraction Layer

Raw PDFs / scans / weird vendor formats

converted into structured rows upstream



This mission is for Layer 1 first.

Do NOT let raw-PDF extraction complexity contaminate the core deterministic guardrail tests.



\## Scope

Build the canonical Phase B test harness and fixtures.



Specifically:



1\. Add a proper tests/ structure

2\. Create deterministic fixtures based on the Adel / IPSI pack

3\. Define explicit expected outcomes

4\. Add unit/integration tests around the current system behavior

5\. Document the truth assumptions for this pack

6\. Do NOT perform broad logic rewrites



\## Boundaries

\### In scope

\- tests directory

\- pytest setup if missing

\- small fixture files derived from the canonical pack

\- helper loaders/parsers only if needed for tests

\- documentation of expected mappings/findings for this dataset

\- minimal code changes only if absolutely required to enable testing infrastructure



\### Out of scope

\- broad refactor of production logic

\- raw OCR pipeline buildout

\- “fix everything” reconciliation rewrite

\- frontend work

\- architecture drift

\- introducing large new dependencies unless clearly justified



\## Protected Files

Treat these production modules as protected unless a minimal test-enabling change is required:

\- backend/app/main.py

\- backend/app/bid\_validation/\*

\- backend/app/quote\_reconciliation/\*

\- backend/app/pdf\_validation/\*

\- backend/app/audit/\*

\- backend/app/storage/\*

\- frontend/\*



You may touch protected files only if:

\- it is necessary to expose/import current behavior for testing

\- the change is small, surgical, and clearly explained

\- it does NOT become a hidden Phase C logic rewrite



\## Architecture Rules

1\. Keep core reconciliation deterministic.

2\. Keep structured-truth tests separate from raw-document tests.

3\. Distinguish clearly between:

&#x20;  - proposal line number

&#x20;  - DOT item number

&#x20;  - vendor quote bid item number

4\. Do not assume vendor quote total should equal the full project bid total.

5\. The IPSI quote is a partial-scope quote, not the whole project.

6\. Unit equivalence must eventually support at least obvious equivalents like:

&#x20;  - EA ↔ EACH

&#x20;  - LS ↔ LUMP SUM

&#x20;  But do not silently rewrite production matching behavior in this mission unless needed and explicitly documented.

7\. Truth first, repairs later.



\## Ground Truth Expectations for This Dataset

For this Adel / IPSI pack, the system should ultimately recognize:



\### Bid side truths

\- The Iowa DOT proposal contains proposal line numbers and separate full DOT item numbers.

\- The CSV without filled numbers is the clean structural control file.

\- The XLSX values-only bid file is the canonical structured numeric truth source.



\### Quote side truths

\- The IPSI quote covers a subset of the Adel bid, not the whole project.

\- The quote appears to reference proposal line numbers such as:

&#x20; 520, 530, 540, 550, 560, 570, 580, 600, 610, 620, 630, 650, 670, 690

\- These should be treated as candidate mappings to corresponding Adel proposal line numbers, not assumed to be DOT item numbers.

\- The raw quote total shown on the PDF is $78,513.75.

\- The quote contains notes/qualifications that should be preserved for future normalizer logic.



\### Expected reconciliation shape

\- The quote should be treated as partial-scope.

\- Matched subset should be evaluated against corresponding bid rows.

\- Unquoted bid items outside the subset should not automatically be treated as quote failure.

\- Quantity differences should be surfaced as findings, not hidden.

\- Example likely difference already visible:

&#x20; item 580 quantity differs between quote and bid references.



\## Deliverables

Create and return all of the following:



\### 1. Test Harness Structure

A clear tests/ tree, likely including:

\- tests/test\_bid\_ingest.py

\- tests/test\_quote\_ingest.py

\- tests/test\_normalization.py

\- tests/test\_reconciliation\_adel\_ipsi.py

\- tests/test\_validate\_endpoint.py



Adjust names if needed, but keep them clean and deterministic.



\### 2. Fixture Strategy

Create a fixture set that distinguishes:

\- clean structured fixtures

\- raw reference fixtures



Recommended organization:

\- tests/fixtures/adel\_ipsi/structured/

\- tests/fixtures/adel\_ipsi/raw\_reference/



Do not bloat the repo with junk. Keep only what is needed.



\### 3. Truth Document

Create a short canonical truth doc in the repo, for example:

\- tests/fixtures/adel\_ipsi/README.md

or similar



It must explain:

\- what each file is for

\- what is truth vs reference

\- what the expected mappings are

\- what is partial scope vs full scope



\### 4. Deterministic Tests

At minimum, add tests that prove:

\- clean bid structure can be ingested deterministically

\- structured quote can be ingested deterministically

\- Adel/IPSI mapping is recognized as partial-scope subset

\- current behavior is captured, even if failing

\- quote total expectation is grounded in the quote truth source

\- endpoint /validate can be exercised in a controlled way if practical



\### 5. Gap Report

At the end of the run, provide:

\- what tests pass now

\- what tests fail now

\- which failures reflect known broken logic vs test/environment issues

\- the recommended first Phase C repair target



\## Decision Rights

Claude may:

\- inspect the repo

\- add tests

\- add small fixture files

\- add pytest config if needed

\- make minimal production changes only if strictly necessary for testability

\- checkpoint when appropriate



Claude must not:

\- perform an uncontrolled rewrite

\- silently fix reconciliation logic under the guise of test setup

\- treat raw PDF extraction as solved

\- collapse proposal line number and DOT item number into one concept without proof

\- claim the system is correct without tests proving it



\## Stop Conditions

Stop and report if any of the following occur:

1\. The uploaded files are insufficient to build deterministic structured fixtures

2\. The repo lacks a runnable test environment and requires non-trivial environment reconstruction

3\. Any needed production change becomes larger than a small test-enabling patch

4\. The current code cannot express partial-scope quote testing without meaningful logic changes

5\. Fixture derivation creates ambiguity about truth that cannot be resolved from the provided files



\## Execution Plan

1\. Inspect current repo test/dev environment

2\. Create a pre-run checkpoint if any meaningful code changes will occur

3\. Build tests/ structure

4\. Create Adel/IPSI fixture set

5\. Write truth documentation for the fixture pack

6\. Write deterministic tests around ingest and reconciliation

7\. Run tests

8\. Report exact pass/fail status

9\. Recommend next Phase C mission based on evidence



\## Validation

Success for this mission means:

\- the repo now has a real canonical Phase B test harness

\- Adel/IPSI is captured as a reusable truth pack

\- future repairs can be measured against explicit expected outcomes

\- we are no longer guessing



\## Audit Output

Return all of the following in your final response:

\- files created/changed

\- test tree created

\- fixture tree created

\- truth assumptions documented

\- tests passing

\- tests failing

\- whether any protected production files were touched

\- exact recommendation for the next mission


\# MISSION\_NORMALIZATION\_ARCHITECTURE\_001



\## Objective

Design the canonical normalization architecture for Bid Guardrail so the product can ingest raw bid/quote documents, normalize them into a shared internal schema, compare them through the existing guardrail engine, and output actionable findings.



This is an architecture/design mission, not a broad implementation mission.



\## Context

Current progress has established a deterministic comparison core:

\- structured bid ingest works

\- structured quote ingest works

\- line-number mapping adapter exists

\- unit canonicalization exists

\- reconciliation works on normalized structured data



However, the real product goal is not to depend on staff converting PDFs to Excel.

The real product goal is:

raw documents in → normalized internal schema → comparison engine → actionable output



So we now need a clear architecture for the normalization layer.



\## Product Goal

Build toward a system that accepts:

\- subcontractor quote PDFs

\- scanned quotes

\- emailed estimate attachments

\- bid spreadsheets

\- DOT proposal forms

\- mixed-format inputs



and converts them into one canonical internal representation suitable for deterministic comparison.



\## Required Deliverables



\### 1. Canonical Schema Proposal

Define the internal normalized schema for:

\- bid document

\- quote document

\- line item

\- totals

\- notes/exclusions/qualifications

\- source metadata

\- confidence flags

\- review flags



The schema should distinguish clearly between:

\- source raw value

\- normalized value

\- canonical value

\- confidence / warnings



\### 2. Adapter Architecture

Propose a source-adapter model for at least:

\- structured spreadsheet adapter

\- DOT proposal/bid form adapter

\- vendor PDF table adapter

\- scanned PDF/OCR adapter



For each adapter, define:

\- what it is responsible for

\- what it should output

\- what it should never guess

\- what confidence or review flags it should emit



\### 3. Pipeline Design

Define the end-to-end flow:

raw file intake → source classification → extraction → normalization → reconciliation preparation → comparison → output



Include:

\- where line-number mapping belongs

\- where unit canonicalization belongs

\- where totals/summary row handling belongs

\- where exclusions/notes parsing belongs

\- where fail-closed behavior belongs



\### 4. Confidence / Review Model

Define when the system should:

\- auto-accept normalized rows

\- flag rows for human review

\- refuse comparison entirely



This must be reality-based and safe for business use.



\### 5. Storage / Audit Model

Define what should be persisted for trust and traceability:

\- original raw file

\- extracted intermediate data

\- normalized canonical data

\- comparison output

\- user corrections / overrides

\- audit metadata



\### 6. Near-Term Build Plan

Recommend a phased implementation roadmap:

\- what to build first

\- what to delay

\- what to prototype with known fixtures

\- how to avoid contaminating the deterministic core with parser chaos



\## Scope

\### In scope

\- architecture

\- schema design

\- adapter design

\- confidence model

\- audit/storage design

\- phased roadmap



\### Out of scope

\- broad code implementation

\- OCR buildout

\- frontend buildout

\- production deployment

\- random parser experimentation



\## Architecture Rules

1\. Keep deterministic comparison core protected.

2\. Normalization must feed the core; it must not pollute the core.

3\. Separate source-specific extraction from canonical normalization.

4\. Preserve fail-closed behavior for uncertain data.

5\. Every normalized field should be traceable back to source.

6\. Staff review should be exception-based, not the main workflow.

7\. The design must support future productization beyond one contractor or quote style.



\## Deliverable Format

Return:

\- canonical schema proposal

\- adapter model

\- pipeline diagram in text

\- confidence/review rules

\- audit/storage model

\- near-term roadmap

\- explicit recommendation for the next implementation mission



\## Stop Conditions

Stop and report if:

\- the current codebase makes a clean normalization layer impossible without major restructuring

\- the existing comparison core is too unstable to serve as the downstream target

\- the proposed schema cannot represent both DOT bids and vendor quotes without major ambiguity



\## Success Condition

Success means:

\- we have a clear architecture for raw-doc normalization

\- future parser/adapter work has a canonical target

\- the product path is now “documents in → decision out,” not “staff converts to Excel first”


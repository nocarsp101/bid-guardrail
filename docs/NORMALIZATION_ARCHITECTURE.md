# Normalization Architecture — Bid Guardrail

## Status

Architecture proposal. Design mission output. Not yet implemented.

---

## 1. Canonical Schema Proposal

### 1.1 Document Envelope

Every ingested document (bid or quote) is wrapped in a document envelope
that carries source metadata and processing state.

```
NormalizedDocument {
    doc_id:             str           # unique, system-assigned
    doc_type:           enum          # PRIME_BID | QUOTE
    source_file:        SourceFile    # original file metadata
    extraction_method:  str           # adapter that produced this
    extraction_ts:      datetime      # when extraction ran
    confidence:         Confidence    # document-level confidence
    line_items:         [CanonicalLineItem]
    totals:             TotalsBlock | null
    notes:              [DocumentNote]
    review_flags:       [ReviewFlag]
    meta:               dict          # adapter-specific passthrough
}
```

### 1.2 Source File Metadata

```
SourceFile {
    original_filename:  str
    file_hash_sha256:   str
    file_size_bytes:    int
    mime_type:          str
    upload_ts:          datetime
    stored_path:        str          # path in run storage
}
```

### 1.3 Canonical Line Item

Every line item carries three value layers: raw, normalized, canonical.

```
CanonicalLineItem {
    _row_index:         int          # position in source (0-based)

    # --- Identifier fields ---
    item: {
        raw:            str          # exactly as read from source
        normalized:     str          # trimmed, leading-zero-stripped
        canonical:      str | null   # DOT item number (if mapped)
        source_type:    enum         # DOT_ITEM | PROPOSAL_LINE | VENDOR_REF | UNKNOWN
        confidence:     Confidence
    }

    # --- Description ---
    description: {
        raw:            str
        normalized:     str          # trimmed, whitespace-collapsed
        confidence:     Confidence
    }

    # --- Unit ---
    unit: {
        raw:            str          # exactly as read
        normalized:     str          # uppercase, periods stripped
        canonical:      str          # output of canonicalize_unit()
        confidence:     Confidence
    }

    # --- Quantity ---
    qty: {
        raw:            str | number
        value:          float | null
        confidence:     Confidence
    }

    # --- Unit Price ---
    unit_price: {
        raw:            str | number
        value:          float | null
        confidence:     Confidence
    }

    # --- Total ---
    total: {
        raw:            str | number | null
        value:          float | null
        computed:        bool         # true if total = qty * unit_price
        confidence:     Confidence
    }

    # --- Notes / exclusions ---
    notes:              str | null
    excluded:           bool
    exclusion_reason:   str | null

    # --- Row-level flags ---
    review_flags:       [ReviewFlag]
    is_summary_row:     bool         # detected as subtotal/total/rollup
}
```

### 1.4 Confidence Model

```
Confidence {
    level:  enum    # HIGH | MEDIUM | LOW | UNVERIFIED
    reason: str     # human-readable explanation
}
```

Level definitions:

| Level | Meaning | Source Example |
|-------|---------|---------------|
| HIGH | Machine-readable structured data, exact cell value | XLSX cell, CSV field |
| MEDIUM | Parsed from structured PDF with known template | DOT proposal table extraction |
| LOW | Extracted from unstructured layout, heuristic-dependent | Vendor PDF table, OCR region |
| UNVERIFIED | Value present but system cannot verify correctness | OCR with low char confidence |

### 1.5 Totals Block

```
TotalsBlock {
    stated_total: {
        raw:        str | number | null
        value:      float | null
        source:     str             # "cell B15", "PDF footer", etc.
        confidence: Confidence
    }
    computed_total:     float | null # sum of line_items[].total.value
    delta:              float | null # stated - computed
    crosscheck_status:  enum        # MATCH | MISMATCH | UNAVAILABLE
}
```

### 1.6 Document Notes

```
DocumentNote {
    type:       enum    # QUALIFICATION | EXCLUSION | SCOPE_LIMIT | GENERAL
    text:       str
    source:     str     # where in source this was found
    confidence: Confidence
}
```

### 1.7 Review Flags

```
ReviewFlag {
    flag_type:  str     # e.g. "low_confidence_extraction", "unit_unknown", etc.
    field:      str     # which field triggered this
    row_index:  int | null
    message:    str
    severity:   enum    # BLOCK | REVIEW | INFO
}
```

---

## 2. Adapter Architecture

### 2.1 Adapter Interface

Every adapter implements the same contract:

```
Adapter.extract(source_file: Path, hints: dict) -> NormalizedDocument
```

Each adapter is responsible for:
- reading its specific format
- extracting line items into the CanonicalLineItem schema
- setting confidence levels honestly
- emitting review flags for anything uncertain
- marking summary/total rows

Each adapter must NOT:
- guess values it cannot extract
- silently substitute defaults for missing data
- perform reconciliation or business-rule evaluation
- apply line-number-to-DOT-item mapping (that's a pipeline step, not adapter work)

### 2.2 Adapter Types

#### A. Structured Spreadsheet Adapter

**Exists today** as `bid_validation/ingest.py` and `quote_reconciliation/ingest.py`.

| Aspect | Detail |
|--------|--------|
| Inputs | XLSX, CSV |
| Confidence | HIGH for all extracted fields |
| Header detection | Alias-based deterministic mapping (current system) |
| Review flags | Emitted only for unmapped optional columns |
| Failure mode | IngestError on missing required headers or ambiguity |
| Summary detection | Current `_is_summary_row()` heuristic |

Migration path: wrap existing ingest functions to output `NormalizedDocument` instead of `(rows, meta)` tuple.

#### B. DOT Proposal Form Adapter

**Does not exist yet.** For structured PDF forms with known Iowa DOT layout.

| Aspect | Detail |
|--------|--------|
| Inputs | PDF matching DOT proposal template |
| Confidence | MEDIUM-HIGH (template-aware extraction) |
| Strategy | Template-anchored table extraction using page geometry |
| Header detection | Known fixed positions from template definition |
| Review flags | LOW on any cell where extraction confidence is below threshold |
| Failure mode | Refuse extraction if template match score is too low |
| Key challenge | Handwritten cells, scan quality, form version drift |

This adapter should use template definitions (page regions, expected column positions) rather than generic table detection.

#### C. Vendor PDF Table Adapter

**Does not exist yet.** For semi-structured vendor quotes (typed PDFs with tables).

| Aspect | Detail |
|--------|--------|
| Inputs | Typed/digital PDF with tabular data |
| Confidence | MEDIUM (layout heuristic extraction) |
| Strategy | Generic table detection + header alias matching |
| Header detection | Apply existing alias dictionaries to detected table headers |
| Review flags | LOW on any row where column alignment is uncertain |
| Failure mode | If no table detected or headers don't match, emit BLOCK flag |
| Key challenge | Multi-page tables, merged cells, footnotes in table region |

#### D. Scanned PDF / OCR Adapter

**Does not exist yet.** Lowest confidence; highest human review requirement.

| Aspect | Detail |
|--------|--------|
| Inputs | Scanned PDF, image-based PDF |
| Confidence | LOW to UNVERIFIED |
| Strategy | OCR → text → table detection → alias matching |
| Review flags | REVIEW on every extracted row; BLOCK if OCR confidence < threshold |
| Failure mode | If overall OCR quality is too low, refuse extraction entirely |
| Key challenge | Handwriting, stamps, poor scan quality, mixed orientations |

### 2.3 Adapter Selection

```
classify_source(file: Path) -> AdapterType:
    if file is XLSX/CSV:
        return STRUCTURED_SPREADSHEET
    if file is PDF:
        if is_scanned(file):       # heuristic: <50 chars text per page
            return SCANNED_OCR
        if matches_dot_template(file):
            return DOT_PROPOSAL_FORM
        else:
            return VENDOR_PDF_TABLE
```

This classification runs before extraction and determines which adapter processes the file.

---

## 3. Pipeline Design

### 3.1 End-to-End Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                        INTAKE LAYER                                 │
│  raw file upload → file validation → source classification          │
│                          ↓                                          │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                   EXTRACTION LAYER                           │   │
│  │  Adapter A (Spreadsheet)                                     │   │
│  │  Adapter B (DOT Form)      → NormalizedDocument              │   │
│  │  Adapter C (Vendor PDF)                                      │   │
│  │  Adapter D (OCR/Scan)                                        │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                          ↓                                          │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │               NORMALIZATION LAYER                            │   │
│  │  1. Header alias resolution       (existing)                 │   │
│  │  2. Summary/total row detection   (existing, improve)        │   │
│  │  3. Item code normalization       (existing)                 │   │
│  │  4. Unit canonicalization         (existing — C-3)           │   │
│  │  5. Notes/exclusion parsing       (new)                      │   │
│  │  6. Confidence aggregation        (new)                      │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                          ↓                                          │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │            RECONCILIATION PREP LAYER                         │   │
│  │  7. Line-number-to-DOT-item mapping   (existing — C-2)      │   │
│  │  8. Scope classification              (partial vs full)      │   │
│  │  9. Review gate                       (block if too low)     │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                          ↓                                          │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │            COMPARISON ENGINE (protected core)                │   │
│  │  10. Deterministic match on canonical item                   │   │
│  │  11. Unit comparison (canonical)                             │   │
│  │  12. Unit price guardrail                                    │   │
│  │  13. Quantity divergence detection                           │   │
│  │  14. Totals cross-check                                     │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                          ↓                                          │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                    OUTPUT LAYER                               │   │
│  │  15. Findings aggregation                                    │   │
│  │  16. Review flag surfacing                                   │   │
│  │  17. Audit event creation                                    │   │
│  │  18. Response construction                                   │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

### 3.2 Where Each Concern Lives

| Concern | Pipeline Stage | Current Status |
|---------|---------------|----------------|
| File validation | Intake | Exists (main.py extension checks) |
| Source classification | Intake | New — needed for multi-format |
| Header alias resolution | Extraction/Normalization | Exists (aliases.py) |
| Summary/total row detection | Normalization | Exists (ingest.py bid-side only) |
| Item code normalization | Normalization | Exists (normalize.py) |
| Unit canonicalization | Normalization | Exists (unit_canonicalization.py) |
| Notes/exclusion parsing | Normalization | Stub only (field exists, not parsed) |
| Line-number-to-DOT-item mapping | Reconciliation Prep | Exists as adapter (test-path only) |
| Scope classification | Reconciliation Prep | Implicit (not formalized) |
| Review gate | Reconciliation Prep | New — needed for low-confidence data |
| Deterministic matching | Comparison Engine | Exists (rules.py) |
| Unit comparison | Comparison Engine | Exists (rules.py + canonicalize_unit) |
| Price guardrail | Comparison Engine | Exists (rules.py) |
| Quantity divergence | Comparison Engine | Visible in comparisons, no distinct finding |
| Totals cross-check | Comparison Engine | Exists (rules.py, WARN) |
| Audit logging | Output | Exists (audit/writer.py) |

### 3.3 Critical Boundary: Normalization ↔ Comparison

The comparison engine (steps 10–14) MUST receive fully normalized
`CanonicalLineItem` data. It must NEVER:
- perform extraction
- guess missing values
- apply format-specific heuristics
- inspect source confidence

The comparison engine treats all input as canonical truth. If upstream
layers are uncertain, they must either resolve the uncertainty or block
the row from reaching comparison.

---

## 4. Confidence / Review Model

### 4.1 Decision Rules

| Condition | Action |
|-----------|--------|
| All fields HIGH confidence | Auto-accept. Route to comparison. |
| Any field MEDIUM confidence | Accept with INFO review flag. Route to comparison. |
| Any field LOW confidence | Accept with REVIEW flag. Route to comparison, but surface flag in output. |
| Any field UNVERIFIED | BLOCK row from comparison. Require human review. |
| Required field missing/unextracted | BLOCK row. Emit finding. |
| Adapter extraction failed entirely | BLOCK document. Return extraction error. |

### 4.2 Confidence Inheritance

Document-level confidence is the minimum of all its line items' confidence levels.

Line-item confidence is the minimum of its field-level confidence values
for required fields (item, unit, qty, unit_price).

### 4.3 Review Queue

When a row or document has REVIEW or BLOCK flags:

1. The system produces a `review_required` finding.
2. The finding includes the specific fields and reasons.
3. The user can:
   - Confirm extracted values → mark as reviewed, override confidence to HIGH.
   - Correct extracted values → system re-runs comparison with corrected data.
   - Reject the document → comparison aborted for this source.

### 4.4 Override Model

The existing `OverrideInfo` model in `audit/models.py` already supports
overrides. Extend it to support field-level corrections:

```
FieldCorrection {
    row_index:      int
    field:          str         # "unit_price", "qty", etc.
    original_value: str
    corrected_value: str
    corrected_by:   str         # actor
    corrected_at:   datetime
    reason:         str | null
}
```

---

## 5. Storage / Audit Model

### 5.1 What to Persist

| Stage | Artifact | Storage | Retention |
|-------|----------|---------|-----------|
| Upload | Original raw file | `runs/{run_id}/uploads/` | Permanent |
| Upload | File hash + metadata | `run_meta.json` | Permanent |
| Extraction | Adapter output (NormalizedDocument as JSON) | `runs/{run_id}/extracted/` | Permanent |
| Normalization | Canonical line items + confidence flags | Part of NormalizedDocument | Permanent |
| Comparison | Findings + comparisons + summary | `runs/{run_id}/results/` | Permanent |
| Corrections | Field-level overrides | `runs/{run_id}/corrections/` | Permanent |
| Audit | Immutable audit event | `audit/audit_log.jsonl` | Permanent |

### 5.2 Traceability Chain

For any finding or comparison result, the system must support tracing back:

```
finding → comparison row → canonical line item → extracted data → raw file cell/region
```

This is achieved by:
- `_row_index` linking canonical items to extraction positions
- `raw` values preserved alongside `normalized` and `canonical`
- Source file hash linking to the original upload
- Adapter metadata recording extraction coordinates (cell ref, PDF page/region)

### 5.3 Existing Infrastructure

The current `RunStorage` and `AuditWriter` classes are sufficient for
the near-term. They already support:
- Per-run directory isolation
- File upload persistence
- Append-only audit log

Extension needed:
- Save `NormalizedDocument` JSON alongside uploads
- Save comparison results JSON
- Save corrections JSON

This is additive, not restructuring.

---

## 6. Near-Term Build Plan

### Phase 1: Schema Formalization (1–2 missions)

**Goal:** Formalize `NormalizedDocument` and `CanonicalLineItem` as Pydantic models.

- Create `backend/app/models/canonical.py` with the schema from Section 1.
- Wrap existing structured ingest to output `NormalizedDocument`.
- All existing tests continue to pass — this is additive.
- No new adapters yet. Only the structured spreadsheet adapter is formalized.

**Why first:** Everything downstream depends on a stable schema. Until the
schema exists as code, adapters have no target.

### Phase 2: Production Line-Mapping Integration (1 mission)

**Goal:** Move the line-number mapping adapter from test-path to production.

- The adapter already exists and is tested.
- Define how mapping is provided (upload alongside quote? auto-detect from bid CSV?).
- Integrate into `/validate` endpoint for QUOTE mode.
- Add mapping source to `NormalizedDocument.meta`.

**Why second:** This is the smallest gap between current test-path behavior
and production behavior. Low risk, high value.

### Phase 3: Quote Summary Row Filtering (1 small mission)

**Goal:** Filter TOTAL/summary rows during quote ingest.

- Port `_is_summary_row()` logic from bid ingest to quote ingest.
- Eliminates the `missing_unit_price` finding from the TOTAL row.
- Small, surgical, already identified.

### Phase 4: Quantity Divergence Finding (1 small mission)

**Goal:** Surface quantity differences as a distinct finding type.

- When quote qty ≠ bid qty for a matched item, emit `quote_bid_qty_mismatch`.
- Item 580 (306.9 vs 884.25) becomes an explicit finding, not just comparison data.
- Small addition to `rules.py`.

### Phase 5: Vendor PDF Table Adapter (2–3 missions)

**Goal:** Extract tabular data from typed/digital vendor quote PDFs.

- Use PyMuPDF (already a dependency) for text extraction.
- Implement generic table detection from text blocks.
- Apply existing alias dictionaries to detected headers.
- Confidence: MEDIUM.
- Test against IPSI QUOTE.pdf (available in raw_reference).

**Why delay until here:** This is Layer 2 work. The deterministic core
(Layer 1) must be stable and schema-formalized first. Starting PDF
extraction before the schema exists means the adapter has no stable
contract to target.

### Phase 6: DOT Proposal Form Adapter (2–3 missions)

**Goal:** Extract from Iowa DOT proposal form PDFs using template-anchored parsing.

- Define DOT form template (page regions, column positions).
- Extract into `NormalizedDocument` with MEDIUM-HIGH confidence.
- Test against Adel Blank Bid Document.pdf and Adel Missing subtotal and total.pdf.
- Handle handwritten cells: set confidence to LOW/UNVERIFIED, flag for review.

### Phase 7: Review Queue UI (frontend)

**Goal:** Let staff review and correct flagged extractions.

- Display review flags from `NormalizedDocument`.
- Allow field-level corrections.
- Persist corrections and re-run comparison.
- Out of scope for backend-focused missions.

### Phase 8: OCR/Scanned PDF Adapter (future)

**Goal:** Handle scanned documents.

- Add OCR dependency (Tesseract or cloud OCR service).
- All outputs at LOW/UNVERIFIED confidence.
- Heavy review queue dependency — should not be attempted before Phase 7.

### What to Delay

| Item | Reason |
|------|--------|
| OCR adapter | Requires review queue (Phase 7) to be useful |
| Frontend rework | Not blocking backend architecture |
| Multi-project support | Current single-project model is fine for MVP |
| Cloud deployment | Local-first development is appropriate |
| Live standards ingestion | Static alias tables are sufficient for now |

### How to Avoid Contaminating the Deterministic Core

1. **Schema boundary:** Adapters produce `NormalizedDocument`. The comparison
   engine consumes `CanonicalLineItem`. The schema IS the firewall.

2. **Confidence gate:** Any row with UNVERIFIED confidence is blocked before
   reaching comparison. The core never processes uncertain data.

3. **Separate modules:** Adapters live in `backend/app/adapters/`. The core
   lives in `backend/app/quote_reconciliation/rules.py`. They share only
   the schema — no imports from adapters into core.

4. **Test isolation:** Adapter tests use their own fixtures. Core tests use
   pre-normalized canonical fixtures. Neither depends on the other.

---

## Pipeline Diagram (Text)

```
                    ┌──────────────┐
                    │  Raw File    │
                    │  (PDF/XLSX/  │
                    │   CSV/scan)  │
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │   CLASSIFY   │
                    │  file type   │
                    │  + format    │
                    └──────┬───────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
        ┌─────▼────┐ ┌────▼─────┐ ┌────▼─────┐
        │Spreadshee│ │ PDF Form │ │ OCR/Scan │
        │t Adapter │ │ Adapter  │ │ Adapter  │
        │ (HIGH)   │ │(MED-HIGH)│ │  (LOW)   │
        └─────┬────┘ └────┬─────┘ └────┬─────┘
              │            │            │
              └────────────┼────────────┘
                           │
                    ┌──────▼───────┐
                    │ Normalized   │
                    │ Document     │
                    │ (canonical   │
                    │  schema)     │
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │ NORMALIZE    │
                    │ • unit canon │
                    │ • item codes │
                    │ • summary    │
                    │   row filter │
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │ RECON PREP   │
                    │ • line→DOT   │
                    │   mapping    │
                    │ • scope      │
                    │   classify   │
                    │ • confidence │
                    │   gate       │
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │  COMPARISON  │
                    │  ENGINE      │◄── protected core
                    │  (deterministic,
                    │   fail-closed)│
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │   OUTPUT     │
                    │ • findings   │
                    │ • review     │
                    │   flags      │
                    │ • audit log  │
                    └──────────────┘
```

---

## Recommendation for Next Implementation Mission

**Phase C-4: Schema Formalization**

Create `backend/app/models/canonical.py` with Pydantic models for
`NormalizedDocument`, `CanonicalLineItem`, `Confidence`, `TotalsBlock`,
`ReviewFlag`, and `FieldCorrection`. Wrap the existing structured
spreadsheet ingest to produce `NormalizedDocument` output alongside
the current `(rows, meta)` tuple (additive, not replacing). This gives
all future adapters and pipeline stages a stable typed contract to target.

Alternatively, if the user prefers faster visible progress over
architectural formalization:

**Phase C-4alt: Quote Summary Row Filter + Quantity Divergence Finding**

Two small surgical fixes that complete the Adel/IPSI reconciliation
story and produce cleaner output for the existing test pack. These
can be done in a single mission.

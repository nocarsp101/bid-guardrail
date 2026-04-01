# Adel / IPSI Canonical Test Pack — Truth Document

## Overview

This fixture set captures the Adel (Iowa DOT) prime bid and IPSI subcontractor
quote as a deterministic truth pack for the Bid Guardrail reconciliation engine.

## File Inventory

### structured/ (Layer 1 — deterministic tests)

| File | Role | Source |
|------|------|--------|
| bid_items.xlsx | Canonical structured prime bid truth | BidItems_FinalTab_values_only.xlsx |
| quote_lines.xlsx | Canonical structured quote truth | Unorganized data for IPSI.xlsx |
| bid_structural.csv | Clean structural control input (no prices) | ADEL CSV without Numbers filled in.csv |
| bid_truth.json | Machine-readable bid rows (93 items) | Derived from bid_items.xlsx |
| quote_truth.json | Machine-readable quote rows (14 items) | Derived from quote_lines.xlsx |
| line_to_item_mapping.json | Proposal line number -> DOT item number mapping | Derived from bid_structural.csv |

### raw_reference/ (Layer 2 — future extraction work)

| File | Role |
|------|------|
| NOTE.md | Pointers to PDF reference files (not in repo) |

## Truth vs Reference

- **Truth sources**: bid_items.xlsx, quote_lines.xlsx, and derived JSON files.
  These drive deterministic assertions.
- **Reference sources**: PDFs and the CSV are structural/visual references.
  The CSV provides the line-number-to-DOT-item mapping but is not the numeric
  truth source.

## Key Identifiers — Do Not Collapse

| Concept | Example | Used In |
|---------|---------|---------|
| Proposal line number | 520, 530, 580 | CSV "Line" column; IPSI quote "Bid Item #" |
| DOT item number | 2524-6765010 | Bid XLSX "ITEM NO."; CSV "Item" column |
| Vendor quote bid item | 520 | IPSI quote "Bid Item #" (these are line numbers, NOT DOT items) |

The IPSI quote references proposal line numbers, not DOT item numbers.
The CSV provides the authoritative mapping between these two identifier spaces.

## Partial Scope

The IPSI quote covers 14 of 93 bid items — a partial-scope subset.
Quoted proposal lines: 520, 530, 540, 550, 560, 570, 580, 600, 610, 620, 630, 650, 670, 690.

Unquoted bid items should NOT be treated as quote failures.

## Expected Reconciliation Mappings

| Quote Line | DOT Item | Description | Quote Qty | Bid Qty | Unit Match? | Price Comparison |
|------------|----------|-------------|-----------|---------|-------------|------------------|
| 520 | 2524-6765010 | Remove/Reinstall Sign | 1 EA | 1 EACH | EA!=EACH | 275 < 288.75 |
| 530 | 2524-6765016 | Remove/Reinstall Ref Loc Sign | 2 EA | 2 EACH | EA!=EACH | 275 < 288.75 |
| 540 | 2524-6765210 | Removal Type A Sign Assy | 9 EA | 9 EACH | EA!=EACH | 100 < 105 |
| 550 | 2524-9276010 | Perf Square Steel Tube Posts | 323.5 LF | 323.5 LF | match | 25 < 26.9 |
| 560 | 2524-9276021 | Steel Post Anchor, Soil | 19 EA | 19 EACH | EA!=EACH | 100 < 105 |
| 570 | 2524-9276024 | Steel Post Anchor, Conc | 13 EA | 13 EACH | EA!=EACH | 250 < 267.5 |
| 580 | 2524-9325001 | Type A Signs, Sheet Alum | 306.9 SF | 884.25 SF | match | 25 < 31.5 |
| 600 | 2527-9263217 | Painted Pav't Mark, Durable | 86 STA | 86 STA | match | 145 < 152.25 |
| 610 | 2527-9270112 | Grooves Cut Pav't Mark | 77.87 STA | 77.87 STA | match | 125 < 157.5 |
| 620 | 2528-2518000 | Safety Closure | 4 EA | 4 EACH | EA!=EACH | 100 < 105 |
| 630 | 2528-8400048 | Temp Barrier Rail, Conc | 1100 LF | 1100 LF | match | 14 < 14.7 |
| 650 | 2528-8445110 | Traffic Control | 1 LS | 1 LUMP SUM | LS!=LUMP SUM | 9975 < 10415 |
| 670 | 2528-9290050 | Portable DMS | 56 CDAY | 56 CDAY | match | 100 < 105 |
| 690 | 2551-0000110 | Temp Crash Cushion | 2 EA | 2 EACH | EA!=EACH | 1150 < 1207.5 |

## Known Issues Visible in This Dataset

1. **Quote uses line numbers, bid uses DOT items**: Reconciliation requires the
   CSV-derived mapping. Current system has no line-number-to-DOT-item bridge.

2. **Unit equivalence gap**: 8 of 14 items have EA vs EACH mismatch; 1 has
   LS vs LUMP SUM. Current system does exact string match — these will FAIL.

3. **Quantity divergence on item 580**: Quote qty=306.9, Bid qty=884.25 for
   DOT item 2524-9325001 (Type A Signs). This is a real discrepancy that
   should be surfaced as a finding.

4. **Quote header alias gap**: IPSI headers "Bid Item #" and "Per Unit" are not
   in the current alias dictionary. Quote ingest will fail before reconciliation.

5. **All quote unit prices are below bid unit prices**: No guardrail violation
   (quote_up > bid_up) expected for any correctly matched line.

## Quote Total

Raw quote total from IPSI: $78,513.75
Computed from structured data: sum of (qty * per_unit) for 14 rows = $78,513.75

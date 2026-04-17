"""
C26 — Real quote corpus coverage audit.

Deterministic, machine-readable coverage measurement for the quote lane.
Produces per-document and aggregate metrics so future rule work can be
guided by actual observed outcomes rather than speculation.

Two distinct audit modes:

    audit_quote_document(pdf_path, label=None)
        Runs the quote staging pipeline on a single PDF and measures:
          - document class, extraction source
          - accepted_rows_count / rejected_candidates_count
          - rows_with_line_ref / rows_with_qty / rows_with_unit
          - rows_with_unit_price / rows_with_amount
          - rows_enriched_qty_unit (E1 or E2)
          - rows_enriched_by_e1 / rows_enriched_by_e2
          - aggregated_rows_count (C24 promotions)
          - table_header_pages / table_header_page_count
          - rejection_reason_counts
          - dominant_limitation (structured, closed vocabulary)

    audit_paired_documents(quote_pdf, dot_pdf, label=None)
        Runs the full DOT → staging → pairing → mapping → reconciliation
        → classification → prioritization pipeline on a quote+DOT pair
        and measures downstream coverage:
          - everything in the quote-only audit, plus:
          - pairing_status
          - packet_status
          - reconciliation_status
          - rows_mapped / rows_unmapped / rows_ambiguous
          - rows_comparable / rows_non_comparable
          - rows_matched / rows_mismatched / rows_blocked
          - priority_counts by class
          - dominant_downstream_limitation (closed vocabulary)

Both modes never mutate any input. Both return stable dicts with the
same top-level shape: `audit_mode`, `label`, `metrics`, `document_diagnostics`.

Corpus aggregation:

    audit_corpus(runs)
        Consumes a list of audit dicts from the functions above and
        produces an aggregate summary, a per-document summary, a gap
        summary, and coverage ratios. Aggregation keeps quote-only and
        paired metrics distinct so they are never silently averaged
        together.

Hard rules:
    - No metric depends on guessed values. Every count is observable
      directly from the governed pipeline output.
    - Dominant-limitation classification uses a closed vocabulary.
    - Aggregate counts are sums; ratios are computed as num/den with
      den reported explicitly so downstream callers can inspect both.
    - Never hides failures. "failed" documents contribute their counts
      to the gap_summary.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

COVERAGE_AUDIT_VERSION = "coverage_audit/v1"

AUDIT_MODE_QUOTE_ONLY = "quote_only"
AUDIT_MODE_PAIRED = "paired"

# ---------------------------------------------------------------------------
# Dominant-limitation vocabulary (closed set).
# ---------------------------------------------------------------------------

LIM_UNKNOWN_DOCUMENT = "unknown_document_class"
LIM_NO_QUOTE_ROWS = "no_quote_rows_detected"
LIM_NO_ENRICHMENT_SIGNAL = "no_qty_unit_tokens_in_source"
LIM_NO_TABLE_HEADER = "no_table_header_detected"
LIM_NO_BLOCKS = "no_multi_line_blocks"
LIM_STRUCTURAL_COVERAGE_OK = "structural_coverage_ok"

# Downstream-limitation vocabulary (closed set).
D_LIM_BLOCKED_BY_PAIRING = "blocked_by_pairing"
D_LIM_ALL_NON_COMPARABLE = "all_mapped_rows_non_comparable"
D_LIM_MAJORITY_UNMAPPED = "majority_unmapped"
D_LIM_MAJORITY_AMBIGUOUS = "majority_ambiguous"
D_LIM_PARTIAL_COMPARABILITY = "partial_comparability"
D_LIM_FULLY_COMPARABLE = "fully_comparable"
D_LIM_NO_ACCEPTED_ROWS = "no_accepted_rows"


# ---------------------------------------------------------------------------
# Quote-only audit
# ---------------------------------------------------------------------------

def audit_quote_document(
    pdf_path: str,
    label: Optional[str] = None,
) -> Dict[str, Any]:
    """Run the quote staging pipeline and return a coverage audit dict."""
    # Imported lazily so the module remains usable in fast test contexts
    # that exercise aggregation without touching the parser.
    from .quote_normalization import normalize_quote_from_pdf
    from .extractor import ExtractionError

    try:
        staging = normalize_quote_from_pdf(pdf_path)
    except ExtractionError as e:
        meta = e.meta or {}
        return _empty_quote_audit(
            label=label or pdf_path,
            failure_reason=meta.get("failure_reason"),
            doc_class=meta.get("document_class_detected", "unknown"),
        )

    metrics = _measure_quote_only(staging)
    metrics["dominant_limitation"] = _classify_quote_limitation(staging, metrics)

    return {
        "audit_mode": AUDIT_MODE_QUOTE_ONLY,
        "coverage_audit_version": COVERAGE_AUDIT_VERSION,
        "label": label or pdf_path,
        "metrics": metrics,
        "document_diagnostics": staging.get("document_diagnostics") or {},
    }


def _measure_quote_only(staging: Dict[str, Any]) -> Dict[str, Any]:
    accepted = staging.get("accepted_rows") or []
    rejected = staging.get("rejected_candidates") or []
    diag = staging.get("document_diagnostics") or {}
    table_meta = diag.get("table_metadata") or {}
    agg_meta = diag.get("aggregation_meta") or {}

    rows_with_line_ref = sum(1 for r in accepted if r.get("line_ref"))
    rows_with_qty = sum(1 for r in accepted if r.get("qty") is not None)
    rows_with_unit = sum(1 for r in accepted if r.get("unit"))
    rows_with_unit_price = sum(1 for r in accepted if r.get("unit_price") is not None)
    rows_with_amount = sum(1 for r in accepted if r.get("amount") is not None)

    rows_enriched_e1 = 0
    rows_enriched_e2 = 0
    for r in accepted:
        src = (r.get("field_sources") or {}).get("qty")
        if src == "explicit_inline_qty_unit":
            rows_enriched_e1 += 1
        elif src == "explicit_table_header_qty":
            rows_enriched_e2 += 1
    rows_enriched_qty_unit = rows_enriched_e1 + rows_enriched_e2

    # C24 aggregation: promoted rows carry row_id >= 100_000.
    aggregated_rows_count = sum(1 for r in accepted if (r.get("row_id") or 0) >= 100_000)

    header_pages = [p for p, md in table_meta.items() if md.get("header_detected")]

    rejection_reason_counts: Dict[str, int] = {}
    for cand in rejected:
        reason = cand.get("rejection_reason") or "unknown"
        rejection_reason_counts[reason] = rejection_reason_counts.get(reason, 0) + 1

    return {
        "document_class_detected": staging.get("document_class_detected"),
        "extraction_source": staging.get("extraction_source"),
        "staging_status": diag.get("status"),
        "failure_reason": diag.get("failure_reason"),
        "accepted_rows_count": len(accepted),
        "rejected_candidates_count": len(rejected),
        "rows_with_line_ref": rows_with_line_ref,
        "rows_with_qty": rows_with_qty,
        "rows_with_unit": rows_with_unit,
        "rows_with_unit_price": rows_with_unit_price,
        "rows_with_amount": rows_with_amount,
        "rows_enriched_qty_unit": rows_enriched_qty_unit,
        "rows_enriched_by_e1": rows_enriched_e1,
        "rows_enriched_by_e2": rows_enriched_e2,
        "aggregated_rows_count": aggregated_rows_count,
        "table_header_pages": sorted(header_pages),
        "table_header_page_count": len(header_pages),
        "blocks_attempted": int(agg_meta.get("blocks_attempted", 0)),
        "blocks_promoted": int(agg_meta.get("blocks_promoted", 0)),
        "rejection_reason_counts": rejection_reason_counts,
    }


def _classify_quote_limitation(
    staging: Dict[str, Any],
    metrics: Dict[str, Any],
) -> str:
    doc_class = metrics.get("document_class_detected")
    accepted_count = metrics.get("accepted_rows_count", 0)

    if doc_class != "quote":
        return LIM_UNKNOWN_DOCUMENT
    if accepted_count == 0:
        return LIM_NO_QUOTE_ROWS

    # Accepted rows exist. Decide where the biggest structural gap is.
    rows_qty = metrics.get("rows_with_qty", 0)
    rows_unit = metrics.get("rows_with_unit", 0)
    header_pages = metrics.get("table_header_page_count", 0)
    blocks_attempted = metrics.get("blocks_attempted", 0)

    if rows_qty == 0 and rows_unit == 0:
        return LIM_NO_ENRICHMENT_SIGNAL
    if header_pages == 0 and rows_qty < accepted_count:
        return LIM_NO_TABLE_HEADER
    if blocks_attempted == 0 and rows_qty < accepted_count:
        return LIM_NO_BLOCKS
    return LIM_STRUCTURAL_COVERAGE_OK


def _empty_quote_audit(
    label: str,
    failure_reason: Optional[str],
    doc_class: str,
) -> Dict[str, Any]:
    return {
        "audit_mode": AUDIT_MODE_QUOTE_ONLY,
        "coverage_audit_version": COVERAGE_AUDIT_VERSION,
        "label": label,
        "metrics": {
            "document_class_detected": doc_class,
            "extraction_source": None,
            "staging_status": "extraction_failed",
            "failure_reason": failure_reason,
            "accepted_rows_count": 0,
            "rejected_candidates_count": 0,
            "rows_with_line_ref": 0,
            "rows_with_qty": 0,
            "rows_with_unit": 0,
            "rows_with_unit_price": 0,
            "rows_with_amount": 0,
            "rows_enriched_qty_unit": 0,
            "rows_enriched_by_e1": 0,
            "rows_enriched_by_e2": 0,
            "aggregated_rows_count": 0,
            "table_header_pages": [],
            "table_header_page_count": 0,
            "blocks_attempted": 0,
            "blocks_promoted": 0,
            "rejection_reason_counts": {},
            "dominant_limitation": (
                LIM_UNKNOWN_DOCUMENT if doc_class != "quote" else LIM_NO_QUOTE_ROWS
            ),
        },
        "document_diagnostics": {},
    }


# ---------------------------------------------------------------------------
# Paired audit
# ---------------------------------------------------------------------------

def audit_paired_documents(
    quote_pdf: str,
    dot_pdf: str,
    label: Optional[str] = None,
) -> Dict[str, Any]:
    """Run the full governed pipeline on a quote+DOT pair and measure."""
    from .service import extract_bid_items_from_pdf
    from .quote_normalization import normalize_quote_from_pdf
    from .pairing_guardrails import analyze_pairing
    from .quote_to_bid_mapping import map_quote_to_bid
    from .review_packet import build_review_packet
    from .reconciliation_foundation import reconcile_packet
    from .reconciliation_contract import build_reconciliation_contract
    from .discrepancy_classification import classify_contract
    from .review_prioritization import prioritize_classified_contract
    from .extractor import ExtractionError

    effective_label = label or f"{quote_pdf} | {dot_pdf}"

    try:
        bid_rows, bid_summary = extract_bid_items_from_pdf(dot_pdf)
    except ExtractionError as e:
        return _empty_paired_audit(effective_label, "dot_extraction_failed",
                                   (e.meta or {}).get("failure_reason"))

    try:
        staging = normalize_quote_from_pdf(quote_pdf)
    except ExtractionError as e:
        return _empty_paired_audit(effective_label, "quote_staging_failed",
                                   (e.meta or {}).get("failure_reason"))

    quote_metrics = _measure_quote_only(staging)
    quote_metrics["dominant_limitation"] = _classify_quote_limitation(staging, quote_metrics)

    accepted_rows = staging.get("accepted_rows") or []

    if not accepted_rows:
        return {
            "audit_mode": AUDIT_MODE_PAIRED,
            "coverage_audit_version": COVERAGE_AUDIT_VERSION,
            "label": effective_label,
            "metrics": {
                **quote_metrics,
                "pairing_status": None,
                "packet_status": None,
                "reconciliation_status": None,
                "rows_mapped": 0,
                "rows_unmapped": 0,
                "rows_ambiguous": 0,
                "rows_comparable": 0,
                "rows_non_comparable": 0,
                "rows_matched": 0,
                "rows_mismatched": 0,
                "rows_blocked": 0,
                "priority_counts": {},
                "dominant_downstream_limitation": D_LIM_NO_ACCEPTED_ROWS,
            },
            "document_diagnostics": staging.get("document_diagnostics") or {},
        }

    pairing = analyze_pairing(accepted_rows, bid_rows)
    if not pairing.get("allow_mapping"):
        packet = build_review_packet(
            pairing_diagnostics=pairing, mapping_result=None,
            accepted_rows=accepted_rows,
            quote_diagnostics=staging.get("document_diagnostics") or {},
            bid_summary={"rows_extracted": bid_summary.get("rows_extracted")},
        )
        mapping = None
    else:
        mapping = map_quote_to_bid(accepted_rows, bid_rows)
        packet = build_review_packet(
            pairing_diagnostics=pairing, mapping_result=mapping,
            accepted_rows=accepted_rows,
            quote_diagnostics=staging.get("document_diagnostics") or {},
            bid_summary={"rows_extracted": bid_summary.get("rows_extracted")},
        )

    recon = reconcile_packet(packet)
    contract = build_reconciliation_contract(recon, packet)
    classified = classify_contract(contract)
    prioritized = prioritize_classified_contract(classified)

    mapping_summary = packet.get("mapping_summary") or {}
    recon_summary = prioritized.get("reconciliation_summary") or {}
    priority_summary = prioritized.get("priority_summary") or {}

    metrics = dict(quote_metrics)
    metrics.update({
        "pairing_status": pairing.get("pairing_status"),
        "packet_status": packet.get("packet_status"),
        "reconciliation_status": prioritized.get("reconciliation_status"),
        "rows_mapped": mapping_summary.get("mapped_count", 0),
        "rows_unmapped": mapping_summary.get("unmapped_count", 0),
        "rows_ambiguous": mapping_summary.get("ambiguous_count", 0),
        "rows_comparable": recon_summary.get("rows_compared", 0),
        "rows_non_comparable": recon_summary.get("rows_non_comparable", 0),
        "rows_matched": recon_summary.get("rows_matched", 0),
        "rows_mismatched": recon_summary.get("rows_mismatched", 0),
        "rows_blocked": recon_summary.get("rows_blocked", 0),
        "priority_counts": {
            "critical": priority_summary.get("critical_count", 0),
            "high": priority_summary.get("high_count", 0),
            "medium": priority_summary.get("medium_count", 0),
            "low": priority_summary.get("low_count", 0),
            "informational": priority_summary.get("informational_count", 0),
        },
    })
    metrics["dominant_downstream_limitation"] = _classify_downstream_limitation(metrics)

    return {
        "audit_mode": AUDIT_MODE_PAIRED,
        "coverage_audit_version": COVERAGE_AUDIT_VERSION,
        "label": effective_label,
        "metrics": metrics,
        "document_diagnostics": staging.get("document_diagnostics") or {},
    }


def _classify_downstream_limitation(metrics: Dict[str, Any]) -> str:
    packet_status = metrics.get("packet_status")
    if packet_status == "blocked":
        return D_LIM_BLOCKED_BY_PAIRING

    mapped = metrics.get("rows_mapped", 0)
    unmapped = metrics.get("rows_unmapped", 0)
    ambiguous = metrics.get("rows_ambiguous", 0)
    non_comparable = metrics.get("rows_non_comparable", 0)
    comparable = metrics.get("rows_comparable", 0)
    total_mapped_like = mapped + unmapped + ambiguous

    if total_mapped_like == 0:
        return D_LIM_NO_ACCEPTED_ROWS
    if unmapped > ambiguous and unmapped > mapped:
        return D_LIM_MAJORITY_UNMAPPED
    if ambiguous > unmapped and ambiguous > mapped:
        return D_LIM_MAJORITY_AMBIGUOUS
    if mapped > 0 and comparable == 0 and non_comparable >= mapped:
        return D_LIM_ALL_NON_COMPARABLE
    if comparable > 0 and non_comparable > 0:
        return D_LIM_PARTIAL_COMPARABILITY
    if comparable > 0 and non_comparable == 0:
        return D_LIM_FULLY_COMPARABLE
    return D_LIM_PARTIAL_COMPARABILITY


def _empty_paired_audit(label: str, stage: str, failure_reason: Optional[str]) -> Dict[str, Any]:
    return {
        "audit_mode": AUDIT_MODE_PAIRED,
        "coverage_audit_version": COVERAGE_AUDIT_VERSION,
        "label": label,
        "metrics": {
            "document_class_detected": None,
            "extraction_source": None,
            "staging_status": stage,
            "failure_reason": failure_reason,
            "accepted_rows_count": 0,
            "rejected_candidates_count": 0,
            "rows_with_line_ref": 0,
            "rows_with_qty": 0,
            "rows_with_unit": 0,
            "rows_with_unit_price": 0,
            "rows_with_amount": 0,
            "rows_enriched_qty_unit": 0,
            "rows_enriched_by_e1": 0,
            "rows_enriched_by_e2": 0,
            "aggregated_rows_count": 0,
            "table_header_pages": [],
            "table_header_page_count": 0,
            "blocks_attempted": 0,
            "blocks_promoted": 0,
            "rejection_reason_counts": {},
            "dominant_limitation": LIM_NO_QUOTE_ROWS,
            "pairing_status": None,
            "packet_status": None,
            "reconciliation_status": None,
            "rows_mapped": 0,
            "rows_unmapped": 0,
            "rows_ambiguous": 0,
            "rows_comparable": 0,
            "rows_non_comparable": 0,
            "rows_matched": 0,
            "rows_mismatched": 0,
            "rows_blocked": 0,
            "priority_counts": {},
            "dominant_downstream_limitation": D_LIM_NO_ACCEPTED_ROWS,
        },
        "document_diagnostics": {},
    }


# ---------------------------------------------------------------------------
# Corpus aggregation
# ---------------------------------------------------------------------------

def audit_corpus(runs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Aggregate a list of audit runs into a stable corpus summary.

    Quote-only and paired runs are counted separately so the aggregates
    never average across incompatible measurement shapes.
    """
    quote_only_runs = [r for r in runs if r.get("audit_mode") == AUDIT_MODE_QUOTE_ONLY]
    paired_runs = [r for r in runs if r.get("audit_mode") == AUDIT_MODE_PAIRED]

    def _sum(metric: str, runs_: List[Dict[str, Any]]) -> int:
        return sum(int((r.get("metrics") or {}).get(metric, 0) or 0) for r in runs_)

    def _limitation_histogram(runs_: List[Dict[str, Any]], key: str) -> Dict[str, int]:
        out: Dict[str, int] = {}
        for r in runs_:
            val = (r.get("metrics") or {}).get(key)
            if val is None:
                continue
            out[val] = out.get(val, 0) + 1
        return dict(sorted(out.items()))

    quote_summary = {
        "document_count": len(quote_only_runs),
        "accepted_rows_total": _sum("accepted_rows_count", quote_only_runs),
        "rows_enriched_qty_unit_total": _sum("rows_enriched_qty_unit", quote_only_runs),
        "rows_enriched_by_e1_total": _sum("rows_enriched_by_e1", quote_only_runs),
        "rows_enriched_by_e2_total": _sum("rows_enriched_by_e2", quote_only_runs),
        "rows_with_unit_price_total": _sum("rows_with_unit_price", quote_only_runs),
        "rows_with_amount_total": _sum("rows_with_amount", quote_only_runs),
        "aggregated_rows_total": _sum("aggregated_rows_count", quote_only_runs),
        "documents_with_table_header": sum(
            1 for r in quote_only_runs
            if ((r.get("metrics") or {}).get("table_header_page_count", 0) or 0) > 0
        ),
        "limitation_histogram": _limitation_histogram(quote_only_runs, "dominant_limitation"),
    }

    paired_summary = {
        "document_count": len(paired_runs),
        "rows_mapped_total": _sum("rows_mapped", paired_runs),
        "rows_unmapped_total": _sum("rows_unmapped", paired_runs),
        "rows_ambiguous_total": _sum("rows_ambiguous", paired_runs),
        "rows_comparable_total": _sum("rows_comparable", paired_runs),
        "rows_non_comparable_total": _sum("rows_non_comparable", paired_runs),
        "rows_matched_total": _sum("rows_matched", paired_runs),
        "rows_mismatched_total": _sum("rows_mismatched", paired_runs),
        "rows_blocked_total": _sum("rows_blocked", paired_runs),
        "pairing_status_histogram": _limitation_histogram(paired_runs, "pairing_status"),
        "packet_status_histogram": _limitation_histogram(paired_runs, "packet_status"),
        "downstream_limitation_histogram": _limitation_histogram(
            paired_runs, "dominant_downstream_limitation"
        ),
    }

    # Ratios — always report numerator + denominator.
    def _ratio(num_key: str, den_key: str, summary: Dict[str, Any]) -> Dict[str, Any]:
        num = summary.get(num_key, 0)
        den = summary.get(den_key, 0)
        return {
            "numerator": num,
            "denominator": den,
            "ratio": (num / den) if den else None,
        }

    quote_summary["ratios"] = {
        "rows_with_qty_per_accepted": _ratio_from_runs(
            quote_only_runs, "rows_with_qty", "accepted_rows_count"
        ),
        "rows_enriched_qty_unit_per_accepted": _ratio_from_runs(
            quote_only_runs, "rows_enriched_qty_unit", "accepted_rows_count"
        ),
    }
    paired_summary["ratios"] = {
        "rows_comparable_per_mapped": _ratio_from_runs(
            paired_runs, "rows_comparable", "rows_mapped"
        ),
        "rows_mapped_per_accepted": _ratio_from_runs(
            paired_runs, "rows_mapped", "accepted_rows_count"
        ),
    }

    gap_summary = _build_gap_summary(quote_only_runs, paired_runs)

    return {
        "coverage_audit_version": COVERAGE_AUDIT_VERSION,
        "run_count": len(runs),
        "quote_only_summary": quote_summary,
        "paired_summary": paired_summary,
        "documents": [
            {
                "label": r.get("label"),
                "audit_mode": r.get("audit_mode"),
                "metrics": r.get("metrics"),
            }
            for r in runs
        ],
        "gap_summary": gap_summary,
    }


def _ratio_from_runs(
    runs: List[Dict[str, Any]],
    num_key: str,
    den_key: str,
) -> Dict[str, Any]:
    num = sum(int((r.get("metrics") or {}).get(num_key, 0) or 0) for r in runs)
    den = sum(int((r.get("metrics") or {}).get(den_key, 0) or 0) for r in runs)
    return {
        "numerator": num,
        "denominator": den,
        "ratio": (num / den) if den else None,
    }


def _build_gap_summary(
    quote_only_runs: List[Dict[str, Any]],
    paired_runs: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Explicit coverage-gap indicators that downstream C27/C28 layers
    can consume to decide whether rule expansion or exception surfacing
    is justified."""
    documents_with_no_accepted_rows = sum(
        1 for r in (quote_only_runs + paired_runs)
        if ((r.get("metrics") or {}).get("accepted_rows_count", 0) or 0) == 0
    )
    documents_with_zero_enrichment = sum(
        1 for r in (quote_only_runs + paired_runs)
        if ((r.get("metrics") or {}).get("accepted_rows_count", 0) or 0) > 0
        and ((r.get("metrics") or {}).get("rows_enriched_qty_unit", 0) or 0) == 0
    )
    documents_with_no_header_detected = sum(
        1 for r in (quote_only_runs + paired_runs)
        if ((r.get("metrics") or {}).get("table_header_page_count", 0) or 0) == 0
    )
    paired_blocked = sum(
        1 for r in paired_runs
        if (r.get("metrics") or {}).get("packet_status") == "blocked"
    )

    return {
        "documents_with_no_accepted_rows": documents_with_no_accepted_rows,
        "documents_with_zero_enrichment": documents_with_zero_enrichment,
        "documents_with_no_header_detected": documents_with_no_header_detected,
        "paired_documents_blocked": paired_blocked,
    }

"""
C28 — Exception surfacing + feedback loop.

Turns pipeline outcomes (coverage audit runs, prioritized findings
packets, workflow packets) into a deterministic, templated exception
summary that tells the office and future rule work:

    - which extraction/enrichment gaps dominate
    - which mapping/reconciliation gaps dominate
    - which pairing limitations recur
    - which review-workflow bottlenecks recur

The output is evidence-first, machine-readable, and purely templated.
No narrative. No speculation. Every statement is a fixed template
populated with real counts from the inputs.

Exception category vocabulary (closed set):

    Extraction / enrichment:
        E_NO_QUOTE_ROWS_DETECTED
        E_UNKNOWN_DOCUMENT_CLASS
        E_NO_TABLE_HEADER_DETECTED
        E_NO_INLINE_QTY_UNIT_DETECTED
        E_NO_MULTI_ROW_GROUP_CANDIDATES
        E_LOW_ENRICHMENT_COVERAGE

    Mapping:
        M_UNMAPPED_AFTER_TRUSTED_PAIRING
        M_AMBIGUOUS_MAPPING_DETECTED

    Pairing:
        P_BLOCKED_BY_PAIRING

    Reconciliation:
        R_ROWS_NON_COMPARABLE_MISSING_QUOTE_FIELDS
        R_ROWS_NON_COMPARABLE_MISSING_BID_FIELDS
        R_LOW_COMPARABILITY_COVERAGE

    Review workflow:
        W_REVIEW_QUEUE_CONCENTRATED_IN_HIGH_PRIORITY
        W_REVIEW_QUEUE_BACKLOG_UNTOUCHED

Categories are never merged across layers. Extraction gaps and review
bottlenecks are listed in separate top-level buckets.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Iterable, List, Optional, Tuple

EXCEPTION_FEEDBACK_VERSION = "exception_feedback/v1"

# Thresholds for low-coverage exceptions. Closed constants so behavior
# is deterministic and test-traceable.
_LOW_ENRICHMENT_THRESHOLD = 0.10   # < 10% enrichment rate
_LOW_COMPARABILITY_THRESHOLD = 0.10  # < 10% comparability-per-mapped rate
_HIGH_PRIORITY_CONCENTRATION = 0.70  # ≥ 70% of queue is critical+high


# ---------------------------------------------------------------------------
# Exception category definitions
# ---------------------------------------------------------------------------

# Category -> (bucket, human-readable label — used ONLY for templated text)
_CATEGORIES: Dict[str, Tuple[str, str]] = {
    # extraction
    "E_NO_QUOTE_ROWS_DETECTED": ("extraction", "No quote rows detected"),
    "E_UNKNOWN_DOCUMENT_CLASS": ("extraction", "Document classified as unknown"),
    "E_NO_TABLE_HEADER_DETECTED": ("extraction", "No table header detected"),
    "E_NO_INLINE_QTY_UNIT_DETECTED": ("extraction", "No inline qty/unit tokens detected"),
    "E_NO_MULTI_ROW_GROUP_CANDIDATES": ("extraction", "No multi-row block candidates"),
    "E_LOW_ENRICHMENT_COVERAGE": ("extraction", "Enrichment coverage below threshold"),
    # mapping
    "M_UNMAPPED_AFTER_TRUSTED_PAIRING": ("mapping", "Rows unmapped after trusted pairing"),
    "M_AMBIGUOUS_MAPPING_DETECTED": ("mapping", "Ambiguous mapping candidates"),
    # pairing
    "P_BLOCKED_BY_PAIRING": ("pairing", "Pairing rejected; packet blocked"),
    # reconciliation
    "R_ROWS_NON_COMPARABLE_MISSING_QUOTE_FIELDS": (
        "reconciliation", "Rows non-comparable due to missing quote fields"),
    "R_ROWS_NON_COMPARABLE_MISSING_BID_FIELDS": (
        "reconciliation", "Rows non-comparable due to missing bid fields"),
    "R_LOW_COMPARABILITY_COVERAGE": (
        "reconciliation", "Comparability rate below threshold"),
    # review workflow
    "W_REVIEW_QUEUE_CONCENTRATED_IN_HIGH_PRIORITY": (
        "review_workflow", "Review queue concentrated in critical+high priority"),
    "W_REVIEW_QUEUE_BACKLOG_UNTOUCHED": (
        "review_workflow", "Review queue entirely unreviewed"),
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def surface_exceptions(
    audit_runs: Optional[List[Dict[str, Any]]] = None,
    findings_packets: Optional[List[Dict[str, Any]]] = None,
    workflow_packets: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Build a deterministic exception-surfacing summary from governed
    pipeline outputs. Every input list is optional; missing inputs
    simply contribute zero counts.

    Returns a dict shaped:

        {
            "exception_feedback_version": "...",
            "inputs": {
                "audit_runs": n,
                "findings_packets": n,
                "workflow_packets": n,
            },
            "exception_counts": { "<CATEGORY>": int, ... },
            "top_failure_patterns": [ ... ranked dict entries ... ],
            "top_review_bottlenecks": [ ... ranked dict entries ... ],
            "document_exception_map": [ { "label": ..., "exceptions": [...] }, ... ],
            "queue_pressure_summary": { ... },
            "templated_statements": [ ... fixed-template strings ... ],
        }
    """
    audit_runs = audit_runs or []
    findings_packets = findings_packets or []
    workflow_packets = workflow_packets or []

    doc_entries: List[Dict[str, Any]] = []
    counts: Dict[str, int] = {cat: 0 for cat in _CATEGORIES}

    # ---- Audit-run exception classification ----
    for run in audit_runs:
        excs = _classify_audit_run(run)
        for cat in excs:
            counts[cat] += 1
        doc_entries.append({
            "label": run.get("label"),
            "audit_mode": run.get("audit_mode"),
            "exceptions": excs,
        })

    # ---- Findings-packet exception classification ----
    for fp in findings_packets:
        excs = _classify_findings_packet(fp)
        for cat in excs:
            counts[cat] += 1
        doc_entries.append({
            "label": (fp.get("label") or fp.get("packet_version") or "findings_packet"),
            "audit_mode": "findings_packet",
            "exceptions": excs,
        })

    # ---- Workflow-packet classification + queue pressure ----
    queue_pressure = _empty_queue_pressure()
    for wp in workflow_packets:
        excs, pressure = _classify_workflow_packet(wp)
        for cat in excs:
            counts[cat] += 1
        doc_entries.append({
            "label": wp.get("label") or "workflow_packet",
            "audit_mode": "workflow_packet",
            "exceptions": excs,
        })
        _merge_queue_pressure(queue_pressure, pressure)

    templated = _build_templated_statements(counts, queue_pressure)
    top_failures = _rank_top([
        cat for cat, meta in _CATEGORIES.items() if meta[0] != "review_workflow"
    ], counts)
    top_bottlenecks = _rank_top([
        cat for cat, meta in _CATEGORIES.items() if meta[0] == "review_workflow"
    ], counts)

    return {
        "exception_feedback_version": EXCEPTION_FEEDBACK_VERSION,
        "inputs": {
            "audit_runs": len(audit_runs),
            "findings_packets": len(findings_packets),
            "workflow_packets": len(workflow_packets),
        },
        "exception_counts": counts,
        "top_failure_patterns": top_failures,
        "top_review_bottlenecks": top_bottlenecks,
        "document_exception_map": doc_entries,
        "queue_pressure_summary": queue_pressure,
        "templated_statements": templated,
    }


# ---------------------------------------------------------------------------
# Audit-run classification
# ---------------------------------------------------------------------------

def _classify_audit_run(run: Dict[str, Any]) -> List[str]:
    m = run.get("metrics") or {}
    excs: List[str] = []

    doc_class = m.get("document_class_detected")
    accepted = int(m.get("accepted_rows_count") or 0)

    if doc_class == "unknown":
        excs.append("E_UNKNOWN_DOCUMENT_CLASS")
    if accepted == 0 and doc_class != "unknown":
        excs.append("E_NO_QUOTE_ROWS_DETECTED")

    if accepted > 0:
        header_pages = int(m.get("table_header_page_count") or 0)
        if header_pages == 0:
            excs.append("E_NO_TABLE_HEADER_DETECTED")
        enriched = int(m.get("rows_enriched_qty_unit") or 0)
        if enriched == 0:
            excs.append("E_NO_INLINE_QTY_UNIT_DETECTED")
        else:
            if enriched / accepted < _LOW_ENRICHMENT_THRESHOLD:
                excs.append("E_LOW_ENRICHMENT_COVERAGE")
        blocks_attempted = int(m.get("blocks_attempted") or 0)
        if blocks_attempted == 0:
            excs.append("E_NO_MULTI_ROW_GROUP_CANDIDATES")

    # Paired-audit specifics — downstream buckets.
    if run.get("audit_mode") == "paired":
        pairing_status = m.get("pairing_status")
        if pairing_status == "rejected" or m.get("packet_status") == "blocked":
            excs.append("P_BLOCKED_BY_PAIRING")
            return excs  # pairing blocked supersedes mapping/reconciliation buckets

        mapped = int(m.get("rows_mapped") or 0)
        unmapped = int(m.get("rows_unmapped") or 0)
        ambiguous = int(m.get("rows_ambiguous") or 0)
        comparable = int(m.get("rows_comparable") or 0)
        non_comparable = int(m.get("rows_non_comparable") or 0)

        if pairing_status == "trusted" and unmapped > 0:
            excs.append("M_UNMAPPED_AFTER_TRUSTED_PAIRING")
        if ambiguous > 0:
            excs.append("M_AMBIGUOUS_MAPPING_DETECTED")

        if mapped > 0 and comparable == 0 and non_comparable > 0:
            excs.append("R_ROWS_NON_COMPARABLE_MISSING_QUOTE_FIELDS")
        elif mapped > 0 and comparable / mapped < _LOW_COMPARABILITY_THRESHOLD:
            excs.append("R_LOW_COMPARABILITY_COVERAGE")

    return excs


# ---------------------------------------------------------------------------
# Findings-packet classification
# ---------------------------------------------------------------------------

def _classify_findings_packet(fp: Dict[str, Any]) -> List[str]:
    excs: List[str] = []
    if fp.get("packet_status") == "blocked":
        excs.append("P_BLOCKED_BY_PAIRING")
        return excs

    discrepancy_summary = fp.get("discrepancy_summary") or {}
    rows_total = int(discrepancy_summary.get("rows_total") or 0)
    unmapped = int(discrepancy_summary.get("unmapped_count") or 0)
    ambiguous = int(discrepancy_summary.get("ambiguous_count") or 0)
    missing_quote = int(discrepancy_summary.get("missing_quote_info_count") or 0)
    missing_bid = int(discrepancy_summary.get("missing_bid_info_count") or 0)

    if unmapped > 0:
        excs.append("M_UNMAPPED_AFTER_TRUSTED_PAIRING")
    if ambiguous > 0:
        excs.append("M_AMBIGUOUS_MAPPING_DETECTED")
    if missing_quote > 0:
        excs.append("R_ROWS_NON_COMPARABLE_MISSING_QUOTE_FIELDS")
    if missing_bid > 0:
        excs.append("R_ROWS_NON_COMPARABLE_MISSING_BID_FIELDS")

    # Low comparability: if zero rows were classified comparable_match but
    # rows exist, low comparability coverage fires.
    match_count = int(discrepancy_summary.get("comparable_match_count") or 0)
    if rows_total > 0 and match_count == 0:
        excs.append("R_LOW_COMPARABILITY_COVERAGE")

    return excs


# ---------------------------------------------------------------------------
# Workflow-packet classification + queue pressure
# ---------------------------------------------------------------------------

def _classify_workflow_packet(wp: Dict[str, Any]) -> Tuple[List[str], Dict[str, Any]]:
    excs: List[str] = []
    summary = wp.get("queue_summary") or {}
    total = int(summary.get("rows_total") or 0)
    critical_open = int(summary.get("critical_open") or 0)
    high_open = int(summary.get("high_open") or 0)
    medium_open = int(summary.get("medium_open") or 0)
    low_open = int(summary.get("low_open") or 0)
    unreviewed = int(summary.get("rows_unreviewed") or 0)

    if total > 0:
        high_concentration = (critical_open + high_open) / total
        if high_concentration >= _HIGH_PRIORITY_CONCENTRATION:
            excs.append("W_REVIEW_QUEUE_CONCENTRATED_IN_HIGH_PRIORITY")
        if unreviewed == total and total > 0:
            excs.append("W_REVIEW_QUEUE_BACKLOG_UNTOUCHED")

    if wp.get("packet_status") == "blocked":
        excs.append("P_BLOCKED_BY_PAIRING")

    pressure = {
        "rows_total": total,
        "critical_open": critical_open,
        "high_open": high_open,
        "medium_open": medium_open,
        "low_open": low_open,
        "unreviewed": unreviewed,
    }
    return excs, pressure


# ---------------------------------------------------------------------------
# Aggregation helpers
# ---------------------------------------------------------------------------

def _empty_queue_pressure() -> Dict[str, Any]:
    return {
        "workflow_packet_count": 0,
        "rows_total": 0,
        "critical_open": 0,
        "high_open": 0,
        "medium_open": 0,
        "low_open": 0,
        "unreviewed": 0,
    }


def _merge_queue_pressure(acc: Dict[str, Any], one: Dict[str, Any]) -> None:
    acc["workflow_packet_count"] += 1
    for key in ("rows_total", "critical_open", "high_open", "medium_open",
                "low_open", "unreviewed"):
        acc[key] += int(one.get(key) or 0)


def _rank_top(categories: Iterable[str], counts: Dict[str, int]) -> List[Dict[str, Any]]:
    """Return categories with nonzero counts, sorted desc by count, then
    alpha by category name for deterministic tie-breaking."""
    ranked: List[Dict[str, Any]] = []
    for cat in categories:
        n = counts.get(cat, 0)
        if n > 0:
            bucket, label = _CATEGORIES[cat]
            ranked.append({
                "category": cat,
                "bucket": bucket,
                "label": label,
                "count": n,
            })
    ranked.sort(key=lambda e: (-e["count"], e["category"]))
    return ranked


def _build_templated_statements(
    counts: Dict[str, int],
    queue_pressure: Dict[str, Any],
) -> List[str]:
    """Deterministic templated statements populated with real counts.

    Every statement is a fixed format string. No dynamic sentence
    generation. Each statement is only emitted when its category has a
    nonzero count or queue_pressure reflects an actual non-empty queue.
    """
    out: List[str] = []

    def _emit(cat: str, template: str) -> None:
        n = counts.get(cat, 0)
        if n > 0:
            out.append(template.format(n=n))

    _emit("E_NO_QUOTE_ROWS_DETECTED",
          "{n} documents produced no quote rows.")
    _emit("E_UNKNOWN_DOCUMENT_CLASS",
          "{n} documents classified as unknown_document_class.")
    _emit("E_NO_TABLE_HEADER_DETECTED",
          "{n} documents had no explicit table header detected.")
    _emit("E_NO_INLINE_QTY_UNIT_DETECTED",
          "{n} documents had accepted rows but zero inline qty/unit tokens.")
    _emit("E_NO_MULTI_ROW_GROUP_CANDIDATES",
          "{n} documents had no multi-row block candidates.")
    _emit("E_LOW_ENRICHMENT_COVERAGE",
          "{n} documents had enrichment below threshold.")

    _emit("M_UNMAPPED_AFTER_TRUSTED_PAIRING",
          "{n} trusted-pair documents still carry unmapped rows.")
    _emit("M_AMBIGUOUS_MAPPING_DETECTED",
          "{n} documents had ambiguous mapping candidates.")

    _emit("P_BLOCKED_BY_PAIRING",
          "{n} documents are blocked by pairing.")

    _emit("R_ROWS_NON_COMPARABLE_MISSING_QUOTE_FIELDS",
          "{n} documents had mapped rows non-comparable due to missing quote fields.")
    _emit("R_ROWS_NON_COMPARABLE_MISSING_BID_FIELDS",
          "{n} documents had mapped rows non-comparable due to missing bid fields.")
    _emit("R_LOW_COMPARABILITY_COVERAGE",
          "{n} documents had comparability below threshold.")

    _emit("W_REVIEW_QUEUE_CONCENTRATED_IN_HIGH_PRIORITY",
          "{n} workflow packets have queues concentrated in critical+high priority.")
    _emit("W_REVIEW_QUEUE_BACKLOG_UNTOUCHED",
          "{n} workflow packets have fully unreviewed queues.")

    # Queue pressure aggregate.
    wp_count = queue_pressure.get("workflow_packet_count", 0)
    if wp_count > 0 and queue_pressure.get("rows_total", 0) > 0:
        out.append(
            "Across {wp} workflow packets: {c} critical, {h} high, "
            "{m} medium, {l} low rows open; {u} rows unreviewed of {t} total.".format(
                wp=wp_count,
                c=queue_pressure["critical_open"],
                h=queue_pressure["high_open"],
                m=queue_pressure["medium_open"],
                l=queue_pressure["low_open"],
                u=queue_pressure["unreviewed"],
                t=queue_pressure["rows_total"],
            )
        )

    return out

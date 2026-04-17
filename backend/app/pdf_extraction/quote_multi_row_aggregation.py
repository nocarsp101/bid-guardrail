"""
C24 — Deterministic multi-row aggregation.

Promotes explicit two-line quote block candidates (emitted by C11's
pass-2 block detector inside `quote_parser`) into accepted quote rows
when the resulting logical row is fully explicit and parseable as a
single C9 row.

The only accepted grouping shape is:

    line 1:  <line_ref>  <alpha-only description>          (no '$')
    line 2:  $unit_price  $amount                          (price-only)

Both lines must come from the same page. The block candidate carries
`raw_text = "line1\nline2"` with `candidate_type = "block"` and
`rejection_reason = "unstable_boundary"` (meaning pass-2 detected the
deterministic split-row boundary). Any other block shape remains a
rejected candidate — no heuristic grouping is ever performed.

Aggregation strictly reuses the C9 `_parse_quote_line` parser against a
reconstructed `"<line_ref> <description> $unit_price $amount"` string,
then validates with `_check_quote_row`. If either step fails, the block
stays in rejected_candidates with a refined reason.

Hard rules:
    - Never merges more than 2 lines.
    - Never merges across pages.
    - Never modifies the source lines.
    - Never overrides validator rules.
    - Aggregation trace preserves raw source fragments for audit.
    - Aggregated rows are assigned fresh deterministic row_ids with a
      stable prefix so they never collide with pass-1 row_ids.
    - Promoted rows go through the same C20+C23 enrichment pipeline the
      caller runs afterwards — this module produces a bare parsed-row
      dict, not a contract-level row.
"""
from __future__ import annotations

import re
from copy import deepcopy
from typing import Any, Dict, List, Tuple

from .quote_parser import (
    _parse_quote_line,
    _DOLLAR_RE,
    _PRICE_ONLY_LINE_RE,
    _FRAGMENT_LINE_REF_RE,
    CT_BLOCK,
    R_UNSTABLE,
    R_INSUFFICIENT_STRUCTURE,
)
from .quote_validator import _check_quote_row

AGGREGATOR_VERSION = "quote_multi_row_aggregation/v1"

# Aggregation outcomes (closed set).
AGG_APPLIED = "aggregated_two_line_row"
AGG_SKIPPED_NOT_BLOCK = "not_a_block_candidate"
AGG_SKIPPED_BAD_LINE_COUNT = "unsupported_line_count"
AGG_SKIPPED_BAD_LINE1 = "line1_not_line_ref_plus_description"
AGG_SKIPPED_BAD_LINE2 = "line2_not_price_only"
AGG_SKIPPED_PARSE_FAILED = "reconstructed_row_parse_failed"
AGG_SKIPPED_VALIDATION_FAILED = "reconstructed_row_validation_failed"

# New rejection reasons surfaced when an aggregation attempt fails.
R_INCOMPLETE_GROUP_FIELDS = "incomplete_group_fields"
R_CONFLICTING_GROUP_VALUES = "conflicting_group_values"
R_AMBIGUOUS_GROUP_STRUCTURE = "ambiguous_group_structure"


def aggregate_block_candidates(
    accepted_rows: List[Dict[str, Any]],
    rejected_candidates: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    """
    Inspect the rejected_candidates list for two-line block candidates
    and try to promote each one into an accepted row.

    Returns (new_accepted_rows, new_rejected_candidates, aggregation_meta).

    The returned lists are NEW lists; originals are never mutated.
    Aggregated rows are APPENDED to the existing accepted_rows. The
    remaining rejected list contains everything unchanged except that
    successfully promoted block candidates are removed.
    """
    out_accepted: List[Dict[str, Any]] = list(accepted_rows)
    out_rejected: List[Dict[str, Any]] = []

    attempted = 0
    promoted = 0
    # Start aggregated row_ids high enough to never collide with pass-1
    # ordinals. The prefix is `_deterministic`: we use 100_000 + index
    # so ordering is stable and distinct.
    agg_ordinal = 0

    for cand in rejected_candidates:
        if cand.get("candidate_type") != CT_BLOCK:
            out_rejected.append(deepcopy(cand))
            continue

        attempted += 1
        outcome = _try_aggregate_block(cand, 100_000 + agg_ordinal)

        if outcome["status"] == AGG_APPLIED:
            row = outcome["row"]
            row["_aggregation_trace"] = {
                "aggregator_version": AGGREGATOR_VERSION,
                "aggregation_rule": "two_line_lineref_desc_plus_price",
                "aggregation_status": AGG_APPLIED,
                "source_candidate_id": cand.get("candidate_id"),
                "source_fragments": outcome["fragments"],
                "grouped_line_count": 2,
            }
            # Promoted rows carry the original source_text as the joined
            # block so provenance remains visible downstream.
            row["source_text"] = cand.get("raw_text", "")
            out_accepted.append(row)
            promoted += 1
            agg_ordinal += 1
            continue

        # Aggregation failed → keep the block as a rejected candidate,
        # but refine the rejection reason to describe why aggregation
        # did not succeed.
        refined = deepcopy(cand)
        refined["aggregation_attempted"] = True
        refined["aggregation_status"] = outcome["status"]
        refined["aggregator_version"] = AGGREGATOR_VERSION
        refined_reason = _refine_rejection_reason(outcome["status"])
        if refined_reason is not None:
            refined["rejection_reason"] = refined_reason
        out_rejected.append(refined)

    meta = {
        "aggregator_version": AGGREGATOR_VERSION,
        "blocks_attempted": attempted,
        "blocks_promoted": promoted,
    }
    return out_accepted, out_rejected, meta


# ---------------------------------------------------------------------------
# Aggregation rule
# ---------------------------------------------------------------------------

def _try_aggregate_block(
    cand: Dict[str, Any],
    aggregated_row_id: int,
) -> Dict[str, Any]:
    """Attempt to aggregate a single block candidate. Returns an outcome dict
    with `status` and, when successful, `row` and `fragments`."""
    raw_text = cand.get("raw_text") or ""
    lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]

    if len(lines) != 2:
        return {"status": AGG_SKIPPED_BAD_LINE_COUNT, "grouped_line_count": len(lines)}

    line1, line2 = lines[0], lines[1]

    # Line 1 must match the C11 fragment pattern: `<line_ref> <alpha desc>`
    # with no inline dollar amount.
    m1 = _FRAGMENT_LINE_REF_RE.match(line1)
    if m1 is None or _DOLLAR_RE.search(line1):
        return {"status": AGG_SKIPPED_BAD_LINE1}

    line_ref = m1.group(1)
    description = m1.group(2).strip()
    if not description:
        return {"status": AGG_SKIPPED_BAD_LINE1}

    # Line 2 must be a price-only line (1 or 2 dollars, nothing else).
    if _PRICE_ONLY_LINE_RE.match(line2) is None:
        return {"status": AGG_SKIPPED_BAD_LINE2}

    dollars_on_line2 = _DOLLAR_RE.findall(line2)
    if len(dollars_on_line2) not in (1, 2):
        return {"status": AGG_SKIPPED_BAD_LINE2}

    # Reconstruct a single-line form the existing C9 parser can handle
    # and run it through `_parse_quote_line`. This is the same deterministic
    # parser used by pass 1; no new parsing behavior is introduced.
    reconstructed = f"{line_ref} {description} {line2}"
    page_idx = cand.get("source_page", 0)
    parsed, reason = _parse_quote_line(reconstructed, page_idx, aggregated_row_id)
    if parsed is None:
        return {"status": AGG_SKIPPED_PARSE_FAILED, "parse_reason": reason}

    # Validate the parsed row with the same validator used in pass 1.
    issues = _check_quote_row(parsed)
    if issues:
        return {"status": AGG_SKIPPED_VALIDATION_FAILED, "validation_issues": issues}

    return {
        "status": AGG_APPLIED,
        "row": parsed,
        "fragments": [line1, line2],
    }


def _refine_rejection_reason(status: str) -> str | None:
    """Map an aggregation outcome into a richer rejection reason."""
    if status == AGG_SKIPPED_BAD_LINE_COUNT:
        return R_AMBIGUOUS_GROUP_STRUCTURE
    if status == AGG_SKIPPED_BAD_LINE1:
        return R_AMBIGUOUS_GROUP_STRUCTURE
    if status == AGG_SKIPPED_BAD_LINE2:
        return R_INCOMPLETE_GROUP_FIELDS
    if status == AGG_SKIPPED_PARSE_FAILED:
        return R_INCOMPLETE_GROUP_FIELDS
    if status == AGG_SKIPPED_VALIDATION_FAILED:
        return R_CONFLICTING_GROUP_VALUES
    return None

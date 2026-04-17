# backend/app/pdf_extraction/quote_parser.py
"""
Deterministic quote row parser for subcontractor / vendor quote PDFs.

COMPLETELY SEPARATE from the DOT schedule parser.
Does NOT assume DOT item numbers, DOT proposal line numbers, or DOT layout.

Rules:
    - Identify candidate rows by explicit $X.XX patterns on a line.
    - Reject totals, subtotals, headers, and boilerplate lines explicitly —
      do NOT silently drop them. Every rejected candidate is preserved with
      raw_text + source_page + rejection_reason so downstream staging can
      surface evidence.
    - Reject rows whose numeric relationship is ambiguous (e.g., more than
      two dollar amounts on a line — we cannot deterministically decide
      which is unit_price and which is amount, and we do not guess).
    - Optional leading line reference (3-4 digit) may be preserved as
      line_ref. Never invented.
    - qty and unit are only populated if explicitly present. They are
      never inferred from context.

Fail-closed: if no candidate rows are found, raises ExtractionError with
an explicit `failure_reason` code. Rejected candidates are carried in the
error meta so staging can still surface them as evidence.
"""
from __future__ import annotations

import re
from typing import List, Dict, Any, Optional, Tuple

from .extractor import ExtractionError


# Explicit dollar amount: $1,234.56 — must have $ prefix and two decimals
_DOLLAR_RE = re.compile(r'\$([\d,]+\.\d{2})')

# Leading line reference: 3-4 digit number at start of line (e.g., "530", "0520")
_LINE_REF_RE = re.compile(r'^(\d{3,4})\s+')

# Subtotal pattern (checked BEFORE total so we can tag it distinctly)
_SUBTOTAL_RE = re.compile(r'\b(?:sub\s*total|subtotal)\b', re.IGNORECASE)

# Total / grand-total lines — never emitted as rows
_TOTAL_RE = re.compile(
    r'\b(?:total|grand\s+total)\b',
    re.IGNORECASE,
)

# Header / column-label lines — never emitted as rows
_HEADER_RE = re.compile(
    r'\b(?:description\s+unit|qty\s+unit|unit\s+price|bid\s+item|item\s+number|line\s+item)\b',
    re.IGNORECASE,
)

# Boilerplate / contact info — never emitted as rows
_SKIP_PATTERNS = [
    re.compile(r'^\s*$'),
    re.compile(
        r'^conditions|^notes?|^bond|^this quote|^lump sum|^mark\b|^cell\s*#|'
        r'^[\w.]+@[\w.]+|^fax|^phone|^email',
        re.IGNORECASE,
    ),
    re.compile(
        r'associated general|safer roads|telephone|\bfax\b|p\.o\.\s+box|'
        r'\bavenue\b|letting date|\bcounty\b|\bproject\b',
        re.IGNORECASE,
    ),
]


# ---------------------------------------------------------------------------
# Rejection reason taxonomy — also re-used by the normalization staging layer
# ---------------------------------------------------------------------------
REASON_NO_CANDIDATES = "no_candidate_quote_rows"
REASON_NOT_DETERMINISTIC = "quote_rows_not_deterministic"

R_TOTAL = "total_row"
R_SUBTOTAL = "subtotal_row"
R_HEADER = "header_row"
R_BOILERPLATE = "boilerplate_row"
R_AMBIGUOUS = "ambiguous_numeric"
R_MISSING_DESC = "missing_description"
R_NON_POSITIVE = "non_positive_numeric"
R_UNSTABLE = "unstable_boundary"
R_INSUFFICIENT_STRUCTURE = "insufficient_structure"

# candidate_type values
CT_LINE = "line"           # a single-line skip (total/header/boilerplate)
CT_ROW_LIKE = "row_like"   # had dollars and tried to become a row, but failed
CT_BLOCK = "block"         # multi-line row-fragment group preserved as evidence

# C11: line-ref + alphabetic description with NO inline dollar amount.
# Used for deterministic block grouping of split-line quote fragments.
_FRAGMENT_LINE_REF_RE = re.compile(r'^\s*(\d{3,4})\s+([A-Za-z][^\$\n]*?)$')

# C11: stand-alone $-only price line (no leading description). Allowed as
# the trailing line of a split row block.
_PRICE_ONLY_LINE_RE = re.compile(r'^\s*\$[\d,]+\.\d{2}(?:\s+\$[\d,]+\.\d{2})?\s*$')


def parse_quote_rows(
    pages: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    """
    Parse quote rows from all pages.

    Two-pass design (C9 + C11):
        Pass 1 (single-line): walk every line. Lines with explicit $X.XX
            structure become accepted rows or row_like rejects. Lines that
            match total/subtotal/header/boilerplate skip patterns AND
            contain a dollar amount are preserved as `line` rejects.
        Pass 2 (block): walk consecutive non-empty lines that were NOT
            consumed by pass 1. Detect deterministic split-line row
            fragments (line_ref + alpha description with no inline price,
            optionally followed by a price-only line) and preserve them
            as `block` candidates with reason=insufficient_structure or
            unstable_boundary. Block grouping NEVER produces accepted rows.

    Returns:
        (accepted_rows, rejected_candidates, parse_meta)

    Raises ExtractionError with meta carrying `rejected_candidates` if no
    accepted rows were parsed. Block candidates from pass 2 are included.
    """
    accepted_rows: List[Dict[str, Any]] = []
    rejected_candidates: List[Dict[str, Any]] = []
    rejection_counts: Dict[str, int] = {}
    row_ordinal = 0
    candidate_ordinal = 0
    block_ordinal = 0

    def _reject(raw_text: str, page_idx: int, reason: str, candidate_type: str) -> None:
        nonlocal candidate_ordinal
        rejected_candidates.append({
            "candidate_id": f"c{page_idx}-{candidate_ordinal}",
            "raw_text": raw_text,
            "source_page": page_idx,
            "rejection_reason": reason,
            "candidate_type": candidate_type,
        })
        rejection_counts[reason] = rejection_counts.get(reason, 0) + 1
        candidate_ordinal += 1

    def _emit_block(
        page_idx: int,
        block_lines: List[str],
        reason: str,
    ) -> None:
        nonlocal block_ordinal
        if not block_lines:
            return
        rejected_candidates.append({
            "candidate_id": f"b{page_idx}-{block_ordinal}",
            "raw_text": "\n".join(block_lines),
            "source_page": page_idx,
            "rejection_reason": reason,
            "candidate_type": CT_BLOCK,
        })
        rejection_counts[reason] = rejection_counts.get(reason, 0) + 1
        block_ordinal += 1

    # ----- Pass 1: single-line accept/reject -----
    consumed_per_page: Dict[int, set] = {}

    for page in pages:
        page_idx = page["page_index"]
        text = page["text"]
        consumed: set = set()

        for line_idx, line in enumerate(text.splitlines()):
            stripped = line.strip()
            if not stripped:
                continue

            dollars = _DOLLAR_RE.findall(stripped)
            has_dollar = bool(dollars)

            if _SUBTOTAL_RE.search(stripped):
                if has_dollar:
                    _reject(stripped, page_idx, R_SUBTOTAL, CT_LINE)
                consumed.add(line_idx)
                continue

            if _TOTAL_RE.search(stripped):
                if has_dollar:
                    _reject(stripped, page_idx, R_TOTAL, CT_LINE)
                consumed.add(line_idx)
                continue

            if _HEADER_RE.search(stripped):
                if has_dollar:
                    _reject(stripped, page_idx, R_HEADER, CT_LINE)
                consumed.add(line_idx)
                continue

            if _is_skip_line(stripped):
                if has_dollar:
                    _reject(stripped, page_idx, R_BOILERPLATE, CT_LINE)
                consumed.add(line_idx)
                continue

            if not has_dollar:
                # Leave for pass 2 block detection.
                continue

            # Defer pure price-only lines (no description before the $)
            # to pass 2. They are not standalone row candidates, but they
            # can serve as the deterministic terminator of a split-line
            # block. If pass 2 doesn't claim them, they are silently
            # dropped — a bare price line is not useful evidence on its own.
            if _PRICE_ONLY_LINE_RE.match(stripped):
                continue

            parsed, reason = _parse_quote_line(stripped, page_idx, row_ordinal)
            if parsed is None:
                _reject(stripped, page_idx, reason or R_UNSTABLE, CT_ROW_LIKE)
                consumed.add(line_idx)
                continue

            accepted_rows.append(parsed)
            row_ordinal += 1
            consumed.add(line_idx)

        consumed_per_page[page_idx] = consumed

    # ----- Pass 2: deterministic block grouping -----
    for page in pages:
        page_idx = page["page_index"]
        text = page["text"]
        consumed = consumed_per_page.get(page_idx, set())
        lines = text.splitlines()

        i = 0
        while i < len(lines):
            stripped = lines[i].strip()
            if not stripped or i in consumed:
                i += 1
                continue

            # Block start condition: a line_ref + alpha description with no
            # inline dollar amount. This is the only deterministic anchor
            # for a multi-line row fragment.
            if not _FRAGMENT_LINE_REF_RE.match(stripped) or _DOLLAR_RE.search(stripped):
                i += 1
                continue

            block_lines: List[str] = [stripped]
            j = i + 1
            saw_price_only = False
            while j < len(lines):
                next_stripped = lines[j].strip()
                if not next_stripped:
                    break  # blank line ends the block
                if j in consumed:
                    break  # consumed lines are not part of this block
                # Another line_ref starts a NEW block — end the current one.
                if _FRAGMENT_LINE_REF_RE.match(next_stripped):
                    break
                # A skip pattern ends the block.
                if (_TOTAL_RE.search(next_stripped) or _SUBTOTAL_RE.search(next_stripped)
                        or _HEADER_RE.search(next_stripped) or _is_skip_line(next_stripped)):
                    break
                # A price-only line is allowed as the deterministic terminator.
                if _PRICE_ONLY_LINE_RE.match(next_stripped):
                    block_lines.append(next_stripped)
                    saw_price_only = True
                    j += 1
                    break
                # A continuation alpha line (description wrap) is allowed.
                if next_stripped[:1].isalpha():
                    block_lines.append(next_stripped)
                    j += 1
                    continue
                break

            # Only emit a block if it has ≥ 2 lines (i.e., a true multi-line
            # group). A solitary line_ref+desc fragment is preserved as a
            # row_like single-line candidate instead.
            if len(block_lines) >= 2:
                reason = R_UNSTABLE if saw_price_only else R_INSUFFICIENT_STRUCTURE
                _emit_block(page_idx, block_lines, reason)
            else:
                # Single fragment line: preserve as row_like evidence.
                _reject(stripped, page_idx, R_INSUFFICIENT_STRUCTURE, CT_ROW_LIKE)

            for k in range(i, j):
                consumed.add(k)
            i = j if j > i else i + 1

    parse_meta: Dict[str, Any] = {
        "rows_detected": len(accepted_rows),
        "candidates_rejected": len(rejected_candidates),
        "block_candidates": sum(
            1 for c in rejected_candidates if c["candidate_type"] == CT_BLOCK
        ),
        "format": "quote_tabular",
        "rejection_counts": rejection_counts,
    }

    if len(accepted_rows) == 0:
        parse_meta["failure_reason"] = REASON_NO_CANDIDATES
        parse_meta["rejected_candidates"] = rejected_candidates
        raise ExtractionError(
            "No deterministic quote rows could be parsed. "
            "Document may not contain structured tabular pricing with explicit "
            "$X.XX values.",
            meta=parse_meta,
        )

    return accepted_rows, rejected_candidates, parse_meta


def _parse_quote_line(
    line: str,
    page_idx: int,
    ordinal: int,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Parse a single quote line into a structured row.

    Returns (row, rejection_reason). One of them is always None.

    Accepted patterns (no ambiguity):
        [line_ref] description $unit_price $amount
        [line_ref] description $amount
        description $unit_price $amount
        description $amount

    Rejected:
        Lines with >2 dollar amounts (cannot deterministically assign
        unit_price vs amount without guessing).
    """
    # Extract optional leading line reference
    line_ref: Optional[str] = None
    m = _LINE_REF_RE.match(line)
    if m:
        line_ref = m.group(1)

    # Find dollar positions in the original line
    dollar_positions = [
        (mm.start(), mm.end(), mm.group(1))
        for mm in _DOLLAR_RE.finditer(line)
    ]

    if not dollar_positions:
        return None, R_UNSTABLE

    # Reject ambiguous numeric relationships: more than 2 dollar amounts on
    # a single line means we cannot deterministically choose which is
    # unit_price and which is amount. Fail closed rather than guess.
    if len(dollar_positions) > 2:
        return None, R_AMBIGUOUS

    # Description is everything before the first dollar sign
    first_dollar_pos = dollar_positions[0][0]
    desc_text = line[:first_dollar_pos].strip()

    # If we extracted a line_ref, strip it from the description
    if line_ref and desc_text.startswith(line_ref):
        desc_text = desc_text[len(line_ref):].strip()
        # Clean up OCR artifacts like leading pipe chars
        desc_text = desc_text.lstrip('|').strip()

    if not desc_text:
        return None, R_MISSING_DESC

    unit_price: Optional[float] = None
    amount: Optional[float] = None

    if len(dollar_positions) == 2:
        unit_price = _parse_dollar(dollar_positions[0][2])
        amount = _parse_dollar(dollar_positions[1][2])
    elif len(dollar_positions) == 1:
        amount = _parse_dollar(dollar_positions[0][2])

    if amount is None:
        return None, R_UNSTABLE

    if amount <= 0:
        return None, R_NON_POSITIVE

    return {
        "row_id": ordinal,
        "line_ref": line_ref,
        "description": desc_text,
        "qty": None,       # NOT inferred — absent from quote text
        "unit": None,      # NOT inferred — absent from quote text
        "unit_price": unit_price,
        "amount": amount,
        "source_page": page_idx,
        # C12: preserve the exact original source line for downstream
        # provenance / mapping audit.
        "source_text": line,
    }, None


def _parse_dollar(s: str) -> Optional[float]:
    """Parse a dollar string like '1,234.56' into float."""
    try:
        return float(s.replace(",", ""))
    except (ValueError, TypeError):
        return None


def _is_skip_line(line: str) -> bool:
    """Check if line is boilerplate / header / notes / contact info."""
    for pat in _SKIP_PATTERNS:
        if pat.search(line):
            return True
    return False

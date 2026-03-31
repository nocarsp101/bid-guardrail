from __future__ import annotations
from typing import List
from app.audit.models import Finding


def adjust_pdf_findings_by_doc_type(findings: List[Finding], doc_type: str) -> List[Finding]:
    """
    Adjust severities without changing detection logic.
    Example policy (tune with client):
      - QUOTE: downgrade some non-critical findings to INFO
      - PRIME BID: keep as detected
    """
    dt = (doc_type or "PRIME_BID").strip().upper()

    if dt not in ("PRIME_BID", "QUOTE"):
        return findings

    adjusted: List[Finding] = []
    for f in findings:
        nf = f.model_copy(deep=True)

        if dt == "QUOTE":
            # Quotes often have less standardized formatting; keep FAILs as FAIL, downgrade certain WARNs to INFO.
            if nf.severity == "WARN" and nf.type in ("near_blank_pages", "duplicate_pages"):
                nf.severity = "INFO"
        # PRIME_BID: keep original
        adjusted.append(nf)

    return adjusted

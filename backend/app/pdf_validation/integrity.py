from __future__ import annotations

from typing import List, Dict, Tuple
import fitz  # PyMuPDF

from app.audit.models import Finding
from app.utils.hashing import sha256_text


def validate_pdf_integrity(pdf_path: str) -> List[Finding]:
    findings: List[Finding] = []

    # 1) Open / corruption check
    try:
        doc = fitz.open(pdf_path)
    except Exception as e:
        return [Finding(
            type="pdf_open",
            severity="FAIL",
            message=f"PDF cannot be opened (corrupt/unreadable): {str(e)}",
            pages=[],
            meta={}
        )]

    if doc.page_count <= 0:
        return [Finding(
            type="pdf_open",
            severity="FAIL",
            message="PDF has zero pages.",
            pages=[],
            meta={}
        )]

    # 2) Blank / near-blank detection
    blank_pages, near_blank_pages = _detect_blank_and_near_blank_pages(doc)
    if blank_pages:
        findings.append(Finding(
            type="blank_pages",
            severity="WARN",
            message=f"Blank pages detected: {blank_pages}",
            pages=blank_pages,
            meta={"thresholds": _blank_thresholds_meta()}
        ))
    if near_blank_pages:
        findings.append(Finding(
            type="near_blank_pages",
            severity="WARN",
            message=f"Near-blank pages detected: {near_blank_pages}",
            pages=near_blank_pages,
            meta={"thresholds": _blank_thresholds_meta()}
        ))

    # 3) Duplicate pages detection
    dup_groups = _detect_duplicate_pages(doc)
    if dup_groups:
        findings.append(Finding(
            type="duplicate_pages",
            severity="WARN",
            message=f"Duplicate page groups detected (0-based indices): {dup_groups}",
            pages=sorted({p for g in dup_groups for p in g}),
            meta={"groups": dup_groups}
        ))

    # 4) Missing last page / truncation heuristic
    heuristic = _missing_last_page_heuristic(doc)
    if heuristic is not None:
        findings.append(heuristic)

    doc.close()
    return findings


def _blank_thresholds_meta() -> Dict:
    return {
        "min_text_chars_blank": 5,
        "min_text_chars_near_blank": 40,
        "min_objects_blank": 3,
        "min_objects_near_blank": 8,
    }


def _detect_blank_and_near_blank_pages(doc: fitz.Document) -> Tuple[List[int], List[int]]:
    """
    Deterministic heuristic (Week-1):
      - text length + object count
      - no rasterization required
    """
    tmeta = _blank_thresholds_meta()
    blank: List[int] = []
    near_blank: List[int] = []

    for i in range(doc.page_count):
        page = doc.load_page(i)

        # Extract text (fast) and count objects
        text = (page.get_text("text") or "").strip()
        text_len = len(text)

        # Object count heuristic: drawings + images + "blocks"
        # drawings: vector paths, etc.
        drawings = page.get_drawings()
        img_list = page.get_images(full=True)
        blocks = page.get_text("blocks") or []

        obj_count = len(drawings) + len(img_list) + len(blocks)

        # classify
        is_blank = (text_len < tmeta["min_text_chars_blank"]) and (obj_count < tmeta["min_objects_blank"])
        is_near_blank = (text_len < tmeta["min_text_chars_near_blank"]) and (obj_count < tmeta["min_objects_near_blank"])

        if is_blank:
            blank.append(i)
        elif is_near_blank:
            near_blank.append(i)

    return blank, near_blank


def _detect_duplicate_pages(doc: fitz.Document) -> List[List[int]]:
    """
    Duplicate detection (Week-1):
      - create per-page signature from normalized text + basic content stats
      - group identical hashes
    """
    sig_map: Dict[str, List[int]] = {}

    for i in range(doc.page_count):
        page = doc.load_page(i)
        text = (page.get_text("text") or "").strip()

        # normalize text to reduce minor differences
        norm = " ".join(text.split()).lower()

        # add basic signals so empty pages don't all collide
        drawings = len(page.get_drawings())
        images = len(page.get_images(full=True))
        blocks = len(page.get_text("blocks") or [])

        signature = f"{norm}||d={drawings}||i={images}||b={blocks}"
        h = sha256_text(signature)

        sig_map.setdefault(h, []).append(i)

    dup_groups = [pages for pages in sig_map.values() if len(pages) >= 2]

    # Optional: ignore very small-text collisions by requiring some content
    cleaned: List[List[int]] = []
    for group in dup_groups:
        p0 = doc.load_page(group[0])
        t0 = (p0.get_text("text") or "").strip()
        if len(t0) >= 10 or len(p0.get_images(full=True)) > 0 or len(p0.get_drawings()) > 0:
            cleaned.append(group)

    return cleaned


def _missing_last_page_heuristic(doc: fitz.Document) -> Finding | None:
    """
    Missing/truncated last page heuristic (Week-1):
      - compare last page 'content density' vs median of previous pages
      - checks for abrupt drop on last page(s)
    """
    if doc.page_count < 3:
        return None

    densities: List[float] = []
    for i in range(doc.page_count):
        page = doc.load_page(i)
        text_len = len((page.get_text("text") or "").strip())
        obj_count = len(page.get_drawings()) + len(page.get_images(full=True)) + len(page.get_text("blocks") or [])
        densities.append(text_len + (obj_count * 20.0))  # weighted score

    # median of all except last page
    base = sorted(densities[:-1])
    mid = base[len(base)//2]
    last = densities[-1]

    # Heuristic thresholds (tune after sample files)
    # FAIL if last page density is extremely low vs typical
    if mid > 0 and last < (0.15 * mid):
        return Finding(
            type="missing_last_page_heuristic",
            severity="FAIL",
            message="Final page content density dropped sharply; possible truncation/missing last page (heuristic).",
            pages=[doc.page_count - 1],
            meta={"median_density_excl_last": mid, "last_density": last, "ratio": (last / mid if mid else None)}
        )

    # WARN if moderately low
    if mid > 0 and last < (0.30 * mid):
        return Finding(
            type="missing_last_page_heuristic",
            severity="WARN",
            message="Final page content density is unusually low; review for truncation/missing last page (heuristic).",
            pages=[doc.page_count - 1],
            meta={"median_density_excl_last": mid, "last_density": last, "ratio": (last / mid if mid else None)}
        )

    return None

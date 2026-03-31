# backend/app/bid_validation/normalize.py
from __future__ import annotations

import re
from typing import Any, Optional


_WS_RE = re.compile(r"\s+")


def norm_header(h: Any) -> str:
    """
    Customer requirement: exact match after normalization.
    We treat any whitespace (spaces/tabs/newlines) as whitespace and collapse it.
    """
    if h is None:
        return ""
    s = str(h)
    # normalize NBSP etc.
    s = s.replace("\u00A0", " ")
    s = s.strip().upper()
    # collapse internal whitespace/newlines to single space
    s = _WS_RE.sub(" ", s)
    return s


def norm_item_code(v: Any) -> str:
    """
    Deterministic item normalization:
      - trim whitespace
      - strip leading zeros
    """
    if v is None:
        return ""
    s = str(v).strip()
    # if numeric-like, preserve decimals; only strip leading zeros on integer-like tokens
    # customer asked for "0010" -> "10" normalization
    if re.fullmatch(r"0*\d+", s):
        s = str(int(s))  # int("0010") => 10
    else:
        # for strings with leading zeros but non-pure digits, strip only the leading zeros portion
        s = re.sub(r"^0+(\d)", r"\1", s)
    return s


def to_num(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return None
    s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


def to_bool(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "x", "checked")
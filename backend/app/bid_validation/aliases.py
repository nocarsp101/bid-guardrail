# backend/app/bid_validation/aliases.py
from __future__ import annotations

CANONICAL_REQUIRED_FIELDS = [
    "item",
    "unit",
    "qty",
    "unit_price",
    # total can be missing; compute deterministically if absent
]

CANONICAL_OPTIONAL_FIELDS = [
    "total",
    "description",
    "notes",
    "excluded_flag",
]

# NOTE:
# - Headers are normalized via norm_header(): strip + uppercase + collapse whitespace/newlines
# - Exact match only (no substrings / no fuzzy)
ALIASES = {
    # IMPORTANT: We do NOT include bare "ITEM" here because many DOT sheets use "Item" as DESCRIPTION,
    # and "Item No." as the item code. Including "ITEM" would create ambiguity (fail).
    #
    # IMPORTANT (Test 13 fix):
    # DOT/ASHTOWare exports often include BOTH "Item Number" (pay item) and "Line Number" (row index).
    # If we treat LINE NUMBER as an "item" alias, header mapping becomes ambiguous and must FAIL.
    # Therefore, PRIME_BID "item" aliases MUST NOT include LINE* headers.
    "item": [
        "ITEM NO.",
        "ITEM NO",
        "ITEM NUMBER",
        "ITEM #",
        "BID ITEM NO.",
        "BID ITEM NO",
        "BID ITEM NUMBER",
        "PAY ITEM",
        "PAY ITEM NO.",
        "PAY ITEM NO",
        # REMOVED (to prevent ambiguity with "ITEM NUMBER"):
        # "LINE NO.",
        # "LINE NO",
        # "LINE NUMBER",
        # "LINE NUM",
        # "LINE #",
    ],
    "qty": [
        "QTY",
        "QUANTITY",
        "BID QUANTITY",
        "ESTIMATED QTY",
        "ESTIMATED QUANTITY",
        "BID QTY",
    ],
    "unit": [
        "UNIT",
        "UOM",
        "UNITS",
    ],
    "unit_price": [
        "UNIT PRICE",
        "PRICE",
        "BID PRICE",
        "UNIT BID PRICE",
        "UNIT COST",
        "RATE",
    ],
    "total": [
        "TOTAL",
        "TOTAL PRICE",
        "TOTAL AMOUNT",
        "EXTENDED AMOUNT",
        "EXTENSION",
        "AMOUNT",
        "LINE TOTAL",
        "EXT AMOUNT",
        "EXTENDED TOTAL",
    ],
    "description": [
        "DESCRIPTION",
        "ITEM DESCRIPTION",
        "BID ITEM DESCRIPTION",
        # DOT sheets often label the description column as just "Item"
        "ITEM",
        "BID ITEM",
    ],
    "notes": [
        "NOTES",
        "NOTE",
        "REMARKS",
        "COMMENTS",
        "COMMENT",
        "EXPLANATION",
    ],
    "excluded_flag": [
        "EXCLUDE",
        "EXCLUDED",
        "EXCLUSION",
        "EXCLUSION FLAG",
        "EXCLUDED FLAG",
    ],
}
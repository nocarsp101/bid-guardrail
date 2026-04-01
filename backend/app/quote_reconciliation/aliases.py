# backend/app/quote_reconciliation/aliases.py
from __future__ import annotations

# QUOTE canonical required fields (for compare)
CANONICAL_REQUIRED_FIELDS = [
    "item",        # LINE NUMBER (0010, 0020, etc)
    "unit",
    "qty",
    "unit_price",
]

CANONICAL_OPTIONAL_FIELDS = [
    "total",
    "description",
    "pay_item",    # DOT "Item"/"Item Number"/"Pay Item" captured separately
    "notes",
]

# Deterministic alias dictionary (normalized headers: STRIP + UPPERCASE + collapse whitespace/newlines)
# IMPORTANT:
# - QUOTE "item" is reserved for LINE identifiers only
# - DOT-style files often include BOTH "Line" and "Item" columns; "Item" must NOT map to canonical "item"
ALIASES = {
    # LINE / row identifier
    "item": [
        "LINE NUMBER",
        "LINE NO.",
        "LINE NO",
        "LINE #",
        "LINE",
        "BID ITEM #",  # IPSI-style: "Bid Item #" contains proposal line numbers
    ],

    # PAY ITEM / bid item identifier
    "pay_item": [
        "ITEM",            # <-- critical for DOT CSVs that have a column literally named "Item"
        "ITEM NUMBER",
        "PAY ITEM",
        "PAY ITEM NO.",
        "PAY ITEM NO",
        "BID ITEM NUMBER",
        "BID ITEM",
        "ITEM NO.",
        "ITEM NO",
        "ITEM #",
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
        "PER UNIT",  # IPSI-style: "Per Unit" means unit price
    ],
    "total": [
        "TOTAL",
        "TOTAL PRICE",
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
    ],
    "notes": [
        "NOTES",
        "NOTE",
        "REMARKS",
        "COMMENTS",
        "COMMENT",
        "EXPLANATION",
    ],
}
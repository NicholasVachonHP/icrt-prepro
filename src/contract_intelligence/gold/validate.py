"""Per-type structural validation of extracted field values.

Independent of the evidence-quote check and the LLM judge: this asks only
*is the value the right shape/format for its declared type?* -- a boolean is a
real boolean, an integer is an integer, a list parses to a non-empty list, a
struct_list's items carry their declared keys, etc. The result feeds
:func:`contract_intelligence.gold.evidence.derive_trust` as a hard gate: a
structurally invalid value can never be ``high`` trust, however confident the
judge.

:func:`validate_value` runs on the *coerced* wide value (the same value written
to the gold column -- so ``list`` / ``struct_list`` arrive as JSON strings) and
returns one of:

* :data:`VALID`          -- conforms to the declared type/format.
* :data:`INVALID`        -- present but malformed for its type.
* :data:`NOT_APPLICABLE` -- nothing to validate (null value, or an empty
  collection, i.e. "none found").

This module imports only the standard library so it stays a leaf dependency of
both ``fields`` and ``evidence``.
"""

import json
import re

VALID = "valid"
INVALID = "invalid"
NOT_APPLICABLE = "not_applicable"


def _validate_string(value, field):
    """A string value: non-empty, and matching an optional ``validation_pattern``.

    Blank strings are treated as "nothing claimed" (``not_applicable``). If the
    field declares a ``validation_pattern`` (e.g. a date regex), the trimmed
    value must match it; a malformed pattern in config never penalises the value.
    """
    if not isinstance(value, str):
        return INVALID
    s = value.strip()
    if not s:
        return NOT_APPLICABLE
    pattern = field.get("validation_pattern")
    if pattern:
        try:
            return VALID if re.search(pattern, s) else INVALID
        except re.error:
            return VALID
    return VALID


def _validate_collection(value, field, ftype):
    """A ``list`` / ``struct_list`` value, stored as a JSON string.

    Must parse to a JSON list. An empty list means "none found"
    (``not_applicable``). For ``struct_list`` each item must be an object with at
    least one of its declared ``item_fields`` populated; for a plain ``list``
    items must be scalars.
    """
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
    except (TypeError, ValueError):
        return INVALID
    if not isinstance(parsed, list):
        return INVALID
    if not parsed:
        return NOT_APPLICABLE

    if ftype == "struct_list":
        required = [it["name"] for it in field.get("item_fields", [])]
        for item in parsed:
            if not isinstance(item, dict):
                return INVALID
            if required and not any(
                item.get(k) not in (None, "") for k in required
            ):
                return INVALID
        return VALID

    # plain list: scalar items only.
    for item in parsed:
        if isinstance(item, (dict, list)):
            return INVALID
    return VALID


def validate_value(value, field):
    """Validate one coerced field value against its declared type.

    Returns :data:`VALID`, :data:`INVALID`, or :data:`NOT_APPLICABLE`.
    """
    if value is None:
        return NOT_APPLICABLE

    ftype = (field.get("type") or "string").lower()
    if ftype == "boolean":
        return VALID if isinstance(value, bool) else INVALID
    if ftype == "integer":
        return VALID if isinstance(value, int) and not isinstance(value, bool) else INVALID
    if ftype in ("number", "float", "double"):
        ok = isinstance(value, (int, float)) and not isinstance(value, bool)
        return VALID if ok else INVALID
    if ftype in ("list", "struct_list"):
        return _validate_collection(value, field, ftype)
    return _validate_string(value, field)

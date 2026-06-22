"""Resolve each extraction field to a strategy and group fields by it.

A field's **strategy** decides *how* its value is extracted. It is derived from
the field's declared ``type`` with two optional per-field overrides, in priority
order:

1. ``extraction_strategy`` -- an explicit strategy name on the field.
2. ``source`` -- a coarse hint (``"tables"`` / ``"full_text"``) mapped to a
   strategy.
3. the ``type`` default (see :data:`_TYPE_DEFAULT`).

Strategies (best read alongside ``gold/fields.py``):

* ``retrieve_classify`` -- boolean presence/property fields (does an obligation
  exist?). Recall-first.
* ``rag`` -- single-point scalar facts (date, term, governing law...).
* ``tables`` -- values that live in tables/schedules (contract value, SLAs).
* ``map_reduce`` -- enumerations scattered across the document (parties,
  services): processed chunk-by-chunk, then merged.
* ``full_text`` -- whole-document reasoning where retrieval would defeat the
  question (notable clauses, "notable relative to the rest").

:func:`group_fields` splits the all-fields extraction into one focused call per
strategy (each with a per-type JSON schema). The execution attached to each
strategy lives in ``gold/fields.py``: ``tables`` reads the structured
``contract_tables`` rows, ``rag`` / ``retrieve_classify`` retrieve chunks from
the AI Search index, ``map_reduce`` processes the contract chunk-by-chunk then
merges, and ``full_text`` (plus every fallback) sees the whole contract text.
"""

# Strategy names (stored verbatim on the evidence table later).
RETRIEVE_CLASSIFY = "retrieve_classify"
RAG = "rag"
TABLES = "tables"
MAP_REDUCE = "map_reduce"
FULL_TEXT = "full_text"

VALID_STRATEGIES = frozenset(
    {RETRIEVE_CLASSIFY, RAG, TABLES, MAP_REDUCE, FULL_TEXT}
)

# Default strategy per declared field ``type``.
_TYPE_DEFAULT = {
    "boolean": RETRIEVE_CLASSIFY,
    "string": RAG,
    "integer": RAG,
    "number": RAG,
    "float": RAG,
    "double": RAG,
    "list": MAP_REDUCE,
    "struct_list": FULL_TEXT,
}

# Coarse ``source`` hint -> strategy.
_SOURCE_STRATEGY = {
    "tables": TABLES,
    "full_text": FULL_TEXT,
}


def resolve_strategy(field):
    """Return the extraction strategy for one field definition.

    Priority: explicit ``extraction_strategy`` > ``source`` hint > ``type``
    default. Raises ``ValueError`` on an unknown explicit ``extraction_strategy``
    so config typos fail fast rather than silently mis-routing a field.
    """
    explicit = (field.get("extraction_strategy") or "").strip().lower()
    if explicit:
        if explicit not in VALID_STRATEGIES:
            raise ValueError(
                f"Field '{field.get('field_name')}' has unknown "
                f"extraction_strategy '{explicit}'. Valid: "
                f"{sorted(VALID_STRATEGIES)}."
            )
        return explicit

    source = (field.get("source") or "").strip().lower()
    if source in _SOURCE_STRATEGY:
        return _SOURCE_STRATEGY[source]

    ftype = (field.get("type") or "string").lower()
    return _TYPE_DEFAULT.get(ftype, RAG)


def group_fields(fields):
    """Group field definitions by resolved strategy, preserving field order.

    Returns ``dict[strategy, list[field]]``. Each group becomes one extraction
    call (fields in a group are bundled into a single model request).
    """
    groups = {}
    for f in fields:
        groups.setdefault(resolve_strategy(f), []).append(f)
    return groups

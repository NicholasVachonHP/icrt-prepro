"""Gold-layer structured field extraction for the contract intelligence pipeline.

Reads extracted contract text from the silver ``contract_text`` table (the silver
lakehouse is mounted, not attached), asks a chat model (gpt-4.1 via the Microsoft
Foundry project) a configurable set of questions per contract, and writes one wide
row per contract *version* into the gold ``contract_fields`` delta table — one
column per configured field — to support side-by-side contract comparison. The
table is a **Slowly Changing Dimension Type 2**: every
``(relative_path, silver_version_id, code_hash)`` version is retained, with
``is_current`` marking the live extraction; the ``contract_fields_active`` view
exposes only live rows.

Extraction is incremental: a contract is only (re)processed when it is new, the
silver text changed (new raw content *or* new silver extraction logic, both of
which bump the silver ``version_id``), or the gold extraction ``code_hash``
changed. The code_hash fingerprints this module's code plus the system prompt,
the field / question definitions, the model name and the input cap -- so editing
the prompt or the field config re-runs extraction over existing, unchanged
contracts. Contracts tombstoned in silver are tombstoned here too.

Alongside the wide value table, each field's **provenance** is written to the
tall ``contract_field_evidence`` table (one row per contract version *per
field*): the verbatim ``evidence`` quote the model cited, a mechanical
``match_type`` saying whether that quote is actually in the contract text, an
LLM ``judge_verdict`` saying whether the value+evidence correctly answer the
field's question, and a fused categorical ``trust`` (high / review / low /
unknown). See :mod:`contract_intelligence.gold.evidence`. The judge prompt and
fuzzy threshold also feed the ``code_hash``.

Designed to run inside a Microsoft Fabric notebook attached to the *gold*
lakehouse, with the silver lakehouse mounted read-only and the *ictr_dev*
environment attached (provides ``openai`` / ``azure-ai-projects`` and secrets).
"""

import hashlib
import json
import os
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from . import evidence as ev
from . import retrieval
from . import scheduler
from . import strategy as strat
from . import validate as vld
from . import vision
from ..common.ai_clients import get_openai_client
from ..common.versioning import code_fingerprint
from ..common.scd2 import (
    scd2_merge,
    scd2_expire_and_append,
    resolve_force_paths,
    apply_force,
)

# Default cap on contract characters sent to the model. Sized to gpt-4.1's
# ~1,047,576-token context window: reserving the 32,768-token max output plus a
# few thousand tokens for the system prompt and field questions leaves ~1.0M
# input tokens. At a conservative ~3 chars/token for dense legal English that is
# ~3,000,000 chars, so full contracts (e.g. the long Royal Mail agreement) are
# no longer truncated before clauses like UK data residency. Override per env
# via the ``gold.max_input_chars`` config key.
_DEFAULT_MAX_INPUT_CHARS = 3_000_000

# Fixed (non-field) columns always present on the gold table. SCD Type 2: one
# row per (relative_path, content_hash, code_hash) version; ``is_current``
# marks the live extraction of each contract.
_META_FIELDS = [
    StructField("version_id", StringType(), False),      # surrogate version key
    StructField("relative_path", StringType(), False),   # join key to silver
    StructField("file_name", StringType(), False),
    StructField("content_hash", StringType(), False),    # copied from silver row
    StructField("code_hash", StringType(), False),       # prompt/code/model version
    StructField("model", StringType(), True),
    StructField("extraction_error", StringType(), True),
    StructField("valid_from", TimestampType(), False),   # when this version began
    StructField("valid_to", TimestampType(), True),      # null while current
    StructField("is_current", BooleanType(), False),     # live version of the contract
    StructField("doc_deleted", BooleanType(), False),
]

# Tall companion-table columns. One row per (contract version, field): the
# extracted value, the verbatim evidence quote, whether that quote is real
# (``match_type``), whether it correctly answers the question (``judge_verdict``)
# and the fused categorical ``trust``. SCD2 like the wide table.
_EVIDENCE_META = [
    StructField("evidence_id", StringType(), False),     # sha(version_id|field)
    StructField("version_id", StringType(), False),      # = wide row's version_id
    StructField("relative_path", StringType(), False),
    StructField("file_name", StringType(), False),
    StructField("field_name", StringType(), False),
    StructField("extraction_strategy", StringType(), True),  # how value was extracted
    StructField("retrieved_chunk_ids", StringType(), True),  # JSON list of chunk ids used
    StructField("value", StringType(), True),            # stringified wide value
    StructField("evidence_text", StringType(), True),    # verbatim quote
    StructField("match_type", StringType(), False),      # is the quote real?
    StructField("type_validation", StringType(), True),  # structural check: valid/invalid/na
    StructField("judge_verdict", StringType(), True),    # answers the question?
    StructField("judge_rationale", StringType(), True),
    StructField("judge_error", StringType(), True),      # non-fatal judge failure
    StructField("evidence_di_confidence", DoubleType(), True),  # DI conf of evidence span (Plan 02)
    StructField("evidence_page", IntegerType(), True),   # source page of the evidence block
    StructField("source_verified", StringType(), True),  # high/low/confirmed (Source->DI gate)
    StructField("vision_verdict", StringType(), True),   # confirmed/contradicted/unclear (Phase 2)
    StructField("vision_rationale", StringType(), True), # one-line vision justification
    StructField("vision_action", StringType(), True),    # none/verify/correct (Plan 03)
    StructField("value_source", StringType(), True),     # model_text/vision (Plan 03)
    StructField("original_value", StringType(), True),   # pre-correction value (Plan 03 calibration hook)
    StructField("trust", StringType(), False),           # high/review/low/unknown
    StructField("contract_truncated", BooleanType(), False),
    StructField("model", StringType(), True),
    StructField("judge_model", StringType(), True),
    StructField("valid_from", TimestampType(), False),
    StructField("valid_to", TimestampType(), True),
    StructField("is_current", BooleanType(), False),
    StructField("doc_deleted", BooleanType(), False),
]

_EVIDENCE_SCHEMA = StructType(_EVIDENCE_META)

_SYSTEM_PROMPT = (
    "You are a meticulous contracts analyst. You extract structured facts from a "
    "single contract. Only use information present in the contract text. If a value "
    "is not stated, return null — never guess. For every field return a JSON "
    "object with two keys: \"value\" (the answer, in the requested shape, or null) "
    "and \"evidence\" (a short verbatim quote copied EXACTLY from the contract "
    "text that supports the value, or null when the value is null). Never "
    "paraphrase the evidence. Respond with a single JSON object whose keys are "
    "exactly the requested field names."
)


def _load_fields(notebookutils, gold_cfg):
    """Load the field/question definitions from the shared lakehouse config."""
    shared = notebookutils.fs.getMountPath("/shared_code")
    rel = gold_cfg.get("fields_config", "config/extraction_fields.json")
    with open(f"{shared}/{rel}") as f:
        fields = json.load(f)
    if not fields:
        raise ValueError(f"No extraction fields defined in '{rel}'.")
    return fields


# Mapping from extraction_fields.json scalar "type" values to Spark types.
_SCALAR_TYPE_MAP = {
    "string":  StringType(),
    "boolean": BooleanType(),
    "integer": IntegerType(),
    "number":  DoubleType(),
    "float":   DoubleType(),
    "double":  DoubleType(),
    "list":    ArrayType(StringType()),
}


def _scalar_spark_type(type_str):
    """Spark type for a scalar/list type name (defaults to STRING)."""
    return _SCALAR_TYPE_MAP.get((type_str or "string").lower(), StringType())


def _field_spark_type(field):
    """Return the Spark DataType for a field definition.

    Scalar fields map to their native Spark type. Complex fields -- ``list`` and
    ``struct_list`` -- are stored as a **JSON string** (``StringType``) rather than
    a native ``ARRAY`` / ``ARRAY<STRUCT<...>>`` column, because the Fabric SQL
    analytics endpoint, Direct Lake and Azure AI Search cannot represent nested
    types: they silently drop such columns and stall the endpoint metadata sync
    (the cause of multi-minute Lakehouse previews). Consumers ``json.loads`` these
    columns when they need the inner structure.
    """
    ftype = (field.get("type") or "string").lower()
    if ftype in ("list", "struct_list"):
        return StringType()
    return _scalar_spark_type(ftype)


def _coerce_scalar(v, type_str):
    """Coerce one value to a scalar/list Python type for its column."""
    if v is None:
        return None
    ftype = (type_str or "string").lower()
    if ftype == "boolean":
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            return v.strip().lower() in ("true", "1", "yes")
        return bool(v)
    if ftype == "integer":
        try:
            return int(v)
        except (ValueError, TypeError):
            return None
    if ftype in ("number", "float", "double"):
        try:
            return float(v)
        except (ValueError, TypeError):
            return None
    if ftype == "list":
        if isinstance(v, list):
            return [str(x) for x in v if x is not None]
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
                if isinstance(parsed, list):
                    return [str(x) for x in parsed if x is not None]
            except json.JSONDecodeError:
                pass
            return [x.strip() for x in v.split(",") if x.strip()]
        return [str(v)]
    # string (default): stringify non-string values
    if isinstance(v, str):
        return v
    return json.dumps(v, ensure_ascii=False)


def _coerce(v, field):
    """Coerce an LLM-returned value to match its declared column type.

    Complex fields (``list``, ``struct_list``) are serialized to a JSON string so
    they fit a scalar ``StringType`` column (see :func:`_field_spark_type`);
    ``None`` stays ``None``. Scalars are coerced to their native Python type.
    """
    ftype = (field.get("type") or "string").lower()

    if ftype == "list":
        lst = _coerce_scalar(v, "list")
        return None if lst is None else json.dumps(lst, ensure_ascii=False)

    if ftype != "struct_list":
        return _coerce_scalar(v, ftype)

    # struct_list: a list of objects matching the declared item_fields, stored as
    # a JSON string.
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except json.JSONDecodeError:
            return None
    if not isinstance(v, list):
        return None
    item_fields = field.get("item_fields", [])
    items = []
    for obj in v:
        if not isinstance(obj, dict):
            continue
        items.append(
            {
                it["name"]: _coerce_scalar(obj.get(it["name"]), it.get("type"))
                for it in item_fields
            }
        )
    return json.dumps(items, ensure_ascii=False)


def _build_schema(fields):
    """Wide gold schema: metadata columns + one typed column per field."""
    field_cols = [
        StructField(f["field_name"], _field_spark_type(f), True)
        for f in fields
    ]
    return StructType(_META_FIELDS + field_cols)


def _build_prompt(fields, text, max_chars):
    """Compose the user prompt: the field questions plus the contract text."""
    if len(text) > max_chars:
        text = text[:max_chars]
    questions = "\n".join(
        f'- "{f["field_name"]}" ({f.get("type", "string")}): {f["question"]}'
        for f in fields
    )
    return (
        "Extract the following fields from the contract. Return a JSON object with "
        "exactly these keys; each maps to "
        '{"value": <answer or null>, "evidence": <verbatim supporting quote or '
        'null>}:\n'
        f"{questions}\n\n"
        "Contract text:\n"
        '"""\n'
        f"{text}\n"
        '"""'
    )


def _stringify_value(v):
    """Render a (possibly already-coerced) wide value as a string for the tall
    ``value`` column and the judge prompt. ``None`` stays ``None``."""
    if v is None:
        return None
    if isinstance(v, str):
        return v
    return json.dumps(v, ensure_ascii=False)


def _evidence_id(version_id, field_name):
    """Stable unique key for a (contract version, field) evidence row."""
    return hashlib.sha256(f"{version_id}|{field_name}".encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Per-type structured output (JSON schema response_format)
# ---------------------------------------------------------------------------

def _json_scalar(type_str):
    """JSON-schema scalar type name for an extraction_fields scalar type."""
    t = (type_str or "string").lower()
    if t == "boolean":
        return "boolean"
    if t == "integer":
        return "integer"
    if t in ("number", "float", "double"):
        return "number"
    return "string"


def _value_schema(field):
    """JSON schema for a field's ``value`` (nullable, typed per declared type)."""
    ftype = (field.get("type") or "string").lower()
    if ftype == "list":
        return {"type": ["array", "null"], "items": {"type": "string"}}
    if ftype == "struct_list":
        item_fields = field.get("item_fields", [])
        props = {
            it["name"]: {"type": [_json_scalar(it.get("type")), "null"]}
            for it in item_fields
        }
        return {
            "type": ["array", "null"],
            "items": {
                "type": "object",
                "properties": props,
                "required": [it["name"] for it in item_fields],
                "additionalProperties": False,
            },
        }
    return {"type": [_json_scalar(ftype), "null"]}


def _build_response_format(fields):
    """Strict ``json_schema`` response_format: each field -> {value, evidence}.

    Values are typed per the field's declared ``type`` (boolean / integer /
    number / string / array / struct array); evidence is a nullable string.
    Strict structured outputs require every property listed in ``required`` and
    ``additionalProperties: false`` at each level.
    """
    props = {
        f["field_name"]: {
            "type": "object",
            "properties": {
                "value": _value_schema(f),
                "evidence": {"type": ["string", "null"]},
            },
            "required": ["value", "evidence"],
            "additionalProperties": False,
        }
        for f in fields
    }
    schema = {
        "type": "object",
        "properties": props,
        "required": [f["field_name"] for f in fields],
        "additionalProperties": False,
    }
    return {
        "type": "json_schema",
        "json_schema": {
            "name": "field_extraction",
            "strict": True,
            "schema": schema,
        },
    }


def _parse_extraction(data, fields):
    """Pull ``(values, evidence)`` out of one extraction response for ``fields``.

    The model returns ``{"value": ..., "evidence": ...}`` per field; a bare scalar
    (no object) is tolerated as a value with no evidence. Values are coerced to
    the declared column type.
    """
    values, evidence = {}, {}
    for f in fields:
        name = f["field_name"]
        item = data.get(name)
        if isinstance(item, dict) and ("value" in item or "evidence" in item):
            raw_value = item.get("value")
            quote = item.get("evidence")
        else:
            raw_value = item          # bare scalar (no object) fallback
            quote = None
        values[name] = _coerce(raw_value, f)
        evidence[name] = quote if isinstance(quote, str) and quote else None
    return values, evidence


def _extract_group(client, model, fields, text, max_chars, structured_output):
    """Call the model for ONE strategy group's fields.

    Returns ``(values, evidence, error_or_None)``. When ``structured_output`` is
    set, a per-type strict ``json_schema`` response_format is tried first; on any
    failure it falls back to a plain ``json_object`` call before giving up, so a
    deployment that does not support structured outputs still works.
    """
    prompt = _build_prompt(fields, text, max_chars)
    messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": prompt},
    ]
    formats = []
    if structured_output:
        formats.append(_build_response_format(fields))
    formats.append({"type": "json_object"})

    data, last_err = None, None
    for rf in formats:
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=messages,
                response_format=rf,
                temperature=0,
            )
            data = json.loads(resp.choices[0].message.content)
            break
        except Exception as e:  # noqa: BLE001 - try next format, else report
            last_err = e
            continue
    if data is None:
        return {}, {}, str(last_err)

    values, evidence = _parse_extraction(data, fields)
    return values, evidence, None


def _extract_grouped(client, model, groups, text, max_chars, structured_output):
    """Extract every strategy group for one contract and merge the results.

    Returns ``(values, evidence, error_or_None)`` over all fields. Each group is
    one chat completion (Phase 1: every group still sees the full text; retrieval
    and map-reduce attach to specific strategies in later phases). If any group
    fails, its fields are left unset and the combined error is returned so the
    contract is marked errored and auto-retries next run (matching the prior
    single-call semantics).
    """
    values, evidence, errors = {}, {}, []
    for stg, gfields in groups.items():
        v, e, err = _extract_group(
            client, model, gfields, text, max_chars, structured_output
        )
        if err:
            errors.append(f"{stg}: {err}")
            continue
        values.update(v)
        evidence.update(e)
    return values, evidence, ("; ".join(errors) if errors else None)


def _est_tokens(text, output_reserve=2000):
    """Rough token estimate for one model call: ~4 chars/token of input plus a
    fixed output reserve. Used only to size the token-budget limiter, so an
    approximation is deliberately conservative rather than exact."""
    return max(1, len(text or "") // 4 + output_reserve)


def _mk_single(client, model, gfields, text, max_chars, structured_output):
    """No-arg extraction task over one text context for one strategy group.
    Returns ``(values, evidence, error)``."""
    return lambda: _extract_group(
        client, model, gfields, text, max_chars, structured_output
    )


def _mk_judge(client, judge_model, fields, extractions, text, max_chars):
    """No-arg judge task; returns ``(verdicts, judge_error)`` and never raises so
    the stage runner can continue (judge failure degrades to locate-only trust)."""

    def _judge():
        try:
            return (
                ev.judge_fields(
                    client, judge_model, fields, extractions, text, max_chars
                ),
                None,
            )
        except Exception as e:  # noqa: BLE001 - non-fatal; locate-only trust
            return {}, str(e)

    return _judge


def _reduce_llm(client, model, field, items):
    """Consolidate a deterministically-unioned list with one LLM call -- merge
    near-duplicates ('Acme Ltd' / 'Acme Limited'), normalise, drop noise.
    Returns ``(cleaned_list, error)``; on failure the caller keeps the union."""
    name = field["field_name"]
    desc = field.get("question", name)
    prompt = (
        f"These list items for the field '{name}' ({desc}) were extracted "
        "piecewise from sections of one contract. Merge duplicates and "
        "near-duplicates, normalise each entry, and drop noise, preserving "
        'meaning. Return JSON {"items": [strings]}.\n\n'
        f"Candidates:\n{json.dumps(items, ensure_ascii=False)}"
    )
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": "You consolidate extracted list items into a "
                    "clean, deduplicated list.",
                },
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0,
        )
        data = json.loads(resp.choices[0].message.content)
        out = data.get("items")
        if isinstance(out, list):
            return [str(x) for x in out if x is not None and str(x).strip()], None
        return None, "reduce: response missing 'items' list"
    except Exception as e:  # noqa: BLE001
        return None, str(e)


def _mk_reduce_llm(client, model, field, items):
    """No-arg map-reduce consolidation task."""
    return lambda: _reduce_llm(client, model, field, items)


def _union_partials(partials, name, field):
    """Union one map-reduce field across all per-chunk partials.

    Returns ``(union_items, evidence_quote)``: items de-duplicated
    case-insensitively (struct items keyed by their sorted JSON) preserving
    first-seen order, and the first non-empty quote any chunk cited."""
    seen, union, quote = set(), [], None
    for values, evidence in partials:
        if quote is None:
            q = evidence.get(name)
            if isinstance(q, str) and q:
                quote = q
        raw = values.get(name)
        if raw is None:
            continue
        try:
            items = json.loads(raw) if isinstance(raw, str) else raw
        except (TypeError, ValueError):
            items = [raw]
        if not isinstance(items, list):
            items = [items]
        for it in items:
            if isinstance(it, (dict, list)):
                key = json.dumps(it, sort_keys=True, ensure_ascii=False).lower()
            else:
                key = str(it).strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            union.append(it)
    return union, quote


def _finalize_contract(
    fields, strategies, text, values, evidence, chunk_ids_by_field, verdicts,
    judge_error, judge_enabled, max_chars, fuzzy_threshold, evidence_max_chars,
    type_validation_enabled, blocks, di_quality_flag, di_conf_threshold, error,
):
    """Locate evidence, validate, and derive trust for one contract's merged
    extraction. Returns ``(values, error, truncated, field_rows)`` -- the shape
    the wide/tall writers consume. On extraction ``error`` no field rows are
    produced (the contract auto-retries next run, like the wide table).

    ``blocks`` are the contract's silver semantic blocks (with DI ``conf_min``);
    together with ``di_quality_flag`` and ``di_conf_threshold`` they drive the
    Source->DI confidence gate (Plan 02): each field's evidence span confidence
    becomes ``source_verified`` (``low`` demotes a ``high`` trust to ``review``)."""
    if error:
        return values, error, False, []

    truncated = len(text) > max_chars
    doc_low = di_quality_flag == "low"
    field_rows = []
    for f in fields:
        name = f["field_name"]
        value_str = _stringify_value(values.get(name))
        quote = evidence.get(name)
        if value_str is None:
            match_type = ev.MATCH_NA_NULL
        elif not quote:
            match_type = ev.MATCH_NOT_FOUND
        else:
            match_type = ev.locate_evidence(quote, text, fuzzy_threshold)

        verdict_obj = verdicts.get(name, {})
        verdict = verdict_obj.get("verdict")
        rationale = verdict_obj.get("rationale")
        validation = (
            vld.validate_value(values.get(name), f) if type_validation_enabled else None
        )

        # Source->DI confidence: read the DI conf_min / page of the block the
        # evidence quote came from, then classify how well the source backs it.
        evidence_di_confidence, evidence_page = _block_confidence_for_quote(
            quote, blocks
        )
        if doc_low:
            source_verified = ev.SOURCE_LOW   # whole document scanned poorly
        elif evidence_di_confidence is None:
            source_verified = None            # no confidence signal for this span
        elif evidence_di_confidence < di_conf_threshold:
            source_verified = ev.SOURCE_LOW   # DI read the cited span poorly
        else:
            source_verified = ev.SOURCE_HIGH

        trust = ev.derive_trust(
            value_str, match_type, verdict, judge_error, judge_enabled, validation,
            source_verified,
        )
        if quote and evidence_max_chars and len(quote) > evidence_max_chars:
            quote = quote[:evidence_max_chars]
        cids = chunk_ids_by_field.get(name)
        field_rows.append(
            {
                "field_name": name,
                "extraction_strategy": strategies.get(name),
                "retrieved_chunk_ids": json.dumps(cids) if cids else None,
                "value": value_str,
                "evidence_text": quote,
                "match_type": match_type,
                "type_validation": validation,
                "judge_verdict": verdict,
                "judge_rationale": rationale,
                "judge_error": judge_error,
                "evidence_di_confidence": evidence_di_confidence,
                "evidence_page": evidence_page,
                "source_verified": source_verified,
                "vision_verdict": None,
                "vision_rationale": None,
                "vision_action": "none",
                "value_source": "model_text",
                "original_value": None,
                "trust": trust,
                "contract_truncated": truncated,
            }
        )
    return values, None, truncated, field_rows


def _extract_all_contracts(
    client, search_client, chunks_by_path, tables_by_path, blocks_by_path, fields,
    groups, strategies, pending_rows,
    *, model, judge_model, max_chars, fuzzy_threshold, evidence_max_chars,
    judge_enabled, structured_output, type_validation_enabled, retrieval_top_k,
    retrieval_min_chunks, token_budget, di_conf_threshold,
):
    """Extract every pending contract under one global token budget.

    All model calls from all contracts share a single work queue throttled by an
    in-flight *token* budget (see :mod:`contract_intelligence.gold.scheduler`)
    rather than a fixed per-contract thread count. Calls run in three successive
    budget stages whose boundaries are the pipeline's only dependency edges:

      A. **extraction** -- one call per single-call group (tables groups read the
         contract's structured tables, RAG / retrieve-classify groups retrieve
         their chunks, else full text) plus one call per chunk for map-reduce
         list groups;
      B. **reduce** -- per map-reduce field, a deterministic cross-chunk union
         (plus an optional LLM consolidation when ``reduce == "llm"``);
      C. **judge** -- one faithfulness/relevance call per contract.

    A tables group with no extracted tables, retrieval, and a
    sub-``retrieval_min_chunks`` result all fall back to full text, so recall
    never drops below the pre-retrieval baseline. Returns a list of
    ``(values, error, truncated, field_rows)`` aligned to ``pending_rows``.
    """
    mr_fields = groups.get(strat.MAP_REDUCE, [])
    contexts = [
        {
            "row": r,
            "text": r["extracted_text"],
            "values": {},
            "evidence": {},
            "chunk_ids_by_field": {},
            "map_partials": [],
            "map_chunk_ids": [],
            "errors": [],
            "verdicts": {},
            "judge_error": None,
        }
        for r in pending_rows
    ]

    # ---- Stage A: extraction (single-call groups + per-chunk map calls) ----
    a_tasks, a_meta = [], []
    for ci, ctx in enumerate(contexts):
        rel = ctx["row"]["relative_path"]
        full_text = ctx["text"]
        for stg, gfields in groups.items():
            if stg == strat.MAP_REDUCE:
                chunks = chunks_by_path.get(rel) or []
                if not chunks:
                    # No chunks indexed -> one full-text call for the group.
                    a_meta.append(
                        {
                            "ctx": ci,
                            "kind": "single",
                            "field_names": [f["field_name"] for f in gfields],
                            "chunk_ids": None,
                        }
                    )
                    a_tasks.append(
                        (
                            _est_tokens(full_text[:max_chars]),
                            _mk_single(
                                client, model, gfields, full_text, max_chars,
                                structured_output,
                            ),
                        )
                    )
                    continue
                ctx["map_chunk_ids"] = [c["chunk_id"] for c in chunks]
                for c in chunks:
                    a_meta.append({"ctx": ci, "kind": "map"})
                    a_tasks.append(
                        (
                            _est_tokens(c.get("text") or ""),
                            _mk_single(
                                client, model, gfields, c.get("text") or "",
                                max_chars, structured_output,
                            ),
                        )
                    )
                continue

            # Single-call groups: tables read the contract's structured tables,
            # RAG / retrieve-classify retrieve chunks, else full text.
            used_cids = None
            if stg == strat.TABLES:
                tbls = tables_by_path.get(rel) or []
                context_text = _render_tables(tbls) if tbls else full_text
            elif (
                stg in (strat.RETRIEVE_CLASSIFY, strat.RAG)
                and search_client is not None
            ):
                rchunks, cids = retrieval.retrieve_group_chunks(
                    search_client, gfields, rel, retrieval_top_k
                )
                if len(rchunks) >= retrieval_min_chunks:
                    context_text = "\n\n".join(
                        c["text"] for c in rchunks if c.get("text")
                    )
                    used_cids = cids
                else:
                    context_text = full_text  # fallback: no recall regression
            else:
                context_text = full_text
            a_meta.append(
                {
                    "ctx": ci,
                    "kind": "single",
                    "field_names": [f["field_name"] for f in gfields],
                    "chunk_ids": used_cids,
                }
            )
            a_tasks.append(
                (
                    _est_tokens(context_text[:max_chars]),
                    _mk_single(
                        client, model, gfields, context_text, max_chars,
                        structured_output,
                    ),
                )
            )

    for meta, res in zip(a_meta, scheduler.run_token_budget(a_tasks, token_budget)):
        ctx = contexts[meta["ctx"]]
        values, evidence, error = res
        if error:
            ctx["errors"].append(error)
            continue
        if meta["kind"] == "single":
            ctx["values"].update(values)
            ctx["evidence"].update(evidence)
            cids = meta.get("chunk_ids")
            if cids:
                for fn in meta["field_names"]:
                    ctx["chunk_ids_by_field"][fn] = cids
        else:  # map partial
            ctx["map_partials"].append((values, evidence))

    # ---- Stage B: reduce map-reduce fields (deterministic union + optional LLM) ----
    b_tasks, b_meta = [], []
    for ci, ctx in enumerate(contexts):
        # Skip contexts that errored, have no list fields, or whose map-reduce
        # group fell back to a single full-text call (no per-chunk partials) --
        # the latter already populated values in Stage A, so unioning an empty
        # partial set must not wipe them.
        if ctx["errors"] or not mr_fields or not ctx["map_partials"]:
            continue
        cids = ctx["map_chunk_ids"] or None
        for f in mr_fields:
            name = f["field_name"]
            union_items, quote = _union_partials(ctx["map_partials"], name, f)
            ctx["values"][name] = _coerce(union_items, f) if union_items else None
            ctx["evidence"][name] = quote
            if cids:
                ctx["chunk_ids_by_field"][name] = cids
            if union_items and (f.get("reduce") or "").lower() == "llm":
                b_meta.append({"ctx": ci, "field": f})
                b_tasks.append(
                    (
                        _est_tokens(json.dumps(union_items)),
                        _mk_reduce_llm(client, model, f, union_items),
                    )
                )

    for meta, res in zip(b_meta, scheduler.run_token_budget(b_tasks, token_budget)):
        cleaned, err = res
        if err or cleaned is None:
            continue  # keep the deterministic union
        ctx = contexts[meta["ctx"]]
        ctx["values"][meta["field"]["field_name"]] = _coerce(cleaned, meta["field"])

    # ---- Stage C: judge (one call per contract, all fields) ----
    if judge_enabled:
        c_tasks, c_meta = [], []
        for ci, ctx in enumerate(contexts):
            if ctx["errors"]:
                continue
            extractions = {
                f["field_name"]: {
                    "value": _stringify_value(ctx["values"].get(f["field_name"])),
                    "evidence": ctx["evidence"].get(f["field_name"]),
                }
                for f in fields
            }
            c_meta.append(ci)
            c_tasks.append(
                (
                    _est_tokens(ctx["text"][:max_chars]),
                    _mk_judge(
                        client, judge_model, fields, extractions, ctx["text"],
                        max_chars,
                    ),
                )
            )
        for ci, res in zip(c_meta, scheduler.run_token_budget(c_tasks, token_budget)):
            verdicts, judge_error = res
            contexts[ci]["verdicts"] = verdicts
            contexts[ci]["judge_error"] = judge_error

    # ---- Finalize (locate + validate + derive_trust) per contract ----
    results = []
    for ctx in contexts:
        error = "; ".join(ctx["errors"]) if ctx["errors"] else None
        rel = ctx["row"]["relative_path"]
        di_quality_flag = ctx["row"].asDict().get("di_quality_flag")
        results.append(
            _finalize_contract(
                fields, strategies, ctx["text"], ctx["values"], ctx["evidence"],
                ctx["chunk_ids_by_field"], ctx["verdicts"], ctx["judge_error"],
                judge_enabled, max_chars, fuzzy_threshold, evidence_max_chars,
                type_validation_enabled, blocks_by_path.get(rel),
                di_quality_flag, di_conf_threshold, error,
            )
        )
    return results


def _render_tables(tables):
    """Render a contract's structured tables as labelled markdown for the
    tables-first extraction context (one block per table, page-tagged)."""
    parts = []
    for t in tables:
        page = t.get("page")
        label = f"[Table {t.get('table_index')}"
        label += f", page {page}]" if page is not None else "]"
        parts.append(f"{label}\n{t.get('markdown') or ''}")
    return "\n\n".join(parts)


def _load_tables_by_path(spark, silver_tables_path, cfg, pending_paths):
    """Load silver structured tables (rendered markdown) for the pending
    contracts, grouped by ``relative_path`` and ordered by ``table_index``, for
    tables-first extraction.

    Reads the *current*, non-deleted rows of the silver tables table once on the
    driver. Returns ``{relative_path: [{table_index, page, markdown}, ...]}``, or
    an empty dict if the table is absent (callers then use full text)."""
    tables_table = cfg.get("gold", {}).get("silver_tables_table", "dbo/contract_tables")
    tables_path = f"{silver_tables_path}/{tables_table}"
    try:
        df = spark.read.format("delta").load(tables_path)
    except Exception as e:  # noqa: BLE001 - no tables -> full-text fallback
        print(f"[gold] tables: source unavailable ({e}); using full text.")
        return {}
    rows = (
        df.where(
            (F.col("is_current") == True)  # noqa: E712
            & (F.col("doc_deleted") == False)  # noqa: E712
            & F.col("relative_path").isin(pending_paths)
        )
        .select("relative_path", "table_index", "page", "markdown")
        .orderBy("relative_path", "table_index")
        .collect()
    )
    by_path = {}
    for r in rows:
        by_path.setdefault(r["relative_path"], []).append(
            {
                "table_index": r["table_index"],
                "page": r["page"],
                "markdown": r["markdown"],
            }
        )
    return by_path


def _load_chunks_by_path(spark, silver_tables_path, cfg, pending_paths):
    """Load silver per-chunk text for the pending contracts, grouped by
    ``relative_path`` and ordered by ``chunk_index``, for map-reduce extraction.

    Reads the *current*, non-deleted rows of the silver chunks table once on the
    driver. Returns ``{relative_path: [{chunk_id, text, chunk_index}, ...]}``, or
    an empty dict if the table is absent (callers then use full text)."""
    chunks_table = cfg.get("gold", {}).get("silver_chunks_table", "dbo/contract_chunks")
    chunks_path = f"{silver_tables_path}/{chunks_table}"
    try:
        df = spark.read.format("delta").load(chunks_path)
    except Exception as e:  # noqa: BLE001 - no chunks -> full-text fallback
        print(f"[gold] map-reduce: chunks unavailable ({e}); using full text.")
        return {}
    rows = (
        df.where(
            (F.col("is_current") == True)  # noqa: E712
            & (F.col("doc_deleted") == False)  # noqa: E712
            & F.col("relative_path").isin(pending_paths)
        )
        .select("relative_path", "chunk_id", "text", "chunk_index")
        .orderBy("relative_path", "chunk_index")
        .collect()
    )
    by_path = {}
    for r in rows:
        by_path.setdefault(r["relative_path"], []).append(
            {
                "chunk_id": r["chunk_id"],
                "text": r["text"],
                "chunk_index": r["chunk_index"],
            }
        )
    return by_path


def _load_blocks_by_path(spark, silver_tables_path, cfg, pending_paths):
    """Load silver semantic blocks (with DI confidence) for the pending
    contracts, grouped by ``relative_path`` and ordered by ``block_index``.

    Used by the Source->DI confidence gate (Plan 02): each extracted field's
    evidence quote is located within these blocks to read the DI ``conf_min`` of
    the span it came from and the source ``page``. Reads the *current*,
    non-deleted rows once on the driver. Returns
    ``{relative_path: [{page, text, conf_min}, ...]}`` (empty if the table or the
    ``conf_min`` column is absent -- the gate then degrades to ``None``)."""
    blocks_table = cfg.get("gold", {}).get("silver_blocks_table", "dbo/contract_blocks")
    blocks_path = f"{silver_tables_path}/{blocks_table}"
    try:
        df = spark.read.format("delta").load(blocks_path)
    except Exception as e:  # noqa: BLE001 - no blocks -> confidence gate disabled
        print(f"[gold] DI confidence: blocks unavailable ({e}); gate disabled.")
        return {}
    has_conf = "conf_min" in df.columns
    df = df.where(
        (F.col("is_current") == True)  # noqa: E712
        & (F.col("doc_deleted") == False)  # noqa: E712
        & F.col("relative_path").isin(pending_paths)
    )
    if not has_conf:
        # Pre-Plan-02 blocks table (no confidence persisted yet): expose the
        # rows but with null confidence so the gate is a clean no-op.
        df = df.withColumn("conf_min", F.lit(None).cast("double"))
    rows = (
        df.select("relative_path", "block_index", "page", "text", "conf_min")
        .orderBy("relative_path", "block_index")
        .collect()
    )
    by_path = {}
    for r in rows:
        by_path.setdefault(r["relative_path"], []).append(
            {
                "page": r["page"],
                "text": r["text"],
                "conf_min": r["conf_min"],
            }
        )
    return by_path


def _block_confidence_for_quote(quote, blocks):
    """Find the DI confidence and source page backing one evidence quote.

    Locates the first block whose (whitespace-normalised) text contains the
    (whitespace-normalised) quote and returns ``(conf_min, page)`` for it.
    Returns ``(None, None)`` when there is no quote, no blocks, or no containing
    block -- the confidence gate then leaves ``source_verified`` unset."""
    if not quote or not blocks:
        return None, None
    needle = " ".join(quote.split()).lower()
    if not needle:
        return None, None
    for b in blocks:
        text = b.get("text")
        if not text:
            continue
        if needle in " ".join(text.split()).lower():
            return b.get("conf_min"), b.get("page")
    return None, None


# ---------------------------------------------------------------------------
# Phase 2 vision stage (Plan 02 verify / Plan 03 correct)
# ---------------------------------------------------------------------------

# Most-uncertain-first ranks used to order (and cap) the per-contract correction
# set in correct mode (Plan 03 §4.1).
_TRUST_UNCERTAINTY = {
    ev.TRUST_LOW: 0,
    ev.TRUST_REVIEW: 1,
    ev.TRUST_UNKNOWN: 2,
    ev.TRUST_HIGH: 3,
}
_MATCH_UNCERTAINTY = {
    ev.MATCH_NOT_FOUND: 0,
    ev.MATCH_FUZZY: 1,
    ev.MATCH_NA_NULL: 2,
    ev.MATCH_NORMALIZED: 3,
    ev.MATCH_EXACT: 4,
    ev.MATCH_VISION_PAGE: 3,
}


def _vision_escalates(fr, di_quality_flag, di_conf_threshold):
    """The escalation gate: is this field uncertain enough to re-read its source
    page? A fuzzy / not-found quote, a ``review`` / ``low`` trust, a
    low-confidence DI evidence span, or a poorly-scanned document all qualify
    (Plan 02 §4.1, reused by Plan 03)."""
    conf = fr["evidence_di_confidence"]
    return (
        fr["match_type"] in (ev.MATCH_FUZZY, ev.MATCH_NOT_FOUND)
        or fr["trust"] in (ev.TRUST_REVIEW, ev.TRUST_LOW)
        or (conf is not None and conf < di_conf_threshold)
        or di_quality_flag == "low"
    )


def _vision_uncertainty_key(fr):
    """Sort key (most-uncertain first) for capping corrections per contract."""
    conf = fr["evidence_di_confidence"]
    return (
        _TRUST_UNCERTAINTY.get(fr["trust"], 1),
        _MATCH_UNCERTAINTY.get(fr["match_type"], 2),
        conf if conf is not None else 1.0,
    )


def _render_first_page(file_path, pages, cache):
    """Render the first of ``pages`` that yields an image (best-effort), caching
    per ``(file_path, page)``. Returns ``(png_bytes, page_number)`` or
    ``(None, None)`` when none render."""
    for page in pages:
        key = (file_path, page)
        if key not in cache:
            cache[key] = vision.render_page_png(file_path, page)
        png = cache[key]
        if png:
            return png, page
    return None, None


def _run_vision_stage(
    client, search_client, fields, pending_rows, results, *,
    bronze_files_dir, mode, vision_model, vision_max_chars, correct_max_fields,
    page_scan_limit, di_conf_threshold, judge_enabled, judge_model, max_chars,
    fuzzy_threshold, evidence_max_chars, retrieval_top_k,
):
    """Re-read source pages for escalated fields (verify / correct), in place.

    Per contract: select the escalated fields; resolve each one's source page(s)
    (known evidence page -> retrieval page -> bounded scan); render the first that
    succeeds; run vision *verify* against the existing value. In ``correct`` mode,
    when verify can't confirm, ``correct_field`` re-reads the value straight off
    the page, the new value is re-located in silver text (or marked
    ``vision_page`` when the re-judge agrees, §9.1), and all corrected fields of
    the contract are re-judged in a single batched judge call before trust is
    re-derived. Mutates the field rows inside ``results``; returns nothing."""
    field_defs = {f["field_name"]: f for f in fields}
    questions = {f["field_name"]: f.get("question", f["field_name"]) for f in fields}
    page_cache = {}      # (file_path, page) -> png bytes | None
    verified_n = corrected_n = 0

    for r, (_v, error, _t, field_rows) in zip(pending_rows, results):
        if error or not field_rows:
            continue
        rel = r["relative_path"]
        text = r["extracted_text"]
        di_quality_flag = r.asDict().get("di_quality_flag")
        file_path = f"{bronze_files_dir}/{rel}"

        candidates = [
            fr for fr in field_rows
            if _vision_escalates(fr, di_quality_flag, di_conf_threshold)
        ]
        # The correction set is bounded per contract (Plan 03 §4.1); verify mode
        # keeps Plan 02 behaviour (every escalated field, no cap).
        if mode == vision.MODE_CORRECT and correct_max_fields >= 0:
            candidates = sorted(candidates, key=_vision_uncertainty_key)[
                :correct_max_fields
            ]

        corrected = []   # field rows whose value vision replaced (need re-judge)
        for fr in candidates:
            has_value = fr["value"] is not None
            # verify needs an existing value to confirm; only correct can fill in
            # a missing one.
            if not has_value and mode != vision.MODE_CORRECT:
                continue
            question = questions.get(fr["field_name"], fr["field_name"])

            # Page selection: known evidence page, else the pages carried by the
            # field's top retrieved chunks, else a bounded scan.
            retrieval_pages = []
            if fr["evidence_page"] is None and search_client is not None:
                retrieval_pages = [
                    c.get("page")
                    for c in retrieval.retrieve_chunks(
                        search_client, question, rel, retrieval_top_k
                    )
                ]
            pages = vision.resolve_pages(
                fr["evidence_page"], retrieval_pages, page_scan_limit
            )
            png, used_page = _render_first_page(file_path, pages, page_cache)
            if png is None:
                continue

            # ---- verify (both modes; skipped for a still-missing value) ----
            verdict = vision.VERDICT_UNCLEAR
            if has_value:
                verdict, rationale = vision.verify_field(
                    client, vision_model, png, question, fr["value"],
                    max_chars=vision_max_chars,
                )
                fr["vision_verdict"] = verdict
                fr["vision_rationale"] = rationale
                fr["vision_action"] = vision.MODE_VERIFY
                fr["source_verified"] = vision.verdict_to_source_verified(
                    verdict, fr["source_verified"]
                )
                fr["trust"] = ev.derive_trust(
                    fr["value"], fr["match_type"], fr["judge_verdict"],
                    fr["judge_error"], judge_enabled, fr["type_validation"],
                    fr["source_verified"],
                )
                verified_n += 1

            # ---- correct (Plan 03; only when verify can't confirm) ----
            if mode == vision.MODE_CORRECT and verdict != vision.VERDICT_CONFIRMED:
                corr = vision.correct_field(
                    client, vision_model, png, question, used_page,
                    max_chars=vision_max_chars,
                )
                if corr and corr["verdict"] == vision.CORRECTION_OK and corr["value"]:
                    fr["original_value"] = fr["value"]
                    fr["value"] = corr["value"]
                    fr["value_source"] = "vision"
                    fr["vision_action"] = vision.MODE_CORRECT
                    if corr["rationale"]:
                        fr["vision_rationale"] = corr["rationale"]
                    fr["evidence_page"] = used_page
                    new_quote = corr["evidence"]
                    # Re-locate the corrected value's quote in silver text. When
                    # it isn't there (the premise is that DI text was wrong) it is
                    # a provisional not_found, upgraded to vision_page only if the
                    # re-judge confirms the value (Plan 03 §9.1 guardrail).
                    fr["match_type"] = (
                        ev.locate_evidence(new_quote, text, fuzzy_threshold)
                        if new_quote else ev.MATCH_NOT_FOUND
                    )
                    if new_quote and evidence_max_chars and len(new_quote) > evidence_max_chars:
                        new_quote = new_quote[:evidence_max_chars]
                    fr["evidence_text"] = new_quote
                    fr["source_verified"] = ev.SOURCE_CONFIRMED
                    corrected.append(fr)

        # ---- re-judge corrected fields (one batched call) + re-derive trust ----
        if corrected:
            rejudge = {}
            if judge_enabled:
                cfields = [field_defs[fr["field_name"]] for fr in corrected]
                extractions = {
                    fr["field_name"]: {
                        "value": fr["value"], "evidence": fr["evidence_text"]
                    }
                    for fr in corrected
                }
                try:
                    rejudge = ev.judge_fields(
                        client, judge_model, cfields, extractions, text, max_chars
                    )
                except Exception:  # noqa: BLE001 - non-fatal; keep prior signals
                    rejudge = {}
            for fr in corrected:
                v = rejudge.get(fr["field_name"], {})
                if v.get("verdict") is not None:
                    fr["judge_verdict"] = v.get("verdict")
                    fr["judge_rationale"] = v.get("rationale")
                # Page-as-evidence: an unlocatable corrected quote counts as
                # located only when the re-judge confirms the value (§9.1).
                if (
                    fr["match_type"] == ev.MATCH_NOT_FOUND
                    and fr["judge_verdict"] == ev.VERDICT_CORRECT
                ):
                    fr["match_type"] = ev.MATCH_VISION_PAGE
                fr["trust"] = ev.derive_trust(
                    fr["value"], fr["match_type"], fr["judge_verdict"],
                    fr["judge_error"], judge_enabled, fr["type_validation"],
                    fr["source_verified"],
                )
                corrected_n += 1

    if verified_n or corrected_n:
        print(
            f"[gold] vision stage ({mode}): verified {verified_n}, "
            f"corrected {corrected_n} field(s)."
        )


def run(spark, notebookutils, config=None, silver_tables_path=None, force_paths=None,
        bronze_files_dir=None):
    """Extract structured comparison fields from silver text into the gold table.

    Args:
        spark: Active Spark session.
        notebookutils: Fabric notebook utilities (used for AI auth + config read).
        config: Environment config dict; uses ``gold`` and ``azure_openai`` sections.
        silver_tables_path: ABFS path to the silver lakehouse ``Tables`` folder
            (silver is mounted, not attached, so its tables are read by path).
        force_paths: Optional list of ``relative_path`` values (or the string
            "ALL") to force-reprocess even when unchanged; falls back to the
            ``reprocess.force_paths`` config key.
        bronze_files_dir: Optional path to the bronze ``Files`` area holding the
            original source documents. Required only for the Phase 2 vision stage
            (Plan 02 verify / Plan 03 correct): escalated fields render their
            source page from ``{bronze_files_dir}/{relative_path}``. When ``None``
            (or ``gold.vision.mode`` is ``off``) the vision stage is a clean
            no-op.
    """
    cfg = config or {}
    gold_cfg = cfg.get("gold", {})

    if not silver_tables_path:
        raise ValueError(
            "silver_tables_path is required; resolve it in the notebook after "
            "mounting the silver lakehouse."
        )

    fields_table = gold_cfg.get("fields_table", "contract_fields")
    fields_view = gold_cfg.get("fields_active_view", "contract_fields_active")
    evidence_table = gold_cfg.get("fields_evidence_table", "contract_field_evidence")
    evidence_view = gold_cfg.get(
        "fields_evidence_active_view", "contract_field_evidence_active"
    )
    silver_text_table = gold_cfg.get("silver_text_table", "dbo/contract_text")
    max_chars = int(gold_cfg.get("max_input_chars", _DEFAULT_MAX_INPUT_CHARS))
    # Evidence + trust knobs. judge_enabled=False degrades cleanly to extract +
    # locate only (trust derived from match_type alone, never ``high``).
    judge_enabled = bool(gold_cfg.get("judge_enabled", True))
    fuzzy_threshold = float(gold_cfg.get("fuzzy_threshold", 0.85))
    evidence_max_chars = int(gold_cfg.get("evidence_max_chars", 600))
    # Per-type structured output: build a strict JSON-schema response_format per
    # strategy group so the model returns correctly-typed values (boolean /
    # integer / array / struct) instead of freeform JSON. Degrades to a plain
    # json_object call if the deployment rejects json_schema. Toggle off with
    # gold.structured_output.
    structured_output = bool(gold_cfg.get("structured_output", True))
    # Per-type structural validation gate (see gold/validate.py): a value that
    # fails its declared-type/format check can never be 'high' trust. Toggle
    # with gold.type_validation_enabled.
    type_validation_enabled = bool(gold_cfg.get("type_validation_enabled", True))
    # Retrieval knobs (Phase 3). RAG / retrieve-classify groups pull the
    # ``retrieval_top_k`` most relevant chunks per field question from the
    # existing AI Search index; a group with fewer than ``retrieval_min_chunks``
    # hits falls back to full text so recall never regresses. ``token_budget``
    # caps the summed in-flight token cost across the global work queue (the
    # real concurrency throttle, replacing a fixed thread count) -- like the old
    # max_concurrency it is a pure runtime knob, NOT part of the code
    # fingerprint, so tuning it never forces reprocessing.
    retrieval_top_k = int(gold_cfg.get("retrieval_top_k", 6))
    retrieval_min_chunks = int(gold_cfg.get("retrieval_min_chunks", 3))
    token_budget = int(gold_cfg.get("token_budget", 500_000))
    # Source->DI confidence gate (Plan 02): an evidence span whose DI conf_min is
    # below this threshold (or whose document scored di_quality_flag == 'low') is
    # marked source_verified='low', which can never be 'high' trust. Part of the
    # code fingerprint so tuning it re-derives trust over existing contracts.
    evidence_di_confidence_threshold = float(
        gold_cfg.get("evidence_di_confidence_threshold", 0.7)
    )
    # Targeted vision verification & correction (Plan 02 -> Plan 03). The nested
    # gold.vision block drives an escalating, mutually-exclusive mode:
    #   off     -> no vision (clean no-op).
    #   verify  -> Plan 02: re-read the source page to confirm/contradict an
    #              existing value (folds into source_verified; never changes it).
    #   correct -> Plan 03: when verify can't confirm, re-read the value straight
    #              off the page image, re-locate + re-judge it, and re-derive
    #              trust -- the only path a base-'review' field can reach 'high'.
    # When mode != off and the bronze source files are reachable (bronze_files_dir)
    # the stage runs; otherwise it is a clean no-op. All gold.vision.* keys feed
    # the code fingerprint, so changing them re-derives trust over existing
    # contracts.
    vision_cfg = gold_cfg.get("vision", {}) or {}
    vision_mode = (vision_cfg.get("mode") or vision.MODE_OFF).strip().lower()
    if vision_mode not in vision._MODES:
        vision_mode = vision.MODE_OFF
    vision_max_chars = int(vision_cfg.get("max_chars", 4000))
    vision_correct_max_fields = int(vision_cfg.get("correct_max_fields", 4))
    vision_page_scan_limit = int(vision_cfg.get("page_scan_limit", 3))
    model = os.environ.get("MAIN_MODEL") or cfg.get("azure_openai", {}).get(
        "completion_model", "gpt-4.1"
    )
    # Vision model: explicit config override, else the silver DI vision model,
    # else the main (vision-capable) model.
    vision_model = (
        vision_cfg.get("model")
        or cfg.get("silver", {}).get("document_intelligence", {}).get("vision_model")
        or model
    )
    # Judge model defaults to the main model unless overridden.
    judge_model = (
        gold_cfg.get("judge_model")
        or os.environ.get("MAIN_MODEL")
        or cfg.get("azure_openai", {}).get("completion_model", "gpt-4.1")
    )

    fields = _load_fields(notebookutils, gold_cfg)
    # Group fields by extraction strategy; each group is one focused extraction
    # call (Phase 1: every group still sees the full text).
    groups = strat.group_fields(fields)
    strategies = {f["field_name"]: strat.resolve_strategy(f) for f in fields}
    schema = _build_schema(fields)
    silver_text_path = f"{silver_tables_path}/{silver_text_table}"

    # Reprocessing fingerprint: changes with this module's code, the evidence
    # module's code, the system / judge prompts, the field/question definitions,
    # the model, the input cap, or the evidence/judge knobs.
    current_code = code_fingerprint(
        [__file__, ev.__file__, strat.__file__, vld.__file__, retrieval.__file__,
         scheduler.__file__, vision.__file__],
        {
            "system_prompt": _SYSTEM_PROMPT,
            "judge_system_prompt": ev.JUDGE_SYSTEM_PROMPT,
            "judge_enabled": judge_enabled,
            "judge_model": judge_model,
            "fuzzy_threshold": fuzzy_threshold,
            "structured_output": structured_output,
            "type_validation_enabled": type_validation_enabled,
            "retrieval_top_k": retrieval_top_k,
            "retrieval_min_chunks": retrieval_min_chunks,
            "evidence_di_confidence_threshold": evidence_di_confidence_threshold,
            "vision_mode": vision_mode,
            "vision_model": vision_model,
            "vision_max_chars": vision_max_chars,
            "vision_correct_max_fields": vision_correct_max_fields,
            "vision_page_scan_limit": vision_page_scan_limit,
            "strategies": strategies,
            "fields": fields,
            "model": model,
            "max_input_chars": max_chars,
        },
    )
    print(
        f"[gold] silver={silver_text_path}, target={fields_table}, "
        f"evidence={evidence_table}, model={model}, judge={judge_model if judge_enabled else 'off'}, "
        f"fields={len(fields)}, groups={len(groups)}, code={current_code}"
    )

    # Active, successfully-extracted contracts only -- the *current* silver
    # version of each (SCD2 history rows excluded). Stamp each with the gold
    # code_hash and derive the version_id that identifies this extraction.
    # The gold version_id chains the silver row's own version_id so gold
    # re-extracts whenever the *silver text* changes -- whether from new raw
    # content (silver content_hash) or new silver extraction logic (silver
    # code_hash). Using the raw-file content_hash alone would miss the latter.
    silver_df = spark.read.format("delta").load(silver_text_path).where(
        F.col("is_current") == True  # noqa: E712
    )
    # Carry the document-level DI quality flag through to gold for the
    # Source->DI confidence gate. Older silver tables predate Plan 02 and lack
    # the column; default it to null so the gate degrades to a clean no-op.
    if "di_quality_flag" not in silver_df.columns:
        silver_df = silver_df.withColumn(
            "di_quality_flag", F.lit(None).cast("string")
        )
    source = (
        silver_df
        .where((F.col("doc_deleted") == False) & F.col("extracted_text").isNotNull())  # noqa: E712
        .select(
            "relative_path", "file_name", "content_hash", "extracted_text",
            "di_quality_flag",
            F.col("version_id").alias("silver_version_id"),
        )
        .withColumn("code_hash", F.lit(current_code))
        .withColumn(
            "version_id",
            F.expr(
                "substr(sha2(concat_ws('|', relative_path, silver_version_id, "
                "code_hash), 256), 1, 16)"
            ),
        )
    )

    # Determine which contracts need (re)extraction: a current contract version
    # whose version_id is not already the live gold row (new path, changed raw
    # content or silver extraction -> new silver_version_id, or a changed
    # prompt/code/model/field -> new gold code_hash).
    if spark.catalog.tableExists(fields_table):
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        existing = (
            spark.table(fields_table)
            .where(
                (F.col("is_current") == True)  # noqa: E712
                & F.col("extraction_error").isNull()  # failed rows auto-retry
            )
            .select("version_id")
            .distinct()
        )
        pending = source.join(existing, on="version_id", how="left_anti")
    else:
        pending = source

    # Force-reprocess selected (or all) active contracts even when their
    # version_id is unchanged; scd2_merge overwrites the matching live row.
    force_paths = resolve_force_paths(force_paths, cfg)
    pending = apply_force(source, pending, force_paths)

    pending_rows = pending.collect()

    if not pending_rows:
        print("No contracts require (re)field-extraction.")
    else:
        client = get_openai_client(notebookutils)
        extracted_at = datetime.now(timezone.utc)
        rows = []
        evidence_rows = []
        evidence_keys = []  # relative_paths that got evidence (no extract error)
        errors = 0

        # Build retrieval inputs once on the driver. Map-reduce list fields
        # consume the silver per-chunk table; tables-first fields consume the
        # silver structured-tables table; RAG / retrieve-classify fields query
        # the AI Search chunk index (each skipped, with full-text fallback, when
        # its source is absent / not configured).
        pending_paths = [r["relative_path"] for r in pending_rows]
        chunks_by_path = {}
        if strat.MAP_REDUCE in groups:
            chunks_by_path = _load_chunks_by_path(
                spark, silver_tables_path, cfg, pending_paths
            )
        tables_by_path = {}
        if strat.TABLES in groups:
            tables_by_path = _load_tables_by_path(
                spark, silver_tables_path, cfg, pending_paths
            )
        # Silver semantic blocks (with DI conf_min) back the Source->DI
        # confidence gate; loaded unconditionally since every field's evidence is
        # scored against them.
        blocks_by_path = _load_blocks_by_path(
            spark, silver_tables_path, cfg, pending_paths
        )
        needs_search = any(
            s in groups for s in (strat.RETRIEVE_CLASSIFY, strat.RAG)
        )
        search_client = retrieval.get_search_client(cfg) if needs_search else None

        # One global token-budget work queue spans every model call from every
        # contract (extraction -> reduce -> judge stages); a single OpenAI client
        # is safe to share across the pool's threads. ``_extract_all_contracts``
        # captures its own per-call failures and returns one
        # ``(values, error, truncated, field_rows)`` tuple per pending row, in
        # input order.
        results = _extract_all_contracts(
            client, search_client, chunks_by_path, tables_by_path, blocks_by_path,
            fields, groups, strategies, pending_rows,
            model=model, judge_model=judge_model, max_chars=max_chars,
            fuzzy_threshold=fuzzy_threshold, evidence_max_chars=evidence_max_chars,
            judge_enabled=judge_enabled, structured_output=structured_output,
            type_validation_enabled=type_validation_enabled, retrieval_top_k=retrieval_top_k,
            retrieval_min_chunks=retrieval_min_chunks, token_budget=token_budget,
            di_conf_threshold=evidence_di_confidence_threshold,
        )

        # ---- Phase 2: vision verification & correction (Plan 02 -> Plan 03) ----
        # Re-read source pages for the small escalated subset of fields. ``verify``
        # confirms or contradicts an existing value (folds into source_verified);
        # ``correct`` (Plan 03) re-reads a wrong/missing value straight off the
        # page image when verify can't confirm, re-locates + re-judges it, and
        # re-derives trust. A clean no-op when mode is off or the bronze source
        # files are unreachable. In correct mode a search client is ensured (even
        # when no RAG group needs one) so unlocated fields get retrieval-first
        # page selection rather than only a bounded page scan.
        if vision_mode != vision.MODE_OFF and bronze_files_dir:
            vision_search_client = search_client or (
                retrieval.get_search_client(cfg)
                if vision_mode == vision.MODE_CORRECT else None
            )
            _run_vision_stage(
                client, vision_search_client, fields, pending_rows, results,
                bronze_files_dir=bronze_files_dir, mode=vision_mode,
                vision_model=vision_model, vision_max_chars=vision_max_chars,
                correct_max_fields=vision_correct_max_fields,
                page_scan_limit=vision_page_scan_limit,
                di_conf_threshold=evidence_di_confidence_threshold,
                judge_enabled=judge_enabled, judge_model=judge_model,
                max_chars=max_chars, fuzzy_threshold=fuzzy_threshold,
                evidence_max_chars=evidence_max_chars,
                retrieval_top_k=retrieval_top_k,
            )

        for r, (values, error, _truncated, field_rows) in zip(pending_rows, results):
            if error:
                errors += 1
            row = {
                "version_id": r["version_id"],
                "relative_path": r["relative_path"],
                "file_name": r["file_name"],
                "content_hash": r["content_hash"],
                "code_hash": current_code,
                "model": model,
                "extraction_error": error,
                "valid_from": extracted_at,
                "valid_to": None,
                "is_current": True,
                "doc_deleted": False,
            }
            for f in fields:
                row[f["field_name"]] = values.get(f["field_name"])
            rows.append(Row(**row))

            # Tall evidence rows only for contracts that extracted successfully
            # (errored contracts skip the tall write and auto-retry next run).
            if error:
                continue
            evidence_keys.append(r["relative_path"])
            for fr in field_rows:
                evidence_rows.append(
                    Row(
                        evidence_id=_evidence_id(r["version_id"], fr["field_name"]),
                        version_id=r["version_id"],
                        relative_path=r["relative_path"],
                        file_name=r["file_name"],
                        field_name=fr["field_name"],
                        extraction_strategy=fr["extraction_strategy"],
                        retrieved_chunk_ids=fr["retrieved_chunk_ids"],
                        value=fr["value"],
                        evidence_text=fr["evidence_text"],
                        match_type=fr["match_type"],
                        type_validation=fr["type_validation"],
                        judge_verdict=fr["judge_verdict"],
                        judge_rationale=fr["judge_rationale"],
                        judge_error=fr["judge_error"],
                        evidence_di_confidence=fr["evidence_di_confidence"],
                        evidence_page=fr["evidence_page"],
                        source_verified=fr["source_verified"],
                        vision_verdict=fr["vision_verdict"],
                        vision_rationale=fr["vision_rationale"],
                        vision_action=fr["vision_action"],
                        value_source=fr["value_source"],
                        original_value=fr["original_value"],
                        trust=fr["trust"],
                        contract_truncated=fr["contract_truncated"],
                        model=model,
                        judge_model=judge_model if judge_enabled else None,
                        valid_from=extracted_at,
                        valid_to=None,
                        is_current=True,
                        doc_deleted=False,
                    )
                )

        source_df = spark.createDataFrame(rows, schema=schema)

        # SCD2 upsert: a changed version expires the prior current row and
        # inserts the new one; full version history is retained for comparison.
        scd2_merge(
            spark,
            fields_table,
            source_df,
            key="relative_path",
            now=extracted_at,
        )
        print(f"Wrote {source_df.count()} new contract version(s) to '{fields_table}'.")

        ok = len(rows) - errors
        print(f"Extracted fields for {ok} of {len(rows)} contract(s).")
        if errors:
            print(f"  {errors} contract(s) failed; see 'extraction_error' column.")

        # Tall evidence table: expire the prior current rows of each reprocessed
        # contract and append the new per-field versions (many current rows per
        # key, like ``contract_chunks``).
        if evidence_rows:
            evidence_df = spark.createDataFrame(evidence_rows, schema=_EVIDENCE_SCHEMA)
            scd2_expire_and_append(
                spark,
                evidence_table,
                evidence_df,
                evidence_keys,
                key="relative_path",
                now=extracted_at,
            )
            print(
                f"Wrote {evidence_df.count()} evidence row(s) to '{evidence_table}' "
                f"for {len(evidence_keys)} contract(s)."
            )

    # Tombstone the current gold version of contracts no longer active in silver.
    # Driven by the *active* silver set so gold self-heals when contracts are
    # deleted or re-keyed. History rows (is_current = false) are left untouched.
    if spark.catalog.tableExists(fields_table):
        silver_active = spark.read.format("delta").load(silver_text_path).where(
            F.col("is_current") == True  # noqa: E712
        )
        active_paths = [
            r["relative_path"]
            for r in silver_active
            .where(F.col("doc_deleted") == False)  # noqa: E712
            .select("relative_path")
            .distinct()
            .collect()
        ]
        now_ts = locals().get("extracted_at") or datetime.now(timezone.utc)
        tgt = DeltaTable.forName(spark, fields_table)
        tgt.update(
            condition=(~F.col("relative_path").isin(active_paths))
            & (F.col("is_current") == True)  # noqa: E712
            & (F.col("doc_deleted") == False),  # noqa: E712
            set={"doc_deleted": F.lit(True), "valid_to": F.lit(now_ts)},
        )
        print(f"Synced gold tombstones to {len(active_paths)} active contract(s).")

        spark.sql(
            f"CREATE OR REPLACE VIEW {fields_view} AS "
            f"SELECT * FROM {fields_table} "
            f"WHERE is_current = true AND doc_deleted = false"
        )
        active = spark.table(fields_view).count()
        print(f"View '{fields_view}' up to date: {active} active contract(s).")

    # Mirror the tombstone sync + active view onto the tall evidence table so it
    # self-heals when contracts leave silver. ``active_paths`` is reused from the
    # wide-table block above (same active silver set).
    if spark.catalog.tableExists(evidence_table):
        now_ts = locals().get("extracted_at") or datetime.now(timezone.utc)
        active_paths = locals().get("active_paths")
        if active_paths is None:
            silver_active = spark.read.format("delta").load(silver_text_path).where(
                F.col("is_current") == True  # noqa: E712
            )
            active_paths = [
                r["relative_path"]
                for r in silver_active
                .where(F.col("doc_deleted") == False)  # noqa: E712
                .select("relative_path")
                .distinct()
                .collect()
            ]
        ev_tgt = DeltaTable.forName(spark, evidence_table)
        ev_tgt.update(
            condition=(~F.col("relative_path").isin(active_paths))
            & (F.col("is_current") == True)  # noqa: E712
            & (F.col("doc_deleted") == False),  # noqa: E712
            set={"doc_deleted": F.lit(True), "valid_to": F.lit(now_ts)},
        )

        spark.sql(
            f"CREATE OR REPLACE VIEW {evidence_view} AS "
            f"SELECT * FROM {evidence_table} "
            f"WHERE is_current = true AND doc_deleted = false"
        )
        active_ev = spark.table(evidence_view).count()
        print(f"View '{evidence_view}' up to date: {active_ev} active evidence row(s).")

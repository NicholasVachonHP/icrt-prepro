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
from concurrent.futures import ThreadPoolExecutor
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
    StructField("value", StringType(), True),            # stringified wide value
    StructField("evidence_text", StringType(), True),    # verbatim quote
    StructField("match_type", StringType(), False),      # is the quote real?
    StructField("judge_verdict", StringType(), True),    # answers the question?
    StructField("judge_rationale", StringType(), True),
    StructField("judge_error", StringType(), True),      # non-fatal judge failure
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


def _extract_one(client, model, fields, text, max_chars):
    """Call the model for one contract.

    Returns ``(values_dict, evidence_dict, error_or_None)`` where ``values_dict``
    holds the type-coerced wide value per field and ``evidence_dict`` the verbatim
    supporting quote (string or ``None``) per field. The model now returns
    ``{"value": ..., "evidence": ...}`` per field; a bare scalar (no object) is
    tolerated as a value with no evidence.
    """
    prompt = _build_prompt(fields, text, max_chars)
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0,
        )
        data = json.loads(resp.choices[0].message.content)
    except Exception as e:  # noqa: BLE001 - record and continue with next contract
        return {}, {}, str(e)

    # Coerce each field to the declared type so it matches the Delta column, and
    # pull out the verbatim evidence quote.
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
    return values, evidence, None


def _process_one(
    client, model, judge_model, fields, text, max_chars,
    fuzzy_threshold, evidence_max_chars, judge_enabled,
):
    """Extract, locate evidence, and judge one contract.

    Returns ``(values, error, truncated, field_rows)``. ``field_rows`` is a list
    of per-field dicts (everything for the tall table except the identity /
    SCD2 columns the caller stamps on). On extraction error no field rows are
    produced (the contract auto-retries next run, like the wide table).
    """
    values, evidence, error = _extract_one(client, model, fields, text, max_chars)
    if error:
        return values, error, False, []

    truncated = len(text) > max_chars

    # Step 2: classify each quote against the *full* contract text.
    located = {}  # field_name -> (value_str, quote, match_type)
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
        located[name] = (value_str, quote, match_type)

    # Step 3: one judge call per contract (faithfulness + relevance).
    verdicts, judge_error = {}, None
    if judge_enabled:
        try:
            verdicts = ev.judge_fields(
                client,
                judge_model,
                fields,
                {
                    n: {"value": located[n][0], "evidence": located[n][1]}
                    for n in (f["field_name"] for f in fields)
                },
                text,
                max_chars,
            )
        except Exception as e:  # noqa: BLE001 - non-fatal; fall back to locate-only
            judge_error = str(e)

    field_rows = []
    for f in fields:
        name = f["field_name"]
        value_str, quote, match_type = located[name]
        verdict_obj = verdicts.get(name, {})
        verdict = verdict_obj.get("verdict")
        rationale = verdict_obj.get("rationale")
        trust = ev.derive_trust(
            value_str, match_type, verdict, judge_error, judge_enabled
        )
        if quote and evidence_max_chars and len(quote) > evidence_max_chars:
            quote = quote[:evidence_max_chars]
        field_rows.append(
            {
                "field_name": name,
                "value": value_str,
                "evidence_text": quote,
                "match_type": match_type,
                "judge_verdict": verdict,
                "judge_rationale": rationale,
                "judge_error": judge_error,
                "trust": trust,
                "contract_truncated": truncated,
            }
        )
    return values, None, truncated, field_rows


def run(spark, notebookutils, config=None, silver_tables_path=None, force_paths=None):
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
    # Number of contracts whose field extraction runs concurrently. Field
    # extraction is one chat completion per contract (I/O-bound), so a bounded
    # thread pool turns a serial driver loop into parallel calls. Cap this at or
    # below the model deployment's requests-per-minute headroom. Not part of the
    # code fingerprint, so tuning it never forces reprocessing.
    max_concurrency = max(1, int(gold_cfg.get("max_concurrency", 8)))
    model = os.environ.get("MAIN_MODEL") or cfg.get("azure_openai", {}).get(
        "completion_model", "gpt-4.1"
    )
    # Judge model defaults to the main model unless overridden.
    judge_model = (
        gold_cfg.get("judge_model")
        or os.environ.get("MAIN_MODEL")
        or cfg.get("azure_openai", {}).get("completion_model", "gpt-4.1")
    )

    fields = _load_fields(notebookutils, gold_cfg)
    schema = _build_schema(fields)
    silver_text_path = f"{silver_tables_path}/{silver_text_table}"

    # Reprocessing fingerprint: changes with this module's code, the evidence
    # module's code, the system / judge prompts, the field/question definitions,
    # the model, the input cap, or the evidence/judge knobs.
    current_code = code_fingerprint(
        [__file__, ev.__file__],
        {
            "system_prompt": _SYSTEM_PROMPT,
            "judge_system_prompt": ev.JUDGE_SYSTEM_PROMPT,
            "judge_enabled": judge_enabled,
            "judge_model": judge_model,
            "fuzzy_threshold": fuzzy_threshold,
            "fields": fields,
            "model": model,
            "max_input_chars": max_chars,
        },
    )
    print(
        f"[gold] silver={silver_text_path}, target={fields_table}, "
        f"evidence={evidence_table}, model={model}, judge={judge_model if judge_enabled else 'off'}, "
        f"fields={len(fields)}, code={current_code}"
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
    source = (
        silver_df
        .where((F.col("doc_deleted") == False) & F.col("extracted_text").isNotNull())  # noqa: E712
        .select(
            "relative_path", "file_name", "content_hash", "extracted_text",
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

        # Fan the per-contract work (extract + locate + judge) out across a
        # bounded thread pool rather than one-at-a-time on the driver.
        # ``_process_one`` captures its own exceptions and returns an error
        # string, so no call escapes the pool; ``executor.map`` preserves input
        # order, and a single OpenAI client is safe to share across threads.
        workers = min(max_concurrency, len(pending_rows))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            results = list(
                pool.map(
                    lambda r: _process_one(
                        client, model, judge_model, fields, r["extracted_text"],
                        max_chars, fuzzy_threshold, evidence_max_chars,
                        judge_enabled,
                    ),
                    pending_rows,
                )
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
                        value=fr["value"],
                        evidence_text=fr["evidence_text"],
                        match_type=fr["match_type"],
                        judge_verdict=fr["judge_verdict"],
                        judge_rationale=fr["judge_rationale"],
                        judge_error=fr["judge_error"],
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

"""Gold-layer accuracy evaluation against a hand-authored ground truth.

This is *Stage 5* of the contract intelligence pipeline — a **quality gate**, not
a data-producing stage. It scores the structured fields written by
``gold/fields.py`` (the ``contract_fields`` table) against a small, hand-curated
ground-truth dataset (``config/eval/contract_truth.json``) derived from a human
contract review (``docs/Contract_Review.docx``).

Design intent
-------------
The daily pipeline is idempotent and ``code_hash``-driven: over unchanged
contracts with an unchanged prompt the gold output is identical day to day, so
re-scoring every morning tells you nothing new. This module is therefore meant
to be run **on demand** (a standalone notebook) whenever something that affects
extraction changes — ``gold/fields.py``, the system prompt,
``extraction_fields.json``, the model, or a contract's source text. It is a
**regression guard** for those changes, not part of the 04:00 ingestion run.

Matching is **type-aware**, because a naive ``==`` reports false failures on
free-text legal language:

================================  ============================================
Field kind                         Match strategy
================================  ============================================
boolean                            normalized boolean equality
integer / number                   numeric equality
date (``effective_date``)          ISO-date normalized equality
governing_law                      normalized string equality
free-text scalar (term_length,     fuzzy ratio + substring (``_fuzzy``)
  notice_period, contract_value,
  liability_cap)
list (parties, services_offered)   set precision / recall / F1 (``_set_f1``)
struct_list (service_level_        **LLM judge** — gpt-4.1 decides whether the
  agreements, notable_clauses)        extraction covers the expected obligations
================================  ============================================

Per-field strategies are declared in :data:`_FIELD_STRATEGY` (falling back to a
sensible default by declared type) and can be overridden per-field in the truth
file. Results are appended, run-stamped, to the ``gold_eval_results`` Delta table
in the gold lakehouse and summarised to stdout.
"""

import json
import os
import re
from datetime import datetime, timezone
from difflib import SequenceMatcher

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ..common.ai_clients import get_openai_client

# Pass thresholds for the non-exact strategies (score in [0, 1]).
_FUZZY_THRESHOLD = 0.72   # free-text scalar similarity
_SET_F1_THRESHOLD = 0.70  # list overlap F1
_JUDGE_THRESHOLD = 0.70   # LLM-judge coverage score

# Per-field override of the matching strategy. Anything not listed falls back to
# ``_default_strategy`` based on the field's declared type in
# extraction_fields.json. Keep this aligned with the gold field config.
_FIELD_STRATEGY = {
    "parties": "set_f1",
    "effective_date": "date",
    "term_length": "fuzzy",
    "auto_renewal": "bool",
    "notice_period": "fuzzy",
    "contract_value": "fuzzy",
    "payment_terms": "numeric",
    "governing_law": "string",
    "data_residency_uk": "bool",
    "liability_cap": "fuzzy",
    "confidentiality_clause": "bool",
    "services_offered": "set_f1",
    "service_level_agreements": "llm_judge",
    "notable_clauses": "llm_judge",
}

_RESULTS_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("evaluated_at", TimestampType(), False),
    StructField("relative_path", StringType(), True),
    StructField("file_name", StringType(), False),
    StructField("field_name", StringType(), False),
    StructField("strategy", StringType(), False),
    StructField("expected", StringType(), True),
    StructField("actual", StringType(), True),
    StructField("score", DoubleType(), False),
    StructField("passed", BooleanType(), False),
    StructField("rationale", StringType(), True),
    StructField("gold_code_hash", StringType(), True),
    StructField("gold_version_id", StringType(), True),
])

_JUDGE_SYSTEM_PROMPT = (
    "You are a precise contracts QA reviewer. You compare an EXPECTED answer "
    "(human ground truth) with an ACTUAL answer produced by an extraction model "
    "for one field of one contract. Judge whether ACTUAL adequately captures the "
    "substance of EXPECTED. Ignore wording, ordering and formatting differences; "
    "focus on whether the same facts/obligations are present. Penalise missing "
    "items and fabricated items. Respond with a single JSON object: "
    '{"score": <float 0..1>, "verdict": "pass|partial|fail", "rationale": "<short>"}.'
)


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

def _norm_text(v):
    """Lowercase, strip punctuation, collapse whitespace."""
    if v is None:
        return ""
    s = str(v).lower().strip()
    s = re.sub(r"[^\w\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _as_list(v):
    """Coerce a value to a list of strings (tolerates JSON strings / scalars)."""
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x) for x in v if x is not None]
    if isinstance(v, str):
        try:
            parsed = json.loads(v)
            if isinstance(parsed, list):
                return [str(x) for x in parsed if x is not None]
        except json.JSONDecodeError:
            pass
        return [p.strip() for p in re.split(r"[,\n;]", v) if p.strip()]
    return [str(v)]


def _to_bool(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("true", "1", "yes", "y"):
        return True
    if s in ("false", "0", "no", "n"):
        return False
    return None


def _to_date(v):
    """Best-effort normalize to an ISO date string (YYYY-MM-DD) or None."""
    if v is None:
        return None
    s = str(v).strip()
    m = re.search(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", s)
    if m:
        return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    for fmt in ("%d %B %Y", "%d %b %Y", "%B %d, %Y", "%b %d, %Y", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return s  # fall back to the raw string for a normalized compare


def _to_number(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    m = re.search(r"-?\d+(?:\.\d+)?", str(v).replace(",", ""))
    return float(m.group()) if m else None


def _stringify(v):
    if v is None:
        return None
    if isinstance(v, (list, dict)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


# ---------------------------------------------------------------------------
# Matchers — each returns (score: float, passed: bool, rationale: str|None)
# ---------------------------------------------------------------------------

def _match_bool(expected, actual):
    e, a = _to_bool(expected), _to_bool(actual)
    ok = e is not None and e == a
    return (1.0 if ok else 0.0), ok, None


def _match_numeric(expected, actual):
    e, a = _to_number(expected), _to_number(actual)
    ok = e is not None and a is not None and abs(e - a) < 1e-9
    return (1.0 if ok else 0.0), ok, None


def _match_date(expected, actual):
    e, a = _to_date(expected), _to_date(actual)
    ok = bool(e) and _norm_text(e) == _norm_text(a)
    return (1.0 if ok else 0.0), ok, None


def _match_string(expected, actual):
    e, a = _norm_text(expected), _norm_text(actual)
    if not e and not a:
        return 1.0, True, None
    ok = e == a
    return (1.0 if ok else 0.0), ok, None


def _match_fuzzy(expected, actual):
    e, a = _norm_text(expected), _norm_text(actual)
    if not e and not a:
        return 1.0, True, None
    if not e or not a:
        return 0.0, False, None
    ratio = SequenceMatcher(None, e, a).ratio()
    if e in a or a in e:
        ratio = max(ratio, 0.9)
    return ratio, ratio >= _FUZZY_THRESHOLD, None


def _match_set_f1(expected, actual):
    exp = {_norm_text(x) for x in _as_list(expected) if _norm_text(x)}
    act = {_norm_text(x) for x in _as_list(actual) if _norm_text(x)}
    if not exp and not act:
        return 1.0, True, "both empty"
    if not exp or not act:
        return 0.0, False, "one side empty"

    def _hit(e):
        return any(e == a or e in a or a in e for a in act)

    def _hit_rev(a):
        return any(a == e or a in e or e in a for e in exp)

    tp_e = sum(1 for e in exp if _hit(e))
    recall = tp_e / len(exp)
    tp_a = sum(1 for a in act if _hit_rev(a))
    precision = tp_a / len(act)
    f1 = 0.0 if (precision + recall) == 0 else 2 * precision * recall / (precision + recall)
    rationale = f"precision={precision:.2f} recall={recall:.2f} f1={f1:.2f}"
    return f1, f1 >= _SET_F1_THRESHOLD, rationale


def _match_llm_judge(client, model, field_name, question, expected, actual):
    prompt = (
        f"FIELD: {field_name}\n"
        f"QUESTION: {question}\n\n"
        f"EXPECTED (ground truth):\n{_stringify(expected)}\n\n"
        f"ACTUAL (model extraction):\n{_stringify(actual)}\n\n"
        "Return the JSON verdict."
    )
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": _JUDGE_SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0,
        )
        data = json.loads(resp.choices[0].message.content)
        score = float(data.get("score", 0.0))
        score = max(0.0, min(1.0, score))
        rationale = f"{data.get('verdict', '?')}: {data.get('rationale', '')}".strip()
        return score, score >= _JUDGE_THRESHOLD, rationale
    except Exception as e:  # noqa: BLE001 - record and continue
        return 0.0, False, f"judge_error: {e}"


# ---------------------------------------------------------------------------
# Config / data loading
# ---------------------------------------------------------------------------

def _load_json(notebookutils, rel):
    shared = notebookutils.fs.getMountPath("/shared_code")
    with open(f"{shared}/{rel}") as f:
        return json.load(f)


def _load_fields(notebookutils, gold_cfg):
    rel = gold_cfg.get("fields_config", "config/extraction_fields.json")
    return _load_json(notebookutils, rel)


def _default_strategy(field):
    ftype = (field.get("type") or "string").lower()
    return {
        "boolean": "bool",
        "integer": "numeric",
        "number": "numeric",
        "list": "set_f1",
        "struct_list": "llm_judge",
    }.get(ftype, "string")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(spark, notebookutils, config=None, truth_path=None):
    """Score ``contract_fields`` against the ground-truth dataset.

    Args:
        spark: Active Spark session.
        notebookutils: Fabric notebook utilities (config read + AI auth).
        config: Environment config dict; uses ``gold`` and ``eval`` sections.
        truth_path: Optional override for the ground-truth JSON path, relative to
            the shared lakehouse ``Files`` (defaults to ``eval.truth_config`` or
            ``config/eval/contract_truth.json``).

    Returns:
        A summary dict: overall accuracy, per-field accuracy, per-contract
        accuracy, and pass/total counts.
    """
    cfg = config or {}
    gold_cfg = cfg.get("gold", {})
    eval_cfg = cfg.get("eval", {})

    fields_table = gold_cfg.get("fields_table", "contract_fields")
    results_table = eval_cfg.get("results_table", "gold_eval_results")
    join_key = eval_cfg.get("join_key", "file_name")  # truth is keyed by file_name
    truth_rel = truth_path or eval_cfg.get(
        "truth_config", "config/eval/contract_truth.json"
    )
    model = os.environ.get("MAIN_MODEL") or cfg.get("azure_openai", {}).get(
        "completion_model", "gpt-4.1"
    )

    fields = _load_fields(notebookutils, gold_cfg)
    fields_by_name = {f["field_name"]: f for f in fields}
    truth = _load_json(notebookutils, truth_rel)
    if not truth:
        raise ValueError(f"Ground-truth dataset '{truth_rel}' is empty.")

    # Live gold rows only (current, not tombstoned).
    gold_df = (
        spark.table(fields_table)
        .where((F.col("is_current") == True) & (F.col("doc_deleted") == False))  # noqa: E712
    )
    gold_rows = {}
    for r in gold_df.collect():
        d = r.asDict(recursive=True)
        gold_rows[_norm_text(d.get(join_key))] = d

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    evaluated_at = datetime.now(timezone.utc)
    client = None  # lazily created only if an LLM-judge field is scored

    out_rows = []
    missing = []
    for contract in truth:
        key_val = contract.get(join_key) or contract.get("file_name")
        gold = gold_rows.get(_norm_text(key_val))
        expected_fields = contract.get("fields", {})
        if gold is None:
            missing.append(key_val)
            print(f"⚠ no live gold row for {join_key}='{key_val}' — skipped")
            continue

        for field_name, expected in expected_fields.items():
            fdef = fields_by_name.get(field_name, {})
            strategy = contract.get("strategy_overrides", {}).get(field_name) or \
                _FIELD_STRATEGY.get(field_name) or _default_strategy(fdef)
            actual = gold.get(field_name)

            if strategy == "bool":
                score, passed, rationale = _match_bool(expected, actual)
            elif strategy == "numeric":
                score, passed, rationale = _match_numeric(expected, actual)
            elif strategy == "date":
                score, passed, rationale = _match_date(expected, actual)
            elif strategy == "string":
                score, passed, rationale = _match_string(expected, actual)
            elif strategy == "fuzzy":
                score, passed, rationale = _match_fuzzy(expected, actual)
            elif strategy == "set_f1":
                score, passed, rationale = _match_set_f1(expected, actual)
            elif strategy == "llm_judge":
                if client is None:
                    client = get_openai_client(notebookutils)
                score, passed, rationale = _match_llm_judge(
                    client, model, field_name,
                    fdef.get("question", ""), expected, actual,
                )
            else:
                score, passed, rationale = _match_string(expected, actual)

            out_rows.append(Row(
                run_id=run_id,
                evaluated_at=evaluated_at,
                relative_path=gold.get("relative_path"),
                file_name=str(key_val),
                field_name=field_name,
                strategy=strategy,
                expected=_stringify(expected),
                actual=_stringify(actual),
                score=float(score),
                passed=bool(passed),
                rationale=rationale,
                gold_code_hash=gold.get("code_hash"),
                gold_version_id=gold.get("version_id"),
            ))

    if not out_rows:
        raise ValueError(
            "No fields were scored. Check that the truth file's join key "
            f"('{join_key}') matches live gold rows. Missing: {missing}"
        )

    results_df = spark.createDataFrame(out_rows, schema=_RESULTS_SCHEMA)
    results_df.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(results_table)

    # ---- Summary -----------------------------------------------------------
    total = len(out_rows)
    passed_n = sum(1 for r in out_rows if r["passed"])
    overall = passed_n / total if total else 0.0

    by_field = {}
    by_contract = {}
    for r in out_rows:
        by_field.setdefault(r["field_name"], []).append(r["passed"])
        by_contract.setdefault(r["file_name"], []).append(r["passed"])

    print(f"\n=== Gold eval {run_id} (model={model}) ===")
    print(f"Overall: {passed_n}/{total} fields passed ({overall:.1%})")
    if missing:
        print(f"Contracts with no live gold row (skipped): {missing}")

    print("\nPer-field accuracy:")
    for name in (f["field_name"] for f in fields):
        vals = by_field.get(name)
        if vals:
            print(f"  {name:<26} {sum(vals)}/{len(vals)}  ({sum(vals)/len(vals):.0%})")

    print("\nPer-contract accuracy:")
    for name, vals in by_contract.items():
        print(f"  {name:<40} {sum(vals)}/{len(vals)}  ({sum(vals)/len(vals):.0%})")

    print("\nFailures:")
    fails = [r for r in out_rows if not r["passed"]]
    if not fails:
        print("  (none)")
    for r in fails:
        print(
            f"  [{r['file_name']}] {r['field_name']} ({r['strategy']}, "
            f"score={r['score']:.2f})\n"
            f"      expected: {str(r['expected'])[:160]}\n"
            f"      actual:   {str(r['actual'])[:160]}"
            + (f"\n      judge:    {r['rationale']}" if r['rationale'] else "")
        )

    return {
        "run_id": run_id,
        "overall_accuracy": overall,
        "passed": passed_n,
        "total": total,
        "per_field": {k: sum(v) / len(v) for k, v in by_field.items()},
        "per_contract": {k: sum(v) / len(v) for k, v in by_contract.items()},
        "missing_contracts": missing,
        "results_table": results_table,
    }

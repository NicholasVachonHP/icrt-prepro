"""Evidence + trust signals for gold field extraction.

For every extracted field this module answers two independent questions and
fuses them into one categorical ``trust`` signal stored in the tall
``contract_field_evidence`` table:

1. **Is the quote real?** -- :func:`locate_evidence` matches the model's verbatim
   ``evidence`` quote against the *full* silver ``extracted_text`` (mechanical, no
   LLM). Catches hallucinated quotes.
2. **Does it answer the question?** -- :func:`judge_fields` asks an LLM judge,
   in a single call per contract, whether each ``(value, evidence)`` pair is both
   *faithful* to the evidence and *relevant* to the field's question. Catches a
   real quote that answers the wrong thing.

:func:`derive_trust` turns those two signals into ``high`` / ``review`` / ``low``
/ ``unknown`` -- no numeric confidence, no weights. The judge's own self-reported
certainty is intentionally **not** captured (it measures the judge's sureness of
its verdict, not the probability the value is correct).

The judge prompts and the fuzzy threshold feed the gold ``code_hash`` (via
``fields.run``), so editing them re-runs extraction over existing contracts.
"""

import json
from difflib import SequenceMatcher

from . import validate as vld

# match_type values, ordered best -> worst.
MATCH_EXACT = "exact"
MATCH_NORMALIZED = "normalized"
MATCH_FUZZY = "fuzzy"
MATCH_VISION_PAGE = "vision_page"   # image-backed corrected value (Plan 03)
MATCH_NOT_FOUND = "not_found"
MATCH_NA_NULL = "na_null"          # value is null -> no quote expected

# Located = the cited evidence is genuinely backed: found in silver text
# (exact/normalized/fuzzy) OR read straight off the source page image by a vision
# correction (vision_page, Plan 03 §9.1 -- assigned only when the re-judge agrees).
_LOCATED = (MATCH_EXACT, MATCH_NORMALIZED, MATCH_FUZZY, MATCH_VISION_PAGE)

# judge_verdict values.
VERDICT_CORRECT = "correct"
VERDICT_PARTIAL = "partial"
VERDICT_INCORRECT = "incorrect"
VERDICT_UNVERIFIABLE = "unverifiable"

# trust values.
TRUST_HIGH = "high"
TRUST_REVIEW = "review"
TRUST_LOW = "low"
TRUST_UNKNOWN = "unknown"

# source_verified values -- how well the source document backs the value at the
# OCR/vision layer (Plan 02). ``high`` = DI read the evidence span confidently;
# ``low`` = DI confidence was poor (or the document scored ``di_quality_flag ==
# 'low'``); ``confirmed`` = a vision pass re-read the source page and agreed.
# ``None`` means the signal was not computed.
SOURCE_HIGH = "high"
SOURCE_LOW = "low"
SOURCE_CONFIRMED = "confirmed"


# ---------------------------------------------------------------------------
# Step 2 -- is the quote real?
# ---------------------------------------------------------------------------

def _normalize(text):
    """Collapse all runs of whitespace and l-case, for whitespace/OCR-tolerant
    matching."""
    return " ".join(text.split()).lower()


def _best_fuzzy_ratio(nq, nt):
    """Bounded fuzzy similarity of a normalized quote ``nq`` against a normalized
    (possibly huge) text ``nt``.

    Full-document ``SequenceMatcher`` is O(len(nq) * len(nt)) and infeasible over
    million-char contracts, so anchor on a distinctive slice of the quote to find
    a candidate region cheaply, then score only a quote-sized window there.
    """
    if not nq or not nt:
        return 0.0
    anchor = nq[:40]
    idx = nt.find(anchor)
    if idx == -1 and len(nq) > 80:
        # Quote start may be garbled; try a mid-quote anchor.
        mid_start = len(nq) // 2
        mid = nq[mid_start:mid_start + 40]
        found = nt.find(mid) if mid else -1
        if found != -1:
            idx = max(0, found - mid_start)
    if idx == -1:
        return 0.0
    window = nt[idx:idx + len(nq) + 20]
    return SequenceMatcher(None, nq, window).ratio()


def locate_evidence(quote, full_text, fuzzy_threshold=0.85):
    """Classify how well ``quote`` is found in ``full_text``.

    Returns one of :data:`MATCH_EXACT`, :data:`MATCH_NORMALIZED`,
    :data:`MATCH_FUZZY` or :data:`MATCH_NOT_FOUND`. (The :data:`MATCH_NA_NULL`
    case -- a null value with no quote -- is decided by the caller, which knows
    the value.) Matching is against the *full* silver text, not the truncated
    prompt slice, so a quote is only ``not_found`` when it is genuinely absent --
    e.g. hallucinated, or from a clause past the model's input cap.
    """
    if not quote or not full_text:
        return MATCH_NOT_FOUND
    if quote in full_text:
        return MATCH_EXACT
    nq, nt = _normalize(quote), _normalize(full_text)
    if nq and nq in nt:
        return MATCH_NORMALIZED
    if _best_fuzzy_ratio(nq, nt) >= fuzzy_threshold:
        return MATCH_FUZZY
    return MATCH_NOT_FOUND


# ---------------------------------------------------------------------------
# Step 3 -- does value+evidence answer the field's question?
# ---------------------------------------------------------------------------

JUDGE_SYSTEM_PROMPT = (
    "You are a senior contracts reviewer auditing an automated extraction. For "
    "each field you are given the question that was asked, the extracted value, "
    "and the verbatim evidence quote the extractor cited. Judge whether the "
    "value, supported by that evidence, correctly answers the question. Apply two "
    "tests: (1) FAITHFULNESS -- is the value actually supported by the evidence "
    "quote, not invented or distorted; (2) RELEVANCE -- does the evidence and "
    "value respond to exactly what the question asked (e.g. the effective date, "
    "not some other date). Use the contract text only to corroborate. Respond "
    "with a single JSON object whose keys are exactly the field names; each value "
    'is an object {"verdict": one of "correct"|"partial"|"incorrect"|'
    '"unverifiable", "rationale": one short sentence}. Use "correct" only when '
    'both tests pass, "partial" when partially supported or only partially on-'
    'point, "incorrect" when the evidence contradicts the value or answers a '
    'different question, and "unverifiable" when you cannot tell from the '
    "available text."
)


def build_judge_prompt(fields, extractions, text, max_chars):
    """Compose the judge user prompt for one contract (all fields in one call).

    Args:
        fields: list of field definitions (``field_name`` + ``question``).
        extractions: dict ``field_name -> {"value": str|None, "evidence": str|None}``.
        text: full contract text (truncated to ``max_chars`` for the judge).
        max_chars: input cap.
    """
    if len(text) > max_chars:
        text = text[:max_chars]
    items = []
    for f in fields:
        name = f["field_name"]
        ex = extractions.get(name, {})
        items.append(
            {
                "field": name,
                "question": f["question"],
                "extracted_value": ex.get("value"),
                "evidence": ex.get("evidence"),
            }
        )
    payload = json.dumps(items, ensure_ascii=False, indent=2)
    return (
        "Audit the following extracted fields. Return a JSON object keyed by the "
        'exact field names, each mapping to {"verdict": ..., "rationale": ...}.\n\n'
        "Fields:\n"
        f"{payload}\n\n"
        "Contract text (for corroboration):\n"
        '"""\n'
        f"{text}\n"
        '"""'
    )


def judge_fields(client, model, fields, extractions, text, max_chars):
    """Judge all fields of one contract in a single chat completion.

    Returns ``dict field_name -> {"verdict": str, "rationale": str}``. Raises on
    API/parse failure; the caller records the error and falls back to a
    locate-only trust signal.
    """
    prompt = build_judge_prompt(fields, extractions, text, max_chars)
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": JUDGE_SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        response_format={"type": "json_object"},
        temperature=0,
    )
    data = json.loads(resp.choices[0].message.content)
    out = {}
    for f in fields:
        name = f["field_name"]
        v = data.get(name)
        if isinstance(v, dict):
            out[name] = {
                "verdict": v.get("verdict"),
                "rationale": v.get("rationale"),
            }
        else:
            out[name] = {"verdict": None, "rationale": None}
    return out


# ---------------------------------------------------------------------------
# Fuse the two signals into one categorical trust value.
# ---------------------------------------------------------------------------

def derive_trust(
    value,
    match_type,
    judge_verdict,
    judge_error,
    judge_enabled,
    validation=None,
    source_verified=None,
):
    """Combine the locate result and the judge verdict into a trust category.

    Decision order (mitigations from the design baked in):

    * ``value`` is null -> ``unknown`` (nothing was claimed; ``na_null``).
    * judge errored -> ``review`` (persist, flag for a human).
    * judge disabled -> ``review`` if the quote was located, else ``unknown``
      (correctness was never assessed, so never ``high``).
    * verdict ``incorrect`` -> ``low``.
    * verdict ``partial`` -> ``review``.
    * verdict ``unverifiable`` -> ``unknown``.
    * verdict ``correct`` + quote located -> ``high``.
    * verdict ``correct`` + quote NOT located -> ``review`` (value may be right
      but the cited quote is paraphrased/unlocatable -- do not silently trust).

    ``validation`` is an independent structural gate (see
    :func:`contract_intelligence.gold.validate.validate_value`). A value that is
    structurally ``invalid`` for its type can never be ``high``; it is demoted to
    ``review`` so a human looks before the value is trusted. ``None`` (validation
    disabled) leaves the verdict untouched.

    ``source_verified`` is the Source->DI confidence gate (Plan 02). When the
    source layer is ``low`` (DI read the evidence span poorly, or the whole
    document scored ``di_quality_flag == 'low'``) a ``high`` is demoted to
    ``review`` -- the model may be faithful to garbled text. ``high`` /
    ``confirmed`` / ``None`` leave the verdict untouched, so a vision pass that
    *confirms* the page lets a genuine ``high`` stand.
    """
    if value is None:
        return TRUST_UNKNOWN
    trust = _judge_trust(match_type, judge_verdict, judge_error, judge_enabled)
    if validation == vld.INVALID and trust == TRUST_HIGH:
        trust = TRUST_REVIEW
    if source_verified == SOURCE_LOW and trust == TRUST_HIGH:
        trust = TRUST_REVIEW
    return trust


def _judge_trust(match_type, judge_verdict, judge_error, judge_enabled):
    """Base trust from the locate result + judge verdict (pre-validation gate)."""
    if judge_error:
        return TRUST_REVIEW
    if not judge_enabled or judge_verdict is None:
        return TRUST_REVIEW if match_type in _LOCATED else TRUST_UNKNOWN
    if judge_verdict == VERDICT_INCORRECT:
        return TRUST_LOW
    if judge_verdict == VERDICT_PARTIAL:
        return TRUST_REVIEW
    if judge_verdict == VERDICT_UNVERIFIABLE:
        return TRUST_UNKNOWN
    if judge_verdict == VERDICT_CORRECT:
        return TRUST_HIGH if match_type in _LOCATED else TRUST_REVIEW
    return TRUST_REVIEW

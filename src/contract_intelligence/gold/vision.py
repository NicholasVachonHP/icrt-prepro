"""Targeted multimodal source verification & correction for gold fields.

The text pipeline can only ever be as faithful as the Document Intelligence read
of the source. When that read is weak -- a low-confidence OCR span, a document
scored ``di_quality_flag == 'low'``, an unlocatable quote, or a value the judge
was unsure about -- a *second pair of eyes* is cheaper and far more reliable than
re-reasoning over the same garbled text: render the original source page back to
an image and ask a multimodal model to read it directly.

This module exposes a two-rung escalation (Plan 02 -> Plan 03):

* **verify** (Plan 02) -- :func:`verify_field` confirms or contradicts an
  *existing* value against the page. It can restore a ``high`` a weak OCR read
  demoted, or force a value off ``high``, but it can never change the value.
* **correct** (Plan 03) -- :func:`correct_field` re-reads the value straight off
  the page image, bypassing DI text entirely, so a *wrong or missing* value can
  be fixed. The caller still passes any corrected value back through the LLM
  judge before it can be trusted (a probabilistic model never silently
  overwrites a field).

The module is deliberately:

* **Best-effort** -- every entry point degrades to a clean no-op (``None`` /
  ``"unclear"``) rather than raising, so a missing renderer, a non-PDF source, or
  a flaky vision call never fails the gold run. The caller only escalates a small,
  bounded subset of fields, so the added cost is capped.
* **Stateless / Spark-free** -- like :mod:`contract_intelligence.silver.di_extract`
  it takes bytes and clients, returns plain values, and is unit-testable. The
  caller (:mod:`contract_intelligence.gold.fields`) owns page rendering caching,
  the escalation gate, page selection, the re-judge, and folding results back
  into ``trust``.

Verdict -> ``source_verified`` mapping for *verify* (the caller applies it):

* ``confirmed``    -> ``source_verified = "confirmed"`` (may restore ``high``).
* ``contradicted`` -> ``source_verified = "low"``       (forces non-``high``).
* ``unclear``      -> leave the Phase 1 ``source_verified`` untouched.
"""

import base64

# Vision escalation modes (the gold.vision.mode config enum). Escalating tiers,
# mutually exclusive: off does nothing; verify reproduces Plan 02 exactly;
# correct runs verify first and only escalates to a correction when verify can't
# confirm the value in place.
MODE_OFF = "off"
MODE_VERIFY = "verify"
MODE_CORRECT = "correct"

_MODES = (MODE_OFF, MODE_VERIFY, MODE_CORRECT)

# Verdicts the *verify* model may return (mapped to source_verified by the caller).
VERDICT_CONFIRMED = "confirmed"
VERDICT_CONTRADICTED = "contradicted"
VERDICT_UNCLEAR = "unclear"

_VERDICTS = (VERDICT_CONFIRMED, VERDICT_CONTRADICTED, VERDICT_UNCLEAR)

# Verdicts the *correct* model may return: did it read a usable value off the page?
CORRECTION_OK = "correct"
CORRECTION_UNCLEAR = "unclear"

_CORRECTION_VERDICTS = (CORRECTION_OK, CORRECTION_UNCLEAR)

_VERIFY_SYSTEM_PROMPT = (
    "You verify a single fact that was extracted from a contract by reading the "
    "ORIGINAL contract page image directly. You are given a question, the value a "
    "text pipeline extracted for it, and the rendered source page. Decide whether "
    "the page supports that value. Report only what is visibly present on the "
    "page; never guess. Respond with a single JSON object: "
    '{"verdict": "confirmed" | "contradicted" | "unclear", "rationale": "<one '
    'short sentence>"}. Use "confirmed" only when the page clearly supports the '
    'value, "contradicted" when the page clearly states something different, and '
    '"unclear" when the page does not settle it (value not on this page, '
    "illegible, or ambiguous)."
)

_CORRECT_SYSTEM_PROMPT = (
    "You extract a single fact directly from the ORIGINAL contract page image "
    "because an upstream text pipeline may have misread it. You are given a "
    "question and the rendered source page. Read the answer straight off the "
    "page; report only what is visibly present and never guess. Respond with a "
    "single JSON object: "
    '{"value": "<the answer as plain text, or null if it is not on this page>", '
    '"evidence": "<a short verbatim quote from the page that supports the value, '
    'or null>", "verdict": "correct" | "unclear", "rationale": "<one short '
    'sentence>"}. Use "correct" only when the value is clearly and unambiguously '
    'readable on this page; use "unclear" when the answer is not on this page, is '
    "illegible, or is ambiguous (and then return null for value)."
)


def render_page_png(file_path, page_number, *, zoom=2.0):
    """Render one page of a source document to PNG bytes (best-effort).

    Uses PyMuPDF (``fitz``) when available; ``page_number`` is 1-based to match
    the DI ``page`` stored on blocks. Returns ``None`` -- never raises -- when
    PyMuPDF is not installed, the file cannot be opened, the format is not
    page-renderable (e.g. a ``.docx``), or the page is out of range, so the
    caller simply skips vision for that field.

    ``zoom`` scales the 72-dpi default (2.0 -> ~144 dpi), enough for a vision
    model to read dense legal text without oversized payloads.
    """
    if not file_path or page_number is None:
        return None
    try:
        import fitz  # PyMuPDF
    except Exception:  # noqa: BLE001 - renderer optional; vision degrades to no-op
        return None
    try:
        with fitz.open(file_path) as doc:
            idx = int(page_number) - 1
            if idx < 0 or idx >= doc.page_count:
                return None
            page = doc.load_page(idx)
            matrix = fitz.Matrix(zoom, zoom)
            pix = page.get_pixmap(matrix=matrix, alpha=False)
            return pix.tobytes("png")
    except Exception:  # noqa: BLE001 - unreadable / non-paged source -> no-op
        return None


def resolve_pages(evidence_page, retrieval_pages, scan_limit):
    """Decide which source page(s) to render for an escalated field.

    Page-selection priority (Plan 03 §9.2, *retrieval-first, scan fallback*):

    1. ``evidence_page`` -- when the field's quote already located in a silver
       block, render exactly that page.
    2. ``retrieval_pages`` -- otherwise the distinct ``page`` values carried by
       the field's top retrieved chunks (best first); the chunk index publishes
       ``page`` natively, so no block-text-match heuristic is needed.
    3. a bounded scan of the first ``scan_limit`` pages -- last resort when no
       chunk carries a page.

    Returns a de-duplicated list of 1-based page numbers, best first (possibly
    empty). The caller renders them in order and uses the first that succeeds.
    """
    if evidence_page is not None:
        return [int(evidence_page)]
    pages = []
    for p in retrieval_pages or []:
        if p is None:
            continue
        ip = int(p)
        if ip not in pages:
            pages.append(ip)
    if pages:
        return pages
    if scan_limit and scan_limit > 0:
        return list(range(1, int(scan_limit) + 1))
    return []


def _normalize_verdict(raw):
    """Map a model-reported verify verdict to a known value, else ``unclear``."""
    v = (raw or "").strip().lower()
    return v if v in _VERDICTS else VERDICT_UNCLEAR


def _normalize_correction_verdict(raw):
    """Map a model-reported correction verdict to a known value, else ``unclear``."""
    v = (raw or "").strip().lower()
    return v if v in _CORRECTION_VERDICTS else CORRECTION_UNCLEAR


def _vision_messages(system_prompt, user_text, page_png):
    """Compose a system + multimodal-user message pair for a vision call."""
    b64 = base64.b64encode(page_png).decode("ascii")
    return [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": [
                {"type": "text", "text": user_text},
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/png;base64,{b64}"},
                },
            ],
        },
    ]


def verify_field(
    client, model, page_png, field_question, value, *, max_chars=4000
):
    """Ask a multimodal model whether ``page_png`` supports ``value``.

    Returns ``(verdict, rationale)`` where ``verdict`` is one of ``confirmed`` /
    ``contradicted`` / ``unclear``. Best-effort: any failure (no image, bad
    response, API error) returns ``("unclear", None)`` so escalation never breaks
    the gold run. ``value`` is truncated to ``max_chars`` before being shown.
    """
    if not page_png or client is None or not model:
        return VERDICT_UNCLEAR, None

    import json

    value_text = "" if value is None else str(value)
    if max_chars and len(value_text) > max_chars:
        value_text = value_text[:max_chars]
    user_text = (
        "Question: "
        f"{field_question}\n"
        "Value extracted by the text pipeline: "
        f"{value_text or '(none)'}\n"
        "Does the contract page below support this value?"
    )
    try:
        resp = client.chat.completions.create(
            model=model,
            temperature=0,
            response_format={"type": "json_object"},
            messages=_vision_messages(_VERIFY_SYSTEM_PROMPT, user_text, page_png),
        )
        data = json.loads(resp.choices[0].message.content)
    except Exception:  # noqa: BLE001 - non-fatal; treat as unclear
        return VERDICT_UNCLEAR, None

    verdict = _normalize_verdict(data.get("verdict"))
    rationale = data.get("rationale")
    if rationale is not None:
        rationale = str(rationale).strip() or None
    return verdict, rationale


def correct_field(
    client, model, page_png, field_question, page_number, *, max_chars=4000
):
    """Re-read a field's value straight off the source page image (Plan 03).

    Bypasses DI text entirely: the model reads the answer to ``field_question``
    from ``page_png`` directly, for cases where the text pipeline misread or
    missed it. Returns a ``Correction`` dict
    ``{"value", "evidence", "page", "verdict", "rationale"}`` -- ``verdict`` is
    ``correct`` only when the model read a clear value, else ``unclear`` (and
    ``value`` is then ``None``). Best-effort: any failure returns ``None`` so the
    caller keeps the original value. ``value`` is the model's plain-text reading;
    the caller still re-judges it before it can be trusted.
    """
    if not page_png or client is None or not model:
        return None

    import json

    user_text = (
        "Question: "
        f"{field_question}\n"
        "Read the answer directly from the contract page image below. If the "
        "answer is not visibly on this page, return null."
    )
    try:
        resp = client.chat.completions.create(
            model=model,
            temperature=0,
            response_format={"type": "json_object"},
            messages=_vision_messages(_CORRECT_SYSTEM_PROMPT, user_text, page_png),
        )
        data = json.loads(resp.choices[0].message.content)
    except Exception:  # noqa: BLE001 - non-fatal; caller keeps original value
        return None

    verdict = _normalize_correction_verdict(data.get("verdict"))
    value = data.get("value")
    if isinstance(value, str):
        value = value.strip() or None
    elif value is not None:
        value = str(value)
    evidence = data.get("evidence")
    if evidence is not None:
        evidence = str(evidence).strip() or None
    rationale = data.get("rationale")
    if rationale is not None:
        rationale = str(rationale).strip() or None
    if max_chars and value and len(value) > max_chars:
        value = value[:max_chars]
    # A "correct" verdict with no value is contradictory; treat as unclear.
    if verdict == CORRECTION_OK and not value:
        verdict = CORRECTION_UNCLEAR
    return {
        "value": value,
        "evidence": evidence,
        "page": page_number,
        "verdict": verdict,
        "rationale": rationale,
    }


def verdict_to_source_verified(verdict, current):
    """Fold a vision ``verdict`` into the ``source_verified`` signal.

    ``confirmed`` upgrades to ``"confirmed"`` (lets a genuine ``high`` stand),
    ``contradicted`` forces ``"low"`` (never ``high``), and ``unclear`` leaves the
    Phase 1 ``current`` value untouched.
    """
    if verdict == VERDICT_CONFIRMED:
        return "confirmed"
    if verdict == VERDICT_CONTRADICTED:
        return "low"
    return current

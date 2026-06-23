"""Targeted multimodal source verification for gold field extraction (Plan 02).

The text pipeline can only ever be as faithful as the Document Intelligence read
of the source. When that read is weak -- a low-confidence OCR span, a document
scored ``di_quality_flag == 'low'``, an unlocatable quote, or a value the judge
was unsure about -- a *second pair of eyes* is cheaper and far more reliable than
re-reasoning over the same garbled text: render the original source page back to
an image and ask a multimodal model to confirm or contradict the extracted value
directly against the page.

This module is the Phase 2 escalation primitive. It is deliberately:

* **Best-effort** -- every entry point degrades to a clean no-op (``None`` /
  ``"unclear"``) rather than raising, so a missing renderer, a non-PDF source, or
  a flaky vision call never fails the gold run. The caller only escalates a small
  subset of fields, so the added cost is bounded.
* **Stateless / Spark-free** -- like :mod:`contract_intelligence.silver.di_extract`
  it takes bytes and clients, returns plain values, and is unit-testable. The
  caller (:mod:`contract_intelligence.gold.fields`) owns page rendering caching,
  the escalation gate, and folding the verdict back into ``trust``.

Verdict -> ``source_verified`` mapping (the caller applies it):

* ``confirmed``    -> ``source_verified = "confirmed"`` (may restore ``high``).
* ``contradicted`` -> ``source_verified = "low"``       (forces non-``high``).
* ``unclear``      -> leave the Phase 1 ``source_verified`` untouched.
"""

import base64

# Verdicts the vision model may return (mapped to source_verified by the caller).
VERDICT_CONFIRMED = "confirmed"
VERDICT_CONTRADICTED = "contradicted"
VERDICT_UNCLEAR = "unclear"

_VERDICTS = (VERDICT_CONFIRMED, VERDICT_CONTRADICTED, VERDICT_UNCLEAR)

_SYSTEM_PROMPT = (
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


def _normalize_verdict(raw):
    """Map a model-reported verdict string to a known value, else ``unclear``."""
    v = (raw or "").strip().lower()
    return v if v in _VERDICTS else VERDICT_UNCLEAR


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
    b64 = base64.b64encode(page_png).decode("ascii")
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
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": user_text},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/png;base64,{b64}"
                            },
                        },
                    ],
                },
            ],
        )
        data = json.loads(resp.choices[0].message.content)
    except Exception:  # noqa: BLE001 - non-fatal; treat as unclear
        return VERDICT_UNCLEAR, None

    verdict = _normalize_verdict(data.get("verdict"))
    rationale = data.get("rationale")
    if rationale is not None:
        rationale = str(rationale).strip() or None
    return verdict, rationale


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

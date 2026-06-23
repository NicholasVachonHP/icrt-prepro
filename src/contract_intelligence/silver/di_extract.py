"""Azure Document Intelligence extraction for the contract intelligence pipeline.

Replaces the previous ``pypdf`` / ``python-docx`` text extraction with a single
``prebuilt-layout`` analysis that returns layout-aware **markdown** plus the
structured ``tables``/``figures`` objects from the *same* response (one call,
one charge). The result is decomposed into **ordered semantic blocks** that are
the unit of both downstream consumers:

* ``prose``  -- narrative paragraphs (section headings update block context).
* ``table``  -- rendered as markdown *and* kept as structured cells; an atomic
                 block (never split mid-table downstream).
* ``figure`` -- a bounded, extraction-oriented caption for an embedded figure;
                 an atomic block (never split downstream).

Four artifacts are produced per document (plus optional image bytes):

1. ``text``   -- DI's complete, faithful ``result.content`` markdown for the
                  document (what the gold layer reads as ``extracted_text``):
                  every clause is present, tables are rendered inline, and
                  image regions keep their OCR text. Completeness matters far
                  more than tidiness for downstream field extraction, so this is
                  deliberately the *whole* document, not a filtered rebuild.
2. ``blocks`` -- the ordered block list (persisted to ``contract_blocks``; the
                  unit of chunking). Unlike ``text`` this *is* filtered
                  (page-header/footer noise dropped, figures collapsed to
                  captions) because retrieval wants clean, self-contained units.
3. ``tables`` -- the structured tables (persisted to ``contract_tables`` so
                  pricing/SLA grids can be queried as data, not just prose).
4. ``figures`` -- the cropped figure bytes returned by DI for each ``figure``
                  (``AnalyzeOutputOption.FIGURES``); the caller persists the
                  bytes to the lakehouse and each figure block's ``figure_uri``
                  points at the saved file. When a ``vision_client`` is supplied
                  these figures also enrich the captions.

Note on terminology: ``figure`` is the domain concept throughout this pipeline
-- the DI-detected element and everything derived from it: the ``figure`` block
type, the ``has_figures`` column, the ``figure_uri`` reference and raw
``figure_bytes`` artifact, the ``*_figures`` config keys, helpers and file-path
prefixes, and the Azure Document Intelligence SDK's own names
(``result.figures``, ``figure.id`` / ``.caption`` / ``.spans``,
``AnalyzeOutputOption.FIGURES``, ``get_analyze_result_figure``). ``image``
survives only inside external API surfaces -- the OpenAI ``image_url`` chat
field and ``image/png`` MIME content types.

This module is intentionally free of Spark / notebookutils so it can be unit
tested; the caller (``silver.extract``) wraps the returned dicts into Delta rows
and writes the image bytes to the lakehouse.

Environment / config:
  - AZURE_DOC_INTELLIGENCE_ENDPOINT : DI resource endpoint
  - AZURE_DOC_INTELLIGENCE_KEY      : DI resource API key
  - silver.document_intelligence.model_id          (default "prebuilt-layout")
  - silver.document_intelligence.caption_figures   (default True)
  - silver.document_intelligence.max_caption_chars (default 2000)
  - silver.document_intelligence.vision_model       (multimodal chat deployment
                                                     used to caption images)
"""

import os

# Paragraph roles that are layout noise rather than contract content. These are
# dropped from prose blocks so headers/footers/page numbers do not pollute the
# extracted text, chunks, or field extraction.
_NOISE_ROLES = {"pageHeader", "pageFooter", "pageNumber"}

# System prompt for the optional multimodal figure captioner. The caption feeds
# both retrieval chunks and the gold ``extracted_text``, so it must be factual
# and free of speculation.
_VISION_SYSTEM_PROMPT = (
    "You describe images embedded in legal contracts so they can be searched "
    "and have facts extracted from them. Report only what is visibly present: "
    "any text, numbers, labels, headings, logos, stamps, signatures, hand-written "
    "marks, table-like structure, diagrams and their evident purpose. Do not "
    "speculate or invent details. Write a single plain-text description with no "
    "preamble."
)


# ---------------------------------------------------------------------------
# Document Intelligence client + analysis
# ---------------------------------------------------------------------------

def get_di_client():
    """Return a ``DocumentIntelligenceClient`` bound to the DI resource (API key)."""
    from azure.ai.documentintelligence import DocumentIntelligenceClient
    from azure.core.credentials import AzureKeyCredential

    endpoint = os.environ["AZURE_DOC_INTELLIGENCE_ENDPOINT"]
    key = os.environ["AZURE_DOC_INTELLIGENCE_KEY"]
    return DocumentIntelligenceClient(
        endpoint=endpoint, credential=AzureKeyCredential(key)
    )


def analyze_layout(client, path, model_id="prebuilt-layout", *, with_figures=False):
    """Run DI ``prebuilt-layout`` over a file and return ``(result, operation_id)``.

    When ``with_figures`` is set, the analysis additionally requests cropped
    figure images (``AnalyzeOutputOption.FIGURES``); the returned ``operation_id``
    is then passed to :func:`fetch_figure_images` to download each figure's bytes.
    """
    with open(path, "rb") as f:
        data = f.read()
    kwargs = {}
    if with_figures:
        try:
            from azure.ai.documentintelligence.models import AnalyzeOutputOption

            kwargs["output"] = [AnalyzeOutputOption.FIGURES]
        except Exception:  # noqa: BLE001 - SDK too old for image output; degrade
            kwargs = {}
    poller = client.begin_analyze_document(
        model_id,
        body=data,
        content_type="application/octet-stream",
        output_content_format="markdown",
        **kwargs,
    )
    result = poller.result()
    operation_id = None
    try:
        operation_id = poller.details.get("operation_id")
    except Exception:  # noqa: BLE001 - details unavailable; images simply skipped
        operation_id = None
    return result, operation_id


def fetch_figure_images(client, result, operation_id):
    """Download cropped image bytes for each DI figure in a completed analysis.

    Returns a dict mapping each DI ``figure.id`` to its raw image bytes. Figures
    without an id, or whose download fails, are omitted so captioning can fall
    back to the OCR-text path. One bad image never fails the whole document.
    """
    images = {}
    if not operation_id:
        return images
    model_id = getattr(result, "model_id", None)
    for figure in (getattr(result, "figures", None) or []):
        fid = getattr(figure, "id", None)
        if not fid:
            continue
        try:
            stream = client.get_analyze_result_figure(
                model_id=model_id, result_id=operation_id, figure_id=fid
            )
            images[fid] = b"".join(stream)
        except Exception:  # noqa: BLE001 - one bad image must not fail the doc
            continue
    return images


# ---------------------------------------------------------------------------
# Span / offset helpers (DI elements carry spans = offsets into result.content)
# ---------------------------------------------------------------------------

def _span_bounds(spans):
    """Return (start, end) covering all spans of a DI element, or (inf, inf)."""
    if not spans:
        return float("inf"), float("inf")
    start = min(s.offset for s in spans)
    end = max(s.offset + (s.length or 0) for s in spans)
    return start, end


def _first_page(element):
    """First page number of a DI element (1-based), or None."""
    regions = getattr(element, "bounding_regions", None)
    if regions:
        return regions[0].page_number
    return None


def _figure_page_fraction(figure, result):
    """Figure bounding-box area as a fraction of its page area (0..1), or None.

    Unit-independent (works whether DI reports inches or pixels) because it is a
    ratio of two areas in the same unit. Used to drop tiny decorative marks --
    logos, header/footer glyphs -- that are a negligible fraction of the page.
    Returns ``None`` when geometry is unavailable, so callers keep the figure
    rather than guess.
    """
    regions = getattr(figure, "bounding_regions", None)
    if not regions:
        return None
    region = regions[0]
    poly = getattr(region, "polygon", None)
    if not poly or len(poly) < 6:
        return None
    xs = poly[0::2]
    ys = poly[1::2]
    fig_area = (max(xs) - min(xs)) * (max(ys) - min(ys))
    if fig_area <= 0:
        return None
    pages = getattr(result, "pages", None) or []
    pno = getattr(region, "page_number", None)
    if not pno or pno < 1 or pno > len(pages):
        return None
    page = pages[pno - 1]
    pw = getattr(page, "width", None)
    ph = getattr(page, "height", None)
    if not pw or not ph or pw * ph <= 0:
        return None
    return fig_area / (pw * ph)


def _slice_content(content, spans, max_chars):
    """Concatenate the content covered by an element's spans, bounded in length."""
    if not content or not spans:
        return ""
    parts = []
    for s in spans:
        start = s.offset
        end = s.offset + (s.length or 0)
        parts.append(content[start:end])
    text = " ".join(" ".join(p.split()) for p in parts).strip()
    return text[:max_chars]


# ---------------------------------------------------------------------------
# Per-word OCR confidence (DI exposes a confidence per recognised word)
# ---------------------------------------------------------------------------

# A word is "confident" at or above this score; the document quality signal is
# the fraction of words that clear it. DI word confidence is a coarse 0..1.
_HIGH_CONFIDENCE = 0.9


def _word_confidences(result):
    """Flatten DI per-word confidence into ``(start, end, confidence)`` tuples.

    DI ``prebuilt-layout`` reports a confidence per recognised word on
    ``result.pages[].words[]``; each word's span offsets index into
    ``result.content`` (the same coordinate space blocks/tables use). Words
    without a span or confidence are skipped. Returns ``[]`` when the result
    carries no word-level confidence (e.g. a born-digital PDF DI read losslessly)
    so callers treat confidence as simply unavailable.
    """
    out = []
    for page in (getattr(result, "pages", None) or []):
        for w in (getattr(page, "words", None) or []):
            conf = getattr(w, "confidence", None)
            if conf is None:
                continue
            span = getattr(w, "span", None)
            if span is None:
                spans = getattr(w, "spans", None) or []
                span = spans[0] if spans else None
            if span is None:
                continue
            start = getattr(span, "offset", None)
            if start is None:
                continue
            length = getattr(span, "length", 0) or 0
            out.append((start, start + length, float(conf)))
    return out


def _span_confidence(word_confs, lo, hi):
    """Min and mean confidence of the words whose start offset falls in ``[lo, hi)``.

    Returns ``(None, None)`` when no word maps to the span (no confidence data,
    or a synthetic span such as a rendered markdown table), so the caller stores
    NULL rather than a misleading number.
    """
    if not word_confs or lo is None or hi is None:
        return None, None
    vals = [c for (s, _e, c) in word_confs if lo <= s < hi]
    if not vals:
        return None, None
    return min(vals), sum(vals) / len(vals)


def document_quality(word_confs, high_threshold=_HIGH_CONFIDENCE):
    """Fraction of OCR words at/above ``high_threshold`` confidence (0..1).

    A cheap document-level triage signal: 1.0 means every word read cleanly, low
    values flag a scanned/garbled source whose extracted text should not be
    trusted without a closer look. Returns ``None`` when no word confidences are
    available (nothing to score).
    """
    if not word_confs:
        return None
    high = sum(1 for (_s, _e, c) in word_confs if c >= high_threshold)
    return high / len(word_confs)



# ---------------------------------------------------------------------------
# Table rendering
# ---------------------------------------------------------------------------

def _cell_text(cell):
    return (cell.content or "").replace("\n", " ").replace("|", "\\|").strip()


def table_to_markdown(table):
    """Render a DI table as a GitHub-flavoured markdown table (row 0 = header)."""
    n_rows = table.row_count or 0
    n_cols = table.column_count or 0
    if n_rows == 0 or n_cols == 0:
        return ""
    grid = [["" for _ in range(n_cols)] for _ in range(n_rows)]
    for cell in (table.cells or []):
        r, c = cell.row_index, cell.column_index
        if 0 <= r < n_rows and 0 <= c < n_cols:
            grid[r][c] = _cell_text(cell)
    lines = []
    for i, row in enumerate(grid):
        lines.append("| " + " | ".join(row) + " |")
        if i == 0:  # markdown requires a header separator row
            lines.append("| " + " | ".join(["---"] * n_cols) + " |")
    return "\n".join(lines)


def table_cells(table):
    """Structured cell list for ``contract_tables`` (JSON-serialisable)."""
    cells = []
    for cell in (table.cells or []):
        cells.append(
            {
                "row": cell.row_index,
                "col": cell.column_index,
                "row_span": getattr(cell, "row_span", None) or 1,
                "col_span": getattr(cell, "column_span", None) or 1,
                "kind": getattr(cell, "kind", None),
                "text": cell.content or "",
            }
        )
    return cells


# ---------------------------------------------------------------------------
# Figure captioning
# ---------------------------------------------------------------------------

def _vision_caption(client, model, figure_bytes, content_type, max_chars):
    """Caption a figure with a multimodal chat model; raise on failure."""
    import base64

    b64 = base64.b64encode(figure_bytes).decode("ascii")
    resp = client.chat.completions.create(
        model=model,
        temperature=0,
        max_tokens=max(64, min(1024, max_chars // 3)),
        messages=[
            {"role": "system", "content": _VISION_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "Describe this image from a contract for "
                        "downstream field extraction and retrieval.",
                    },
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:{content_type};base64,{b64}"},
                    },
                ],
            },
        ],
    )
    return (resp.choices[0].message.content or "").strip()[:max_chars]


def caption_figure(
    figure,
    content,
    *,
    max_chars,
    figure_bytes=None,
    content_type="image/png",
    vision_client=None,
    vision_model=None,
):
    """Produce a bounded, extraction-oriented caption for a figure block.

    ``figure`` is the source DI figure element (for its ``.caption`` / ``.spans``);
    ``figure_bytes`` is the rendered image of that element. Combines up to three
    sources, in order: DI's detected caption, a multimodal vision description of
    the rendered image (only when a ``vision_client``/``vision_model`` and
    ``figure_bytes`` are all available), and the OCR'd text inside the image
    region (sliced from the markdown ``content`` by span). The vision step
    degrades silently to the OCR-only path on any error, so captioning never
    fails the extraction.
    """
    caption = ""
    fig_caption = getattr(figure, "caption", None)
    if fig_caption is not None and getattr(fig_caption, "content", None):
        caption = fig_caption.content.strip()

    ocr_text = _slice_content(content, getattr(figure, "spans", None), max_chars)

    vision_text = ""
    if vision_client is not None and vision_model and figure_bytes:
        try:
            vision_text = _vision_caption(
                vision_client, vision_model, figure_bytes, content_type, max_chars
            )
        except Exception:  # noqa: BLE001 - vision is best-effort; fall back to OCR
            vision_text = ""

    parts, seen = [], []
    for piece in (caption, vision_text, ocr_text):
        piece = (piece or "").strip()
        if not piece:
            continue
        # Skip a source already wholly contained in one we kept (or vice versa).
        if any(piece in kept or kept in piece for kept in seen):
            continue
        seen.append(piece)
        parts.append(piece)
    if not parts:
        return "(figure with no extractable text)"
    return " — ".join(parts)[:max_chars]


# ---------------------------------------------------------------------------
# Block assembly
# ---------------------------------------------------------------------------

def _covered_ranges(result):
    """Offset ranges occupied by tables/figures (so their inner paragraphs are skipped)."""
    ranges = []
    for table in (getattr(result, "tables", None) or []):
        ranges.append(_span_bounds(getattr(table, "spans", None)))
    for figure in (getattr(result, "figures", None) or []):
        ranges.append(_span_bounds(getattr(figure, "spans", None)))
    return ranges


def _inside_any(start, ranges):
    return any(lo <= start < hi for lo, hi in ranges)


def build_blocks_and_tables(
    result,
    *,
    max_caption_chars,
    figures_by_id=None,
    figure_uri_prefix=None,
    min_figure_page_fraction=0.0,
    vision_client=None,
    vision_model=None,
    word_confs=None,
):
    """Decompose a DI AnalyzeResult into ordered blocks + structured tables.

    Returns ``(blocks, tables, figures, has_tables, has_figures)`` where ``blocks``
    is a list of dicts (block_index, type, section, page, text, table_id,
    figure_uri, char_count, conf_min, conf_mean) in reading order, ``tables`` is a
    list of dicts ready for the ``contract_tables`` table, and ``figures`` is a
    list of ``{figure_uri, figure_bytes}`` for the caller to persist.
    ``figures_by_id`` maps DI figure ids to raw bytes (from
    :func:`fetch_figure_images`); when bytes exist for a figure and
    ``figure_uri_prefix`` is given, the figure block's ``figure_uri`` becomes
    ``{prefix}/p{page}_f{idx}.png``.

    ``word_confs`` is the per-word confidence index from
    :func:`_word_confidences`; when supplied, each block records the ``conf_min``
    and ``conf_mean`` of the DI words covering its span (``None`` when no word
    maps to it, e.g. a rendered markdown table). Pass ``None`` to skip confidence.

    ``min_figure_page_fraction`` drops decorative marks (logos, header/footer
    glyphs) whose bounding box is a smaller fraction of the page than this
    threshold, *before* captioning and persistence -- so such figures cost no
    vision call, save no image, and never reach chunking. ``0.0`` disables it;
    figures lacking geometry are always kept.
    """
    content = getattr(result, "content", "") or ""
    covered = _covered_ranges(result)
    figures_by_id = figures_by_id or {}
    figures_out = []
    word_confs = word_confs or []

    # Collect placement entries: (start_offset, kind, payload).
    entries = []

    # Prose paragraphs that are not inside a table/image region.
    for para in (getattr(result, "paragraphs", None) or []):
        role = getattr(para, "role", None)
        if role in _NOISE_ROLES:
            continue
        start, _ = _span_bounds(getattr(para, "spans", None))
        if _inside_any(start, covered):
            continue
        entries.append((start, "prose", para))

    # Tables (also build the structured table records).
    tables_out = []
    for t_idx, table in enumerate(getattr(result, "tables", None) or []):
        table_id = f"t{t_idx}"
        start, _ = _span_bounds(getattr(table, "spans", None))
        entries.append((start, "table", (table_id, table)))
        tables_out.append(
            {
                "table_id": table_id,
                "table_index": t_idx,
                "page": _first_page(table),
                "n_rows": table.row_count or 0,
                "n_cols": table.column_count or 0,
                "markdown": table_to_markdown(table),
                "cells": table_cells(table),
            }
        )

    # Images (one per DI figure).
    for f_idx, figure in enumerate(getattr(result, "figures", None) or []):
        start, _ = _span_bounds(getattr(figure, "spans", None))
        entries.append((start, "image", (f_idx, figure)))

    # Order everything by position in the document.
    entries.sort(key=lambda e: e[0])

    blocks = []
    current_section = None
    for _, kind, payload in entries:
        if kind == "prose":
            para = payload
            text = (para.content or "").strip()
            if not text:
                continue
            role = getattr(para, "role", None)
            if role in ("title", "sectionHeading"):
                current_section = text
            p_lo, p_hi = _span_bounds(getattr(para, "spans", None))
            c_min, c_mean = _span_confidence(word_confs, p_lo, p_hi)
            blocks.append(
                {
                    "type": "prose",
                    "section": current_section,
                    "page": _first_page(para),
                    "text": text,
                    "table_id": None,
                    "figure_uri": None,
                    "conf_min": c_min,
                    "conf_mean": c_mean,
                }
            )
        elif kind == "table":
            table_id, table = payload
            md = table_to_markdown(table)
            if not md:
                continue
            t_lo, t_hi = _span_bounds(getattr(table, "spans", None))
            c_min, c_mean = _span_confidence(word_confs, t_lo, t_hi)
            blocks.append(
                {
                    "type": "table",
                    "section": current_section,
                    "page": _first_page(table),
                    "text": md,
                    "table_id": table_id,
                    "figure_uri": None,
                    "conf_min": c_min,
                    "conf_mean": c_mean,
                }
            )
        else:  # figure
            f_idx, figure = payload
            page = _first_page(figure)
            # Drop decorative marks (logos, header/footer glyphs): figures whose
            # bounding box is a negligible fraction of the page. Done here, before
            # captioning/persistence, so they cost no vision call, save no image,
            # and never reach chunking. Figures lacking geometry are kept.
            if min_figure_page_fraction > 0:
                frac = _figure_page_fraction(figure, result)
                if frac is not None and frac < min_figure_page_fraction:
                    continue
            fid = getattr(figure, "id", None)
            figure_bytes = figures_by_id.get(fid) if fid else None
            caption = caption_figure(
                figure,
                content,
                max_chars=max_caption_chars,
                figure_bytes=figure_bytes,
                vision_client=vision_client,
                vision_model=vision_model,
            )
            if figure_bytes and figure_uri_prefix:
                figure_uri = f"{figure_uri_prefix}/p{page}_f{f_idx}.png"
                figures_out.append(
                    {"figure_uri": figure_uri, "figure_bytes": figure_bytes}
                )
            else:
                # No rendered image available -> keep the synthetic provenance ref.
                figure_uri = f"page={page}&figure={f_idx}"
            f_lo, f_hi = _span_bounds(getattr(figure, "spans", None))
            c_min, c_mean = _span_confidence(word_confs, f_lo, f_hi)
            blocks.append(
                {
                    "type": "figure",
                    "section": current_section,
                    "page": page,
                    "text": f"[FIGURE p.{page} — {caption}]",
                    "table_id": None,
                    "figure_uri": figure_uri,
                    "conf_min": c_min,
                    "conf_mean": c_mean,
                }
            )

    # Assign reading-order index + char_count.
    for i, b in enumerate(blocks):
        b["block_index"] = i
        b["char_count"] = len(b["text"])

    has_tables = bool(tables_out)
    has_figures = any(b["type"] == "figure" for b in blocks)
    return blocks, tables_out, figures_out, has_tables, has_figures


def blocks_to_text(blocks):
    """Concatenate blocks in reading order into the canonical ``extracted_text``."""
    return "\n\n".join(b["text"] for b in blocks if (b.get("text") or "").strip())


# ---------------------------------------------------------------------------
# Top-level per-document entry point
# ---------------------------------------------------------------------------

def extract_document(
    path,
    di_client,
    *,
    model_id="prebuilt-layout",
    max_caption_chars=2000,
    caption_figures=True,
    figure_uri_prefix=None,
    min_figure_page_fraction=0.0,
    vision_client=None,
    vision_model=None,
    persist_confidence=False,
):
    """Analyse one document and return its text, blocks, tables, and flags.

    When ``caption_figures`` is set, cropped figure bytes are requested from DI
    and (a) used to enrich figure captions if a ``vision_client``/``vision_model``
    is supplied, and (b) returned under ``figures`` for the caller to persist.
    ``min_figure_page_fraction`` drops decorative marks (logos) below that share
    of the page before captioning/persistence (``0.0`` disables it).

    When ``persist_confidence`` is set, DI per-word confidence is aggregated onto
    each block (``conf_min`` / ``conf_mean``) and summarised into a document-level
    ``di_quality`` (fraction of words read at high confidence); otherwise those
    are ``None``.

    Returns a dict:
        {
          "text": str,            # DI's full result.content markdown (extracted_text)
          "page_count": int,
          "has_tables": bool,
          "has_figures": bool,
          "di_quality": float|None,  # fraction of words >= high confidence
          "blocks": list[dict],   # ordered semantic blocks (filtered, for chunking)
          "tables": list[dict],   # structured tables
          "figures": list[dict],  # {figure_uri, figure_bytes} for caller to save
        }
    """
    # Request cropped figure bytes only when we intend to caption/persist them.
    want_figures = bool(caption_figures)
    result, operation_id = analyze_layout(
        di_client, path, model_id=model_id, with_figures=want_figures
    )

    figures_by_id = {}
    if want_figures and (getattr(result, "figures", None) or []):
        figures_by_id = fetch_figure_images(di_client, result, operation_id)

    # Per-word OCR confidence index (offsets into result.content), computed once
    # and shared by the block aggregation and the document-quality score.
    word_confs = _word_confidences(result) if persist_confidence else []

    blocks, tables, figures, has_tables, has_figures = build_blocks_and_tables(
        result,
        max_caption_chars=max_caption_chars,
        figures_by_id=figures_by_id,
        figure_uri_prefix=figure_uri_prefix,
        min_figure_page_fraction=min_figure_page_fraction,
        vision_client=(vision_client if caption_figures else None),
        vision_model=vision_model,
        word_confs=word_confs,
    )
    # Gold reads the *complete* document: use DI's faithful markdown content
    # (every clause, tables inline, image OCR retained). Only fall back to the
    # filtered block reconstruction if DI returned no content string.
    text = (getattr(result, "content", "") or "").strip()
    if not text:
        text = blocks_to_text(blocks)
    pages = getattr(result, "pages", None) or []
    di_quality = document_quality(word_confs) if persist_confidence else None

    return {
        "text": text,
        "page_count": len(pages),
        "has_tables": has_tables,
        "has_figures": has_figures,
        "di_quality": di_quality,
        "blocks": blocks,
        "tables": tables,
        "figures": figures,
    }

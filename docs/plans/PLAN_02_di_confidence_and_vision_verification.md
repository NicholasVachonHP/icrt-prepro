# Plan 02 — DI confidence + targeted vision verification

**Status:** Implemented · **Date:** 2026-06-22 · **Scope:** silver `di_extract.py` (+ schema) and gold trust model

> **Implemented 2026-06-23.** Both phases shipped. **Phase 1 (always-on DI
> confidence):** `silver/di_extract.py` aggregates DI per-word read confidence
> onto blocks (`conf_min` / `conf_mean` on `contract_blocks`) and a document
> `di_quality` / `di_quality_flag` on `contract_text` (gated by
> `silver.document_intelligence.persist_confidence` / `di_quality_threshold`);
> gold locates each evidence quote's span to surface `evidence_di_confidence`
> and `evidence_page`, and `gold/evidence.py` `derive_trust` gained a
> `source_verified` gate (`low` can never be `high`). **Phase 2 (opt-in vision):**
> new module `gold/vision_verify.py` renders the source page (PyMuPDF) and
> re-reads escalated fields with a multimodal model, persisting `source_verified`,
> `vision_verdict`, `vision_rationale`; wired into `gold/fields.run()` via the new
> `bronze_files_dir` arg (gold notebook now mounts bronze read-only). Config gained
> `evidence_di_confidence_threshold`, `vision_verify_enabled` (default off),
> `vision_verify_model`, `vision_verify_max_chars`. `contract_field_evidence`
> gained 5 columns: `evidence_di_confidence`, `evidence_page`, `source_verified`,
> `vision_verdict`, `vision_rationale`. All new keys feed the silver/gold code
> fingerprints, so enabling them re-extracts.

## 1. Goal

Close the **layer-1 blind spot**: today `match_type` checks the model's quote
against *DI's own output*, so if DI mis-read the source, a faithful quote of
garbled text still scores `exact` → false `high`. This plan makes every surfaced
field rest on either **confident DI text** or a **vision confirmation** of the
original page — without paying to vision-review text no field uses.

Three failure layers (only 2 & 3 covered today):

| Layer | Failure | Today | This plan |
|---|---|---|---|
| 1 Source→DI | OCR error, garbled table, missed checkbox | ❌ assumed perfect | **DI confidence + vision** |
| 2 DI text→value | model misreads | `match_type` | unchanged |
| 3 value↔question | right quote, wrong question | `judge_verdict` | unchanged |

## 2. Design principle (why not validate everything at extraction time)

- DI emits confidence per word; most low-confidence spans are boilerplate no
  field touches → blanket vision review is wasted spend.
- Document-level "confident DI" cannot guarantee a field is safe (the one number
  may sit in the 1% garbled cell). The precise check needs the **evidence
  span**, known only *after* gold locates evidence.
- Never let a probabilistic model **rewrite** canonical silver text (breaks
  `content_hash`/SCD2; reprocesses everything each ingest).

→ **Annotate the confidence signal upstream (silver, free); spend vision lazily
downstream (gold, per uncertain field).**

## 3. Phase 1 — Cheap, always-on signals (silver + trust fusion)

### 3.1 Persist DI confidence (silver `di_extract.py`)
- DI `prebuilt-layout` exposes per-word confidence on `result.pages[].words[]`
  (and selection marks). `build_blocks_and_tables` currently drops it.
- Add a span→confidence aggregation: for each block (and table cell), compute
  `min` and `mean` confidence of the words whose spans fall in the block span.
  Helpers `_span_bounds`/`_slice_content` already give the span machinery.
- Persist on `contract_blocks`: `conf_min`, `conf_mean` (double). Optionally on
  `contract_tables` cells.
- Compute a **document-level quality score** on `contract_text`:
  `di_quality` (e.g. fraction of words ≥ 0.9 confidence) + `di_quality_flag`
  (`ok`/`low`). Cheap doc-level triage.

### 3.2 Surface evidence span confidence in gold
- When gold locates evidence (`ev.locate_evidence`), map the matched offset back
  to the overlapping `contract_blocks` and read `conf_min` for that span →
  `evidence_di_confidence`.
- Add a deterministic rule: `evidence_di_confidence < threshold` (config) →
  this field is **not** eligible for `high` (forces `review`).

### 3.3 Document quality gate
- If `di_quality_flag == "low"`, cap trust for the whole contract (no `high`)
  until vision confirms (phase 2).

### 3.4 Fuse into `derive_trust` (shared seam)
Extend signature (coordinate with Plan 01 §8):
```
derive_trust(value, match_type, judge_verdict, judge_error, judge_enabled,
             validation=None,         # Plan 01
             source_verified=None)    # Plan 02: None | "high" | "low" | "confirmed"
```
- `source_verified == "low"` (low DI conf or low doc quality, no vision yet) →
  never `high`; downgrade to `review`.
- `source_verified == "confirmed"` (phase-2 vision agreed) → may restore `high`.

## 4. Phase 2 — Targeted vision verification (gold)

### 4.1 Escalation gate (cost-bounded)
Only call vision for a field when **any**:
- `match_type` ∈ {`fuzzy`, `not_found`}, or
- `trust` ∈ {`review`, `low`} after phase-1 fusion, or
- `evidence_di_confidence` < threshold, or
- `di_quality_flag == "low"`.

This bounds vision to the uncertain minority, not the ~95% already fine.

### 4.2 Page image acquisition (key decision — see §7)
The vision plumbing already exists: `_vision_caption(client, model,
image_bytes, content_type, max_chars)` and the multimodal `gpt-4.1` vision
deployment (`silver.document_intelligence.vision_model`). What's missing is a
**page image**:
- DI provides cropped *figure* images (`get_analyze_result_figure`) but **not
  arbitrary page images**.
- Option A: render the source PDF page to PNG with `pymupdf`/`pdf2image` (needs
  the page number from `contract_blocks.page`, already persisted).
- Option B: persist page images in silver during extraction (heavier storage).
- Recommend A (render on demand in gold; only for escalated fields).

### 4.3 Verify call
- New `gold/vision_verify.py`: `verify_field(client, model, page_png,
  field_question, value)` → `confirmed | contradicted | unclear`, with a short
  rationale. Reads the **original page image**, bypassing DI text entirely.
- Map verdict → `source_verified`: `confirmed`→`"confirmed"`,
  `contradicted`→`"low"`, `unclear`→leave phase-1 signal.

### 4.4 Persist on `contract_field_evidence`
Add columns: `evidence_di_confidence` (double), `evidence_page` (int),
`source_verified` (string), `vision_verdict` (string), `vision_rationale`
(string). Additive; `schema.autoMerge` enabled.

## 5. Config changes

`config/{dev,prod}.json`:
- silver: `document_intelligence.persist_confidence` (bool), `di_quality_threshold`.
- gold: `evidence_di_confidence_threshold`, `vision_verify_enabled` (bool),
  `vision_verify_model` (default `MAIN_MODEL`/vision_model),
  `vision_verify_max_chars`.
All gold keys join `code_fingerprint`; silver confidence params join the silver
fingerprint.

## 6. Reprocessing impact

- Silver: adding confidence aggregation changes silver extraction code →
  silver `version_id` bumps → **re-extracts all contracts** once (DI re-run).
  Accepted: dev has only 5 contracts, so a full silver+gold re-extract is
  cheap and needs no special scheduling. The raw `AnalyzeResult` therefore does
  **not** need to be persisted/replayed — just re-run DI.
- Gold: new params + module files in fingerprint → re-extract all once.

## 7. Open decisions

- Page-image acquisition (render-on-demand A vs persist-in-silver B). Leaning A.
- DI confidence is coarse (per word, ~0–1); pick `evidence_di_confidence` and
  `di_quality` thresholds from a sample, not a priori.

*Resolved:* full DI re-extract on every contract is acceptable (dev, 5
contracts), so phase 1 simply re-runs DI — no need to persist/replay raw DI JSON.

## 8. Phases & sequencing

1. **Phase 1** (DI confidence persist + span surfacing + doc gate + trust
   fusion). Cheap, biggest-bang-for-buck; ships first. Re-runs DI over all 5
   dev contracts — acceptable.
2. **Phase 2** (page render + targeted vision verify). Adds the only true
   layer-1 check; gated tightly to control cost.

## 9. Shared seam with Plan 01

Both plans edit `derive_trust`. Single owner; first-to-land adds its parameter
with a `None` default. Combined trust becomes a fusion of `match_type`
(faithfulness) × `judge_verdict` (correctness) × `validation` (type sanity) ×
`source_verified` (source fidelity), still collapsed to
`high`/`review`/`low`/`unknown`.

## 10. Validation

- Hand-pick a contract known to OCR poorly (scanned/table-heavy); confirm low
  `conf_min` on the right block, escalation fires, vision verdict recorded.
- Confirm a clean contract makes **zero** extra vision calls (gate holds).
- Re-run `eval/gold_eval.py`; expect fewer false-`high` on garbled fields.

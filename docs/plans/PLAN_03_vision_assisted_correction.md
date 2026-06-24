# Plan 03 — Vision-assisted correction & re-judge for review fields

**Status:** Implemented · **Date:** 2026-06-23 · **Scope:** gold trust loop + `gold/vision.py` (rename) + config consolidation

## 1. Goal

Plan 02 made vision a **verifier**: it can *confirm* a value (restore a `high`
that DI confidence demoted) or *contradict* it (demote), but it **cannot fix a
wrong or missing value**. So the hardest reviews — the ones whose *base* trust is
already `review` — stay stuck there forever:

| Review cause (base trust) | Plan 02 vision | This plan |
|---|---|---|
| judge `partial` | ❌ can't help | re-read page → new value → re-judge |
| judge `correct` but quote **not located** | ❌ no page anchor | resolve page → re-read → re-locate |
| `fuzzy` match on garbled DI text | ⚠️ confirm/contradict only | re-extract the true value from the image |

Plan 03 turns vision into a **corrector**: for escalated fields, re-read the
source page to produce a *(possibly new)* value, **run the judge again** on that
value, and re-derive trust. This is the only path by which a base-`review` field
can legitimately reach `high` (see [trust_derivation.mmd](../trust_derivation.mmd)).

## 2. Design principle

- **Verify before correct.** Confirming an existing value (1 vision call, no
  re-judge) is cheaper and lower-risk than minting a new one. Correction is a
  strict *escalation* of verification, not a replacement.
- **Vision may propose a value; the judge still arbitrates.** A probabilistic
  model never silently overwrites a field — every corrected value passes back
  through the existing LLM judge before it can be trusted.
- **Bounded, single pass.** No open convergence loop: one correction pass per
  contract, capped per field count and per page-render budget. Deterministic
  enough for SCD2 and cost-predictable.
- **Provenance is mandatory.** Every corrected field records what it was, what
  vision proposed, and each step's verdict — both for human auditability *and*
  as the feedstock for the future calibration loop (§12).

## 3. Relationship to Plan 02 (extend, don't replace)

Nothing in Plan 02 is removed — Plan 03 is the top rung of a three-rung ladder
(**DI confidence → verify → correct**). Phase 1 (DI confidence: `conf_min`/
`conf_mean`, `di_quality`, `evidence_di_confidence`, the `source_verified` gate)
is **unchanged and load-bearing** — it is how escalation candidates are selected.

What *is* redundant is the **two-flag / two-function surface**, which Plan 03
consolidates:

| Plan 02 (today) | Plan 03 (consolidated) | Note |
|---|---|---|
| `gold.vision_verify_enabled` (bool) | `gold.vision.mode` = `off \| verify \| correct` | escalating tiers, mutually exclusive |
| `gold.vision_verify_model` | `gold.vision.model` | now shared by both modes |
| `gold.vision_verify_max_chars` | `gold.vision.max_chars` | now shared by both modes |
| module `gold/vision_verify.py` | module `gold/vision.py` | misnomer once it also corrects |
| `verify_field(...) -> verdict` | `verify_field(...)` *(unchanged)* + new `correct_field(...)` | one module, two clearly named entry points |

`mode = verify` reproduces Plan 02 behavior exactly; `mode = correct` runs verify
first and only escalates to a correction when verify can't confirm in place.

## 4. The correction loop (gold)

Runs in `gold/fields.py` **after** the existing extract + (first) judge pass that
produces `field_rows` with a phase-1 `trust`. Per contract:

1. **Select the correction set** — the same escalation gate as Plan 02 §4.1:
   `match_type ∈ {fuzzy, not_found}` OR `trust ∈ {review, low}` OR
   `evidence_di_confidence < threshold` OR `di_quality_flag == "low"`.
   Order by uncertainty; take at most `vision.correct_max_fields` per contract.
2. **Resolve the page(s)** (`resolve_pages`, see §9 decision 2):
   - `evidence_page` known → that page.
   - not located → retrieval-to-page (top retrieved chunk's `page`), else a
     bounded scan of the first `vision.page_scan_limit` pages.
3. **Act by mode:**
   - `verify` → `verify_field(...)` → `confirmed | contradicted | unclear`
     (Plan 02 behavior; no value change).
   - `correct` → `verify_field` first; if it can't confirm, `correct_field(...)`
     → `{value, evidence, page, verdict, rationale}` read from the page image,
     bypassing DI text entirely.
4. **Fold a correction back in** (correct mode only, when vision returns a value):
   - record `original_value`; set the field value to the vision value;
     `value_source = "vision"`, `vision_action = "correct"`.
   - re-locate evidence for the new value → new `match_type`; when the value is
     image-backed rather than present in DI text it gets `match_type =
     "vision_page"` (cited by `evidence_page`), which counts as *located* (§9.1).
   - **re-run the judge** on the corrected `(value, evidence)` — one *batched*
     judge call over all corrected fields of the contract.
   - set `source_verified = "confirmed"` for the vision-sourced value.
   - **recompute** `trust = derive_trust(...)` with the new `match_type`,
     `judge_verdict`, and `source_verified`.
5. **Persist** the enriched evidence rows (§6).

The loop is a **single pass** per contract (no iteration). A contract with
corrections therefore makes at most **two** judge calls — the initial
full-contract judge plus one batched re-judge over the corrected fields; clean
contracts still make one. The `token_budget` scheduler absorbs the extra calls.

## 5. Modules & functions (naming)

`gold/vision.py` (renamed from `gold/vision_verify.py`):

| Symbol | Role |
|---|---|
| `MODE_OFF / MODE_VERIFY / MODE_CORRECT` | the `vision.mode` enum values |
| `render_page_png(file_path, page_number, *, zoom)` | unchanged (PyMuPDF, best-effort) |
| `resolve_pages(rel, evidence_page, retrieval, scan_limit)` | **new** — page selection for unlocated fields |
| `verify_field(client, model, page_png, field_question, value, *, max_chars)` | unchanged — confirm/contradict |
| `correct_field(client, model, page_png, field_question, *, max_chars)` | **new** — returns a `Correction` `{value, evidence, page, verdict, rationale}` |
| `verdict_to_source_verified(verdict, current)` | unchanged |

`gold/fields.py`: the Plan 02 vision stage is generalized into a verify/correct
stage driven by `vision.mode`; adds the second batched judge call for corrected
fields; updates imports + `code_fingerprint` (module rename + new keys).

`gold/evidence.py`: `derive_trust` signature is **unchanged** — corrections flow
through the existing `match_type × judge_verdict × validation × source_verified`
fusion. Adds one new constant `MATCH_VISION_PAGE = "vision_page"` to the
`_LOCATED` set (§9.1) so an image-backed corrected value can be treated as
located; `verify_field` keeps returning `confirmed | contradicted | unclear`.

## 6. Schema changes (`contract_field_evidence`)

Additive (keep all Plan 02 columns; `schema.autoMerge` enabled):

| Column | Type | Meaning |
|---|---|---|
| `vision_action` | string | `none \| verify \| correct` — what the vision stage did |
| `value_source` | string | `model_text \| vision` — provenance of the final value |
| `original_value` | string | the pre-correction value (null when not corrected) — **calibration hook (§12)** |

`vision_verdict` / `vision_rationale` / `source_verified` / `evidence_di_confidence`
/ `evidence_page` are reused from Plan 02.

## 7. Config changes

`config/{dev,prod}.json` — replace the three Plan 02 `vision_verify_*` keys with
a nested `gold.vision` block:

```jsonc
"gold": {
  // ... existing ...
  "evidence_di_confidence_threshold": 0.7,   // unchanged (Plan 02)
  "vision": {
    "mode": "off",            // off | verify | correct   (was vision_verify_enabled)
    "model": null,            // default MAIN_MODEL/vision_model   (was vision_verify_model)
    "max_chars": 4000,        // (was vision_verify_max_chars)
    "correct_max_fields": 4,  // NEW — cap corrections per contract
    "page_scan_limit": 3      // NEW — cap page renders when no anchor
  }
}
```

All `gold.vision.*` keys + the module rename join the gold `code_fingerprint`.
**Migration:** `vision_verify_enabled:false → vision.mode:"off"`,
`vision_verify_model → vision.model`, `vision_verify_max_chars → vision.max_chars`.

## 8. Trust derivation impact

The trust *rules* are unchanged; corrections simply re-enter them with fresh
inputs. A vision-corrected value reaches `high` only when the **re-judge** returns
`correct` **and** its evidence is located — including the new `vision_page`
match (§9.1) — **and** `source_verified` is not `low`. The trust enum stays
`high/review/low/unknown`; a vision-sourced `high` is distinguished only by
`value_source = "vision"` (§9.4), not a new tier. Verify-only behavior is
identical to Plan 02. Update [trust_derivation.mmd](../trust_derivation.mmd) to
show the correct→re-judge→re-derive cycle alongside the existing verify path.

## 9. Resolved decisions

**9.1 Evidence semantics for a vision-corrected value — *page-as-evidence*.**
Because the premise is that DI text was wrong, a corrected value's quote often
won't locate in silver text. Resolution: introduce `match_type = "vision_page"`
(added to `evidence._LOCATED`), cited by `evidence_page`, so an image-backed
value counts as *located*. **Guardrail:** `vision_page` is assigned only when the
re-judge returns `correct` **and** `correct_field`'s own verdict is not `unclear`;
the value also carries `source_verified = "confirmed"`. Fully auditable via
`value_source = "vision"` + `match_type = "vision_page"` + `evidence_page`.

**9.2 Page selection when no anchor exists — *retrieval-first, scan fallback*.**
Reuse gold AI Search retrieval for the field question and read `page` directly
off the top retrieved chunk; if no chunk carries a page, fall back to a bounded
scan of the first `vision.page_scan_limit` pages. *Dependency confirmed:* the
chunk index already publishes `page` as a filterable/sortable field
([serving/search_index.py](../../src/contract_intelligence/serving/search_index.py)),
`contract_chunks` carries `page` per chunk, and `retrieve_chunks` already selects
and returns it ([gold/retrieval.py](../../src/contract_intelligence/gold/retrieval.py)) —
so `resolve_pages` reads the page natively with **no block-text-match heuristic**;
the scan fallback only covers chunks with a null page.

**9.3 Loop bounds — *single pass*.** One correction pass per contract
(`max_passes = 1`, a constant, not yet a config knob). Since each correction
already re-judges, one pass captures essentially all the gain while keeping cost
and SCD2 determinism predictable.

**9.4 Trust ceiling for corrected values — *allow `high`, always tag*.** A
vision-corrected, judge-`correct` value may reach full `high`, but **always**
carries `value_source = "vision"` (and `match_type = "vision_page"`) so a human or
downstream consumer can filter vision-sourced highs. No `high_vision` sub-tier —
the trust enum stays stable at `high/review/low/unknown`.

**9.5 Re-judge cost — *accepted*.** `mode = correct` makes at most two judge
calls per *corrected* contract (initial full judge + one batched re-judge);
clean contracts make one. Throttled by the existing `token_budget` scheduler.

## 10. Reprocessing impact

- Gold: module rename + new/renamed config keys → gold `code_hash` bumps →
  **re-extract all contracts once** (dev = 5; accepted, as in Plan 02).
- Silver: untouched.
- `contract_field_evidence` gains 3 columns (additive); drop the table once for
  the schema change (manual, per repo convention).

## 11. Validation

- Pick a contract with a **known OCR error** that produced a wrong value at
  `review`; confirm `mode=correct` fixes the value, the re-judge returns
  `correct`, trust becomes `high`, and `original_value`/`value_source=vision` are
  recorded.
- Confirm `mode=verify` reproduces Plan 02 results exactly (regression).
- Confirm `correct_max_fields` and `page_scan_limit` bound the work (no runaway
  vision calls on a messy contract).
- Confirm a clean contract triggers **zero** corrections.
- Re-run `eval/gold_eval.py`; expect fewer *wrong* values (not just fewer
  false-`high`) vs the Plan 02 baseline.

## 12. Roadmap (future plans — not in scope here)

Plan 03's provenance (`original_value`, `value_source`, `vision_action`,
`vision_verdict`) is deliberately the **feedstock** for what comes next:

- **PLAN_04 — Append-only audit trail.** Record each pipeline step
  (extract → judge → vision_verify → vision_correct → rejudge) as an immutable row
  in `contract_field_audit`, so the *initial* judge/verify verdicts and rationales
  that Plan 03 overwrites are preserved. Keeps `contract_field_evidence` as the
  slim final projection; the audit table is joined only when drilling in. *(See
  [PLAN_04_audit_trail.md](PLAN_04_audit_trail.md).)*
- **PLAN_05 — Feedback & calibration loop.** Capture human review decisions
  (accept / override / correct) in a `contract_field_review` table; join with
  `gold_eval_results` to *measure* per-tier trust precision/recall and *tune*
  thresholds (`fuzzy_threshold`, `evidence_di_confidence_threshold`,
  `di_quality_threshold`) from data instead of by hand — enabling safe
  auto-accept of `high`. Corrected-value acceptances are direct labels for "was
  the vision correction right?".
- **PLAN_06 — Self-consistency confidence.** Sample each (high-value) field K
  times (temperature or retrieval perturbation); use agreement as a *graded*
  `consistency_score` that complements categorical trust and flags unstable
  fields. Applied selectively under the existing `token_budget` scheduler.

These close the remaining state-of-the-art gaps (auditable + calibrated + graded
confidence); Plan 03 completes the extraction-fidelity ladder they build on.

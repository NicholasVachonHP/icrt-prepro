# Plan 04 — Append-only audit trail for the trust pipeline

**Status:** Proposed · **Date:** 2026-06-24 · **Scope:** gold provenance — new append-only `contract_field_audit` table + slim `contract_field_evidence` projection

## 1. Goal

Plan 03 lets vision **rewrite** a value (`original_value`, `value_source`,
`vision_action`), but the per-field row in `contract_field_evidence` only keeps
the *final* state. Several signals are **overwritten in place** during a
correction and are unrecoverable afterward:

| Signal | Kept today? | "Before" that is lost |
|---|---|---|
| `value` | ✅ `original_value` | — (the one good pattern already) |
| `judge_verdict` / `judge_rationale` | ❌ re-judge overwrites | the **initial** judge's verdict + reasoning |
| `vision_rationale` | ❌ correction overwrites | the **verify** rung's rationale |
| correction's own verdict | ❌ never stored | `correct`/`unclear` from `correct_field` |
| `match_type` | ❌ overwritten | the located match before it became `vision_page` |
| `evidence_text` | ❌ overwritten | the original DI quote |
| `source_verified` / `trust` | ❌ folded / re-derived | the pre-vision values |

So today you can see *that* a value changed but not *why the original was
distrusted* nor *what the first judge thought*. Plan 04 makes the trust pipeline
**fully auditable** by recording each processing step as an immutable row —
**without** overwriting and **without** widening the read path.

## 2. Design principle

- **Final projection + append-only log.** `contract_field_evidence` stays the
  authoritative *current* state (the answer + trust); a new
  `contract_field_audit` table is the ordered, immutable *history* of how that
  state was reached. The answer path never touches the audit table.
- **The actor is a value, not a column.** Every step shares one generic shape
  (`stage`, `verdict`, `rationale`, `*_after`). Adding a future rung is a new
  enum value, not a new pair of columns — no schema sprawl.
- **One source of truth in code.** The evidence row's final fields and the *last*
  audit row's `*_after` fields are written from the **same in-memory field-row**
  at the end of processing, so they can never drift.
- **Audit is a log, not a dimension.** The audit table is **append-only /
  immutable** — no SCD2, no `is_current`, no in-place updates. A reprocess writes
  a fresh set of rows under the new gold `version_id`.
- **Gated to what matters.** Intermediate steps are recorded only for fields that
  did something non-trivial (escalated / corrected / distrusted); clean fields
  stay cheap (§6).

## 3. Relationship to Plan 03 (extend, don't replace)

Nothing in Plan 02 / 03 changes. Plan 04 is **purely additive plumbing**:

- `contract_field_evidence` keeps every column it has today, still read as the
  final per-field state. (Optionally `original_value` *could* later move into the
  audit table, but this plan keeps it inline for convenience — §9.3.)
- `contract_fields` (wide) is untouched.
- The vision stage (`gold/fields.py::_run_vision_stage`) already mutates an
  in-memory `field_row` through the stages; Plan 04 simply **emits an audit row
  at each stage transition** from that same object. No logic in the trust rules
  changes.

This plan **takes the PLAN_04 slot** previously reserved in PLAN_03 §12 for the
feedback/calibration loop; that roadmap renumbers to PLAN_05 (calibration) and
PLAN_06 (self-consistency). The audit trail is a *prerequisite* for calibration —
it is the raw record those later plans measure against.

## 4. The audit table (`contract_field_audit`)

**Grain:** one row per *(gold `version_id`, `field_name`, processing step)*.
**Lifecycle:** append-only / immutable (never expired or overwritten).

A row is the state of a field *immediately after* one pipeline stage acted on it.
Read top-to-bottom by `step_seq`, the rows reconstruct the entire lifecycle of a
field for one extraction version.

### Stages (the `stage` enum)

| `stage` | Emitted when | `verdict` carries | `rationale` carries |
|---|---|---|---|
| `extract` | value first produced (Stage A/B) | — (null) | extraction method note |
| `judge` | initial full-contract judge | `correct/partial/incorrect/unverifiable` | the **initial** judge reasoning |
| `vision_verify` | vision verify rung ran | `confirmed/contradicted/unclear` | verify reasoning |
| `vision_correct` | vision rewrote the value | `correct/unclear` (from `correct_field`) | what vision read off the page |
| `rejudge` | judge re-ran on corrected value | `correct/partial/incorrect/unverifiable` | re-judge reasoning |

(`vision_verify` / `vision_correct` / `rejudge` rows appear **only** for fields
the vision stage actually touched.)

## 5. Schema (`contract_field_audit`)

```text
audit_id                string   not null  # sha(version_id|field_name|stage|step_seq)
version_id              string   not null  # joins evidence/wide; ties log to one extraction
relative_path           string   not null
file_name               string   not null
field_name              string   not null
step_seq                int      not null  # 1,2,3… preserves causal order
stage                   string   not null  # extract|judge|vision_verify|vision_correct|rejudge
verdict                 string   nullable  # that stage's verdict (null for extract)
rationale               string   nullable  # that stage's justification
value_after             string   nullable  # value as it stood after this step
evidence_text_after     string   nullable
match_type_after        string   nullable
source_verified_after   string   nullable
trust_after             string   nullable  # trust at this point (null before first judge)
page_after              int      nullable  # evidence/source page in play at this step
model                   string   nullable  # model that performed the step
created_at              timestamp not null # append timestamp
```

No `is_current` / `valid_from` / `valid_to` — this is a log, not an SCD2 table.
The **last** row (max `step_seq`) for a `(version_id, field_name)` is, by
construction, equal to that field's row in `contract_field_evidence`.

**Optional convenience view** `contract_field_audit_current` = audit rows whose
`version_id` is the live gold version (inner-join to the evidence active view),
so the default audit query excludes superseded extraction versions.

## 6. What gets written (gating)

`gold.audit.mode` controls verbosity:

| mode | Writes |
|---|---|
| `off` | nothing (table not written) |
| `gated` *(recommended)* | every step for **interesting** fields + a single terminal `extract`/`judge` row for the rest |
| `full` | every step for every field |

A field is **interesting** when `vision_action != "none"` **or**
`judge_verdict != "correct"` **or** final `trust != "high"` — i.e. anything a
human might want to inspect. In `gated` mode a clean, high-trust, vision-untouched
field produces **one** row (its terminal state), keeping the table small and
reserved for genuine audit. **Convention:** a field with only a terminal row
means "nothing notable happened" — *not* missing data (§8 issue 2).

## 7. Friendly labels (data dictionary)

The naming work collapses to the slim evidence columns (unchanged) plus the
audit table's generic columns and the `stage` enum:

| Column / enum | Friendly label | Meaning |
|---|---|---|
| `stage = extract` | **Extracted** | text model produced the value |
| `stage = judge` | **Initial Judge** | first judge on the original value |
| `stage = vision_verify` | **Vision Check** | confirm/contradict the value on its source page |
| `stage = vision_correct` | **Vision Correction** | value re-read off the page image |
| `stage = rejudge` | **Re-Judge** | judge re-run on the corrected value |
| `step_seq` | **Step Order** | 1, 2, 3… causal order |
| `verdict` | **Stage Verdict** | the verdict this stage produced |
| `rationale` | **Stage Reasoning** | this stage's justification |
| `value_after` | **Value After This Step** | value as it stood after the stage |
| `match_type_after` | **Match After This Step** | evidence match at this point |
| `source_verified_after` | **Source Verification After** | source_verified at this point |
| `trust_after` | **Trust After This Step** | trust at this point |
| `page_after` | **Source Page** | page in play for this step |
| `model` | **Model** | model that performed the step |
| `created_at` | **Logged At** | append timestamp |

## 8. Potential issues (and mitigations)

1. **Drift between evidence and audit.** The evidence final fields and the last
   audit `*_after` must agree. *Mitigation:* derive both from the same in-memory
   field-row at end of processing — evidence = "final state," audit = "the
   intermediate snapshots that produced it"; never compute them independently.
2. **"No audit rows" ambiguity.** In `gated` mode, absence of intermediate rows
   must read as "nothing notable happened," not "data missing." *Mitigation:*
   always emit at least the terminal row, and document the convention (§6).
3. **Unbounded growth.** Append-only + reprocessing accumulates rows under each
   `version_id`. *Mitigation:* a retention policy — **open decision** (§9.1).
4. **Version linkage.** Audit rows must carry the gold `version_id` so a
   reprocess never interleaves old/new steps and joins stay clean.
5. **Outputs, not config.** The audit table and its columns stay **out of**
   `code_fingerprint` — adding audit must not, by itself, force a re-extract.
6. **History is forward-only.** Because audit is not fingerprinted, existing
   contracts gain no retroactive history; the trail is populated from the next
   run onward (re-run gold to backfill the current versions).

## 9. Open decisions

**9.1 Retention.** How long to keep superseded-version audit rows? Candidates:
keep last *N* gold versions per contract (e.g. `gold.audit.retention_versions =
3`), keep *D* days, or keep everything (dev scale = trivial). Recommend a config
knob defaulting to "keep all" in dev, a bounded value in prod.

**9.2 `gated` thresholds.** The "interesting" predicate (§6) reuses
`vision_action`, `judge_verdict`, `trust`. Confirm this captures the fields a
reviewer cares about before committing.

**9.3 Keep `original_value` inline?** It is now duplicated by the audit log
(`value_after` of the `extract`/`judge` rows). Keep it on the evidence row for
single-row convenience (recommended), or drop it to avoid duplication once the
audit table is trusted. Defer.

## 10. Schema / reprocessing impact

- **New table** `contract_field_audit` (append-only) — create on first write.
- `contract_field_evidence` / `contract_fields` — **unchanged** (no new columns).
- **No `code_hash` bump** (audit is not fingerprinted) → **no forced
  re-extraction**. Re-run gold once to populate the audit trail for the current
  contract versions (§8 issue 6).
- Config gains a `gold.audit` block (`mode`, `retention_*`) — runtime knobs, not
  fingerprinted.

## 11. Validation

- Run the Boots `notable_clauses` correction case; confirm five ordered audit
  rows (`extract → judge → vision_verify → vision_correct → rejudge`), each with
  its own untouched `verdict`/`rationale`, and that the final `rejudge.trust_after`
  equals the evidence row's `trust` (`high`).
- Confirm a clean, high-trust, vision-untouched field produces exactly **one**
  audit row in `gated` mode.
- Confirm the answer/trust read path (`contract_fields`,
  `contract_field_evidence`) returns identical results **without** joining the
  audit table.
- Confirm `mode = off` writes no audit table and changes nothing else.
- Confirm a reprocess writes new rows under the new `version_id` and leaves prior
  rows intact (until retention prunes them).

## 12. Pipeline orchestration (`ictr_pl`)

The daily pipeline `ictr_pl` (id `c97ff2c1-b640-4143-8c96-23f1a01e94cb`)
currently fans **nb_03** (chunk/embed/index) and **nb_04** (gold fields) out in
**parallel** after silver. That was safe before, but gold now *reads* the AI
Search index — RAG / retrieve-classify groups, Plan 03 vision retrieval-first
page selection, and the Plan 04 re-judge — so a parallel run lets gold query a
half-rebuilt index.

**Change (orchestration only, no code):** serialize
`bronze → silver → nb_03 → nb_04`, i.e. make nb_04 depend **On success** of
nb_03. Keep `RECREATE_INDEX = False`: the index build is incremental and
self-healing — it upserts live chunks and deletes superseded ones (a replaced
company contract flips `is_current = false` and its chunks are pruned), so gold
always reads a current index
([serving/search_index.py](../../src/contract_intelligence/serving/search_index.py)).
Cost is a slightly longer nightly window (sum, not max, of the two notebooks),
accepted at this scale.

**Tooling reality — applied manually.** The agent's Fabric tools expose only
*list / get / create / run* for pipelines (no edit-definition API), and OneLake
writes are unauthorized here (403). So the agent **cannot push this edit** — it
is a one-time change in the Fabric pipeline UI: set nb_04's activity dependency
to *On success* of nb_03 and remove the parallel edge. The agent will read the
current definition (`get-pipeline`) to give the exact activity names and confirm
no other activity depends on the old parallel layout before you make the change.

**Out of scope:** nb_04 stays a **single** notebook (extract → judge → verify →
correct → re-judge → audit in one transaction). Splitting "find answers" from
"verify/correct" is revisited only if vision ever needs a separate cadence/budget
from extraction — not required here.

## 13. Roadmap (renumbered from PLAN_03 §12)

- **PLAN_05 — Feedback & calibration loop.** Capture human review decisions
  (accept / override / correct) in `contract_field_review`; join with the audit
  trail + `gold_eval_results` to measure per-tier trust precision/recall and tune
  thresholds (`fuzzy_threshold`, `evidence_di_confidence_threshold`,
  `di_quality_threshold`) from data — enabling safe auto-accept of `high`.
  Corrected-value acceptances are direct labels for "was the vision correction
  right?" The audit table is its feedstock.
- **PLAN_06 — Self-consistency confidence.** Sample each high-value field K times
  (temperature / retrieval perturbation); use agreement as a graded
  `consistency_score` that complements categorical trust, under the existing
  `token_budget` scheduler.

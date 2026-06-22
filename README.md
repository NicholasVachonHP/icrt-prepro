# Contract Intelligence — Shared Code & Config

This is the **shared lakehouse** (`ictr_lh_shared`) for the Contract Intelligence
pipeline. It holds the Python package, environment configuration, and design docs
that every orchestration notebook mounts at runtime. No data tables live here —
only the code and config that the bronze/silver/gold notebooks import.

The pipeline ingests contracts (PDF / DOCX / DOC) from SharePoint, extracts and
chunks their text, embeds the chunks into Azure AI Search for semantic
retrieval, and uses GPT-4.1 to extract structured comparison fields — each with
the **verbatim evidence quote** it came from and an LLM-judged **trust** signal —
all on a **medallion architecture** (bronze → silver → gold) in Microsoft Fabric,
with full **Slowly Changing Dimension Type 2 (SCD2)** version history at every
layer.

---

## Table of Contents

- [1. Folder Layout](#1-folder-layout)
- [2. Pipeline Overview](#2-pipeline-overview)
- [3. Medallion Layers & Tables](#3-medallion-layers--tables)
- [4. Versioning & Change Tracking (SCD2)](#4-versioning--change-tracking-scd2)
- [5. Forcing a Re-run](#5-forcing-a-re-run)
- [6. Configuration](#6-configuration)
- [7. Secrets](#7-secrets)
- [8. How a Notebook Bootstraps](#8-how-a-notebook-bootstraps)
- [9. Running the Pipeline](#9-running-the-pipeline)
- [10. Rebuilding Tables After a Schema Change](#10-rebuilding-tables-after-a-schema-change)
- [11. Source Modules](#11-source-modules)
- [12. Local Git Mirror (`sync.ps1`)](#12-local-git-mirror-syncps1)

---

## 1. Folder Layout

```
Files/
├── README.md                  ← this file
├── config/
│   ├── dev.json               ← dev environment config
│   ├── prod.json              ← prod environment config
│   └── extraction_fields.json ← gold field/question definitions (GPT prompt)
├── docs/
│   ├── ADR_ContractIntelligence_2026-06-08.md
│   └── ICTR_Architecture.mmd  ← Mermaid architecture diagram
└── src/
    └── contract_intelligence/
        ├── common/            ← shared helpers (bootstrap, config, scd2, versioning, ai_clients)
        ├── bronze/            ← ingest.py
        ├── silver/            ← extract.py, chunk.py
        ├── gold/              ← fields.py, evidence.py
        └── serving/           ← search_index.py
```

---

## 2. Pipeline Overview

```
SharePoint (PDF / DOCX / DOC)
        │  OneLake shortcut
        ▼
Bronze  ictr_lh_bronze_dev   contract_inventory      (file inventory + content hash)
        ▼
Silver  ictr_lh_silver_dev   contract_text           (extracted plain text)
        ├──────────────► contract_chunks         (512-token chunks, 64 overlap)
        │                       │ embed
        │                       ▼
        │                Azure AI Search  (ictr_dev index, HNSW / cosine / 3072-dim)
        ▼
Gold    ictr_lh_gold_dev     contract_fields        (10 structured fields via GPT-4.1)
        └──────────────► contract_field_evidence  (per-field evidence quote + trust)
```

Orchestrated by a Fabric Data Pipeline (`nb01 → nb02 → nb03 & nb04 in parallel`).
Each notebook is a thin shell; **all logic lives in this package** so it can be
unit-reasoned, reviewed, and reused across notebooks.

---

## 3. Medallion Layers & Tables

| Layer | Lakehouse | Table | Produced by | Contents |
|-------|-----------|-------|-------------|----------|
| Bronze | `ictr_lh_bronze_dev` | `contract_inventory` | `bronze/ingest.py` | One row per file content version: name, path, size, `content_hash` |
| Silver | `ictr_lh_silver_dev` | `contract_text` | `silver/extract.py` | Extracted plain text per contract version |
| Silver | `ictr_lh_silver_dev` | `contract_chunks` | `silver/chunk.py` | Overlapping token chunks (AI Search document keys) |
| Gold | `ictr_lh_gold_dev` | `contract_fields` | `gold/fields.py` | 10 structured fields (parties, dates, value, governing law, …) |
| Gold | `ictr_lh_gold_dev` | `contract_field_evidence` | `gold/fields.py` + `gold/evidence.py` | One row per field per contract version: the verbatim evidence quote, `match_type` (is the quote real), `judge_verdict` (does it answer the question), and a fused `trust` (high / review / low / unknown) |
| Serving | Azure AI Search | `ictr_dev` index | `serving/search_index.py` | Embedded live chunks for semantic / vector search |

Each table also has a companion **`*_active` view** exposing only the live
version of each contract (`WHERE is_current = true AND doc_deleted = false`).

---

## 4. Versioning & Change Tracking (SCD2)

Every layer keeps **full version history**. A unified metadata contract is used
across all tables:

| Column | Meaning |
|--------|---------|
| `version_id` | Surrogate key = `sha256(natural_key \| content_hash \| code_hash)[:16]` |
| `content_hash` | Did the **input data** change? |
| `code_hash` | Did the **processing code / prompt / params** change? |
| `valid_from` / `valid_to` | Version lifespan (`valid_to` is `NULL` while live) |
| `is_current` | Marks the live version of a key |
| `doc_deleted` | Source document removed upstream (tombstone) |
| `file_name` | Standardized document name |

A contract is **reprocessed by a stage when either** its `content_hash` changed
(new input) **or** that stage's `code_hash` changed (you edited the code, prompt,
chunk size, model, etc.). The `code_hash` is computed by `common/versioning.py`
from the stage's source file(s) plus its relevant config — so editing
`gold/fields.py` or the system prompt automatically re-runs gold over unchanged
contracts. This applies to **all four layers, including bronze** (editing
`bronze/ingest.py` re-versions every file on the next run).

The SCD2 write mechanics live in `common/scd2.py`:
- **`scd2_merge`** — tables with one current row per key (`contract_text`,
  `contract_fields`). A changed `version_id` expires the prior row and inserts the
  new one; an identical `version_id` (a forced rerun or error retry) **overwrites
  the live row in place** so corrected results actually persist without polluting
  history.
- **`scd2_expire_and_append`** — tables with many current rows per key
  (`contract_chunks`, `contract_field_evidence`). Re-chunking / re-extraction
  expires the old row set and appends the new one; an identical-version rerun
  uses **delete-on-same-version** so `chunk_id` / `evidence_id` keys never
  duplicate.

---

## 5. Forcing a Re-run

The pipeline is idempotent, so a transient bug or connection failure won't
re-process a contract whose `content_hash` and `code_hash` are unchanged. Two
mechanisms cover recovery:

**Automatic — error-aware retry.** Silver and gold automatically re-pick any live
row whose previous attempt recorded an `extraction_error`. No action needed.

**Manual — `force_paths`.** Each stage's `run()` accepts a `force_paths` argument
(and the notebooks expose a `FORCE_PATHS` parameter cell):

| Value | Effect |
|-------|--------|
| `None` | Normal incremental run (default) |
| `["folder/a.pdf", "b.docx"]` | Force just these `relative_path` values |
| `"ALL"` | Force every active contract |

A `reprocess.force_paths` key in the env config acts as a fallback when the
notebook parameter is `None`. Forced rows are overwritten in place (no history
pollution). Bronze has no `force_paths` because it re-scans and re-hashes every
file on each run.

---

## 6. Configuration

`config/dev.json` and `config/prod.json` are the source of truth (selected by the
notebook's `ENV` parameter). Key sections:

| Section | Purpose |
|---------|---------|
| `lakehouse` | Names of the bronze / silver / gold / shared lakehouses |
| `bronze` | Scan folder (`files_dir`, `scan_subdir`), target table, active view |
| `silver` | Source bronze table, target text/chunks tables and views |
| `gold` | Source silver text table, target fields table, `fields_config`, `max_input_chars`; evidence/trust keys (`fields_evidence_table`, `judge_enabled`, `judge_model`, `fuzzy_threshold`, `evidence_max_chars`) |
| `serving` | Chunks source table, upload batch size |
| `chunking` | `chunk_size` (512), `chunk_overlap` (64), `embedding_dimensions` (3072) |
| `reprocess` | `force_paths` fallback for forced re-runs |
| `azure_openai` / `ai_search` | Reference values (endpoints/keys read from env at runtime) |

`config/extraction_fields.json` defines the 10 fields (and their natural-language
questions) that gold extracts; editing it changes gold's `code_hash` and triggers
re-extraction.

---

## 7. Secrets

Secrets are **not** stored in this lakehouse. They are loaded at runtime from the
Fabric **environment resource** file `.env_temp_fabric_ictr` (attached via the
`ictr_dev` Spark environment) into `os.environ` by `common/config.py`
(`load_secrets`). Endpoints and keys (Azure OpenAI, Azure AI Search) are read via
`os.getenv`.

> ⚠️ **Post-MVP:** migrate these secrets to Azure Key Vault and read them with
> `notebookutils.credentials.getSecret(...)`.

---

## 8. How a Notebook Bootstraps

Each orchestration notebook is intentionally thin:

1. **Stub cell** — mounts `ictr_lh_shared` Files at `/shared_code` and adds
   `src/` to `sys.path` (chicken-and-egg: shared code can't be imported until the
   shared lakehouse is mounted).
2. **`bootstrap(...)`** (`common/bootstrap.py`) — loads config, **verifies the
   attached default lakehouse matches the expected layer** (hard-fails on
   mismatch), loads secrets, and optionally mounts other lakehouses read-only
   (e.g. silver mounts bronze; gold mounts silver). Returns a context dict with
   `cfg` and mount paths.
3. **`run(...)` / `ingest(...)`** — calls the layer module with `config=cfg` plus
   any mount paths and `force_paths`.

---

## 9. Running the Pipeline

| # | Notebook | Default lakehouse | Mounts | Calls |
|---|----------|-------------------|--------|-------|
| 01 | `ictr_nb_01_bronze_ingest` | bronze | — | `bronze.ingest.ingest()` |
| 02 | `ictr_nb_02_silver_extract` | silver | bronze (read) | `silver.extract.run()` |
| 03 | `ictr_nb_03_silver_chunk_embed_index` | silver | — | `silver.chunk.run()` + `serving.search_index.run()` |
| 04 | `ictr_nb_04_gold_fields` | gold | silver (read) | `gold.fields.run()` |

Run order: **01 → 02 → (03 ‖ 04)**. Set the `ENV` parameter (`"dev"` / `"prod"`)
at the top of each notebook; set `FORCE_PATHS` only when recovering specific
contracts.

---

## 10. Rebuilding Tables After a Schema Change

When a layer's table schema changes (e.g. adding `code_hash` to bronze), drop the
old table + view in that layer's lakehouse and re-run from that stage forward:

```python
# In the affected layer's lakehouse notebook
spark.sql("DROP VIEW IF EXISTS contract_inventory_active")
spark.sql("DROP TABLE IF EXISTS contract_inventory")
```

If the `contract_chunks` `chunk_id` scheme changes, also delete and let
`serving.search_index.run()` recreate the Azure AI Search index so stale document
keys don't orphan.

---

## 11. Source Modules

| Module | Responsibility |
|--------|----------------|
| `common/bootstrap.py` | Mount lakehouses, load config/secrets, verify attached layer |
| `common/config.py` | Load `{env}.json`; parse `.env` secrets into `os.environ` |
| `common/versioning.py` | `code_fingerprint()` — deterministic `code_hash` per stage |
| `common/scd2.py` | SCD2 writers, `version_id()`, and `force_paths` helpers |
| `common/ai_clients.py` | Azure OpenAI client + embedding helpers |
| `bronze/ingest.py` | Scan files, hash content, MERGE `contract_inventory` |
| `silver/extract.py` | Extract text from PDF/DOCX/DOC → `contract_text` |
| `silver/chunk.py` | Token-chunk text → `contract_chunks` |
| `gold/fields.py` | GPT-4.1 structured field extraction → `contract_fields`, plus per-field evidence/trust → `contract_field_evidence` |
| `gold/evidence.py` | Locate the evidence quote in the contract, LLM-judge value↔question, derive the `trust` category |
| `serving/search_index.py` | Embed live chunks; upsert/prune the AI Search index |

For the full design rationale, see
[docs/ADR_ContractIntelligence_2026-06-08.md](docs/ADR_ContractIntelligence_2026-06-08.md)
and the architecture diagram [docs/ICTR_Architecture.mmd](docs/ICTR_Architecture.mmd).

---

## 12. Local Git Mirror (`sync.ps1`)

> This section applies **only when working in the local Git clone of this repo**,
> not in the Fabric lakehouse. `sync.ps1` exists only on your machine — it is
> git-ignored and excluded from the mirror, so it has no effect anywhere else.

`sync.ps1` keeps a **local Git copy** of this lakehouse's `Files/` in step with
OneLake so the code and config can be version-tracked. Run it from the root of
the local repo:

```powershell
.\sync.ps1
```

When run in the local repo it performs **four steps**:

1. **Refresh prompt** — reminds you to right-click the OneLake folder in Windows
   File Explorer, choose **"Sync from OneLake"**, and wait for it to finish, then
   asks `y/n` before continuing. (There is no CLI to force that cloud refresh, so
   this manual step ensures the local OneLake cache holds the latest cloud changes.)
2. **Hydration** — downloads any cloud-only placeholder files and waits until
   they are all present locally (`-TimeoutMinutes`, default 10).
3. **Mirror** — `robocopy /MIR` copies the OneLake `Files/` into the repo,
   excluding `sync.ps1`, `.gitignore`, `.git/`, `__pycache__/`, `*.pyc`, and `*.pyo`.
4. **Handoff** — staging, commit, and push are left to you.

After it finishes, review and commit the changes:

```powershell
git status
git add .
git commit -m "Sync from OneLake"
git push
```


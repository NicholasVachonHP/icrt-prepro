# Plan OPS — Stand up the isolated `Contract Intelligence - Dev` workspace

**Status:** Proposed · **Date:** 2026-06-30 · **Scope:** operational — finish moving
the ICTR project off the shared sandbox into its own Fabric workspace backed by the
`CCoE_Agents` monorepo. Solo developer. Hand this file to an agent running in VS Code
**connected to the new workspace**.

> **Sibling plans.** [PLAN_OPS_productionization.md](PLAN_OPS_productionization.md) is
> the full dev→demo→prod track (wheel, Variable Library, Key Vault, deployment
> pipeline, serving tier). **This** plan only does the one-time dev cut-over.
> [PLAN_04_audit_trail.md](PLAN_04_audit_trail.md) §12 covers the
> nb_03→nb_04 pipeline ordering (already applied).

---

## 0. Context & current state (read first)

You are an agent in VS Code connected to the Fabric workspace
**`Contract Intelligence - Dev`**. A solo developer has already done the repo + sync
groundwork; your job is the lakehouse/data/runtime wiring (Steps A–F below).

**Monorepo:** `CCoE_Agents` (Azure DevOps, many folders). Two folders matter here:

| Folder | Contents | Role |
|---|---|---|
| `ICTR-Fabric/` | `ictr_dev.Environment`, `ictr_pl.DataPipeline`, `ictr-notebooks/` (**nb01–nb05**) | Fabric **items** — Git-synced to the workspace |
| `ICTR-Core/` | empty for now; will hold the `contract-intelligence` **package** source | **Code** — NOT a Fabric item, never Git-synced to Fabric |

**Already done (do NOT redo):**
- ✅ `Contract Intelligence - Dev` workspace **is Git-synced** to
  `CCoE_Agents/ICTR-Fabric`. The four pipeline notebooks, `nb05`, `ictr_pl`, and the
  `ictr_dev` Environment **already exist as items** in the workspace.

**Still to do = this plan (Steps A–F).**

### The two delivery systems (the model that explains everything)

Fabric Git integration can carry *item definitions* but **cannot carry lakehouse
`Files/` (your code) or lakehouse data**. So there are two independent paths:

| What | Lives in | Delivered by | Source of truth |
|---|---|---|---|
| Notebooks, `ictr_pl`, `ictr_dev` Environment | the workspace | **Fabric Git** ← `ICTR-Fabric/` | `ICTR-Fabric/` |
| Library code `contract_intelligence/…` | lakehouse `ictr_lh_shared/Files/src` at runtime | **OneLake Explorer upload** (wheel later) | **`ICTR-Core/`** |
| Data (bronze/silver/gold tables) | each workspace's lakehouses | not delivered — re-ingested | n/a |

Notebooks load the library at **runtime** via the mount-and-reload block
(`sys.path.append('…/Files/src'); import contract_intelligence; reload_package()`).
The coupling is an import path, not a Git link. **No wheel is built yet** — dev keeps
the fast mount loop; the wheel is introduced only at the dev→demo boundary
(PLAN_OPS §5), built later from the `ICTR-Core` tree (never a forked copy).

---

## 1. Inputs the developer must provide before you start

- **Capacity:** confirm `Contract Intelligence - Dev` is assigned to a Fabric capacity
  (required to run Spark).
- **Secrets:** the `.env_temp_fabric_ictr` Environment-resource contents (Azure AI
  Search key, Azure OpenAI key, etc.). **Never print or request secret values via
  chat** — have the developer paste them directly into the `ictr_dev` Environment
  resource in the Fabric UI.
- **Bronze source:** the dev **SharePoint** folder/connection `nb01_bronze_ingest`
  reads (~4 dev contracts).
- **AI Search / Azure OpenAI:** confirm this workspace may use the same services, or
  provision dev-scoped ones. `config/dev.json` uses index `ictr_dev_di_vision`.

---

## 2. Step A — Create the lakehouses (no env suffix)

Create four lakehouses in **this** workspace, using **suffix-free names**:

| Lakehouse | Notes |
|---|---|
| `ictr_lh_bronze` | empty shell |
| `ictr_lh_silver` | **schema-enabled** (silver writes `Tables/dbo/<name>`) |
| `ictr_lh_gold` | empty shell |
| `ictr_lh_shared` | hosts code `Files/src`, `Files/config`, `Files/docs` |

Data is **not** promoted — these start empty and are repopulated by the pipeline (Step F).

- **Verify:** list workspace items; all four lakehouses exist; silver is schema-enabled.

---

## 3. Step B — Rewire item bindings to THIS workspace

After a cross-workspace Git sync, references can still point at the **old sandbox**
(workspace id `c1b5c810-423c-44b8-b8e6-bb0a61e497da`). Fix both:

1. **Pipeline `ictr_pl`** — each notebook activity embeds a `workspaceId` and
   `notebookId`. The sandbox notebook ids were nb01 `f216ef99…`, nb02 `0bf1ba35…`,
   nb03 `e9cdd45a…`, nb04 `74a705b4…` (the **synced** copies have new ids in this
   workspace). Confirm every activity now references **this workspace's** notebooks.
   The chain must read **nb01 → nb02 → nb03 → nb04** (nb04 depends on **nb03** *On
   success* — gold reads the index nb03 builds). If pipeline tooling here is
   list/get/create/run only, this is **[MANUAL]** in the pipeline canvas: open each
   activity → reselect the notebook in this workspace → Save/Publish.
2. **Each notebook's default lakehouse** — synced notebooks keep whatever default the
   definition recorded (likely the sandbox's). Repoint each to this workspace's
   lakehouses (`fabric_setDefaultLakehouseTool` if available, else **[MANUAL]** per
   notebook).

- **Verify:** pipeline graph is nb01→nb02→nb03→nb04 with no edge/reference to
  `c1b5c810…`; each notebook shows a default lakehouse in this workspace.

---

## 4. Step C — Repoint the `contract-intelligence` package to `ICTR-Core`  **[MANUAL / developer git]**

`ICTR-Core` is empty and currently linked to **another (old) remote**; that link must
change so the package lives in `CCoE_Agents/ICTR-Core` as its single source of truth.

1. Move the package source (today under `ictr_lh_shared/Files/src/contract_intelligence/`)
   into `CCoE_Agents/ICTR-Core/` (suggested: `ICTR-Core/src/contract_intelligence/`).
2. Repoint the working copy's remote from the old URL to `CCoE_Agents`, then push:
   ```pwsh
   git remote set-url origin <CCoE_Agents repo URL>
   git push origin <branch>
   ```
3. **One source of truth:** from now on you author the library in `ICTR-Core` and
   **mirror** it into the lakehouse `Files/src` for the dev mount loop (Step D). Do
   **not** keep a second authoritative copy. (When the wheel arrives later, build it
   from this same `ICTR-Core` tree.)

- **Verify:** `ICTR-Core` contains the package and its remote points at `CCoE_Agents`;
  the old remote is no longer referenced.

---

## 5. Step D — Upload code + config + docs to `ictr_lh_shared`  **[MANUAL]**

Fabric Git does not carry lakehouse `Files/`, so push these via **OneLake Explorer**
into `ictr_lh_shared/Files/`:

- `Files/src/contract_intelligence/…`  (the `ICTR-Core` package, mirrored)
- `Files/config/`  (`dev.json`, and `prod.json` if present)
- `Files/docs/`  (**including this plan and the other `docs/plans/*`** — note 5)

Keep the dev mount-and-reload inner loop (no wheel yet).

- **Verify:** `Files/config/dev.json`, `Files/src/contract_intelligence/gold/fields.py`,
  and `Files/docs/plans/` are all present. (OneLake **writes via MCP may 403** in some
  workspaces — if so this stays a manual OneLake Explorer action, not an agent write.)

---

## 6. Step E — Drop the `_dev` suffix in config  (edit the **uploaded** copy)

Lakehouses are now suffix-free (Step A), so update the **uploaded** `dev.json` in
**this** workspace's `ictr_lh_shared/Files/config/dev.json` to match. **Do not edit the
sandbox's source copy** — the sandbox still uses the suffixed names and must keep
working.

Rename every lakehouse-name reference:

| Old key/value | New |
|---|---|
| `ictr_lh_bronze_dev` | `ictr_lh_bronze` |
| `ictr_lh_silver_dev` | `ictr_lh_silver` |
| `ictr_lh_gold_dev` | `ictr_lh_gold` |

⚠️ Gold reads silver **by path**; some gold keys embed the silver lakehouse name plus a
`dbo/` schema prefix — update those name segments too, leaving `dbo/` intact. After
editing, **re-read the file** to confirm it is still valid JSON (a broken brace won't be
caught by lint).

- `prod.json` suffix cleanup is **deferred** until the prod workspace is created
  (PLAN_OPS §8).
- **Verify:** every `ictr_lh_*` reference in `dev.json` is suffix-free and resolves to a
  lakehouse that exists in this workspace.

---

## 7. Step F — Environment, first run, audit backfill

1. **Environment `ictr_dev`:** confirm it carries the `.env_temp_fabric_ictr` resource
   and the **`pymupdf`** library (vision page render). If missing, **[MANUAL]** add the
   resource + `pymupdf` under Custom Libraries → **Publish** (minutes), then attach the
   Environment to the notebooks / set as workspace default.
2. **First full run:** trigger `ictr_pl` with `ENV=dev` against the dev SharePoint
   folder → repopulates bronze→silver→gold for the ~4 dev contracts.
3. **Backfill the audit table:** `contract_field_audit` is append-only and is created
   **only when contracts are actually processed**; enabling audit does not backfill and
   is *not* fingerprinted. Run gold once **force-reprocessed** — set
   `nb04_gold_fields`'s `FORCE_PATHS="ALL"` (config `reprocess.force_paths=ALL` / the
   notebook `FORCE_PATHS` param) so all contracts are pending → the table **and** the
   `contract_field_audit_current` view get created.
   - Pre-existing quirk (don't "fix" without asking): nb03 uses `FORCE_PATH`
     (singular); nb02/nb04 use `FORCE_PATHS` (plural); the pipeline `FORCE_PATH_RUN`
     default may look like stray backticks rather than empty.

- **Verify:** silver `dbo/*` tables, gold `contract_field_evidence` +
  `contract_field_audit`, and the `ictr_dev_di_vision` Search index are populated; a
  query on `contract_field_audit_current` returns one terminal row per
  `(version_id, field_name)`.

### nb05 (standalone — not in `ictr_pl`)
`nb05` (eval / quality gate) is **not** part of the pipeline chain; it runs on demand
after a gold run. Do **not** add it to `ictr_pl`. Just confirm it imports
`contract_intelligence` cleanly and points at this workspace's lakehouses.

---

## 8. Gotchas specific to this setup

- **Two Git relationships — don't conflate.** Fabric Git carries items via
  `ICTR-Fabric/`; the package is delivered separately (`ICTR-Core` → OneLake upload now,
  wheel later). They meet only at notebook **runtime** via the import.
- **`ICTR-Core` remote must actually change.** Until its remote points at
  `CCoE_Agents`, you have a split-brain package source. Fix it (Step C) before editing
  code in two places.
- **Don't sync this workspace to anything but `ICTR-Fabric`.** The monorepo holds many
  other CCoE projects.
- **AI Search index collision.** If this workspace and the old sandbox both write
  `ictr_dev_di_vision` on the same Search service, they clobber each other. Use a fresh
  dev index/service, or retire the sandbox first.
- **Scheduled identity ≠ interactive identity.** Scheduled `ictr_pl` runs as the
  workspace/pipeline identity; Search / AOAI / SharePoint (and later Key Vault) access
  must be granted to *that* identity.
- **Config JSON breakage is silent.** Lint won't catch a bad brace after the suffix
  rename — re-read `dev.json` to confirm it parses.
- **Confirm what `ICTR-Fabric` actually serialized.** If any item didn't round-trip
  through Git, recreate it by hand.

---

## 9. Done when

- All pipeline notebooks run green via `ictr_pl` (nb01→nb02→nb03→nb04, `ENV=dev`)
  against the dev source, with no reference to the old sandbox (`c1b5c810…`).
- bronze/silver/gold tables **and** the `ictr_dev_di_vision` index are populated in
  **this** workspace.
- `contract_field_audit` exists after the one-time force-reprocess.
- `dev.json` uses suffix-free lakehouse names that resolve in this workspace.
- `ICTR-Core` is the package's single source of truth (remote = `CCoE_Agents`); the
  lakehouse `Files/src` is a mirror for the dev mount loop.
- `Files/docs` (incl. `docs/plans/*`) is present in `ictr_lh_shared`.

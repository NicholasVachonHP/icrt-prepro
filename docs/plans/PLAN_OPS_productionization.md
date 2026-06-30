# Plan OPS — Dev → Demo → Prod productionization

**Status:** Proposed · **Date:** 2026-06-29 · **Scope:** operational/infra track —
3 mirrored workspaces, code as a Python package, Variable Library config, Key
Vault secrets, Fabric deployment pipeline, and the serving tier (DAB on ACA →
Foundry agent). Solo developer.

> **Track note.** This is an *operational* plan, parallel to the analytical
> roadmap. It does **not** consume the PLAN_05 (calibration) or PLAN_06
> (self-consistency) numbers — it runs alongside them. Execute it **after Plan 04
> lands and is stable in dev**, so you package a settled codebase.

## 1. Goal

Move from a single shared dev workspace to three promotion stages —
**dev → demo → prod** — with **one** source of truth for code and a clean,
repeatable promotion path, sized for a **single developer**:

| Stage | Purpose | Contracts | Posture |
|---|---|---|---|
| **dev** | Build anything, fast inner loop | ~4 | `vision.mode=correct`, tiny budget |
| **demo** | Integration rehearsal, prod-like | ~25 | prod settings, real promotion test |
| **prod** | Live | ~1000 | cost-tuned (likely `vision.mode=verify`) |

## 2. Current state (honest snapshot)

- **Fabric items already in Git** (Azure DevOps): notebooks, pipeline (`ictr_pl`),
  environments are versioned. ✅
- **Lakehouse code is versioned by push only:** you sync
  `ictr_lh_shared/Files/src/contract_intelligence/` locally via OneLake Explorer
  and push to an ADO repo (there is a `.git/` under `Files/`). It is *not*
  pip-installable and *not* promoted by Fabric.
- **Code is loaded by mounting** `Files/src` + `reload_package()` — works for one
  workspace, but lakehouse **Files are not promoted** by Git or deployment
  pipelines, so 3 workspaces would mean hand-copying code three times.
- **Secrets** come from a Fabric *environment resource* file
  (`.env_temp_fabric_ictr`) parsed by `common.config.load_secrets`. `config.py`
  already carries a `TODO (security)` to move these to Key Vault via
  `notebookutils.credentials.getSecret`. ([common/config.py](../../src/contract_intelligence/common/config.py))
- **Config** is a per-env JSON loaded by `common.config.load_config` from
  `Files/config/{env}.json` (today: `dev.json`, `prod.json`).

## 3. Target architecture

```
        Azure DevOps                          Fabric
  ┌────────────────────┐          ┌──────────────────────────────┐
  │ ictr monorepo:     │──Git────▶│ dev workspace                │
  │  fabric/ (nb, pl,  │          │  notebooks · ictr_pl · env   │──┐
  │   env, var lib)    │          │  lakehouses(bronze/silver/   │  │ deployment
  ├────────────────────┤          │   gold) · Variable Library   │  │ pipeline
  │  core/ (library)   │  build   │  (value set: dev)            │  │ (promote
  │  dab/ (→ §9)        │──wheel──▶│  Environment ← .whl          │  │  defs only)
  └────────────────────┘          ├──────────────────────────────┤  ▼
                                  │ demo workspace (value set:   │ ...
  ┌────────────────────┐          │  demo, ~25 contracts)        │
  │ Key Vault (nonprod)│◀─getSecret┤ prod workspace (value set:  │
  │ Key Vault (prod)   │◀─getSecret┤  prod, ~1000 contracts)     │
  └────────────────────┘          └──────────────────────────────┘
```

**One source of truth:** one `ictr` monorepo — `core/` builds the wheel, `fabric/`
is Git-synced to the dev workspace, `dab/` builds the serving container; per-env
values = Variable Library. Each workspace's lakehouses hold *its own data*
(separate SharePoint folder per env) — data is never promoted, only item
definitions. The serving tier (DAB + Foundry agent) is a **separate lifecycle**
that binds to a stage by config (§9).

## 4. Decisions (resolved)

1. **Topology:** 3 mirrored workspaces (dev/demo/prod), promoted by a **Fabric
   deployment pipeline**. Notebooks exist in all three; you edit only **dev**.
   Each notebook binds to the lakehouses **in its own workspace** — no
   cross-workspace wiring.
2. **Code:** packaged as a **wheel** attached to a **Fabric Environment**. The
   Environment is a Git/deployment-pipeline item, so the code **travels with
   promotion**. Retire the mounted `Files/src` for demo/prod (kept for dev's fast
   loop — see §5.5).
3. **Config:** per-env differences in a **Fabric Variable Library** (one *value
   set* per stage); static defaults live in the package. Selection is automatic
   by stage — no rebuild to tweak prod.
4. **Secrets & repos:** one **Azure DevOps monorepo** `ictr` with folders
   `fabric/` (Git-synced to the workspace), `core/` (builds the wheel), `dab/`
   (builds the serving container). **2 Key Vaults** — one nonprod (dev+demo), one
   prod — public endpoint + RBAC, read via `notebookutils.credentials.getSecret`.
   No VNet/private endpoint.
5. **Serving tier is decoupled.** **DAB** runs on **Azure Container Apps**
   (read-only, MCP) over each stage's gold SQL endpoint; the **Foundry
   `ictr-agent`** (prompt agent) consumes it plus the **AI Search** index. Both
   deploy on their **own** lifecycle and bind to a stage purely by config — *not*
   via the Fabric deployment pipeline (§9).

## 5. Step 1 — Package the code (your first wheel)

You've never built a package, so this is the detailed part. A wheel is just a zip
of your `src/` plus a metadata file describing the project. Four moving parts:
repo layout → `pyproject.toml` → `python -m build` → upload to a Fabric
Environment.

### 5.1 Repo layout (src-layout)

Restructure the **package repo** (the one you push via OneLake Explorer) to:

```
contract-intelligence/
  pyproject.toml
  README.md
  src/
    contract_intelligence/
      __init__.py            # set __version__
      bronze/  common/  silver/  gold/  serving/  eval/   # your existing code, unchanged
```

Your code already lives under `src/contract_intelligence/...`, so this is mostly
adding `pyproject.toml` at the root and confirming every subpackage has an
`__init__.py`.

### 5.2 `pyproject.toml`

```toml
[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "contract-intelligence"
version = "0.4.0"                 # bump on every release; match the plan you ship
description = "HP contract intelligence medallion pipeline"
requires-python = ">=3.10"
dependencies = [
  # List ONLY deps not already in the Fabric Spark runtime base image.
  # pyspark / pandas / numpy are provided by Fabric — do NOT pin them here.
  "pymupdf",                      # vision page render (Plan 03)
  # "azure-search-documents", "openai"  # add only if not in the base runtime
]

[tool.setuptools.packages.find]
where = ["src"]
```

> Keep `dependencies` minimal: anything the Fabric runtime already ships
> (pyspark, pandas, numpy, delta) must **not** be declared, or you risk version
> conflicts on the cluster. `pymupdf` is the main genuine add.

### 5.3 Build the wheel (locally)

```pwsh
python -m pip install --upgrade build
python -m build            # produces dist/contract_intelligence-0.4.0-py3-none-any.whl
```

### 5.4 Attach to a Fabric Environment

1. In the **dev** workspace, create (or reuse) an **Environment** item.
2. **Custom Libraries → Upload** the `.whl` → **Publish** (publish takes a few
   minutes; it bakes the library into the Spark image).
3. Attach this Environment to the notebooks (or set it as the workspace default).
4. In notebooks, replace the mount/reload block with a plain import:
   ```python
   from contract_intelligence.gold import fields as g
   from contract_intelligence.common import config as cfg_mod
   ```
   Delete `reload_package()` and the `sys.path.append(Files/src)` lines for
   promoted notebooks.

### 5.5 Keep dev fast (recommended hybrid)

Rebuilding + republishing a wheel on every code change is slow. So:

- **dev:** keep the **mounted `Files/src` + `reload_package()`** inner loop for
  iteration. Cut a wheel only when you're ready to promote.
- **demo / prod:** use the **published wheel** exclusively (no mounted source).

A small `try: import contract_intelligence  except ImportError: <mount + sys.path>`
guard lets the *same* notebook work both ways: installed wheel wins, mount is the
dev fallback.

## 6. Step 2 — Config via Variable Library

Create a **Variable Library** item in the dev workspace with one **value set per
stage** (dev / demo / prod). Move only the values that *differ* between
environments; everything structural stays as package defaults / the existing
`config/{env}.json` shape.

Variables to externalize (from today's `dev.json` / `prod.json`):

| Variable | dev | demo | prod |
|---|---|---|---|
| `env` | dev | demo | prod |
| `ai_search.index_name` | ictr_dev_di_vision | ictr_demo_di_vision | ictr_prod_di_vision |
| `gold.vision.mode` | correct | verify | verify |
| `gold.token_budget` | small | medium | large |
| `sharepoint_folder` | dev folder | demo folder (~25) | prod folder (~1000) |
| lakehouse ids/names | dev | demo | prod |

In notebooks, read the active value set via `notebookutils.variableLibrary`
(confirm the exact accessor at execution — the call resolves to the value set of
the **workspace the notebook runs in**, which is what makes promotion automatic).
The deployment pipeline swaps the active value set per stage; no code changes
between environments.

> **Fallback if you defer Variable Libraries:** keep a per-workspace
> `Files/config/{env}.json`, edited in place in each workspace (env-specific,
> never promoted). `load_config` already supports this; it's just less elegant
> than a single promotion-aware item.

## 7. Step 3 — Secrets via Key Vault

Replace the `.env_temp_fabric_ictr` resource (and its `load_secrets` parser) with
Key Vault reads. The `TODO (security)` in `config.py` already names the mechanism.

1. **Two vaults:** `kv-ictr-nonprod` (dev + demo) and `kv-ictr-prod`. Same
   **secret names** in both (`search-api-key`, `aoai-key`, …) so code is
   identical across stages.
2. **RBAC:** grant the runtime identity **Key Vault Secrets User** on the matching
   vault. Critical: scheduled pipeline runs execute as the **pipeline owner /
   workspace identity**, *not* your interactive identity — grant access to *that*
   identity, not just yourself.
3. **New loader** (sketch — replaces `load_secrets`):
   ```python
   def load_secrets_kv(notebookutils, vault_url, names):
       import os
       for n in names:
           os.environ[n.replace("-", "_").upper()] = \
               notebookutils.credentials.getSecret(vault_url, n)
   ```
   `vault_url` comes from the Variable Library (differs nonprod vs prod);
   downstream modules keep reading `os.getenv(...)` unchanged.
4. **Public endpoint + RBAC** is sufficient. No VNet / private endpoint unless
   compliance later demands it.

## 8. Step 4 — Workspaces + deployment pipeline

1. Create **demo** and **prod** workspaces. In each, create the lakehouses
   (bronze/silver/gold) — these are *empty shells*; **data is not promoted**.
2. Create a **Fabric Deployment Pipeline** with stages **dev → demo → prod**;
   assign each workspace to its stage.
3. Bind each stage's workspace to the matching **Git branch** and **Variable
   Library value set**.
4. Set **deployment rules** for anything that can't be a variable (e.g. lakehouse
   default bindings, connection references) so promotion rewires them per stage.
5. **First promotion → demo:** deploy item definitions, point bronze at the demo
   SharePoint folder (~25 contracts), run `ictr_pl`, validate end-to-end.
6. **Then → prod:** same, with the prod SharePoint folder (~1000). Re-check cost
   knobs (`vision.mode`, `token_budget`, embeddings) at 1000-contract scale.

## 9. Serving & consumption tier — DAB on ACA + Foundry agent

The gold T-SQL tables are consumed by a **Microsoft Foundry prompt agent**
(`ictr-agent`) through **two tools** — one structured, one semantic:

| Tool | Backed by | Answers | Per-env binding |
|---|---|---|---|
| **DAB MCP** | Data API Builder on **Azure Container Apps**, read-only over the gold SQL endpoint | structured / aggregate questions over gold fields ("value of field X across contracts", filters, counts) | `DAB_FABRIC_CONN` → that stage's gold endpoint |
| **Azure AI Search** | the `ictr_<env>_di_vision` index (built by nb_03) | semantic / RAG questions over contract chunks ("find the clause that says…") | `index_name` (already an env-varying value) |

So the agent blends **structured** (DAB → gold fields) and **unstructured**
(Search → chunks) retrieval. In each environment both tools point at *that*
stage's resources — the same `index_name` the notebooks use, and the matching DAB
ACA endpoint.

### 9.1 DAB lives on its own track (not the Fabric deployment pipeline)

`dab/` is a monorepo folder (Dockerfile + `dab-config.json` + ACA bicep/azd) with
its **own** lifecycle: build image → push to ACR → deploy ACA. It is **not** a
Fabric item and does **not** ride the Fabric deployment pipeline. `dab-config.json`
(entities + read-only permissions) is baked into the image and identical across
stages; only the injected connection string changes.

```
  Fabric gold SQL endpoint (dev / demo / prod)
            ▲  Entra auth (ACA Managed Identity)
            │  @env(DAB_FABRIC_CONN)
  ┌─────────┴────────────────────────────┐
  │ Azure Container Apps: DAB (read-only) │ ── MCP ──┐
  │  dab-config.json baked in image       │          │
  └───────────────────────────────────────┘          ▼
   Azure AI Search index ───────▶ ┌───────────────────────────────────┐
   (ictr_<env>_di_vision)         │ Microsoft Foundry: ictr-agent     │
                                  │  tools: [ DAB MCP, AI Search ]    │
                                  └───────────────────────────────────┘
```

### 9.2 Auth & security (the parts that bite)

- **DAB → Fabric SQL is Entra-only — no SQL password.** Use the **ACA managed
  identity**: `Authentication=Active Directory Managed Identity` in
  `DAB_FABRIC_CONN`. Then grant that identity access **in Fabric** (workspace /
  item access) **and** `GRANT SELECT` on the gold tables. This two-part grant is
  the usual stall point. Bonus: no secret in the connection string at all.
- **Lock down the MCP ingress.** The ACA HTTPS ingress serves gold contract data
  — require Entra auth so only `ictr-agent` can call it. Never ship an open,
  unauthenticated MCP endpoint.
- **Pin the DAB image** (not `:latest`) for demo/prod reproducibility; confirm the
  pinned version's MCP endpoint is supported.
- **Agent → Search** uses the search endpoint/key from **Key Vault** — the *same*
  secret the notebooks read; reuse it, don't duplicate.

### 9.3 Per-environment binding (3 lifecycles, joined by config)

Each environment has three independently-deployed pieces that bind by config, not
by a shared pipeline:

| Piece | dev | demo | prod | Promoted by |
|---|---|---|---|---|
| Fabric data tier (nb, gold) | dev ws | demo ws | prod ws | Fabric deployment pipeline |
| DAB on ACA | dev ACA | demo ACA | prod ACA | container build + ACA deploy |
| Foundry `ictr-agent` | dev agent | demo agent | prod agent | Foundry (azd / agent.yaml) |

The agent's two tool bindings (`DAB endpoint`, `index_name`) are the only knobs
that change per stage — keep them in the agent's own per-env config, mirroring the
Variable Library on the Fabric side.

## 10. Order of operations

```
0. (prereq) Plan 04 merged & stable in dev
1. Restructure monorepo (core/ fabric/ dab/); build first wheel; validate in dev
2. Swap secrets → Key Vault (getSecret); test in dev
3. Create Variable Library; move env-specific keys; dev value set works
4. Create demo + prod workspaces + lakehouses (empty)
5. Create deployment pipeline dev→demo→prod; bind branches + value sets + rules
6. Promote → demo (25 contracts), validate; then → prod (1000), validate
7. Deploy DAB to ACA per env (MI auth + Fabric grants); wire DAB_FABRIC_CONN to gold
8. Point ictr-agent's tools at each env's DAB MCP + AI Search index; validate Q&A
```

Each step is independently reversible and leaves dev working. Steps 7–8 (serving
tier) proceed once a stage's gold is populated — dev first, then demo/prod.

## 11. Risks & gotchas

- **Lakehouse data is never promoted.** Tables/Files don't travel — each env
  ingests its own SharePoint folder. Plan for a first full bronze→gold run per
  new workspace.
- **Scheduled identity ≠ your identity.** Key Vault RBAC must cover the pipeline's
  run-as identity, or nightly runs fail on `getSecret`.
- **Environment publish latency.** A wheel version bump requires re-upload +
  re-publish (minutes) before notebooks see it — that's why dev keeps the mount
  loop.
- **Variable Library accessor.** Confirm the exact `notebookutils.variableLibrary`
  call during execution; treat §6 as the intent, not verified syntax.
- **Prod cost at 1000 contracts.** `vision.mode=correct` + embeddings + judging
  scale linearly — prod likely runs `verify`, not `correct`. Validate budget in
  demo first.
- **Monorepo, three sync paths.** One `ictr` repo, but `fabric/` is Git-synced to
  the dev workspace, `core/` builds the wheel, and `dab/` builds a container —
  Fabric Git carries *neither* the wheel nor the image.
- **DAB managed-identity grants.** The ACA identity needs *both* Fabric workspace
  access *and* `GRANT SELECT` on gold, or the MCP tool returns empty/forbidden.
- **Secure the MCP ingress.** An open ACA endpoint exposes gold contract data;
  require Entra auth so only `ictr-agent` can query it.
- **Three lifecycles, joined by config.** Fabric (deployment pipeline), DAB (ACA
  deploy), and the Foundry agent (azd) promote separately — keep their per-env
  bindings (`index_name`, `DAB_FABRIC_CONN`, agent tool config) in sync.
- **Config changes must validate in dev before prod promotion.** The shared
  `evidence.py` applies to both environments immediately, but config-driven
  changes (e.g., `fuzzy_threshold` 0.85→0.80, `vision.correct_max_fields` 4→8)
  should hold in `prod.json` until dev validation confirms precision is held (via
  nb05 eval). The trust-promotion code change (correct + confirmed → high) already
  benefits prod via the shared codebase, but deliberate config tuning belongs in
  the dev→demo→prod gate.
- **Audit trail length requires lifecycle management.** The `contract_field_audit`
  table is **append-only, forward-only** with no built-in retention or pruning.
  Each contract reprocess appends a new step log (in gated mode: full causal trail
  for "interesting" fields + terminal row for clean fields). Over time and at
  1000-contract scale, the audit table will grow unbounded. Plan a **retention
  policy** (e.g., keep only the last N versions per contract, or partition by date
  and archive old audit rows to cold storage) before prod launch, or monitor
  storage cost and trigger manual purges. The table schema carries no SCD2 / time
  bounds by design (operational simplicity), so cleanup is external.

## 12. Out of scope / deferred

- **ADO CI to auto-build the wheel** on push (nice later; manual `python -m build`
  is fine for solo now).
- **Azure Artifacts private feed** + `%pip install` (only if you outgrow
  wheel-on-Environment).
- **Private endpoints / managed VNet** for Key Vault.
- **Automated tests gating promotion.** Your `eval/` could feed this later.

## 13. Done criteria

- demo & prod notebooks `import contract_intelligence` from the **published wheel**
  (no mounted `src/`).
- **One** wheel build is promoted across all three stages.
- Secrets read from **Key Vault** (`.env_temp_fabric_ictr` retired).
- Per-env values come from the **Variable Library** value set (no per-stage code
  edits).
- The **deployment pipeline** promotes dev → demo → prod with green end-to-end
  runs at 25 and 1000 contracts.
- **DAB** runs on ACA per env with managed-identity auth, MCP ingress locked to
  the agent, bound to that stage's gold via `DAB_FABRIC_CONN`.
- **`ictr-agent`** answers using both tools — DAB MCP (structured gold) and the
  `ictr_<env>_di_vision` Search index (semantic) — against the matching env.

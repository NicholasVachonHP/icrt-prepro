"""Silver-layer text extraction for the contract intelligence pipeline.

Reads the bronze ``contract_inventory`` delta table by path (live versions only)
and, for every document that is new, whose ``content_hash`` changed, or whose
extraction ``code_hash`` (code fingerprint) changed, runs **Azure Document
Intelligence** (``prebuilt-layout``) to produce layout-aware markdown plus the
structured tables and images from the same analysis (see
``silver.di_extract``). Three Slowly Changing Dimension Type 2 tables are written:

* ``contract_text``   -- one current row per document; ``extracted_text`` is the
                          ordered blocks concatenated (prose + markdown tables +
                          ``[FIGURE …]`` captions). New: ``page_count``,
                          ``has_tables``, ``has_figures``.
* ``contract_blocks`` -- the ordered semantic blocks (prose | table | figure);
                          the unit of chunking downstream.
* ``contract_tables`` -- structured tables (markdown + JSON cells) so pricing /
                          SLA grids can be queried as data.

Cropped images for each embedded figure are downloaded from DI and written to
the lakehouse Files area (``has_figures`` flags documents that contain any); each
figure block's ``figure_uri`` points at the saved file.

All three share the same per-document ``version_id`` (``natural_key |
content_hash | code_hash``) so they version in lock-step. Tracking ``code_hash``
means editing this extraction code (or the DI parameters) re-runs over existing,
unchanged documents. Documents no longer active in bronze are tombstoned here.

Designed to run inside a Microsoft Fabric notebook attached to the *silver*
lakehouse, where ``spark`` and ``notebookutils`` are available in global scope
and the *bronze* lakehouse is mounted read-only.
"""

import json
import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from . import di_extract
from ..common.ai_clients import get_openai_client
from ..common.versioning import code_fingerprint, file_fingerprint
from ..common.scd2 import (
    scd2_merge,
    scd2_expire_and_append,
    version_id,
    resolve_force_paths,
    apply_force,
)

# Silver target tables and views produced by this script.
TARGET_TABLE = "contract_text"
ACTIVE_VIEW = "contract_text_active"
BLOCKS_TABLE = "contract_blocks"
BLOCKS_VIEW = "contract_blocks_active"
TABLES_TABLE = "contract_tables"
TABLES_VIEW = "contract_tables_active"

# SCD Type 2 schema for the silver text table. One current row per document;
# ``is_current`` marks the live version. The text is now Document Intelligence
# markdown (prose + inline tables + figure captions).
EXTRACTION_SCHEMA = StructType(
    [
        StructField("version_id", StringType(), False),     # surrogate version key
        StructField("relative_path", StringType(), False),  # join key to bronze
        StructField("file_name", StringType(), False),
        StructField("content_hash", StringType(), False),   # hash at extraction time
        StructField("code_hash", StringType(), False),      # extraction-code version
        StructField("extracted_text", StringType(), True),
        StructField("page_count", IntegerType(), True),     # NEW: from DI
        StructField("has_tables", BooleanType(), True),     # NEW: drives table logic
        StructField("has_figures", BooleanType(), True),    # NEW: doc contains figure(s)
        StructField("extraction_error", StringType(), True),
        StructField("valid_from", TimestampType(), False),  # when this version began
        StructField("valid_to", TimestampType(), True),     # null while current
        StructField("is_current", BooleanType(), False),    # live version of the path
        StructField("doc_deleted", BooleanType(), False),
    ]
)

# SCD Type 2 schema for the ordered semantic blocks (many rows per document).
BLOCKS_SCHEMA = StructType(
    [
        StructField("block_id", StringType(), False),       # version-unique block key
        StructField("version_id", StringType(), False),     # shared with contract_text
        StructField("relative_path", StringType(), False),
        StructField("file_name", StringType(), False),
        StructField("block_index", IntegerType(), False),   # reading order
        StructField("type", StringType(), False),           # prose | table | figure
        StructField("section", StringType(), True),         # nearest heading (context)
        StructField("page", IntegerType(), True),
        StructField("text", StringType(), False),
        StructField("table_id", StringType(), True),        # links table blocks to contract_tables
        StructField("figure_uri", StringType(), True),      # figure provenance
        StructField("char_count", IntegerType(), False),
        StructField("content_hash", StringType(), False),
        StructField("code_hash", StringType(), False),
        StructField("valid_from", TimestampType(), False),
        StructField("valid_to", TimestampType(), True),
        StructField("is_current", BooleanType(), False),
        StructField("doc_deleted", BooleanType(), False),
    ]
)

# SCD Type 2 schema for the structured tables (many rows per document).
TABLES_SCHEMA = StructType(
    [
        StructField("table_uid", StringType(), False),      # version-unique table key
        StructField("version_id", StringType(), False),     # shared with contract_text
        StructField("relative_path", StringType(), False),
        StructField("file_name", StringType(), False),
        StructField("table_id", StringType(), False),       # local id within the document
        StructField("table_index", IntegerType(), False),
        StructField("page", IntegerType(), True),
        StructField("n_rows", IntegerType(), False),
        StructField("n_cols", IntegerType(), False),
        StructField("markdown", StringType(), True),        # rendered table
        StructField("cells_json", StringType(), True),      # structured cells (JSON array)
        StructField("content_hash", StringType(), False),
        StructField("code_hash", StringType(), False),
        StructField("valid_from", TimestampType(), False),
        StructField("valid_to", TimestampType(), True),
        StructField("is_current", BooleanType(), False),
        StructField("doc_deleted", BooleanType(), False),
    ]
)


def _safe_key(relative_path, suffix, version_id):
    """Build a version-unique, filesystem/search-safe key for blocks and tables."""
    base = re.sub(r"[^A-Za-z0-9_\-=]", "_", relative_path)
    return f"{base}_{suffix}__{version_id}"


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run(spark, notebookutils, config=None, bronze_tables_path=None, bronze_files_dir=None, force_paths=None):  # noqa: ARG001 - notebookutils reserved for future FS ops
    """Run silver Document Intelligence extraction end to end.

    1. Reads the bronze ``contract_inventory`` delta table by path (the bronze
       lakehouse is mounted, not attached) and keeps only active rows.
    2. Compares bronze content_hash with silver's last-extracted hash to find
       files that are new, changed, code-changed, or previously failed.
    3. Runs Document Intelligence on each pending file under ``bronze_files_dir``.
    4. MERGEs text into ``contract_text`` and appends versioned blocks/tables.
    5. Tombstones rows for contracts now inactive in bronze, across all three tables.

    Args:
        spark: Active Spark session.
        notebookutils: Fabric notebook utilities.
        config: Environment config dict with a ``silver`` section.
        bronze_tables_path: ABFS path to the bronze lakehouse ``Tables`` folder.
        bronze_files_dir: Local mount path to the bronze lakehouse ``Files`` folder.
        force_paths: Optional list of ``relative_path`` values (or ``"ALL"``) to
            force-reprocess; falls back to the ``reprocess.force_paths`` config key.
    """
    cfg = config or {}
    silver_cfg = cfg.get("silver", {})
    di_cfg = silver_cfg.get("document_intelligence", {})

    target_table = silver_cfg.get("table", TARGET_TABLE)
    blocks_table = silver_cfg.get("blocks_table", BLOCKS_TABLE)
    tables_table = silver_cfg.get("tables_table", TABLES_TABLE)
    bronze_table = silver_cfg.get("bronze_table", "contract_inventory")

    # Number of contracts whose Document Intelligence analysis runs concurrently.
    # DI (plus optional figure captioning) is one I/O-bound analyze call per
    # contract, so a bounded thread pool turns the serial driver loop into
    # parallel calls. Cap this at or below the DI / vision deployment's
    # requests-per-minute headroom. Not part of the code fingerprint, so tuning
    # it never forces reprocessing.
    max_concurrency = max(1, int(silver_cfg.get("max_concurrency", 8)))

    model_id = di_cfg.get("model_id", "prebuilt-layout")
    max_caption_chars = int(di_cfg.get("max_caption_chars", 2000))
    caption_figures = bool(di_cfg.get("caption_figures", True))

    # Optional multimodal figure captioning + cropped-image persistence. The
    # vision model name and image-saving toggle feed the code fingerprint below
    # so changing either re-extracts existing documents under the new logic.
    azure_openai_cfg = cfg.get("azure_openai", {})
    vision_model = di_cfg.get(
        "vision_model", azure_openai_cfg.get("completion_model", "gpt-4.1")
    )
    save_figure_images = bool(di_cfg.get("save_figure_images", True))
    figures_subdir = di_cfg.get("figures_subdir", "contract_figures")
    figures_root = di_cfg.get("figures_root", "/lakehouse/default/Files")

    # Drop decorative figures (logos, header/footer glyphs) whose bounding box is
    # below this fraction of the page, before captioning/persistence. 0 disables.
    min_figure_page_fraction = float(di_cfg.get("min_figure_page_fraction", 0.0))

    if not bronze_tables_path or not bronze_files_dir:
        raise ValueError(
            "bronze_tables_path and bronze_files_dir are required; resolve them "
            "in the notebook after mounting the bronze lakehouse."
        )

    bronze_delta_path = f"{bronze_tables_path}/{bronze_table}"

    # Reprocessing fingerprint: bump automatically whenever this module, the DI
    # extraction helper, or the DI parameters change, so existing documents are
    # re-extracted under the new logic.
    current_code = code_fingerprint(
        [__file__, di_extract.__file__],
        {
            "model_id": model_id,
            "max_caption_chars": max_caption_chars,
            "caption_figures": caption_figures,
            "vision_model": vision_model,
            "save_figure_images": save_figure_images,
            "min_figure_page_fraction": min_figure_page_fraction,
        },
    )
    print(f"[silver] bronze={bronze_delta_path}, table={target_table}, code={current_code} "
          f"(extract={file_fingerprint(__file__)[:8]}, "
          f"di_extract={file_fingerprint(di_extract.__file__)[:8]})")

    # Read the bronze inventory by path; keep only the live (current,
    # non-deleted) version of each document.
    bronze_all = spark.read.format("delta").load(bronze_delta_path)
    bronze_active = bronze_all.where(
        (F.col("is_current") == True) & (F.col("doc_deleted") == False)  # noqa: E712
    )

    # Determine which active files are new or changed since the last extraction.
    if spark.catalog.tableExists(target_table):
        # Allow new columns to be added to a pre-existing silver table.
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # Compare bronze's current version against silver's *current* version
        # only (SCD2 history rows must not fan out the join).
        silver_df = spark.table(target_table).where(F.col("is_current") == True)  # noqa: E712
        # (Re)extract when: never treated (no silver row) OR content_hash changed
        # OR the extraction code changed (code_hash) OR the previous attempt
        # failed (so transient errors auto-retry). content_hash is computed in
        # bronze and copied here; not recalculated.
        needs_extraction = (
            bronze_active.alias("b")
            .join(silver_df.alias("s"), "relative_path", "left")
            .where(
                F.col("s.relative_path").isNull()  # never treated
                | (F.col("b.content_hash") != F.col("s.content_hash"))  # changed
                | (F.col("s.code_hash") != F.lit(current_code))  # code changed
                | F.col("s.extraction_error").isNotNull()  # previous attempt failed
            )
            .select("b.*")
        )
    else:
        # First run: extract every active contract.
        needs_extraction = bronze_active

    # Force-reprocess selected (or all) active contracts even when their
    # content_hash and code_hash are unchanged (e.g. recovering from a bug or a
    # transient failure). scd2_merge overwrites the matching live row in place.
    force_paths = resolve_force_paths(force_paths, cfg)
    needs_extraction = apply_force(bronze_active, needs_extraction, force_paths)

    pending = needs_extraction.collect()

    if not pending:
        print("No contracts require (re)extraction.")
    else:
        di_client = di_extract.get_di_client()
        # Build the multimodal vision client lazily and best-effort: when figure
        # captioning is enabled but the openai package / endpoint are not yet
        # available in this environment, captioning silently falls back to the
        # deterministic OCR-text path (images are still saved).
        vision_client = None
        if caption_figures:
            try:
                vision_client = get_openai_client(notebookutils)
            except Exception as e:  # noqa: BLE001 - vision optional; OCR fallback
                print(f"[silver] figure vision captioning disabled (client init failed): {e}")
        extracted_at = datetime.now(timezone.utc)
        text_rows = []
        block_rows = []
        table_rows = []
        errors = {}

        def _extract_row(row):
            """Run Document Intelligence for one contract (thread worker).

            Captures its own exceptions and returns ``(row, vid, doc, error)`` so
            no call escapes the pool. Only this slow analyze call runs in
            parallel; row assembly, figure persistence and the Delta writes below
            stay on the driver thread.
            """
            rel = row["relative_path"]
            file_path = f"{bronze_files_dir}/{rel}"
            vid = version_id(rel, row["content_hash"], current_code)
            figure_prefix = f"{figures_subdir}/{_safe_key(rel, 'fig', vid)}"
            try:
                doc = di_extract.extract_document(
                    file_path,
                    di_client,
                    model_id=model_id,
                    max_caption_chars=max_caption_chars,
                    caption_figures=caption_figures,
                    figure_uri_prefix=figure_prefix,
                    min_figure_page_fraction=min_figure_page_fraction,
                    vision_client=vision_client,
                    vision_model=vision_model,
                )
                return row, vid, doc, None
            except Exception as e:  # noqa: BLE001 - record and keep going
                return row, vid, None, str(e)

        # Fan the per-contract Document Intelligence calls out across a bounded
        # thread pool instead of running them one-at-a-time on the driver. The
        # DI / vision clients are safe to share across threads and
        # ``executor.map`` preserves order, so the SCD2 rows are assembled
        # deterministically below.
        workers = min(max_concurrency, len(pending))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            extractions = list(pool.map(_extract_row, pending))

        for row, vid, doc, error in extractions:
            rel = row["relative_path"]
            if error:
                errors[rel] = error

            text_rows.append(
                Row(
                    version_id=vid,
                    relative_path=rel,
                    file_name=row["file_name"],
                    content_hash=row["content_hash"],  # copied from bronze, not recalculated
                    code_hash=current_code,
                    extracted_text=(doc["text"] if doc else None),
                    page_count=(doc["page_count"] if doc else None),
                    has_tables=(doc["has_tables"] if doc else None),
                    has_figures=(doc["has_figures"] if doc else None),
                    extraction_error=error,
                    valid_from=extracted_at,
                    valid_to=None,
                    is_current=True,
                    doc_deleted=False,
                )
            )

            if not doc:
                continue

            # Persist cropped images to the lakehouse Files area; the figure
            # blocks' ``figure_uri`` resolves against ``figures_root``.
            if save_figure_images and doc.get("figures"):
                _save_figure_images(doc["figures"], figures_root)

            for b in doc["blocks"]:
                block_rows.append(
                    Row(
                        block_id=_safe_key(rel, b["block_index"], vid),
                        version_id=vid,
                        relative_path=rel,
                        file_name=row["file_name"],
                        block_index=int(b["block_index"]),
                        type=b["type"],
                        section=b["section"],
                        page=(int(b["page"]) if b["page"] is not None else None),
                        text=b["text"],
                        table_id=b["table_id"],
                        figure_uri=b["figure_uri"],
                        char_count=int(b["char_count"]),
                        content_hash=row["content_hash"],
                        code_hash=current_code,
                        valid_from=extracted_at,
                        valid_to=None,
                        is_current=True,
                        doc_deleted=False,
                    )
                )

            for t in doc["tables"]:
                table_rows.append(
                    Row(
                        table_uid=_safe_key(rel, t["table_id"], vid),
                        version_id=vid,
                        relative_path=rel,
                        file_name=row["file_name"],
                        table_id=t["table_id"],
                        table_index=int(t["table_index"]),
                        page=(int(t["page"]) if t["page"] is not None else None),
                        n_rows=int(t["n_rows"]),
                        n_cols=int(t["n_cols"]),
                        markdown=t["markdown"],
                        cells_json=json.dumps(t["cells"], ensure_ascii=False),
                        content_hash=row["content_hash"],
                        code_hash=current_code,
                        valid_from=extracted_at,
                        valid_to=None,
                        is_current=True,
                        doc_deleted=False,
                    )
                )

        changed_paths = [r["relative_path"] for r in text_rows]

        # contract_text: one current row per key (SCD2 merge).
        text_df = spark.createDataFrame(text_rows, schema=EXTRACTION_SCHEMA)
        scd2_merge(spark, target_table, text_df, key="relative_path", now=extracted_at)
        print(f"Wrote {text_df.count()} new contract version(s) to '{target_table}'.")

        # contract_blocks: many current rows per key (expire + append). Empty
        # only if every pending document failed extraction.
        if block_rows:
            blocks_df = spark.createDataFrame(block_rows, schema=BLOCKS_SCHEMA)
            scd2_expire_and_append(
                spark, blocks_table, blocks_df, changed_paths, key="relative_path", now=extracted_at
            )
            print(f"Wrote {blocks_df.count()} block(s) to '{blocks_table}'.")

        # contract_tables: many current rows per key (expire + append).
        if table_rows:
            tables_df = spark.createDataFrame(table_rows, schema=TABLES_SCHEMA)
            scd2_expire_and_append(
                spark, tables_table, tables_df, changed_paths, key="relative_path", now=extracted_at
            )
            print(f"Wrote {tables_df.count()} table(s) to '{tables_table}'.")

        success_count = len(text_rows) - len(errors)
        print(f"Extracted {success_count} of {len(text_rows)} file(s) via Document Intelligence.")
        if errors:
            print(f"Failed to extract {len(errors)} file(s); first error:")
            err_path, err_msg = next(iter(errors.items()))
            print(f"  {err_path}: {err_msg}")

    # Tombstone the current version of contracts no longer active in bronze,
    # across all three tables. Driven by the *active* bronze set (not bronze
    # tombstone rows) so silver self-heals when contracts are deleted or
    # re-keyed. History rows (is_current = false) are left untouched.
    if spark.catalog.tableExists(target_table):
        active_keys = [
            r["relative_path"]
            for r in bronze_active.select("relative_path").distinct().collect()
        ]
        now_ts = locals().get("extracted_at") or datetime.now(timezone.utc)
        _tombstone_removed(spark, target_table, active_keys, now_ts)
        if spark.catalog.tableExists(blocks_table):
            _tombstone_removed(spark, blocks_table, active_keys, now_ts)
        if spark.catalog.tableExists(tables_table):
            _tombstone_removed(spark, tables_table, active_keys, now_ts)
        print(f"Synced silver tombstones to {len(active_keys)} active bronze contract(s).")

    # Create or update the active views (live version of each contract only).
    if spark.catalog.tableExists(target_table):
        active_view = silver_cfg.get("active_view", ACTIVE_VIEW)
        blocks_view = silver_cfg.get("blocks_active_view", BLOCKS_VIEW)
        tables_view = silver_cfg.get("tables_active_view", TABLES_VIEW)
        _create_active_view(spark, target_table, active_view)
        if spark.catalog.tableExists(blocks_table):
            _create_active_view(spark, blocks_table, blocks_view)
        if spark.catalog.tableExists(tables_table):
            _create_active_view(spark, tables_table, tables_view)
        print(f"Silver extraction complete: {spark.table(active_view).count()} active contract(s).")


def _save_figure_images(figures, figures_root):
    """Persist cropped figure images under ``figures_root`` (lakehouse Files).

    ``figure_uri`` is a lakehouse-relative path set by ``di_extract`` (e.g.
    ``contract_figures/<key>/p1_f0.png``); the bytes are written to
    ``{figures_root}/{figure_uri}`` so the silver ``figure_uri`` column resolves
    against the lakehouse Files root. Best-effort per figure.
    """
    for fig in figures:
        uri = fig.get("figure_uri")
        data = fig.get("figure_bytes")
        if not uri or not data:
            continue
        dest = os.path.join(figures_root, *uri.split("/"))
        try:
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(dest, "wb") as f:
                f.write(data)
        except Exception as e:  # noqa: BLE001 - persistence is best-effort
            print(f"[silver] failed to save figure image '{uri}': {e}")


def _tombstone_removed(spark, table, active_keys, now_ts):
    """Tombstone live rows whose ``relative_path`` is no longer active in bronze."""
    tgt = DeltaTable.forName(spark, table)
    tgt.update(
        condition=(~F.col("relative_path").isin(active_keys))
        & (F.col("is_current") == True)  # noqa: E712
        & (F.col("doc_deleted") == False),  # noqa: E712
        set={"doc_deleted": F.lit(True), "is_current": F.lit(False), "valid_to": F.lit(now_ts)},
    )


def _create_active_view(spark, table, view):
    """(Re)create a view exposing only the live rows of an SCD2 table."""
    spark.sql(
        f"CREATE OR REPLACE VIEW {view} AS "
        f"SELECT * FROM {table} "
        f"WHERE is_current = true AND doc_deleted = false"
    )

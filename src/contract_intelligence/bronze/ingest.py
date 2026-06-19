"""Bronze-layer ingestion for the contract intelligence pipeline.

Scans the default lakehouse's ``Files`` folder for contract documents
(``.pdf``, ``.docx``, ``.doc``), computes a SHA-256 content hash for each
file, and maintains a ``contract_inventory`` delta table as a **Slowly Changing
Dimension Type 2**: every distinct content version of a document is preserved as
its own row. When a file's **content** changes (new ``content_hash``) *or* this
ingestion code changes (new ``code_hash``), the prior version is closed out
(``is_current = false``, ``valid_to`` set) and a new current row is inserted, so
the full version history of each document remains queryable. Files that
disappear from the folder have their current version tombstoned
(``doc_deleted = true``) while history is retained.

A ``contract_inventory_active`` view exposes only the live version of each
document (``is_current = true AND doc_deleted = false``).

Designed to run inside a Microsoft Fabric notebook where ``spark`` and
``notebookutils`` are available in the global scope.
"""

import hashlib
import os
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ..common.versioning import code_fingerprint
from ..common.scd2 import version_id

# File extensions handled by this bronze ingestion.
SUPPORTED_EXTENSIONS = (".pdf", ".docx", ".doc")

# Explicit schema for the SCD Type 2 bronze inventory table. One row per
# (document, content version); ``is_current`` marks the live version of a path.
INVENTORY_SCHEMA = StructType(
    [
        StructField("version_id", StringType(), False),     # surrogate version key
        StructField("file_name", StringType(), False),
        StructField("relative_path", StringType(), False),  # natural (document) key
        StructField("size_bytes", LongType(), False),
        StructField("content_hash", StringType(), False),   # version discriminator
        StructField("code_hash", StringType(), False),      # ingest-code version
        StructField("first_seen_at", TimestampType(), False),
        StructField("last_seen_at", TimestampType(), False),
        StructField("valid_from", TimestampType(), False),  # when this version began
        StructField("valid_to", TimestampType(), True),     # null while current
        StructField("is_current", BooleanType(), False),    # live version of the path
        StructField("doc_deleted", BooleanType(), False),   # path no longer present
    ]
)


def file_sha256(local_path, chunk_size=8 * 1024 * 1024):
    """Compute the SHA-256 hash of a file's contents (used as a change detector)."""
    h = hashlib.sha256()
    with open(local_path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def list_files(notebookutils, path):
    """Recursively list every file under ``path`` using the lakehouse FS API."""
    results = []
    for entry in notebookutils.fs.ls(path):
        if entry.isDir:
            results.extend(list_files(notebookutils, entry.path))
        else:
            results.append(entry)
    return results


def is_supported(name):
    """Return True when the file name has a supported contract extension."""
    return os.path.splitext(name)[1].lower() in SUPPORTED_EXTENSIONS


def ingest(spark, notebookutils, config=None):
    """Run the bronze ingestion end to end.

    Builds the inventory of supported contract files, extracts their text,
    MERGEs the results into the ``contract_inventory`` delta table, tombstones
    files that no longer exist, and (re)creates the active view.

    Args:
        spark: Active Spark session
        notebookutils: Fabric notebook utilities
        config: Environment config dict with "bronze" section containing:
                - files_dir: Path to scan (default: /lakehouse/default/Files)
                - table: Target table name (default: contract_inventory)
                - active_view: Active view name (default: contract_inventory_active)
    """
    cfg = config or {}
    bronze_cfg = cfg.get("bronze", {})
    
    # Config with inline defaults (config file is still the source of truth)
    local_files_dir = bronze_cfg.get("files_dir", "/lakehouse/default/Files")
    target_table = bronze_cfg.get("table", "contract_inventory")
    active_view = bronze_cfg.get("active_view", "contract_inventory_active")

    # Optional: restrict the scan to a subfolder of Files (e.g. a OneLake
    # SharePoint shortcut). relative_path is still keyed off ``files_dir`` (the
    # Files root) so downstream layers resolve documents the same way.
    scan_subdir = bronze_cfg.get("scan_subdir")
    scan_dir = (
        f"{local_files_dir}/{scan_subdir.strip('/')}" if scan_subdir else local_files_dir
    )

    print(f"[bronze] files_dir={local_files_dir}, scan_dir={scan_dir}, table={target_table}")

    # Reprocessing fingerprint: changes whenever this module's code changes, so
    # editing the ingestion logic re-versions every file on the next run even
    # when the file bytes are unchanged.
    current_code = code_fingerprint([__file__])

    file_entries = [
        entry
        for entry in list_files(notebookutils, f"file:{scan_dir}")
        if is_supported(entry.name)
    ]

    seen_at = datetime.now(timezone.utc)
    rows = []
    for entry in file_entries:
        # entry.path looks like file:/lakehouse/default/Files/...
        local_path = entry.path.replace("file:", "", 1)
        relative_path = local_path.replace(f"{local_files_dir}/", "", 1)
        content_hash = file_sha256(local_path)  # change detector / version key

        rows.append(
            Row(
                version_id=version_id(relative_path, content_hash, current_code),
                file_name=entry.name,
                relative_path=relative_path,          # natural (document) key
                size_bytes=int(entry.size),
                content_hash=content_hash,
                code_hash=current_code,
                first_seen_at=seen_at,
                last_seen_at=seen_at,
                valid_from=seen_at,
                valid_to=None,
                is_current=True,
                doc_deleted=False,
            )
        )

    if not rows:
        print("No supported contract files (.pdf/.docx/.doc) found.")
        return

    source_df = spark.createDataFrame(rows, schema=INVENTORY_SCHEMA)

    # Column-for-column mapping used when inserting a brand-new path or a new
    # version of an existing path (always lands as the current row).
    insert_values = {
        "version_id": "s.version_id",
        "file_name": "s.file_name",
        "relative_path": "s.relative_path",
        "size_bytes": "s.size_bytes",
        "content_hash": "s.content_hash",
        "code_hash": "s.code_hash",
        "first_seen_at": "s.first_seen_at",
        "last_seen_at": "s.last_seen_at",
        "valid_from": "s.valid_from",
        "valid_to": "s.valid_to",
        "is_current": F.lit(True),
        "doc_deleted": F.lit(False),
    }

    if not spark.catalog.tableExists(target_table):
        (
            source_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )
        print(f"Created '{target_table}' with {source_df.count()} current version(s).")
    else:
        tgt = DeltaTable.forName(spark, target_table)
        current = spark.table(target_table).where(
            "is_current = true AND doc_deleted = false"
        )

        # Files whose live version differs (content OR ingest code) need a new
        # version. ``version_id`` encodes both content_hash and code_hash.
        changed = (
            source_df.alias("s")
            .join(current.alias("c"), "relative_path")
            .where("s.version_id <> c.version_id")
            .select("s.*")
        )

        # SCD2 staging trick: a NULL mergeKey row forces an INSERT of the new
        # version, while the natural-key row expires the old current version.
        staged = changed.withColumn(
            "mergeKey", F.lit(None).cast("string")
        ).unionByName(source_df.withColumn("mergeKey", F.col("relative_path")))

        (
            tgt.alias("t")
            .merge(
                staged.alias("s"),
                "t.relative_path = s.mergeKey AND t.is_current = true "
                "AND t.doc_deleted = false",
            )
            # Version changed (content or ingest code): close out the prior one.
            .whenMatchedUpdate(
                condition="t.version_id <> s.version_id",
                set={
                    "is_current": F.lit(False),
                    "valid_to": "s.valid_from",
                },
            )
            # Same version: refresh the heartbeat and revive if it had been
            # tombstoned and reappeared unchanged.
            .whenMatchedUpdate(
                condition="t.version_id = s.version_id",
                set={
                    "last_seen_at": "s.last_seen_at",
                    "doc_deleted": F.lit(False),
                },
            )
            # Brand-new path, or the new version of a changed path.
            .whenNotMatchedInsert(values=insert_values)
            .execute()
        )

        # Tombstone the current version of files that disappeared (keeps history).
        current_keys = [r["relative_path"] for r in rows]
        tgt.update(
            condition=(~F.col("relative_path").isin(current_keys))
            & (F.col("is_current") == True)  # noqa: E712 - Spark column comparison
            & (F.col("doc_deleted") == False),  # noqa: E712
            set={
                "doc_deleted": F.lit(True),
                "is_current": F.lit(False),
                "valid_to": F.lit(seen_at),
            },
        )
        print(f"Merged {source_df.count()} current version(s) into '{target_table}'.")

    # Expose only the live version of each document through a stable view.
    spark.sql(
        f"CREATE OR REPLACE VIEW {active_view} AS "
        f"SELECT * FROM {target_table} WHERE is_current = true AND doc_deleted = false"
    )
    print(f"View '{active_view}' is up to date.")
    print(f"Bronze ingestion complete: {len(rows)} file(s) inventoried.")

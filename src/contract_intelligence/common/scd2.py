"""Slowly Changing Dimension (Type 2) write helpers for the contract pipeline.

Every processing layer keeps full version history: each distinct ``version_id``
(a surrogate over ``natural_key | content_hash | code_hash``) is preserved as
its own row. The *live* row of a key is ``is_current = true AND doc_deleted =
false``; prior versions are retained with ``is_current = false`` and a
``valid_to`` timestamp. This supports side-by-side comparison of how a document,
its extraction, its chunks, or its extracted fields changed over time -- whether
the change came from new input content or from new processing code/prompts.

Two write shapes are provided:

* :func:`scd2_merge` -- for tables with **one current row per key** (silver
  ``contract_text``, gold ``contract_fields``).
* :func:`scd2_expire_and_append` -- for tables with **many current rows per key**
  (silver ``contract_chunks``: one row per chunk index).

Tables are assumed to already carry the SCD2 column set; there is no in-place
Type-1 migration (tables are created fresh under this schema).
"""

import re
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F

# SCD2 bookkeeping columns every versioned table carries.
SCD2_COLUMNS = ("version_id", "valid_from", "valid_to", "is_current")


def version_id(natural_key, content_hash, code_hash):
    """Deterministic surrogate key for a (key, content, code) version."""
    import hashlib

    return hashlib.sha256(
        f"{natural_key}|{content_hash}|{code_hash}".encode()
    ).hexdigest()[:16]


def scd2_merge(spark, table, source_df, *, key="relative_path", now=None):
    """Upsert ``source_df`` as new current versions (one current row per key).

    ``source_df`` must already carry the full SCD2 column set (``version_id``,
    ``valid_from`` = now, ``valid_to`` = null, ``is_current`` = true,
    ``doc_deleted`` = false).

    When a key's incoming ``version_id`` differs from its live row, the live row
    is expired (``is_current = false``, ``valid_to`` set) and the new version is
    inserted. When the incoming ``version_id`` equals the live row (a forced
    rerun or an error-retry of identical content+code), the live row is
    overwritten in place -- payload refreshed, ``valid_from`` reset, tombstone
    cleared -- rather than duplicated, so transient failures actually persist a
    corrected result and history is not polluted with no-op versions.
    """
    now = now or datetime.now(timezone.utc)

    if not spark.catalog.tableExists(table):
        source_df.write.format("delta").option("mergeSchema", "true").saveAsTable(table)
        return

    tgt = DeltaTable.forName(spark, table)
    current = spark.table(table).where("is_current = true AND doc_deleted = false")

    # Rows whose version differs from the current row for the same key.
    changed = (
        source_df.alias("s")
        .join(current.alias("c"), key)
        .where("s.version_id <> c.version_id")
        .select("s.*")
    )

    # NULL-mergeKey staging: the null row forces INSERT of the new version while
    # the natural-key row expires/refreshes the existing current row.
    staged = changed.withColumn("mergeKey", F.lit(None).cast("string")).unionByName(
        source_df.withColumn("mergeKey", F.col(key))
    )

    insert_values = {c: f"s.{c}" for c in source_df.columns}
    insert_values["is_current"] = F.lit(True)
    insert_values["valid_to"] = F.lit(None).cast("timestamp")
    insert_values["doc_deleted"] = F.lit(False)

    # Same-version overwrite: a forced rerun or error-retry of identical
    # content+code refreshes the live row in place (all payload columns plus
    # ``valid_from`` from source, ``is_current`` re-asserted, tombstone cleared)
    # instead of inserting a duplicate version_id.
    refresh_values = dict(insert_values)

    (
        tgt.alias("t")
        .merge(
            staged.alias("s"),
            f"t.{key} = s.mergeKey AND t.is_current = true AND t.doc_deleted = false",
        )
        # Version changed: close out the prior current version.
        .whenMatchedUpdate(
            condition="t.version_id <> s.version_id",
            set={"is_current": F.lit(False), "valid_to": "s.valid_from"},
        )
        # Same version reprocessed: overwrite the live row in place.
        .whenMatchedUpdate(
            condition="t.version_id = s.version_id", set=refresh_values
        )
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )


def scd2_expire_and_append(spark, table, source_df, changed_keys, *, key="relative_path", now=None):
    """Versioned write for tables with many current rows per key (chunks).

    Expires (``is_current = false``, ``valid_to`` set) every current row of each
    key in ``changed_keys``, then appends ``source_df`` (already stamped with the
    SCD2 columns) as the new current version. History is retained.
    """
    now = now or datetime.now(timezone.utc)

    if not spark.catalog.tableExists(table):
        source_df.write.format("delta").option("mergeSchema", "true").saveAsTable(table)
        return

    if changed_keys:
        tgt = DeltaTable.forName(spark, table)
        incoming_versions = [
            r["version_id"] for r in source_df.select("version_id").distinct().collect()
        ]
        # A forced/error-retry rerun yields an identical ``version_id`` (hence an
        # identical ``chunk_id``) for a key. Delete those current rows outright so
        # the append below cannot create duplicate ``chunk_id`` keys; genuinely
        # new versions (different version_id -> different chunk_id) are instead
        # expired to history.
        if incoming_versions:
            tgt.delete(
                F.col(key).isin(changed_keys)
                & (F.col("is_current") == True)  # noqa: E712
                & (F.col("doc_deleted") == False)  # noqa: E712
                & (F.col("version_id").isin(incoming_versions))
            )
        tgt.update(
            condition=F.col(key).isin(changed_keys)
            & (F.col("is_current") == True)  # noqa: E712
            & (F.col("doc_deleted") == False),  # noqa: E712
            set={"is_current": F.lit(False), "valid_to": F.lit(now)},
        )
    source_df.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(table)


# ---------------------------------------------------------------------------
# Forced reprocessing helpers
# ---------------------------------------------------------------------------

def resolve_force_paths(force_paths, config):
    """Normalize a force-reprocess selection from an explicit arg or config.

    Precedence: an explicit ``force_paths`` argument wins; otherwise the
    ``reprocess.force_paths`` config key is used. Returns ``None`` (normal
    incremental run), the sentinel string ``"ALL"``, or a list of
    ``relative_path`` values to force-reprocess even when content_hash and
    code_hash are unchanged.

    Inputs may be a Python value (``None``, a list, or ``"ALL"``) *or* a single
    string as delivered by a Fabric pipeline parameter. Pipeline strings are
    tolerated: ``""`` / ``"none"`` / ``"null"`` mean a normal incremental run,
    ``"ALL"`` (any case) forces everything, and a comma- or newline-separated
    string is split into a list of paths. This lets one pipeline parameter drive
    forced reruns across every stage without per-notebook parsing.
    """
    if force_paths is None:
        force_paths = (config or {}).get("reprocess", {}).get("force_paths")
    if not force_paths:
        return None
    if isinstance(force_paths, str):
        raw = force_paths.strip()
        if raw == "" or raw.lower() in ("none", "null"):
            return None
        if raw.upper() == "ALL":
            return "ALL"
        parts = [p.strip() for p in re.split(r"[,\n]", raw) if p.strip()]
        return parts or None
    paths = [str(p) for p in force_paths]
    if any(p.strip().upper() == "ALL" for p in paths):
        return "ALL"
    return paths


def apply_force(active_df, pending_df, force_paths, *, key="relative_path"):
    """Union the force-selected active rows into the detected pending set.

    ``active_df`` is the full set of live candidates for the stage; ``pending_df``
    is the subset that normal change-detection already selected. With
    ``force_paths == "ALL"`` every active row is processed; with a list, the named
    paths are added to ``pending_df`` (de-duplicated on ``key``).
    """
    if not force_paths:
        return pending_df
    if force_paths == "ALL":
        return active_df
    forced = active_df.where(F.col(key).isin(list(force_paths)))
    return pending_df.unionByName(forced, allowMissingColumns=True).dropDuplicates([key])

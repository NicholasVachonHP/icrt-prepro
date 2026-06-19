"""Silver-layer block-aware chunking for the contract intelligence pipeline.

Reads the ordered semantic blocks from the silver ``contract_blocks`` table
(produced by ``silver.extract`` via Document Intelligence) and packs them into
retrieval chunks written to the ``contract_chunks`` delta table as a **Slowly
Changing Dimension Type 2**: every ``(relative_path, silver_version_id, code_hash)``
version of a contract's chunks is retained, with ``is_current`` marking the live
set. Chunking is incremental: a contract is only (re)chunked when it is new, the
silver blocks changed (new raw content *or* new silver extraction logic, both of
which bump the silver ``version_id``), or the chunking ``code_hash`` (code +
chunk-size/overlap fingerprint) changed.

Block-aware packing keeps structure intact so retrieval never sees a half-table
or a split figure caption:

* ``prose`` blocks are packed up to the token budget and only *prose* is ever
  split (token sliding window).
* ``table`` blocks are atomic; a table larger than the budget is split **by
  rows, repeating the header row** so each piece is self-describing.
* ``figure`` blocks are atomic and never split (the caption is bounded at
  extraction instead).

Each chunk carries its ``section`` heading as a context prefix plus
``block_type``/``page``/``table_id``/``figure_uri`` metadata for typed retrieval
and citation. The version-unique ``chunk_id`` doubles as the Azure AI Search
document key. Live chunks are tombstoned whenever they no longer match a current
contract version produced by the current chunking code.

Designed to run inside a Microsoft Fabric notebook attached to the *silver*
lakehouse, where ``spark`` and ``notebookutils`` are in global scope.
"""

import re
from collections import defaultdict
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

from ..common.versioning import code_fingerprint
from ..common.scd2 import scd2_expire_and_append, resolve_force_paths, apply_force

# SCD Type 2 schema: each (relative_path, silver_version_id, code_hash) version
# of a contract's chunks is retained; ``is_current`` marks the live set.
CHUNKS_SCHEMA = StructType(
    [
        StructField("chunk_id", StringType(), False),       # AI Search document key (version-unique)
        StructField("version_id", StringType(), False),     # surrogate version key
        StructField("relative_path", StringType(), False),  # join key to silver/bronze
        StructField("file_name", StringType(), False),
        StructField("chunk_index", IntegerType(), False),
        StructField("text", StringType(), False),
        StructField("char_count", IntegerType(), False),
        StructField("block_type", StringType(), True),      # prose | table | figure | mixed
        StructField("section", StringType(), True),         # nearest heading (context)
        StructField("page", IntegerType(), True),
        StructField("table_id", StringType(), True),        # set for single-table chunks
        StructField("figure_uri", StringType(), True),      # set for single-figure chunks
        StructField("content_hash", StringType(), False),   # copied from silver text row
        StructField("code_hash", StringType(), False),      # chunking-code + params version
        StructField("valid_from", TimestampType(), False),  # when this version began
        StructField("valid_to", TimestampType(), True),     # null while current
        StructField("is_current", BooleanType(), False),    # live version of the chunks
        StructField("doc_deleted", BooleanType(), False),
    ]
)


def _safe_key(relative_path, idx, version_id):
    """Build a version-unique, Azure AI Search-safe document key.

    Search keys may only contain letters, digits, dash, underscore and equals.
    The ``version_id`` (which already encodes path + content_hash + code_hash)
    makes the key unique across SCD2 versions so historical chunks of the same
    path/index do not collide.
    """
    base = re.sub(r"[^A-Za-z0-9_\-=]", "_", relative_path)
    return f"{base}_{idx}__{version_id}"


def _split_with_tokens(text, size, overlap):
    """Token-based sliding window using tiktoken (cl100k_base)."""
    import tiktoken

    enc = tiktoken.get_encoding("cl100k_base")
    tokens = enc.encode(text)
    step = max(1, size - overlap)
    chunks = []
    for start in range(0, len(tokens), step):
        piece = tokens[start : start + size]
        if not piece:
            break
        chunks.append(enc.decode(piece))
        if start + size >= len(tokens):
            break
    return chunks


def split_text(text, size, overlap):
    """Split text into overlapping chunks.

    Prefers token-based chunking (``size``/``overlap`` are token counts). Falls
    back to a character-based window (~4 chars/token) when tiktoken is absent.
    """
    text = " ".join((text or "").split())  # normalise whitespace
    if not text:
        return []
    try:
        return _split_with_tokens(text, size, overlap)
    except Exception:  # noqa: BLE001 - tiktoken unavailable -> char fallback
        csize, coverlap = size * 4, overlap * 4
        step = max(1, csize - coverlap)
        chunks = []
        for start in range(0, len(text), step):
            piece = text[start : start + csize].strip()
            if piece:
                chunks.append(piece)
            if start + csize >= len(text):
                break
        return chunks


def _encoder():
    """Return a tiktoken encoder, or None when tiktoken is unavailable."""
    try:
        import tiktoken

        return tiktoken.get_encoding("cl100k_base")
    except Exception:  # noqa: BLE001 - fall back to char-based estimate
        return None


def _tok_len(text, enc):
    """Token length of ``text`` (≈4 chars/token when tiktoken is absent)."""
    if not text:
        return 0
    if enc is not None:
        try:
            return len(enc.encode(text))
        except Exception:  # noqa: BLE001
            pass
    return max(1, len(text) // 4)


def _split_table_by_rows(md, size, enc):
    """Split an oversized markdown table by rows, repeating the header on each piece."""
    lines = [ln for ln in md.split("\n") if ln != ""]
    if len(lines) < 3:  # header, separator, >=1 body row
        return [md]
    header, sep, body = lines[0], lines[1], lines[2:]
    pieces, cur = [], []
    for rowln in body:
        trial = "\n".join([header, sep] + cur + [rowln])
        if cur and _tok_len(trial, enc) > size:
            pieces.append("\n".join([header, sep] + cur))
            cur = [rowln]
        else:
            cur.append(rowln)
    if cur:
        pieces.append("\n".join([header, sep] + cur))
    return pieces or [md]


def _pack_blocks(blocks, size, overlap, enc):
    """Pack ordered blocks into chunks.

    Prose is packed up to the token budget and only prose is ever split; tables
    and figures are atomic (a table over budget is split by rows with the header
    repeated; a figure is never split). Each emitted chunk carries its section
    heading as a context prefix plus typed metadata.
    """
    # 1) Expand blocks into atomic placement units.
    units = []
    for b in blocks:
        text = (b["text"] or "").strip()
        if not text:
            continue
        btype, section, page = b["type"], b["section"], b["page"]
        if btype == "prose":
            if _tok_len(text, enc) <= size:
                units.append({"text": text, "type": "prose", "section": section, "page": page, "table_id": None, "figure_uri": None})
            else:
                for piece in split_text(text, size, overlap):
                    units.append({"text": piece, "type": "prose", "section": section, "page": page, "table_id": None, "figure_uri": None})
        elif btype == "table":
            if _tok_len(text, enc) <= size:
                units.append({"text": text, "type": "table", "section": section, "page": page, "table_id": b["table_id"], "figure_uri": None})
            else:
                for piece in _split_table_by_rows(text, size, enc):
                    units.append({"text": piece, "type": "table", "section": section, "page": page, "table_id": b["table_id"], "figure_uri": None})
        else:  # figure -- never split
            units.append({"text": text, "type": "figure", "section": section, "page": page, "table_id": None, "figure_uri": b["figure_uri"]})

    # 2) Greedily pack consecutive prose units up to the budget; atomic units
    #    (table/figure) always stand alone so they stay whole and citable.
    chunks, cur, cur_tok = [], [], 0

    def flush():
        nonlocal cur, cur_tok
        if not cur:
            return
        section = next((u["section"] for u in cur if u["section"]), None)
        body = "\n\n".join(u["text"] for u in cur)
        text = f"## {section}\n\n{body}" if section else body
        types = {u["type"] for u in cur}
        btype = next(iter(types)) if len(types) == 1 else "mixed"
        single = len(cur) == 1
        chunks.append(
            {
                "text": text,
                "block_type": btype,
                "section": section,
                "page": cur[0]["page"],
                "table_id": cur[0]["table_id"] if (btype == "table" and single) else None,
                "figure_uri": cur[0]["figure_uri"] if (btype == "figure" and single) else None,
            }
        )
        cur, cur_tok = [], 0

    for u in units:
        utok = _tok_len(u["text"], enc)
        is_atomic = u["type"] in ("table", "figure")
        if cur and (cur_tok + utok > size or is_atomic):
            flush()
        cur.append(u)
        cur_tok += utok
        if is_atomic:
            flush()
    flush()
    return chunks


def run(spark, notebookutils, config=None, force_paths=None):  # noqa: ARG001 - notebookutils reserved
    """Pack new/changed silver contract blocks into the ``contract_chunks`` table.

    Args:
        spark: Active Spark session.
        notebookutils: Fabric notebook utilities (reserved).
        config: Environment config dict; uses ``silver`` and ``chunking`` sections.
        force_paths: Optional list of ``relative_path`` values (or the string
            "ALL") to force-rechunk even when unchanged; falls back to the
            ``reprocess.force_paths`` config key.
    """
    cfg = config or {}
    silver_cfg = cfg.get("silver", {})
    chunk_cfg = cfg.get("chunking", {})

    blocks_table = silver_cfg.get("blocks_table", "contract_blocks")
    chunks_table = silver_cfg.get("chunks_table", "contract_chunks")
    chunks_view = silver_cfg.get("chunks_active_view", "contract_chunks_active")
    size = int(chunk_cfg.get("chunk_size", 512))
    overlap = int(chunk_cfg.get("chunk_overlap", 64))

    # Reprocessing fingerprint: changes when this module OR the chunk params change.
    current_code = code_fingerprint([__file__], {"chunk_size": size, "chunk_overlap": overlap})

    print(
        f"[chunk] source={blocks_table}, target={chunks_table}, "
        f"size={size}, overlap={overlap}, code={current_code}"
    )

    if not spark.catalog.tableExists(blocks_table):
        print(f"Blocks table '{blocks_table}' does not exist yet; run silver extraction first.")
        return

    # Live blocks only (current, non-deleted). Each contract has exactly one
    # current version, so its blocks share a single content_hash.
    blocks_live = spark.table(blocks_table).where(
        (F.col("is_current") == True) & (F.col("doc_deleted") == False)  # noqa: E712
    )

    # One row per contract; derive the chunk version_id by chaining the silver
    # blocks' own version_id (path + silver version + chunk code). The silver
    # version_id already folds in the raw content_hash *and* the silver
    # extraction code, so chunking re-runs whenever the blocks change -- whether
    # from new raw content or from new silver extraction logic. Keying on the raw
    # content_hash alone would miss the latter and leave chunks stale.
    contracts = (
        blocks_live
        .select(
            "relative_path", "file_name", "content_hash",
            F.col("version_id").alias("silver_version_id"),
        )
        .distinct()
        .withColumn("code_hash", F.lit(current_code))
        .withColumn(
            "version_id",
            F.expr(
                "substr(sha2(concat_ws('|', relative_path, silver_version_id, "
                "code_hash), 256), 1, 16)"
            ),
        )
    )

    # Determine which contracts need (re)chunking: a current contract version
    # whose version_id is not already the live chunk set.
    if spark.catalog.tableExists(chunks_table):
        existing = (
            spark.table(chunks_table)
            .where(F.col("is_current") == True)  # noqa: E712
            .select("version_id")
            .distinct()
        )
        pending = contracts.join(existing, on="version_id", how="left_anti")
    else:
        pending = contracts

    # Force-reprocess selected (or all) active contracts even when their
    # version_id is unchanged; scd2_expire_and_append replaces the live chunk set
    # in place (delete-on-same-version) so chunk_id keys do not duplicate.
    force_paths = resolve_force_paths(force_paths, cfg)
    pending = apply_force(contracts, pending, force_paths)

    pending_rows = pending.collect()

    if not pending_rows:
        print("No contracts require (re)chunking.")
    else:
        created_at = datetime.now(timezone.utc)
        changed_paths = [r["relative_path"] for r in pending_rows]
        meta = {
            r["relative_path"]: (r["version_id"], r["content_hash"], r["file_name"])
            for r in pending_rows
        }

        # Fetch the ordered blocks of the pending contracts and group by path.
        block_rows = (
            blocks_live
            .where(F.col("relative_path").isin(changed_paths))
            .select(
                "relative_path", "block_index", "type", "section",
                "page", "text", "table_id", "figure_uri",
            )
            .orderBy("relative_path", "block_index")
            .collect()
        )
        grouped = defaultdict(list)
        for b in block_rows:
            grouped[b["relative_path"]].append(b)

        enc = _encoder()
        rows = []
        for path, blist in grouped.items():
            vid, chash, fname = meta[path]
            for i, ch in enumerate(_pack_blocks(blist, size, overlap, enc)):
                rows.append(
                    Row(
                        chunk_id=_safe_key(path, i, vid),
                        version_id=vid,
                        relative_path=path,
                        file_name=fname,
                        chunk_index=i,
                        text=ch["text"],
                        char_count=len(ch["text"]),
                        block_type=ch["block_type"],
                        section=ch["section"],
                        page=ch["page"],
                        table_id=ch["table_id"],
                        figure_uri=ch["figure_uri"],
                        content_hash=chash,
                        code_hash=current_code,
                        valid_from=created_at,
                        valid_to=None,
                        is_current=True,
                        doc_deleted=False,
                    )
                )

        source_df = spark.createDataFrame(rows, schema=CHUNKS_SCHEMA)

        # SCD2: expire the current chunk set of each (re)chunked contract and
        # append the new set; prior versions' chunks are retained as history.
        scd2_expire_and_append(
            spark,
            chunks_table,
            source_df,
            changed_paths,
            key="relative_path",
            now=created_at,
        )
        print(
            f"Re-chunked {len(changed_paths)} contract(s) -> "
            f"{source_df.count()} new chunk(s) in '{chunks_table}'."
        )

    # Tombstone any *currently live* chunk that no longer reflects a current
    # contract version produced by the current chunking code. ``version_id``
    # encodes path + silver_version_id + code_hash, so a single membership test
    # against the current valid versions covers every self-heal case: contract
    # deleted/re-keyed, re-chunked under a new hash, extraction now failing (no
    # live blocks -> path absent from ``contracts``), or chunking code/params
    # changed. History rows (is_current = false) are left intact.
    if spark.catalog.tableExists(chunks_table):
        created_at_ts = locals().get("created_at") or datetime.now(timezone.utc)
        valid_versions = contracts.select("version_id").distinct()
        live = spark.table(chunks_table).where(
            (F.col("is_current") == True) & (F.col("doc_deleted") == False)  # noqa: E712
        )
        stale_ids = [
            r["chunk_id"]
            for r in live.select("chunk_id", "version_id")
            .join(valid_versions, on="version_id", how="left_anti")
            .collect()
        ]

        tgt = DeltaTable.forName(spark, chunks_table)
        if stale_ids:
            tgt.update(
                condition=F.col("chunk_id").isin(stale_ids)
                & (F.col("doc_deleted") == False),  # noqa: E712
                set={
                    "doc_deleted": F.lit(True),
                    "is_current": F.lit(False),
                    "valid_to": F.lit(created_at_ts),
                },
            )
        print(
            f"Synced chunk tombstones: {len(stale_ids)} stale chunk(s) retired "
            f"against {valid_versions.count()} valid contract version(s)."
        )

        spark.sql(
            f"CREATE OR REPLACE VIEW {chunks_view} AS "
            f"SELECT * FROM {chunks_table} "
            f"WHERE is_current = true AND doc_deleted = false"
        )
        active_count = spark.table(chunks_view).count()
        print(f"View '{chunks_view}' up to date: {active_count} active chunk(s).")

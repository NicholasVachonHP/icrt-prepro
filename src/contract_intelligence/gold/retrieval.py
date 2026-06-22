"""Retrieval over the existing Azure AI Search chunk index for gold extraction.

Gold queries the same per-chunk index that ``serving/search_index.py`` builds,
using the index's **integrated (server-side) vectorizer**: gold sends the field
question as plain text and AI Search embeds it with the same
``text-embedding-3-large`` deployment that built the index, so query and document
vectors always match (a model/version mismatch would silently degrade recall).
Results are filtered to one contract (``document_id`` = the silver
``relative_path``) and returned with their chunk id / section / page for evidence
linkage. All failures degrade to an empty result so the caller falls back to the
full-text path -- retrieval never regresses recall below today's baseline.
"""

import os


def get_search_client(cfg):
    """Build a ``SearchClient`` for the chunk index, or ``None`` if AI Search is
    not configured (no endpoint/key in the environment) -- callers then use the
    full-text path."""
    endpoint = os.environ.get("AZURE_SEARCH_ENDPOINT")
    api_key = os.environ.get("AZURE_SEARCH_API_KEY")
    if not endpoint or not api_key:
        return None

    from azure.core.credentials import AzureKeyCredential
    from azure.search.documents import SearchClient

    ai_cfg = (cfg or {}).get("ai_search", {})
    index_name = os.environ.get("AZURE_SEARCH_INDEX_NAME", ai_cfg.get("index_name"))
    if not index_name:
        return None
    return SearchClient(
        endpoint=endpoint,
        index_name=index_name,
        credential=AzureKeyCredential(api_key),
    )


def _odata_escape(value):
    """Escape a string literal for an OData ``$filter`` (single quotes doubled)."""
    return str(value).replace("'", "''")


def retrieve_chunks(search_client, question, relative_path, top_k, vector_field="embedding"):
    """Hybrid (vector + keyword) search for one contract's chunks most relevant
    to ``question``.

    Returns a list of dicts (``chunk_id``, ``text``, ``section``, ``page``,
    ``chunk_index``, ``score``), best first. Returns ``[]`` on any failure so the
    caller can fall back to full text.
    """
    from azure.search.documents.models import VectorizableTextQuery

    try:
        vq = VectorizableTextQuery(
            text=question, k_nearest_neighbors=top_k, fields=vector_field
        )
        results = search_client.search(
            search_text=question,
            vector_queries=[vq],
            filter=f"document_id eq '{_odata_escape(relative_path)}'",
            top=top_k,
            select=["chunk_id", "text", "section", "page", "chunk_index"],
        )
        return [
            {
                "chunk_id": r.get("chunk_id"),
                "text": r.get("text"),
                "section": r.get("section"),
                "page": r.get("page"),
                "chunk_index": r.get("chunk_index"),
                "score": r.get("@search.score"),
            }
            for r in results
        ]
    except Exception:  # noqa: BLE001 - retrieval failure -> full-text fallback
        return []


def retrieve_group_chunks(search_client, fields, relative_path, top_k):
    """Union of retrieved chunks across every field question in a strategy group.

    Each field's question is retrieved independently (``top_k`` each); chunks are
    de-duplicated by ``chunk_id`` (keeping the higher score) and returned in
    document order. Returns ``(chunks, chunk_ids)``.
    """
    by_id = {}
    for f in fields:
        question = f.get("question") or f.get("field_name", "")
        for c in retrieve_chunks(search_client, question, relative_path, top_k):
            cid = c.get("chunk_id")
            if cid is None:
                continue
            prev = by_id.get(cid)
            if prev is None or (c.get("score") or 0) > (prev.get("score") or 0):
                by_id[cid] = c
    chunks = sorted(
        by_id.values(),
        key=lambda c: (
            c.get("chunk_index") if c.get("chunk_index") is not None else 1_000_000
        ),
    )
    return chunks, [c["chunk_id"] for c in chunks]

"""Embed silver chunks and publish them to an Azure AI Search index.

Reads the *live* rows of the silver ``contract_chunks`` SCD Type 2 table
(``is_current`` AND NOT ``is_deleted``), embeds each chunk with the Foundry
embedding deployment, and upserts the vectors + searchable text into Azure AI
Search. Non-live chunks — tombstones and superseded historical versions
(``is_current = false``) — are removed from the index so only the current
version of every contract is searchable.

Connection settings are read from environment variables (loaded from the Fabric
environment resource by ``common.config.load_secrets``):
  - AZURE_SEARCH_ENDPOINT
  - AZURE_SEARCH_API_KEY
  - AZURE_SEARCH_INDEX_NAME      (falls back to ai_search.index_name)
  - AZURE_OPENAI_ENDPOINT / EMBEDDING_MODEL  (client-side embedding + the index's integrated query vectorizer)
  - AZURE_AI_PROJECT_ENDPOINT    (used by common.ai_clients)

Designed to run inside a Microsoft Fabric notebook attached to the *silver*
lakehouse, where ``spark`` and ``notebookutils`` are in global scope.
"""

import os

from pyspark.sql import functions as F


def ensure_index(
    endpoint,
    api_key,
    index_name,
    dimensions,
    *,
    profile_name,
    vectorizer_name,
    semantic_config_name,
    resource_url,
    deployment,
    aoai_api_key,
    recreate=False,
):
    """Create or update the chunk search index (vector + semantic + integrated
    vectorization). Idempotent: re-running keeps the schema, vectorizer and
    semantic configuration in sync -- unlike portal-added settings, which a
    code-driven recreate would silently drop. Pass ``recreate=True`` to drop and
    rebuild (needed when field types/analyzers change)."""
    from azure.core.credentials import AzureKeyCredential
    from azure.search.documents.indexes import SearchIndexClient
    from azure.search.documents.indexes.models import (
        SearchableField,
        SearchField,
        SearchFieldDataType,
        SearchIndex,
        SimpleField,
    )

    from contract_intelligence.serving.search_common import (
        build_semantic_search,
        build_vector_search,
        upsert_index,
    )

    client = SearchIndexClient(endpoint=endpoint, credential=AzureKeyCredential(api_key))

    fields = [
        SimpleField(name="chunk_id", type=SearchFieldDataType.String, key=True, filterable=True, sortable=True),
        SimpleField(name="document_id", type=SearchFieldDataType.String, filterable=True, facetable=True),
        SearchableField(name="source_document", type=SearchFieldDataType.String, filterable=True, facetable=True),
        SimpleField(name="chunk_index", type=SearchFieldDataType.Int32, filterable=True, sortable=True),
        SearchableField(name="text", type=SearchFieldDataType.String),
        SimpleField(name="char_count", type=SearchFieldDataType.Int32, filterable=True),
        SimpleField(name="block_type", type=SearchFieldDataType.String, filterable=True, facetable=True),
        SearchableField(name="section", type=SearchFieldDataType.String, filterable=True),
        SimpleField(name="page", type=SearchFieldDataType.Int32, filterable=True, sortable=True),
        SimpleField(name="figure_uri", type=SearchFieldDataType.String),
        SearchField(
            name="embedding",
            type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
            searchable=True,
            vector_search_dimensions=dimensions,
            vector_search_profile_name=profile_name,
        ),
    ]

    vector_search = build_vector_search(
        profile_name=profile_name,
        vectorizer_name=vectorizer_name,
        resource_url=resource_url,
        deployment=deployment,
        aoai_api_key=aoai_api_key,
    )
    semantic_search = build_semantic_search(
        config_name=semantic_config_name,
        title_field="source_document",
        content_fields=["text", "section"],
        keyword_fields=["block_type"],
    )

    index = SearchIndex(
        name=index_name,
        fields=fields,
        vector_search=vector_search,
        semantic_search=semantic_search,
    )
    upsert_index(client, index, recreate=recreate)


def run(spark, notebookutils, config=None, recreate=False):
    """Embed active silver chunks and upsert them into Azure AI Search."""
    from azure.core.credentials import AzureKeyCredential
    from azure.search.documents import SearchClient

    from contract_intelligence.common import ai_clients
    from contract_intelligence.serving.search_common import aoai_resource_url

    cfg = config or {}
    silver_cfg = cfg.get("silver", {})
    serving_cfg = cfg.get("serving", {})
    chunk_cfg = cfg.get("chunking", {})
    ai_cfg = cfg.get("ai_search", {})

    endpoint = os.environ["AZURE_SEARCH_ENDPOINT"]
    api_key = os.environ["AZURE_SEARCH_API_KEY"]
    index_name = os.environ.get(
        "AZURE_SEARCH_INDEX_NAME", ai_cfg.get("index_name")
    )
    dimensions = int(chunk_cfg.get("embedding_dimensions", 3072))
    batch_size = int(serving_cfg.get("upload_batch_size", 100))
    chunks_table = silver_cfg.get("chunks_table", "contract_chunks")

    profile_name = ai_cfg.get("vector_profile_name", "embedding-profile")
    vectorizer_name = ai_cfg.get("vectorizer_name", "aoai-embedding-vectorizer")
    semantic_config_name = ai_cfg.get("semantic_config_name", "ictr-semantic")
    deployment = os.environ.get("EMBEDDING_MODEL") or ai_cfg.get(
        "embedding_deployment", "text-embedding-3-large"
    )
    resource_url = aoai_resource_url(os.environ["AZURE_OPENAI_ENDPOINT"])
    aoai_api_key = os.environ["AZURE_AI_PROJECT_API_KEY"]

    print(f"[serving] index={index_name}, source={chunks_table}, batch={batch_size}")

    ensure_index(
        endpoint,
        api_key,
        index_name,
        dimensions,
        profile_name=profile_name,
        vectorizer_name=vectorizer_name,
        semantic_config_name=semantic_config_name,
        resource_url=resource_url,
        deployment=deployment,
        aoai_api_key=aoai_api_key,
        recreate=recreate,
    )

    search_client = SearchClient(
        endpoint=endpoint, index_name=index_name, credential=AzureKeyCredential(api_key)
    )
    openai_client = ai_clients.get_openai_client(notebookutils)

    # The chunks table is SCD Type 2: only the live version of each contract's
    # chunks (is_current AND NOT doc_deleted) is published; every other row
    # (historical versions and tombstones) is removed from the index.
    chunks_df = spark.table(chunks_table)

    active = (
        chunks_df
        .where((F.col("is_current") == True) & (F.col("doc_deleted") == False))  # noqa: E712
        .select(
            "chunk_id", "relative_path", "file_name", "chunk_index", "text", "char_count",
            "block_type", "section", "page", "figure_uri",
        )
        .collect()
    )

    uploaded = 0
    for start in range(0, len(active), batch_size):
        batch = active[start : start + batch_size]
        vectors = ai_clients.embed_texts(openai_client, [r["text"] for r in batch])
        docs = [
            {
                "chunk_id": r["chunk_id"],
                "document_id": r["relative_path"],
                "source_document": r["file_name"],
                "chunk_index": int(r["chunk_index"]),
                "text": r["text"],
                "char_count": int(r["char_count"]),
                "block_type": r["block_type"],
                "section": r["section"],
                "page": (int(r["page"]) if r["page"] is not None else None),
                "figure_uri": r["figure_uri"],
                "embedding": vec,
            }
            for r, vec in zip(batch, vectors)
        ]
        search_client.merge_or_upload_documents(documents=docs)
        uploaded += len(docs)
        print(f"  uploaded {uploaded}/{len(active)} chunk(s)")

    # Remove from the index every chunk that is not currently live: tombstoned
    # chunks AND superseded historical versions (is_current = false).
    stale = chunks_df.where(
        (F.col("doc_deleted") == True) | (F.col("is_current") == False)  # noqa: E712
    )
    stale_ids = [r["chunk_id"] for r in stale.select("chunk_id").distinct().collect()]
    if stale_ids:
        search_client.delete_documents(
            documents=[{"chunk_id": cid} for cid in stale_ids]
        )
        print(f"  deleted {len(stale_ids)} non-live chunk(s) from index")

    print(f"[serving] complete: {uploaded} active chunk(s) indexed in '{index_name}'.")

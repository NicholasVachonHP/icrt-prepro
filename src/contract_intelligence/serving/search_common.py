"""Shared Azure AI Search builders for the contract-intelligence serving indexes.

Both serving indexes -- the chunk index (``serving.search_index``, one doc per
silver chunk) and the contract-fields index (``serving.contracts_index``, one doc
per contract) -- use the same query-time *integrated vectorization*
(``AzureOpenAIVectorizer`` over the Foundry embedding deployment) and semantic
ranking. These helpers build the reusable ``VectorSearch`` / ``SemanticSearch``
objects and the create-or-update logic so the two indexers stay in lock-step and
the vectorizer + semantic config are defined **in code** (not hand-added in the
portal, where they are silently lost on any code-driven recreate).

Requires ``azure-search-documents>=11.5.1`` (the GA classes
``AzureOpenAIVectorizer`` / ``AzureOpenAIVectorizerParameters`` and the
``semantic_search`` index argument).
"""

from urllib.parse import urlparse


def aoai_resource_url(openai_endpoint):
    """Return the bare Azure OpenAI resource URL (``scheme://host``).

    ``AZURE_OPENAI_ENDPOINT`` is the OpenAI-v1 form
    (``https://<res>.openai.azure.com/openai/v1``); the search vectorizer wants
    the resource root (``https://<res>.openai.azure.com``).
    """
    parsed = urlparse(openai_endpoint or "")
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"Invalid AZURE_OPENAI_ENDPOINT: {openai_endpoint!r}")
    return f"{parsed.scheme}://{parsed.netloc}"


def build_vector_search(
    *,
    profile_name,
    vectorizer_name,
    resource_url,
    deployment,
    aoai_api_key,
    algorithm_name="hnsw-config",
):
    """Build a ``VectorSearch`` with an HNSW algorithm, a profile, and an
    ``AzureOpenAIVectorizer`` bound to ``profile_name`` so queries can be
    vectorized server-side (no client-side embedding of the query text).

    ``aoai_api_key`` is the **Azure OpenAI** resource key (AZURE_AI_PROJECT_API_KEY)
    used by the vectorizer to call the embedding deployment -- NOT the Search
    admin key (passing the Search key yields a 401 at query time)."""
    from azure.search.documents.indexes.models import (
        AzureOpenAIVectorizer,
        AzureOpenAIVectorizerParameters,
        HnswAlgorithmConfiguration,
        HnswParameters,
        VectorSearch,
        VectorSearchAlgorithmKind,
        VectorSearchAlgorithmMetric,
        VectorSearchProfile,
    )

    return VectorSearch(
        algorithms=[
            HnswAlgorithmConfiguration(
                name=algorithm_name,
                kind=VectorSearchAlgorithmKind.HNSW,
                parameters=HnswParameters(
                    m=4,
                    ef_construction=400,
                    ef_search=500,
                    metric=VectorSearchAlgorithmMetric.COSINE,
                ),
            )
        ],
        profiles=[
            VectorSearchProfile(
                name=profile_name,
                algorithm_configuration_name=algorithm_name,
                vectorizer_name=vectorizer_name,
            )
        ],
        vectorizers=[
            AzureOpenAIVectorizer(
                vectorizer_name=vectorizer_name,
                parameters=AzureOpenAIVectorizerParameters(
                    resource_url=resource_url,
                    deployment_name=deployment,
                    model_name=deployment,
                    api_key=aoai_api_key,
                ),
            )
        ],
    )


def build_semantic_search(*, config_name, title_field, content_fields, keyword_fields=None):
    """Build a ``SemanticSearch`` with a single configuration that prioritises the
    given title / content / keyword fields for L2 semantic re-ranking."""
    from azure.search.documents.indexes.models import (
        SemanticConfiguration,
        SemanticField,
        SemanticPrioritizedFields,
        SemanticSearch,
    )

    prioritized = SemanticPrioritizedFields(
        title_field=SemanticField(field_name=title_field) if title_field else None,
        content_fields=[SemanticField(field_name=f) for f in content_fields],
        keywords_fields=[SemanticField(field_name=f) for f in (keyword_fields or [])],
    )
    return SemanticSearch(
        default_configuration_name=config_name,
        configurations=[
            SemanticConfiguration(name=config_name, prioritized_fields=prioritized)
        ],
    )


def upsert_index(client, index, *, recreate=False):
    """Create or update ``index`` idempotently.

    With ``recreate=True`` an existing index is dropped first (use when changing
    field types/analyzers, which ``create_or_update`` cannot do in place).
    """
    existing = {i.name for i in client.list_indexes()}
    if recreate and index.name in existing:
        client.delete_index(index.name)
        existing.discard(index.name)
        print(f"[serving] dropped existing index '{index.name}' for recreate.")
    client.create_or_update_index(index)
    verb = "updated" if index.name in existing else "created"
    print(f"[serving] {verb} index '{index.name}'.")

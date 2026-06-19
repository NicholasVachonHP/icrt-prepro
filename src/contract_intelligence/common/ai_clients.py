"""AI client helpers for the contract intelligence pipeline.

Provides an OpenAI client bound to the Microsoft Foundry resource's Azure OpenAI
``/openai/v1`` (OpenAI-compatible) endpoint, authenticated with an API key. Using
the plain ``openai.OpenAI`` client against the v1 surface avoids both the AAD/RBAC
data-action requirement and ``AzureOpenAI``'s api-version handling.

Environment variables (loaded from the .env resource by ``common.config``):
  - AZURE_OPENAI_ENDPOINT     : Azure OpenAI v1 base URL
                               (https://<res>.openai.azure.com/openai/v1)
  - AZURE_AI_PROJECT_API_KEY  : API key for the Foundry / Azure OpenAI resource
  - MAIN_MODEL                : chat/completion deployment name (e.g. gpt-4.1)
  - EMBEDDING_MODEL           : embedding deployment name (text-embedding-3-large)

TODO (security): the API key is read from the .env resource. Migrate it to Azure
Key Vault and read it with ``notebookutils.credentials.getSecret`` once available.
"""

import os


def get_openai_client(notebookutils=None):  # noqa: ARG001 - kept for call-site compatibility
    """Return an ``openai.OpenAI`` client bound to the Foundry v1 endpoint (API key)."""
    from openai import OpenAI

    return OpenAI(
        base_url=os.environ["AZURE_OPENAI_ENDPOINT"],
        api_key=os.environ["AZURE_AI_PROJECT_API_KEY"],
    )


def embed_texts(client, texts, model=None):
    """Embed a list of strings, returning a list of vectors (order preserved)."""
    model = model or os.environ["EMBEDDING_MODEL"]
    resp = client.embeddings.create(model=model, input=texts)
    return [d.embedding for d in resp.data]

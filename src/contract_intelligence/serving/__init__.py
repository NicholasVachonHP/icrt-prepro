"""Serving layer for the contract intelligence pipeline.

Publishes silver ``contract_chunks`` to an external Azure AI Search index
(embeddings + searchable text). This is intentionally separate from the
medallion lakehouse tables: it pushes already-prepared data to an external query
engine consumed downstream by a Microsoft Foundry RAG agent.
"""

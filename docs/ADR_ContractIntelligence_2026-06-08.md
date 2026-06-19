# Architecture Design Report
##  Contract Intelligence Extraction Pipeline - DEV

**Date:** Monday, 8 June 2026  
**Decision Makers:** Amir, Nicholas  
**Status:** APPROVED  
**Last Updated:** 10 June 2026

---

## Executive Summary

This ADR documents the architecture for a daily contract intelligence extraction pipeline in Microsoft Fabric. The system automatically injects contracts from SharePoint (PDF and DOCX formats), extracts and chunks text, embeds content for semantic search, and uses GPT-4.1 to extract structured fields (parties, dates, values, terms, etc.). The entire pipeline is orchestrated via Fabric Data Pipeline with a daily 04:00 GMT schedule.

---

## 1. Problem Statement

**Business Need:**  
Automate the ingestion, processing, and structured extraction of contract data from unstructured documents (PDF, DOCX) stored in SharePoint. Enable rapid discovery and comparison of contract terms across the organization.

**Current Challenges:**
- Manual contract review is time-consuming and error-prone
- No centralized index for semantic search across contracts
- Extraction of key terms (parties, effective dates, renewal clauses, liability caps, etc.) requires manual effort
- No unified view of contract metadata

**Solution Scope:**
- Daily automated ingestion from SharePoint via OneLake shortcuts
- Text extraction (Document Intelligence Layout for images/tables)
- Chunked text storage for context management
- Vector embeddings for semantic search
- Structured field extraction via large language model (GPT-4.1)
- Azure AI Search indexing for retrieval-augmented generation (RAG) use cases

---

## 2. Architecture Overview

### 2.1 High-Level Topology

```
SharePoint Contracts (PDF/DOCX)
    ↓
OneLake Shortcut → Bronze Lakehouse (contract_inventory)
    ↓
Silver Lakehouse (contract_text table)
    ├─→ Chunk & Embed → contract_chunks table
    │       ↓
    │   Azure AI Search Index (ictr_dev)
    │       ↓
    │   [RAG Agent / Retrieval - out of scope for MVP]
    │
    └─→ Structured Extraction (gpt-4.1) → Gold Lakehouse (contract_fields)
```

### 2.2 System Components

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Ingestion** | Microsoft Fabric Lakehouse (Bronze) | Store raw contracts with metadata (hash, timestamps, deletion soft-flags) |
| **Text Extraction** | Python notebooks +  Document Intelligence | Extraction of text, tables and image in structured way |
| **Image Verbatime** | Azure OpenAI (gpt-4.1) | Describe image with words
| **Chunking** | Custom Python (tiktoken tokenizer) | Split text into 512-token chunks (64-token overlap) for context windows |
| **Embeddings** | Azure OpenAI (text-embedding-3-large) | 3072-dimensional vector embeddings for semantic search |
| **Search Index** | Azure AI Search (Free SKU) | HNSW cosine similarity index over chunks |
| **Field Extraction** | Azure OpenAI (gpt-4.1) | JSON-structured extraction of 10 contract fields via system prompt |
| **Orchestration** | Microsoft Fabric Data Pipeline | Daily schedule (05:00 GMT); fan-out: nb01 → nb02 → (nb03 & nb04 parallel) |
| **Storage** | Microsoft Fabric Lakehouse (Silver, Gold) | Delta Lake tables (ACID, versioning, soft-delete support) |
| **Runtime** | Spark 3.4 (Fabric Runtime 1.3) | Distributed compute for data processing |

---

## 3. Infrastructure & Endpoints

| Service | Endpoint | Purpose | Auth |
|---------|----------|---------|------|
| **Fabric REST API** | `https://api.fabric.microsoft.com/v1` | Workspace, lakehouse, notebook, pipeline CRUD | Bearer (Fabric) |
| **Azure OpenAI** | `https://fp-sprag-dev-uks-01-resource.openai.azure.com/openai/v1` | gpt-4.1, text-embedding-3-large | API key |
| **Azure AI Search** | `https://srch-sprag-dev-uks-01.search.windows.net` | Index CRUD, vector search | API key |
| **Document Intelligence** | `https://di-sprag-dev-uks-01.cognitiveservices.azure.com/` | Layout analysis (phase 2) | API key |

**Secrets:** `.env_temp_fabric_ictr` (Environment resource file) → Migrate to Key Vault (post-MVP)

---

## 4. Lakehouses & Notebooks

| Name | Type | Purpose | Items |
|------|------|---------|-------|
| **ictr_lh_bronze_dev** | Lakehouse | Raw contracts + metadata | table: contract_inventory |
| **ictr_lh_silver_dev** | Lakehouse | Extracted text + chunks | tables: contract_text, contract_chunks |
| **ictr_lh_gold_dev** | Lakehouse | Structured fields | table: contract_fields |
| **ictr_lh_shared** | Lakehouse | Shared code (mounted) | src/contract_intelligence/ |

**Notebooks:**
- **nb01:** Bronze ingestion (SharePoint → contract_inventory) — 4 notebooks
- **nb02:** Silver text extraction (Document Intelligence Layout)
- **nb03:** Chunk, embed, index (→ AI Search ictr_dev)
- **nb04:** Gold field extraction (gpt-4.1)

**Orchestration:** Data Pipeline `ictr_pl_contract_intelligence_daily`
- Schedule: Daily 04:00 GMT
- Topology: nb01 → nb02 → (nb03 & nb04 parallel)
- Env: ictr_dev (Spark Runtime 1.3, packages: pypdf, python-docx, azure-search-documents, openai, tiktoken)

---

## 5. Data Schema

| Layer | Table | Key Columns | Key Logic |
|-------|-------|-------------|-----------|
| **Bronze** | contract_inventory | relative_path (PK), content_hash, is_deleted | SHA-256, MERGE, soft-delete |
| **Silver** | contract_text | relative_path (PK), extracted_text, is_deleted | Document Intelligence Layout extraction |
| **Silver** | contract_chunks | chunk_id (PK), text, is_deleted | 512-token chunks, overlap 64 tokens |
| **Gold** | contract_fields | relative_path (PK), [10 fields], model | gpt-4.1 extraction (parties, dates, terms, values, etc.) |

**AI Search Index:** ictr_dev (HNSW, cosine, 3072-dim embeddings)

---

## 6. Key Decisions

| Decision | Chosen | Rationale |
|----------|--------|-----------|
| Workspace | Reuse existing "HP - Data Warehouse" | Cost, governance, no isolation needed |
| Auth | API-key (openai.OpenAI client) | Avoids AAD permission issues; keys in .env |
| Text Extraction | Document Intelligence Layout | Images, tables, per-page split out-of-box |
| Chunking | Token-based (512, 64-overlap) | LLM-aligned, predictable token counts |
| Gold Schema | Wide (1 col/field, 17 total) | Cross-contract comparison, SQL aggregations |
| Search | Azure AI Search (Free SKU) | Integrated, managed, HNSW, Free SKU sufficient (50 MB) |


---

## 7. Approvals

| Stakeholder | Role | Date |
|-------------|------|------|
| Amir | Architect | 8 June 2026 |
| Nicholas | Implementation Lead | 8 June 2026 |


"""
Authoritative Milvus collection schema for rag_chunks — schema version v1.

This file is the single source of truth. The Helm post-install job
(helm/rag-pipeline/templates/milvus-init-job.yaml) uses an inline copy of this
schema to create the collection on first deploy. If you change the schema here,
bump the version comment and update the init job to match.

To migrate schema:
  1. Increment schema version below
  2. Update milvus-init-job.yaml to recreate the collection (drop + create)
  3. Re-run the full embedding pipeline to repopulate
"""
import os

from pymilvus import CollectionSchema, DataType, FieldSchema

COLLECTION_NAME = "rag_chunks"
EMBEDDING_DIM   = int(os.environ.get("EMBEDDING_DIM", "384"))

# Schema version: v1 (2024-01)
FIELDS = [
    FieldSchema(name="chunk_id",      dtype=DataType.VARCHAR,      max_length=64,   is_primary=True),
    FieldSchema(name="url",            dtype=DataType.VARCHAR,      max_length=2048),
    FieldSchema(name="title",          dtype=DataType.VARCHAR,      max_length=512),
    FieldSchema(name="chunk_index",    dtype=DataType.INT64),
    FieldSchema(name="total_chunks",   dtype=DataType.INT64),
    FieldSchema(name="text",           dtype=DataType.VARCHAR,      max_length=4096),
    FieldSchema(name="content_hash",   dtype=DataType.VARCHAR,      max_length=64),
    FieldSchema(name="created_at",     dtype=DataType.INT64),
    FieldSchema(name="embedding",      dtype=DataType.FLOAT_VECTOR, dim=EMBEDDING_DIM),
]

SCHEMA = CollectionSchema(
    fields=FIELDS,
    description="RAG pipeline document chunks — schema v1",
)

INDEX_PARAMS = {
    "metric_type": "COSINE",
    "index_type":  "IVF_FLAT",
    # nlist=128: number of Voronoi cells. Good recall for <500K vectors.
    # For >500K vectors: switch to HNSW with M=16, ef_construction=200.
    "params":      {"nlist": 128},
}

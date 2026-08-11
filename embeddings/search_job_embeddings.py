"""
search_job_embeddings.py

Query-side counterpart to ingest_job_embeddings.py. Embeds a query string
with the same model used at ingestion time and runs a cosine-similarity
search against job_copilot.job_embeddings (pgvector), returning the parent
job_documents rows ranked by best-matching chunk.

This is the entire retrieval path now -- no Databricks Vector Search
endpoint, no Delta sync. Just pgvector's <=> operator against Lakebase.
"""

import logging
from typing import Optional

from app.lakebase import get_connection

logger = logging.getLogger(__name__)

DEFAULT_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM = 384

_model = None
_model_name_loaded: Optional[str] = None


def get_model(model_name: str = DEFAULT_MODEL_NAME):
    """Lazily load the SentenceTransformer model once per process.
    Mirrors ingest_job_embeddings.get_model() so query-time and
    ingestion-time embeddings are always produced by the same model."""
    global _model, _model_name_loaded
    if _model is None or _model_name_loaded != model_name:
        from sentence_transformers import SentenceTransformer
        logger.info("Loading embedding model: %s", model_name)
        _model = SentenceTransformer(model_name)
        _model_name_loaded = model_name
    return _model


def embed_query(query_text: str, model_name: str = DEFAULT_MODEL_NAME) -> list[float]:
    model = get_model(model_name)
    vector = model.encode([query_text], show_progress_bar=False, normalize_embeddings=True)[0]
    vector = vector.tolist()
    if len(vector) != EMBEDDING_DIM:
        raise ValueError(f"Embedding dimension mismatch: expected {EMBEDDING_DIM}, got {len(vector)}")
    return vector


# Cosine distance: pgvector's <=> operator returns distance (lower = closer).
# We convert to a similarity score (1 - distance) for a more intuitive result.
SEARCH_SQL = """
SELECT
    d.id            AS document_id,
    d.source_type,
    e.chunk_index,
    e.chunk_text,
    1 - (e.embedding <=> %(query_vector)s::vector) AS similarity
FROM job_copilot.job_embeddings AS e
JOIN job_copilot.job_documents AS d ON d.id = e.document_id
WHERE (%(source_type)s IS NULL OR e.source_type = %(source_type)s)
ORDER BY e.embedding <=> %(query_vector)s::vector
LIMIT %(top_k)s;
"""


def search_job_embeddings(
    query_text: str,
    top_k: int = 10,
    source_type: Optional[str] = None,
    model_name: str = DEFAULT_MODEL_NAME,
) -> list[dict]:
    """
    Semantic search over job_embeddings (chunk-level).

    Args:
        query_text: natural-language search query.
        top_k: max number of chunk matches to return.
        source_type: optional filter, 'job_posting' or 'resume'.
        model_name: embedding model — must match ingestion to be meaningful.

    Returns:
        List of dicts: document_id, source_type, chunk_index, chunk_text,
        similarity (0-1, higher is closer).
    """
    query_vector = embed_query(query_text, model_name=model_name)

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            SEARCH_SQL,
            {"query_vector": query_vector, "source_type": source_type, "top_k": top_k},
        )
        return [dict(row) for row in cur.fetchall()]

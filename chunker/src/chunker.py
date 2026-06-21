"""
Fixed-size chunking with overlap using tiktoken for token counting.

Why tiktoken (cl100k_base) and not the sentence-transformers tokenizer?
  - The embedding service uses all-MiniLM-L6-v2, which has its own WordPiece
    tokenizer with a slightly different token count per string.
  - In practice, cl100k_base token counts are within ~10% of WordPiece counts
    for English text, which is close enough for a 512-token window.
  - Using tiktoken avoids a heavy torch/transformers import in the chunker,
    keeping the container image lean (~50 MB vs ~2 GB).
  - If precise token-boundary alignment matters (e.g. for models with strict
    sequence limits), swap tiktoken for the model's own tokenizer here.
"""
import tiktoken

_ENCODING = tiktoken.get_encoding("cl100k_base")


def chunk_text(
    text: str,
    chunk_size: int = 512,
    overlap: int = 64,
) -> list[str]:
    """
    Split text into overlapping fixed-size token windows.

    Returns a list of decoded chunk strings. Each chunk is at most chunk_size
    tokens. Consecutive chunks share `overlap` tokens at the boundary so that
    a sentence spanning a chunk edge appears in full in at least one chunk.

    Edge case: if the entire text is shorter than chunk_size tokens, a single
    chunk covering the full text is returned.
    """
    if overlap >= chunk_size:
        raise ValueError(f"overlap ({overlap}) must be < chunk_size ({chunk_size})")

    tokens = _ENCODING.encode(text)
    if not tokens:
        return []

    stride = chunk_size - overlap
    chunks: list[str] = []

    start = 0
    while start < len(tokens):
        end = min(start + chunk_size, len(tokens))
        chunk_tokens = tokens[start:end]
        chunks.append(_ENCODING.decode(chunk_tokens))
        if end == len(tokens):
            break
        start += stride

    return chunks


def token_count(text: str) -> int:
    return len(_ENCODING.encode(text))

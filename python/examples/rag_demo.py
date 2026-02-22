#!/usr/bin/env python3
"""Mini RAG (Retrieval-Augmented Generation) pipeline demo.

This script shows how to wire together:
  1. A sentence-transformer model to create dense embeddings.
  2. The kv-store API (via kvstore_client) to persist and retrieve chunks.
  3. A trivial "answer" assembled by concatenating the top-k contexts.

Usage
-----
Install dependencies::

    pip install -r python/requirements.txt

Start the Next.js dev server::

    npm run dev          # runs on http://localhost:3000 by default

Run the demo::

    python python/examples/rag_demo.py

You can also point the demo at a deployed instance::

    BASE_URL=https://your-deployment.vercel.app python python/examples/rag_demo.py
"""

from __future__ import annotations

import os
import sys
import textwrap

# Make sure the local package is importable when running from the repo root.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from kvstore_client import KVStoreClient

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = os.environ.get("BASE_URL", "http://localhost:3000")

# Sample document – split into small chunks for demonstration purposes.
DOCUMENT = textwrap.dedent(
    """\
    A key-value store is a simple database that uses a dictionary-style
    interface. Each piece of data is stored as a pair of a unique key and
    its associated value.

    Consistent hashing is a technique that allows a distributed key-value
    store to spread data across many nodes so that adding or removing a
    node requires minimal reorganisation of the data.

    LRU (Least Recently Used) cache eviction removes the entry that was
    accessed least recently when the cache is full, keeping hot data in
    memory for fast retrieval.

    Replication copies data to multiple nodes, providing fault tolerance
    and high availability. If one node fails, another replica can serve
    the request.

    A RAG (Retrieval-Augmented Generation) pipeline first retrieves
    relevant context from a knowledge base using vector similarity search,
    then uses that context to generate an accurate answer.
    """
)

QUERY_TEXT = "How does consistent hashing help with distributed data?"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def chunk_text(text: str, max_words: int = 40) -> list[str]:
    """Split *text* into chunks of at most *max_words* words."""
    words = text.split()
    chunks: list[str] = []
    for i in range(0, len(words), max_words):
        chunks.append(" ".join(words[i : i + max_words]))
    return [c for c in chunks if c.strip()]


def load_embedder():
    """Load a small sentence-transformer model.

    Returns the model so that callers can call ``model.encode(sentences)``.
    """
    try:
        from sentence_transformers import SentenceTransformer  # type: ignore
    except ImportError:
        print(
            "[demo] sentence-transformers is not installed.\n"
            "       Run: pip install sentence-transformers",
            file=sys.stderr,
        )
        sys.exit(1)

    print("[demo] Loading embedding model (all-MiniLM-L6-v2) …")
    return SentenceTransformer("all-MiniLM-L6-v2")


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def main() -> None:
    client = KVStoreClient(base_url=BASE_URL)
    model = load_embedder()

    # ---- 1. Embed and upsert document chunks --------------------------------
    chunks = chunk_text(DOCUMENT)
    print(f"\n[demo] Upserting {len(chunks)} chunks to {BASE_URL} …")

    embeddings = model.encode(chunks, show_progress_bar=False)

    for i, (chunk, emb) in enumerate(zip(chunks, embeddings)):
        key = f"rag:demo:chunk:{i}"
        ok = client.upsert(
            key=key,
            text=chunk,
            embedding=emb.tolist(),
            metadata={"source": "rag_demo", "chunk_index": i},
        )
        status = "✓" if ok else "✗"
        print(f"  {status} {key}")

    # ---- 2. Query by embedding ----------------------------------------------
    print(f'\n[demo] Query: "{QUERY_TEXT}"')
    query_embedding = model.encode([QUERY_TEXT], show_progress_bar=False)[0]

    top_k = 3
    results = client.query(
        embedding=query_embedding.tolist(),
        top_k=top_k,
        key_prefix="rag:demo:",
    )

    if not results:
        print("[demo] No results returned – is the server running?")
        return

    print(f"\n[demo] Top-{top_k} contexts (by cosine similarity):")
    for r in results:
        print(f"  [{r['score']:.4f}] {r['key']}")
        print(f"    {r['text'][:80]}…" if len(r["text"]) > 80 else f"    {r['text']}")

    # ---- 3. Trivial answer assembly ------------------------------------------
    context = "\n".join(r["text"] for r in results)
    print("\n[demo] ── Assembled answer (proof-of-connection) ──────────────────")
    print(context)
    print("──────────────────────────────────────────────────────────────────────")


if __name__ == "__main__":
    main()

"""
Mini RAG demo using the KVStoreClient.

This script:
1. Generates random "document embeddings" (stand-ins for real encoder outputs)
2. Upserts them into the vector store
3. Issues a query and retrieves the nearest neighbours
4. Prints a simulated RAG response

Usage:
    python python/examples/rag_demo.py [--url http://localhost:3000]

Prerequisites:
    • The Next.js dev server must be running (`npm run dev` in the project root).
    • Optional: replace `fake_embed()` with a real embedding model call.
"""

from __future__ import annotations

import argparse
import math
import random
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from kvstore_client import KVStoreClient


# ── Embedding stub ──────────────────────────────────────────────────────────

DIMENSION = 128


def fake_embed(text: str) -> list[float]:
    """Return a deterministic pseudo-embedding for demo purposes."""
    random.seed(hash(text) & 0xFFFFFFFF)
    vec = [random.gauss(0, 1) for _ in range(DIMENSION)]
    norm = math.sqrt(sum(x * x for x in vec)) or 1.0
    return [x / norm for x in vec]


# ── Corpus ──────────────────────────────────────────────────────────────────

DOCUMENTS = [
    "The quick brown fox jumps over the lazy dog.",
    "Vector databases enable semantic similarity search at scale.",
    "Write-ahead logs provide durability guarantees in storage engines.",
    "IVF-Flat is an approximate nearest neighbour algorithm based on k-means.",
    "Python is a popular language for machine learning workloads.",
    "Next.js supports both server-side rendering and static generation.",
    "Cosine similarity measures the angle between two vectors.",
    "Compaction merges segment files and removes stale tombstones.",
    "Large language models produce dense vector representations of text.",
    "RAG combines retrieval with generation for knowledge-grounded responses.",
]

QUERY = "How does semantic search work with vector embeddings?"


# ── Main ────────────────────────────────────────────────────────────────────


def main(base_url: str) -> None:
    client = KVStoreClient(base_url)

    print("── Ingesting documents ─────────────────────────────────────")
    records = [
        {"id": f"doc-{i}", "vector": fake_embed(doc), "metadata": {"text": doc}}
        for i, doc in enumerate(DOCUMENTS)
    ]
    result = client.upsert_batch(records)
    print(f"  upserted: {result.get('upserted')}")

    print("\n── Stats ───────────────────────────────────────────────────")
    stats = client.stats()
    print(f"  totalRecords : {stats.get('totalRecords')}")
    print(f"  dimension    : {stats.get('dimension')}")
    print(f"  ivfTrained   : {stats.get('ivfTrained')}")

    print(f"\n── Query: {QUERY!r}")
    query_vec = fake_embed(QUERY)
    results = client.query(query_vec, top_k=3)

    print("\n── Top-3 results ───────────────────────────────────────────")
    for rank, hit in enumerate(results, 1):
        text = (hit.get("metadata") or {}).get("text", "")
        print(f"  {rank}. [{hit['score']:.4f}]  {text}")

    print("\n── Simulated RAG response ──────────────────────────────────")
    context = "\n".join(
        f"[{i+1}] {(r.get('metadata') or {}).get('text', '')}" for i, r in enumerate(results)
    )
    print(f"Context:\n{context}")
    print(
        "\nAnswer (mock): Based on the retrieved context, semantic search works by"
        " encoding text as dense vectors and finding documents with the highest"
        " cosine similarity to the query vector."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mini RAG demo")
    parser.add_argument("--url", default="http://localhost:3000", help="KV-Store base URL")
    args = parser.parse_args()
    main(args.url)

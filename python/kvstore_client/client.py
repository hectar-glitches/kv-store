"""Simple API wrappers for the kv-store Next.js backend."""

from __future__ import annotations

import json
import math
from typing import Any

import requests


class KVStoreClient:
    """Thin client for the kv-store REST API.

    Parameters
    ----------
    base_url:
        Root URL of the running Next.js server, e.g. ``http://localhost:3000``.
    timeout:
        Per-request timeout in seconds (default: 10).
    """

    def __init__(self, base_url: str = "http://localhost:3000", timeout: int = 10) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------

    def _set(self, key: str, value: Any, ttl: int | None = None) -> bool:
        """Store *value* (serialised to JSON) under *key*."""
        payload: dict[str, Any] = {"key": key, "value": json.dumps(value)}
        if ttl is not None:
            payload["ttl"] = ttl
        resp = requests.post(
            f"{self.base_url}/api/kvstore/set",
            json=payload,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json().get("success", False)

    def _get(self, key: str) -> Any | None:
        """Retrieve the JSON-decoded value stored under *key*, or ``None``."""
        resp = requests.get(
            f"{self.base_url}/api/kvstore/get",
            params={"key": key},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data.get("found"):
            return None
        raw = data.get("value")
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return raw

    def _keys(self) -> list[str]:
        """Return all keys currently stored."""
        resp = requests.get(
            f"{self.base_url}/api/kvstore/keys",
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json().get("keys", [])

    def _delete(self, key: str) -> bool:
        """Delete *key* from the store."""
        resp = requests.delete(
            f"{self.base_url}/api/kvstore/delete",
            json={"key": key},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json().get("success", False)

    # ------------------------------------------------------------------
    # RAG-oriented helpers
    # ------------------------------------------------------------------

    def upsert(
        self,
        key: str,
        text: str,
        embedding: list[float],
        metadata: dict[str, Any] | None = None,
        ttl: int | None = None,
    ) -> bool:
        """Store a document chunk together with its dense embedding.

        The value written to the store is a JSON object::

            {
                "text": "<chunk text>",
                "embedding": [0.1, 0.2, ...],
                "metadata": { ... }   # optional
            }

        Parameters
        ----------
        key:
            Unique identifier for this chunk (e.g. ``"doc:0:chunk:3"``).
        text:
            The raw text of the chunk.
        embedding:
            Dense vector representation of *text*.
        metadata:
            Arbitrary extra fields (source file, page number, …).
        ttl:
            Optional time-to-live in milliseconds.
        """
        doc = {"text": text, "embedding": embedding, "metadata": metadata or {}}
        return self._set(key, doc, ttl=ttl)

    def query(
        self,
        embedding: list[float],
        top_k: int = 5,
        key_prefix: str | None = None,
    ) -> list[dict[str, Any]]:
        """Retrieve the *top_k* chunks most similar to *embedding*.

        Similarity is computed with cosine similarity.  All keys in the store
        that were written via :meth:`upsert` (i.e. whose values contain an
        ``"embedding"`` field) are considered candidates.

        Parameters
        ----------
        embedding:
            Query vector.
        top_k:
            Maximum number of results to return.
        key_prefix:
            If given, only keys that start with this string are considered.

        Returns
        -------
        List of result dicts ordered by descending similarity::

            [
                {
                    "key": "doc:0:chunk:3",
                    "score": 0.97,
                    "text": "...",
                    "metadata": { ... }
                },
                ...
            ]
        """
        all_keys = self._keys()
        if key_prefix:
            all_keys = [k for k in all_keys if k.startswith(key_prefix)]

        results: list[dict[str, Any]] = []
        for key in all_keys:
            doc = self._get(key)
            if not isinstance(doc, dict) or "embedding" not in doc:
                continue
            score = _cosine_similarity(embedding, doc["embedding"])
            results.append(
                {
                    "key": key,
                    "score": score,
                    "text": doc.get("text", ""),
                    "metadata": doc.get("metadata", {}),
                }
            )

        results.sort(key=lambda r: r["score"], reverse=True)
        return results[:top_k]


# ------------------------------------------------------------------
# Utility
# ------------------------------------------------------------------


def _dot(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b))


def _norm(v: list[float]) -> float:
    return math.sqrt(sum(x * x for x in v))


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """Return the cosine similarity between two vectors."""
    denom = _norm(a) * _norm(b)
    if denom == 0.0:
        return 0.0
    return _dot(a, b) / denom

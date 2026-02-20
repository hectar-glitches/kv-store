"""KVStoreClient – wraps the /api/v1 endpoints of the Next.js vector store."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from urllib.request import urlopen, Request
from urllib.error import HTTPError


class KVStoreClient:
    """Thin HTTP client for the KV-Store vector API.

    Parameters
    ----------
    base_url : str
        Base URL of the Next.js app, e.g. ``http://localhost:3000``.
    """

    def __init__(self, base_url: str = "http://localhost:3000") -> None:
        self.base_url = base_url.rstrip("/")

    # ── internal helpers ────────────────────────────────────────────────────

    def _post(self, path: str, body: Any) -> Any:
        data = json.dumps(body).encode()
        req = Request(
            f"{self.base_url}{path}",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(req) as resp:
                return json.loads(resp.read())
        except HTTPError as exc:
            raise RuntimeError(f"HTTP {exc.code}: {exc.read().decode()}") from exc

    def _get(self, path: str) -> Any:
        req = Request(f"{self.base_url}{path}", method="GET")
        try:
            with urlopen(req) as resp:
                return json.loads(resp.read())
        except HTTPError as exc:
            raise RuntimeError(f"HTTP {exc.code}: {exc.read().decode()}") from exc

    # ── public API ──────────────────────────────────────────────────────────

    def upsert(
        self,
        id: str,
        vector: List[float],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Upsert a single record."""
        body: Dict[str, Any] = {"id": id, "vector": vector}
        if metadata:
            body["metadata"] = metadata
        return self._post("/api/v1/upsert", body)

    def upsert_batch(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Upsert a batch of records.

        Each record must have ``id`` and ``vector`` keys; ``metadata`` is optional.
        """
        return self._post("/api/v1/upsert", records)

    def query(
        self,
        vector: List[float],
        top_k: int = 10,
        nprobe: int = 8,
        metadata_filter: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Query for the nearest neighbours.

        Returns a list of ``{"id", "score", "metadata"}`` dicts.
        """
        body: Dict[str, Any] = {"vector": vector, "topK": top_k, "nprobe": nprobe}
        if metadata_filter:
            body["filter"] = metadata_filter
        return self._post("/api/v1/query", body).get("results", [])

    def delete(self, id: str) -> List[str]:
        """Delete a record by id. Returns list of actually-deleted ids."""
        return self._post("/api/v1/delete", {"id": id}).get("deleted", [])

    def delete_batch(self, ids: List[str]) -> List[str]:
        """Delete multiple records by id."""
        return self._post("/api/v1/delete", {"ids": ids}).get("deleted", [])

    def stats(self) -> Dict[str, Any]:
        """Retrieve store statistics."""
        return self._get("/api/v1/stats")

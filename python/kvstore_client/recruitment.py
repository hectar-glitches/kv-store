"""Recruitment-specific helpers built on top of :class:`KVStoreClient`."""

from __future__ import annotations

from typing import Any

from .client import KVStoreClient


class RecruitmentClient(KVStoreClient):
    """A :class:`KVStoreClient` extended with recruiter-focused convenience
    methods for storing candidate profiles and searching for matches.

    Parameters
    ----------
    base_url:
        Root URL of the running Next.js server, e.g. ``http://localhost:3000``.
    timeout:
        Per-request timeout in seconds (default: 10).

    Example
    -------
    >>> from kvstore_client.recruitment import RecruitmentClient
    >>> client = RecruitmentClient("http://localhost:3000")
    >>> client.store_candidate(
    ...     candidate_id="candidate:alice-smith",
    ...     name="Alice Smith",
    ...     headline="Senior ML Engineer",
    ...     experience_summary="10 years building Python data pipelines and LLM apps.",
    ...     skills=["Python", "PyTorch", "RAG", "LLMs"],
    ...     embedder=model,
    ... )
    >>> results = client.find_candidates(
    ...     "Looking for an ML engineer experienced with LLMs and Python",
    ...     embedder=model,
    ... )
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def store_candidate(
        self,
        candidate_id: str,
        name: str,
        headline: str,
        experience_summary: str,
        skills: list[str] | None = None,
        embedding: list[float] | None = None,
        embedder: Any = None,
        metadata: dict[str, Any] | None = None,
        ttl: int | None = None,
    ) -> bool:
        """Persist a candidate profile together with its dense embedding.

        The combined profile text used for embedding is::

            "<name> | <headline> | <experience_summary> | Skills: <skills>"

        Either supply a pre-computed *embedding* or an *embedder* object that
        has an ``encode(sentences)`` method (e.g. a ``SentenceTransformer``).

        Parameters
        ----------
        candidate_id:
            Unique key, e.g. ``"candidate:alice-smith"``.
        name:
            Full name of the candidate.
        headline:
            Short professional headline.
        experience_summary:
            Free-text description of the candidate's background.
        skills:
            Optional list of skill keywords.
        embedding:
            Pre-computed dense vector.  If ``None``, *embedder* must be given.
        embedder:
            Object with ``encode(sentences, show_progress_bar=False)`` method.
        metadata:
            Extra fields to persist alongside the profile (merged with the
            auto-generated ``name``, ``headline``, and ``skills`` fields).
        ttl:
            Optional time-to-live in milliseconds.

        Returns
        -------
        bool
            ``True`` if the store confirmed the write.
        """
        if embedding is None and embedder is None:
            raise ValueError("Provide either 'embedding' or 'embedder'.")

        profile_text = _build_profile_text(name, headline, experience_summary, skills)

        if embedding is None:
            embedding = embedder.encode([profile_text], show_progress_bar=False)[0].tolist()

        combined_metadata: dict[str, Any] = {
            "name": name,
            "headline": headline,
            "skills": skills or [],
        }
        if metadata:
            combined_metadata.update(metadata)

        return self.upsert(
            key=candidate_id,
            text=profile_text,
            embedding=embedding,
            metadata=combined_metadata,
            ttl=ttl,
        )

    def find_candidates(
        self,
        job_description: str,
        top_k: int = 5,
        required_skills: list[str] | None = None,
        embedder: Any = None,
        query_embedding: list[float] | None = None,
        key_prefix: str = "candidate:",
    ) -> list[dict[str, Any]]:
        """Return the *top_k* candidate profiles that best match *job_description*.

        Similarity is measured by cosine distance between the job-description
        embedding and each stored candidate's profile embedding.

        Parameters
        ----------
        job_description:
            Free-text description of the open role.
        top_k:
            Maximum number of results to return.
        required_skills:
            If given, only candidates whose stored ``skills`` metadata contains
            **all** of the listed items (case-insensitive) are returned.
        embedder:
            Object with ``encode(sentences, show_progress_bar=False)`` method.
        query_embedding:
            Pre-computed query vector.  If ``None``, *embedder* must be given.
        key_prefix:
            Only keys that begin with this string are considered as candidates
            (default ``"candidate:"``).

        Returns
        -------
        list[dict]
            Ordered by descending similarity score::

                [
                    {
                        "key": "candidate:alice-smith",
                        "score": 0.92,
                        "text": "...",
                        "metadata": {"name": "Alice Smith", "skills": [...]}
                    },
                    ...
                ]
        """
        if query_embedding is None and embedder is None:
            raise ValueError("Provide either 'query_embedding' or 'embedder'.")

        if query_embedding is None:
            query_embedding = embedder.encode(
                [job_description], show_progress_bar=False
            )[0].tolist()

        # Over-fetch when we need post-retrieval skill filtering so that the
        # final result list can still contain `top_k` items after filtering.
        # A 4× multiplier gives reasonable recall for most candidate pools
        # without fetching an unbounded number of records.
        _SKILL_FILTER_MULTIPLIER = 4
        fetch_k = top_k * _SKILL_FILTER_MULTIPLIER if required_skills else top_k
        results = self.query(
            embedding=query_embedding,
            top_k=fetch_k,
            key_prefix=key_prefix,
        )

        if required_skills:
            lc_required = [s.lower() for s in required_skills]
            filtered: list[dict[str, Any]] = []
            for r in results:
                candidate_skills = [
                    s.lower()
                    for s in (r.get("metadata") or {}).get("skills", [])
                ]
                if all(req in candidate_skills for req in lc_required):
                    filtered.append(r)
            results = filtered

        return results[:top_k]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _build_profile_text(
    name: str,
    headline: str,
    experience_summary: str,
    skills: list[str] | None,
) -> str:
    """Combine candidate fields into a single string for embedding."""
    parts = [name, headline, experience_summary]
    if skills:
        parts.append("Skills: " + ", ".join(skills))
    return " | ".join(parts)

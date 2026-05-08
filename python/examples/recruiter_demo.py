#!/usr/bin/env python3
"""Recruiter demo — candidate search with the kv-store RAG pipeline.

This script shows how a recruiter can use the kv-store vector API to:
  1. Ingest a pool of candidate profiles (name, headline, experience, skills).
  2. Embed each profile with a sentence-transformer model.
  3. Search for the best candidates for a given job description.
  4. Narrow results further by requiring specific skills.

Usage
-----
Install dependencies::

    pip install -r python/requirements.txt

Start the Next.js dev server::

    npm run dev          # http://localhost:3000

Run the demo::

    python python/examples/recruiter_demo.py

Point it at a deployed instance::

    BASE_URL=https://your-deployment.vercel.app python python/examples/recruiter_demo.py
"""

from __future__ import annotations

import os
import sys
import textwrap

# Make sure the local package is importable when running from the repo root.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from kvstore_client.recruitment import RecruitmentClient

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = os.environ.get("BASE_URL", "http://localhost:3000")

# ---------------------------------------------------------------------------
# Sample candidate pool
# ---------------------------------------------------------------------------

CANDIDATES = [
    {
        "id": "candidate:alice-smith",
        "name": "Alice Smith",
        "headline": "Senior Machine Learning Engineer",
        "experience": textwrap.dedent(
            """\
            8 years of experience designing and deploying large-scale ML systems.
            Led development of a real-time recommendation engine serving 50M users.
            Deep expertise in PyTorch, transformer fine-tuning, and MLOps pipelines.
            """
        ).strip(),
        "skills": ["Python", "PyTorch", "LLMs", "MLOps", "RAG", "Kubernetes"],
    },
    {
        "id": "candidate:bob-jones",
        "name": "Bob Jones",
        "headline": "Full-Stack Software Engineer",
        "experience": textwrap.dedent(
            """\
            6 years building web applications with React and Node.js.
            Delivered three SaaS products from zero to production.
            Comfortable with PostgreSQL, Redis, and REST API design.
            """
        ).strip(),
        "skills": ["JavaScript", "TypeScript", "React", "Node.js", "PostgreSQL", "Redis"],
    },
    {
        "id": "candidate:carol-white",
        "name": "Carol White",
        "headline": "Data Scientist & NLP Specialist",
        "experience": textwrap.dedent(
            """\
            5 years applying NLP techniques to enterprise data problems.
            Built a document-classification pipeline used by 200+ analysts.
            Published research on retrieval-augmented generation and semantic search.
            """
        ).strip(),
        "skills": ["Python", "NLP", "Hugging Face", "RAG", "spaCy", "SQL"],
    },
    {
        "id": "candidate:david-lee",
        "name": "David Lee",
        "headline": "DevOps / Site Reliability Engineer",
        "experience": textwrap.dedent(
            """\
            7 years managing cloud infrastructure across AWS and GCP.
            Reduced p99 latency by 40% through Kubernetes autoscaling improvements.
            Champion of observability: Prometheus, Grafana, and distributed tracing.
            """
        ).strip(),
        "skills": ["Kubernetes", "AWS", "GCP", "Terraform", "Python", "Go"],
    },
    {
        "id": "candidate:eve-chen",
        "name": "Eve Chen",
        "headline": "AI/ML Research Engineer",
        "experience": textwrap.dedent(
            """\
            4 years at the intersection of research and production ML.
            Developed retrieval-augmented generation prototypes for legal-tech clients.
            Hands-on with vector databases, embedding models, and LLM prompt engineering.
            """
        ).strip(),
        "skills": ["Python", "LLMs", "RAG", "Vector Databases", "PyTorch", "LangChain"],
    },
]

# ---------------------------------------------------------------------------
# Job description to search against
# ---------------------------------------------------------------------------

JOB_DESCRIPTION = textwrap.dedent(
    """\
    We are hiring a Senior AI Engineer to build and scale our Retrieval-Augmented
    Generation (RAG) platform. You will design embedding pipelines, fine-tune large
    language models, and work closely with product teams to deploy intelligent search
    and recommendation features. Strong Python skills and hands-on LLM experience
    are required.
    """
).strip()

REQUIRED_SKILLS = ["Python", "RAG", "LLMs"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def load_embedder():
    """Load a small sentence-transformer model for local embedding."""
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


MAX_DISPLAY_LENGTH = 120


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def main() -> None:
    client = RecruitmentClient(base_url=BASE_URL)
    model = load_embedder()

    # ---- 1. Ingest candidate profiles ----------------------------------------
    print(f"\n[demo] Storing {len(CANDIDATES)} candidate profiles to {BASE_URL} …")
    for c in CANDIDATES:
        ok = client.store_candidate(
            candidate_id=c["id"],
            name=c["name"],
            headline=c["headline"],
            experience_summary=c["experience"],
            skills=c["skills"],
            embedder=model,
            metadata={"source": "recruiter_demo"},
        )
        status = "✓" if ok else "✗"
        print(f"  {status} {c['id']}  ({c['headline']})")

    # ---- 2. Semantic search: best matches for the job description ------------
    print(f"\n[demo] Job description:\n  {JOB_DESCRIPTION[:MAX_DISPLAY_LENGTH]}…")
    print(f"\n[demo] Searching top-5 candidates (semantic similarity) …")

    top_matches = client.find_candidates(
        job_description=JOB_DESCRIPTION,
        top_k=5,
        embedder=model,
    )

    if not top_matches:
        print("[demo] No results — is the Next.js server running?")
        return

    print(f"\n[demo] Top-5 candidates (by cosine similarity):")
    for rank, r in enumerate(top_matches, start=1):
        meta = r.get("metadata", {})
        skills_str = ", ".join(meta.get("skills", []))
        print(
            f"  {rank}. [{r['score']:.4f}] {meta.get('name', r['key'])}"
            f"  —  {meta.get('headline', '')}"
        )
        print(f"         Skills: {skills_str}")

    # ---- 3. Filtered search: require specific skills -------------------------
    skills_label = ", ".join(REQUIRED_SKILLS)
    print(f"\n[demo] Filtered search — required skills: {skills_label}")

    filtered_matches = client.find_candidates(
        job_description=JOB_DESCRIPTION,
        top_k=3,
        required_skills=REQUIRED_SKILLS,
        embedder=model,
    )

    if not filtered_matches:
        print("[demo] No candidates matched all required skills.")
        return

    print(f"\n[demo] Candidates with all required skills ({skills_label}):")
    for rank, r in enumerate(filtered_matches, start=1):
        meta = r.get("metadata", {})
        skills_str = ", ".join(meta.get("skills", []))
        print(
            f"  {rank}. [{r['score']:.4f}] {meta.get('name', r['key'])}"
            f"  —  {meta.get('headline', '')}"
        )
        print(f"         Skills: {skills_str}")

    print("\n[demo] ── Done ──────────────────────────────────────────────────────")


if __name__ == "__main__":
    main()

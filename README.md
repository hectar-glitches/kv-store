# kv-store

A distributed, in-memory key-value store built with Next.js.  
Features: consistent hashing · LRU cache · replication · fault tolerance.

---

## Quick start (Next.js app)

```bash
npm install
npm run dev          # http://localhost:3000
```

---

## Python client & Mini RAG Pipeline

The `python/` directory contains a lightweight Python package that wraps the
kv-store REST API and a demo Retrieval-Augmented Generation (RAG) pipeline.

### Directory layout

```
python/
├── kvstore_client/       # importable Python package
│   ├── __init__.py
│   └── client.py         # KVStoreClient (upsert, query, …)
├── examples/
│   └── rag_demo.py       # end-to-end RAG demo
├── requirements.txt
└── setup.py
```

### Install

```bash
# from the repo root
pip install -r python/requirements.txt

# or install the package in editable mode (core only, no sentence-transformers)
pip install -e python/

# with the RAG extras (sentence-transformers + numpy)
pip install -e "python/[rag]"
```

### Using the client

```python
from kvstore_client import KVStoreClient

client = KVStoreClient(base_url="http://localhost:3000")

# Store a document chunk with its embedding
client.upsert(
    key="doc:1:chunk:0",
    text="Consistent hashing spreads data across nodes.",
    embedding=[0.1, 0.2, ...],          # list[float] from your embedder
    metadata={"source": "my_doc.txt"},
)

# Query – returns top-k chunks ordered by cosine similarity
results = client.query(embedding=[0.1, 0.2, ...], top_k=5)
for r in results:
    print(r["score"], r["text"])
```

The client also exposes low-level helpers that map 1-to-1 to the REST API:

| Method | API endpoint |
|---|---|
| `client._set(key, value)` | `POST /api/kvstore/set` |
| `client._get(key)` | `GET  /api/kvstore/get?key=…` |
| `client._delete(key)` | `DELETE /api/kvstore/delete` |
| `client._keys()` | `GET  /api/kvstore/keys` |

### Running the RAG demo

Make sure the Next.js server is running, then:

```bash
python python/examples/rag_demo.py
```

Against a deployed instance:

```bash
BASE_URL=https://your-deployment.vercel.app python python/examples/rag_demo.py
```

The demo will:
1. Split a sample document into small chunks.
2. Embed each chunk with `all-MiniLM-L6-v2` (downloaded automatically on
   first run, ~90 MB).
3. Upsert every chunk + embedding to the kv-store via the REST API.
4. Embed a query string and run a top-3 similarity search.
5. Print the retrieved contexts as a trivial "answer".

Example output:

```
[demo] Loading embedding model (all-MiniLM-L6-v2) …
[demo] Upserting 5 chunks to http://localhost:3000 …
  ✓ rag:demo:chunk:0
  ✓ rag:demo:chunk:1
  …
[demo] Query: "How does consistent hashing help with distributed data?"

[demo] Top-3 contexts (by cosine similarity):
  [0.8821] rag:demo:chunk:1
    Consistent hashing is a technique that allows a distributed key-value …
  …

[demo] ── Assembled answer (proof-of-connection) ──────────────────
Consistent hashing is a technique …
──────────────────────────────────────────────────────────────────────
```

---

## API reference

| Endpoint | Method | Body / Params | Description |
|---|---|---|---|
| `/api/kvstore/set` | POST | `{ key, value, ttl? }` | Set a key |
| `/api/kvstore/get` | GET | `?key=…` | Get a value |
| `/api/kvstore/delete` | DELETE | `{ key }` | Delete a key |
| `/api/kvstore/keys` | GET | — | List all keys |
| `/api/kvstore/stats` | GET | — | Store statistics |
| `/api/kvstore/nodes` | GET | — | Node information |

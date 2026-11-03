# kv-store

A Next.js application that demonstrates two in-memory stores:

- A key-value store with a REST API, an LRU cache, consistent-hash sharding, and simulated replication and fault-tolerance controls.
- A separate single-collection vector store with a REST API for upserting and querying vectors by cosine similarity.

The node, replication, and fault-tolerance features are simulations within one server process. The stores are held in memory; data is not restored after a server restart. This project is a demo, not a production distributed database.

## Run the app

Requirements: Node.js and npm.

```bash
npm install
npm run dev
```

Open [http://localhost:3000](http://localhost:3000). To create a production build and run it:

```bash
npm run build
npm start
```

## Key-value API

All key-value endpoints use the `/api/kvstore` prefix. Values are strings; `ttl`, when supplied to `set`, is a duration in milliseconds.

| Method | Endpoint | Request | Result |
|---|---|---|---|
| `POST` | `/api/kvstore/set` | JSON `{ "key": "greeting", "value": "hello", "ttl": 60000 }` (`ttl` optional) | `{ "success": true, "message": "Key set successfully" }` |
| `GET` | `/api/kvstore/get?key=greeting` | `key` query parameter | `{ "success": true, "value": "hello", "found": true }` |
| `DELETE` | `/api/kvstore/delete` | JSON `{ "key": "greeting" }` | `{ "success": true, "message": "Key deleted successfully" }` |
| `GET` | `/api/kvstore/keys` | — | `{ "success": true, "keys": [...], "count": 1 }` |
| `GET` | `/api/kvstore/stats` | — | Store, cache, sharding, replication, and fault-tolerance statistics |
| `GET` | `/api/kvstore/nodes` | — | Configured node information |
| `POST` | `/api/kvstore/nodes` | JSON `{ "nodeId": "node-4", "host": "localhost", "port": 6382 }` | Adds a simulated node |
| `DELETE` | `/api/kvstore/nodes` | JSON `{ "nodeId": "node-4" }` | Removes a simulated node |

For example:

```bash
curl -X POST http://localhost:3000/api/kvstore/set \
  -H 'Content-Type: application/json' \
  -d '{"key":"greeting","value":"hello"}'

curl 'http://localhost:3000/api/kvstore/get?key=greeting'
```

## Vector API

The vector API is separate from the key-value API and uses one in-memory collection. Vectors in a collection must have the same dimension. Query filters match exact, top-level metadata fields.

| Method | Endpoint | Request / result |
|---|---|---|
| `POST` | `/api/v1/upsert` | Request: `{ "vectors": [{ "id": "item-1", "values": [0.1, 0.2], "metadata": { "category": "example" } }] }`; result: `{ "upsertedCount": 1 }` |
| `POST` | `/api/v1/query` | Request: `{ "vector": [0.1, 0.2], "topK": 10, "filter": { "category": "example" }, "includeMetadata": true, "includeValues": false }`; result: `{ "matches": [{ "id": "item-1", "score": 1, "metadata": { "category": "example" } }] }` |
| `POST` | `/api/v1/delete` | Request: `{ "ids": ["item-1"] }`; result: `{ "deleted": 1 }` |
| `GET` | `/api/v1/stats` | Vector count and dimension information |

`topK` defaults to `10`, `includeMetadata` to `true`, and `includeValues` to `false`. Similarity scores use cosine similarity.

## Python client

The optional `python/` package wraps the key-value API and provides helpers for storing and searching embedded text chunks. Its similarity query fetches the key list and reads values through the key-value API; it is not backed by the `/api/v1` vector store.

Install the client and RAG demo dependencies from the repository root:

```bash
python -m pip install -r python/requirements.txt
```

Or install only the client (with `requests`) and add the RAG dependencies when needed:

```bash
python -m pip install -e python/
python -m pip install -e "python/[rag]"
```

Example client use:

```python
from kvstore_client import KVStoreClient

client = KVStoreClient(base_url="http://localhost:3000")
client.upsert(
    key="doc:1:chunk:0",
    text="Consistent hashing distributes keys across nodes.",
    embedding=[0.1, 0.2, 0.3],
    metadata={"source": "example"},
)

results = client.query(embedding=[0.1, 0.2, 0.3], top_k=5)
for result in results:
    print(result["score"], result["text"])
```

Run the end-to-end demo while the Next.js server is running:

```bash
python python/examples/rag_demo.py
```

The demo loads `all-MiniLM-L6-v2` through `sentence-transformers`, splits a sample document into chunks, stores embeddings through the key-value API, and retrieves the three most similar chunks. The model is downloaded on first use. To target another server:

```bash
BASE_URL=https://your-deployment.example python python/examples/rag_demo.py
```

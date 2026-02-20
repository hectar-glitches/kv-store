#!/usr/bin/env ts-node
/**
 * Benchmark script for the vector store.
 * Usage:  npx ts-node scripts/bench.ts [n] [dim]
 *   n   – number of vectors  (default 10 000)
 *   dim – vector dimension   (default 128)
 */

import { VectorEngine } from "../lib/kv/index"
import { cosineSimilarity } from "../lib/kv/metrics"

const N = parseInt(process.argv[2] ?? "10000", 10)
const DIM = parseInt(process.argv[3] ?? "128", 10)

function randomVec(dim: number): number[] {
  return Array.from({ length: dim }, () => Math.random() * 2 - 1)
}

function percentile(sorted: number[], p: number): number {
  const idx = Math.ceil((p / 100) * sorted.length) - 1
  return sorted[Math.max(0, idx)]
}

async function main() {
  console.log(`\n=== KV-Store Vector Bench ===`)
  console.log(`Vectors: ${N.toLocaleString()}  Dimension: ${DIM}\n`)

  const engine = new VectorEngine("/tmp/bench-data", "bench")
  await engine.open()

  // ── Ingest ──────────────────────────────────────────────────────────────
  const vectors: number[][] = Array.from({ length: N }, () => randomVec(DIM))
  const ingestStart = Date.now()

  for (let i = 0; i < N; i++) {
    await engine.upsert({ id: `vec-${i}`, vector: vectors[i] })
  }

  const ingestMs = Date.now() - ingestStart
  const qps = Math.round((N / ingestMs) * 1000)
  console.log(`Ingest: ${ingestMs} ms  (${qps} vec/s)`)

  // ── Query latency ────────────────────────────────────────────────────────
  const QUERIES = 200
  const latencies: number[] = []
  const recallHits: number[] = []

  for (let q = 0; q < QUERIES; q++) {
    const queryVec = randomVec(DIM)
    const t0 = performance.now()
    const results = engine.query({ vector: queryVec, topK: 10, nprobe: 16 })
    latencies.push(performance.now() - t0)

    // Recall@10: compare against brute-force top-10
    const bruteForceDists = vectors
      .map((v, i) => ({ id: `vec-${i}`, score: cosineSimilarity(queryVec, v) }))
      .sort((a, b) => b.score - a.score)
      .slice(0, 10)
      .map((r) => r.id)

    const resultIds = new Set(results.map((r) => r.id))
    const hits = bruteForceDists.filter((id) => resultIds.has(id)).length
    recallHits.push(hits / 10)
  }

  latencies.sort((a, b) => a - b)
  const avgLatency = latencies.reduce((s, v) => s + v, 0) / latencies.length
  const p95 = percentile(latencies, 95)
  const p99 = percentile(latencies, 99)
  const recall = recallHits.reduce((s, v) => s + v, 0) / recallHits.length

  console.log(`\nQuery (${QUERIES} queries, topK=10):`)
  console.log(`  avg: ${avgLatency.toFixed(2)} ms`)
  console.log(`  p95: ${p95.toFixed(2)} ms`)
  console.log(`  p99: ${p99.toFixed(2)} ms`)
  console.log(`  recall@10: ${(recall * 100).toFixed(1)}%`)

  const stats = engine.getStats()
  console.log(`\nStore stats:`)
  console.log(`  totalRecords: ${stats.totalRecords}`)
  console.log(`  segments: ${stats.segments}`)
  console.log(`  ivfTrained: ${stats.ivfTrained}  centroids: ${stats.ivfCentroids}`)

  await engine.close()
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})

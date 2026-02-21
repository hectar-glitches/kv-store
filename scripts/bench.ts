#!/usr/bin/env node
/**
 * scripts/bench.ts — Vector Store Benchmark
 *
 * Benchmarks:
 *   - Upsert throughput  (vectors / second)
 *   - Query latency      p50 / p95 / p99  (brute-force & IVF)
 *   - Recall@k           IVF vs. brute-force ground truth
 *
 * Usage:
 *   node scripts/bench.ts [options]
 *   npm run bench -- [options]
 *
 * Options:
 *   --vectors <n>   Vectors to ingest       (default: 10 000)
 *   --dim     <n>   Embedding dimension     (default: 128)
 *   --k       <n>   Nearest neighbours      (default: 10)
 *   --queries <n>   Query vectors           (default: 200)
 *   --nlist   <n>   IVF cluster count       (default: ⌈√vectors⌉)
 *   --nprobe  <n>   IVF clusters to probe   (default: max(1, nlist/10))
 *   --ci            CI mode: 1 000 × 64, fast sanity-check
 *   --primary       Primary benchmark: 1 000 000 × 768 (~6 GB RAM)
 *   --scale         Scale benchmark: 10 000 000 × 128 (~50 GB RAM)
 */

import { parseArgs } from "node:util"
import { performance } from "node:perf_hooks"

// ============================================================
// CLI
// ============================================================

const { values: flags } = parseArgs({
  options: {
    vectors: { type: "string", default: "10000" },
    dim:     { type: "string", default: "128"   },
    k:       { type: "string", default: "10"    },
    queries: { type: "string", default: "200"   },
    nlist:   { type: "string"                   },
    nprobe:  { type: "string"                   },
    ci:      { type: "boolean", default: false  },
    primary: { type: "boolean", default: false  },
    scale:   { type: "boolean", default: false  },
  },
  strict: false,
})

interface BenchConfig {
  numVectors: number
  dim:        number
  k:          number
  numQueries: number
  nlist:      number
  nprobe:     number
}

function getConfig(): BenchConfig {
  if (flags.scale) {
    console.warn("⚠️  Scale mode (10 M × 128) requires ~50 GB RAM and may take several minutes.")
    return { numVectors: 10_000_000, dim: 128, k: 10, numQueries: 100, nlist: 3162, nprobe: 32 }
  }
  if (flags.primary) {
    console.warn("⚠️  Primary mode (1 M × 768) requires ~6 GB RAM and may take several minutes.")
    return { numVectors: 1_000_000, dim: 768, k: 10, numQueries: 100, nlist: 1000, nprobe: 20 }
  }
  if (flags.ci) {
    return { numVectors: 1_000, dim: 64, k: 10, numQueries: 50, nlist: 32, nprobe: 4 }
  }
  const numVectors = parseInt(flags.vectors as string, 10)
  const dim        = parseInt(flags.dim     as string, 10)
  const k          = parseInt(flags.k       as string, 10)
  const numQueries = parseInt(flags.queries as string, 10)
  const nlist  = flags.nlist
    ? parseInt(flags.nlist  as string, 10)
    : Math.max(1, Math.ceil(Math.sqrt(numVectors)))
  const nprobe = flags.nprobe
    ? parseInt(flags.nprobe as string, 10)
    : Math.max(1, Math.ceil(nlist / 10))
  return { numVectors, dim, k, numQueries, nlist, nprobe }
}

// ============================================================
// Vector utilities
// ============================================================

/** Returns a stateful Box-Muller Gaussian sampler. */
function makeRandnSampler(): () => number {
  let spare: number | null = null
  return function randn(): number {
    if (spare !== null) { const z = spare; spare = null; return z }
    const u1  = Math.random() || 1e-10
    const u2  = Math.random()
    const mag = Math.sqrt(-2 * Math.log(u1))
    spare = mag * Math.sin(2 * Math.PI * u2)
    return   mag * Math.cos(2 * Math.PI * u2)
  }
}
const randn = makeRandnSampler()

/** Random unit vector sampled uniformly from the unit hypersphere. */
function randomUnitVector(dim: number): Float32Array {
  const v = new Float32Array(dim)
  let sumSq = 0
  for (let i = 0; i < dim; i++) { v[i] = randn(); sumSq += v[i] * v[i] }
  const norm = Math.sqrt(sumSq) || 1
  for (let i = 0; i < dim; i++) v[i] /= norm
  return v
}

function dotProduct(a: Float32Array, b: Float32Array): number {
  let s = 0
  for (let i = 0; i < a.length; i++) s += a[i] * b[i]
  return s
}

function percentile(sorted: number[], p: number): number {
  const idx = Math.floor((p / 100) * (sorted.length - 1))
  return sorted[Math.max(0, Math.min(sorted.length - 1, idx))]
}

function shuffleIndices(n: number): number[] {
  const arr = Array.from({ length: n }, (_, i) => i)
  for (let i = n - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]]
  }
  return arr
}

// ============================================================
// In-memory vector store  (backed by the KV-store pattern)
// ============================================================

class VectorStore {
  /** Flat storage: vectors[i] is the embedding for id ids[i]. */
  private readonly vecs: Float32Array[] = []
  private readonly ids:  number[]       = []

  upsert(id: number, vec: Float32Array): void {
    this.ids.push(id)
    this.vecs.push(vec)
  }

  size(): number { return this.vecs.length }

  vec(i: number): Float32Array { return this.vecs[i] }
  id(i:  number): number       { return this.ids[i]  }
  all(): Float32Array[]        { return this.vecs     }
}

// ============================================================
// Brute-force exact k-NN
// ============================================================

function bruteForceKNN(store: VectorStore, query: Float32Array, k: number): number[] {
  const n = store.size()
  // Use a simple partial sort: collect all scores then sort top-k.
  const scores = new Float64Array(n)
  for (let i = 0; i < n; i++) scores[i] = dotProduct(store.vec(i), query)

  // Partial selection of top-k (faster than full sort for large n)
  const indices = Array.from({ length: n }, (_, i) => i)
  indices.sort((a, b) => scores[b] - scores[a])
  return indices.slice(0, k).map(i => store.id(i))
}

// ============================================================
// IVF (Inverted File Index) — approximate k-NN
// ============================================================

interface IVFEntry { id: number; vec: Float32Array }

class IVFIndex {
  private centroids: Float32Array[]           = []
  private lists:     Map<number, IVFEntry[]>  = new Map()
  private readonly nprobe: number

  constructor(nprobe: number) { this.nprobe = nprobe }

  /**
   * Build the index via k-means clustering.
   * For large datasets a random subsample is used for centroid estimation
   * to keep build time manageable.
   */
  build(store: VectorStore, nlist: number, maxIter = 20): void {
    const n    = store.size()
    const dim  = store.vec(0).length
    nlist      = Math.min(nlist, n)

    // ---- K-means init: random centroids from a subsample ----
    const maxSample = Math.min(n, 50_000)
    const sampleIdx = shuffleIndices(n).slice(0, maxSample)

    const shuffled = shuffleIndices(sampleIdx.length)
    this.centroids = shuffled.slice(0, nlist).map(si => Float32Array.from(store.vec(sampleIdx[si])))

    // ---- Lloyd iterations on the subsample ----
    const assignments = new Int32Array(maxSample)
    for (let iter = 0; iter < maxIter; iter++) {
      let changed = 0
      for (let si = 0; si < maxSample; si++) {
        const vec = store.vec(sampleIdx[si])
        let best = 0, bestScore = -Infinity
        for (let c = 0; c < nlist; c++) {
          const score = dotProduct(vec, this.centroids[c])
          if (score > bestScore) { bestScore = score; best = c }
        }
        if (assignments[si] !== best) { assignments[si] = best; changed++ }
      }

      // Update centroids as the normalised mean of assigned vectors
      const sums   = Array.from({ length: nlist }, () => new Float32Array(dim))
      const counts = new Int32Array(nlist)
      for (let si = 0; si < maxSample; si++) {
        const c   = assignments[si]
        const vec = store.vec(sampleIdx[si])
        for (let d = 0; d < dim; d++) sums[c][d] += vec[d]
        counts[c]++
      }
      for (let c = 0; c < nlist; c++) {
        if (counts[c] === 0) continue
        let sumSq = 0
        for (let d = 0; d < dim; d++) { sums[c][d] /= counts[c]; sumSq += sums[c][d] * sums[c][d] }
        const norm = Math.sqrt(sumSq) || 1
        for (let d = 0; d < dim; d++) sums[c][d] /= norm
        this.centroids[c] = sums[c]
      }

      if (changed === 0) break
    }

    // ---- Assign all n vectors to their nearest centroid ----
    for (let c = 0; c < nlist; c++) this.lists.set(c, [])
    for (let i = 0; i < n; i++) {
      const vec = store.vec(i)
      let best = 0, bestScore = -Infinity
      for (let c = 0; c < nlist; c++) {
        const score = dotProduct(vec, this.centroids[c])
        if (score > bestScore) { bestScore = score; best = c }
      }
      this.lists.get(best)!.push({ id: store.id(i), vec })
    }
  }

  search(query: Float32Array, k: number): number[] {
    // Rank centroids by dot product and probe the top nprobe lists
    const centroidScores: [number, number][] = this.centroids.map((c, i) => [dotProduct(c, query), i])
    centroidScores.sort((a, b) => b[0] - a[0])

    const candidates: [number, number][] = []
    const nprobe = Math.min(this.nprobe, this.centroids.length)
    for (let pi = 0; pi < nprobe; pi++) {
      const ci   = centroidScores[pi][1]
      const list = this.lists.get(ci) ?? []
      for (const { id, vec } of list) {
        candidates.push([dotProduct(vec, query), id])
      }
    }
    candidates.sort((a, b) => b[0] - a[0])
    return candidates.slice(0, k).map(([, id]) => id)
  }
}

// ============================================================
// Benchmark helpers
// ============================================================

interface LatencyStats { p50: number; p95: number; p99: number; mean: number }

function measureLatencies(fns: Array<() => void>): LatencyStats {
  const times: number[] = []
  for (const fn of fns) {
    const t0 = performance.now()
    fn()
    times.push(performance.now() - t0)
  }
  times.sort((a, b) => a - b)
  const mean = times.reduce((s, t) => s + t, 0) / times.length
  return { p50: percentile(times, 50), p95: percentile(times, 95), p99: percentile(times, 99), mean }
}

function computeRecall(groundTruth: number[][], approx: number[][]): number {
  let hits = 0, total = 0
  for (let i = 0; i < groundTruth.length; i++) {
    const gt = new Set(groundTruth[i])
    for (const id of approx[i]) { if (gt.has(id)) hits++ }
    total += groundTruth[i].length
  }
  return total === 0 ? 0 : hits / total
}

// ============================================================
// Table printer
// ============================================================

function printTable(headers: string[], rows: string[][]): void {
  const widths = headers.map((h, i) => Math.max(h.length, ...rows.map(r => (r[i] ?? "").length)))
  const line   = "+-" + widths.map(w => "-".repeat(w)).join("-+-") + "-+"
  const fmt    = (cells: string[]) => "| " + cells.map((c, i) => c.padEnd(widths[i])).join(" | ") + " |"
  console.log(line)
  console.log(fmt(headers))
  console.log(line)
  for (const row of rows) console.log(fmt(row))
  console.log(line)
}

// ============================================================
// Main
// ============================================================

async function main(): Promise<void> {
  const cfg = getConfig()

  console.log("\n🚀  Vector Store Benchmark")
  console.log(`   Vectors : ${cfg.numVectors.toLocaleString()}`)
  console.log(`   Dim     : ${cfg.dim}`)
  console.log(`   k       : ${cfg.k}`)
  console.log(`   Queries : ${cfg.numQueries}`)
  console.log(`   nlist   : ${cfg.nlist}   nprobe: ${cfg.nprobe}`)
  console.log()

  // ── 1. Ingest ─────────────────────────────────────────────
  process.stdout.write("Generating & ingesting vectors ... ")
  const store   = new VectorStore()
  const t0      = performance.now()
  for (let i = 0; i < cfg.numVectors; i++) store.upsert(i, randomUnitVector(cfg.dim))
  const ingestMs        = performance.now() - t0
  const upsertPerSecond = Math.round(cfg.numVectors / (ingestMs / 1000))
  console.log(`done  (${ingestMs.toFixed(0)} ms)`)

  // ── 2. Build IVF index ────────────────────────────────────
  process.stdout.write("Building IVF index            ... ")
  const tBuild = performance.now()
  const ivf    = new IVFIndex(cfg.nprobe)
  ivf.build(store, cfg.nlist)
  const buildMs = performance.now() - tBuild
  console.log(`done  (${buildMs.toFixed(0)} ms)`)

  // ── 3. Query vectors ──────────────────────────────────────
  const queries = Array.from({ length: cfg.numQueries }, () => randomUnitVector(cfg.dim))

  // ── 4. Ground truth via brute-force ───────────────────────
  process.stdout.write("Computing brute-force results ... ")
  const tGT       = performance.now()
  const groundTruth = queries.map(q => bruteForceKNN(store, q, cfg.k))
  const gtMs      = performance.now() - tGT
  console.log(`done  (${gtMs.toFixed(0)} ms)`)

  // ── 5. IVF results ────────────────────────────────────────
  const ivfResults = queries.map(q => ivf.search(q, cfg.k))

  // ── 6. Latency measurements ───────────────────────────────
  const bfLatency  = measureLatencies(queries.map(q => () => bruteForceKNN(store, q, cfg.k)))
  const ivfLatency = measureLatencies(queries.map(q => () => ivf.search(q, cfg.k)))

  // ── 7. Recall@k ───────────────────────────────────────────
  const recall = computeRecall(groundTruth, ivfResults)

  // ── 8. Print results ──────────────────────────────────────
  console.log("\n📊  Results\n")

  console.log("Ingest")
  printTable(
    ["Metric", "Value"],
    [
      ["Vectors ingested",   cfg.numVectors.toLocaleString()],
      ["Dimensions",         String(cfg.dim)                ],
      ["Ingest time (ms)",   ingestMs.toFixed(0)            ],
      ["Throughput (vec/s)", upsertPerSecond.toLocaleString()],
    ],
  )

  console.log("\nQuery Latency (ms)")
  printTable(
    ["Method",                               "p50",                    "p95",                    "p99",                    "Mean"                  ],
    [
      ["Brute-force",                         bfLatency.p50.toFixed(3), bfLatency.p95.toFixed(3), bfLatency.p99.toFixed(3), bfLatency.mean.toFixed(3)],
      [`IVF (nlist=${cfg.nlist}, nprobe=${cfg.nprobe})`, ivfLatency.p50.toFixed(3), ivfLatency.p95.toFixed(3), ivfLatency.p99.toFixed(3), ivfLatency.mean.toFixed(3)],
    ],
  )

  console.log("\nRecall")
  printTable(
    ["Metric",            "Value"                          ],
    [
      ["k",               String(cfg.k)                   ],
      ["Query count",     String(cfg.numQueries)           ],
      [`Recall@${cfg.k}`, `${(recall * 100).toFixed(1)}%` ],
    ],
  )

  console.log()
}

main().catch(err => { console.error(err); process.exit(1) })

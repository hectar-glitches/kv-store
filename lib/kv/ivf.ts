// IVF-Flat (Inverted File Index – Flat) Approximate Nearest Neighbor Search
//
// Build steps:
//   1. Train k-means centroids over a representative sample of vectors.
//   2. Assign every indexed vector to its nearest centroid (posting list).
//
// Query steps:
//   1. Score all centroids against the query; keep the top-nprobe.
//   2. Scan each selected posting list with exact similarity.
//   3. Return the globally top-k results.

import {
  type SimilarityMetric,
  score,
  l2DistanceSq,
  cosineSimilarity,
  dotProduct,
} from "./metrics"

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

export interface IVFConfig {
  /** Number of Voronoi cells (centroids). Default: 256. */
  nlist?: number
  /** Number of cells probed at query time. Default: 8. */
  nprobe?: number
  /** Similarity metric used for scoring. Default: "l2". */
  metric?: SimilarityMetric
  /** Max k-means iterations during training. Default: 25. */
  maxIter?: number
}

export interface SearchResult {
  id: string
  score: number
}

export interface IVFStatus {
  trained: boolean
  nlist: number
  nprobe: number
  metric: SimilarityMetric
  dim: number | null
  totalVectors: number
  postingListSizes: number[]
}

/** Serialisable snapshot produced by {@link IVFFlatIndex.serialize}. */
export interface IVFSnapshot {
  nlist: number
  nprobe: number
  metric: SimilarityMetric
  dim: number
  centroids: number[][]
  postingLists: Array<Array<{ id: string; vector: number[] }>>
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/** Pick `k` distinct random indices from [0, n). */
function randomSample(n: number, k: number): number[] {
  if (k >= n) return Array.from({ length: n }, (_, i) => i)
  const indices = new Set<number>()
  while (indices.size < k) {
    indices.add(Math.floor(Math.random() * n))
  }
  return Array.from(indices)
}

/** Find the index of the nearest centroid for a given vector. */
function nearestCentroid(
  vec: Float32Array,
  centroids: Float32Array[],
  metric: SimilarityMetric,
): number {
  let bestIdx = 0
  let bestScore = score(vec, centroids[0], metric)
  for (let i = 1; i < centroids.length; i++) {
    const s = score(vec, centroids[i], metric)
    if (s > bestScore) {
      bestScore = s
      bestIdx = i
    }
  }
  return bestIdx
}

/** Element-wise mean of a list of vectors (in-place into `out`). */
function computeMean(
  vectors: Float32Array[],
  out: Float32Array,
): void {
  out.fill(0)
  for (const v of vectors) {
    for (let d = 0; d < v.length; d++) {
      out[d] += v[d]
    }
  }
  for (let d = 0; d < out.length; d++) {
    out[d] /= vectors.length
  }
}

// ---------------------------------------------------------------------------
// IVFFlatIndex
// ---------------------------------------------------------------------------

export class IVFFlatIndex {
  private readonly nlist: number
  private readonly nprobe: number
  private readonly metric: SimilarityMetric
  private readonly maxIter: number

  private dim: number | null = null
  private centroids: Float32Array[] = []
  private postingLists: Array<Map<string, Float32Array>> = []
  private trained = false

  constructor(config: IVFConfig = {}) {
    this.nlist = config.nlist ?? 256
    this.nprobe = Math.min(config.nprobe ?? 8, this.nlist)
    this.metric = config.metric ?? "l2"
    this.maxIter = config.maxIter ?? 25
  }

  // -------------------------------------------------------------------------
  // Training (k-means)
  // -------------------------------------------------------------------------

  /**
   * Train the index by clustering a sample of vectors into `nlist` centroids.
   *
   * @param vectors - Training set.  May be a subset of the full corpus.
   */
  train(vectors: Float32Array[]): void {
    if (vectors.length === 0) throw new Error("Training set must not be empty")

    const dim = vectors[0].length
    if (vectors.some((v) => v.length !== dim)) {
      throw new Error("All training vectors must have the same dimension")
    }

    this.dim = dim

    // If we have fewer vectors than cells, reduce nlist to avoid empty centroids
    const k = Math.min(this.nlist, vectors.length)

    // Initialise centroids with k-means++ style (random, distinct vectors)
    const seedIndices = randomSample(vectors.length, k)
    let centroids: Float32Array[] = seedIndices.map(
      (i) => new Float32Array(vectors[i]),
    )

    for (let iter = 0; iter < this.maxIter; iter++) {
      // Assignment step
      const clusters: Float32Array[][] = Array.from({ length: k }, () => [])
      for (const v of vectors) {
        const idx = nearestCentroid(v, centroids, this.metric)
        clusters[idx].push(v)
      }

      // Update step — recompute each centroid as mean of its cluster
      let changed = false
      const newCentroids: Float32Array[] = []
      for (let c = 0; c < k; c++) {
        if (clusters[c].length === 0) {
          // Empty cluster: keep the old centroid
          newCentroids.push(centroids[c])
          continue
        }
        const mean = new Float32Array(dim)
        computeMean(clusters[c], mean)
        // Check for convergence (L2 distance between old and new centroid)
        if (l2DistanceSq(centroids[c], mean) > 1e-10) changed = true
        newCentroids.push(mean)
      }

      centroids = newCentroids
      if (!changed) break
    }

    this.centroids = centroids
    // Re-initialise posting lists (preserves any previously added vectors)
    const prevLists = this.postingLists
    this.postingLists = Array.from({ length: k }, () => new Map())

    if (prevLists.length > 0) {
      // Re-assign existing vectors to new centroids
      for (const list of prevLists) {
        for (const [id, vec] of list) {
          const idx = nearestCentroid(vec, this.centroids, this.metric)
          this.postingLists[idx].set(id, vec)
        }
      }
    }

    this.trained = true
  }

  // -------------------------------------------------------------------------
  // Indexing
  // -------------------------------------------------------------------------

  /**
   * Add a single vector to the index.
   * The index must be trained before vectors can be added.
   */
  add(id: string, vector: Float32Array): void {
    this.assertTrained()
    this.assertDim(vector)

    const idx = nearestCentroid(vector, this.centroids, this.metric)
    this.postingLists[idx].set(id, vector)
  }

  /**
   * Add multiple vectors at once.  Vectors are provided as a plain object
   * map from id → Float32Array for ergonomic use from callers.
   */
  addBatch(entries: Record<string, Float32Array>): void {
    for (const [id, vec] of Object.entries(entries)) {
      this.add(id, vec)
    }
  }

  /** Remove a vector from the index by id. */
  remove(id: string): boolean {
    for (const list of this.postingLists) {
      if (list.delete(id)) return true
    }
    return false
  }

  // -------------------------------------------------------------------------
  // Search
  // -------------------------------------------------------------------------

  /**
   * Approximate nearest-neighbour search.
   *
   * @param query   - Query vector.
   * @param k       - Number of results to return (default: 10).
   * @param nprobe  - Override the instance-level nprobe for this query.
   * @returns Sorted list of {id, score} pairs (best first).
   */
  search(
    query: Float32Array,
    k = 10,
    nprobe?: number,
  ): SearchResult[] {
    this.assertTrained()
    this.assertDim(query)

    const probeCount = Math.min(nprobe ?? this.nprobe, this.centroids.length)

    // Score all centroids and pick the top-probeCount ones
    const centroidScores = this.centroids.map((c, i) => ({
      idx: i,
      s: score(query, c, this.metric),
    }))
    centroidScores.sort((a, b) => b.s - a.s)

    // Scan selected posting lists
    const candidates: SearchResult[] = []
    for (let p = 0; p < probeCount; p++) {
      const listIdx = centroidScores[p].idx
      for (const [id, vec] of this.postingLists[listIdx]) {
        candidates.push({ id, score: score(query, vec, this.metric) })
      }
    }

    // Return top-k globally
    candidates.sort((a, b) => b.score - a.score)
    return candidates.slice(0, k)
  }

  /**
   * Exact (brute-force) search over all indexed vectors.
   * Useful as a recall baseline.
   */
  bruteForceSearch(query: Float32Array, k = 10): SearchResult[] {
    this.assertDim(query)

    const results: SearchResult[] = []
    for (const list of this.postingLists) {
      for (const [id, vec] of list) {
        results.push({ id, score: score(query, vec, this.metric) })
      }
    }

    results.sort((a, b) => b.score - a.score)
    return results.slice(0, k)
  }

  // -------------------------------------------------------------------------
  // Re-train hook
  // -------------------------------------------------------------------------

  /**
   * Re-train hook: train the index on a fresh set of training vectors, then
   * re-assign all currently indexed vectors to the new centroids.
   *
   * Use this when the data distribution has drifted significantly.
   * Distinct from {@link train} in that it explicitly signals an in-place
   * refresh of an already-serving index rather than initial construction.
   */
  retrain(vectors: Float32Array[]): void {
    this.train(vectors)
  }

  // -------------------------------------------------------------------------
  // Status / diagnosis
  // -------------------------------------------------------------------------

  /** Returns a diagnostic snapshot of the current index state. */
  status(): IVFStatus {
    const sizes = this.postingLists.map((l) => l.size)
    const totalVectors = sizes.reduce((a, b) => a + b, 0)
    return {
      trained: this.trained,
      nlist: this.centroids.length,
      nprobe: this.nprobe,
      metric: this.metric,
      dim: this.dim,
      totalVectors,
      postingListSizes: sizes,
    }
  }

  // -------------------------------------------------------------------------
  // Serialisation
  // -------------------------------------------------------------------------

  /**
   * Export the complete index state as a plain-object snapshot that can be
   * JSON-stringified and stored per segment.
   */
  serialize(): IVFSnapshot {
    this.assertTrained()
    return {
      nlist: this.centroids.length,
      nprobe: this.nprobe,
      metric: this.metric,
      dim: this.dim!,
      centroids: this.centroids.map((c) => Array.from(c)),
      postingLists: this.postingLists.map((list) =>
        Array.from(list.entries()).map(([id, vec]) => ({
          id,
          vector: Array.from(vec),
        })),
      ),
    }
  }

  /**
   * Restore an index from a previously serialised snapshot.
   * Returns a fully trained and populated IVFFlatIndex.
   */
  static deserialize(snapshot: IVFSnapshot): IVFFlatIndex {
    const idx = new IVFFlatIndex({
      nlist: snapshot.nlist,
      nprobe: snapshot.nprobe,
      metric: snapshot.metric,
    })
    idx.dim = snapshot.dim
    idx.centroids = snapshot.centroids.map((c) => new Float32Array(c))
    idx.postingLists = snapshot.postingLists.map((list) => {
      const map = new Map<string, Float32Array>()
      for (const entry of list) {
        map.set(entry.id, new Float32Array(entry.vector))
      }
      return map
    })
    idx.trained = true
    return idx
  }

  // -------------------------------------------------------------------------
  // Benchmarking
  // -------------------------------------------------------------------------

  /**
   * Benchmark the IVF index against brute-force search.
   *
   * @param queries    - Array of query vectors.
   * @param k          - Top-k for both methods.
   * @param nprobe     - nprobe override for this benchmark run.
   * @returns Per-query recall (fraction of true top-k found by IVF) and
   *          mean latency in milliseconds for both methods.
   */
  benchmark(
    queries: Float32Array[],
    k = 10,
    nprobe?: number,
  ): BenchmarkResult {
    this.assertTrained()

    let totalRecall = 0
    let ivfMs = 0
    let bfMs = 0

    for (const q of queries) {
      const t0 = performance.now()
      const ivfResults = this.search(q, k, nprobe)
      ivfMs += performance.now() - t0

      const t1 = performance.now()
      const bfResults = this.bruteForceSearch(q, k)
      bfMs += performance.now() - t1

      const trueSet = new Set(bfResults.map((r) => r.id))
      const hits = ivfResults.filter((r) => trueSet.has(r.id)).length
      totalRecall += hits / Math.max(bfResults.length, 1)
    }

    const n = queries.length || 1
    return {
      meanRecall: totalRecall / n,
      meanIvfLatencyMs: ivfMs / n,
      meanBruteForceLatencyMs: bfMs / n,
      speedup: ivfMs > 0 ? bfMs / ivfMs : null,
    }
  }

  // -------------------------------------------------------------------------
  // Private guards
  // -------------------------------------------------------------------------

  private assertTrained(): void {
    if (!this.trained) {
      throw new Error(
        "IVFFlatIndex is not trained. Call train() before using the index.",
      )
    }
  }

  private assertDim(vec: Float32Array): void {
    if (this.dim !== null && vec.length !== this.dim) {
      throw new Error(
        `Vector dimension mismatch: expected ${this.dim}, got ${vec.length}`,
      )
    }
  }
}

// ---------------------------------------------------------------------------
// Benchmark result type
// ---------------------------------------------------------------------------

export interface BenchmarkResult {
  /** Fraction of true top-k results returned by IVF (0–1). */
  meanRecall: number
  /** Mean query latency for IVF search in milliseconds. */
  meanIvfLatencyMs: number
  /** Mean query latency for brute-force search in milliseconds. */
  meanBruteForceLatencyMs: number
  /** bfMs / ivfMs — how much faster IVF is than brute-force. Null when IVF completed in zero measurable time. */
  speedup: number | null
}

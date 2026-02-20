// IVF-Flat (Inverted File Index) for Approximate Nearest Neighbour search.
// Training: k-means clustering to produce centroids.
// Query: find the nprobe nearest centroids, then brute-force within those lists.

import { euclideanDistanceSquared, centroid } from "./metrics"
import type { VectorRecord } from "./types"

export interface IVFIndex {
  centroids: number[][]
  lists: Map<number, VectorRecord[]>
  dimension: number
}

/** Simple k-means with a fixed number of iterations. */
function kMeans(vectors: number[][], k: number, maxIter = 20): number[][] {
  if (vectors.length === 0 || k <= 0) return []
  k = Math.min(k, vectors.length)

  // Initialize centroids by picking k random vectors
  const indices = new Set<number>()
  while (indices.size < k) {
    indices.add(Math.floor(Math.random() * vectors.length))
  }
  let centroids = Array.from(indices).map((i) => vectors[i].slice())

  for (let iter = 0; iter < maxIter; iter++) {
    // Assignment step
    const clusters: number[][][] = Array.from({ length: k }, () => [])
    for (const v of vectors) {
      let best = 0
      let bestDist = euclideanDistanceSquared(v, centroids[0])
      for (let c = 1; c < k; c++) {
        const d = euclideanDistanceSquared(v, centroids[c])
        if (d < bestDist) {
          bestDist = d
          best = c
        }
      }
      clusters[best].push(v)
    }

    // Update step
    let changed = false
    for (let c = 0; c < k; c++) {
      if (clusters[c].length === 0) continue
      const newCentroid = centroid(clusters[c])
      if (euclideanDistanceSquared(newCentroid, centroids[c]) > 1e-10) {
        centroids[c] = newCentroid
        changed = true
      }
    }
    if (!changed) break
  }

  return centroids
}

/** Assign a vector to its nearest centroid index. */
function assignToCentroid(v: number[], centroids: number[][]): number {
  let best = 0
  let bestDist = euclideanDistanceSquared(v, centroids[0])
  for (let c = 1; c < centroids.length; c++) {
    const d = euclideanDistanceSquared(v, centroids[c])
    if (d < bestDist) {
      bestDist = d
      best = c
    }
  }
  return best
}

/** Build an IVF index from a collection of records. */
export function buildIVF(records: VectorRecord[], nCentroids = 64): IVFIndex | null {
  const live = records.filter((r) => !r.deleted && r.vector.length > 0)
  if (live.length === 0) return null

  const dimension = live[0].vector.length
  const vectors = live.map((r) => r.vector)
  const centroids = kMeans(vectors, nCentroids)

  const lists = new Map<number, VectorRecord[]>()
  for (let c = 0; c < centroids.length; c++) lists.set(c, [])

  for (const record of live) {
    const c = assignToCentroid(record.vector, centroids)
    lists.get(c)!.push(record)
  }

  return { centroids, lists, dimension }
}

/** Query the IVF index for the topK nearest neighbours. */
export function queryIVF(
  index: IVFIndex,
  queryVec: number[],
  topK = 10,
  nprobe = 8,
): Array<{ record: VectorRecord; distSq: number }> {
  const { centroids, lists } = index

  // Find closest nprobe centroids
  const centroidDists = centroids.map((c, i) => ({
    idx: i,
    dist: euclideanDistanceSquared(queryVec, c),
  }))
  centroidDists.sort((a, b) => a.dist - b.dist)
  const probeList = centroidDists.slice(0, Math.min(nprobe, centroids.length))

  // Collect candidates from selected posting lists
  const candidates: Array<{ record: VectorRecord; distSq: number }> = []
  for (const { idx } of probeList) {
    const posting = lists.get(idx) ?? []
    for (const record of posting) {
      candidates.push({
        record,
        distSq: euclideanDistanceSquared(queryVec, record.vector),
      })
    }
  }

  // Sort and return topK
  candidates.sort((a, b) => a.distSq - b.distSq)
  return candidates.slice(0, topK)
}

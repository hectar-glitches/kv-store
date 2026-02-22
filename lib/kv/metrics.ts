// Vector similarity metrics for ANN search

export type SimilarityMetric = "cosine" | "l2" | "dot"

/**
 * Returns the L2 (Euclidean) distance squared between two vectors.
 * Using squared distance avoids a sqrt and preserves ordering.
 */
export function l2DistanceSq(a: Float32Array, b: Float32Array): number {
  if (a.length !== b.length) {
    throw new Error(`Vector dimension mismatch: ${a.length} vs ${b.length}`)
  }
  let sum = 0
  for (let i = 0; i < a.length; i++) {
    const d = a[i] - b[i]
    sum += d * d
  }
  return sum
}

/** Dot product of two vectors. */
export function dotProduct(a: Float32Array, b: Float32Array): number {
  if (a.length !== b.length) {
    throw new Error(`Vector dimension mismatch: ${a.length} vs ${b.length}`)
  }
  let sum = 0
  for (let i = 0; i < a.length; i++) {
    sum += a[i] * b[i]
  }
  return sum
}

/** Cosine similarity in [−1, 1]. Returns 0 for zero vectors. */
export function cosineSimilarity(a: Float32Array, b: Float32Array): number {
  const dot = dotProduct(a, b)
  const normA = Math.sqrt(dotProduct(a, a))
  const normB = Math.sqrt(dotProduct(b, b))
  if (normA === 0 || normB === 0) return 0
  return dot / (normA * normB)
}

/**
 * Unified scorer: higher return value always means more similar.
 * For l2 we negate the squared distance so the semantics stay consistent.
 */
export function score(
  a: Float32Array,
  b: Float32Array,
  metric: SimilarityMetric,
): number {
  switch (metric) {
    case "cosine":
      return cosineSimilarity(a, b)
    case "dot":
      return dotProduct(a, b)
    case "l2":
      return -l2DistanceSq(a, b)
  }
}

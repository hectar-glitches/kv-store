// Vector similarity and distance metrics

export function dotProduct(a: number[], b: number[]): number {
  let sum = 0
  for (let i = 0; i < a.length; i++) {
    sum += a[i] * b[i]
  }
  return sum
}

export function magnitude(v: number[]): number {
  return Math.sqrt(dotProduct(v, v))
}

export function cosineSimilarity(a: number[], b: number[]): number {
  const magA = magnitude(a)
  const magB = magnitude(b)
  if (magA === 0 || magB === 0) return 0
  return dotProduct(a, b) / (magA * magB)
}

export function euclideanDistance(a: number[], b: number[]): number {
  let sum = 0
  for (let i = 0; i < a.length; i++) {
    const diff = a[i] - b[i]
    sum += diff * diff
  }
  return Math.sqrt(sum)
}

export function euclideanDistanceSquared(a: number[], b: number[]): number {
  let sum = 0
  for (let i = 0; i < a.length; i++) {
    const diff = a[i] - b[i]
    sum += diff * diff
  }
  return sum
}

export function normalize(v: number[]): number[] {
  const mag = magnitude(v)
  if (mag === 0) return v.slice()
  return v.map((x) => x / mag)
}

export function centroid(vectors: number[][]): number[] {
  if (vectors.length === 0) return []
  const dim = vectors[0].length
  const sum = new Array(dim).fill(0)
  for (const v of vectors) {
    for (let i = 0; i < dim; i++) {
      sum[i] += v[i]
    }
  }
  return sum.map((x) => x / vectors.length)
}

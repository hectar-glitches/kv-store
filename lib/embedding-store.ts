// Embedding / Vector Store Engine
// Supports upsert, nearest-neighbor query, stats, and delete for a single collection.

export interface VectorRecord {
  id: string
  values: number[]
  metadata?: Record<string, unknown>
}

export interface QueryMatch {
  id: string
  score: number
  metadata?: Record<string, unknown>
  values?: number[]
}

export interface UpsertResult {
  upsertedCount: number
}

export interface DeleteResult {
  deleted: number
}

export interface CollectionStats {
  totalVectorCount: number
  dimension: number | null
  namespaces: { "": { vectorCount: number } }
  indexFullness: number
}

// --- helpers -----------------------------------------------------------------

function cosineSimilarity(a: number[], b: number[]): number {
  if (a.length !== b.length) return 0
  let dot = 0
  let normA = 0
  let normB = 0
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i]
    normA += a[i] * a[i]
    normB += b[i] * b[i]
  }
  const denom = Math.sqrt(normA) * Math.sqrt(normB)
  return denom === 0 ? 0 : dot / denom
}

/** Simple flat metadata filter: every key in `filter` must match the record's metadata. */
function matchesFilter(
  metadata: Record<string, unknown> | undefined,
  filter: Record<string, unknown>,
): boolean {
  if (!metadata) return false
  return Object.entries(filter).every(([k, v]) => metadata[k] === v)
}

// --- store class -------------------------------------------------------------

class EmbeddingStore {
  private records: Map<string, VectorRecord> = new Map()
  private dimension: number | null = null

  upsert(vectors: VectorRecord[]): UpsertResult {
    let upsertedCount = 0
    for (const vec of vectors) {
      if (!vec.id || !Array.isArray(vec.values) || vec.values.length === 0) {
        continue
      }
      // Validate / lock dimension on first insert
      if (this.dimension === null) {
        this.dimension = vec.values.length
      } else if (vec.values.length !== this.dimension) {
        throw new Error(
          `Dimension mismatch: expected ${this.dimension}, got ${vec.values.length}`,
        )
      }
      this.records.set(vec.id, {
        id: vec.id,
        values: vec.values,
        metadata: vec.metadata ?? {},
      })
      upsertedCount++
    }
    return { upsertedCount }
  }

  query(
    vector: number[],
    topK: number,
    filter?: Record<string, unknown>,
    includeMetadata = true,
    includeValues = false,
  ): QueryMatch[] {
    if (this.records.size === 0) return []
    if (this.dimension !== null && vector.length !== this.dimension) {
      throw new Error(
        `Query dimension mismatch: expected ${this.dimension}, got ${vector.length}`,
      )
    }

    const candidates: Array<{ id: string; score: number; record: VectorRecord }> = []

    for (const record of this.records.values()) {
      if (filter && Object.keys(filter).length > 0) {
        if (!matchesFilter(record.metadata, filter)) continue
      }
      const score = cosineSimilarity(vector, record.values)
      candidates.push({ id: record.id, score, record })
    }

    candidates.sort((a, b) => b.score - a.score)

    return candidates.slice(0, topK).map(({ id, score, record }) => {
      const match: QueryMatch = { id, score }
      if (includeMetadata) match.metadata = record.metadata
      if (includeValues) match.values = record.values
      return match
    })
  }

  delete(ids: string[]): DeleteResult {
    let deleted = 0
    for (const id of ids) {
      if (this.records.delete(id)) deleted++
    }
    // Reset dimension if collection is now empty
    if (this.records.size === 0) this.dimension = null
    return { deleted }
  }

  getStats(): CollectionStats {
    return {
      totalVectorCount: this.records.size,
      dimension: this.dimension,
      namespaces: { "": { vectorCount: this.records.size } },
      indexFullness: 0,
    }
  }
}

// Singleton
let embeddingStoreInstance: EmbeddingStore | null = null

export function getEmbeddingStore(): EmbeddingStore {
  if (!embeddingStoreInstance) {
    embeddingStoreInstance = new EmbeddingStore()
    console.log("[EmbeddingStore] Initialized single-collection vector store")
  }
  return embeddingStoreInstance
}

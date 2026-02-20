// Core types for the vector/embedding store

export interface VectorRecord {
  id: string
  vector: number[]
  metadata?: Record<string, unknown>
  timestamp: number
  deleted?: boolean
}

export interface UpsertRequest {
  id: string
  vector: number[]
  metadata?: Record<string, unknown>
}

export interface QueryRequest {
  vector: number[]
  topK?: number
  nprobe?: number
  filter?: Record<string, unknown>
}

export interface QueryResult {
  id: string
  score: number
  metadata?: Record<string, unknown>
}

export interface StoreStats {
  totalRecords: number
  deletedRecords: number
  dimension: number | null
  walEntries: number
  segments: number
  collection: string
  dataDir: string
  uptime: number
  ivfTrained: boolean
  ivfCentroids: number
}

export interface WALEntry {
  seq: number
  op: "upsert" | "delete"
  record: VectorRecord
}

export interface SegmentMeta {
  filename: string
  minSeq: number
  maxSeq: number
  count: number
}

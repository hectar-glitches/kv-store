// Engine entrypoint: ties together WAL, segments, compaction, and IVF index.

import path from "path"
import { WAL } from "./wal"
import { SegmentWriter, SegmentReader, listSegments, deleteSegment } from "./segment"
import { compact } from "./compaction"
import { buildIVF, queryIVF, type IVFIndex } from "./ivf"
import { cosineSimilarity } from "./metrics"
import type {
  VectorRecord,
  UpsertRequest,
  QueryRequest,
  QueryResult,
  StoreStats,
  SegmentMeta,
} from "./types"

// Configurable via env
const DATA_DIR = process.env.KVSTORE_DATA_DIR ?? "./data"
const COLLECTION = process.env.KVSTORE_COLLECTION ?? "default"
const FLUSH_THRESHOLD = parseInt(process.env.KVSTORE_FLUSH_THRESHOLD ?? "256", 10)
const IVF_CENTROIDS = parseInt(process.env.KVSTORE_IVF_CENTROIDS ?? "64", 10)
const COMPACT_THRESHOLD = parseInt(process.env.KVSTORE_COMPACT_THRESHOLD ?? "4", 10)

export class VectorEngine {
  private dataDir: string
  private collection: string
  private wal: WAL
  private memtable: Map<string, VectorRecord> = new Map()
  private memtableMinSeq: number = 0
  private segments: SegmentMeta[] = []
  private ivfIndex: IVFIndex | null = null
  private startTime = Date.now()
  private ready = false

  constructor(dataDir = DATA_DIR, collection = COLLECTION) {
    this.dataDir = path.resolve(dataDir)
    this.collection = collection
    this.wal = new WAL(this.dataDir, this.collection)
  }

  /** Must be called once before using the engine. */
  async open(): Promise<void> {
    if (this.ready) return
    this.wal.open()
    // Replay WAL into memtable
    const entries = this.wal.replay()
    for (const entry of entries) {
      if (entry.op === "upsert") {
        this.memtable.set(entry.record.id, entry.record)
      } else {
        this.memtable.set(entry.record.id, { ...entry.record, deleted: true })
      }
    }
    // Load existing segments
    this.segments = listSegments(this.dataDir)
    // Rebuild IVF from all data
    this.rebuildIVF()
    this.ready = true
  }

  async upsert(req: UpsertRequest): Promise<void> {
    this.ensureReady()
    const record: VectorRecord = {
      id: req.id,
      vector: req.vector,
      metadata: req.metadata,
      timestamp: Date.now(),
    }
    this.wal.appendUpsert(record)
    if (this.memtable.size === 0) {
      this.memtableMinSeq = this.wal.currentSeq
    }
    this.memtable.set(req.id, record)

    if (this.memtable.size >= FLUSH_THRESHOLD) {
      await this.flush()
    } else {
      // Incrementally update the IVF index
      this.rebuildIVF()
    }
  }

  async delete(id: string): Promise<boolean> {
    this.ensureReady()
    const exists = this.has(id)
    if (!exists) return false

    this.wal.appendDelete(id)
    const tombstone: VectorRecord = { id, vector: [], timestamp: Date.now(), deleted: true }
    this.memtable.set(id, tombstone)
    this.rebuildIVF()
    return true
  }

  query(req: QueryRequest): QueryResult[] {
    this.ensureReady()
    const topK = req.topK ?? 10
    const nprobe = req.nprobe ?? 8
    const queryVec = req.vector

    // Merge memtable + segments into one map (memtable wins)
    const all = this.loadAll()

    if (this.ivfIndex && all.size > FLUSH_THRESHOLD) {
      // Use IVF for large collections
      const hits = queryIVF(this.ivfIndex, queryVec, topK, nprobe)
      return hits.map(({ record, distSq: _ }) => ({
        id: record.id,
        score: cosineSimilarity(queryVec, record.vector),
        metadata: record.metadata,
      }))
    }

    // Brute-force for small collections or when IVF not ready
    const results: QueryResult[] = []
    for (const record of all.values()) {
      if (record.deleted || record.vector.length === 0) continue
      if (!this.matchesFilter(record, req.filter)) continue
      const score = cosineSimilarity(queryVec, record.vector)
      results.push({ id: record.id, score, metadata: record.metadata })
    }
    results.sort((a, b) => b.score - a.score)
    return results.slice(0, topK)
  }

  getStats(): StoreStats {
    const all = this.loadAll()
    let total = 0
    let deleted = 0
    let dimension: number | null = null
    for (const r of all.values()) {
      if (r.deleted) {
        deleted++
      } else {
        total++
        if (dimension === null && r.vector.length > 0) dimension = r.vector.length
      }
    }
    return {
      totalRecords: total,
      deletedRecords: deleted,
      dimension,
      walEntries: this.wal.entryCount,
      segments: this.segments.length,
      collection: this.collection,
      dataDir: this.dataDir,
      uptime: Date.now() - this.startTime,
      ivfTrained: this.ivfIndex !== null,
      ivfCentroids: this.ivfIndex?.centroids.length ?? 0,
    }
  }

  /** Flush memtable to an immutable segment file. */
  async flush(): Promise<void> {
    if (this.memtable.size === 0) return

    const records = Array.from(this.memtable.values())
    const minSeq = this.memtableMinSeq
    const maxSeq = this.wal.currentSeq

    const writer = new SegmentWriter(this.dataDir, Math.max(0, minSeq), maxSeq)
    const meta = writer.write(records)
    this.segments.push(meta)
    this.memtable.clear()
    this.memtableMinSeq = 0

    // Truncate WAL after checkpoint
    this.wal.truncate()

    // Trigger compaction if we have too many segments
    if (this.segments.length >= COMPACT_THRESHOLD) {
      await this.runCompaction()
    }

    this.rebuildIVF()
  }

  private async runCompaction(): Promise<void> {
    if (this.segments.length < 2) return
    const minSeq = this.segments[0].minSeq
    const maxSeq = this.segments[this.segments.length - 1].maxSeq
    const newMeta = compact(this.dataDir, this.segments, minSeq, maxSeq)
    this.segments = newMeta ? [newMeta] : []
  }

  private rebuildIVF(): void {
    const all = this.loadAll()
    const live = Array.from(all.values()).filter((r) => !r.deleted && r.vector.length > 0)
    if (live.length === 0) {
      this.ivfIndex = null
      return
    }
    this.ivfIndex = buildIVF(live, IVF_CENTROIDS)
  }

  private loadAll(): Map<string, VectorRecord> {
    const merged = new Map<string, VectorRecord>()

    // Load from segment files (oldest first)
    for (const seg of this.segments) {
      const reader = new SegmentReader(this.dataDir, seg.filename)
      for (const record of reader.read()) {
        merged.set(record.id, record)
      }
    }

    // Apply memtable (latest writes win)
    for (const [id, record] of this.memtable.entries()) {
      merged.set(id, record)
    }

    return merged
  }

  private has(id: string): boolean {
    if (this.memtable.has(id)) {
      const r = this.memtable.get(id)!
      return !r.deleted
    }
    const all = this.loadAll()
    const r = all.get(id)
    return r !== undefined && !r.deleted
  }

  private matchesFilter(
    record: VectorRecord,
    filter?: Record<string, unknown>,
  ): boolean {
    if (!filter || Object.keys(filter).length === 0) return true
    const meta = record.metadata ?? {}
    for (const [k, v] of Object.entries(filter)) {
      if (meta[k] !== v) return false
    }
    return true
  }

  private ensureReady(): void {
    if (!this.ready) throw new Error("VectorEngine not opened. Call open() first.")
  }

  async close(): Promise<void> {
    await this.flush()
    this.wal.close()
    this.ready = false
  }
}

// Singleton for Next.js server usage
let engineInstance: VectorEngine | null = null
let engineReady: Promise<void> | null = null

export async function getEngine(): Promise<VectorEngine> {
  if (!engineInstance) {
    engineInstance = new VectorEngine()
    engineReady = engineInstance.open()
  }
  await engineReady
  return engineInstance!
}

// In-memory table: id → { vector, metadata, seq, tombstone }

export interface MemtableEntry {
  id: string
  vector: number[]
  metadata: Record<string, unknown>
  seq: number
  tombstone: boolean
}

export class Memtable {
  private table: Map<string, MemtableEntry> = new Map()

  // Insert or update an entry
  upsert(id: string, vector: number[], metadata: Record<string, unknown>, seq: number): void {
    this.table.set(id, {
      id,
      vector: [...vector],
      metadata: JSON.parse(JSON.stringify(metadata)),
      seq,
      tombstone: false,
    })
    console.log(`[Memtable] UPSERT id=${id} seq=${seq}`)
  }

  // Mark an entry as deleted via tombstone
  delete(id: string, seq: number): void {
    const existing = this.table.get(id)
    if (existing) {
      existing.tombstone = true
      existing.seq = seq
    } else {
      // Record a tombstone even for unknown IDs so compaction can drop them
      this.table.set(id, { id, vector: [], metadata: {}, seq, tombstone: true })
    }
    console.log(`[Memtable] DELETE id=${id} seq=${seq}`)
  }

  get(id: string): MemtableEntry | undefined {
    return this.table.get(id)
  }

  entries(): MemtableEntry[] {
    return Array.from(this.table.values())
  }

  get size(): number {
    return this.table.size
  }

  // Returns true when the number of entries meets or exceeds the flush threshold
  isFull(threshold: number): boolean {
    return this.table.size >= threshold
  }

  clear(): void {
    this.table.clear()
    console.log("[Memtable] Cleared")
  }
}

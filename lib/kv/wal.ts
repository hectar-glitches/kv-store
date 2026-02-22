// Write-Ahead Log for vector store mutations

export type WALEntryType = "upsert" | "delete"

export interface WALEntry {
  seq: number
  type: WALEntryType
  id: string
  vector?: number[]
  metadata?: Record<string, unknown>
  timestamp: number
}

export class WAL {
  private log: WALEntry[] = []
  private nextSeq = 1

  // Append a mutation to the WAL and return the created entry
  append(
    type: WALEntryType,
    id: string,
    vector?: number[],
    metadata?: Record<string, unknown>,
  ): WALEntry {
    const entry: WALEntry = {
      seq: this.nextSeq++,
      type,
      id,
      vector: vector ? [...vector] : undefined,
      metadata: metadata ? { ...metadata } : undefined,
      timestamp: Date.now(),
    }
    this.log.push(entry)
    console.log(`[WAL] Appended ${type} id=${id} seq=${entry.seq}`)
    return entry
  }

  // Replay all entries in order (e.g. on boot to rebuild memtable)
  replay(): WALEntry[] {
    console.log(`[WAL] Replaying ${this.log.length} entries`)
    return [...this.log]
  }

  // Truncate the log after a successful segment flush
  truncate(): void {
    const count = this.log.length
    this.log = []
    console.log(`[WAL] Truncated ${count} entries after segment flush`)
  }

  // Return a snapshot of all current entries
  getEntries(): WALEntry[] {
    return [...this.log]
  }

  get size(): number {
    return this.log.length
  }
}

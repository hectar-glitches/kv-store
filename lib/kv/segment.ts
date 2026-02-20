// Immutable segment: frozen snapshot of memtable entries written to "disk"

import type { MemtableEntry } from "./memtable"

export interface SegmentManifest {
  id: string
  createdAt: number
  entryCount: number
  minSeq: number
  maxSeq: number
}

// Module-level counter ensures unique IDs even when called within the same millisecond.
// Safe in JavaScript's single-threaded execution model.
let segmentCounter = 0

// Generate a monotonically increasing segment ID
export function createSegmentId(): string {
  return `seg_${Date.now()}_${++segmentCounter}`
}

export class Segment {
  private readonly data: Map<string, MemtableEntry>
  readonly manifest: SegmentManifest

  constructor(id: string, entries: MemtableEntry[]) {
    // Deep-copy entries so the segment is truly immutable
    this.data = new Map(
      entries.map((e) => [
        e.id,
        { ...e, vector: [...e.vector], metadata: JSON.parse(JSON.stringify(e.metadata)) },
      ]),
    )

    const seqs = entries.map((e) => e.seq)
    this.manifest = {
      id,
      createdAt: Date.now(),
      entryCount: entries.length,
      minSeq: seqs.length > 0 ? Math.min(...seqs) : 0,
      maxSeq: seqs.length > 0 ? Math.max(...seqs) : 0,
    }

    console.log(
      `[Segment] Created ${id} with ${entries.length} entries` +
        ` (seq ${this.manifest.minSeq}–${this.manifest.maxSeq})`,
    )
  }

  // Look up a single entry by ID
  read(id: string): MemtableEntry | undefined {
    return this.data.get(id)
  }

  // Return all entries in the segment
  getEntries(): MemtableEntry[] {
    return Array.from(this.data.values())
  }

  // Return all IDs stored in the segment
  getIds(): string[] {
    return Array.from(this.data.keys())
  }

  getManifest(): SegmentManifest {
    return { ...this.manifest }
  }
}

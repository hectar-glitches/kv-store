// Compaction: merge segments, drop tombstoned IDs, deduplicate by latest seq

import { Segment, createSegmentId } from "./segment"
import type { MemtableEntry } from "./memtable"

export interface CompactionResult {
  segment: Segment
  droppedTombstones: number
  deduplicated: number
  inputSegments: number
}

// Merge all provided segments into one, keeping the highest-seq entry per ID
// and discarding tombstoned entries from the final output.
// Requires at least one segment; callers should validate before invoking.
export function compact(segments: Segment[]): CompactionResult {
  if (segments.length === 0) {
    throw new Error("[Compaction] compact() requires at least one segment")
  }

  const totalInputEntries = segments.reduce((sum, s) => sum + s.getEntries().length, 0)

  // Collect the latest version of each entry across all segments
  const latest = new Map<string, MemtableEntry>()
  for (const segment of segments) {
    for (const entry of segment.getEntries()) {
      const existing = latest.get(entry.id)
      if (!existing || entry.seq > existing.seq) {
        latest.set(entry.id, entry)
      }
    }
  }

  const allLatest = Array.from(latest.values())
  const deduplicated = totalInputEntries - allLatest.length

  // Drop tombstones — these IDs have been deleted and need not be retained
  const liveEntries = allLatest.filter((e) => !e.tombstone)
  const droppedTombstones = allLatest.length - liveEntries.length

  const mergedSegment = new Segment(createSegmentId(), liveEntries)

  console.log(
    `[Compaction] ${segments.length} segments → ${liveEntries.length} live entries` +
      ` (deduped ${deduplicated}, dropped ${droppedTombstones} tombstones)`,
  )

  return {
    segment: mergedSegment,
    droppedTombstones,
    deduplicated,
    inputSegments: segments.length,
  }
}

// Returns true when segment count meets or exceeds the compaction threshold
export function shouldCompact(segments: Segment[], threshold: number): boolean {
  return segments.length >= threshold
}

export class CompactionManager {
  private segments: Segment[] = []
  private readonly threshold: number

  constructor(threshold = 4) {
    this.threshold = threshold
    console.log(`[CompactionManager] Initialized with threshold=${threshold}`)
  }

  addSegment(segment: Segment): void {
    this.segments.push(segment)
    console.log(
      `[CompactionManager] Added segment ${segment.manifest.id}` +
        ` (total: ${this.segments.length})`,
    )
    if (shouldCompact(this.segments, this.threshold)) {
      this.runCompaction()
    }
  }

  // Manually trigger compaction regardless of threshold.
  // Requires at least 2 segments; returns null otherwise.
  runCompaction(): CompactionResult | null {
    if (this.segments.length < 2) {
      console.log("[CompactionManager] Not enough segments to compact (need ≥ 2)")
      return null
    }
    const result = compact(this.segments)
    this.segments = [result.segment]
    return result
  }

  getSegments(): Segment[] {
    return [...this.segments]
  }

  needsCompaction(): boolean {
    return shouldCompact(this.segments, this.threshold)
  }
}

// Compaction: merge multiple segment files into one, dropping tombstones.

import type { VectorRecord, SegmentMeta } from "./types"
import { SegmentReader, SegmentWriter, deleteSegment } from "./segment"

/**
 * Merge the given segments into a single new segment.
 * Later entries (higher seq) win for the same id.
 * Tombstones (deleted=true) are dropped from the output.
 * Returns the new SegmentMeta, or null if no live records remain.
 */
export function compact(
  dataDir: string,
  segments: SegmentMeta[],
  newMinSeq: number,
  newMaxSeq: number,
): SegmentMeta | null {
  if (segments.length === 0) return null

  // Merge: last-write-wins per id (segments sorted oldest to newest)
  const merged = new Map<string, VectorRecord>()
  for (const seg of segments) {
    const reader = new SegmentReader(dataDir, seg.filename)
    for (const record of reader.read()) {
      merged.set(record.id, record)
    }
  }

  // Filter out tombstones
  const live = Array.from(merged.values()).filter((r) => !r.deleted)

  // Delete old segment files
  for (const seg of segments) {
    deleteSegment(dataDir, seg.filename)
  }

  if (live.length === 0) return null

  const writer = new SegmentWriter(dataDir, newMinSeq, newMaxSeq)
  return writer.write(live)
}

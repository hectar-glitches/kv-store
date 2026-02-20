// Immutable segment files: written once, read many times.
// Format: newline-delimited JSON records (one VectorRecord per line).

import fs from "fs"
import path from "path"
import type { VectorRecord, SegmentMeta } from "./types"

export class SegmentWriter {
  private filePath: string
  private count = 0
  private minSeq: number
  private maxSeq: number

  constructor(dataDir: string, minSeq: number, maxSeq: number) {
    this.minSeq = minSeq
    this.maxSeq = maxSeq
    const name = `seg_${String(minSeq).padStart(12, "0")}_${String(maxSeq).padStart(12, "0")}.jsonl`
    this.filePath = path.join(dataDir, name)
  }

  write(records: VectorRecord[]): SegmentMeta {
    fs.mkdirSync(path.dirname(this.filePath), { recursive: true })
    const lines = records.map((r) => JSON.stringify(r)).join("\n") + "\n"
    fs.writeFileSync(this.filePath, lines, "utf-8")
    this.count = records.length
    return this.meta()
  }

  meta(): SegmentMeta {
    return {
      filename: path.basename(this.filePath),
      minSeq: this.minSeq,
      maxSeq: this.maxSeq,
      count: this.count,
    }
  }

  get path(): string {
    return this.filePath
  }
}

export class SegmentReader {
  private filePath: string

  constructor(dataDir: string, filename: string) {
    this.filePath = path.join(dataDir, filename)
  }

  read(): VectorRecord[] {
    if (!fs.existsSync(this.filePath)) return []
    const content = fs.readFileSync(this.filePath, "utf-8")
    const records: VectorRecord[] = []
    for (const line of content.split("\n")) {
      const trimmed = line.trim()
      if (!trimmed) continue
      try {
        records.push(JSON.parse(trimmed) as VectorRecord)
      } catch {
        // skip malformed lines
      }
    }
    return records
  }
}

/** List all segment files in a directory, sorted by minSeq. */
export function listSegments(dataDir: string): SegmentMeta[] {
  if (!fs.existsSync(dataDir)) return []
  const files = fs.readdirSync(dataDir).filter((f) => f.startsWith("seg_") && f.endsWith(".jsonl"))
  files.sort()
  return files.map((filename) => {
    const parts = filename.replace(".jsonl", "").split("_")
    return {
      filename,
      minSeq: parseInt(parts[1], 10),
      maxSeq: parseInt(parts[2], 10),
      count: 0,
    }
  })
}

/** Delete a segment file from disk. */
export function deleteSegment(dataDir: string, filename: string): void {
  const filePath = path.join(dataDir, filename)
  if (fs.existsSync(filePath)) {
    fs.unlinkSync(filePath)
  }
}

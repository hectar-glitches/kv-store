// Write-Ahead Log: append-only log for durability
// Each entry is written as a newline-delimited JSON record.

import fs from "fs"
import path from "path"
import type { WALEntry, VectorRecord } from "./types"

export class WAL {
  private filePath: string
  private fd: number | null = null
  private seq: number = 0

  constructor(dataDir: string, collection: string) {
    this.filePath = path.join(dataDir, `${collection}.wal`)
  }

  open(): void {
    fs.mkdirSync(path.dirname(this.filePath), { recursive: true })
    this.fd = fs.openSync(this.filePath, "a")
  }

  close(): void {
    if (this.fd !== null) {
      fs.closeSync(this.fd)
      this.fd = null
    }
  }

  /** Append an upsert entry and return the sequence number assigned. */
  appendUpsert(record: VectorRecord): number {
    const entry: WALEntry = { seq: ++this.seq, op: "upsert", record }
    this.write(entry)
    return this.seq
  }

  /** Append a delete entry (tombstone). */
  appendDelete(id: string): number {
    const record: VectorRecord = { id, vector: [], timestamp: Date.now(), deleted: true }
    const entry: WALEntry = { seq: ++this.seq, op: "delete", record }
    this.write(entry)
    return this.seq
  }

  private write(entry: WALEntry): void {
    const line = JSON.stringify(entry) + "\n"
    if (this.fd !== null) {
      fs.writeSync(this.fd, line)
    }
  }

  /** Replay all entries from disk; resets internal seq counter. */
  replay(): WALEntry[] {
    if (!fs.existsSync(this.filePath)) return []
    const content = fs.readFileSync(this.filePath, "utf-8")
    const entries: WALEntry[] = []
    for (const line of content.split("\n")) {
      const trimmed = line.trim()
      if (!trimmed) continue
      try {
        const entry = JSON.parse(trimmed) as WALEntry
        entries.push(entry)
        if (entry.seq > this.seq) this.seq = entry.seq
      } catch {
        // skip malformed lines
      }
    }
    return entries
  }

  /** Truncate the WAL (called after a flush/checkpoint). */
  truncate(): void {
    this.close()
    fs.writeFileSync(this.filePath, "")
    this.open()
  }

  get currentSeq(): number {
    return this.seq
  }

  get entryCount(): number {
    if (!fs.existsSync(this.filePath)) return 0
    const content = fs.readFileSync(this.filePath, "utf-8")
    return content
      .split("\n")
      .filter((l) => l.trim())
      .length
  }
}

// Storage Layout Manager
// Handles the on-disk layout for the embedding store:
//   <dataDir>/<collection>/wal.log
//   <dataDir>/<collection>/segments/seg-NNNNNN/
//   <dataDir>/<collection>/CURRENT
import fs from "fs"
import path from "path"

// Config loaded from environment variables
export const DATA_DIR = process.env.KVSTORE_DATA_DIR ?? "./data"
export const COLLECTION = process.env.KVSTORE_COLLECTION ?? "default"

// Derived paths
export function getCollectionDir(dataDir = DATA_DIR, collection = COLLECTION): string {
  return path.join(dataDir, collection)
}

export function getWALPath(dataDir = DATA_DIR, collection = COLLECTION): string {
  return path.join(getCollectionDir(dataDir, collection), "wal.log")
}

export function getSegmentsDir(dataDir = DATA_DIR, collection = COLLECTION): string {
  return path.join(getCollectionDir(dataDir, collection), "segments")
}

export function getCurrentPath(dataDir = DATA_DIR, collection = COLLECTION): string {
  return path.join(getCollectionDir(dataDir, collection), "CURRENT")
}

export function formatSegmentName(index: number): string {
  return `seg-${String(index).padStart(6, "0")}`
}

/**
 * Initialise the on-disk directory structure for the collection.
 * Creates directories and seed files if they do not yet exist.
 */
export function initStorageLayout(dataDir = DATA_DIR, collection = COLLECTION): void {
  const collectionDir = getCollectionDir(dataDir, collection)
  const segmentsDir = getSegmentsDir(dataDir, collection)
  const walPath = getWALPath(dataDir, collection)
  const currentPath = getCurrentPath(dataDir, collection)

  // Create directories recursively
  fs.mkdirSync(collectionDir, { recursive: true })
  fs.mkdirSync(segmentsDir, { recursive: true })

  // Create the initial segment directory
  const initialSegment = formatSegmentName(1)
  fs.mkdirSync(path.join(segmentsDir, initialSegment), { recursive: true })

  // Touch wal.log if it doesn't exist
  if (!fs.existsSync(walPath)) {
    fs.writeFileSync(walPath, "", "utf8")
  }

  // Write CURRENT file pointing to the initial segment if it doesn't exist
  if (!fs.existsSync(currentPath)) {
    fs.writeFileSync(currentPath, initialSegment, "utf8")
  }

  console.log(
    `[Storage] Initialised layout at ${collectionDir} (DATA_DIR=${dataDir}, COLLECTION=${collection})`,
  )
}

/**
 * Append a WAL entry.  Each entry is a JSON line terminated with a newline.
 */
export function appendWAL(
  entry: { op: string; key?: string; value?: string; ttl?: number; ts: number },
  dataDir = DATA_DIR,
  collection = COLLECTION,
): void {
  const walPath = getWALPath(dataDir, collection)
  fs.appendFileSync(walPath, JSON.stringify(entry) + "\n", "utf8")
}

/**
 * Read the name of the most-recent manifest/segment from CURRENT.
 */
export function readCurrent(dataDir = DATA_DIR, collection = COLLECTION): string {
  const currentPath = getCurrentPath(dataDir, collection)
  return fs.readFileSync(currentPath, "utf8").trim()
}

/**
 * Update the CURRENT pointer to a new segment name.
 */
export function writeCurrent(
  segmentName: string,
  dataDir = DATA_DIR,
  collection = COLLECTION,
): void {
  const currentPath = getCurrentPath(dataDir, collection)
  fs.writeFileSync(currentPath, segmentName, "utf8")
}

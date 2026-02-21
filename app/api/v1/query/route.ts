import { type NextRequest, NextResponse } from "next/server"
import { getEmbeddingStore } from "@/lib/embedding-store"

export async function POST(request: NextRequest) {
  try {
    const body = await request.json()
    const { vector, topK = 10, filter, includeMetadata = true, includeValues = false } = body ?? {}

    if (!Array.isArray(vector) || vector.length === 0) {
      return NextResponse.json(
        { error: "Request body must include a non-empty 'vector' array" },
        { status: 400 },
      )
    }

    const store = getEmbeddingStore()
    const matches = store.query(vector, topK, filter, includeMetadata, includeValues)

    return NextResponse.json({ matches })
  } catch (error) {
    const message = error instanceof Error ? error.message : "Internal server error"
    console.error("[v1/query] error:", error)
    return NextResponse.json({ error: message }, { status: 500 })
  }
}

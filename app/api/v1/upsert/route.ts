import { type NextRequest, NextResponse } from "next/server"
import { getEmbeddingStore } from "@/lib/embedding-store"

export async function POST(request: NextRequest) {
  try {
    const body = await request.json()
    const vectors = body?.vectors

    if (!Array.isArray(vectors) || vectors.length === 0) {
      return NextResponse.json(
        { error: "Request body must include a non-empty 'vectors' array" },
        { status: 400 },
      )
    }

    const store = getEmbeddingStore()
    const result = store.upsert(vectors)

    return NextResponse.json(result)
  } catch (error) {
    const message = error instanceof Error ? error.message : "Internal server error"
    console.error("[v1/upsert] error:", error)
    return NextResponse.json({ error: message }, { status: 500 })
  }
}

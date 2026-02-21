import { type NextRequest, NextResponse } from "next/server"
import { getEmbeddingStore } from "@/lib/embedding-store"

export async function POST(request: NextRequest) {
  try {
    const body = await request.json()
    const ids = body?.ids

    if (!Array.isArray(ids) || ids.length === 0) {
      return NextResponse.json(
        { error: "Request body must include a non-empty 'ids' array" },
        { status: 400 },
      )
    }

    const store = getEmbeddingStore()
    const result = store.delete(ids)

    return NextResponse.json(result)
  } catch (error) {
    const message = error instanceof Error ? error.message : "Internal server error"
    console.error("[v1/delete] error:", error)
    return NextResponse.json({ error: message }, { status: 500 })
  }
}

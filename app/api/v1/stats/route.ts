import { NextResponse } from "next/server"
import { getEmbeddingStore } from "@/lib/embedding-store"

export async function GET() {
  try {
    const store = getEmbeddingStore()
    return NextResponse.json(store.getStats())
  } catch (error) {
    console.error("[v1/stats] error:", error)
    return NextResponse.json({ error: "Internal server error" }, { status: 500 })
  }
}

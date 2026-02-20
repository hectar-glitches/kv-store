import { NextRequest, NextResponse } from "next/server"
import { getEngine } from "@/lib/kv"
import type { QueryRequest } from "@/lib/kv/types"

export async function POST(request: NextRequest) {
  try {
    const body = (await request.json()) as QueryRequest
    if (!Array.isArray(body.vector)) {
      return NextResponse.json({ error: "vector is required" }, { status: 400 })
    }

    const engine = await getEngine()
    const results = engine.query(body)

    return NextResponse.json({ results })
  } catch (error) {
    console.error("Query error:", error)
    return NextResponse.json({ error: "Internal server error" }, { status: 500 })
  }
}

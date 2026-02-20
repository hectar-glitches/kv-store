import { NextRequest, NextResponse } from "next/server"
import { getEngine } from "@/lib/kv"
import type { UpsertRequest } from "@/lib/kv/types"

export async function POST(request: NextRequest) {
  try {
    const body = (await request.json()) as UpsertRequest | UpsertRequest[]
    const engine = await getEngine()

    const items = Array.isArray(body) ? body : [body]
    for (const item of items) {
      if (!item.id || !Array.isArray(item.vector)) {
        return NextResponse.json({ error: "Each record must have id and vector" }, { status: 400 })
      }
      await engine.upsert(item)
    }

    return NextResponse.json({ upserted: items.length })
  } catch (error) {
    console.error("Upsert error:", error)
    return NextResponse.json({ error: "Internal server error" }, { status: 500 })
  }
}

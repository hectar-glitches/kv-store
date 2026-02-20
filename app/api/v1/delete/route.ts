import { NextRequest, NextResponse } from "next/server"
import { getEngine } from "@/lib/kv"

export async function POST(request: NextRequest) {
  try {
    const body = (await request.json()) as { id: string } | { ids: string[] }
    const engine = await getEngine()

    const ids = "ids" in body ? body.ids : [body.id]
    if (!ids || ids.length === 0) {
      return NextResponse.json({ error: "id or ids required" }, { status: 400 })
    }

    const deleted: string[] = []
    for (const id of ids) {
      const ok = await engine.delete(id)
      if (ok) deleted.push(id)
    }

    return NextResponse.json({ deleted })
  } catch (error) {
    console.error("Delete error:", error)
    return NextResponse.json({ error: "Internal server error" }, { status: 500 })
  }
}

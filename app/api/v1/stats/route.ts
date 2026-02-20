import { NextResponse } from "next/server"
import { getEngine } from "@/lib/kv"

export async function GET() {
  try {
    const engine = await getEngine()
    const stats = engine.getStats()
    return NextResponse.json(stats)
  } catch (error) {
    console.error("Stats error:", error)
    return NextResponse.json({ error: "Internal server error" }, { status: 500 })
  }
}

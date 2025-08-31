import { NextResponse } from "next/server"
import { getKVStore } from "@/lib/kvstore"

export async function GET() {
  try {
    const kvStore = getKVStore()
    const stats = kvStore.getStats()

    return NextResponse.json(stats)
  } catch (error) {
    console.error("STATS API error:", error)
    return NextResponse.json({ error: "Internal server error" }, { status: 500 })
  }
}

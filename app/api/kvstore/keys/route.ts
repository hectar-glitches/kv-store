import { NextResponse } from "next/server"
import { getKVStore } from "@/lib/kvstore"

export async function GET() {
  try {
    const kvStore = getKVStore()
    const keys = kvStore.keys()

    return NextResponse.json({
      success: true,
      keys,
      count: keys.length
    })
  } catch (error) {
    console.error("KEYS API error:", error)
    return NextResponse.json({ 
      success: false, 
      error: "Failed to retrieve keys" 
    }, { status: 500 })
  }
}

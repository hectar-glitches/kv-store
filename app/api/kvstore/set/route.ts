import { type NextRequest, NextResponse } from "next/server"
import { getKVStore } from "@/lib/kvstore"

export async function POST(request: NextRequest) {
  try {
    const { key, value, ttl } = await request.json()

    if (!key || value === undefined) {
      return NextResponse.json({ success: false, error: "Key and value are required" }, { status: 400 })
    }

    const kvStore = getKVStore()
    const success = kvStore.set(key, value, ttl)

    return NextResponse.json({
      success,
      message: success ? "Key set successfully" : "Failed to set key",
    })
  } catch (error) {
    console.error("SET API error:", error)
    return NextResponse.json({ success: false, error: "Internal server error" }, { status: 500 })
  }
}

import { type NextRequest, NextResponse } from "next/server"
import { getKVStore } from "@/lib/kvstore"

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const key = searchParams.get("key")

    if (!key) {
      return NextResponse.json({ success: false, error: "Key parameter is required" }, { status: 400 })
    }

    const kvStore = getKVStore()
    const value = kvStore.get(key)

    return NextResponse.json({
      success: true,
      value,
      found: value !== null,
    })
  } catch (error) {
    console.error("GET API error:", error)
    return NextResponse.json({ success: false, error: "Internal server error" }, { status: 500 })
  }
}

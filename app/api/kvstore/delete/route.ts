import { type NextRequest, NextResponse } from "next/server"
import { getKVStore } from "@/lib/kvstore"

export async function DELETE(request: NextRequest) {
  try {
    const { key } = await request.json()

    if (!key) {
      return NextResponse.json({ success: false, error: "Key is required" }, { status: 400 })
    }

    const kvStore = getKVStore()
    const success = kvStore.delete(key)

    return NextResponse.json({
      success,
      message: success ? "Key deleted successfully" : "Key not found",
    })
  } catch (error) {
    console.error("DELETE API error:", error)
    return NextResponse.json({ success: false, error: "Internal server error" }, { status: 500 })
  }
}

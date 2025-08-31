import { NextResponse } from "next/server"
import { getKVStore } from "@/lib/kvstore"

export async function GET() {
  try {
    const kvStore = getKVStore()
    const nodes = kvStore.getNodes()

    return NextResponse.json({
      success: true,
      nodes,
    })
  } catch (error) {
    console.error("NODES API error:", error)
    return NextResponse.json({ success: false, error: "Internal server error" }, { status: 500 })
  }
}

export async function POST(request: Request) {
  try {
    const { nodeId, host, port } = await request.json()

    if (!nodeId) {
      return NextResponse.json({ success: false, error: "Node ID is required" }, { status: 400 })
    }

    const kvStore = getKVStore()
    const success = kvStore.addNode(nodeId, host || "localhost", port || 6379)

    return NextResponse.json({
      success,
      message: success ? "Node added successfully" : "Failed to add node",
    })
  } catch (error) {
    console.error("ADD NODE API error:", error)
    return NextResponse.json({ success: false, error: "Internal server error" }, { status: 500 })
  }
}

export async function DELETE(request: Request) {
  try {
    const { nodeId } = await request.json()

    if (!nodeId) {
      return NextResponse.json({ success: false, error: "Node ID is required" }, { status: 400 })
    }

    const kvStore = getKVStore()
    const success = kvStore.removeNode(nodeId)

    return NextResponse.json({
      success,
      message: success ? "Node removed successfully" : "Node not found",
    })
  } catch (error) {
    console.error("REMOVE NODE API error:", error)
    return NextResponse.json({ success: false, error: "Internal server error" }, { status: 500 })
  }
}

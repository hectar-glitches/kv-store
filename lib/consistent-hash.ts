import crypto from "crypto"

export interface ShardNode {
  id: string
  host: string
  port: number
  isHealthy: boolean
  keyCount: number
  lastSeen: number
}

export interface VirtualNode {
  hash: number
  nodeId: string
  virtualIndex: number
}

export interface ShardingStats {
  totalNodes: number
  healthyNodes: number
  virtualNodesPerNode: number
  totalVirtualNodes: number
  keyDistribution: Record<string, number>
  rebalanceOperations: number
}

export class ConsistentHashRing {
  private nodes: Map<string, ShardNode> = new Map()
  private virtualNodes: VirtualNode[] = []
  private virtualNodesPerNode: number
  private stats: ShardingStats

  constructor(virtualNodesPerNode = 150) {
    this.virtualNodesPerNode = virtualNodesPerNode
    this.stats = {
      totalNodes: 0,
      healthyNodes: 0,
      virtualNodesPerNode,
      totalVirtualNodes: 0,
      keyDistribution: {},
      rebalanceOperations: 0,
    }

    console.log(`[ConsistentHash] Initialized with ${virtualNodesPerNode} virtual nodes per physical node`)
  }

  // Hash function using SHA-1 for consistent results
  private hash(key: string): number {
    const hash = crypto.createHash("sha1").update(key).digest("hex")
    // Convert first 8 characters to integer for 32-bit hash space
    return Number.parseInt(hash.substring(0, 8), 16)
  }

  // Add a new node to the ring
  addNode(nodeId: string, host = "localhost", port = 6379): boolean {
    try {
      if (this.nodes.has(nodeId)) {
        console.log(`[ConsistentHash] Node ${nodeId} already exists`)
        return false
      }

      const node: ShardNode = {
        id: nodeId,
        host,
        port,
        isHealthy: true,
        keyCount: 0,
        lastSeen: Date.now(),
      }

      this.nodes.set(nodeId, node)

      // Create virtual nodes for this physical node
      for (let i = 0; i < this.virtualNodesPerNode; i++) {
        const virtualKey = `${nodeId}:${i}`
        const hash = this.hash(virtualKey)

        this.virtualNodes.push({
          hash,
          nodeId,
          virtualIndex: i,
        })
      }

      // Sort virtual nodes by hash value for binary search
      this.virtualNodes.sort((a, b) => a.hash - b.hash)

      this.updateStats()
      this.stats.rebalanceOperations++

      console.log(`[ConsistentHash] Added node ${nodeId} with ${this.virtualNodesPerNode} virtual nodes`)
      return true
    } catch (error) {
      console.error(`[ConsistentHash] Failed to add node ${nodeId}:`, error)
      return false
    }
  }

  // Remove a node from the ring
  removeNode(nodeId: string): boolean {
    try {
      if (!this.nodes.has(nodeId)) {
        console.log(`[ConsistentHash] Node ${nodeId} not found`)
        return false
      }

      this.nodes.delete(nodeId)

      // Remove all virtual nodes for this physical node
      this.virtualNodes = this.virtualNodes.filter((vnode) => vnode.nodeId !== nodeId)

      this.updateStats()
      this.stats.rebalanceOperations++

      console.log(`[ConsistentHash] Removed node ${nodeId}`)
      return true
    } catch (error) {
      console.error(`[ConsistentHash] Failed to remove node ${nodeId}:`, error)
      return false
    }
  }

  // Find the node responsible for a given key
  getNodeForKey(key: string): string | null {
    if (this.virtualNodes.length === 0) {
      return null
    }

    const keyHash = this.hash(key)

    // Binary search for the first virtual node with hash >= keyHash
    let left = 0
    let right = this.virtualNodes.length - 1
    let result = 0

    while (left <= right) {
      const mid = Math.floor((left + right) / 2)

      if (this.virtualNodes[mid].hash >= keyHash) {
        result = mid
        right = mid - 1
      } else {
        left = mid + 1
      }
    }

    // If no virtual node found with hash >= keyHash, wrap around to first node
    if (result === 0 && this.virtualNodes[0].hash < keyHash) {
      result = 0
    }

    const targetNode = this.virtualNodes[result]
    const node = this.nodes.get(targetNode.nodeId)

    // Return healthy node or find next healthy node
    if (node?.isHealthy) {
      return targetNode.nodeId
    }

    // Find next healthy node
    for (let i = 1; i < this.virtualNodes.length; i++) {
      const nextIndex = (result + i) % this.virtualNodes.length
      const nextVNode = this.virtualNodes[nextIndex]
      const nextNode = this.nodes.get(nextVNode.nodeId)

      if (nextNode?.isHealthy) {
        console.log(`[ConsistentHash] Failover: ${key} routed from ${targetNode.nodeId} to ${nextVNode.nodeId}`)
        return nextVNode.nodeId
      }
    }

    console.error(`[ConsistentHash] No healthy nodes available for key: ${key}`)
    return null
  }

  // Get all nodes in the ring
  getNodes(): ShardNode[] {
    return Array.from(this.nodes.values())
  }

  // Get healthy nodes only
  getHealthyNodes(): ShardNode[] {
    return Array.from(this.nodes.values()).filter((node) => node.isHealthy)
  }

  // Mark a node as unhealthy
  markNodeUnhealthy(nodeId: string): boolean {
    const node = this.nodes.get(nodeId)
    if (node) {
      node.isHealthy = false
      node.lastSeen = Date.now()
      this.updateStats()
      console.log(`[ConsistentHash] Marked node ${nodeId} as unhealthy`)
      return true
    }
    return false
  }

  // Mark a node as healthy
  markNodeHealthy(nodeId: string): boolean {
    const node = this.nodes.get(nodeId)
    if (node) {
      node.isHealthy = true
      node.lastSeen = Date.now()
      this.updateStats()
      console.log(`[ConsistentHash] Marked node ${nodeId} as healthy`)
      return true
    }
    return false
  }

  // Update key count for a node
  updateNodeKeyCount(nodeId: string, keyCount: number): void {
    const node = this.nodes.get(nodeId)
    if (node) {
      node.keyCount = keyCount
      node.lastSeen = Date.now()
    }
  }

  // Get sharding statistics
  getStats(): ShardingStats {
    return { ...this.stats }
  }

  // Get key distribution across nodes
  getKeyDistribution(keys: string[]): Record<string, string[]> {
    const distribution: Record<string, string[]> = {}

    // Initialize distribution for all nodes
    for (const node of this.nodes.keys()) {
      distribution[node] = []
    }

    // Distribute keys
    for (const key of keys) {
      const nodeId = this.getNodeForKey(key)
      if (nodeId && distribution[nodeId]) {
        distribution[nodeId].push(key)
      }
    }

    return distribution
  }

  // Simulate key distribution for analysis
  analyzeDistribution(sampleSize = 10000): Record<string, number> {
    const distribution: Record<string, number> = {}

    // Initialize counters
    for (const node of this.nodes.keys()) {
      distribution[node] = 0
    }

    // Generate sample keys and count distribution
    for (let i = 0; i < sampleSize; i++) {
      const sampleKey = `sample_key_${i}`
      const nodeId = this.getNodeForKey(sampleKey)
      if (nodeId) {
        distribution[nodeId]++
      }
    }

    return distribution
  }

  private updateStats(): void {
    this.stats.totalNodes = this.nodes.size
    this.stats.healthyNodes = Array.from(this.nodes.values()).filter((n) => n.isHealthy).length
    this.stats.totalVirtualNodes = this.virtualNodes.length

    // Update key distribution
    this.stats.keyDistribution = {}
    for (const [nodeId, node] of this.nodes.entries()) {
      this.stats.keyDistribution[nodeId] = node.keyCount
    }
  }

  // Clear all nodes (for testing)
  clear(): void {
    this.nodes.clear()
    this.virtualNodes = []
    this.stats = {
      totalNodes: 0,
      healthyNodes: 0,
      virtualNodesPerNode: this.virtualNodesPerNode,
      totalVirtualNodes: 0,
      keyDistribution: {},
      rebalanceOperations: 0,
    }
    console.log("[ConsistentHash] Cleared all nodes")
  }

  // Get ring visualization data
  getRingVisualization(): Array<{ hash: number; nodeId: string; virtualIndex: number }> {
    return this.virtualNodes.map((vnode) => ({
      hash: vnode.hash,
      nodeId: vnode.nodeId,
      virtualIndex: vnode.virtualIndex,
    }))
  }
}

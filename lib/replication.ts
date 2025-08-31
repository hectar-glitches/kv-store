export interface ReplicaNode {
  nodeId: string
  host: string
  port: number
  isHealthy: boolean
  isMaster: boolean
  replicationLag: number
  lastSync: number
  syncedOperations: number
}

export interface ReplicationConfig {
  replicationFactor: number
  syncReplication: boolean
  maxReplicationLag: number
  healthCheckInterval: number
}

export interface ReplicationStats {
  totalReplicas: number
  healthyReplicas: number
  replicationFactor: number
  averageLag: number
  syncOperations: number
  asyncOperations: number
  failoverEvents: number
  replicationEnabled: boolean
}

export interface ReplicationOperation {
  id: string
  type: "SET" | "DELETE" | "CLEAR"
  key?: string
  value?: string
  ttl?: number
  timestamp: number
  nodeId: string
  replicated: boolean
  replicaCount: number
}

export class ReplicationManager {
  private replicas: Map<string, ReplicaNode> = new Map()
  private config: ReplicationConfig
  private stats: ReplicationStats
  private operationLog: ReplicationOperation[] = []
  private operationCounter = 0

  constructor(config: Partial<ReplicationConfig> = {}) {
    this.config = {
      replicationFactor: 2,
      syncReplication: false,
      maxReplicationLag: 1000, // 1 second
      healthCheckInterval: 5000, // 5 seconds
      ...config,
    }

    this.stats = {
      totalReplicas: 0,
      healthyReplicas: 0,
      replicationFactor: this.config.replicationFactor,
      averageLag: 0,
      syncOperations: 0,
      asyncOperations: 0,
      failoverEvents: 0,
      replicationEnabled: true,
    }

    console.log(`[Replication] Initialized with factor: ${this.config.replicationFactor}`)
  }

  // Add a replica node
  addReplica(nodeId: string, host = "localhost", port = 6379, isMaster = false): boolean {
    try {
      if (this.replicas.has(nodeId)) {
        console.log(`[Replication] Replica ${nodeId} already exists`)
        return false
      }

      const replica: ReplicaNode = {
        nodeId,
        host,
        port,
        isHealthy: true,
        isMaster,
        replicationLag: 0,
        lastSync: Date.now(),
        syncedOperations: 0,
      }

      this.replicas.set(nodeId, replica)
      this.updateStats()

      console.log(`[Replication] Added replica ${nodeId} (master: ${isMaster})`)
      return true
    } catch (error) {
      console.error(`[Replication] Failed to add replica ${nodeId}:`, error)
      return false
    }
  }

  // Remove a replica node
  removeReplica(nodeId: string): boolean {
    try {
      const replica = this.replicas.get(nodeId)
      if (!replica) {
        console.log(`[Replication] Replica ${nodeId} not found`)
        return false
      }

      this.replicas.delete(nodeId)
      this.updateStats()

      // If master was removed, promote a healthy replica
      if (replica.isMaster) {
        this.promoteNewMaster()
      }

      console.log(`[Replication] Removed replica ${nodeId}`)
      return true
    } catch (error) {
      console.error(`[Replication] Failed to remove replica ${nodeId}:`, error)
      return false
    }
  }

  // Get replica nodes for a key (based on consistent hashing)
  getReplicasForKey(key: string, excludeNodeId?: string): string[] {
    const healthyReplicas = Array.from(this.replicas.values())
      .filter((replica) => replica.isHealthy && replica.nodeId !== excludeNodeId)
      .sort((a, b) => a.nodeId.localeCompare(b.nodeId)) // Deterministic ordering

    const replicaCount = Math.min(this.config.replicationFactor, healthyReplicas.length)

    // Use key hash to determine which replicas to use
    const keyHash = this.hashKey(key)
    const startIndex = keyHash % healthyReplicas.length
    const selectedReplicas: string[] = []

    for (let i = 0; i < replicaCount; i++) {
      const index = (startIndex + i) % healthyReplicas.length
      selectedReplicas.push(healthyReplicas[index].nodeId)
    }

    return selectedReplicas
  }

  // Replicate an operation to replica nodes
  async replicateOperation(operation: ReplicationOperation): Promise<boolean> {
    try {
      const replicas = this.getReplicasForKey(operation.key || "global", operation.nodeId)

      if (replicas.length === 0) {
        console.log(`[Replication] No replicas available for operation ${operation.id}`)
        return true // No replicas to replicate to
      }

      operation.replicaCount = replicas.length
      this.operationLog.push(operation)

      // Keep operation log size manageable
      if (this.operationLog.length > 1000) {
        this.operationLog = this.operationLog.slice(-500)
      }

      if (this.config.syncReplication) {
        // Synchronous replication - wait for all replicas
        const replicationPromises = replicas.map((replicaId) => this.replicateToNode(replicaId, operation))

        const results = await Promise.allSettled(replicationPromises)
        const successCount = results.filter((result) => result.status === "fulfilled").length

        operation.replicated = successCount > 0
        this.stats.syncOperations++

        console.log(`[Replication] Sync replication: ${successCount}/${replicas.length} replicas`)
        return successCount > 0
      } else {
        // Asynchronous replication - fire and forget
        replicas.forEach((replicaId) => {
          this.replicateToNode(replicaId, operation).catch((error) => {
            console.error(`[Replication] Async replication failed to ${replicaId}:`, error)
          })
        })

        operation.replicated = true
        this.stats.asyncOperations++
        return true
      }
    } catch (error) {
      console.error(`[Replication] Replication failed for operation ${operation.id}:`, error)
      return false
    }
  }

  // Simulate replication to a specific node
  private async replicateToNode(nodeId: string, operation: ReplicationOperation): Promise<void> {
    const replica = this.replicas.get(nodeId)
    if (!replica || !replica.isHealthy) {
      throw new Error(`Replica ${nodeId} is not healthy`)
    }

    // Simulate network delay
    await new Promise((resolve) => setTimeout(resolve, Math.random() * 50))

    // Update replica sync info
    replica.lastSync = Date.now()
    replica.syncedOperations++
    replica.replicationLag = Date.now() - operation.timestamp

    console.log(`[Replication] Replicated ${operation.type} to ${nodeId}`)
  }

  // Get master node
  getMaster(): ReplicaNode | null {
    for (const replica of this.replicas.values()) {
      if (replica.isMaster && replica.isHealthy) {
        return replica
      }
    }
    return null
  }

  // Promote a new master when current master fails
  private promoteNewMaster(): boolean {
    const healthyReplicas = Array.from(this.replicas.values())
      .filter((replica) => replica.isHealthy && !replica.isMaster)
      .sort((a, b) => b.syncedOperations - a.syncedOperations) // Most synced replica first

    if (healthyReplicas.length === 0) {
      console.error("[Replication] No healthy replicas available for master promotion")
      return false
    }

    const newMaster = healthyReplicas[0]
    newMaster.isMaster = true
    this.stats.failoverEvents++

    console.log(`[Replication] Promoted ${newMaster.nodeId} to master`)
    return true
  }

  // Mark replica as unhealthy
  markReplicaUnhealthy(nodeId: string): boolean {
    const replica = this.replicas.get(nodeId)
    if (replica) {
      replica.isHealthy = false
      this.updateStats()

      if (replica.isMaster) {
        this.promoteNewMaster()
      }

      console.log(`[Replication] Marked replica ${nodeId} as unhealthy`)
      return true
    }
    return false
  }

  // Mark replica as healthy
  markReplicaHealthy(nodeId: string): boolean {
    const replica = this.replicas.get(nodeId)
    if (replica) {
      replica.isHealthy = true
      replica.lastSync = Date.now()
      this.updateStats()
      console.log(`[Replication] Marked replica ${nodeId} as healthy`)
      return true
    }
    return false
  }

  // Get replication statistics
  getStats(): ReplicationStats {
    this.updateStats()
    return { ...this.stats }
  }

  // Get all replicas
  getReplicas(): ReplicaNode[] {
    return Array.from(this.replicas.values())
  }

  // Get recent operations
  getRecentOperations(limit = 10): ReplicationOperation[] {
    return this.operationLog.slice(-limit).reverse()
  }

  // Update configuration
  updateConfig(newConfig: Partial<ReplicationConfig>): void {
    this.config = { ...this.config, ...newConfig }
    this.stats.replicationFactor = this.config.replicationFactor
    console.log("[Replication] Configuration updated:", this.config)
  }

  private hashKey(key: string): number {
    let hash = 0
    for (let i = 0; i < key.length; i++) {
      const char = key.charCodeAt(i)
      hash = (hash << 5) - hash + char
      hash = hash & hash // Convert to 32-bit integer
    }
    return Math.abs(hash)
  }

  private updateStats(): void {
    const replicas = Array.from(this.replicas.values())
    this.stats.totalReplicas = replicas.length
    this.stats.healthyReplicas = replicas.filter((r) => r.isHealthy).length

    // Calculate average replication lag
    const healthyReplicas = replicas.filter((r) => r.isHealthy)
    if (healthyReplicas.length > 0) {
      this.stats.averageLag = healthyReplicas.reduce((sum, r) => sum + r.replicationLag, 0) / healthyReplicas.length
    } else {
      this.stats.averageLag = 0
    }
  }

  // Generate unique operation ID
  generateOperationId(): string {
    return `op_${Date.now()}_${++this.operationCounter}`
  }

  // Clear all data
  clear(): void {
    this.replicas.clear()
    this.operationLog = []
    this.operationCounter = 0
    this.stats = {
      totalReplicas: 0,
      healthyReplicas: 0,
      replicationFactor: this.config.replicationFactor,
      averageLag: 0,
      syncOperations: 0,
      asyncOperations: 0,
      failoverEvents: 0,
      replicationEnabled: true,
    }
    console.log("[Replication] Cleared all replication data")
  }
}

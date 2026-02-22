// Core Key-Value Store Engine
import { LRUCache, type CacheStats } from "./lru-cache"
import { ConsistentHashRing, type ShardingStats } from "./consistent-hash"
import { ReplicationManager, type ReplicationStats, type ReplicationOperation } from "./replication"
import { FaultToleranceManager, type FaultToleranceStats, type CircuitBreakerState } from "./fault-tolerance"
import { initStorageLayout, appendWAL } from "./storage"

export interface KVPair {
  key: string
  value: string
  timestamp: number
  ttl?: number
  accessCount: number
  lastAccessed: number
}

export interface StoreStats {
  totalKeys: number
  memoryUsage: number
  operations: number
  uptime: number
  cache: CacheStats
  cacheEnabled: boolean
  sharding: ShardingStats
  shardingEnabled: boolean
  replication: ReplicationStats
  replicationEnabled: boolean
  faultTolerance: FaultToleranceStats
  faultToleranceEnabled: boolean
}

class DistributedKVStore {
  private store: Map<string, KVPair> = new Map()
  private stats: StoreStats
  private startTime: number
  private cache: LRUCache
  private cacheEnabled = true
  private hashRing: ConsistentHashRing
  private shardingEnabled = true
  private nodeStores: Map<string, Map<string, KVPair>> = new Map()
  private replicationManager: ReplicationManager
  private replicationEnabled = true
  private faultToleranceManager: FaultToleranceManager
  private faultToleranceEnabled = true

  constructor(cacheCapacity = 1000) {
    this.startTime = Date.now()
    this.cache = new LRUCache(cacheCapacity)
    this.hashRing = new ConsistentHashRing(150) // 150 virtual nodes per physical node
    this.replicationManager = new ReplicationManager({
      replicationFactor: 2,
      syncReplication: false,
      maxReplicationLag: 1000,
    })
    this.faultToleranceManager = new FaultToleranceManager()

    this.initializeDefaultNodes()
    initStorageLayout()

    this.stats = {
      totalKeys: 0,
      memoryUsage: 0,
      operations: 0,
      uptime: 0,
      cache: this.cache.getStats(),
      cacheEnabled: this.cacheEnabled,
      sharding: this.hashRing.getStats(),
      shardingEnabled: this.shardingEnabled,
      replication: this.replicationManager.getStats(),
      replicationEnabled: this.replicationEnabled,
      faultTolerance: this.faultToleranceManager.getStats(),
      faultToleranceEnabled: this.faultToleranceEnabled,
    }
  }

  private initializeDefaultNodes(): void {
    this.hashRing.addNode("node-1", "localhost", 6379)
    this.hashRing.addNode("node-2", "localhost", 6380)
    this.hashRing.addNode("node-3", "localhost", 6381)

    // Initialize node stores
    this.nodeStores.set("node-1", new Map())
    this.nodeStores.set("node-2", new Map())
    this.nodeStores.set("node-3", new Map())

    // Initialize replication nodes
    this.replicationManager.addReplica("node-1", "localhost", 6379, true) // Master
    this.replicationManager.addReplica("node-2", "localhost", 6380, false) // Replica
    this.replicationManager.addReplica("node-3", "localhost", 6381, false) // Replica

    this.faultToleranceManager.registerNode("node-1")
    this.faultToleranceManager.registerNode("node-2")
    this.faultToleranceManager.registerNode("node-3")

    console.log("[KV-Store] Initialized with 3 default nodes, replication, and fault tolerance")
  }

  private getNodeStore(key: string): Map<string, KVPair> | null {
    if (!this.shardingEnabled) {
      return this.store
    }

    const nodeId = this.hashRing.getNodeForKey(key)
    if (!nodeId) {
      console.error(`[KV-Store] No healthy node found for key: ${key}`)
      return null
    }

    let nodeStore = this.nodeStores.get(nodeId)
    if (!nodeStore) {
      nodeStore = new Map()
      this.nodeStores.set(nodeId, nodeStore)
    }

    return nodeStore
  }

  async set(key: string, value: string, ttl?: number): Promise<boolean> {
    try {
      const now = Date.now()
      const kvPair: KVPair = {
        key,
        value,
        timestamp: now,
        ttl: ttl ? now + ttl : undefined,
        accessCount: 0,
        lastAccessed: now,
      }

      const nodeId = this.shardingEnabled ? this.hashRing.getNodeForKey(key) : "local"

      const setOperation = async () => {
        const nodeStore = this.getNodeStore(key)
        if (!nodeStore) {
          throw new Error(`No available node store for key: ${key}`)
        }
        nodeStore.set(key, kvPair)
        return true
      }

      const fallbackOperation = async () => {
        // Fallback to local store if sharding fails
        this.store.set(key, kvPair)
        console.log(`[KV-Store] Fallback: SET ${key} to local store`)
        return true
      }

      let success = false
      if (this.faultToleranceEnabled && nodeId !== "local") {
        success = await this.faultToleranceManager.executeWithFaultTolerance(
          nodeId || "unknown",
          setOperation,
          fallbackOperation,
        )
      } else {
        success = await setOperation()
      }

      if (this.cacheEnabled) {
        this.cache.set(key, value, ttl)
      }

      if (this.replicationEnabled) {
        const operation: ReplicationOperation = {
          id: this.replicationManager.generateOperationId(),
          type: "SET",
          key,
          value,
          ttl,
          timestamp: now,
          nodeId: nodeId || "unknown",
          replicated: false,
          replicaCount: 0,
        }

        await this.replicationManager.replicateOperation(operation)
      }

      this.stats.operations++
      this.updateStats()

      try {
        appendWAL({ op: "SET", key, value, ttl, ts: now })
      } catch (walErr) {
        console.error(`[KV-Store] WAL write failed for SET ${key}:`, walErr)
      }
      console.log(`[KV-Store] SET ${key} = ${value} (node: ${nodeId})`)
      return success
    } catch (error) {
      console.error(`[KV-Store] SET failed for ${key}:`, error)
      return false
    }
  }

  async get(key: string): Promise<string | null> {
    try {
      if (this.cacheEnabled) {
        const cachedValue = this.cache.get(key)
        if (cachedValue !== null) {
          this.stats.operations++
          this.updateStats()
          console.log(`[KV-Store] GET ${key} = ${cachedValue} (from cache)`)
          return cachedValue
        }
      }

      const nodeId = this.shardingEnabled ? this.hashRing.getNodeForKey(key) : "local"

      const getOperation = async () => {
        const nodeStore = this.getNodeStore(key)
        if (!nodeStore) {
          throw new Error(`No available node store for key: ${key}`)
        }

        const kvPair = nodeStore.get(key)
        if (!kvPair) {
          return null
        }

        // Check TTL expiration
        if (kvPair.ttl && Date.now() > kvPair.ttl) {
          nodeStore.delete(key)
          if (this.cacheEnabled) {
            this.cache.delete(key)
          }
          return null
        }

        // Update access statistics for LRU
        kvPair.accessCount++
        kvPair.lastAccessed = Date.now()

        return kvPair.value
      }

      const fallbackOperation = async () => {
        // Fallback to local store
        const kvPair = this.store.get(key)
        if (!kvPair || (kvPair.ttl && Date.now() > kvPair.ttl)) {
          return null
        }
        console.log(`[KV-Store] Fallback: GET ${key} from local store`)
        return kvPair.value
      }

      let result: string | null = null
      if (this.faultToleranceEnabled && nodeId !== "local") {
        result = await this.faultToleranceManager.executeWithRetry(async () => {
          return await this.faultToleranceManager.executeWithFaultTolerance(
            nodeId || "unknown",
            getOperation,
            fallbackOperation,
          )
        }, nodeId || undefined)
      } else {
        result = await getOperation()
      }

      if (result && this.cacheEnabled) {
        this.cache.set(key, result)
      }

      this.stats.operations++
      this.updateStats()
      console.log(`[KV-Store] GET ${key} = ${result} (from node: ${nodeId})`)
      return result
    } catch (error) {
      console.error(`[KV-Store] GET failed for ${key}:`, error)
      return null
    }
  }

  async delete(key: string): Promise<boolean> {
    try {
      const now = Date.now()
      const nodeId = this.shardingEnabled ? this.hashRing.getNodeForKey(key) : "local"

      const deleteOperation = async () => {
        const nodeStore = this.getNodeStore(key)
        if (!nodeStore) {
          throw new Error(`No available node store for key: ${key}`)
        }
        return nodeStore.delete(key)
      }

      const fallbackOperation = async () => {
        // Fallback to local store
        const existed = this.store.delete(key)
        console.log(`[KV-Store] Fallback: DELETE ${key} from local store`)
        return existed
      }

      let existed = false
      if (this.faultToleranceEnabled && nodeId !== "local") {
        existed = await this.faultToleranceManager.executeWithFaultTolerance(
          nodeId || "unknown",
          deleteOperation,
          fallbackOperation,
        )
      } else {
        existed = await deleteOperation()
      }

      if (this.cacheEnabled) {
        this.cache.delete(key)
      }

      if (this.replicationEnabled && existed) {
        const operation: ReplicationOperation = {
          id: this.replicationManager.generateOperationId(),
          type: "DELETE",
          key,
          timestamp: Date.now(),
          nodeId: nodeId || "unknown",
          replicated: false,
          replicaCount: 0,
        }

        await this.replicationManager.replicateOperation(operation)
      }

      this.stats.operations++
      this.updateStats()

      if (existed) {
        try {
          appendWAL({ op: "DELETE", key, ts: now })
        } catch (walErr) {
          console.error(`[KV-Store] WAL write failed for DELETE ${key}:`, walErr)
        }
      }
      console.log(`[KV-Store] DELETE ${key} = ${existed ? "OK" : "Key not found"} (node: ${nodeId})`)
      return existed
    } catch (error) {
      console.error(`[KV-Store] DELETE failed for ${key}:`, error)
      return false
    }
  }

  exists(key: string): boolean {
    if (this.cacheEnabled && this.cache.has(key)) {
      return true
    }

    const nodeStore = this.getNodeStore(key)
    if (!nodeStore) {
      return false
    }

    const kvPair = nodeStore.get(key)
    if (!kvPair) return false

    // Check TTL expiration
    if (kvPair.ttl && Date.now() > kvPair.ttl) {
      nodeStore.delete(key)
      if (this.cacheEnabled) {
        this.cache.delete(key)
      }
      this.updateStats()
      return false
    }

    return true
  }

  keys(): string[] {
    const validKeys: string[] = []
    const now = Date.now()

    if (this.shardingEnabled) {
      for (const nodeStore of this.nodeStores.values()) {
        for (const [key, kvPair] of nodeStore.entries()) {
          if (!kvPair.ttl || now <= kvPair.ttl) {
            validKeys.push(key)
          } else {
            // Clean up expired keys
            nodeStore.delete(key)
            if (this.cacheEnabled) {
              this.cache.delete(key)
            }
          }
        }
      }
    } else {
      for (const [key, kvPair] of this.store.entries()) {
        if (!kvPair.ttl || now <= kvPair.ttl) {
          validKeys.push(key)
        } else {
          // Clean up expired keys
          this.store.delete(key)
          if (this.cacheEnabled) {
            this.cache.delete(key)
          }
        }
      }
    }

    this.updateStats()
    return validKeys
  }

  getStats(): StoreStats {
    this.stats.uptime = Date.now() - this.startTime
    this.stats.cache = this.cache.getStats()
    this.stats.sharding = this.hashRing.getStats()
    this.stats.replication = this.replicationManager.getStats()
    this.stats.faultTolerance = this.faultToleranceManager.getStats()
    return { ...this.stats }
  }

  private updateStats(): void {
    if (this.shardingEnabled) {
      this.stats.totalKeys = Array.from(this.nodeStores.values()).reduce(
        (total, nodeStore) => total + nodeStore.size,
        0,
      )

      // Update memory usage across all shards
      this.stats.memoryUsage = Array.from(this.nodeStores.values()).reduce((total, nodeStore) => {
        return (
          total +
          Array.from(nodeStore.entries()).reduce((nodeTotal, [key, kvPair]) => {
            return nodeTotal + key.length + kvPair.value.length + 100 // metadata overhead
          }, 0)
        )
      }, 0)

      // Update node key counts in hash ring
      for (const [nodeId, nodeStore] of this.nodeStores.entries()) {
        this.hashRing.updateNodeKeyCount(nodeId, nodeStore.size)
      }
    } else {
      this.stats.totalKeys = this.store.size
      this.stats.memoryUsage = Array.from(this.store.entries()).reduce((total, [key, kvPair]) => {
        return total + key.length + kvPair.value.length + 100 // metadata overhead
      }, 0)
    }

    this.stats.cache = this.cache.getStats()
    this.stats.sharding = this.hashRing.getStats()
    this.stats.replication = this.replicationManager.getStats()
    this.stats.faultTolerance = this.faultToleranceManager.getStats()
  }

  // Method to get all key-value pairs (for debugging/admin)
  getAllPairs(): KVPair[] {
    const now = Date.now()
    const validPairs: KVPair[] = []

    if (this.shardingEnabled) {
      for (const nodeStore of this.nodeStores.values()) {
        for (const [key, kvPair] of nodeStore.entries()) {
          if (!kvPair.ttl || now <= kvPair.ttl) {
            validPairs.push(kvPair)
          } else {
            // Clean up expired keys
            nodeStore.delete(key)
            if (this.cacheEnabled) {
              this.cache.delete(key)
            }
          }
        }
      }
    } else {
      for (const [key, kvPair] of this.store.entries()) {
        if (!kvPair.ttl || now <= kvPair.ttl) {
          validPairs.push(kvPair)
        } else {
          this.store.delete(key)
          if (this.cacheEnabled) {
            this.cache.delete(key)
          }
        }
      }
    }

    this.updateStats()
    return validPairs
  }

  // Clear all data (for testing)
  async clear(): Promise<void> {
    if (this.shardingEnabled) {
      for (const nodeStore of this.nodeStores.values()) {
        nodeStore.clear()
      }
    } else {
      this.store.clear()
    }
    if (this.cacheEnabled) {
      this.cache.clear()
    }

    if (this.replicationEnabled) {
      const operation: ReplicationOperation = {
        id: this.replicationManager.generateOperationId(),
        type: "CLEAR",
        timestamp: Date.now(),
        nodeId: "all",
        replicated: false,
        replicaCount: 0,
      }

      await this.replicationManager.replicateOperation(operation)
    }

    this.stats.operations++
    this.updateStats()
    try {
      appendWAL({ op: "CLEAR", ts: Date.now() })
    } catch (walErr) {
      console.error("[KV-Store] WAL write failed for CLEAR:", walErr)
    }
    console.log("[KV-Store] CLEAR - All data cleared")
  }

  // Enable/Disable Cache
  enableCache(): void {
    this.cacheEnabled = true
    this.stats.cacheEnabled = true
    console.log("[KV-Store] Cache enabled")
  }

  disableCache(): void {
    this.cacheEnabled = false
    this.stats.cacheEnabled = false
    console.log("[KV-Store] Cache disabled")
  }

  setCacheCapacity(capacity: number): void {
    this.cache.setCapacity(capacity)
    this.updateStats()
    console.log(`[KV-Store] Cache capacity set to: ${capacity}`)
  }

  getCacheKeys(): string[] {
    return this.cache.getKeysInOrder()
  }

  // Node Management
  addNode(nodeId: string, host = "localhost", port = 6379): boolean {
    const success = this.hashRing.addNode(nodeId, host, port)
    if (success) {
      this.nodeStores.set(nodeId, new Map())
      this.faultToleranceManager.registerNode(nodeId)
      this.updateStats()
    }
    return success
  }

  removeNode(nodeId: string): boolean {
    const success = this.hashRing.removeNode(nodeId)
    if (success) {
      this.nodeStores.delete(nodeId)
      this.faultToleranceManager.unregisterNode(nodeId)
      this.updateStats()
    }
    return success
  }

  getNodes() {
    return this.hashRing.getNodes()
  }

  getKeyDistribution(): Record<string, string[]> {
    const allKeys = this.keys()
    return this.hashRing.getKeyDistribution(allKeys)
  }

  // Enable/Disable Sharding
  enableSharding(): void {
    this.shardingEnabled = true
    this.stats.shardingEnabled = true
    console.log("[KV-Store] Sharding enabled")
  }

  disableSharding(): void {
    this.shardingEnabled = false
    this.stats.shardingEnabled = false
    console.log("[KV-Store] Sharding disabled")
  }

  // Replication Management
  addReplica(nodeId: string, host = "localhost", port = 6379, isMaster = false): boolean {
    return this.replicationManager.addReplica(nodeId, host, port, isMaster)
  }

  removeReplica(nodeId: string): boolean {
    return this.replicationManager.removeReplica(nodeId)
  }

  getReplicas() {
    return this.replicationManager.getReplicas()
  }

  getRecentOperations(limit = 10) {
    return this.replicationManager.getRecentOperations(limit)
  }

  enableReplication(): void {
    this.replicationEnabled = true
    this.stats.replicationEnabled = true
    console.log("[KV-Store] Replication enabled")
  }

  disableReplication(): void {
    this.replicationEnabled = false
    this.stats.replicationEnabled = false
    console.log("[KV-Store] Replication disabled")
  }

  updateReplicationConfig(config: any): void {
    this.replicationManager.updateConfig(config)
    this.updateStats()
  }

  getNodeHealth() {
    return this.faultToleranceManager.getAllNodeHealth()
  }

  getHealthyNodes() {
    return this.faultToleranceManager.getHealthyNodes()
  }

  getUnhealthyNodes() {
    return this.faultToleranceManager.getUnhealthyNodes()
  }

  simulateNetworkPartition(nodeIds: string[], duration = 10000) {
    this.faultToleranceManager.simulateNetworkPartition(nodeIds, duration)
  }

  setCircuitBreakerState(nodeId: string, state: CircuitBreakerState) {
    return this.faultToleranceManager.setCircuitBreakerState(nodeId, state)
  }

  enableFaultTolerance(): void {
    this.faultToleranceEnabled = true
    this.stats.faultToleranceEnabled = true
    console.log("[KV-Store] Fault tolerance enabled")
  }

  disableFaultTolerance(): void {
    this.faultToleranceEnabled = false
    this.stats.faultToleranceEnabled = false
    console.log("[KV-Store] Fault tolerance disabled")
  }
}

// Singleton instance
let kvStoreInstance: DistributedKVStore | null = null

export function getKVStore(cacheCapacity?: number): DistributedKVStore {
  if (!kvStoreInstance) {
    kvStoreInstance = new DistributedKVStore(cacheCapacity)
    console.log("[KV-Store] Core engine initialized")
  }
  return kvStoreInstance
}

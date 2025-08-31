// LRU Cache Implementation with Doubly Linked List
export interface CacheNode {
  key: string
  value: string
  timestamp: number
  ttl?: number
  prev: CacheNode | null
  next: CacheNode | null
}

export interface CacheStats {
  size: number
  capacity: number
  hits: number
  misses: number
  evictions: number
  hitRate: number
}

export class LRUCache {
  private capacity: number
  private cache: Map<string, CacheNode> = new Map()
  private head: CacheNode
  private tail: CacheNode
  private stats: CacheStats

  constructor(capacity = 1000) {
    this.capacity = capacity
    this.stats = {
      size: 0,
      capacity,
      hits: 0,
      misses: 0,
      evictions: 0,
      hitRate: 0,
    }

    // Create dummy head and tail nodes for easier list manipulation
    this.head = {
      key: "",
      value: "",
      timestamp: 0,
      prev: null,
      next: null,
    }
    this.tail = {
      key: "",
      value: "",
      timestamp: 0,
      prev: null,
      next: null,
    }

    this.head.next = this.tail
    this.tail.prev = this.head

    console.log(`[LRU-Cache] Initialized with capacity: ${capacity}`)
  }

  get(key: string): string | null {
    const node = this.cache.get(key)

    if (!node) {
      this.stats.misses++
      this.updateHitRate()
      console.log(`[LRU-Cache] MISS ${key}`)
      return null
    }

    // Check TTL expiration
    if (node.ttl && Date.now() > node.ttl) {
      this.delete(key)
      this.stats.misses++
      this.updateHitRate()
      console.log(`[LRU-Cache] MISS ${key} (expired)`)
      return null
    }

    // Move to head (most recently used)
    this.moveToHead(node)
    this.stats.hits++
    this.updateHitRate()
    console.log(`[LRU-Cache] HIT ${key}`)
    return node.value
  }

  set(key: string, value: string, ttl?: number): void {
    const existingNode = this.cache.get(key)

    if (existingNode) {
      // Update existing node
      existingNode.value = value
      existingNode.timestamp = Date.now()
      existingNode.ttl = ttl ? Date.now() + ttl : undefined
      this.moveToHead(existingNode)
      console.log(`[LRU-Cache] UPDATE ${key}`)
      return
    }

    // Create new node
    const newNode: CacheNode = {
      key,
      value,
      timestamp: Date.now(),
      ttl: ttl ? Date.now() + ttl : undefined,
      prev: null,
      next: null,
    }

    // Add to cache
    this.cache.set(key, newNode)
    this.addToHead(newNode)
    this.stats.size++

    // Check capacity and evict if necessary
    if (this.cache.size > this.capacity) {
      const tail = this.removeTail()
      if (tail) {
        this.cache.delete(tail.key)
        this.stats.size--
        this.stats.evictions++
        console.log(`[LRU-Cache] EVICTED ${tail.key}`)
      }
    }

    console.log(`[LRU-Cache] SET ${key}`)
  }

  delete(key: string): boolean {
    const node = this.cache.get(key)

    if (!node) {
      return false
    }

    this.cache.delete(key)
    this.removeNode(node)
    this.stats.size--
    console.log(`[LRU-Cache] DELETE ${key}`)
    return true
  }

  has(key: string): boolean {
    const node = this.cache.get(key)
    if (!node) return false

    // Check TTL expiration
    if (node.ttl && Date.now() > node.ttl) {
      this.delete(key)
      return false
    }

    return true
  }

  clear(): void {
    this.cache.clear()
    this.head.next = this.tail
    this.tail.prev = this.head
    this.stats.size = 0
    this.stats.hits = 0
    this.stats.misses = 0
    this.stats.evictions = 0
    this.stats.hitRate = 0
    console.log("[LRU-Cache] CLEAR - All cache cleared")
  }

  getStats(): CacheStats {
    return { ...this.stats }
  }

  keys(): string[] {
    const validKeys: string[] = []
    const now = Date.now()

    for (const [key, node] of this.cache.entries()) {
      if (!node.ttl || now <= node.ttl) {
        validKeys.push(key)
      } else {
        // Clean up expired keys
        this.delete(key)
      }
    }

    return validKeys
  }

  // Get keys in LRU order (most recent first)
  getKeysInOrder(): string[] {
    const keys: string[] = []
    let current = this.head.next

    while (current && current !== this.tail) {
      const now = Date.now()
      if (!current.ttl || now <= current.ttl) {
        keys.push(current.key)
      }
      current = current.next
    }

    return keys
  }

  private addToHead(node: CacheNode): void {
    node.prev = this.head
    node.next = this.head.next

    if (this.head.next) {
      this.head.next.prev = node
    }
    this.head.next = node
  }

  private removeNode(node: CacheNode): void {
    if (node.prev) {
      node.prev.next = node.next
    }
    if (node.next) {
      node.next.prev = node.prev
    }
  }

  private moveToHead(node: CacheNode): void {
    this.removeNode(node)
    this.addToHead(node)
  }

  private removeTail(): CacheNode | null {
    const lastNode = this.tail.prev
    if (lastNode && lastNode !== this.head) {
      this.removeNode(lastNode)
      return lastNode
    }
    return null
  }

  private updateHitRate(): void {
    const total = this.stats.hits + this.stats.misses
    this.stats.hitRate = total > 0 ? (this.stats.hits / total) * 100 : 0
  }

  // Method to resize cache capacity
  setCapacity(newCapacity: number): void {
    this.capacity = newCapacity
    this.stats.capacity = newCapacity

    // Evict excess items if new capacity is smaller
    while (this.cache.size > this.capacity) {
      const tail = this.removeTail()
      if (tail) {
        this.cache.delete(tail.key)
        this.stats.size--
        this.stats.evictions++
      }
    }

    console.log(`[LRU-Cache] Capacity updated to: ${newCapacity}`)
  }
}

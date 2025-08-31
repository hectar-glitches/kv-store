export interface CircuitBreakerConfig {
  failureThreshold: number
  recoveryTimeout: number
  monitoringWindow: number
  halfOpenMaxCalls: number
}

export interface RetryConfig {
  maxRetries: number
  baseDelay: number
  maxDelay: number
  backoffMultiplier: number
}

export interface HealthCheckConfig {
  interval: number
  timeout: number
  unhealthyThreshold: number
  healthyThreshold: number
}

export interface FaultToleranceStats {
  circuitBreakerTrips: number
  totalRetries: number
  successfulRecoveries: number
  healthCheckFailures: number
  networkPartitions: number
  gracefulDegradations: number
  averageResponseTime: number
  errorRate: number
}

export enum CircuitBreakerState {
  CLOSED = "CLOSED",
  OPEN = "OPEN",
  HALF_OPEN = "HALF_OPEN",
}

export interface NodeHealth {
  nodeId: string
  isHealthy: boolean
  lastHealthCheck: number
  consecutiveFailures: number
  consecutiveSuccesses: number
  responseTime: number
  errorCount: number
  circuitBreakerState: CircuitBreakerState
}

export class CircuitBreaker {
  private state: CircuitBreakerState = CircuitBreakerState.CLOSED
  private failureCount = 0
  private lastFailureTime = 0
  private halfOpenCalls = 0
  private config: CircuitBreakerConfig

  constructor(config: CircuitBreakerConfig) {
    this.config = config
  }

  async execute<T>(operation: () => Promise<T>): Promise<T> {
    if (this.state === CircuitBreakerState.OPEN) {
      if (Date.now() - this.lastFailureTime > this.config.recoveryTimeout) {
        this.state = CircuitBreakerState.HALF_OPEN
        this.halfOpenCalls = 0
        console.log("[CircuitBreaker] Transitioning to HALF_OPEN state")
      } else {
        throw new Error("Circuit breaker is OPEN")
      }
    }

    if (this.state === CircuitBreakerState.HALF_OPEN && this.halfOpenCalls >= this.config.halfOpenMaxCalls) {
      throw new Error("Circuit breaker HALF_OPEN call limit exceeded")
    }

    try {
      const result = await operation()
      this.onSuccess()
      return result
    } catch (error) {
      this.onFailure()
      throw error
    }
  }

  private onSuccess(): void {
    this.failureCount = 0
    if (this.state === CircuitBreakerState.HALF_OPEN) {
      this.halfOpenCalls++
      if (this.halfOpenCalls >= this.config.halfOpenMaxCalls) {
        this.state = CircuitBreakerState.CLOSED
        console.log("[CircuitBreaker] Recovered - transitioning to CLOSED state")
      }
    }
  }

  private onFailure(): void {
    this.failureCount++
    this.lastFailureTime = Date.now()

    if (this.state === CircuitBreakerState.HALF_OPEN) {
      this.state = CircuitBreakerState.OPEN
      console.log("[CircuitBreaker] Failed during HALF_OPEN - transitioning to OPEN state")
    } else if (this.failureCount >= this.config.failureThreshold) {
      this.state = CircuitBreakerState.OPEN
      console.log(`[CircuitBreaker] Failure threshold reached (${this.failureCount}) - OPENING circuit`)
    }
  }

  getState(): CircuitBreakerState {
    return this.state
  }

  getFailureCount(): number {
    return this.failureCount
  }

  reset(): void {
    this.state = CircuitBreakerState.CLOSED
    this.failureCount = 0
    this.lastFailureTime = 0
    this.halfOpenCalls = 0
  }
}

export class FaultToleranceManager {
  private nodeHealthMap: Map<string, NodeHealth> = new Map()
  private circuitBreakers: Map<string, CircuitBreaker> = new Map()
  private stats: FaultToleranceStats
  private healthCheckInterval: NodeJS.Timeout | null = null
  private config: {
    circuitBreaker: CircuitBreakerConfig
    retry: RetryConfig
    healthCheck: HealthCheckConfig
  }

  constructor() {
    this.config = {
      circuitBreaker: {
        failureThreshold: 5,
        recoveryTimeout: 30000, // 30 seconds
        monitoringWindow: 60000, // 1 minute
        halfOpenMaxCalls: 3,
      },
      retry: {
        maxRetries: 3,
        baseDelay: 100,
        maxDelay: 5000,
        backoffMultiplier: 2,
      },
      healthCheck: {
        interval: 5000, // 5 seconds
        timeout: 2000, // 2 seconds
        unhealthyThreshold: 3,
        healthyThreshold: 2,
      },
    }

    this.stats = {
      circuitBreakerTrips: 0,
      totalRetries: 0,
      successfulRecoveries: 0,
      healthCheckFailures: 0,
      networkPartitions: 0,
      gracefulDegradations: 0,
      averageResponseTime: 0,
      errorRate: 0,
    }

    this.startHealthChecking()
    console.log("[FaultTolerance] Initialized with health monitoring")
  }

  // Register a node for health monitoring
  registerNode(nodeId: string): void {
    if (!this.nodeHealthMap.has(nodeId)) {
      this.nodeHealthMap.set(nodeId, {
        nodeId,
        isHealthy: true,
        lastHealthCheck: Date.now(),
        consecutiveFailures: 0,
        consecutiveSuccesses: 0,
        responseTime: 0,
        errorCount: 0,
        circuitBreakerState: CircuitBreakerState.CLOSED,
      })

      this.circuitBreakers.set(nodeId, new CircuitBreaker(this.config.circuitBreaker))
      console.log(`[FaultTolerance] Registered node ${nodeId} for monitoring`)
    }
  }

  // Unregister a node
  unregisterNode(nodeId: string): void {
    this.nodeHealthMap.delete(nodeId)
    this.circuitBreakers.delete(nodeId)
    console.log(`[FaultTolerance] Unregistered node ${nodeId}`)
  }

  // Execute operation with fault tolerance
  async executeWithFaultTolerance<T>(
    nodeId: string,
    operation: () => Promise<T>,
    fallbackOperation?: () => Promise<T>,
  ): Promise<T> {
    const circuitBreaker = this.circuitBreakers.get(nodeId)
    if (!circuitBreaker) {
      throw new Error(`Node ${nodeId} not registered for fault tolerance`)
    }

    try {
      const startTime = Date.now()
      const result = await circuitBreaker.execute(operation)
      const responseTime = Date.now() - startTime

      this.updateNodeHealth(nodeId, true, responseTime)
      return result
    } catch (error) {
      this.updateNodeHealth(nodeId, false)

      // Try fallback operation if available
      if (fallbackOperation) {
        try {
          console.log(`[FaultTolerance] Executing fallback for node ${nodeId}`)
          this.stats.gracefulDegradations++
          return await fallbackOperation()
        } catch (fallbackError) {
          console.error(`[FaultTolerance] Fallback failed for node ${nodeId}:`, fallbackError)
        }
      }

      throw error
    }
  }

  // Execute operation with retry logic
  async executeWithRetry<T>(operation: () => Promise<T>, nodeId?: string): Promise<T> {
    let lastError: Error | null = null
    let delay = this.config.retry.baseDelay

    for (let attempt = 0; attempt <= this.config.retry.maxRetries; attempt++) {
      try {
        if (attempt > 0) {
          console.log(`[FaultTolerance] Retry attempt ${attempt} for ${nodeId || "operation"}`)
          await this.sleep(delay)
          delay = Math.min(delay * this.config.retry.backoffMultiplier, this.config.retry.maxDelay)
        }

        const result = await operation()
        if (attempt > 0) {
          this.stats.successfulRecoveries++
        }
        return result
      } catch (error) {
        lastError = error as Error
        this.stats.totalRetries++

        if (nodeId) {
          this.updateNodeHealth(nodeId, false)
        }

        console.log(`[FaultTolerance] Attempt ${attempt + 1} failed: ${lastError.message}`)
      }
    }

    throw lastError || new Error("Max retries exceeded")
  }

  // Update node health status
  private updateNodeHealth(nodeId: string, success: boolean, responseTime = 0): void {
    const health = this.nodeHealthMap.get(nodeId)
    if (!health) return

    health.lastHealthCheck = Date.now()
    health.responseTime = responseTime

    if (success) {
      health.consecutiveFailures = 0
      health.consecutiveSuccesses++

      if (!health.isHealthy && health.consecutiveSuccesses >= this.config.healthCheck.healthyThreshold) {
        health.isHealthy = true
        this.stats.successfulRecoveries++
        console.log(`[FaultTolerance] Node ${nodeId} recovered to healthy state`)
      }
    } else {
      health.consecutiveSuccesses = 0
      health.consecutiveFailures++
      health.errorCount++

      if (health.isHealthy && health.consecutiveFailures >= this.config.healthCheck.unhealthyThreshold) {
        health.isHealthy = false
        this.stats.healthCheckFailures++
        console.log(`[FaultTolerance] Node ${nodeId} marked as unhealthy`)
      }
    }

    // Update circuit breaker state
    const circuitBreaker = this.circuitBreakers.get(nodeId)
    if (circuitBreaker) {
      health.circuitBreakerState = circuitBreaker.getState()
    }
  }

  // Start health checking
  private startHealthChecking(): void {
    this.healthCheckInterval = setInterval(() => {
      this.performHealthChecks()
    }, this.config.healthCheck.interval)
  }

  // Perform health checks on all nodes
  private async performHealthChecks(): Promise<void> {
    const healthCheckPromises = Array.from(this.nodeHealthMap.keys()).map((nodeId) =>
      this.performNodeHealthCheck(nodeId),
    )

    await Promise.allSettled(healthCheckPromises)
    this.updateStats()
  }

  // Perform health check on a specific node
  private async performNodeHealthCheck(nodeId: string): Promise<void> {
    try {
      const startTime = Date.now()

      // Simulate health check (in real implementation, this would be an actual network call)
      await this.simulateHealthCheck(nodeId)

      const responseTime = Date.now() - startTime
      this.updateNodeHealth(nodeId, true, responseTime)
    } catch (error) {
      this.updateNodeHealth(nodeId, false)
      console.log(`[FaultTolerance] Health check failed for node ${nodeId}`)
    }
  }

  // Simulate health check (replace with actual implementation)
  private async simulateHealthCheck(nodeId: string): Promise<void> {
    // Simulate network delay and potential failures
    await this.sleep(Math.random() * 100)

    // Simulate occasional failures for demonstration
    if (Math.random() < 0.05) {
      // 5% failure rate
      throw new Error(`Health check failed for ${nodeId}`)
    }
  }

  // Get node health status
  getNodeHealth(nodeId: string): NodeHealth | null {
    return this.nodeHealthMap.get(nodeId) || null
  }

  // Get all node health statuses
  getAllNodeHealth(): NodeHealth[] {
    return Array.from(this.nodeHealthMap.values())
  }

  // Get healthy nodes
  getHealthyNodes(): string[] {
    return Array.from(this.nodeHealthMap.entries())
      .filter(([, health]) => health.isHealthy)
      .map(([nodeId]) => nodeId)
  }

  // Get unhealthy nodes
  getUnhealthyNodes(): string[] {
    return Array.from(this.nodeHealthMap.entries())
      .filter(([, health]) => !health.isHealthy)
      .map(([nodeId]) => nodeId)
  }

  // Force circuit breaker state
  setCircuitBreakerState(nodeId: string, state: CircuitBreakerState): boolean {
    const circuitBreaker = this.circuitBreakers.get(nodeId)
    if (circuitBreaker) {
      if (state === CircuitBreakerState.CLOSED) {
        circuitBreaker.reset()
      }
      const health = this.nodeHealthMap.get(nodeId)
      if (health) {
        health.circuitBreakerState = state
      }
      return true
    }
    return false
  }

  // Get fault tolerance statistics
  getStats(): FaultToleranceStats {
    return { ...this.stats }
  }

  // Update statistics
  private updateStats(): void {
    const allNodes = Array.from(this.nodeHealthMap.values())
    const healthyNodes = allNodes.filter((node) => node.isHealthy)

    if (healthyNodes.length > 0) {
      this.stats.averageResponseTime =
        healthyNodes.reduce((sum, node) => sum + node.responseTime, 0) / healthyNodes.length
    }

    const totalOperations = this.stats.totalRetries + this.stats.successfulRecoveries
    if (totalOperations > 0) {
      this.stats.errorRate = (this.stats.totalRetries / totalOperations) * 100
    }

    // Count circuit breaker trips
    this.stats.circuitBreakerTrips = Array.from(this.circuitBreakers.values()).filter(
      (cb) => cb.getState() === CircuitBreakerState.OPEN,
    ).length
  }

  // Simulate network partition
  simulateNetworkPartition(nodeIds: string[], duration = 10000): void {
    console.log(`[FaultTolerance] Simulating network partition for nodes: ${nodeIds.join(", ")}`)
    this.stats.networkPartitions++

    nodeIds.forEach((nodeId) => {
      const health = this.nodeHealthMap.get(nodeId)
      if (health) {
        health.isHealthy = false
        health.consecutiveFailures = this.config.healthCheck.unhealthyThreshold
      }
    })

    // Recover after duration
    setTimeout(() => {
      console.log(`[FaultTolerance] Network partition recovered for nodes: ${nodeIds.join(", ")}`)
      nodeIds.forEach((nodeId) => {
        const health = this.nodeHealthMap.get(nodeId)
        if (health) {
          health.consecutiveFailures = 0
          health.consecutiveSuccesses = this.config.healthCheck.healthyThreshold
          health.isHealthy = true
        }
      })
    }, duration)
  }

  // Update configuration
  updateConfig(newConfig: Partial<typeof this.config>): void {
    this.config = { ...this.config, ...newConfig }
    console.log("[FaultTolerance] Configuration updated")
  }

  // Utility function for sleep
  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms))
  }

  // Cleanup
  destroy(): void {
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval)
      this.healthCheckInterval = null
    }
    this.nodeHealthMap.clear()
    this.circuitBreakers.clear()
    console.log("[FaultTolerance] Destroyed and cleaned up resources")
  }
}

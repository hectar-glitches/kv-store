"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Badge } from "@/components/ui/badge"
import { Separator } from "@/components/ui/separator"
import { Activity, Database, Server, Zap, Network } from "lucide-react"

interface KVPair {
  key: string
  value: string
  timestamp: number
  ttl?: number
}

interface StoreStats {
  totalKeys: number
  memoryUsage: number
  operations: number
  uptime: number
  cache: {
    size: number
    capacity: number
    hits: number
    misses: number
    evictions: number
    hitRate: number
  }
  cacheEnabled: boolean
  sharding: {
    totalNodes: number
    healthyNodes: number
    virtualNodesPerNode: number
    totalVirtualNodes: number
    keyDistribution: Record<string, number>
    rebalanceOperations: number
  }
  shardingEnabled: boolean
  replication: {
    totalReplicas: number
    healthyReplicas: number
    replicationFactor: number
    averageLag: number
    syncOperations: number
    asyncOperations: number
    failoverEvents: number
    replicationEnabled: boolean
  }
  replicationEnabled: boolean
  faultTolerance: {
    circuitBreakerTrips: number
    totalRetries: number
    successfulRecoveries: number
    healthCheckFailures: number
    networkPartitions: number
    gracefulDegradations: number
    averageResponseTime: number
    errorRate: number
  }
  faultToleranceEnabled: boolean
}

export default function DistributedKVStore() {
  const [key, setKey] = useState("")
  const [value, setValue] = useState("")
  const [getKey, setGetKey] = useState("")
  const [result, setResult] = useState<string | null>(null)
  const [stats, setStats] = useState<StoreStats>({
    totalKeys: 0,
    memoryUsage: 0,
    operations: 0,
    uptime: 0,
    cache: {
      size: 0,
      capacity: 0,
      hits: 0,
      misses: 0,
      evictions: 0,
      hitRate: 0,
    },
    cacheEnabled: false,
    sharding: {
      totalNodes: 0,
      healthyNodes: 0,
      virtualNodesPerNode: 0,
      totalVirtualNodes: 0,
      keyDistribution: {},
      rebalanceOperations: 0,
    },
    shardingEnabled: false,
    replication: {
      totalReplicas: 0,
      healthyReplicas: 0,
      replicationFactor: 0,
      averageLag: 0,
      syncOperations: 0,
      asyncOperations: 0,
      failoverEvents: 0,
      replicationEnabled: false,
    },
    replicationEnabled: false,
    faultTolerance: {
      circuitBreakerTrips: 0,
      totalRetries: 0,
      successfulRecoveries: 0,
      healthCheckFailures: 0,
      networkPartitions: 0,
      gracefulDegradations: 0,
      averageResponseTime: 0,
      errorRate: 0,
    },
    faultToleranceEnabled: false,
  })
  const [recentOperations, setRecentOperations] = useState<string[]>([])
  const [isLoading, setIsLoading] = useState(false)

  useEffect(() => {
    fetchStats()
    const interval = setInterval(fetchStats, 2000)
    return () => clearInterval(interval)
  }, [])

  const fetchStats = async () => {
    try {
      const response = await fetch("/api/kvstore/stats")
      const data = await response.json()
      setStats(data)
    } catch (error) {
      console.error("Failed to fetch stats:", error)
    }
  }

  const handleSet = async () => {
    if (!key || !value) return

    setIsLoading(true)
    try {
      const response = await fetch("/api/kvstore/set", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ key, value }),
      })

      const data = await response.json()
      if (data.success) {
        setResult(`SET ${key} = ${value}`)
        addOperation(`SET ${key}`)
        setKey("")
        setValue("")
        fetchStats()
      }
    } catch (error) {
      setResult("Error: Failed to set key")
    } finally {
      setIsLoading(false)
    }
  }

  const handleGet = async () => {
    if (!getKey) return

    setIsLoading(true)
    try {
      const response = await fetch(`/api/kvstore/get?key=${encodeURIComponent(getKey)}`)
      const data = await response.json()

      if (data.success) {
        setResult(`GET ${getKey} = ${data.value || "null"}`)
        addOperation(`GET ${getKey}`)
      } else {
        setResult(`GET ${getKey} = null (key not found)`)
      }
    } catch (error) {
      setResult("Error: Failed to get key")
    } finally {
      setIsLoading(false)
    }
  }

  const handleDelete = async () => {
    if (!getKey) return

    setIsLoading(true)
    try {
      const response = await fetch("/api/kvstore/delete", {
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ key: getKey }),
      })

      const data = await response.json()
      setResult(`DELETE ${getKey} = ${data.success ? "OK" : "Key not found"}`)
      addOperation(`DELETE ${getKey}`)
      fetchStats()
    } catch (error) {
      setResult("Error: Failed to delete key")
    } finally {
      setIsLoading(false)
    }
  }

  const addOperation = (operation: string) => {
    setRecentOperations((prev) => [`${new Date().toLocaleTimeString()}: ${operation}`, ...prev.slice(0, 9)])
  }

  return (
    <div className="min-h-screen bg-background p-6">
      <div className="max-w-7xl mx-auto space-y-6">
        {/* Navigation */}
        <div className="flex justify-between items-center">
          <div className="space-x-4">
            <a href="/" className="text-blue-600 hover:underline">Home</a>
            <a href="/dashboard" className="text-blue-600 hover:underline">Dashboard</a>
          </div>
        </div>

        {/* Header */}
        <div className="text-center space-y-2">
          <h1 className="text-4xl font-bold text-foreground">Distributed Key-Value Store</h1>
          <p className="text-muted-foreground">
            Redis-like distributed storage with LRU cache, consistent hash sharding, replication, and fault tolerance
          </p>
        </div>

        {/* Stats Dashboard */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Total Keys</CardTitle>
              <Database className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{stats.totalKeys}</div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Memory Usage</CardTitle>
              <Server className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{(stats.memoryUsage / 1024).toFixed(1)}KB</div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Operations</CardTitle>
              <Zap className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{stats.operations}</div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Uptime</CardTitle>
              <Activity className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{Math.floor(stats.uptime / 1000)}s</div>
            </CardContent>
          </Card>
        </div>

        {/* LRU Cache Statistics */}
        <Card>
          <CardHeader>
            <CardTitle>LRU Cache Statistics</CardTitle>
            <CardDescription>Real-time cache performance metrics</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 md:grid-cols-6 gap-4">
              <div className="text-center">
                <div className="text-2xl font-bold text-blue-600">{stats.cache?.size || 0}</div>
                <div className="text-sm text-muted-foreground">Cache Size</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-gray-600">{stats.cache?.capacity || 0}</div>
                <div className="text-sm text-muted-foreground">Capacity</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-green-600">{stats.cache?.hits || 0}</div>
                <div className="text-sm text-muted-foreground">Cache Hits</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-red-600">{stats.cache?.misses || 0}</div>
                <div className="text-sm text-muted-foreground">Cache Misses</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-orange-600">{stats.cache?.evictions || 0}</div>
                <div className="text-sm text-muted-foreground">Evictions</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-purple-600">{(stats.cache?.hitRate || 0).toFixed(1)}%</div>
                <div className="text-sm text-muted-foreground">Hit Rate</div>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Sharding Statistics */}
        <Card>
          <CardHeader>
            <CardTitle>Consistent Hash Sharding</CardTitle>
            <CardDescription>Distributed key placement and node health</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 md:grid-cols-6 gap-4 mb-4">
              <div className="text-center">
                <div className="text-2xl font-bold text-indigo-600">{stats.sharding?.totalNodes || 0}</div>
                <div className="text-sm text-muted-foreground">Total Nodes</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-green-600">{stats.sharding?.healthyNodes || 0}</div>
                <div className="text-sm text-muted-foreground">Healthy Nodes</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-cyan-600">{stats.sharding?.virtualNodesPerNode || 0}</div>
                <div className="text-sm text-muted-foreground">Virtual/Node</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-teal-600">{stats.sharding?.totalVirtualNodes || 0}</div>
                <div className="text-sm text-muted-foreground">Total Virtual</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-amber-600">{stats.sharding?.rebalanceOperations || 0}</div>
                <div className="text-sm text-muted-foreground">Rebalances</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-rose-600">
                  {stats.sharding?.totalNodes > 0
                    ? ((stats.sharding.healthyNodes / stats.sharding.totalNodes) * 100).toFixed(0)
                    : 0}
                  %
                </div>
                <div className="text-sm text-muted-foreground">Health Rate</div>
              </div>
            </div>

            {/* Key Distribution */}
            {stats.sharding?.keyDistribution && Object.keys(stats.sharding.keyDistribution).length > 0 && (
              <div>
                <Label className="text-sm font-medium mb-2 block">Key Distribution Across Nodes</Label>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-2">
                  {Object.entries(stats.sharding.keyDistribution).map(([nodeId, keyCount]) => (
                    <div key={nodeId} className="flex items-center justify-between p-2 bg-muted/50 rounded">
                      <div className="flex items-center gap-2">
                        <Network className="h-4 w-4 text-muted-foreground" />
                        <span className="text-sm font-mono">{nodeId}</span>
                      </div>
                      <Badge variant="outline">{keyCount} keys</Badge>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </CardContent>
        </Card>

        {/* Replication Statistics */}
        <Card>
          <CardHeader>
            <CardTitle>Replication Protocol</CardTitle>
            <CardDescription>Data durability and high availability metrics</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 md:grid-cols-7 gap-4">
              <div className="text-center">
                <div className="text-2xl font-bold text-emerald-600">{stats.replication?.totalReplicas || 0}</div>
                <div className="text-sm text-muted-foreground">Total Replicas</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-green-600">{stats.replication?.healthyReplicas || 0}</div>
                <div className="text-sm text-muted-foreground">Healthy Replicas</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-blue-600">{stats.replication?.replicationFactor || 0}</div>
                <div className="text-sm text-muted-foreground">Replication Factor</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-yellow-600">
                  {(stats.replication?.averageLag || 0).toFixed(0)}ms
                </div>
                <div className="text-sm text-muted-foreground">Avg Lag</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-cyan-600">{stats.replication?.syncOperations || 0}</div>
                <div className="text-sm text-muted-foreground">Sync Ops</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-indigo-600">{stats.replication?.asyncOperations || 0}</div>
                <div className="text-sm text-muted-foreground">Async Ops</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-red-600">{stats.replication?.failoverEvents || 0}</div>
                <div className="text-sm text-muted-foreground">Failovers</div>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Fault Tolerance Statistics */}
        <Card>
          <CardHeader>
            <CardTitle>Fault Tolerance Layer</CardTitle>
            <CardDescription>Circuit breakers, health monitoring, and resilience metrics</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 md:grid-cols-8 gap-4">
              <div className="text-center">
                <div className="text-2xl font-bold text-red-600">{stats.faultTolerance?.circuitBreakerTrips || 0}</div>
                <div className="text-sm text-muted-foreground">Circuit Trips</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-orange-600">{stats.faultTolerance?.totalRetries || 0}</div>
                <div className="text-sm text-muted-foreground">Total Retries</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-green-600">
                  {stats.faultTolerance?.successfulRecoveries || 0}
                </div>
                <div className="text-sm text-muted-foreground">Recoveries</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-yellow-600">
                  {stats.faultTolerance?.healthCheckFailures || 0}
                </div>
                <div className="text-sm text-muted-foreground">Health Failures</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-purple-600">{stats.faultTolerance?.networkPartitions || 0}</div>
                <div className="text-sm text-muted-foreground">Partitions</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-blue-600">
                  {stats.faultTolerance?.gracefulDegradations || 0}
                </div>
                <div className="text-sm text-muted-foreground">Degradations</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-teal-600">
                  {(stats.faultTolerance?.averageResponseTime || 0).toFixed(0)}ms
                </div>
                <div className="text-sm text-muted-foreground">Avg Response</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-rose-600">
                  {(stats.faultTolerance?.errorRate || 0).toFixed(1)}%
                </div>
                <div className="text-sm text-muted-foreground">Error Rate</div>
              </div>
            </div>
          </CardContent>
        </Card>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Operations Panel */}
          <Card>
            <CardHeader>
              <CardTitle>Key-Value Operations</CardTitle>
              <CardDescription>Perform SET, GET, and DELETE operations</CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              {/* SET Operation */}
              <div className="space-y-3">
                <Label className="text-sm font-medium">SET Operation</Label>
                <div className="flex gap-2">
                  <Input placeholder="Key" value={key} onChange={(e) => setKey(e.target.value)} className="flex-1" />
                  <Input
                    placeholder="Value"
                    value={value}
                    onChange={(e) => setValue(e.target.value)}
                    className="flex-1"
                  />
                  <Button onClick={handleSet} disabled={isLoading || !key || !value}>
                    SET
                  </Button>
                </div>
              </div>

              <Separator />

              {/* GET/DELETE Operations */}
              <div className="space-y-3">
                <Label className="text-sm font-medium">GET / DELETE Operations</Label>
                <div className="flex gap-2">
                  <Input
                    placeholder="Key to retrieve or delete"
                    value={getKey}
                    onChange={(e) => setGetKey(e.target.value)}
                    className="flex-1"
                  />
                  <Button onClick={handleGet} disabled={isLoading || !getKey} variant="outline">
                    GET
                  </Button>
                  <Button onClick={handleDelete} disabled={isLoading || !getKey} variant="destructive">
                    DELETE
                  </Button>
                </div>
              </div>

              {/* Result Display */}
              {result && (
                <div className="p-3 bg-muted rounded-md">
                  <Label className="text-sm font-medium">Result:</Label>
                  <p className="font-mono text-sm mt-1">{result}</p>
                </div>
              )}
            </CardContent>
          </Card>

          {/* Recent Operations */}
          <Card>
            <CardHeader>
              <CardTitle>Recent Operations</CardTitle>
              <CardDescription>Live feed of recent key-value operations</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-2 max-h-80 overflow-y-auto">
                {recentOperations.length === 0 ? (
                  <p className="text-muted-foreground text-sm">No operations yet</p>
                ) : (
                  recentOperations.map((op, index) => (
                    <div key={index} className="flex items-center gap-2 p-2 bg-muted/50 rounded text-sm">
                      <Badge variant="outline" className="text-xs">
                        {op.split(":")[1]?.trim().split(" ")[0] || "OP"}
                      </Badge>
                      <span className="font-mono text-xs">{op}</span>
                    </div>
                  ))
                )}
              </div>
            </CardContent>
          </Card>
        </div>

        {/* System Status */}
        <Card>
          <CardHeader>
            <CardTitle>System Status</CardTitle>
            <CardDescription>Current system health and configuration</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="flex items-center gap-2">
                <Badge variant="default">Core Engine</Badge>
                <span className="text-sm text-green-600">Active</span>
              </div>
              <div className="flex items-center gap-2">
                <Badge variant="default">LRU Cache</Badge>
                <span className="text-sm text-green-600">Active</span>
              </div>
              <div className="flex items-center gap-2">
                <Badge variant="default">Consistent Hashing</Badge>
                <span className="text-sm text-green-600">Active</span>
              </div>
              <div className="flex items-center gap-2">
                <Badge variant="default">Replication Protocol</Badge>
                <span className="text-sm text-green-600">Active</span>
              </div>
              <div className="flex items-center gap-2">
                <Badge variant="default">Fault Tolerance</Badge>
                <span className="text-sm text-green-600">Active</span>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

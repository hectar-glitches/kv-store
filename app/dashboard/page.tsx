"use client"

import React, { useEffect, useState } from "react"

export default function Dashboard() {
  const [stats, setStats] = useState<any>(null)
  const [keys, setKeys] = useState<string[]>([])
  const [selectedKey, setSelectedKey] = useState<string>("")
  const [value, setValue] = useState<string>("")
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    fetchStats()
    fetchKeys()
  }, [])

  async function fetchStats() {
    setLoading(true)
    try {
      const res = await fetch("/api/kvstore/stats")
      setStats(await res.json())
    } catch {}
    setLoading(false)
  }

  async function fetchKeys() {
    setLoading(true)
    try {
      const res = await fetch("/api/kvstore/keys")
      const data = await res.json()
      setKeys(data.keys || [])
    } catch {}
    setLoading(false)
  }

  async function fetchValue(key: string) {
    setSelectedKey(key)
    setLoading(true)
    try {
      const res = await fetch(`/api/kvstore/get?key=${key}`)
      const data = await res.json()
      setValue(data.value || "")
    } catch {}
    setLoading(false)
  }

  return (
    <div className="p-8">
      <h1 className="text-2xl font-bold mb-4">Distributed KV Store Dashboard</h1>
      <div className="mb-6">
        <button onClick={fetchStats} className="mr-2">Refresh Stats</button>
        <button onClick={fetchKeys}>Refresh Keys</button>
      </div>
      <div className="grid grid-cols-2 gap-8">
        <div>
          <h2 className="font-semibold mb-2">Keys</h2>
          <ul className="border rounded p-2 h-64 overflow-auto">
            {keys.map((key) => (
              <li key={key} className="mb-1">
                <button onClick={() => fetchValue(key)} className="underline text-blue-600">{key}</button>
              </li>
            ))}
          </ul>
        </div>
        <div>
          <h2 className="font-semibold mb-2">Selected Key</h2>
          <div className="border rounded p-2 h-64">
            <div className="mb-2">Key: <span className="font-mono">{selectedKey}</span></div>
            <div>Value: <span className="font-mono">{value}</span></div>
          </div>
        </div>
      </div>
      <div className="mt-8">
        <h2 className="font-semibold mb-2">Stats</h2>
        <pre className="border rounded p-2 bg-gray-50 text-xs h-48 overflow-auto">{JSON.stringify(stats, null, 2)}</pre>
      </div>
    </div>
  )
}

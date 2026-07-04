import { useState, useEffect } from 'react'
import { PieChart, Pie, Cell, Tooltip, ResponsiveContainer, Legend } from 'recharts'

const DEVICE_COLORS = {
  mobile:  '#7c7cff',
  desktop: '#2dd4bf',
  tablet:  '#ffb84d'
}

const SOURCE_COLORS = {
  organic: '#2dd4bf',
  paid:    '#7c7cff',
  email:   '#ffb84d',
  direct:  '#f87171',
  social:  '#a78bfa'
}

const DEFAULT_COLORS = ['#7c7cff','#2dd4bf','#ffb84d','#f87171','#a78bfa','#34d399']

export default function DeviceStats({ defaultView = 'device' }) {
  const [data, setData] = useState({ by_device: [], by_source: [] })

  async function fetchDevices() {
    try {
      const res  = await fetch('/api/stats/devices')
      const json = await res.json()
      setData(json)
    } catch (err) {
      console.warn('device stats fetch failed', err)
    }
  }

  useEffect(() => {
    fetchDevices()
    const interval = setInterval(fetchDevices, 5000)
    return () => clearInterval(interval)
  }, [])

  const chartData = defaultView === 'device' ? data.by_device : data.by_source
  const colorMap  = defaultView === 'device' ? DEVICE_COLORS  : SOURCE_COLORS
  const title     = defaultView === 'device' ? 'traffic by device' : 'traffic by source'

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>{title}</h2>
        <span style={{ fontSize: 10, color: 'var(--muted)', fontFamily: 'DM Mono, monospace' }}>
        </span>
      </div>

      {chartData.length === 0 ? (
        <p className="empty">waiting for session data...</p>
      ) : (
        <ResponsiveContainer width="100%" height={210}>
          <PieChart>
            <Pie
              data={chartData}
              dataKey="value"
              nameKey="name"
              cx="50%"
              cy="50%"
              innerRadius={45}
              outerRadius={72}
              paddingAngle={3}
            >
              {chartData.map((entry, i) => (
                <Cell
                  key={i}
                  fill={colorMap[entry.name] || DEFAULT_COLORS[i % DEFAULT_COLORS.length]}
                />
              ))}
            </Pie>
            <Tooltip
              formatter={(val, name) => [val.toLocaleString(), name]}
              contentStyle={{
                background:   '#11131a',
                border:       '1px solid rgba(255,255,255,0.08)',
                borderRadius:  8,
                color:        '#f4f7ff',
                fontSize:      12
              }}
            />
            <Legend
              formatter={value => (
                <span style={{ fontSize: 11, color: '#8a90a8' }}>{value}</span>
              )}
            />
          </PieChart>
        </ResponsiveContainer>
      )}
    </div>
  )
}
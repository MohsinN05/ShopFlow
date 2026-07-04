import { useState, useEffect } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, Tooltip,
  ResponsiveContainer, Cell, CartesianGrid
} from 'recharts'

const COLORS = ['#7c7cff','#2dd4bf','#ffb84d','#f87171','#a78bfa','#34d399','#60a5fa']

export default function CategorySales() {
  const [data, setData] = useState([])

  async function fetchCategories() {
    try {
      const res  = await fetch('/api/stats/top-categories')
      const json = await res.json()
      setData(json)
    } catch (err) {
      console.warn('category fetch failed', err)
    }
  }

  useEffect(() => {
    fetchCategories()
    const interval = setInterval(fetchCategories, 5000)
    return () => clearInterval(interval)
  }, [])

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>sales by category</h2>
        <span style={{ fontSize: 10, color: 'var(--muted)', fontFamily: 'DM Mono, monospace' }}>
          live · 
        </span>
      </div>

      {data.length === 0 ? (
        <p className="empty">waiting for streamed events...</p>
      ) : (
        <ResponsiveContainer width="100%" height={200}>
          <BarChart
            data={data}
            layout="vertical"
            margin={{ top: 4, right: 60, left: 70, bottom: 4 }}
          >
            <CartesianGrid stroke="rgba(255,255,255,0.04)" horizontal={false} />
            <XAxis
              type="number"
              tick={{ fill: '#8a90a8', fontSize: 10 }}
              axisLine={false}
              tickLine={false}
              tickFormatter={v => v > 1000 ? `$${(v/1000).toFixed(0)}k` : `$${v}`}
            />
            <YAxis
              type="category"
              dataKey="category"
              tick={{ fill: '#8a90a8', fontSize: 11 }}
              axisLine={false}
              tickLine={false}
              width={70}
            />
            <Tooltip
              formatter={(val) => [`$${val.toLocaleString()}`, 'Revenue']}
              contentStyle={{
                background:   '#11131a',
                border:       '1px solid rgba(255,255,255,0.08)',
                borderRadius:  8,
                color:        '#f4f7ff',
                fontSize:      12
              }}
            />
            <Bar dataKey="total_revenue" radius={[0, 4, 4, 0]}>
              {data.map((_, i) => (
                <Cell key={i} fill={COLORS[i % COLORS.length]} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      )}
    </div>
  )
}
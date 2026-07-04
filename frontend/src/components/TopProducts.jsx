import { useState, useEffect } from 'react'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, CartesianGrid } from 'recharts'

const SCORE_MAP = { page_view: 1, add_to_cart: 5, checkout: 7, purchase: 10 }

export default function TopProducts({ events }) {
  const [productNames, setProductNames] = useState({})

  // Fetch product name map once
  useEffect(() => {
    async function fetchNames() {
      try {
        const res = await fetch('/api/recommend/top?limit=200')
        const data = await res.json()
        const map = {}
        data.forEach(p => { map[p.product_id] = p.name })
        setProductNames(map)
      } catch (err) {
        console.warn('product name fetch failed', err)
      }
    }
    fetchNames()
  }, [])

  const agg = {}
  events.forEach(ev => {
    const id = ev.product_id
    agg[id] = (agg[id] || 0) + (SCORE_MAP[ev.event_type] || 1)
  })

  const data = Object.entries(agg)
    .map(([id, score]) => ({
      name: productNames[parseInt(id)] || `product_${id}`,
      score
    }))
    .sort((a, b) => b.score - a.score)
    .slice(0, 8)

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>top products</h2>
      </div>
      {data.length === 0 ? (
        <p className="empty">waiting for events...</p>
      ) : (
        <ResponsiveContainer width="100%" height={220}>
          <BarChart data={data} margin={{ top: 8, right: 8, left: -16, bottom: 40 }}>
            <CartesianGrid stroke="rgba(255,255,255,0.06)" vertical={false} />
            <XAxis
              dataKey="name"
              tick={{ fill: '#8a90a8', fontSize: 10 }}
              axisLine={false}
              tickLine={false}
              angle={-25}
              textAnchor="end"
              interval={0}
            />
            <YAxis tick={{ fill: '#8a90a8', fontSize: 11 }} axisLine={false} tickLine={false} />
            <Tooltip
              cursor={{ fill: 'rgba(124,124,255,0.08)' }}
              contentStyle={{
                background: '#11131a',
                border: '1px solid rgba(255,255,255,0.08)',
                borderRadius: 8,
                color: '#f4f7ff'
              }}
            />
            <Bar dataKey="score" radius={[4, 4, 0, 0]}>
              {data.map((_, i) => (
                <Cell key={i} fill={i === 0 ? '#7c7cff' : i === 1 ? '#2dd4bf' : '#ffb84d'} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      )}
    </div>
  )
}
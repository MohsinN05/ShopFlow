import { useMemo } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, Tooltip,
  ResponsiveContainer, Cell
} from 'recharts'

const MUTED = '#2a2a35'

const SCORE_MAP = {
  page_view: 1,
  add_to_cart: 5,
  checkout: 7,
  purchase: 10
}

export default function TopProducts({ events }) {

  const data = useMemo(() => {
    const agg = {}

    events.forEach(ev => {
      const id = ev.product_id
      agg[id] = (agg[id] || 0) + (SCORE_MAP[ev.event_type] || 1)
    })

    return Object.entries(agg)
      .map(([id, score]) => ({
        name: `product_${id}`,
        score
      }))
      .sort((a, b) => b.score - a.score)
      .slice(0, 8)

  }, [events])

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>top products</h2>
      </div>

      {data.length === 0 ? (
        <p className="empty">waiting for events...</p>
      ) : (
        <ResponsiveContainer width="100%" height={220}>
          <BarChart data={data}>
            <XAxis dataKey="name" />
            <YAxis />
            <Tooltip />
            <Bar dataKey="score">
              {data.map((_, i) => (
                <Cell key={i} fill={MUTED} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      )}
    </div>
  )
}
import { useState } from 'react'
import EventStream from './components/EventStream'
import RecommendationPanel from './components/RecommendationPanel'
import TopProducts from './components/TopProducts'

const SCORE_MAP = {
  page_view: 1,
  add_to_cart: 5,
  checkout: 7,
  purchase: 10
}

export default function App() {
  const [allEvents, setAllEvents] = useState([])
  const [totalEvents, setTotalEvents] = useState(0)
  const [purchases, setPurchases] = useState(0)
  const [activeUsers, setActiveUsers] = useState(new Set())

  function handleNewEvent(ev) {
    setAllEvents(prev => [ev, ...prev].slice(0, 200))
    setTotalEvents(n => n + 1)
    if (ev.event_type === 'purchase') setPurchases(n => n + 1)
    setActiveUsers(prev => new Set([...prev, ev.user_id]))
  }

  const avgScore = allEvents.length
    ? Math.round(
        allEvents.reduce((sum, ev) => sum + (SCORE_MAP[ev.event_type] || 1), 0) / allEvents.length
      )
    : 0

  const metrics = [
    { label: 'total events', value: totalEvents },
    { label: 'active users', value: activeUsers.size },
    { label: 'purchases', value: purchases },
    { label: 'avg score', value: avgScore, accent: true }
  ]

  return (
    <div className="app">
      <header className="topbar">
        <div className="logo">
          <span className="pulse" />
          RecoStream
        </div>
        <span className="badge">kafka:9092 ●</span>
      </header>

      <div className="metric-bar">
        {metrics.map(m => (
          <div className="metric" key={m.label}>
            <span className={`metric-val ${m.accent ? 'metric-accent' : ''}`}>{m.value}</span>
            <span className="metric-lbl">{m.label}</span>
          </div>
        ))}
      </div>

      <main className="grid">
        <div className="col-left">
          <EventStream onNewEvent={handleNewEvent} />
          <TopProducts events={allEvents} />
        </div>
        <div className="col-right">
          <RecommendationPanel />
        </div>
      </main>
    </div>
  )
}

import { useState, useEffect } from 'react'
import EventStream         from './components/EventStream'
import RecommendationPanel from './components/RecommendationPanel'
import TopProducts         from './components/TopProducts'
import SalesTrend          from './components/SalesTrend'
import CountryMap          from './components/CountryMap'
import CategorySales       from './components/CategorySales'
import TopUsers            from './components/TopUsers'
import DeviceStats         from './components/DeviceStats'

export default function App() {
  const [allEvents,  setAllEvents]  = useState([])
  const [dailyStats, setDailyStats] = useState({
    total_events: 0, active_users: 0, purchases: 0, avg_score: 0, data_date: null
  })

  function handleNewEvent(ev) {
    setAllEvents(prev => [ev, ...prev].slice(0, 200))
  }

  async function fetchDailyStats() {
    try {
      const res  = await fetch('/api/stats/daily')
      const data = await res.json()
      setDailyStats(data)
    } catch (err) {
      console.warn('daily stats fetch failed', err)
    }
  }

  useEffect(() => {
    fetchDailyStats()
    const interval = setInterval(fetchDailyStats, 5000)
    return () => clearInterval(interval)
  }, [])

  const metrics = [
    { label: `events · ${dailyStats.data_date || '...'}`, value: dailyStats.total_events, icon: '⚡' },
    { label: 'active users',                              value: dailyStats.active_users,  icon: '👤' },
    { label: 'purchases',                                 value: dailyStats.purchases,     icon: '🛒' },
    { label: 'avg score', accent: true,                   value: dailyStats.avg_score,     icon: '📈' }
  ]

  return (
    <div className="app">

      {/* ── Topbar ── */}
      <header className="topbar">
        <div className="logo">
          <span className="pulse" />
          <div>
            <div className="logo-title">ShopFlow</div>
            <div className="logo-subtitle">real-time commerce pulse</div>
          </div>
        </div>
        <span className="badge">kafka:9092 ●</span>
      </header>

      {/* ── KPI Strip ── */}
      <div className="metric-bar">
        {metrics.map(m => (
          <div className="metric" key={m.label}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
              <span className={`metric-val ${m.accent ? 'metric-accent' : ''}`}>{m.value}</span>
              <span style={{ fontSize: 18, opacity: 0.4 }}>{m.icon}</span>
            </div>
            <span className="metric-lbl">{m.label}</span>
          </div>
        ))}
      </div>

      {/* ── Main dashboard ── */}
      <main className="dashboard">

        {/* ── Right column: Recs + Device + Source ── */}
        <div className="area-right-col">
          <RecommendationPanel />
          <DeviceStats defaultView="device" />
          <DeviceStats defaultView="source" />
        </div>

        {/* Row 1: Event stream */}
        <div className="area-stream">
          <EventStream onNewEvent={handleNewEvent} />
        </div>

        {/* Row 2: Sales trend */}
        <div className="area-trend">
          <SalesTrend />
        </div>

        {/* Row 3: Top products + Top users */}
        <div className="area-middle">
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 14 }}>
            <TopProducts events={allEvents} />
            <TopUsers />
          </div>
        </div>

        {/* Row 4: Category sales + Country map */}
        <div className="area-bottom">
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1.6fr', gap: 14 }}>
            <CategorySales />
            <CountryMap />
          </div>
        </div>

      </main>
    </div>
  )
}
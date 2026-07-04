import { useState, useEffect, useRef } from 'react'

const TYPE_COLORS = {
  page_view:   '#6b6b80',
  add_to_cart: '#ef9f27',
  checkout:    '#7f77dd',
  purchase:    '#1d9e75'
}

const SCORE_MAP = { page_view: 1, add_to_cart: 5, checkout: 7, purchase: 10 }

export default function EventStream({ onNewEvent }) {
  const [events,    setEvents]    = useState([])
  const [connected, setConnected] = useState(false)
  const [eventDate, setEventDate] = useState(null)
  const seenIds = useRef(new Set())

  useEffect(() => {
    const ws = new WebSocket('ws://localhost:8000/ws/events')

    ws.onopen = () => setConnected(true)

    ws.onmessage = (msg) => {
      const data = JSON.parse(msg.data)

      setEvents(prev => {
        const combined = [...data, ...prev]

        // Sort chronologically — latest on top
        combined.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))

        // Show the date of the most recent event
        if (combined.length > 0) {
          const latest = combined[0].timestamp
          const date = new Date(latest)
          setEventDate(date.toLocaleDateString('en-GB', {
            day:   '2-digit',
            month: 'short',
            year:  'numeric'
          }))
        }

        return combined.slice(0, 100)
      })

      // Fire onNewEvent only for unseen events
      data.forEach(ev => {
        const key = `${ev.user_id}-${ev.product_id}-${ev.timestamp}`
        if (!seenIds.current.has(key)) {
          seenIds.current.add(key)
          onNewEvent?.({ ...ev, score: SCORE_MAP[ev.event_type] || 1 })
        }
      })
    }

    ws.onerror = () => setConnected(false)
    ws.onclose = () => setConnected(false)

    return () => ws.close()
  }, [])

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>live event stream</h2>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          {eventDate && (
            <span style={{
              fontSize: 11,
              fontFamily: 'DM Mono, monospace',
              color: 'var(--muted)'
            }}>
              {eventDate}
            </span>
          )}
          <span className="badge" style={{
            fontSize:   11,
            background: connected ? 'rgba(29,158,117,0.15)' : 'rgba(226,75,74,0.15)',
            color:      connected ? '#1d9e75' : '#e24b4a',
            border:     `0.5px solid ${connected ? '#1d9e75' : '#e24b4a'}`
          }}>
            {connected ? '● live' : '○ disconnected'}
          </span>
        </div>
      </div>

      <div className="event-list">
        {events.length === 0 && (
          <p className="empty">waiting for events from Kafka...</p>
        )}
        {events.map((ev, i) => (
          <div
            key={i}
            className="event-row"
            style={{ borderLeftColor: TYPE_COLORS[ev.event_type] || '#6b6b80' }}
          >
            <span className="ev-time">{ev.timestamp?.slice(11, 19)}</span>
            <div className="ev-body">
              <span className="ev-main">user_{ev.user_id} → product_{ev.product_id}</span>
            </div>
            <span
              className="ev-badge"
              style={{
                background: (TYPE_COLORS[ev.event_type] || '#6b6b80') + '22',
                color: TYPE_COLORS[ev.event_type] || '#6b6b80'
              }}
            >
              {ev.event_type}
            </span>
            <span className="ev-score">+{SCORE_MAP[ev.event_type] || 1}</span>
          </div>
        ))}
      </div>
    </div>
  )
}
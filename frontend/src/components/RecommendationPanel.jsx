import { useState, useEffect, useRef } from 'react'
import { fetchRecommendations } from '../api'

export default function RecommendationPanel() {
  const [inputVal, setInputVal] = useState('')
  const [userId, setUserId] = useState(null)
  const [recs, setRecs] = useState([])
  const [loading, setLoading] = useState(false)
  const debounceRef = useRef(null)

  async function loadRecs(uid, signal) {
  setLoading(true)
  try {
    const url = uid ? `/api/recommend/${uid}` : `/api/recommend/top`
    const res = await fetch(url, { signal })
    const data = await res.json()

if (!Array.isArray(data)) {
  console.error("Invalid API response:", data)
  setRecs([])
  return
}

setRecs(data)
    setRecs(data)
  } catch (err) {
    if (err.name !== "AbortError") {
      console.error("recommendation fetch failed:", err)
      setRecs([])
    }
  } finally {
    setLoading(false)
  }
}

  // Debounce search input
  function handleInput(e) {
    const val = e.target.value
    setInputVal(val)
    clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => {
      const parsed = parseInt(val)
      const uid = !isNaN(parsed) && val.trim() !== '' ? parsed : null
      setUserId(uid)
    }, 400)
  }

  function clearSearch() {
    setInputVal('')
    setUserId(null)
  }

  // Poll every 3 seconds
  useEffect(() => {
  const controller = new AbortController()

  const poll = async () => {
    await loadRecs(userId, controller.signal)
  }

  poll()
  const interval = setInterval(poll, 3000)

  return () => {
    controller.abort()
    clearInterval(interval)
  }
}, [userId])

  const maxScore = recs?.length ? Math.max(...recs.map(r => Number(r.score) || 0)): 1

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>recommendations</h2>
        <span style={{ fontSize: 11, color: 'var(--muted)' }}>
          {userId ? `user_${userId}` : 'all users'}
        </span>
      </div>

      {/* Search bar */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
        <div style={{ position: 'relative', flex: 1 }}>
          <input
            type="number"
            placeholder="enter user id (1 – 20000)"
            value={inputVal}
            onChange={handleInput}
            style={{
              width: '100%',
              background: 'var(--surface2)',
              border: '0.5px solid var(--border)',
              borderRadius: 4,
              padding: '6px 32px 6px 10px',
              color: 'var(--text)',
              fontSize: 12,
              fontFamily: 'DM Mono, monospace'
            }}
          />
          {inputVal && (
            <span
              onClick={clearSearch}
              style={{
                position: 'absolute', right: 8, top: '50%',
                transform: 'translateY(-50%)',
                cursor: 'pointer', color: 'var(--muted)', fontSize: 14
              }}
            >✕</span>
          )}
        </div>
      </div>

      {loading && recs.length === 0 && <p className="empty">loading...</p>}
      {!loading && recs.length === 0 && (
        <p className="empty">
          {userId ? `no recommendations for user_${userId}` : 'no data yet'}
        </p>
      )}

      <div className="rec-list">
        {recs.map((r, i) => (
          <div className="rec-row" key={`${r.user_id ?? 'global'}-${r.product_id}`}>
            <span className={`rank ${i < 3 ? 'rank-top' : ''}`}>#{i + 1}</span>
            <div className="rec-info">
              <span className="rec-name">{r.name}</span>
              <span className="rec-cat">{r.category}</span>
            </div>
            <div className="score-bar-wrap">
              <div
                className="score-bar"
                style={{ width: `${Math.round((r.score / maxScore) * 100)}%` }}
              />
            </div>
            <span className="score-val">{r.score}</span>
          </div>
        ))}
      </div>
    </div>
  )
}
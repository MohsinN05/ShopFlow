import { useState, useEffect } from 'react'

const MEDALS = ['🥇', '🥈', '🥉']

export default function TopUsers() {
  const [users, setUsers] = useState([])

  useEffect(() => {
    async function fetch_() {
      try {
        const res  = await fetch('/api/stats/top-users')
        const json = await res.json()
        setUsers(json)
      } catch (err) {
        console.warn('top users fetch failed', err)
      }
    }
    fetch_()
    const interval = setInterval(fetch_, 10000)
    return () => clearInterval(interval)
  }, [])

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>top users</h2>
      </div>
      {users.length === 0 ? (
        <p className="empty">loading...</p>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          {users.map((u, i) => (
            <div key={u.user_id} style={{
              display: 'grid',
              gridTemplateColumns: '24px 1fr auto',
              alignItems: 'center',
              gap: 10,
              padding: '8px 10px',
              background: 'var(--surface)',
              borderRadius: 6,
              border: '0.5px solid var(--border)'
            }}>
              <span style={{ fontSize: 16 }}>{MEDALS[i]}</span>
              <div>
                <div style={{ fontSize: 13, fontWeight: 500 }}>{u.name}</div>
                <div style={{ fontSize: 11, color: 'var(--muted)' }}>{u.country} · user_{u.user_id}</div>
              </div>
              <span style={{
                fontFamily: 'DM Mono, monospace',
                fontSize: 12,
                color: '#7c7cff'
              }}>{u.total_score}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
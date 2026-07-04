import { useState, useEffect } from 'react'
import {
  LineChart, Line, XAxis, YAxis, Tooltip,
  ResponsiveContainer, CartesianGrid, Legend
} from 'recharts'

const MONTH_NAMES = ['','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
const GRANULARITIES = ['hour', 'day', 'month', 'year']

export default function SalesTrend() {
  const [data,        setData]        = useState([])
  const [granularity, setGranularity] = useState('month')
  const [year,        setYear]        = useState('')
  const [month,       setMonth]       = useState('')
  const [day,         setDay]         = useState('')
  const [loading,     setLoading]     = useState(false)
  const [availYears,  setAvailYears]  = useState([])
  const [availMonths, setAvailMonths] = useState([])
  const [availDays,   setAvailDays]   = useState([])

  useEffect(() => {
    async function fetchFilters() {
      try {
        const res  = await fetch('/api/stats/sales-trend/filters')
        const json = await res.json()
        setAvailYears(json.years  || [])
        setAvailMonths(json.months || [])
        setAvailDays(json.days   || [])
      } catch (err) {
        console.warn('filter fetch failed', err)
      }
    }
    fetchFilters()
  }, [])

  async function fetchTrend() {
    setLoading(true)
    try {
      const params = new URLSearchParams({ granularity })
      if (year  && granularity !== 'year')  params.append('year',  year)
      if (month && granularity === 'hour')  params.append('month', month)
      if (month && granularity === 'day')   params.append('month', month)
      if (day   && granularity === 'hour')  params.append('day',   day)
      const res  = await fetch(`/api/stats/sales-trend?${params}`)
      const json = await res.json()
      setData(json.map(r => ({ ...r, period: formatPeriod(r.period, granularity) })))
    } catch (err) {
      console.warn('sales trend fetch failed', err)
    } finally {
      setLoading(false)
    }
  }

  // Auto-refresh every 5 seconds
  useEffect(() => {
    fetchTrend()
    const interval = setInterval(fetchTrend, 5000)
    return () => clearInterval(interval)
  }, [granularity, year, month, day])

  function formatPeriod(iso, gran) {
    const d = new Date(iso)
    if (gran === 'hour')  return `${d.getUTCHours()}:00`
    if (gran === 'day')   return `${d.getUTCDate()} ${MONTH_NAMES[d.getUTCMonth() + 1]}`
    if (gran === 'month') return `${MONTH_NAMES[d.getUTCMonth() + 1]} ${d.getUTCFullYear()}`
    if (gran === 'year')  return `${d.getUTCFullYear()}`
    return iso
  }

  function handleGranularity(g) {
    setGranularity(g)
    // Reset filters that don't apply to new granularity
    if (g === 'year')  { setYear(''); setMonth(''); setDay('') }
    if (g === 'month') { setMonth(''); setDay('') }
    if (g === 'day')   { setDay('') }
  }

  const selectStyle = {
    background:   'var(--surface2)',
    border:       '0.5px solid var(--border)',
    borderRadius:  4,
    color:        'var(--text)',
    fontSize:      11,
    fontFamily:   'DM Mono, monospace',
    padding:      '3px 6px',
    cursor:       'pointer'
  }

  // Show only relevant filters per granularity
  const showYear  = granularity !== 'year'
  const showMonth = granularity === 'day' || granularity === 'hour'
  const showDay   = granularity === 'hour'

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>sales trend</h2>
        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', alignItems: 'center' }}>

          <select style={selectStyle} value={granularity} onChange={e => handleGranularity(e.target.value)}>
            {GRANULARITIES.map(g => <option key={g} value={g}>{g}</option>)}
          </select>

          {showYear && (
            <select style={selectStyle} value={year} onChange={e => setYear(e.target.value)}>
              <option value="">all years</option>
              {availYears.map(y => <option key={y} value={y}>{y}</option>)}
            </select>
          )}

          {showMonth && (
            <select style={selectStyle} value={month} onChange={e => setMonth(e.target.value)}>
              <option value="">all months</option>
              {availMonths.map(m => <option key={m} value={m}>{MONTH_NAMES[m]}</option>)}
            </select>
          )}

          {showDay && (
            <select style={selectStyle} value={day} onChange={e => setDay(e.target.value)}>
              <option value="">all days</option>
              {availDays.map(d => <option key={d} value={d}>{d}</option>)}
            </select>
          )}

          <span style={{ fontSize: 10, color: 'var(--muted)', fontFamily: 'DM Mono, monospace' }}>
            5s
          </span>
        </div>
      </div>

      {loading && data.length === 0 && <p className="empty">loading...</p>}
      {!loading && data.length === 0 && <p className="empty">no data for selected range</p>}

      {data.length > 0 && (
        <ResponsiveContainer width="100%" height={220}>
          <LineChart data={data} margin={{ top: 8, right: 12, left: -16, bottom: 4 }}>
            <CartesianGrid stroke="rgba(255,255,255,0.04)" vertical={false} />
            <XAxis
              dataKey="period"
              tick={{ fill: '#8a90a8', fontSize: 10 }}
              axisLine={false}
              tickLine={false}
            />
            <YAxis
              yAxisId="left"
              tick={{ fill: '#8a90a8', fontSize: 10 }}
              axisLine={false}
              tickLine={false}
              tickFormatter={v => `$${v >= 1000 ? (v/1000).toFixed(0)+'k' : v}`}
            />
            <YAxis
              yAxisId="right"
              orientation="right"
              tick={{ fill: '#8a90a8', fontSize: 10 }}
              axisLine={false}
              tickLine={false}
            />
            <Tooltip
              contentStyle={{
                background:   '#11131a',
                border:       '1px solid rgba(255,255,255,0.08)',
                borderRadius:  8,
                color:        '#f4f7ff',
                fontSize:      12
              }}
              formatter={(val, name) =>
                name === 'Revenue (USD)' ? [`$${val.toLocaleString()}`, name] : [val, name]
              }
            />
            <Legend wrapperStyle={{ fontSize: 11, color: '#8a90a8' }} />
            <Line yAxisId="left"  type="monotone" dataKey="revenue"     stroke="#7c7cff" strokeWidth={2} dot={false} name="Revenue (USD)" />
            <Line yAxisId="right" type="monotone" dataKey="order_count" stroke="#2dd4bf" strokeWidth={2} dot={false} name="Orders" />
          </LineChart>
        </ResponsiveContainer>
      )}
    </div>
  )
}
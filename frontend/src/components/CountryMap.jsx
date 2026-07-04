import { useState, useEffect } from 'react'
import { ComposableMap, Geographies, Geography, ZoomableGroup } from 'react-simple-maps'
import { scaleLinear } from 'd3-scale'

const GEO_URL = 'https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json'

// ISO alpha-2 → numeric map for world-atlas matching
const COUNTRY_NAME_MAP = {
  US: 'United States of America', GB: 'United Kingdom', DE: 'Germany',
  FR: 'France', IN: 'India', JP: 'Japan', BR: 'Brazil', CN: 'China',
  CA: 'Canada', AU: 'Australia', NL: 'Netherlands', ES: 'Spain',
  IT: 'Italy', MX: 'Mexico', KR: 'South Korea', PL: 'Poland',
  SE: 'Sweden', NO: 'Norway', RU: 'Russia', ZA: 'South Africa',
  NG: 'Nigeria', EG: 'Egypt', AR: 'Argentina', CL: 'Chile',
  TR: 'Turkey', SA: 'Saudi Arabia', PK: 'Pakistan', ID: 'Indonesia',
  TH: 'Thailand', MY: 'Malaysia', PH: 'Philippines',
  SG: 'Singapore', AE: 'United Arab Emirates',
  NZ: 'New Zealand', PT: 'Portugal', BE: 'Belgium', CH: 'Switzerland',
  AT: 'Austria', DK: 'Denmark', FI: 'Finland', CZ: 'Czech Republic',
}

export default function CountryMap() {
  const [countryData, setCountryData] = useState({})
  const [tooltip, setTooltip]         = useState(null)
  const [max, setMax]                 = useState(1)

  useEffect(() => {
    async function fetch_() {
      try {
        const res  = await fetch('/api/stats/active-countries')
        const json = await res.json()
        const map  = {}
        let maxVal = 1
        json.forEach(r => {
          const fullName = COUNTRY_NAME_MAP[r.country] || r.country
          map[fullName]  = r.active_users
          if (r.active_users > maxVal) maxVal = r.active_users
        })
        setCountryData(map)
        setMax(maxVal)
      } catch (err) {
        console.warn('country map fetch failed', err)
      }
    }
    fetch_()
    const interval = setInterval(fetch_, 15000)
    return () => clearInterval(interval)
  }, [])

  const colorScale = scaleLinear()
    .domain([0, max])
    .range(['#1c1c2e', '#7c7cff'])

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>active users by country</h2>
      </div>

      {tooltip && (
        <div style={{
          fontSize: 11,
          color: 'var(--muted)',
          marginBottom: 6,
          fontFamily: 'DM Mono, monospace'
        }}>
          {tooltip.name}: <span style={{ color: '#7c7cff' }}>{tooltip.users} users</span>
        </div>
      )}

      <ComposableMap
        projection="geoMercator"
        style={{ width: '100%', height: 200 }}
        projectionConfig={{ scale: 90, center: [0, 20] }}
      >
        <ZoomableGroup>
          <Geographies geography={GEO_URL}>
            {({ geographies }) =>
              geographies.map(geo => {
                const name  = geo.properties.name
                const users = countryData[name] || 0
                return (
                  <Geography
                    key={geo.rsmKey}
                    geography={geo}
                    fill={users > 0 ? colorScale(users) : '#1c1c2e'}
                    stroke="#2a2a35"
                    strokeWidth={0.3}
                    style={{
                      default:  { outline: 'none' },
                      hover:    { fill: '#2dd4bf', outline: 'none', cursor: 'pointer' },
                      pressed:  { outline: 'none' }
                    }}
                    onMouseEnter={() => setTooltip({ name, users })}
                    onMouseLeave={() => setTooltip(null)}
                  />
                )
              })
            }
          </Geographies>
        </ZoomableGroup>
      </ComposableMap>
    </div>
  )
}
const BASE = '/api'

export async function fetchRecommendations(userId) {
  const res = await fetch(`${BASE}/recommend/${userId}`)
  if (!res.ok) throw new Error('Failed to fetch recommendations')
  return res.json()
}

export async function fetchSearch(query, minPrice = 0, maxPrice = 1e9) {
  const params = new URLSearchParams({ query, min_price: minPrice, max_price: maxPrice })
  const res = await fetch(`${BASE}/search?${params}`)
  if (!res.ok) throw new Error('Search failed')
  return res.json()
}

export async function logEvent(userId, productId, eventType = 'page_view') {
  const params = new URLSearchParams({
    user_id: userId,
    product_id: productId,
    event_type: eventType
  })
  const res = await fetch(`${BASE}/log-click?${params}`, { method: 'POST' })
  if (!res.ok) throw new Error('Event log failed')
  return res.json()
}

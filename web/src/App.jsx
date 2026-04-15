import React, { useState, useEffect, useMemo } from 'react'
import {
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from 'recharts'

const COLORS = ['#2563eb', '#7c3aed', '#059669', '#d97706', '#dc2626', '#0891b2', '#4f46e5', '#be185d']
const ROUND_COLORS = {
  seed: '#f59e0b', 'pre-a': '#fb923c', a: '#3b82f6', b: '#6366f1',
  c: '#8b5cf6', d: '#a855f7', debt: '#ef4444', strategic: '#10b981',
  angel: '#ec4899', unknown: '#9ca3af',
}

function formatAmount(val) {
  if (!val) return '-'
  if (val >= 10000) return `${(val / 10000).toFixed(1)}億円`
  return `${val}万円`
}

function RoundBadge({ type }) {
  const cls = `badge badge-${type === 'pre-a' ? 'a' : type || 'unknown'}`
  return <span className={cls}>{type || 'unknown'}</span>
}

export default function App() {
  const [data, setData] = useState(null)
  const [tab, setTab] = useState('overview')
  const [sectorFilter, setSectorFilter] = useState('')
  const [roundFilter, setRoundFilter] = useState('')

  useEffect(() => {
    fetch('./data.json')
      .then(r => r.json())
      .then(setData)
      .catch(() => setData({ error: true }))
  }, [])

  if (!data) return <div className="app"><div className="empty-state">Loading...</div></div>
  if (data.error) return <div className="app"><div className="empty-state">data.json not found. Run: python3 src/analyzer/trends.py</div></div>

  const { sector_trends, round_distribution, top_investors, co_investment_pairs, monthly_summary } = data
  const totalDeals = monthly_summary?.reduce((s, m) => s + m.deal_count, 0) || 0
  const totalAmount = monthly_summary?.reduce((s, m) => s + m.total_amount_jpy, 0) || 0
  const uniqueSectors = [...new Set((sector_trends || []).map(s => s.sector))].sort()
  const uniqueRounds = [...new Set((round_distribution || []).map(r => r.round_type))].sort()

  return (
    <div className="app">
      <header>
        <h1>Investment Signal Radar</h1>
        <p>VC investment data collection and foresight signal detection</p>
        <p style={{ fontSize: 12, color: '#999' }}>Generated: {data.generated_at}</p>
      </header>

      <div className="stats-grid">
        <StatCard label="Total Deals" value={totalDeals} />
        <StatCard label="Total Amount" value={formatAmount(totalAmount)} />
        <StatCard label="Sectors" value={uniqueSectors.length} />
        <StatCard label="Top Investors" value={top_investors?.length || 0} />
      </div>

      <div className="tabs">
        {['overview', 'investors', 'sectors', 'deals'].map(t => (
          <button key={t} className={`tab ${tab === t ? 'active' : ''}`} onClick={() => setTab(t)}>
            {t === 'overview' ? 'Overview' : t === 'investors' ? 'Investors' : t === 'sectors' ? 'Sectors' : 'Deals'}
          </button>
        ))}
      </div>

      {tab === 'overview' && <OverviewTab data={data} />}
      {tab === 'investors' && <InvestorsTab investors={top_investors} pairs={co_investment_pairs} />}
      {tab === 'sectors' && <SectorsTab trends={sector_trends} />}
      {tab === 'deals' && <DealsTab distribution={round_distribution} />}
    </div>
  )
}

function StatCard({ label, value }) {
  return (
    <div className="stat-card">
      <div className="label">{label}</div>
      <div className="value">{value}</div>
    </div>
  )
}

function OverviewTab({ data }) {
  const { monthly_summary, round_distribution } = data
  const pieData = (round_distribution || []).map(r => ({
    name: r.round_type, value: r.count,
  }))

  return (
    <>
      <div className="section">
        <h2>Monthly Deal Activity</h2>
        <div className="chart-container">
          <ResponsiveContainer>
            <BarChart data={monthly_summary || []}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="month" fontSize={12} />
              <YAxis fontSize={12} />
              <Tooltip />
              <Bar dataKey="deal_count" fill="#2563eb" name="Deals" radius={[4,4,0,0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="section">
        <h2>Round Distribution</h2>
        <div className="chart-container">
          <ResponsiveContainer>
            <PieChart>
              <Pie data={pieData} dataKey="value" nameKey="name" cx="50%" cy="50%"
                   outerRadius={100} label={({ name, value }) => `${name}: ${value}`}>
                {pieData.map((entry, i) => (
                  <Cell key={i} fill={ROUND_COLORS[entry.name] || COLORS[i % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>
    </>
  )
}

function InvestorsTab({ investors, pairs }) {
  return (
    <>
      <div className="section">
        <h2>Most Active Investors</h2>
        <table>
          <thead><tr><th>Investor</th><th>Type</th><th>Deals</th><th>Lead</th></tr></thead>
          <tbody>
            {(investors || []).map((inv, i) => (
              <tr key={i}>
                <td>{inv.investor_name}</td>
                <td>{inv.investor_type || '-'}</td>
                <td>{inv.deal_count}</td>
                <td>{inv.lead_count}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {pairs && pairs.length > 0 && (
        <div className="section">
          <h2>Co-Investment Pairs</h2>
          <table>
            <thead><tr><th>Investor A</th><th>Investor B</th><th>Shared Deals</th></tr></thead>
            <tbody>
              {pairs.map((p, i) => (
                <tr key={i}>
                  <td>{p.investor_a}</td>
                  <td>{p.investor_b}</td>
                  <td>{p.shared_deals}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  )
}

function SectorsTab({ trends }) {
  const sectors = [...new Set((trends || []).map(t => t.sector))]
  const chartData = sectors.map(s => {
    const items = (trends || []).filter(t => t.sector === s)
    return { sector: s, count: items.reduce((a, b) => a + b.count, 0) }
  }).sort((a, b) => b.count - a.count)

  return (
    <div className="section">
      <h2>Investment by Sector</h2>
      <div className="chart-container">
        <ResponsiveContainer>
          <BarChart data={chartData} layout="vertical" margin={{ left: 120 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis type="number" fontSize={12} />
            <YAxis dataKey="sector" type="category" fontSize={11} width={110} />
            <Tooltip />
            <Bar dataKey="count" fill="#7c3aed" name="Deals" radius={[0,4,4,0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}

function DealsTab({ distribution }) {
  return (
    <div className="section">
      <h2>Deals by Round Type</h2>
      <table>
        <thead><tr><th>Round</th><th>Count</th><th>Total Amount</th></tr></thead>
        <tbody>
          {(distribution || []).map((r, i) => (
            <tr key={i}>
              <td><RoundBadge type={r.round_type} /></td>
              <td>{r.count}</td>
              <td>{formatAmount(r.total_amount_jpy)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

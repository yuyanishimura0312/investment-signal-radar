import React, { useState, useEffect, useMemo } from 'react'
import {
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
  RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis, Radar,
  Treemap,
} from 'recharts'

const COLORS = ['#F0A671', '#CEA26F', '#DC8766', '#B07256', '#7A4033', '#966D5E', '#F2C792', '#F0BE83', '#E8C4A0', '#D8B88E', '#5F322A', '#4F2922']

const TAB_ORDER = ['overview', 'signals', 'scores', 'investors', 'sectors', 'deals', 'press_releases', 'data', 'about']
const TAB_LABELS = {
  overview: '概要',
  signals: 'シグナル',
  scores: '企業スコア',
  investors: '投資家',
  sectors: 'セクター',
  deals: 'ラウンド',
  press_releases: 'プレスリリース',
  data: '��ータヘルス',
  about: 'About',
}

function normalizeRoundType(t) {
  if (!t) return 'unknown'
  const map = {
    pre_seed: 'pre-seed', seed: 'seed',
    series_a: 'a', series_b: 'b', series_c: 'c', series_d: 'd',
    series_e: 'e', series_f: 'f', series_g: 'g',
    late_stage: 'late', corporate_round: 'strategic',
    convertible_note: 'convertible', j_kiss: 'j-kiss',
    strategic: 'strategic', debt: 'debt', grant: 'grant',
    angel: 'angel', ipo: 'ipo', secondary: 'secondary', unknown: 'unknown',
  }
  return map[t] || t
}

const ROUND_LABEL = {
  pre_seed: 'Pre-Seed', seed: 'Seed',
  series_a: 'Series A', series_b: 'Series B', series_c: 'Series C',
  series_d: 'Series D', series_e: 'Series E', series_f: 'Series F', series_g: 'Series G',
  late_stage: 'Late', corporate_round: 'Corporate',
  convertible_note: 'Convertible', j_kiss: 'J-KISS',
  strategic: 'Strategic', debt: 'Debt', grant: 'Grant',
  angel: 'Angel', ipo: 'IPO', secondary: 'Secondary', unknown: '不明',
}

const ROUND_COLORS = {
  seed: '#F2C792', 'pre-seed': '#F0BE83',
  a: '#DC8766', b: '#B07256', c: '#966D5E', d: '#7A4033',
  e: '#5F322A', f: '#4F2922', g: '#3F2019',
  debt: '#CEA26F', strategic: '#F0BE83', angel: '#F7BEA2', grant: '#D8B88E',
  'j-kiss': '#E8C4A0', convertible: '#E0B090',
  ipo: '#CE8766', secondary: '#B8956A', late: '#7A4033', unknown: '#EFC4A4',
}

const SIGNAL_TYPE_LABELS = {
  investment_surge: '投資サージ',
  new_sector: '新セクター出現',
  event_surge: 'イベント急増',
  network_shift: 'ネットワーク変動',
  round_escalation: 'ラウンド拡大',
  funding_drought: '資金枯渇',
}

const EVENT_TYPE_LABELS = {
  funding: '資金調達', partnership: '提携', acquisition: '買収/Exit',
  hiring: '採用', product_launch: '製品リリース', accelerator: 'アクセラレーター',
}

function formatAmount(val) {
  if (val === null || val === undefined) return '-'
  if (val === 0) return '非公開'
  if (val >= 100000000) return `${(val / 100000000).toFixed(1)}億円`
  if (val >= 10000) return `${(val / 10000).toFixed(1)}万円`
  return `${val.toLocaleString()}円`
}

function formatAmountMan(val) {
  if (val === null || val === undefined) return '-'
  if (val === 0) return '非公開'
  if (val >= 10000) return `${(val / 10000).toFixed(1)}億円`
  return `${val.toLocaleString()}万円`
}

function formatDateTime(iso) {
  if (!iso) return '-'
  try { return new Date(iso).toLocaleString('ja-JP') } catch { return iso }
}

function formatDate(iso) {
  if (!iso) return '-'
  try { return new Date(iso).toLocaleDateString('ja-JP') } catch { return iso }
}

function normalizeSector(s) { return (s || '').replace(/\s*\/\s*/g, '/').trim() }

function RoundBadge({ type }) {
  const norm = normalizeRoundType(type)
  const label = ROUND_LABEL[type] || type || '不明'
  return <span className={`badge badge-${norm}`}>{label}</span>
}

export default function App() {
  const [data, setData] = useState(null)
  const [tab, setTab] = useState('overview')
  const [sectorFilter, setSectorFilter] = useState('')
  const [roundFilter, setRoundFilter] = useState('')

  useEffect(() => {
    fetch(`./data.json?t=${Date.now()}`)
      .then(r => r.json())
      .then(setData)
      .catch(() => setData({ error: true }))
  }, [])

  useEffect(() => {
    if (data && !data.error) {
      document.title = `${TAB_LABELS[tab] || ''} | Investment Signal Radar`
    }
  }, [tab, data])

  if (!data) return <div className="app"><div className="empty-state">データを読み込んでいます…</div></div>
  if (data.error) return <div className="app"><div className="empty-state">data.json が見つかりません。</div></div>

  const { stats, schema_version } = data
  const uniqueSectors = [...new Set((data.sector_trends || []).map(s => normalizeSector(s.sector)))].sort()
  const uniqueRounds = [...new Set((data.round_distribution || []).map(r => r.round_type))].sort()

  const onTabKeyDown = (e) => {
    const idx = TAB_ORDER.indexOf(tab)
    if (e.key === 'ArrowRight') { e.preventDefault(); setTab(TAB_ORDER[(idx + 1) % TAB_ORDER.length]) }
    else if (e.key === 'ArrowLeft') { e.preventDefault(); setTab(TAB_ORDER[(idx - 1 + TAB_ORDER.length) % TAB_ORDER.length]) }
  }

  return (
    <div className="app">
      <header>
        <h1>Investment Signal Radar</h1>
        <p>VC投資データから構造的変化のシグナルを検出するフォーサイト・プラットフォーム</p>
        <p className="generated-at">最終更新: {formatDateTime(data.generated_at)}</p>
      </header>

      {/* KPI Cards */}
      <div className="stats-grid">
        <StatCard label="企業数" value={stats?.companies ?? 0} />
        <StatCard label="投資家数" value={stats?.investors ?? 0} />
        <StatCard label="ラウンド数" value={stats?.funding_rounds ?? 0} />
        <StatCard label="イベント数" value={stats?.events ?? 0} />
        <StatCard label="シグナル数" value={stats?.signals ?? 0} />
        <StatCard label="タグ数" value={stats?.tags ?? 0} />
      </div>

      <div className="tabs" role="tablist">
        {TAB_ORDER.map(t => (
          <button key={t} role="tab" aria-selected={tab === t}
            className={`tab ${tab === t ? 'active' : ''}`}
            onClick={() => setTab(t)} onKeyDown={onTabKeyDown}>
            {TAB_LABELS[t]}
          </button>
        ))}
      </div>

      {tab === 'overview' && <OverviewTab data={data} sectorFilter={sectorFilter} setSectorFilter={setSectorFilter} roundFilter={roundFilter} setRoundFilter={setRoundFilter} uniqueSectors={uniqueSectors} uniqueRounds={uniqueRounds} />}
      {tab === 'signals' && <SignalsTab data={data} />}
      {tab === 'scores' && <ScoresTab data={data} />}
      {tab === 'investors' && <InvestorsTab data={data} />}
      {tab === 'sectors' && <SectorsTab data={data} />}
      {tab === 'deals' && <DealsTab distribution={data.round_distribution} />}
      {tab === 'press_releases' && <PressReleasesTab data={data} />}
      {tab === 'data' && <DataHealthTab data={data} />}
      {tab === 'about' && <AboutTab data={data} />}

      <footer className="footer">
        <span>Schema: <code>{schema_version || 'v1'}</code></span>
        <span> | </span>
        <span>{stats?.organizations ?? 0} organizations / {stats?.funding_rounds ?? 0} rounds / {stats?.signals ?? 0} signals</span>
        <span> | </span>
        <a href="https://github.com/yuyanishimura0312/investment-signal-radar" rel="noopener">GitHub</a>
      </footer>
    </div>
  )
}

function StatCard({ label, value }) {
  return (
    <div className="stat-card">
      <div className="label">{label}</div>
      <div className="value">{typeof value === 'number' ? value.toLocaleString() : value}</div>
    </div>
  )
}

/* ================================================================
 * OVERVIEW TAB — integrated dashboard with key charts
 * ================================================================ */
function OverviewTab({ data, sectorFilter, setSectorFilter, roundFilter, setRoundFilter, uniqueSectors, uniqueRounds }) {
  const { monthly_summary, round_distribution, sector_trends, event_momentum, top_organizations_by_score, signals } = data

  const filteredRoundDist = (round_distribution || []).filter(r => !roundFilter || r.round_type === roundFilter)
  const pieData = filteredRoundDist.map(r => ({ name: ROUND_LABEL[r.round_type] || r.round_type, key: r.round_type, value: r.count }))

  // Sector summary for treemap
  const sectorData = (data.sector_summary || [])
    .filter(s => s.deal_count > 0)
    .map(s => ({ name: s.sector_name, size: s.deal_count, amount: s.total_raised_jpy }))
    .sort((a, b) => b.size - a.size)
    .slice(0, 15)

  return (
    <>
      <div className="filters" role="group" aria-label="フィルタ">
        <label>
          <span className="filter-label">セクター:</span>
          <select value={sectorFilter} onChange={(e) => setSectorFilter(e.target.value)}>
            <option value="">全セクター</option>
            {uniqueSectors.map(s => <option key={s} value={s}>{s}</option>)}
          </select>
        </label>
        <label>
          <span className="filter-label">ラウンド:</span>
          <select value={roundFilter} onChange={(e) => setRoundFilter(e.target.value)}>
            <option value="">全ラウンド</option>
            {uniqueRounds.map(r => <option key={r} value={r}>{ROUND_LABEL[r] || r}</option>)}
          </select>
        </label>
        {(sectorFilter || roundFilter) && (
          <button className="filter-clear" onClick={() => { setSectorFilter(''); setRoundFilter('') }}>クリア</button>
        )}
      </div>

      {/* Monthly deals chart */}
      <div className="section">
        <h2>月次ディール推移</h2>
        {monthly_summary?.length > 0 ? (
          <div className="chart-container">
            <ResponsiveContainer>
              <BarChart data={monthly_summary}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="month" fontSize={11} />
                <YAxis fontSize={12} />
                <Tooltip formatter={(v, n) => [v, n === 'deal_count' ? '件数' : '金額']} />
                <Bar dataKey="deal_count" fill="#DC8766" name="件数" radius={[4,4,0,0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        ) : <EmptyInline text="月次データはまだ蓄積されていません。" />}
      </div>

      <div className="grid-2col">
        {/* Round distribution pie */}
        <div className="section">
          <h2>ラウンド構成</h2>
          {pieData.length > 0 ? (
            <div className="chart-container" style={{ height: 280 }}>
              <ResponsiveContainer>
                <PieChart>
                  <Pie data={pieData} dataKey="value" nameKey="name" cx="50%" cy="50%"
                    outerRadius={90} label={({ name, value }) => `${name}: ${value}`}>
                    {pieData.map((entry, i) => <Cell key={i} fill={ROUND_COLORS[normalizeRoundType(entry.key)] || COLORS[i % COLORS.length]} />)}
                  </Pie>
                  <Tooltip formatter={(v) => [v, '件数']} />
                </PieChart>
              </ResponsiveContainer>
            </div>
          ) : <EmptyInline text="データなし" />}
        </div>

        {/* Event momentum */}
        <div className="section">
          <h2>イベント種別（直近90日）</h2>
          {event_momentum?.length > 0 ? (
            <div className="chart-container" style={{ height: 280 }}>
              <ResponsiveContainer>
                <BarChart data={event_momentum.map(e => ({ ...e, label: EVENT_TYPE_LABELS[e.event_type] || e.event_type }))}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="label" fontSize={11} />
                  <YAxis fontSize={12} />
                  <Tooltip formatter={(v) => [v, '件数']} />
                  <Bar dataKey="count" fill="#B07256" name="件数" radius={[4,4,0,0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : <EmptyInline text="イベントなし" />}
        </div>
      </div>

      {/* Sector treemap */}
      {sectorData.length > 0 && (
        <div className="section">
          <h2>セクター別投資件数マップ</h2>
          <div className="chart-container" style={{ height: 320 }}>
            <ResponsiveContainer>
              <Treemap data={sectorData} dataKey="size" nameKey="name" stroke="#fff"
                content={<TreemapCell />} />
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* Top scored organizations */}
      {top_organizations_by_score?.length > 0 && (
        <div className="section">
          <h2>注目企業スコア TOP 10</h2>
          <table>
            <thead><tr><th>#</th><th>企業名</th><th>総合スコア</th></tr></thead>
            <tbody>
              {top_organizations_by_score.slice(0, 10).map((org, i) => (
                <tr key={i}>
                  <td>{i + 1}</td>
                  <td>{org.name}</td>
                  <td><ScoreBar value={org.composite_score} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  )
}

/* ================================================================
 * SIGNALS TAB — detected foresight signals
 * ================================================================ */
function SignalsTab({ data }) {
  const signals = data.signals || []
  const byType = {}
  signals.forEach(s => { byType[s.signal_type] = (byType[s.signal_type] || 0) + 1 })
  const typeData = Object.entries(byType).map(([k, v]) => ({ type: SIGNAL_TYPE_LABELS[k] || k, count: v })).sort((a, b) => b.count - a.count)

  return (
    <>
      <div className="section">
        <h2>シグナル種別分布</h2>
        <p className="hint">投資データの異常パターンから自動検出された変化のシグナルです。</p>
        {typeData.length > 0 ? (
          <div className="chart-container" style={{ height: 220 }}>
            <ResponsiveContainer>
              <BarChart data={typeData} layout="vertical" margin={{ left: 120 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis type="number" fontSize={12} />
                <YAxis dataKey="type" type="category" fontSize={12} width={110} />
                <Tooltip formatter={(v) => [v, '件数']} />
                <Bar dataKey="count" fill="#DC8766" radius={[0,4,4,0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        ) : <EmptyInline text="シグナルはまだ検出されていません。" />}
      </div>

      <div className="section">
        <h2>検出シグナル一覧（最新50件）</h2>
        {signals.length > 0 ? (
          <div className="table-scroll">
            <table>
              <thead>
                <tr><th>種別</th><th>セクター</th><th>検出日</th><th>ベースライン</th><th>現在</th><th>���速度</th><th>説明</th></tr>
              </thead>
              <tbody>
                {signals.map((s, i) => (
                  <tr key={i}>
                    <td><span className={`badge badge-signal-${s.signal_type}`}>{SIGNAL_TYPE_LABELS[s.signal_type] || s.signal_type}</span></td>
                    <td>{s.sector_name || '-'}</td>
                    <td style={{ whiteSpace: 'nowrap' }}>{formatDate(s.detected_at)}</td>
                    <td>{s.baseline_count ?? '-'}</td>
                    <td>{s.current_count ?? '-'}</td>
                    <td>{s.acceleration_ratio != null ? `${Number(s.acceleration_ratio).toFixed(1)}x` : '-'}</td>
                    <td className="text-wrap">{s.description || '-'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : <EmptyInline text="シグナルデータなし" />}
      </div>
    </>
  )
}

/* ================================================================
 * SCORES TAB — organization scores
 * ================================================================ */
function ScoresTab({ data }) {
  const scores = data.top_organizations_by_score || []

  return (
    <>
      <div className="section">
        <h2>企業スコアランキング TOP 20</h2>
        <p className="hint">
          Momentum（資金調達の勢い）・Funding（調達規模）・Market（セクター成長性）の3軸で算出した総合スコア。
          CB Insights Mosaic Scoreに着想を得た設計です。
        </p>
        {scores.length > 0 ? (
          <table>
            <thead><tr><th>#</th><th>企業名</th><th>総合スコア</th><th>算出日</th></tr></thead>
            <tbody>
              {scores.map((org, i) => (
                <tr key={i}>
                  <td>{i + 1}</td>
                  <td>{org.name}</td>
                  <td><ScoreBar value={org.composite_score} /></td>
                  <td style={{ whiteSpace: 'nowrap', fontSize: 12 }}>{formatDate(org.calculated_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : <EmptyInline text="スコアデータなし" />}
      </div>

      <div className="section">
        <h2>スコア分布</h2>
        {scores.length > 0 ? (
          <div className="chart-container" style={{ height: 300 }}>
            <ResponsiveContainer>
              <BarChart data={scores.slice(0, 20).map(s => ({ name: s.name.length > 12 ? s.name.slice(0, 12) + '…' : s.name, score: s.composite_score }))}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="name" fontSize={10} angle={-30} textAnchor="end" height={80} />
                <YAxis domain={[0, 1]} fontSize={12} />
                <Tooltip formatter={(v) => [Number(v).toFixed(3), 'スコア']} />
                <Bar dataKey="score" fill="#DC8766" radius={[4,4,0,0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        ) : <EmptyInline text="データなし" />}
      </div>
    </>
  )
}

/* ================================================================
 * INVESTORS TAB — rankings + network centrality
 * ================================================================ */
function InvestorsTab({ data }) {
  const investors = data.top_investors || []
  const pairs = data.co_investment_pairs || []
  const network = data.network_top_investors || []

  return (
    <>
      <div className="section">
        <h2>投資家ランキング（ディール数）</h2>
        {investors.length > 0 ? (
          <table>
            <thead><tr><th>#</th><th>投資家</th><th>種別</th><th>ディール</th><th>リード</th></tr></thead>
            <tbody>
              {investors.map((inv, i) => (
                <tr key={i}>
                  <td>{i + 1}</td>
                  <td>{inv.investor_name}</td>
                  <td><code>{inv.investor_type || '-'}</code></td>
                  <td>{inv.deal_count}</td>
                  <td>{inv.lead_count}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : <EmptyInline text="投資家データなし" />}
      </div>

      {/* Network centrality */}
      {network.length > 0 && (
        <div className="section">
          <h2>投資家ネットワーク中心性 TOP 20</h2>
          <p className="hint">共同投資関係から算出したネットワーク指標。Degree=つながりの広さ、Betweenness=橋渡し力、Eigenvector=影���力。</p>
          <table>
            <thead>
              <tr><th>#</th><th>投資家</th><th>Degree</th><th>Between.</th><th>Eigen.</th><th>共同投資数</th><th>パートナー数</th></tr>
            </thead>
            <tbody>
              {network.slice(0, 20).map((n, i) => (
                <tr key={i}>
                  <td>{i + 1}</td>
                  <td>{n.investor_name}</td>
                  <td>{Number(n.degree || 0).toFixed(3)}</td>
                  <td>{Number(n.betweenness || 0).toFixed(3)}</td>
                  <td>{Number(n.eigenvector || 0).toFixed(3)}</td>
                  <td>{Math.round(n.co_investments || 0)}</td>
                  <td>{Math.round(n.unique_partners || 0)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Co-investment pairs */}
      {pairs.length > 0 && (
        <div className="section">
          <h2>共同投資ペア TOP 30</h2>
          <table>
            <thead><tr><th>投資家A</th><th>投資家B</th><th>共通ディール</th></tr></thead>
            <tbody>
              {pairs.slice(0, 30).map((p, i) => (
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

/* ================================================================
 * SECTORS TAB — sector analysis + tags
 * ================================================================ */
function SectorsTab({ data }) {
  const summary = data.sector_summary || []
  const tagDist = data.tag_distribution || []

  const sortedSectors = [...summary].filter(s => s.deal_count > 0).sort((a, b) => b.deal_count - a.deal_count)

  // Group tags by category
  const tagsByCategory = {}
  tagDist.forEach(t => {
    if (!tagsByCategory[t.tag_category]) tagsByCategory[t.tag_category] = []
    tagsByCategory[t.tag_category].push(t)
  })

  const TAG_CAT_LABELS = { technology: 'テクノロジー', business_model: 'ビジネスモデル', market: 'マーケット' }

  return (
    <>
      <div className="section">
        <h2>セクター別投資サマリー</h2>
        {sortedSectors.length > 0 ? (
          <>
            <div className="chart-container" style={{ height: Math.max(300, sortedSectors.length * 28) }}>
              <ResponsiveContainer>
                <BarChart data={sortedSectors} layout="vertical" margin={{ left: 150 }}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis type="number" fontSize={12} />
                  <YAxis dataKey="sector_name" type="category" fontSize={11} width={140} />
                  <Tooltip formatter={(v, n) => [n === 'deal_count' ? v : formatAmountMan(v), n === 'deal_count' ? '件数' : '調達額']} />
                  <Bar dataKey="deal_count" fill="#B07256" name="deal_count" radius={[0,4,4,0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
            <table style={{ marginTop: 16 }}>
              <thead><tr><th>セクター</th><th>ディール</th><th>企業数</th><th>投資家数</th><th>合計調達額</th><th>平均額</th></tr></thead>
              <tbody>
                {sortedSectors.map((s, i) => (
                  <tr key={i}>
                    <td>{s.sector_name}</td>
                    <td>{s.deal_count}</td>
                    <td>{s.company_count}</td>
                    <td>{s.investor_count}</td>
                    <td>{formatAmountMan(s.total_raised_jpy)}</td>
                    <td>{formatAmountMan(s.avg_deal_size_jpy)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </>
        ) : <EmptyInline text="セクターデータなし" />}
      </div>

      {/* Tag distribution by category */}
      {Object.keys(tagsByCategory).length > 0 && (
        <div className="section">
          <h2>タグ分布（3カテゴリ）</h2>
          <p className="hint">セクター（単一固定）に加え、technology / business_model / market の3カテゴリでタグ付けされています。</p>
          <div className="grid-3col">
            {Object.entries(tagsByCategory).map(([cat, tags]) => (
              <div key={cat} className="tag-category-card">
                <h3>{TAG_CAT_LABELS[cat] || cat}</h3>
                <div className="tag-cloud">
                  {tags.sort((a, b) => b.org_count - a.org_count).map((t, i) => (
                    <span key={i} className="tag-pill" style={{ fontSize: Math.max(11, Math.min(18, 11 + t.org_count / 30)) }}>
                      {t.tag_name} <small>{t.org_count}</small>
                    </span>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </>
  )
}

/* ================================================================
 * DEALS TAB
 * ================================================================ */
function DealsTab({ distribution }) {
  return (
    <div className="section">
      <h2>ラウンドタイプ別ディール</h2>
      {distribution?.length > 0 ? (
        <>
          <div className="chart-container" style={{ height: Math.max(250, distribution.length * 30) }}>
            <ResponsiveContainer>
              <BarChart data={distribution.map(r => ({ ...r, label: ROUND_LABEL[r.round_type] || r.round_type }))} layout="vertical" margin={{ left: 100 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis type="number" fontSize={12} />
                <YAxis dataKey="label" type="category" fontSize={11} width={90} />
                <Tooltip formatter={(v, n) => [n === 'count' ? v : formatAmountMan(v), n === 'count' ? '件数' : '調達額']} />
                <Bar dataKey="count" fill="#DC8766" radius={[0,4,4,0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
          <table style={{ marginTop: 16 }}>
            <thead><tr><th>ラウンド</th><th>件数</th><th>合計調達額</th></tr></thead>
            <tbody>
              {distribution.map((r, i) => (
                <tr key={i}>
                  <td><RoundBadge type={r.round_type} /></td>
                  <td>{r.count}</td>
                  <td>{formatAmountMan(r.total_amount_jpy)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      ) : <EmptyInline text="ラウンドデータなし" />}
    </div>
  )
}

/* ================================================================
 * PRESS RELEASES TAB
 * ================================================================ */
const PR_CATEGORY_LABELS = {
  funding: '資金調達', partnership: '提携', product_launch: '製品リリース',
  hiring: '採用', exit: 'Exit', accelerator: 'アクセラレーター', other: 'その他',
}

function PressReleasesTab({ data }) {
  const prData = data?.press_releases
  if (!prData || prData.total_count === 0) return <div className="empty-state">プレスリリースデータなし</div>

  const fundingPct = ((prData.funding_related_count / prData.total_count) * 100).toFixed(1)
  const monthlyData = (prData.by_month || []).map(m => ({ ...m, non_funding_count: m.count - (m.funding_count || 0) }))
  const categoryData = Object.entries(prData.by_category || {}).map(([k, v]) => ({ category: PR_CATEGORY_LABELS[k] || k, count: v })).sort((a, b) => b.count - a.count)

  return (
    <>
      <div className="stats-grid" style={{ marginBottom: 'var(--space-lg)' }}>
        <StatCard label="総数" value={prData.total_count} />
        <StatCard label="資金調達関連" value={`${prData.funding_related_count} (${fundingPct}%)`} />
      </div>

      <div className="grid-2col">
        <div className="section">
          <h2>月次プレスリリース</h2>
          {monthlyData.length > 0 ? (
            <div className="chart-container" style={{ height: 260 }}>
              <ResponsiveContainer>
                <BarChart data={monthlyData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="month" fontSize={11} />
                  <YAxis fontSize={12} />
                  <Tooltip />
                  <Legend formatter={v => v === 'funding_count' ? '資金調達' : 'その他'} />
                  <Bar dataKey="funding_count" stackId="pr" fill="#DC8766" />
                  <Bar dataKey="non_funding_count" stackId="pr" fill="#F2C792" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : <EmptyInline text="データなし" />}
        </div>

        <div className="section">
          <h2>カテゴリ分布</h2>
          {categoryData.length > 0 ? (
            <div className="chart-container" style={{ height: 260 }}>
              <ResponsiveContainer>
                <BarChart data={categoryData} layout="vertical" margin={{ left: 100 }}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis type="number" fontSize={12} />
                  <YAxis dataKey="category" type="category" fontSize={11} width={90} />
                  <Tooltip formatter={v => [v, '件数']} />
                  <Bar dataKey="count" fill="#B07256" radius={[0,4,4,0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : <EmptyInline text="データなし" />}
        </div>
      </div>

      {prData.recent_releases?.length > 0 && (
        <div className="section">
          <h2>最新プレスリリース</h2>
          <div className="table-scroll">
            <table>
              <thead><tr><th>タイトル</th><th>企業</th><th>日付</th><th>種別</th></tr></thead>
              <tbody>
                {prData.recent_releases.slice(0, 20).map((pr, i) => (
                  <tr key={i}>
                    <td>{pr.source_url ? <a href={pr.source_url} target="_blank" rel="noopener" className="pr-title-link">{pr.title}</a> : pr.title}</td>
                    <td>{pr.company_name || '-'}</td>
                    <td style={{ whiteSpace: 'nowrap' }}>{formatDate(pr.published_at)}</td>
                    <td>{pr.is_funding_related && <span className="badge badge-funding">資金調達</span>}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </>
  )
}

/* ================================================================
 * DATA HEALTH TAB
 * ================================================================ */
function DataHealthTab({ data }) {
  const { stats, data_freshness, data_source_breakdown, event_momentum, corporate_enrichment } = data

  const freshnessData = [
    { name: 'Fresh', key: 'fresh', value: data_freshness?.fresh || 0 },
    { name: 'Stale', key: 'stale', value: data_freshness?.stale || 0 },
    { name: 'Expired', key: 'expired', value: data_freshness?.expired || 0 },
  ]

  const enrichPct = corporate_enrichment ? (corporate_enrichment.enrichment_rate * 100).toFixed(1) : '0'

  return (
    <>
      <div className="section">
        <h2>データ規模</h2>
        <div className="stats-grid">
          <StatCard label="組織数" value={stats?.organizations ?? 0} />
          <StatCard label="企業" value={stats?.companies ?? 0} />
          <StatCard label="投資家" value={stats?.investors ?? 0} />
          <StatCard label="ラウンド" value={stats?.funding_rounds ?? 0} />
          <StatCard label="イベント" value={stats?.events ?? 0} />
          <StatCard label="シグナル" value={stats?.signals ?? 0} />
          <StatCard label="スコア" value={stats?.signal_scores ?? 0} />
          <StatCard label="タグ" value={stats?.tags ?? 0} />
        </div>
      </div>

      <div className="grid-2col">
        <div className="section">
          <h2>データ鮮度</h2>
          <div className="freshness-bar">
            {freshnessData.map(f => (
              <div key={f.key} className={`freshness-segment freshness-${f.key}`} style={{ flex: f.value || 0.01 }}>
                <span>{f.name}: {f.value}</span>
              </div>
            ))}
          </div>
        </div>

        <div className="section">
          <h2>エンリッチメント進捗</h2>
          <div style={{ fontSize: 14, marginBottom: 8 }}>
            {corporate_enrichment?.enriched_count ?? 0} / {corporate_enrichment?.total_organizations ?? 0} ({enrichPct}%)
          </div>
          <div className="enrichment-progress-bar">
            <div className="enrichment-progress-fill" style={{ width: `${Math.min(parseFloat(enrichPct), 100)}%` }} />
          </div>
        </div>
      </div>

      {data_source_breakdown?.length > 0 && (
        <div className="section">
          <h2>データソース内訳</h2>
          <table>
            <thead><tr><th>ソース</th><th>種別</th><th>組織数</th><th>ラウンド数</th></tr></thead>
            <tbody>
              {data_source_breakdown.map((s, i) => (
                <tr key={i}>
                  <td><code>{s.source}</code></td>
                  <td>{s.source_type}</td>
                  <td>{s.org_count}</td>
                  <td>{s.round_count}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  )
}

/* ================================================================
 * ABOUT TAB (trimmed for brevity)
 * ================================================================ */
function AboutTab({ data }) {
  return (
    <>
      <div className="section about-section">
        <h2>Investment Signal Radarとは</h2>
        <p>
          VC投資データを自動収集・構造化し、投資トレンドの変化をシグナルとして検出するフォーサイト・プラットフォームです。
          どのセクターに資金が集まっているか、どの投資家が活発に動いているかを可視化し、テクノロジーや社会の構造的変化の兆候を早期に捉えます。
        </p>
      </div>

      <div className="section about-section">
        <h2>データベース v2.0</h2>
        <ul>
          <li><strong>Organization中心モデル</strong>：企業・投資家を統合、複数役割対応</li>
          <li><strong>イベント駆動</strong>：資金調達・採用・提携・Exit等を統一追跡</li>
          <li><strong>データ来歴</strong>：情報ソース・信頼度・TTLによる鮮度管理</li>
          <li><strong>3層タグ分類</strong>：セクター（固定）+ technology / business_model / market タグ</li>
          <li><strong>コンポーネント別スコア</strong>：Momentum / Funding / Market の3軸 + 総合スコア</li>
          <li><strong>ネットワーク分析</strong>：共同投資グラフから中心性指標を算出</li>
          <li><strong>シグナル検出</strong>：投資サージ・新セクター出現・ネットワーク変動を自動検出</li>
        </ul>
      </div>

      <div className="section about-section">
        <h2>データ収集パイプライン</h2>
        <p>毎時RSSフィード（PR TIMES / The Bridge）を収集 → キーワードフィルタ → Claude APIで構造化抽出 → DB登録。バッチ処理で歴史データも拡充しています。</p>
        <table>
          <thead><tr><th>ソース</th><th>種別</th><th>内容</th></tr></thead>
          <tbody>
            <tr><td><code>pr_times_rss</code></td><td>news</td><td>PR TIMESプレスリリース（毎時）</td></tr>
            <tr><td><code>the_bridge_rss</code></td><td>news</td><td>The Bridgeスタートアップニュース</td></tr>
            <tr><td><code>claude_extracted</code></td><td>ml_inferred</td><td>Claude APIによる構造化抽出</td></tr>
            <tr><td><code>gbizinfo</code></td><td>official</td><td>gBizINFO法人基本情報</td></tr>
          </tbody>
        </table>
      </div>

      <div className="section about-section">
        <h2>用語解説</h2>
        <dl className="glossary">
          <dt>Signal of Change</dt>
          <dd>投資パターンの異常な加速・変動。直近4週間と過去3ヶ月の比較で1.5x以上の変化を自動検出。</dd>
          <dt>Composite Score</dt>
          <dd>Momentum(0.35) + Funding(0.25) + Market(0.15) の重み付き総合スコア。0〜1のスケール。</dd>
          <dt>Network Centrality</dt>
          <dd>共同投資ネットワーク上の影響力指標。Degree=接続数、Betweenness=橋渡し力、Eigenvector=影響力。</dd>
          <dt>データ鮮度</dt>
          <dd>Fresh(TTL内)→Stale(ソフトTTL超過)→Expired(ハードTTL超過)の3段階で信頼性を管理。</dd>
        </dl>
      </div>

      <div className="section about-section">
        <h2>更新情報</h2>
        <p>最終データ生成: {formatDateTime(data?.generated_at)}</p>
        <p>データは毎時自動収集・更新されます。</p>
      </div>
    </>
  )
}

/* ================================================================
 * SHARED COMPONENTS
 * ================================================================ */
function EmptyInline({ text }) {
  return <div className="empty-state-inline">{text}</div>
}

function ScoreBar({ value }) {
  const pct = Math.round((value || 0) * 100)
  return (
    <div className="score-bar-wrapper">
      <div className="score-bar">
        <div className="score-bar-fill" style={{ width: `${pct}%` }} />
      </div>
      <span className="score-bar-label">{Number(value || 0).toFixed(3)}</span>
    </div>
  )
}

function TreemapCell({ x, y, width, height, name, size }) {
  if (width < 40 || height < 20) return null
  return (
    <g>
      <rect x={x} y={y} width={width} height={height} fill={COLORS[Math.abs(hashCode(name || '')) % COLORS.length]} stroke="#fff" strokeWidth={2} rx={4} />
      {width > 60 && height > 30 && (
        <>
          <text x={x + 6} y={y + 16} fontSize={11} fill="#fff" fontWeight={600}>{(name || '').slice(0, Math.floor(width / 8))}</text>
          <text x={x + 6} y={y + 30} fontSize={10} fill="rgba(255,255,255,0.8)">{size}件</text>
        </>
      )}
    </g>
  )
}

function hashCode(str) {
  let hash = 0
  for (let i = 0; i < str.length; i++) { hash = ((hash << 5) - hash) + str.charCodeAt(i); hash |= 0 }
  return hash
}

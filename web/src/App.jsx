import React, { useState, useEffect, useMemo } from 'react'
import {
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from 'recharts'

// Miratuku CI 12-color palette for charts
const COLORS = ['#F0A671', '#CEA26F', '#DC8766', '#B07256', '#7A4033', '#966D5E', '#F2C792', '#F0BE83']

// Tab ordering is used for keyboard navigation
const TAB_ORDER = ['overview', 'investors', 'sectors', 'deals', 'press_releases', 'data', 'about']
const TAB_LABELS = {
  overview: '概要',
  investors: '投資家',
  sectors: 'セクター',
  deals: 'ラウンド',
  press_releases: 'プレスリリース',
  data: 'データヘルス',
  about: 'このツールについて',
}

// v2 schema uses round types like series_a/series_b; v1 used a/b.
// Normalize to the short key used for CSS badges.
function normalizeRoundType(t) {
  if (!t) return 'unknown'
  const map = {
    pre_seed: 'pre-seed', 'pre-seed': 'pre-seed',
    seed: 'seed',
    series_a: 'a', 'series-a': 'a', 'pre-a': 'a', pre_a: 'a', a: 'a',
    series_b: 'b', 'series-b': 'b', b: 'b',
    series_c: 'c', 'series-c': 'c', c: 'c',
    series_d: 'd', 'series-d': 'd', d: 'd',
    series_e: 'e', 'series-e': 'e', e: 'e',
    series_f: 'f', 'series-f': 'f', f: 'f',
    series_g: 'g', 'series-g': 'g', g: 'g',
    late_stage: 'late',
    corporate_round: 'strategic',
    convertible_note: 'convertible',
    j_kiss: 'j-kiss', 'j-kiss': 'j-kiss',
    strategic: 'strategic', debt: 'debt', grant: 'grant',
    angel: 'angel', ipo: 'ipo', secondary: 'secondary',
    unknown: 'unknown',
  }
  return map[t] || t
}

// Display label for each round type (short, readable)
const ROUND_LABEL = {
  pre_seed: 'Pre-Seed', 'pre-seed': 'Pre-Seed', seed: 'Seed',
  series_a: 'Series A', series_b: 'Series B', series_c: 'Series C',
  series_d: 'Series D', series_e: 'Series E', series_f: 'Series F',
  series_g: 'Series G',
  late_stage: 'Late', corporate_round: 'Corporate',
  convertible_note: 'Convertible', j_kiss: 'J-KISS',
  strategic: 'Strategic', debt: 'Debt', grant: 'Grant',
  angel: 'Angel', ipo: 'IPO', secondary: 'Secondary',
  unknown: '不明',
}

const ROUND_COLORS = {
  seed: '#F2C792', 'pre-seed': '#F0BE83',
  a: '#DC8766', b: '#B07256', c: '#966D5E', d: '#7A4033',
  e: '#5F322A', f: '#4F2922', g: '#3F2019',
  debt: '#CEA26F', strategic: '#F0BE83',
  angel: '#F7BEA2', grant: '#D8B88E',
  'j-kiss': '#E8C4A0', convertible: '#E0B090',
  ipo: '#CE8766', secondary: '#B8956A',
  late: '#7A4033', unknown: '#EFC4A4',
}

function formatAmount(val) {
  if (val === null || val === undefined) return '-'
  if (val === 0) return '非公開'
  if (val >= 10000) return `${(val / 10000).toFixed(1)}億円`
  return `${val.toLocaleString()}万円`
}

function formatDateTime(iso) {
  if (!iso) return '-'
  try {
    return new Date(iso).toLocaleString('ja-JP')
  } catch {
    return iso
  }
}

// Normalize sector name (handle minor whitespace variance)
function normalizeSector(s) {
  return (s || '').replace(/\s*\/\s*/g, '/').trim()
}

function RoundBadge({ type }) {
  const norm = normalizeRoundType(type)
  const label = ROUND_LABEL[type] || type || '不明'
  return (
    <span className={`badge badge-${norm}`} aria-label={`ラウンド: ${label}`}>
      {label}
    </span>
  )
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

  // Update document title with current tab
  useEffect(() => {
    if (data && !data.error) {
      document.title = `${TAB_LABELS[tab] || ''} | Investment Signal Radar`
    }
  }, [tab, data])

  if (!data) {
    return (
      <div className="app">
        <div className="empty-state" role="status" aria-live="polite">
          データを読み込んでいます…
        </div>
      </div>
    )
  }
  if (data.error) {
    return (
      <div className="app">
        <div className="empty-state" role="alert">
          data.json が見つかりません。<br />
          <code>python3 src/analyzer/trends_v2.py</code> を実行してください。
        </div>
      </div>
    )
  }

  const {
    sector_trends, round_distribution, top_investors, co_investment_pairs,
    monthly_summary, stats, schema_version,
  } = data

  const totalDeals = monthly_summary?.reduce((s, m) => s + m.deal_count, 0) || 0
  const totalAmount = monthly_summary?.reduce((s, m) => s + (m.total_amount_jpy || 0), 0) || 0
  const uniqueSectors = [...new Set((sector_trends || []).map(s => normalizeSector(s.sector)))].sort()
  const uniqueRounds = [...new Set((round_distribution || []).map(r => r.round_type))].sort()

  const onTabKeyDown = (e) => {
    const idx = TAB_ORDER.indexOf(tab)
    if (e.key === 'ArrowRight') {
      e.preventDefault()
      setTab(TAB_ORDER[(idx + 1) % TAB_ORDER.length])
    } else if (e.key === 'ArrowLeft') {
      e.preventDefault()
      setTab(TAB_ORDER[(idx - 1 + TAB_ORDER.length) % TAB_ORDER.length])
    } else if (e.key === 'Home') {
      e.preventDefault()
      setTab(TAB_ORDER[0])
    } else if (e.key === 'End') {
      e.preventDefault()
      setTab(TAB_ORDER[TAB_ORDER.length - 1])
    }
  }

  const isSmallData = stats?.funding_rounds != null && stats.funding_rounds < 50

  return (
    <div className="app">
      <header>
        <h1>Investment Signal Radar</h1>
        <p>VC投資データから、テクノロジー・社会の構造的変化のシグナルを検出するフォーサイト・プラットフォーム</p>
        <p className="generated-at">最終データ生成: {formatDateTime(data.generated_at)}</p>
      </header>

      {schema_version === 'v2.0' && isSmallData && (
        <div className="banner banner-info" role="status">
          <strong>データ蓄積フェーズ</strong>：
          現在 {stats.funding_rounds} 件のラウンドデータで動作しています。
          v2スキーマへの移行直後のため、サンプル数は限定的です。
          データは毎時自動収集され、蓄積が進むにつれてシグナル検出の精度が向上します。
        </div>
      )}

      <div className="stats-grid">
        <StatCard label="ディール数" value={totalDeals} />
        <StatCard label="合計調達額" value={formatAmount(totalAmount)} />
        <StatCard label="セクター数" value={uniqueSectors.length} />
        <StatCard label="活発な投資家" value={top_investors?.length || 0} />
      </div>

      <div className="tabs" role="tablist" aria-label="ダッシュボード切替">
        {TAB_ORDER.map(t => (
          <button
            key={t}
            role="tab"
            id={`tab-${t}`}
            aria-selected={tab === t}
            aria-controls={`panel-${t}`}
            tabIndex={tab === t ? 0 : -1}
            className={`tab ${tab === t ? 'active' : ''}`}
            onClick={() => setTab(t)}
            onKeyDown={onTabKeyDown}
          >
            {TAB_LABELS[t]}
          </button>
        ))}
      </div>

      {tab === 'overview' && (
        <div role="tabpanel" id="panel-overview" aria-labelledby="tab-overview">
          <OverviewTab
            data={data}
            sectorFilter={sectorFilter} setSectorFilter={setSectorFilter}
            roundFilter={roundFilter} setRoundFilter={setRoundFilter}
            uniqueSectors={uniqueSectors} uniqueRounds={uniqueRounds}
          />
        </div>
      )}
      {tab === 'investors' && (
        <div role="tabpanel" id="panel-investors" aria-labelledby="tab-investors">
          <InvestorsTab investors={top_investors} pairs={co_investment_pairs} />
        </div>
      )}
      {tab === 'sectors' && (
        <div role="tabpanel" id="panel-sectors" aria-labelledby="tab-sectors">
          <SectorsTab trends={sector_trends} tagDistribution={data.tag_distribution} />
        </div>
      )}
      {tab === 'deals' && (
        <div role="tabpanel" id="panel-deals" aria-labelledby="tab-deals">
          <DealsTab distribution={round_distribution} />
        </div>
      )}
      {tab === 'press_releases' && (
        <div role="tabpanel" id="panel-press_releases" aria-labelledby="tab-press_releases">
          <PressReleasesTab data={data} />
        </div>
      )}
      {tab === 'data' && (
        <div role="tabpanel" id="panel-data" aria-labelledby="tab-data">
          <DataHealthTab data={data} />
        </div>
      )}
      {tab === 'about' && (
        <div role="tabpanel" id="panel-about" aria-labelledby="tab-about">
          <AboutTab data={data} />
        </div>
      )}

      <footer className="footer">
        <span>Schema: <code>{schema_version || 'v1'}</code></span>
        <span> · </span>
        <a href="https://github.com/yuyanishimura0312/investment-signal-radar" rel="noopener">
          GitHub
        </a>
      </footer>
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

function OverviewTab({ data, sectorFilter, setSectorFilter, roundFilter, setRoundFilter,
                      uniqueSectors, uniqueRounds }) {
  const { monthly_summary, round_distribution, sector_trends } = data

  // Filter sector_trends by selected round/sector (best-effort on aggregated data)
  const filteredSectorTrends = (sector_trends || []).filter(s => {
    if (sectorFilter && normalizeSector(s.sector) !== sectorFilter) return false
    return true
  })
  const filteredRoundDist = (round_distribution || []).filter(r => {
    if (roundFilter && r.round_type !== roundFilter) return false
    return true
  })

  const pieData = filteredRoundDist.map(r => ({
    name: ROUND_LABEL[r.round_type] || r.round_type,
    key: r.round_type,
    value: r.count,
  }))

  const tooltipFormatter = (value, name) => {
    if (name === 'deal_count' || name === 'count' || name === 'value') {
      return [value, '件数']
    }
    if (name === 'total_amount_jpy') return [formatAmount(value), '金額']
    return [value, name]
  }

  return (
    <>
      <div className="filters" role="group" aria-label="フィルタ">
        <label>
          <span className="filter-label">セクター:</span>
          <select
            value={sectorFilter}
            onChange={(e) => setSectorFilter(e.target.value)}
            aria-label="セクターで絞り込み"
          >
            <option value="">全セクター</option>
            {uniqueSectors.map(s => <option key={s} value={s}>{s}</option>)}
          </select>
        </label>
        <label>
          <span className="filter-label">ラウンド:</span>
          <select
            value={roundFilter}
            onChange={(e) => setRoundFilter(e.target.value)}
            aria-label="ラウンドタイプで絞り込み"
          >
            <option value="">全ラウンド</option>
            {uniqueRounds.map(r => (
              <option key={r} value={r}>{ROUND_LABEL[r] || r}</option>
            ))}
          </select>
        </label>
        {(sectorFilter || roundFilter) && (
          <button
            type="button"
            className="filter-clear"
            onClick={() => { setSectorFilter(''); setRoundFilter('') }}
          >
            フィルタをクリア
          </button>
        )}
      </div>

      <div className="section">
        <h2>月次ディール件数</h2>
        {monthly_summary && monthly_summary.length > 0 ? (
          <div className="chart-container">
            <ResponsiveContainer>
              <BarChart data={monthly_summary}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="month" fontSize={12} />
                <YAxis fontSize={12} />
                <Tooltip formatter={tooltipFormatter} />
                <Bar dataKey="deal_count" fill="#DC8766" name="件数" radius={[4,4,0,0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <div className="empty-state-inline">月次データはまだ蓄積されていません。</div>
        )}
      </div>

      <div className="section">
        <h2>ラウンドタイプ分布</h2>
        {pieData.length > 0 ? (
          <div className="chart-container">
            <ResponsiveContainer>
              <PieChart>
                <Pie data={pieData} dataKey="value" nameKey="name" cx="50%" cy="50%"
                     outerRadius={100} label={({ name, value }) => `${name}: ${value}`}>
                  {pieData.map((entry, i) => (
                    <Cell key={i} fill={ROUND_COLORS[normalizeRoundType(entry.key)] || COLORS[i % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip formatter={(v, n) => [v, '件数']} />
              </PieChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <div className="empty-state-inline">
            フィルタ条件に合致するラウンドがありません。
          </div>
        )}
      </div>
    </>
  )
}

function InvestorsTab({ investors, pairs }) {
  return (
    <>
      <div className="section">
        <h2>活発な投資家ランキング</h2>
        {investors && investors.length > 0 ? (
          <table>
            <thead>
              <tr>
                <th scope="col">投資家</th>
                <th scope="col">種別</th>
                <th scope="col">ディール数</th>
                <th scope="col">リード</th>
              </tr>
            </thead>
            <tbody>
              {investors.map((inv, i) => (
                <tr key={i}>
                  <td>{inv.investor_name}</td>
                  <td>{inv.investor_type || '-'}</td>
                  <td>{inv.deal_count}</td>
                  <td>{inv.lead_count}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <div className="empty-state-inline">投資家データがまだありません。</div>
        )}
      </div>

      <div className="section">
        <h2>共同投資ペア</h2>
        {pairs && pairs.length > 0 ? (
          <table>
            <thead>
              <tr>
                <th scope="col">投資家A</th>
                <th scope="col">投資家B</th>
                <th scope="col">共通ディール</th>
              </tr>
            </thead>
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
        ) : (
          <div className="empty-state-inline">
            <p>現在、複数案件で共同投資している投資家ペアは検出されていません。</p>
            <p className="hint">
              データ蓄積が進み、同じ投資家が複数ディールに参加すると、
              ここに戦略的アライアンスのパターンが現れます。
            </p>
          </div>
        )}
      </div>
    </>
  )
}

function SectorsTab({ trends, tagDistribution }) {
  const sectors = [...new Set((trends || []).map(t => normalizeSector(t.sector)))]
  const chartData = sectors.map(s => {
    const items = (trends || []).filter(t => normalizeSector(t.sector) === s)
    return { sector: s, count: items.reduce((a, b) => a + b.count, 0) }
  }).sort((a, b) => b.count - a.count)

  return (
    <>
      <div className="section">
        <h2>セクター別投資件数</h2>
        {chartData.length > 0 ? (
          <div className="chart-container">
            <ResponsiveContainer>
              <BarChart data={chartData} layout="vertical" margin={{ left: 140 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis type="number" fontSize={12} />
                <YAxis dataKey="sector" type="category" fontSize={11} width={130} />
                <Tooltip formatter={(v) => [v, '件数']} />
                <Bar dataKey="count" fill="#B07256" name="件数" radius={[0,4,4,0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <div className="empty-state-inline">セクターデータがまだありません。</div>
        )}
      </div>

      {tagDistribution && tagDistribution.length > 0 && (
        <div className="section">
          <h2>タグ分布（v2ファセット分類）</h2>
          <p className="hint">
            v2スキーマでは、セクター（単一）に加えて技術・ビジネスモデル・市場の3カテゴリで
            複数タグを付与できます。タグ蓄積が進むと、産業横断的なクラスタが見えるようになります。
          </p>
          <table>
            <thead>
              <tr>
                <th scope="col">カテゴリ</th>
                <th scope="col">タグ</th>
                <th scope="col">組織数</th>
              </tr>
            </thead>
            <tbody>
              {tagDistribution.map((t, i) => (
                <tr key={i}>
                  <td><code>{t.tag_category}</code></td>
                  <td>{t.tag_name}</td>
                  <td>{t.org_count}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  )
}

function DealsTab({ distribution }) {
  return (
    <div className="section">
      <h2>ラウンドタイプ別ディール</h2>
      {distribution && distribution.length > 0 ? (
        <table>
          <thead>
            <tr>
              <th scope="col">ラウンド</th>
              <th scope="col">件数</th>
              <th scope="col">合計調達額</th>
            </tr>
          </thead>
          <tbody>
            {distribution.map((r, i) => (
              <tr key={i}>
                <td><RoundBadge type={r.round_type} /></td>
                <td>{r.count}</td>
                <td>{formatAmount(r.total_amount_jpy)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <div className="empty-state-inline">ラウンドデータがまだありません。</div>
      )}
    </div>
  )
}

// Category labels for press releases
const PR_CATEGORY_LABELS = {
  funding: '資金調達',
  partnership: '提携・協業',
  product_launch: '製品・サービスリリース',
  hiring: '採用・人事',
  other: 'その他',
}

// Source labels for press releases
const PR_SOURCE_LABELS = {
  prtimes: 'PR TIMES',
  bridge: 'Bridge',
}

function PressReleasesTab({ data }) {
  const prData = data?.press_releases

  // Graceful handling when data is not yet available
  if (!prData || prData.total_count === 0) {
    return (
      <div className="empty-state">
        プレスリリースデータを収集しています…
      </div>
    )
  }

  const fundingPct = prData.total_count > 0
    ? ((prData.funding_related_count / prData.total_count) * 100).toFixed(1)
    : 0

  // Build source breakdown text
  const sourceText = Object.entries(prData.by_source || {})
    .map(([k, v]) => `${PR_SOURCE_LABELS[k] || k}: ${v}`)
    .join(' / ')

  // Monthly chart data with non-funding count
  const monthlyData = (prData.by_month || []).map(m => ({
    ...m,
    non_funding_count: m.count - (m.funding_count || 0),
  }))

  // Pie chart data for source distribution
  const sourceData = Object.entries(prData.by_source || {}).map(([k, v]) => ({
    name: PR_SOURCE_LABELS[k] || k,
    value: v,
  }))

  // Category distribution data
  const categoryData = Object.entries(prData.by_category || {})
    .map(([k, v]) => ({
      category: PR_CATEGORY_LABELS[k] || k,
      count: v,
    }))
    .sort((a, b) => b.count - a.count)

  return (
    <>
      {/* Summary cards */}
      <div className="stats-grid" style={{ marginBottom: 'var(--space-lg)' }}>
        <StatCard label="プレスリリース総数" value={prData.total_count} />
        <StatCard label="資金調達関連" value={`${prData.funding_related_count} (${fundingPct}%)`} />
        <StatCard label="ソース内訳" value={sourceText || '-'} />
      </div>

      {/* Monthly volume chart */}
      <div className="section">
        <h2>月次プレスリリース件数</h2>
        {monthlyData.length > 0 ? (
          <div className="chart-container">
            <ResponsiveContainer>
              <BarChart data={monthlyData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="month" fontSize={12} />
                <YAxis fontSize={12} />
                <Tooltip formatter={(v, n) => [
                  v,
                  n === 'funding_count' ? '資金調達関連' : 'その他',
                ]} />
                <Legend formatter={(v) => v === 'funding_count' ? '資金調達関連' : 'その他'} />
                <Bar dataKey="funding_count" stackId="pr" fill="#DC8766" name="funding_count" radius={[0,0,0,0]} />
                <Bar dataKey="non_funding_count" stackId="pr" fill="#F2C792" name="non_funding_count" radius={[4,4,0,0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <div className="empty-state-inline">月次データはまだありません。</div>
        )}
      </div>

      {/* Source distribution pie chart */}
      {sourceData.length > 0 && (
        <div className="section">
          <h2>ソース分布</h2>
          <div className="chart-container" style={{ height: 260 }}>
            <ResponsiveContainer>
              <PieChart>
                <Pie data={sourceData} dataKey="value" nameKey="name" cx="50%" cy="50%"
                     outerRadius={90} label={({ name, value }) => `${name}: ${value}`}>
                  {sourceData.map((_, i) => (
                    <Cell key={i} fill={COLORS[i % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip formatter={(v) => [v, '件数']} />
              </PieChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* Category distribution */}
      {categoryData.length > 0 && (
        <div className="section">
          <h2>カテゴリ分布</h2>
          <div className="chart-container" style={{ height: Math.max(200, categoryData.length * 50) }}>
            <ResponsiveContainer>
              <BarChart data={categoryData} layout="vertical" margin={{ left: 140 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis type="number" fontSize={12} />
                <YAxis dataKey="category" type="category" fontSize={11} width={130} />
                <Tooltip formatter={(v) => [v, '件数']} />
                <Bar dataKey="count" fill="#B07256" name="件数" radius={[0,4,4,0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* Recent press releases */}
      <div className="section">
        <h2>最新プレスリリース</h2>
        {prData.recent_releases && prData.recent_releases.length > 0 ? (
          <div className="pr-list-scroll">
            <table>
              <thead>
                <tr>
                  <th scope="col">タイトル</th>
                  <th scope="col">企業名</th>
                  <th scope="col">ソース</th>
                  <th scope="col">日付</th>
                  <th scope="col">種別</th>
                </tr>
              </thead>
              <tbody>
                {prData.recent_releases.slice(0, 30).map((pr, i) => (
                  <tr key={i}>
                    <td>
                      {pr.source_url ? (
                        <a href={pr.source_url} target="_blank" rel="noopener noreferrer"
                           className="pr-title-link">
                          {pr.title}
                        </a>
                      ) : pr.title}
                    </td>
                    <td>{pr.company_name || '-'}</td>
                    <td>
                      <span className={`badge badge-source-${pr.source || 'unknown'}`}>
                        {PR_SOURCE_LABELS[pr.source] || pr.source || '-'}
                      </span>
                    </td>
                    <td style={{ whiteSpace: 'nowrap' }}>{formatDateTime(pr.published_at)}</td>
                    <td>
                      {pr.is_funding_related && (
                        <span className="badge badge-funding">資金調達</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="empty-state-inline">プレスリリースデータがまだありません。</div>
        )}
      </div>

      {/* Top companies by PR count */}
      {prData.top_companies_by_pr_count && prData.top_companies_by_pr_count.length > 0 && (
        <div className="section">
          <h2>企業別プレスリリース数ランキング</h2>
          <table>
            <thead>
              <tr>
                <th scope="col">順位</th>
                <th scope="col">企業名</th>
                <th scope="col">件数</th>
              </tr>
            </thead>
            <tbody>
              {prData.top_companies_by_pr_count.map((c, i) => (
                <tr key={i}>
                  <td>{i + 1}</td>
                  <td>{c.company_name}</td>
                  <td>{c.count}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  )
}

function DataHealthTab({ data }) {
  const { stats, data_freshness, data_source_breakdown, event_momentum } = data

  const freshnessData = [
    { name: 'Fresh', key: 'fresh', value: data_freshness?.fresh || 0, color: '#CEA26F' },
    { name: 'Stale', key: 'stale', value: data_freshness?.stale || 0, color: '#F0BE83' },
    { name: 'Expired', key: 'expired', value: data_freshness?.expired || 0, color: '#B07256' },
  ]
  const freshnessTotal = data_freshness?.total || freshnessData.reduce((a, b) => a + b.value, 0)

  return (
    <>
      <div className="section">
        <h2>データ規模（v2スキーマ）</h2>
        <div className="stats-grid">
          <StatCard label="組織数" value={stats?.organizations ?? 0} />
          <StatCard label="うち企業" value={stats?.companies ?? 0} />
          <StatCard label="うち投資家" value={stats?.investors ?? 0} />
          <StatCard label="資金調達ラウンド" value={stats?.funding_rounds ?? 0} />
          <StatCard label="ラウンド参加者" value={stats?.round_participants ?? 0} />
          <StatCard label="イベント" value={stats?.events ?? 0} />
          <StatCard label="セクター" value={stats?.sectors ?? 0} />
          <StatCard label="タグ" value={stats?.tags ?? 0} />
        </div>
        <p className="hint">
          v2では組織（Organization）が中心モデルです。
          企業数と投資家数の合計が組織数より多い場合、
          「両方の役割を持つ組織」（CVCを擁する事業会社など）が存在することを示します。
        </p>
      </div>

      <div className="section">
        <h2>データ新鮮度</h2>
        <p className="hint">
          各組織レコードの最終収集日時から Fresh / Stale / Expired を判定します。
          Stale以上のレコードはバックグラウンドで再検証キューに入ります。
        </p>
        {freshnessTotal > 0 ? (
          <>
            <div
              className="freshness-bar"
              role="img"
              aria-label={`Fresh ${freshnessData[0].value}件, Stale ${freshnessData[1].value}件, Expired ${freshnessData[2].value}件`}
            >
              {freshnessData.map((f) => (
                <div key={f.key} className={`freshness-segment freshness-${f.key}`}
                     style={{ flex: f.value || 0.01 }}>
                  <span>{f.name}: {f.value}</span>
                </div>
              ))}
            </div>
            <dl className="freshness-legend">
              <dt>Fresh</dt><dd>ソフトTTL以内に更新（信頼可能）</dd>
              <dt>Stale</dt><dd>ソフトTTL超過・ハードTTL未満（再検証推奨）</dd>
              <dt>Expired</dt><dd>ハードTTL超過（要再収集）</dd>
            </dl>
          </>
        ) : (
          <div className="empty-state-inline">新鮮度データがまだありません。</div>
        )}
      </div>

      <div className="section">
        <h2>データソース内訳</h2>
        <p className="hint">
          v2スキーマでは各レコードに情報ソースが紐付いており、
          信頼度・TTL・出所追跡が可能です。
        </p>
        {data_source_breakdown && data_source_breakdown.length > 0 ? (
          <table>
            <thead>
              <tr>
                <th scope="col">ソース</th>
                <th scope="col">種別</th>
                <th scope="col">組織数</th>
                <th scope="col">ラウンド数</th>
              </tr>
            </thead>
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
        ) : (
          <div className="empty-state-inline">ソース情報がまだありません。</div>
        )}
      </div>

      <div className="section">
        <h2>イベントモメンタム（直近90日）</h2>
        <p className="hint">
          v2では資金調達以外のイベント（採用・特許・製品発表など）も追跡対象です。
          種別ごとの件数と平均重要度を表示します。
        </p>
        {event_momentum && event_momentum.length > 0 ? (
          <div className="chart-container" style={{ height: 220 }}>
            <ResponsiveContainer>
              <BarChart data={event_momentum}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="event_type" fontSize={12} />
                <YAxis fontSize={12} />
                <Tooltip formatter={(v, n) => [
                  n === 'count' ? v : Number(v).toFixed(2),
                  n === 'count' ? '件数' : '平均重要度',
                ]} />
                <Bar dataKey="count" fill="#DC8766" name="件数" radius={[4,4,0,0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <div className="empty-state-inline">
            直近90日のイベントはまだ検出されていません。
          </div>
        )}
      </div>

      {/* Corporate enrichment section (gBizINFO) */}
      <CorporateEnrichmentSection data={data} />
    </>
  )
}

function CorporateEnrichmentSection({ data }) {
  const enrichData = data?.corporate_enrichment
  if (!enrichData) return null

  const enrichPct = (enrichData.enrichment_rate * 100).toFixed(1)

  return (
    <div className="section">
      <h2>法人情報エンリッチメント（gBizINFO）</h2>
      <p className="hint">
        gBizINFO（経済産業省）と連携し、収集した組織の資本金・設立年・従業員数等を補完しています。
      </p>
      <div className="stats-grid" style={{ marginBottom: 'var(--space-md)' }}>
        <StatCard
          label="エンリッチ済み"
          value={`${enrichData.enriched_count} / ${enrichData.total_organizations}`}
        />
        <StatCard label="エンリッチ率" value={`${enrichPct}%`} />
      </div>

      {/* Enrichment rate progress bar */}
      <div className="enrichment-progress-wrapper">
        <div
          className="enrichment-progress-bar"
          role="progressbar"
          aria-valuenow={enrichPct}
          aria-valuemin={0}
          aria-valuemax={100}
          aria-label={`エンリッチメント進捗: ${enrichPct}%`}
        >
          <div
            className="enrichment-progress-fill"
            style={{ width: `${Math.min(enrichData.enrichment_rate * 100, 100)}%` }}
          />
        </div>
      </div>

      {/* Capital distribution chart */}
      {enrichData.capital_distribution && enrichData.capital_distribution.length > 0 && (
        <>
          <h3 style={{ fontSize: 14, fontWeight: 600, margin: '20px 0 8px', color: 'var(--accent)' }}>
            資本金分布
          </h3>
          <div className="chart-container" style={{ height: 220 }}>
            <ResponsiveContainer>
              <BarChart data={enrichData.capital_distribution}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="range" fontSize={12} />
                <YAxis fontSize={12} />
                <Tooltip formatter={(v) => [v, '企業数']} />
                <Bar dataKey="count" fill="#CEA26F" name="企業数" radius={[4,4,0,0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </>
      )}
    </div>
  )
}

function AboutTab({ data }) {
  const generatedDate = formatDateTime(data?.generated_at)

  return (
    <>
      <div className="section about-section">
        <h2>Investment Signal Radarとは</h2>
        <p>
          Investment Signal Radarは、VC（ベンチャーキャピタル）の投資実行データを自動収集・構造化し、
          投資トレンドの変化をシグナルとして検出するフォーサイト（先見）プラットフォームです。
          どのセクターに資金が集まっているか、どの投資家が活発に動いているかを可視化することで、
          テクノロジーや社会の構造的変化の兆候を早期に捉えることを目的としています。
        </p>
      </div>

      <div className="section about-section">
        <h2>データベース v2.0（2026-04-17 〜）</h2>
        <p>
          2026年4月に、Crunchbase / PitchBook / CB Insights / Dealroom / Harmonic.ai / OpenCorporates など
          主要プラットフォームのベストプラクティスを踏まえてデータベースを全面的にリデザインしました。
          設計の柱は以下の5つです。
        </p>
        <ul>
          <li><strong>Organization中心モデル</strong>：企業・投資家・アクセラレーター等を単一の <code>organizations</code> テーブルに統合。1組織が複数の役割を持てる（CVC等）。</li>
          <li><strong>イベント駆動</strong>：資金調達だけでなく、採用・特許・提携・製品発表などもイベントとして記録し、モメンタム分析の基盤にする。</li>
          <li><strong>データ来歴（Provenance）分離</strong>：各レコードに情報ソース・信頼度・ソフト/ハードTTLを付与。鮮度管理とゴールデンレコード構成に使う。</li>
          <li><strong>ファセット3層分類</strong>：セクター（固定）+ 技術タグ + ビジネスモデルタグ + 市場タグ。産業横断企業にも対応。</li>
          <li><strong>コンポーネント別スコア</strong>：CB Insights Mosaicの思想に基づき、Momentum / Funding / Market / Team / Technology / Network の次元別スコアを独立保存。</li>
        </ul>
      </div>

      <div className="section about-section">
        <h2>データの収集方法</h2>
        <p>現在、以下のソースから自動収集しています。v2では <code>data_sources</code> テーブルで出所を明示的に管理します。</p>
        <table>
          <thead><tr><th scope="col">ソース</th><th scope="col">種別</th><th scope="col">内容</th></tr></thead>
          <tbody>
            <tr><td><code>pr_times_rss</code></td><td>news</td><td>PR TIMESの資金調達プレスリリース（毎時）</td></tr>
            <tr><td><code>the_bridge_rss</code></td><td>news</td><td>スタートアップ専門メディア The Bridge（毎時）</td></tr>
            <tr><td><code>claude_extracted</code></td><td>ml_inferred</td><td>記事本文からClaude APIで抽出した構造化情報</td></tr>
            <tr><td><code>migrated_v1</code></td><td>manual</td><td>v1スキーマから移行された既存データ</td></tr>
            <tr><td><code>houjin_bangou</code></td><td>official</td><td>国税庁法人番号API（今後連携予定）</td></tr>
            <tr><td><code>sec_edgar_form_d</code></td><td>official</td><td>SEC EDGAR Form D（今後連携予定）</td></tr>
            <tr><td><code>gbizinfo</code></td><td>official</td><td>gBizINFO（経済産業省）法人基本情報・資本金・従業員数</td></tr>
            <tr><td><code>pr_times_full</code></td><td>news</td><td>PR TIMESプレスリリース全文（カテゴリ分類付き）</td></tr>
            <tr><td><code>bridge_full</code></td><td>news</td><td>Bridgeプレスリリース全文（カテゴリ分類付き）</td></tr>
          </tbody>
        </table>
        <p>
          収集した記事は2段階キーワードフィルタ（強キーワード1つでOK / 弱キーワード2つ以上で採用）を経て、
          Claude APIで構造化されます。信頼度が low と判定された場合は、より高精度なモデルで再抽出されます。
        </p>
      </div>

      <div className="section about-section">
        <h2>プレスリリース収集</h2>
        <p>
          「プレスリリース」タブでは、PR TIMESおよびBridgeから収集したスタートアップ関連のプレスリリースを
          一覧・分析しています。各プレスリリースはClaude APIによって自動的にカテゴリ分類（資金調達・提携・
          製品リリース・採用・その他）され、資金調達関連のものは投資シグナルとして重点的に追跡されます。
          企業名の自動抽出とマッチングにより、組織データとの紐付けも行われています。
        </p>
      </div>

      <div className="section about-section">
        <h2>法人情報エンリッチメント（gBizINFO）</h2>
        <p>
          gBizINFO（経済産業省が提供する法人情報データベース）を活用し、収集した企業の基本情報を補完しています。
          法人番号をキーとして、資本金・設立年月日・従業員数・業種分類などの公式データを取得し、
          データベース上の組織レコードに統合します。これにより、資金調達データだけでは見えない企業の規模感や
          成長ステージの推定が可能になります。「データヘルス」タブでエンリッチメントの進捗を確認できます。
        </p>
      </div>

      <div className="section about-section">
        <h2>データベース構造（v2.0）</h2>
        <p>主要テーブルは以下の通りです（17テーブル + 4ビュー）。</p>
        <table>
          <thead><tr><th scope="col">テーブル</th><th scope="col">内容</th><th scope="col">主なカラム</th></tr></thead>
          <tbody>
            <tr><td><code>organizations</code></td><td>組織（企業・投資家・両方）</td><td>slug、名称、primary_role、is_company、is_investor、国、ステータス、data_source_id、confidence_score、TTL</td></tr>
            <tr><td><code>funding_rounds</code></td><td>資金調達ラウンド</td><td>organization_id、round_type、調達額(JPY/USD)、バリュエーション、URL、信頼度</td></tr>
            <tr><td><code>round_participants</code></td><td>ラウンド参加者（多対多）</td><td>funding_round_id、investor_id、is_lead、出資額</td></tr>
            <tr><td><code>events</code></td><td>汎用イベントログ</td><td>event_type（funding/hiring/patent/product_launch等）、event_date、significance、JSON payload</td></tr>
            <tr><td><code>sectors</code> / <code>organization_sectors</code></td><td>セクター分類（固定 + 1対多）</td><td>セクター名、primary flag</td></tr>
            <tr><td><code>tags</code> / <code>organization_tags</code> / <code>tag_synonyms</code></td><td>動的タグ（3カテゴリ）</td><td>technology / business_model / market</td></tr>
            <tr><td><code>signal_scores</code> / <code>score_models</code></td><td>コンポーネント別スコアとモデルバージョン管理</td><td>Momentum / Funding / Team / Network など</td></tr>
            <tr><td><code>people</code> / <code>organization_people</code></td><td>人物と組織の関係</td><td>役職、現職/退任、期間</td></tr>
            <tr><td><code>network_metrics</code></td><td>共投資ネットワーク中心性キャッシュ</td><td>次数中心性、固有ベクトル中心性、共投資回数</td></tr>
            <tr><td><code>trend_snapshots</code></td><td>セクター/タグごとの時系列スナップショット</td><td>件数、金額、velocity、acceleration</td></tr>
            <tr><td><code>signals</code></td><td>検出されたフォーサイトシグナル</td><td>signal_type、加速度、期間</td></tr>
            <tr><td><code>data_sources</code></td><td>情報ソースマスタ</td><td>名称、種別、base_confidence、デフォルトTTL</td></tr>
            <tr><td><code>watchlists</code> / <code>watchlist_items</code></td><td>ウォッチリスト</td><td>名前、組織ID</td></tr>
          </tbody>
        </table>
      </div>

      <div className="section about-section">
        <h2>各タブの見方</h2>
        <table>
          <thead><tr><th scope="col">タブ</th><th scope="col">表示内容</th><th scope="col">活用ポイント</th></tr></thead>
          <tbody>
            <tr><td><strong>概要</strong></td><td>月次ディール件数・ラウンド分布・フィルタ</td><td>投資市場全体の活況度とステージ構成を俯瞰</td></tr>
            <tr><td><strong>投資家</strong></td><td>活発な投資家ランキング・共同投資ペア</td><td>積極的な投資家と戦略的アライアンスの特定</td></tr>
            <tr><td><strong>セクター</strong></td><td>セクター別投資件数・タグ分布（v2）</td><td>資金集中領域と産業横断クラスタの把握</td></tr>
            <tr><td><strong>ラウンド</strong></td><td>ラウンドタイプ別のディール件数と合計調達額</td><td>シード/アーリー比率から市場の成熟度を判断</td></tr>
            <tr><td><strong>プレスリリース</strong></td><td>PR TIMES・Bridge収集のプレスリリース一覧・カテゴリ分析</td><td>資金調達以外の企業動向（提携・採用・製品リリース）も把握</td></tr>
            <tr><td><strong>データヘルス</strong></td><td>組織数・データ鮮度・ソース内訳・イベントモメンタム・法人エンリッチメント</td><td>データベースの信頼度と網羅性を可視化（v2新設）</td></tr>
            <tr><td><strong>このツールについて</strong></td><td>仕組み・用語解説・更新情報</td><td>ダッシュボードの前提知識と解釈のガイド</td></tr>
          </tbody>
        </table>
      </div>

      <div className="section about-section">
        <h2>用語解説</h2>
        <dl className="glossary">
          <dt>ラウンドタイプ</dt>
          <dd>
            スタートアップの資金調達段階。
            <strong>Pre-Seed / Seed</strong>（創業初期）→
            <strong>Series A 〜 G</strong>（成長段階）→
            <strong>Late / IPO</strong> の順に進みます。
            <strong>Strategic</strong>は事業会社による戦略的出資、
            <strong>Debt</strong>は融資型、
            <strong>J-KISS</strong>は日本で使われる転換型投資契約です。
          </dd>
          <dt>投資家タイプ</dt>
          <dd>
            <strong>vc</strong>：独立系ベンチャーキャピタル、
            <strong>cvc</strong>：コーポレートVC（事業会社のファンド）、
            <strong>angel</strong>：エンジェル投資家、
            <strong>corporate</strong>：事業会社の直接投資、
            <strong>gov</strong>：政府系機関、
            <strong>bank</strong>：銀行系、
            <strong>accelerator</strong>：アクセラレーター。
          </dd>
          <dt>リード投資家</dt>
          <dd>
            ラウンドで最大の出資額を担い、投資条件交渉を主導する投資家。
            リード投資家の存在は案件の信頼性・注目度の高さを示します。
          </dd>
          <dt>データ鮮度（Fresh / Stale / Expired）</dt>
          <dd>
            OpenCorporates方式のTTL管理。
            各組織レコードに <code>ttl_soft_days</code> と <code>ttl_hard_days</code> が付与され、
            ソフトTTLを超えた時点でバックグラウンド再検証キューに入り、
            ハードTTLを超えたレコードは「Expired」としてフラグされます。
          </dd>
          <dt>PESTLE分析</dt>
          <dd>
            Political / Economic / Social / Technological / Legal / Environmental の
            6観点からマクロ環境を分析するフレームワーク。各投資案件がどの領域に属するかを自動分類しています。
          </dd>
          <dt>Signal of Change（変化のシグナル）</dt>
          <dd>
            将来の大きな変化を予兆する小さな兆候。
            直近4週間の投資件数を過去3ヶ月の月平均と比較し、1.5倍以上の加速が見られるセクターを自動検出します。
          </dd>
        </dl>
      </div>

      <div className="section about-section">
        <h2>更新情報</h2>
        <p>最終データ生成: {generatedDate}</p>
        <p>
          データは毎時自動で収集・更新されます。
          ダッシュボードのデータ（data.json）は収集完了時に再生成されます。
        </p>
      </div>
    </>
  )
}

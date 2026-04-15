import React, { useState, useEffect, useMemo } from 'react'
import {
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from 'recharts'

// Miratuku CI 12-color palette for charts
const COLORS = ['#F0A671', '#CEA26F', '#DC8766', '#B07256', '#7A4033', '#966D5E', '#F2C792', '#F0BE83']
const ROUND_COLORS = {
  seed: '#F2C792', 'pre-a': '#F0A671', a: '#DC8766', b: '#B07256',
  c: '#966D5E', d: '#7A4033', debt: '#CEA26F', strategic: '#F0BE83',
  angel: '#F7BEA2', unknown: '#EFC4A4',
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
    fetch(`./data.json?t=${Date.now()}`)
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
        {['overview', 'investors', 'sectors', 'deals', 'about'].map(t => (
          <button key={t} className={`tab ${tab === t ? 'active' : ''}`} onClick={() => setTab(t)}>
            {{ overview: 'Overview', investors: 'Investors', sectors: 'Sectors', deals: 'Deals', about: 'About' }[t]}
          </button>
        ))}
      </div>

      {tab === 'overview' && <OverviewTab data={data} />}
      {tab === 'investors' && <InvestorsTab investors={top_investors} pairs={co_investment_pairs} />}
      {tab === 'sectors' && <SectorsTab trends={sector_trends} />}
      {tab === 'deals' && <DealsTab distribution={round_distribution} />}
      {tab === 'about' && <AboutTab data={data} />}
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
              <Bar dataKey="deal_count" fill="#DC8766" name="Deals" radius={[4,4,0,0]} />
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
            <Bar dataKey="count" fill="#B07256" name="Deals" radius={[0,4,4,0]} />
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

function AboutTab({ data }) {
  const generatedDate = data?.generated_at ? new Date(data.generated_at).toLocaleString('ja-JP') : '-'

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
        <h2>データの収集方法</h2>
        <p>
          データは以下の2つのRSSフィードから自動収集されています。
        </p>
        <table>
          <thead><tr><th>ソース</th><th>内容</th><th>更新頻度</th></tr></thead>
          <tbody>
            <tr><td>PR TIMES</td><td>日本最大のプレスリリース配信サービス。資金調達に関するプレスリリースをキーワードフィルタで抽出</td><td>毎時</td></tr>
            <tr><td>The Bridge</td><td>スタートアップ専門メディア。資金調達ニュースを自動取得</td><td>毎時</td></tr>
          </tbody>
        </table>
        <p style={{ marginTop: 12 }}>
          収集した記事は、キーワードフィルタリング（2段階: 強キーワード/弱キーワード）で資金調達関連記事のみを選別しています。
          「資金調達」「シリーズA」などの強キーワードは1つでも含まれていれば採用、
          「VC」「ファンド」などの弱キーワードは2つ以上の同時出現で採用します。
        </p>
      </div>

      <div className="section about-section">
        <h2>データの構造化</h2>
        <p>
          フィルタを通過した記事は、Claude API（Anthropic社のAI）を使って自動的に構造化されます。
          各記事から以下の情報が抽出されます。
        </p>
        <table>
          <thead><tr><th>項目</th><th>説明</th><th>例</th></tr></thead>
          <tbody>
            <tr><td>企業名</td><td>資金調達を実施した企業の正式名称</td><td>Acompany</td></tr>
            <tr><td>調達額</td><td>原文表記と日本円換算（万円単位）</td><td>5億円 / 50,000万円</td></tr>
            <tr><td>ラウンド</td><td>資金調達のステージ分類</td><td>seed, pre-a, a, b, c, d, strategic, debt</td></tr>
            <tr><td>投資家</td><td>参加した投資家名・リード投資家の識別・投資家タイプ</td><td>DNX Ventures (vc, lead)</td></tr>
            <tr><td>セクター</td><td>企業の事業領域</td><td>AI/機械学習、建設テック</td></tr>
            <tr><td>信頼度</td><td>抽出精度の自己評価（high/medium/low）</td><td>high</td></tr>
          </tbody>
        </table>
        <p style={{ marginTop: 12 }}>
          信頼度が「low」と判定された場合は、より高精度なモデル（Claude Sonnet）で自動的に再抽出を行い、
          データ品質を担保しています。
        </p>
      </div>

      <div className="section about-section">
        <h2>分析機能</h2>

        <h3>PESTLE分類</h3>
        <p>
          各投資案件は、PESTLE分析のフレームワークに基づいて自動分類されます。
          Political（政治）、Economic（経済）、Social（社会）、Technological（技術）、
          Legal（法律）、Environmental（環境）の6カテゴリのうち、
          最も適切なものがAIによって判定されます。
        </p>

        <h3>シグナル検出（Signal of Change）</h3>
        <p>
          特定セクターへの投資が急増している場合、それを「変化のシグナル」として検出します。
          直近4週間の投資件数を過去3ヶ月の月平均と比較し、
          1.5倍以上の加速が見られるセクターを自動的にフラグします。
          データの蓄積が進むにつれて、この検出精度は向上します。
        </p>

        <h3>共同投資ネットワーク</h3>
        <p>
          複数の案件で共同投資を行っている投資家ペアを自動検出します。
          共同投資パターンは、投資家間の戦略的アライアンスや
          特定セクターへの集中投資の傾向を示す指標となります。
        </p>
      </div>

      <div className="section about-section">
        <h2>各タブの見方</h2>
        <table>
          <thead><tr><th>タブ</th><th>表示内容</th><th>活用のポイント</th></tr></thead>
          <tbody>
            <tr>
              <td><strong>Overview</strong></td>
              <td>月次ディール件数の推移と、ラウンドタイプ別の分布（円グラフ）</td>
              <td>投資市場全体の活況度と、どのステージの案件が多いかを俯瞰できます</td>
            </tr>
            <tr>
              <td><strong>Investors</strong></td>
              <td>活発な投資家ランキング（ディール数・リード数）と共同投資ペア</td>
              <td>今どの投資家が積極的に動いているか、投資家間の連携パターンを把握できます</td>
            </tr>
            <tr>
              <td><strong>Sectors</strong></td>
              <td>セクター別の投資件数（横棒グラフ）</td>
              <td>資金が集中しているセクター＝成長期待が高い領域を特定できます</td>
            </tr>
            <tr>
              <td><strong>Deals</strong></td>
              <td>ラウンドタイプ別のディール件数と合計調達額</td>
              <td>シード/アーリーが多ければ新興領域、レイターが多ければ成熟期の兆候です</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div className="section about-section">
        <h2>データベース構造</h2>
        <p>
          収集したデータはSQLiteデータベースに格納されています。
          主要テーブルは以下の通りです。
        </p>
        <table>
          <thead><tr><th>テーブル</th><th>内容</th><th>主なカラム</th></tr></thead>
          <tbody>
            <tr><td>investments</td><td>投資案件</td><td>企業ID、調達額、ラウンド、日付、PESTLE分類、信頼度</td></tr>
            <tr><td>companies</td><td>企業マスタ</td><td>名称、説明、セクター、国</td></tr>
            <tr><td>investors</td><td>投資家マスタ</td><td>名称、タイプ（VC/CVC/Angel等）</td></tr>
            <tr><td>sectors</td><td>セクターマスタ</td><td>セクター名</td></tr>
            <tr><td>signals</td><td>検出されたシグナル</td><td>シグナル種別、加速度、期間</td></tr>
            <tr><td>sources</td><td>情報ソース</td><td>ソース名、URL、種別</td></tr>
          </tbody>
        </table>
      </div>

      <div className="section about-section">
        <h2>用語解説</h2>
        <dl className="glossary">
          <dt>ラウンドタイプ</dt>
          <dd>
            スタートアップの資金調達段階を示します。
            <strong>seed</strong>（創業初期）→ <strong>pre-a</strong>（シリーズA前）→
            <strong>a</strong>（シリーズA）→ <strong>b/c/d</strong>（成長段階）の順に進みます。
            <strong>strategic</strong>は事業会社による戦略的出資、
            <strong>debt</strong>は融資型の資金調達を指します。
          </dd>

          <dt>投資家タイプ</dt>
          <dd>
            <strong>vc</strong>: ベンチャーキャピタル（独立系ファンド）、
            <strong>cvc</strong>: コーポレートベンチャーキャピタル（事業会社系ファンド）、
            <strong>angel</strong>: エンジェル投資家（個人投資家）、
            <strong>corporate</strong>: 事業会社による直接投資、
            <strong>gov</strong>: 政府系機関
          </dd>

          <dt>リード投資家</dt>
          <dd>
            そのラウンドで最大の出資額を担い、投資条件の交渉を主導する投資家です。
            リード投資家の存在は、案件の信頼性と注目度の高さを示します。
          </dd>

          <dt>PESTLE分析</dt>
          <dd>
            Political（政治）、Economic（経済）、Social（社会）、
            Technological（技術）、Legal（法律）、Environmental（環境）の
            6つの観点からマクロ環境を分析するフレームワークです。
            各投資案件がどの領域に属するかを自動分類しています。
          </dd>

          <dt>Signal of Change（変化のシグナル）</dt>
          <dd>
            フューチャーズ（未来学）の概念で、将来の大きな変化を予兆する小さな兆候を指します。
            このダッシュボードでは、投資の急増を検出することで、
            テクノロジーや社会の構造的変化の兆しを捉えます。
          </dd>
        </dl>
      </div>

      <div className="section about-section">
        <h2>更新情報</h2>
        <p>
          最終データ生成: {generatedDate}
        </p>
        <p>
          データは毎時自動で収集・更新されます。
          ダッシュボードのデータファイル（data.json）はデプロイスクリプトの実行時に再生成されます。
        </p>
      </div>
    </>
  )
}

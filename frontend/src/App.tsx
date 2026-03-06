import { useEffect, useMemo, useState } from 'react'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from 'recharts'

const API_BASE = (import.meta as any).env?.VITE_API_BASE || 'http://localhost:5000'
const LOGO_URL = (import.meta as any).env?.VITE_LOGO_URL || '/logo-crisa.png'

const FAMILIAS = ['AR', 'BL', 'MC', 'ME', 'MU', 'OT', 'PR', 'PV', 'SI', 'TA']
const DIAS = [15, 30, 60]
const ALERTAS = [
  { value: 'Quiebre de stock', label: 'Quiebre de stock', tone: 'danger' },
  { value: 'Stock de Seguridad', label: 'Stock de Seguridad', tone: 'warning' },
  { value: 'Pto de Pedido', label: 'Pto de Pedido', tone: 'info' },
  { value: 'Sobrestock', label: 'Sobrestock', tone: 'purple' },
  { value: 'Sin rotación', label: 'Sin rotación', tone: 'muted' },
  { value: 'OK', label: 'OK', tone: 'success' },
]

type MatrixResponse = {
  columns: string[]
  rows: Record<string, number | string>[]
  source_rows: number
}

type SugerenciaResponse = {
  rows: Record<string, any>[]
  total: number
}

type KpiRow = {
  anio: number
  mes_num: number
  ventas_unidades: number
  ventas_importe: number
  stock_total: number
}

type MultiPickerProps = {
  label: string
  emptyLabel: string
  allLabel: string
  searchPlaceholder: string
  options: string[]
  value: string[]
  onChange: (next: string[]) => void
}

function MultiPicker({
  label,
  emptyLabel,
  allLabel,
  searchPlaceholder,
  options,
  value,
  onChange,
}: MultiPickerProps) {
  const [filter, setFilter] = useState('')
  const filtered = useMemo(() => {
    const q = filter.trim().toLowerCase()
    if (!q) return options
    return options.filter((o) => o.toLowerCase().includes(q))
  }, [filter, options])

  const summary = value.length === 0
    ? emptyLabel
    : value.length === options.length
      ? allLabel
      : `${value.length} seleccionadas`

  return (
    <div className="field">
      <label>{label}</label>
      <details className="multi">
        <summary>{summary}</summary>
        <div className="multi-panel" onClick={(e) => e.stopPropagation()}>
          <div className="multi-toolbar">
            <input
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              placeholder={searchPlaceholder}
            />
            <div className="multi-actions">
              <button type="button" onClick={() => onChange([...options])}>
                Seleccionar todas
              </button>
              <button type="button" onClick={() => onChange([])}>
                Limpiar
              </button>
            </div>
          </div>
          <div className="multi-options">
            {filtered.length === 0 ? (
              <div className="multi-empty">Sin resultados</div>
            ) : (
              filtered.map((opt) => (
                <label key={opt} className="multi-option">
                  <input
                    type="checkbox"
                    checked={value.includes(opt)}
                    onChange={() => {
                      onChange(
                        value.includes(opt)
                          ? value.filter((v) => v !== opt)
                          : [...value, opt]
                      )
                    }}
                  />
                  <span>{opt}</span>
                </label>
              ))
            )}
          </div>
        </div>
      </details>
    </div>
  )
}

const formatNumber = (v: number) =>
  new Intl.NumberFormat('es-AR', { maximumFractionDigits: 2 }).format(v)

const formatMes = (mes: number) =>
  ['ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic'][mes - 1] || ''

const formatSync = (value: string) => {
  if (!value || value === 'Sin datos') return 'Sin datos'
  const d = new Date(value)
  if (!Number.isNaN(d.getTime())) {
    return new Intl.DateTimeFormat('es-AR', {
      day: '2-digit',
      month: '2-digit',
      year: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
    }).format(d)
  }
  return value
}

export default function App() {
  const [tab, setTab] = useState<'distribucion' | 'sugerencia' | 'kpi'>('distribucion')
  const [syncInfo, setSyncInfo] = useState<any>(null)
  const [sucursales, setSucursales] = useState<string[]>([])
  const [selSuc, setSelSuc] = useState<string[]>([])
  const [selFamilias, setSelFamilias] = useState<string[]>([])
  const [selAlertas, setSelAlertas] = useState<string[]>(ALERTAS.map((a) => a.value))
  const [dias, setDias] = useState<number>(30)
  const [codigoInput, setCodigoInput] = useState<string>('')
  const [codigo, setCodigo] = useState<string>('')
  const [matrix, setMatrix] = useState<MatrixResponse>({ columns: [], rows: [], source_rows: 0 })
  const [sugerencia, setSugerencia] = useState<SugerenciaResponse>({ rows: [], total: 0 })
  const [kpi, setKpi] = useState<KpiRow[]>([])
  const [sortCol, setSortCol] = useState<string>('Total')
  const [sortDesc, setSortDesc] = useState<boolean>(true)
  const [logoOk, setLogoOk] = useState<boolean>(true)

  useEffect(() => {
    fetch(`${API_BASE}/sync-info`)
      .then((r) => r.json())
      .then(setSyncInfo)
      .catch(() => setSyncInfo(null))

    fetch(`${API_BASE}/sucursales`)
      .then((r) => r.json())
      .then((d) => setSucursales(d.sucursales || []))
      .catch(() => setSucursales([]))
  }, [])

  const queryMatrix = useMemo(() => {
    const params = new URLSearchParams()
    params.set('dias', String(dias))
    if (selAlertas.length) params.set('alertas', selAlertas.join(','))
    if (selSuc.length) params.set('sucursales', selSuc.join(','))
    if (selFamilias.length) params.set('familias', selFamilias.join(','))
    if (codigo.trim()) params.set('codigos', codigo.trim())
    return params.toString()
  }, [dias, selAlertas, selSuc, selFamilias, codigo])

  useEffect(() => {
    fetch(`${API_BASE}/matriz-distribucion?${queryMatrix}`)
      .then((r) => r.json())
      .then((d) => {
        setMatrix(d)
        if (d?.columns?.includes('Total')) setSortCol('Total')
      })
      .catch(() => setMatrix({ columns: [], rows: [], source_rows: 0 }))
  }, [queryMatrix])

  useEffect(() => {
    if (!matrix.columns.length) return
    if (!matrix.columns.includes(sortCol)) {
      const fallback = matrix.columns.find((c) => c !== baseKey && c !== artKey) || matrix.columns[0]
      setSortCol(fallback)
    }
  }, [matrix.columns, sortCol, baseKey, artKey])

  useEffect(() => {
    if (tab !== 'sugerencia') return
    const params = new URLSearchParams()
    params.set('dias', String(dias))
    fetch(`${API_BASE}/sugerencia-distribucion?${params}`)
      .then((r) => r.json())
      .then((d) => setSugerencia(d))
      .catch(() => setSugerencia({ rows: [], total: 0 }))
  }, [tab, dias])

  useEffect(() => {
    if (tab !== 'kpi') return
    const params = new URLSearchParams()
    if (selSuc.length === 1) params.set('sucursal', selSuc[0])
    fetch(`${API_BASE}/kpi-evolucion?${params}`)
      .then((r) => r.json())
      .then((d) => setKpi(d.rows || []))
      .catch(() => setKpi([]))
  }, [tab, selSuc])

  const baseKey = 'Cód. base / artículo'
  const artKey = 'Cód. Artículo'

  const displayRows = useMemo(() => {
    const rows = [...(matrix.rows || [])]
    const hasTotal = rows.some((r) => String(r[baseKey] || '').toLowerCase() === 'total')
    if (!hasTotal && rows.length && matrix.columns.length) {
      const total: Record<string, any> = {}
      matrix.columns.forEach((c) => {
        if (c === baseKey) total[c] = 'Total'
        else if (c === artKey) total[c] = ''
        else {
          const sum = rows.reduce((acc, r) => acc + (Number(r[c]) || 0), 0)
          total[c] = sum
        }
      })
      rows.push(total)
    }
    return rows
  }, [matrix.rows, matrix.columns, baseKey, artKey])

  const sortedRows = useMemo(() => {
    if (!displayRows.length || !sortCol) return displayRows
    const totalRow = displayRows.find((r) => String(r[baseKey] || '').toLowerCase() === 'total')
    const data = displayRows.filter((r) => r !== totalRow)
    data.sort((a, b) => {
      const va = Number(a[sortCol] ?? 0)
      const vb = Number(b[sortCol] ?? 0)
      if (Number.isNaN(va) && Number.isNaN(vb)) return 0
      if (Number.isNaN(va)) return 1
      if (Number.isNaN(vb)) return -1
      return sortDesc ? vb - va : va - vb
    })
    return totalRow ? [...data, totalRow] : data
  }, [displayRows, sortCol, sortDesc, baseKey])

  const kpiChart = useMemo(() => {
    const rows = [...kpi]
    return rows.map((r) => ({
      ...r,
      mes: `${formatMes(r.mes_num)} ${String(r.anio).slice(-2)}`,
    }))
  }, [kpi])

  const lastSyncLabel = syncInfo?.ultima_sync_saldo_historial ||
    syncInfo?.ultima_sync_ventas ||
    syncInfo?.ultima_sync_saldo ||
    syncInfo?.ultima_sync_precios ||
    syncInfo?.ultima_sync_costos ||
    'Sin datos'

  const totalVentas = kpi.reduce((acc, r) => acc + (r.ventas_unidades || 0), 0)
  const totalImporte = kpi.reduce((acc, r) => acc + (r.ventas_importe || 0), 0)
  const totalStock = kpi.reduce((acc, r) => acc + (r.stock_total || 0), 0)

  return (
    <div className="page">
      <div className="header">
        <div className="logo">
          {logoOk ? (
            <img src={LOGO_URL} alt="Grupo CRISA" onError={() => setLogoOk(false)} />
          ) : (
            <div className="logo-text">CRISA</div>
          )}
          <div className="logo-sub">Grupo CRISA</div>
        </div>
        <div className="title">
          <h1>Reposición de Sucursales</h1>
          <p>Distribución basada en venta y stock real.</p>
        </div>
        <div className="sync">
          <div className="sync-time">{formatSync(String(lastSyncLabel))}</div>
          <div className="sync-label">Última actualización</div>
        </div>
      </div>

      <section className="filters">
        <div className="filter-grid">
          <MultiPicker
            label="Selecciona sucursal"
            emptyLabel="Todas"
            allLabel="Todas"
            searchPlaceholder="Buscar sucursal..."
            options={sucursales}
            value={selSuc}
            onChange={setSelSuc}
          />
          <div className="field">
            <label>Selecciona código base / artículo</label>
            <input
              value={codigoInput}
              onChange={(e) => {
                const val = e.target.value
                setCodigoInput(val)
                if (!val.trim()) {
                  setCodigo('')
                }
              }}
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  e.preventDefault()
                  setCodigo(codigoInput.trim())
                }
              }}
              placeholder="Ej: TA148L* o TA148L01 (separá con coma)"
            />
            <small className="hint">Presioná Enter para aplicar</small>
          </div>
          <MultiPicker
            label="Selecciona familia"
            emptyLabel="Todas las familias"
            allLabel="Todas las familias"
            searchPlaceholder="Buscar familia..."
            options={FAMILIAS}
            value={selFamilias}
            onChange={setSelFamilias}
          />
        </div>

        <div className="filter-row">
          <div className="field">
            <label>Selecciona rango de días</label>
            <div className="segmented">
              {DIAS.map((d) => (
                <button key={d} className={d === dias ? 'active' : ''} onClick={() => setDias(d)}>
                  {d} días
                </button>
              ))}
            </div>
          </div>
          <div className="field">
            <label>Selecciona punto de pedido</label>
            <div className="chip-actions">
              <button type="button" onClick={() => setSelAlertas(ALERTAS.map((a) => a.value))}>Todas</button>
              <button
                type="button"
                onClick={() => setSelAlertas([
                  'Quiebre de stock',
                  'Stock de Seguridad',
                  'Pto de Pedido',
                ])}
              >
                Críticas
              </button>
              <button type="button" onClick={() => setSelAlertas([])}>Limpiar</button>
            </div>
            <div className="chip-row">
              {ALERTAS.map((a) => (
                <button
                  key={a.value}
                  className={`chip ${a.tone} ${selAlertas.includes(a.value) ? 'on' : ''}`}
                  onClick={() => {
                    setSelAlertas((prev) =>
                      prev.includes(a.value) ? prev.filter((p) => p !== a.value) : [...prev, a.value]
                    )
                  }}
                >
                  {a.label}
                </button>
              ))}
            </div>
          </div>
        </div>
      </section>

      <section className="tabs">
        <button className={tab === 'distribucion' ? 'active' : ''} onClick={() => setTab('distribucion')}>
          Distribución
        </button>
        <button className={tab === 'sugerencia' ? 'active' : ''} onClick={() => setTab('sugerencia')}>
          Sugerencia de Distribución
        </button>
        <button className={tab === 'kpi' ? 'active' : ''} onClick={() => setTab('kpi')}>
          KPI
        </button>
      </section>

      {tab === 'distribucion' && (
        <section className="panel">
          <div className="panel-head">
            <div>
              <h2>Necesidad de distribución</h2>
              <p>Filtrado por venta y stock en CDD.</p>
            </div>
            <div className="sort-box">
              <label>Ordenar matriz por</label>
              <select value={sortCol} onChange={(e) => setSortCol(e.target.value)}>
                {(matrix.columns || [])
                  .filter((c) => c !== baseKey && c !== artKey)
                  .map((c) => (
                    <option key={c} value={c}>{c}</option>
                  ))}
              </select>
              <label className="checkbox">
                <input type="checkbox" checked={sortDesc} onChange={(e) => setSortDesc(e.target.checked)} />
                Mayor a menor
              </label>
            </div>
          </div>

          <div className="matrix-wrap">
            {matrix.columns.length === 0 ? (
              <div className="empty">Sin datos para mostrar con los filtros seleccionados.</div>
            ) : (
              <table className="matrix">
                <thead>
                  <tr>
                    {matrix.columns.map((c, idx) => (
                      <th key={c} className={idx < 2 ? 'text' : 'num'}>
                        {c}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {sortedRows.map((row, i) => {
                    const rowKey = `${row[baseKey] ?? ''}-${row[artKey] ?? ''}-${i}`
                    return (
                      <tr key={rowKey}>
                        {matrix.columns.map((c, idx) => {
                          const v = row[c] ?? ''
                          const n = typeof v === 'number' ? v : Number(v)
                          const cls = idx < 2 ? 'text' : 'num'
                          const tone = idx < 2
                            ? ''
                            : Number.isFinite(n)
                              ? n < 0 ? 'neg' : n > 0 ? 'pos' : 'zero'
                              : ''
                          return (
                            <td key={c} className={`${cls} ${tone}`}>
                              {typeof v === 'number' ? formatNumber(v) : (Number.isFinite(n) ? formatNumber(n) : String(v))}
                            </td>
                          )
                        })}
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            )}
          </div>
        </section>
      )}

      {tab === 'sugerencia' && (
        <section className="panel">
          <div className="panel-head">
            <div>
              <h2>Sugerencia de distribución</h2>
              <p>Listado por sucursal y artículo.</p>
            </div>
          </div>
          <div className="table-wrap">
            {sugerencia.rows.length === 0 ? (
              <div className="empty">Sin sugerencias para el período seleccionado.</div>
            ) : (
              <table className="simple">
                <thead>
                  <tr>
                    {Object.keys(sugerencia.rows[0]).map((c) => (
                      <th key={c}>{c}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {sugerencia.rows.map((r, idx) => (
                    <tr key={idx}>
                      {Object.keys(r).map((c) => (
                        <td key={c}>{String(r[c])}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </section>
      )}

      {tab === 'kpi' && (
        <section className="panel">
          <div className="panel-head">
            <div>
              <h2>KPI de evolución</h2>
              <p>Ventas y stock agregados por mes.</p>
            </div>
          </div>
          <div className="kpi-grid">
            <div className="kpi-card accent-blue">
              <div className="kpi-title">Ventas (unidades)</div>
              <div className="kpi-value">{formatNumber(totalVentas)}</div>
            </div>
            <div className="kpi-card accent-amber">
              <div className="kpi-title">Ventas ($)</div>
              <div className="kpi-value">{formatNumber(totalImporte)}</div>
            </div>
            <div className="kpi-card accent-teal">
              <div className="kpi-title">Stock total</div>
              <div className="kpi-value">{formatNumber(totalStock)}</div>
            </div>
          </div>
          <div className="chart">
            {kpiChart.length === 0 ? (
              <div className="empty">Sin datos de KPI para mostrar.</div>
            ) : (
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={kpiChart}>
                  <XAxis dataKey="mes" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Line type="monotone" dataKey="ventas_unidades" name="Ventas (u)" stroke="#0f766e" strokeWidth={2} />
                  <Line type="monotone" dataKey="stock_total" name="Stock" stroke="#f97316" strokeWidth={2} />
                </LineChart>
              </ResponsiveContainer>
            )}
          </div>
        </section>
      )}
    </div>
  )
}

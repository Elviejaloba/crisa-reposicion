import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'

type RechartsModule = typeof import('recharts')

const defaultApiBase =
  typeof window !== 'undefined'
    ? (() => {
        const host = window.location.hostname
        const safeHost = !host || host === '0.0.0.0' || host === '[::]' ? '127.0.0.1' : host
        return `http://${safeHost}:5003`
      })()
    : 'http://127.0.0.1:5003'
const API_BASE = (import.meta as any).env?.VITE_API_BASE || defaultApiBase
const LOGO_URL = (import.meta as any).env?.VITE_LOGO_URL || '/logo-crisa.png'
const APP_VERSION =
  (import.meta as any).env?.VITE_APP_VERSION ||
  (import.meta as any).env?.VITE_GIT_SHA ||
  'dev'

const DEFAULT_FAMILIAS = ['AR', 'BL', 'MC', 'ME', 'MU', 'OT', 'PR', 'PV', 'SI', 'TA']
const DIAS = [15, 30, 60]
const ALERTAS = [
  { value: 'Quiebre de stock', label: 'Quiebre de stock', tone: 'danger' },
  { value: 'Stock de Seguridad', label: 'Stock de Seguridad', tone: 'warning' },
  { value: 'Pto de Pedido', label: 'Pto de Pedido', tone: 'info' },
  { value: 'Sobrestock', label: 'Sobrestock', tone: 'purple' },
  { value: 'Sin rotación', label: 'Sin rotación', tone: 'muted' },
  { value: 'OK', label: 'OK', tone: 'success' },
]

const BASE_KEY = 'Cód. base / artículo'
const ART_KEY = 'Cód. Artículo'

const SUC_NUESTRAS = [
  'LA TIJERA MENDOZA',
  'LA TIJERA SAN JUAN',
  'LA TIJERA SAN LUIS',
]

const SUC_FRANQUICIAS = [
  'LA TIJERA LUJAN',
  'LA TIJERA SAN RAFAEL',
  'LA TIJERA SMARTIN',
  'LA TIJERA TUNUYAN',
  'LA TIJERA MAIPU',
]
const SUC_CRISA_TELAS = ['CRISA 2']

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
  const detailsRef = useRef<HTMLDetailsElement | null>(null)
  const filtered = useMemo(() => {
    const q = filter.trim().toLowerCase()
    if (!q) return options
    return options.filter((o) => o.toLowerCase().includes(q))
  }, [filter, options])

  const summary = value.length === 0
    ? emptyLabel
    : value.length === options.length
      ? allLabel
      : (() => {
          const first = value.slice(0, 2)
          const rest = value.length - first.length
          return rest > 0 ? `${first.join(', ')} +${rest}` : first.join(', ')
        })()

  return (
    <div className="field">
      <label>{label}</label>
      <details className="multi" ref={detailsRef}>
        <summary title={value.length ? value.join(', ') : summary}>{summary}</summary>
        <div className="multi-panel" onClick={(e) => e.stopPropagation()}>
          <div className="multi-toolbar">
            <input
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              placeholder={searchPlaceholder}
            />
            <div className="multi-actions">
              <button
                type="button"
                onClick={() => {
                  onChange([...options])
                  detailsRef.current?.removeAttribute('open')
                }}
              >
                Seleccionar todas
              </button>
              <button
                type="button"
                onClick={() => {
                  onChange([])
                  detailsRef.current?.removeAttribute('open')
                }}
              >
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
                      detailsRef.current?.removeAttribute('open')
                    }}
                  />
                  <span className="multi-label">{opt}</span>
                  {value.length > 1 && (
                    <button
                      type="button"
                      className="multi-only"
                      onClick={(e) => {
                        e.preventDefault()
                        e.stopPropagation()
                        onChange([opt])
                        detailsRef.current?.removeAttribute('open')
                      }}
                    >
                      Solo
                    </button>
                  )}
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

const formatMoney = (v: number) =>
  new Intl.NumberFormat('es-AR', { style: 'currency', currency: 'ARS', maximumFractionDigits: 0 }).format(v)

const parseLocaleNumber = (value: unknown): number => {
  if (typeof value === 'number') return value
  if (typeof value !== 'string') return Number.NaN
  const s = value.trim()
  if (!s) return Number.NaN
  const normalized = s.replace(/\./g, '').replace(',', '.')
  if (!/^-?\d+(\.\d+)?$/.test(normalized)) return Number.NaN
  return Number(normalized)
}

const formatMes = (mes: number) =>
  ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'][mes - 1] || ''

const normalizeDateString = (value: string) => {
  const trimmed = value.trim()
  if (!trimmed) return trimmed
  const withT = trimmed.includes('T') ? trimmed : trimmed.replace(' ', 'T')
  return withT.replace(/(\.\d{3})\d+/, '$1')
}

const formatSync = (value: string) => {
  if (!value || value === 'Sin datos') return 'Sin datos'
  const normalized = normalizeDateString(value)
  const d = new Date(normalized)
  if (!Number.isNaN(d.getTime())) {
    return new Intl.DateTimeFormat('es-AR', {
      day: '2-digit',
      month: '2-digit',
      year: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
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
  const [temporada, setTemporada] = useState<string>('')
  const [familias, setFamilias] = useState<string[]>(DEFAULT_FAMILIAS)
  const [matrix, setMatrix] = useState<MatrixResponse>({ columns: [], rows: [], source_rows: 0 })
  const [matrixLoading, setMatrixLoading] = useState<boolean>(false)
  const [matrixError, setMatrixError] = useState<string>('')
  const [sugerencia, setSugerencia] = useState<SugerenciaResponse>({ rows: [], total: 0 })
  const [sugerenciaLoading, setSugerenciaLoading] = useState<boolean>(false)
  const [sugerenciaError, setSugerenciaError] = useState<string>('')
  const [sugSortCol, setSugSortCol] = useState<string>('stock_sucursal')
  const [sugSortDesc, setSugSortDesc] = useState<boolean>(true)
  const [sugRowLimit, setSugRowLimit] = useState<number>(200)
  const [sugOnlyPositive, setSugOnlyPositive] = useState<boolean>(true)
  const [kpi, setKpi] = useState<KpiRow[]>([])
  const [kpiMeta, setKpiMeta] = useState<any>(null)
  const [kpiLoading, setKpiLoading] = useState<boolean>(false)
  const [kpiError, setKpiError] = useState<string>('')
  const [kpiRanking, setKpiRanking] = useState<any[]>([])
  const [kpiRankingTotal, setKpiRankingTotal] = useState<number>(0)
  const [kpiRankingLoading, setKpiRankingLoading] = useState<boolean>(false)
  const [kpiRankingError, setKpiRankingError] = useState<string>('')
  const [kpiFamilias, setKpiFamilias] = useState<any[]>([])
  const [kpiFamiliasLoading, setKpiFamiliasLoading] = useState<boolean>(false)
  const [kpiFamiliasError, setKpiFamiliasError] = useState<string>('')
  const [kpiFocusPanel, setKpiFocusPanel] = useState<'ventas' | 'ranking' | 'familias' | null>(null)
  const [rechartsMod, setRechartsMod] = useState<RechartsModule | null>(null)
  const [sortCol, setSortCol] = useState<string>('CRISA CENTRAL')
  const [sortDesc, setSortDesc] = useState<boolean>(true)
  const [logoOk, setLogoOk] = useState<boolean>(true)
  const [rowLimit, setRowLimit] = useState<number>(200)
  const [soloNuevos, setSoloNuevos] = useState<boolean>(false)
  const [filtersOpen, setFiltersOpen] = useState<boolean>(true)
  const matrixRef = useRef<HTMLTableElement | null>(null)
  const matrixWrapRef = useRef<HTMLDivElement | null>(null)
  const sugerenciaWrapRef = useRef<HTMLDivElement | null>(null)

  const nuestrasDisponibles = useMemo(
    () => SUC_NUESTRAS.filter((s) => sucursales.includes(s)),
    [sucursales]
  )
  const franquiciasDisponibles = useMemo(
    () => SUC_FRANQUICIAS.filter((s) => sucursales.includes(s)),
    [sucursales]
  )
  const telasDisponibles = useMemo(
    () => SUC_CRISA_TELAS.filter((s) => sucursales.includes(s)),
    [sucursales]
  )
  const isSameSelection = (a: string[], b: string[]) =>
    a.length === b.length && a.every((v) => b.includes(v))
  const toggleQuickSuc = (target: string[]) => {
    setSelSuc((prev) => (isSameSelection(prev, target) ? [] : target))
  }

  const summarizeList = (items: string[], emptyLabel: string, max = 2) => {
    if (!items.length) return emptyLabel
    const clean = items.map((v) => String(v)).filter((v) => v)
    if (clean.length <= max) return clean.join(', ')
    return `${clean.slice(0, max).join(', ')} +${clean.length - max}`
  }

  const clearFilters = () => {
    setSelSuc([])
    setSelFamilias([])
    setCodigo('')
    setCodigoInput('')
    setSelAlertas(ALERTAS.map((a) => a.value))
    setDias(30)
    setTemporada('')
    setSoloNuevos(false)
  }

  const periodoVentas = useMemo(() => {
    const today = new Date()
    const fmt = (d: Date) =>
      new Intl.DateTimeFormat('es-AR', { day: '2-digit', month: '2-digit', year: '2-digit' }).format(d)
    if (temporada) {
      const year = today.getFullYear()
      if (temporada === 'invierno') {
        let start = new Date(year, 2, 1)
        let end = new Date(year, 7, 31)
        if (today < start) {
          start = new Date(year - 1, 2, 1)
          end = new Date(year - 1, 7, 31)
        }
        if (today < end) end = today
        return `${fmt(start)} - ${fmt(end)}`
      }
      if (temporada === 'verano') {
        let start: Date
        let end: Date
        if (today.getMonth() >= 8) {
          start = new Date(year, 8, 1)
          const febLast = new Date(year + 1, 2, 0).getDate()
          end = new Date(year + 1, 1, febLast)
        } else {
          start = new Date(year - 1, 8, 1)
          const febLast = new Date(year, 2, 0).getDate()
          end = new Date(year, 1, febLast)
        }
        if (today < end) end = today
        return `${fmt(start)} - ${fmt(end)}`
      }
    }
    const end = new Date(today)
    const start = new Date(today)
    start.setDate(end.getDate() - Math.max(dias - 1, 0))
    return `${fmt(start)} - ${fmt(end)}`
  }, [dias, temporada])

  useEffect(() => {
    if (typeof window === 'undefined') return
    const mq = window.matchMedia('(max-width: 900px)')
    const apply = () => {
      if (mq.matches) {
        setFiltersOpen(true)
      }
    }
    apply()
    const legacyMq = mq as MediaQueryList & {
      addListener?: (listener: (this: MediaQueryList, ev: MediaQueryListEvent) => void) => void
      removeListener?: (listener: (this: MediaQueryList, ev: MediaQueryListEvent) => void) => void
    }
    if (legacyMq.addEventListener) {
      legacyMq.addEventListener('change', apply)
      return () => legacyMq.removeEventListener('change', apply)
    }
    legacyMq.addListener?.(apply)
    return () => legacyMq.removeListener?.(apply)
  }, [])

  useEffect(() => {
    fetch(`${API_BASE}/sync-info`)
      .then((r) => r.json())
      .then(setSyncInfo)
      .catch(() => setSyncInfo(null))

    fetch(`${API_BASE}/sucursales`)
      .then((r) => r.json())
      .then((d) => setSucursales(d.sucursales || []))
      .catch(() => setSucursales([]))

    fetch(`${API_BASE}/familias`)
      .then((r) => r.json())
      .then((d) => {
        const list = (d?.familias || []).filter((f: string) => f)
        setFamilias(list.length ? list : DEFAULT_FAMILIAS)
      })
      .catch(() => setFamilias(DEFAULT_FAMILIAS))
  }, [])

  useEffect(() => {
    if (tab !== 'kpi' || rechartsMod) return
    let active = true
    import('recharts')
      .then((mod) => {
        if (active) setRechartsMod(mod)
      })
      .catch(() => {})
    return () => {
      active = false
    }
  }, [tab, rechartsMod])

  const queryMatrixRaw = useMemo(() => {
    const params = new URLSearchParams()
    params.set('dias', String(dias))
    if (selAlertas.length) params.set('alertas', selAlertas.join(','))
    if (selSuc.length) params.set('sucursales', selSuc.join(','))
    if (selFamilias.length) params.set('familias', selFamilias.join(','))
    if (codigo.trim()) params.set('codigos', codigo.trim())
    if (temporada) params.set('temporada', temporada)
    if (soloNuevos) params.set('solo_nuevos', 'true')
    if (soloNuevos) params.set('solo_nuevos', 'true')
    params.set('limit', String(rowLimit))
    return params.toString()
  }, [dias, selAlertas, selSuc, selFamilias, codigo, rowLimit, temporada, soloNuevos])

  const [queryMatrix, setQueryMatrix] = useState(queryMatrixRaw)

  useEffect(() => {
    const t = setTimeout(() => setQueryMatrix(queryMatrixRaw), 300)
    return () => clearTimeout(t)
  }, [queryMatrixRaw])

  useEffect(() => {
    setMatrixLoading(true)
    setMatrixError('')
    const controller = new AbortController()
    let timedOut = false
    const timeoutId = window.setTimeout(() => {
      timedOut = true
      controller.abort()
    }, 20000)
    fetch(`${API_BASE}/matriz-distribucion?${queryMatrix}`, { signal: controller.signal })
      .then(async (r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then((d) => {
        setMatrix(d)
        if (d?.columns?.includes('CRISA CENTRAL')) setSortCol('CRISA CENTRAL')
      })
      .catch((err) => {
        if (err?.name === 'AbortError') {
          if (timedOut) setMatrixError('Tiempo de espera agotado')
          return
        }
        setMatrix({ columns: [], rows: [], source_rows: 0 })
        setMatrixError(err?.message || 'Error al cargar')
      })
      .finally(() => {
        window.clearTimeout(timeoutId)
        setMatrixLoading(false)
      })
    return () => {
      window.clearTimeout(timeoutId)
      controller.abort()
    }
  }, [queryMatrix])

  useEffect(() => {
    if (!matrix.columns.length) return
    if (!matrix.columns.includes(sortCol)) {
      const fallback = matrix.columns.find((c) => c !== BASE_KEY && c !== ART_KEY) || matrix.columns[0]
      setSortCol(fallback)
    }
  }, [matrix.columns, sortCol])

  useEffect(() => {
    if (tab !== 'sugerencia') return
    const params = new URLSearchParams()
    params.set('dias', String(dias))
    params.set('limit', String(sugRowLimit))
    params.set('solo_sugeridos', String(sugOnlyPositive))
    if (selAlertas.length) params.set('alertas', selAlertas.join(','))
    if (selSuc.length) params.set('sucursales', selSuc.join(','))
    if (selFamilias.length) params.set('familias', selFamilias.join(','))
    if (codigo.trim()) params.set('codigos', codigo.trim())
    if (temporada) params.set('temporada', temporada)
    if (soloNuevos) params.set('solo_nuevos', 'true')
    setSugerenciaLoading(true)
    setSugerenciaError('')
    const controller = new AbortController()
    let timedOut = false
    const timeoutId = window.setTimeout(() => {
      timedOut = true
      controller.abort()
    }, 20000)
    fetch(`${API_BASE}/sugerencia-distribucion?${params}`, { signal: controller.signal })
      .then(async (r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then((d) => setSugerencia(d))
      .catch((err) => {
        if (err?.name === 'AbortError') {
          if (timedOut) setSugerenciaError('Tiempo de espera agotado')
          return
        }
        setSugerencia({ rows: [], total: 0 })
        setSugerenciaError(err?.message || 'Error al cargar')
      })
      .finally(() => {
        window.clearTimeout(timeoutId)
        setSugerenciaLoading(false)
      })
    return () => {
      window.clearTimeout(timeoutId)
      controller.abort()
    }
  }, [tab, dias, sugRowLimit, sugOnlyPositive, selAlertas, selSuc, selFamilias, codigo, temporada, soloNuevos])

  useEffect(() => {
    if (!sugerencia.rows.length) return
    const keys = Object.keys(sugerencia.rows[0])
    if (!keys.includes(sugSortCol)) {
      const fallback =
        keys.includes('stock_sucursal')
          ? 'stock_sucursal'
          : (keys.find((k) => k !== 'sucursal' && k !== 'cod_articulo') || keys[0])
      setSugSortCol(fallback)
      setSugSortDesc(true)
    }
  }, [sugerencia.rows, sugSortCol])

  useEffect(() => {
    if (tab !== 'sugerencia') return
    // Al entrar en la pestaña, forzar orden por Stock_Sucursal desc
    setSugSortCol('stock_sucursal')
    setSugSortDesc(true)
  }, [tab])

  const sugerenciaColumns = useMemo(() => {
    if (!sugerencia.rows.length) return []
    const keys = Object.keys(sugerencia.rows[0]).filter((k) => k !== 'is_nuevo')
    const preferred = [
      'sucursal',
      'cod_base',
      'cod_articulo',
      'stock_sucursal',
      'stock_cdd',
      'ventas_periodo',
      'venta_promedio_diaria',
      'cobertura_dias',
      'meses_stock',
      'alerta_stock',
      'prioridad',
      'costo_unitario',
      'necesidad',
      'sugerencia_distribuir',
      'valor_reponer_costo',
    ]
    const ordered = preferred.filter((k) => keys.includes(k))
    const rest = keys.filter((k) => !ordered.includes(k))
    return [...ordered, ...rest]
  }, [sugerencia.rows])

  const sugerenciaLabels: Record<string, string> = {
    sucursal: 'Sucursal',
    cod_base: 'Cod. base',
    cod_articulo: 'Cod. artículo',
    stock_sucursal: 'Stock sucursal',
    stock_cdd: 'Stock CDD',
    ventas_periodo: 'Ventas periodo',
    venta_promedio_diaria: 'Venta prom. diaria',
    cobertura_dias: 'Cobertura (dias)',
    meses_stock: 'Meses stock',
    alerta_stock: 'Alerta stock',
    prioridad: 'Prioridad',
    costo_unitario: 'Costo Rep.',
    necesidad: 'Necesidad',
    sugerencia_distribuir: 'Sugerencia',
    valor_reponer_costo: 'Monto a reponer (costo)',
  }

  const sugerenciaHints: Record<string, string> = {
    sucursal: 'Sucursal de la tienda',
    cod_base: 'Codigo base del artículo',
    cod_articulo: 'Codigo especifico del artículo',
    stock_sucursal: 'Stock disponible en la sucursal',
    stock_cdd: 'Stock disponible en CDD',
    ventas_periodo: `Ventas del periodo seleccionado (ERP) (${periodoVentas})`,
    venta_promedio_diaria: `Ventas promedio por dia (${periodoVentas})`,
    cobertura_dias: 'Dias de cobertura: stock / venta diaria',
    meses_stock: 'Meses de cobertura: stock / venta mensual',
    alerta_stock: 'Estado de stock segun cobertura',
    prioridad: 'Prioridad de reposicion',
    costo_unitario: 'Costo de reposicion por unidad',
    necesidad: 'Ventas periodo - stock sucursal',
    sugerencia_distribuir: 'Unidades sugeridas a reponer',
    valor_reponer_costo: 'Sugerencia x Costo Rep.',
  }

  const sugerenciaSortedRows = useMemo(() => {
    if (!sugerencia.rows.length || !sugSortCol) return sugerencia.rows
    const data = [...sugerencia.rows]
    data.sort((a, b) => {
      const vaRaw = a[sugSortCol]
      const vbRaw = b[sugSortCol]
      const va = parseLocaleNumber(vaRaw)
      const vb = parseLocaleNumber(vbRaw)
      const bothNumeric = Number.isFinite(va) && Number.isFinite(vb)
      if (bothNumeric) return sugSortDesc ? vb - va : va - vb
      const sa = String(vaRaw ?? '')
      const sb = String(vbRaw ?? '')
      const cmp = sa.localeCompare(sb, 'es', { numeric: true, sensitivity: 'base' })
      return sugSortDesc ? -cmp : cmp
    })
    return data
  }, [sugerencia.rows, sugSortCol, sugSortDesc])

  const sugerenciaResumen = useMemo(() => {
    const counts: Record<string, number> = {}
    sugerenciaSortedRows.forEach((r) => {
      const k = String(r.prioridad || '').trim() || 'Sin prioridad'
      counts[k] = (counts[k] || 0) + 1
    })
    return counts
  }, [sugerenciaSortedRows])

  const sugerenciaTotales = useMemo(() => {
    const totalUnidades = sugerenciaSortedRows.reduce((acc, r) => acc + (parseLocaleNumber(r.sugerencia_distribuir) || 0), 0)
    const totalVenta = sugerenciaSortedRows.reduce((acc, r) => acc + (parseLocaleNumber(r.valor_reponer_venta) || 0), 0)
    const totalCosto = sugerenciaSortedRows.reduce((acc, r) => acc + (parseLocaleNumber(r.valor_reponer_costo) || 0), 0)
    return { totalUnidades, totalVenta, totalCosto }
  }, [sugerenciaSortedRows])

  useEffect(() => {
    if (tab !== 'kpi') return
    const params = new URLSearchParams()
    if (selSuc.length) params.set('sucursales', selSuc.join(','))
    if (selFamilias.length) params.set('familias', selFamilias.join(','))
    if (codigo.trim()) params.set('codigos', codigo.trim())
    setKpiLoading(true)
    setKpiError('')
    const controller = new AbortController()
    let timedOut = false
    const timeoutId = window.setTimeout(() => {
      timedOut = true
      controller.abort()
    }, 20000)
    fetch(`${API_BASE}/kpi-evolucion?${params}`, { signal: controller.signal })
      .then(async (r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then((d) => {
        setKpi(d.rows || [])
        setKpiMeta(d.stock_hist || null)
      })
      .catch((err) => {
        if (err?.name === 'AbortError') {
          if (timedOut) setKpiError('Tiempo de espera agotado')
          return
        }
        setKpi([])
        setKpiError(err?.message || 'Error al cargar')
      })
      .finally(() => {
        window.clearTimeout(timeoutId)
        setKpiLoading(false)
      })
    return () => {
      window.clearTimeout(timeoutId)
      controller.abort()
    }
  }, [tab, selSuc, selFamilias, codigo])

  useEffect(() => {
    if (tab !== 'kpi') return
    const params = new URLSearchParams()
    params.set('dias', String(dias))
    if (selSuc.length) params.set('sucursales', selSuc.join(','))
    if (selFamilias.length) params.set('familias', selFamilias.join(','))
    if (codigo.trim()) params.set('codigos', codigo.trim())
    if (temporada) params.set('temporada', temporada)
    setKpiRankingLoading(true)
    setKpiRankingError('')
    const controller = new AbortController()
    let timedOut = false
    const timeoutId = window.setTimeout(() => {
      timedOut = true
      controller.abort()
    }, 20000)
    fetch(`${API_BASE}/kpi-alertas-criticas?${params}`, { signal: controller.signal })
      .then(async (r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then((d) => {
        setKpiRanking(d.rows || [])
        setKpiRankingTotal(Number(d.total_monto) || 0)
      })
      .catch((err) => {
        if (err?.name === 'AbortError') {
          if (timedOut) setKpiRankingError('Tiempo de espera agotado')
          return
        }
        setKpiRanking([])
        setKpiRankingTotal(0)
        setKpiRankingError(err?.message || 'Error al cargar')
      })
      .finally(() => {
        window.clearTimeout(timeoutId)
        setKpiRankingLoading(false)
      })
    return () => {
      window.clearTimeout(timeoutId)
      controller.abort()
    }
  }, [tab, dias, selSuc, selFamilias, codigo, temporada, soloNuevos])

  useEffect(() => {
    if (tab !== 'kpi') return
    const params = new URLSearchParams()
    params.set('dias', String(dias))
    if (selSuc.length) params.set('sucursales', selSuc.join(','))
    if (selFamilias.length) params.set('familias', selFamilias.join(','))
    if (codigo.trim()) params.set('codigos', codigo.trim())
    if (temporada) params.set('temporada', temporada)
    if (soloNuevos) params.set('solo_nuevos', 'true')
    setKpiFamiliasLoading(true)
    setKpiFamiliasError('')
    const controller = new AbortController()
    let timedOut = false
    const timeoutId = window.setTimeout(() => {
      timedOut = true
      controller.abort()
    }, 20000)
    fetch(`${API_BASE}/kpi-familias-reponer?${params}`, { signal: controller.signal })
      .then(async (r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`)
        return r.json()
      })
      .then((d) => setKpiFamilias(d.rows || []))
      .catch((err) => {
        if (err?.name === 'AbortError') {
          if (timedOut) setKpiFamiliasError('Tiempo de espera agotado')
          return
        }
        setKpiFamilias([])
        setKpiFamiliasError(err?.message || 'Error al cargar')
      })
      .finally(() => {
        window.clearTimeout(timeoutId)
        setKpiFamiliasLoading(false)
      })
    return () => {
      window.clearTimeout(timeoutId)
      controller.abort()
    }
  }, [tab, dias, selSuc, selFamilias, codigo, temporada, soloNuevos])

  const matrixColumns = useMemo(
    () => matrix.columns.filter((c) => c !== 'is_nuevo'),
    [matrix.columns]
  )

  const displayRows = useMemo(() => {
    const rows = [...(matrix.rows || [])]
    const hasTotal = rows.some((r) => String(r[BASE_KEY] || '').toLowerCase() === 'total')
    const dataRows = rows.filter((r) => String(r[BASE_KEY] || '').toLowerCase() !== 'total')
    const totalCol = 'Total'

    // Filtrar solo artículos con necesidad (Total > 0)
    const filtered = dataRows.filter((r) => {
      const n = parseLocaleNumber(r[totalCol])
      return Number.isFinite(n) ? n > 0 : false
    })

    const total: Record<string, any> = {}
    if (filtered.length && matrixColumns.length) {
      matrixColumns.forEach((c) => {
        if (c === BASE_KEY) total[c] = 'Total'
        else if (c === ART_KEY) total[c] = ''
        else {
          const sum = filtered.reduce((acc, r) => acc + (parseLocaleNumber(r[c]) || 0), 0)
          total[c] = sum
        }
      })
    }

    if (!hasTotal && filtered.length && matrixColumns.length) {
      return [...filtered, total]
    }
    if (hasTotal && filtered.length) {
      return [...filtered, total]
    }
    return filtered
  }, [matrix.rows, matrixColumns])

  const displayColumns = useMemo(() => {
    if (!matrixColumns.length) return []
    const cols = matrixColumns
    const preferred = [
      BASE_KEY,
      ART_KEY,
      'Stock CDD',
      'CRISA CENTRAL',
      'MENDOZA',
      'CRISA 2',
      'LUJAN',
      'MAIPU',
      'SAN JUAN',
      'SAN LUIS',
      'SAN RAFAEL',
      'SMARTIN',
      'TUNUYAN',
      'Total',
    ]
    const ordered = preferred.filter((c) => cols.includes(c))
    const rest = cols.filter((c) => !ordered.includes(c))
    return [...ordered, ...rest]
  }, [matrix.columns])

  const sortedRows = useMemo(() => {
    if (!displayRows.length || !sortCol) return displayRows
    const totalRow = displayRows.find((r) => String(r[BASE_KEY] || '').toLowerCase() === 'total')
    const data = displayRows.filter((r) => r !== totalRow)
    data.sort((a, b) => {
      const vaRaw = a[sortCol]
      const vbRaw = b[sortCol]
      const va = parseLocaleNumber(vaRaw)
      const vb = parseLocaleNumber(vbRaw)
      const bothNumeric = Number.isFinite(va) && Number.isFinite(vb)
      if (bothNumeric) {
        return sortDesc ? vb - va : va - vb
      }
      const sa = String(vaRaw ?? '')
      const sb = String(vbRaw ?? '')
      const cmp = sa.localeCompare(sb, 'es', { numeric: true, sensitivity: 'base' })
      return sortDesc ? -cmp : cmp
    })
    return totalRow ? [...data, totalRow] : data
  }, [displayRows, sortCol, sortDesc])

  const isMatrixAutoFit = displayColumns.length > 0 && displayColumns.length <= 6
  const isSugerenciaAutoFit = sugerenciaColumns.length > 0 && sugerenciaColumns.length <= 6

  const visibleRows = useMemo(() => {
    if (!sortedRows.length) return sortedRows
    const totalRow = sortedRows.find((r) => String(r[BASE_KEY] || '').toLowerCase() === 'total')
    const data = sortedRows.filter((r) => r !== totalRow)
    const sliced = data.slice(0, rowLimit)
    return totalRow ? [...sliced, totalRow] : sliced
  }, [sortedRows, rowLimit])

  useLayoutEffect(() => {
    const table = matrixRef.current
    if (!table) return
    const getHeader = (key: string) => {
      const esc = (window as any).CSS?.escape ? (window as any).CSS.escape(key) : key.replace(/"/g, '\\"')
      return table.querySelector(`th[data-col="${esc}"]`) as HTMLTableCellElement | null
    }
    const update = () => {
      const base = getHeader(BASE_KEY)
      const stock = getHeader('Stock CDD')
      if (base) table.style.setProperty('--col-base', `${base.offsetWidth}px`)
      if (stock) table.style.setProperty('--col-stock', `${stock.offsetWidth}px`)
    }
    update()
    const raf1 = requestAnimationFrame(update)
    const raf2 = requestAnimationFrame(update)
    let ro: ResizeObserver | null = null
    if ('ResizeObserver' in window) {
      ro = new ResizeObserver(() => update())
      ro.observe(table)
    }
    window.addEventListener('resize', update)
    return () => {
      ro?.disconnect()
      window.removeEventListener('resize', update)
      cancelAnimationFrame(raf1)
      cancelAnimationFrame(raf2)
    }
  }, [matrix, displayColumns, visibleRows.length])

  useEffect(() => {
    if (tab !== 'distribucion') return
    const wrap = matrixWrapRef.current
    if (wrap) {
      wrap.scrollLeft = 0
      wrap.scrollTop = 0
    }
  }, [tab, selSuc, selFamilias, selAlertas, codigo, dias, temporada, soloNuevos, rowLimit, displayColumns.length, sortedRows.length])

  useEffect(() => {
    if (tab !== 'sugerencia') return
    const wrap = sugerenciaWrapRef.current
    if (wrap) {
      wrap.scrollLeft = 0
      wrap.scrollTop = 0
    }
  }, [tab, selSuc, selFamilias, selAlertas, codigo, dias, temporada, soloNuevos, sugRowLimit, sugOnlyPositive, sugerenciaColumns.length, sugerenciaSortedRows.length])

  const kpiChart = useMemo(() => {
    const rows = [...kpi]
    return rows.map((r) => ({
      ...r,
      mes: `${formatMes(r.mes_num)} ${r.anio}`,
    }))
  }, [kpi])

  const kpiFamiliasChart = useMemo(() => {
    const rows = [...kpiFamilias]
    return rows
      .map((r) => {
        const raw = String(r.familia ?? '').trim()
        const familia = !raw || raw.toLowerCase() === 'none' ? 'SIN FAMILIA' : raw
        return {
          familia,
          monto: parseLocaleNumber(r.monto_reponer_costo) || 0,
        }
      })
      .sort((a, b) => b.monto - a.monto)
      .slice(0, 8)
  }, [kpiFamilias])

  const stockHistStatus = useMemo(() => {
    const stockMonths = kpi.filter((r) => r.stock_total != null).length
    const meses = kpiMeta?.meses ?? stockMonths
    if (!meses) {
      return {
        tone: 'warn',
        text: 'Stock histórico: sin datos disponibles. El KPI de stock puede verse parcial.',
      }
    }
    if (meses === 1) {
      return {
        tone: 'warn',
        text: `Stock histórico incompleto: solo 1 mes (${kpiMeta?.desde || 'último'}). El KPI de stock puede verse parcial.`,
      }
    }
    if (meses < 3) {
      return {
        tone: 'warn',
        text: `Stock histórico parcial: ${meses} meses. El KPI de stock puede variar.`,
      }
    }
    if (kpiMeta?.desde && kpiMeta?.hasta) {
      return {
        tone: 'ok',
        text: `Stock histórico: ${meses} meses (${kpiMeta.desde} a ${kpiMeta.hasta}).`,
      }
    }
    return {
      tone: 'ok',
      text: `Stock histórico: ${meses} meses.`,
    }
  }, [kpi, kpiMeta])

  const lastSyncLabel = syncInfo?.ultima_sync_saldo_historial ||
    syncInfo?.ultima_sync_ventas ||
    syncInfo?.ultima_sync_saldo ||
    syncInfo?.ultima_sync_precios ||
    syncInfo?.ultima_sync_costos ||
    'Sin datos'

  const Recharts = rechartsMod

  const totalVentas = kpi.reduce((acc, r) => acc + (r.ventas_unidades || 0), 0)
  const totalImporte = kpi.reduce((acc, r) => acc + (r.ventas_importe || 0), 0)
  const totalStock = kpi.reduce((acc, r) => acc + (r.stock_total || 0), 0)

  const downloadExcel = async () => {
    if (!displayColumns.length || !sortedRows.length) return
    const { utils, writeFile } = await import('xlsx')
    const header = displayColumns.map((c) => {
      if (c === BASE_KEY) return 'Cod. base'
      if (c === ART_KEY) return 'Cod. artículo'
      return c
    })
    const rows = sortedRows.map((r) => {
      const row: Record<string, any> = {}
      displayColumns.forEach((c, idx) => {
        const key = header[idx]
        const v = r[c]
        if (c === BASE_KEY || c === ART_KEY) {
          row[key] = v ?? ''
        } else {
          const n = parseLocaleNumber(v)
          row[key] = Number.isFinite(n) ? n : (v ?? '')
        }
      })
      return row
    })
    const ws = utils.json_to_sheet(rows, { header })
    const wb = utils.book_new()
    utils.book_append_sheet(wb, ws, 'Matriz')
    writeFile(wb, `matriz_distribucion_${new Date().toISOString().slice(0, 10)}.xlsx`)
  }

  const downloadSugerenciaExcel = async () => {
    if (!sugerenciaSortedRows.length) return
    const { utils, writeFile } = await import('xlsx')
    const header = sugerenciaColumns.length ? sugerenciaColumns : Object.keys(sugerenciaSortedRows[0])
    const rows = sugerenciaSortedRows.map((r) => {
      const row: Record<string, any> = {}
      header.forEach((k) => {
        row[k] = r[k]
      })
      return row
    })
    const ws = utils.json_to_sheet(rows, { header })
    const wb = utils.book_new()
    utils.book_append_sheet(wb, ws, 'Sugerencia')
    writeFile(wb, `sugerencia_distribucion_${new Date().toISOString().slice(0, 10)}.xlsx`)
  }

  return (
    <div className="page">
      <div className="header">
        <div className="logo">
          {logoOk ? (
            <img src={LOGO_URL} alt="Grupo CRISA" onError={() => setLogoOk(false)} />
          ) : (
            <div className="logo-text">CRISA</div>
          )}
        </div>
        <div className="title">
          <h1>Reposición de Sucursales</h1>
          <p>Distribución basada en venta y stock real.</p>
        </div>
        <div className="sync">
          <div className="sync-time">{formatSync(String(lastSyncLabel))}</div>
          <div className="sync-label">Última actualización</div>
          <div className="sync-next">
            <span className="sync-clock" aria-hidden="true"></span>
            Próxima actualización: cada 60 min
          </div>
        </div>
        <span className="version-badge">v{APP_VERSION}</span>
      </div>

      <section className="filters" data-open={filtersOpen ? 'true' : 'false'}>
        <div className="filters-head">
          <div className="filters-title">Filtros</div>
          <button
            type="button"
            className="filters-toggle"
            onClick={() => {
              setFiltersOpen((open) => !open)
            }}
            aria-expanded={filtersOpen}
            aria-controls="filters-body"
          >
            {filtersOpen ? 'Ocultar filtros' : 'Mostrar filtros'}
          </button>
        </div>
        <div className="filters-body" id="filters-body">
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
            <label>Selecciona código / descripción / sinónimo</label>
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
              placeholder="Ej: TA148L* , REMERA , SINONIMO (separá con coma)"
            />
            <small className="hint">Podés buscar por código, descripción o sinónimo. Presioná Enter para aplicar.</small>
          </div>
          <MultiPicker
            label="Selecciona familia"
            emptyLabel="Todas las familias"
            allLabel="Todas las familias"
            searchPlaceholder="Buscar familia..."
            options={familias}
            value={selFamilias}
            onChange={setSelFamilias}
          />
        </div>

          <div className="active-filters">
            <div className="active-label">Filtros activos</div>
            <div className="chip-row">
              <span className="mini-pill">Sucursales: {summarizeList(selSuc, 'Todas')}</span>
              <span className="mini-pill">Familias: {summarizeList(selFamilias, 'Todas')}</span>
              <span className="mini-pill">Código: {codigo ? codigo : 'Todos'}</span>
              <span className="mini-pill">Alertas: {selAlertas.length === 0 ? 'Ninguna' : selAlertas.length === ALERTAS.length ? 'Todas' : summarizeList(selAlertas, 'Todas')}</span>
              <span className="mini-pill">Productos: {temporada ? (temporada === 'invierno' ? 'Invierno' : 'Verano') : 'Todas'}</span>
              <span className="mini-pill">Artículos nuevos: {soloNuevos ? 'Sí' : 'No'}</span>
              <span className="mini-pill">Días: {dias}</span>
              <button type="button" className="mini-pill action" onClick={clearFilters}>Limpiar filtros</button>
            </div>
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
              <div className="season-row">
                <div className="season-label">Productos por temporada (ventas)</div>
                <div className="chip-row">
                  <button
                    type="button"
                    className={`chip info ${temporada === 'invierno' ? 'on' : ''}`}
                    onClick={() => setTemporada(temporada === 'invierno' ? '' : 'invierno')}
                  >
                    Productos de Invierno
                  </button>
                  <button
                    type="button"
                    className={`chip warning ${temporada === 'verano' ? 'on' : ''}`}
                    onClick={() => setTemporada(temporada === 'verano' ? '' : 'verano')}
                  >
                    Productos de Verano
                  </button>
                </div>
                {temporada ? (
                  <div className="season-range">Periodo aplicado: {periodoVentas}</div>
                ) : null}
              </div>
              <div className="season-row">
                <div className="season-label">Artículos nuevos</div>
                <div className="chip-row">
                  <button
                    type="button"
                    className={`chip new ${soloNuevos ? 'on' : ''}`}
                    onClick={() => setSoloNuevos((v) => !v)}
                    title="Fecha de alta dentro de los últimos 6 meses"
                  >
                    Artículos nuevos
                  </button>
                </div>
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
        </div>
      </section>

      <section className="tabs">
        <button className={tab === 'distribucion' ? 'active' : ''} onClick={() => setTab('distribucion')}>
          Necesidad de Distribución
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
            <div className="panel-title">
              <h2>Necesidad de distribución</h2>
              <p>
                Resumen por artículo con necesidad de distribución según ventas y stock CDD.
                Verde: necesidad positiva (faltante a reponer). Rojo: excedente o sobrestock (sobran unidades).
              </p>
              <div className="quick-filters">
                <button
                  type="button"
                  className={`quick-chip ${isSameSelection(selSuc, nuestrasDisponibles) ? 'on' : ''}`}
                  onClick={() => toggleQuickSuc(nuestrasDisponibles)}
                >
                  Sucursales Nuestras
                </button>
                <button
                  type="button"
                  className={`quick-chip ${isSameSelection(selSuc, franquiciasDisponibles) ? 'on' : ''}`}
                  onClick={() => toggleQuickSuc(franquiciasDisponibles)}
                >
                  Franquicias
                </button>
                <button
                  type="button"
                  className={`quick-chip ${isSameSelection(selSuc, telasDisponibles) ? 'on' : ''}`}
                  onClick={() => toggleQuickSuc(telasDisponibles)}
                >
                  Crisa Telas
                </button>
              </div>
            </div>
            <div className="sort-box">
              <div className="sort-row">
                <div className="sort-item">
                  <label>Filas a mostrar</label>
                  <select value={rowLimit} onChange={(e) => setRowLimit(Number(e.target.value))}>
                    {[100, 200, 500, 1000, 2000].map((n) => (
                      <option key={n} value={n}>{n}</option>
                    ))}
                  </select>
                </div>
              </div>
              <div className="sort-meta">
                <div className="hint">
                  Mostrando {Math.min(rowLimit, Math.max(0, sortedRows.length - 1))} de {Math.max(0, sortedRows.length - 1)}
                </div>
                <button type="button" className="download-btn" onClick={downloadExcel} title="Descargar Excel">
                  <span className="download-icon" aria-hidden="true"></span>
                  Excel
                </button>
              </div>
            </div>
          </div>

          <div className="matrix-wrap" ref={matrixWrapRef}>
            {matrixLoading ? (
              <div className="overlay-loading">
                <div className="spinner"></div>
                <div className="overlay-text">Buscando datos...</div>
              </div>
            ) : matrixError ? (
              <div className="empty error">Error al cargar datos: {matrixError}</div>
            ) : matrix.columns.length === 0 ? (
              <div className="empty">Sin datos para mostrar con los filtros seleccionados.</div>
            ) : (
              <table className={`matrix matrix-need ${isMatrixAutoFit ? 'auto-fit' : ''}`} ref={matrixRef} key={displayColumns.join('|')}>
                <thead>
                  <tr>
                    {displayColumns.map((c, idx) => {
                      const label = c === BASE_KEY ? 'Cod. base' : c === ART_KEY ? 'Cod. artículo' : c
                      const sticky =
                        c === BASE_KEY ? 'sticky-base col-base'
                          : c === 'Stock CDD' ? 'sticky-stock col-stock'
                            : ''
                      const isActive = sortCol === c
                      return (
                        <th key={c} data-col={c} className={`${idx < 2 ? 'text' : 'num'} ${sticky}`}>
                          <button
                            type="button"
                            className="th-btn"
                            onClick={() => {
                              if (isActive) {
                                setSortDesc(!sortDesc)
                              } else {
                                setSortCol(c)
                                setSortDesc(true)
                              }
                            }}
                          >
                            <span>{label}</span>
                            <span className={`sort-caret ${isActive ? (sortDesc ? 'desc' : 'asc') : ''}`} aria-hidden="true"></span>
                          </button>
                        </th>
                      )
                    })}
                  </tr>
                </thead>
                <tbody>
                  {visibleRows.map((row, i) => {
                    const rowKey = `${row[BASE_KEY] ?? ''}-${row[ART_KEY] ?? ''}-${i}`
                    const isTotalRow = String(row[BASE_KEY] || '').toLowerCase() === 'total'
                    const isNuevo = !isTotalRow && Number((row as any).is_nuevo ?? 0) > 0
                    return (
                      <tr key={rowKey} className={isTotalRow ? 'total-row' : ''}>
                        {displayColumns.map((c, idx) => {
                          const v = row[c] ?? ''
                          const n = parseLocaleNumber(v)
                          const cls = idx < 2 ? 'text' : 'num'
                          const sticky =
                            c === BASE_KEY ? 'sticky-base col-base'
                              : c === 'Stock CDD' ? 'sticky-stock col-stock'
                                : ''
                          const isCodeCol = c === BASE_KEY || c === ART_KEY
                          const tone = idx < 2
                            ? ''
                            : Number.isFinite(n)
                              ? n < 0 ? 'neg' : n > 0 ? 'pos' : 'zero'
                              : ''
                          return (
                            <td key={c} className={`${cls} ${tone} ${sticky}`}>
                              {isCodeCol ? (
                                <span className="code-cell">
                                  <span>{String(v ?? '')}</span>
                                  {c === BASE_KEY && isNuevo ? (
                                    <span className="new-tag" title="Artículo nuevo (últimos 6 meses)">Nuevo</span>
                                  ) : null}
                                </span>
                              ) : (Number.isFinite(n) ? formatNumber(n) : String(v))}
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
              <h2>Sugerencia de Distribución</h2>
              <p>Analisis de distribucion.</p>
              <div className="period-pill">Periodo de ventas: {periodoVentas}</div>
              <div className="quick-filters">
                <button
                  type="button"
                  className={`quick-chip ${isSameSelection(selSuc, nuestrasDisponibles) ? 'on' : ''}`}
                  onClick={() => toggleQuickSuc(nuestrasDisponibles)}
                >
                  Sucursales Nuestras
                </button>
                <button
                  type="button"
                  className={`quick-chip ${isSameSelection(selSuc, franquiciasDisponibles) ? 'on' : ''}`}
                  onClick={() => toggleQuickSuc(franquiciasDisponibles)}
                >
                  Franquicias
                </button>
                <button
                  type="button"
                  className={`quick-chip ${isSameSelection(selSuc, telasDisponibles) ? 'on' : ''}`}
                  onClick={() => toggleQuickSuc(telasDisponibles)}
                >
                  Crisa Telas
                </button>
              </div>
              {sugerenciaSortedRows.length > 0 ? (
                <div className="mini-stats">
                  {Object.entries(sugerenciaResumen).map(([k, v]) => (
                    <span key={k} className="mini-pill">{k}: {v}</span>
                  ))}
                  <span className="mini-pill">Unidades sugeridas: {formatNumber(sugerenciaTotales.totalUnidades)}</span>
                  <span className="mini-pill">Monto a reponer (costo): {formatMoney(sugerenciaTotales.totalCosto)}</span>
                </div>
              ) : null}
            </div>
            <div className="sort-box">
              <div className="sort-meta">
                <div className="hint">Filas: {sugerenciaSortedRows.length}</div>
                <div className="toggle-group">
                  <button
                    type="button"
                    className={`toggle-chip ${sugOnlyPositive ? 'on' : ''}`}
                    onClick={() => setSugOnlyPositive((v) => !v)}
                    title="Muestra solo artículos con necesidad positiva (faltantes)"
                  >
                    Mostrar solo faltantes
                  </button>
                  <span className="toggle-hint">Oculta stock 0 o sobrante</span>
                </div>
                <div className="sort-item">
                  <label>Filas a mostrar</label>
                  <select value={sugRowLimit} onChange={(e) => setSugRowLimit(Number(e.target.value))}>
                    {[100, 200, 500, 1000, 2000].map((n) => (
                      <option key={n} value={n}>{n}</option>
                    ))}
                  </select>
                </div>
                <button
                  type="button"
                  className="download-btn"
                  onClick={downloadSugerenciaExcel}
                  title="Descargar Excel"
                >
                  <span className="download-icon" aria-hidden="true"></span>
                  Excel ({sugerenciaSortedRows.length})
                </button>
              </div>
            </div>
          </div>
          <div className="table-wrap" ref={sugerenciaWrapRef}>
            {sugerenciaLoading ? (
              <div className="overlay-loading">
                <div className="spinner"></div>
                <div className="overlay-text">Buscando datos...</div>
              </div>
            ) : sugerenciaError ? (
              <div className="empty error">Error al cargar datos: {sugerenciaError}</div>
            ) : sugerencia.rows.length === 0 ? (
              <div className="empty">Sin sugerencias para el periodo seleccionado.</div>
            ) : (
              <table className={`matrix matrix-sug ${isSugerenciaAutoFit ? 'auto-fit' : ''}`} key={sugerenciaColumns.join('|')}>
                <thead>
                  <tr>
                    {sugerenciaColumns.map((c) => {
                      const isActive = sugSortCol === c
                      const isNumeric =
                        /(stock|venta|meses|necesidad|sugerencia|promedio|importe|cantidad|cobertura|precio|costo|valor|margen)/i.test(c)
                      const label = sugerenciaLabels[c] || c.replace(/_/g, ' ')
                      const hint = sugerenciaHints[c] || label
                      return (
                        <th key={c} className={isNumeric ? 'num' : 'text'}>
                          <button
                            type="button"
                            className="th-btn"
                            onClick={() => {
                              if (isActive) {
                                setSugSortDesc(!sugSortDesc)
                              } else {
                                setSugSortCol(c)
                                setSugSortDesc(true)
                              }
                            }}
                          >
                            <span className="th-text">
                              <span className="th-main">{label.toUpperCase()}</span>
                              {hint ? <span className="th-help" title={hint}>?</span> : null}
                            </span>
                            <span className={`sort-caret ${isActive ? (sugSortDesc ? 'desc' : 'asc') : ''}`} aria-hidden="true"></span>
                          </button>
                        </th>
                      )
                    })}
                  </tr>
                </thead>
                <tbody>
                  {sugerenciaSortedRows.map((r, idx) => {
                    const isNuevo = Number(r.is_nuevo ?? 0) > 0
                    return (
                      <tr key={idx}>
                        {sugerenciaColumns.map((c) => {
                          const v = r[c]
                          const n = parseLocaleNumber(v)
                          const isNumeric =
                            /(stock|venta|meses|necesidad|sugerencia|promedio|importe|cantidad|cobertura|precio|costo|valor|margen)/i.test(c)
                          const isMoney =
                            /(costo_unitario|valor_reponer_costo|valor_reponer_venta|margen_estimado|precio_unitario)/i.test(c)
                          const label = sugerenciaLabels[c] || c.replace(/_/g, ' ')
                          if (c === 'prioridad') {
                            const tag = String(v ?? '').toLowerCase().replace(/\s+/g, '-')
                            return (
                              <td key={c} className="text" title={label}>
                                <span className={`badge ${tag}`}>{String(v ?? '')}</span>
                              </td>
                            )
                          }
                          const displayValue =
                            isNumeric && Number.isFinite(n)
                              ? (isMoney ? formatMoney(n) : formatNumber(n))
                              : String(v ?? '')
                          const showNuevo = isNuevo && c === 'cod_articulo'
                          return (
                            <td key={c} className={isNumeric ? 'num' : 'text'} title={`${label}: ${displayValue}`}>
                              {showNuevo ? (
                                <span className="code-cell">
                                  <span>{displayValue}</span>
                                  <span className="new-tag" title="Artículo nuevo (últimos 6 meses)">Nuevo</span>
                                </span>
                              ) : (
                                displayValue
                              )}
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

      {tab === 'kpi' && (
        <section className="panel">
          <div className="panel-head">
            <div>
              <h2>KPI de Evolucion</h2>
              <p>Indicadores historicos de ventas y stock por mes.</p>
              {stockHistStatus?.text ? (
                <small className={`hint ${stockHistStatus.tone === 'warn' ? 'warn' : ''}`}>
                  {stockHistStatus.text}
                </small>
              ) : null}
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
            <div className="kpi-card accent-rose">
              <div className="kpi-title">Reposicion critica (costo)</div>
              <div className="kpi-value">{formatMoney(kpiRankingTotal)}</div>
            </div>
          </div>

          <div className="kpi-ranking kpi-section">
            <div className="kpi-ranking-head">
              <div>
                <h3>Ranking alertas criticas</h3>
                <p>Quiebre de stock, Stock de Seguridad y Pto de Pedido.</p>
                <div className="kpi-ranking-total">Monto a reponer (costo reposición): {formatMoney(kpiRankingTotal)}</div>
              </div>
              <div className="kpi-ranking-actions">
                <button className="focus-btn" type="button" onClick={() => setKpiFocusPanel('ranking')}>Modo enfoque</button>
              </div>
            </div>
            {kpiRankingLoading ? (
              <div className="overlay-loading">
                <div className="spinner"></div>
                <div className="overlay-text">Calculando ranking...</div>
              </div>
            ) : kpiRankingError ? (
              <div className="empty error">Error al cargar ranking: {kpiRankingError}</div>
            ) : kpiRanking.length === 0 ? (
              <div className="empty">Sin alertas criticas para mostrar.</div>
            ) : (
              <div className="ranking-list">
                {kpiRanking.map((r, idx) => {
                  const monto = parseLocaleNumber(r.monto_reponer_costo) || 0
                  const max = Math.max(...kpiRanking.map((x) => parseLocaleNumber(x.monto_reponer_costo) || 0), 1)
                  const width = Math.round((monto / max) * 100)
                  return (
                    <div key={`${r.sucursal}-${idx}`} className="ranking-row">
                      <div className="ranking-label">{String(r.sucursal || '')}</div>
                      <div className="ranking-bar">
                        <span style={{ width: `${width}%` }}></span>
                      </div>
                      <div className="ranking-value">{formatMoney(monto)}</div>
                    </div>
                  )
                })}
              </div>
            )}
          </div>

          <div className="chart kpi-section">
            {kpiLoading ? (
              <div className="overlay-loading">
                <div className="spinner"></div>
                <div className="overlay-text">Buscando datos...</div>
              </div>
            ) : kpiError ? (
              <div className="empty error">Error al cargar datos: {kpiError}</div>
            ) : kpiChart.length === 0 ? (
              <div className="empty">Sin datos de KPI para mostrar.</div>
            ) : !Recharts ? (
              <div className="overlay-loading">
                <div className="spinner"></div>
                <div className="overlay-text">Cargando gráficos...</div>
              </div>
            ) : (
              <div className="chart-card">
                <div className="chart-head">
                  <div className="chart-title">Evolucion ventas y stock</div>
                  <button className="focus-btn" type="button" onClick={() => setKpiFocusPanel('ventas')}>Modo enfoque</button>
                </div>
                <div className="chart-body">
                  <Recharts.ResponsiveContainer width="100%" height={300}>
                    <Recharts.LineChart data={kpiChart}>
                      <Recharts.XAxis dataKey="mes" />
                      <Recharts.YAxis />
                      <Recharts.Tooltip />
                      <Recharts.Legend />
                      <Recharts.Line type="monotone" dataKey="ventas_unidades" name="Ventas (u)" stroke="#0f766e" strokeWidth={2} />
                      <Recharts.Line type="monotone" dataKey="stock_total" name="Stock" stroke="#f97316" strokeWidth={2} connectNulls={false} />
                    </Recharts.LineChart>
                  </Recharts.ResponsiveContainer>
                </div>
              </div>
            )}
          </div>

          <div className="kpi-ranking kpi-section kpi-familias">
            <div className="kpi-ranking-head">
              <div>
                <h3>Top familias a reponer (costo)</h3>
                <p>Ranking por monto a reponer en alertas criticas.</p>
                <div className="chip-row">
                  <span className="badge critica">Quiebre de stock</span>
                  <span className="badge media">Stock de Seguridad</span>
                  <span className="badge alta">Pto de Pedido</span>
                </div>
              </div>
              <div className="kpi-ranking-actions">
                <button className="focus-btn" type="button" onClick={() => setKpiFocusPanel('familias')}>Modo enfoque</button>
              </div>
            </div>
            {kpiFamiliasLoading ? (
              <div className="overlay-loading">
                <div className="spinner"></div>
                <div className="overlay-text">Calculando familias...</div>
              </div>
            ) : kpiFamiliasError ? (
              <div className="empty error">Error al cargar familias: {kpiFamiliasError}</div>
            ) : kpiFamiliasChart.length === 0 ? (
              <div className="empty">Sin datos para familias.</div>
            ) : !Recharts ? (
              <div className="overlay-loading">
                <div className="spinner"></div>
                <div className="overlay-text">Cargando gráficos...</div>
              </div>
            ) : (
              <Recharts.ResponsiveContainer width="100%" height={260}>
                <Recharts.BarChart data={kpiFamiliasChart} layout="vertical" margin={{ left: 10, right: 20 }}>
                  <Recharts.XAxis type="number" tickFormatter={(v) => formatMoney(Number(v))} />
                  <Recharts.YAxis
                    type="category"
                    dataKey="familia"
                    width={120}
                    tickFormatter={(v) => {
                      const label = String(v || '')
                      return label.length > 16 ? `${label.slice(0, 16)}...` : label
                    }}
                  />
                  <Recharts.Tooltip formatter={(v: any) => formatMoney(Number(v))} />
                  <Recharts.Bar dataKey="monto" fill="#f97316" radius={[4, 4, 4, 4]} />
                </Recharts.BarChart>
              </Recharts.ResponsiveContainer>
            )}
          </div>

          {kpiFocusPanel ? (
            <div className="focus-overlay" onClick={() => setKpiFocusPanel(null)}>
              <div className="focus-panel" onClick={(e) => e.stopPropagation()}>
                <div className="focus-head">
                  <div>Modo enfoque</div>
                  <button type="button" className="focus-close" onClick={() => setKpiFocusPanel(null)}>Cerrar</button>
                </div>
                <div className="focus-body">
                  {kpiFocusPanel === 'ventas' ? (
                    kpiLoading ? (
                      <div className="overlay-loading">
                        <div className="spinner"></div>
                        <div className="overlay-text">Buscando datos...</div>
                      </div>
                    ) : kpiError ? (
                      <div className="empty error">Error al cargar datos: {kpiError}</div>
                    ) : !Recharts ? (
                      <div className="overlay-loading">
                        <div className="spinner"></div>
                        <div className="overlay-text">Cargando gráficos...</div>
                      </div>
                    ) : (
                      <Recharts.ResponsiveContainer width="100%" height={520}>
                        <Recharts.LineChart data={kpiChart}>
                          <Recharts.XAxis dataKey="mes" />
                          <Recharts.YAxis />
                          <Recharts.Tooltip />
                          <Recharts.Legend />
                          <Recharts.Line type="monotone" dataKey="ventas_unidades" name="Ventas (u)" stroke="#0f766e" strokeWidth={2} />
                          <Recharts.Line type="monotone" dataKey="stock_total" name="Stock" stroke="#f97316" strokeWidth={2} connectNulls={false} />
                        </Recharts.LineChart>
                      </Recharts.ResponsiveContainer>
                    )
                  ) : kpiFocusPanel === 'ranking' ? (
                    kpiRankingLoading ? (
                      <div className="overlay-loading">
                        <div className="spinner"></div>
                        <div className="overlay-text">Calculando ranking...</div>
                      </div>
                    ) : kpiRankingError ? (
                      <div className="empty error">Error al cargar ranking: {kpiRankingError}</div>
                    ) : (
                      <div className="ranking-list">
                        {kpiRanking.map((r, idx) => {
                          const monto = parseLocaleNumber(r.monto_reponer_costo) || 0
                          const max = Math.max(...kpiRanking.map((x) => parseLocaleNumber(x.monto_reponer_costo) || 0), 1)
                          const width = Math.round((monto / max) * 100)
                          return (
                            <div key={`${r.sucursal}-${idx}`} className="ranking-row">
                              <div className="ranking-label">{String(r.sucursal || '')}</div>
                              <div className="ranking-bar">
                                <span style={{ width: `${width}%` }}></span>
                              </div>
                              <div className="ranking-value">{formatMoney(monto)}</div>
                            </div>
                          )
                        })}
                      </div>
                    )
                  ) : (
                    kpiFamiliasLoading ? (
                      <div className="overlay-loading">
                        <div className="spinner"></div>
                        <div className="overlay-text">Calculando familias...</div>
                      </div>
                    ) : kpiFamiliasError ? (
                      <div className="empty error">Error al cargar familias: {kpiFamiliasError}</div>
                    ) : !Recharts ? (
                      <div className="overlay-loading">
                        <div className="spinner"></div>
                        <div className="overlay-text">Cargando gráficos...</div>
                      </div>
                    ) : (
                      <Recharts.ResponsiveContainer width="100%" height={520}>
                        <Recharts.BarChart data={kpiFamiliasChart} layout="vertical" margin={{ left: 10, right: 20 }}>
                          <Recharts.XAxis type="number" tickFormatter={(v) => formatMoney(Number(v))} />
                          <Recharts.YAxis
                            type="category"
                            dataKey="familia"
                            width={160}
                            tickFormatter={(v) => {
                              const label = String(v || '')
                              return label.length > 22 ? `${label.slice(0, 22)}...` : label
                            }}
                          />
                          <Recharts.Tooltip formatter={(v: any) => formatMoney(Number(v))} />
                          <Recharts.Bar dataKey="monto" fill="#f97316" radius={[4, 4, 4, 4]} />
                        </Recharts.BarChart>
                      </Recharts.ResponsiveContainer>
                    )
                  )}
                </div>
              </div>
            </div>
          ) : null}
        </section>
      )}
    </div>
  )
}











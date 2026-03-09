# Medidas Power BI

## Vta. Año Anterior - Análisis
```DAX
VAR _N =
    SELECTEDVALUE( RangosFechas[Dias], 120 )  -- rango seleccionado (30, 60, 90, 120, 180)
VAR _fechaMaxCal =
    CALCULATE( MAX( 'Calendario'[Fecha] ), ALL('Calendario') )
VAR _fechaHoy =
    MIN( TODAY(), _fechaMaxCal )

-- Fecha inicio del análisis actual
VAR _inicioAnalisis =
    _fechaHoy

-- Fecha inicio del período del año anterior (ajustado por meses cortos)
VAR _ultimoDiaMesLY =
    DAY( EOMONTH( DATE( YEAR(_inicioAnalisis) - 1, MONTH(_inicioAnalisis), 1 ), 0 ) )
VAR _diaLY =
    MIN( DAY(_inicioAnalisis), _ultimoDiaMesLY )
VAR _inicioLY =
    DATE( YEAR(_inicioAnalisis) - 1, MONTH(_inicioAnalisis), _diaLY )

-- Fecha fin del período del año anterior (inicio + N días)
VAR _finLY =
    _inicioLY + _N - 1

RETURN
COALESCE(
    CALCULATE(
        [Vta. Año Act. (u) Base],
        KEEPFILTERS( ALLSELECTED( 'Calendario' ) ),   -- mantiene el contexto de los segmentadores
        DATESBETWEEN( 'Calendario'[Fecha], _inicioLY, _finLY )
    ),
    0
)
```
## VTA AÑO ANTERIOR
```DAX
VAR _N =
    SELECTEDVALUE ( RangosFechas[Dias], 120 )

VAR _finAct =
    MIN (
        TODAY (),
        CALCULATE ( MAX ( 'Calendario'[Fecha] ), ALL ( 'Calendario' ) )
    )

-- misma fecha que hoy pero un año atrás (segura para meses cortos)
VAR _ultimoDiaMesLY =
    DAY ( EOMONTH ( DATE ( YEAR ( _finAct ) - 1, MONTH ( _finAct ), 1 ), 0 ) )

VAR _diaLY =
    MIN ( DAY ( _finAct ), _ultimoDiaMesLY )

VAR _finLY =
    DATE ( YEAR ( _finAct ) - 1, MONTH ( _finAct ), _diaLY )

-- rolling N días hacia atrás, SIN corte de año
VAR _inicioLY =
    _finLY - _N + 1

RETURN
COALESCE (
    CALCULATE (
        [Vta. Año Act. (u) Base],
        REMOVEFILTERS ( 'Calendario' ),
        DATESBETWEEN ( 'Calendario'[Fecha], _inicioLY, _finLY )
    ),
    0
)
```
## VTA AÑO ACTUAL
```DAX
VAR _N =
    SELECTEDVALUE ( 'RangosFechas'[Dias], 120 )
VAR _fin =
    MIN (
        TODAY (),
        CALCULATE ( MAX ( 'Calendario'[Fecha] ), ALL ( 'Calendario' ) )
    )
VAR _inicio =
    _fin - _N + 1
RETURN
COALESCE (
    CALCULATE (
        [Vta. Año Act. (u) Base],
        REMOVEFILTERS ( 'Calendario' ),
        DATESBETWEEN ( 'Calendario'[Fecha], _inicio, _fin )
    ),
    0
)
```
## Necesidad
```DAX
VAR _DiasFijos = SELECTEDVALUE ( RangosFechas[Dias], 120 )

VAR _VtaAA_90d =
    CALCULATE (
        [Vta. Año Anterior - Análisis],
        REMOVEFILTERS ( 'Calendario' ),
        REMOVEFILTERS ( 'Slicer PtoPedido' )
    )

VAR _VtaAct_90d =
    CALCULATE (
        [VTA AÑO ACTUAL],
        REMOVEFILTERS ( 'Calendario' ),
        REMOVEFILTERS ( 'Slicer PtoPedido' )
    )

-- Prioridad: primero AA_Analisis; si no hay, usa Actual
VAR _Consumo90d =
    IF ( COALESCE ( _VtaAA_90d, 0 ) > 0, _VtaAA_90d, _VtaAct_90d )

VAR _StockAct =
    CALCULATE (
        [Stock Actual],
        REMOVEFILTERS ( 'Calendario' ),
        REMOVEFILTERS ( RangosFechas ),
        REMOVEFILTERS ( 'Slicer PtoPedido' )
    )

RETURN
    ROUND ( _Consumo90d - _StockAct, 2 )
```
## Sugerencia Distribuir
```DAX
VAR Obj =
    [Stock Objetivo Dist]        -- lo que debería tener para los próximos N días
VAR Stock =
    SUM ( SALDO[Saldo Stock] )   -- sumamos la columna para el contexto actual
RETURN
MAX ( 0, Obj - Stock )
```
## Pedido
```DAX
VAR _nec = [Necesidad]
RETURN MAX ( 0, ROUNDUP ( _nec, 0 ) )
```
## Meses de stock (fija)
```DAX
VAR StockActual =
    CALCULATE (
        SUM ( SALDO[Saldo stock] ),
        REMOVEFILTERS ( 'Calendario' ),
        REMOVEFILTERS ( RangosFechas ),
        REMOVEFILTERS ( 'Slicer PtoPedido' )   -- opcional, si este slicer existe
    )

-- ventas SIEMPRE para 30 días (fijas)
VAR VentasAnt30 =
    CALCULATE (
        [Vta. Año Anterior - Análisis],
        REMOVEFILTERS ( 'Calendario' ),
        REMOVEFILTERS ( RangosFechas ),
        REMOVEFILTERS ( 'Slicer PtoPedido' ),  -- opcional
        RangosFechas[Dias] = 30
    )

VAR VentasAct30 =
    CALCULATE (
        [VTA AÑO ACTUAL],
        REMOVEFILTERS ( 'Calendario' ),
        REMOVEFILTERS ( RangosFechas ),
        REMOVEFILTERS ( 'Slicer PtoPedido' ),  -- opcional
        RangosFechas[Dias] = 30
    )

VAR VentasPeriodo =
    IF ( NOT ISBLANK ( VentasAnt30 ) && VentasAnt30 > 0, VentasAnt30, VentasAct30 )

RETURN
IF (
    StockActual <= 0
        || ISBLANK ( VentasPeriodo )
        || VentasPeriodo <= 0,
    0,
    DIVIDE ( StockActual, VentasPeriodo )
)
```
## Alerta de Suc
```DAX
VAR mesesStock =
    CALCULATE (
        [Meses de stock (RANGO DIAS) BEFORE],
        ALL ( 'Slicer PtoPedido'[PtoPedido] )
    )
VAR ventasActual =
    CALCULATE (
        [VTA AÑO ACTUAL],
        ALL ( 'Slicer PtoPedido'[PtoPedido] )
    )
RETURN
SWITCH (
    TRUE (),

    ISBLANK ( mesesStock ), BLANK (),

    mesesStock = 0 && ventasActual = 0, "🟠 Sin rotación",
    mesesStock < 1 && ventasActual > 1, "⚠️ Quiebre de stock",
    mesesStock >= 1 && mesesStock < 2, "❗ Stock de Seguridad",
    mesesStock >= 2 && mesesStock < 3, "📍 Pto de Pedido",
    mesesStock >= 3 && mesesStock < 4, "✅ OK",
    mesesStock >= 4 && ventasActual = 0, "🟠 Sin rotación",
    mesesStock >= 4, "📦 Sobrestock"
)
```
# Correcciones propuestas (v2)
> Estas versiones respetan el rango seleccionado, el contexto de fechas del reporte y evitan caracteres especiales en alertas.

## VTA AÑO ACTUAL (v2)
```DAX
VAR _N = SELECTEDVALUE ( 'RangosFechas'[Dias], 120 )
VAR _finCtx = CALCULATE ( MAX ( 'Calendario'[Fecha] ), ALLSELECTED ( 'Calendario' ) )
VAR _fin = MIN ( TODAY (), _finCtx )
VAR _inicio = _fin - _N + 1
RETURN
COALESCE (
    CALCULATE (
        [Vta. Año Act. (u) Base],
        REMOVEFILTERS ( 'Calendario' ),
        DATESBETWEEN ( 'Calendario'[Fecha], _inicio, _fin )
    ),
    0
)
```

## VTA AÑO ANTERIOR (v2)
```DAX
VAR _N = SELECTEDVALUE ( RangosFechas[Dias], 120 )
VAR _finActCtx = CALCULATE ( MAX ( 'Calendario'[Fecha] ), ALLSELECTED ( 'Calendario' ) )
VAR _finAct = MIN ( TODAY (), _finActCtx )
VAR _ultimoDiaMesLY = DAY ( EOMONTH ( DATE ( YEAR ( _finAct ) - 1, MONTH ( _finAct ), 1 ), 0 ) )
VAR _diaLY = MIN ( DAY ( _finAct ), _ultimoDiaMesLY )
VAR _finLY = DATE ( YEAR ( _finAct ) - 1, MONTH ( _finAct ), _diaLY )
VAR _inicioLY = _finLY - _N + 1
RETURN
COALESCE (
    CALCULATE (
        [Vta. Año Act. (u) Base],
        REMOVEFILTERS ( 'Calendario' ),
        DATESBETWEEN ( 'Calendario'[Fecha], _inicioLY, _finLY )
    ),
    0
)
```

## Vta. Año Anterior - Análisis (v2)
```DAX
VAR _N = SELECTEDVALUE ( RangosFechas[Dias], 120 )
VAR _finActCtx = CALCULATE ( MAX ( 'Calendario'[Fecha] ), ALLSELECTED ( 'Calendario' ) )
VAR _inicioAnalisis = MIN ( TODAY (), _finActCtx )
VAR _ultimoDiaMesLY = DAY ( EOMONTH ( DATE ( YEAR ( _inicioAnalisis ) - 1, MONTH ( _inicioAnalisis ), 1 ), 0 ) )
VAR _diaLY = MIN ( DAY ( _inicioAnalisis ), _ultimoDiaMesLY )
VAR _inicioLY = DATE ( YEAR ( _inicioAnalisis ) - 1, MONTH ( _inicioAnalisis ), _diaLY )
VAR _finLY = _inicioLY + _N - 1
RETURN
COALESCE (
    CALCULATE (
        [Vta. Año Act. (u) Base],
        REMOVEFILTERS ( 'Calendario' ),
        DATESBETWEEN ( 'Calendario'[Fecha], _inicioLY, _finLY )
    ),
    0
)
```

## Necesidad (v2)
```DAX
VAR _DiasFijos = SELECTEDVALUE ( RangosFechas[Dias], 120 )
VAR _VtaAA =
    CALCULATE (
        [Vta. Año Anterior - Análisis (v2)],
        REMOVEFILTERS ( 'Calendario' ),
        REMOVEFILTERS ( 'Slicer PtoPedido' ),
        RangosFechas[Dias] = _DiasFijos
    )
VAR _VtaAct =
    CALCULATE (
        [VTA AÑO ACTUAL (v2)],
        REMOVEFILTERS ( 'Calendario' ),
        REMOVEFILTERS ( 'Slicer PtoPedido' ),
        RangosFechas[Dias] = _DiasFijos
    )
VAR _Consumo = IF ( COALESCE ( _VtaAA, 0 ) > 0, _VtaAA, _VtaAct )
VAR _StockAct =
    CALCULATE (
        [Stock Actual],
        REMOVEFILTERS ( 'Calendario' ),
        REMOVEFILTERS ( RangosFechas ),
        REMOVEFILTERS ( 'Slicer PtoPedido' )
    )
RETURN ROUND ( _Consumo - _StockAct, 2 )
```

## Sugerencia Distribuir (v2)
```DAX
VAR Obj = [Stock Objetivo Dist (v2)]
VAR Stock = SUM ( SALDO[Saldo Stock] )
VAR StockCDD = [Stock CDD Disponible]
RETURN MAX ( 0, MIN ( Obj - Stock, StockCDD ) )
```

## Pedido (v2)
```DAX
VAR _nec = [Necesidad (v2)]
RETURN MAX ( 0, ROUNDUP ( _nec, 0 ) )
```

## Meses de stock (fija) (v2)
```DAX
VAR StockActual =
    CALCULATE (
        SUM ( SALDO[Saldo stock] ),
        REMOVEFILTERS ( 'Calendario' ),
        REMOVEFILTERS ( RangosFechas ),
        REMOVEFILTERS ( 'Slicer PtoPedido' )
    )
VAR VentasAnt30 =
    CALCULATE (
        [Vta. Año Anterior - Análisis (v2)],
        REMOVEFILTERS ( 'Calendario' ),
        REMOVEFILTERS ( RangosFechas ),
        REMOVEFILTERS ( 'Slicer PtoPedido' ),
        RangosFechas[Dias] = 30
    )
VAR VentasAct30 =
    CALCULATE (
        [VTA AÑO ACTUAL (v2)],
        REMOVEFILTERS ( 'Calendario' ),
        REMOVEFILTERS ( RangosFechas ),
        REMOVEFILTERS ( 'Slicer PtoPedido' ),
        RangosFechas[Dias] = 30
    )
VAR VentasPeriodo = IF ( NOT ISBLANK ( VentasAnt30 ) && VentasAnt30 > 0, VentasAnt30, VentasAct30 )
RETURN
IF ( StockActual <= 0 || ISBLANK ( VentasPeriodo ) || VentasPeriodo <= 0, 0, DIVIDE ( StockActual, VentasPeriodo ) )
```

## Alerta de Suc (v2) - sin emojis
```DAX
VAR mesesStock =
    CALCULATE (
        [Meses de stock (RANGO DIAS) BEFORE],
        ALL ( 'Slicer PtoPedido'[PtoPedido] )
    )
VAR ventasActual =
    CALCULATE (
        [VTA AÑO ACTUAL (v2)],
        ALL ( 'Slicer PtoPedido'[PtoPedido] )
    )
RETURN
SWITCH (
    TRUE (),
    ISBLANK ( mesesStock ), BLANK (),
    mesesStock = 0 && ventasActual = 0, "Sin rotación",
    mesesStock < 1 && ventasActual > 1, "Quiebre de stock",
    mesesStock >= 1 && mesesStock < 2, "Stock de Seguridad",
    mesesStock >= 2 && mesesStock < 3, "Pto de Pedido",
    mesesStock >= 3 && mesesStock < 4, "OK",
    mesesStock >= 4 && ventasActual = 0, "Sin rotación",
    mesesStock >= 4, "Sobrestock"
)
```

## Stock Objetivo Dist (v2)
```DAX
VAR _N = SELECTEDVALUE ( RangosFechas[Dias], 120 )
VAR _LeadTime = 15  -- dias. Ajustar a tu operacion.
VAR _PeriodoRevision = 7  -- dias.
VAR _VentasPeriodo = [VTA AÑO ACTUAL (v2)]
VAR _ConsumoDiario = DIVIDE ( _VentasPeriodo, _N )
VAR _StockSeguridad = _ConsumoDiario * 7  -- 7 dias de seguridad (recomendado)
RETURN (_ConsumoDiario * ( _LeadTime + _PeriodoRevision )) + _StockSeguridad
```

## Stock CDD Disponible (v2) (referencial)
```DAX
-- Ajustar filtro de deposito segun tu modelo.
CALCULATE ( SUM ( SALDO[Saldo stock] ), SALDO[Deposito] = "CDD" )
```

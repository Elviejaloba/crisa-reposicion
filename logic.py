import pandas as pd
from typing import List, Dict, Any
from datetime import datetime, timedelta

def calcular_venta_promedio_diaria(df_ventas: pd.DataFrame) -> pd.DataFrame:
    df_ventas = df_ventas.copy()
    df_ventas["fecha"] = pd.to_datetime(df_ventas["fecha"], errors="coerce")
    df_ventas = df_ventas.dropna(subset=["fecha"])
    
    if df_ventas.empty:
        return pd.DataFrame(columns=["cod_articulo", "sucursal", "total_venta", "venta_promedio_diaria"])
    
    fecha_min = df_ventas["fecha"].min()
    fecha_max = df_ventas["fecha"].max()
    dias_periodo = (fecha_max - fecha_min).days + 1
    
    ventas_agrupadas = df_ventas.groupby(["cod_articulo", "sucursal"]).agg(
        total_venta=("cantidad_venta", "sum")
    ).reset_index()
    
    ventas_agrupadas["venta_promedio_diaria"] = ventas_agrupadas["total_venta"] / dias_periodo
    
    return ventas_agrupadas

def calcular_meses_stock(df_saldo: pd.DataFrame, df_ventas_promedio: pd.DataFrame) -> pd.DataFrame:
    df_resultado = pd.merge(
        df_saldo,
        df_ventas_promedio[["cod_articulo", "sucursal", "venta_promedio_diaria", "total_venta"]],
        on=["cod_articulo", "sucursal"],
        how="left"
    )
    
    df_resultado["venta_promedio_diaria"] = df_resultado["venta_promedio_diaria"].fillna(0)
    df_resultado["total_venta"] = df_resultado["total_venta"].fillna(0)
    
    df_resultado["venta_mensual_proyectada"] = df_resultado["venta_promedio_diaria"] * 30
    
    df_resultado["meses_stock"] = df_resultado.apply(
        lambda row: round(row["stock_1"] / row["venta_mensual_proyectada"], 2) 
        if row["venta_mensual_proyectada"] > 0 else 999,
        axis=1
    )
    
    return df_resultado

def determinar_alertas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    def get_alerta(row):
        if row["venta_mensual_proyectada"] == 0 and row["stock_1"] > 0:
            return "Sin rotación"
        elif row["meses_stock"] > 6:
            return "Sobrestock"
        elif row["meses_stock"] < 1:
            return "Quiebre"
        elif row["meses_stock"] < 2:
            return "Stock de Seguridad"
        else:
            return "Normal"
    
    df["alerta_stock"] = df.apply(get_alerta, axis=1)
    
    return df

def calcular_necesidad(df: pd.DataFrame, objetivo_dias: int = 90) -> pd.DataFrame:
    df = df.copy()
    
    df["necesidad"] = df.apply(
        lambda row: max(0, (row["venta_promedio_diaria"] * objetivo_dias) - row["stock_1"]),
        axis=1
    )
    
    df["dias_stock_actual"] = df.apply(
        lambda row: row["stock_1"] / row["venta_promedio_diaria"] 
        if row["venta_promedio_diaria"] > 0 else 999,
        axis=1
    )
    
    return df

def procesar_datos_completos(saldo: List[Dict], ventas: List[Dict], objetivo_dias: int = 90) -> pd.DataFrame:
    df_saldo = pd.DataFrame(saldo)
    df_ventas = pd.DataFrame(ventas)
    
    if df_saldo.empty:
        return pd.DataFrame()
    
    df_ventas_promedio = calcular_venta_promedio_diaria(df_ventas)
    
    df_resultado = calcular_meses_stock(df_saldo, df_ventas_promedio)
    
    df_resultado = determinar_alertas(df_resultado)
    
    df_resultado = calcular_necesidad(df_resultado, objetivo_dias)
    
    return df_resultado

def calcular_variacion_interanual(df_ventas: pd.DataFrame) -> pd.DataFrame:
    df = df_ventas.copy()
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df["año"] = df["fecha"].dt.year
    
    ventas_por_año = df.groupby(["cod_articulo", "sucursal", "año"]).agg(
        venta_anual=("cantidad_venta", "sum")
    ).reset_index()
    
    años_unicos = sorted(ventas_por_año["año"].unique())
    
    if len(años_unicos) < 2:
        ventas_por_año["variacion"] = 0
        ventas_por_año["variacion_pct"] = 0
        return ventas_por_año
    
    año_actual = años_unicos[-1]
    año_anterior = años_unicos[-2]
    
    ventas_actual = ventas_por_año[ventas_por_año["año"] == año_actual].copy()
    ventas_anterior = ventas_por_año[ventas_por_año["año"] == año_anterior].copy()
    
    ventas_actual = ventas_actual.rename(columns={"venta_anual": "vta_año_actual"})
    ventas_anterior = ventas_anterior.rename(columns={"venta_anual": "vta_año_anterior"})
    
    resultado = pd.merge(
        ventas_actual[["cod_articulo", "sucursal", "vta_año_actual"]],
        ventas_anterior[["cod_articulo", "sucursal", "vta_año_anterior"]],
        on=["cod_articulo", "sucursal"],
        how="outer"
    )
    
    resultado["vta_año_actual"] = resultado["vta_año_actual"].fillna(0)
    resultado["vta_año_anterior"] = resultado["vta_año_anterior"].fillna(0)
    
    resultado["variacion"] = resultado["vta_año_actual"] - resultado["vta_año_anterior"]
    resultado["variacion_pct"] = resultado.apply(
        lambda row: round((row["variacion"] / row["vta_año_anterior"]) * 100, 2) 
        if row["vta_año_anterior"] > 0 else 0,
        axis=1
    )
    
    return resultado

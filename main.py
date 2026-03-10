import os
import logging

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from decimal import Decimal
import math
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import pandas as pd
from datetime import datetime, timezone, timedelta
try:
    from zoneinfo import ZoneInfo
    AR_TZ = ZoneInfo("America/Argentina/Buenos_Aires")
except Exception:
    AR_TZ = timezone(timedelta(hours=-3))
import database as db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api")

def now_ar():
    # Guardar sin tzinfo para evitar que Postgres convierta a UTC en TIMESTAMP sin zona
    return datetime.now(AR_TZ).replace(tzinfo=None)

# Asegurar estructura de base al iniciar servicio API (sin bloquear el arranque)
app = FastAPI(title="Sistema de Análisis Comercial")

import threading


def _init_db_async():
    try:
        db.init_database()
    except Exception as e:
        logger.warning(f"No se pudo inicializar estructura de base: {e}")


@app.on_event("startup")
def startup_init_db():
    threading.Thread(target=_init_db_async, daemon=True).start()
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://0.0.0.0:5173",
        "http://localhost:5000",
        "http://127.0.0.1:5000",
        "https://frontend-ny3cdjm4n-leonardoreyesarg79-gmailcoms-projects.vercel.app",
    ],
    allow_origin_regex=r"https://.*\.vercel\.app",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class SyncData(BaseModel):
    saldo: Optional[List[dict]] = None
    ventas: Optional[List[dict]] = None
    precios: Optional[List[dict]] = None
    costos: Optional[List[dict]] = None
    articulos: Optional[List[dict]] = None
    append: Optional[bool] = False
    reset: Optional[bool] = False
    calculate_metrics: Optional[bool] = False
    incremental: Optional[bool] = True  # Usar UPSERT por defecto

def normalize_saldo_columns(records: List[dict]) -> List[dict]:
    normalized = []
    for r in records:
        stock_val = r.get("Stock 1", r.get("stock_1", 0))
        # Buscar cod_articulo en mÃºltiples formatos posibles
        cod_art = r.get("Cod. Articulo", r.get("Cód. Artículo", r.get("Cod. base / articulo", 
                  r.get("Cód. base / artículo", r.get("cod_articulo", "")))))
        cod_base = r.get("Cod. base / articulo", r.get("Cód. base / artículo", r.get("cod_base", "")))
        desc_base = r.get("Desc. Base / Articulo", r.get("Desc. Base / ArtÃ­culo", r.get("desc_base", "")))
        sinonimo = r.get("Sinonimo", r.get("SinÃ³nimo", r.get("sinonimo", "")))
        cod_dep = r.get("Cod. Deposito", r.get("CÃ³d. DepÃ³sito", r.get("cod_deposito", "")))
        normalized.append({
            "cod_articulo": str(cod_art) if cod_art else "",
            "descripcion": str(r.get("Articulo", r.get("ArtÃ­culo", r.get("Desc. Base / Articulo", 
                           r.get("Desc. Base / ArtÃ­culo", r.get("descripcion", "")))))),
            "sinonimo": str(sinonimo) if sinonimo else "",
            "cod_base": str(cod_base) if cod_base else "",
            "desc_base": str(desc_base) if desc_base else "",
            "sucursal": str(r.get("Sucursal", r.get("sucursal", ""))),
            "nro_sucursal": r.get("Nro. Sucursal", r.get("nro_sucursal", 0)),
            "deposito": str(r.get("Deposito", r.get("DepÃ³sito", r.get("deposito", "")))),
            "cod_deposito": str(cod_dep) if cod_dep else "",
            "familia": str(r.get("Cod. escala 1", r.get("CÃ³d. escala 1", r.get("familia", "")))),
            "desc_familia": str(r.get("Desc. escala 1", r.get("desc_familia", ""))),
            "um_stock": str(r.get("U.M. stock", r.get("um_stock", ""))),
            "stock_1": float(stock_val) if stock_val is not None else 0.0
        })
    return normalized

def normalize_ventas_columns(records: List[dict]) -> List[dict]:
    normalized = []
    for r in records:
        cant_val = r.get("Cantidad venta", r.get("cantidad_venta", 0))
        imp_val = r.get("Imp. prop. c/IVA", r.get("importe", 0))
        # Buscar cod_articulo en mÃºltiples formatos
        cod_art = r.get("Cod. Articulo", r.get("Cód. Artículo", r.get("cod_articulo", "")))
        cod_base = r.get("Cod. base / articulo", r.get("Cód. base / artículo", r.get("cod_base", "")))
        desc_base = r.get("Desc. Base / Articulo", r.get("Desc. Base / ArtÃ­culo", r.get("desc_base", "")))
        sinonimo = r.get("Sinonimo", r.get("SinÃ³nimo", r.get("sinonimo", "")))
        normalized.append({
            "cod_articulo": str(cod_art) if cod_art else "",
            "descripcion": str(r.get("Descripcion", r.get("DescripciÃ³n", r.get("descripcion", "")))),
            "sinonimo": str(sinonimo) if sinonimo else "",
            "cod_base": str(cod_base) if cod_base else "",
            "desc_base": str(desc_base) if desc_base else "",
            "sucursal": str(r.get("Desc. sucursal", r.get("sucursal", ""))),
            "nro_sucursal": r.get("Nro. Sucursal", r.get("nro_sucursal", 0)),
            "fecha": str(r.get("Fecha", r.get("fecha", ""))),
            "cantidad_venta": float(cant_val) if cant_val is not None else 0.0,
            "importe": float(imp_val) if imp_val is not None else 0.0,
            "familia": str(r.get("Cod. Familia (Articulo)", r.get("CÃ³d. Familia (ArtÃ­culo)", r.get("familia", "")))),
            "desc_familia": str(r.get("Descripcion Familia (Articulo)", r.get("DescripciÃ³n Familia (ArtÃ­culo)", r.get("desc_familia", "")))),
            "um_stock": str(r.get("U.M. stock", r.get("um_stock", ""))),
            "unidad_normalizada": str(r.get("Unidad Normalizada", r.get("unidad_normalizada", ""))),
            "rubro_macro": str(r.get("Rubro Macro", r.get("rubro_macro", ""))),
            "categoria_unm": str(r.get("Categoria UNM", r.get("categoria_unm", ""))),
            "tipo_venta": str(r.get("Tipo de Venta", r.get("tipo_venta", ""))),
            "sub_rubro": str(r.get("Sub Rubro", r.get("sub_rubro", "")))
        })
    return normalized

def normalize_precios_columns(records: List[dict]) -> List[dict]:
    normalized = []
    for r in records:
        precio_val = r.get("Precio", r.get("precio", 0))
        # Buscar cod_articulo en mÃºltiples formatos
        cod_art = r.get("Cod. Articulo", r.get("Cód. Artículo", r.get("cod_articulo", "")))
        normalized.append({
            "cod_articulo": str(cod_art) if cod_art else "",
            "descripcion": str(r.get("Descripcion", r.get("DescripciÃ³n", r.get("descripcion", "")))),
            "sinonimo": str(r.get("Sinonimo", r.get("SinÃ³nimo", r.get("sinonimo", "")))),
            "cod_familia": str(r.get("Cod. familia", r.get("CÃ³d. familia", r.get("cod_familia", "")))),
            "familia": str(r.get("Familia", r.get("familia", ""))),
            "precio": float(precio_val) if precio_val is not None else 0.0,
            "nro_lista": str(r.get("Cod. Lista de Precios", r.get("CÃ³d. Lista de Precios", r.get("nro_lista", "")))),
            "nombre_lista": str(r.get("Lista de precios", r.get("nombre_lista", ""))),
            "fecha_modificacion": str(r.get("Fecha de ultima modificacion", r.get("Fecha de Ãºltima modificaciÃ³n", r.get("fecha_modificacion", ""))))
        })
    return normalized

def normalize_costos_columns(records: List[dict]) -> List[dict]:
    normalized = []
    for r in records:
        costo_val = r.get("Costo", r.get("costo", r.get("costo_reposicion", 0)))
        cod_art = r.get("Cod. Articulo", r.get("Cód. Artículo", r.get("cod_articulo", "")))
        normalized.append({
            "cod_articulo": str(cod_art) if cod_art else "",
            "descripcion": str(r.get("Descripcion", r.get("DescripciÃ³n", r.get("descripcion", "")))),
            "costo_reposicion": float(costo_val) if costo_val is not None else 0.0,
            "moneda": "ARS"
        })
    return normalized

def normalize_articulos_columns(records: List[dict]) -> List[dict]:
    normalized = []
    for r in records:
        normalized.append({
            "cod_articulo": str(r.get("Cod. Articulo", r.get("Cód. Artículo", r.get("cod_articulo", "")))),
            "descripcion": str(r.get("Descripcion", r.get("DescripciÃ³n", r.get("descripcion", "")))),
            "desc_adicional": str(r.get("Desc. Adicional", r.get("desc_adicional", ""))),
            "sinonimo": str(r.get("Sinonimo", r.get("SinÃ³nimo", r.get("sinonimo", "")))),
            "cod_base": str(r.get("Cod. base / articulo", r.get("Cód. base / artículo", r.get("cod_base", "")))),
            "desc_base": str(r.get("Desc. Articulo Base", r.get("Desc. ArtÃ­culo Base", r.get("desc_base", "")))),
            "familia": str(r.get("Familia", r.get("familia", ""))),
            "cod_agrupacion": str(r.get("Cod. agrupacion", r.get("CÃ³d. agrupaciÃ³n", r.get("cod_agrupacion", "")))),
            "desc_agrupacion": str(r.get("Desc. agrupacion", r.get("Desc. agrupaciÃ³n", r.get("desc_agrupacion", "")))),
            "codigo_barra": str(r.get("Codigo de Barras", r.get("CÃ³digo de Barras", r.get("codigo_barra", "")))),
            "fecha_alta": r.get("Fecha de alta", r.get("fecha_alta")),
            "um_stock": str(r.get("U.M. stock", r.get("um_stock", ""))),
            "lleva_stock": str(r.get("Lleva stock asociado", r.get("lleva_stock", ""))),
            "doble_um": str(r.get("Lleva doble unidad de medida", r.get("doble_um", ""))),
        })
    return normalized

def calcular_metricas(df_saldo: pd.DataFrame, df_ventas: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula mÃ©tricas de stock usando lÃ³gica de Power BI:
    - Prox 3 meses (AA): Ventas del mismo perÃ­odo de 3 meses del aÃ±o anterior
    - Promedio mensual (COEF): MAX(venta_AA/3, venta_actual/3)
    - Meses de stock: Stock / CoefMensual
    """
    if df_saldo.empty:
        return pd.DataFrame()
    
    df_resultado = df_saldo.copy()
    df_resultado["total_venta"] = 0.0
    df_resultado["venta_promedio_diaria"] = 0.0
    df_resultado["venta_mensual_proyectada"] = 0.0
    df_resultado["meses_stock"] = 0.0
    df_resultado["venta_aa"] = 0.0
    df_resultado["venta_actual"] = 0.0
    
    if not df_ventas.empty:
        df_ventas = df_ventas.copy()
        df_ventas["fecha"] = pd.to_datetime(df_ventas["fecha"], errors="coerce")
        df_ventas["cantidad_venta"] = pd.to_numeric(df_ventas["cantidad_venta"], errors="coerce").fillna(0)  # type: ignore
        df_ventas = df_ventas.dropna(subset=["fecha"])
    
    if not df_saldo.empty:
        df_saldo["stock_1"] = pd.to_numeric(df_saldo["stock_1"], errors="coerce").fillna(0)  # type: ignore
        
        if not df_ventas.empty:
            from dateutil.relativedelta import relativedelta

            hoy = pd.Timestamp.now().normalize()
            dias_periodo = int(os.environ.get("METRIC_DIAS", "120"))

            fin_actual = hoy
            inicio_actual = fin_actual - pd.Timedelta(days=dias_periodo - 1)

            fin_aa = hoy - relativedelta(years=1)
            inicio_aa = fin_aa - pd.Timedelta(days=dias_periodo - 1)

            inicio_aa_analisis = hoy - relativedelta(years=1)
            fin_aa_analisis = inicio_aa_analisis + pd.Timedelta(days=dias_periodo - 1)

            ventas_aa_analisis = df_ventas[
                (df_ventas["fecha"] >= inicio_aa_analisis) &
                (df_ventas["fecha"] <= fin_aa_analisis)
            ].groupby(["cod_articulo", "sucursal"]).agg(
                vta_aa_analisis=("cantidad_venta", "sum")
            ).reset_index()

            ventas_aa = df_ventas[
                (df_ventas["fecha"] >= inicio_aa) &
                (df_ventas["fecha"] <= fin_aa)
            ].groupby(["cod_articulo", "sucursal"]).agg(
                vta_aa=("cantidad_venta", "sum")
            ).reset_index()

            ventas_actual = df_ventas[
                (df_ventas["fecha"] >= inicio_actual) &
                (df_ventas["fecha"] <= fin_actual)
            ].groupby(["cod_articulo", "sucursal"]).agg(
                vta_actual=("cantidad_venta", "sum")
            ).reset_index()

            ventas_total = df_ventas.groupby(["cod_articulo", "sucursal"]).agg(
                total_venta=("cantidad_venta", "sum")
            ).reset_index()

            df_resultado = pd.merge(df_saldo, ventas_total, on=["cod_articulo", "sucursal"], how="left")
            df_resultado = pd.merge(df_resultado, ventas_aa_analisis, on=["cod_articulo", "sucursal"], how="left")
            df_resultado = pd.merge(df_resultado, ventas_aa, on=["cod_articulo", "sucursal"], how="left")
            df_resultado = pd.merge(df_resultado, ventas_actual, on=["cod_articulo", "sucursal"], how="left")

            df_resultado["total_venta"] = df_resultado["total_venta"].fillna(0)
            df_resultado["vta_aa_analisis"] = df_resultado["vta_aa_analisis"].fillna(0)
            df_resultado["vta_aa"] = df_resultado["vta_aa"].fillna(0)
            df_resultado["vta_actual"] = df_resultado["vta_actual"].fillna(0)

            df_resultado["variacion"] = df_resultado["vta_actual"] - df_resultado["vta_aa"]
            df_resultado["variacion_pct"] = df_resultado.apply(
                lambda row: round((row["variacion"] / row["vta_aa"]) * 100, 0) if row["vta_aa"] > 0 else 0,
                axis=1
            )

            df_resultado["proyeccion"] = df_resultado["vta_aa_analisis"] + df_resultado["variacion"]
            df_resultado["necesidad"] = (df_resultado["proyeccion"] - df_resultado["stock_1"]).round(2)
            import math
            df_resultado["pedido"] = df_resultado["necesidad"].apply(lambda x: max(0, math.ceil(x)) if x > 0 else 0)

            # Venta de referencia: AA anÃ¡lisis si existe, si no actual
            df_resultado["venta_ref"] = df_resultado.apply(
                lambda r: r["vta_aa_analisis"] if r["vta_aa_analisis"] > 0 else r["vta_actual"],
                axis=1
            )
            dias_factor = max(dias_periodo / 30, 1)
            df_resultado["venta_mensual_proyectada"] = df_resultado["venta_ref"] / dias_factor
            df_resultado["venta_promedio_diaria"] = df_resultado["venta_ref"] / max(dias_periodo, 1)

            def calcular_meses_stock(row):
                stock = row["stock_1"]
                venta_ref = row["venta_ref"]
                if stock <= 0 or venta_ref <= 0:
                    return 0.0
                return (stock / venta_ref) * dias_factor

            df_resultado["meses_stock"] = df_resultado.apply(calcular_meses_stock, axis=1)

            df_resultado = df_resultado.drop(columns=["proyeccion", "venta_ref"], errors="ignore")
    
    def determinar_alerta(row):
        meses_stock = row.get("meses_stock", 0)
        ventas_actual = row.get("vta_actual", 0)

        if meses_stock is None or pd.isna(meses_stock):
            return ""

        if meses_stock == 0 and ventas_actual == 0:
            return "ðŸŸ  Sin rotaciÃ³n"
        if meses_stock < 1 and ventas_actual > 1:
            return "âš ï¸ Quiebre de stock"
        if meses_stock >= 1 and meses_stock < 2:
            return "â— Stock de Seguridad"
        if meses_stock >= 2 and meses_stock < 3:
            return "ðŸ“ Pto de Pedido"
        if meses_stock >= 3 and meses_stock < 4:
            return "âœ… OK"
        if meses_stock >= 4 and ventas_actual == 0:
            return "ðŸŸ  Sin rotaciÃ³n"
        if meses_stock >= 4:
            return "ðŸ“¦ Sobrestock"
        return ""
    
    df_resultado["alerta_stock"] = df_resultado.apply(determinar_alerta, axis=1)
    
    df_resultado = df_resultado.drop(columns=["venta_actual"], errors="ignore")
    
    return df_resultado

@app.post("/sync")
async def sync_data(data: SyncData):
    try:
        timestamp = now_ar()
        
        # Comando para limpiar tablas
        if data.reset:
            db.clear_tables()
            return {"status": "ok", "message": "Tablas limpiadas"}
        
        # Comando para calcular mÃ©tricas
        if data.calculate_metrics:
            saldo_records = db.get_all_saldo()
            ventas_records = db.get_all_ventas()
            
            df_saldo = pd.DataFrame(saldo_records)
            df_ventas = pd.DataFrame(ventas_records)
            
            df_resultado = calcular_metricas(df_saldo, df_ventas)
            metricas_records = df_resultado.to_dict(orient="records") if not df_resultado.empty else []
            
            db.clear_metricas()
            db.insert_metricas(metricas_records, timestamp)
            db.log_sync(len(saldo_records), len(ventas_records), len(metricas_records), "ok", "MÃ©tricas calculadas")
            
            return {
                "status": "ok",
                "message": f"MÃ©tricas calculadas: {len(metricas_records)} registros",
                "registros": len(metricas_records)
            }
        
        # Insertar datos en lotes (incremental usa UPSERT)
        inserted = {"saldo": 0, "ventas": 0, "precios": 0, "costos": 0, "articulos": 0}
        
        if data.saldo:
            saldo_norm = normalize_saldo_columns(data.saldo)
            if data.incremental:
                inserted["saldo"] = db.upsert_saldo(saldo_norm, timestamp)
            else:
                db.insert_saldo(saldo_norm, timestamp)
                inserted["saldo"] = len(saldo_norm)
            # Guardar snapshot histÃ³rico para KPI de evoluciÃ³n real de stock
            db.insert_saldo_historial_snapshot(saldo_norm, timestamp)
        
        if data.ventas:
            ventas_norm = normalize_ventas_columns(data.ventas)
            if data.incremental:
                inserted["ventas"] = db.upsert_ventas(ventas_norm, timestamp)
            else:
                db.insert_ventas(ventas_norm, timestamp)
                inserted["ventas"] = len(ventas_norm)

        if data.articulos:
            articulos_norm = normalize_articulos_columns(data.articulos)
            inserted["articulos"] = db.upsert_articulos(articulos_norm, timestamp)
            # Regenerar categorÃ­as a partir de nÃ³mina actual
            if inserted["articulos"] > 0:
                db.refresh_categorias_from_articulos()
        
        if data.precios:
            precios_norm = normalize_precios_columns(data.precios)
            if data.incremental:
                inserted["precios"] = db.upsert_precios(precios_norm, timestamp)
            else:
                db.insert_precios(precios_norm, timestamp)
                inserted["precios"] = len(precios_norm)
        
        if data.costos:
            costos_norm = normalize_costos_columns(data.costos)
            inserted["costos"] = db.upsert_costos(costos_norm)
        
        return {
            "status": "ok",
            "message": "Lote recibido (incremental)" if data.incremental else "Lote recibido",
            "inserted": inserted,
            "timestamp": timestamp.isoformat()
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }

@app.get("/data")
async def get_data(sucursal: Optional[str] = None, alerta: Optional[str] = None):
    metricas = db.get_metricas(sucursal, alerta)
    last_sync = db.get_last_sync()
    
    if not metricas:
        return {"status": "empty", "message": "No hay datos sincronizados"}
    
    return {
        "status": "ok",
        "data": {
            "resultado": metricas,
            "last_sync": last_sync
        }
    }

@app.get("/sucursales")
async def get_sucursales():
    return {"sucursales": db.get_sucursales()}

@app.get("/alertas")
async def get_alertas():
    return {"alertas": db.get_alertas_count()}

@app.get("/totales")
async def get_totales(sucursal: Optional[str] = None):
    return db.get_totales(sucursal)

@app.get("/ventas/{cod_articulo}")
async def get_ventas_articulo(cod_articulo: str, sucursal: Optional[str] = None):
    ventas = db.get_ventas_articulo(cod_articulo, sucursal)  # type: ignore
    return {"ventas": ventas, "total": len(ventas)}

@app.get("/precios")
async def get_precios(cod_articulo: Optional[str] = None, nro_lista: Optional[str] = None):
    precios = db.get_precios(cod_articulo, nro_lista)  # type: ignore
    return {"precios": precios, "total": len(precios)}

@app.get("/precios/{cod_articulo}")
async def get_precio_articulo(cod_articulo: str):
    precios = db.get_precio_articulo(cod_articulo)
    return {"precios": precios, "total": len(precios)}

@app.get("/listas-precios")
async def get_listas_precios():
    return {"listas": db.get_listas_precios()}

@app.get("/articulos")
async def get_articulos():
    return {"articulos": db.get_articulos_base()}

# ============== ENDPOINTS DE MATRIZ / KPI ==============

def _parse_csv_param(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]

def _expand_alertas(alertas: List[str]) -> List[str]:
    mapping = {
        "Quiebre de stock": ["?? Quiebre de stock", "Quiebre de stock"],
        "?? Quiebre de stock": ["?? Quiebre de stock", "Quiebre de stock"],
        "Stock de Seguridad": ["? Stock de Seguridad", "Stock de Seguridad"],
        "? Stock de Seguridad": ["? Stock de Seguridad", "Stock de Seguridad"],
        "Pto de Pedido": ["?? Pto de Pedido", "Pto de Pedido"],
        "?? Pto de Pedido": ["?? Pto de Pedido", "Pto de Pedido"],
        "Sobrestock": ["?? Sobrestock", "Sobrestock", "Sobre stock"],
        "?? Sobrestock": ["?? Sobrestock", "Sobrestock", "Sobre stock"],
        "Sin rotación": ["?? Sin rotación", "Sin rotación (sin stock)", "Sin rotación (con sobrestock)", "Sin rotación"],
        "?? Sin rotación": ["?? Sin rotación", "Sin rotación (sin stock)", "Sin rotación (con sobrestock)", "Sin rotación"],
        "OK": ["? OK", "OK"],
        "? OK": ["? OK", "OK"],
    }
    result: List[str] = []
    for alerta in alertas:
        key = alerta.strip()
        for item in mapping.get(key, [key]):
            if item and item not in result:
                result.append(item)
    return result

@app.get("/matriz-distribucion")
async def get_matriz_distribucion(
    dias: int = 30,
    alertas: Optional[str] = None,
    sucursales: Optional[str] = None,
    familias: Optional[str] = None,
    codigos: Optional[str] = None,
    limit: int = 200,
):
    """
    Retorna matriz pivotada lista para grilla.
    Params separados por coma: alertas, sucursales, familias, codigos (admite * como prefijo).
    """
    alertas_list = _expand_alertas(_parse_csv_param(alertas))
    suc_list = _parse_csv_param(sucursales)
    fam_list = [f.upper() for f in _parse_csv_param(familias)]
    cod_list = [c.upper() for c in _parse_csv_param(codigos)]
    cod_prefix = [c[:-1] for c in cod_list if c.endswith("*")]
    cod_contains = [c for c in cod_list if not c.endswith("*")]

    data = db.get_matriz_distribucion(
        dias_proyeccion=dias,
        familias=None,
        alertas=alertas_list if alertas_list else None,
        sucursales=suc_list if suc_list else None,
        prefijos_familia=fam_list if fam_list else None,
        codigos_prefix=cod_prefix if cod_prefix else None,
        codigos_contains=cod_contains if cod_contains else None,
    )
    if not data:
        return {"columns": [], "rows": [], "source_rows": 0}

    df = pd.DataFrame(data)
    if "cod_base" in df.columns and "cod_articulo" in df.columns:
        df["cod_base"] = df["cod_base"].fillna(df["cod_articulo"])

    if df.empty:
        return {"columns": [], "rows": [], "source_rows": 0}

    piv = df.pivot_table(
        index=["cod_base", "cod_articulo"],
        columns="sucursal",
        values="necesidad",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    cdd = df.groupby(["cod_base", "cod_articulo"], as_index=False)["stock_cdd"].sum()
    cdd = cdd.rename(columns={"stock_cdd": "Stock CDD"})
    piv = piv.merge(cdd, on=["cod_base", "cod_articulo"], how="left")
    piv["Stock CDD"] = pd.to_numeric(piv["Stock CDD"], errors="coerce").fillna(0.0)

    orden = [
        "CRISA 2", "CRISA CENTRAL", "LA TIJERA LUJAN", "LA TIJERA MAIPU",
        "LA TIJERA MENDOZA", "LA TIJERA SAN JUAN", "LA TIJERA SAN LUIS",
        "LA TIJERA SAN RAFAEL", "LA TIJERA SMARTIN", "LA TIJERA TUNUYAN",
    ]
    base_cols = ["cod_base", "cod_articulo", "Stock CDD"]
    suc_cols = [c for c in orden if c in piv.columns]
    other_cols = [c for c in piv.columns if c not in base_cols + suc_cols]
    piv = piv[base_cols + suc_cols + other_cols]
    piv["Total"] = piv[suc_cols].sum(axis=1) if suc_cols else 0.0

    ren = {
        "LA TIJERA LUJAN": "LUJAN",
        "LA TIJERA MAIPU": "MAIPU",
        "LA TIJERA MENDOZA": "MENDOZA",
        "LA TIJERA SAN JUAN": "SAN JUAN",
        "LA TIJERA SAN LUIS": "SAN LUIS",
        "LA TIJERA SAN RAFAEL": "SAN RAFAEL",
        "LA TIJERA SMARTIN": "SMARTIN",
        "LA TIJERA TUNUYAN": "TUNUYAN",
        "cod_base": "Cód. base / artículo",
        "cod_articulo": "Cód. Artículo",
    }
    piv = piv.rename(columns=ren)

    # Orden default: CRISA CENTRAL desc (si existe), si no Total, sino primera numérica
    sort_col = "CRISA CENTRAL" if "CRISA CENTRAL" in piv.columns else ("Total" if "Total" in piv.columns else None)
    if not sort_col:
        skip_cols = {"Cód. base / artículo", "Cód. Artículo", "Cód. base / artículo", "Cód. Artículo"}
        for c in piv.columns:
            if c not in skip_cols:
                sort_col = c
                break
    if sort_col:
        piv = piv.sort_values(by=sort_col, ascending=False)

    # Limitar filas para evitar payloads enormes
    safe_limit = max(0, min(int(limit) if str(limit).isdigit() else 0, 5000))
    if safe_limit:
        piv = piv.head(safe_limit)

    columns = list(piv.columns)
    rows = piv.to_dict(orient="records")
    return {"columns": columns, "rows": rows, "source_rows": len(df)}

@app.get("/sugerencia-distribucion")
async def get_sugerencia_distribucion(
    dias: int = 30,
    sucursal: Optional[str] = None,
    limit: int = 200,
    alertas: Optional[str] = None,
    sucursales: Optional[str] = None,
    familias: Optional[str] = None,
    codigos: Optional[str] = None,
    solo_sugeridos: Optional[bool] = True,
    lista_precio: Optional[str] = None,
):
    alertas_list = _expand_alertas(_parse_csv_param(alertas))
    suc_list = _parse_csv_param(sucursales)
    fam_list = [f.upper() for f in _parse_csv_param(familias)]
    cod_list = [c.upper() for c in _parse_csv_param(codigos)]
    cod_prefix = [c[:-1] for c in cod_list if c.endswith("*")]
    cod_contains = [c for c in cod_list if not c.endswith("*")]

    sucursales_incluir = set(suc_list)
    if suc_list:
        for suc in suc_list:
            for excluida, principal in db.SUCURSALES_UNIFICAR.items():
                if suc == principal:
                    sucursales_incluir.add(excluida)

    data = db.get_sugerencia_distribucion(
        dias_proyeccion=dias,
        familias=None,
        limit=limit,
        sucursales=list(sucursales_incluir) if sucursales_incluir else None,
        prefijos_familia=fam_list if fam_list else None,
        codigos_prefix=cod_prefix if cod_prefix else None,
        codigos_contains=cod_contains if cod_contains else None,
        alertas=alertas_list if alertas_list else None,
        solo_sugeridos=solo_sugeridos,
        lista_precio=lista_precio,
    )
    if not data:
        return {"rows": [], "total": 0}
    df = pd.DataFrame(data)
    if sucursal:
        df = df[df["sucursal"] == sucursal]
    return {"rows": df.to_dict(orient="records"), "total": len(df)}

@app.get("/kpi-evolucion")
async def get_kpi_evolucion(
    sucursal: Optional[str] = None,
    sucursales: Optional[str] = None,
    familias: Optional[str] = None,
    codigos: Optional[str] = None,
):
    conn = None
    try:
        conn = db.get_connection()
        suc_list = _parse_csv_param(sucursales)
        if sucursal and sucursal != "Todas" and sucursal not in suc_list:
            suc_list.append(sucursal)
        fam_list = [f.upper() for f in _parse_csv_param(familias)]
        cod_list = [c.upper() for c in _parse_csv_param(codigos)]
        cod_prefix = [c[:-1] for c in cod_list if c.endswith("*")]
        cod_contains = [c for c in cod_list if not c.endswith("*")]

        params: List[str] = []
        where_parts: List[str] = []
        if suc_list:
            placeholders = ",".join(["%s"] * len(suc_list))
            where_parts.append(f"v.sucursal IN ({placeholders})")
            params.extend(suc_list)
        if fam_list:
            placeholders = ",".join(["%s"] * len(fam_list))
            where_parts.append(f"LEFT(UPPER(v.cod_articulo), 2) IN ({placeholders})")
            params.extend(fam_list)
        cod_filters: List[str] = []
        for p in cod_prefix:
            cod_filters.append("(UPPER(v.cod_articulo) LIKE %s OR UPPER(v.cod_base) LIKE %s)")
            params.extend([f"{p}%", f"{p}%"])
        for c in cod_contains:
            cod_filters.append("(UPPER(v.cod_articulo) LIKE %s OR UPPER(v.cod_base) LIKE %s)")
            params.extend([f"%{c}%", f"%{c}%"])
        if cod_filters:
            where_parts.append("(" + " OR ".join(cod_filters) + ")")

        where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

        q_ventas = f"""
            SELECT
                EXTRACT(YEAR FROM fecha)::int AS anio,
                EXTRACT(MONTH FROM fecha)::int AS mes_num,
                COALESCE(SUM(cantidad_venta), 0) AS ventas_unidades,
                COALESCE(SUM(importe), 0) AS ventas_importe
            FROM ventas v
            {where_sql}
            GROUP BY 1,2
            ORDER BY 1,2
        """
        df_v = pd.read_sql(q_ventas, conn, params=params)

        hist_where_parts: List[str] = []
        hist_params: List[str] = []
        if suc_list:
            placeholders = ",".join(["%s"] * len(suc_list))
            hist_where_parts.append(f"h.sucursal IN ({placeholders})")
            hist_params.extend(suc_list)
        if fam_list:
            placeholders = ",".join(["%s"] * len(fam_list))
            hist_where_parts.append(f"LEFT(UPPER(h.cod_articulo), 2) IN ({placeholders})")
            hist_params.extend(fam_list)
        hist_cod_filters: List[str] = []
        for p in cod_prefix:
            hist_cod_filters.append("(UPPER(h.cod_articulo) LIKE %s OR UPPER(h.cod_base) LIKE %s)")
            hist_params.extend([f"{p}%", f"{p}%"])
        for c in cod_contains:
            hist_cod_filters.append("(UPPER(h.cod_articulo) LIKE %s OR UPPER(h.cod_base) LIKE %s)")
            hist_params.extend([f"%{c}%", f"%{c}%"])
        if hist_cod_filters:
            hist_where_parts.append("(" + " OR ".join(hist_cod_filters) + ")")

        filtro_hist = ""
        filtro_hist_join = ""
        if hist_where_parts:
            filtro_hist = "AND " + " AND ".join(hist_where_parts)
            filtro_hist_join = "WHERE " + " AND ".join(hist_where_parts)

        q_stock_hist = f"""
            WITH ult_mes AS (
                SELECT
                    DATE_TRUNC('month', snapshot_ts) AS mes,
                    MAX(snapshot_ts) AS ts_ult
                FROM saldo_historial h
                WHERE snapshot_ts IS NOT NULL
                {filtro_hist}
                GROUP BY 1
            )
            SELECT
                EXTRACT(YEAR FROM u.mes)::int AS anio,
                EXTRACT(MONTH FROM u.mes)::int AS mes_num,
                COALESCE(SUM(h.stock_1), 0) AS stock_total
            FROM ult_mes u
            JOIN saldo_historial h
              ON h.snapshot_ts = u.ts_ult
            {filtro_hist_join}
            GROUP BY 1,2
            ORDER BY 1,2
        """
        try:
            df_s = pd.read_sql(q_stock_hist, conn, params=hist_params + hist_params)
        except Exception:
            fallback_parts: List[str] = []
            fallback_params: List[str] = []
            if suc_list:
                placeholders = ",".join(["%s"] * len(suc_list))
                fallback_parts.append(f"sucursal IN ({placeholders})")
                fallback_params.extend(suc_list)
            if fam_list:
                placeholders = ",".join(["%s"] * len(fam_list))
                fallback_parts.append(f"LEFT(UPPER(cod_articulo), 2) IN ({placeholders})")
                fallback_params.extend(fam_list)
            fallback_cod_filters: List[str] = []
            for p in cod_prefix:
                fallback_cod_filters.append("(UPPER(cod_articulo) LIKE %s OR UPPER(cod_base) LIKE %s)")
                fallback_params.extend([f"{p}%", f"{p}%"])
            for c in cod_contains:
                fallback_cod_filters.append("(UPPER(cod_articulo) LIKE %s OR UPPER(cod_base) LIKE %s)")
                fallback_params.extend([f"%{c}%", f"%{c}%"])
            if fallback_cod_filters:
                fallback_parts.append("(" + " OR ".join(fallback_cod_filters) + ")")
            fallback_filter = f"AND {' AND '.join(fallback_parts)}" if fallback_parts else ""
            q_stock_fallback = f"""
                SELECT
                    EXTRACT(YEAR FROM sync_timestamp)::int AS anio,
                    EXTRACT(MONTH FROM sync_timestamp)::int AS mes_num,
                    COALESCE(SUM(stock_1), 0) AS stock_total
                FROM saldo
                WHERE sync_timestamp IS NOT NULL
                {fallback_filter}
                GROUP BY 1,2
                ORDER BY 1,2
            """
            df_s = pd.read_sql(q_stock_fallback, conn, params=fallback_params)

        base = pd.DataFrame(columns=["anio", "mes_num"])
        if not df_v.empty:
            base = pd.concat([base, df_v[["anio", "mes_num"]]], ignore_index=True)
        if not df_s.empty:
            base = pd.concat([base, df_s[["anio", "mes_num"]]], ignore_index=True)
        if base.empty:
            return {"rows": []}

        base = base.drop_duplicates()
        df_kpi = base.merge(df_v, on=["anio", "mes_num"], how="left")
        df_kpi = df_kpi.merge(df_s, on=["anio", "mes_num"], how="left")
        for col in ["ventas_unidades", "ventas_importe"]:
            if col not in df_kpi.columns:
                df_kpi[col] = 0
            df_kpi[col] = pd.to_numeric(df_kpi[col], errors="coerce").fillna(0.0)

        if "stock_total" not in df_kpi.columns:
            df_kpi["stock_total"] = None
        else:
            df_kpi["stock_total"] = pd.to_numeric(df_kpi["stock_total"], errors="coerce")

        df_kpi = df_kpi.sort_values(["anio", "mes_num"])
        df_kpi = df_kpi.where(pd.notnull(df_kpi), None)

        stock_hist = {"meses": 0}
        if not df_s.empty:
            df_s = df_s.sort_values(["anio", "mes_num"])
            first = df_s.iloc[0]
            last = df_s.iloc[-1]
            stock_hist = {
                "meses": int(df_s[["anio", "mes_num"]].drop_duplicates().shape[0]),
                "desde": f"{int(first.mes_num):02d}-{int(first.anio)}",
                "hasta": f"{int(last.mes_num):02d}-{int(last.anio)}",
            }

        def _json_safe(value):
            if value is None:
                return None
            if isinstance(value, (str, int, float, bool)):
                if isinstance(value, float):
                    if not math.isfinite(value):
                        return None
                return value
            if isinstance(value, Decimal):
                return float(value)
            if hasattr(value, "item"):
                try:
                    v = value.item()
                    if isinstance(v, float) and not math.isfinite(v):
                        return None
                    return v
                except Exception:
                    pass
            return value

        rows = [
            {k: _json_safe(v) for k, v in row.items()}
            for row in df_kpi.to_dict(orient="records")
        ]
        stock_hist = {k: _json_safe(v) for k, v in stock_hist.items()}
        return {"rows": rows, "stock_hist": stock_hist}
    except Exception as e:
        logger.exception("Error en kpi-evolucion")
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

@app.get("/kpi-alertas-criticas")
async def get_kpi_alertas_criticas(
    dias: int = 30,
    sucursales: Optional[str] = None,
    familias: Optional[str] = None,
    codigos: Optional[str] = None,
):
    suc_list = _parse_csv_param(sucursales)
    fam_list = [f.upper() for f in _parse_csv_param(familias)]
    cod_list = [c.upper() for c in _parse_csv_param(codigos)]
    cod_prefix = [c[:-1] for c in cod_list if c.endswith("*")]
    cod_contains = [c for c in cod_list if not c.endswith("*")]

    data = db.get_kpi_alertas_criticas(
        dias_proyeccion=dias,
        sucursales=suc_list if suc_list else None,
        prefijos_familia=fam_list if fam_list else None,
        codigos_prefix=cod_prefix if cod_prefix else None,
        codigos_contains=cod_contains if cod_contains else None,
    )
    total_unidades = sum([r.get("unidades_sugeridas") or 0 for r in data])
    total_monto = sum([r.get("monto_reponer_costo") or 0 for r in data])
    return {
        "rows": data,
        "total_unidades": total_unidades,
        "total_monto": total_monto,
    }

@app.get("/kpi-familias-reponer")
async def get_kpi_familias_reponer(
    dias: int = 30,
    sucursales: Optional[str] = None,
    familias: Optional[str] = None,
    codigos: Optional[str] = None,
):
    suc_list = _parse_csv_param(sucursales)
    fam_list = [f.upper() for f in _parse_csv_param(familias)]
    cod_list = [c.upper() for c in _parse_csv_param(codigos)]
    cod_prefix = [c[:-1] for c in cod_list if c.endswith("*")]
    cod_contains = [c for c in cod_list if not c.endswith("*")]

    data = db.get_kpi_familias_reponer(
        dias_proyeccion=dias,
        sucursales=suc_list if suc_list else None,
        prefijos_familia=fam_list if fam_list else None,
        codigos_prefix=cod_prefix if cod_prefix else None,
        codigos_contains=cod_contains if cod_contains else None,
    )
    return {"rows": data}

# ============== ENDPOINTS DE COSTOS ==============

@app.get("/costos")
async def get_costos():
    """Obtener todos los costos de reposiciÃ³n."""
    costos = db.get_all_costos()
    return {"costos": costos, "total": len(costos)}

@app.get("/costos/{cod_articulo}")
async def get_costo_articulo(cod_articulo: str):
    """Obtener costo de un artÃ­culo especÃ­fico."""
    costo = db.get_costo_articulo(cod_articulo)
    if costo:
        return costo
    return {"error": "ArtÃ­culo no encontrado", "cod_articulo": cod_articulo}

@app.post("/costos")
async def upload_costos(request: Request):
    """Subir o actualizar costos de reposiciÃ³n."""
    try:
        data = await request.json()
        costos_list = data.get("costos", [])
        
        if not costos_list:
            return {"error": "No se recibieron costos", "registros": 0}
        
        count = db.upsert_costos(costos_list)
        return {
            "status": "ok",
            "message": f"Se procesaron {count} costos",
            "registros": count
        }
    except Exception as e:
        return {"error": str(e), "registros": 0}

@app.delete("/costos")
async def delete_costos():
    """Eliminar todos los costos."""
    try:
        db.delete_all_costos()
        return {"status": "ok", "message": "Costos eliminados"}
    except Exception as e:
        return {"error": str(e)}

@app.get("/metricas-costos")
async def get_metricas_con_costos(sucursal: Optional[str] = None, alerta: Optional[str] = None, familia: Optional[str] = None):
    """Obtener mÃ©tricas con costos de reposiciÃ³n integrados."""
    data = db.get_metricas_con_costos(sucursal, alerta, familia)
    return {"data": data, "total": len(data)}

@app.get("/resumen-costos")
async def get_resumen_costos():
    """Obtener resumen de valores de stock y reposiciÃ³n por sucursal."""
    resumen = db.get_resumen_costos_por_sucursal()
    return {"resumen": resumen}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.get("/sync-info")
async def get_sync_info():
    """Obtener info para sincronizaciÃ³n incremental"""
    info = db.get_sync_info()
    # Convertir fechas a string para JSON
    if info.get("ultima_fecha_ventas"):
        info["ultima_fecha_ventas"] = str(info["ultima_fecha_ventas"])
    if info.get("ultima_sync_saldo"):
        info["ultima_sync_saldo"] = str(info["ultima_sync_saldo"])
    if info.get("ultima_sync_saldo_historial"):
        info["ultima_sync_saldo_historial"] = str(info["ultima_sync_saldo_historial"])
    if info.get("ultima_sync_precios"):
        info["ultima_sync_precios"] = str(info["ultima_sync_precios"])
    if info.get("ultima_sync_costos"):
        info["ultima_sync_costos"] = str(info["ultima_sync_costos"])
    if info.get("ultima_sync_articulos"):
        info["ultima_sync_articulos"] = str(info["ultima_sync_articulos"])
    return info

@app.get("/quality")
async def get_quality():
    """DiagnÃ³stico de cobertura de datos para monitoreo UI/servicio."""
    return db.get_data_quality_summary()

@app.post("/recalcular-metricas")
async def recalcular_metricas():
    """Recalcular mÃ©tricas desde los datos existentes"""
    try:
        timestamp = now_ar()
        
        # Obtener datos actuales
        saldos = db.get_all_saldos()
        ventas = db.get_all_ventas()
        
        if not saldos:
            return {"status": "error", "message": "No hay saldos para calcular mÃ©tricas"}
        
        df_saldo = pd.DataFrame(saldos)
        df_ventas = pd.DataFrame(ventas) if ventas else pd.DataFrame()
        
        # Calcular mÃ©tricas
        df_resultado = calcular_metricas(df_saldo, df_ventas)
        
        # Guardar mÃ©tricas
        metricas_records = df_resultado.to_dict(orient="records")
        db.clear_metricas()
        db.insert_metricas(metricas_records, timestamp)
        
        # Registrar sync
        db.log_sync(
            registros_saldo=len(saldos),
            registros_ventas=len(ventas) if ventas else 0,
            registros_metricas=len(metricas_records),
            status="ok",
            message="MÃ©tricas recalculadas"
        )
        
        return {
            "status": "ok",
            "message": "MÃ©tricas recalculadas",
            "registros": len(metricas_records),
            "timestamp": timestamp.isoformat()
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

class WhatsAppRequest(BaseModel):
    numero_destino: str
    tipo_mensaje: str = "resumen"
    sucursal: Optional[str] = None

@app.post("/whatsapp/enviar")
async def enviar_whatsapp_alerta(request: WhatsAppRequest):
    """
    Enviar alerta de stock por WhatsApp
    tipo_mensaje: 'resumen' | 'comercial' | 'sucursal'
    """
    import whatsapp_alerts as wa
    
    try:
        datos = wa.obtener_datos_desde_db()
        
        if request.tipo_mensaje == "resumen":
            resultado = wa.enviar_resumen_general(request.numero_destino, datos)
        elif request.tipo_mensaje == "comercial":
            resultado = wa.enviar_resumen_comercial(request.numero_destino, datos)
        elif request.tipo_mensaje == "sucursal":
            if not request.sucursal:
                return {"success": False, "error": "Se requiere nombre de sucursal"}
            resultado = wa.enviar_alerta_sucursal(request.numero_destino, request.sucursal, datos)
        else:
            return {"success": False, "error": "Tipo de mensaje no vÃ¡lido"}
        
        return resultado
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/whatsapp/preview/{tipo}")
async def preview_mensaje_whatsapp(tipo: str, sucursal: Optional[str] = None):
    """
    Ver preview del mensaje sin enviar
    tipo: 'resumen' | 'comercial' | 'sucursal'
    """
    import whatsapp_alerts as wa
    
    try:
        datos = wa.obtener_datos_desde_db()
        
        if tipo == "resumen":
            mensaje = wa.generar_mensaje_resumen_general(datos)
        elif tipo == "comercial":
            mensaje = wa.generar_mensaje_comercial(datos)
        elif tipo == "sucursal":
            if not sucursal:
                return {"success": False, "error": "Se requiere parÃ¡metro sucursal"}
            suc_data = next((s for s in datos if s["sucursal"].upper() == sucursal.upper()), None)
            if not suc_data:
                return {"success": False, "error": f"Sucursal {sucursal} no encontrada"}
            mensaje = wa.generar_mensaje_alerta_sucursal(suc_data)
        else:
            return {"success": False, "error": "Tipo no vÃ¡lido. Use: resumen, comercial, sucursal"}
        
        return {"success": True, "mensaje": mensaje, "tipo": tipo}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/whatsapp/alertas-rojas")
async def enviar_alertas_sucursales_rojas(numeros: dict):
    """
    Enviar alertas a todas las sucursales en rojo
    Body: {"MENDOZA": "+549261XXX", "SAN LUIS": "+549261XXX"}
    """
    import whatsapp_alerts as wa
    
    try:
        datos = wa.obtener_datos_desde_db()
        resultados = wa.enviar_alertas_sucursales_rojas(numeros, datos)
        return {"success": True, "resultados": resultados}
    except Exception as e:
        return {"success": False, "error": str(e)}

# ==================== EMAIL ALERTS ====================

class EmailRequest(BaseModel):
    email_destino: str
    tipo_mensaje: str  # "resumen", "sucursal", "comercial"
    sucursal: Optional[str] = None
    dias: Optional[int] = 30

@app.post("/email/enviar")
async def enviar_email_alerta(request: EmailRequest):
    """
    Enviar alerta por email
    Tipos: resumen, sucursal, comercial
    """
    import email_alerts as ea
    
    try:
        dias = request.dias or 30
        if request.tipo_mensaje == "resumen":
            resultado = ea.enviar_resumen_general(request.email_destino, dias)
        elif request.tipo_mensaje == "sucursal":
            if not request.sucursal:
                return {"success": False, "error": "Debe especificar sucursal"}
            resultado = ea.enviar_alerta_sucursal(request.email_destino, request.sucursal, dias)
        elif request.tipo_mensaje == "comercial":
            resultado = ea.enviar_resumen_comercial(request.email_destino, dias)
        else:
            return {"success": False, "error": "Tipo de mensaje no vÃ¡lido. Use: resumen, sucursal, comercial"}
        
        return resultado
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/email/preview/{tipo}")
async def preview_email(tipo: str, sucursal: Optional[str] = None, dias: int = 30):
    """
    Ver preview del email en HTML
    """
    import email_alerts as ea
    
    try:
        if tipo == "resumen":
            html = ea.generar_html_resumen_general(dias)
        elif tipo == "sucursal":
            if not sucursal:
                return {"error": "Debe especificar sucursal"}
            html = ea.generar_html_alerta_sucursal(sucursal, dias)
        elif tipo == "comercial":
            html = ea.generar_html_resumen_comercial(dias)
        else:
            return {"error": "Tipo no vÃ¡lido"}
        
        return HTMLResponse(content=html)
    except Exception as e:
        return {"error": str(e)}

@app.post("/email/alertas-rojas")
async def enviar_emails_sucursales_rojas(emails: dict, dias: int = 30):
    """
    Enviar alertas por email a todas las sucursales en rojo
    Body: {"MENDOZA": "email@empresa.com", "SAN LUIS": "otro@empresa.com"}
    """
    import email_alerts as ea
    
    try:
        alertas = ea.get_datos_alertas(dias)
        resultados = []
        
        for alerta in alertas:
            if alerta['valor'] >= 50000000:  # Solo rojas
                sucursal = alerta['sucursal']
                if sucursal in emails:
                    resultado = ea.enviar_alerta_sucursal(emails[sucursal], sucursal, dias)
                    resultados.append({
                        "sucursal": sucursal,
                        "email": emails[sucursal],
                        "resultado": resultado
                    })
        
        return {"success": True, "enviados": len(resultados), "resultados": resultados}
    except Exception as e:
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)


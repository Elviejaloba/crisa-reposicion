import os
import sys
import subprocess
import asyncio
import logging
import httpx
import aiohttp

from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel
from typing import List, Optional
import pandas as pd
from datetime import datetime
import database as db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("proxy")

STREAMLIT_PORT = 8002
streamlit_process = None
streamlit_log = []
streamlit_ready = False

app = FastAPI(title="Sistema de Análisis Comercial")

@app.on_event("startup")
async def start_streamlit():
    global streamlit_process, streamlit_ready
    streamlit_process = subprocess.Popen(
        [
            sys.executable, "-m", "streamlit", "run", "app.py",
            "--server.port", str(STREAMLIT_PORT),
            "--server.address", "127.0.0.1",
            "--server.headless", "true",
            "--server.enableCORS", "false",
            "--server.enableXsrfProtection", "false",
            "--server.enableWebsocketCompression", "false",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    async def read_streamlit_output():
        import io
        loop = asyncio.get_event_loop()
        while streamlit_process and streamlit_process.poll() is None:
            line = await loop.run_in_executor(None, streamlit_process.stdout.readline)
            if line:
                decoded = line.decode("utf-8", errors="replace").rstrip()
                streamlit_log.append(decoded)
                if len(streamlit_log) > 200:
                    streamlit_log.pop(0)
                logger.info(f"[streamlit] {decoded}")

    asyncio.create_task(read_streamlit_output())

    for i in range(60):
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(f"http://127.0.0.1:{STREAMLIT_PORT}/_stcore/health")
                if resp.status_code == 200:
                    streamlit_ready = True
                    logger.info(f"Streamlit ready after {i+1}s")
                    break
        except Exception:
            pass
        await asyncio.sleep(1)

@app.on_event("shutdown")
async def stop_streamlit():
    global streamlit_process
    if streamlit_process:
        streamlit_process.terminate()

@app.get("/debug")
async def debug_info():
    import socket
    port_check = False
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        s.connect(("127.0.0.1", STREAMLIT_PORT))
        s.close()
        port_check = True
    except Exception:
        pass

    ws_check = "unknown"
    try:
        async with aiohttp.ClientSession() as session:
            ws = await session.ws_connect(
                f"http://127.0.0.1:{STREAMLIT_PORT}/_stcore/stream",
                timeout=5,
            )
            ws_check = "connected"
            await ws.close()
    except Exception as e:
        ws_check = f"failed: {e}"

    health_check = "unknown"
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(f"http://127.0.0.1:{STREAMLIT_PORT}/_stcore/health")
            health_check = f"{r.status_code}: {r.text}"
    except Exception as e:
        health_check = f"failed: {e}"

    return {
        "streamlit_pid": streamlit_process.pid if streamlit_process else None,
        "streamlit_running": streamlit_process.poll() is None if streamlit_process else False,
        "streamlit_returncode": streamlit_process.poll() if streamlit_process else None,
        "streamlit_ready": streamlit_ready,
        "port_open": port_check,
        "health_check": health_check,
        "ws_check": ws_check,
        "last_logs": streamlit_log[-30:],
    }

@app.get("/wstest")
async def wstest():
    html = """<!DOCTYPE html><html><head><title>WS Test</title></head><body style="background:#111;color:#0f0;font-family:monospace;padding:20px;">
<h2>WebSocket Diagnostic</h2><div id="log"></div>
<script>
function log(msg){document.getElementById('log').innerHTML+='<p>'+new Date().toISOString()+' - '+msg+'</p>';}
log('Page loaded, testing WebSocket...');
var proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
var url = proto + '//' + location.host + '/wsecho';
log('Connecting to: ' + url);
var ws = new WebSocket(url);
ws.onopen = function(){log('<b style="color:lime">CONNECTED!</b>'); ws.send('hello');};
ws.onmessage = function(e){log('Message received: ' + e.data);};
ws.onclose = function(e){log('<b style="color:red">CLOSED</b> code=' + e.code + ' reason=' + e.reason + ' wasClean=' + e.wasClean);};
ws.onerror = function(e){log('<b style="color:red">ERROR</b>');};
setTimeout(function(){
    log('Testing Streamlit WS path...');
    var url2 = proto + '//' + location.host + '/_stcore/stream';
    log('Connecting to: ' + url2);
    var ws2 = new WebSocket(url2);
    ws2.onopen = function(){log('<b style="color:lime">STREAMLIT WS CONNECTED!</b>');};
    ws2.onclose = function(e){log('<b style="color:red">STREAMLIT WS CLOSED</b> code=' + e.code + ' reason=' + e.reason);};
    ws2.onerror = function(e){log('<b style="color:red">STREAMLIT WS ERROR</b>');};
}, 2000);
</script></body></html>"""
    return HTMLResponse(content=html)

@app.websocket("/wsecho")
async def wsecho(ws: WebSocket):
    await ws.accept()
    logger.info("WSECHO: client connected")
    try:
        while True:
            data = await ws.receive_text()
            logger.info(f"WSECHO: received {data}")
            await ws.send_text(f"echo: {data}")
    except Exception as e:
        logger.info(f"WSECHO: closed {e}")

API_PATHS = {"/sync", "/health", "/data", "/sucursales", "/alertas", 
             "/totales", "/precios", "/listas-precios", "/costos",
             "/metricas-costos", "/resumen-costos",
             "/whatsapp/enviar", "/whatsapp/alertas-rojas"}

class SyncData(BaseModel):
    saldo: Optional[List[dict]] = None
    ventas: Optional[List[dict]] = None
    precios: Optional[List[dict]] = None
    costos: Optional[List[dict]] = None
    append: Optional[bool] = False
    reset: Optional[bool] = False
    calculate_metrics: Optional[bool] = False
    incremental: Optional[bool] = True  # Usar UPSERT por defecto

def normalize_saldo_columns(records: List[dict]) -> List[dict]:
    normalized = []
    for r in records:
        stock_val = r.get("Stock 1", r.get("stock_1", 0))
        # Buscar cod_articulo en múltiples formatos posibles
        cod_art = r.get("Cod. Articulo", r.get("Cód. Artículo", r.get("Cod. base / articulo", 
                  r.get("Cód. base / artículo", r.get("cod_articulo", "")))))
        cod_dep = r.get("Cod. Deposito", r.get("Cód. Depósito", r.get("cod_deposito", "")))
        normalized.append({
            "cod_articulo": str(cod_art) if cod_art else "",
            "descripcion": str(r.get("Articulo", r.get("Artículo", r.get("Desc. Base / Articulo", 
                           r.get("Desc. Base / Artículo", r.get("descripcion", "")))))),
            "sucursal": str(r.get("Sucursal", r.get("sucursal", ""))),
            "nro_sucursal": r.get("Nro. Sucursal", r.get("nro_sucursal", 0)),
            "deposito": str(r.get("Deposito", r.get("Depósito", r.get("deposito", "")))),
            "cod_deposito": str(cod_dep) if cod_dep else "",
            "familia": str(r.get("Cod. escala 1", r.get("Cód. escala 1", r.get("familia", "")))),
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
        # Buscar cod_articulo en múltiples formatos
        cod_art = r.get("Cod. Articulo", r.get("Cód. Artículo", r.get("cod_articulo", "")))
        normalized.append({
            "cod_articulo": str(cod_art) if cod_art else "",
            "descripcion": str(r.get("Descripcion", r.get("Descripción", r.get("descripcion", "")))),
            "sucursal": str(r.get("Desc. sucursal", r.get("sucursal", ""))),
            "nro_sucursal": r.get("Nro. Sucursal", r.get("nro_sucursal", 0)),
            "fecha": str(r.get("Fecha", r.get("fecha", ""))),
            "cantidad_venta": float(cant_val) if cant_val is not None else 0.0,
            "importe": float(imp_val) if imp_val is not None else 0.0,
            "familia": str(r.get("Cod. Familia (Articulo)", r.get("Cód. Familia (Artículo)", r.get("familia", "")))),
            "desc_familia": str(r.get("Descripcion Familia (Articulo)", r.get("Descripción Familia (Artículo)", r.get("desc_familia", "")))),
            "um_stock": str(r.get("U.M. stock", r.get("um_stock", "")))
        })
    return normalized

def normalize_precios_columns(records: List[dict]) -> List[dict]:
    normalized = []
    for r in records:
        precio_val = r.get("Precio", r.get("precio", 0))
        # Buscar cod_articulo en múltiples formatos
        cod_art = r.get("Cod. Articulo", r.get("Cód. Artículo", r.get("cod_articulo", "")))
        normalized.append({
            "cod_articulo": str(cod_art) if cod_art else "",
            "descripcion": str(r.get("Descripcion", r.get("Descripción", r.get("descripcion", "")))),
            "sinonimo": str(r.get("Sinonimo", r.get("Sinónimo", r.get("sinonimo", "")))),
            "cod_familia": str(r.get("Cod. familia", r.get("Cód. familia", r.get("cod_familia", "")))),
            "familia": str(r.get("Familia", r.get("familia", ""))),
            "precio": float(precio_val) if precio_val is not None else 0.0,
            "nro_lista": str(r.get("Cod. Lista de Precios", r.get("Cód. Lista de Precios", r.get("nro_lista", "")))),
            "nombre_lista": str(r.get("Lista de precios", r.get("nombre_lista", ""))),
            "fecha_modificacion": str(r.get("Fecha de ultima modificacion", r.get("Fecha de última modificación", r.get("fecha_modificacion", ""))))
        })
    return normalized

def normalize_costos_columns(records: List[dict]) -> List[dict]:
    normalized = []
    for r in records:
        costo_val = r.get("Costo", r.get("costo", r.get("costo_reposicion", 0)))
        cod_art = r.get("Cod. Articulo", r.get("Cód. Artículo", r.get("cod_articulo", "")))
        normalized.append({
            "cod_articulo": str(cod_art) if cod_art else "",
            "descripcion": str(r.get("Descripcion", r.get("Descripción", r.get("descripcion", "")))),
            "costo_reposicion": float(costo_val) if costo_val is not None else 0.0,
            "moneda": "ARS"
        })
    return normalized

def calcular_metricas(df_saldo: pd.DataFrame, df_ventas: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula métricas de stock usando lógica de Power BI:
    - Prox 3 meses (AA): Ventas del mismo período de 3 meses del año anterior
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
            
            dias_periodo = 120
            
            fin_actual = hoy
            inicio_actual = fin_actual - pd.Timedelta(days=dias_periodo - 1)
            
            fin_aa_analisis = hoy - relativedelta(years=1)
            inicio_aa_analisis = fin_aa_analisis - pd.Timedelta(days=dias_periodo - 1)
            
            inicio_aa = hoy - relativedelta(months=12)
            fin_aa = hoy - relativedelta(months=9) - pd.Timedelta(days=1)
            
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
            
            df_resultado["coef_aa"] = df_resultado["vta_aa"] / 3
            df_resultado["coef_actual"] = df_resultado["vta_actual"] / 3
            df_resultado["coef_mensual"] = df_resultado[["coef_aa", "coef_actual"]].max(axis=1)
            
            df_resultado["venta_mensual_proyectada"] = df_resultado["coef_mensual"]
            df_resultado["venta_promedio_diaria"] = df_resultado["coef_mensual"] / 30
            
            def calcular_meses_stock(row):
                stock = row["stock_1"]
                coef = row["coef_mensual"]
                venta_base = max(row["vta_aa"], row["vta_actual"])
                
                if stock <= 0 or coef <= 0 or venta_base <= 0:
                    return 0.0
                return stock / coef
            
            df_resultado["meses_stock"] = df_resultado.apply(calcular_meses_stock, axis=1)
            
            df_resultado = df_resultado.drop(columns=["coef_aa", "coef_actual", "coef_mensual", "proyeccion"], errors="ignore")
    
    def determinar_alerta(row):
        meses_stock = row.get("meses_stock", 0)
        dias_stock = meses_stock * 30
        ventas_actual = row.get("venta_actual", row.get("venta_mensual_proyectada", 0))
        
        if meses_stock is None or pd.isna(meses_stock):
            return ""
        
        if meses_stock == 0 and ventas_actual == 0:
            return "Sin rotación (sin stock)"
        
        if dias_stock > 0 and dias_stock < 15 and ventas_actual >= 1:
            return "Quiebre de stock"
        
        if dias_stock >= 15 and dias_stock < 30:
            return "Stock de Seguridad"
        
        if dias_stock >= 30 and dias_stock < 60:
            return "Pto de Pedido"
        
        if dias_stock >= 60 and dias_stock < 90:
            return "OK"
        
        if dias_stock >= 90 and ventas_actual == 0:
            return "Sin rotación (con sobrestock)"
        
        if dias_stock >= 90:
            return "Sobre stock"
        
        return ""
    
    df_resultado["alerta_stock"] = df_resultado.apply(determinar_alerta, axis=1)
    
    df_resultado = df_resultado.drop(columns=["venta_actual"], errors="ignore")
    
    return df_resultado

@app.post("/sync")
async def sync_data(data: SyncData):
    try:
        timestamp = datetime.now()
        
        # Comando para limpiar tablas
        if data.reset:
            db.clear_tables()
            return {"status": "ok", "message": "Tablas limpiadas"}
        
        # Comando para calcular métricas
        if data.calculate_metrics:
            saldo_records = db.get_all_saldo()
            ventas_records = db.get_all_ventas()
            
            df_saldo = pd.DataFrame(saldo_records)
            df_ventas = pd.DataFrame(ventas_records)
            
            df_resultado = calcular_metricas(df_saldo, df_ventas)
            metricas_records = df_resultado.to_dict(orient="records") if not df_resultado.empty else []
            
            db.clear_metricas()
            db.insert_metricas(metricas_records, timestamp)
            db.log_sync(len(saldo_records), len(ventas_records), len(metricas_records), "ok", "Métricas calculadas")
            
            return {
                "status": "ok",
                "message": f"Métricas calculadas: {len(metricas_records)} registros",
                "registros": len(metricas_records)
            }
        
        # Insertar datos en lotes (incremental usa UPSERT)
        inserted = {"saldo": 0, "ventas": 0, "precios": 0, "costos": 0}
        
        if data.saldo:
            saldo_norm = normalize_saldo_columns(data.saldo)
            if data.incremental:
                inserted["saldo"] = db.upsert_saldo(saldo_norm, timestamp)
            else:
                db.insert_saldo(saldo_norm, timestamp)
                inserted["saldo"] = len(saldo_norm)
        
        if data.ventas:
            ventas_norm = normalize_ventas_columns(data.ventas)
            if data.incremental:
                inserted["ventas"] = db.upsert_ventas(ventas_norm, timestamp)
            else:
                db.insert_ventas(ventas_norm, timestamp)
                inserted["ventas"] = len(ventas_norm)
        
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

# ============== ENDPOINTS DE COSTOS ==============

@app.get("/costos")
async def get_costos():
    """Obtener todos los costos de reposición."""
    costos = db.get_all_costos()
    return {"costos": costos, "total": len(costos)}

@app.get("/costos/{cod_articulo}")
async def get_costo_articulo(cod_articulo: str):
    """Obtener costo de un artículo específico."""
    costo = db.get_costo_articulo(cod_articulo)
    if costo:
        return costo
    return {"error": "Artículo no encontrado", "cod_articulo": cod_articulo}

@app.post("/costos")
async def upload_costos(request: Request):
    """Subir o actualizar costos de reposición."""
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
    """Obtener métricas con costos de reposición integrados."""
    data = db.get_metricas_con_costos(sucursal, alerta, familia)
    return {"data": data, "total": len(data)}

@app.get("/resumen-costos")
async def get_resumen_costos():
    """Obtener resumen de valores de stock y reposición por sucursal."""
    resumen = db.get_resumen_costos_por_sucursal()
    return {"resumen": resumen}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.get("/sync-info")
async def get_sync_info():
    """Obtener info para sincronización incremental"""
    info = db.get_sync_info()
    # Convertir fechas a string para JSON
    if info.get("ultima_fecha_ventas"):
        info["ultima_fecha_ventas"] = str(info["ultima_fecha_ventas"])
    if info.get("ultima_sync_saldo"):
        info["ultima_sync_saldo"] = str(info["ultima_sync_saldo"])
    return info

@app.post("/recalcular-metricas")
async def recalcular_metricas():
    """Recalcular métricas desde los datos existentes"""
    try:
        timestamp = datetime.now()
        
        # Obtener datos actuales
        saldos = db.get_all_saldos()
        ventas = db.get_all_ventas()
        
        if not saldos:
            return {"status": "error", "message": "No hay saldos para calcular métricas"}
        
        df_saldo = pd.DataFrame(saldos)
        df_ventas = pd.DataFrame(ventas) if ventas else pd.DataFrame()
        
        # Calcular métricas
        df_resultado = calcular_metricas(df_saldo, df_ventas)
        
        # Guardar métricas
        metricas_records = df_resultado.to_dict(orient="records")
        db.clear_metricas()
        db.insert_metricas(metricas_records, timestamp)
        
        # Registrar sync
        db.log_sync(
            registros_saldo=len(saldos),
            registros_ventas=len(ventas) if ventas else 0,
            registros_metricas=len(metricas_records),
            status="ok",
            message="Métricas recalculadas"
        )
        
        return {
            "status": "ok",
            "message": "Métricas recalculadas",
            "registros": len(metricas_records),
            "timestamp": timestamp.isoformat()
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    totales: dict = db.get_totales()
    alertas: dict = db.get_alertas_count()
    last_sync = db.get_last_sync()
    
    sync_info = "Sin sincronizar"
    if last_sync:
        sync_info = f"{last_sync.get('timestamp', 'N/A')}"
    
    html = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Dashboard - Analisis Comercial</title>
        <style>
            * {{ margin: 0; padding: 0; box-sizing: border-box; }}
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f5f5; padding: 20px; }}
            .container {{ max-width: 1200px; margin: 0 auto; }}
            h1 {{ color: #1f2937; margin-bottom: 20px; font-size: 1.8rem; }}
            .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 30px; }}
            .card {{ background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            .card h3 {{ font-size: 0.9rem; color: #6b7280; margin-bottom: 8px; }}
            .card .value {{ font-size: 1.8rem; font-weight: bold; color: #1f2937; }}
            .quiebre {{ border-left: 4px solid #ef4444; }}
            .quiebre .value {{ color: #ef4444; }}
            .normal {{ border-left: 4px solid #22c55e; }}
            .sobrestock {{ border-left: 4px solid #f59e0b; }}
            .seguridad {{ border-left: 4px solid #3b82f6; }}
            .sync-info {{ background: #e0f2fe; padding: 15px; border-radius: 8px; margin-bottom: 20px; }}
            .refresh-btn {{ background: #3b82f6; color: white; padding: 10px 20px; border: none; border-radius: 6px; cursor: pointer; font-size: 1rem; }}
            .refresh-btn:hover {{ background: #2563eb; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Dashboard - Analisis Comercial de Reposicion</h1>
            
            <div class="sync-info">
                Ultima sincronizacion: {sync_info}
                <button class="refresh-btn" onclick="location.reload()" style="margin-left: 20px;">Actualizar</button>
            </div>
            
            <div class="grid">
                <div class="card">
                    <h3>Total Articulos</h3>
                    <div class="value">{totales.get('total_articulos', 0):,}</div>
                </div>
                <div class="card">
                    <h3>Stock Total</h3>
                    <div class="value">{float(totales.get('stock_total', 0)):,.0f}</div>
                </div>
                <div class="card">
                    <h3>Venta Total</h3>
                    <div class="value">{float(totales.get('venta_total', 0)):,.0f}</div>
                </div>
                <div class="card quiebre">
                    <h3>En Quiebre</h3>
                    <div class="value">{alertas.get('Quiebre', 0):,}</div>
                </div>
                <div class="card seguridad">
                    <h3>Stock de Seguridad</h3>
                    <div class="value">{alertas.get('Stock de Seguridad', 0):,}</div>
                </div>
                <div class="card normal">
                    <h3>Stock Normal</h3>
                    <div class="value">{alertas.get('Normal', 0):,}</div>
                </div>
                <div class="card sobrestock">
                    <h3>Sobrestock</h3>
                    <div class="value">{alertas.get('Sobrestock', 0):,}</div>
                </div>
                <div class="card">
                    <h3>Sin Rotacion</h3>
                    <div class="value">{alertas.get('Sin rotación', 0):,}</div>
                </div>
            </div>
            
            <p style="color: #6b7280; font-size: 0.9rem;">
                Para el dashboard completo con filtros y graficos, accede al Streamlit Dashboard en el puerto 8000.
            </p>
        </div>
    </body>
    </html>
    """
    return html

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
            return {"success": False, "error": "Tipo de mensaje no válido"}
        
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
                return {"success": False, "error": "Se requiere parámetro sucursal"}
            suc_data = next((s for s in datos if s["sucursal"].upper() == sucursal.upper()), None)
            if not suc_data:
                return {"success": False, "error": f"Sucursal {sucursal} no encontrada"}
            mensaje = wa.generar_mensaje_alerta_sucursal(suc_data)
        else:
            return {"success": False, "error": "Tipo no válido. Use: resumen, comercial, sucursal"}
        
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
            return {"success": False, "error": "Tipo de mensaje no válido. Use: resumen, sucursal, comercial"}
        
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
            return {"error": "Tipo no válido"}
        
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

ws_proxy_log = []

def wlog(msg):
    logger.info(msg)
    ws_proxy_log.append(f"{datetime.now().isoformat()} {msg}")
    if len(ws_proxy_log) > 100:
        ws_proxy_log.pop(0)

@app.get("/wsdebug")
async def wsdebug():
    return {"ws_proxy_log": ws_proxy_log}

@app.websocket("/_stcore/stream")
async def ws_proxy(ws: WebSocket):
    await ws.accept()
    wlog("client connected")
    cookie_header = ws.headers.get("cookie", "")
    origin = ws.headers.get("origin", "")
    wlog(f"client headers: cookie={bool(cookie_header)} origin={origin}")
    headers = {}
    if cookie_header:
        headers["Cookie"] = cookie_header
    if origin:
        headers["Origin"] = origin
    session = aiohttp.ClientSession()
    try:
        backend = await session.ws_connect(
            f"http://127.0.0.1:{STREAMLIT_PORT}/_stcore/stream",
            max_msg_size=2**23,
            heartbeat=30,
            headers=headers,
        )
        wlog("backend connected")

        async def client_to_server():
            c2s_count = 0
            try:
                while True:
                    msg = await ws.receive()
                    msg_type = msg.get("type", "unknown")
                    if msg_type == "websocket.disconnect":
                        wlog(f"c2s: client disconnect after {c2s_count} msgs")
                        break
                    if "text" in msg and msg["text"] is not None:
                        c2s_count += 1
                        wlog(f"c2s: text len={len(msg['text'])} (msg #{c2s_count})")
                        await backend.send_str(msg["text"])
                    elif "bytes" in msg and msg["bytes"] is not None:
                        c2s_count += 1
                        wlog(f"c2s: bytes len={len(msg['bytes'])} (msg #{c2s_count})")
                        await backend.send_bytes(msg["bytes"])
                    else:
                        wlog(f"c2s: unknown msg type={msg_type} keys={list(msg.keys())}")
            except Exception as e:
                wlog(f"c2s ended: {type(e).__name__}: {e} after {c2s_count} msgs")

        async def server_to_client():
            s2c_count = 0
            try:
                async for msg in backend:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        s2c_count += 1
                        wlog(f"s2c: text len={len(msg.data)} (msg #{s2c_count})")
                        await ws.send_text(msg.data)
                    elif msg.type == aiohttp.WSMsgType.BINARY:
                        s2c_count += 1
                        wlog(f"s2c: bytes len={len(msg.data)} (msg #{s2c_count})")
                        await ws.send_bytes(msg.data)
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        wlog(f"s2c: backend closed/error type={msg.type} after {s2c_count} msgs")
                        break
                    elif msg.type == aiohttp.WSMsgType.CLOSING:
                        wlog(f"s2c: backend closing after {s2c_count} msgs")
                        break
                    else:
                        wlog(f"s2c: unknown type={msg.type} after {s2c_count} msgs")
            except Exception as e:
                wlog(f"s2c ended: {type(e).__name__}: {e} after {s2c_count} msgs")

        done, pending = await asyncio.wait(
            [asyncio.create_task(client_to_server()), asyncio.create_task(server_to_client())],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        wlog(f"proxy tasks done, {len(pending)} cancelled")
    except Exception as e:
        wlog(f"proxy error: {type(e).__name__}: {e}")
    finally:
        try:
            await backend.close()
        except Exception:
            pass
        await session.close()
        try:
            await ws.close()
        except Exception:
            pass
        wlog("proxy closed")

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD"])
async def proxy_to_streamlit(request: Request, path: str = ""):
    for api_path in API_PATHS:
        if f"/{path}".startswith(api_path):
            return Response(status_code=404, content="Not Found")

    target_url = f"http://127.0.0.1:{STREAMLIT_PORT}/{path}"
    if request.url.query:
        target_url += f"?{request.url.query}"

    headers = dict(request.headers)
    headers.pop("host", None)

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            body = await request.body()
            resp = await client.request(
                method=request.method,
                url=target_url,
                headers=headers,
                content=body,
            )
            excluded = {"transfer-encoding", "content-encoding", "content-length"}
            resp_headers = {k: v for k, v in resp.headers.items() if k.lower() not in excluded}
            return Response(
                content=resp.content,
                status_code=resp.status_code,
                headers=resp_headers,
            )
    except Exception:
        return Response(status_code=502, content="Streamlit not ready")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)

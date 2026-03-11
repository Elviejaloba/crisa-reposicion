import os
import pyodbc
import pandas as pd
import requests
import json
import time
from datetime import datetime, time as dt_time, timedelta
import urllib3

def load_env(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())
    except Exception:
        pass

load_env(os.path.join(os.path.dirname(__file__), ".env"))

# Deshabilitar warnings de SSL para conexiones corporativas
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
SSL_VERIFY = False  # Cambiar a True si no hay problemas de certificados

conn_str = (
    "Driver={ODBC Driver 11 for SQL Server};"
    "Server=tangoserver;"
    "Database=crisa_real1;"
    "UID=Axoft;"
    "PWD=Axoft;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
)

# ==============================================================
# URL del API - LOCAL / PRODUCCIÓN
# ==============================================================
# Por defecto apunta al API local. Se puede sobrescribir con SYNC_URL.
API_URL = os.environ.get("SYNC_URL", "http://localhost:5000")

# URL alternativa para DESARROLLO (si necesitás un endpoint remoto):
# API_URL = "https://tu-api-remota.com"

SYNC_INTERVAL = 3600  # 60 minutos entre sincronizaciones
BATCH_SIZE = 500     # Lotes más chicos para evitar timeouts
MAX_RETRIES = 3      # Reintentos por lote
DB_MAX_RETRIES = int(os.environ.get("DB_MAX_RETRIES", "4"))
DB_RETRY_BACKOFF = [2, 5, 10, 20]
DB_LOCK_TIMEOUT_MS = int(os.environ.get("DB_LOCK_TIMEOUT_MS", "30000"))

SYNC_HORARIO_INICIO = dt_time(8, 0)   # Desde las 8:00
SYNC_HORARIO_FIN = dt_time(21, 0)     # Hasta las 21:00
SYNC_DIAS_HABILITADOS = {0, 1, 2, 3, 4, 5}  # 0=Lun ... 5=Sab
SYNC_SOLO_EN_HORARIO = True  # True = solo sync dentro del horario y días definidos

CACHE_DIR = os.path.join(os.path.dirname(__file__), "bridge_cache")
os.makedirs(CACHE_DIR, exist_ok=True)
SALDO_SNAPSHOT = os.path.join(CACHE_DIR, "saldo_snapshot.pkl")
COSTO_SNAPSHOT = os.path.join(CACHE_DIR, "costos_snapshot.pkl")

def _build_signature(df, cols):
    return df[cols].astype(str).agg("|".join, axis=1)

def filtrar_incremental_local(df, key_cols, snapshot_path, label):
    if df.empty:
        return df, False
    try:
        sig_cols = list(df.columns)
        curr = df.copy()
        curr["__sig__"] = _build_signature(curr, sig_cols)

        if not os.path.exists(snapshot_path):
            return df, True

        prev = pd.read_pickle(snapshot_path)
        if "__sig__" not in prev.columns:
            print(f"    Aviso: snapshot previo inválido para {label}, se envía completo.")
            return df, True
        prev = prev[key_cols + ["__sig__"]]
        merged = curr[key_cols + ["__sig__"]].merge(
            prev, on=key_cols, how="left", suffixes=("", "_prev")
        )
        if "__sig__prev" not in merged.columns:
            print(f"    Aviso: snapshot previo incompleto para {label}, se envía completo.")
            return df, True
        changed = merged["__sig__"] != merged["__sig__prev"]
        df_filtrado = df.loc[changed].copy()
        print(f"    Incremental local ({label}): {len(df_filtrado)} / {len(df)} registros")
        return df_filtrado, True
    except Exception as e:
        print(f"    Aviso: no se pudo filtrar incremental local de {label}: {e}")
        return df, False

def guardar_snapshot(df, key_cols, snapshot_path, label):
    if df.empty:
        return
    try:
        sig_cols = list(df.columns)
        snap = df.copy()
        snap["__sig__"] = _build_signature(snap, sig_cols)
        snap = snap[key_cols + ["__sig__"]]
        snap.to_pickle(snapshot_path)
    except Exception as e:
        print(f"    Aviso: no se pudo guardar snapshot de {label}: {e}")

def normalizar_unidad(cod):
    cod = str(cod or "").strip().upper()
    if cod in ("KG", "KGS", "KGR"):
        return "Kilo"
    if cod in ("MT", "MTS", "M"):
        return "Metro"
    if cod in ("U", "UN", "UNS", "UNI"):
        return "Unidad"
    return "Otro"

def rubro_macro(cod_familia):
    cod = str(cod_familia or "").strip().upper()
    if cod.startswith("ME"):
        return "Mercería"
    if cod == "BL":
        return "Blanco"
    if cod in {"TA", "TI", "TV", "TF", "TM", "TD", "TC", "PV", "82", "84"}:
        return "Telas"
    if cod in {"AR", "BO", "CO", "MU", "OT", "MC"}:
        return "Impulso"
    return "Otros"

def categoria_unm(unidad, cantidad):
    try:
        q = float(cantidad or 0)
    except Exception:
        q = 0
    u = str(unidad or "")
    if u == "Kilo":
        if q >= 100: return "GRAN MAYORISTA"
        if q >= 20: return "MAYORISTA 1"
        if q >= 10: return "MAYORISTA 2"
        if q >= 5: return "MAYORISTA 3"
        return None
    if u == "Metro":
        if q >= 250: return "GRAN MAYORISTA"
        if q >= 200: return "MAYORISTA 1"
        if q >= 150: return "MAYORISTA 2"
        if q >= 100: return "MAYORISTA 3"
        return None
    if u == "Unidad":
        if q >= 3000: return "GRAN MAYORISTA"
        if q >= 200: return "MAYORISTA 1"
        if q >= 150: return "MAYORISTA 2"
        if q >= 100: return "MAYORISTA 3"
        return None
    return None

def tipo_venta(desc_sucursal, tipo_comp):
    suc = str(desc_sucursal or "").strip().upper()
    comp = str(tipo_comp or "").strip().upper()
    es_mayorista_sucursal = suc in {"LA TIJERA MAYORISTA MENDOZA", "LA TIJERA MAYORISTA SJUAN"}
    es_mayorista_prefijo = any(comp.startswith(p) for p in ["ARC", "NCX", "XFA"])
    minorista_comprobantes = {"C20", "C24", "C25", "FAC", "N/C", "NCD2"}
    if suc == "LA TIJERA MENDOZA":
        return "Comp. Minorista"
    if es_mayorista_sucursal:
        return "Comp. Mayorista"
    if es_mayorista_prefijo:
        return "Comp. Mayorista"
    if comp in minorista_comprobantes:
        return "Comp. Minorista"
    return None

def sub_rubro(cod_articulo, descripcion):
    cod = str(cod_articulo or "").strip().upper()
    desc = str(descripcion or "").strip().upper()
    if cod.startswith("PV") and (desc.startswith("MEDIAS") or desc.startswith("SOQUETE")):
        return "Medias"
    if cod.startswith("PV") and desc.startswith("REMERA"):
        return "Remeras"
    if cod.startswith("TC") and desc.startswith("PACK"):
        return "Pack de Remeras"
    if cod.startswith("OT") and (desc.startswith("PAÑO") or desc.startswith("MOPA") or desc.startswith("REJI") or desc.startswith("FRAN")):
        return "Limpieza"
    if (cod.startswith("AR") and (desc.startswith("AROM") or desc.startswith("DIFU"))) or (cod.startswith("HS") and desc.startswith("HOMES")):
        return "Aromatización"
    if cod.startswith("AR"):
        return descripcion
    return "Sin Clasificar"


def json_serial(obj):
    """Serializar cualquier tipo de fecha/timestamp a string"""
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    return str(obj)


def _es_error_transitorio_db(err: Exception) -> bool:
    msg = str(err).lower()
    return ("1205" in msg or "deadlock" in msg or "1222" in msg or "lock request time out" in msg or "timeout" in msg)


def _resumen_error_db(err: Exception) -> str:
    msg = str(err)
    if not msg:
        return ""
    lines = [l.strip() for l in msg.splitlines() if l.strip()]
    return lines[-1] if lines else msg


def read_sql_with_retry(query, conn, params=None, label="consulta"):
    """Ejecuta SELECT con reintentos ante deadlock/timeout para no colgar SQL"""
    last_err = None
    for intento in range(DB_MAX_RETRIES):
        try:
            return pd.read_sql(query, conn, params=params)
        except pyodbc.Error as e:
            last_err = e
            if _es_error_transitorio_db(e) and intento < DB_MAX_RETRIES - 1:
                wait_time = DB_RETRY_BACKOFF[min(intento, len(DB_RETRY_BACKOFF) - 1)]
                print(f"    [DB] {label}: deadlock/timeout, reintento {intento + 1}/{DB_MAX_RETRIES} en {wait_time}s...")
                time.sleep(wait_time)
                continue
            resumen = _resumen_error_db(e)
            print(f"    [DB] {label}: error no recuperable: {resumen}")
            raise
    raise last_err


def esta_en_horario_sync():
    if not SYNC_SOLO_EN_HORARIO:
        return True

    now = datetime.now()
    if now.weekday() not in SYNC_DIAS_HABILITADOS:
        return False

    ahora = now.time()
    if SYNC_HORARIO_INICIO > SYNC_HORARIO_FIN:
        return ahora >= SYNC_HORARIO_INICIO or ahora < SYNC_HORARIO_FIN
    else:
        return SYNC_HORARIO_INICIO <= ahora < SYNC_HORARIO_FIN


def get_sync_info():
    """Obtener información de sincronización desde el API"""
    try:
        response = requests.get(f"{API_URL}/sync-info", timeout=30, verify=SSL_VERIFY)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"  Aviso: No se pudo obtener sync-info: {e}")
    return {}

def esperar_api(max_intentos=5, espera_seg=5):
    for _ in range(max_intentos):
        try:
            r = requests.get(f"{API_URL}/health", timeout=10, verify=SSL_VERIFY)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(espera_seg)
    return False


def enviar_en_lotes(url, nombre, df, batch_size=2000, incremental=True):
    """Enviar datos en lotes pequeños con UPSERT y reintentos"""
    total = len(df)
    if total == 0:
        return True, 0
    
    registros = df.to_dict(orient="records")
    enviados = 0
    
    for i in range(0, total, batch_size):
        lote = registros[i:i+batch_size]
        lote_num = (i // batch_size) + 1
        total_lotes = (total + batch_size - 1) // batch_size
        
        for intento in range(MAX_RETRIES):
            try:
                if intento > 0:
                    wait_time = 5 * (2 ** intento)
                    print(f"    Reintento {intento + 1}/{MAX_RETRIES} en {wait_time}s...")
                    time.sleep(wait_time)
                
                print(f"    {nombre}: Lote {lote_num}/{total_lotes} ({len(lote)} registros)...")
                
                data = {nombre: lote, "incremental": incremental}
                json_data = json.dumps(data, default=json_serial)
                
                response = requests.post(
                    f"{url}/sync",
                    data=json_data,
                    headers={'Content-Type': 'application/json'},
                    timeout=300,
                    verify=SSL_VERIFY
                )
                
                if response.status_code == 200:
                    enviados += len(lote)
                    break
                elif response.status_code in [502, 503, 504]:
                    print(f"    Timeout servidor (HTTP {response.status_code}), reintentando...")
                    if intento == MAX_RETRIES - 1:
                        print(f"    Error persistente en lote {lote_num} después de {MAX_RETRIES} intentos")
                        break
                else:
                    print(f"    Error en lote {lote_num}: {response.status_code} - {response.text[:200]}")
                    break
                    
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                print(f"    Timeout/Conexion en lote {lote_num}: {type(e).__name__}")
                if intento == MAX_RETRIES - 1:
                    print(f"    Continuando con siguiente lote...")
                    break
            except Exception as e:
                print(f"    Error enviando lote {lote_num}: {e}")
                break
    
    return True, enviados


def get_data():
    print("=" * 60)
    print("BRIDGE SQL - Sincronización Incremental Tango -> API")
    print("=" * 60)
    print(f"Servidor: tangoserver")
    print(f"Base de datos: crisa_real1")
    print(f"URL destino: {API_URL}")
    print(f"Tamaño de lote: {BATCH_SIZE} registros")
    print(f"Modo: EJECUCIÓN ÚNICA (UPSERT)")
    print("=" * 60)

    try:
        print(f"\n[{datetime.now()}] Iniciando sincronización...")
        if not esperar_api():
            print("  Aviso: API no disponible, se reintentará en el próximo ciclo.")
            return
        
        # Obtener info de última sincronización
        sync_info = get_sync_info()
        ultima_fecha_ventas = sync_info.get("ultima_fecha_ventas")
        total_ventas_existentes = sync_info.get("total_ventas", 0)
        total_saldos_existentes = sync_info.get("total_saldos", 0)
        total_precios_existentes = sync_info.get("total_precios", 0)
        total_costos_existentes = sync_info.get("total_costos", 0)
        total_articulos_existentes = sync_info.get("total_articulos", 0)
        ultima_sync_precios = sync_info.get("ultima_sync_precios")
        ultima_sync_costos = sync_info.get("ultima_sync_costos")
        ultima_sync_articulos = sync_info.get("ultima_sync_articulos")
        
        print(f"  Estado actual en API:")
        print(f"    - Saldos: {total_saldos_existentes}")
        print(f"    - Ventas: {total_ventas_existentes}")
        print(f"    - Precios: {total_precios_existentes}")
        print(f"    - Costos: {total_costos_existentes}")
        print(f"    - Articulos: {total_articulos_existentes}")
        if ultima_fecha_ventas:
            print(f"    - Última fecha ventas: {ultima_fecha_ventas}")
        if ultima_sync_precios:
            print(f"    - Última sync precios: {ultima_sync_precios}")
        if ultima_sync_articulos:
            print(f"    - Última sync artículos: {ultima_sync_articulos}")
        
        # Determinar si es primera sincronización
        es_primera_sync = total_saldos_existentes == 0

        conn = pyodbc.connect(conn_str, timeout=30, autocommit=True)
        conn.timeout = 120

        # ============================================================
        # SALDOS - Siempre actualizar (UPSERT)
        # ============================================================
        print(f"\n  [SALDOS] Consultando stock actual...")
        
        query_saldo = """
            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
            SET DATEFORMAT DMY
            SET DATEFIRST 7
            SET LOCK_TIMEOUT 30000;
            SET DEADLOCK_PRIORITY LOW;
            SELECT
                CTA_ARTICULO.COD_CTA_ARTICULO AS [Cod. Articulo] ,
                CTA_ARTICULO.DESC_CTA_ARTICULO AS [Articulo] ,
                CTA_ARTICULO.SINONIMO AS [Sinonimo] ,
                CTA_DEPOSITO.COD_CTA_DEPOSITO AS [Cod. Deposito] ,
                SUCURSAL.NRO_SUCURSAL AS [Nro. Sucursal] ,
                SUCURSAL.DESC_SUCURSAL AS [Sucursal] ,
                CTA_DEPOSITO.DESC_CTA_DEPOSITO AS [Deposito] ,
                (CASE CTA_ARTICULO.BASE when '' then CTA_ARTICULO.COD_ARTICULO ELSE CTA_ARTICULO.BASE end) AS [Cod. base / articulo] ,
                (CASE CTA_ARTICULO.BASE when '' then CTA_ARTICULO.DESC_CTA_ARTICULO ELSE BASE.DESC_CTA_ARTICULO end) AS [Desc. Base / Articulo] ,
                CTA_ARTICULO.ESCALA_1 AS [Cod. escala 1] ,
                STA33.DESC_VALOR AS [Desc. escala 1] ,
                MEDIDA_STOCK.SIGLA_MEDIDA AS [U.M. stock] ,
                SUM(CTA_SALDO_ARTICULO_DEPOSITO.CANTIDAD_STOCK) AS [Stock 1]
            FROM
            CTA_SALDO_ARTICULO_DEPOSITO RIGHT JOIN (SELECT ID_CTA_ARTICULO, ID_CTA_DEPOSITO,ID_SUCURSAL,MAX(FECHA) AS [FECHA_MAX] FROM CTA_SALDO_ARTICULO_DEPOSITO GROUP BY ID_CTA_ARTICULO, ID_CTA_DEPOSITO,ID_SUCURSAL) AS ULT_SALDO ON (CTA_SALDO_ARTICULO_DEPOSITO.ID_CTA_ARTICULO = ULT_SALDO.ID_CTA_ARTICULO AND CTA_SALDO_ARTICULO_DEPOSITO.ID_CTA_DEPOSITO = ULT_SALDO.ID_CTA_DEPOSITO AND CTA_SALDO_ARTICULO_DEPOSITO.ID_SUCURSAL = ULT_SALDO.ID_SUCURSAL AND CTA_SALDO_ARTICULO_DEPOSITO.FECHA = ULT_SALDO.FECHA_MAX)
            LEFT JOIN CTA_ARTICULO ON (CTA_SALDO_ARTICULO_DEPOSITO.ID_CTA_ARTICULO = CTA_ARTICULO.ID_CTA_ARTICULO)
            LEFT JOIN (SELECT COD_ARTICULO, DESC_CTA_ARTICULO FROM CTA_ARTICULO WHERE USA_ESC = 'B') AS BASE ON (BASE.COD_ARTICULO = CTA_ARTICULO.BASE)
            LEFT JOIN SUCURSAL ON (CTA_SALDO_ARTICULO_DEPOSITO.ID_SUCURSAL = SUCURSAL.ID_SUCURSAL)
            LEFT JOIN CTA_DEPOSITO ON (CTA_SALDO_ARTICULO_DEPOSITO.ID_CTA_DEPOSITO = CTA_DEPOSITO.ID_CTA_DEPOSITO)
            LEFT JOIN STA33 ON (CTA_ARTICULO.ESCALA_1 = STA33.COD_ESCALA AND CTA_ARTICULO.VALOR1 = STA33.COD_VALOR)
            LEFT JOIN STA33 AS STA33_BIS ON (CTA_ARTICULO.ESCALA_2 = STA33_BIS.COD_ESCALA AND CTA_ARTICULO.VALOR2 = STA33_BIS.COD_VALOR)
            LEFT JOIN CTA_ARTICULO_SUCURSAL ON (CTA_SALDO_ARTICULO_DEPOSITO.ID_CTA_ARTICULO = CTA_ARTICULO_SUCURSAL.ID_CTA_ARTICULO AND CTA_SALDO_ARTICULO_DEPOSITO.ID_SUCURSAL = CTA_ARTICULO_SUCURSAL.ID_SUCURSAL)
            LEFT JOIN CTA_MEDIDA AS MEDIDA_STOCK ON (CTA_ARTICULO_SUCURSAL.ID_CTA_MEDIDA_STOCK = MEDIDA_STOCK.ID_CTA_MEDIDA)
            LEFT JOIN CTA_MEDIDA AS MEDIDA_STOCK_2 ON (CTA_ARTICULO_SUCURSAL.ID_CTA_MEDIDA_STOCK_2 = MEDIDA_STOCK_2.ID_CTA_MEDIDA)
            WHERE
            CTA_ARTICULO.STOCK = 1
            GROUP BY
                CTA_ARTICULO.COD_CTA_ARTICULO, CTA_ARTICULO.DESC_CTA_ARTICULO, CTA_ARTICULO.SINONIMO, CTA_DEPOSITO.COD_CTA_DEPOSITO, SUCURSAL.NRO_SUCURSAL, SUCURSAL.DESC_SUCURSAL, CTA_DEPOSITO.DESC_CTA_DEPOSITO, (CASE CTA_ARTICULO.BASE when '' then CTA_ARTICULO.COD_ARTICULO ELSE CTA_ARTICULO.BASE end), (CASE CTA_ARTICULO.BASE when '' then CTA_ARTICULO.DESC_CTA_ARTICULO ELSE BASE.DESC_CTA_ARTICULO end), CTA_ARTICULO.ESCALA_1, STA33.DESC_VALOR, MEDIDA_STOCK.SIGLA_MEDIDA
        """

        df_saldo = read_sql_with_retry(query_saldo, conn, label="saldos")
        print(f"    Obtenidos: {len(df_saldo)} registros")
        df_saldo_full = df_saldo
        df_saldo, _ = filtrar_incremental_local(
            df_saldo_full,
            ["Cod. Articulo", "Cod. Deposito", "Sucursal"],
            SALDO_SNAPSHOT,
            "saldos"
        )

        # ============================================================
        # VENTAS - INCREMENTAL: Solo desde última fecha o 7 días atrás
        # ============================================================
        if ultima_fecha_ventas and total_ventas_existentes > 0:
            # Restar 3 días para capturar modificaciones recientes
            fecha_desde = (datetime.strptime(ultima_fecha_ventas, "%Y-%m-%d") - timedelta(days=3)).strftime("%d/%m/%Y")
            print(f"\n  [VENTAS] Modo incremental desde {fecha_desde}")
        else:
            # Si no hay histórico, traer solo ventana reciente (no fija 2024)
            fecha_desde = (datetime.now() - timedelta(days=120)).strftime("%d/%m/%Y")
            print(f"\n  [VENTAS] Sin histórico: ventana reciente desde {fecha_desde}")

        fecha_hasta = datetime.now().strftime("%d/%m/%Y")

        query_ventas = f"""
            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
            SET DATEFORMAT DMY
            SET DATEFIRST 7
            SET LOCK_TIMEOUT 30000;
            SET DEADLOCK_PRIORITY LOW;
            SELECT
                CTA03.FECHA_MOV AS [Fecha] ,
                CTA02.NRO_SUCURS AS [Nro. Sucursal] ,
                SUCURSAL.DESC_SUCURSAL AS [Desc. sucursal] ,
                CTA02.T_COMP AS [Tipo de comprobante] ,
                CTA03.Cod_Articu AS [Cod. Articulo] ,
                CTA_ARTICULO.DESC_CTA_ARTICULO AS [Descripcion] ,
                CTA_ARTICULO.SINONIMO AS [Sinonimo] ,
                (CASE CTA_ARTICULO.BASE when '' then CTA_ARTICULO.COD_ARTICULO ELSE CTA_ARTICULO.BASE end) AS [Cod. base / articulo] ,
                (CASE CTA_ARTICULO.BASE when '' then CTA_ARTICULO.DESC_CTA_ARTICULO ELSE BASE.DESC_CTA_ARTICULO end) AS [Desc. Base / Articulo] ,
                ISNULL(FAMILIA_ART.COD_AGR,'') AS [Cod. Familia (Articulo)] ,
                FAMILIA_ART.NOM_AGR AS [Descripcion Familia (Articulo)] ,
                SUM(CASE CTA03.TCOMP_IN_V WHEN 'CC' THEN(-1) ELSE(1) END * CTA03.CANTIDAD / CASE WHEN CAN_EQUI_V = 0 THEN 1 ELSE CAN_EQUI_V END) AS [Cantidad venta] ,
                SUM(CASE CTA03.TCOMP_IN_V WHEN 'CC' THEN(-1) ELSE(1) END * CTA03.CANTIDAD) AS [Cantidad venta ERP] ,
                MAX(CASE WHEN CAN_EQUI_V = 0 THEN 1 ELSE CAN_EQUI_V END) AS [Factor Equiv] ,
                MEDIDA_STOCK.SIGLA_MEDIDA AS [U.M. stock] ,
                SUM(CASE CTA03.TCOMP_IN_V WHEN 'CC' THEN (-1) ELSE (1) END *
                    CASE 'BIMONCTE'
                        WHEN 'BIMONCTE' THEN
                            CASE CTA02.MON_CTE
                                WHEN 1 THEN CTA03.IMP_NETO_P * (CASE WHEN CTA02.IMPORTE_IV = 0 THEN 1 ELSE (1 + (CTA03.PORC_IVA/100)) END)
                                ELSE CTA03.IMP_NETO_P * (CASE WHEN CTA02.IMPORTE_IV = 0 THEN 1 ELSE (1 + (CTA03.PORC_IVA/100)) END) * CTA02.COTIZ
                            END
                        WHEN 'BIORIGEN' THEN
                            CASE CTA02.MON_CTE
                                WHEN 1 THEN CASE CTA02.COTIZ WHEN 0 THEN 0 ELSE CTA03.IMP_NETO_P * (CASE WHEN CTA02.IMPORTE_IV = 0 THEN 1 ELSE (1 + (CTA03.PORC_IVA/100)) END) / CTA02.COTIZ END
                                ELSE CTA03.IMP_NETO_P * (CASE WHEN CTA02.IMPORTE_IV = 0 THEN 1 ELSE (1 + (CTA03.PORC_IVA/100)) END)
                            END
                        WHEN 'BICOTIZ' THEN
                            CASE 1 WHEN 0 THEN 0 ELSE
                                CASE CTA02.MON_CTE
                                    WHEN 1 THEN CTA03.IMP_NETO_P * (CASE WHEN CTA02.IMPORTE_IV = 0 THEN 1 ELSE (1 + (CTA03.PORC_IVA/100)) END) / 1
                                    ELSE CTA03.IMP_NETO_P * (CASE WHEN CTA02.IMPORTE_IV = 0 THEN 1 ELSE (1 + (CTA03.PORC_IVA/100)) END) * CTA02.COTIZ / 1
                                END
                            END
                    END) AS [Imp. prop. c/IVA]
            FROM
                CTA03 (NOLOCK)
                INNER JOIN CTA02 (NOLOCK) ON (CTA02.N_COMP = CTA03.N_COMP AND CTA02.T_COMP = CTA03.T_COMP AND CTA03.NRO_SUCURS = CTA02.NRO_SUCURS)
                INNER JOIN SUCURSAL (NOLOCK) ON CTA02.NRO_SUCURS = SUCURSAL.NRO_SUCURSAL
                LEFT JOIN CTA_ARTICULO (NOLOCK) ON CTA03.Cod_Articu = CTA_ARTICULO.COD_ARTICULO
                LEFT JOIN STA16 ON 1=1
                LEFT JOIN STA29 FAMILIA_ART (NOLOCK) ON SUBSTRING(CTA_ARTICULO.COD_ARTICULO, 1, LONG_FAM_A) = FAMILIA_ART.COD_AGR
                LEFT JOIN (SELECT COD_ARTICULO, DESC_CTA_ARTICULO FROM CTA_ARTICULO WHERE USA_ESC = 'B') AS BASE ON (BASE.COD_ARTICULO = CTA_ARTICULO.BASE)
                LEFT JOIN (SELECT * FROM CTA_MEDIDA) AS MEDIDA_STOCK ON CTA03.ID_MEDIDA_STOCK = MEDIDA_STOCK.ID_CTA_MEDIDA
            WHERE
                CTA03.Cod_Articu NOT IN ('Art. Ajuste')
                AND (CTA03.Cod_Articu <> '')
                AND CTA02.T_COMP <> 'REC'
                AND (CTA03.FECHA_MOV BETWEEN '{fecha_desde}' AND '{fecha_hasta}')
                AND ((ISNULL(CTA03.RENGL_PADR,0) = 0) OR (ISNULL(CTA03.INSUMO_KIT_SEPARADO,0) = 1))
            GROUP BY
                CTA03.FECHA_MOV, CTA02.NRO_SUCURS, SUCURSAL.DESC_SUCURSAL, CTA02.T_COMP, CTA03.Cod_Articu, CTA_ARTICULO.DESC_CTA_ARTICULO, CTA_ARTICULO.SINONIMO,
                (CASE CTA_ARTICULO.BASE when '' then CTA_ARTICULO.COD_ARTICULO ELSE CTA_ARTICULO.BASE end),
                (CASE CTA_ARTICULO.BASE when '' then CTA_ARTICULO.DESC_CTA_ARTICULO ELSE BASE.DESC_CTA_ARTICULO end),
                ISNULL(FAMILIA_ART.COD_AGR,''), FAMILIA_ART.NOM_AGR, MEDIDA_STOCK.SIGLA_MEDIDA
        """

        df_ventas = read_sql_with_retry(query_ventas, conn, label="ventas")
        print(f"    Obtenidos: {len(df_ventas)} registros")

        if not df_ventas.empty:
            df_ventas["Unidad Normalizada"] = df_ventas["U.M. stock"].apply(normalizar_unidad)
            df_ventas["Rubro Macro"] = df_ventas["Cod. Familia (Articulo)"].apply(rubro_macro)
            df_ventas["Categoria UNM"] = df_ventas.apply(
                lambda r: categoria_unm(r.get("Unidad Normalizada"), r.get("Cantidad venta")), axis=1
            )
            df_ventas["Tipo de Venta"] = df_ventas.apply(
                lambda r: tipo_venta(r.get("Desc. sucursal"), r.get("Tipo de comprobante") or r.get("Tipo de comprobante", "")),
                axis=1
            )
            df_ventas["Sub Rubro"] = df_ventas.apply(
                lambda r: sub_rubro(r.get("Cod. Articulo"), r.get("Descripcion")), axis=1
            )

        # ============================================================
        # PRECIOS - Siempre actualizar (UPSERT)
        # ============================================================
        print(f"\n  [PRECIOS] Consultando listas 2 y 102...")
        
        query_precios = """
            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
            SET DATEFORMAT DMY
            SET DATEFIRST 7
            SET LOCK_TIMEOUT 30000;
            SET DEADLOCK_PRIORITY LOW;
            SELECT
                STA11.COD_ARTICU AS [Cod. Articulo] ,
                STA11.DESCRIPCIO AS [Descripcion] ,
                STA11.SINONIMO AS [Sinonimo] ,
                FAMILIA_ART.COD_AGR AS [Cod. familia] ,
                FAMILIA_ART.NOM_AGR AS [Familia] ,
                GVA17.PRECIO AS [Precio] ,
                GVA10.NRO_DE_LIS AS [Cod. Lista de Precios] ,
                GVA10.NOMBRE_LIS AS [Lista de precios] ,
                GVA17.FECHA_MODI AS [Fecha de ultima modificacion]
            FROM
             gva17 (nolock)  inner join gva10 (nolock) on gva10.NRO_DE_LIS = gva17.NRO_DE_LIS  inner join sta11 (nolock) on sta11.COD_ARTICU = gva17.COD_ARTICU
            LEFT JOIN STA16 ON 1=1
            LEFT JOIN STA29 FAMILIA_ART (NOLOCK) ON  SUBSTRING(STA11.COD_ARTICU, 1, LONG_FAM_A) = FAMILIA_ART.COD_AGR
            LEFT JOIN STA29 GRUPO_ART (NOLOCK) ON  SUBSTRING(STA11.COD_ARTICU, 0, LONG_FAM_A + LONG_GRU_A + 1) = GRUPO_ART.COD_AGR
            LEFT JOIN (SELECT COD_ARTICU, DESCRIPCIO FROM STA11 WHERE USA_ESC = 'B') AS BASE ON  (CASE sta11.base when '' then STA11.COD_ARTICU ELSE STA11.BASE end) = BASE.COD_ARTICU
            LEFT JOIN MEDIDA ON MEDIDA.ID_MEDIDA = STA11.ID_MEDIDA_STOCK
            LEFT JOIN (SELECT N1.CODE, N1.IDFOLDER,  F1.PADRE0, F1.PADRE1, F1.PADRE2, F1.PADRE3, F1.PADRE4, F1.PADRE5, F1.PADRE6, F1.PADRE7, F1.PADRE8, F1.PADRE9, F1.PADRE10, F1.PADRE11  FROM STA11ITC N1  JOIN V_LI_CLASIFICADOR_STA11FLD F1 ON  (N1.IDFOLDER= F1.IDFOLDER_V)  ) AS CLASIF_ITEMS ON CLASIF_ITEMS.CODE = STA11.COD_ARTICU
            WHERE
            sta11.perfil <> 'N'
             AND
             GVA10.HABILITADA = 1 AND GVA10.NRO_DE_LIS IN ( '2' , '102' )
            GROUP BY
                STA11.COD_ARTICU , STA11.DESCRIPCIO , STA11.SINONIMO , FAMILIA_ART.COD_AGR , FAMILIA_ART.NOM_AGR , GVA17.PRECIO , GVA10.NRO_DE_LIS , GVA10.NOMBRE_LIS , GVA17.FECHA_MODI
        """

        df_precios = read_sql_with_retry(query_precios, conn, label="precios")
        if ultima_sync_precios and not df_precios.empty:
            try:
                df_precios["Fecha de ultima modificacion"] = pd.to_datetime(df_precios["Fecha de ultima modificacion"], errors="coerce")
                df_precios = df_precios[df_precios["Fecha de ultima modificacion"] >= pd.to_datetime(ultima_sync_precios)]
                print(f"    Filtrados incrementales: {len(df_precios)} registros")
            except Exception as e:
                print(f"    Aviso: no se pudo filtrar incremental de precios: {e}")
        print(f"    Obtenidos: {len(df_precios)} registros")

        # ============================================================
        # COSTOS - Siempre actualizar (UPSERT)
        # ============================================================
        print(f"\n  [COSTOS] Consultando costos de reposición...")
        
        query_costos = """
            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
            SET DATEFORMAT DMY 
            SET DATEFIRST 7 
            SET LOCK_TIMEOUT 30000;
            SET DEADLOCK_PRIORITY LOW;
            SELECT 
                STA11.COD_ARTICU AS [Cod. Articulo],
                STA11.DESCRIPCIO AS [Descripcion],
                STA11.SINONIMO AS [Sinonimo],
                ISNULL(FAMILIA_ART.COD_AGR, '') AS [Cod. Familia],
                ROUND(ISNULL((SELECT TOP 1 
                    (CASE 'BIMONCTE' 
                        WHEN 'BIMONCTE' THEN (case when sta12.mon_cte = 1 then PRECIO_REP else (PRECIO_REP * CASE WHEN 0 > 0 THEN 0 ELSE GVA16.COTIZ END) end)
                        ELSE (case when sta12.mon_cte = 0 then PRECIO_REP else (PRECIO_REP / CASE WHEN 0 > 0 THEN 0 ELSE GVA16.COTIZ END) end) 
                    END) AS PRECIO 
                    FROM STA12 WHERE STA12.COD_ARTICU = STA11.COD_ARTICU), 0), 
                    ISNULL((SELECT PRECIOS FROM TGANUM), 2)) AS [Costo]
            FROM 
            STA11 
            LEFT JOIN GVA16 ON 1=1 
            LEFT JOIN STA12 ON STA12.COD_ARTICU = STA11.COD_ARTICU 
            LEFT JOIN TGANUM ON 1=1 
            LEFT JOIN STA16 ON 1 = 1  
            LEFT JOIN STA29 FAMILIA_ART ON SUBSTRING(STA11.COD_ARTICU, 1, STA16.LONG_FAM_A) = FAMILIA_ART.COD_AGR  
            WHERE 
                STA11.Stock = 1 AND STA11.PERFIL <> 'N' 
            GROUP BY 
                STA11.COD_ARTICU, STA11.DESCRIPCIO, STA11.SINONIMO, ISNULL(FAMILIA_ART.COD_AGR, ''), GVA16.COTIZ
        """

        df_costos = read_sql_with_retry(query_costos, conn, label="costos")
        # Costos no tienen fecha de modificación confiable -> se envían completos (UPSERT)
        print(f"    Obtenidos: {len(df_costos)} registros")
        df_costos_full = df_costos
        df_costos, _ = filtrar_incremental_local(
            df_costos_full,
            ["Cod. Articulo"],
            COSTO_SNAPSHOT,
            "costos"
        )

        # ============================================================
        # ARTÍCULOS - Nómina base
        # ============================================================
        print(f"\n  [ARTICULOS] Consultando nómina de artículos...")
        query_articulos = """
            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
            SET DATEFORMAT DMY
            SET DATEFIRST 7
            SET LOCK_TIMEOUT 30000;
            SET DEADLOCK_PRIORITY LOW;
            SELECT
                STA11.COD_ARTICU AS [Cod. Articulo],
                STA11.DESCRIPCIO AS [Descripcion],
                STA11.DESC_ADIC AS [Desc. Adicional],
                STA11.SINONIMO AS [Sinonimo],
                CASE STA11.BASE WHEN '' THEN STA11.COD_ARTICU ELSE STA11.BASE END AS [Cod. base / articulo],
                BASE.DESCRIPCIO AS [Desc. Articulo Base],
                FAMILIA_ART.NOM_AGR AS [Familia],
                GRUPO_ART.COD_AGR AS [Cod. agrupacion],
                GRUPO_ART.NOM_AGR AS [Desc. agrupacion],
                STA11.COD_BARRA AS [Codigo de Barras],
                STA11.Fecha_Alta AS [Fecha de alta],
                MEDIDA_STOCK.SIGLA_MEDIDA AS [U.M. stock],
                CASE STOCK WHEN 0 THEN 'No' ELSE 'Si' END AS [Lleva stock asociado],
                CASE STA11.LLEVA_DOBLE_UNIDAD_MEDIDA WHEN 0 THEN 'No' ELSE 'Si' END AS [Lleva doble unidad de medida]
            FROM STA11
            LEFT JOIN STA16 ON 1=1
            LEFT JOIN STA29 FAMILIA_ART ON SUBSTRING(STA11.COD_ARTICU, 1, LONG_FAM_A) = FAMILIA_ART.COD_AGR
            LEFT JOIN STA29 GRUPO_ART ON SUBSTRING(STA11.COD_ARTICU, 0, LONG_FAM_A+LONG_GRU_A+1) = GRUPO_ART.COD_AGR
            LEFT JOIN (SELECT COD_ARTICU, DESCRIPCIO FROM STA11 WHERE USA_ESC = 'B') AS BASE
                ON (CASE STA11.BASE WHEN '' THEN STA11.COD_ARTICU ELSE STA11.BASE END) = BASE.COD_ARTICU
            LEFT JOIN MEDIDA AS MEDIDA_STOCK ON STA11.ID_MEDIDA_STOCK = MEDIDA_STOCK.ID_MEDIDA
            GROUP BY
                STA11.COD_ARTICU, STA11.DESCRIPCIO, STA11.DESC_ADIC, STA11.SINONIMO,
                CASE STA11.BASE WHEN '' THEN STA11.COD_ARTICU ELSE STA11.BASE END,
                BASE.DESCRIPCIO, FAMILIA_ART.NOM_AGR, GRUPO_ART.COD_AGR, GRUPO_ART.NOM_AGR,
                STA11.COD_BARRA, STA11.Fecha_Alta, MEDIDA_STOCK.SIGLA_MEDIDA,
                CASE STOCK WHEN 0 THEN 'No' ELSE 'Si' END,
                CASE STA11.LLEVA_DOBLE_UNIDAD_MEDIDA WHEN 0 THEN 'No' ELSE 'Si' END
        """
        df_articulos = read_sql_with_retry(query_articulos, conn, label="articulos")
        if ultima_sync_articulos and not df_articulos.empty:
            try:
                df_articulos["Fecha de alta"] = pd.to_datetime(df_articulos["Fecha de alta"], errors="coerce")
                df_articulos = df_articulos[df_articulos["Fecha de alta"] >= pd.to_datetime(ultima_sync_articulos)]
                print(f"    Filtrados incrementales: {len(df_articulos)} registros")
            except Exception as e:
                print(f"    Aviso: no se pudo filtrar incremental de articulos: {e}")
        print(f"    Obtenidos: {len(df_articulos)} registros")

        conn.close()

        # ============================================================
        # ENVIAR EN LOTES
        # ============================================================
        print(f"\n  Enviando datos al API ({API_URL})...")
        
        # Saldos
        if len(df_saldo) > 0:
            ok, n = enviar_en_lotes(API_URL, "saldo", df_saldo, BATCH_SIZE)
            if ok:
                guardar_snapshot(df_saldo_full, ["Cod. Articulo", "Cod. Deposito", "Sucursal"], SALDO_SNAPSHOT, "saldos")
            print(f"    [OK] Saldos sincronizados: {n} registros")

        # Artículos (primero para poblar catálogos)
        if len(df_articulos) > 0:
            ok, n = enviar_en_lotes(API_URL, "articulos", df_articulos, BATCH_SIZE)
            print(f"    [OK] Artículos sincronizados: {n} registros")
        
        # Precios
        if len(df_precios) > 0:
            ok, n = enviar_en_lotes(API_URL, "precios", df_precios, BATCH_SIZE)
            print(f"    [OK] Precios sincronizados: {n} registros")
        
        # Costos
        if len(df_costos) > 0:
            ok, n = enviar_en_lotes(API_URL, "costos", df_costos, BATCH_SIZE)
            if ok:
                guardar_snapshot(df_costos_full, ["Cod. Articulo"], COSTO_SNAPSHOT, "costos")
            print(f"    [OK] Costos sincronizados: {n} registros")
        
        # Ventas (al final por volumen)
        if len(df_ventas) > 0:
            ok, n = enviar_en_lotes(API_URL, "ventas", df_ventas, BATCH_SIZE)
            print(f"    [OK] Ventas sincronizadas: {n} registros")
        
        # Recalcular métricas
        print(f"\n  Recalculando métricas...")
        try:
            response = requests.post(f"{API_URL}/recalcular-metricas", timeout=120, verify=SSL_VERIFY)
            if response.status_code == 200:
                print(f"    [OK] Métricas recalculadas")
            else:
                print(f"    [AVISO] Métricas: {response.status_code}")
        except Exception as e:
            print(f"    [AVISO] No se pudieron recalcular métricas: {e}")

        # Diagnóstico de calidad de datos post-sync
        try:
            q = requests.get(f"{API_URL}/quality", timeout=30, verify=SSL_VERIFY)
            if q.status_code == 200:
                quality = q.json()
                faltantes = quality.get("datasets_faltantes", [])
                print("  Calidad de datos:")
                print(f"    - Articulos: {quality.get('articulos', 0)}")
                print(f"    - Saldos: {quality.get('saldo', 0)}")
                print(f"    - Saldos historial: {quality.get('saldo_historial', 0)}")
                print(f"    - Ventas: {quality.get('ventas', 0)}")
                print(f"    - Métricas: {quality.get('metricas', 0)}")
                print(f"    - Precios: {quality.get('precios', 0)}")
                print(f"    - Costos: {quality.get('costos', 0)}")
                print(f"    - Categorias: {quality.get('categorias', 0)}")
                if faltantes:
                    print(f"    [AVISO] Datasets faltantes: {', '.join(faltantes)}")
                else:
                    print("    [OK] Cobertura completa")
        except Exception as e:
            print(f"    [AVISO] No se pudo consultar calidad post-sync: {e}")

        print(f"\n[{datetime.now()}] Sincronización completada exitosamente")

    except pyodbc.Error as db_error:
        resumen = _resumen_error_db(db_error)
        print(f"[{datetime.now()}] Error de base de datos: {resumen}")
    except requests.exceptions.RequestException as req_error:
        print(f"[{datetime.now()}] Error de conexión al API: {req_error}")
    except Exception as e:
        print(f"[{datetime.now()}] Error general: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    while True:
        try:
            if esta_en_horario_sync():
                get_data()
            else:
                print(f"[{datetime.now()}] Fuera de horario de sync. Esperando...")
        except Exception as e:
            print(f"[{datetime.now()}] Error en ciclo principal: {e}")
        time.sleep(SYNC_INTERVAL)


import os
import psycopg2

# Cargar variables de entorno desde .env si existe (opcional)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass
from psycopg2.extras import RealDictCursor, execute_values
from datetime import datetime, timedelta, date
import calendar
from typing import Optional, List

DATABASE_URL = os.environ.get("DATABASE_URL")

def _resolve_period(dias_proyeccion: int, start_date: Optional[date] = None, end_date: Optional[date] = None):
    if start_date and end_date:
        start = start_date
        end = end_date
    else:
        end = datetime.now().date()
        start = end - timedelta(days=dias_proyeccion - 1)
    dias = max((end - start).days + 1, 1)
    return start, end, dias

def _months_ago(base_date: date, months: int) -> date:
    if months <= 0:
        return base_date
    y = base_date.year
    m = base_date.month - months
    while m <= 0:
        m += 12
        y -= 1
    last_day = calendar.monthrange(y, m)[1]
    d = min(base_date.day, last_day)
    return date(y, m, d)

SUCURSALES_UNIFICAR = {
    "LA TIJERA MAYORISTA MENDOZA": "LA TIJERA MENDOZA",
    "LA TIJERA MAYORISTA SJUAN": "LA TIJERA SAN JUAN",
    "LA TIJERA MAYORISTA SAN JUAN": "LA TIJERA SAN JUAN",
}

SUCURSALES_EXCLUIR = list(SUCURSALES_UNIFICAR.keys())

def get_connection():
    return psycopg2.connect(DATABASE_URL)

def init_database():
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS saldo (
            id SERIAL PRIMARY KEY,
            cod_articulo VARCHAR(100),
            descripcion TEXT,
            sucursal VARCHAR(200),
            nro_sucursal INTEGER,
            deposito VARCHAR(200),
            cod_deposito VARCHAR(50),
            familia VARCHAR(100),
            desc_familia VARCHAR(200),
            um_stock VARCHAR(20),
            stock_1 DECIMAL(18,4) DEFAULT 0,
            sync_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(cod_articulo, cod_deposito, sucursal)
        )
    """)

    # HistÃ³rico de snapshots de stock para anÃ¡lisis KPI mensual/anual real
    cur.execute("""
        CREATE TABLE IF NOT EXISTS saldo_historial (
            id BIGSERIAL PRIMARY KEY,
            cod_articulo VARCHAR(100),
            descripcion TEXT,
            sucursal VARCHAR(200),
            nro_sucursal INTEGER,
            deposito VARCHAR(200),
            cod_deposito VARCHAR(50),
            cod_base VARCHAR(100),
            familia VARCHAR(100),
            desc_familia VARCHAR(200),
            um_stock VARCHAR(20),
            stock_1 DECIMAL(18,4) DEFAULT 0,
            snapshot_date DATE NOT NULL DEFAULT CURRENT_DATE,
            snapshot_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ventas (
            id SERIAL PRIMARY KEY,
            cod_articulo VARCHAR(100),
            descripcion TEXT,
            sucursal VARCHAR(200),
            nro_sucursal INTEGER,
            fecha DATE,
            cantidad_venta DECIMAL(18,4) DEFAULT 0,
            cantidad_venta_erp DECIMAL(18,4) DEFAULT 0,
            can_equi_v DECIMAL(18,6) DEFAULT 0,
            importe DECIMAL(18,4) DEFAULT 0,
            familia VARCHAR(100),
            desc_familia VARCHAR(200),
            um_stock VARCHAR(20),
            sync_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS metricas (
            id SERIAL PRIMARY KEY,
            cod_articulo VARCHAR(100),
            descripcion TEXT,
            sucursal VARCHAR(200),
            nro_sucursal INTEGER,
            deposito VARCHAR(200),
            familia VARCHAR(100),
            desc_familia VARCHAR(200),
            stock_1 DECIMAL(18,4) DEFAULT 0,
            total_venta DECIMAL(18,4) DEFAULT 0,
            vta_aa_analisis DECIMAL(18,4) DEFAULT 0,
            vta_aa DECIMAL(18,4) DEFAULT 0,
            vta_actual DECIMAL(18,4) DEFAULT 0,
            variacion DECIMAL(18,4) DEFAULT 0,
            variacion_pct DECIMAL(18,4) DEFAULT 0,
            necesidad DECIMAL(18,4) DEFAULT 0,
            pedido DECIMAL(18,4) DEFAULT 0,
            venta_promedio_diaria DECIMAL(18,4) DEFAULT 0,
            venta_mensual_proyectada DECIMAL(18,4) DEFAULT 0,
            meses_stock DECIMAL(18,4) DEFAULT 0,
            alerta_stock VARCHAR(50),
            sync_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_log (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            registros_saldo INTEGER DEFAULT 0,
            registros_ventas INTEGER DEFAULT 0,
            registros_metricas INTEGER DEFAULT 0,
            status VARCHAR(50),
            message TEXT
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS precios (
            id SERIAL PRIMARY KEY,
            cod_articulo VARCHAR(100),
            descripcion TEXT,
            sinonimo VARCHAR(100),
            cod_familia VARCHAR(50),
            familia VARCHAR(200),
            precio DECIMAL(18,4) DEFAULT 0,
            nro_lista VARCHAR(20),
            nombre_lista VARCHAR(200),
            fecha_modificacion DATE,
            sync_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(cod_articulo, nro_lista)
        )
    """)
    
    cur.execute("CREATE INDEX IF NOT EXISTS idx_saldo_articulo ON saldo(cod_articulo)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_saldo_sucursal ON saldo(sucursal)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_saldo_hist_ts ON saldo_historial(snapshot_ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_saldo_hist_sucursal ON saldo_historial(sucursal)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_saldo_hist_cod_art ON saldo_historial(cod_articulo)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ventas_articulo ON ventas(cod_articulo)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ventas_sucursal ON ventas(sucursal)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ventas_fecha ON ventas(fecha)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_metricas_articulo ON metricas(cod_articulo)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_metricas_sucursal ON metricas(sucursal)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_metricas_alerta ON metricas(alerta_stock)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_precios_articulo ON precios(cod_articulo)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_precios_lista ON precios(nro_lista)")
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS costos (
            id SERIAL PRIMARY KEY,
            cod_articulo VARCHAR(100),
            descripcion TEXT,
            costo_reposicion DECIMAL(18,4) DEFAULT 0,
            moneda VARCHAR(10) DEFAULT 'ARS',
            fecha_actualizacion DATE DEFAULT CURRENT_DATE,
            sync_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(cod_articulo)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_costos_articulo ON costos(cod_articulo)")

    # Tabla de artÃ­culos (nÃ³mina)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS articulos (
            id SERIAL PRIMARY KEY,
            cod_articulo VARCHAR(100) UNIQUE,
            descripcion TEXT,
            desc_adicional TEXT,
            sinonimo VARCHAR(100),
            cod_base VARCHAR(100),
            desc_base TEXT,
            familia VARCHAR(200),
            cod_agrupacion VARCHAR(100),
            desc_agrupacion TEXT,
            codigo_barra VARCHAR(100),
            fecha_alta DATE,
            um_stock VARCHAR(20),
            lleva_stock VARCHAR(5),
            doble_um VARCHAR(5),
            sync_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_articulos_cod_base ON articulos(cod_base)")

    # Extender tablas existentes si faltan columnas (compatibilidad)
    try:
        cur.execute("ALTER TABLE saldo ADD COLUMN IF NOT EXISTS cod_base VARCHAR(100)")
        cur.execute("ALTER TABLE saldo ADD COLUMN IF NOT EXISTS desc_base TEXT")
        cur.execute("ALTER TABLE saldo ADD COLUMN IF NOT EXISTS sinonimo VARCHAR(100)")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE ventas ADD COLUMN IF NOT EXISTS cod_base VARCHAR(100)")
        cur.execute("ALTER TABLE ventas ADD COLUMN IF NOT EXISTS desc_base TEXT")
        cur.execute("ALTER TABLE ventas ADD COLUMN IF NOT EXISTS sinonimo VARCHAR(100)")
        cur.execute("ALTER TABLE ventas ADD COLUMN IF NOT EXISTS cantidad_venta_erp DECIMAL(18,4) DEFAULT 0")
        cur.execute("ALTER TABLE ventas ADD COLUMN IF NOT EXISTS can_equi_v DECIMAL(18,6) DEFAULT 0")
        cur.execute("ALTER TABLE ventas ADD COLUMN IF NOT EXISTS unidad_normalizada VARCHAR(50)")
        cur.execute("ALTER TABLE ventas ADD COLUMN IF NOT EXISTS rubro_macro VARCHAR(50)")
        cur.execute("ALTER TABLE ventas ADD COLUMN IF NOT EXISTS categoria_unm VARCHAR(50)")
        cur.execute("ALTER TABLE ventas ADD COLUMN IF NOT EXISTS tipo_venta VARCHAR(50)")
        cur.execute("ALTER TABLE ventas ADD COLUMN IF NOT EXISTS sub_rubro VARCHAR(100)")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE saldo_historial ADD COLUMN IF NOT EXISTS snapshot_date DATE")
        cur.execute("UPDATE saldo_historial SET snapshot_date = COALESCE(snapshot_date, snapshot_ts::date)")
        cur.execute("ALTER TABLE saldo_historial ALTER COLUMN snapshot_date SET NOT NULL")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_saldo_hist_date ON saldo_historial(snapshot_date)")
    except Exception:
        pass

    # Tabla de categorÃ­as (si no existe)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS categorias (
            id SERIAL PRIMARY KEY,
            cod_articulo VARCHAR(100),
            categoria VARCHAR(200),
            subcategoria VARCHAR(200)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_categorias_articulo ON categorias(cod_articulo)")
    
    # Crear constraints Ãºnicos si no existen (para tablas existentes)
    try:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_saldo_unique ON saldo(cod_articulo, cod_deposito, sucursal)")
    except:
        pass
    try:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_precios_unique ON precios(cod_articulo, nro_lista)")
    except:
        pass
    try:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_ventas_unique ON ventas(cod_articulo, sucursal, fecha)")
    except:
        pass
    try:
        cur.execute("""
            WITH ranked AS (
                SELECT
                    ctid,
                    ROW_NUMBER() OVER (
                        PARTITION BY cod_articulo, cod_deposito, sucursal, snapshot_date
                        ORDER BY snapshot_ts DESC, id DESC
                    ) AS rn
                FROM saldo_historial
            )
            DELETE FROM saldo_historial s
            USING ranked r
            WHERE s.ctid = r.ctid
              AND r.rn > 1
        """)
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_saldo_hist_unique_day
            ON saldo_historial(cod_articulo, cod_deposito, sucursal, snapshot_date)
        """)
    except:
        pass
    
    conn.commit()
    cur.close()
    conn.close()

def clear_tables():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        TRUNCATE TABLE
            saldo,
            saldo_historial,
            ventas,
            metricas,
            precios,
            costos,
            articulos,
            categorias
        RESTART IDENTITY
    """)
    conn.commit()
    cur.close()
    conn.close()

def clear_metricas():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE metricas RESTART IDENTITY")
    conn.commit()
    cur.close()
    conn.close()

def get_all_saldo():
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM saldo")
    results = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(r) for r in results]

def get_all_saldos():
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM saldo")
    results = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(r) for r in results]

def get_all_ventas():
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM ventas")
    results = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(r) for r in results]

def insert_saldo(records: list, timestamp: datetime):
    if not records:
        return
    conn = get_connection()
    cur = conn.cursor()
    
    values = [
        (
            r.get("cod_articulo", ""),
            r.get("descripcion", ""),
            r.get("sinonimo", ""),
            r.get("cod_base", ""),
            r.get("desc_base", ""),
            r.get("sucursal", ""),
            r.get("nro_sucursal", 0),
            r.get("deposito", ""),
            r.get("cod_deposito", ""),
            r.get("familia", ""),
            r.get("desc_familia", ""),
            r.get("um_stock", ""),
            r.get("stock_1", 0),
            timestamp
        )
        for r in records
    ]
    
    execute_values(cur, """
        INSERT INTO saldo (cod_articulo, descripcion, sinonimo, cod_base, desc_base, sucursal, nro_sucursal, deposito, 
                          cod_deposito, familia, desc_familia, um_stock, stock_1, sync_timestamp)
        VALUES %s
    """, values)
    
    conn.commit()
    cur.close()
    conn.close()

def upsert_saldo(records: list, timestamp: datetime):
    """UPSERT saldos - actualiza si existe, inserta si no"""
    if not records:
        return 0
    conn = get_connection()
    cur = conn.cursor()

    values = [
        (
            r.get("cod_articulo", ""),
            r.get("descripcion", ""),
            r.get("sinonimo", ""),
            r.get("cod_base", ""),
            r.get("desc_base", ""),
            r.get("sucursal", ""),
            r.get("nro_sucursal", 0),
            r.get("deposito", ""),
            r.get("cod_deposito", ""),
            r.get("familia", ""),
            r.get("desc_familia", ""),
            r.get("um_stock", ""),
            r.get("stock_1", 0),
            timestamp,
        )
        for r in records
    ]

    execute_values(cur, """
        INSERT INTO saldo (
            cod_articulo, descripcion, sinonimo, cod_base, desc_base, sucursal, nro_sucursal, deposito,
            cod_deposito, familia, desc_familia, um_stock, stock_1, sync_timestamp
        )
        VALUES %s
        ON CONFLICT (cod_articulo, cod_deposito, sucursal)
        DO UPDATE SET
            descripcion = EXCLUDED.descripcion,
            sinonimo = EXCLUDED.sinonimo,
            cod_base = EXCLUDED.cod_base,
            desc_base = EXCLUDED.desc_base,
            nro_sucursal = EXCLUDED.nro_sucursal,
            deposito = EXCLUDED.deposito,
            familia = EXCLUDED.familia,
            desc_familia = EXCLUDED.desc_familia,
            um_stock = EXCLUDED.um_stock,
            stock_1 = EXCLUDED.stock_1,
            sync_timestamp = EXCLUDED.sync_timestamp
    """, values, page_size=2000)

    conn.commit()
    cur.close()
    conn.close()
    return len(values)

def insert_saldo_historial_snapshot(records: list, timestamp: datetime):
    """
    Inserta snapshot histÃ³rico de saldo para evoluciÃ³n temporal.
    No hace UPSERT para conservar la traza de cada corrida.
    """
    if not records:
        return 0

    conn = get_connection()
    cur = conn.cursor()
    snapshot_date = timestamp.date()
    values = [
        (
            r.get("cod_articulo", ""),
            r.get("descripcion", ""),
            r.get("sucursal", ""),
            r.get("nro_sucursal", 0),
            r.get("deposito", ""),
            r.get("cod_deposito", ""),
            r.get("cod_base", ""),
            r.get("familia", ""),
            r.get("desc_familia", ""),
            r.get("um_stock", ""),
            r.get("stock_1", 0),
            snapshot_date,
            timestamp,
        )
        for r in records
    ]

    execute_values(cur, """
        INSERT INTO saldo_historial (
            cod_articulo, descripcion, sucursal, nro_sucursal, deposito,
            cod_deposito, cod_base, familia, desc_familia, um_stock, stock_1, snapshot_date, snapshot_ts
        )
        VALUES %s
        ON CONFLICT (cod_articulo, cod_deposito, sucursal, snapshot_date)
        DO UPDATE SET
            descripcion = EXCLUDED.descripcion,
            nro_sucursal = EXCLUDED.nro_sucursal,
            deposito = EXCLUDED.deposito,
            cod_base = EXCLUDED.cod_base,
            familia = EXCLUDED.familia,
            desc_familia = EXCLUDED.desc_familia,
            um_stock = EXCLUDED.um_stock,
            stock_1 = EXCLUDED.stock_1,
            snapshot_ts = EXCLUDED.snapshot_ts
    """, values, page_size=2000)
    count = len(values)
    conn.commit()
    cur.close()
    conn.close()
    return count

def insert_ventas(records: list, timestamp: datetime):
    if not records:
        return
    conn = get_connection()
    cur = conn.cursor()
    
    values = [
        (
            r.get("cod_articulo", ""),
            r.get("descripcion", ""),
            r.get("sinonimo", ""),
            r.get("cod_base", ""),
            r.get("desc_base", ""),
            r.get("sucursal", ""),
            r.get("nro_sucursal", 0),
            r.get("fecha"),
            r.get("cantidad_venta", 0),
            r.get("cantidad_venta_erp", 0),
            r.get("can_equi_v", 0),
            r.get("importe", 0),
            r.get("familia", ""),
            r.get("desc_familia", ""),
            r.get("um_stock", ""),
            r.get("unidad_normalizada", ""),
            r.get("rubro_macro", ""),
            r.get("categoria_unm", ""),
            r.get("tipo_venta", ""),
            r.get("sub_rubro", ""),
            timestamp
        )
        for r in records
    ]
    
    execute_values(cur, """
        INSERT INTO ventas (cod_articulo, descripcion, sinonimo, cod_base, desc_base, sucursal, nro_sucursal, fecha,
                           cantidad_venta, cantidad_venta_erp, can_equi_v, importe, familia, desc_familia, um_stock,
                           unidad_normalizada, rubro_macro, categoria_unm, tipo_venta, sub_rubro, sync_timestamp)
        VALUES %s
    """, values)
    
    conn.commit()
    cur.close()
    conn.close()

def upsert_ventas(records: list, timestamp: datetime):
    """UPSERT ventas - actualiza si existe, inserta si no (por articulo+sucursal+fecha)"""
    if not records:
        return 0
    conn = get_connection()
    cur = conn.cursor()

    values = [
        (
            r.get("cod_articulo", ""),
            r.get("descripcion", ""),
            r.get("sinonimo", ""),
            r.get("cod_base", ""),
            r.get("desc_base", ""),
            r.get("sucursal", ""),
            r.get("nro_sucursal", 0),
            r.get("fecha"),
            r.get("cantidad_venta", 0),
            r.get("cantidad_venta_erp", 0),
            r.get("can_equi_v", 0),
            r.get("importe", 0),
            r.get("familia", ""),
            r.get("desc_familia", ""),
            r.get("um_stock", ""),
            r.get("unidad_normalizada", ""),
            r.get("rubro_macro", ""),
            r.get("categoria_unm", ""),
            r.get("tipo_venta", ""),
            r.get("sub_rubro", ""),
            timestamp,
        )
        for r in records
    ]

    execute_values(cur, """
        INSERT INTO ventas (
            cod_articulo, descripcion, sinonimo, cod_base, desc_base, sucursal, nro_sucursal, fecha,
            cantidad_venta, cantidad_venta_erp, can_equi_v, importe, familia, desc_familia, um_stock,
            unidad_normalizada, rubro_macro, categoria_unm, tipo_venta, sub_rubro, sync_timestamp
        )
        VALUES %s
        ON CONFLICT (cod_articulo, sucursal, fecha)
        DO UPDATE SET
            descripcion = EXCLUDED.descripcion,
            sinonimo = EXCLUDED.sinonimo,
            cod_base = EXCLUDED.cod_base,
            desc_base = EXCLUDED.desc_base,
            nro_sucursal = EXCLUDED.nro_sucursal,
            cantidad_venta = EXCLUDED.cantidad_venta,
            cantidad_venta_erp = EXCLUDED.cantidad_venta_erp,
            can_equi_v = EXCLUDED.can_equi_v,
            importe = EXCLUDED.importe,
            familia = EXCLUDED.familia,
            desc_familia = EXCLUDED.desc_familia,
            um_stock = EXCLUDED.um_stock,
            unidad_normalizada = EXCLUDED.unidad_normalizada,
            rubro_macro = EXCLUDED.rubro_macro,
            categoria_unm = EXCLUDED.categoria_unm,
            tipo_venta = EXCLUDED.tipo_venta,
            sub_rubro = EXCLUDED.sub_rubro,
            sync_timestamp = EXCLUDED.sync_timestamp
    """, values, page_size=2000)

    conn.commit()
    cur.close()
    conn.close()
    return len(values)

def get_ultima_fecha_ventas():
    """Obtener la Ãºltima fecha de ventas sincronizada"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT MAX(fecha) FROM ventas")
    result = cur.fetchone()
    cur.close()
    conn.close()
    return result[0] if result and result[0] else None

def insert_metricas(records: list, timestamp: datetime):
    if not records:
        return
    conn = get_connection()
    cur = conn.cursor()
    
    values = [
        (
            r.get("cod_articulo", ""),
            r.get("descripcion", ""),
            r.get("sucursal", ""),
            r.get("nro_sucursal", 0),
            r.get("deposito", ""),
            r.get("familia", ""),
            r.get("desc_familia", ""),
            r.get("stock_1", 0),
            r.get("total_venta", 0),
            r.get("vta_aa_analisis", 0),
            r.get("vta_aa", 0),
            r.get("vta_actual", 0),
            r.get("variacion", 0),
            r.get("variacion_pct", 0),
            r.get("necesidad", 0),
            r.get("pedido", 0),
            r.get("venta_promedio_diaria", 0),
            r.get("venta_mensual_proyectada", 0),
            r.get("meses_stock", 0),
            r.get("alerta_stock", ""),
            timestamp
        )
        for r in records
    ]
    
    execute_values(cur, """
        INSERT INTO metricas (cod_articulo, descripcion, sucursal, nro_sucursal, deposito,
                             familia, desc_familia, stock_1, total_venta, 
                             vta_aa_analisis, vta_aa, vta_actual, variacion, variacion_pct, necesidad, pedido,
                             venta_promedio_diaria, venta_mensual_proyectada, meses_stock, alerta_stock, sync_timestamp)
        VALUES %s
    """, values)
    
    conn.commit()
    cur.close()
    conn.close()

def insert_precios(records: list, timestamp: datetime):
    if not records:
        return
    conn = get_connection()
    cur = conn.cursor()
    
    values = [
        (
            r.get("cod_articulo", ""),
            r.get("descripcion", ""),
            r.get("sinonimo", ""),
            r.get("cod_familia", ""),
            r.get("familia", ""),
            r.get("precio", 0),
            r.get("nro_lista", ""),
            r.get("nombre_lista", ""),
            r.get("fecha_modificacion"),
            timestamp
        )
        for r in records
    ]
    
    execute_values(cur, """
        INSERT INTO precios (cod_articulo, descripcion, sinonimo, cod_familia, familia,
                            precio, nro_lista, nombre_lista, fecha_modificacion, sync_timestamp)
        VALUES %s
    """, values)
    
    conn.commit()
    cur.close()
    conn.close()

def upsert_precios(records: list, timestamp: datetime):
    """UPSERT precios - actualiza si existe, inserta si no"""
    if not records:
        return 0
    conn = get_connection()
    cur = conn.cursor()
    
    updated = 0
    for r in records:
        cur.execute("""
            INSERT INTO precios (cod_articulo, descripcion, sinonimo, cod_familia, familia,
                                precio, nro_lista, nombre_lista, fecha_modificacion, sync_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (cod_articulo, nro_lista) 
            DO UPDATE SET 
                descripcion = EXCLUDED.descripcion,
                sinonimo = EXCLUDED.sinonimo,
                cod_familia = EXCLUDED.cod_familia,
                familia = EXCLUDED.familia,
                precio = EXCLUDED.precio,
                nombre_lista = EXCLUDED.nombre_lista,
                fecha_modificacion = EXCLUDED.fecha_modificacion,
                sync_timestamp = EXCLUDED.sync_timestamp
        """, (
            r.get("cod_articulo", ""),
            r.get("descripcion", ""),
            r.get("sinonimo", ""),
            r.get("cod_familia", ""),
            r.get("familia", ""),
            r.get("precio", 0),
            r.get("nro_lista", ""),
            r.get("nombre_lista", ""),
            r.get("fecha_modificacion"),
            timestamp
        ))
        updated += 1
    
    conn.commit()
    cur.close()
    conn.close()
    return updated

def get_sync_info():
    """Obtener info para sincronizaciÃ³n incremental"""
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT 
            (SELECT COUNT(*) FROM saldo) as total_saldos,
            (SELECT COUNT(*) FROM saldo_historial) as total_saldos_historial,
            (SELECT COUNT(*) FROM ventas) as total_ventas,
            (SELECT COUNT(*) FROM precios) as total_precios,
            (SELECT COUNT(*) FROM costos) as total_costos,
            (SELECT COUNT(*) FROM articulos) as total_articulos,
            (SELECT COUNT(*) FROM categorias) as total_categorias,
            (SELECT MAX(fecha) FROM ventas) as ultima_fecha_ventas,
            (SELECT MAX(sync_timestamp) FROM ventas) as ultima_sync_ventas,
            (SELECT MAX(sync_timestamp) FROM saldo) as ultima_sync_saldo,
            (SELECT MAX(snapshot_ts) FROM saldo_historial) as ultima_sync_saldo_historial,
            (SELECT MAX(sync_timestamp) FROM precios) as ultima_sync_precios,
            (SELECT MAX(sync_timestamp) FROM costos) as ultima_sync_costos,
            (SELECT MAX(sync_timestamp) FROM articulos) as ultima_sync_articulos
    """)
    result = cur.fetchone()
    cur.close()
    conn.close()
    return dict(result) if result else {}

def log_sync(registros_saldo: int, registros_ventas: int, registros_metricas: int, status: str, message: str = "", registros_precios: int = 0):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO sync_log (registros_saldo, registros_ventas, registros_metricas, status, message)
        VALUES (%s, %s, %s, %s, %s)
    """, (registros_saldo, registros_ventas, registros_metricas, status, f"{message} | Precios: {registros_precios}"))
    conn.commit()
    cur.close()
    conn.close()

def get_metricas(sucursal: Optional[str] = None, alerta: Optional[str] = None):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    query = "SELECT * FROM metricas WHERE 1=1"
    params = []
    
    if sucursal:
        sucursales_incluir = [sucursal]
        for excluida, principal in SUCURSALES_UNIFICAR.items():
            if sucursal == principal:
                sucursales_incluir.append(excluida)
        
        placeholders = ','.join(['%s'] * len(sucursales_incluir))
        query += f" AND sucursal IN ({placeholders})"
        params.extend(sucursales_incluir)
    
    if alerta:
        # aceptar labels antiguos y nuevos
        if alerta in ("âš ï¸ Quiebre de stock", "Quiebre de stock"):
            query += " AND alerta_stock IN (%s, %s)"
            params.extend(["âš ï¸ Quiebre de stock", "Quiebre de stock"])
        elif alerta in ("â— Stock de Seguridad", "Stock de Seguridad"):
            query += " AND alerta_stock IN (%s, %s)"
            params.extend(["â— Stock de Seguridad", "Stock de Seguridad"])
        elif alerta in ("ðŸ“ Pto de Pedido", "Pto de Pedido"):
            query += " AND alerta_stock IN (%s, %s)"
            params.extend(["ðŸ“ Pto de Pedido", "Pto de Pedido"])
        elif alerta in ("âœ… OK", "OK"):
            query += " AND alerta_stock IN (%s, %s)"
            params.extend(["âœ… OK", "OK"])
        elif alerta in ("ðŸ“¦ Sobrestock", "Sobre stock", "Sobrestock"):
            query += " AND alerta_stock IN (%s, %s, %s)"
            params.extend(["ðŸ“¦ Sobrestock", "Sobre stock", "Sobrestock"])
        elif alerta in ("ðŸŸ  Sin rotaciÃ³n", "Sin rotaciÃ³n (sin stock)", "Sin rotaciÃ³n (con sobrestock)"):
            query += " AND alerta_stock IN (%s, %s, %s)"
            params.extend(["ðŸŸ  Sin rotaciÃ³n", "Sin rotaciÃ³n (sin stock)", "Sin rotaciÃ³n (con sobrestock)"])
        else:
            query += " AND alerta_stock = %s"
            params.append(alerta)
    
    query += " ORDER BY cod_articulo, sucursal"
    
    cur.execute(query, params)
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    
    datos = []
    for r in results:
        d = dict(r)
        if d.get("sucursal") in SUCURSALES_UNIFICAR:
            d["sucursal"] = SUCURSALES_UNIFICAR[d["sucursal"]]
        datos.append(d)
    
    return datos

def get_sucursales():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT sucursal FROM metricas ORDER BY sucursal")
    results = [r[0] for r in cur.fetchall() if r[0] not in SUCURSALES_EXCLUIR]
    if not results:
        cur.execute("SELECT DISTINCT sucursal FROM saldo ORDER BY sucursal")
        results = [r[0] for r in cur.fetchall() if r[0] not in SUCURSALES_EXCLUIR]
    if not results:
        cur.execute("SELECT DISTINCT sucursal FROM ventas ORDER BY sucursal")
        results = [r[0] for r in cur.fetchall() if r[0] not in SUCURSALES_EXCLUIR]
    cur.close()
    conn.close()
    return results

def get_alertas_count(sucursal: Optional[str] = None):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    if sucursal:
        cur.execute("""
            SELECT alerta_stock, COUNT(*) as count 
            FROM metricas 
            WHERE sucursal = %s
            GROUP BY alerta_stock
        """, (sucursal,))
    else:
        cur.execute("""
            SELECT alerta_stock, COUNT(*) as count 
            FROM metricas 
            GROUP BY alerta_stock
        """)
    results = {r["alerta_stock"]: r["count"] for r in cur.fetchall()}
    cur.close()
    conn.close()
    return results

def get_totales(sucursal: Optional[str] = None):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    query = """
        SELECT 
            COUNT(*) as total_articulos,
            COALESCE(SUM(stock_1), 0) as stock_total,
            COALESCE(SUM(total_venta), 0) as venta_total,
            COALESCE(AVG(CASE WHEN meses_stock < 999 THEN meses_stock END), 0) as meses_stock_promedio,
            COALESCE(SUM(CASE WHEN alerta_stock = 'Quiebre' THEN 1 ELSE 0 END), 0) as quiebres
        FROM metricas
    """
    
    params = []
    if sucursal:
        query += " WHERE sucursal = %s"
        params.append(sucursal)
    
    cur.execute(query, params)
    row = cur.fetchone()
    result = dict(row) if row else {"total_articulos": 0, "stock_total": 0, "venta_total": 0, "meses_stock_promedio": 0, "quiebres": 0}
    
    cur.close()
    conn.close()
    return result

def get_last_sync():
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM sync_log ORDER BY timestamp DESC LIMIT 1")
    result = cur.fetchone()
    cur.close()
    conn.close()
    return dict(result) if result else None

def get_ventas_articulo(cod_articulo: str, sucursal: Optional[str] = None):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    query = "SELECT * FROM ventas WHERE cod_articulo = %s"
    params = [cod_articulo]
    
    if sucursal:
        query += " AND sucursal = %s"
        params.append(sucursal)
    
    query += " ORDER BY fecha"
    
    cur.execute(query, params)
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return [dict(r) for r in results]

def get_precios(cod_articulo: Optional[str] = None, nro_lista: Optional[str] = None):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    query = "SELECT * FROM precios WHERE 1=1"
    params = []
    
    if cod_articulo:
        query += " AND cod_articulo = %s"
        params.append(cod_articulo)
    
    if nro_lista:
        query += " AND nro_lista = %s"
        params.append(nro_lista)
    
    query += " ORDER BY cod_articulo, nro_lista"
    
    cur.execute(query, params)
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return [dict(r) for r in results]

def get_listas_precios():
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT DISTINCT nro_lista, nombre_lista, COUNT(*) as articulos
        FROM precios 
        GROUP BY nro_lista, nombre_lista
        ORDER BY nro_lista
    """)
    results = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(r) for r in results]

def get_precio_articulo(cod_articulo: str):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT * FROM precios 
        WHERE cod_articulo = %s 
        ORDER BY nro_lista
    """, (cod_articulo,))
    results = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(r) for r in results]

def get_familias():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT familia, desc_familia
        FROM (
            SELECT UPPER(TRIM(familia)) AS familia, desc_familia FROM saldo
            UNION
            SELECT UPPER(TRIM(familia)) AS familia, desc_familia FROM metricas
            UNION
            SELECT UPPER(TRIM(familia)) AS familia, NULL AS desc_familia FROM articulos
        ) t
        WHERE familia IS NOT NULL AND familia != ''
        ORDER BY familia
    """)
    results = [(r[0], r[1]) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return results

def get_articulos_unicos():
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT DISTINCT cod_articulo, descripcion, familia 
        FROM metricas 
        ORDER BY cod_articulo
    """)
    results = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(r) for r in results]

def get_matriz_distribucion(
    dias_proyeccion: int = 30,
    familias: Optional[List] = None,
    alertas: Optional[List] = None,
    sucursales: Optional[List] = None,
    prefijos_familia: Optional[List] = None,
    codigos_prefix: Optional[List] = None,
    codigos_contains: Optional[List] = None,
    solo_nuevos: bool = False,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    start_date, end_date, dias_periodo = _resolve_period(dias_proyeccion, start_date, end_date)
    nuevo_cutoff = _months_ago(datetime.now().date(), 6)

    query = """
        WITH ventas_p AS (
            SELECT
                cod_articulo,
                sucursal,
                SUM(cantidad_venta) AS ventas_periodo_stock,
                SUM(COALESCE(NULLIF(cantidad_venta_erp, 0), cantidad_venta)) AS ventas_periodo_erp
            FROM ventas
            WHERE fecha >= %s AND fecha <= %s
            GROUP BY 1,2
        ),
        stock_s AS (
            SELECT cod_articulo, sucursal,
                   SUM(stock_1) AS stock_sucursal,
                   MAX(cod_base) AS cod_base,
                   MAX(familia) AS familia,
                   MAX(descripcion) AS descripcion,
                   MAX(sinonimo) AS sinonimo
            FROM saldo
            GROUP BY 1,2
        ),
        base AS (
            SELECT cod_articulo, sucursal FROM stock_s
            UNION
            SELECT cod_articulo, sucursal FROM ventas_p
        ),
        cdd AS (
            SELECT
                COALESCE(cod_base, cod_articulo) AS cod_base,
                cod_articulo,
                SUM(stock_1) AS stock_cdd
            FROM saldo
            WHERE sucursal = 'CRISA CENTRAL'
              AND (
                RIGHT(COALESCE(cod_deposito, ''), 2) IN ('01','30')
                OR RIGHT(COALESCE(deposito, ''), 2) IN ('01','30')
            )
            GROUP BY 1,2
        ),
        art_norm AS (
            SELECT
                cod_articulo,
                cod_base,
                descripcion,
                sinonimo,
                fecha_alta,
                REPLACE(UPPER(cod_articulo), ' ', '') AS cod_norm
            FROM articulos
        ),
        calc AS (
            SELECT
                COALESCE(a.cod_base, s.cod_base, b.cod_articulo) AS cod_base,
                b.cod_articulo,
                b.sucursal,
                COALESCE(s.stock_sucursal, 0) AS stock_sucursal,
                COALESCE(v.ventas_periodo_stock, 0) AS ventas_periodo_stock,
                COALESCE(v.ventas_periodo_erp, 0) AS ventas_periodo_erp,
                COALESCE(v.ventas_periodo_stock, 0) / %s AS venta_promedio_diaria,
                CASE
                    WHEN COALESCE(v.ventas_periodo_stock, 0) = 0 THEN 0
                    ELSE COALESCE(s.stock_sucursal, 0) / NULLIF((COALESCE(v.ventas_periodo_stock, 0) / %s) * 30, 0)
                END AS meses_stock,
                (COALESCE(v.ventas_periodo_stock, 0) - COALESCE(s.stock_sucursal, 0)) AS necesidad,
                CASE
                    WHEN COALESCE(v.ventas_periodo_stock, 0) = 0 THEN 'Sin rotaciÃ³n'
                    WHEN (COALESCE(s.stock_sucursal, 0) / NULLIF((COALESCE(v.ventas_periodo_stock, 0) / %s) * 30, 0)) < 1 THEN 'Quiebre de stock'
                    WHEN (COALESCE(s.stock_sucursal, 0) / NULLIF((COALESCE(v.ventas_periodo_stock, 0) / %s) * 30, 0)) >= 1
                         AND (COALESCE(s.stock_sucursal, 0) / NULLIF((COALESCE(v.ventas_periodo_stock, 0) / %s) * 30, 0)) < 2 THEN 'Stock de Seguridad'
                    WHEN (COALESCE(s.stock_sucursal, 0) / NULLIF((COALESCE(v.ventas_periodo_stock, 0) / %s) * 30, 0)) >= 2
                         AND (COALESCE(s.stock_sucursal, 0) / NULLIF((COALESCE(v.ventas_periodo_stock, 0) / %s) * 30, 0)) < 3 THEN 'Pto de Pedido'
                    WHEN (COALESCE(s.stock_sucursal, 0) / NULLIF((COALESCE(v.ventas_periodo_stock, 0) / %s) * 30, 0)) >= 3
                         AND (COALESCE(s.stock_sucursal, 0) / NULLIF((COALESCE(v.ventas_periodo_stock, 0) / %s) * 30, 0)) < 4 THEN 'OK'
                    WHEN (COALESCE(s.stock_sucursal, 0) / NULLIF((COALESCE(v.ventas_periodo_stock, 0) / %s) * 30, 0)) >= 4 THEN 'Sobrestock'
                    ELSE 'OK'
                END AS alerta_stock,
                COALESCE(cdd.stock_cdd, 0) AS stock_cdd,
                COALESCE(s.familia, '') AS familia,
                COALESCE(s.descripcion, a.descripcion, '') AS descripcion,
                COALESCE(s.sinonimo, a.sinonimo, '') AS sinonimo,
                CASE
                    WHEN a.fecha_alta IS NOT NULL AND a.fecha_alta >= %s THEN 1
                    ELSE 0
                END AS is_nuevo
            FROM base b
            LEFT JOIN stock_s s ON s.cod_articulo = b.cod_articulo AND s.sucursal = b.sucursal
            LEFT JOIN ventas_p v ON v.cod_articulo = b.cod_articulo AND v.sucursal = b.sucursal
            LEFT JOIN art_norm a ON a.cod_norm = REPLACE(UPPER(b.cod_articulo), ' ', '')
            LEFT JOIN cdd ON cdd.cod_articulo = b.cod_articulo
        )
        SELECT * FROM calc WHERE 1=1
    """

    params = [
        start_date,
        end_date,
        dias_periodo,
        dias_periodo,
        dias_periodo,
        dias_periodo,
        dias_periodo,
        dias_periodo,
        dias_periodo,
        dias_periodo,
        dias_periodo,
        dias_periodo,
        nuevo_cutoff,
    ]

    if alertas and len(alertas) > 0:
        placeholders = ','.join(['%s'] * len(alertas))
        query += f" AND alerta_stock IN ({placeholders})"
        params.extend(alertas)

    if sucursales and len(sucursales) > 0:
        sucursales_incluir = set()
        for suc in sucursales:
            sucursales_incluir.add(suc)
            for excluida, principal in SUCURSALES_UNIFICAR.items():
                if suc == principal:
                    sucursales_incluir.add(excluida)
        placeholders = ','.join(['%s'] * len(sucursales_incluir))
        query += f" AND sucursal IN ({placeholders})"
        params.extend(list(sucursales_incluir))

    if prefijos_familia and len(prefijos_familia) > 0:
        placeholders = ','.join(['%s'] * len(prefijos_familia))
        query += f" AND LEFT(UPPER(cod_articulo), 2) IN ({placeholders})"
        params.extend([str(x).strip().upper() for x in prefijos_familia if str(x).strip()])

    cod_filters = []
    if codigos_prefix and len(codigos_prefix) > 0:
        for p in codigos_prefix:
            p = str(p).strip().upper()
            if not p:
                continue
            cod_filters.append("(UPPER(cod_articulo) LIKE %s OR UPPER(cod_base) LIKE %s OR UPPER(descripcion) LIKE %s OR UPPER(sinonimo) LIKE %s)")
            params.extend([f"{p}%", f"{p}%", f"{p}%", f"{p}%"])

    if codigos_contains and len(codigos_contains) > 0:
        for c in codigos_contains:
            c = str(c).strip().upper()
            if not c:
                continue
            cod_filters.append("(UPPER(cod_articulo) LIKE %s OR UPPER(cod_base) LIKE %s OR UPPER(descripcion) LIKE %s OR UPPER(sinonimo) LIKE %s)")
            params.extend([f"%{c}%", f"%{c}%", f"%{c}%", f"%{c}%"])

    if cod_filters:
        query += " AND (" + " OR ".join(cod_filters) + ")"

    if solo_nuevos:
        query += " AND is_nuevo = 1"

    query += " ORDER BY cod_base, cod_articulo, sucursal"

    cur.execute(query, params)
    results = cur.fetchall()
    cur.close()
    conn.close()
    
    datos = []
    for r in results:
        d = dict(r)
        if d.get("sucursal") in SUCURSALES_UNIFICAR:
            d["sucursal"] = SUCURSALES_UNIFICAR[d["sucursal"]]
        datos.append(d)
    
    return datos

def get_sugerencia_distribucion(
    dias_proyeccion: int = 30,
    familias: Optional[List] = None,
    limit: int = 200,
    sucursales: Optional[List] = None,
    prefijos_familia: Optional[List] = None,
    codigos_prefix: Optional[List] = None,
    codigos_contains: Optional[List] = None,
    alertas: Optional[List] = None,
    solo_sugeridos: Optional[bool] = True,
    lista_precio: Optional[str] = None,
    solo_nuevos: bool = False,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    start_date, end_date, dias_periodo = _resolve_period(dias_proyeccion, start_date, end_date)
    nuevo_cutoff = _months_ago(datetime.now().date(), 6)

    price_filter = ""
    price_params: List = []
    if lista_precio:
        price_filter = "WHERE nro_lista = %s OR nombre_lista ILIKE %s"
        price_params = [lista_precio, f"%{lista_precio}%"]

    sucursal_case = """
        CASE
            WHEN UPPER(TRIM(sucursal)) = 'LA TIJERA MAYORISTA MENDOZA' THEN 'LA TIJERA MENDOZA'
            WHEN UPPER(TRIM(sucursal)) IN (
                'LA TIJERA MAYORISTA SJUAN',
                'LA TIJERA MAYORISTA SAN JUAN',
                'LA TIJERA MAYORISTA SANJUAN'
            ) THEN 'LA TIJERA SAN JUAN'
            ELSE TRIM(sucursal)
        END
    """

    query = f"""
        WITH ventas_p AS (
            SELECT
                cod_articulo,
                {sucursal_case} AS sucursal,
                SUM(cantidad_venta) AS ventas_periodo_stock,
                SUM(COALESCE(NULLIF(cantidad_venta_erp, 0), cantidad_venta)) AS ventas_periodo_erp
            FROM ventas
            WHERE fecha >= %s AND fecha <= %s
            GROUP BY 1,2
        ),
        stock_s AS (
            SELECT cod_articulo, {sucursal_case} AS sucursal,
                   SUM(stock_1) AS stock_sucursal,
                   MAX(familia) AS familia,
                   MAX(desc_familia) AS desc_familia,
                   MAX(descripcion) AS descripcion,
                   MAX(sinonimo) AS sinonimo
            FROM saldo
            GROUP BY 1,2
        ),
        base AS (
            SELECT cod_articulo, sucursal FROM stock_s
            UNION
            SELECT cod_articulo, sucursal FROM ventas_p
        ),
        cdd AS (
            SELECT cod_articulo, SUM(stock_1) AS stock_cdd
            FROM saldo
            WHERE sucursal = 'CRISA CENTRAL'
              AND (
                RIGHT(COALESCE(cod_deposito, ''), 2) IN ('01','30')
                OR RIGHT(COALESCE(deposito, ''), 2) IN ('01','30')
              )
            GROUP BY 1
        ),
        precio_u AS (
            SELECT DISTINCT ON (cod_articulo)
                cod_articulo,
                precio,
                nro_lista,
                nombre_lista,
                fecha_modificacion,
                sync_timestamp
            FROM precios
            {price_filter}
            ORDER BY cod_articulo, fecha_modificacion DESC NULLS LAST, sync_timestamp DESC
        ),
        costo_u AS (
            SELECT cod_articulo, costo_reposicion
            FROM costos
        ),
        art_norm AS (
            SELECT
                cod_articulo,
                cod_base,
                descripcion,
                sinonimo,
                fecha_alta,
                REPLACE(UPPER(cod_articulo), ' ', '') AS cod_norm
            FROM articulos
        ),
        calc AS (
            SELECT
                COALESCE(a.cod_base, b.cod_articulo) AS cod_base,
                b.sucursal,
                b.cod_articulo,
                COALESCE(s.stock_sucursal, 0) AS stock_sucursal,
                COALESCE(v.ventas_periodo_stock, 0) AS ventas_periodo_stock,
                COALESCE(v.ventas_periodo_erp, 0) AS ventas_periodo_erp,
                COALESCE(v.ventas_periodo_stock, 0) / %s AS venta_promedio_diaria,
                CASE
                    WHEN COALESCE(v.ventas_periodo_stock, 0) = 0 THEN 0
                    ELSE COALESCE(s.stock_sucursal, 0) / NULLIF((COALESCE(v.ventas_periodo_stock, 0) / %s) * 30, 0)
                END AS meses_stock,
                (COALESCE(v.ventas_periodo_stock, 0) - COALESCE(s.stock_sucursal, 0)) AS necesidad,
                CASE
                    WHEN COALESCE(v.ventas_periodo_stock, 0) = 0 THEN 'Sin rotaciÃ³n'
                    WHEN (COALESCE(s.stock_sucursal, 0) / NULLIF((COALESCE(v.ventas_periodo_stock, 0) / %s) * 30, 0)) < 1 THEN 'Quiebre de stock'
                    WHEN (COALESCE(s.stock_sucursal, 0) / NULLIF((COALESCE(v.ventas_periodo_stock, 0) / %s) * 30, 0)) >= 1
                         AND (COALESCE(s.stock_sucursal, 0) / NULLIF((COALESCE(v.ventas_periodo_stock, 0) / %s) * 30, 0)) < 2 THEN 'Stock de Seguridad'
                    WHEN (COALESCE(s.stock_sucursal, 0) / NULLIF((COALESCE(v.ventas_periodo_stock, 0) / %s) * 30, 0)) >= 2
                         AND (COALESCE(s.stock_sucursal, 0) / NULLIF((COALESCE(v.ventas_periodo_stock, 0) / %s) * 30, 0)) < 3 THEN 'Pto de Pedido'
                    WHEN (COALESCE(s.stock_sucursal, 0) / NULLIF((COALESCE(v.ventas_periodo_stock, 0) / %s) * 30, 0)) >= 3
                         AND (COALESCE(s.stock_sucursal, 0) / NULLIF((COALESCE(v.ventas_periodo_stock, 0) / %s) * 30, 0)) < 4 THEN 'OK'
                    WHEN (COALESCE(s.stock_sucursal, 0) / NULLIF((COALESCE(v.ventas_periodo_stock, 0) / %s) * 30, 0)) >= 4 THEN 'Sobrestock'
                    ELSE 'OK'
                END AS alerta_stock,
                COALESCE(cdd.stock_cdd, 0) AS stock_cdd,
                COALESCE(p.precio, 0) AS precio_unitario,
                COALESCE(c.costo_reposicion, 0) AS costo_unitario,
                COALESCE(s.familia, '') AS familia,
                COALESCE(s.descripcion, a.descripcion, '') AS descripcion,
                COALESCE(s.sinonimo, a.sinonimo, '') AS sinonimo,
                CASE
                    WHEN a.fecha_alta IS NOT NULL AND a.fecha_alta >= %s THEN 1
                    ELSE 0
                END AS is_nuevo
            FROM base b
            LEFT JOIN stock_s s ON s.cod_articulo = b.cod_articulo AND s.sucursal = b.sucursal
            LEFT JOIN ventas_p v ON v.cod_articulo = b.cod_articulo AND v.sucursal = b.sucursal
            LEFT JOIN art_norm a ON a.cod_norm = REPLACE(UPPER(b.cod_articulo), ' ', '')
            LEFT JOIN cdd ON cdd.cod_articulo = b.cod_articulo
            LEFT JOIN precio_u p ON p.cod_articulo = b.cod_articulo
            LEFT JOIN costo_u c ON c.cod_articulo = b.cod_articulo
        )
        SELECT
            sucursal,
            cod_base,
            cod_articulo,
            is_nuevo,
            stock_sucursal,
            stock_cdd,
            ventas_periodo_erp AS ventas_periodo,
            venta_promedio_diaria,
            meses_stock,
            CASE
                WHEN venta_promedio_diaria = 0 THEN NULL
                ELSE stock_sucursal / NULLIF(venta_promedio_diaria, 0)
            END AS cobertura_dias,
            alerta_stock,
            CASE
                WHEN ventas_periodo_stock = 0 THEN 'Sin rotacion'
                WHEN meses_stock < 1 THEN 'Critica'
                WHEN meses_stock < 2 THEN 'Alta'
                WHEN meses_stock < 3 THEN 'Media'
                WHEN meses_stock < 4 THEN 'OK'
                ELSE 'Sobrestock'
            END AS prioridad,
            precio_unitario,
            costo_unitario,
            necesidad,
            GREATEST(necesidad, 0) AS sugerencia_distribuir,
            (GREATEST(necesidad, 0) * precio_unitario) AS valor_reponer_venta,
            (GREATEST(necesidad, 0) * costo_unitario) AS valor_reponer_costo,
            (GREATEST(necesidad, 0) * precio_unitario) - (GREATEST(necesidad, 0) * costo_unitario) AS margen_estimado
        FROM calc
        WHERE 1=1
    """

    params: List = [start_date, end_date]
    if price_params:
        params.extend(price_params)
    params.extend([
        dias_periodo,
        dias_periodo,
        dias_periodo,
        dias_periodo,
        dias_periodo,
        dias_periodo,
        dias_periodo,
        dias_periodo,
        dias_periodo,
        dias_periodo,
        nuevo_cutoff,
    ])

    if alertas and len(alertas) > 0:
        placeholders = ','.join(['%s'] * len(alertas))
        query += f" AND alerta_stock IN ({placeholders})"
        params.extend(alertas)

    if sucursales and len(sucursales) > 0:
        sucursales_norm = []
        for s in sucursales:
            s = SUCURSALES_UNIFICAR.get(s, s)
            if s not in sucursales_norm:
                sucursales_norm.append(s)
        placeholders = ','.join(['%s'] * len(sucursales_norm))
        query += f" AND sucursal IN ({placeholders})"
        params.extend(list(sucursales_norm))

    familias_uso = prefijos_familia if prefijos_familia else familias
    if familias_uso and len(familias_uso) > 0:
        placeholders = ','.join(['%s'] * len(familias_uso))
        query += f" AND LEFT(UPPER(cod_articulo), 2) IN ({placeholders})"
        params.extend([str(x).strip().upper() for x in familias_uso if str(x).strip()])

    cod_filters = []
    if codigos_prefix and len(codigos_prefix) > 0:
        for p in codigos_prefix:
            p = str(p).strip().upper()
            if not p:
                continue
            cod_filters.append("(UPPER(cod_articulo) LIKE %s OR UPPER(cod_base) LIKE %s OR UPPER(descripcion) LIKE %s OR UPPER(sinonimo) LIKE %s)")
            params.extend([f"{p}%", f"{p}%", f"{p}%", f"{p}%"])

    if codigos_contains and len(codigos_contains) > 0:
        for c in codigos_contains:
            c = str(c).strip().upper()
            if not c:
                continue
            cod_filters.append("(UPPER(cod_articulo) LIKE %s OR UPPER(cod_base) LIKE %s OR UPPER(descripcion) LIKE %s OR UPPER(sinonimo) LIKE %s)")
            params.extend([f"%{c}%", f"%{c}%", f"%{c}%", f"%{c}%"])

    if cod_filters:
        query += " AND (" + " OR ".join(cod_filters) + ")"

    if solo_sugeridos:
        query += " AND necesidad > 0"

    if solo_nuevos:
        query += " AND is_nuevo = 1"

    query += " ORDER BY sugerencia_distribuir DESC, necesidad DESC, sucursal, cod_articulo"
    if limit and limit > 0:
        query += " LIMIT %s"
        params.append(int(limit))

    cur.execute(query, params)
    results = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(r) for r in results]

def get_kpi_alertas_criticas(
    dias_proyeccion: int = 30,
    sucursales: Optional[List] = None,
    prefijos_familia: Optional[List] = None,
    codigos_prefix: Optional[List] = None,
    codigos_contains: Optional[List] = None,
    alertas: Optional[List] = None,
    solo_nuevos: bool = False,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    start_date, end_date, dias_periodo = _resolve_period(dias_proyeccion, start_date, end_date)
    nuevo_cutoff = _months_ago(datetime.now().date(), 6)

    alertas_criticas = alertas or ["Quiebre de stock", "Stock de Seguridad", "Pto de Pedido"]

    query = """
        WITH ventas_p AS (
            SELECT cod_articulo, sucursal, SUM(cantidad_venta) AS ventas_periodo
            FROM ventas
            WHERE fecha >= %s AND fecha <= %s
            GROUP BY 1,2
        ),
        stock_s AS (
            SELECT cod_articulo, sucursal,
                   SUM(stock_1) AS stock_sucursal,
                   MAX(familia) AS familia,
                   MAX(desc_familia) AS desc_familia,
                   MAX(descripcion) AS descripcion,
                   MAX(sinonimo) AS sinonimo
            FROM saldo
            GROUP BY 1,2
        ),
        base AS (
            SELECT cod_articulo, sucursal FROM stock_s
            UNION
            SELECT cod_articulo, sucursal FROM ventas_p
        ),
        art_norm AS (
            SELECT
                cod_articulo,
                cod_base,
                descripcion,
                sinonimo,
                fecha_alta,
                REPLACE(UPPER(cod_articulo), ' ', '') AS cod_norm
            FROM articulos
        ),
        calc_base AS (
            SELECT
                COALESCE(a.cod_base, b.cod_articulo) AS cod_base,
                b.sucursal,
                b.cod_articulo,
                COALESCE(s.stock_sucursal, 0) AS stock_sucursal,
                COALESCE(v.ventas_periodo, 0) AS ventas_periodo,
                COALESCE(v.ventas_periodo, 0) / %s AS venta_promedio_diaria,
                CASE
                    WHEN COALESCE(v.ventas_periodo, 0) = 0 THEN 0
                    ELSE COALESCE(s.stock_sucursal, 0) / NULLIF((COALESCE(v.ventas_periodo, 0) / %s) * 30, 0)
                END AS meses_stock,
                (COALESCE(v.ventas_periodo, 0) - COALESCE(s.stock_sucursal, 0)) AS necesidad,
                COALESCE(c.costo_reposicion, 0) AS costo_unitario,
                COALESCE(s.familia, '') AS familia,
                COALESCE(s.descripcion, a.descripcion, '') AS descripcion,
                COALESCE(s.sinonimo, a.sinonimo, '') AS sinonimo,
                CASE
                    WHEN a.fecha_alta IS NOT NULL AND a.fecha_alta >= %s THEN 1
                    ELSE 0
                END AS is_nuevo
            FROM base b
            LEFT JOIN stock_s s ON s.cod_articulo = b.cod_articulo AND s.sucursal = b.sucursal
            LEFT JOIN ventas_p v ON v.cod_articulo = b.cod_articulo AND v.sucursal = b.sucursal
            LEFT JOIN articulos a ON a.cod_articulo = b.cod_articulo
            LEFT JOIN costos c ON c.cod_articulo = b.cod_articulo
        ),
        calc AS (
            SELECT
                *,
                CASE
                    WHEN ventas_periodo = 0 THEN 'Sin rotacion'
                    WHEN meses_stock < 1 THEN 'Quiebre de stock'
                    WHEN meses_stock < 2 THEN 'Stock de Seguridad'
                    WHEN meses_stock < 3 THEN 'Pto de Pedido'
                    WHEN meses_stock < 4 THEN 'OK'
                    ELSE 'Sobrestock'
                END AS alerta_stock
            FROM calc_base
        )
        SELECT
            sucursal,
            SUM(GREATEST(necesidad, 0)) AS unidades_sugeridas,
            SUM(GREATEST(necesidad, 0) * costo_unitario) AS monto_reponer_costo
        FROM calc
        WHERE 1=1
    """

    params: List = [start_date, end_date, dias_periodo, dias_periodo, nuevo_cutoff]

    if alertas_criticas:
        placeholders = ','.join(['%s'] * len(alertas_criticas))
        query += f" AND alerta_stock IN ({placeholders})"
        params.extend(alertas_criticas)

    if sucursales and len(sucursales) > 0:
        placeholders = ','.join(['%s'] * len(sucursales))
        query += f" AND sucursal IN ({placeholders})"
        params.extend(list(sucursales))

    if prefijos_familia and len(prefijos_familia) > 0:
        placeholders = ','.join(['%s'] * len(prefijos_familia))
        query += f" AND LEFT(UPPER(cod_articulo), 2) IN ({placeholders})"
        params.extend([str(x).strip().upper() for x in prefijos_familia if str(x).strip()])

    cod_filters = []
    if codigos_prefix and len(codigos_prefix) > 0:
        for p in codigos_prefix:
            p = str(p).strip().upper()
            if not p:
                continue
            cod_filters.append("(UPPER(cod_articulo) LIKE %s OR UPPER(cod_base) LIKE %s OR UPPER(descripcion) LIKE %s OR UPPER(sinonimo) LIKE %s)")
            params.extend([f"{p}%", f"{p}%", f"{p}%", f"{p}%"])

    if codigos_contains and len(codigos_contains) > 0:
        for c in codigos_contains:
            c = str(c).strip().upper()
            if not c:
                continue
            cod_filters.append("(UPPER(cod_articulo) LIKE %s OR UPPER(cod_base) LIKE %s OR UPPER(descripcion) LIKE %s OR UPPER(sinonimo) LIKE %s)")
            params.extend([f"%{c}%", f"%{c}%", f"%{c}%", f"%{c}%"])

    if cod_filters:
        query += " AND (" + " OR ".join(cod_filters) + ")"

    if solo_nuevos:
        query += " AND is_nuevo = 1"

    query += " AND necesidad > 0 GROUP BY sucursal ORDER BY monto_reponer_costo DESC"

    cur.execute(query, params)
    results = cur.fetchall()
    cur.close()
    conn.close()

    merged: dict = {}
    for r in results:
        d = dict(r)
        suc = d.get("sucursal")
        if suc in SUCURSALES_UNIFICAR:
            suc = SUCURSALES_UNIFICAR[suc]
        d["sucursal"] = suc
        if suc in merged:
            merged[suc]["unidades_sugeridas"] += d.get("unidades_sugeridas") or 0
            merged[suc]["monto_reponer_costo"] += d.get("monto_reponer_costo") or 0
        else:
            merged[suc] = d

    merged_list = list(merged.values())
    merged_list.sort(key=lambda x: x.get("monto_reponer_costo") or 0, reverse=True)
    return merged_list

def get_kpi_familias_reponer(
    dias_proyeccion: int = 30,
    sucursales: Optional[List] = None,
    prefijos_familia: Optional[List] = None,
    codigos_prefix: Optional[List] = None,
    codigos_contains: Optional[List] = None,
    alertas: Optional[List] = None,
    solo_nuevos: bool = False,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    start_date, end_date, dias_periodo = _resolve_period(dias_proyeccion, start_date, end_date)
    nuevo_cutoff = _months_ago(datetime.now().date(), 6)

    alertas_criticas = alertas or ["Quiebre de stock", "Stock de Seguridad", "Pto de Pedido"]

    query = """
        WITH ventas_p AS (
            SELECT cod_articulo, sucursal, SUM(cantidad_venta) AS ventas_periodo
            FROM ventas
            WHERE fecha >= %s AND fecha <= %s
            GROUP BY 1,2
        ),
        stock_s AS (
            SELECT cod_articulo, sucursal,
                   SUM(stock_1) AS stock_sucursal,
                   MAX(familia) AS familia,
                   MAX(desc_familia) AS desc_familia,
                   MAX(descripcion) AS descripcion,
                   MAX(sinonimo) AS sinonimo
            FROM saldo
            GROUP BY 1,2
        ),
        base AS (
            SELECT cod_articulo, sucursal FROM stock_s
            UNION
            SELECT cod_articulo, sucursal FROM ventas_p
        ),
        art_norm AS (
            SELECT
                cod_articulo,
                cod_base,
                descripcion,
                sinonimo,
                fecha_alta,
                REPLACE(UPPER(cod_articulo), ' ', '') AS cod_norm
            FROM articulos
        ),
        calc_base AS (
            SELECT
                b.sucursal,
                b.cod_articulo,
                COALESCE(s.familia, '') AS familia,
                COALESCE(s.desc_familia, '') AS desc_familia,
                COALESCE(s.stock_sucursal, 0) AS stock_sucursal,
                COALESCE(v.ventas_periodo, 0) AS ventas_periodo,
                COALESCE(v.ventas_periodo, 0) / %s AS venta_promedio_diaria,
                CASE
                    WHEN COALESCE(v.ventas_periodo, 0) = 0 THEN 0
                    ELSE COALESCE(s.stock_sucursal, 0) / NULLIF((COALESCE(v.ventas_periodo, 0) / %s) * 30, 0)
                END AS meses_stock,
                (COALESCE(v.ventas_periodo, 0) - COALESCE(s.stock_sucursal, 0)) AS necesidad,
                COALESCE(c.costo_reposicion, 0) AS costo_unitario,
                COALESCE(s.descripcion, a.descripcion, '') AS descripcion,
                COALESCE(s.sinonimo, a.sinonimo, '') AS sinonimo,
                CASE
                    WHEN a.fecha_alta IS NOT NULL AND a.fecha_alta >= %s THEN 1
                    ELSE 0
                END AS is_nuevo
            FROM base b
            LEFT JOIN stock_s s ON s.cod_articulo = b.cod_articulo AND s.sucursal = b.sucursal
            LEFT JOIN ventas_p v ON v.cod_articulo = b.cod_articulo AND v.sucursal = b.sucursal
            LEFT JOIN art_norm a ON a.cod_norm = REPLACE(UPPER(b.cod_articulo), ' ', '')
            LEFT JOIN costos c ON c.cod_articulo = b.cod_articulo
        ),
        calc AS (
            SELECT
                *,
                CASE
                    WHEN ventas_periodo = 0 THEN 'Sin rotacion'
                    WHEN meses_stock < 1 THEN 'Quiebre de stock'
                    WHEN meses_stock < 2 THEN 'Stock de Seguridad'
                    WHEN meses_stock < 3 THEN 'Pto de Pedido'
                    WHEN meses_stock < 4 THEN 'OK'
                    ELSE 'Sobrestock'
                END AS alerta_stock
            FROM calc_base
        )
        SELECT
            COALESCE(NULLIF(desc_familia, ''), NULLIF(familia, ''), 'SIN FAMILIA') AS familia,
            SUM(GREATEST(necesidad, 0) * costo_unitario) AS monto_reponer_costo
        FROM calc
        WHERE 1=1
    """

    params: List = [start_date, end_date, dias_periodo, dias_periodo, nuevo_cutoff]

    if alertas_criticas:
        placeholders = ','.join(['%s'] * len(alertas_criticas))
        query += f" AND alerta_stock IN ({placeholders})"
        params.extend(alertas_criticas)

    if sucursales and len(sucursales) > 0:
        placeholders = ','.join(['%s'] * len(sucursales))
        query += f" AND sucursal IN ({placeholders})"
        params.extend(list(sucursales))

    if prefijos_familia and len(prefijos_familia) > 0:
        placeholders = ','.join(['%s'] * len(prefijos_familia))
        query += f" AND LEFT(UPPER(cod_articulo), 2) IN ({placeholders})"
        params.extend([str(x).strip().upper() for x in prefijos_familia if str(x).strip()])

    cod_filters = []
    if codigos_prefix and len(codigos_prefix) > 0:
        for p in codigos_prefix:
            p = str(p).strip().upper()
            if not p:
                continue
            cod_filters.append("(UPPER(cod_articulo) LIKE %s OR UPPER(descripcion) LIKE %s OR UPPER(sinonimo) LIKE %s)")
            params.extend([f"{p}%", f"{p}%", f"{p}%"])

    if codigos_contains and len(codigos_contains) > 0:
        for c in codigos_contains:
            c = str(c).strip().upper()
            if not c:
                continue
            cod_filters.append("(UPPER(cod_articulo) LIKE %s OR UPPER(descripcion) LIKE %s OR UPPER(sinonimo) LIKE %s)")
            params.extend([f"%{c}%", f"%{c}%", f"%{c}%"])

    if cod_filters:
        query += " AND (" + " OR ".join(cod_filters) + ")"

    if solo_nuevos:
        query += " AND is_nuevo = 1"

    query += " AND necesidad > 0 GROUP BY COALESCE(NULLIF(desc_familia, ''), NULLIF(familia, ''), 'SIN FAMILIA') ORDER BY monto_reponer_costo DESC"

    cur.execute(query, params)
    results = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(r) for r in results]

def get_categorias():
    """Obtener todas las categorÃ­as Ãºnicas"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT categoria FROM categorias WHERE categoria IS NOT NULL AND categoria != '' ORDER BY categoria")
    results = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return results

def get_subcategorias(categoria: Optional[str] = None):
    """Obtener subcategorÃ­as, opcionalmente filtradas por categorÃ­a"""
    conn = get_connection()
    cur = conn.cursor()
    if categoria:
        cur.execute("SELECT DISTINCT subcategoria FROM categorias WHERE subcategoria IS NOT NULL AND subcategoria != '' AND categoria = %s ORDER BY subcategoria", (categoria,))
    else:
        cur.execute("SELECT DISTINCT subcategoria FROM categorias WHERE subcategoria IS NOT NULL AND subcategoria != '' ORDER BY subcategoria")
    results = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return results

def get_articulos_por_categoria(categorias: Optional[List] = None, subcategorias: Optional[List] = None):
    """Obtener cÃ³digos de artÃ­culos filtrados por categorÃ­a y/o subcategorÃ­a"""
    conn = get_connection()
    cur = conn.cursor()
    query = "SELECT DISTINCT cod_articulo FROM categorias WHERE 1=1"
    params = []
    
    if categorias and len(categorias) > 0:
        placeholders = ','.join(['%s'] * len(categorias))
        query += f" AND categoria IN ({placeholders})"
        params.extend(categorias)
    
    if subcategorias and len(subcategorias) > 0:
        placeholders = ','.join(['%s'] * len(subcategorias))
        query += f" AND subcategoria IN ({placeholders})"
        params.extend(subcategorias)
    
    cur.execute(query, params)
    results = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return results

def upsert_articulos(records: list, timestamp: datetime):
    if not records:
        return 0
    conn = get_connection()
    cur = conn.cursor()
    values = [
        (
            r.get("cod_articulo", ""),
            r.get("descripcion", ""),
            r.get("desc_adicional", ""),
            r.get("sinonimo", ""),
            r.get("cod_base", ""),
            r.get("desc_base", ""),
            r.get("familia", ""),
            r.get("cod_agrupacion", ""),
            r.get("desc_agrupacion", ""),
            r.get("codigo_barra", ""),
            r.get("fecha_alta"),
            r.get("um_stock", ""),
            r.get("lleva_stock", ""),
            r.get("doble_um", ""),
            timestamp
        )
        for r in records
    ]
    execute_values(cur, """
        INSERT INTO articulos (cod_articulo, descripcion, desc_adicional, sinonimo, cod_base, desc_base, familia,
                              cod_agrupacion, desc_agrupacion, codigo_barra, fecha_alta, um_stock, lleva_stock, doble_um,
                              sync_timestamp)
        VALUES %s
        ON CONFLICT (cod_articulo) DO UPDATE SET
            descripcion = EXCLUDED.descripcion,
            desc_adicional = EXCLUDED.desc_adicional,
            sinonimo = EXCLUDED.sinonimo,
            cod_base = EXCLUDED.cod_base,
            desc_base = EXCLUDED.desc_base,
            familia = EXCLUDED.familia,
            cod_agrupacion = EXCLUDED.cod_agrupacion,
            desc_agrupacion = EXCLUDED.desc_agrupacion,
            codigo_barra = EXCLUDED.codigo_barra,
            fecha_alta = EXCLUDED.fecha_alta,
            um_stock = EXCLUDED.um_stock,
            lleva_stock = EXCLUDED.lleva_stock,
            doble_um = EXCLUDED.doble_um,
            sync_timestamp = EXCLUDED.sync_timestamp
    """, values)
    count = len(values)
    conn.commit()
    cur.close()
    conn.close()
    return count

def refresh_categorias_from_articulos():
    """
    Reconstruye categorÃ­as a partir de nÃ³mina de artÃ­culos.
    categoria   -> familia
    subcategoria-> desc_agrupacion
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE categorias RESTART IDENTITY")
    cur.execute("""
        INSERT INTO categorias (cod_articulo, categoria, subcategoria)
        SELECT
            cod_articulo,
            NULLIF(TRIM(COALESCE(familia, '')), '') as categoria,
            NULLIF(TRIM(COALESCE(desc_agrupacion, '')), '') as subcategoria
        FROM articulos
        WHERE cod_articulo IS NOT NULL
          AND TRIM(cod_articulo) <> ''
    """)
    count = cur.rowcount if cur.rowcount is not None else 0
    conn.commit()
    cur.close()
    conn.close()
    return int(count)

def get_data_quality_summary():
    """
    DiagnÃ³stico rÃ¡pido de cobertura de datos.
    """
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT
            (SELECT COUNT(*) FROM articulos) AS articulos,
            (SELECT COUNT(*) FROM saldo) AS saldo,
            (SELECT COUNT(*) FROM saldo_historial) AS saldo_historial,
            (SELECT COUNT(*) FROM ventas) AS ventas,
            (SELECT COUNT(*) FROM metricas) AS metricas,
            (SELECT COUNT(*) FROM precios) AS precios,
            (SELECT COUNT(*) FROM costos) AS costos,
            (SELECT COUNT(*) FROM categorias) AS categorias,
            (SELECT MIN(fecha) FROM ventas) AS min_fecha_ventas,
            (SELECT MAX(fecha) FROM ventas) AS max_fecha_ventas,
            (SELECT MAX(snapshot_ts) FROM saldo_historial) AS ultima_snapshot_stock
    """)
    row = dict(cur.fetchone() or {})
    cur.close()
    conn.close()

    faltantes = []
    if (row.get("costos") or 0) == 0:
        faltantes.append("costos")
    if (row.get("categorias") or 0) == 0:
        faltantes.append("categorias")
    if (row.get("saldo_historial") or 0) == 0:
        faltantes.append("saldo_historial")

    row["datasets_faltantes"] = faltantes
    row["estado"] = "ok" if not faltantes else "warning"
    return row

def get_articulos_base(limit: int = 5000):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT DISTINCT cod_base, desc_base
        FROM articulos
        WHERE cod_base IS NOT NULL AND cod_base != ''
        ORDER BY cod_base
        LIMIT %s
    """, (limit,))
    results = cur.fetchall()
    # Fallback si articulos aÃºn no fue sincronizada
    if not results:
        cur.execute("""
            SELECT DISTINCT cod_base, desc_base
            FROM saldo
            WHERE cod_base IS NOT NULL AND cod_base != ''
            ORDER BY cod_base
            LIMIT %s
        """, (limit,))
        results = cur.fetchall()
    if not results:
        cur.execute("""
            SELECT DISTINCT cod_base, desc_base
            FROM ventas
            WHERE cod_base IS NOT NULL AND cod_base != ''
            ORDER BY cod_base
            LIMIT %s
        """, (limit,))
        results = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(r) for r in results]

def get_detalle_sucursal(sucursal: str, lista_precio: str, dias: int):
    """Obtener detalle de artÃ­culos crÃ­ticos para una sucursal con valorizaciÃ³n por perÃ­odo"""
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    sucursal_buscar = sucursal
    if not sucursal.startswith("LA TIJERA") and not sucursal.startswith("CRISA"):
        sucursal_buscar = f"LA TIJERA {sucursal}"
    
    query = """
        SELECT 
            m.cod_articulo,
            m.descripcion,
            COALESCE(m.stock_1, 0) as stock,
            COALESCE(m.venta_promedio_diaria, 0) as venta_diaria,
            COALESCE(m.venta_promedio_diaria, 0) * %s as necesidad,
            GREATEST(0, (COALESCE(m.venta_promedio_diaria, 0) * %s) - COALESCE(m.stock_1, 0)) as faltante,
            COALESCE(p.precio, 0) as precio,
            GREATEST(0, (COALESCE(m.venta_promedio_diaria, 0) * %s) - COALESCE(m.stock_1, 0)) * COALESCE(p.precio, 0) as valor
        FROM metricas m
        LEFT JOIN precios p ON m.cod_articulo = p.cod_articulo AND p.nro_lista = %s
        WHERE (m.sucursal = %s OR m.sucursal LIKE %s)
        AND m.alerta_stock IN ('âš ï¸ Quiebre de stock', 'â— Stock de Seguridad')
        AND GREATEST(0, (COALESCE(m.venta_promedio_diaria, 0) * %s) - COALESCE(m.stock_1, 0)) > 0
        ORDER BY valor DESC
        LIMIT 100
    """
    
    cur.execute(query, (dias, dias, dias, lista_precio, sucursal_buscar, f"%{sucursal}%", dias))
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return [dict(r) for r in results]

def get_resumen_reposicion(dias=30):
    """Obtener resumen de reposiciÃ³n por sucursal para el dashboard"""
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    query_cards = """
        SELECT 
            m.sucursal,
            COUNT(DISTINCT c.categoria) as grupos,
            SUM(CASE WHEN m.alerta_stock = 'âš ï¸ Quiebre de stock' THEN 1 ELSE 0 END) as quiebre_qty,
            COALESCE(SUM(CASE WHEN m.alerta_stock = 'âš ï¸ Quiebre de stock' 
                THEN GREATEST(0, m.venta_promedio_diaria * %s - m.stock_1) * COALESCE(p.precio, 0) ELSE 0 END), 0) as quiebre_val,
            SUM(CASE WHEN m.alerta_stock = 'â— Stock de Seguridad' THEN 1 ELSE 0 END) as seguridad_qty,
            COALESCE(SUM(CASE WHEN m.alerta_stock = 'â— Stock de Seguridad' 
                THEN GREATEST(0, m.venta_promedio_diaria * %s - m.stock_1) * COALESCE(p.precio, 0) ELSE 0 END), 0) as seguridad_val,
            SUM(CASE WHEN m.alerta_stock = 'ðŸ“ Pto de Pedido' THEN 1 ELSE 0 END) as pedido_qty,
            COALESCE(SUM(CASE WHEN m.alerta_stock = 'ðŸ“ Pto de Pedido' 
                THEN GREATEST(0, m.venta_promedio_diaria * %s - m.stock_1) * COALESCE(p.precio, 0) ELSE 0 END), 0) as pedido_val,
            SUM(CASE WHEN m.alerta_stock = 'ðŸ“¦ Sobrestock' THEN 1 ELSE 0 END) as sobrestock_qty,
            COALESCE(SUM(CASE WHEN m.alerta_stock = 'ðŸ“¦ Sobrestock' 
                THEN m.stock_1 * COALESCE(p.precio, 0) ELSE 0 END), 0) as sobrestock_val,
            SUM(CASE WHEN m.alerta_stock IN ('ðŸŸ  Sin rotaciÃ³n') THEN 1 ELSE 0 END) as sinrot_qty,
            COALESCE(SUM(CASE WHEN m.alerta_stock IN ('ðŸŸ  Sin rotaciÃ³n') 
                THEN m.stock_1 * COALESCE(p.precio, 0) ELSE 0 END), 0) as sinrot_val,
            SUM(CASE WHEN m.alerta_stock = 'âœ… OK' THEN 1 ELSE 0 END) as ok_qty
        FROM metricas m
        LEFT JOIN precios p ON m.cod_articulo = p.cod_articulo AND p.nro_lista = '2'
        LEFT JOIN categorias c ON m.cod_articulo = c.cod_articulo
        WHERE m.sucursal NOT IN ('LA TIJERA MAYORISTA MENDOZA')
        GROUP BY m.sucursal
        ORDER BY (COALESCE(SUM(CASE WHEN m.alerta_stock = 'âš ï¸ Quiebre de stock' 
                THEN GREATEST(0, m.venta_promedio_diaria * %s - m.stock_1) * COALESCE(p.precio, 0) ELSE 0 END), 0) +
                 COALESCE(SUM(CASE WHEN m.alerta_stock = 'â— Stock de Seguridad' 
                THEN GREATEST(0, m.venta_promedio_diaria * %s - m.stock_1) * COALESCE(p.precio, 0) ELSE 0 END), 0)) DESC
        LIMIT 10
    """
    
    cur.execute(query_cards, (dias, dias, dias, dias, dias))
    cards_raw = cur.fetchall()
    
    cards = []
    for row in cards_raw:
        sucursal_nombre = row['sucursal'].replace('LA TIJERA ', '') if row['sucursal'] else 'N/A'
        cards.append({
            'sucursal': sucursal_nombre,
            'grupos': int(row['grupos'] or 0),
            'quiebre_qty': int(row['quiebre_qty'] or 0),
            'quiebre_val': float(row['quiebre_val'] or 0),
            'seguridad_qty': int(row['seguridad_qty'] or 0),
            'seguridad_val': float(row['seguridad_val'] or 0),
            'pedido_qty': int(row['pedido_qty'] or 0),
            'pedido_val': float(row['pedido_val'] or 0),
            'sobrestock_qty': int(row['sobrestock_qty'] or 0),
            'sobrestock_val': float(row['sobrestock_val'] or 0),
            'sinrot_qty': int(row['sinrot_qty'] or 0),
            'sinrot_val': float(row['sinrot_val'] or 0),
            'ok_qty': int(row['ok_qty'] or 0)
        })
    
    query_tabla = """
        SELECT 
            m.sucursal,
            COALESCE(c.categoria, 'Sin CategorÃ­a') as grupo,
            COUNT(DISTINCT m.cod_articulo) as articulos,
            SUM(CASE WHEN m.alerta_stock IN ('âš ï¸ Quiebre de stock', 'â— Stock de Seguridad') THEN 1 ELSE 0 END) as faltantes,
            SUM(GREATEST(0, m.venta_promedio_diaria * %s - m.stock_1)) as cant_reponer,
            COALESCE(SUM(
                GREATEST(0, m.venta_promedio_diaria * %s - m.stock_1) * COALESCE(p.precio, 0)
            ), 0) as valor
        FROM metricas m
        LEFT JOIN precios p ON m.cod_articulo = p.cod_articulo AND p.nro_lista = '2'
        LEFT JOIN categorias c ON m.cod_articulo = c.cod_articulo
        WHERE m.alerta_stock IN ('âš ï¸ Quiebre de stock', 'â— Stock de Seguridad')
        AND m.sucursal NOT IN ('LA TIJERA MAYORISTA MENDOZA')
        GROUP BY m.sucursal, c.categoria
        HAVING SUM(GREATEST(0, m.venta_promedio_diaria * %s - m.stock_1)) > 0
        ORDER BY valor DESC
    """
    
    cur.execute(query_tabla, (dias, dias, dias))
    tabla_raw = cur.fetchall()
    
    tabla = []
    for row in tabla_raw:
        sucursal_nombre = row['sucursal'].replace('LA TIJERA ', '') if row['sucursal'] else 'N/A'
        tabla.append({
            'sucursal': sucursal_nombre,
            'grupo': row['grupo'] or 'Sin CategorÃ­a',
            'articulos': int(row['articulos'] or 0),
            'faltantes': int(row['faltantes'] or 0),
            'cant_reponer': int(row['cant_reponer'] or 0),
            'valor': float(row['valor'] or 0)
        })
    
    cur.close()
    conn.close()
    
    return {
        'cards': cards,
        'tabla': tabla
    }

def get_prioridades_distribucion(dias=30):
    """Obtener prioridades de distribuciÃ³n por sucursal para el jefe logÃ­stico"""
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    query = """
        WITH necesidades AS (
            SELECT 
                m.sucursal,
                COALESCE(c.categoria, 'SIN CATEGORIA') as categoria,
                COUNT(DISTINCT m.cod_articulo) as articulos,
                SUM(GREATEST(0, m.venta_promedio_diaria * %s - m.stock_1)) as unidades_necesarias,
                SUM(CASE WHEN m.alerta_stock = 'âš ï¸ Quiebre de stock' THEN 1 ELSE 0 END) as quiebres
            FROM metricas m
            LEFT JOIN categorias c ON m.cod_articulo = c.cod_articulo
            WHERE m.sucursal NOT IN ('CRISA CENTRAL', 'LA TIJERA MAYORISTA MENDOZA')
            AND m.alerta_stock IN ('âš ï¸ Quiebre de stock', 'â— Stock de Seguridad', 'ðŸ“ Pto de Pedido')
            GROUP BY m.sucursal, c.categoria
        )
        SELECT 
            sucursal,
            SUM(articulos) as total_articulos,
            SUM(unidades_necesarias) as total_unidades,
            SUM(quiebres) as total_quiebres,
            ARRAY_AGG(DISTINCT categoria ORDER BY categoria) as categorias
        FROM necesidades
        WHERE unidades_necesarias > 0
        GROUP BY sucursal
        ORDER BY SUM(quiebres) DESC, SUM(unidades_necesarias) DESC
    """
    
    cur.execute(query, (dias,))
    results = cur.fetchall()
    cur.close()
    conn.close()
    
    prioridades = []
    for row in results:
        quiebres = int(row['total_quiebres'] or 0)
        articulos = int(row['total_articulos'] or 0)
        
        # Determinar prioridad basada en cantidad de quiebres
        if quiebres >= 50:
            prioridad = 'ALTA'
        elif quiebres >= 20:
            prioridad = 'MEDIA'
        else:
            prioridad = 'BAJA'
        
        sucursal_nombre = row['sucursal'].replace('LA TIJERA ', '') if row['sucursal'] else 'N/A'
        
        prioridades.append({
            'sucursal': sucursal_nombre,
            'articulos': articulos,
            'unidades': float(row['total_unidades'] or 0),
            'quiebres': quiebres,
            'prioridad': prioridad,
            'categorias': row['categorias'] or []
        })
    
    return prioridades

# ============== FUNCIONES DE COSTOS ==============

def upsert_costos(costos_list):
    """Insertar o actualizar costos de reposiciÃ³n."""
    if not costos_list:
        return 0
    
    conn = get_connection()
    cur = conn.cursor()
    
    insert_query = """
        INSERT INTO costos (cod_articulo, descripcion, costo_reposicion, moneda, fecha_actualizacion)
        VALUES %s
        ON CONFLICT (cod_articulo) 
        DO UPDATE SET 
            descripcion = EXCLUDED.descripcion,
            costo_reposicion = EXCLUDED.costo_reposicion,
            moneda = EXCLUDED.moneda,
            fecha_actualizacion = EXCLUDED.fecha_actualizacion,
            sync_timestamp = CURRENT_TIMESTAMP
    """
    
    values = []
    for c in costos_list:
        values.append((
            c.get('cod_articulo', ''),
            c.get('descripcion', ''),
            c.get('costo_reposicion', 0),
            c.get('moneda', 'ARS'),
            c.get('fecha_actualizacion', datetime.now().date())
        ))
    
    execute_values(cur, insert_query, values)
    count = len(values)
    
    conn.commit()
    cur.close()
    conn.close()
    
    return count

def get_all_costos():
    """Obtener todos los costos de reposiciÃ³n."""
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    cur.execute("""
        SELECT cod_articulo, descripcion, costo_reposicion, moneda, 
               fecha_actualizacion, sync_timestamp
        FROM costos
        ORDER BY cod_articulo
    """)
    
    results = cur.fetchall()
    cur.close()
    conn.close()
    
    return [dict(r) for r in results]

def get_costo_articulo(cod_articulo):
    """Obtener costo de un artÃ­culo especÃ­fico."""
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    cur.execute("""
        SELECT cod_articulo, descripcion, costo_reposicion, moneda, 
               fecha_actualizacion, sync_timestamp
        FROM costos
        WHERE cod_articulo = %s
    """, (cod_articulo,))
    
    result = cur.fetchone()
    cur.close()
    conn.close()
    
    return dict(result) if result else None

def get_metricas_con_costos(sucursal=None, alerta=None, familia=None):
    """Obtener mÃ©tricas con costos de reposiciÃ³n integrados."""
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    query = """
        SELECT 
            m.cod_articulo,
            m.descripcion,
            m.sucursal,
            m.deposito,
            m.familia,
            m.desc_familia,
            m.stock_1,
            m.total_venta,
            m.venta_promedio_diaria,
            m.venta_mensual_proyectada,
            m.meses_stock,
            m.alerta_stock,
            m.necesidad,
            m.pedido,
            COALESCE(c.costo_reposicion, 0) as costo_unitario,
            COALESCE(c.moneda, 'ARS') as moneda,
            COALESCE(c.costo_reposicion * m.stock_1, 0) as valor_stock,
            COALESCE(c.costo_reposicion * GREATEST(0, m.necesidad), 0) as valor_reposicion,
            m.sync_timestamp
        FROM metricas m
        LEFT JOIN costos c ON m.cod_articulo = c.cod_articulo
        WHERE m.sucursal NOT IN ('CRISA CENTRAL', 'LA TIJERA MAYORISTA MENDOZA')
    """
    
    params = []
    if sucursal:
        query += " AND m.sucursal = %s"
        params.append(sucursal)
    if alerta:
        query += " AND m.alerta_stock = %s"
        params.append(alerta)
    if familia:
        query += " AND m.familia = %s"
        params.append(familia)
    
    query += " ORDER BY m.sucursal, m.alerta_stock, m.cod_articulo"
    
    cur.execute(query, params)
    results = cur.fetchall()
    cur.close()
    conn.close()
    
    return [dict(r) for r in results]

def get_resumen_costos_por_sucursal():
    """Obtener resumen de valores de stock y reposiciÃ³n por sucursal."""
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    query = """
        SELECT 
            m.sucursal,
            COUNT(DISTINCT m.cod_articulo) as total_articulos,
            SUM(m.stock_1) as total_unidades,
            SUM(COALESCE(c.costo_reposicion * m.stock_1, 0)) as valor_stock_total,
            SUM(CASE WHEN m.alerta_stock IN ('âš ï¸ Quiebre de stock', 'â— Stock de Seguridad', 'ðŸ“ Pto de Pedido')
                THEN COALESCE(c.costo_reposicion * GREATEST(0, m.necesidad), 0) ELSE 0 END) as valor_reposicion_urgente,
            SUM(CASE WHEN m.alerta_stock = 'âš ï¸ Quiebre de stock' THEN 1 ELSE 0 END) as quiebres,
            SUM(CASE WHEN m.alerta_stock = 'â— Stock de Seguridad' THEN 1 ELSE 0 END) as seguridad,
            SUM(CASE WHEN m.alerta_stock = 'ðŸ“¦ Sobrestock' THEN 1 ELSE 0 END) as sobrestock
        FROM metricas m
        LEFT JOIN costos c ON m.cod_articulo = c.cod_articulo
        WHERE m.sucursal NOT IN ('CRISA CENTRAL', 'LA TIJERA MAYORISTA MENDOZA')
        GROUP BY m.sucursal
        ORDER BY valor_reposicion_urgente DESC
    """
    
    cur.execute(query)
    results = cur.fetchall()
    cur.close()
    conn.close()
    
    return [dict(r) for r in results]

def delete_all_costos():
    """Eliminar todos los costos."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE costos RESTART IDENTITY")
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    init_database()





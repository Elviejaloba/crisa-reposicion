import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from datetime import datetime
from typing import Optional, List

DATABASE_URL = os.environ.get("DATABASE_URL")

SUCURSALES_UNIFICAR = {
    "LA TIJERA MAYORISTA MENDOZA": "LA TIJERA MENDOZA"
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
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ventas (
            id SERIAL PRIMARY KEY,
            cod_articulo VARCHAR(100),
            descripcion TEXT,
            sucursal VARCHAR(200),
            nro_sucursal INTEGER,
            fecha DATE,
            cantidad_venta DECIMAL(18,4) DEFAULT 0,
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
    
    # Crear constraints únicos si no existen (para tablas existentes)
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
    
    conn.commit()
    cur.close()
    conn.close()

def clear_tables():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE saldo, ventas, metricas, precios RESTART IDENTITY")
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
        INSERT INTO saldo (cod_articulo, descripcion, sucursal, nro_sucursal, deposito, 
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
    
    updated = 0
    for r in records:
        cur.execute("""
            INSERT INTO saldo (cod_articulo, descripcion, sucursal, nro_sucursal, deposito, 
                              cod_deposito, familia, desc_familia, um_stock, stock_1, sync_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (cod_articulo, cod_deposito, sucursal) 
            DO UPDATE SET 
                descripcion = EXCLUDED.descripcion,
                nro_sucursal = EXCLUDED.nro_sucursal,
                deposito = EXCLUDED.deposito,
                familia = EXCLUDED.familia,
                desc_familia = EXCLUDED.desc_familia,
                um_stock = EXCLUDED.um_stock,
                stock_1 = EXCLUDED.stock_1,
                sync_timestamp = EXCLUDED.sync_timestamp
        """, (
            r.get("cod_articulo", ""),
            r.get("descripcion", ""),
            r.get("sucursal", ""),
            r.get("nro_sucursal", 0),
            r.get("deposito", ""),
            r.get("cod_deposito", ""),
            r.get("familia", ""),
            r.get("desc_familia", ""),
            r.get("um_stock", ""),
            r.get("stock_1", 0),
            timestamp
        ))
        updated += 1
    
    conn.commit()
    cur.close()
    conn.close()
    return updated

def insert_ventas(records: list, timestamp: datetime):
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
            r.get("fecha"),
            r.get("cantidad_venta", 0),
            r.get("importe", 0),
            r.get("familia", ""),
            r.get("desc_familia", ""),
            r.get("um_stock", ""),
            timestamp
        )
        for r in records
    ]
    
    execute_values(cur, """
        INSERT INTO ventas (cod_articulo, descripcion, sucursal, nro_sucursal, fecha,
                           cantidad_venta, importe, familia, desc_familia, um_stock, sync_timestamp)
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
    
    updated = 0
    for r in records:
        cur.execute("""
            INSERT INTO ventas (cod_articulo, descripcion, sucursal, nro_sucursal, fecha,
                               cantidad_venta, importe, familia, desc_familia, um_stock, sync_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (cod_articulo, sucursal, fecha) 
            DO UPDATE SET 
                descripcion = EXCLUDED.descripcion,
                nro_sucursal = EXCLUDED.nro_sucursal,
                cantidad_venta = EXCLUDED.cantidad_venta,
                importe = EXCLUDED.importe,
                familia = EXCLUDED.familia,
                desc_familia = EXCLUDED.desc_familia,
                um_stock = EXCLUDED.um_stock,
                sync_timestamp = EXCLUDED.sync_timestamp
        """, (
            r.get("cod_articulo", ""),
            r.get("descripcion", ""),
            r.get("sucursal", ""),
            r.get("nro_sucursal", 0),
            r.get("fecha"),
            r.get("cantidad_venta", 0),
            r.get("importe", 0),
            r.get("familia", ""),
            r.get("desc_familia", ""),
            r.get("um_stock", ""),
            timestamp
        ))
        updated += 1
    
    conn.commit()
    cur.close()
    conn.close()
    return updated

def get_ultima_fecha_ventas():
    """Obtener la última fecha de ventas sincronizada"""
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
    """Obtener info para sincronización incremental"""
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT 
            (SELECT COUNT(*) FROM saldo) as total_saldos,
            (SELECT COUNT(*) FROM ventas) as total_ventas,
            (SELECT COUNT(*) FROM precios) as total_precios,
            (SELECT MAX(fecha) FROM ventas) as ultima_fecha_ventas,
            (SELECT MAX(sync_timestamp) FROM saldo) as ultima_sync_saldo
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
        FROM metricas 
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

def get_matriz_distribucion(dias_proyeccion: int = 30, familias: Optional[List] = None, alertas: Optional[List] = None):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    query = """
        SELECT 
            cod_articulo,
            descripcion,
            familia,
            sucursal,
            stock_1,
            venta_promedio_diaria,
            venta_mensual_proyectada,
            meses_stock,
            alerta_stock,
            (venta_promedio_diaria * %s) as necesidad_periodo,
            (venta_promedio_diaria * %s) - stock_1 as diferencia
        FROM metricas
        WHERE 1=1
    """
    params = [dias_proyeccion, dias_proyeccion]
    
    if familias and len(familias) > 0:
        placeholders = ','.join(['%s'] * len(familias))
        query += f" AND desc_familia IN ({placeholders})"
        params.extend(familias)
    
    if alertas and len(alertas) > 0:
        placeholders = ','.join(['%s'] * len(alertas))
        query += f" AND alerta_stock IN ({placeholders})"
        params.extend(alertas)
    
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

def get_sugerencia_distribucion(dias_proyeccion: int = 30, familias: Optional[List] = None):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    query = """
        WITH articulo_stats AS (
            SELECT 
                cod_articulo,
                descripcion,
                SUM(stock_1) as stock_total,
                SUM(venta_promedio_diaria) as venta_diaria_total,
                SUM(CASE WHEN meses_stock > 6 OR (venta_mensual_proyectada = 0 AND stock_1 > 0) 
                    THEN stock_1 ELSE 0 END) as stock_excedente
            FROM metricas
            WHERE 1=1
    """
    params = []
    
    if familias and len(familias) > 0:
        placeholders = ','.join(['%s'] * len(familias))
        query += f" AND desc_familia IN ({placeholders})"
        params.extend(familias)
    
    query += """
            GROUP BY cod_articulo, descripcion
        ),
        deficit AS (
            SELECT 
                m.cod_articulo,
                m.descripcion,
                m.sucursal,
                m.stock_1 as stock_sucursal,
                m.venta_promedio_diaria,
                m.meses_stock,
                m.alerta_stock,
                ast.stock_total as stock_cdd,
                ast.stock_excedente,
                (m.venta_promedio_diaria * %s) as necesidad,
                GREATEST(0, (m.venta_promedio_diaria * %s) - m.stock_1) as cantidad_faltante
            FROM metricas m
            JOIN articulo_stats ast ON m.cod_articulo = ast.cod_articulo
            WHERE m.alerta_stock IN ('Quiebre', 'Stock de Seguridad')
    """
    params.extend([dias_proyeccion, dias_proyeccion])
    
    if familias and len(familias) > 0:
        placeholders = ','.join(['%s'] * len(familias))
        query += f" AND m.familia IN ({placeholders})"
        params.extend(familias)
    
    query += """
        )
        SELECT 
            cod_articulo,
            descripcion,
            sucursal,
            stock_cdd,
            stock_sucursal,
            necesidad,
            LEAST(cantidad_faltante, stock_excedente) as sugerencia_distribuir,
            stock_excedente as pedido,
            meses_stock,
            alerta_stock
        FROM deficit 
        WHERE cantidad_faltante > 0 AND stock_excedente > 0
        ORDER BY cod_articulo, sucursal
    """
    
    cur.execute(query, params)
    results = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(r) for r in results]

def get_categorias():
    """Obtener todas las categorías únicas"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT categoria FROM categorias WHERE categoria IS NOT NULL AND categoria != '' ORDER BY categoria")
    results = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return results

def get_subcategorias(categoria: Optional[str] = None):
    """Obtener subcategorías, opcionalmente filtradas por categoría"""
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
    """Obtener códigos de artículos filtrados por categoría y/o subcategoría"""
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

def get_detalle_sucursal(sucursal: str, lista_precio: str, dias: int):
    """Obtener detalle de artículos críticos para una sucursal con valorización por período"""
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
        AND m.alerta_stock IN ('Quiebre de stock', 'Stock de Seguridad')
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
    """Obtener resumen de reposición por sucursal para el dashboard"""
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    query_cards = """
        SELECT 
            m.sucursal,
            COUNT(DISTINCT c.categoria) as grupos,
            SUM(CASE WHEN m.alerta_stock = 'Quiebre de stock' THEN 1 ELSE 0 END) as quiebre_qty,
            COALESCE(SUM(CASE WHEN m.alerta_stock = 'Quiebre de stock' 
                THEN GREATEST(0, m.venta_promedio_diaria * %s - m.stock_1) * COALESCE(p.precio, 0) ELSE 0 END), 0) as quiebre_val,
            SUM(CASE WHEN m.alerta_stock = 'Stock de Seguridad' THEN 1 ELSE 0 END) as seguridad_qty,
            COALESCE(SUM(CASE WHEN m.alerta_stock = 'Stock de Seguridad' 
                THEN GREATEST(0, m.venta_promedio_diaria * %s - m.stock_1) * COALESCE(p.precio, 0) ELSE 0 END), 0) as seguridad_val,
            SUM(CASE WHEN m.alerta_stock = 'Pto de Pedido' THEN 1 ELSE 0 END) as pedido_qty,
            COALESCE(SUM(CASE WHEN m.alerta_stock = 'Pto de Pedido' 
                THEN GREATEST(0, m.venta_promedio_diaria * %s - m.stock_1) * COALESCE(p.precio, 0) ELSE 0 END), 0) as pedido_val,
            SUM(CASE WHEN m.alerta_stock = 'Sobre stock' THEN 1 ELSE 0 END) as sobrestock_qty,
            COALESCE(SUM(CASE WHEN m.alerta_stock = 'Sobre stock' 
                THEN m.stock_1 * COALESCE(p.precio, 0) ELSE 0 END), 0) as sobrestock_val,
            SUM(CASE WHEN m.alerta_stock IN ('Sin rotación (sin stock)', 'Sin rotación (con sobrestock)') THEN 1 ELSE 0 END) as sinrot_qty,
            COALESCE(SUM(CASE WHEN m.alerta_stock IN ('Sin rotación (sin stock)', 'Sin rotación (con sobrestock)') 
                THEN m.stock_1 * COALESCE(p.precio, 0) ELSE 0 END), 0) as sinrot_val,
            SUM(CASE WHEN m.alerta_stock = 'OK' THEN 1 ELSE 0 END) as ok_qty
        FROM metricas m
        LEFT JOIN precios p ON m.cod_articulo = p.cod_articulo AND p.nro_lista = '2'
        LEFT JOIN categorias c ON m.cod_articulo = c.cod_articulo
        WHERE m.sucursal NOT IN ('LA TIJERA MAYORISTA MENDOZA')
        GROUP BY m.sucursal
        ORDER BY (COALESCE(SUM(CASE WHEN m.alerta_stock = 'Quiebre de stock' 
                THEN GREATEST(0, m.venta_promedio_diaria * %s - m.stock_1) * COALESCE(p.precio, 0) ELSE 0 END), 0) +
                 COALESCE(SUM(CASE WHEN m.alerta_stock = 'Stock de Seguridad' 
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
            COALESCE(c.categoria, 'Sin Categoría') as grupo,
            COUNT(DISTINCT m.cod_articulo) as articulos,
            SUM(CASE WHEN m.alerta_stock IN ('Quiebre de stock', 'Stock de Seguridad') THEN 1 ELSE 0 END) as faltantes,
            SUM(GREATEST(0, m.venta_promedio_diaria * %s - m.stock_1)) as cant_reponer,
            COALESCE(SUM(
                GREATEST(0, m.venta_promedio_diaria * %s - m.stock_1) * COALESCE(p.precio, 0)
            ), 0) as valor
        FROM metricas m
        LEFT JOIN precios p ON m.cod_articulo = p.cod_articulo AND p.nro_lista = '2'
        LEFT JOIN categorias c ON m.cod_articulo = c.cod_articulo
        WHERE m.alerta_stock IN ('Quiebre de stock', 'Stock de Seguridad')
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
            'grupo': row['grupo'] or 'Sin Categoría',
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
    """Obtener prioridades de distribución por sucursal para el jefe logístico"""
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    query = """
        WITH necesidades AS (
            SELECT 
                m.sucursal,
                COALESCE(c.categoria, 'SIN CATEGORIA') as categoria,
                COUNT(DISTINCT m.cod_articulo) as articulos,
                SUM(GREATEST(0, m.venta_promedio_diaria * %s - m.stock_1)) as unidades_necesarias,
                SUM(CASE WHEN m.alerta_stock = 'Quiebre de stock' THEN 1 ELSE 0 END) as quiebres
            FROM metricas m
            LEFT JOIN categorias c ON m.cod_articulo = c.cod_articulo
            WHERE m.sucursal NOT IN ('CRISA CENTRAL', 'LA TIJERA MAYORISTA MENDOZA')
            AND m.alerta_stock IN ('Quiebre de stock', 'Stock de Seguridad', 'Pto de Pedido')
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
    """Insertar o actualizar costos de reposición."""
    if not costos_list:
        return 0
    
    conn = get_connection()
    cur = conn.cursor()
    
    insert_query = """
        INSERT INTO costos (cod_articulo, descripcion, costo_reposicion, moneda, fecha_actualizacion, sync_timestamp)
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
    """Obtener todos los costos de reposición."""
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
    """Obtener costo de un artículo específico."""
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
    """Obtener métricas con costos de reposición integrados."""
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
    """Obtener resumen de valores de stock y reposición por sucursal."""
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    query = """
        SELECT 
            m.sucursal,
            COUNT(DISTINCT m.cod_articulo) as total_articulos,
            SUM(m.stock_1) as total_unidades,
            SUM(COALESCE(c.costo_reposicion * m.stock_1, 0)) as valor_stock_total,
            SUM(CASE WHEN m.alerta_stock IN ('Quiebre de stock', 'Stock de Seguridad', 'Pto de Pedido')
                THEN COALESCE(c.costo_reposicion * GREATEST(0, m.necesidad), 0) ELSE 0 END) as valor_reposicion_urgente,
            SUM(CASE WHEN m.alerta_stock = 'Quiebre de stock' THEN 1 ELSE 0 END) as quiebres,
            SUM(CASE WHEN m.alerta_stock = 'Stock de Seguridad' THEN 1 ELSE 0 END) as seguridad,
            SUM(CASE WHEN m.alerta_stock = 'Sobrestock' THEN 1 ELSE 0 END) as sobrestock
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

init_database()

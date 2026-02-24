# Sistema de Análisis Comercial y Reposición

## Overview
Sistema para cruzar datos de stock, ventas y precios desde SQL Server local (Tango ERP), calculando métricas de reposición y alertas de stock. Los datos se almacenan en PostgreSQL.

## Database Schema (PostgreSQL)

### Tabla: saldo
| Columna | Tipo | Descripción |
|---------|------|-------------|
| id | SERIAL | PK |
| cod_articulo | VARCHAR(100) | Código del artículo |
| descripcion | TEXT | Descripción |
| sucursal | VARCHAR(200) | Nombre sucursal |
| nro_sucursal | INTEGER | Número sucursal |
| deposito | VARCHAR(200) | Nombre depósito |
| cod_deposito | VARCHAR(50) | Código depósito |
| familia | VARCHAR(100) | Código familia |
| desc_familia | VARCHAR(200) | Descripción familia |
| um_stock | VARCHAR(20) | Unidad de medida |
| stock_1 | DECIMAL(18,4) | Stock actual |
| sync_timestamp | TIMESTAMP | Fecha sincronización |

### Tabla: ventas
| Columna | Tipo | Descripción |
|---------|------|-------------|
| id | SERIAL | PK |
| cod_articulo | VARCHAR(100) | Código del artículo |
| descripcion | TEXT | Descripción |
| sucursal | VARCHAR(200) | Nombre sucursal |
| nro_sucursal | INTEGER | Número sucursal |
| fecha | DATE | Fecha de venta |
| cantidad_venta | DECIMAL(18,4) | Cantidad vendida |
| importe | DECIMAL(18,4) | Importe c/IVA |
| familia | VARCHAR(100) | Código familia |
| desc_familia | VARCHAR(200) | Descripción familia |
| um_stock | VARCHAR(20) | Unidad de medida |
| sync_timestamp | TIMESTAMP | Fecha sincronización |

### Tabla: precios
| Columna | Tipo | Descripción |
|---------|------|-------------|
| id | SERIAL | PK |
| cod_articulo | VARCHAR(100) | Código del artículo |
| descripcion | TEXT | Descripción |
| sinonimo | VARCHAR(100) | Sinónimo |
| cod_familia | VARCHAR(50) | Código familia |
| familia | VARCHAR(200) | Nombre familia |
| precio | DECIMAL(18,4) | Precio unitario |
| nro_lista | VARCHAR(20) | Número lista (2, 102) |
| nombre_lista | VARCHAR(200) | Nombre lista |
| fecha_modificacion | DATE | Última modificación |
| sync_timestamp | TIMESTAMP | Fecha sincronización |

### Tabla: metricas
| Columna | Tipo | Descripción |
|---------|------|-------------|
| id | SERIAL | PK |
| cod_articulo | VARCHAR(100) | Código del artículo |
| descripcion | TEXT | Descripción |
| sucursal | VARCHAR(200) | Nombre sucursal |
| stock_1 | DECIMAL(18,4) | Stock actual |
| total_venta | DECIMAL(18,4) | Total vendido |
| venta_promedio_diaria | DECIMAL(18,4) | Venta promedio diaria |
| venta_mensual_proyectada | DECIMAL(18,4) | Venta mensual × 30 |
| meses_stock | DECIMAL(18,4) | Meses de stock |
| alerta_stock | VARCHAR(50) | Tipo de alerta |
| sync_timestamp | TIMESTAMP | Fecha sincronización |

### Tabla: sync_log
| Columna | Tipo | Descripción |
|---------|------|-------------|
| id | SERIAL | PK |
| timestamp | TIMESTAMP | Fecha/hora sync |
| registros_saldo | INTEGER | Cantidad saldos |
| registros_ventas | INTEGER | Cantidad ventas |
| registros_metricas | INTEGER | Cantidad métricas |
| status | VARCHAR(50) | Estado (ok/error) |
| message | TEXT | Mensaje detalle |

## API Endpoints
| Método | Endpoint | Descripción |
|--------|----------|-------------|
| POST | /sync | Sincronizar datos desde bridge |
| GET | /data | Obtener métricas con filtros |
| GET | /sucursales | Listar sucursales |
| GET | /alertas | Conteo por tipo alerta |
| GET | /totales | Totales agregados |
| GET | /ventas/{codigo} | Ventas de un artículo |
| GET | /precios | Todos los precios |
| GET | /precios/{codigo} | Precios de un artículo |
| GET | /listas-precios | Listas de precios disponibles |
| GET | /health | Estado del servidor |
| POST | /whatsapp/enviar | Enviar alerta WhatsApp |
| GET | /whatsapp/preview/{tipo} | Ver preview de mensaje |
| POST | /whatsapp/alertas-rojas | Enviar alertas a sucursales rojas |

## Alertas WhatsApp (Twilio)
Archivo: `whatsapp_alerts.py`

### Tipos de Mensaje
- **resumen**: Reporte ejecutivo general con semáforo (🔴🟡🟢)
- **comercial**: Resumen de montos para área comercial
- **sucursal**: Alerta específica y urgente por sucursal

### Uso desde API
```python
POST /whatsapp/enviar
{
    "numero_destino": "whatsapp:+549261XXXXXXX",
    "tipo_mensaje": "resumen",
    "sucursal": "MENDOZA"  # solo para tipo 'sucursal'
}
```

### Uso desde Código
```python
import whatsapp_alerts as wa
wa.enviar_resumen_general("+549261XXXXXXX")
wa.enviar_alerta_sucursal("+549261XXXXXXX", "MENDOZA")
wa.enviar_resumen_comercial("+549261XXXXXXX")
```

### Configuración Twilio
Las credenciales se obtienen automáticamente desde Replit Connectors.

## Workflows
- API Server: Puerto 8000 (FastAPI) - Mapea a puerto 80 externo
- Streamlit Dashboard: Puerto 5000

## Bridge SQL - Consultas
1. Saldos de stock (CTA_SALDO_ARTICULO_DEPOSITO)
2. Ventas (CTA03/CTA02 con período 2024-2026)
3. Precios (GVA17/GVA10 listas 2 y 102)

## Alertas de Stock
- Quiebre: < 1 mes
- Stock de Seguridad: 1-2 meses
- Normal: 2-6 meses
- Sobrestock: > 6 meses
- Sin rotación: Sin ventas

## Pestaña Distribución
La pestaña de Distribución permite:
- Filtrar por sucursal, artículo, familia y período (30/60/90 días)
- Ver matriz de necesidad: artículos × sucursales con diferencias coloreadas
- Sugerencias de redistribución basadas en excedente disponible

### Lógica de Distribución
- **Necesidad** = Venta diaria × Días de proyección
- **Diferencia** = Necesidad - Stock actual
- **Stock excedente** = Stock de sucursales con >6 meses o sin rotación
- **Sug. Distribuir** = MIN(cantidad_faltante, stock_excedente)

## Pestaña Costos
La pestaña de Costos permite gestionar los costos de reposición de artículos.

### Tabla: costos
| Columna | Tipo | Descripción |
|---------|------|-------------|
| id | SERIAL | PK |
| cod_articulo | VARCHAR(100) | Código del artículo (UNIQUE) |
| descripcion | TEXT | Descripción |
| costo_reposicion | DECIMAL(18,4) | Costo unitario |
| moneda | VARCHAR(10) | Moneda (ARS default) |
| fecha_actualizacion | DATE | Última actualización |
| sync_timestamp | TIMESTAMP | Fecha sincronización |

### API Endpoints Costos
| Método | Endpoint | Descripción |
|--------|----------|-------------|
| GET | /costos | Obtener todos los costos |
| GET | /costos/{codigo} | Costo de un artículo |
| POST | /costos | Subir/actualizar costos (JSON) |
| DELETE | /costos | Eliminar todos los costos |
| GET | /metricas-costos | Métricas con costos integrados |
| GET | /resumen-costos | Resumen por sucursal |

### Formato de Archivo Excel/CSV
Columnas requeridas:
- `cod_articulo`: Código del artículo (obligatorio)
- `costo_reposicion`: Costo unitario (obligatorio)
- `descripcion`: Descripción (opcional)
- `moneda`: Moneda, default ARS (opcional)

### Métricas con Costos
- **Valor Stock** = costo_reposicion × stock_actual
- **Valor Reposición** = costo_reposicion × necesidad (solo alertas rojas/amarillas)

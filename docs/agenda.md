# Agenda de Decisiones

## Flujo Oficial de Sincronización (vigente)
- Origen: Tango (SQL Server)
- Proceso: `bridge_sql.py`
- Destino: API local (`/sync`)
- Base de datos: **Neo (PostgreSQL en la nube)**
- Visualización: Dashboard / Vercel

**Regla clave:** No se usa Postgres local como intermediario. El flujo válido es **Py -> API -> Neo**.

## Validación de Datos (referencia)
- Las fechas y la “última actualización” se toman desde Neo vía `GET /sync-info`.
- Cualquier validación debe compararse contra Neo, no contra bases locales.

## Cambios Futuros
- Mantener la lógica actual de sincronización.
- Cualquier ajuste debe respetar el flujo definido arriba.

## UI - Autoajuste de grillas
- El autoajuste de columnas en las grillas mantiene el ancho máximo observado.
- Una vez ajustado, no se reduce automáticamente para evitar saltos visuales.

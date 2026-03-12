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

## Publicación y entornos
- GitHub: rama principal 'main' es la fuente de verdad.
- Vercel: publicación desde GitHub si está conectado (auto‑deploy). Si no, usar 'vercel link' y 'vercel --prod'.
- Render: publicación desde GitHub si está conectado (auto‑deploy). Si no, usar deploy hook.
- Neon: base de datos en la nube; se actualiza sólo vía sync del py.
- URLs/servicios: completar y mantener actualizados en este bloque.
- Vercel (frontend): https://srctextilcrisa.vercel.app
- Render (API): https://crisa-reposicion.onrender.com/


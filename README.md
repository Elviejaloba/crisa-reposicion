# CRISA - Sistema de Análisis Comercial y Reposición

Sistema para **La Tijera / Grupo CRISA** que conecta con Tango ERP (SQL Server) para gestión de stock, ventas, precios y costos.

## Características
- Dashboard Streamlit interactivo con semáforo de alertas
- Sincronización incremental desde Tango ERP via bridge SQL Server
- Alertas por WhatsApp (Twilio) y Email
- Distribución inteligente entre sucursales
- Gestión de costos de reposición
- Protección por contraseña

## Arquitectura
- **Frontend**: Streamlit (puerto 5000)
- **Backend**: FastAPI (puerto 8001)
- **Base de datos**: PostgreSQL
- **Bridge**: Script Python que corre en PC local conectado a SQL Server

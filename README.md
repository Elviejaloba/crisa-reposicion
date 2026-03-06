# CRISA - Sistema de Reposición de Sucursales

Sistema para **La Tijera / Grupo CRISA** que conecta con Tango ERP (SQL Server) para gestión de stock, ventas, precios y costos.

## Características
- Frontend web en React + Vite
- API FastAPI
- Sincronización incremental desde Tango ERP vía bridge SQL Server
- Alertas por WhatsApp (Twilio) y Email
- Distribución inteligente entre sucursales
- Gestión de costos de reposición

## Arquitectura
- **Frontend**: React (Vite)
- **Backend**: FastAPI (puerto 5000)
- **Base de datos**: PostgreSQL
- **Bridge**: script Python en PC local conectado a SQL Server

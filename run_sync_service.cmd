@echo off
setlocal

REM Servicio de sync para Render (no requiere usuario logueado)
set "SYNC_URL=https://crisa-reposicion.onrender.com"
set "PYTHONUNBUFFERED=1"

cd /d C:\bridge2\Reposicionsuc

if not exist .venv\Scripts\python.exe (
  echo [ERROR] No se encontro Python del venv en C:\bridge2\Reposicionsuc\.venv\Scripts\python.exe
  exit /b 1
)

if not exist logs mkdir logs
echo.>> logs\sync_service.log
echo [%date% %time%] Iniciando servicio de sync...>> logs\sync_service.log

.venv\Scripts\python.exe bridge_sql.py >> logs\sync_service.log 2>> logs\sync_service.err
endlocal

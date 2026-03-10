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
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd_HH-mm-ss"') do set LOG_TS=%%i
set "LOG_OUT=logs\sync_service_%LOG_TS%.log"
set "LOG_ERR=logs\sync_service_%LOG_TS%.err"

echo.>> "%LOG_OUT%"
echo [%date% %time%] Iniciando servicio de sync...>> "%LOG_OUT%"

.venv\Scripts\python.exe bridge_sql.py >> "%LOG_OUT%" 2>> "%LOG_ERR%"
endlocal

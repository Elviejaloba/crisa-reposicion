"""
Módulo de Alertas por Email - CRISA Sistema de Reposición
Compatible con Outlook 2007+
"""

import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from datetime import datetime
from database import get_resumen_reposicion

# Configuración SMTP
SMTP_SERVER = "smtp.textilcrisa.com"
SMTP_PORT = 26
SMTP_USER = "reportes@textilcrisa.com"
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
PUBLIC_APP_URL = os.environ.get("PUBLIC_APP_URL", "http://localhost:5173")

# Colores corporativos
COLORS = {
    'primary': '#2d3748',
    'header_bg': '#4a5568',
    'red': '#dc3545',
    'yellow': '#ffc107',
    'green': '#28a745',
    'light': '#f7f7f7',
    'dark': '#333333',
    'border': '#dddddd',
    'text': '#555555'
}

def get_datos_alertas(dias=30):
    """Obtiene datos de alertas desde la base de datos"""
    try:
        resultado = get_resumen_reposicion(dias=dias)
        cards = resultado.get('cards', [])
        
        if not cards:
            return []
        
        alertas = []
        for card in cards:
            alertas.append({
                'sucursal': card.get('sucursal', 'N/A'),
                'valor': float(card.get('valor', 0)),
                'articulos': int(card.get('articulos_criticos', 0)),
                'categorias': int(card.get('grupos', 0))
            })
        return alertas
    except Exception as e:
        print(f"Error obteniendo datos: {e}")
        return []

def clasificar_alerta(valor):
    """Clasifica el nivel de alerta según el valor"""
    if valor >= 50000000:
        return "CRITICO", COLORS['red'], "#ffffff"
    elif valor >= 20000000:
        return "PRECAUCION", COLORS['yellow'], "#333333"
    else:
        return "ESTABLE", COLORS['green'], "#ffffff"

def generar_html_resumen_general(dias=30):
    """Genera HTML del resumen general compatible con Outlook"""
    alertas = get_datos_alertas(dias)
    fecha = datetime.now().strftime("%d/%m/%Y %H:%M")
    
    total_valor = sum(a['valor'] for a in alertas)
    total_articulos = sum(a['articulos'] for a in alertas)
    sucursales_rojas = sum(1 for a in alertas if a['valor'] >= 50000000)
    sucursales_amarillas = sum(1 for a in alertas if 20000000 <= a['valor'] < 50000000)
    sucursales_verdes = sum(1 for a in alertas if a['valor'] < 20000000)
    
    # Filas de sucursales
    filas_sucursales = ""
    for i, alerta in enumerate(sorted(alertas, key=lambda x: x['valor'], reverse=True)):
        nivel, color, text_color = clasificar_alerta(alerta['valor'])
        bg = "#ffffff" if i % 2 == 0 else "#f9f9f9"
        
        # Indicador de color
        indicador = f'<span style="color:{color};font-size:16px;font-weight:bold;">&#9679;</span>'
        
        filas_sucursales += f"""
        <tr style="background-color:{bg};">
            <td style="padding:12px 15px;border-bottom:1px solid #eeeeee;font-family:Arial,sans-serif;font-size:14px;">
                {indicador} <strong>{alerta['sucursal']}</strong>
            </td>
            <td style="padding:12px 15px;border-bottom:1px solid #eeeeee;text-align:right;font-family:Arial,sans-serif;font-size:14px;color:{color};font-weight:bold;">
                ${alerta['valor']:,.0f}
            </td>
            <td style="padding:12px 15px;border-bottom:1px solid #eeeeee;text-align:center;font-family:Arial,sans-serif;font-size:14px;color:#333333;">
                {alerta['articulos']}
            </td>
            <td style="padding:12px 15px;border-bottom:1px solid #eeeeee;text-align:center;font-family:Arial,sans-serif;font-size:14px;color:#333333;">
                {alerta['categorias']}
            </td>
        </tr>
        """
    
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <!--[if mso]>
    <style type="text/css">
        table {{border-collapse:collapse;}}
        td {{font-family:Arial,sans-serif;}}
    </style>
    <![endif]-->
</head>
<body style="margin:0;padding:0;background-color:#e5e5e5;font-family:Arial,Helvetica,sans-serif;">
    
    <!-- Container principal -->
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="background-color:#e5e5e5;">
        <tr>
            <td align="center" style="padding:30px 15px;">
                
                <!-- Email wrapper -->
                <table role="presentation" width="650" cellspacing="0" cellpadding="0" border="0" style="background-color:#ffffff;border:1px solid #dddddd;">
                    
                    <!-- Header con Logo -->
                    <tr>
                        <td style="background-color:{COLORS['header_bg']};padding:30px;text-align:center;">
                            <img src="cid:logo" alt="GRUPO CRISA" width="180" style="display:block;margin:0 auto 15px;">
                            <h1 style="color:#ffffff;margin:0;font-family:Arial,sans-serif;font-size:22px;font-weight:bold;">
                                REPORTE DE REPOSICION
                            </h1>
                            <p style="color:#cccccc;margin:10px 0 0;font-family:Arial,sans-serif;font-size:13px;">
                                Generado: {fecha} | Proyeccion: {dias} dias
                            </p>
                        </td>
                    </tr>
                    
                    <!-- Semáforo de sucursales -->
                    <tr>
                        <td style="padding:25px 30px 15px;">
                            <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0">
                                <tr>
                                    <td width="33%" align="center" style="padding:10px;">
                                        <table role="presentation" cellspacing="0" cellpadding="0" border="0" style="border:2px solid {COLORS['red']};background-color:#fff5f5;width:100%;">
                                            <tr>
                                                <td style="padding:15px;text-align:center;">
                                                    <div style="font-family:Arial,sans-serif;font-size:32px;font-weight:bold;color:{COLORS['red']};">{sucursales_rojas}</div>
                                                    <div style="font-family:Arial,sans-serif;font-size:11px;color:{COLORS['red']};text-transform:uppercase;font-weight:bold;">Criticas</div>
                                                </td>
                                            </tr>
                                        </table>
                                    </td>
                                    <td width="33%" align="center" style="padding:10px;">
                                        <table role="presentation" cellspacing="0" cellpadding="0" border="0" style="border:2px solid {COLORS['yellow']};background-color:#fffbf0;width:100%;">
                                            <tr>
                                                <td style="padding:15px;text-align:center;">
                                                    <div style="font-family:Arial,sans-serif;font-size:32px;font-weight:bold;color:#b38600;">{sucursales_amarillas}</div>
                                                    <div style="font-family:Arial,sans-serif;font-size:11px;color:#b38600;text-transform:uppercase;font-weight:bold;">Precaucion</div>
                                                </td>
                                            </tr>
                                        </table>
                                    </td>
                                    <td width="33%" align="center" style="padding:10px;">
                                        <table role="presentation" cellspacing="0" cellpadding="0" border="0" style="border:2px solid {COLORS['green']};background-color:#f0fff4;width:100%;">
                                            <tr>
                                                <td style="padding:15px;text-align:center;">
                                                    <div style="font-family:Arial,sans-serif;font-size:32px;font-weight:bold;color:{COLORS['green']};">{sucursales_verdes}</div>
                                                    <div style="font-family:Arial,sans-serif;font-size:11px;color:{COLORS['green']};text-transform:uppercase;font-weight:bold;">Estables</div>
                                                </td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    
                    <!-- Totales destacados -->
                    <tr>
                        <td style="padding:10px 30px 25px;">
                            <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="background-color:{COLORS['primary']};">
                                <tr>
                                    <td width="50%" style="padding:25px;text-align:center;border-right:1px solid #4a5568;">
                                        <div style="font-family:Arial,sans-serif;font-size:26px;font-weight:bold;color:#ffffff;">${total_valor:,.0f}</div>
                                        <div style="font-family:Arial,sans-serif;font-size:11px;color:#aaaaaa;text-transform:uppercase;margin-top:5px;">Monto Total a Reponer</div>
                                    </td>
                                    <td width="50%" style="padding:25px;text-align:center;">
                                        <div style="font-family:Arial,sans-serif;font-size:26px;font-weight:bold;color:#ffffff;">{total_articulos}</div>
                                        <div style="font-family:Arial,sans-serif;font-size:11px;color:#aaaaaa;text-transform:uppercase;margin-top:5px;">Articulos Criticos</div>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    
                    <!-- Título tabla -->
                    <tr>
                        <td style="padding:0 30px 10px;">
                            <h3 style="font-family:Arial,sans-serif;font-size:16px;color:#333333;margin:0;font-weight:bold;">
                                Estado por Sucursal
                            </h3>
                        </td>
                    </tr>
                    
                    <!-- Tabla de sucursales -->
                    <tr>
                        <td style="padding:0 30px 25px;">
                            <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="border:1px solid #dddddd;">
                                <tr style="background-color:{COLORS['primary']};">
                                    <th style="padding:12px 15px;text-align:left;font-family:Arial,sans-serif;font-size:12px;color:#ffffff;font-weight:bold;">Sucursal</th>
                                    <th style="padding:12px 15px;text-align:right;font-family:Arial,sans-serif;font-size:12px;color:#ffffff;font-weight:bold;">Valor a Reponer</th>
                                    <th style="padding:12px 15px;text-align:center;font-family:Arial,sans-serif;font-size:12px;color:#ffffff;font-weight:bold;">Articulos</th>
                                    <th style="padding:12px 15px;text-align:center;font-family:Arial,sans-serif;font-size:12px;color:#ffffff;font-weight:bold;">Categorias</th>
                                </tr>
                                {filas_sucursales}
                            </table>
                        </td>
                    </tr>
                    
                    <!-- Botón CTA -->
                    <tr>
                        <td style="padding:10px 30px 30px;text-align:center;">
                            <table role="presentation" cellspacing="0" cellpadding="0" border="0" align="center">
                                <tr>
                                    <td style="background-color:{COLORS['primary']};padding:12px 30px;">
                                        <a href="{PUBLIC_APP_URL}" style="color:#ffffff;font-family:Arial,sans-serif;font-size:14px;text-decoration:none;font-weight:bold;">
                                            Ver Dashboard Completo
                                        </a>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    
                    <!-- Footer -->
                    <tr>
                        <td style="background-color:#f5f5f5;padding:20px 30px;border-top:1px solid #dddddd;">
                            <p style="font-family:Arial,sans-serif;font-size:12px;color:#888888;margin:0;text-align:center;">
                                Este es un correo automatico del <strong>Sistema de Reposicion CRISA</strong>
                            </p>
                            <p style="font-family:Arial,sans-serif;font-size:11px;color:#888888;margin:8px 0 0;text-align:center;">
                                Grupo CRISA | Mendoza, Argentina
                            </p>
                        </td>
                    </tr>
                    
                </table>
                
            </td>
        </tr>
    </table>
    
</body>
</html>
    """
    return html

def generar_html_alerta_sucursal(sucursal, dias=30):
    """Genera HTML de alerta urgente para una sucursal específica - Compatible Outlook"""
    alertas = get_datos_alertas(dias)
    fecha = datetime.now().strftime("%d/%m/%Y %H:%M")
    
    datos_sucursal = next((a for a in alertas if a['sucursal'].upper() == sucursal.upper()), None)
    
    if not datos_sucursal:
        return None
    
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin:0;padding:0;background-color:#e5e5e5;font-family:Arial,Helvetica,sans-serif;">
    
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="background-color:#e5e5e5;">
        <tr>
            <td align="center" style="padding:30px 15px;">
                
                <table role="presentation" width="600" cellspacing="0" cellpadding="0" border="0" style="background-color:#ffffff;border:1px solid #dddddd;">
                    
                    <!-- Header Rojo -->
                    <tr>
                        <td style="background-color:{COLORS['red']};padding:30px;text-align:center;">
                            <img src="cid:logo" alt="GRUPO CRISA" width="160" style="display:block;margin:0 auto 15px;">
                            <h1 style="color:#ffffff;margin:0;font-family:Arial,sans-serif;font-size:24px;font-weight:bold;">
                                ALERTA URGENTE DE STOCK
                            </h1>
                            <p style="color:#ffcccc;margin:10px 0 0;font-family:Arial,sans-serif;font-size:16px;font-weight:bold;">
                                Sucursal: {sucursal.upper()}
                            </p>
                        </td>
                    </tr>
                    
                    <!-- Caja de alerta -->
                    <tr>
                        <td style="padding:25px 30px 15px;">
                            <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="background-color:#fff3cd;border:1px solid {COLORS['yellow']};">
                                <tr>
                                    <td style="padding:15px;">
                                        <p style="font-family:Arial,sans-serif;font-size:14px;color:#856404;margin:0;line-height:1.5;">
                                            <strong>Se requiere atencion inmediata.</strong><br>
                                            Esta sucursal presenta niveles criticos de stock que requieren reposicion urgente.
                                        </p>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    
                    <!-- Datos -->
                    <tr>
                        <td style="padding:15px 30px;">
                            <h3 style="font-family:Arial,sans-serif;font-size:16px;color:#333333;margin:0 0 15px;font-weight:bold;">
                                Situacion Actual
                            </h3>
                            <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="background-color:#f8f8f8;">
                                <tr>
                                    <td style="padding:15px;border-bottom:1px solid #eeeeee;">
                                        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0">
                                            <tr>
                                                <td style="font-family:Arial,sans-serif;font-size:14px;color:#666666;">Valor a Reponer</td>
                                                <td style="font-family:Arial,sans-serif;font-size:20px;font-weight:bold;color:{COLORS['red']};text-align:right;">${datos_sucursal['valor']:,.0f}</td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                                <tr>
                                    <td style="padding:15px;border-bottom:1px solid #eeeeee;">
                                        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0">
                                            <tr>
                                                <td style="font-family:Arial,sans-serif;font-size:14px;color:#666666;">Articulos Criticos</td>
                                                <td style="font-family:Arial,sans-serif;font-size:18px;font-weight:bold;color:#333333;text-align:right;">{datos_sucursal['articulos']}</td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                                <tr>
                                    <td style="padding:15px;border-bottom:1px solid #eeeeee;">
                                        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0">
                                            <tr>
                                                <td style="font-family:Arial,sans-serif;font-size:14px;color:#666666;">Categorias Afectadas</td>
                                                <td style="font-family:Arial,sans-serif;font-size:18px;font-weight:bold;color:#333333;text-align:right;">{datos_sucursal['categorias']}</td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                                <tr>
                                    <td style="padding:15px;">
                                        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0">
                                            <tr>
                                                <td style="font-family:Arial,sans-serif;font-size:14px;color:#666666;">Fecha del Reporte</td>
                                                <td style="font-family:Arial,sans-serif;font-size:14px;font-weight:bold;color:#333333;text-align:right;">{fecha}</td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    
                    <!-- Acción requerida -->
                    <tr>
                        <td style="padding:15px 30px;">
                            <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="background-color:{COLORS['primary']};">
                                <tr>
                                    <td style="padding:15px;">
                                        <p style="font-family:Arial,sans-serif;font-size:13px;color:#ffffff;margin:0;line-height:1.5;">
                                            <strong>Accion Requerida:</strong><br>
                                            Coordine con el Centro de Distribucion para programar la reposicion de los articulos criticos lo antes posible.
                                        </p>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    
                    <!-- Botón -->
                    <tr>
                        <td style="padding:15px 30px 30px;text-align:center;">
                            <table role="presentation" cellspacing="0" cellpadding="0" border="0" align="center">
                                <tr>
                                    <td style="background-color:{COLORS['red']};padding:12px 30px;">
                                        <a href="{PUBLIC_APP_URL}" style="color:#ffffff;font-family:Arial,sans-serif;font-size:14px;text-decoration:none;font-weight:bold;">
                                            Ver Detalle en Dashboard
                                        </a>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    
                    <!-- Footer -->
                    <tr>
                        <td style="background-color:#f5f5f5;padding:20px 30px;border-top:1px solid #dddddd;">
                            <p style="font-family:Arial,sans-serif;font-size:12px;color:#888888;margin:0;text-align:center;">
                                Sistema de Reposicion CRISA | Grupo CRISA - La Tijera Textil
                            </p>
                        </td>
                    </tr>
                    
                </table>
            </td>
        </tr>
    </table>
</body>
</html>
    """
    return html

def generar_html_resumen_comercial(dias=30):
    """Genera HTML del resumen comercial - Compatible Outlook"""
    alertas = get_datos_alertas(dias)
    fecha = datetime.now().strftime("%d/%m/%Y %H:%M")
    
    criticas = [a for a in alertas if a['valor'] >= 50000000]
    precaucion = [a for a in alertas if 20000000 <= a['valor'] < 50000000]
    estables = [a for a in alertas if a['valor'] < 20000000]
    
    total_criticas = sum(a['valor'] for a in criticas)
    total_precaucion = sum(a['valor'] for a in precaucion)
    total_estables = sum(a['valor'] for a in estables)
    total_general = total_criticas + total_precaucion + total_estables
    total_articulos = sum(a['articulos'] for a in alertas)
    
    # Detalle sucursales críticas
    detalle_criticas = ""
    for a in sorted(criticas, key=lambda x: x['valor'], reverse=True):
        detalle_criticas += f"""
        <tr>
            <td style="padding:8px 0;font-family:Arial,sans-serif;font-size:13px;color:#333333;border-bottom:1px solid #f0f0f0;">
                {a['sucursal']}
            </td>
            <td style="padding:8px 0;font-family:Arial,sans-serif;font-size:13px;font-weight:bold;color:{COLORS['red']};text-align:right;border-bottom:1px solid #f0f0f0;">
                ${a['valor']:,.0f}
            </td>
        </tr>
        """
    
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
</head>
<body style="margin:0;padding:0;background-color:#e5e5e5;font-family:Arial,Helvetica,sans-serif;">
    
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="background-color:#e5e5e5;">
        <tr>
            <td align="center" style="padding:30px 15px;">
                
                <table role="presentation" width="650" cellspacing="0" cellpadding="0" border="0" style="background-color:#ffffff;border:1px solid #dddddd;">
                    
                    <!-- Header -->
                    <tr>
                        <td style="background-color:{COLORS['header_bg']};padding:30px;text-align:center;">
                            <img src="cid:logo" alt="GRUPO CRISA" width="180" style="display:block;margin:0 auto 15px;">
                            <h1 style="color:#ffffff;margin:0;font-family:Arial,sans-serif;font-size:22px;font-weight:bold;">
                                RESUMEN COMERCIAL
                            </h1>
                            <p style="color:#cccccc;margin:10px 0 0;font-family:Arial,sans-serif;font-size:13px;">
                                Reporte: {fecha} | Proyeccion: {dias} dias
                            </p>
                        </td>
                    </tr>
                    
                    <!-- Título -->
                    <tr>
                        <td style="padding:25px 30px 15px;">
                            <h3 style="font-family:Arial,sans-serif;font-size:16px;color:#333333;margin:0;font-weight:bold;">
                                Inversion Requerida en Reposicion
                            </h3>
                        </td>
                    </tr>
                    
                    <!-- Grupo Críticas -->
                    <tr>
                        <td style="padding:0 30px 15px;">
                            <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="border-left:4px solid {COLORS['red']};background-color:#fff5f5;">
                                <tr>
                                    <td style="padding:15px 20px;">
                                        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0">
                                            <tr>
                                                <td style="font-family:Arial,sans-serif;font-size:15px;font-weight:bold;color:#333333;">
                                                    Sucursales Criticas
                                                    <br><span style="font-size:12px;font-weight:normal;color:#666666;">{len(criticas)} sucursales requieren atencion urgente</span>
                                                </td>
                                                <td style="font-family:Arial,sans-serif;font-size:24px;font-weight:bold;color:{COLORS['red']};text-align:right;">
                                                    ${total_criticas:,.0f}
                                                </td>
                                            </tr>
                                        </table>
                                        {f'<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="margin-top:15px;padding-top:15px;border-top:1px solid #ffcccc;">{detalle_criticas}</table>' if detalle_criticas else ''}
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    
                    <!-- Grupo Precaución -->
                    <tr>
                        <td style="padding:0 30px 15px;">
                            <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="border-left:4px solid {COLORS['yellow']};background-color:#fffbf0;">
                                <tr>
                                    <td style="padding:15px 20px;">
                                        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0">
                                            <tr>
                                                <td style="font-family:Arial,sans-serif;font-size:15px;font-weight:bold;color:#333333;">
                                                    Sucursales en Precaucion
                                                    <br><span style="font-size:12px;font-weight:normal;color:#666666;">{len(precaucion)} sucursales a monitorear</span>
                                                </td>
                                                <td style="font-family:Arial,sans-serif;font-size:24px;font-weight:bold;color:#b38600;text-align:right;">
                                                    ${total_precaucion:,.0f}
                                                </td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    
                    <!-- Grupo Estables -->
                    <tr>
                        <td style="padding:0 30px 20px;">
                            <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="border-left:4px solid {COLORS['green']};background-color:#f0fff4;">
                                <tr>
                                    <td style="padding:15px 20px;">
                                        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0">
                                            <tr>
                                                <td style="font-family:Arial,sans-serif;font-size:15px;font-weight:bold;color:#333333;">
                                                    Sucursales Estables
                                                    <br><span style="font-size:12px;font-weight:normal;color:#666666;">{len(estables)} sucursales con stock adecuado</span>
                                                </td>
                                                <td style="font-family:Arial,sans-serif;font-size:24px;font-weight:bold;color:{COLORS['green']};text-align:right;">
                                                    ${total_estables:,.0f}
                                                </td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    
                    <!-- Resumen ejecutivo -->
                    <tr>
                        <td style="padding:0 30px 25px;">
                            <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="background-color:{COLORS['primary']};">
                                <tr>
                                    <td style="padding:20px;">
                                        <h3 style="font-family:Arial,sans-serif;font-size:14px;color:#ffffff;margin:0 0 15px;font-weight:bold;">Resumen Ejecutivo</h3>
                                        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0">
                                            <tr>
                                                <td style="padding:8px 0;font-family:Arial,sans-serif;font-size:13px;color:#aaaaaa;border-bottom:1px solid #4a5568;">Articulos a Reponer</td>
                                                <td style="padding:8px 0;font-family:Arial,sans-serif;font-size:13px;color:#ffffff;text-align:right;border-bottom:1px solid #4a5568;font-weight:bold;">{total_articulos}</td>
                                            </tr>
                                            <tr>
                                                <td style="padding:8px 0;font-family:Arial,sans-serif;font-size:13px;color:#aaaaaa;border-bottom:1px solid #4a5568;">Prioridad 1 (Criticas)</td>
                                                <td style="padding:8px 0;font-family:Arial,sans-serif;font-size:13px;color:#ff8a8a;text-align:right;border-bottom:1px solid #4a5568;font-weight:bold;">${total_criticas:,.0f}</td>
                                            </tr>
                                            <tr>
                                                <td style="padding:8px 0;font-family:Arial,sans-serif;font-size:13px;color:#aaaaaa;border-bottom:1px solid #4a5568;">Prioridad 2 (Precaucion)</td>
                                                <td style="padding:8px 0;font-family:Arial,sans-serif;font-size:13px;color:#ffe066;text-align:right;border-bottom:1px solid #4a5568;font-weight:bold;">${total_precaucion:,.0f}</td>
                                            </tr>
                                            <tr>
                                                <td style="padding:12px 0 5px;font-family:Arial,sans-serif;font-size:16px;color:#ffffff;font-weight:bold;">MONTO TOTAL</td>
                                                <td style="padding:12px 0 5px;font-family:Arial,sans-serif;font-size:20px;color:#ffffff;text-align:right;font-weight:bold;">${total_general:,.0f}</td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    
                    <!-- Botón -->
                    <tr>
                        <td style="padding:10px 30px 30px;text-align:center;">
                            <table role="presentation" cellspacing="0" cellpadding="0" border="0" align="center">
                                <tr>
                                    <td style="background-color:{COLORS['primary']};padding:12px 30px;">
                                        <a href="{PUBLIC_APP_URL}" style="color:#ffffff;font-family:Arial,sans-serif;font-size:14px;text-decoration:none;font-weight:bold;">
                                            Ver Dashboard en Tiempo Real
                                        </a>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    
                    <!-- Footer -->
                    <tr>
                        <td style="background-color:#f5f5f5;padding:20px 30px;border-top:1px solid #dddddd;">
                            <p style="font-family:Arial,sans-serif;font-size:12px;color:#888888;margin:0;text-align:center;">
                                Sistema de Reposicion CRISA | Grupo CRISA - La Tijera Textil
                            </p>
                        </td>
                    </tr>
                    
                </table>
            </td>
        </tr>
    </table>
</body>
</html>
    """
    return html

def enviar_email(destinatario, asunto, html_content):
    """Envía un email con contenido HTML y logo adjunto como CID"""
    try:
        msg = MIMEMultipart('related')
        msg['Subject'] = asunto
        msg['From'] = f"CRISA Reportes <{SMTP_USER}>"
        msg['To'] = destinatario
        
        # Parte HTML
        msg_alternative = MIMEMultipart('alternative')
        msg.attach(msg_alternative)
        
        # Texto plano
        texto_plano = "Este correo requiere un cliente que soporte HTML. Visite {PUBLIC_APP_URL}"
        part1 = MIMEText(texto_plano, 'plain', 'utf-8')
        part2 = MIMEText(html_content, 'html', 'utf-8')
        
        msg_alternative.attach(part1)
        msg_alternative.attach(part2)
        
        # Adjuntar logo como imagen embebida
        logo_paths = ["static/logo_optimized.png", "static/logo_email.png", "static/logo.png"]
        for logo_path in logo_paths:
            try:
                with open(logo_path, 'rb') as f:
                    img = MIMEImage(f.read())
                    img.add_header('Content-ID', '<logo>')
                    img.add_header('Content-Disposition', 'inline', filename='logo.png')
                    msg.attach(img)
                    break
            except FileNotFoundError:
                continue
        
        # Conectar y enviar
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, destinatario, msg.as_string())
        
        return {"success": True, "message": f"Email enviado exitosamente a {destinatario}"}
    except Exception as e:
        return {"success": False, "error": str(e)}

def enviar_resumen_general(destinatario, dias=30):
    """Envía el resumen general por email"""
    asunto = f"CRISA - Reporte de Reposicion ({dias} dias)"
    html = generar_html_resumen_general(dias)
    return enviar_email(destinatario, asunto, html)

def enviar_alerta_sucursal(destinatario, sucursal, dias=30):
    """Envía alerta de sucursal específica por email"""
    asunto = f"ALERTA URGENTE - Sucursal {sucursal.upper()}"
    html = generar_html_alerta_sucursal(sucursal, dias)
    if html is None:
        return {"success": False, "error": f"Sucursal {sucursal} no encontrada"}
    return enviar_email(destinatario, asunto, html)

def enviar_resumen_comercial(destinatario, dias=30):
    """Envía el resumen comercial por email"""
    asunto = f"CRISA - Resumen Comercial de Reposicion ({dias} dias)"
    html = generar_html_resumen_comercial(dias)
    return enviar_email(destinatario, asunto, html)

if __name__ == "__main__":
    print("Modulo de Alertas por Email - Compatible Outlook 2007+")
    print(f"SMTP: {SMTP_SERVER}:{SMTP_PORT}")
    print(f"Usuario: {SMTP_USER}")


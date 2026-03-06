"""
CRISA: Sistema de Alertas de Stock por WhatsApp
Envía notificaciones automáticas usando la API de Twilio
"""
import os
from datetime import datetime
from twilio.rest import Client

PUBLIC_APP_URL = os.environ.get("PUBLIC_APP_URL", "http://localhost:5173")

def get_twilio_credentials():
    """Obtener credenciales de Twilio desde variables de entorno"""
    account_sid = os.environ.get('TWILIO_ACCOUNT_SID')
    api_key = os.environ.get('TWILIO_API_KEY')
    api_key_secret = os.environ.get('TWILIO_API_KEY_SECRET')
    auth_token = os.environ.get('TWILIO_AUTH_TOKEN')
    phone_number = os.environ.get('TWILIO_WHATSAPP_FROM')

    if account_sid and api_key and api_key_secret:
        return {
            'account_sid': account_sid,
            'api_key': api_key,
            'api_key_secret': api_key_secret,
            'auth_token': None,
            'phone_number': phone_number
        }
    if account_sid and auth_token:
        return {
            'account_sid': account_sid,
            'api_key': None,
            'api_key_secret': None,
            'auth_token': auth_token,
            'phone_number': phone_number
        }
    raise Exception('Twilio no est� configurado. Defin� TWILIO_ACCOUNT_SID y TWILIO_AUTH_TOKEN (o API key/secret).')

def get_twilio_client():
    """Crear cliente de Twilio autenticado"""
    creds = get_twilio_credentials()
    if creds.get('api_key') and creds.get('api_key_secret'):
        return Client(creds['api_key'], creds['api_key_secret'], creds['account_sid'])
    return Client(creds['account_sid'], creds['auth_token'])

def get_twilio_from_number():
    """Obtener n�mero de origen de Twilio"""
    creds = get_twilio_credentials()
    return creds['phone_number']
DATOS_SUCURSALES = [
    {
        "sucursal": "MENDOZA",
        "color": "rojo",
        "valor": 130925551,
        "articulos_criticos": 636,
        "categorias": 7
    },
    {
        "sucursal": "SAN LUIS",
        "color": "rojo",
        "valor": 50395644,
        "articulos_criticos": 354,
        "categorias": 7
    },
    {
        "sucursal": "CRISA 2",
        "color": "amarillo",
        "valor": 36242511,
        "articulos_criticos": 277,
        "categorias": 6
    },
    {
        "sucursal": "SAN JUAN",
        "color": "verde",
        "valor": 14823585,
        "articulos_criticos": 273,
        "categorias": 8
    }
]

def get_emoji_color(color):
    """Obtener emoji según el color de alerta"""
    emojis = {
        "rojo": "🔴",
        "amarillo": "🟡",
        "verde": "🟢"
    }
    return emojis.get(color, "⚪")

def format_currency(valor):
    """Formatear valor como moneda argentina"""
    return f"${valor:,.0f}".replace(",", ".")

def generar_mensaje_resumen_general(datos=None):
    """
    Plantilla A: Resumen General Ejecutivo
    Para el Jefe del Centro de Distribución
    """
    if datos is None:
        datos = DATOS_SUCURSALES
    
    fecha = datetime.now().strftime("%d/%m/%Y %H:%M")
    
    total_valor = sum(s["valor"] for s in datos)
    total_articulos = sum(s["articulos_criticos"] for s in datos)
    sucursales_rojas = len([s for s in datos if s["color"] == "rojo"])
    
    mensaje = f"""*📊 CRISA - Reporte de Reposición*
_Generado: {fecha}_

*🏢 Estado General de Sucursales:*

"""
    
    for suc in sorted(datos, key=lambda x: x["valor"], reverse=True):
        emoji = get_emoji_color(suc["color"])
        mensaje += f"{emoji} *{suc['sucursal']}*\n"
        mensaje += f"   💰 Valor: {format_currency(suc['valor'])}\n"
        mensaje += f"   📦 Art. críticos: {suc['articulos_criticos']}\n"
        mensaje += f"   🏷️ Categorías: {suc['categorias']}\n\n"
    
    mensaje += f"""─────────────────
*📈 TOTALES:*
💵 Monto total: *{format_currency(total_valor)}*
📦 Artículos críticos: *{total_articulos}*
🔴 Sucursales en alerta roja: *{sucursales_rojas}*

👉 Ver dashboard completo:
{PUBLIC_APP_URL}
"""
    
    return mensaje

def generar_mensaje_alerta_sucursal(sucursal_data):
    """
    Plantilla B: Alerta Específica por Sucursal
    Para encargados de sucursales en estado rojo
    """
    fecha = datetime.now().strftime("%d/%m/%Y %H:%M")
    emoji = get_emoji_color(sucursal_data["color"])
    
    mensaje = f"""*⚠️ ALERTA URGENTE DE STOCK*
{emoji} *Sucursal: {sucursal_data['sucursal']}*
_Fecha: {fecha}_

*📋 Situación Actual:*
• Valor a reponer: *{format_currency(sucursal_data['valor'])}*
• Artículos críticos: *{sucursal_data['articulos_criticos']}*
• Categorías afectadas: *{sucursal_data['categorias']}*

*🎯 Acción Requerida:*
Se requiere atención inmediata para evitar quiebre de stock.

*📱 Revisar detalles:*
{PUBLIC_APP_URL}

_Por favor, coordine con el Centro de Distribución lo antes posible._
"""
    
    return mensaje

def generar_mensaje_comercial(datos=None):
    """
    Plantilla C: Resumen Comercial
    Montos totales para el área comercial
    """
    if datos is None:
        datos = DATOS_SUCURSALES
    
    fecha = datetime.now().strftime("%d/%m/%Y %H:%M")
    
    total_valor = sum(s["valor"] for s in datos)
    total_articulos = sum(s["articulos_criticos"] for s in datos)
    
    rojas = [s for s in datos if s["color"] == "rojo"]
    amarillas = [s for s in datos if s["color"] == "amarillo"]
    verdes = [s for s in datos if s["color"] == "verde"]
    
    valor_rojas = sum(s["valor"] for s in rojas)
    valor_amarillas = sum(s["valor"] for s in amarillas)
    valor_verdes = sum(s["valor"] for s in verdes)
    
    mensaje = f"""*💼 CRISA - Resumen Comercial*
_Reporte: {fecha}_

*💰 Inversión Requerida en Reposición:*

🔴 *Sucursales Críticas:* {format_currency(valor_rojas)}
   ({len(rojas)} sucursales)

🟡 *Sucursales en Precaución:* {format_currency(valor_amarillas)}
   ({len(amarillas)} sucursales)

🟢 *Sucursales Estables:* {format_currency(valor_verdes)}
   ({len(verdes)} sucursales)

─────────────────
*📊 RESUMEN EJECUTIVO:*
💵 *Monto Total:* {format_currency(total_valor)}
📦 *Artículos a Reponer:* {total_articulos}

*🎯 Prioridad de Inversión:*
1. Sucursales en rojo: 🔴 {format_currency(valor_rojas)}
2. Sucursales en amarillo: 🟡 {format_currency(valor_amarillas)}

👉 Dashboard en tiempo real:
{PUBLIC_APP_URL}
"""
    
    return mensaje

def enviar_whatsapp(numero_destino, mensaje):
    """
    Enviar mensaje de WhatsApp usando Twilio
    numero_destino: formato 'whatsapp:+549261XXXXXXX'
    """
    try:
        client = get_twilio_client()
        from_number = get_twilio_from_number()
        
        if not numero_destino.startswith('whatsapp:'):
            numero_destino = f'whatsapp:{numero_destino}'
        
        if not from_number.startswith('whatsapp:'):
            from_whatsapp = f'whatsapp:{from_number}'
        else:
            from_whatsapp = from_number
        
        message = client.messages.create(
            body=mensaje,
            from_=from_whatsapp,
            to=numero_destino
        )
        
        return {
            "success": True,
            "sid": message.sid,
            "status": message.status
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

def enviar_resumen_general(numero_destino, datos=None):
    """Enviar reporte ejecutivo general"""
    mensaje = generar_mensaje_resumen_general(datos)
    return enviar_whatsapp(numero_destino, mensaje)

def enviar_alerta_sucursal(numero_destino, sucursal_nombre, datos=None):
    """Enviar alerta específica de una sucursal"""
    if datos is None:
        datos = DATOS_SUCURSALES
    
    sucursal = next((s for s in datos if s["sucursal"].upper() == sucursal_nombre.upper()), None)
    if not sucursal:
        return {"success": False, "error": f"Sucursal {sucursal_nombre} no encontrada"}
    
    mensaje = generar_mensaje_alerta_sucursal(sucursal)
    return enviar_whatsapp(numero_destino, mensaje)

def enviar_alertas_sucursales_rojas(numeros_por_sucursal, datos=None):
    """
    Enviar alertas a todas las sucursales en rojo
    numeros_por_sucursal: dict {"MENDOZA": "+549261XXX", "SAN LUIS": "+549261XXX"}
    """
    if datos is None:
        datos = DATOS_SUCURSALES
    
    resultados = []
    sucursales_rojas = [s for s in datos if s["color"] == "rojo"]
    
    for sucursal in sucursales_rojas:
        nombre = sucursal["sucursal"]
        if nombre in numeros_por_sucursal:
            resultado = enviar_alerta_sucursal(numeros_por_sucursal[nombre], nombre, datos)
            resultados.append({
                "sucursal": nombre,
                **resultado
            })
    
    return resultados

def enviar_resumen_comercial(numero_destino, datos=None):
    """Enviar resumen comercial"""
    mensaje = generar_mensaje_comercial(datos)
    return enviar_whatsapp(numero_destino, mensaje)

def obtener_datos_desde_db():
    """
    Obtener datos actualizados desde la base de datos
    Usar esta función para datos en tiempo real
    """
    try:
        import database as db
        resumen = db.get_resumen_reposicion(dias=30)
        
        if not resumen or not resumen.get('cards'):
            return DATOS_SUCURSALES
        
        datos = []
        for card in resumen['cards']:
            valor = card.get('valor', 0)
            
            if valor > 50000000:
                color = "rojo"
            elif valor > 20000000:
                color = "amarillo"
            else:
                color = "verde"
            
            datos.append({
                "sucursal": card.get('sucursal', 'N/A'),
                "color": color,
                "valor": valor,
                "articulos_criticos": card.get('articulos_criticos', 0),
                "categorias": card.get('grupos', 0)
            })
        
        return datos if datos else DATOS_SUCURSALES
    except Exception as e:
        print(f"Error obteniendo datos de DB: {e}")
        return DATOS_SUCURSALES


if __name__ == "__main__":
    print("=" * 50)
    print("CRISA - Sistema de Alertas WhatsApp")
    print("=" * 50)
    
    print("\n📊 PLANTILLA A - Resumen General:")
    print("-" * 40)
    print(generar_mensaje_resumen_general())
    
    print("\n⚠️ PLANTILLA B - Alerta Sucursal (MENDOZA):")
    print("-" * 40)
    print(generar_mensaje_alerta_sucursal(DATOS_SUCURSALES[0]))
    
    print("\n💼 PLANTILLA C - Resumen Comercial:")
    print("-" * 40)
    print(generar_mensaje_comercial())






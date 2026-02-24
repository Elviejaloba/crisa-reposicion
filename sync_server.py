"""
Servidor de sincronización que corre en el puerto 5000 (expuesto públicamente).
Este servidor actúa como proxy hacia el API principal en puerto 8001.
"""
from flask import Flask, request, jsonify
import requests
import threading

app = Flask(__name__)
API_URL = "http://localhost:8001"

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy", "proxy": True})

@app.route('/sync', methods=['POST'])
def sync():
    try:
        response = requests.post(
            f"{API_URL}/sync",
            data=request.data,
            headers={'Content-Type': 'application/json'},
            timeout=120
        )
        return response.text, response.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/sync-info', methods=['GET'])
def sync_info():
    try:
        response = requests.get(f"{API_URL}/sync-info", timeout=30)
        return response.text, response.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/recalcular-metricas', methods=['POST'])
def recalcular_metricas():
    try:
        response = requests.post(f"{API_URL}/recalcular-metricas", timeout=120)
        return response.text, response.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def run_server():
    app.run(host='0.0.0.0', port=5000, threaded=True)

if __name__ == '__main__':
    print("Iniciando servidor de sincronización en puerto 5000...")
    run_server()

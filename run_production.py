import subprocess
import sys
import os
import signal
import time
import urllib.request

processes = []

def cleanup(signum, frame):
    for p in processes:
        try:
            p.terminate()
        except Exception:
            pass
    sys.exit(0)

signal.signal(signal.SIGTERM, cleanup)
signal.signal(signal.SIGINT, cleanup)

env = os.environ.copy()

api = subprocess.Popen([
    sys.executable, "-m", "uvicorn", "main:app",
    "--host", "0.0.0.0", "--port", "8001"
], env=env)
processes.append(api)

time.sleep(2)

streamlit = subprocess.Popen([
    sys.executable, "-m", "streamlit", "run", "app.py",
    "--server.port", "8000",
    "--server.address", "0.0.0.0",
    "--server.headless", "true",
    "--server.enableCORS", "false",
    "--server.enableXsrfProtection", "false",
    "--server.baseUrlPath", "",
], env=env)
processes.append(streamlit)

for _ in range(30):
    try:
        resp = urllib.request.urlopen("http://127.0.0.1:8000/_stcore/health", timeout=2)
        if resp.status == 200:
            break
    except Exception:
        pass
    time.sleep(1)

streamlit.wait()

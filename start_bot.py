"""
start_bot.py — Arranca DataTalk + ngrok y muestra la URL para configurar en Azure Bot.

Uso:
    python start_bot.py

Requiere:
    pip install pyngrok botbuilder-integration-aiohttp botbuilder-core botbuilder-schema
    ngrok authtoken <tu_token>  (solo la primera vez)
"""

import os
import sys
import time
import subprocess
import threading
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def check_env():
    required = [
        "AZURE_OPENAI_ENDPOINT",
        "AZURE_OPENAI_API_KEY",
        "AZURE_OPENAI_DEPLOYMENT_NAME",
        "MICROSOFT_APP_ID",
        "MICROSOFT_APP_PASSWORD",
    ]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        print("❌ Faltan variables de entorno en .env:")
        for k in missing:
            print(f"   {k}")
        print("\nCopiá .env.example a .env y completá los valores.")
        sys.exit(1)
    print("✅ Variables de entorno OK")


def start_ngrok(port: int = 8000) -> str:
    """Inicia ngrok y retorna la URL pública HTTPS."""
    try:
        from pyngrok import ngrok, conf
        
        # Si tenés authtoken configurado en ngrok, ya funciona.
        # Si no: ngrok.set_auth_token("TU_TOKEN_AQUI")
        
        tunnel = ngrok.connect(port, "http")
        url = tunnel.public_url
        
        # ngrok siempre da HTTP, necesitamos HTTPS
        if url.startswith("http://"):
            url = url.replace("http://", "https://")
        
        return url
    except ImportError:
        print("❌ pyngrok no está instalado. Corré: pip install pyngrok")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error iniciando ngrok: {e}")
        print("   Asegurate de haber configurado el authtoken: ngrok config add-authtoken <TOKEN>")
        sys.exit(1)


def start_uvicorn(port: int = 8000):
    """Arranca el servidor FastAPI en un thread separado."""
    def run():
        import uvicorn
        uvicorn.run(
            "datatalk.api.main:app",
            host="0.0.0.0",
            port=port,
            reload=False,  # reload=False para producción con bot
            log_level="info",
        )
    
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread


def main():
    print("=" * 60)
    print("  DataTalk Bot — Iniciando...")
    print("=" * 60)
    
    # 1. Verificar variables de entorno
    check_env()
    
    port = int(os.environ.get("PORT", 8000))
    
    # 2. Arrancar FastAPI
    print(f"\n⚡ Iniciando API en puerto {port}...")
    start_uvicorn(port)
    time.sleep(3)  # Esperar que levante
    print("✅ API lista")
    
    # 3. Arrancar ngrok
    print("\n🌐 Iniciando ngrok...")
    ngrok_url = start_ngrok(port)
    messaging_endpoint = f"{ngrok_url}/api/messages"
    
    print("\n" + "=" * 60)
    print("  ✅ DataTalk Bot está corriendo!")
    print("=" * 60)
    print(f"\n  URL pública (ngrok):   {ngrok_url}")
    print(f"  Messaging Endpoint:    {messaging_endpoint}")
    print(f"\n  📋 PASO SIGUIENTE:")
    print(f"  1. Abrí Azure Portal → datatalk-bot → Configuration")
    print(f"  2. En 'Messaging endpoint' pegá:")
    print(f"     {messaging_endpoint}")
    print(f"  3. Guardá y ya podés probar el bot en Teams!")
    print(f"\n  API Docs: http://localhost:{port}/docs")
    print(f"  Health:   http://localhost:{port}/health")
    print("\n  [Ctrl+C para detener]\n")
    
    # 4. Mantener vivo
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n👋 DataTalk Bot detenido.")
        from pyngrok import ngrok
        ngrok.kill()


if __name__ == "__main__":
    main()
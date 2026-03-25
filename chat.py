"""
DataTalk — Prueba interactiva del Orquestador
Corre desde CMD: python chat.py

Permite hacer preguntas en lenguaje natural sobre los 3 archivos de datos.
"""

import sys
import os
from pathlib import Path

# Asegurar que Python encuentra el paquete datatalk
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

from datatalk.agents import orchestrator

# ── Archivos disponibles ────────────────────────────────────────────────────
ARCHIVOS = {
    "1": ("ventas_retail.xlsx",      "Ventas retail — 18 meses, caída en Lácteos"),
    "2": ("logistica_entregas.xlsx", "Logística — demoras por zona"),
    "3": ("rrhh_asistencia.xlsx",    "RRHH — ausentismo por área (SENSIBLE)"),
}

def seleccionar_archivo() -> str:
    print("\n╔══════════════════════════════════════════════╗")
    print("║         DataTalk — Agente Analítico          ║")
    print("╚══════════════════════════════════════════════╝")
    print("\nArchivos disponibles:")
    for key, (nombre, desc) in ARCHIVOS.items():
        print(f"  [{key}] {nombre} — {desc}")
    print()

    while True:
        opcion = input("Seleccioná un archivo (1-3): ").strip()
        if opcion in ARCHIVOS:
            nombre, desc = ARCHIVOS[opcion]
            path = Path(__file__).parent / "datatalk" / "data" / nombre
            if not path.exists():
                print(f"  ⚠ Archivo no encontrado: {path}")
                print("  Ejecutá primero: python datatalk/data/generate_all_data.py")
                continue
            print(f"\n  ✓ Archivo cargado: {nombre}")
            return str(path)
        print("  Opción inválida. Ingresá 1, 2 o 3.")


def mostrar_resultado(result: dict):
    print("\n" + "─" * 50)

    # Intent
    print(f"  Intención detectada : {result.get('intent', '—')}")
    print(f"  Intentos usados     : {result.get('attempts', '—')}")

    if result.get("autocorrected"):
        print("  ⚡ Autocorregido automáticamente")

    if result.get("warnings"):
        for w in result["warnings"]:
            print(f"  ⚠ {w}")

    # SQL
    sql = result.get("sql", "")
    if sql:
        print(f"\n  SQL generado:")
        for line in sql.strip().splitlines():
            print(f"    {line}")

    print("─" * 50)

    # Resultado
    if not result.get("success"):
        print(f"\n  ✗ {result.get('explanation') or result.get('user_message')}")
        return

    df = result.get("data")
    if df is not None and not df.empty:
        print(f"\n  Resultado ({len(df)} filas):")
        print()
        # Formatear tabla simple
        col_widths = {col: max(len(str(col)), df[col].astype(str).str.len().max()) for col in df.columns}
        header = "  " + "  ".join(str(col).ljust(col_widths[col]) for col in df.columns)
        print(header)
        print("  " + "-" * (sum(col_widths.values()) + 2 * len(df.columns)))
        for _, row in df.iterrows():
            print("  " + "  ".join(str(row[col]).ljust(col_widths[col]) for col in df.columns))
    else:
        print("\n  Sin resultados para esta consulta.")

    # Explicación
    explanation = result.get("explanation", "").strip()
    if explanation:
        print(f"\n  Conclusión:")
        # Wrap a 60 chars
        words = explanation.split()
        line = "  "
        for word in words:
            if len(line) + len(word) > 62:
                print(line)
                line = "  " + word + " "
            else:
                line += word + " "
        if line.strip():
            print(line)

    # Dashboard
    chart = result.get("chart")
    if chart and chart.get("success"):
        print(f"\n  Gráfico generado: {chart.get('chart_type', 'bar')}")
        if chart.get("png_base64"):
            # Guardar PNG para ver
            import base64
            png_path = Path(__file__).parent / "chart_resultado.png"
            with open(png_path, "wb") as f:
                f.write(base64.b64decode(chart["png_base64"]))
            print(f"  PNG guardado en: {png_path.name}")
            print(f"  Abrilo con: start {png_path.name}")


def main():
    file_path = seleccionar_archivo()

    print("\n" + "═" * 50)
    print("  Escribí tu pregunta en español.")
    print("  Comandos: 'cambiar' (otro archivo) | 'salir'")
    print("═" * 50)

    # Preguntas de ejemplo según el archivo
    nombre_archivo = Path(file_path).name
    if "ventas" in nombre_archivo:
        ejemplos = [
            "¿Cuáles son las 3 categorías con más ventas?",
            "¿Cómo evolucionaron las ventas mes a mes?",
            "¿Por qué cayeron las ventas de Lácteos?",
        ]
    elif "logistica" in nombre_archivo:
        ejemplos = [
            "¿Qué zona tiene más demoras?",
            "Comparar el porcentaje de demoras entre zona Norte y el resto",
            "¿Cuántos pedidos hay por transportista?",
        ]
    else:
        ejemplos = [
            "¿Cuál es el ausentismo promedio por área?",
            "¿Qué área tiene más ausencias?",
            "Total de días de ausencia por área",
        ]

    print("\n  Ejemplos de preguntas:")
    for ej in ejemplos:
        print(f"    → {ej}")
    print()

    while True:
        try:
            pregunta = input("Pregunta: ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\n\n  Hasta luego.")
            break

        if not pregunta:
            continue

        if pregunta.lower() == "salir":
            print("\n  Hasta luego.")
            break

        if pregunta.lower() == "cambiar":
            file_path = seleccionar_archivo()
            continue

        # Detectar si pide gráfico
        pide_grafico = any(k in pregunta.lower() for k in
                           ["grafico", "gráfico", "dashboard", "chart", "visual", "graficame"])

        print("\n  Procesando...", end="", flush=True)

        try:
            result = orchestrator.run(
                question=pregunta,
                file_path=file_path,
                generate_chart=pide_grafico,
            )
            print(" listo.")
            mostrar_resultado(result)
        except Exception as e:
            print(f"\n  Error: {e}")
            print("  Verificá que el .env tiene las credenciales de Azure OpenAI.")

        print()


if __name__ == "__main__":
    main()

"""
DataTalk — Prueba interactiva del Orquestador
Corre desde CMD: python chat.py

Permite hacer preguntas en lenguaje natural sobre los 3 archivos de datos.
"""

import sys
import os
import textwrap
import base64
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
    LINE = "─" * 52

    # ── Encabezado ──────────────────────────────────────
    print(f"\n{LINE}")
    intent = result.get("intent", "—")
    intentos = result.get("attempts", "—")
    autocorr = "  ⚡ Autocorregido" if result.get("autocorrected") else ""
    print(f"  Intención : {intent}   |   Intentos : {intentos}{autocorr}")

    # Advertencias del schema
    for w in result.get("warnings", []):
        print(f"  ⚠  {w}")

    # ── SQL generado ─────────────────────────────────────
    sql = result.get("sql", "").strip()
    if sql:
        print(f"\n  SQL generado:")
        print(f"  {'─'*46}")
        for line in sql.splitlines():
            print(f"    {line}")
        print(f"  {'─'*46}")

    # ── Error ────────────────────────────────────────────
    if not result.get("success"):
        print(f"\n  ✗ {result.get('explanation') or result.get('user_message')}")
        print(LINE)
        return

    # ── Tabla de resultados (máx 10 filas) ───────────────
    df = result.get("data")
    if df is not None and not df.empty:
        total = len(df)
        df_show = df.head(10)

        print(f"\n  Resultados — {total} fila{'s' if total != 1 else ''} totales"
              + (f" (mostrando primeras 10)" if total > 10 else "") + ":\n")

        # Calcular anchos de columna
        col_widths = {}
        for col in df_show.columns:
            max_data = df_show[col].astype(str).str.len().max()
            col_widths[col] = max(len(str(col)), max_data, 6)

        # Encabezado de tabla
        header = "  " + "  ".join(str(col).ljust(col_widths[col]) for col in df_show.columns)
        sep    = "  " + "  ".join("─" * col_widths[col] for col in df_show.columns)
        print(header)
        print(sep)

        # Filas
        for _, row in df_show.iterrows():
            print("  " + "  ".join(str(row[col]).ljust(col_widths[col]) for col in df_show.columns))

        if total > 10:
            print(f"\n  ... y {total - 10} filas más")
    else:
        print("\n  Sin resultados para esta consulta.")

    # ── Conclusión de negocio ────────────────────────────
    explanation = result.get("explanation", "").strip()
    if explanation:
        print(f"\n{LINE}")
        print(f"  💡 Conclusión de negocio:\n")
        for line in textwrap.wrap(explanation, width=56):
            print(f"     {line}")

    # ── Gráfico ──────────────────────────────────────────
    chart = result.get("chart")
    if chart and chart.get("success"):
        chart_type = chart.get("chart_type", "bar")
        print(f"\n  📊 Gráfico generado: {chart_type}")
        if chart.get("png_base64"):
            png_path = Path(__file__).parent / "chart_resultado.png"
            with open(png_path, "wb") as f:
                f.write(base64.b64decode(chart["png_base64"]))
            print(f"  PNG guardado → {png_path.name}")
            print(f"  Abrilo con : start {png_path.name}")

    print(f"{LINE}\n")


def main():
    file_path = seleccionar_archivo()

    print("\n" + "═" * 52)
    print("  Escribí tu pregunta en español.")
    print("  Comandos: 'cambiar' (otro archivo) | 'salir'")
    print("  Tip: agregá 'gráfico' para generar un chart.")
    print("═" * 52)

    nombre_archivo = Path(file_path).name
    if "ventas" in nombre_archivo:
        ejemplos = [
            "¿Cuáles son las 3 categorías con más ventas?",
            "¿Cómo evolucionaron las ventas mes a mes?",
            "¿Por qué cayeron las ventas de Lácteos?",
            "Mostrame un gráfico de ventas por zona",
        ]
    elif "logistica" in nombre_archivo:
        ejemplos = [
            "¿Qué zona tiene más demoras?",
            "Comparar el porcentaje de demoras entre zona Norte y el resto",
            "¿Cuántos pedidos hay por transportista?",
            "Mostrame un gráfico de demoras por zona",
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

        pide_grafico = any(k in pregunta.lower() for k in
                           ["grafico", "gráfico", "dashboard", "chart", "visual", "graficame"])

        print("\n  ⏳ Procesando...", end="", flush=True)

        try:
            result = orchestrator.run(
                question=pregunta,
                file_path=file_path,
                generate_chart=pide_grafico,
            )
            print(" listo.\n")
            mostrar_resultado(result)
        except Exception as e:
            print(f"\n  ❌ Error: {e}")
            print("  Verificá que el .env tiene las credenciales de Azure OpenAI.\n")


if __name__ == "__main__":
    main()
"""
Genera los 3 archivos de datos sintéticos del proyecto:
  - ventas_retail.xlsx       (800 filas, caída Lácteos desde mayo)
  - logistica_entregas.xlsx  (600 filas, zona Norte con 40% demora)
  - rrhh_asistencia.xlsx     (500 filas, Operaciones con 3x ausentismo — sensible)
"""

import pandas as pd
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(42)
OUTPUT_DIR = Path(__file__).parent


def generar_ventas_retail():
    categorias = ["Lácteos", "Bebidas", "Panadería", "Limpieza", "Carnes"]
    zonas = ["Norte", "Sur", "Centro", "Este"]
    rows = []
    start = date(2023, 1, 1)
    for _ in range(800):
        d = start + timedelta(days=random.randint(0, 547))
        cat = random.choice(categorias)
        zona = random.choice(zonas)
        base = random.uniform(500, 5000)
        if cat == "Lácteos" and d >= date(2023, 5, 1):
            base *= 0.30
        rows.append({"Fecha Venta": d.isoformat(), "Categoría": cat, "Zona": zona,
                     "Ventas Total": round(base, 2), "Unidades": random.randint(5, 300),
                     "Descuento %": random.choice([0, 5, 10, 15])})
    df = pd.DataFrame(rows)
    path = OUTPUT_DIR / "ventas_retail.xlsx"
    df.to_excel(path, index=False)
    print(f"✓ {path.name} — {len(df)} filas")


def generar_logistica():
    zonas = ["Norte", "Sur", "Centro", "Este", "Oeste"]
    rows = []
    start = date(2024, 1, 1)
    for _ in range(600):
        d = start + timedelta(days=random.randint(0, 365))
        zona = random.choice(zonas)
        demorado = random.random() < (0.40 if zona == "Norte" else 0.10)
        tiempo = random.randint(1, 3) if not demorado else random.randint(5, 14)
        rows.append({"Fecha Entrega": d.isoformat(), "Zona": zona, "Demorado": demorado,
                     "Días Entrega": tiempo, "Pedidos": random.randint(10, 100),
                     "Transportista": random.choice(["TransNorte", "LogiSur", "RapidEx", "FlexLog"])})
    df = pd.DataFrame(rows)
    path = OUTPUT_DIR / "logistica_entregas.xlsx"
    df.to_excel(path, index=False)
    print(f"✓ {path.name} — {len(df)} filas")


def generar_rrhh():
    areas = ["Operaciones", "Ventas", "Administración", "Marketing", "IT"]
    rows = []
    start = date(2024, 1, 1)
    for _ in range(500):
        d = start + timedelta(days=random.randint(0, 365))
        area = random.choice(areas)
        ausente = random.random() < (0.30 if area == "Operaciones" else 0.10)
        rows.append({"Fecha": d.isoformat(), "Área": area,
                     "Empleado ID": f"EMP{random.randint(1000, 9999)}",
                     "Ausente": ausente,
                     "Horas Trabajadas": 0 if ausente else random.randint(6, 9),
                     "Turno": random.choice(["Mañana", "Tarde", "Noche"])})
    df = pd.DataFrame(rows)
    path = OUTPUT_DIR / "rrhh_asistencia.xlsx"
    df.to_excel(path, index=False)
    print(f"✓ {path.name} — {len(df)} filas")


if __name__ == "__main__":
    print("Generando archivos de datos sintéticos...")
    generar_ventas_retail()
    generar_logistica()
    generar_rrhh()
    print(f"\nTodos los archivos generados en: {OUTPUT_DIR}")

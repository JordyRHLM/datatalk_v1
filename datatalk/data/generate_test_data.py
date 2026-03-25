"""
Genera ventas_test.xlsx — 300 filas sintéticas para tests.
Se importa en test_schema_agent.py — si el archivo ya existe, no lo regenera.
"""

import pandas as pd
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(42)

_out = Path(__file__).parent / "ventas_test.xlsx"

if not _out.exists():
    categorias = ["Lácteos", "Bebidas", "Panadería", "Limpieza", "Carnes"]
    zonas = ["Norte", "Sur", "Centro", "Este"]
    rows = []
    start = date(2023, 1, 1)

    for i in range(300):
        d = start + timedelta(days=random.randint(0, 540))
        cat = random.choice(categorias)
        zona = random.choice(zonas)
        base = random.uniform(500, 3000)
        if cat == "Lácteos" and d >= date(2023, 5, 1):
            base *= 0.30
        rows.append({
            "Fecha Venta": d.isoformat(),
            "Categoría": cat,
            "Zona": zona,
            "Ventas Total": round(base, 2),
            "Unidades": random.randint(5, 200),
        })

    df = pd.DataFrame(rows)
    _out.parent.mkdir(exist_ok=True)
    df.to_excel(_out, index=False)
    print(f"Generado: {_out} ({len(df)} filas)")

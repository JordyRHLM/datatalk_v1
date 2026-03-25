"""
Schema Agent — DataTalk
Lee un archivo Excel o CSV con DuckDB, infiere tipos de columna
y devuelve el schema como diccionario listo para inyectar en prompts.
"""

import re
import duckdb
import pandas as pd
from pathlib import Path


# Tipos DuckDB → categoría legible para el prompt
_TYPE_MAP = {
    "DATE": "fecha",
    "TIMESTAMP": "fecha",
    "TIMESTAMP WITH TIME ZONE": "fecha",
    "INTEGER": "número entero",
    "BIGINT": "número entero",
    "HUGEINT": "número entero",
    "SMALLINT": "número entero",
    "TINYINT": "número entero",
    "DOUBLE": "número decimal",
    "FLOAT": "número decimal",
    "DECIMAL": "número decimal",
    "BOOLEAN": "booleano",
    "VARCHAR": "texto",
    "BLOB": "texto",
}

# Columnas con más de este porcentaje de nulos reciben una advertencia
_NULL_WARN_THRESHOLD = 0.20


def _sanitize_column_name(name: str) -> str:
    """Convierte 'Fecha Venta' → 'fecha_venta'. Sin espacios ni caracteres especiales."""
    name = name.strip().lower()
    name = name.replace(" ", "_")
    # Reemplaza tildes y ñ
    replacements = {"á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ñ": "n", "ü": "u"}
    for orig, repl in replacements.items():
        name = name.replace(orig, repl)
    # Elimina cualquier caracter que no sea alfanumérico o guión bajo
    name = re.sub(r"[^\w]", "_", name)
    # Colapsa guiones bajos múltiples
    name = re.sub(r"_+", "_", name).strip("_")
    return name


def _infer_category(duck_type: str) -> str:
    """Devuelve la categoría legible del tipo DuckDB."""
    duck_type_upper = duck_type.upper()
    for key, value in _TYPE_MAP.items():
        if duck_type_upper.startswith(key):
            return value
    return "texto"


def run(file_path: str) -> dict:
    """
    Lee un Excel o CSV, infiere el schema y devuelve un diccionario con:
    - table_name: nombre de tabla sanitizado
    - row_count: cantidad de filas
    - columns: lista de dicts con nombre, tipo_duck, categoría, ejemplos, nulos
    - warnings: lista de advertencias (columnas con muchos nulos, etc.)
    - sql_ready: True si el archivo se procesó sin errores

    Args:
        file_path: Ruta al archivo .xlsx, .xls o .csv

    Returns:
        dict con el schema completo

    Raises:
        FileNotFoundError: si el archivo no existe
        ValueError: si el formato no es soportado
    """
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"Archivo no encontrado: {file_path}")

    suffix = path.suffix.lower()
    if suffix not in (".xlsx", ".xls", ".csv", ".tsv"):
        raise ValueError(f"Formato no soportado: {suffix}. Usar .xlsx, .xls, .csv o .tsv")

    con = duckdb.connect()
    warnings = []

    # --- Carga del archivo ---
    if suffix in (".xlsx", ".xls"):
        # DuckDB no lee Excel directo: cargamos con pandas y registramos la tabla
        df_raw = pd.read_excel(file_path)
        # Sanitizamos los nombres de columna
        df_raw.columns = [_sanitize_column_name(c) for c in df_raw.columns]
        con.register("data_table", df_raw)
        table_name = "data_table"
    else:
        # CSV: DuckDB lo lee directo con inferencia automática
        sep = "\t" if suffix == ".tsv" else ","
        # Sanitizamos después de leer con pandas también
        df_raw = pd.read_csv(file_path, sep=sep)
        df_raw.columns = [_sanitize_column_name(c) for c in df_raw.columns]
        con.register("data_table", df_raw)
        table_name = "data_table"

    # --- Cantidad de filas ---
    row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    if row_count == 0:
        warnings.append("El archivo está vacío o solo tiene encabezados.")

    # --- Tipos de columna vía DESCRIBE ---
    describe_rows = con.execute(f"DESCRIBE {table_name}").fetchall()
    # describe_rows: [(column_name, column_type, null, key, default, extra), ...]

    columns = []
    for row in describe_rows:
        col_name = row[0]
        duck_type = row[1]
        category = _infer_category(duck_type)

        # Ejemplos: hasta 3 valores no nulos distintos
        try:
            examples_raw = con.execute(
                f"""
                SELECT DISTINCT CAST("{col_name}" AS VARCHAR)
                FROM {table_name}
                WHERE "{col_name}" IS NOT NULL
                LIMIT 3
                """
            ).fetchall()
            examples = [r[0] for r in examples_raw if r[0] is not None]
        except Exception:
            examples = []

        # Porcentaje de nulos
        try:
            null_count = con.execute(
                f'SELECT COUNT(*) FROM {table_name} WHERE "{col_name}" IS NULL'
            ).fetchone()[0]
            null_pct = null_count / row_count if row_count > 0 else 0
        except Exception:
            null_count = 0
            null_pct = 0

        if null_pct > _NULL_WARN_THRESHOLD:
            warnings.append(
                f"Columna '{col_name}' tiene {null_pct:.0%} de valores nulos — "
                f"los resultados pueden ser incompletos."
            )

        columns.append({
            "name": col_name,
            "type_duck": duck_type,
            "category": category,
            "examples": examples,
            "null_pct": round(null_pct, 4),
        })

    con.close()

    return {
        "table_name": table_name,
        "file_path": str(path.resolve()),
        "row_count": row_count,
        "columns": columns,
        "warnings": warnings,
        "sql_ready": True,
    }


def schema_to_prompt_text(schema: dict) -> str:
    """
    Convierte el schema dict a texto plano optimizado para inyectar en un prompt.
    
    Ejemplo de salida:
        Tabla: data_table (1500 filas)
        Columnas disponibles:
          - fecha_venta (fecha) → ejemplos: 2024-01-15, 2024-02-03
          - producto (texto) → ejemplos: Lácteos, Bebidas, Panadería
          - ventas_total (número decimal) → ejemplos: 1500.0, 320.5
    """
    lines = [
        f"Tabla: {schema['table_name']} ({schema['row_count']} filas)",
        "Columnas disponibles:",
    ]
    for col in schema["columns"]:
        examples_str = ", ".join(col["examples"]) if col["examples"] else "sin ejemplos"
        null_note = f" ⚠ {col['null_pct']:.0%} nulos" if col["null_pct"] > _NULL_WARN_THRESHOLD else ""
        lines.append(f"  - {col['name']} ({col['category']}) → ejemplos: {examples_str}{null_note}")

    if schema["warnings"]:
        lines.append("\nAdvertencias:")
        for w in schema["warnings"]:
            lines.append(f"  ⚠ {w}")

    return "\n".join(lines)

"""
Schema Inspector — inspecciona el esquema de las tablas en DuckDB.
Le da contexto al LLM para generar SQL correcto desde el primer intento.
"""
from datatalk.data.duck_engine import get_connection, list_tables


def get_schema_context() -> str:
    """
    Genera un string con el schema de todas las tablas registradas.
    Este string se inyecta en el prompt del SQL Agent.
    """
    tables = list_tables()
    if not tables:
        return "No hay tablas registradas. El usuario debe subir un archivo primero."

    conn = get_connection()
    context_parts = []

    for table in tables:
        schema = conn.execute(f"DESCRIBE {table}").fetchdf()
        sample = conn.execute(f"SELECT * FROM {table} LIMIT 3").fetchdf()

        cols = "\n".join(
            f"  - {row['column_name']} ({row['column_type']})"
            for _, row in schema.iterrows()
        )
        sample_str = sample.to_string(index=False, max_rows=3)

        context_parts.append(
            f"Tabla: {table}\nColumnas:\n{cols}\n\nEjemplo de datos:\n{sample_str}"
        )

    return "\n\n---\n\n".join(context_parts)

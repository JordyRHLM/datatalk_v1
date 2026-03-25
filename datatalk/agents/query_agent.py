"""
Query Agent — DataTalk
Genera SQL a partir de una pregunta en lenguaje natural,
lo ejecuta en DuckDB e implementa un validation loop de 3 intentos
con autocorrección automática via Azure OpenAI.
"""

import os
import re
import duckdb
import pandas as pd
from openai import AzureOpenAI
from dotenv import load_dotenv

from datatalk.agents.schema_agent import run as schema_run, schema_to_prompt_text

load_dotenv()

# --- Cliente Azure OpenAI (lazy — se inicializa en el primer uso) ---
_client = None
_DEPLOYMENT = None


def _get_client() -> AzureOpenAI:
    """Devuelve el cliente Azure OpenAI, inicializándolo si es necesario."""
    global _client, _DEPLOYMENT
    if _client is None:
        _client = AzureOpenAI(
            azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
            api_key=os.environ["AZURE_OPENAI_API_KEY"],
            api_version=os.environ.get("AZURE_OPENAI_API_VERSION", "2024-02-01"),
        )
        _DEPLOYMENT = os.environ["AZURE_OPENAI_DEPLOYMENT_NAME"]
    return _client

MAX_ATTEMPTS = 3


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

_SQL_SYSTEM_PROMPT = """Eres un experto en SQL analítico. Tu única tarea es generar una consulta SQL válida para DuckDB.

Reglas estrictas:
1. Devuelve ÚNICAMENTE el SQL, sin explicaciones, sin markdown, sin bloques de código.
2. Usa exactamente los nombres de columna del schema provisto — sin inventar columnas.
3. Todas las columnas con espacios o caracteres especiales van entre comillas dobles.
4. Para fechas usa funciones DuckDB: DATE_TRUNC, DATE_DIFF, EXTRACT, strftime.
5. El nombre de la tabla es exactamente el que aparece en el schema.
6. Si la pregunta implica tendencia temporal, ordena por fecha ASC.
7. Si la pregunta implica ranking, ordena por la métrica DESC y usa LIMIT apropiado.
8. No uses WITH RECURSIVE. No uses funciones que no existan en DuckDB 0.10.
"""

_SQL_USER_TEMPLATE = """Schema de los datos:
{schema_text}

Tipo de análisis: {intent}

Pregunta del usuario: {question}

Genera el SQL que responde esta pregunta."""


_CORRECTION_SYSTEM_PROMPT = """Eres un experto en SQL para DuckDB. Recibirás un SQL que falló y el mensaje de error.
Tu tarea es corregir el SQL para que funcione.

Reglas:
1. Devuelve ÚNICAMENTE el SQL corregido, sin explicaciones ni markdown.
2. Analiza el mensaje de error para identificar la causa raíz.
3. Si el error es 'column not found', busca la columna más similar en el schema y úsala.
4. Si el error es de tipo, agrega el CAST necesario.
5. Si el nombre de columna tiene espacios, ponlo entre comillas dobles.
6. Mantén la intención original de la consulta.
"""

_CORRECTION_USER_TEMPLATE = """Schema disponible:
{schema_text}

SQL que falló:
{sql_failed}

Error de DuckDB:
{error_message}

Corrige el SQL para que funcione."""


_SIMPLIFICATION_SYSTEM_PROMPT = """Eres un experto en SQL para DuckDB. El SQL ha fallado 2 veces.
Generá una versión simplificada que responda la intención mínima de la pregunta.

Reglas:
1. Devuelve ÚNICAMENTE el SQL simplificado, sin markdown.
2. Eliminá subconsultas complejas, CTE anidadas, y condiciones derivadas.
3. Usá solo las columnas más relevantes para la pregunta.
4. Priorizá que funcione sobre que sea completo.
"""

_SIMPLIFICATION_USER_TEMPLATE = """Schema disponible:
{schema_text}

Pregunta original: {question}
Intención: {intent}

SQL del intento 2 (falló):
{sql_failed}

Error:
{error_message}

Generá un SQL simplificado que responda lo esencial de la pregunta."""


# ---------------------------------------------------------------------------
# Funciones internas
# ---------------------------------------------------------------------------

def _extract_sql(raw: str) -> str:
    """Limpia el SQL de posibles bloques markdown que el modelo devuelva."""
    raw = raw.strip()
    # Elimina ```sql ... ``` o ``` ... ```
    raw = re.sub(r"^```(?:sql)?\s*", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\s*```$", "", raw)
    return raw.strip()


def _call_llm(system: str, user: str) -> str:
    """Llama a Azure OpenAI y devuelve el texto generado."""
    client = _get_client()
    response = client.chat.completions.create(
        model=_DEPLOYMENT,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0,        # Determinístico para SQL
        max_tokens=1000,
    )
    return response.choices[0].message.content or ""


def _execute_sql(sql: str, con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Ejecuta SQL en DuckDB y devuelve un DataFrame. Lanza excepción si falla."""
    return con.execute(sql).df()


# ---------------------------------------------------------------------------
# Tarea 7 — Generación SQL básica
# ---------------------------------------------------------------------------

def generate_sql(intent: str, schema: dict, question: str) -> str:
    """
    Genera una consulta SQL a partir de la intención, el schema y la pregunta.
    Llama a Azure OpenAI con el schema inyectado en el prompt.

    Args:
        intent: Tipo de análisis — RANKING, TENDENCIA, COMPARATIVA, ANOMALIA, AGREGACION
        schema: Diccionario devuelto por schema_agent.run()
        question: Pregunta del usuario en lenguaje natural

    Returns:
        SQL generado como string limpio (sin markdown)
    """
    schema_text = schema_to_prompt_text(schema)
    user_msg = _SQL_USER_TEMPLATE.format(
        schema_text=schema_text,
        intent=intent,
        question=question,
    )
    raw_sql = _call_llm(_SQL_SYSTEM_PROMPT, user_msg)
    return _extract_sql(raw_sql)


def run_basic(sql: str, file_path: str) -> pd.DataFrame:
    """
    Ejecuta el SQL generado en DuckDB y devuelve el DataFrame resultado.
    Sin validation loop — lanza excepción directamente si falla.

    Args:
        sql: SQL generado por generate_sql()
        file_path: Ruta al archivo de datos (para registrar en DuckDB)

    Returns:
        DataFrame con los resultados

    Raises:
        Exception: si el SQL falla (usar run_with_validation para manejo automático)
    """
    schema = schema_run(file_path)
    con = duckdb.connect()
    _register_file(con, file_path, schema)
    try:
        return _execute_sql(sql, con)
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Tarea 8 — Validation loop completo
# ---------------------------------------------------------------------------

def run_with_validation(
    intent: str,
    question: str,
    file_path: str,
) -> dict:
    """
    Flujo completo: genera SQL, lo ejecuta, y si falla intenta corregirlo
    hasta 3 veces antes de devolver un mensaje amigable.

    Args:
        intent: Tipo de análisis (RANKING, TENDENCIA, etc.)
        question: Pregunta del usuario
        file_path: Ruta al archivo de datos

    Returns:
        dict con las claves:
          - success (bool)
          - data (DataFrame | None)
          - sql_final (str) — el SQL que funcionó o el último intento
          - attempts (int) — cuántos intentos usó
          - autocorrected (bool) — True si necesitó corrección
          - user_message (str) — mensaje para mostrar al usuario
          - error_detail (str | None) — solo para logging interno, nunca al usuario
    """
    # 1. Leer schema del archivo
    schema = schema_run(file_path)
    schema_text = schema_to_prompt_text(schema)

    # 2. Abrir conexión DuckDB y registrar el archivo
    con = duckdb.connect()
    _register_file(con, file_path, schema)

    sql = None
    last_error = None

    try:
        for attempt in range(1, MAX_ATTEMPTS + 1):

            # --- Generar o corregir SQL ---
            if attempt == 1:
                sql = generate_sql(intent, schema, question)

            elif attempt == 2:
                # Corrección estándar: SQL roto + error
                user_msg = _CORRECTION_USER_TEMPLATE.format(
                    schema_text=schema_text,
                    sql_failed=sql,
                    error_message=str(last_error),
                )
                raw = _call_llm(_CORRECTION_SYSTEM_PROMPT, user_msg)
                sql = _extract_sql(raw)

            else:  # attempt == 3
                # Simplificación: versión mínima viable
                user_msg = _SIMPLIFICATION_USER_TEMPLATE.format(
                    schema_text=schema_text,
                    question=question,
                    intent=intent,
                    sql_failed=sql,
                    error_message=str(last_error),
                )
                raw = _call_llm(_SIMPLIFICATION_SYSTEM_PROMPT, user_msg)
                sql = _extract_sql(raw)

            # --- Ejecutar ---
            try:
                df = _execute_sql(sql, con)
                # Éxito
                return {
                    "success": True,
                    "data": df,
                    "sql_final": sql,
                    "attempts": attempt,
                    "autocorrected": attempt > 1,
                    "user_message": _success_message(df, attempt),
                    "error_detail": None,
                }
            except Exception as e:
                last_error = e
                # Continúa al siguiente intento

        # --- 3 fallos: respuesta amigable ---
        return {
            "success": False,
            "data": None,
            "sql_final": sql,
            "attempts": MAX_ATTEMPTS,
            "autocorrected": True,
            "user_message": _failure_message(question, last_error),
            "error_detail": str(last_error),  # Solo para logs, nunca al usuario
        }

    finally:
        con.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _register_file(con: duckdb.DuckDBPyConnection, file_path: str, schema: dict) -> None:
    """Registra el archivo en DuckDB usando el mismo proceso del Schema Agent."""
    import pandas as pd
    from pathlib import Path

    path = Path(file_path)
    suffix = path.suffix.lower()

    if suffix in (".xlsx", ".xls"):
        df = pd.read_excel(file_path)
    else:
        sep = "\t" if suffix == ".tsv" else ","
        df = pd.read_csv(file_path, sep=sep)

    # Sanitizamos nombres igual que el Schema Agent para consistencia
    from datatalk.agents.schema_agent import _sanitize_column_name
    df.columns = [_sanitize_column_name(c) for c in df.columns]
    con.register(schema["table_name"], df)


def _success_message(df: pd.DataFrame, attempt: int) -> str:
    rows = len(df)
    if attempt == 1:
        return f"Consulta ejecutada. Se encontraron {rows} resultado{'s' if rows != 1 else ''}."
    return f"Consulta refinada automáticamente. Se encontraron {rows} resultado{'s' if rows != 1 else ''}."


def _failure_message(question: str, error: Exception) -> str:
    """
    Mensaje amigable para el usuario cuando los 3 intentos fallan.
    NUNCA incluye el stack trace ni el mensaje técnico de DuckDB.
    """
    # Detectamos causas comunes para dar un mensaje más específico
    error_str = str(error).lower()

    if "column" in error_str and "not found" in error_str:
        return (
            "No pude encontrar la columna necesaria para responder tu pregunta. "
            "¿Podrías verificar que el archivo tiene esa información o reformular la pregunta?"
        )
    if "syntax" in error_str:
        return (
            "La consulta generada no es compatible con el formato de tus datos. "
            "Intentá reformular la pregunta de forma más específica."
        )
    if "cast" in error_str or "conversion" in error_str:
        return (
            "Hay un problema con el tipo de datos en una columna. "
            "¿Los valores numéricos o de fecha están en el formato correcto en tu archivo?"
        )

    return (
        "No pude generar una consulta válida para esa pregunta después de varios intentos. "
        "Intentá reformularla o verificá que el archivo contiene la información que buscás."
    )

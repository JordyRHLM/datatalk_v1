"""
Orquestador — DataTalk
Recibe la pregunta del usuario, clasifica la intención,
coordina Schema Agent + Query Agent + Dashboard Agent
y devuelve el resultado explicado en lenguaje de negocio.
"""

import os
import pandas as pd
from openai import AzureOpenAI
from dotenv import load_dotenv

from datatalk.agents.prompts import (
    INTENT_SYSTEM_PROMPT,
    INTENT_USER_TEMPLATE,
    EXPLANATION_SYSTEM_PROMPT,
    EXPLANATION_USER_TEMPLATE,
)
from datatalk.agents import schema_agent, query_agent, dashboard_agent
from datatalk.agents import history_cache
from datatalk.core import cache as _cache

load_dotenv()

_client = None
_DEPLOYMENT = None

VALID_INTENTS = {"RANKING", "TENDENCIA", "COMPARATIVA", "ANOMALIA", "AGREGACION"}


def _get_client() -> AzureOpenAI:
    global _client, _DEPLOYMENT
    if _client is None:
        _client = AzureOpenAI(
            azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
            api_key=os.environ["AZURE_OPENAI_API_KEY"],
            api_version=os.environ.get("AZURE_OPENAI_API_VERSION", "2024-02-01"),
        )
        _DEPLOYMENT = os.environ["AZURE_OPENAI_DEPLOYMENT_NAME"]
    return _client


def classify_intent(question: str) -> str:
    """
    Clasifica la pregunta en una de las 5 intenciones analíticas.

    Returns:
        str: RANKING | TENDENCIA | COMPARATIVA | ANOMALIA | AGREGACION
    """
    # ── Cache hit ────────────────────────────────────────────────────────────
    cached_intent = _cache.IntentCache.get(question)
    if cached_intent is not None:
        return cached_intent

    client = _get_client()
    response = client.chat.completions.create(
        model=_DEPLOYMENT,
        messages=[
            {"role": "system", "content": INTENT_SYSTEM_PROMPT},
            {"role": "user", "content": INTENT_USER_TEMPLATE.format(question=question)},
        ],
        temperature=0,
        max_tokens=20,
    )
    intent = (response.choices[0].message.content or "AGREGACION").strip().upper()
    intent = intent if intent in VALID_INTENTS else "AGREGACION"

    # ── Cache set ─────────────────────────────────────────────────────────────
    _cache.IntentCache.set(question, intent)

    return intent


def _explain_results(question: str, intent: str, df: pd.DataFrame) -> str:
    """Genera explicación en lenguaje de negocio del resultado."""
    if df is None or df.empty:
        return "No se encontraron datos para esta consulta."

    results_summary = df.head(10).to_string(index=False)
    client = _get_client()
    response = client.chat.completions.create(
        model=_DEPLOYMENT,
        messages=[
            {"role": "system", "content": EXPLANATION_SYSTEM_PROMPT},
            {"role": "user", "content": EXPLANATION_USER_TEMPLATE.format(
                question=question,
                intent=intent,
                results_summary=results_summary,
            )},
        ],
        temperature=0.3,
        max_tokens=200,
    )
    return (response.choices[0].message.content or "").strip()


def run(question: str, file_path: str, generate_chart: bool = False, user_id: str = "anon") -> dict:
    """
    Flujo completo de DataTalk.

    Args:
        question: Pregunta del usuario en lenguaje natural
        file_path: Ruta al archivo de datos (.xlsx o .csv)
        generate_chart: Si True, genera también el dashboard Plotly

    Returns:
        dict con:
          - success (bool)
          - intent (str)
          - sql (str)
          - data (DataFrame | None)
          - explanation (str)        — resultado en lenguaje de negocio
          - user_message (str)       — mensaje de estado para la UI
          - autocorrected (bool)
          - attempts (int)
          - chart (dict | None)      — resultado del Dashboard Agent si generate_chart=True
          - warnings (list)          — advertencias del Schema Agent
    """
    # 0. Verificar si es referencia al historial
    ref = history_cache.resolve_reference(user_id, question)
    if ref["found"] and ref["entry"]:
        entry = ref["entry"]
        import pandas as pd
        df = pd.DataFrame(entry.get("preview", []))
        return {
            "success": True,
            "intent": entry.get("intent", ""),
            "sql": entry.get("sql", ""),
            "data": df if not df.empty else None,
            "explanation": entry.get("explanation", ""),
            "user_message": ref["message"],
            "autocorrected": False,
            "attempts": 0,
            "chart": None,
            "warnings": [],
            "_from_cache": True,
        }
    if not ref["found"] and ref["candidates"]:
        return {
            "success": False,
            "intent": "",
            "sql": "",
            "data": None,
            "explanation": ref["message"],
            "user_message": ref["message"],
            "autocorrected": False,
            "attempts": 0,
            "chart": None,
            "warnings": [],
            "_from_cache": False,
        }

    # 1. Clasificar intención
    intent = classify_intent(question)

    # 2. Leer schema (para advertencias)
    schema = schema_agent.run(file_path)

    # 3. Detectar si pide gráfico
    chart_keywords = ["dashboard", "gráfico", "grafico", "chart", "visual", "mostrar", "graficá", "graficame"]
    wants_chart = generate_chart or any(k in question.lower() for k in chart_keywords)

    # ── Query cache hit ───────────────────────────────────────────────────────
    cached_query = _cache.QueryCache.get(file_path, question, intent)
    if cached_query is not None and not wants_chart:
        cached_query["_from_cache"] = True
        cached_query["warnings"] = schema.get("warnings", [])
        return cached_query

    # 4. Ejecutar Query Agent con validation loop
    query_result = query_agent.run_with_validation(
        intent=intent,
        question=question,
        file_path=file_path,
    )

    if not query_result["success"]:
        return {
            "success": False,
            "intent": intent,
            "sql": query_result["sql_final"],
            "data": None,
            "explanation": query_result["user_message"],
            "user_message": query_result["user_message"],
            "autocorrected": query_result["autocorrected"],
            "attempts": query_result["attempts"],
            "chart": None,
            "warnings": schema["warnings"],
        }

    df = query_result["data"]

    # 5. Explicar resultados en lenguaje de negocio
    explanation = _explain_results(question, intent, df)

    # 6. Generar dashboard si se pidió
    chart = None
    if wants_chart and df is not None and not df.empty:
        chart = dashboard_agent.generate_dashboard(
            df=df,
            intent=intent,
            question=question,
        )

    # Guardar en caché
    history_cache.save(user_id, question, {
        "success": True,
        "intent": intent,
        "sql": query_result["sql_final"],
        "data": df,
        "explanation": explanation,
        "autocorrected": query_result["autocorrected"],
        "attempts": query_result["attempts"],
    }, file_path)

    final_result = {
        "success": True,
        "intent": intent,
        "sql": query_result["sql_final"],
        "data": df,
        "explanation": explanation,
        "user_message": query_result["user_message"],
        "autocorrected": query_result["autocorrected"],
        "attempts": query_result["attempts"],
        "chart": chart,
        "warnings": schema["warnings"],
        "_from_cache": False,
    }

    # ── Query cache set ───────────────────────────────────────────────────────
    if not wants_chart:
        _cache.QueryCache.set(file_path, question, intent, final_result)

    return final_result

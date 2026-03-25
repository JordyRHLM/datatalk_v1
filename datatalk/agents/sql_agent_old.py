"""
SQL Agent — convierte preguntas en lenguaje natural a SQL validado.
Corazón del proyecto: Text-to-SQL con explicabilidad y nivel de confianza.
"""
from openai import AsyncAzureOpenAI
from datatalk.core.config import get_settings
from datatalk.data.duck_engine import execute_sql

settings = get_settings()

SYSTEM_PROMPT = """Eres un experto en SQL analítico. Tu trabajo es:
1. Convertir preguntas en lenguaje natural a SQL correcto para DuckDB.
2. Aplicar filtros de seguridad (RBAC) si se proporcionan.
3. Retornar SOLO un JSON con este formato exacto:
{
  "sql": "SELECT ...",
  "explanation": "Esta consulta hace X porque Y",
  "confidence": 0.95,
  "assumptions": ["asumí que fecha_venta es la fecha relevante"]
}

Reglas:
- Usa solo las tablas y columnas del schema proporcionado.
- Si la pregunta es ambigua, elige la interpretación más razonable y declárala en assumptions.
- Nunca uses DROP, DELETE, UPDATE, INSERT o ALTER.
- El SQL debe ser compatible con DuckDB.
- confidence va de 0.0 a 1.0 según qué tan seguro estás del SQL generado.
"""


class SQLAgent:
    def __init__(self):
        self.client = AsyncAzureOpenAI(
            api_key=settings.azure_openai_api_key,
            azure_endpoint=settings.azure_openai_endpoint,
            api_version=settings.azure_openai_api_version,
        )

    async def generate(
        self,
        question: str,
        schema_context: str,
        rbac_filter: str | None = None,
    ) -> dict:
        """Genera SQL a partir de una pregunta en lenguaje natural."""
        import json

        filter_note = ""
        if rbac_filter:
            filter_note = f"\n\nFILTRO DE SEGURIDAD OBLIGATORIO: Agrega siempre WHERE {rbac_filter} a todas las consultas."

        user_prompt = f"""Schema disponible:
{schema_context}
{filter_note}

Pregunta del usuario: {question}"""

        response = await self.client.chat.completions.create(
            model=settings.azure_openai_deployment,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.1,  # Baja temperatura para SQL determinístico
            response_format={"type": "json_object"},
        )

        return json.loads(response.choices[0].message.content)

    async def execute(self, sql: str) -> dict:
        """Ejecuta el SQL aprobado y genera un resumen en lenguaje empresarial."""
        import json

        try:
            df = execute_sql(sql)
            data = df.to_dict(orient="records")
            rows = len(df)

            # Generar resumen en lenguaje natural
            summary_prompt = f"""Los resultados de la consulta SQL son:
{df.to_string(index=False, max_rows=10)}

Explica estos resultados en 2-3 oraciones en lenguaje empresarial claro, 
sin jerga técnica. Sé concreto con los números."""

            summary_response = await self.client.chat.completions.create(
                model=settings.azure_openai_deployment,
                messages=[{"role": "user", "content": summary_prompt}],
                temperature=0.3,
            )
            summary = summary_response.choices[0].message.content

            return {"data": data, "rows_returned": rows, "summary": summary}

        except Exception as e:
            return {"error": str(e), "data": None, "rows_returned": 0}

"""
Dashboard Agent — DataTalk
Recibe el DataFrame del Query Agent y genera:
  - Plotly JSON  → para renderizar en Next.js (interactivo)
  - PNG base64   → para mandar a Teams como imagen en Adaptive Card
"""

import io
import json
import base64
import os
import pandas as pd
from openai import AzureOpenAI
from dotenv import load_dotenv

load_dotenv()

_client = None
_DEPLOYMENT = None


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


_DASHBOARD_SYSTEM_PROMPT = """Eres un experto en visualización de datos.
Tu tarea es generar una configuración de gráfico Plotly en JSON.

Reglas estrictas:
1. Devuelve ÚNICAMENTE JSON válido, sin markdown, sin explicaciones.
2. El JSON debe tener exactamente dos claves: "traces" y "layout".
3. En los traces, usa los nombres de columna EXACTOS como strings en x e y.
   Ejemplo: {"type": "bar", "x": "categoria", "y": "ventas_total"}
   El sistema reemplazará esos strings por los datos reales del DataFrame.
4. Elige el tipo de gráfico más apropiado para el intent:
   - RANKING     → bar horizontal (type: 'bar', orientation: 'h')
   - TENDENCIA   → scatter con mode: 'lines+markers'
   - COMPARATIVA → bar vertical agrupado
   - ANOMALIA    → scatter con markers
   - AGREGACION  → pie o bar simple
5. El layout debe incluir title.text, xaxis.title, yaxis.title.
6. Color principal: '#6366f1', acento: '#f59e0b'
"""

_DASHBOARD_USER_TEMPLATE = """Intent: {intent}
Pregunta: {question}

Columnas disponibles:
{columns_info}

Primeras 5 filas:
{sample_data}

Genera el JSON de configuración Plotly."""


def generate_dashboard(df: pd.DataFrame, intent: str, question: str) -> dict:
    """
    Genera configuración de dashboard a partir del DataFrame del Query Agent.

    Returns:
        dict con plotly_json, png_base64, chart_type, success, error
    """
    if df is None or df.empty:
        return {"success": False, "plotly_json": None, "png_base64": None,
                "chart_type": None, "error": "DataFrame vacío."}

    columns_info = "\n".join(
        f"  - {col}: {df[col].dtype} (ej: {df[col].dropna().iloc[0] if not df[col].dropna().empty else 'N/A'})"
        for col in df.columns
    )

    try:
        client = _get_client()
        response = client.chat.completions.create(
            model=_DEPLOYMENT,
            messages=[
                {"role": "system", "content": _DASHBOARD_SYSTEM_PROMPT},
                {"role": "user", "content": _DASHBOARD_USER_TEMPLATE.format(
                    intent=intent,
                    question=question,
                    columns_info=columns_info,
                    sample_data=df.head(5).to_string(index=False),
                )},
            ],
            temperature=0,
            max_tokens=1500,
        )
        raw = (response.choices[0].message.content or "").strip()
        raw = raw.lstrip("```json").lstrip("```").rstrip("```").strip()
        plotly_config = json.loads(raw)

    except json.JSONDecodeError as e:
        return {"success": False, "plotly_json": None, "png_base64": None,
                "chart_type": None, "error": f"JSON inválido: {e}"}
    except Exception as e:
        return {"success": False, "plotly_json": None, "png_base64": None,
                "chart_type": None, "error": str(e)}

    plotly_config = _inject_dataframe(plotly_config, df)
    chart_type = plotly_config.get("traces", [{}])[0].get("type", "bar")
    png_base64 = _render_png(plotly_config)

    return {
        "success": True,
        "plotly_json": plotly_config,
        "png_base64": png_base64,
        "chart_type": chart_type,
        "error": None,
    }


def _inject_dataframe(config: dict, df: pd.DataFrame) -> dict:
    """Reemplaza nombres de columna en los traces con los datos reales."""
    for trace in config.get("traces", []):
        for axis in ("x", "y", "labels", "values"):
            val = trace.get(axis)
            if isinstance(val, str) and val in df.columns:
                trace[axis] = df[val].tolist()
            elif isinstance(val, str):
                numeric_cols = df.select_dtypes(include="number").columns.tolist()
                if axis in ("y", "values") and numeric_cols:
                    trace[axis] = df[numeric_cols[0]].tolist()
                elif axis in ("x", "labels"):
                    trace[axis] = df.iloc[:, 0].astype(str).tolist()
    return config


def _render_png(plotly_config: dict) -> str | None:
    """Renderiza el gráfico como PNG usando matplotlib. Devuelve base64 o None."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(10, 5))
        fig.patch.set_facecolor("#1e1e2e")
        ax.set_facecolor("#1e1e2e")

        colors = ["#6366f1", "#f59e0b", "#10b981", "#ef4444", "#3b82f6"]
        traces = plotly_config.get("traces", [])
        layout = plotly_config.get("layout", {})

        for i, trace in enumerate(traces):
            x = trace.get("x", [])
            y = trace.get("y", [])
            t = trace.get("type", "bar")
            color = colors[i % len(colors)]
            name = trace.get("name", "")

            if t == "bar":
                if trace.get("orientation") == "h":
                    ax.barh(x, y, color=color, label=name, alpha=0.85)
                else:
                    ax.bar(x, y, color=color, label=name, alpha=0.85)
            elif t in ("scatter", "line"):
                ax.plot(x, y, color=color, marker="o", linewidth=2, label=name)
            elif t == "pie":
                labels = trace.get("labels", [])
                values = trace.get("values", [])
                if labels and values:
                    ax.pie(values, labels=labels, colors=colors[:len(labels)],
                           autopct="%1.1f%%", textprops={"color": "white"})

        title = layout.get("title", {})
        title_text = title.get("text", "") if isinstance(title, dict) else str(title)
        ax.set_title(title_text, color="white", fontsize=14, pad=15)

        xaxis = layout.get("xaxis", {})
        yaxis = layout.get("yaxis", {})
        ax.set_xlabel(xaxis.get("title", ""), color="#9ca3af", fontsize=11)
        ax.set_ylabel(yaxis.get("title", ""), color="#9ca3af", fontsize=11)
        ax.tick_params(colors="#9ca3af")
        for spine in ax.spines.values():
            spine.set_edgecolor("#374151")

        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=150, bbox_inches="tight",
                    facecolor=fig.get_facecolor())
        plt.close(fig)
        buf.seek(0)
        return base64.b64encode(buf.read()).decode("utf-8")

    except ImportError:
        return None


def build_teams_card(question: str, sql: str, user_message: str, image_url: str = None) -> dict:
    """
    Construye una Adaptive Card de Teams con el gráfico y el resumen.

    Args:
        question: pregunta original del usuario
        sql: SQL ejecutado
        user_message: mensaje amigable del Query Agent
        image_url: URL pública del PNG en Azure Blob Storage

    Returns:
        dict con el JSON de la Adaptive Card listo para Power Automate
    """
    card = {
        "type": "AdaptiveCard",
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.5",
        "body": [
            {
                "type": "TextBlock",
                "text": "📊 DataTalk — Resultado de análisis",
                "weight": "Bolder",
                "size": "Medium",
                "color": "Accent",
            },
            {
                "type": "TextBlock",
                "text": f"**Pregunta:** {question}",
                "wrap": True,
            },
            {
                "type": "TextBlock",
                "text": user_message,
                "wrap": True,
                "color": "Good",
            },
        ],
    }

    if image_url:
        card["body"].append({
            "type": "Image",
            "url": image_url,
            "size": "Stretch",
            "altText": "Gráfico de resultados",
        })

    card["body"].append({
        "type": "ActionSet",
        "actions": [{
            "type": "Action.ShowCard",
            "title": "Ver SQL ejecutado",
            "card": {
                "type": "AdaptiveCard",
                "body": [{
                    "type": "TextBlock",
                    "text": sql,
                    "fontType": "Monospace",
                    "wrap": True,
                    "size": "Small",
                }],
            },
        }],
    })

    return card

def to_recharts(plotly_json: dict, df: pd.DataFrame) -> dict:
    """
    Convierte el plotly_json + DataFrame a un formato directo para Recharts.
    Detecta automáticamente el tipo de chart más adecuado.
    
    Returns:
        dict con: chart_type, data (lista de dicts), keys (x_key, y_keys[]), kpis
    """
    if not plotly_json or df is None or df.empty:
        return {"chart_type": "none", "data": [], "keys": {}, "kpis": []}

    traces = plotly_json.get("traces", [])
    layout = plotly_json.get("layout", {})
    chart_type = traces[0].get("type", "bar") if traces else "bar"
    orientation = traces[0].get("orientation", "v") if traces else "v"

    # Convertir DataFrame a lista de dicts (formato nativo de Recharts)
    data = df.head(50).to_dict(orient="records")

    # Detectar columnas
    cols = df.columns.tolist()
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    text_cols = [c for c in cols if c not in numeric_cols]

    x_key = text_cols[0] if text_cols else cols[0]
    y_keys = numeric_cols[:3]  # máximo 3 métricas

    # Mapear tipo Plotly → tipo Recharts
    type_map = {
        "bar": "bar_horizontal" if orientation == "h" else "bar",
        "scatter": "line",
        "pie": "pie",
    }
    recharts_type = type_map.get(chart_type, "bar")

    # KPIs: si hay una sola columna numérica y pocas filas, mostrar como KPI
    kpis = []
    if len(df) == 1 or (len(numeric_cols) >= 1 and len(df) <= 5 and len(text_cols) == 0):
        for col in numeric_cols[:4]:
            kpis.append({
                "label": col.replace("_", " ").title(),
                "value": df[col].iloc[0] if len(df) == 1 else df[col].sum(),
            })
        recharts_type = "kpi"

    title = layout.get("title", {})
    title_text = title.get("text", "") if isinstance(title, dict) else str(title)

    return {
        "chart_type": recharts_type,
        "data": data,
        "keys": {
            "x": x_key,
            "y": y_keys,
        },
        "kpis": kpis,
        "title": title_text,
    }
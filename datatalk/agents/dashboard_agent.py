"""
Dashboard Agent — DataTalk
El LLM decide qué tipo de gráfico es más apropiado para los datos.
Soporta: bar, barh, bar_grouped, line, area, scatter, pie, heatmap, waterfall, funnel.
Produce PNG para Teams y Plotly JSON para Next.js.
"""

import io, json, base64, os, logging
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from openai import AzureOpenAI
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

_client = None
_DEPLOYMENT = None

CHART_COLORS = ["#6366f1", "#f59e0b", "#10b981", "#ef4444", "#3b82f6", "#ec4899", "#8b5cf6", "#14b8a6", "#f97316"]
BG_DARK    = "#1e1e2e"
TEXT_COLOR = "#c2c0b6"
GRID_COLOR = "#2a2a3e"

VALID_CHART_TYPES = {"bar", "barh", "bar_grouped", "line", "area", "scatter", "pie", "heatmap", "waterfall", "funnel"}


def _get_client():
    global _client, _DEPLOYMENT
    if _client is None:
        _client = AzureOpenAI(
            azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
            api_key=os.environ["AZURE_OPENAI_API_KEY"],
            api_version=os.environ.get("AZURE_OPENAI_API_VERSION", "2024-02-01"),
        )
        _DEPLOYMENT = os.environ["AZURE_OPENAI_DEPLOYMENT_NAME"]
    return _client


# ── Selección de gráfico por LLM ────────────────────────────────────────────

_CHART_SYSTEM = """Eres un experto en visualización de datos. Elegís el tipo de gráfico más apropiado.

Tipos disponibles:
- bar: comparar categorías con una métrica (hasta 10 elementos)
- barh: ranking horizontal (nombres largos o más de 6 elementos)
- bar_grouped: múltiples métricas por categoría al mismo tiempo
- line: evolución temporal, tendencias a lo largo del tiempo
- area: evolución temporal enfatizando volumen acumulado
- scatter: correlación entre dos variables numéricas o anomalías
- pie: distribución proporcional (máximo 6 categorías)
- heatmap: cruce de dos dimensiones categóricas con una métrica
- waterfall: contribución incremental de factores a un total
- funnel: tasas de conversión o etapas secuenciales

Responde ÚNICAMENTE con el nombre del tipo en minúsculas. Sin explicación."""

_CHART_USER = """Tipo de análisis: {intent}
Pregunta: {question}

DataFrame (primeras filas):
{df_info}

Columnas numéricas: {numeric_cols}
Columnas categoría: {cat_cols}
Columnas fecha: {date_cols}
Filas: {n_rows}

¿Qué tipo de gráfico?"""


def _select_chart_type_ai(intent: str, df: pd.DataFrame, question: str) -> str:
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    cat_cols     = df.select_dtypes(include="object").columns.tolist()
    date_cols    = [c for c in df.columns if any(k in c.lower() for k in ["fecha", "date", "mes", "año", "semana"])]

    try:
        df_info = df.head(4).to_string(index=False, max_cols=8)
    except Exception:
        df_info = str(list(df.columns))

    user_msg = _CHART_USER.format(
        intent=intent, question=question, df_info=df_info,
        numeric_cols=numeric_cols or "ninguna",
        cat_cols=cat_cols or "ninguna",
        date_cols=date_cols or "ninguna",
        n_rows=len(df),
    )

    try:
        client = _get_client()
        response = client.chat.completions.create(
            model=_DEPLOYMENT,
            messages=[
                {"role": "system", "content": _CHART_SYSTEM},
                {"role": "user",   "content": user_msg},
            ],
            temperature=0,
            max_tokens=15,
        )
        chart_type = (response.choices[0].message.content or "").strip().lower().split()[0]
        if chart_type in VALID_CHART_TYPES:
            logger.info(f"LLM eligió gráfico: {chart_type}")
            return chart_type
        logger.warning(f"LLM devolvió tipo inválido '{chart_type}', usando fallback")
    except Exception as e:
        logger.warning(f"LLM chart selection falló: {e}, usando fallback")

    return _fallback_chart(intent, df, question)


def _fallback_chart(intent: str, df: pd.DataFrame, question: str) -> str:
    q = question.lower()
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    date_cols    = [c for c in df.columns if any(k in c.lower() for k in ["fecha", "date", "mes"])]

    if any(k in q for k in ["embudo", "funnel", "conversión", "etapa"]):      return "funnel"
    if any(k in q for k in ["waterfall", "cascada", "contribución"]):         return "waterfall"
    if any(k in q for k in ["calor", "heatmap", "cruce", "matriz"]):          return "heatmap"
    if any(k in q for k in ["pie", "torta", "porcentaje", "proporción"]):     return "pie"
    if any(k in q for k in ["tendencia", "tiempo", "mes a mes", "linea"]):    return "line"
    if any(k in q for k in ["dispersión", "scatter", "correlación"]):         return "scatter"
    if any(k in q for k in ["área", "acumulado"]):                            return "area"
    if any(k in q for k in ["ranking", "top", "horizontal"]):                 return "barh"
    if intent == "TENDENCIA" or date_cols:                                     return "line"
    if intent == "RANKING":                                                    return "barh"
    if intent == "COMPARATIVA":   return "bar_grouped" if len(numeric_cols) > 1 else "bar"
    if intent == "ANOMALIA":                                                   return "scatter"
    if intent == "AGREGACION":
        cat_cols = df.select_dtypes(include="object").columns.tolist()
        return "pie" if cat_cols and len(df) <= 6 else "bar"
    return "bar"


# ── Función principal ────────────────────────────────────────────────────────

def generate_dashboard(df: pd.DataFrame, intent: str, question: str) -> dict:
    if df is None or df.empty:
        return {"success": False, "plotly_json": None, "png_base64": None,
                "chart_type": None, "error": "DataFrame vacío."}

    chart_type = _select_chart_type_ai(intent, df, question)

    try:
        png_base64    = _render_chart(df, chart_type, intent, question)
        plotly_config = _build_plotly_config(df, chart_type, intent, question)
        recharts = to_recharts(plotly_config, df, chart_type, question[:60])
        return {"success": True, "plotly_json": plotly_config,
                "png_base64": png_base64, "chart_type": chart_type,
                "recharts": recharts, "error": None}
    except Exception as e:
        logger.error(f"Dashboard render error ({chart_type}): {e}")
        try:
            png_base64    = _render_chart(df, "bar", intent, question)
            plotly_config = _build_plotly_config(df, "bar", intent, question)
            recharts = to_recharts(plotly_config, df, "bar", question[:60])
            return {"success": True, "plotly_json": plotly_config,
                    "png_base64": png_base64, "chart_type": "bar",
                    "recharts": recharts, "error": None}
        except Exception as e2:
            return {"success": False, "plotly_json": None, "png_base64": None,
                    "chart_type": None, "recharts": None, "error": str(e2)}


# ── Helpers ──────────────────────────────────────────────────────────────────

def _get_cols(df: pd.DataFrame):
    numeric  = df.select_dtypes(include="number").columns.tolist()
    categ    = df.select_dtypes(include="object").columns.tolist()
    date_cols = [c for c in df.columns if any(k in c.lower() for k in ["fecha", "date", "mes", "año"])]
    cat_col  = date_cols[0] if date_cols else (categ[0] if categ else df.columns[0])
    val_col  = numeric[0] if numeric else df.columns[-1]
    return cat_col, val_col


def _setup_ax(ax, fig, title=""):
    fig.patch.set_facecolor(BG_DARK)
    ax.set_facecolor(BG_DARK)
    ax.tick_params(colors=TEXT_COLOR, labelsize=9)
    for spine in ax.spines.values():
        spine.set_edgecolor(GRID_COLOR)
    ax.grid(color=GRID_COLOR, linestyle="--", linewidth=0.5, alpha=0.7)
    if title:
        ax.set_title(title, color=TEXT_COLOR, fontsize=11, pad=12, fontweight="bold")


def _to_png(fig) -> str:
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


# ── Renderizado ──────────────────────────────────────────────────────────────

def _render_chart(df: pd.DataFrame, chart_type: str, intent: str, question: str) -> str:
    cat_col, val_col = _get_cols(df)
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    title = question[:65] if question else intent

    # Gráficos con figura propia
    if chart_type == "pie":
        fig, ax = plt.subplots(figsize=(8, 6))
        fig.patch.set_facecolor(BG_DARK); ax.set_facecolor(BG_DARK)
        ax.set_title(title, color=TEXT_COLOR, fontsize=11, pad=12, fontweight="bold")
        _render_pie(ax, df, cat_col, val_col)
        return _to_png(fig)
    if chart_type == "heatmap":
        return _render_heatmap(df, title)
    if chart_type == "funnel":
        return _render_funnel(df, cat_col, val_col, title)

    fig, ax = plt.subplots(figsize=(10, 5))
    _setup_ax(ax, fig, title)

    if chart_type == "waterfall":
        _render_waterfall(ax, df, cat_col, val_col)
    elif chart_type == "barh":
        _render_barh(ax, df, cat_col, val_col)
    elif chart_type == "bar_grouped":
        _render_bar_grouped(ax, df, cat_col, numeric_cols)
    elif chart_type == "line":
        _render_line(ax, df, cat_col, val_col)
    elif chart_type == "area":
        _render_area(ax, df, cat_col, val_col)
    elif chart_type == "scatter":
        _render_scatter(ax, df, numeric_cols)
    else:
        _render_bar(ax, df, cat_col, val_col)

    plt.tight_layout()
    return _to_png(fig)


def _render_bar(ax, df, cat_col, val_col):
    bars = ax.bar(range(len(df)), df[val_col], color=CHART_COLORS[0], alpha=0.85, edgecolor="none")
    ax.set_xticks(range(len(df)))
    ax.set_xticklabels(df[cat_col].astype(str), rotation=30, ha="right", color=TEXT_COLOR)
    ax.set_ylabel(val_col, color=TEXT_COLOR)
    for bar in bars:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2, h * 1.01,
                f"{h:,.0f}", ha="center", va="bottom", fontsize=8, color=TEXT_COLOR)


def _render_barh(ax, df, cat_col, val_col):
    df_s = df.sort_values(val_col, ascending=True)
    colors = [CHART_COLORS[i % len(CHART_COLORS)] for i in range(len(df_s))]
    bars = ax.barh(range(len(df_s)), df_s[val_col], color=colors, alpha=0.85, edgecolor="none")
    ax.set_yticks(range(len(df_s)))
    ax.set_yticklabels(df_s[cat_col].astype(str), color=TEXT_COLOR)
    ax.set_xlabel(val_col, color=TEXT_COLOR)
    for bar in bars:
        w = bar.get_width()
        ax.text(w * 1.01, bar.get_y() + bar.get_height() / 2,
                f"{w:,.0f}", va="center", fontsize=8, color=TEXT_COLOR)


def _render_bar_grouped(ax, df, cat_col, numeric_cols):
    n_bars = min(len(numeric_cols), 4)
    x = np.arange(len(df))
    width = 0.8 / n_bars
    for i, col in enumerate(numeric_cols[:n_bars]):
        ax.bar(x + i * width, df[col], width, label=col,
               color=CHART_COLORS[i % len(CHART_COLORS)], alpha=0.85)
    ax.set_xticks(x + width * (n_bars - 1) / 2)
    ax.set_xticklabels(df[cat_col].astype(str), rotation=30, ha="right", color=TEXT_COLOR)
    ax.legend(facecolor="#2a2a3e", edgecolor="none", labelcolor=TEXT_COLOR, fontsize=8)


def _render_line(ax, df, cat_col, val_col):
    xs = range(len(df))
    ax.plot(xs, df[val_col], color=CHART_COLORS[0], linewidth=2.5, marker="o", markersize=5)
    ax.fill_between(xs, df[val_col], alpha=0.12, color=CHART_COLORS[0])
    step = max(1, len(df) // 8)
    ax.set_xticks(range(0, len(df), step))
    ax.set_xticklabels(df[cat_col].astype(str).iloc[::step], rotation=30, ha="right", color=TEXT_COLOR)
    ax.set_ylabel(val_col, color=TEXT_COLOR)


def _render_area(ax, df, cat_col, val_col):
    xs = range(len(df))
    ax.fill_between(xs, df[val_col], alpha=0.4, color=CHART_COLORS[0])
    ax.plot(xs, df[val_col], color=CHART_COLORS[0], linewidth=2)
    step = max(1, len(df) // 8)
    ax.set_xticks(range(0, len(df), step))
    ax.set_xticklabels(df[cat_col].astype(str).iloc[::step], rotation=30, ha="right", color=TEXT_COLOR)
    ax.set_ylabel(val_col, color=TEXT_COLOR)


def _render_scatter(ax, df, numeric_cols):
    if len(numeric_cols) >= 2:
        ax.scatter(df[numeric_cols[0]], df[numeric_cols[1]],
                   color=CHART_COLORS[0], alpha=0.7, edgecolors="none", s=60)
        ax.set_xlabel(numeric_cols[0], color=TEXT_COLOR)
        ax.set_ylabel(numeric_cols[1], color=TEXT_COLOR)
    else:
        ax.scatter(range(len(df)), df[numeric_cols[0]],
                   color=CHART_COLORS[0], alpha=0.7, edgecolors="none", s=60)
        ax.set_ylabel(numeric_cols[0], color=TEXT_COLOR)


def _render_pie(ax, df, cat_col, val_col):
    wedges, texts, autotexts = ax.pie(
        df[val_col], labels=df[cat_col].astype(str),
        colors=CHART_COLORS[:len(df)], autopct="%1.1f%%",
        pctdistance=0.82, wedgeprops={"edgecolor": BG_DARK, "linewidth": 2}, startangle=90,
    )
    for t in texts:   t.set_color(TEXT_COLOR); t.set_fontsize(9)
    for at in autotexts: at.set_color("#ffffff"); at.set_fontsize(8); at.set_fontweight("bold")


def _render_waterfall(ax, df, cat_col, val_col):
    values  = df[val_col].tolist()
    labels  = df[cat_col].astype(str).tolist()
    running = 0
    bottoms = []
    for v in values:
        bottoms.append(running if v >= 0 else running + v)
        running += v
    colors = ["#10b981" if v >= 0 else "#ef4444" for v in values]
    labels.append("Total"); values.append(running); bottoms.append(0); colors.append(CHART_COLORS[2])

    x = range(len(labels))
    ax.bar(x, [abs(v) for v in values], bottom=bottoms, color=colors, alpha=0.85, edgecolor=BG_DARK)
    for i in range(len(labels) - 2):
        top = bottoms[i] + values[i]
        ax.plot([i + 0.4, i + 0.6], [top, top], color=TEXT_COLOR, linewidth=1, linestyle="--", alpha=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=30, ha="right", color=TEXT_COLOR)
    ax.axhline(0, color=TEXT_COLOR, linewidth=0.8, alpha=0.4)
    for i, (v, b) in enumerate(zip(values, bottoms)):
        ax.text(i, b + abs(v) + abs(running) * 0.01,
                f"{v:+,.0f}" if i < len(labels) - 1 else f"{v:,.0f}",
                ha="center", va="bottom", fontsize=8, color=TEXT_COLOR)


def _render_heatmap(df: pd.DataFrame, title: str) -> str:
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    cat_cols     = df.select_dtypes(include="object").columns.tolist()

    if len(cat_cols) >= 2 and numeric_cols:
        try:
            pivot = df.pivot_table(index=cat_cols[0], columns=cat_cols[1],
                                   values=numeric_cols[0], aggfunc="sum", fill_value=0)
            fig, ax = plt.subplots(figsize=(max(8, len(pivot.columns)), max(5, len(pivot))))
            fig.patch.set_facecolor(BG_DARK); ax.set_facecolor(BG_DARK)
            im = ax.imshow(pivot.values, cmap="YlOrRd", aspect="auto")
            ax.set_xticks(range(len(pivot.columns)))
            ax.set_xticklabels(pivot.columns.astype(str), rotation=45, ha="right", color=TEXT_COLOR)
            ax.set_yticks(range(len(pivot.index)))
            ax.set_yticklabels(pivot.index.astype(str), color=TEXT_COLOR)
            ax.set_title(title, color=TEXT_COLOR, fontsize=11, pad=12, fontweight="bold")
            for i in range(len(pivot.index)):
                for j in range(len(pivot.columns)):
                    val = pivot.values[i, j]
                    ax.text(j, i, f"{val:,.0f}", ha="center", va="center", fontsize=7,
                            color="white" if val > pivot.values.max() * 0.5 else "#333")
            plt.colorbar(im, ax=ax).ax.tick_params(colors=TEXT_COLOR)
            plt.tight_layout()
            return _to_png(fig)
        except Exception:
            pass

    if len(numeric_cols) >= 2:
        corr = df[numeric_cols].corr()
        fig, ax = plt.subplots(figsize=(max(6, len(numeric_cols)), max(5, len(numeric_cols))))
        fig.patch.set_facecolor(BG_DARK); ax.set_facecolor(BG_DARK)
        im = ax.imshow(corr.values, cmap="RdYlGn", vmin=-1, vmax=1, aspect="auto")
        ax.set_xticks(range(len(numeric_cols))); ax.set_xticklabels(numeric_cols, rotation=45, ha="right", color=TEXT_COLOR)
        ax.set_yticks(range(len(numeric_cols))); ax.set_yticklabels(numeric_cols, color=TEXT_COLOR)
        ax.set_title(f"Correlación — {title}", color=TEXT_COLOR, fontsize=11, fontweight="bold")
        for i in range(len(numeric_cols)):
            for j in range(len(numeric_cols)):
                ax.text(j, i, f"{corr.values[i,j]:.2f}", ha="center", va="center", fontsize=8, color="white")
        plt.colorbar(im, ax=ax).ax.tick_params(colors=TEXT_COLOR)
        plt.tight_layout()
        return _to_png(fig)

    # Último fallback
    fig, ax = plt.subplots(figsize=(10, 5))
    cat_col, val_col = _get_cols(df)
    _setup_ax(ax, fig, title)
    _render_bar(ax, df, cat_col, val_col)
    plt.tight_layout()
    return _to_png(fig)


def _render_funnel(df: pd.DataFrame, cat_col: str, val_col: str, title: str) -> str:
    df_s   = df.sort_values(val_col, ascending=False).reset_index(drop=True)
    values = df_s[val_col].tolist()
    labels = df_s[cat_col].astype(str).tolist()
    max_val = values[0] if values else 1

    fig, ax = plt.subplots(figsize=(10, max(4, len(df_s) * 0.8)))
    fig.patch.set_facecolor(BG_DARK); ax.set_facecolor(BG_DARK)
    ax.set_title(title, color=TEXT_COLOR, fontsize=11, pad=12, fontweight="bold")

    for i, (label, val) in enumerate(zip(labels, values)):
        pct   = val / max_val
        color = CHART_COLORS[i % len(CHART_COLORS)]
        ax.barh(i, pct, left=(1 - pct) / 2, color=color, alpha=0.85, edgecolor=BG_DARK, height=0.65)
        ax.text(0.5, i, f"{label}  —  {val:,.0f}", ha="center", va="center",
                fontsize=9, color="white", fontweight="bold")
        if i > 0 and values[i - 1]:
            conv = val / values[i - 1] * 100
            ax.text(1.02, i, f"↓ {conv:.0f}%", va="center", fontsize=8, color="#f59e0b")

    ax.set_xlim(0, 1.15); ax.invert_yaxis(); ax.axis("off")
    plt.tight_layout()
    return _to_png(fig)


# ── Plotly JSON para Next.js ─────────────────────────────────────────────────

def _build_plotly_config(df: pd.DataFrame, chart_type: str, intent: str, question: str) -> dict:
    cat_col, val_col = _get_cols(df)
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    title = question[:65] if question else intent

    layout = {
        "title": {"text": title, "font": {"color": "#c2c0b6"}},
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor":  "rgba(30,30,46,0.8)",
        "font": {"color": "#c2c0b6"},
        "xaxis": {"gridcolor": "#2a2a3e", "title": cat_col},
        "yaxis": {"gridcolor": "#2a2a3e", "title": val_col},
    }

    if chart_type in ("bar", "barh"):
        trace = {
            "type": "bar",
            "x": df[val_col].tolist() if chart_type == "barh" else df[cat_col].astype(str).tolist(),
            "y": df[cat_col].astype(str).tolist() if chart_type == "barh" else df[val_col].tolist(),
            "orientation": "h" if chart_type == "barh" else "v",
            "marker": {"color": CHART_COLORS[0]},
        }
        return {"traces": [trace], "layout": layout}

    if chart_type == "bar_grouped":
        traces = [{"type": "bar", "name": col,
                   "x": df[cat_col].astype(str).tolist(), "y": df[col].tolist(),
                   "marker": {"color": CHART_COLORS[i % len(CHART_COLORS)]}}
                  for i, col in enumerate(numeric_cols[:4])]
        layout["barmode"] = "group"
        return {"traces": traces, "layout": layout}

    if chart_type in ("line", "area"):
        trace = {
            "type": "scatter", "mode": "lines+markers",
            "x": df[cat_col].astype(str).tolist(), "y": df[val_col].tolist(),
            "line": {"color": CHART_COLORS[0], "width": 2.5},
            "fill": "tozeroy" if chart_type == "area" else "none",
        }
        return {"traces": [trace], "layout": layout}

    if chart_type == "scatter":
        trace = {
            "type": "scatter", "mode": "markers",
            "x": df[numeric_cols[0]].tolist() if len(numeric_cols) >= 2 else list(range(len(df))),
            "y": df[numeric_cols[1]].tolist() if len(numeric_cols) >= 2 else df[numeric_cols[0]].tolist(),
            "marker": {"color": CHART_COLORS[0], "size": 8},
        }
        return {"traces": [trace], "layout": layout}

    if chart_type == "pie":
        trace = {
            "type": "pie",
            "labels": df[cat_col].astype(str).tolist(),
            "values": df[val_col].tolist(),
            "marker": {"colors": CHART_COLORS[:len(df)]},
            "hole": 0.3,
        }
        layout.pop("xaxis", None); layout.pop("yaxis", None)
        return {"traces": [trace], "layout": layout}

    if chart_type == "waterfall":
        trace = {
            "type": "waterfall",
            "x": df[cat_col].astype(str).tolist(),
            "y": df[val_col].tolist(),
            "connector": {"line": {"color": TEXT_COLOR}},
            "increasing": {"marker": {"color": "#10b981"}},
            "decreasing": {"marker": {"color": "#ef4444"}},
            "totals":     {"marker": {"color": CHART_COLORS[2]}},
        }
        return {"traces": [trace], "layout": layout}

    if chart_type == "funnel":
        df_s = df.sort_values(val_col, ascending=False)
        trace = {
            "type": "funnel",
            "y": df_s[cat_col].astype(str).tolist(),
            "x": df_s[val_col].tolist(),
            "textinfo": "value+percent initial",
            "marker": {"color": CHART_COLORS[:len(df_s)]},
        }
        layout.pop("xaxis", None); layout.pop("yaxis", None)
        return {"traces": [trace], "layout": layout}

    if chart_type == "heatmap":
        cat_cols = df.select_dtypes(include="object").columns.tolist()
        if len(cat_cols) >= 2 and numeric_cols:
            try:
                pivot = df.pivot_table(index=cat_cols[0], columns=cat_cols[1],
                                       values=numeric_cols[0], aggfunc="sum", fill_value=0)
                trace = {
                    "type": "heatmap",
                    "z": pivot.values.tolist(),
                    "x": pivot.columns.astype(str).tolist(),
                    "y": pivot.index.astype(str).tolist(),
                    "colorscale": "YlOrRd",
                }
                layout.pop("xaxis", None); layout.pop("yaxis", None)
                return {"traces": [trace], "layout": layout}
            except Exception:
                pass
        if len(numeric_cols) >= 2:
            corr = df[numeric_cols].corr()
            trace = {"type": "heatmap", "z": corr.values.tolist(),
                     "x": numeric_cols, "y": numeric_cols, "colorscale": "RdYlGn", "zmin": -1, "zmax": 1}
            return {"traces": [trace], "layout": layout}

    return {"traces": [], "layout": layout}


# ── Recharts ─────────────────────────────────────────────────────────────────

# Todos los tipos disponibles — se exponen al frontend para el selector
AVAILABLE_CHART_TYPES = [
    {"value": "bar",         "label": "Barras",          "icon": "📊"},
    {"value": "bar_horizontal", "label": "Barras horiz.", "icon": "📊"},
    {"value": "line",        "label": "Línea",           "icon": "📈"},
    {"value": "area",        "label": "Área",            "icon": "📉"},
    {"value": "pie",         "label": "Torta",           "icon": "🥧"},
    {"value": "scatter",     "label": "Dispersión",      "icon": "🔵"},
    {"value": "bar_grouped", "label": "Barras agrupadas","icon": "📊"},
    {"value": "waterfall",   "label": "Cascada",         "icon": "🌊"},
    {"value": "funnel",      "label": "Embudo",          "icon": "🔻"},
    {"value": "heatmap",     "label": "Mapa de calor",   "icon": "🌡️"},
]

# Mapeo Plotly → Recharts
_PLOTLY_TO_RECHARTS = {
    "bar":     "bar",
    "scatter": "line",
    "pie":     "pie",
}

def to_recharts(plotly_json: dict, df: pd.DataFrame,
                chart_type: str = "bar", title: str = "") -> dict:
    """
    Convierte plotly_json + DataFrame al formato que espera el frontend Recharts.
    Incluye chart_type garantizado (nunca None/undefined), title, kpis y
    available_types para el selector de gráfico en la UI.
    """
    if df is None or df.empty:
        return {
            "chart_type": "bar", "title": title, "data": [],
            "keys": {"x": "", "y": []}, "kpis": [],
            "available_types": AVAILABLE_CHART_TYPES,
        }

    cat_col, val_col = _get_cols(df)
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    text_cols    = [c for c in df.columns if c not in numeric_cols]
    layout       = (plotly_json or {}).get("layout", {})

    # Normalizar chart_type — nunca None ni string vacío
    safe_type = (chart_type or "bar").lower().strip()

    # Mapear barh → bar_horizontal para Recharts
    if safe_type == "barh":
        safe_type = "bar_horizontal"

    # Datos: máx 50 filas para no sobrecargar el render
    data = df.head(50).to_dict(orient="records")

    x_key  = text_cols[0] if text_cols else df.columns[0]
    y_keys = numeric_cols[:3]

    # KPIs automáticos: cuando el resultado tiene 1 sola fila o solo números
    kpis = []
    if len(df) == 1:
        for col in numeric_cols[:4]:
            val = df[col].iloc[0]
            kpis.append({
                "label": col.replace("_", " ").title(),
                "value": round(float(val), 2) if val is not None else 0,
                "trend": None,
            })
        if kpis:
            safe_type = "kpi"

    # Título: prioridad → parámetro → layout del plotly → vacío
    layout_title = layout.get("title", {})
    final_title = (
        title
        or (layout_title.get("text") if isinstance(layout_title, dict) else str(layout_title))
        or ""
    )

    return {
        "chart_type":      safe_type,           # garantizado string no vacío
        "title":           final_title,
        "data":            data,
        "keys":            {"x": x_key, "y": y_keys},
        "kpis":            kpis,
        "available_types": AVAILABLE_CHART_TYPES,  # para el selector en la UI
        "color":           CHART_COLORS[0],
        "colors":          CHART_COLORS[:6],
    }


# ── Teams ────────────────────────────────────────────────────────────────────

def build_teams_card(question: str, sql: str, user_message: str,
                     explanation: str = "", image_url: str = None,
                     png_base64: str = None, chart_type: str = "") -> dict:
    body = [
        {"type": "TextBlock", "text": "📊 DataTalk — Resultado de análisis",
         "weight": "Bolder", "size": "Medium", "color": "Accent"},
        {"type": "TextBlock", "text": f"**Pregunta:** {question}", "wrap": True},
        {"type": "TextBlock", "text": user_message, "wrap": True, "color": "Good"},
    ]
    if chart_type:
        body.append({"type": "TextBlock", "text": f"_Tipo de gráfico: {chart_type}_",
                     "wrap": True, "isSubtle": True, "size": "Small"})
    if explanation:
        body.append({"type": "TextBlock", "text": explanation, "wrap": True, "isSubtle": True})
    if image_url:
        body.append({"type": "Image", "url": image_url, "size": "Stretch"})
    elif png_base64:
        body.append({"type": "TextBlock",
                     "text": "_(PNG generado — requiere Azure Blob Storage para Teams)_",
                     "wrap": True, "isSubtle": True, "size": "Small"})
    body.append({"type": "ActionSet", "actions": [{"type": "Action.ShowCard",
        "title": "Ver SQL ejecutado",
        "card": {"type": "AdaptiveCard", "body": [
            {"type": "TextBlock", "text": sql or "(sin SQL)",
             "fontType": "Monospace", "wrap": True, "size": "Small"}]}}]})
    return {"type": "AdaptiveCard",
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "version": "1.5", "body": body}


def send_to_teams_webhook(webhook_url: str, card: dict) -> bool:
    import requests
    payload = {"type": "message", "attachments": [{
        "contentType": "application/vnd.microsoft.card.adaptive", "content": card}]}
    try:
        r = requests.post(webhook_url, json=payload, timeout=10)
        return r.status_code in (200, 202)
    except Exception:
        return False

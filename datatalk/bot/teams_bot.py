"""
DataTalk Teams Bot
Recibe mensajes de Teams via Bot Framework y los rutea al orquestador.
Corre en el mismo proceso que FastAPI en /api/messages.
"""

import httpx
import base64
from pathlib import Path
from botbuilder.core import ActivityHandler, TurnContext, CardFactory
from botbuilder.schema import Activity, ActivityTypes, Attachment

# Estado en memoria: user_id → {"file_path": str, "query_id": str, "pending_question": str}
_user_state: dict[str, dict] = {}

UPLOADS_DIR = Path("uploads")
UPLOADS_DIR.mkdir(exist_ok=True)

API_BASE = "http://localhost:8000"


def _get_state(user_id: str) -> dict:
    if user_id not in _user_state:
        _user_state[user_id] = {"file_path": None, "query_id": None, "pending_question": None}
    return _user_state[user_id]


class DataTalkBot(ActivityHandler):

    async def on_message_activity(self, turn_context: TurnContext):
        user_id = turn_context.activity.from_property.id
        state = _get_state(user_id)

        # ── 0. Respuesta de Adaptive Card (Action.Submit) ──────────────────
        value = turn_context.activity.value
        if value and isinstance(value, dict):
            action = value.get("action", "")
            if action == "aprobar":
                await self._handle_approve(turn_context, user_id, state, approved=True)
                return
            if action == "rechazar":
                await self._handle_approve(turn_context, user_id, state, approved=False)
                return
            if action == "listar":
                await self._handle_list_files(turn_context, state)
                return

        text = (turn_context.activity.text or "").strip()

        # ── 1. Archivo adjunto ─────────────────────────────────────────────
        if turn_context.activity.attachments:
            for att in turn_context.activity.attachments:
                if att.content_type in (
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    "application/vnd.ms-excel",
                    "text/csv",
                ):
                    await self._handle_file_upload(turn_context, att, user_id, state)
                    return

        # ── 2. Comandos ────────────────────────────────────────────────────
        if text.lower().startswith("/usar "):
            await self._handle_use_command(turn_context, text[6:].strip(), user_id, state)
            return

        if text.lower() in ("/archivos", "/files", "/listar"):
            await self._handle_list_files(turn_context, state)
            return

        if text.lower() in ("/ayuda", "/help", "/start", "hola", "hello", "hi"):
            await self._send_help(turn_context, state)
            return

        # ── 3. Aprobar/Rechazar por texto (fallback) ───────────────────────
        if text.lower() in ("aprobar", "approve"):
            await self._handle_approve(turn_context, user_id, state, approved=True)
            return
        if text.lower() in ("rechazar", "reject"):
            await self._handle_approve(turn_context, user_id, state, approved=False)
            return

        # ── 4. Sin archivo seleccionado ────────────────────────────────────
        if not state["file_path"]:
            await self._send_no_file_card(turn_context)
            return

        # ── 5. Pregunta en lenguaje natural ───────────────────────────────
        await self._handle_question(turn_context, text, user_id, state)

    # -----------------------------------------------------------------------
    # Handlers
    # -----------------------------------------------------------------------

    async def _handle_file_upload(self, ctx: TurnContext, att, user_id: str, state: dict):
        await ctx.send_activity("⏳ Procesando archivo...")
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                if hasattr(att, "content") and att.content:
                    file_bytes = base64.b64decode(att.content)
                else:
                    r = await client.get(att.content_url)
                    r.raise_for_status()
                    file_bytes = r.content

                file_path = UPLOADS_DIR / att.name
                file_path.write_bytes(file_bytes)

                with open(file_path, "rb") as f:
                    resp = await client.post(
                        f"{API_BASE}/upload",
                        files={"file": (att.name, f)},
                        params={"user_id": user_id},
                    )
                resp.raise_for_status()
                data = resp.json()

            state["file_path"] = str(file_path)
            cols = [c["name"] for c in data.get("columns", [])]
            await ctx.send_activity(Activity(
                type=ActivityTypes.message,
                attachments=[self._build_upload_success_card(att.name, data["row_count"], cols, data.get("warnings", []))],
            ))
        except Exception as e:
            await ctx.send_activity(Activity(
                type=ActivityTypes.message,
                attachments=[self._build_error_card("Error al subir archivo", str(e))],
            ))

    async def _handle_use_command(self, ctx: TurnContext, filename: str, user_id: str, state: dict):
        candidates = list(UPLOADS_DIR.glob(f"*{filename}*"))
        if not candidates:
            files = [f.name for f in UPLOADS_DIR.iterdir() if f.is_file()]
            await ctx.send_activity(Activity(
                type=ActivityTypes.message,
                attachments=[self._build_file_not_found_card(filename, files)],
            ))
            return
        chosen = candidates[0]
        state["file_path"] = str(chosen)
        await ctx.send_activity(Activity(
            type=ActivityTypes.message,
            attachments=[self._build_file_active_card(chosen.name)],
        ))

    async def _handle_list_files(self, ctx: TurnContext, state: dict):
        files = [f.name for f in UPLOADS_DIR.iterdir() if f.is_file() and not f.name.startswith(".")]
        active = Path(state["file_path"]).name if state.get("file_path") else None
        await ctx.send_activity(Activity(
            type=ActivityTypes.message,
            attachments=[self._build_file_list_card(files, active)],
        ))

    async def _handle_question(self, ctx: TurnContext, question: str, user_id: str, state: dict):
        await ctx.send_activity("🧠 Analizando tu pregunta...")
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    f"{API_BASE}/query",
                    json={
                        "question": question,
                        "file_path": state["file_path"],
                        "user_id": user_id,
                        "generate_chart": True,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
        except Exception as e:
            await ctx.send_activity(Activity(
                type=ActivityTypes.message,
                attachments=[self._build_error_card("Error al procesar la pregunta", str(e))],
            ))
            return

        state["query_id"] = data["query_id"]
        state["pending_question"] = question
        await ctx.send_activity(Activity(
            type=ActivityTypes.message,
            attachments=[self._build_approval_card(
                question=question,
                intent=data["intent"],
                sql=data["sql"],
                warnings=data.get("warnings", []),
                sensitive=data.get("sensitive", False),
            )],
        ))

    async def _handle_approve(self, ctx: TurnContext, user_id: str, state: dict, approved: bool):
        if not state.get("query_id"):
            await ctx.send_activity("⚠️ No hay ninguna consulta pendiente de aprobación.")
            return

        if not approved:
            state["query_id"] = None
            state["pending_question"] = None
            await ctx.send_activity(Activity(
                type=ActivityTypes.message,
                attachments=[self._build_simple_card("❌ Consulta cancelada", "Podés reformular tu pregunta cuando quieras.")],
            ))
            return

        await ctx.send_activity("⚙️ Ejecutando consulta...")

        try:
            async with httpx.AsyncClient(timeout=60) as client:
                resp = await client.post(
                    f"{API_BASE}/approve",
                    json={"query_id": state["query_id"], "approved": True, "approved_by": user_id},
                )
                resp.raise_for_status()
                data = resp.json()
        except Exception as e:
            await ctx.send_activity(Activity(
                type=ActivityTypes.message,
                attachments=[self._build_error_card("Error al ejecutar", str(e))],
            ))
            return

        state["query_id"] = None
        state["pending_question"] = None

        if not data.get("success"):
            await ctx.send_activity(Activity(
                type=ActivityTypes.message,
                attachments=[self._build_error_card("No se pudo ejecutar", data.get("message", ""))],
            ))
            return

        rows = data.get("data", [])
        explanation = data.get("explanation", "")
        intent = data.get("intent", "")
        autocorrected = data.get("autocorrected", False)
        attempts = data.get("attempts", 1)

        await ctx.send_activity(Activity(
            type=ActivityTypes.message,
            attachments=[self._build_result_card(rows, explanation, intent, autocorrected, attempts)],
        ))

        chart = data.get("chart")
        if chart and chart.get("success") and chart.get("png_base64"):
            await self._send_chart_image(ctx, chart["png_base64"])

    async def _send_chart_image(self, ctx: TurnContext, png_base64: str):
        try:
            await ctx.send_activity(Activity(
                type=ActivityTypes.message,
                attachments=[Attachment(
                    content_type="image/png",
                    content_url=f"data:image/png;base64,{png_base64}",
                    name="grafico.png",
                )],
            ))
        except Exception:
            pass

    # -----------------------------------------------------------------------
    # Adaptive Cards
    # -----------------------------------------------------------------------

    async def _send_help(self, ctx: TurnContext, state: dict):
        active_file = Path(state["file_path"]).name if state.get("file_path") else None
        card = {
            "type": "AdaptiveCard",
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "version": "1.4",
            "body": [
                {
                    "type": "Container",
                    "style": "accent",
                    "bleed": True,
                    "items": [
                        {
                            "type": "ColumnSet",
                            "columns": [
                                {
                                    "type": "Column", "width": "auto",
                                    "items": [{"type": "TextBlock", "text": "🧠", "size": "ExtraLarge"}]
                                },
                                {
                                    "type": "Column", "width": "stretch",
                                    "items": [
                                        {"type": "TextBlock", "text": "DataTalk", "weight": "Bolder", "size": "ExtraLarge", "color": "Light"},
                                        {"type": "TextBlock", "text": "Agente analítico · Microsoft Innovation Challenge 2025", "size": "Small", "color": "Light", "isSubtle": True, "spacing": "None"},
                                    ]
                                }
                            ]
                        }
                    ]
                },
                {
                    "type": "TextBlock",
                    "text": f"{'✅ Archivo activo: **' + active_file + '**' if active_file else '⚠️ Sin archivo activo — cargá uno para empezar'}",
                    "wrap": True,
                    "spacing": "Medium",
                    "color": "Good" if active_file else "Warning",
                },
                {
                    "type": "TextBlock",
                    "text": "COMANDOS",
                    "weight": "Bolder",
                    "size": "Small",
                    "color": "Accent",
                    "spacing": "Medium",
                },
                {
                    "type": "FactSet",
                    "spacing": "Small",
                    "facts": [
                        {"title": "/usar <archivo>", "value": "Seleccionar archivo de datos activo"},
                        {"title": "/archivos", "value": "Ver todos los archivos cargados"},
                        {"title": "/ayuda", "value": "Mostrar este menú de ayuda"},
                    ]
                },
                {
                    "type": "TextBlock",
                    "text": "EJEMPLOS DE PREGUNTAS",
                    "weight": "Bolder",
                    "size": "Small",
                    "color": "Accent",
                    "spacing": "Medium",
                },
                {
                    "type": "FactSet",
                    "spacing": "Small",
                    "facts": [
                        {"title": "🏆 Ranking", "value": "¿Cuáles son las 3 categorías con más ventas?"},
                        {"title": "📈 Tendencia", "value": "¿Cómo evolucionaron las ventas mes a mes?"},
                        {"title": "⚖️ Comparativa", "value": "Compará ventas entre zona Norte y Sur"},
                        {"title": "🚨 Anomalía", "value": "¿Por qué cayeron las ventas en mayo?"},
                        {"title": "🔢 Agregación", "value": "¿Cuál es el total de ventas del último trimestre?"},
                    ]
                },
                {
                    "type": "TextBlock",
                    "text": "FLUJO DE TRABAJO",
                    "weight": "Bolder",
                    "size": "Small",
                    "color": "Accent",
                    "spacing": "Medium",
                },
                {
                    "type": "FactSet",
                    "spacing": "Small",
                    "facts": [
                        {"title": "1. Subir datos", "value": "Arrastrá un Excel/CSV o usá /usar"},
                        {"title": "2. Preguntar", "value": "Escribí tu pregunta en español"},
                        {"title": "3. Aprobar SQL", "value": "Revisá el SQL generado y aprobá"},
                        {"title": "4. Ver resultado", "value": "Tabla + gráfico automático"},
                    ]
                },
                {
                    "type": "TextBlock",
                    "text": "🔒 Audit log activo · Human-in-the-loop · Responsible AI",
                    "size": "Small",
                    "isSubtle": True,
                    "spacing": "Medium",
                    "horizontalAlignment": "Center",
                }
            ],
            "actions": [
                {"type": "Action.Submit", "title": "📂 Ver archivos disponibles", "data": {"action": "listar"}},
            ]
        }
        await ctx.send_activity(Activity(type=ActivityTypes.message, attachments=[CardFactory.adaptive_card(card)]))

    def _build_upload_success_card(self, filename: str, row_count: int, cols: list, warnings: list) -> Attachment:
        col_preview = ", ".join(cols[:6]) + ("..." if len(cols) > 6 else "")
        body = [
            {
                "type": "Container", "style": "good", "bleed": True,
                "items": [{"type": "TextBlock", "text": "✅ Archivo cargado exitosamente", "weight": "Bolder", "color": "Light", "size": "Medium"}]
            },
            {
                "type": "FactSet", "spacing": "Medium",
                "facts": [
                    {"title": "Archivo", "value": filename},
                    {"title": "Filas", "value": f"{row_count:,}"},
                    {"title": "Columnas", "value": str(len(cols))},
                    {"title": "Vista previa", "value": col_preview},
                ]
            },
            {"type": "TextBlock", "text": "Ya podés hacer preguntas sobre estos datos.", "wrap": True, "spacing": "Medium", "isSubtle": True},
        ]
        if warnings:
            body.append({"type": "TextBlock", "text": "⚠️ " + " | ".join(warnings[:3]), "wrap": True, "color": "Warning", "size": "Small", "spacing": "Small"})
        return CardFactory.adaptive_card({"type": "AdaptiveCard", "$schema": "http://adaptivecards.io/schemas/adaptive-card.json", "version": "1.4", "body": body})

    def _build_approval_card(self, question: str, intent: str, sql: str, warnings: list, sensitive: bool) -> Attachment:
        intent_map = {"RANKING": "🏆 Ranking", "TENDENCIA": "📈 Tendencia", "COMPARATIVA": "⚖️ Comparativa", "ANOMALIA": "🚨 Anomalía", "AGREGACION": "🔢 Agregación"}
        intent_label = intent_map.get(intent, f"📊 {intent}")
        body = [
            {
                "type": "Container", "style": "accent", "bleed": True,
                "items": [{"type": "TextBlock", "text": "🔍 Revisá el SQL antes de ejecutar", "weight": "Bolder", "color": "Light", "size": "Medium"}]
            },
            {
                "type": "FactSet", "spacing": "Medium",
                "facts": [
                    {"title": "Pregunta", "value": question},
                    {"title": "Tipo de análisis", "value": intent_label},
                ]
            },
            {"type": "TextBlock", "text": "SQL generado:", "weight": "Bolder", "spacing": "Medium"},
            {"type": "TextBlock", "text": sql, "fontType": "Monospace", "size": "Small", "wrap": True, "color": "Good", "spacing": "Small"},
        ]
        if sensitive:
            body.append({"type": "TextBlock", "text": "🔒 Archivo sensible — los resultados son confidenciales.", "wrap": True, "color": "Warning", "size": "Small", "spacing": "Medium"})
        if warnings:
            body.append({"type": "TextBlock", "text": "⚠️ " + " | ".join(warnings[:2]), "wrap": True, "color": "Warning", "size": "Small", "spacing": "Small"})
        body.append({"type": "TextBlock", "text": "¿Ejecutamos esta consulta?", "weight": "Bolder", "spacing": "Medium"})
        return CardFactory.adaptive_card({
            "type": "AdaptiveCard", "$schema": "http://adaptivecards.io/schemas/adaptive-card.json", "version": "1.4",
            "body": body,
            "actions": [
                {"type": "Action.Submit", "title": "✅ Aprobar y ejecutar", "style": "positive", "data": {"action": "aprobar"}},
                {"type": "Action.Submit", "title": "❌ Rechazar", "style": "destructive", "data": {"action": "rechazar"}},
            ]
        })

    def _build_result_card(self, rows: list, explanation: str, intent: str, autocorrected: bool, attempts: int) -> Attachment:
        intent_map = {"RANKING": "🏆", "TENDENCIA": "📈", "COMPARATIVA": "⚖️", "ANOMALIA": "🚨", "AGREGACION": "🔢"}
        emoji = intent_map.get(intent, "📊")
        body = [
            {
                "type": "Container", "style": "good", "bleed": True,
                "items": [{"type": "TextBlock", "text": f"{emoji} Resultado — {len(rows)} {'fila' if len(rows)==1 else 'filas'}", "weight": "Bolder", "color": "Light", "size": "Medium"}]
            },
        ]
        if explanation:
            body.append({"type": "TextBlock", "text": f"💡 {explanation}", "wrap": True, "spacing": "Medium"})

        if rows:
            cols = list(rows[0].keys())
            col_defs = [{"type": "TableColumnDefinition", "width": 1} for _ in cols]
            header_cells = [{"type": "TableCell", "items": [{"type": "TextBlock", "text": c, "weight": "Bolder", "size": "Small", "wrap": False}]} for c in cols]
            data_rows = []
            for row in rows[:10]:
                cells = [{"type": "TableCell", "items": [{"type": "TextBlock", "text": str(row.get(c, "")), "size": "Small", "wrap": False}]} for c in cols]
                data_rows.append({"type": "TableRow", "cells": cells})
            body.append({
                "type": "Table",
                "columns": col_defs,
                "rows": [{"type": "TableRow", "cells": header_cells, "style": "accent"}] + data_rows,
                "spacing": "Medium",
                "showGridLines": True,
            })
            if len(rows) > 10:
                body.append({"type": "TextBlock", "text": f"Mostrando 10 de {len(rows)} filas", "isSubtle": True, "size": "Small", "spacing": "Small"})

        if autocorrected:
            body.append({"type": "TextBlock", "text": f"⚡ SQL autocorregido automáticamente ({attempts} intentos)", "isSubtle": True, "size": "Small", "color": "Warning", "spacing": "Small"})

        return CardFactory.adaptive_card({"type": "AdaptiveCard", "$schema": "http://adaptivecards.io/schemas/adaptive-card.json", "version": "1.5", "body": body})

    def _build_file_list_card(self, files: list, active: str) -> Attachment:
        if not files:
            body = [{"type": "TextBlock", "text": "📂 No hay archivos cargados todavía.", "wrap": True}]
        else:
            facts = [{"title": f"{'✅ ' if f == active else ''}{f}", "value": f"/usar {f}"} for f in files[:10]]
            body = [
                {"type": "TextBlock", "text": "📂 Archivos disponibles", "weight": "Bolder", "size": "Medium"},
                {"type": "TextBlock", "text": "Copiá el comando para activar el archivo que necesites.", "isSubtle": True, "size": "Small", "spacing": "None"},
                {"type": "FactSet", "facts": facts, "spacing": "Medium"},
            ]
        return CardFactory.adaptive_card({"type": "AdaptiveCard", "$schema": "http://adaptivecards.io/schemas/adaptive-card.json", "version": "1.4", "body": body})

    def _build_file_active_card(self, filename: str) -> Attachment:
        return CardFactory.adaptive_card({
            "type": "AdaptiveCard", "$schema": "http://adaptivecards.io/schemas/adaptive-card.json", "version": "1.4",
            "body": [
                {"type": "Container", "style": "good", "bleed": True, "items": [
                    {"type": "TextBlock", "text": "✅ Archivo activo seleccionado", "weight": "Bolder", "color": "Light"}
                ]},
                {"type": "TextBlock", "text": filename, "size": "Large", "weight": "Bolder", "spacing": "Medium"},
                {"type": "TextBlock", "text": "Ya podés hacerme preguntas sobre estos datos.", "isSubtle": True, "spacing": "None"},
            ]
        })

    def _build_file_not_found_card(self, filename: str, available: list) -> Attachment:
        facts = [{"title": f, "value": f"/usar {f}"} for f in available[:8]]
        body = [{"type": "TextBlock", "text": f"❌ No encontré: {filename}", "wrap": True, "color": "Attention", "weight": "Bolder"}]
        if facts:
            body += [
                {"type": "TextBlock", "text": "Archivos disponibles:", "weight": "Bolder", "spacing": "Medium"},
                {"type": "FactSet", "facts": facts},
            ]
        else:
            body.append({"type": "TextBlock", "text": "No hay archivos cargados. Subí un Excel o CSV primero.", "isSubtle": True})
        return CardFactory.adaptive_card({"type": "AdaptiveCard", "$schema": "http://adaptivecards.io/schemas/adaptive-card.json", "version": "1.4", "body": body})

    def _build_error_card(self, title: str, detail: str) -> Attachment:
        return CardFactory.adaptive_card({
            "type": "AdaptiveCard", "$schema": "http://adaptivecards.io/schemas/adaptive-card.json", "version": "1.4",
            "body": [
                {"type": "Container", "style": "attention", "bleed": True, "items": [
                    {"type": "TextBlock", "text": f"⚠️ {title}", "weight": "Bolder", "color": "Light"}
                ]},
                {"type": "TextBlock", "text": detail[:300], "wrap": True, "spacing": "Medium", "size": "Small"},
            ]
        })

    def _build_simple_card(self, title: str, body_text: str) -> Attachment:
        return CardFactory.adaptive_card({
            "type": "AdaptiveCard", "$schema": "http://adaptivecards.io/schemas/adaptive-card.json", "version": "1.4",
            "body": [
                {"type": "TextBlock", "text": title, "weight": "Bolder", "size": "Medium"},
                {"type": "TextBlock", "text": body_text, "wrap": True, "isSubtle": True},
            ]
        })

    def _build_no_file_card(self) -> Attachment:
        return CardFactory.adaptive_card({
            "type": "AdaptiveCard", "$schema": "http://adaptivecards.io/schemas/adaptive-card.json", "version": "1.4",
            "body": [
                {"type": "Container", "style": "warning", "bleed": True, "items": [
                    {"type": "TextBlock", "text": "📂 Sin archivo activo", "weight": "Bolder", "color": "Light"}
                ]},
                {"type": "TextBlock", "text": "Necesitás seleccionar un archivo antes de hacer preguntas.", "wrap": True, "spacing": "Medium"},
                {"type": "FactSet", "spacing": "Medium", "facts": [
                    {"title": "Opción 1", "value": "Subí un archivo Excel o CSV directamente en este chat"},
                    {"title": "Opción 2", "value": "Usá /archivos para ver los disponibles"},
                    {"title": "Opción 3", "value": "Usá /usar <nombre> para activar uno existente"},
                ]},
            ]
        })

    async def _send_no_file_card(self, ctx: TurnContext):
        await ctx.send_activity(Activity(type=ActivityTypes.message, attachments=[self._build_no_file_card()]))

    async def on_members_added_activity(self, members_added, turn_context: TurnContext):
        state = _get_state(turn_context.activity.from_property.id)
        for member in members_added:
            if member.id != turn_context.activity.recipient.id:
                await self._send_help(turn_context, state)
"""
API FastAPI — DataTalk
Endpoints: /health, /upload, /query, /approve, /history, /audit, /auth
"""

import os
import uuid
import shutil
import logging
import json
from pathlib import Path

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

from datatalk.agents import orchestrator, guard_agent, schema_agent, query_agent

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="DataTalk API",
    version="1.0.0",
    description="Agente Text-to-SQL con validación y audit log",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("ALLOWED_ORIGINS", "*").split(","),
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)

# ---------------------------------------------------------------------------
# Routers — DESPUÉS de crear app
# ---------------------------------------------------------------------------

from datatalk.api.routes.audit_viewer import router as audit_router
from datatalk.api.routes.auth import router as auth_router

app.include_router(audit_router, prefix="/audit", tags=["Audit"])
app.include_router(auth_router,  prefix="/auth",  tags=["Auth"])

# ---------------------------------------------------------------------------
# Estado en memoria
# ---------------------------------------------------------------------------

_pending: dict = {}

UPLOADS_DIR = Path("uploads")
UPLOADS_DIR.mkdir(exist_ok=True)

# ---------------------------------------------------------------------------
# Modelos
# ---------------------------------------------------------------------------

class QueryRequest(BaseModel):
    question: str
    file_path: str
    user_id: str = "demo_user"
    generate_chart: bool = False


class ApproveRequest(BaseModel):
    query_id: str
    approved: bool
    approved_by: str = "user"
    edited_sql: str = None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health", tags=["Sistema"])
def health():
    return {"status": "ok", "service": "DataTalk API", "version": "1.0.0"}


@app.post("/upload", tags=["Datos"])
async def upload(file: UploadFile = File(...), user_id: str = "demo_user"):
    """
    Sube un archivo Excel/CSV.
    1. Guard: valida acceso
    2. Guarda en uploads/ local
    3. Sube a Azure Blob Storage (si está configurado)
    4. Retorna schema detectado
    """
    allowed_ext = {".xlsx", ".xls", ".csv", ".tsv"}
    ext = Path(file.filename).suffix.lower()
    if ext not in allowed_ext:
        raise HTTPException(400, f"Formato no soportado. Usa: {allowed_ext}")

    file_path = UPLOADS_DIR / file.filename
    with open(file_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    guard = guard_agent.validate_and_log(
        user_id=user_id, question="upload", file_path=str(file_path), action="upload"
    )
    if not guard["allowed"]:
        file_path.unlink(missing_ok=True)
        raise HTTPException(403, guard["reason"])

    try:
        schema = schema_agent.run(str(file_path))
    except Exception as e:
        raise HTTPException(422, f"Error leyendo el archivo: {str(e)}")

    # Blob Storage (opcional)
    blob_url = None
    try:
        from datatalk.data.blob_storage import blob_available, upload_file as blob_upload
        if blob_available():
            blob_url = blob_upload(str(file_path))
    except Exception as e:
        logger.warning(f"Blob upload falló (no crítico): {e}")

    return {
        "file_path": str(file_path),
        "blob_url": blob_url,
        "storage": "azure_blob" if blob_url else "local",
        "table_name": schema["table_name"],
        "row_count": schema["row_count"],
        "columns": schema["columns"],
        "warnings": schema["warnings"],
        "sensitive": guard_agent.is_sensitive_file(str(file_path)),
    }


@app.post("/query", tags=["Consultas"])
def query(req: QueryRequest):
    """
    Recibe pregunta → Guard → clasifica intención → genera SQL.
    Devuelve el SQL para aprobación del usuario (human-in-the-loop).
    """
    guard = guard_agent.validate_and_log(
        user_id=req.user_id, question=req.question, file_path=req.file_path
    )
    if not guard["allowed"]:
        raise HTTPException(403, guard["reason"])

    try:
        intent = orchestrator.classify_intent(req.question)
        schema = schema_agent.run(req.file_path)
        sql = query_agent.generate_sql(intent, schema, req.question)
    except Exception as e:
        raise HTTPException(500, f"Error generando SQL: {str(e)}")

    query_id = str(uuid.uuid4())
    _pending[query_id] = {
        "question":      req.question,
        "file_path":     req.file_path,
        "user_id":       req.user_id,
        "intent":        intent,
        "sql":           sql,
        "generate_chart": req.generate_chart,
        "sensitive":     guard["sensitive"],
    }

    return {
        "query_id": query_id,
        "intent":   intent,
        "sql":      sql,
        "sensitive": guard["sensitive"],
        "message":  "SQL generado. Revisá y aprobá para ejecutar.",
        "warnings": schema.get("warnings", []),
    }


@app.post("/approve", tags=["Consultas"])
def approve(req: ApproveRequest):
    """
    Ejecuta el SQL después de la aprobación explícita del usuario.
    Human-in-the-loop requerido por el reto.
    """
    pending = _pending.get(req.query_id)
    if not pending:
        raise HTTPException(404, "Consulta no encontrada o expirada.")

    if not req.approved:
        del _pending[req.query_id]
        guard_agent.log_query_result(
            user_id=pending["user_id"], question=pending["question"],
            sql=pending["sql"], success=False, attempts=0,
            autocorrected=False, approved_by=None,
        )
        return {"message": "Consulta rechazada. Podés reformular la pregunta."}

    try:
        result = query_agent.run_with_validation(
            intent=pending["intent"],
            question=pending["question"],
            file_path=pending["file_path"],
        )
    except Exception as e:
        raise HTTPException(500, str(e))

    guard_agent.log_query_result(
        user_id=pending["user_id"], question=pending["question"],
        sql=result["sql_final"], success=result["success"],
        attempts=result["attempts"], autocorrected=result["autocorrected"],
        approved_by=req.approved_by,
    )
    del _pending[req.query_id]

    if not result["success"]:
        return {"success": False, "message": result["user_message"]}

    df = result["data"]
    explanation = orchestrator._explain_results(pending["question"], pending["intent"], df)

    chart = None
    if pending["generate_chart"] and df is not None and not df.empty:
        from datatalk.agents import dashboard_agent
        chart = dashboard_agent.generate_dashboard(
            df=df, intent=pending["intent"], question=pending["question"]
        )

    return {
        "success":      True,
        "intent":       pending["intent"],
        "sql":          result["sql_final"],
        "data":         df.to_dict(orient="records") if df is not None else [],
        "explanation":  explanation,
        "autocorrected": result["autocorrected"],
        "attempts":     result["attempts"],
        "chart":        chart,
        "message":      result["user_message"],
    }


@app.get("/history", tags=["Audit"])
def history(limit: int = 50):
    """Devuelve las últimas entradas del audit log."""
    log_path = Path("logs/audit.jsonl")
    if not log_path.exists():
        return {"events": []}
    with open(log_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    events = []
    for line in lines[-limit:]:
        try:
            events.append(json.loads(line))
        except Exception:
            pass
    return {"events": list(reversed(events))}
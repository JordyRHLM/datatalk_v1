"""
Ruta de consultas — recibe pregunta en lenguaje natural, retorna SQL + resultados.
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from datatalk.agents.orchestrator import Orchestrator
from datatalk.core.rbac import UserContext, Role

router = APIRouter()
orchestrator = Orchestrator()


class QueryRequest(BaseModel):
    question: str
    user_id: str = "demo_user"
    role: str = "admin"
    branch_id: str | None = None
    auto_approve: bool = False


class ApproveRequest(BaseModel):
    sql: str
    user_id: str
    role: str
    branch_id: str | None = None
    question: str


@router.post("/ask")
async def ask(request: QueryRequest):
    """
    Convierte una pregunta en SQL, la valida y la muestra para aprobación.
    Si auto_approve=True la ejecuta directamente (solo para desarrollo).
    """
    user = UserContext(
        user_id=request.user_id,
        role=Role(request.role),
        branch_id=request.branch_id,
    )
    try:
        result = await orchestrator.handle_query(
            question=request.question,
            user=user,
            auto_approve=request.auto_approve,
        )
        return result
    except Exception as e:
        raise HTTPException(500, str(e))


@router.post("/approve")
async def approve_and_execute(request: ApproveRequest):
    """
    El usuario revisó el SQL y lo aprobó — ejecutar y retornar resultados.
    Esto implementa el flujo de aprobación humana requerido por el reto.
    """
    from datatalk.agents.query_agent import SQLAgent
    from datatalk.core.audit import log_query

    sql_agent = SQLAgent()
    execution = await sql_agent.execute(request.sql)

    log_query(
        user_id=request.user_id,
        user_role=request.role,
        question=request.question,
        sql_generated=request.sql,
        approved=True,
        rows_returned=execution.get("rows_returned"),
        error=execution.get("error"),
    )

    return {
        "status": "completed",
        "sql": request.sql,
        "results": execution.get("data"),
        "summary": execution.get("summary"),
        "rows_returned": execution.get("rows_returned"),
    }

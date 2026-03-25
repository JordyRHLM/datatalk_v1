"""
Audit log — registra cada consulta al agente.
Responsible AI: trazabilidad completa de quién preguntó qué y cuándo.
"""
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


def log_query(
    user_id: str,
    user_role: str,
    question: str,
    sql_generated: str,
    approved: bool,
    rows_returned: Optional[int] = None,
    error: Optional[str] = None,
) -> None:
    """Registra cada consulta en el audit log."""
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": user_id,
        "user_role": user_role,
        "question": question,
        "sql_generated": sql_generated,
        "approved": approved,
        "rows_returned": rows_returned,
        "error": error,
    }
    # TODO: enviar a Azure Monitor cuando las credenciales estén configuradas
    logger.info("AUDIT_LOG: %s", entry)

"""
Guard Agent — DataTalk
Valida permisos RBAC, escribe audit log y filtra prompt injection.
"""

import os
import json
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

_INJECTION_PATTERNS = [
    "ignore previous instructions", "ignore all instructions",
    "forget your instructions", "you are now", "act as", "jailbreak",
    "bypass", "drop table", "delete from", "truncate table",
    "--", "/*", "xp_cmdshell",
]


def check_prompt_injection(question: str) -> dict:
    question_lower = question.lower()
    for pattern in _INJECTION_PATTERNS:
        if pattern in question_lower:
            return {"safe": False, "reason": f"La pregunta contiene contenido no permitido: '{pattern}'"}
    return {"safe": True, "reason": None}


def validate_access(user_id: str, file_path: str, tenant_id: str = None) -> dict:
    dev_mode = os.environ.get("DEV_MODE", "true").lower() == "true"
    if dev_mode:
        return {"allowed": True, "role": "analyst", "reason": None, "dev_mode": True}
    try:
        import msal
        tenant_id = tenant_id or os.environ.get("AZURE_TENANT_ID")
        app = msal.ConfidentialClientApplication(
            os.environ.get("AZURE_CLIENT_ID"),
            authority=f"https://login.microsoftonline.com/{tenant_id}",
            client_credential=os.environ.get("AZURE_CLIENT_SECRET"),
        )
        result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
        if "access_token" in result:
            return {"allowed": True, "role": "analyst", "reason": None, "dev_mode": False}
        return {"allowed": False, "role": None,
                "reason": "No se pudo validar el token de acceso.", "dev_mode": False}
    except ImportError:
        logger.warning("msal no instalado — usando modo permisivo")
        return {"allowed": True, "role": "analyst", "reason": None, "dev_mode": True}
    except Exception as e:
        logger.error(f"Error validando acceso: {e}")
        return {"allowed": False, "role": None,
                "reason": "Error al validar permisos. Intentá nuevamente.", "dev_mode": False}


def is_sensitive_file(file_path: str) -> bool:
    sensitive_keywords = ["rrhh", "recursos_humanos", "hr_", "salario",
                          "nomina", "finanzas", "auditoria", "confidencial"]
    return any(kw in file_path.lower() for kw in sensitive_keywords)


def write_audit_log(event: dict) -> None:
    event["timestamp"] = datetime.now(timezone.utc).isoformat()
    if os.environ.get("AZURE_LOG_WORKSPACE_ID"):
        _write_azure_monitor(event)
    else:
        _write_local_log(event)


def _write_azure_monitor(event: dict) -> None:
    try:
        from azure.monitor.ingestion import LogsIngestionClient
        from azure.identity import DefaultAzureCredential
        client = LogsIngestionClient(
            endpoint=os.environ.get("AZURE_LOG_ENDPOINT"),
            credential=DefaultAzureCredential(),
        )
        client.upload(
            rule_id=os.environ.get("AZURE_LOG_RULE_ID"),
            stream_name=os.environ.get("AZURE_LOG_STREAM", "Custom-DataTalkLogs"),
            logs=[event],
        )
    except Exception as e:
        logger.error(f"Error escribiendo en Azure Monitor: {e}")
        _write_local_log(event)


def _write_local_log(event: dict) -> None:
    os.makedirs("logs", exist_ok=True)
    with open("logs/audit.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False, default=str) + "\n")


def validate_and_log(user_id: str, question: str, file_path: str, action: str = "query") -> dict:
    injection_check = check_prompt_injection(question)
    if not injection_check["safe"]:
        write_audit_log({"event": "BLOCKED_INJECTION", "user_id": user_id,
                         "question": question[:200], "file_path": file_path,
                         "reason": injection_check["reason"]})
        return {"allowed": False, "reason": injection_check["reason"],
                "sensitive": False, "role": None}

    access = validate_access(user_id, file_path)
    if not access["allowed"]:
        write_audit_log({"event": "BLOCKED_RBAC", "user_id": user_id,
                         "question": question[:200], "file_path": file_path,
                         "reason": access["reason"]})
        return {"allowed": False, "reason": "No tenés permiso para consultar este archivo.",
                "sensitive": False, "role": None}

    sensitive = is_sensitive_file(file_path)
    write_audit_log({"event": "ACCESS_GRANTED", "user_id": user_id,
                     "question": question[:200], "file_path": file_path,
                     "action": action, "role": access["role"], "sensitive": sensitive})
    return {"allowed": True, "reason": None, "sensitive": sensitive, "role": access["role"]}


def log_query_result(user_id: str, question: str, sql: str, success: bool,
                     attempts: int, autocorrected: bool, approved_by: str = None) -> None:
    write_audit_log({
        "event": "QUERY_EXECUTED" if success else "QUERY_FAILED",
        "user_id": user_id, "question": question[:200],
        "sql": sql[:500] if sql else None, "success": success,
        "attempts": attempts, "autocorrected": autocorrected, "approved_by": approved_by,
    })

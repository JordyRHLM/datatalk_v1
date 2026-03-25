"""
Rutas de autenticación — DataTalk
Soporta dos modos:
  DEV_MODE=true  → mock con usuarios hardcodeados (sin Azure)
  DEV_MODE=false → login real con Microsoft Entra ID (MSAL)
"""
import os
import logging
from fastapi import APIRouter, HTTPException, Header
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from typing import Optional

logger = logging.getLogger(__name__)
router = APIRouter()

# ── Usuarios mock para desarrollo ───────────────────────────────────────────
MOCK_USERS = {
    "admin@datatalk.com":   {"role": "admin",   "branch_id": None,    "name": "Admin Demo"},
    "norte@datatalk.com":   {"role": "manager", "branch_id": "norte", "name": "Manager Norte"},
    "sur@datatalk.com":     {"role": "manager", "branch_id": "sur",   "name": "Manager Sur"},
    "analista@datatalk.com":{"role": "analyst", "branch_id": None,    "name": "Analista Demo"},
    "viewer@datatalk.com":  {"role": "viewer",  "branch_id": None,    "name": "Viewer Demo"},
}
MOCK_PASSWORD = "demo1234"


class LoginRequest(BaseModel):
    username: str
    password: str
    branch_id: Optional[str] = None


# ── Endpoints ────────────────────────────────────────────────────────────────

@router.post("/login")
def login(request: LoginRequest):
    """
    Login unificado:
    - DEV_MODE=true  → valida contra MOCK_USERS
    - DEV_MODE=false → redirige a Microsoft Entra ID
    """
    dev_mode = os.environ.get("DEV_MODE", "true").lower() == "true"

    if dev_mode:
        return _mock_login(request)
    else:
        # En modo producción, el login es vía OAuth2 redirect
        # Este endpoint devuelve la URL de login de Microsoft
        from datatalk.core.auth import get_login_url
        login_url = get_login_url()
        return {
            "mode": "entra_id",
            "message": "Redirigir al usuario a login_url para autenticación con Microsoft",
            "login_url": login_url,
        }


@router.get("/microsoft")
def microsoft_login():
    """Inicia el flujo OAuth2 con Microsoft — redirige al login de Microsoft."""
    from datatalk.core.auth import get_login_url
    return RedirectResponse(url=get_login_url())


@router.get("/callback")
def auth_callback(code: str = None, error: str = None, error_description: str = None):
    """
    Callback OAuth2 — Microsoft redirige aquí con el código de autorización.
    Intercambia el código por un token y retorna el perfil del usuario.
    """
    if error:
        raise HTTPException(400, f"Error de autenticación: {error_description}")

    if not code:
        raise HTTPException(400, "Código de autorización no recibido")

    from datatalk.core.auth import exchange_code_for_token, extract_role_from_token

    token_result = exchange_code_for_token(code)
    if not token_result:
        raise HTTPException(401, "No se pudo obtener el token de acceso")

    access_token = token_result["access_token"]
    user_id, role = extract_role_from_token(access_token)

    return {
        "access_token": access_token,
        "token_type": "Bearer",
        "user_id": user_id,
        "role": role,
        "message": f"Autenticado como {user_id} con rol {role}",
    }


@router.get("/me")
def me(authorization: Optional[str] = Header(None)):
    """
    Retorna el perfil del usuario autenticado.
    Acepta: Authorization: Bearer <token>
    En DEV_MODE acepta: Authorization: Bearer mock:<role>
    """
    dev_mode = os.environ.get("DEV_MODE", "true").lower() == "true"

    if not authorization or not authorization.startswith("Bearer "):
        if dev_mode:
            # Sin token en dev → retornar admin por defecto
            return {"user_id": "admin@datatalk.demo", "role": "admin",
                    "branch_id": None, "mode": "dev_mock"}
        raise HTTPException(401, "Token requerido. Incluir: Authorization: Bearer <token>")

    token = authorization.replace("Bearer ", "")

    # Mock token para demos: "mock:admin", "mock:manager:norte"
    if token.startswith("mock:"):
        parts = token.split(":")
        role = parts[1] if len(parts) > 1 else "viewer"
        branch_id = parts[2] if len(parts) > 2 else None
        return {"user_id": f"{role}@datatalk.demo", "role": role,
                "branch_id": branch_id, "mode": "mock"}

    # Token real de Entra ID
    from datatalk.core.auth import extract_role_from_token
    user_id, role = extract_role_from_token(token)
    return {
        "user_id": user_id,
        "role": role,
        "branch_id": None,
        "mode": "entra_id",
        "token_valid": True,
    }


@router.get("/users")
def list_mock_users():
    """Lista los usuarios de demo disponibles (solo en DEV_MODE)."""
    dev_mode = os.environ.get("DEV_MODE", "true").lower() == "true"
    if not dev_mode:
        raise HTTPException(403, "Solo disponible en modo desarrollo")

    return {
        "dev_mode": True,
        "password": MOCK_PASSWORD,
        "users": [
            {"email": email, "role": data["role"],
             "branch_id": data["branch_id"], "name": data["name"]}
            for email, data in MOCK_USERS.items()
        ],
        "tip": "Usar token mock: 'mock:admin', 'mock:manager:norte', 'mock:analyst'",
    }


# ── Helpers ──────────────────────────────────────────────────────────────────

def _mock_login(request: LoginRequest) -> dict:
    user = MOCK_USERS.get(request.username)
    if not user or request.password != MOCK_PASSWORD:
        raise HTTPException(401, "Credenciales incorrectas")

    role      = user["role"]
    branch_id = request.branch_id or user["branch_id"]

    # Token mock legible: "mock:admin" o "mock:manager:norte"
    token_parts = ["mock", role]
    if branch_id:
        token_parts.append(branch_id)
    mock_token = ":".join(token_parts)

    return {
        "access_token": mock_token,
        "token_type":   "Bearer",
        "user_id":      request.username,
        "role":         role,
        "branch_id":    branch_id,
        "display_name": user["name"],
        "mode":         "dev_mock",
        "message":      f"Login exitoso como {role}",
    }


def get_current_user(authorization: Optional[str] = Header(None)):
    """
    Dependency injection para FastAPI.
    Usar en endpoints protegidos:

        from datatalk.api.routes.auth import get_current_user
        from datatalk.core.rbac import UserContext
        from fastapi import Depends

        @app.post("/query")
        def query(req: QueryRequest, user: UserContext = Depends(get_current_user)):
            ...
    """
    from datatalk.core.rbac import user_mock, user_from_token

    dev_mode = os.environ.get("DEV_MODE", "true").lower() == "true"

    if not authorization:
        if dev_mode:
            return user_mock("admin")
        raise HTTPException(401, "Authorization header requerido")

    token = authorization.replace("Bearer ", "")

    if token.startswith("mock:"):
        parts = token.split(":")
        role      = parts[1] if len(parts) > 1 else "viewer"
        branch_id = parts[2] if len(parts) > 2 else None
        return user_mock(role, branch_id)

    return user_from_token(token)
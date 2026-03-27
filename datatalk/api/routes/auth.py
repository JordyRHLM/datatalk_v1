"""
Rutas de autenticación — DataTalk
Mock siempre disponible + Entra ID real cuando DEV_MODE=false
"""
import os
import logging
import urllib.parse
from fastapi import APIRouter, HTTPException, Header
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from typing import Optional

logger = logging.getLogger(__name__)
router = APIRouter()

MOCK_USERS = {
    "admin@datatalk.com":    {"role": "admin",   "branch_id": None,    "name": "Admin Demo"},
    "norte@datatalk.com":    {"role": "manager", "branch_id": "norte", "name": "Manager Norte"},
    "sur@datatalk.com":      {"role": "manager", "branch_id": "sur",   "name": "Manager Sur"},
    "analista@datatalk.com": {"role": "analyst", "branch_id": None,    "name": "Analista Demo"},
    "viewer@datatalk.com":   {"role": "viewer",  "branch_id": None,    "name": "Viewer Demo"},
}
MOCK_PASSWORD = "demo1234"


class LoginRequest(BaseModel):
    username: str
    password: str
    branch_id: Optional[str] = None


def _frontend_url() -> str:
    """URL base del frontend para redirigir después del callback OAuth2."""
    origins = os.environ.get("ALLOWED_ORIGINS", "http://localhost:5173").split(",")
    for o in origins:
        o = o.strip()
        if "5173" in o or "3000" in o:
            return o
    return origins[0].strip()


def _mock_login(request: LoginRequest) -> dict:
    user = MOCK_USERS.get(request.username)
    if not user or request.password != MOCK_PASSWORD:
        raise HTTPException(401, "Credenciales incorrectas")
    role = user["role"]
    branch_id = request.branch_id or user["branch_id"]
    parts = ["mock", role]
    if branch_id:
        parts.append(branch_id)
    return {
        "access_token": ":".join(parts),
        "token_type":   "Bearer",
        "user_id":      request.username,
        "role":         role,
        "branch_id":    branch_id,
        "display_name": user["name"],
        "mode":         "dev_mock",
        "message":      f"Login exitoso como {role}",
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/login")
def login(request: LoginRequest):
    """Login con usuario/contraseña — mock siempre disponible."""
    return _mock_login(request)


@router.get("/microsoft")
def microsoft_login():
    """
    Inicia el flujo OAuth2 con Microsoft.
    Requiere DEV_MODE=false en el .env.
    """
    dev_mode = os.environ.get("DEV_MODE", "true").lower() == "true"
    if dev_mode:
        raise HTTPException(
            400,
            detail={
                "error": "DEV_MODE=true",
                "message": "Cambiá DEV_MODE=false en el .env y reiniciá el servidor para usar Entra ID real.",
                "fix": "En .env: DEV_MODE=false",
            }
        )
    from datatalk.core.auth import get_login_url
    redirect_uri = os.environ.get("AZURE_REDIRECT_URI", "http://localhost:8000/auth/callback")
    return RedirectResponse(url=get_login_url(redirect_uri=redirect_uri))


@router.get("/callback")
def auth_callback(
    code: str = None,
    error: str = None,
    error_description: str = None,
):
    """
    Callback OAuth2 — Microsoft redirige aquí después del login.
    Redirige al frontend con el token en query params.
    """
    frontend = _frontend_url()

    if error:
        logger.error(f"Entra ID error: {error} — {error_description}")
        msg = urllib.parse.quote(error_description or error)
        return RedirectResponse(url=f"{frontend}/auth/callback?error={msg}")

    if not code:
        return RedirectResponse(url=f"{frontend}/auth/callback?error=no_code")

    from datatalk.core.auth import exchange_code_for_token, extract_role_from_token
    redirect_uri = os.environ.get("AZURE_REDIRECT_URI", "http://localhost:8000/auth/callback")
    token_result = exchange_code_for_token(code, redirect_uri=redirect_uri)

    if not token_result:
        return RedirectResponse(url=f"{frontend}/auth/callback?error=token_exchange_failed")

    access_token = token_result["access_token"]
    user_id, role = extract_role_from_token(access_token)

    display_name = user_id
    try:
        from datatalk.core.auth import get_user_info_from_graph
        profile = get_user_info_from_graph(access_token)
        display_name = profile.get("displayName") or user_id
    except Exception:
        pass

    params = urllib.parse.urlencode({
        "token":        access_token,
        "user_id":      user_id,
        "role":         role,
        "display_name": display_name,
        "mode":         "entra_id",
    })
    return RedirectResponse(url=f"{frontend}/auth/callback?{params}")


@router.get("/me")
def me(authorization: Optional[str] = Header(None)):
    """Perfil del usuario autenticado. Acepta tokens mock y tokens reales de Entra ID."""
    dev_mode = os.environ.get("DEV_MODE", "true").lower() == "true"

    if not authorization or not authorization.startswith("Bearer "):
        if dev_mode:
            return {
                "user_id":      "admin@datatalk.demo",
                "role":         "admin",
                "branch_id":    None,
                "display_name": "Admin Demo",
                "mode":         "dev_mock",
            }
        raise HTTPException(401, "Token requerido. Header: Authorization: Bearer <token>")

    token = authorization.replace("Bearer ", "").strip()

    # Token mock
    if token.startswith("mock:"):
        parts = token.split(":")
        role      = parts[1] if len(parts) > 1 else "viewer"
        branch_id = parts[2] if len(parts) > 2 else None
        names = {"admin": "Admin Demo", "manager": "Manager Demo",
                 "analyst": "Analista Demo", "viewer": "Viewer Demo"}
        return {
            "user_id":      f"{role}@datatalk.demo",
            "role":         role,
            "branch_id":    branch_id,
            "display_name": names.get(role, role),
            "mode":         "mock",
        }

    # Token real Entra ID
    try:
        from datatalk.core.auth import extract_role_from_token, validate_token
        payload = validate_token(token)
        if not payload:
            raise HTTPException(401, "Token inválido o expirado. Hacé login de nuevo.")

        user_id, role = extract_role_from_token(token)
        display_name = payload.get("preferred_username") or user_id
        email = payload.get("preferred_username") or payload.get("upn")

        try:
            from datatalk.core.auth import get_user_info_from_graph
            profile = get_user_info_from_graph(token)
            display_name = profile.get("displayName") or display_name
            email        = profile.get("mail") or email
        except Exception:
            pass

        return {
            "user_id":         user_id,
            "role":            role,
            "branch_id":       None,
            "display_name":    display_name,
            "email":           email,
            "mode":            "entra_id",
            "roles_in_token":  payload.get("roles", []),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error validando token: {e}")
        raise HTTPException(401, "Token inválido.")


@router.get("/users")
def list_mock_users():
    """Lista usuarios de demo."""
    return {
        "password": MOCK_PASSWORD,
        "users": [
            {"email": e, "role": d["role"], "branch_id": d["branch_id"], "name": d["name"]}
            for e, d in MOCK_USERS.items()
        ],
    }


@router.get("/verify")
def verify_config():
    """Verifica la configuración de Entra ID. Útil para debugging."""
    dev_mode      = os.environ.get("DEV_MODE", "true").lower() == "true"
    tenant_id     = os.environ.get("AZURE_TENANT_ID", "")
    client_id     = os.environ.get("AZURE_CLIENT_ID", "")
    client_secret = os.environ.get("AZURE_CLIENT_SECRET", "")
    redirect_uri  = os.environ.get("AZURE_REDIRECT_URI", "")

    issues = []
    if not tenant_id:
        issues.append("AZURE_TENANT_ID está vacío")
    if not client_id:
        issues.append("AZURE_CLIENT_ID está vacío")
    if not client_secret or client_secret == "your_client_secret":
        issues.append("AZURE_CLIENT_SECRET no configurado")
    if not redirect_uri:
        issues.append("AZURE_REDIRECT_URI está vacío")

    msal_ok    = False
    msal_error = None
    if not issues and not dev_mode:
        try:
            from datatalk.core.auth import get_app_token
            token  = get_app_token()
            msal_ok = bool(token)
            if not msal_ok:
                msal_error = "MSAL no devolvió token — revisá client_id y client_secret en Azure Portal"
        except Exception as e:
            msal_error = str(e)

    return {
        "dev_mode":          dev_mode,
        "tenant_id":         f"{tenant_id[:8]}..." if tenant_id else "❌ VACÍO",
        "client_id":         f"{client_id[:8]}..." if client_id else "❌ VACÍO",
        "client_secret_set": bool(client_secret and client_secret != "your_client_secret"),
        "redirect_uri":      redirect_uri or "❌ VACÍO",
        "frontend_url":      _frontend_url(),
        "microsoft_login_url": "http://localhost:8000/auth/microsoft",
        "issues":            issues,
        "msal_connection":   "ok" if msal_ok else ("dev_mode_skipped" if dev_mode else f"error: {msal_error}"),
        "status":            "✅ ready" if not issues else "⚠️ needs_attention",
    }


def get_current_user(authorization: Optional[str] = Header(None)):
    """Dependency para endpoints protegidos."""
    from datatalk.core.rbac import user_mock, user_from_token
    dev_mode = os.environ.get("DEV_MODE", "true").lower() == "true"
    if not authorization:
        if dev_mode:
            return user_mock("admin")
        raise HTTPException(401, "Authorization header requerido")
    token = authorization.replace("Bearer ", "").strip()
    if token.startswith("mock:"):
        parts     = token.split(":")
        role      = parts[1] if len(parts) > 1 else "viewer"
        branch_id = parts[2] if len(parts) > 2 else None
        return user_mock(role, branch_id)
    return user_from_token(token)
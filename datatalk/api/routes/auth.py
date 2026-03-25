"""
Ruta de autenticación — integración con Microsoft Entra ID.
Para el hackathon usa un mock simple. TODO: conectar Entra ID real.
"""
from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()


class LoginRequest(BaseModel):
    username: str
    password: str


@router.post("/login")
def login(request: LoginRequest):
    """
    Mock de login para desarrollo.
    TODO: reemplazar con Microsoft Entra ID (MSAL) para producción.
    """
    MOCK_USERS = {
        "admin@datatalk.com":   {"role": "admin",   "branch_id": None},
        "norte@datatalk.com":   {"role": "manager", "branch_id": "norte"},
        "sur@datatalk.com":     {"role": "manager", "branch_id": "sur"},
    }
    user = MOCK_USERS.get(request.username)
    if not user or request.password != "demo1234":
        return {"error": "Credenciales incorrectas"}, 401

    return {
        "user_id": request.username,
        "role": user["role"],
        "branch_id": user["branch_id"],
        "token": "mock_token_replace_with_entra_id",
    }


@router.get("/me")
def me():
    """Retorna el usuario actual. TODO: leer del token JWT de Entra ID."""
    return {"user_id": "demo", "role": "admin", "branch_id": None}

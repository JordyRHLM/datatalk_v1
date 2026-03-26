"""
DataTalk — Autenticación con Microsoft Entra ID
Valida tokens JWT y extrae roles de la app.
"""
import os
import logging
from functools import lru_cache
from typing import Optional

import msal

logger = logging.getLogger(__name__)

# Mapeo de roles Entra ID → roles internos de DataTalk
ROLE_MAP = {
    "DataTalk.Admin":    "admin",
    "DataTalk.Manager":  "manager",
    "DataTalk.Analyst":  "analyst",
    "DataTalk.Viewer":   "viewer",
}


@lru_cache(maxsize=1)
def _get_msal_app():
    """Retorna la app MSAL (singleton)."""
    tenant_id   = os.environ["AZURE_TENANT_ID"]
    client_id   = os.environ["AZURE_CLIENT_ID"]
    client_secret = os.environ["AZURE_CLIENT_SECRET"]

    return msal.ConfidentialClientApplication(
        client_id,
        authority=f"https://login.microsoftonline.com/{tenant_id}",
        client_credential=client_secret,
    )


def get_app_token() -> Optional[str]:
    """
    Obtiene un token de acceso para la propia app (client credentials flow).
    Usado para llamadas server-to-server (Graph API, etc).
    """
    app = _get_msal_app()
    result = app.acquire_token_for_client(
        scopes=["https://graph.microsoft.com/.default"]
    )
    if "access_token" in result:
        return result["access_token"]
    logger.error(f"Error obteniendo token: {result.get('error_description')}")
    return None


def validate_token(token: str) -> Optional[dict]:
    """
    Valida un token JWT de Entra ID usando MSAL.
    Retorna el payload decodificado o None si es inválido.
    """
    tenant_id = os.environ["AZURE_TENANT_ID"]
    client_id = os.environ["AZURE_CLIENT_ID"]

    try:
        import jwt
        # Obtener las claves públicas de Microsoft
        jwks_uri = f"https://login.microsoftonline.com/{tenant_id}/discovery/v2.0/keys"
        import httpx
        jwks = httpx.get(jwks_uri, timeout=5).json()

        header = jwt.get_unverified_header(token)
        key = next((k for k in jwks["keys"] if k["kid"] == header["kid"]), None)
        if not key:
            return None

        from jwt.algorithms import RSAAlgorithm
        public_key = RSAAlgorithm.from_jwk(key)

        payload = jwt.decode(
            token,
            public_key,
            algorithms=["RS256"],
            audience=client_id,
        )
        return payload
    except Exception as e:
        logger.warning(f"Token inválido: {e}")
        return None


def extract_role_from_token(token: str) -> tuple[str, str]:
    """
    Extrae el user_id y rol interno a partir del token JWT.

    Returns:
        (user_id, role) — role es uno de: admin, manager, analyst, viewer
    """
    payload = validate_token(token)
    if not payload:
        return ("unknown", "viewer")

    user_id = payload.get("preferred_username") or payload.get("oid", "unknown")

    # Los roles de la app vienen en el claim "roles"
    token_roles = payload.get("roles", [])

    for entra_role, internal_role in ROLE_MAP.items():
        if entra_role in token_roles:
            return (user_id, internal_role)

    return (user_id, "viewer")  # fallback más restrictivo


def get_login_url(redirect_uri: str = "http://localhost:8000/auth/callback") -> str:
    """Genera la URL de login de Microsoft para el flujo OAuth2."""
    tenant_id = os.environ["AZURE_TENANT_ID"]
    client_id = os.environ["AZURE_CLIENT_ID"]
    scope = "openid profile email"

    return (
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/authorize"
        f"?client_id={client_id}"
        f"&response_type=code"
        f"&redirect_uri={redirect_uri}"
        f"&scope={scope}"
        f"&response_mode=query"
    )


def exchange_code_for_token(
    code: str,
    redirect_uri: str = "http://localhost:8000/auth/callback"
) -> Optional[dict]:
    """
    Intercambia el código de autorización por un token de acceso.
    Usado en el callback OAuth2.
    """
    app = _get_msal_app()
    result = app.acquire_token_by_authorization_code(
        code,
        scopes=["openid", "profile", "email"],
        redirect_uri=redirect_uri,
    )
    if "access_token" in result:
        return result
    logger.error(f"Error en code exchange: {result.get('error_description')}")
    return None
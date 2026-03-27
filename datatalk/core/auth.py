"""
DataTalk — Autenticación con Microsoft Entra ID
"""
import os
import logging
import requests
from functools import lru_cache
from typing import Optional

import msal

logger = logging.getLogger(__name__)

ROLE_MAP = {
    "DataTalk.Admin":   "admin",
    "DataTalk.Manager": "manager",
    "DataTalk.Analyst": "analyst",
    "DataTalk.Viewer":  "viewer",
}


@lru_cache(maxsize=1)
def _get_msal_app():
    return msal.ConfidentialClientApplication(
        os.environ["AZURE_CLIENT_ID"],
        authority=f"https://login.microsoftonline.com/{os.environ['AZURE_TENANT_ID']}",
        client_credential=os.environ["AZURE_CLIENT_SECRET"],
    )


def get_app_token() -> Optional[str]:
    """Token server-to-server para Graph API."""
    try:
        result = _get_msal_app().acquire_token_for_client(
            scopes=["https://graph.microsoft.com/.default"]
        )
        if "access_token" in result:
            return result["access_token"]
        logger.error(f"MSAL error: {result.get('error_description')}")
    except Exception as e:
        logger.error(f"get_app_token falló: {e}")
    return None


def validate_token(token: str) -> Optional[dict]:
    """
    Valida token JWT de Entra ID con las claves públicas JWKS de Microsoft.
    Retorna el payload o None.
    """
    tenant_id = os.environ.get("AZURE_TENANT_ID", "")
    client_id = os.environ.get("AZURE_CLIENT_ID", "")

    if not tenant_id or not client_id:
        return None

    try:
        import jwt
        from jwt.algorithms import RSAAlgorithm

        jwks_uri = f"https://login.microsoftonline.com/{tenant_id}/discovery/v2.0/keys"
        jwks     = requests.get(jwks_uri, timeout=10).json()
        header   = jwt.get_unverified_header(token)
        key_data = next(
            (k for k in jwks.get("keys", []) if k.get("kid") == header.get("kid")),
            None
        )
        if not key_data:
            return None

        public_key = RSAAlgorithm.from_jwk(key_data)
        payload    = jwt.decode(
            token,
            public_key,
            algorithms=["RS256"],
            audience=client_id,
            options={"verify_exp": True},
        )
        return payload
    except Exception as e:
        logger.warning(f"Token inválido: {e}")
        return None


def extract_role_from_token(token: str) -> tuple[str, str]:
    """Retorna (user_id, role_interno) del token. Fallback: ('unknown', 'viewer')."""
    payload = validate_token(token)
    if not payload:
        return ("unknown", "viewer")

    user_id = (
        payload.get("preferred_username")
        or payload.get("upn")
        or payload.get("oid", "unknown")
    )
    for entra_role, internal_role in ROLE_MAP.items():
        if entra_role in payload.get("roles", []):
            return (user_id, internal_role)

    return (user_id, "viewer")


def get_login_url(redirect_uri: str = "http://localhost:8000/auth/callback") -> str:
    """URL de login OAuth2 de Microsoft."""
    tenant_id = os.environ.get("AZURE_TENANT_ID", "")
    client_id = os.environ.get("AZURE_CLIENT_ID", "")
    scope     = "openid profile email offline_access"

    return (
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/authorize"
        f"?client_id={client_id}"
        f"&response_type=code"
        f"&redirect_uri={requests.utils.quote(redirect_uri, safe='')}"
        f"&scope={scope}"
        f"&response_mode=query"
        f"&prompt=select_account"
    )


def exchange_code_for_token(
    code: str,
    redirect_uri: str = "http://localhost:8000/auth/callback",
) -> Optional[dict]:
    """Intercambia código de autorización por tokens. Retorna dict MSAL o None."""
    try:
        result = _get_msal_app().acquire_token_by_authorization_code(
            code,
            scopes=["openid", "profile", "email", "offline_access"],
            redirect_uri=redirect_uri,
        )
        if "access_token" in result:
            return result
        logger.error(f"Code exchange falló: {result.get('error')} — {result.get('error_description')}")
    except Exception as e:
        logger.error(f"exchange_code_for_token error: {e}")
    return None


def get_user_info_from_graph(access_token: str) -> dict:
    """Perfil del usuario desde Microsoft Graph API."""
    try:
        resp = requests.get(
            "https://graph.microsoft.com/v1.0/me",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=10,
        )
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        logger.warning(f"Graph API falló: {e}")
    return {}
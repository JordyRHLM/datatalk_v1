"""
RBAC — control de acceso por rol y sucursal.
Compatible con roles de Microsoft Entra ID:
  DataTalk.Admin   → admin   (ve todas las sucursales)
  DataTalk.Manager → manager (solo su sucursal)
  DataTalk.Analyst → analyst (consultas en datos no sensibles)
  DataTalk.Viewer  → viewer  (solo lectura)
"""
from enum import Enum
from typing import Optional


class Role(str, Enum):
    ADMIN   = "admin"
    MANAGER = "manager"
    ANALYST = "analyst"
    VIEWER  = "viewer"


# Roles que pueden ejecutar consultas
QUERY_ALLOWED_ROLES = {Role.ADMIN, Role.MANAGER, Role.ANALYST}

# Roles que pueden ver archivos sensibles (RRHH, finanzas)
SENSITIVE_ALLOWED_ROLES = {Role.ADMIN}


class UserContext:
    def __init__(
        self,
        user_id: str,
        role: Role,
        branch_id: Optional[str] = None,
        display_name: Optional[str] = None,
    ):
        self.user_id      = user_id
        self.role         = role
        self.branch_id    = branch_id  # None si es admin
        self.display_name = display_name or user_id

    def can_access_branch(self, branch_id: str) -> bool:
        if self.role == Role.ADMIN:
            return True
        return self.branch_id == branch_id

    def can_query(self) -> bool:
        return self.role in QUERY_ALLOWED_ROLES

    def can_access_sensitive(self) -> bool:
        return self.role in SENSITIVE_ALLOWED_ROLES

    def get_sql_filter(self) -> Optional[str]:
        """
        Retorna el filtro SQL que debe aplicarse según el rol.
        Admin → None (sin filtro, ve todo)
        Otros → filtra por su sucursal_id
        """
        if self.role == Role.ADMIN:
            return None
        if self.branch_id:
            return f"sucursal_id = '{self.branch_id}'"
        return None

    def to_dict(self) -> dict:
        return {
            "user_id":      self.user_id,
            "display_name": self.display_name,
            "role":         self.role.value,
            "branch_id":    self.branch_id,
            "can_query":    self.can_query(),
            "can_sensitive": self.can_access_sensitive(),
        }


def user_from_token(token: str, branch_id: Optional[str] = None) -> UserContext:
    """
    Crea un UserContext a partir de un token JWT de Entra ID.
    Úsalo en los endpoints protegidos.
    """
    from datatalk.core.auth import extract_role_from_token
    user_id, role_str = extract_role_from_token(token)

    try:
        role = Role(role_str)
    except ValueError:
        role = Role.VIEWER

    return UserContext(
        user_id=user_id,
        role=role,
        branch_id=branch_id,
    )


def user_mock(role: str = "admin", branch_id: Optional[str] = None) -> UserContext:
    """
    Crea un UserContext mock para desarrollo (DEV_MODE=true).
    """
    try:
        role_enum = Role(role)
    except ValueError:
        role_enum = Role.VIEWER

    return UserContext(
        user_id=f"{role}@datatalk.demo",
        role=role_enum,
        branch_id=branch_id,
        display_name=f"Demo {role.capitalize()}",
    )
"""
RBAC — control de acceso por rol y sucursal.
Responsible AI: cada agente filtra datos según el rol del usuario.
"""
from enum import Enum
from typing import Optional


class Role(str, Enum):
    ADMIN = "admin"           # Ve todas las sucursales
    MANAGER = "manager"       # Solo ve su sucursal asignada
    VIEWER = "viewer"         # Solo lectura de su sucursal


class UserContext:
    def __init__(self, user_id: str, role: Role, branch_id: Optional[str] = None):
        self.user_id = user_id
        self.role = role
        self.branch_id = branch_id  # None si es admin

    def can_access_branch(self, branch_id: str) -> bool:
        if self.role == Role.ADMIN:
            return True
        return self.branch_id == branch_id

    def get_sql_filter(self) -> Optional[str]:
        """Retorna el filtro SQL que debe aplicarse según el rol."""
        if self.role == Role.ADMIN:
            return None  # Sin filtro — ve todo
        return f"sucursal_id = '{self.branch_id}'"

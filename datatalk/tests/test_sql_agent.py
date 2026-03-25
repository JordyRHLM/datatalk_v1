"""Tests básicos para el SQL Agent — sin llamadas reales a la API."""
import pytest
from datatalk.core.rbac import UserContext, Role


def test_rbac_admin_sees_all():
    user = UserContext(user_id="admin@test.com", role=Role.ADMIN)
    assert user.can_access_branch("norte") is True
    assert user.can_access_branch("sur") is True
    assert user.get_sql_filter() is None


def test_rbac_manager_sees_own_branch():
    user = UserContext(user_id="norte@test.com", role=Role.MANAGER, branch_id="norte")
    assert user.can_access_branch("norte") is True
    assert user.can_access_branch("sur") is False
    assert "norte" in user.get_sql_filter()


def test_rbac_manager_filter_format():
    user = UserContext(user_id="sur@test.com", role=Role.MANAGER, branch_id="sur")
    sql_filter = user.get_sql_filter()
    assert sql_filter == "sucursal_id = 'sur'"

"""
Ruta de alertas — anomalías detectadas por el agente.
"""
from fastapi import APIRouter
from datatalk.data.duck_engine import list_tables

router = APIRouter()


@router.get("/")
def get_alerts():
    """Retorna las alertas activas. TODO: conectar con Agente 2."""
    return {"alerts": [], "tables_loaded": list_tables()}

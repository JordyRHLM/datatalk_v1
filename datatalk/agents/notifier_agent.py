"""
Notifier Agent — envía alertas por Teams o Email.
Verifica RBAC antes de enviar: el gerente solo recibe alertas de su sucursal.
"""
import httpx
from datatalk.core.rbac import UserContext, Role


class NotifierAgent:
    def __init__(self, teams_webhook_url: str | None = None):
        self.teams_webhook_url = teams_webhook_url

    def _can_notify(self, user: UserContext, branch_id: str) -> bool:
        """Verifica RBAC antes de enviar notificación."""
        return user.can_access_branch(branch_id)

    async def notify_teams(self, user: UserContext, anomaly: dict) -> dict:
        """Envía Adaptive Card a Teams si el usuario tiene permiso."""
        branch_id = anomaly.get("sucursal_id", "")

        if not self._can_notify(user, branch_id):
            return {"status": "blocked", "reason": "RBAC: sin permiso para esta sucursal"}

        if not self.teams_webhook_url:
            # Mock para desarrollo
            print(f"[MOCK TEAMS] Alerta para {user.user_id}: {anomaly}")
            return {"status": "mock_sent", "anomaly": anomaly}

        card = {
            "type": "message",
            "attachments": [{
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {"type": "TextBlock", "size": "Large", "weight": "Bolder",
                         "text": f"⚠️ Anomalía detectada — Sucursal {branch_id}"},
                        {"type": "FactSet", "facts": [
                            {"title": "Caída", "value": f"{anomaly.get('drop_pct', 0)}%"},
                            {"title": "Ventas actuales", "value": str(anomaly.get('latest_sales', ''))},
                            {"title": "Promedio histórico", "value": str(anomaly.get('avg_sales', ''))},
                        ]},
                    ],
                    "actions": [
                        {"type": "Action.OpenUrl", "title": "Ver dashboard",
                         "url": "http://localhost:3000"},
                    ]
                }
            }]
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(self.teams_webhook_url, json=card)
            return {"status": "sent", "http_status": response.status_code}

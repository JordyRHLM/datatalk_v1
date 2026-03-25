"""
Audit Log Viewer — DataTalk
Endpoint que sirve una página HTML con el audit log en tiempo real.
Agregar en datatalk/api/main.py:

    from datatalk.api.routes.audit_viewer import router as audit_router
    app.include_router(audit_router, prefix="/audit")

Luego abrir: http://localhost:8000/audit/
"""
import json
from pathlib import Path
from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter()

LOG_PATH = Path("logs/audit.jsonl")

_HTML = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>DataTalk — Audit Log</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    font-family: 'Segoe UI', system-ui, sans-serif;
    background: #0f172a;
    color: #e2e8f0;
    min-height: 100vh;
    padding: 32px 24px;
  }

  .header {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 32px;
  }

  .logo {
    font-size: 28px;
    font-weight: 800;
    background: linear-gradient(135deg, #6366f1, #a78bfa);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
  }

  .subtitle {
    color: #94a3b8;
    font-size: 14px;
    margin-top: 2px;
  }

  .badge-responsible {
    margin-left: auto;
    background: #1e293b;
    border: 1px solid #334155;
    border-radius: 20px;
    padding: 6px 14px;
    font-size: 12px;
    color: #94a3b8;
    display: flex;
    align-items: center;
    gap: 6px;
  }

  .badge-responsible::before {
    content: '🛡️';
  }

  .stats {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    gap: 16px;
    margin-bottom: 28px;
  }

  .stat-card {
    background: #1e293b;
    border: 1px solid #334155;
    border-radius: 12px;
    padding: 18px 20px;
  }

  .stat-value {
    font-size: 32px;
    font-weight: 700;
    color: #a78bfa;
  }

  .stat-label {
    font-size: 12px;
    color: #64748b;
    margin-top: 4px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }

  .filters {
    display: flex;
    gap: 10px;
    margin-bottom: 20px;
    flex-wrap: wrap;
  }

  .filter-btn {
    background: #1e293b;
    border: 1px solid #334155;
    border-radius: 8px;
    padding: 6px 14px;
    font-size: 13px;
    color: #94a3b8;
    cursor: pointer;
    transition: all 0.15s;
  }

  .filter-btn:hover, .filter-btn.active {
    background: #6366f1;
    border-color: #6366f1;
    color: white;
  }

  .table-wrapper {
    background: #1e293b;
    border: 1px solid #334155;
    border-radius: 12px;
    overflow: hidden;
  }

  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }

  thead {
    background: #0f172a;
  }

  th {
    padding: 12px 16px;
    text-align: left;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #64748b;
    font-weight: 600;
    border-bottom: 1px solid #334155;
  }

  td {
    padding: 12px 16px;
    border-bottom: 1px solid #1e293b;
    vertical-align: top;
    max-width: 280px;
  }

  tr:last-child td { border-bottom: none; }

  tr:hover td { background: #263148; }

  .event-badge {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 12px;
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.03em;
  }

  .event-ACCESS_GRANTED  { background: #14532d; color: #86efac; }
  .event-QUERY_EXECUTED  { background: #1e3a5f; color: #93c5fd; }
  .event-QUERY_FAILED    { background: #4c1d1d; color: #fca5a5; }
  .event-BLOCKED_INJECTION { background: #451a03; color: #fbbf24; }
  .event-BLOCKED_RBAC    { background: #3b1e5a; color: #c4b5fd; }
  .event-RATE_LIMITED    { background: #4c1d1d; color: #fb923c; }

  .role-badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 8px;
    font-size: 11px;
    font-weight: 500;
  }

  .role-admin   { background: #312e81; color: #a5b4fc; }
  .role-manager { background: #064e3b; color: #6ee7b7; }
  .role-viewer  { background: #1c1917; color: #a8a29e; }
  .role-analyst { background: #0c4a6e; color: #7dd3fc; }

  .question-text {
    color: #e2e8f0;
    max-width: 260px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .timestamp {
    color: #64748b;
    font-size: 12px;
    white-space: nowrap;
  }

  .sensitive-flag {
    color: #f59e0b;
    font-size: 11px;
  }

  .empty-state {
    text-align: center;
    padding: 60px 20px;
    color: #475569;
  }

  .empty-state .icon { font-size: 40px; margin-bottom: 12px; }

  .refresh-btn {
    background: #6366f1;
    border: none;
    border-radius: 8px;
    padding: 8px 18px;
    font-size: 13px;
    color: white;
    cursor: pointer;
    margin-left: auto;
    display: block;
    margin-bottom: 12px;
    transition: background 0.15s;
  }

  .refresh-btn:hover { background: #4f46e5; }
</style>
</head>
<body>

<div class="header">
  <div>
    <div class="logo">🧠 DataTalk</div>
    <div class="subtitle">Audit Log — Trazabilidad completa de consultas</div>
  </div>
  <div class="badge-responsible">Responsible AI</div>
</div>

<div class="stats" id="stats"></div>

<div class="filters">
  <button class="filter-btn active" onclick="filterEvents('ALL')">Todos</button>
  <button class="filter-btn" onclick="filterEvents('ACCESS_GRANTED')">✅ Acceso</button>
  <button class="filter-btn" onclick="filterEvents('QUERY_EXECUTED')">🔍 Ejecutadas</button>
  <button class="filter-btn" onclick="filterEvents('QUERY_FAILED')">❌ Fallidas</button>
  <button class="filter-btn" onclick="filterEvents('BLOCKED_INJECTION')">⚠️ Bloqueadas</button>
</div>

<button class="refresh-btn" onclick="loadEvents()">↻ Actualizar</button>

<div class="table-wrapper">
  <table>
    <thead>
      <tr>
        <th>Timestamp</th>
        <th>Evento</th>
        <th>Usuario</th>
        <th>Rol</th>
        <th>Pregunta</th>
        <th>Detalles</th>
      </tr>
    </thead>
    <tbody id="log-body">
    </tbody>
  </table>
</div>

<script>
let allEvents = [];
let currentFilter = 'ALL';

async function loadEvents() {
  const res = await fetch('/history?limit=100');
  const data = await res.json();
  allEvents = data.events || [];
  renderStats();
  renderTable();
}

function renderStats() {
  const total     = allEvents.length;
  const granted   = allEvents.filter(e => e.event === 'ACCESS_GRANTED').length;
  const executed  = allEvents.filter(e => e.event === 'QUERY_EXECUTED').length;
  const blocked   = allEvents.filter(e => ['BLOCKED_INJECTION','BLOCKED_RBAC','RATE_LIMITED'].includes(e.event)).length;
  const sensitive = allEvents.filter(e => e.sensitive).length;

  document.getElementById('stats').innerHTML = `
    <div class="stat-card"><div class="stat-value">${total}</div><div class="stat-label">Total eventos</div></div>
    <div class="stat-card"><div class="stat-value">${executed}</div><div class="stat-label">Consultas ejecutadas</div></div>
    <div class="stat-card"><div class="stat-value">${blocked}</div><div class="stat-label">Accesos bloqueados</div></div>
    <div class="stat-card"><div class="stat-value">${sensitive}</div><div class="stat-label">Archivos sensibles</div></div>
  `;
}

function filterEvents(type) {
  currentFilter = type;
  document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
  event.target.classList.add('active');
  renderTable();
}

function renderTable() {
  const filtered = currentFilter === 'ALL'
    ? allEvents
    : allEvents.filter(e => e.event === currentFilter);

  const tbody = document.getElementById('log-body');

  if (filtered.length === 0) {
    tbody.innerHTML = `
      <tr><td colspan="6">
        <div class="empty-state">
          <div class="icon">📋</div>
          <div>No hay eventos todavía. Hacé una consulta primero.</div>
        </div>
      </td></tr>`;
    return;
  }

  tbody.innerHTML = filtered.map(e => {
    const ts = e.timestamp
      ? new Date(e.timestamp).toLocaleString('es-AR', {
          month: '2-digit', day: '2-digit',
          hour: '2-digit', minute: '2-digit', second: '2-digit'
        })
      : '—';

    const role = e.role || '—';
    const roleClass = ['admin','manager','viewer','analyst'].includes(role) ? `role-${role}` : '';

    const question = e.question
      ? `<span class="question-text" title="${e.question}">${e.question}</span>`
      : '<span style="color:#475569">—</span>';

    const sensitiveFlag = e.sensitive
      ? '<span class="sensitive-flag">⚠ SENSIBLE</span>'
      : '';

    const details = e.attempts != null
      ? `Intentos: ${e.attempts}${e.autocorrected ? ' ⚡' : ''}`
      : (e.reason || '');

    return `
      <tr>
        <td class="timestamp">${ts}</td>
        <td><span class="event-badge event-${e.event}">${e.event}</span></td>
        <td style="color:#94a3b8;font-size:12px">${e.user_id || '—'}</td>
        <td><span class="role-badge ${roleClass}">${role}</span></td>
        <td>${question} ${sensitiveFlag}</td>
        <td style="color:#64748b;font-size:12px">${details}</td>
      </tr>`;
  }).join('');
}

// Cargar al inicio y auto-refresh cada 10 segundos
loadEvents();
setInterval(loadEvents, 10000);
</script>
</body>
</html>"""


@router.get("/", response_class=HTMLResponse)
def audit_viewer():
    """Sirve la página HTML del audit log."""
    return HTMLResponse(content=_HTML)


@router.get("/export")
def export_log():
    """Exporta el audit log completo como JSON."""
    if not LOG_PATH.exists():
        return {"events": [], "total": 0}

    events = []
    with open(LOG_PATH, "r", encoding="utf-8") as f:
        for line in f:
            try:
                events.append(json.loads(line.strip()))
            except Exception:
                pass

    return {"events": events, "total": len(events)}
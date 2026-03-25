# DataTalk 🧠

**Microsoft Innovation Challenge 2025** — Agente de ingeniería analítica Text-to-SQL

> Convierte preguntas en lenguaje natural en consultas SQL validadas, las ejecuta contra un lake de datos y explica los resultados en lenguaje empresarial claro.

---

## ¿Qué hace?

1. **Sube** un Excel o CSV → se limpia automáticamente y se registra en DuckDB
2. **Pregunta** en lenguaje natural → el agente genera SQL
3. **Revisa y aprueba** el SQL antes de ejecutarlo (human-in-the-loop)
4. **Recibe** los resultados explicados en lenguaje empresarial
5. **Audit log** de cada consulta para trazabilidad completa

---

## Arquitectura

```
Usuario → FastAPI → Orquestador
                      ├── Schema Agent   (inspecciona tablas DuckDB)
                      ├── SQL Agent      (GPT-4o → SQL validado)
                      ├── Anomaly Agent  (detección de caídas)
                      ├── Forecast Agent (predicción Prophet)
                      └── Notifier Agent (Teams + RBAC)

Datos: Excel/CSV → Limpieza Python → DuckDB (lake en memoria)
Producción: → Azure Synapse Analytics + ADLS
```

---

## Estructura del proyecto

```
datatalk/
├── agents/
│   ├── orchestrator.py      # Coordina todos los agentes
│   ├── schema_agent.py      # Inspecciona tablas DuckDB
│   ├── sql_agent.py         # Text-to-SQL con GPT-4o
│   ├── anomaly_agent.py     # Detección de anomalías
│   ├── forecast_agent.py    # Predicción de ventas
│   └── notifier_agent.py    # Alertas Teams + RBAC
├── api/
│   ├── main.py              # FastAPI app
│   └── routes/
│       ├── query.py         # POST /query/ask + /query/approve
│       ├── upload.py        # POST /upload/
│       ├── alerts.py        # GET /alerts/
│       └── auth.py          # POST /auth/login
├── data/
│   ├── cleaner.py           # Limpieza ligera de Excel/CSV
│   ├── duck_engine.py       # Motor DuckDB
│   └── schema_inspector.py  # Contexto de schema para el LLM
├── core/
│   ├── config.py            # Settings desde .env
│   ├── rbac.py              # Control de acceso por rol
│   └── audit.py             # Audit log (Responsible AI)
└── tests/
    ├── test_cleaner.py
    └── test_sql_agent.py
```

---

## Inicio rápido

### 1. Clonar y configurar
```bash
git clone https://github.com/TU_ORG/datatalk.git
cd datatalk
cp .env.example .env
# Editar .env con tus credenciales de Azure OpenAI
```

### 2. Instalar dependencias
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Correr la API
```bash
uvicorn datatalk.api.main:app --reload
# API disponible en http://localhost:8000
# Docs en http://localhost:8000/docs
```

### 4. O con Docker
```bash
docker-compose up --build
```

### 5. Correr tests
```bash
pytest datatalk/tests/ -v
```

---

## Flujo de uso

```bash
# 1. Subir un Excel
curl -X POST http://localhost:8000/upload/ \
  -F "file=@ventas.xlsx"

# 2. Hacer una pregunta
curl -X POST http://localhost:8000/query/ask \
  -H "Content-Type: application/json" \
  -d '{"question": "¿Cuáles son las 3 sucursales con más ventas este mes?", "role": "admin"}'

# 3. Aprobar el SQL y ejecutar
curl -X POST http://localhost:8000/query/approve \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT ...", "user_id": "admin@datatalk.com", "role": "admin", "question": "..."}'
```

---

## Criterios del hackathon

| Criterio | Implementación |
|---|---|
| **Performance 25%** | DuckDB en memoria, FastAPI async, respuestas en < 2s |
| **Innovation 25%** | Text-to-SQL contextual + lake en memoria + anomaly detection |
| **Azure Services 25%** | OpenAI GPT-4o, Entra ID, Functions, Monitor, App Service, Blob |
| **Responsible AI 25%** | Audit log, RBAC, human-in-the-loop, nivel de confianza visible |

---

## Equipo

Microsoft Innovation Challenge 2025

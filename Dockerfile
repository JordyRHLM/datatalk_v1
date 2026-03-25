FROM python:3.11-slim

WORKDIR /app

# Dependencias del sistema
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Código fuente
COPY . .

# Directorio de uploads
RUN mkdir -p datatalk/data/uploads

EXPOSE 8000

CMD ["uvicorn", "datatalk.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

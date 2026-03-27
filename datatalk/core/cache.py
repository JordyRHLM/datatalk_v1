"""
Redis Cache — DataTalk
Cache centralizado para schemas, resultados SQL e intenciones clasificadas.

Modo de conexión: Redis Cloud (URL completa desde .env)
Fallback: si Redis no está disponible, opera en modo sin caché (nunca rompe el flujo).

Keys usadas:
  schema:{file_hash}                  → schema inferido (TTL: 1h)
  intent:{question_hash}              → intent clasificado (TTL: 24h)
  query:{file_hash}:{question_hash}   → resultado SQL + DataFrame serializado (TTL: 30min)
"""

import os
import json
import hashlib
import logging
from functools import wraps
from datetime import timedelta
from typing import Any, Optional

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# TTLs
# ---------------------------------------------------------------------------
TTL_SCHEMA = int(timedelta(hours=1).total_seconds())       # 3600s  — schema de un archivo
TTL_INTENT = int(timedelta(hours=24).total_seconds())      # 86400s — intención de una pregunta
TTL_QUERY  = int(timedelta(minutes=30).total_seconds())    # 1800s  — resultado completo de query

# ---------------------------------------------------------------------------
# Conexión Redis (lazy, con fallback graceful)
# ---------------------------------------------------------------------------
_redis_client = None
_redis_available = None  # None = no probado aún


def _get_redis():
    """Devuelve el cliente Redis. Si falla, marca como no disponible y retorna None."""
    global _redis_client, _redis_available

    if _redis_available is False:
        return None  # ya sabemos que no funciona

    if _redis_client is not None:
        return _redis_client

    try:
        import redis  # type: ignore

        url = os.environ.get("REDIS_URL", "")
        if not url:
            logger.warning("REDIS_URL no configurada — cache deshabilitado")
            _redis_available = False
            return None

        client = redis.from_url(
            url,
            decode_responses=True,
            socket_connect_timeout=3,
            socket_timeout=3,
            retry_on_timeout=False,
        )
        client.ping()  # verifica conexión real
        _redis_client = client
        _redis_available = True
        logger.info("Redis conectado correctamente")
        return _redis_client

    except ImportError:
        logger.warning("Librería 'redis' no instalada — cache deshabilitado")
        _redis_available = False
        return None
    except Exception as e:
        logger.warning(f"Redis no disponible: {e} — operando sin cache")
        _redis_available = False
        return None


def is_available() -> bool:
    """Retorna True si Redis está conectado y operativo."""
    r = _get_redis()
    if r is None:
        return False
    try:
        r.ping()
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Helpers de hashing
# ---------------------------------------------------------------------------

def hash_file(file_path: str) -> str:
    """
    Hash del contenido del archivo (primeros 64KB + tamaño + mtime).
    Rápido y detecta cambios sin leer el archivo entero.
    """
    import os
    try:
        stat = os.stat(file_path)
        prefix = f"{file_path}:{stat.st_size}:{stat.st_mtime}"
        h = hashlib.sha256(prefix.encode()).hexdigest()[:16]
        return h
    except Exception:
        # Fallback: hash del path solamente
        return hashlib.sha256(file_path.encode()).hexdigest()[:16]


def hash_text(text: str) -> str:
    """Hash corto de un string (pregunta, intent, etc.)."""
    return hashlib.sha256(text.strip().lower().encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Serialización — pandas DataFrames necesitan manejo especial
# ---------------------------------------------------------------------------

def _serialize(value: Any) -> str:
    """Serializa a JSON. Los DataFrames se guardan como lista de records."""
    import pandas as pd
    if isinstance(value, pd.DataFrame):
        return json.dumps({
            "__type__": "dataframe",
            "records": value.to_dict(orient="records"),
            "columns": list(value.columns),
        }, default=str)
    return json.dumps(value, default=str)


def _deserialize(raw: str) -> Any:
    """Deserializa desde JSON. Reconstruye DataFrames si corresponde."""
    import pandas as pd
    data = json.loads(raw)
    if isinstance(data, dict) and data.get("__type__") == "dataframe":
        return pd.DataFrame(data["records"], columns=data["columns"])
    return data


# ---------------------------------------------------------------------------
# Operaciones base
# ---------------------------------------------------------------------------

def get(key: str) -> Optional[Any]:
    """Lee un valor de Redis. Retorna None si no existe o si Redis no está disponible."""
    r = _get_redis()
    if r is None:
        return None
    try:
        raw = r.get(key)
        if raw is None:
            return None
        return _deserialize(raw)
    except Exception as e:
        logger.debug(f"Cache GET error ({key}): {e}")
        return None


def set(key: str, value: Any, ttl: int) -> bool:
    """Guarda un valor en Redis con TTL en segundos. Retorna True si tuvo éxito."""
    r = _get_redis()
    if r is None:
        return False
    try:
        r.setex(key, ttl, _serialize(value))
        return True
    except Exception as e:
        logger.debug(f"Cache SET error ({key}): {e}")
        return False


def delete(key: str) -> bool:
    """Elimina una key de Redis."""
    r = _get_redis()
    if r is None:
        return False
    try:
        r.delete(key)
        return True
    except Exception as e:
        logger.debug(f"Cache DELETE error ({key}): {e}")
        return False


def flush_pattern(pattern: str) -> int:
    """Elimina todas las keys que coincidan con un patrón. Retorna cantidad eliminada."""
    r = _get_redis()
    if r is None:
        return 0
    try:
        keys = r.keys(pattern)
        if keys:
            r.delete(*keys)
        return len(keys)
    except Exception as e:
        logger.debug(f"Cache FLUSH error ({pattern}): {e}")
        return 0


# ---------------------------------------------------------------------------
# API de alto nivel — usada directamente por los agentes
# ---------------------------------------------------------------------------

class SchemaCache:
    """Cache para schemas inferidos por schema_agent.run()."""

    @staticmethod
    def key(file_path: str) -> str:
        return f"schema:{hash_file(file_path)}"

    @staticmethod
    def get(file_path: str) -> Optional[dict]:
        return get(SchemaCache.key(file_path))

    @staticmethod
    def set(file_path: str, schema: dict) -> bool:
        return set(SchemaCache.key(file_path), schema, TTL_SCHEMA)

    @staticmethod
    def invalidate(file_path: str) -> bool:
        """Llamar cuando el archivo se reemplaza con uno nuevo."""
        return delete(SchemaCache.key(file_path))


class IntentCache:
    """Cache para intenciones clasificadas por orchestrator.classify_intent()."""

    @staticmethod
    def key(question: str) -> str:
        return f"intent:{hash_text(question)}"

    @staticmethod
    def get(question: str) -> Optional[str]:
        return get(IntentCache.key(question))

    @staticmethod
    def set(question: str, intent: str) -> bool:
        return set(IntentCache.key(question), intent, TTL_INTENT)


class QueryCache:
    """Cache para resultados de queries completos (SQL + DataFrame + explanation)."""

    @staticmethod
    def key(file_path: str, question: str, intent: str) -> str:
        fh = hash_file(file_path)
        qh = hash_text(f"{intent}:{question}")
        return f"query:{fh}:{qh}"

    @staticmethod
    def get(file_path: str, question: str, intent: str) -> Optional[dict]:
        return get(QueryCache.key(file_path, question, intent))

    @staticmethod
    def set(file_path: str, question: str, intent: str, result: dict) -> bool:
        # No cacheamos resultados fallidos
        if not result.get("success"):
            return False
        # El DataFrame se serializa automáticamente en _serialize
        return set(QueryCache.key(file_path, question, intent), result, TTL_QUERY)

    @staticmethod
    def invalidate_file(file_path: str) -> int:
        """Elimina todas las queries cacheadas de un archivo."""
        fh = hash_file(file_path)
        return flush_pattern(f"query:{fh}:*")


# ---------------------------------------------------------------------------
# Endpoint de estado (para /health y /audit)
# ---------------------------------------------------------------------------

def get_stats() -> dict:
    """Retorna estadísticas de uso de Redis para el dashboard de audit."""
    r = _get_redis()
    if r is None:
        return {"available": False, "keys": 0, "memory_mb": 0}
    try:
        info = r.info("memory")
        keys = r.dbsize()
        return {
            "available": True,
            "keys": keys,
            "memory_mb": round(info.get("used_memory", 0) / 1024 / 1024, 2),
            "memory_peak_mb": round(info.get("used_memory_peak", 0) / 1024 / 1024, 2),
        }
    except Exception as e:
        return {"available": False, "error": str(e)}

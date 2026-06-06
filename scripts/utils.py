"""Utilidades compartidas del pipeline cripto (Bronze).

Centraliza configuración por variables de entorno, logging, el cliente de
Google Cloud Storage, las peticiones HTTP con reintentos (principio *Plan for
Failure*) y el guardado de DataFrames a Parquet.

Pertenece a la **Persona 1 (Extracción / Bronze)** según ``PROYECTO.md`` (Parte 1).
Los nombres de datasets, bucket y rutas siguen el contrato compartido (Parte 0).
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

# Carga el .env local si existe (en CI las variables vienen del entorno de Actions).
load_dotenv()

# --- Rutas -----------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"

# --- Configuración desde entorno (contrato Parte 0.8) ----------------------
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "")
BIGQUERY_DATASET_EXTERNAL = os.environ.get("BIGQUERY_DATASET_EXTERNAL", "crypto")
BIGQUERY_DATASET_SILVER = os.environ.get("BIGQUERY_DATASET_SILVER", "crypto_silver")
BIGQUERY_DATASET_GOLD = os.environ.get("BIGQUERY_DATASET_GOLD", "crypto_gold")
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")


def get_logger(name: str = "mci506") -> logging.Logger:
    """Crea (o recupera) un logger con formato de timestamp y nivel.

    Se usa ``logging`` en vez de ``print`` para que los mensajes salgan
    ordenados y con severidad tanto en local como en los logs de GitHub Actions.

    Args:
        name: Nombre del logger. Reutilizar el mismo nombre devuelve la misma
            instancia (no duplica handlers).

    Returns:
        Un ``logging.Logger`` configurado a nivel INFO.

    Example:
        >>> log = get_logger()
        >>> log.info("hola")  # 2026-06-06 03:00:00 | INFO | mci506 | hola
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


log = get_logger()


def http_get_with_retry(
    url: str,
    params: dict | None = None,
    *,
    max_retries: int = 5,
    backoff_base: float = 2.0,
    timeout: int = 30,
) -> requests.Response:
    """Hace un GET con reintentos y backoff exponencial.

    Implementa el principio *Plan for Failure* (Tema 2): ante errores de red,
    timeouts, rate limit (429) o errores 5xx, reintenta esperando
    ``backoff_base ** intento`` segundos. Los errores 4xx (salvo 429) no se
    reintentan porque son fallos del cliente.

    Args:
        url: URL del endpoint.
        params: Query params de la petición.
        max_retries: Número máximo de intentos antes de rendirse.
        backoff_base: Base de la espera exponencial (segundos).
        timeout: Timeout por intento, en segundos.

    Returns:
        El ``requests.Response`` exitoso (status 2xx).

    Raises:
        requests.HTTPError: Si tras agotar los reintentos no se obtuvo éxito.
        requests.RequestException: Ante un error de red persistente.

    Example:
        >>> r = http_get_with_retry("https://api.coingecko.com/api/v3/ping")
        >>> r.status_code
        200
    """
    last_exc: Exception | None = None
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            if response.status_code == 429 or response.status_code >= 500:
                response.raise_for_status()
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < max_retries - 1:
                wait = backoff_base ** attempt
                log.warning(
                    "GET falló (intento %d/%d): %s — reintento en %.1fs",
                    attempt + 1,
                    max_retries,
                    exc,
                    wait,
                )
                time.sleep(wait)
            else:
                log.error("GET falló definitivamente tras %d intentos.", max_retries)
    raise requests.HTTPError(f"GET a {url} falló tras {max_retries} intentos") from last_exc


def to_parquet(df: pd.DataFrame, path: Path | str) -> Path:
    """Guarda un DataFrame en Parquet, creando la carpeta si no existe.

    Helper al estilo del proyecto de referencia ``mci506-f1``: centraliza la
    escritura para no repetir ``mkdir``/``to_parquet`` en cada llamada y deja
    rastro en el log de cuántas filas se escribieron.

    Args:
        df: DataFrame a persistir.
        path: Ruta destino del archivo ``.parquet``.

    Returns:
        El ``Path`` del archivo escrito.

    Example:
        >>> p = to_parquet(df, BRONZE_DIR / "coins_markets.parquet")
        >>> p.exists()
        True
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    log.info("Parquet escrito: %s (%d filas)", path, len(df))
    return path


def gcs_client():
    """Autentica y devuelve un cliente de Google Cloud Storage.

    Usa las credenciales por defecto de la librería (Application Default
    Credentials), es decir, el JSON de service account apuntado por
    ``GOOGLE_APPLICATION_CREDENTIALS`` (en CI se materializa desde el secret
    ``GCP_SA_KEY``). El import de ``google.cloud`` es perezoso para que la
    extracción local funcione sin tener instalado/credencialado GCS.

    Returns:
        Una instancia de ``google.cloud.storage.Client``.

    Raises:
        EnvironmentError: Si no hay credenciales de GCP disponibles.
    """
    from google.cloud import storage  # import perezoso

    if not GOOGLE_APPLICATION_CREDENTIALS_set():
        raise EnvironmentError(
            "Sin credenciales de GCP: define GOOGLE_APPLICATION_CREDENTIALS "
            "(o GCP_SA_KEY en CI) apuntando al JSON de la service account."
        )
    project = GCP_PROJECT_ID or None
    return storage.Client(project=project)


def GOOGLE_APPLICATION_CREDENTIALS_set() -> bool:
    """Indica si hay una ruta de credenciales de GCP configurada y existente.

    Returns:
        ``True`` si ``GOOGLE_APPLICATION_CREDENTIALS`` apunta a un archivo que
        existe; ``False`` en caso contrario.
    """
    cred = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    return bool(cred) and Path(cred).exists()

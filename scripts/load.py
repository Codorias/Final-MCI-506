"""Sube el archivo Parquet generado por extract.py a Google Cloud Storage.

Usa la configuración y el cliente GCS de utils.py para escribir en el bucket
Bronze con particionamiento Hive (dt=YYYY-MM-DD), según el contrato Parte 0.6.
"""

import os
import re
import sys
import logging
from pathlib import Path

from google.cloud.storage import Client as GCSClient
from utils import get_config

logger = logging.getLogger(__name__)


def load_to_gcs(local_path: str) -> str:
    """Sube un archivo Parquet local a GCS con particionamiento Hive.

    Construye la ruta de destino según el contrato de la Parte 0.6:
    gs://<BUCKET>/coins_markets/dt=YYYY-MM-DD/<filename>.parquet

    La fecha de la partición dt= se extrae del nombre del archivo, que debe
    seguir el patrón coins_markets_YYYYMMDDTHHMMSSZ.parquet.

    La idempotencia se garantiza por el timestamp único en el nombre del archivo:
    re-subir el mismo snapshot no sobrescribe ni duplica datos en Bronze.

    Args:
        local_path: Ruta local al archivo .parquet generado por extract.py.

    Returns:
        URI GCS de destino (gs://...).

    Raises:
        FileNotFoundError: Si el archivo local no existe.
        ValueError: Si el nombre del archivo no sigue el patrón esperado
                    o si GCS_BUCKET no está configurado.
        google.cloud.exceptions.GoogleCloudError: Si falla la subida a GCS.

    Example:
        >>> gcs_uri = load_to_gcs("data/coins_markets_20250101T120000Z.parquet")
        >>> print(gcs_uri)
        gs://mci506-crypto-bronze-myproject/coins_markets/dt=2025-01-01/coins_markets_20250101T120000Z.parquet
    """
    logger.info("Iniciando subida a GCS: %s", local_path)

    local_file = Path(local_path)
    if not local_file.exists():
        raise FileNotFoundError(f"No se encontró el archivo: {local_path}")

    config = get_config()
    bucket_name = config.get("GCS_BUCKET") or os.getenv("GCS_BUCKET")

    if not bucket_name:
        raise ValueError(
            "GCS_BUCKET no está definido. "
            "Configúralo en .env o como variable de entorno."
        )

    filename = local_file.name
    match = re.match(r"coins_markets_(\d{8})T\d{6}Z\.parquet", filename)
    if not match:
        raise ValueError(
            f"El nombre del archivo '{filename}' no sigue el patrón "
            "coins_markets_YYYYMMDDTHHMMSSZ.parquet"
        )

    date_str = match.group(1)
    formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    blob_path = f"coins_markets/dt={formatted_date}/{filename}"

    logger.info("Bucket destino: %s", bucket_name)
    logger.info("Ruta GCS: %s", blob_path)

    client = GCSClient()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(str(local_file))

    gcs_uri = f"gs://{bucket_name}/{blob_path}"
    file_size_kb = local_file.stat().st_size / 1024
    logger.info("Subida exitosa: %s (%.2f KB)", gcs_uri, file_size_kb)

    return gcs_uri


def main():
    """Punto de entrada principal.

    Recibe la ruta del archivo Parquet como primer argumento de línea de comandos.
    Configura logging básico como fallback; si utils.py ya configuró logging,
    respeta su configuración.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if len(sys.argv) < 2:
        logger.error("Uso: python scripts/load.py <ruta_archivo.parquet>")
        sys.exit(1)

    local_path = sys.argv[1]

    try:
        gcs_uri = load_to_gcs(local_path)
        print(gcs_uri)
    except (FileNotFoundError, ValueError) as e:
        logger.error("%s", e)
        sys.exit(1)
    except Exception:
        logger.exception("Error inesperado al subir a GCS")
        sys.exit(1)


if __name__ == "__main__":
    main()

"""Extracción del mercado cripto desde CoinGecko hacia Parquet (capa Bronze).

Persona 1 (Extracción / Bronze) — ``PROYECTO.md`` Parte 1.

Descarga el snapshot del top 250 de monedas por capitalización desde el endpoint
``/coins/markets`` de CoinGecko, lo normaliza al **esquema Bronze** del contrato
(Parte 0.7), lo enriquece con ``extracted_at`` y ``snapshot_date`` (UTC) y lo
escribe en un Parquet local con layout particionado estilo Hive (``dt=YYYY-MM-DD``,
Parte 0.6). ``load.py`` (Persona 3) sube ese Parquet a GCS.

Uso:
    python scripts/extract.py
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# Permite ejecutar el script tanto como módulo como directamente.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from utils import (  # noqa: E402
    BRONZE_DIR,
    COINGECKO_API_KEY,
    get_logger,
    http_get_with_retry,
    to_parquet,
)

log = get_logger()

# --- Configuración de la fuente (contrato Parte 0.9) -----------------------
COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"
VS_CURRENCY = "usd"
PER_PAGE = 250
PAGE = 1

# Renombres directos API -> esquema Bronze (Parte 0.7).
RENAME_MAP = {
    "id": "coin_id",
    "symbol": "symbol",
    "name": "name",
    "current_price": "current_price",
    "market_cap": "market_cap",
    "market_cap_rank": "market_cap_rank",
    "total_volume": "total_volume",
    "high_24h": "high_24h",
    "low_24h": "low_24h",
    "price_change_percentage_24h_in_currency": "price_change_pct_24h",
    "price_change_percentage_7d_in_currency": "price_change_pct_7d",
    "price_change_percentage_30d_in_currency": "price_change_pct_30d",
    "circulating_supply": "circulating_supply",
    "total_supply": "total_supply",
    "max_supply": "max_supply",
    "ath": "ath",
    "ath_change_percentage": "ath_change_percentage",
    "atl": "atl",
    "atl_change_percentage": "atl_change_percentage",
    "last_updated": "last_updated",
}

# Casteo por tipo (la API devuelve tipos inconsistentes: ej. current_price a
# veces llega como int). Forzamos los tipos del contrato.
FLOAT_COLS = [
    "current_price", "high_24h", "low_24h", "price_change_pct_24h",
    "price_change_pct_7d", "price_change_pct_30d", "circulating_supply",
    "total_supply", "max_supply", "ath", "ath_change_percentage", "atl",
    "atl_change_percentage",
]
# Enteros nullable (Int64) para tolerar nulos sin romper el tipo entero.
INT_COLS = ["market_cap", "market_cap_rank", "total_volume"]
STRING_COLS = ["coin_id", "symbol", "name", "last_updated"]

# Orden final de columnas = esquema Bronze (Parte 0.7).
BRONZE_COLUMNS = [
    "coin_id", "symbol", "name", "current_price", "market_cap",
    "market_cap_rank", "total_volume", "high_24h", "low_24h",
    "price_change_pct_24h", "price_change_pct_7d", "price_change_pct_30d",
    "circulating_supply", "total_supply", "max_supply", "ath",
    "ath_change_percentage", "atl", "atl_change_percentage", "last_updated",
    "extracted_at", "snapshot_date",
]


def _build_params() -> dict:
    """Arma los query params del endpoint /coins/markets.

    Incluye la Demo API key solo si está disponible (el endpoint también
    funciona keyless, pero usar la key demuestra manejo de credenciales).

    Returns:
        Diccionario de params listo para ``requests``.
    """
    params = {
        "vs_currency": VS_CURRENCY,
        "order": "market_cap_desc",
        "per_page": PER_PAGE,
        "page": PAGE,
        "price_change_percentage": "24h,7d,30d",
    }
    if COINGECKO_API_KEY:
        params["x_cg_demo_api_key"] = COINGECKO_API_KEY
    return params


def extract_markets(vs_currency: str = VS_CURRENCY, per_page: int = PER_PAGE) -> pd.DataFrame:
    """Descarga el snapshot de mercado de CoinGecko y lo devuelve como DataFrame.

    Llama al endpoint ``/coins/markets``, selecciona y renombra columnas al
    esquema Bronze (Parte 0.7), enriquece cada fila con ``extracted_at`` y
    ``snapshot_date`` (UTC) y castea tipos para escritura en Parquet.

    Args:
        vs_currency: Moneda de referencia (ej. ``"usd"``).
        per_page: Cantidad de monedas a traer (máx. 250 por página).

    Returns:
        DataFrame con exactamente las 22 columnas del esquema Bronze, ordenadas.

    Raises:
        requests.HTTPError: Si la API responde con error tras los reintentos.

    Example:
        >>> df = extract_markets("usd", 250)
        >>> df.shape
        (250, 22)
    """
    params = _build_params()
    params["vs_currency"] = vs_currency
    params["per_page"] = per_page

    log.info("Descargando %s monedas desde CoinGecko (%s)...", per_page, vs_currency)
    response = http_get_with_retry(COINGECKO_URL, params=params)
    payload = response.json()
    if not isinstance(payload, list) or not payload:
        raise ValueError(f"Respuesta inesperada de CoinGecko: {str(payload)[:200]}")

    df = pd.DataFrame(payload)

    # Sello temporal del snapshot (UTC), compartido por todas las filas del batch.
    now = datetime.now(timezone.utc)
    extracted_at = now.isoformat().replace("+00:00", "Z")
    snapshot_date = now.strftime("%Y-%m-%d")

    # Selección + renombre al esquema Bronze.
    df = df[list(RENAME_MAP.keys())].rename(columns=RENAME_MAP)

    # Enriquecido.
    df["extracted_at"] = extracted_at
    df["snapshot_date"] = snapshot_date

    # Casteo de tipos del contrato.
    df["symbol"] = df["symbol"].str.upper()
    for col in STRING_COLS:
        df[col] = df[col].astype("string")
    for col in FLOAT_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    for col in INT_COLS:
        # round() absorbe el ruido decimal float de la API antes de pasar a
        # entero nullable; los valores ya enteros no cambian.
        df[col] = pd.to_numeric(df[col], errors="coerce").round().astype("Int64")
    df["extracted_at"] = df["extracted_at"].astype("string")
    df["snapshot_date"] = df["snapshot_date"].astype("string")

    df = df[BRONZE_COLUMNS]
    log.info("Snapshot construido: %d filas x %d columnas", df.shape[0], df.shape[1])
    return df


def bronze_path(df: pd.DataFrame) -> Path:
    """Construye la ruta local particionada (estilo Hive) para el Parquet.

    Sigue el layout del contrato (Parte 0.6):
    ``coins_markets/dt=YYYY-MM-DD/coins_markets_YYYYMMDDTHHMMSSZ.parquet``.

    Args:
        df: DataFrame ya enriquecido (usa ``snapshot_date`` y ``extracted_at``).

    Returns:
        ``Path`` destino del archivo dentro de ``data/bronze/``.
    """
    snapshot_date = str(df["snapshot_date"].iloc[0])
    ts = str(df["extracted_at"].iloc[0])
    # 2026-06-06T03:13:55Z -> 20260606T031355Z
    ts_compact = ts.replace("-", "").replace(":", "").split(".")[0]
    if not ts_compact.endswith("Z"):
        ts_compact += "Z"
    filename = f"coins_markets_{ts_compact}.parquet"
    return BRONZE_DIR / "coins_markets" / f"dt={snapshot_date}" / filename


def main() -> Path:
    """Orquesta la extracción: descarga, valida y escribe el Parquet Bronze local.

    Deja el archivo listo para que ``load.py`` lo suba a GCS. Si la API falla
    tras los reintentos, sale con código distinto de 0 para que GitHub Actions
    marque la corrida en rojo.

    Returns:
        ``Path`` del Parquet escrito.
    """
    try:
        df = extract_markets()
    except Exception as exc:  # noqa: BLE001 — queremos exit code != 0 en CI
        log.error("Extracción fallida: %s", exc)
        sys.exit(1)

    path = to_parquet(df, bronze_path(df))
    log.info("Extracción Bronze completada → %s", path)
    # Los logs van a stderr; imprimimos SOLO la ruta a stdout para que el
    # workflow la capture con PARQUET_PATH=$(python scripts/extract.py) y se la
    # pase a load.py (Persona 3).
    print(path)
    return path


if __name__ == "__main__":
    main()

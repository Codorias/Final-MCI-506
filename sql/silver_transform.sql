-- Tabla: crypto_silver.silver_coins_markets
-- Proposito: snapshots diarios limpios y tipados del top 250 cripto
-- Actualizaciones: Scheduled Query diaria (30 min despues del extract)
-- Logica:
--   1. Leer del external table (Bronze Parquet en GCS)
--   2. Dedupe intra-batch por (coin_id, snapshot_date) con QUALIFY
--   3. Validacion: descartar coin_id NULL y current_price <= 0
--   4. INSERT incremental con WHERE NOT EXISTS (no truncar la tabla)
--   5. Recalcular silver_quality_report
--
-- Nota: reemplazar <PROJECT_ID> por el GCP_PROJECT_ID real (o quitar el prefijo
-- de proyecto si la Scheduled Query corre con el proyecto por defecto).

-- ============================================================
-- 0. External Table (Bronze view sobre Parquet en GCS) — DDL una sola vez
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS `crypto.bronze_coins_markets_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mci506-crypto-bronze-<PROJECT_ID>/coins_markets/*.parquet']
);

-- ============================================================
-- 1. Tabla Silver nativa (particionada por fecha, clusterizada por moneda)
-- ============================================================
CREATE TABLE IF NOT EXISTS `crypto_silver.silver_coins_markets` (
  coin_id               STRING  NOT NULL,
  symbol                STRING,
  name                  STRING,
  current_price         FLOAT64,
  market_cap            INT64,
  market_cap_rank       INT64,
  total_volume          INT64,
  high_24h              FLOAT64,
  low_24h               FLOAT64,
  price_change_pct_24h  FLOAT64,
  price_change_pct_7d   FLOAT64,
  price_change_pct_30d  FLOAT64,
  circulating_supply    FLOAT64,
  total_supply          FLOAT64,
  max_supply            FLOAT64,
  ath                   FLOAT64,
  ath_change_percentage FLOAT64,
  atl                   FLOAT64,
  atl_change_percentage FLOAT64,
  last_updated          TIMESTAMP,
  extracted_at          TIMESTAMP,
  snapshot_date         DATE    NOT NULL
)
PARTITION BY snapshot_date
CLUSTER BY coin_id;

-- ============================================================
-- 2 + 3. Staging con dedupe intra-batch y validacion
-- 4. INSERT incremental (solo claves de negocio nuevas)
-- ============================================================
INSERT INTO `crypto_silver.silver_coins_markets`
WITH staged AS (
  SELECT
    coin_id,
    symbol,
    name,
    current_price,
    market_cap,
    market_cap_rank,
    total_volume,
    high_24h,
    low_24h,
    price_change_pct_24h,
    price_change_pct_7d,
    price_change_pct_30d,
    circulating_supply,
    total_supply,
    max_supply,
    ath,
    ath_change_percentage,
    atl,
    atl_change_percentage,
    SAFE_CAST(last_updated AS TIMESTAMP) AS last_updated,
    SAFE_CAST(extracted_at AS TIMESTAMP) AS extracted_at,
    SAFE_CAST(snapshot_date AS DATE)     AS snapshot_date
  FROM `crypto.bronze_coins_markets_ext`
  WHERE coin_id IS NOT NULL
    AND current_price > 0
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY coin_id, snapshot_date
    ORDER BY extracted_at DESC
  ) = 1
)
SELECT s.*
FROM staged s
WHERE NOT EXISTS (
  SELECT 1
  FROM `crypto_silver.silver_coins_markets` t
  WHERE t.coin_id = s.coin_id
    AND t.snapshot_date = s.snapshot_date
);

-- ============================================================
-- 5. Reporte de calidad por snapshot (idempotente)
-- ============================================================
-- Tabla: crypto_silver.silver_quality_report
-- Proposito: metricas de calidad por dia (filas, nulos, duplicados, rango precio)
CREATE OR REPLACE TABLE `crypto_silver.silver_quality_report` AS
SELECT
  SAFE_CAST(snapshot_date AS DATE)                              AS snapshot_date,
  COUNT(*)                                                      AS rows_loaded,
  COUNTIF(current_price IS NULL OR current_price <= 0)          AS null_price_count,
  COUNT(*) - COUNT(DISTINCT coin_id)                            AS duplicates_detected,
  MIN(current_price)                                            AS min_price,
  MAX(current_price)                                            AS max_price
FROM `crypto.bronze_coins_markets_ext`
GROUP BY snapshot_date;

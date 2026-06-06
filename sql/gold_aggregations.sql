-- Tabla: crypto_gold.gold_market_overview_daily + crypto_gold.gold_coin_performance
-- Proposito: tablas de negocio agregadas para Looker Studio
-- Actualizaciones: Scheduled Query diaria (despues de silver_transform.sql)
-- Logica:
--   - CREATE OR REPLACE (idempotente) a partir de la capa Silver
--   - Overview: 1 fila por dia (mercado global)
--   - Performance: 1 fila por moneda por dia (metricas derivadas)
--
-- Nota: reemplazar <PROJECT_ID> si se usa prefijo de proyecto explicito.

-- ============================================================
-- 1. Vision general del mercado (1 fila por snapshot_date)
-- ============================================================
CREATE OR REPLACE TABLE `crypto_gold.gold_market_overview_daily` AS
WITH base AS (
  SELECT *
  FROM `crypto_silver.silver_coins_markets`
),
agg AS (
  SELECT
    snapshot_date,
    SUM(market_cap)                                   AS total_market_cap,
    SUM(total_volume)                                 AS total_volume_24h,
    COUNT(*)                                          AS num_coins,
    AVG(price_change_pct_24h)                         AS avg_price_change_24h,
    SUM(IF(coin_id = 'bitcoin',  market_cap, 0))      AS btc_market_cap,
    SUM(IF(coin_id = 'ethereum', market_cap, 0))      AS eth_market_cap
  FROM base
  GROUP BY snapshot_date
),
gainers AS (
  SELECT snapshot_date, symbol AS top_gainer_symbol, price_change_pct_24h AS top_gainer_pct_24h
  FROM base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY snapshot_date ORDER BY price_change_pct_24h DESC
  ) = 1
),
losers AS (
  SELECT snapshot_date, symbol AS top_loser_symbol, price_change_pct_24h AS top_loser_pct_24h
  FROM base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY snapshot_date ORDER BY price_change_pct_24h ASC
  ) = 1
)
SELECT
  a.snapshot_date,
  a.total_market_cap,
  a.total_volume_24h,
  a.num_coins,
  SAFE_DIVIDE(a.btc_market_cap, a.total_market_cap) * 100 AS btc_dominance_pct,
  SAFE_DIVIDE(a.eth_market_cap, a.total_market_cap) * 100 AS eth_dominance_pct,
  a.avg_price_change_24h,
  g.top_gainer_symbol,
  g.top_gainer_pct_24h,
  l.top_loser_symbol,
  l.top_loser_pct_24h
FROM agg a
LEFT JOIN gainers g USING (snapshot_date)
LEFT JOIN losers  l USING (snapshot_date);

-- ============================================================
-- 2. Desempeno por moneda (1 fila por moneda por snapshot_date)
-- ============================================================
CREATE OR REPLACE TABLE `crypto_gold.gold_coin_performance` AS
SELECT
  snapshot_date,
  coin_id,
  symbol,
  name,
  market_cap_rank,
  current_price,
  market_cap,
  total_volume,
  price_change_pct_24h,
  price_change_pct_7d,
  price_change_pct_30d,
  SAFE_DIVIDE(high_24h - low_24h, NULLIF(current_price, 0)) * 100 AS intraday_range_pct,
  ath_change_percentage,
  SAFE_DIVIDE(total_volume, NULLIF(market_cap, 0))               AS volume_to_mcap_ratio
FROM `crypto_silver.silver_coins_markets`;

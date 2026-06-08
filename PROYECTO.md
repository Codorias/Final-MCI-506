# PROYECTO FINAL MCI506 — Pipeline de Datos Cripto (CoinGecko)

-----

# PARTE 0 — CONTRATO COMPARTIDO (read-only; cambia solo por PR + review de los 3)

Esta es la **interfaz** entre las piezas. Si algo de aquí cambia, las tres personas se enteran (PR que los tres revisan). Es, además, la base del `ARCHITECTURE.md`.

## 0.1 Resumen y stack

Pipeline medallion **Bronze → Silver → Gold** sobre el mercado cripto (top 250 monedas por capitalización, fuente CoinGecko). Cada corrida = snapshot del momento; corriendo 1×/día se acumulan snapshots diarios.

|Capa                       |Herramienta                             |
|---------------------------|----------------------------------------|
|Extracción                 |Python                                  |
|Almacenamiento (Bronze)    |Google Cloud Storage (GCS) — **Parquet**|
|Procesamiento (Silver/Gold)|BigQuery                                |
|Orquestación del extract   |GitHub Actions (cron)                   |
|Orquestación Silver→Gold   |BigQuery Scheduled Query                |
|Visualización              |Looker Studio                           |

## 0.2 Flujo

```
CoinGecko API → extract.py (JSON→DataFrame→Parquet) → GCS Bronze (Parquet particionado)
                                                              │
                                          BigQuery External Table (lee GCS)
                                                              │  [Scheduled Query, diaria]
                                          Silver (nativa, tipada, dedup incremental)
                                                              │
                                          Gold (2 tablas de negocio)
                                                              │
                                                  Looker Studio (4-5 viz)
```

## 0.3 Repositorio (público)

**Nombre:** `mci506-crypto`

```
mci506-crypto/
├── scripts/
│   ├── extract.py        # Pull CoinGecko + enriquecido + escritura Parquet local
│   ├── load.py           # Subida del Parquet a GCS (Bronze, path particionado)
│   └── utils.py          # Config, logging, cliente GCS, retry HTTP, to_parquet()
├── sql/
│   ├── silver_transform.sql    # DDL + INSERT incremental (WHERE NOT EXISTS) a Silver
│   └── gold_aggregations.sql   # CREATE OR REPLACE de las 2 tablas Gold
├── .github/workflows/
│   └── pipeline.yml      # GitHub Actions: cron diario → extract + load a GCS
├── .env.example          # Variables de entorno (sin secretos reales)
├── README.md             # Responde las 7 preguntas + diagrama
├── ARCHITECTURE.md       # Diagrama, decisiones, costos, escalabilidad (Parte 0)
├── SCHEMA.md             # (opcional) Descripción de tablas y columnas
└── requirements.txt
```

## 0.4 Nombres fijos (datasets, tablas, bucket) — NO cambiar sin PR

- **Bucket GCS:** `mci506-crypto-bronze-<PROJECT_ID>` (globalmente único, minúsculas).
- **Dataset external (Bronze view):** `crypto`
  - `crypto.bronze_coins_markets_ext` (external, formato PARQUET, lee `gs://.../coins_markets/*.parquet`)
- **Dataset Silver:** `crypto_silver`
  - `crypto_silver.silver_coins_markets` (nativa, particionada por `snapshot_date`, clusterizada por `coin_id`)
  - `crypto_silver.silver_quality_report`
- **Dataset Gold:** `crypto_gold`
  - `crypto_gold.gold_market_overview_daily`
  - `crypto_gold.gold_coin_performance`
- **Region/location:** todo en `US` (o `us-central1`), consistente entre bucket y datasets.

## 0.5 Clave de negocio (la usan Silver y la dedup)

`(coin_id, snapshot_date)` — es el análogo cripto del `(year, eventname, driver, lapnumber)` del proyecto F1.

## 0.6 Rutas GCS (layout particionado, estilo Hive)

```
gs://mci506-crypto-bronze-<PROJECT_ID>/
└── coins_markets/
    └── dt=YYYY-MM-DD/
        └── coins_markets_YYYYMMDDTHHMMSSZ.parquet
```

## 0.7 Esquema Bronze (columnas que `extract.py` escribe en el Parquet)

`extract.py` toma el JSON de CoinGecko, lo pasa a DataFrame, castea tipos en pandas y escribe Parquet con **exactamente** estas columnas. Silver depende de estos nombres/tipos.

|Columna                |Tipo (pandas/Parquet)    |Origen en la API                         |
|-----------------------|-------------------------|-----------------------------------------|
|`coin_id`              |string                   |`id`                                     |
|`symbol`               |string                   |`symbol` (UPPER)                         |
|`name`                 |string                   |`name`                                   |
|`current_price`        |float64                  |`current_price`                          |
|`market_cap`           |int64                    |`market_cap`                             |
|`market_cap_rank`      |int64                    |`market_cap_rank`                        |
|`total_volume`         |int64                    |`total_volume`                           |
|`high_24h`             |float64                  |`high_24h`                               |
|`low_24h`              |float64                  |`low_24h`                                |
|`price_change_pct_24h` |float64                  |`price_change_percentage_24h_in_currency`|
|`price_change_pct_7d`  |float64                  |`price_change_percentage_7d_in_currency` |
|`price_change_pct_30d` |float64                  |`price_change_percentage_30d_in_currency`|
|`circulating_supply`   |float64                  |`circulating_supply`                     |
|`total_supply`         |float64                  |`total_supply`                           |
|`max_supply`           |float64                  |`max_supply`                             |
|`ath`                  |float64                  |`ath`                                    |
|`ath_change_percentage`|float64                  |`ath_change_percentage`                  |
|`atl`                  |float64                  |`atl`                                    |
|`atl_change_percentage`|float64                  |`atl_change_percentage`                  |
|`last_updated`         |string (ISO)             |`last_updated`                           |
|`extracted_at`         |string (ISO, UTC)        |**agregado** por extract.py              |
|`snapshot_date`        |string `YYYY-MM-DD` (UTC)|**agregado** por extract.py              |


> Usar Parquet (no JSON) hace que el External Table herede los tipos automáticamente → sin casts manuales en SQL.

## 0.8 Variables de entorno (`.env.example`)

```
GCP_PROJECT_ID=tu-proyecto-id
GCS_BUCKET=mci506-crypto-bronze-tu-proyecto-id
BIGQUERY_DATASET_EXTERNAL=crypto
BIGQUERY_DATASET_SILVER=crypto_silver
BIGQUERY_DATASET_GOLD=crypto_gold
COINGECKO_API_KEY=tu-demo-api-key
GOOGLE_APPLICATION_CREDENTIALS=./sa-key.json
```

## 0.9 Fuente de datos (CoinGecko)

- **Endpoint:** `GET https://api.coingecko.com/api/v3/coins/markets`
- **Params:** `vs_currency=usd&order=market_cap_desc&per_page=250&page=1&price_change_percentage=24h,7d,30d&x_cg_demo_api_key=<COINGECKO_API_KEY>`
- **Auth:** Demo API key gratis (registro sin tarjeta → Developer Dashboard → API Keys). Demo plan: 30 calls/min estable, 10,000 calls/mes. 1 call/día está muy por debajo del límite. (Funciona keyless también, pero usamos key para demostrar manejo de credenciales.)
- **Volumen:** ~250 filas × ~30 columnas → cumple “100+ filas, 5+ columnas”.

## 0.10 Estilo de documentación obligatorio (Temas 4-5)

**Docstrings Python (Google-style), en TODAS las funciones:**

```python
def extract_markets(vs_currency: str = "usd", per_page: int = 250) -> pd.DataFrame:
    """Descarga el snapshot de mercado de CoinGecko y lo devuelve como DataFrame.

    Llama al endpoint /coins/markets, enriquece cada fila con extracted_at y
    snapshot_date (UTC), y castea tipos para escritura en Parquet.

    Args:
        vs_currency: Moneda de referencia (ej. "usd").
        per_page: Cantidad de monedas a traer (máx. 250 por página).

    Returns:
        DataFrame con el esquema Bronze definido en la Parte 0.7.

    Raises:
        requests.HTTPError: Si la API responde con error tras los reintentos.

    Example:
        >>> df = extract_markets("usd", 250)
        >>> df.shape
        (250, 22)
    """
```

**Cabecera de comentario en cada query SQL:**

```sql
-- Tabla: crypto_silver.silver_coins_markets
-- Propósito: snapshots diarios limpios y tipados del top 250 cripto
-- Actualizaciones: Scheduled Query diaria (1 h después del extract)
-- Lógica:
--   1. Leer del external table (Bronze Parquet)
--   2. Dedupe intra-batch por (coin_id, snapshot_date) con QUALIFY
--   3. Validación: descartar coin_id NULL y current_price <= 0
--   4. INSERT incremental con WHERE NOT EXISTS (no truncar la tabla)
```

-----

# PARTE 1 — PERSONA 1: Extracción / Bronze

**Archivos:** `scripts/extract.py`, `scripts/utils.py`, `requirements.txt`

### `utils.py`

- Config desde env (project, bucket, datasets, API key).
- Logging con `logging` (no `print`): formato con timestamp y nivel.
- Cliente GCS (`google.cloud.storage`).
- `http_get_with_retry(url, params)`: `requests` con reintentos y backoff exponencial (principio *Plan for Failure*, Tema 2).
- `to_parquet(df, path)`: helper estilo F1 (crea carpeta si no existe, escribe Parquet, loguea filas). Docstring obligatorio.

### `extract.py`

- `extract_markets(...)`: pull CoinGecko → DataFrame → enriquecer con `extracted_at` y `snapshot_date` → castear tipos → devolver DataFrame con esquema **Parte 0.7**.
- Escribe Parquet local en ruta temporal con nombre `coins_markets_YYYYMMDDTHHMMSSZ.parquet`.
- `main()`: orquesta extract; deja el path listo para que `load.py` lo suba.
- Manejo de errores: try/except con logging claro; si la API falla tras reintentos, salir con código ≠ 0 (para que Actions marque rojo).

### DoD Parte 1

- `python scripts/extract.py` corre local y genera un `.parquet` con ~250 filas y las 22 columnas de la Parte 0.7.
- Tipos correctos (numéricos como float/int, no strings).
- Todas las funciones con docstring Google-style.

-----

# PARTE 2 — PERSONA 2: Transformación (Silver + Gold)

**Archivos:** `sql/silver_transform.sql`, `sql/gold_aggregations.sql` (+ define el SQL de la Scheduled Query, que P3 agenda)

### External Table (DDL, una vez) — `crypto.bronze_coins_markets_ext`

- Formato `PARQUET`, URIs `gs://mci506-crypto-bronze-<PROJECT_ID>/coins_markets/*.parquet`.
- *(Nice-to-have)* hive partitioning con prefijo `gs://.../coins_markets` para exponer `dt`.

### `silver_transform.sql` (multi-statement, idempotente)

1. `CREATE TABLE IF NOT EXISTS crypto_silver.silver_coins_markets (...)` particionada por `snapshot_date`, clusterizada por `coin_id`.
1. Staging con dedupe intra-batch:
   
   ```sql
   QUALIFY ROW_NUMBER() OVER (
     PARTITION BY coin_id, snapshot_date ORDER BY extracted_at DESC
   ) = 1
   ```
- validación `WHERE coin_id IS NOT NULL AND current_price > 0`.
1. INSERT incremental:
   
   ```sql
   INSERT INTO crypto_silver.silver_coins_markets (...)
   SELECT s.* FROM staged s
   WHERE NOT EXISTS (
     SELECT 1 FROM crypto_silver.silver_coins_markets t
     WHERE t.coin_id = s.coin_id AND t.snapshot_date = s.snapshot_date
   );
   ```
1. `crypto_silver.silver_quality_report`: por `snapshot_date`, contar `rows_loaded`, `null_price_count`, `duplicates_detected`, `min_price`, `max_price`.

### `gold_aggregations.sql` (CREATE OR REPLACE, idempotente)

- **`crypto_gold.gold_market_overview_daily`** (1 fila/día): `snapshot_date`, `total_market_cap`, `total_volume_24h`, `num_coins`, `btc_dominance_pct`, `eth_dominance_pct`, `avg_price_change_24h`, `top_gainer_symbol`, `top_gainer_pct_24h`, `top_loser_symbol`, `top_loser_pct_24h`.
- **`crypto_gold.gold_coin_performance`** (1 fila/moneda/día): identificadores + `current_price`, `market_cap`, `total_volume`, `price_change_pct_24h/7d/30d`, `intraday_range_pct` = `(high_24h-low_24h)/NULLIF(current_price,0)*100`, `ath_change_percentage`, `volume_to_mcap_ratio` = `total_volume/NULLIF(market_cap,0)`.

### DoD Parte 2

- `COUNT(*)` de Silver ≈ 250 tras la primera corrida; **re-correr NO aumenta el count** (idempotencia).
- Gold: overview con 1 fila/día; performance ~250 filas/día; `btc_dominance_pct` razonable (~45-60%).
- Cabeceras de comentario en ambos archivos SQL (Parte 0.10).

-----

# PARTE 3 — PERSONA 3: Orquestación / Documentación

**Archivos:** `scripts/load.py`, `.github/workflows/pipeline.yml`, `README.md`, `ARCHITECTURE.md`, `.env.example`
**Tareas manuales** (no generan commits, ver nota en Parte 4): setup GCP, crear Scheduled Query, construir Looker, otorgar accesos.

### `load.py`

- Sube el Parquet local a GCS en el path particionado de la Parte 0.6.
- Idempotente: nombre con timestamp evita sobrescritura; re-subir no rompe.

### `pipeline.yml` (molde de clase, Tema 5)

- Triggers: `schedule: cron '0 10 * * *'` (= **06:00 Bolivia, UTC-4**) + `workflow_dispatch`.
- Steps: `checkout@v4` → `setup-python@v5` (3.11) → `pip install -r requirements.txt` → `python scripts/extract.py` → `python scripts/load.py`.
- `GCP_SA_KEY` como `env` (estilo F1); el código escribe la credencial a archivo y setea `GOOGLE_APPLICATION_CREDENTIALS`.
- Secrets: `GCP_SA_KEY`, `COINGECKO_API_KEY`, `GCS_BUCKET`, `GCP_PROJECT_ID`.
- *(Nice-to-have)* resumen al `$GITHUB_STEP_SUMMARY` (filas, path) y notify opcional en fallo.

### Scheduled Query (BigQuery)

- Multi-statement: ejecuta `silver_transform.sql` y luego `gold_aggregations.sql`. Diaria **10:30 UTC** (30 min después del extract). SQL versionado por P2.

### `README.md` — 7 preguntas (Tema 5) + diagrama

1. ¿QUÉ? top 250 cripto, ~250×22, tipos, dominio. 2. ¿DE DÓNDE? CoinGecko + cómo obtener el key. 3. ¿A DÓNDE? bucket + layout `dt=`. 4. ¿CUÁNDO? Actions 10:00 UTC / SQ 10:30 UTC (zona horaria explícita). 5. ¿CÓMO? medallion + `WHERE NOT EXISTS`. 6. ¿CALIDAD? nulos, precio>0, dedup, `silver_quality_report`. 7. ¿SI FALLA? logs (Actions / SQ history), re-ejecutar (`workflow_dispatch`), idempotencia, contacto.

### `ARCHITECTURE.md`

- Diagrama del flujo (Mermaid/ASCII), decisiones de diseño, costos estimados (GCS + BigQuery), notas de escalabilidad. Refleja la Parte 0.

### DoD Parte 3

- `workflow_dispatch` corre verde y escribe a GCS.
- README responde las 7 preguntas; `ARCHITECTURE.md` con diagrama.
- Accesos otorgados (ver Parte 4).

-----

# PARTE 4 — INTEGRACIÓN, FASES Y FLUJO GIT

## 4.1 Reparto y nombres

|Rol|Persona   |Capa             |Archivos                                                                 |
|---|----------|-----------------|-------------------------------------------------------------------------|
|P1 |*(nombre)*|Extracción/Bronze|`extract.py`, `utils.py`, `requirements.txt`                             |
|P2 |*(nombre)*|Transformación   |`silver_transform.sql`, `gold_aggregations.sql` (+ SQL de la SQ)         |
|P3 |*(nombre)*|Orquestación/Docs|`load.py`, `pipeline.yml`, `README.md`, `ARCHITECTURE.md`, `.env.example`|

## 4.2 Flujo Git por tarea (Temas 1 y 3)

1. `git checkout -b feature/<tarea>` (ej. `feature/extract-coingecko`).
1. Commits chicos y frecuentes, mensajes `feat:` / `fix:` / `docs:`.
1. `git push origin feature/<tarea>`.
1. Abrir **Pull Request** a `main`.
1. **Review** del compañero asignado (cadena 4.4).
1. Merge a `main` → borrar la rama.

## 4.3 Reglas de oro

- `main` **protegida**: nada de push directo; todo entra por PR con **1 aprobación**.
- **Lo que está en la Parte 0 se cambia con PR que los tres revisan.** Lo que está en tu Parte, lo manejás vos.
- **Nunca** subir secrets al repo (Tema 3). Solo `.env.example` sin valores reales.
- Commits frecuentes y pequeños > un commit gigante al final (que todos tengan historial real).

## 4.4 Cadena de reviews

|Revisa|A |Por qué                                         |
|------|--|------------------------------------------------|
|P2    |P1|Silver consume el esquema de Bronze (Parte 0.7) |
|P3    |P2|La orquestación depende del SQL de Silver/Gold  |
|P1    |P3|`extract.py` se integra dentro de `pipeline.yml`|


> Círculo cerrado: los tres revisan y son revisados. Captura el nice-to-have *PR + code reviews*.

## 4.5 Fases con phase gates (no avanzar sin DoD)

- **Fase 0 (manual):** GCP (proyecto, APIs, bucket, datasets, service account + key), repo público, branch protection. → P3 lidera.
- **Fase 1:** Parte 1 (extract → Parquet local). DoD Parte 1.
- **Fase 2:** Parte 2 (external + Silver incremental + Gold). DoD Parte 2.
- **Fase 3:** Scheduled Query (Silver→Gold automática). Corre sola y aparece en el historial.
- **Fase 4:** GitHub Actions (extract automático). `workflow_dispatch` verde + cron programado.
- **Fase 5:** Looker Studio (4-5 viz + filtros). ≥3 viz funcionando, link compartido.
- **Fase 6:** Docs + accesos + auditoría final contra rúbrica.

## 4.6 Accesos obligatorios (no olvidar)

- GitHub: agregar **`auzaluis`** como Collaborator.
- GCP: agregar **`luis.auza@gmail.com`** como **Editor** del proyecto.

## 4.7 Nota de balance de commits

Las tareas manuales de P3 (GCP, Looker, accesos) **no generan commits**. Por eso P3 también escribe código/docs (`load.py`, `pipeline.yml`, `README`, `ARCHITECTURE`). Si igual queda flojo el conteo, redistribuir (ej. P3 toma también `.env.example` y parte del README de P1/P2).

-----

# Mapa de puntaje (objetivo: 90-100)

|Criterio          |Must-have                              |Nice-to-have capturado              |
|------------------|---------------------------------------|------------------------------------|
|Selección de datos|✅ 250 filas, ~22 cols, pública         |✅ Actualización automática (cron)   |
|Extracción        |✅ genera archivos                      |✅ Errores robustos (retry + logging)|
|Bronze (GCS)      |✅ Parquet en GCS                       |✅ Particionamiento `dt=`            |
|Silver            |✅ External + Native, `WHERE NOT EXISTS`|✅ Validaciones + quality report     |
|Gold              |✅ 1 tabla                              |✅ 2 tablas                          |
|GitHub Actions    |✅ Cron + `GCP_SA_KEY` en secrets       |✅ Logs en step summary              |
|Scheduled Query   |✅ Corre sola                           |✅ Multi-statement (2+ queries)      |
|README            |✅ 7 preguntas                          |✅ Diagrama (+ ARCHITECTURE.md)      |
|Docstrings        |✅ Funciones principales                |✅ Todas las funciones               |
|Dashboard         |✅ 3+ viz                               |✅ Controles/filtros                 |
|Equipo            |✅ Commits de todos                     |✅ PR + code reviews                 |

# requirements.txt (referencia)

```
requests
pandas
pyarrow
google-cloud-storage
python-dotenv
```

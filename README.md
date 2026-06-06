# MCI506 — Pipeline de Datos Cripto (CoinGecko)

Proyecto final de **Ingeniería de Datos (MCI506)** — Universidad Católica Boliviana.

Pipeline **medallion Bronze → Silver → Gold** que extrae el mercado cripto (top 250 monedas,
fuente [CoinGecko](https://www.coingecko.com/)) y lo procesa de forma automatizada sobre
**GCS + BigQuery + GitHub Actions + Looker Studio**.

> 📄 El diseño completo, esquemas, nombres y reparto de tareas están en **[`PROYECTO.md`](PROYECTO.md)**
> (contrato de implementación del equipo).

## Stack

| Capa | Herramienta |
|------|-------------|
| Extracción | Python |
| Almacenamiento (Bronze) | Google Cloud Storage (Parquet) |
| Procesamiento (Silver/Gold) | BigQuery |
| Orquestación | GitHub Actions (cron) + BigQuery Scheduled Query |
| Visualización | Looker Studio |

## Estructura

```
Final-MCI-506/
├── scripts/        # extract.py, load.py, utils.py
├── sql/            # silver_transform.sql, gold_aggregations.sql
├── .github/workflows/pipeline.yml
├── .env.example
├── requirements.txt
└── PROYECTO.md     # contrato de implementación
```

## Equipo

| Rol | Capa | Archivos |
|-----|------|----------|
| Persona 1 | Extracción / Bronze | `scripts/extract.py`, `scripts/utils.py`, `requirements.txt` |
| Persona 2 | Transformación (Silver/Gold) | `sql/silver_transform.sql`, `sql/gold_aggregations.sql` |
| Persona 3 | Orquestación / Docs | `scripts/load.py`, `pipeline.yml`, `README.md`, `ARCHITECTURE.md`, `.env.example` |

---

> ℹ️ *README inicial (placeholder). La Persona 3 lo completará con las 7 preguntas, el diagrama y
> la guía de uso, según `PROYECTO.md` (Parte 3).*

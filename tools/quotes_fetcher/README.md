# quotes_fetcher

Descarga de cotizaciones **fuera de Databricks** (plan B si Free Edition bloquea el egress — ADR-008). Mismo contrato que el `QuoteProvider` usado en el pipeline.

- Input: lista de símbolos mapeados (export de `iol_ops.instrument_mapping` con `provider_symbol`).
- Output: `data/quotes_raw/<provider>/<provider_symbol>.json` + `data/quotes_raw/manifest.csv` (una fila por request: provider, symbol, from, to, http_status, attempts, rows_returned, error, requested_at).
- Solo pide (símbolo, rango) que falten en el manifest → reejecutable.
- Credenciales por variables de entorno (`IOL_API_USER`, `IOL_API_PASSWORD`, …), nunca en el repo.
- Luego: subir `data/quotes_raw/` a `/Volumes/<catalog>/iol_bronze/landing/quotes/` y correr el notebook 05 con `quotes_source=volume`.

Ver `docs/specs/05_cotizaciones.md`.

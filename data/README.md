# Datos

## Dataset original
No se publica (lo exige el challenge). Para correr el pipeline completo:
1. Copiar el archivo original a `data/raw/` (gitignored) para desarrollo local.
2. Subirlo en Databricks a `/Volumes/<catalog>/iol_bronze/landing/trades/`.

## Muestra ofuscada — `sample_obfuscated.csv`
**Todavía no está en el repo.** `scripts/make_obfuscated_sample.py` la genera, con este contrato:
- Filas estratificadas por mes × canal × moneda.
- Incluye siempre casos de sábado, variantes de liquidación (AL30/AL30D/AL30C, AAPL/AAPLD/AAPLC) y los casos de posible error de escala.
- `id_cliente` e `id_transaccion` reemplazados por HMAC-SHA256 con salt secreto (no publicado), conservando el formato `CLI…`/`TXN…` y la relación cliente↔operaciones dentro de la muestra.
- Símbolos, descripciones, monedas, canales, fechas, cantidades y precios se conservan (son datos de mercado, no personales).

Con la muestra el pipeline corre de punta a punta; las métricas no son comparables con las del dataset completo.

## Cotizaciones
`data/quotes_raw/` (gitignored): salida de `tools/quotes_fetcher`. No se publica el crudo del proveedor si sus términos no lo permiten; se publica la cobertura.

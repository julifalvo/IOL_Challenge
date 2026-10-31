# ADR-008 — Fuente de cotizaciones y modo de adquisición

- **Estado:** propuesto (completar con el spike del día 1)
- **Requisitos:** R-17, R-18, R-20

## Contexto
- El PDF da libertad de fuente y pide justificarla para el mercado local.
- Free Edition restringe la salida a internet; no hay lista pública de dominios permitidos.

## Spike (8 símbolos: AL30, AL30D, AL30C, AAPL, AAPLD, YPFD, T30J6, YM34D; 2025-12-15..2026-03-13)

| Proveedor | Desde Free Edition | Desde local | Cobertura 8/8 | OHLC | Escala bonos | Límites / términos | Notas |
|---|---|---|---|---|---|---|---|
| API IOL (serie histórica) | | | | | | | requiere cuenta |
| BYMA Open Data | | | | | | | |
| data912 | | | | | | | |
| yfinance (`.BA`) | | | | | | | no oficial |

## Decisión (completar)
- Primario por `asset_type`: …
- Fallback: …
- Modo: `api` desde Databricks si hay egress; si no, `tools/quotes_fetcher` local → Volume → `quotes_raw` con `FileReplayProvider`. Mismo contrato y misma tabla `quote_requests` en ambos modos.

## Criterios de elección
1. Cobertura de variantes D/C y de soberanos (mayor parte de AL30 = 13.448 operaciones).
2. Histórico diario OHLC para el período.
3. Fuente oficial o cercana al mercado > agregador.
4. Términos de uso compatibles con un repo público (no se publican datos crudos del proveedor si los términos no lo permiten; se publica cobertura y agregados).

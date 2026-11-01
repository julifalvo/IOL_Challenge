# ADR-008 — Fuente de cotizaciones y modo de adquisición

- **Estado:** propuesto (completar con el spike del día 1)
- **Requisitos:** R-17, R-18, R-20

## Contexto
- El PDF da libertad de fuente y pide justificarla para el mercado local.
- Free Edition restringe la salida a internet; no hay lista pública de dominios permitidos.

## Egress desde Free Edition (smoke test, 2026-11-01)
`requests.get` a la raíz de cada dominio desde serverless (`smoke_run_id = 1cc453e0-…`). Cualquier respuesta HTTP cuenta como "el dominio no está bloqueado".

| Proveedor | URL probada | Respuesta [MEDIDO] |
|---|---|---|
| API IOL | `https://api.invertironline.com` | HTTP 200 |
| BYMA Open Data | `https://open.bymadata.com.ar` | HTTP 200 |
| data912 | `https://data912.com` | HTTP 200 |
| Yahoo Finance | `https://query1.finance.yahoo.com` | HTTP 429 (Too Many Requests) |

Qué prueba y qué no:
- **Prueba:** la salida a internet de Free Edition no bloquea ninguno de los 4 dominios. El modo `api` desde Databricks es viable a nivel red.
- **No prueba:** que los endpoints de datos devuelvan series para los símbolos del challenge. Solo se pidió la raíz, sin autenticación ni parámetros. Eso se mide en el spike.
- **Yahoo:** el 429 indica rate limiting, probablemente por IP compartida de serverless [HIPÓTESIS]. Que se pueda usar desde Databricks sigue sin verificar.
- Por ahora el fetcher local (`tools/quotes_fetcher`) deja de ser necesario por egress. Sigue como plan B por rate limiting o términos de uso.

## Spike (8 símbolos: AL30, AL30D, AL30C, AAPL, AAPLD, YPFD, T30J6, YM34D; 2025-12-15..2026-03-13)

| Proveedor | Desde Free Edition | Desde local | Cobertura 8/8 | OHLC | Escala bonos | Límites / términos | Notas |
|---|---|---|---|---|---|---|---|
| API IOL (serie histórica) | dominio OK | | | | | | requiere cuenta |
| BYMA Open Data | dominio OK | | | | | | |
| data912 | dominio OK | | | | | | |
| yfinance (`.BA`) | dominio OK, HTTP 429 | | | | | | no oficial |

## Decisión (completar)
- Primario por `asset_type`: …
- Fallback: …
- Modo: `api` desde Databricks si hay egress; si no, `tools/quotes_fetcher` local → Volume → `quotes_raw` con `FileReplayProvider`. Mismo contrato y misma tabla `quote_requests` en ambos modos.

## Criterios de elección
1. Cobertura de variantes D/C y de soberanos (mayor parte de AL30 = 13.448 operaciones).
2. Histórico diario OHLC para el período.
3. Fuente oficial o cercana al mercado > agregador.
4. Términos de uso compatibles con un repo público (no se publican datos crudos del proveedor si los términos no lo permiten; se publica cobertura y agregados).

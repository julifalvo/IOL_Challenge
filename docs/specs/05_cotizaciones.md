# Spec 05 — Enriquecimiento con cotizaciones de mercado (Task 04, 8 pts)

**Requisitos:** R-16..R-21 · **ADR:** 005, 008, 009

## Paso 0 — Spike de proveedores (día 1, timebox 2 h)
Probar con 8 símbolos: `AL30, AL30D, AL30C, AAPL, AAPLD, YPFD, T30J6, una ON USD (ej. YM34D)` para el rango 2025-12-15..2026-03-13.

Candidatos (verificar todo; nada de esto se asume):
| Proveedor | Qué verificar |
|---|---|
| API de IOL (InvertirOnline) — serie histórica por título | Requiere cuenta/token; endpoints, cobertura de variantes D/C, términos de uso. Usar la API del propio entrevistador es buena señal si funciona |
| BYMA Open Data | Histórico disponible o solo día corriente; acceso sin key |
| data912 | Cobertura de bonos/ONs/CEDEARs, histórico |
| yfinance con sufijo `.BA` | Cobertura de variantes D/C, letras y ONs; escala de bonos |

Para cada uno, en ADR-008: accesible desde Free Edition (sí/no), accesible local (sí/no), cobertura de los 8, OHLC o solo cierre, escala, límites. Elegir **primario** y **fallback** por `asset_type`.

## Arquitectura
```
QuoteProvider (interfaz)
  fetch_daily(provider_symbol, date_from, date_to) -> list[QuoteRow] | raises ProviderError
  ├─ IolApiProvider / BymaProvider / Data912Provider / YFinanceProvider   (los que pasen el spike)
  └─ FileReplayProvider  (lee raw de Volume — usado en Databricks si no hay egress, y en tests)
```
- `tools/quotes_fetcher/fetch.py` (local): itera símbolos del mapping, pide solo (símbolo, rango) faltantes, guarda raw por request en `data/quotes_raw/{provider}/{symbol}.json` + `manifest_{provider}.csv` (= filas de `quote_requests`). Subir carpeta al Volume `landing/quotes/`.
- Si hay egress en Databricks, el notebook usa el provider real con el mismo contrato.
- Resiliencia: timeout 10 s, 3 reintentos con backoff exponencial + jitter solo en 429/5xx/timeout; 4xx (símbolo inexistente) no se reintenta; error por símbolo no detiene el lote; rate limit configurable.

## Normalización
`fact_market_quotes.close = close_raw × price_scale_factor`, con el factor por símbolo y proveedor elegido entre candidatos (ADR-005). **Validación por serie** en `iol_ops.quote_series`: con `median(trade.price / close crudo del mismo día)`, después del factor, fuera de [0,5; 2] → `SCALE_SUSPECT`; sesgo mayor a τ → `BIASED`; ninguna serie usable → la operación queda con `match_method` `SCALE_SUSPECT` o `SOURCE_BIASED`. La fila de `quote_series` es el evento: no va a `quality_events`, porque es una falla de la fuente y no de la operación.

## Matching (ADR-009)
Para cada trade elegible con `instrument_sk` mapeado:
1. Cotización con `quote_date = effective_trade_date` → `EXACT_DAY`.
2. Si no: última cotización con `quote_date < effective_trade_date` y antigüedad ≤ `MAX_QUOTE_AGE_BDAYS` (3) → `PRIOR_DAY_IMPUTED`, guardar `quote_date_used` y antigüedad.
3. Si no → `UNAVAILABLE`. **Nunca** cotización posterior (no look-ahead).
- Implementar con join por rango + `row_number()` sobre `quote_date desc`, no con UDF.
- Benchmark = **cierre diario** → se etiqueta "benchmark de cierre", no "precio al instante" (la hora del trade no es confiable, ADR-003).
- `position_vs_range` con high/low si el proveedor los da: comparación más defendible que contra el cierre.
- `is_above_market = price > close × (1 + τ)`, τ por `asset_type`, calibrada en ADR-009 con el rango diario medido.

**Expectativa realista:** opciones (~1.824 operaciones) y ONs probablemente sin histórico en fuentes gratuitas. No se excluyen del denominador: se reportan con cobertura baja por `asset_type`, y la meta de G08 se interpreta junto con esa tabla.

## Cobertura (`iol_ops.quote_coverage`)
Por símbolo: `trades_total, trades_eligible, matched_exact, matched_imputed, unavailable, excluded_by_validation, unmapped, coverage_pct = (exact+imputed)/eligible, last_error`, más la serie elegida. Más resumen por `asset_type` y ponderado por trades. Exportar a `docs/quote_coverage.md` (tabla completa) y top 30 + resumen en README.

## Tests
- Provider que lanza error en un símbolo → los demás se procesan, error en `quote_requests`.
- Sábado matchea con cierre del viernes (`EXACT_DAY` sobre `effective_trade_date`).
- Feriado → `PRIOR_DAY_IMPUTED` con antigüedad correcta.
- Gap de 5 días hábiles → `UNAVAILABLE`.
- Nunca se usa cotización futura (test con cotización del día siguiente disponible).
- Escala: bono con close 87.600 por 100 VN y trade 876 → factor 0,01 pasa; una relación sin candidato, como ×25 → `SCALE_SUSPECT`.
- Sesgo: una relación de 1,15 con factor 0,01 → `BIASED`, y la operación queda `SOURCE_BIASED`.
- 1:1 trades ↔ fact_trade_market.

## Criterios de aceptación
- [ ] ADR-008 con resultados reales del spike.
- [ ] `fact_trade_market` con 100.000 filas.
- [ ] Cobertura calculada, publicada y consistente con `quote_requests`.
- [ ] Pipeline no falla con proveedor caído (demostrado con `FileReplayProvider` vacío para un símbolo).

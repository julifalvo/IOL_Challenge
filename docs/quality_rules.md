# Catálogo de reglas de calidad

Principio: **Bronze preserva, Silver interpreta, nada se descarta.** Cada regla produce un código en `quality_flags`, un evento en `iol_ops.quality_events` (excepto `INFO` masivos, que solo cuentan en métricas) y una métrica en `pipeline_metrics`.

Severidad → `quality_status` (se toma la máxima):
- `CRITICAL`: la fila no es interpretable. Se conserva, no entra a métricas. Si hay >0, Gold = RED.
- `ERROR`: dato interpretable pero inconsistente. No entra a métricas por defecto.
- `WARNING`: anomalía con explicación de mercado posible. **Entra** a métricas y se puede filtrar.
- `INFO`: contexto.

Umbrales parametrizados en `src/iol_pipeline/config.py` (`QUALITY_PARAMS`), versión `rules_v1`.

| Código | Severidad | Regla | Acción | Contexto / argumento | Observado (Free Edition, CSV completo, 2026-11-01) |
|---|---|---|---|---|---|
| DQ001_SCHEMA_MISMATCH | CRITICAL | Columnas ≠ contrato `trades_v1` | Run `FAILED_SCHEMA`: no se escribe Bronze y el archivo queda en landing | Un cambio de esquema rompe todo lo de aguas abajo | 0 |
| DQ002_REQUIRED_NULL | CRITICAL | Nulo/vacío en cualquier campo | Marcar | Sin ID/fecha/símbolo no hay trazabilidad | 0 |
| DQ003_TS_UNPARSEABLE | CRITICAL | `fecha` no parsea ISO-8601 | Marcar | | 0 |
| DQ004_ID_CONFLICT | CRITICAL | Mismo `id_transaccion` con distinto `record_hash` | Conservar ambas versiones en Bronze, marcar en Silver, evento | Doble conteo o pérdida silenciosa = riesgo regulatorio | 0 |
| DQ005_NUMERIC_UNPARSEABLE | CRITICAL | cantidad/precio no numéricos o no representables sin cambiar el dato: cantidad con decimales, precio con más de 8, importe que no entra | Marcar; el valor queda en null | Redondear cambiaría el dato sin aviso | 0 |
| DQ006_NON_POSITIVE | ERROR | qty ≤ 0 o price ≤ 0 | Marcar | Una operación con cantidad o precio ≤0 no existe en BYMA | 0 |
| DQ007_DOMAIN_INVALID | ERROR | `tipoTran`∉{Compra,Venta}, `moneda`∉{ARS,USD}, `origen`∉ catálogo | Marcar; canal desconocido → `UNKNOWN` | | 0 |
| DQ008_MALFORMED_ROW | CRITICAL | Línea con más campos que el header (`extra_fields` no nulo) | Conservar la fila en Bronze; marcar en Silver | No se sabe si los valores están corridos: el precio podría estar en otra columna | 0 |
| DQ010_SATURDAY | WARNING | Fecha local = sábado | `effective_trade_date` = viernes previo hábil | Sin domingos ni Carnaval; lunes empieza ~10 UTC y sábado termina ~19 UTC → cola de la sesión del viernes [HIPÓTESIS, ADR-003] | 10.255 (local) / 12.498 (UTC) |
| DQ011_SUNDAY | ERROR | Fecha local = domingo | Marcar | No hay mecanismo de mercado que lo explique | 0 |
| DQ012_MARKET_HOLIDAY | ERROR | Fecha local = feriado bursátil (calendario en config) | Marcar | El dataset respeta feriados: un trade en feriado es anómalo | 0 |
| DQ013_OUTSIDE_SESSION | INFO | Hora local fuera de 11:00–17:00 | Solo métrica (no evento por fila) | Hora casi uniforme en 24 h: el componente horario no es confiable; usar granularidad diaria [ADR-003] | 70.409 (70,4%) |
| DQ014_OUTSIDE_CALENDAR | ERROR | Fecha local fuera del rango del calendario de feriados | `effective_trade_date` = null; marcar | Sin calendario no se puede asignar el día hábil. Un archivo nuevo con fechas fuera de rango obliga a ampliar `MARKET_HOLIDAYS` | 0 |
| DQ020_EXTREME_AMOUNT | WARNING | \|z robusto\| > 3,5 sobre log(gross_amount) por (asset_type, currency) | Marcar | MAD es robusto a la cola; umbral de Iglewicz–Hoaglin; por tipo de activo porque un BONCAP y un CEDEAR no son comparables | 415 (0,42%) |
| DQ021_POSSIBLE_SCALE_ERROR | ERROR (WARNING en `OPCION`) | price / mediana del símbolo < 0,02 o > 50 | Marcar; excluir de métricas de importe y de comparación con mercado | Ratio ~1/100 sugiere unidad mal cargada (T30A7 a 0,0101 vs mediana del símbolo 1,08). Precios ≈1,00 o 0,01 en IOLnet sugieren valor por defecto. En opciones la prima varía legítimamente ×50, por eso ahí es WARNING | 13 filas (8 IOLnet, 5 opciones) |
| DQ022_EXTREME_QTY | WARNING | qty > p99,9 del asset_type | Marcar | Cantidades grandes son normales en bonos (VN); por eso no se usa qty sola para outliers | 107 (0,11%) |
| DQ030_INSTRUMENT_UNMAPPED | WARNING | Símbolo sin fila en `instrument_mapping`. Si la tabla no existe todavía, no aplica | Marcar; `instrument_sk` = UNKNOWN | La operación existe aunque no sepamos clasificarla | 0 |
| DQ031_SYMBOL_MULTI_DESC | INFO | Símbolo con >1 descripción | Elegir `description_canonical` (la más frecuente) | AZN, IRS2W, X15Y6 | 3 símbolos, 63 filas |
| DQ032_AFTER_MATURITY | ERROR | effective_trade_date > maturity_date | Marcar | No se puede operar un título vencido | 0 |
| DQ033_CURRENCY_VARIANT_MISMATCH | ERROR | settlement_variant USD con moneda ARS o viceversa | Marcar | Detecta mapeos erróneos tipo YPFD | 0 |

## Cómo se calculan las estadísticas de DQ020, DQ021 y DQ022

- **Población:** filas interpretables: todos los campos presentes, fecha y números válidos, moneda dentro del dominio, cantidad y precio positivos, sin línea mal formada y sin conflicto de ID.
- **Grupos:** DQ020 por (tipo de activo, moneda); DQ021 por (símbolo, moneda); DQ022 por tipo de activo. Ningún grupo mezcla monedas para calcular importes o precios.
- **Se calculan sobre todo el período** y se guardan en `iol_ops.quality_baselines`. Por eso un archivo nuevo puede cambiar marcas de filas anteriores: Silver las actualiza y los eventos que ya no corresponden quedan con `is_active = false`. Sirven para detectar anomalías, no para valorar operaciones, así que esto no es look-ahead de precios.
- **`MAD = 0`:** DQ020 no marca, porque sin dispersión el z robusto no está definido.
- **Límites medidos** (Spark local, CSV completo, 2026-11-01):
  - DQ020 es bilateral: 372 importes por el lado alto y 43 por el bajo.
  - DQ021 casi no puede detectar nada con pocas filas: 357 de 1.445 símbolos tienen 2 filas o menos.
  - Los grupos por tipo usan la clasificación por reglas, sin IA: 10 símbolos y 371 operaciones quedan juntos en `NEEDS_REVIEW` (ADR-004).

## Reglas del enriquecimiento (no cambian `quality_status` de Silver, viven en `fact_trade_market.match_method`)

| Código | Significado |
|---|---|
| EXACT_DAY | Cotización del `effective_trade_date` |
| PRIOR_DAY_IMPUTED | Último cierre previo dentro de `MAX_QUOTE_AGE_BDAYS` (ADR-009) |
| UNAVAILABLE | Sin cotización dentro de la ventana |
| UNMAPPED | Sin `provider_symbol` |
| CURRENCY_MISMATCH | Moneda del proveedor ≠ moneda del trade |
| SCALE_SUSPECT | Ratio mediano trade/close del símbolo fuera de [0,5; 2] → mapeo o escala dudosa |

## Reconciliación (invariantes que se testean)

- `count(bronze.trades_raw distinct id+hash) == count(silver.trades) == count(gold.fact_trades) == count(gold.fact_trade_market)`
- Toda fila con `quality_status != 'OK'` tiene ≥1 flag; toda fila con flags no-INFO tiene ≥1 evento.
- `rows_inserted + rows_already_present + rows_duplicate_in_file == rows_read` en cada `ingestion_runs`. `rows_conflict` es un subconjunto de `rows_inserted` (ADR-002).

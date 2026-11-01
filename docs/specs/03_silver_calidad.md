# Spec 03 — Silver: tipado, normalización y reglas de calidad (Task 02)

**Requisitos:** R-08, R-09, R-10 · **ADR:** 003, 005 · **Catálogo:** `docs/quality_rules.md`

## Diseño
- Entrada: `iol_bronze.trades_raw` completa (o filtrada por `ingestion_run_id` en modo incremental).
- Salida: `iol_silver.trades`, **1:1 con Bronze**. `MERGE` por `(trade_id, record_hash)`, que actualiza solo si cambia alguna columna, salvo la corrida y la fecha de proceso: reaplicar es idempotente, una versión nueva de una operación también marca `DQ004` en la fila que ya estaba, y un cambio de calendario llega a las filas existentes.
- Tipado con `try_cast` / `try_to_timestamp` (nunca excepción por fila).
- Normalización: trim, `side` (Compra→BUY, Venta→SELL), `channel_code` (catálogo cerrado de 5 + UNKNOWN), `currency_code`.
- Fechas: con `calendar.with_trade_dates` (ya implementada): `trade_ts_utc`, `trade_hour_utc`, `trade_date_utc`, `trade_ts_local` (`TIMESTAMP_NTZ`, con `convert_timezone`: `from_utc_timestamp` + `to_date` depende de la zona de la sesión), `trade_date_local`, `effective_trade_date` (ADR-003: sábado → viernes hábil previo; si viernes fue feriado, el hábil previo a ese; null fuera del calendario, `DQ014`).
- Reglas: `quality.row_rules()` da la condición de cada código; `quality_flags = filter(array(...), x -> x is not null)`; `quality_status` por severidad máxima (mapa en config).
- Reglas que necesitan estadísticas (DQ020, DQ021, DQ022) usan agregados calculados en el mismo run sobre toda la población elegible y **persistidos** en `iol_ops.quality_baselines` (mediana, MAD, p99,9 por grupo, `rules_version`). Así el run es reproducible y explicable.
- DQ020/DQ022 necesitan `asset_type`: Silver hace un join con la versión de reglas determinísticas de clasificación (spec 04, `classify_by_rules`) — no espera a la IA.
- `quality_events`: una fila por (trade_id, versión, flag) con severidad ≥ WARNING, con `is_active`: un evento cuya marca desaparece no se borra, queda inactivo, `details_json` con los valores que dispararon la regla (ej. `{"price":0.0101,"symbol_median":1.25,"ratio":0.008}`).

## Archivos
`src/iol_pipeline/calendar.py`, `src/iol_pipeline/quality.py`, `src/iol_pipeline/silver.py`, `notebooks/03_quality_silver.py`, `tests/unit/test_quality_rules.py`, `tests/unit/test_calendar.py`.

## Tests (con el fixture de spec 00)
- Cada regla: un caso positivo y uno negativo.
- Sábado 2026-01-10 → `effective_trade_date = 2026-01-09`, flag DQ010.
- Martes de Carnaval → DQ012.
- Precio 0,0101 en BONCAP con mediana 1,25 → DQ021, status ERROR.
- Fila sin problemas → `quality_flags = []`, `OK`.
- `count(silver) == count(bronze)`.
- Reaplicar con misma `rules_version` → 0 updates.

## Salida visible del notebook
1. Tabla: regla, severidad, filas afectadas, % del total, y como ejemplo un prefijo de `record_hash`: trazable a Bronze sin publicar IDs.
2. Distribución de `quality_status`.
3. Reconciliación Bronze vs Silver.
4. 3 ejemplos de `quality_events` con `details_json`.

## Criterios de aceptación
- [ ] 100.000 filas en Silver, 0 CRITICAL.
- [ ] Cada regla del catálogo implementada con test, salvo `DQ030` y `DQ033`, que necesitan `instrument_mapping` y pasan al spec 04.
- [ ] Columna "Observado" de `docs/quality_rules.md` completada con números del output.
- [ ] Reejecución sin cambios en conteos ni hashes.

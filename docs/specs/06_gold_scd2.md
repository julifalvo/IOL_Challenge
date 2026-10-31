# Spec 06 — Modelo dimensional Gold y SCD2 de clientes (Task 03, 15 pts)

**Requisitos:** R-11..R-15 · **ADR:** 006, 007 · **Contrato:** `docs/data_model.md`

## Dimensiones simples
- `dim_date`: generada (2025-12-01..2026-03-31) con calendario de config; `is_partial_period` para marzo (datos hasta 13/03).
- `dim_channel`, `dim_currency`: catálogos cerrados + `UNKNOWN` (sk = -1).
- `dim_instrument`: desde `iol_ops.instrument_mapping` (spec 04), SCD1 con `MERGE`.

## SCD2 `dim_client` — criterio (ADR-007)
El CSV no trae atributos de cliente. Se versionan **atributos operativos derivados**, documentados como tales:

| Atributo | Regla (acumulado hasta el snapshot) | Por qué |
|---|---|---|
| `activity_segment` | `cum_trades` 1 → OCASIONAL · 2–3 → RECURRENTE · 4–9 → ACTIVO · ≥10 → MUY_ACTIVO | Cortes en p75=2, p90≈3–4, p99=10 de la distribución observada. 40.841 clientes (71%) con una operación: un esquema por cuartiles colapsaría todo en un segmento; por eso los cortes van en la cola |
| `preferred_channel` | `channel_code` con más operaciones acumuladas hasta el corte. Desempate determinista: 1) canal de la operación más reciente (mayor `trade_ts_utc` ≤ corte) entre los empatados; 2) orden alfabético ascendente de `channel_code` | 1.741 clientes multicanal → cambios reales |
| `dominant_asset_type` | `asset_type` modal (reglas spec 04) | Perfil inversor sin inventar datos personales |
| `currency_profile` | ARS_ONLY / USD_ONLY / MIXED | Relevante para dolarización de cartera |

- Los cortes se **congelan** en `SEGMENT_RULE_VERSION = seg_v1` (calculados una vez y escritos en config). Recalcularlos en cada run haría que un cliente cambie de segmento sin haber cambiado su comportamiento.
- **Sin leakage:** la versión calculada al cierre del mes M rige desde M+1 día 1. La versión inicial de un cliente nace en su primera operación con atributos calculados solo con esa operación (`activity_segment = OCASIONAL`).

## Mecánica
Snapshots: `2026-01-31`, `2026-02-28`, `2026-03-13` (corte de datos). Se procesan **en orden con la misma función** que correría en producción:

```
apply_scd2(current_dim, snapshot_df, snapshot_effective_date):
  1. hashdiff = sha2(concat_ws('|', activity_segment, preferred_channel, dominant_asset_type, currency_profile))
  2. nuevos clientes → INSERT versión (valid_from = first_trade_date, valid_to = 9999-12-31, is_current = true)
  3. clientes con hashdiff distinto al vigente → UPDATE vigente: valid_to = snapshot_effective_date, is_current = false
                                                 → INSERT nueva versión: valid_from = snapshot_effective_date
  4. hashdiff igual → no-op (idempotencia)
```
Implementación: un solo `MERGE` con el patrón de "staged updates" (unión de filas con `merge_key = client_id` y filas con `merge_key = NULL` para forzar el insert de la versión nueva). `valid_to` **exclusivo**.

## `fact_trades`
- Join point-in-time: `effective_trade_date >= valid_from AND effective_trade_date < valid_to` → `client_sk`.
- Incluye las 100.000 filas (incluso ERROR) con `is_analytics_eligible`.
- `MERGE` por `trade_id`.

## Demostración (celda obligatoria del notebook 06)
1. Elegir con query un cliente real con ≥3 versiones (preferentemente cambio de segmento + cambio de canal).
2. Mostrar sus versiones (`valid_from`, `valid_to`, `is_current`, atributos).
3. Mostrar sus trades con el `client_sk` asignado y el segmento vigente en cada fecha.
4. Mostrar que reejecutar el snapshot de marzo no crea versiones nuevas.
5. Conteo de clientes por número de versiones y matriz de transición de `activity_segment` ene→feb→mar.

## Tests (`tests/unit/test_scd2.py`)
- Cliente nuevo → 1 versión vigente.
- Cambio de atributo → versión anterior cerrada con `valid_to = fecha efectiva`, nueva vigente.
- Sin cambio → 0 filas nuevas.
- Invariantes: exactamente 1 `is_current` por cliente; sin solapamiento; sin huecos entre versiones; cada trade matchea exactamente 1 versión.
- Trade del 15/02 usa la versión del snapshot de enero, no la de febrero (no leakage).
- `preferred_channel`:
  - con empate en cantidad gana el canal de la operación más reciente;
  - con empate en cantidad y en marca de tiempo gana el primero alfabético;
  - el resultado no cambia al permutar el orden de las filas de entrada.

## Criterios de aceptación
- [ ] Todas las tablas Gold creadas y documentadas en `docs/data_model.md`.
- [ ] Demostración con cliente real y outputs.
- [ ] Gates G05 y G06 en verde.
- [ ] ADR-006 y ADR-007 completos (incluye por qué no SCD2 en instrumentos: SCD1 + `mapping_version`).

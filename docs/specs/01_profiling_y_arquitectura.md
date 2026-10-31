# Spec 01 — Profiling reproducible y arquitectura (Task 01, 20 pts)

**Requisitos:** R-01..R-05 · **ADR:** 001–012 (todas las decisiones nacen o se referencian acá)

## Parte A — Notebook `01_data_profiling`

Objetivo: que cada decisión de diseño tenga una celda con evidencia. Recalcula en Spark lo que hoy está en `scripts/profile_local.py`.

Celdas obligatorias (cada una termina con un markdown "→ Decisión: ADR-xxx"):
1. Conteos generales, nulos por columna, duplicados de ID, rango temporal (→ marzo parcial).
2. Distribución por día de semana (UTC y local), días hábiles sin operaciones (→ Carnaval = 0, confirma calendario).
3. Histograma de hora UTC por día de semana; mostrar que lunes arranca ~10 UTC y sábado corta ~19 UTC (→ ADR-003).
4. Canales: conteos, clientes multicanal (→ canal en el hecho, `preferred_channel` en SCD2).
5. Clientes: percentiles de operaciones por cliente, % de una sola operación (→ ADR-007).
6. Instrumentos: categorías por descripción, símbolos USD, sufijos D/C en ARS (`YPFD`, `C`, `BA.C`, `BA37D`), símbolos con >1 descripción (→ ADR-004).
7. Precios: AL30 vs AL30D vs AL30C, GD30/GD30D; MEP implícito; escala por 1 VN (→ ADR-005).
8. Extremos: top 10 por qty y por importe, por canal y tipo; caso T30A7 vs otras BONCAP (→ DQ021).
9. Operaciones por día hábil por mes (normalizar por días hábiles observados: ene 21, feb 18, mar 10).
10. **Viabilidad de SCD2 (antes de construirlo en spec 06)** (→ ADR-007). Objetivo: medir si, con la definición completa de ADR-007, hay cambios reales de atributos para demostrar el SCD2 con datos del dataset y no con un fixture. **No hay valores esperados:** la celda calcula e imprime.
    - Definición, tal como está en ADR-007:
      - versión inicial en la primera operación del cliente, con atributos de esa sola operación;
      - cortes acumulados al 31/01, 28/02 y 13/03, por fecha local efectiva;
      - `activity_segment` con los cortes congelados `seg_v1`;
      - `preferred_channel` = `channel_code` con más operaciones acumuladas hasta el corte. Desempate: 1) canal de la operación más reciente entre los empatados, 2) alfabético.
    - La celda imprime primero la definición aplicada y la `SEGMENT_RULE_VERSION`, después:
      - (a) Cantidad de clientes con ≥2 versiones bajo la definición completa (versión inicial + 3 cortes), con la distribución del número de versiones por cliente.
      - (b) Cantidad de clientes que cambian de `preferred_channel` entre cortes: inicial→31/01, 31/01→28/02 y 28/02→13/03, entre clientes que existen en ambos puntos.
      - (c) Matriz de transición de `activity_segment` a lo largo de los cortes (apoyo a la lectura de a).
    - **Criterio de revisión:**
      - si (b) da 0, se replantea `preferred_channel` en ADR-007;
      - si (a) da muy poco, también se replantea ADR-007 (segmentos o cortes).
      - Se decide con el output a la vista y **antes** de implementar el spec 06.

## Parte B — `ARCHITECTURE.md`

Escrito para un equipo que mantiene sin vos. Secciones:
1. **Contexto y objetivos no funcionales** (trazabilidad, idempotencia, reproceso, costo).
2. **Diagrama de flujo** (Mermaid): landing Volume → Bronze → Silver → mapping/IA → quotes → Gold → gates → consumo; tablas `iol_ops` al costado.
3. **Capas y contratos**: link a `docs/data_model.md`.
4. **Modelo de datos y por qué estrella** (ADR-006): granularidad, SCD2 vs snapshot, por qué `fact_trade_market` separado de `fact_trades` (distinto ciclo de vida: las cotizaciones se re-bajan sin tocar trades).
5. **Estrategia de escalabilidad**:
   - Ingesta incremental: hoy CSV único con `MERGE`; productivo → Auto Loader (`cloudFiles`) o `COPY INTO` sobre landing por fecha, checkpoint por archivo, mismo `record_hash`.
   - Particionado/clustering: liquid clustering por `effective_trade_date_key, instrument_sk`; no particionar por fecha a este volumen (archivos chicos).
   - SCD2 incremental: snapshot diario sobre clientes con actividad del día (no full recompute).
   - Cotizaciones: caché en Delta, solo se piden (símbolo, fecha) faltantes.
   - Reprocesamiento: por `ingestion_run_id` / rango de fechas.
   - Volumen supuesto: [SUPUESTO] documentar escenario (ej. 100k/trimestre hoy; escenario productivo 50k–500k operaciones/día). Pedir a IOL el volumen real en la entrevista.
6. **Comparativa de cómputo** (ADR-010) — ver Parte C.
7. **Operación**: cómo correr, cómo reprocesar, cómo agregar un símbolo al mapeo, qué hacer ante YELLOW/RED.
8. **Limitaciones conocidas.**

## Parte C — Benchmark y comparativa de cómputo (R-05)

1. Script `notebooks/90_benchmark.py`:
   - Genera datasets sintéticos 10x y 100x replicando el CSV con nuevos `trade_id` (`concat(id, '_r', n)`), `client_id` desplazados y fechas preservadas. Se escriben en schema `iol_bench`.
   - Corre Silver + Gold (sin cotizaciones externas: usa `fact_market_quotes` existente) y mide duración por etapa, filas, bytes leídos/escritos (`DESCRIBE HISTORY` → `operationMetrics`).
   - Persiste en `iol_ops.benchmark_results`.
2. Tabla en ADR-010 con 3 configuraciones productivas:

| Config | Descripción | Cuándo conviene | Riesgos |
|---|---|---|---|
| A. Serverless Jobs compute | Pay-per-use, arranque en segundos, sin idle, Photon incluido | Batch diario corto o variable, equipo chico sin ops de infra | Costo por DBU mayor; menos control de configuración |
| B. Job cluster clásico chico (1 driver + 2 workers, workers spot, Photon off) | Cluster efímero por job | Volumen moderado y estable, sensibilidad a costo | Arranque de minutos; interrupción de spot; tuning manual |
| C. Job cluster clásico con Photon + autoscaling (2–8 workers) | Efímero, escalado | Reproceso histórico, 100x, joins/aggregations pesados | Photon cobra más DBU/h; conviene solo si reduce tiempo más de lo que encarece |
| (+) SQL Warehouse serverless para consumo | Separado del pipeline | Analistas sobre Gold | No mezclar costo de ingesta con costo de consulta |

3. Fórmula de costo explícita: `costo_run = DBU/h × horas × $/DBU (+ costo VM en clásico)`. Usar **precios de lista vigentes con fecha de consulta** y link. Distinguir `[MEDIDO]` (duración en Free Edition a 1x/10x/100x) de `[ESTIMADO]` (extrapolación a A/B/C).
4. Recomendación con regla de decisión: ej. "A mientras el job dure < N min/día; C para reprocesos > 10x".

## Criterios de aceptación
- [ ] Notebook 01 con las 10 celdas y outputs.
- [ ] Celda 10 (viabilidad SCD2) con (a) y (b) impresos bajo la definición de ADR-007. Si (b) = 0 o (a) es muy bajo, ADR-007 revisado antes del spec 06.
- [ ] `ARCHITECTURE.md` completo; todo número citado viene de un output.
- [ ] Benchmark 1x/10x/100x medido y persistido.
- [ ] ADR-010 con tabla, fórmula, fuentes de precio fechadas y recomendación.

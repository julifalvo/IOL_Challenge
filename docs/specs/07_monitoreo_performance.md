# Spec 07 — Observabilidad, quality gates y performance (Task 05, 6 pts)

**Requisitos:** R-23..R-25, R-38 · **Docs:** `docs/quality_gates.md` · **ADR:** 012

## Monitoreo
- `src/iol_pipeline/monitoring.py`:
  - `record_metric(run_id, stage, name, value, threshold=None, comparator=None)` → `iol_ops.pipeline_metrics` (MERGE por run+stage+name, idempotente).
  - `stage(run_id, name)` context manager → `iol_ops.pipeline_runs` con inicio, fin, estado, filas in/out, error.
  - `evaluate_gates(run_id) -> GateResult` → implementa G01..G13; escribe `iol_ops.gold_status`.
- Cada notebook registra sus métricas al final (filas, flags, cobertura, duración).
- Notebook `07_quality_gate_monitoring`:
  1. Tabla de gates con estado coloreado (`displayHTML` o DataFrame con columna estado).
  2. Estado global y motivos.
  3. Métricas por etapa del run actual vs run anterior (detecta drift y demuestra idempotencia).
  4. `DESCRIBE DETAIL` de todas las tablas → formato delta, numFiles, sizeInBytes.
- Opcional si Free Edition lo permite: dashboard AI/BI sobre `pipeline_metrics` y `gold_status` (screenshot al README).

## Performance — "aplicar las que correspondan" con criterio medido
Tablas de mayor acceso: `fact_trades`, `fact_trade_market` (todas las q1..q5 las leen).

| Optimización | Dónde | Criterio |
|---|---|---|
| Liquid clustering `CLUSTER BY (effective_trade_date_key, instrument_sk)` | fact_trades, fact_trade_market | Filtros por período e instrumento en q1/q2/q3; liquid clustering evita elegir particiones a mano y se adapta al crecimiento; particionar por fecha a 100k filas generaría archivos chicos |
| `CLUSTER BY (client_id)` | dim_client | Join point-in-time y búsquedas por cliente |
| `OPTIMIZE` tras cargas | tablas Gold | Compacta archivos de MERGEs repetidos |
| `ANALYZE TABLE ... COMPUTE STATISTICS FOR ALL COLUMNS` | Gold | Mejora planes de join |
| Broadcast implícito de dimensiones | joins | Dimensiones < 10 MB; verificar en el plan, no forzar hints salvo necesidad |
| No aplicar | Z-ORDER + particiones, cache | Liquid clustering lo reemplaza; cache no disponible en serverless |

**Medición (obligatoria):** para q1 y q2, antes y después de clustering+OPTIMIZE sobre el dataset 100x (spec 01 Parte C): duración, `numFiles` leídos/podados (de la query profile o `EXPLAIN FORMATTED`). Registrar en ADR-012 como `[MEDIDO]`. Si a 1x no hay diferencia, decirlo: es el resultado esperado y es el argumento de por qué la optimización se justifica recién a escala.

## Tests
- `evaluate_gates` con métricas sintéticas: cada gate dispara RED/YELLOW en su caso.
- Gate RED → notebook 08 lanza excepción con motivo.

## Criterios de aceptación
- [ ] Gates evaluados con outputs visibles; estado del run final en README.
- [ ] Before/after medido a 100x.
- [ ] ADR-012 con criterio y resultados.

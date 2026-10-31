# Spec 10 — Orquestación y tests (Bonus +5)

**Requisitos:** R-31, R-32

## Orquestador `notebooks/00_run_pipeline.py`
- Widgets: `run_mode` (`full` | `from_stage`), `start_stage`, `catalog`, `quotes_source` (`api` | `volume`).
- Genera `pipeline_run_id` (UUID) y lo pasa a cada hijo.
- Secuencia: `01_data_profiling` (opcional, flag) → `02_ingest_bronze` → `03_quality_silver` → `04_instruments` → `09_ai_enrichment` (componente A) → `05_market_quotes` → `06_model_gold` → `07_quality_gate_monitoring` → `08_business_eda_sql` → `09b_ai_eda_summary`.
- Cada hijo termina con `dbutils.notebook.exit(json.dumps({"status": ..., "rows_out": ..., "metrics": ...}))`.
- `dbutils.notebook.run(path, timeout_seconds, {"pipeline_run_id": ..., ...})` con timeout por etapa; si un hijo falla o devuelve `FAILED` → registrar en `pipeline_runs` y abortar. Si el gate devuelve `RED` → no ejecutar 08/09b.
- Output final: tabla de etapas con duración y estado + estado de Gold.
- Fallback documentado: `%run` si `dbutils.notebook.run` no estuviera disponible.

## Tests
Locales, sin cluster (`pytest`, Spark local + delta-spark):

| Archivo | Cubre |
|---|---|
| `tests/unit/test_bronze.py` | hash, lectura, contrato de esquema |
| `tests/unit/test_calendar.py` | effective_trade_date, feriados, sábados |
| `tests/unit/test_quality_rules.py` | cada regla positivo/negativo, severidad → status |
| `tests/unit/test_instruments.py` | reglas de asset_type, variantes, falsos positivos de sufijo, overrides |
| `tests/unit/test_quote_matching.py` | exact/imputed/unavailable, no look-ahead, escala |
| `tests/unit/test_quote_provider.py` | reintentos, error por símbolo no corta el lote (provider fake) |
| `tests/unit/test_scd2.py` | inserción, cierre, no-op, invariantes, no leakage |
| `tests/unit/test_gates.py` | cada gate dispara en su caso |
| `tests/unit/test_ai_validation.py` | schema JSON, números citados existen en input |
| `tests/integration/test_idempotency.py` | pipeline completo sobre fixture 2 veces → mismas tablas (conteos + checksum) |
| `tests/integration/test_degradation.py` | proveedor caído para un símbolo → pipeline termina, cobertura refleja el faltante |
| `tests/integration/test_reconciliation.py` | Bronze = Silver = fact_trades = fact_trade_market |

- `pytest --junitxml=reports/pytest.xml` y copiar resumen al README.
- Opcional: GitHub Actions corriendo `pytest` en cada push (badge en README). Es gratis y demuestra que los tests corren sin cluster.

## Criterios de aceptación
- [ ] Orquestador corre end-to-end en Free Edition con output visible.
- [ ] Reejecución completa: mismos conteos (evidencia en `pipeline_metrics`).
- [ ] `pytest` verde; reporte en README.

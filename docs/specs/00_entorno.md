# Spec 00 — Entorno y scaffolding

**Requisitos:** R-37, R-38 · **ADR:** 001, 008, 011

## Objetivo
Dejar el repo y el workspace listos para que cualquier etapa se pueda desarrollar localmente y correr en Databricks Free Edition sin fricción.

## Tareas
1. Repo: estructura de `CLAUDE.md`, `.gitignore`, `requirements*.txt`, `pyproject.toml`.
2. `src/iol_pipeline/config.py`:
   - `CATALOG`, nombres de schemas, rutas de Volume.
   - `QUALITY_PARAMS` (umbrales de `docs/quality_rules.md`), `GATE_PARAMS` (de `docs/quality_gates.md`).
   - `MARKET_CALENDAR`: feriados bursátiles ene–mar 2026 (al menos 1/1, 16/2, 17/2; verificar contra calendario BYMA y documentar la fuente).
   - `SESSION_HOURS_LOCAL = ("11:00","17:00")`.
   - `MAX_QUOTE_AGE_BDAYS = 3`, `ABOVE_MARKET_TOLERANCE = 0.005` (ADR-009).
   - `RULES_VERSION = "rules_v1"`, `SEGMENT_RULE_VERSION = "seg_v1"`, `MAPPING_VERSION = "map_v1"`.
   - Detección de entorno: `is_databricks()` (env `DATABRICKS_RUNTIME_VERSION`) → mismo código usa tablas UC o paths locales.
3. `src/iol_pipeline/io.py`: helpers `table_name(layer, name)`, `write_delta_merge(...)`, `ensure_schemas(spark)`.
4. `tests/conftest.py`: SparkSession local con Delta (fixture de sesión), schemas temporales, CSV de fixture chico (`tests/fixtures/trades_small.csv`, ~40 filas con casos: sábado, feriado, escala, YPFD, AL30/AL30D/AL30C, ID repetido con mismo y distinto contenido).
5. Notebook `notebooks/00_setup.py`: crea schemas y Volume (`CREATE ... IF NOT EXISTS`), imprime versión de runtime, verifica que el CSV esté en el Volume.

## Smoke test en Free Edition (registrar resultados en ADR-001 / ADR-008 / ADR-011)
- `dbutils.notebook.run("child", 60)` funciona.
- `requests.get` a cada proveedor candidato (spec 05).
- `SELECT ai_query('<modelo>', 'ping')` (spec 09).
- `CREATE TABLE ... CLUSTER BY (...)`.

## Criterios de aceptación
- [ ] `pytest -q` corre localmente (aunque solo haya un test de humo de Spark+Delta).
- [ ] Notebook 00 corre en Free Edition sin errores y deja los schemas creados (output visible).
- [ ] El fixture cubre todos los casos listados.
- [ ] Resultado del smoke test anotado en los ADR correspondientes.

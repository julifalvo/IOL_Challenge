# ADR-001 — Databricks Free Edition como entorno de ejecución

- **Estado:** propuesto (pendiente respuesta de IOL — `docs/plan/consulta_iol.md`)
- **Requisitos:** R-37, R-38

## Contexto
El enunciado exige Databricks Community Edition. Databricks retiró Community Edition el 1/1/2026; su reemplazo es Free Edition: gratuito, solo cómputo serverless, Unity Catalog, salida a internet restringida a dominios confiables.

## Decisión
Ejecutar en Free Edition respetando el espíritu de la restricción: costo cero, sin Databricks Jobs, sin pipelines declarativos (DLT/Lakeflow), Delta en todas las tablas, orquestación con `dbutils.notebook.run()`.

## Implicancias técnicas
- Serverless: sin RDD, sin `sparkContext`, sin cache/persist, sin config de cluster → la lógica se escribe con DataFrame API/SQL.
- Almacenamiento: tablas managed en UC + Volumes (no DBFS root).
- Egress restringido → ADR-008 (fetcher externo como plan B).
- La comparativa de cómputo (ADR-010) se mide en serverless y se extrapola a configuraciones productivas.

## Resultado del smoke test
- **Cómo se obtiene:** corriendo `notebooks/00_setup.py` y `notebooks/00_smoke_test.py`. El detalle queda en `iol_ops.smoke_test_results`, filtrado por `smoke_run_id`.
- **Estado:** ejecutado el 2026-11-01. `00_setup` sin errores; `00_smoke_test` con 9/9 chequeos OK, `smoke_run_id = 1cc453e0-7d65-4a74-b2c2-4387cc92ec8d`.

| Prueba | Resultado | Fecha |
|---|---|---|
| Crear schema + volume | Schemas: OK [MEDIDO], `SHOW SCHEMAS` de `00_setup` lista `iol_bronze`, `iol_gold`, `iol_ops`, `iol_silver`. Volume: OK [MEDIDO], `SHOW VOLUMES` lista `iol_bronze.landing`. CSV en `landing/trades/`: OK [MEDIDO], `data-set-challenge-5-.csv` de 12.242.003 bytes (mismo tamaño que el del profiling local), header = contrato `trades_v1` | 2026-11-01 |
| Git folder + import de `src/` | OK [MEDIDO]: la celda que muestra `SHOW SCHEMAS` llama antes a `io.ensure_schemas`, importado desde `src/` | 2026-11-01 |
| `dbutils.notebook.run` | OK [MEDIDO]: el hijo devolvió `{"status": "SUCCESS", "echo": "<smoke_run_id>"}` | 2026-11-01 |
| Egress a proveedores | Los 4 dominios responden; Yahoo con HTTP 429. Detalle en ADR-008 | 2026-11-01 |
| `ai_query` | OK [MEDIDO] con `databricks-meta-llama-3-3-70b-instruct`. Detalle en ADR-011 | 2026-11-01 |
| Liquid clustering | OK [MEDIDO]: `CREATE TABLE ... CLUSTER BY (d)` y `DESCRIBE DETAIL` devuelve `clusteringColumns=['d']` | 2026-11-01 |
| `write_delta_merge` idempotente en UC | OK [MEDIDO]: 1ra corrida 8 insertadas; 2da corrida 0 insertadas, 8 ya presentes | 2026-11-01 |

## Cuándo revisar
Si IOL indica otro entorno, o si Free Edition cambia sus límites durante el challenge.

# ADR-001 — Databricks Free Edition como entorno de ejecución

- **Estado:** aceptado (2026-11-01)
- **Requisitos:** R-37, R-38

## Contexto
El enunciado exige Databricks Community Edition. Databricks retiró Community Edition el 1/1/2026; su reemplazo es Free Edition: gratuito, solo cómputo serverless, Unity Catalog, salida a internet restringida a dominios confiables.

## Decisión
Ejecutar en Free Edition respetando el espíritu de la restricción: costo cero, sin Databricks Jobs, sin pipelines declarativos (DLT/Lakeflow), Delta en todas las tablas, orquestación con `dbutils.notebook.run()`.

## Por qué no se espera confirmación de IOL
No se envió consulta a IOL (decisión del 2026-11-01). Community Edition ya no existe, así que el texto literal de R-37 no se puede cumplir, y Free Edition es su reemplazo gratuito. El desvío se declara en el README.

## Implicancias técnicas
- Serverless: sin RDD, sin `sparkContext`, sin cache/persist, sin config de cluster → la lógica se escribe con DataFrame API/SQL.
- Almacenamiento: tablas managed en UC + Volumes (no DBFS root).
- Egress restringido → ADR-008 (fetcher externo como plan B).
- La comparativa de cómputo (ADR-010) se mide en serverless y se extrapola a configuraciones productivas.

## Runtime: Databricks y local no son iguales
| | Spark | Python | Origen |
|---|---|---|---|
| Free Edition serverless (`client.6.5`) | 4.2.0 | 3.12.3 | [MEDIDO] `notebooks_executed/00_setup.ipynb`, 2026-11-01 |
| Tests locales | pyspark 4.0.5 + delta-spark 4.0.1 | 3.11.9 | [MEDIDO] `PROGRESS.md`, 2026-10-31 |

- Los tests locales validan la lógica, no el comportamiento específico de Spark 4.2. Lo que dependa del motor se confirma corriendo el notebook en Free Edition.
- `write_delta_merge` se probó en ambos: tests locales y smoke test.
- PyPI ya ofrece pyspark 4.2.x y delta-spark 4.2.0 (`pip index versions`, 2026-11-01). Alinear el entorno local **no se probó**, ni si esas dos versiones son compatibles entre sí.

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

## Qué falta medir en Free Edition
El smoke test no cubre todo lo que pide el challenge. Estos puntos siguen **no ejecutados**:
- Que los endpoints de datos de cotizaciones devuelvan series para los símbolos del challenge (spike de ADR-008). El smoke solo pidió la raíz de cada dominio.
- Que el benchmark 100x entre en los límites de cómputo de Free Edition (ADR-010).
- `ALTER TABLE ... CLUSTER BY` sobre una tabla existente. El smoke probó `CREATE TABLE ... CLUSTER BY`.

## Cuándo revisar
Si IOL indica otro entorno, o si Free Edition cambia sus límites durante el challenge.

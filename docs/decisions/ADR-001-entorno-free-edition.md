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
- **Estado:** ejecutado el 2026-11-01, en dos corridas. La vigente, con los notebooks corregidos: `00_setup` con `status: SUCCESS`; `00_smoke_test` con 10 de 11 chequeos OK, 1 inconcluso y ninguno fallido, `smoke_run_id = d96b79bc-649b-483d-b9ca-aa87e463b212`. La primera (`1cc453e0-…`, 9/9) contaba el 429 de Yahoo como OK.

| Prueba | Resultado | Fecha |
|---|---|---|
| Crear schema + volume | Schemas: OK [MEDIDO], `SHOW SCHEMAS` de `00_setup` lista `iol_bronze`, `iol_gold`, `iol_ops`, `iol_silver`. Volume: OK [MEDIDO], `SHOW VOLUMES` lista `iol_bronze.landing`. CSV en `landing/trades/`: OK [MEDIDO], `data-set-challenge-5-.csv` de 12.242.003 bytes (mismo tamaño que el del profiling local), header = contrato `trades_v1` | 2026-11-01 |
| Git folder + import de `src/` | OK [MEDIDO]: la celda que muestra `SHOW SCHEMAS` llama antes a `io.ensure_schemas`, importado desde `src/` | 2026-11-01 |
| `dbutils.notebook.run` | OK [MEDIDO]: el hijo devolvió `{"status": "SUCCESS", "echo": "<smoke_run_id>"}` | 2026-11-01 |
| Egress a proveedores | IOL, BYMA y data912 con HTTP 200; la raíz de Yahoo con HTTP 429 (`INCONCLUSO`). Los endpoints de datos de BYMA, data912 y Yahoo devuelven series. Detalle en ADR-008 | 2026-11-01 |
| `ai_query` | OK [MEDIDO] con `databricks-meta-llama-3-3-70b-instruct`. Detalle en ADR-011 | 2026-11-01 |
| Liquid clustering | OK [MEDIDO]: con `CREATE TABLE ... CLUSTER BY (d)` y con `ALTER TABLE ... CLUSTER BY (d)`, `DESCRIBE DETAIL` devuelve `clusteringColumns=['d']` | 2026-11-01 |
| Región y catálogo por defecto | `aws:us-east-2`, catálogo `workspace` [MEDIDO] | 2026-11-01 |
| `write_delta_merge` idempotente en UC | OK [MEDIDO]: 1ra corrida 10 insertadas; 2da corrida 0 insertadas, 10 ya presentes | 2026-11-01 |

## Qué falta medir en Free Edition
Siguen **no ejecutados**:
- Que el benchmark 100x entre en los límites de cómputo de Free Edition (ADR-010).
- La descarga de cotizaciones para los 1.445 símbolos: el spike probó 16 (ADR-008).

## Cuándo revisar
Si IOL indica otro entorno, o si Free Edition cambia sus límites durante el challenge.

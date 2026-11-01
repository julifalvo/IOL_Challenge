# Spec 02 — Ingesta a Bronze idempotente (Task 02)

**Requisitos:** R-06, R-07, R-08 · **ADR:** 002

## Diseño
- Fuente: CSV en `/Volumes/{CATALOG}/iol_bronze/landing/trades/`.
- Lectura **todo como STRING**. Validar que las columnas = contrato `trades_v1` → si no, `DQ001`, registrar run `FAILED_SCHEMA`, no escribir. Una línea con más campos que el header entra con lo que sobra en `extra_fields`; un archivo sin filas → `FAILED_EMPTY`; comilla sin cerrar o codificación inválida → `FAILED` (ADR-002).
- `source_row_number`: Spark no garantiza orden de lectura y `zipWithIndex` (RDD) no está disponible en serverless. **Decisión:** a este volumen (~12 MB), leer el archivo desde el Volume con el módulo `csv` de Python (los Volumes se montan como filesystem), numerar filas (1 = primera fila de datos) y crear el DataFrame con `spark.createDataFrame`. No se usa pandas: corre columnas sin error si la primera fila trae campos de más (ADR-002). Documentar en ADR-002 que en el escenario productivo (Auto Loader) la trazabilidad de fila pasa a ser `_metadata.file_path + record_hash`, sin número de línea.
- `record_hash = sha2(concat_ws('\u001f', <10 columnas en orden del contrato>, extra_fields), 256)`. `extra_fields` es null casi siempre, y `concat_ws` lo saltea.
- `source_file_hash`: sha256 del archivo (Python `hashlib` leyendo desde el Volume).
- Dedupe intra-archivo: si un mismo `id_transaccion` aparece 2 veces con mismo hash → se inserta 1 vez, contar `rows_duplicate_in_file`; con hash distinto → ambas filas se insertan (la clave de MERGE es `id_transaccion + record_hash`) y se emite `DQ004` en Silver.

## MERGE
```sql
MERGE INTO iol_bronze.trades_raw t
USING staged s
ON t.id_transaccion = s.id_transaccion AND t.record_hash = s.record_hash
WHEN NOT MATCHED THEN INSERT *
```
- Mismo ID + mismo hash → no-op (`rows_already_present`).
- Mismo ID + hash distinto → nueva fila (conflicto preservado, `rows_conflict`).
- Nunca `WHEN MATCHED UPDATE` en Bronze.

## `iol_ops.ingestion_runs`
`ingestion_run_id, pipeline_run_id, rows_read, rows_inserted, rows_already_present, rows_conflict, rows_duplicate_in_file, duplicate_row_numbers, status (SUCCESS/FAILED_SCHEMA/FAILED_EMPTY/FAILED), error, source_file, source_file_hash, started_at, ended_at`. Cálculo de inserted/present con anti-join previo al MERGE (o `operationMetrics` del MERGE vía `DESCRIBE HISTORY`).

**Invariante (ADR-002):** `read = inserted + already_present + duplicate_in_file`. `conflict` es un subconjunto de `inserted`: las filas insertadas en la corrida cuyo ID ya tenía otra versión, en la tabla o antes en el mismo archivo.

## Archivos
`src/iol_pipeline/bronze.py` (`read_trades_csv`, `add_lineage`, `stage_for_merge`, `merge_bronze`, `ingestion_stats`), `notebooks/02_ingest_bronze.py`, `tests/unit/test_bronze.py`, `tests/integration/test_idempotency.py`.

## Tests
- Hash estable ante el mismo input; cambia si cambia cualquier columna.
- Correr ingesta 2 veces con fixture → `count` igual, segunda corrida `rows_inserted=0`.
- Fixture con ID repetido mismo contenido → 1 fila; distinto contenido → 2 filas + `rows_conflict=1`.
- Contadores con `trades_small.csv`, 1ra corrida: read 40, inserted 39, already_present 0, duplicate_in_file 1, conflict 1. 2da corrida: read 40, inserted 0, already_present 39, duplicate_in_file 1, conflict 0.
- Esquema con columna faltante → `FAILED_SCHEMA`, tabla intacta.

## Criterios de aceptación
- [ ] Notebook 02 muestra: `ingestion_runs` de 2 corridas consecutivas (100.000 insertadas / 0 insertadas + 100.000 presentes).
- [ ] `count(*) = 100000` tras ambas corridas.
- [ ] `DESCRIBE DETAIL` muestra formato delta.
- [ ] Tests verdes.

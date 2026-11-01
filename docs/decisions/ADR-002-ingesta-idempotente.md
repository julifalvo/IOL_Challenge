# ADR-002 — Ingesta Bronze idempotente por (id_transaccion, record_hash)

- **Estado:** propuesto
- **Requisitos:** R-06, R-07, R-08

## Contexto
La trazabilidad es requisito de negocio. Reprocesar el mismo archivo no puede duplicar. Un mismo ID podría reaparecer con contenido distinto (corrección en origen) y eso no se puede perder ni sobrescribir silenciosamente.

## Decisión
- Bronze guarda columnas originales **como texto** + linaje (`record_hash`, `source_file_hash`, `source_row_number`, `ingestion_run_id`).
- `MERGE ... WHEN NOT MATCHED THEN INSERT` con clave `(id_transaccion, record_hash)`. Nunca UPDATE en Bronze.
- Conflictos (mismo ID, distinto hash) quedan ambos en Bronze y se marcan `DQ004` en Silver.
- `iol_ops.ingestion_runs` cuadra `read = inserted + already_present + duplicate_in_file`. `conflict` es un subconjunto de `inserted`: las filas insertadas en la corrida cuyo ID ya tenía otra versión, en la tabla o antes en el mismo archivo.

## Alternativas
| Opción | Por qué no |
|---|---|
| `append` + dedupe en Silver | Bronze crece en cada reejecución; trazabilidad confusa |
| `MERGE` por `id_transaccion` con UPDATE | Pierde la versión anterior: sobrescritura silenciosa |
| Overwrite completo | Idempotente pero borra historia de ingestas; no escala a incremental |
| Bronze tipado | Un error de parseo rompería la ingesta; el tipado es responsabilidad de Silver |

## Escenario productivo
Auto Loader / `COPY INTO` por archivo en landing; misma clave de MERGE; trazabilidad de fila por `_metadata.file_path + record_hash` (sin número de línea).

## Evidencia
Notebook 02: dos corridas consecutivas → 100.000 / 0 insertadas. `[MEDIR]`

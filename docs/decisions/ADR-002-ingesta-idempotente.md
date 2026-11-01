# ADR-002 — Ingesta Bronze idempotente por (id_transaccion, record_hash)

- **Estado:** propuesto
- **Requisitos:** R-06, R-07, R-08

## Contexto
La trazabilidad es requisito de negocio. Reprocesar el mismo archivo no puede duplicar. Un mismo ID podría reaparecer con contenido distinto (corrección en origen) y eso no se puede perder ni sobrescribir silenciosamente.

## Decisión
- Bronze guarda columnas originales **como texto** + linaje (`record_hash`, `source_file_hash`, `source_row_number`, `ingestion_run_id`).
- `MERGE ... WHEN NOT MATCHED THEN INSERT` con clave `(id_transaccion, record_hash)`. Nunca UPDATE en Bronze.
- Conflictos (mismo ID, distinto hash) quedan ambos en Bronze y se marcan `DQ004` en Silver. Un ID vacío no es un conflicto: dos filas sin ID no son versiones de la misma operación, y las marca `DQ002`.
- Se lee con el módulo `csv` de Python para numerar filas, porque `zipWithIndex` (RDD) no existe en serverless y Spark no garantiza el orden. 
- Filas con problemas (R-10: se marcan, no se dejan afuera):
  - Más campos que el header: la fila entra a Bronze, con lo que sobra en `extra_fields`, y Silver la marca `DQ008_MALFORMED_ROW`. Frenar todo el archivo por una línea dejaría afuera a las demás.
  - Campo faltante: queda vacío y lo marca `DQ002`.
  - Comilla sin cerrar o codificación inválida: la corrida queda `FAILED`, porque no se puede saber dónde termina cada fila.
- Duplicado exacto dentro del archivo: entra una vez, la primera, y los números de las descartadas quedan en `ingestion_runs.duplicate_row_numbers` (R-08).
- Archivo sin filas de datos: `FAILED_EMPTY`, no escribe. Un archivo truncado no puede pasar como corrida correcta.
- `ingestion_runs` y `trades_raw` no comparten transacción. Si falla el registro de la corrida después del MERGE, las filas quedan sin corrida: `orphan_ingestion_runs` lo detecta, el notebook 02 termina en `FAILED` y G02 bloquea Gold. Se revisa a mano.
- **No se usa pandas** [MEDIDO, pandas 3.1]: si la primera fila trae campos de más, `read_csv` usa las primeras columnas como índice y corre todas las demás, sin error, y el esquema igual pasa. Con `index_col=False` trunca, también sin error.
- Reejecutar deja `trades_raw` idéntica, linaje incluido: conserva la primera corrida. `ingestion_runs` crece una fila por corrida, a propósito, porque registra cada intento.
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
[MEDIDO, Spark 4.0.5 local sobre el CSV completo, 2026-11-01] Dos corridas consecutivas: 100.000 insertadas y después 0 insertadas con 100.000 presentes. 100.000 pares (id, hash) distintos. El módulo `csv` y el lector de Spark leen las mismas filas, sin diferencias. Falta la corrida del notebook 02 en Free Edition: **no ejecutado**.

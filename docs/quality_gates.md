# Quality gates para publicar Gold

Gold se publica con uno de tres estados en `iol_ops.gold_status`:

- **GREEN** — disponible para consumo analítico.
- **YELLOW (degradado)** — disponible; las consultas que dependen del check fallido deben mostrar la advertencia. Ej.: cobertura de cotizaciones baja → q1, q2 y q5 llevan disclaimer.
- **RED (bloqueado)** — error estructural; el notebook 08 (EDA) aborta con el motivo.

Principio: **lo estructural bloquea, lo analítico degrada.** Un problema que rompe la integridad contable (filas perdidas, IDs en conflicto) bloquea. Un problema que reduce la cobertura de un análisis puntual degrada.

| ID | Check | Umbral | Falla → | Argumento |
|---|---|---|---|---|
| G01 | Esquema de entrada = contrato `trades_v1` | 100% | RED | Sin contrato no hay garantía de significado de columnas |
| G02 | Reconciliación Bronze → Silver → fact_trades → fact_trade_market, y todo `ingestion_run_id` de Bronze con su corrida en `ingestion_runs` | 0 diferencias | RED | Requisito de trazabilidad financiera: ninguna fila desaparece ni queda sin rastro |
| G03 | Filas CRITICAL | 0 | RED | No interpretables; si aparecen en este dataset limpio es un cambio de fuente |
| G04 | `DQ004_ID_CONFLICT` sin resolver | 0 | RED | Riesgo de doble conteo |
| G05 | SCD2: clientes con ≠1 versión vigente; versiones solapadas; trades sin versión | 0 | RED | Rompe joins históricos |
| G06 | Integridad referencial de fact_trades (sk nulos) | 0 (UNKNOWN cuenta como válido) | RED | Las métricas por dimensión se sesgan |
| G07 | % trades con `DQ030_INSTRUMENT_UNMAPPED` | ≤1% YELLOW >1%; RED >5% | YELLOW/RED | El mapeo es manual+IA: algo de cola es esperable; >5% indica regresión |
| G08 | Cobertura de cotizaciones (trades comparables / trades elegibles) | ≥80% GREEN; 50–80% YELLOW; <50% YELLOW + q1/q2/q5 marcadas "no concluyentes" | YELLOW | Fuentes gratuitas no cubren ONs/LECAPs; 80% ≈ CEDEARs+soberanos+acciones líderes. **No es RED** porque la integridad del resto de Gold no depende de esto |
| G09 | Cobertura de cada símbolo del top 20 (38,9% de trades) | ≥95% | YELLOW | Si los más operados no tienen precio, los rankings son engañosos |
| G10 | Símbolos con `SCALE_SUSPECT` dentro del top 20 | 0 | YELLOW + excluir símbolo del análisis | Un error de escala produce desvíos de ±99% que dominan rankings |
| G11 | Drift de anomalías: % sábados, % DQ020, % DQ021 vs baseline del run de referencia | ±50% relativo | YELLOW | Sin historia propia, el baseline es el primer run aprobado; detecta cambios de fuente |
| G12 | Todas las tablas persistentes son Delta | 100% | RED | Restricción del challenge (R-38) |
| G13 | Duración total del pipeline | < 30 min en Free Edition | YELLOW | Señal temprana de regresión de performance |

Los umbrales son **iniciales de diseño**. El README debe mostrar valores observados en el run entregado y explicar cualquier ajuste.

## Salida visible (notebook 07)

1. Tabla resumen: check, observado, umbral, estado (con color).
2. Estado global y motivos.
3. Histórico de `pipeline_metrics` de los últimos runs (para mostrar idempotencia: mismos valores en reejecución).

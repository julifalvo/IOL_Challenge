# Requisitos del PDF — matriz de trazabilidad

Cada requisito del enunciado tiene un ID. Specs, código y README deben referenciarlos. Antes de entregar, cada fila debe tener evidencia (archivo + output).

La columna "Requisito" es una paráfrasis fiel del PDF. Donde el proyecto se aparta del texto (R-37), se cita el literal y se marca el desvío. Las filas marcadas "ver cita" tienen el texto literal al final del documento.

| ID | Task | Requisito (paráfrasis fiel del PDF) | Spec | Evidencia esperada | Pts |
|---|---|---|---|---|---|
| R-01 | 01 | Documentar decisiones de diseño como documento técnico para un equipo que lo mantiene sin vos | 01, 11 | `ARCHITECTURE.md`, ADRs | 20 |
| R-02 | 01 | Diagrama de flujo del pipeline | 01 | Mermaid en README + `ARCHITECTURE.md` | |
| R-03 | 01 | Justificación del modelo de datos elegido | 01, 06 | ADR-006, `docs/data_model.md` | |
| R-04 | 01 | Estrategia de escalabilidad | 01 | `ARCHITECTURE.md` §Escalabilidad | |
| R-05 | 01 | Comparativa de ≥3 configuraciones de cómputo productivas con criterios de costo y performance | 01 | ADR-010 + tabla con benchmark medido 1x/10x/100x | |
| R-06 | 02 | Ingesta del CSV a Bronze con Delta Lake | 02 | `iol_bronze.trades_raw` | 14 |
| R-07 | 02 | Pipeline idempotente | 02 | test + output de reejecución | |
| R-08 | 02 | Trazabilidad de todo lo que entró, incluidos registros fuera de expectativa | 02, 03 | `iol_ops.ingestion_runs`, `quality_events` | |
| R-09 | 02 | Criterio documentado **y argumentado con contexto de mercado** por cada tipo de anomalía detectada (ver cita) | 03 | `docs/quality_rules.md` (cada regla con su justificación de mercado: calendario BYMA, escala por VN, liquidación MEP/CCL) | |
| R-10 | 02 | Registros con problemas se marcan y registran, no se descartan | 03 | reconciliación Bronze=Silver=Gold | |
| R-11 | 03 | Modelo dimensional que responda las consultas de negocio | 06 | `docs/data_model.md`, tablas `iol_gold.*` | 15 |
| R-12 | 03 | Esquema: dimensiones y tabla de hechos con atributos | 06 | DDL + diccionario | |
| R-13 | 03 | Dimensión cliente con cambios de atributos en el tiempo (SCD) | 06 | `dim_client` SCD2 | |
| R-14 | 03 | Demostrar SCD con al menos un caso concreto | 06 | celda con cliente real y sus versiones | |
| R-15 | 03 | Documentar el criterio de **cada decisión de diseño** del modelo, en particular los atributos afectados por la distribución del dataset (segmentación de clientes ante actividad asimétrica) (ver cita) | 01, 06 | ADR-006, ADR-007, `docs/data_model.md` (criterio por atributo), celda de viabilidad SCD2 en notebook 01 | |
| R-16 | 04 | Enriquecer con precio de mercado al momento de la operación | 05 | `fact_trade_market` | 8 |
| R-17 | 04 + Restr. | Fuente externa adecuada por tipo de instrumento; **justificar la elección en el README** (ver cita) | 05, 11 | ADR-008 + README §Fuentes de cotización | |
| R-18 | 04 | No fallar ante ausencia de datos de un ticker | 05 | test de degradación | |
| R-19 | 04 | Estrategia de imputación en días sin cotización oficial | 05 | ADR-009 | |
| R-20 | 04 | Manejo de errores de la API | 05 | `iol_ops.quote_requests` | |
| R-21 | 04 | Cobertura alcanzada por símbolo | 05 | `iol_ops.quote_coverage` + tabla en README | |
| R-22 | 04 | Identificar variantes de liquidación en moneda extranjera y documentar mapeo | 04 | ADR-004, `dim_instrument` | |
| R-23 | 05 | Monitoreo que diga si el pipeline está sano sin leer código | 07 | notebook 07 + `pipeline_metrics` | 6 |
| R-24 | 05 | Controles de calidad previos a considerar Gold disponible, con umbrales argumentados | 07 | `iol_ops.gold_status`, `docs/quality_gates.md` | |
| R-25 | 05 | Optimizaciones de performance en tablas de mayor acceso, criterio documentado | 07 | liquid clustering + before/after | |
| R-26 | 06 | EDA con hallazgos con implicancia operativa (no solo descriptivo) | 08 | notebook 08 | 6 |
| R-27 | 06 | ≥4 de 5 preguntas con SQL sobre Gold | 08 | `sql/q1..q5.sql` + outputs | |
| R-28 | 07 | ≥1 componente de IA con valor analítico | 09 | notebook 09 | 6 |
| R-29 | 07 | Documentar problema, por qué IA agrega valor ahí, limitaciones | 09 | README §IA | |
| R-30 | 07 | Si no hay integración programática: prompt, resultado esperado e integración | 09 | `prompts/`, output ejemplo | |
| R-31 | Bonus | Unit tests sin cluster | 10 | `tests/` + reporte pytest | +5 |
| R-32 | Bonus | Orquestador de notebooks con ejecución secuencial | 10 | `00_run_pipeline` | |
| R-33 | Entrega | Repo público con estructura que refleje la arquitectura; orden de notebooks evidente | 11 | árbol del repo | — |
| R-34 | Entrega | Notebooks `.ipynb`/`.dbc` con celdas ejecutadas; resultados de calidad visibles | 11 | `notebooks_executed/` | — |
| R-35 | Entrega + Restr. | README: diagrama, reproducción, decisiones+tradeoffs, cobertura por símbolo, IA **y justificación de la fuente de cotizaciones** (ver cita) | 11 | `README.md` | — |
| R-36 | Entrega | Dataset ofuscado o instrucciones de descarga; no subir originales | 11 | `data/sample_obfuscated.csv`, `data/README.md` | — |
| R-37 | Restr. | **Literal:** "Databricks Community Edition exclusivamente", sin clusters de pago, Jobs ni DLT (ver cita). **Desvío documentado:** CE fue retirada el 1/1/2026 → se usa Free Edition respetando el resto de la restricción | 00 | ADR-001 + desvío declarado en el README | — |
| R-38 | Restr. | Delta obligatorio para tablas persistentes | todas | `DESCRIBE DETAIL` en notebook 07 | — |
| R-39 | Restr. | Decisiones ambiguas argumentadas y aplicadas consistentemente | todas | ADRs + tests | — |
| R-40 | Contexto | El pipeline debe resolver las 4 situaciones del dataset: días inhábiles, montos extremos, variantes de liquidación, actividad por cliente asimétrica (ver cita) | 01, 03, 04, 06 | Días inhábiles → ADR-003 + reglas de calendario en `quality_rules.md`; extremos → ADR-005, DQ020/DQ021; variantes → ADR-004, `dim_instrument`; asimetría → ADR-007. Cada una con celda de evidencia en notebook 01 | transversal |

## Citas literales del PDF (requisitos corregidos)

Las filas marcadas "ver cita" se apoyan en este texto del enunciado:

- **R-09.** Task 02: "Definir y documentar el criterio adoptado para cada tipo de anomalía detectada en el dataset." Criterios de evaluación, Ingesta y calidad (14 pts): "Idempotencia, trazabilidad de anomalías, criterios documentados y argumentados con contexto de mercado".
- **R-15.** Task 03: "El dataset tiene características de distribución particulares que impactan en cómo se modelan ciertos atributos. Documentar el criterio adoptado para cada decisión de diseño." Criterios de evaluación, Modelo dimensional (15 pts): "Esquema propuesto, SCD funcional con caso demostrado, criterios de segmentación documentados".
- **R-17.** Task 04: "usando la fuente externa que consideres más adecuada para cada tipo de instrumento". Restricciones: "Libertad en la fuente de cotizaciones. Usar la API que mejor resuelva la cobertura para el mercado local. Justificar la elección en el README."
- **R-35.** Entregables: "README.md con diagrama de arquitectura, instrucciones de reproducción, todas las decisiones de diseño con sus tradeoffs, tabla de cobertura de cotizaciones por símbolo, y documentación del componente de IA incorporado." Se suma la justificación de la fuente por Restricciones (cita de R-17).
- **R-37.** Restricciones: "Databricks Community Edition exclusivamente. Sin clusters de pago, sin Databricks Jobs, sin Delta Live Tables. La orquestación entre notebooks puede simularse con %run o dbutils.notebook.run()." Pie del PDF: "Ante consultas técnicas durante el período del challenge, escribir directamente." No se usó esa vía para el entorno: ver ADR-001.
- **R-40.** Contexto: "Hay situaciones en los datos que el pipeline debe resolver: operaciones registradas en días inhábiles, montos en rangos extremos, símbolos con variantes de liquidación, y una distribución de actividad por cliente muy asimétrica. La forma en que se aborden esas situaciones es parte de la evaluación."

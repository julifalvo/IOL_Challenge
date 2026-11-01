# Plan de ejecución (documento privado de trabajo — no es el README entregable)

## Prioridad por puntaje

| Área | Pts | Dónde se gana el puntaje |
|---|---:|---|
| Arquitectura y cómputo | 20 | Comparativa con benchmark medido + costos con fórmula; escalabilidad concreta; ADRs |
| Modelo dimensional | 15 | SCD2 con caso real (snapshots mensuales), criterio de segmentación ante distribución asimétrica |
| Ingesta y calidad | 14 | Idempotencia probada, catálogo de reglas con contexto de mercado (sábados, escala, variantes) |
| Cotizaciones | 8 | Mapeo explícito, normalización de escala, imputación sin look-ahead, cobertura por símbolo |
| Observabilidad | 6 | Gates GREEN/YELLOW/RED con umbrales argumentados, liquid clustering con before/after |
| EDA/SQL | 6 | 5 preguntas, lado compra/venta, tests estadísticos, implicancia operativa |
| IA | 6 | Clasificación de instrumentos dentro del pipeline, validada y versionada |
| Bonus | +5 | pytest local + orquestador |

## Plan de 5 días

| Día | Bloque | Specs | Salida verificable |
|---|---|---|---|
| 1 AM | Entorno + riesgo cotizaciones | 00 | Smoke test Free Edition (Volume, UC, Git folder, egress, `ai_query`), spike de 4 proveedores con 8 símbolos |
| 1 PM | Profiling + base local | 01 | Notebook 01 con outputs; `pytest` corriendo; ADR-001..005 en estado "propuesto" |
| 2 | Bronze + Silver + calidad | 02, 03 | Reejecución idempotente demostrada; reconciliación 100%; `quality_rules.md` final |
| 3 AM | Instrumentos + IA clasificación | 04, 09 | `dim_instrument` con mapeo y escala; enriquecimiento IA cacheado |
| 3 PM | Cotizaciones | 05 | `fact_market_quotes`, `fact_trade_market`, `quote_coverage` real |
| 4 AM | Gold + SCD2 | 06 | `dim_client` con 3 snapshots, caso demostrado, tests SCD2 |
| 4 PM | Gates + performance + EDA | 07, 08 | Gates en verde/amarillo con motivos; q1..q5 con outputs |
| 5 AM | Cómputo + orquestación | 01(benchmark), 10 | Benchmark 1x/10x/100x; `00_run_pipeline` end-to-end |
| 5 PM | Entrega | 11 | README final, notebooks exportados con outputs, auditoría R-01..R-39 |

**Regla:** si la cotización por API no funciona al final del día 1, se activa el fetcher local (plan B) sin discutirlo más.

## Riesgos

| Riesgo | Prob. | Impacto | Mitigación |
|---|---|---|---|
| Egress bloqueado en Free Edition | Alta | Alto (Task 04) | Fetcher local → Volume → Delta (ADR-008). Probar verificación LinkedIn |
| Evaluador exige "Community Edition" literal | Baja | Medio | Desvío declarado en el README; ADR-001 |
| Proveedor sin histórico para ONs/LECAPs | Alta | Medio | Cobertura parcial documentada; gate YELLOW, no RED; top 20 priorizado |
| Escala de precio mal normalizada | Media | Alto (desvíos ×100) | Sanity check automático por símbolo (ratio mediano ≈ 1) |
| SCD2 sin cambios reales | Baja | Alto | Snapshots mensuales sobre atributos derivados; verificar cambios en profiling |
| Tiempo | Media | Alto | IA en modo diseño + batch offline si `ai_query` no está; benchmark 100x acotado |
| Export de notebooks sin outputs | Media | Alto (entregable) | Exportar `.ipynb` con outputs manualmente al final, verificar abriendo en GitHub |

## Smoke test del día 1 (checklist)

- [ ] Cuenta Free Edition creada; anotar región y catálogo por defecto.
- [ ] `CREATE SCHEMA` en el catálogo; `CREATE VOLUME`; subir CSV al Volume.
- [ ] Git folder conectado al repo; `import` de `src/` desde un notebook.
- [ ] `dbutils.notebook.run()` funciona en serverless.
- [ ] `requests.get` a cada proveedor candidato → anotar OK/bloqueado en ADR-008.
- [ ] `SELECT ai_query(...)` con algún modelo disponible → anotar OK/no en ADR-011.
- [ ] `ALTER TABLE ... CLUSTER BY` sobre una tabla de prueba.
- [ ] Export de un notebook a `.ipynb` con outputs y verificación del archivo.

## Arranque con Claude Code (VS Code)

1. Copiar este kit a la raíz del repo, el CSV a `data/raw/` y el PDF a `docs/plan/` (ambos gitignored).
2. Entorno local **Windows nativo (PowerShell)**, verificado el 2026-10-31:
   - Python 3.11.9 (venv) y JDK 17 (Temurin 17.0.20.1).
   - pyspark 4.0.5 / delta-spark 4.0.1, que traen Hadoop 3.4.1.
   - winutils `hadoop-3.4.0-win10-x64` en `C:\hadoop\bin`.

   Detalle y procedencia en CLAUDE.md §Entorno.
   ```powershell
   # por sesión de terminal, sin variables globales
   $env:JAVA_HOME      = "C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot"
   $env:HADOOP_HOME    = "C:\hadoop"
   $env:PATH           = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:PATH"
   $env:PYSPARK_PYTHON = (Resolve-Path .\.venv\Scripts\python.exe).Path
   .\.venv\Scripts\Activate.ps1
   pytest -q -rA
   ```
   - Todos los tests deben dar `PASSED`. Si Spark no arranca, `test_delta_merge_is_idempotent` da `ERROR at setup` con la lista de qué revisar. Solo se saltea con `IOL_SKIP_SPARK_TESTS=1`.
   - La primera corrida descarga los jars de Delta desde Maven Central.
3. En Claude Code:
   - "Leé CLAUDE.md, docs/plan/PLAN.md y docs/plan/requisitos_pdf.md. Resumime en 10 líneas qué vamos a construir y qué riesgos ves. No escribas código."
   - `/implement-stage 00`
   - Correr el smoke test en Free Edition y pegarle los resultados: "Actualizá ADR-001, ADR-008 y ADR-011 con estos resultados: …"
   - `/implement-stage 01` … y así por etapa. Después de cada una: `/review-stage NN`.
   - Al final: `/audit-delivery`.
4. Cada vez que corras algo en Databricks, pegale a Claude Code los outputs relevantes para que complete PROGRESS.md y los ADR con `[MEDIDO]`.

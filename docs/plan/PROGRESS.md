# Progreso

Actualizar al cerrar cada etapa. Columna "Databricks" = corrido en Free Edition con outputs guardados.

| Spec | Estado | Tests locales | Databricks | Pendientes / notas |
|---|---|---|---|---|
| 00 Entorno | hecho (local + Databricks) | 46/46 passed (2026-11-01) | `00_setup` OK; `00_smoke_test` 9/9 OK (2026-11-01), con la versión anterior de ambos notebooks | Review hecho. **Pendiente:** volver a correr y exportar `00_setup` y `00_smoke_test`. Outputs actuales en `notebooks_executed/` |
| 01 Profiling + arquitectura | parcial | 95/95 passed (2026-11-01) | **no ejecutado** | Notebook y `ARCHITECTURE.md` escritos; funciones corridas con Spark local sobre el CSV. Falta correr en Free Edition. Benchmark y ADR-010 pendientes: necesitan Silver y Gold (ver log) |
| 02 Bronze | pendiente | — | — | |
| 03 Silver + calidad | pendiente | — | — | |
| 04 Instrumentos | pendiente | — | — | |
| 05 Cotizaciones | pendiente | — | — | |
| 06 Gold + SCD2 | pendiente | — | — | |
| 07 Monitoreo + performance | pendiente | — | — | |
| 08 EDA + SQL | pendiente | — | — | |
| 09 IA | pendiente | — | — | |
| 10 Orquestación + tests | pendiente | — | — | |
| 11 Entrega | pendiente | — | — | |

## Log de decisiones tomadas en la marcha

- 2026-10-31 — Entorno local Windows nativo (PowerShell), Python 3.11 + JDK 17. ~~**Pendiente:** instalar Python 3.11 (el `py -3.11` registrado apunta a un ejecutable inexistente), JDK 17 (hoy solo JDK 22) y winutils/hadoop.dll; definir `JAVA_HOME`, `HADOOP_HOME` y `PYSPARK_PYTHON`.~~ Resuelto, ver "Entorno local verificado". — CLAUDE.md, PLAN.md
- 2026-10-31 — `requisitos_pdf.md`: R-09, R-15, R-17, R-35 y R-37 corregidos con cita literal; se agrega R-40 (4 situaciones del Contexto). — ADR-001
- 2026-10-31 — Filas IOLnet con escala anómala: 8 de 13 [MEDIDO]; "valor por defecto" pasa a [HIPÓTESIS]. — ADR-005
- 2026-10-31 — **Descartados** los valores esperados de SCD2 (~8.000 clientes con ≥2 versiones; 255/181 cambios de canal) y las mediciones pandas que se hicieron para contrastarlos (8.040 / 16.814 y las 5 reglas de desempate).
  - Por qué: salieron de una exploración con desempate arbitrario de pandas, no de una definición fijada. Ninguna regla los reproducía y el ~8.000 contradecía la versión inicial de ADR-007.
  - No se usan como referencia. La celda 10 del notebook 01 calcula (a) y (b) bajo la definición vigente, sin valores esperados.
  - — ADR-007, spec 01
- 2026-10-31 — Fijado `preferred_channel` = canal con más operaciones acumuladas hasta el corte. Desempate determinista: 1) operación más reciente entre los empatados, 2) alfabético de `channel_code`. — ADR-007, spec 06, `data_model.md`
- 2026-10-31 — ~~**Bloqueo:** no se arranca `/implement-stage 00` hasta que Julián instale el entorno local (Python 3.11, JDK 17, winutils).~~ Resuelto, ver entrada siguiente.
- 2026-10-31 — **Entorno local verificado (Windows nativo).**
  - Versiones:
    - `.venv` con Python 3.11.9 y `requirements-dev.txt` instalado: pyspark 4.0.5, delta-spark 4.0.1, pandas 3.1.0, pyarrow 26.0.0, scipy 1.17.1, pytest 9.2.0.
    - JDK Temurin 17.0.20.1.
  - Sin winutils: Spark no arrancaba (`HADOOP_HOME and hadoop.home.dir are unset`) y la fixture convertía el error en SKIPPED.
  - winutils:
    - pyspark 4.0.5 trae Hadoop 3.4.1 y no hay winutils oficial para esa versión. Se usa `kontext-tech/winutils` `hadoop-3.4.0-win10-x64`, en `C:\hadoop\bin`.
    - Es un binario de terceros. SHA-256:
      - `winutils.exe` F7294ECA96BA9F2EFCF66FF89DB16ACE67AE667692A09B49F024472B32FB354C
      - `hadoop.dll` 8769E86A8AD1BFFB6A6038215EB82F523B25D92CCA3D4DD32F62D4FF02418F8A
  - Resultado de `pytest -q -rA`: **4 passed, 0 skipped** (31 s), incluido `test_delta_merge_is_idempotent` (Delta MERGE real en disco local).
  - Variables (`JAVA_HOME`, `HADOOP_HOME`, `PATH`, `PYSPARK_PYTHON`): por sesión de terminal, sin cambios globales en Windows.
  - Todavía no se probó nada en Databricks Free Edition. El smoke test es la etapa 00.
  - — CLAUDE.md, PLAN.md
- 2026-10-31 — La fixture `spark` ahora **falla** (`pytest.fail` con checklist del entorno) si Spark+Delta no arranca, en lugar de saltear.
  - Skip solo deliberado, con `IOL_SKIP_SPARK_TESTS=1`.
  - Test nuevo: `tests/unit/test_spark_fixture.py`.
  - Verificado:
    - entorno OK → 10 passed;
    - sin `HADOOP_HOME` → `ERROR at setup` con causa real ("HADOOP_HOME and hadoop.home.dir are unset");
    - con la variable → 9 passed, 1 skipped "salteados deliberadamente".
- 2026-10-31 — **Etapa 00 implementada (local).**
  - Código:
    - `config.MARKET_HOLIDAYS` con fuente oficial: JSON de feriados de argentina.gob.ar, 6 feriados entre 2025-12-01 y 2026-03-31. El 23/03 (no laborable turístico) queda excluido porque BYMA operó. — ADR-003
    - `io.py`: `table_name`, `landing_path`, `ensure_schemas`, `write_delta_merge` (solo insert, claves null-safe, falla ante claves repetidas en el origen). — ADR-002
    - `smoke.py`: chequeo de egress.
    - Notebooks `00_setup`, `00_smoke_test` y `00_smoke_test_child`.
    - Tabla nueva `iol_ops.smoke_test_results`. — `data_model.md`
  - Fixture `tests/fixtures/trades_small.csv`: 40 filas sintéticas con todos los casos del spec más positivos para DQ002/003/006/007/011/013. Catálogo de casos en `tests/fixtures/README.md`, verificado por `test_fixture_cases.py`.
  - Se eliminó `config.table()`, que no se usaba y duplicaba a `io.table_name()`. `LANDING_VOLUME` pasa a `io.landing_path()` para que respete el widget `catalog`.
  - Criterios de aceptación:
    - ✅ `pytest -q` corre localmente: 32 passed.
    - ✅ El fixture cubre todos los casos.
    - ⏳ Notebook 00 corre en Free Edition: **no ejecutado**. Cerrado el 2026-11-01, ver entrada siguiente.
    - ⏳ Resultados del smoke test anotados en ADR-001/008/011: **no ejecutado**. Cerrado el 2026-11-01, ver entrada siguiente.
- 2026-11-01 — **Etapa 00 corrida en Free Edition.**
  - `00_setup` sin errores:
    - 4 schemas `iol_*` creados;
    - Volume `iol_bronze.landing` creado;
    - CSV de 12.242.003 bytes (mismo tamaño que el local), header = contrato `trades_v1`.
  - `00_smoke_test` con 9/9 OK, `smoke_run_id = 1cc453e0-7d65-4a74-b2c2-4387cc92ec8d`, guardado en `iol_ops.smoke_test_results`:
    - `notebook.run` funciona;
    - liquid clustering funciona;
    - `write_delta_merge` es idempotente en UC (8 insertadas, después 0);
    - `ai_query` responde con Llama 3.3 70B;
    - egress HTTP 200 a IOL, BYMA y data912; Yahoo responde HTTP 429.
  - Consecuencias:
    - ADR-008: el modo `api` desde Databricks es viable a nivel red, pero los endpoints de datos no se probaron. El fetcher local queda como plan B por rate limiting o términos de uso, no por egress.
    - ADR-011: `ai_query` está disponible; el modelo de la etapa 09 se elige entre los 12 endpoints listados.
  - Criterios de aceptación de la etapa 00: los 4 ✅. ~~Falta exportar los notebooks con outputs a `notebooks_executed/`.~~ Exportados en `8aaba06`.
  - — ADR-001, ADR-008, ADR-011
- 2026-11-01 — **Decisión: Free Edition sin consultar a IOL.** No se envió el mail y se eliminó `docs/plan/consulta_iol.md`. ADR-001 pasa a aceptado; el desvío de R-37 se declara en el README. — ADR-001
- 2026-11-01 — **`/review-stage 00`:** listo con cambios menores, sin severidad alta. Corregidos los de severidad media:
  - Fórmula de ingesta: `read = inserted + already_present + duplicate_in_file`; `conflict` es subconjunto de `inserted`. — ADR-002, `quality_rules.md`, `data_model.md`, spec 02
  - `config.CHANNELS` ahora da 5 `channel_code`; las familias van en `CHANNEL_FAMILIES`. — `data_model.md`
  - `write_delta_merge` falla si el origen trae columnas que el destino no tiene. Antes `INSERT *` las ignoraba sin error [MEDIDO, Spark 4.0.5 local].
  - La clave del MERGE debe determinar el contenido: queda en el docstring y fijado con test. No hay detección de conflictos en el helper.
  - Egress: HTTP 403, 407 y 429 pasan a `INCONCLUSO` y se guarda el header `Server`. — ADR-008
  - Runtime real documentado (Spark 4.2.0, Python 3.12.3) y su diferencia con local. — ADR-001, CLAUDE.md
  - Tests locales: 39 passed.
- 2026-11-01 — **Pendientes abiertos de la etapa 00:**
  - Volver a correr `00_setup` y `00_smoke_test` en Free Edition y exportarlos. Los `.ipynb` actuales son de la versión anterior de los notebooks: **no ejecutado** con el código corregido. El de `00_setup` todavía muestra el email del usuario en un output, y va a seguir en el historial de git aunque se reemplace.
  - Spike de cotizaciones de ADR-008: medido desde local el 2026-11-01. Falta `00_quotes_spike` desde Free Edition. La API de IOL no se evalúa: requiere credenciales personales y se decidió no usarlas.
  - Hallazgos de severidad baja que siguen abiertos:
    - El fixture no tiene casos de DQ005, DQ031 ni `tipoTran`/`origen` inválidos. Se suman en la etapa 03.
    - `write_delta_merge` evalúa el origen 3 veces. Se revisa junto con el benchmark.
- 2026-11-01 — **Corregidos los otros 10 hallazgos de severidad baja:**
  - `00_setup` y `00_smoke_test` devuelven `FAILED` si falta el CSV, no coincide el contrato o falla un chequeo. `INCONCLUSO` no cuenta como falla.
  - El MERGE de `00_smoke_test` tiene `try`, y el resumen se muestra aunque falle.
  - Los límites de texto del smoke pasan a `config.py`.
  - El smoke prueba `ALTER TABLE ... CLUSTER BY` y registra región y catálogo. **No ejecutado** todavía; por eso esos dos ítems siguen abiertos en el checklist de `PLAN.md`.
  - `00_setup` ya no imprime el usuario.
  - ADR-003 etiqueta el calendario como [SUPUESTO] y cita la evidencia; el spec 00 usa `MARKET_HOLIDAYS`.
  - ADR-011 cita el prompt real de `ai_query`.
  - `profile_local.py` toma los feriados de `config`; `pyproject.toml` pide Python 3.11 o superior.
  - Limpiadas las entradas viejas de este log.
  - Tests locales: 46 passed. `profile_local.py` da la misma salida que antes del cambio.
- 2026-11-01 — **Sin acceso de Claude Code a Databricks.** No hay CLI, configuración ni credenciales en la máquina, y no se generó ningún token. Las corridas en Free Edition las hace el usuario.
- 2026-11-01 — **Etapa 01, parcial.**
  - Código: `calendar.py`, `segmentation.py`, `profiling.py`, `notebooks/01_data_profiling.py`, y 44 tests. `calendar.py` y `segmentation.py` se reusan en las etapas 03 y 06.
  - Corrida con Spark 4.0.5 local sobre el CSV completo: coincide con `profile_local.py` en lo comparable (conteos, sábados, canales, percentiles, 279 símbolos, 13 anomalías, 21/18/10 días hábiles). Las fechas no dependen de la zona horaria de la sesión.
  - Celda 10: 16.814 clientes con 2 o más versiones; 458, 401 y 277 cambios de canal entre puntos. ADR-007 sigue vigente. Ese 16.814 coincide con un valor descartado el 2026-10-31, pero ahora sale de la definición fijada, y tiene explicación: son los clientes con 2 o más operaciones. — ADR-007
  - Criterios de aceptación:
    - ⏳ Notebook 01 con las 10 celdas y outputs: escrito, **no ejecutado** en Free Edition.
    - ⏳ Celda 10 con (a) y (b): medida en local, **no ejecutada** en Free Edition.
    - ❌ `ARCHITECTURE.md` completo: escrito, pero §6 no tiene datos porque depende del benchmark. Sus números vienen de la corrida local.
    - ❌ Benchmark 1x/10x/100x: **no ejecutado**. Corre Silver y Gold, que no existen; `PLAN.md` lo ubica en el día 5.
    - ❌ ADR-010 con tabla, fórmula, precios fechados y recomendación: depende del benchmark.
- 2026-11-01 — **Spike de cotizaciones desde local.** `tools/quotes_fetcher/spike.py`.
  - BYMA cubre 8/8, data912 5/8, Yahoo 3/8. IOL da HTTP 401 sin credenciales.
  - BYMA da los bonos soberanos ajustados, y el sanity check de ADR-005 no lo detecta. BYMA y Yahoo dan `YPFD` a ×0,1.
  - Decisión aceptada por el usuario: data912 para bonos soberanos y acciones; BYMA para CEDEARs, letras y ONs. data912 es un proyecto personal sin garantías: es el principal riesgo.
  - Pendientes para la etapa 04 y 05: factor de escala por símbolo y no solo por (proveedor, `asset_type`), revisar el rango del sanity check, y tratar días como el 08/01. — ADR-005, ADR-008

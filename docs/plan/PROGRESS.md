# Progreso

Actualizar al cerrar cada etapa. Columna "Databricks" = corrido en Free Edition con outputs guardados.

| Spec | Estado | Tests locales | Databricks | Pendientes / notas |
|---|---|---|---|---|
| 00 Entorno | hecho (local + Databricks) | 39/39 passed (2026-11-01) | `00_setup` OK; `00_smoke_test` 9/9 OK (2026-11-01), con el criterio anterior de egress | Review hecho. **Pendientes:** volver a correr y exportar `00_smoke_test`, spike ADR-008, hallazgos de severidad baja (ver log del 2026-11-01). Outputs en `notebooks_executed/00_setup.ipynb` y `00_smoke_test.ipynb` |
| 01 Profiling + arquitectura | pendiente | — | — | |
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

- 2026-10-31 — Entorno local Windows nativo (PowerShell), Python 3.11 + JDK 17. **Pendiente:** instalar Python 3.11 (el `py -3.11` registrado apunta a un ejecutable inexistente), JDK 17 (hoy solo JDK 22) y winutils/hadoop.dll; definir `JAVA_HOME`, `HADOOP_HOME` y `PYSPARK_PYTHON`. — CLAUDE.md, PLAN.md
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
  - Criterios de aceptación de la etapa 00: los 4 ✅. Falta exportar los notebooks con outputs a `notebooks_executed/`, que es parte del entregable.
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
  - Volver a correr `00_smoke_test` en Free Edition y exportarlo. El `.ipynb` actual es de la versión anterior del notebook: **no ejecutado** con el código corregido.
  - Spike de cotizaciones de ADR-008 (8 símbolos, 4 proveedores): **no ejecutado**. `PLAN.md` lo ubica en el día 1 y el spec 04 depende de él, así que tiene que estar antes de la etapa 04. La API de IOL requiere cuenta.
  - Hallazgos de severidad baja, sin corregir:
    - ADR-003: etiquetar [SUPUESTO] la regla "feriado bursátil = feriado nacional" y alinear el nombre `MARKET_CALENDAR` del spec 00. Evidencia a citar: entre 02/01 y 13/03 los únicos días hábiles sin operaciones son 16 y 17/02 [MEDIDO].
    - `scripts/profile_local.py:18` tiene su propia lista de feriados.
    - `00_setup` y `00_smoke_test` salen con `SUCCESS` aunque falte el CSV, no coincida el contrato o falle un chequeo.
    - El MERGE de `00_smoke_test` no tiene `try`, aunque la cabecera dice que ningún chequeo corta.
    - Números mágicos 500, 300 y 200 en `00_smoke_test.py` y `smoke.py`.
    - `write_delta_merge` evalúa el origen 3 veces.
    - ADR-011 dice que `ai_query` respondió a `ping`; el prompt real fue `Respond with the single word: pong`.
    - `PLAN.md`: `ALTER TABLE ... CLUSTER BY` sin probar, checklist sin marcar, región sin anotar.
    - Entradas viejas de este log siguen diciendo "pendiente" o "falta exportar".
    - El fixture no tiene casos de DQ005, DQ031 ni `tipoTran`/`origen` inválidos. Se suman en la etapa 03.
    - `00_setup.ipynb` muestra el email del usuario en un output. Decisión del usuario.
    - `pyproject.toml` declara `>=3.10`; CLAUDE.md pide 3.11 o superior.

# Progreso

Actualizar al cerrar cada etapa. Columna "Databricks" = corrido en Free Edition con outputs guardados.

| Spec | Estado | Tests locales | Databricks | Pendientes / notas |
|---|---|---|---|---|
| 00 Entorno | parcial: local OK | 10/10 passed (2026-10-31) | no ejecutado | Falta smoke test en Free Edition (spec 00) |
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

# CLAUDE.md — IOL Senior Data Engineer Challenge 2026

Contexto permanente para Claude Code. Leelo completo al inicio de cada sesión.

## Qué es este proyecto

Pipeline de datos sobre ~100k operaciones bursátiles BYMA (ene–13 mar 2026) para el challenge técnico Senior DE de IOL Inversiones.
Entregable: repo GitHub público + notebooks con outputs + README con decisiones y trade-offs.

**Fuentes de verdad (en este orden):**
1. `docs/plan/challenge-iol-de-sr.pdf` → especificación (no está en git si es confidencial; ver `docs/plan/requisitos_pdf.md`).
2. CSV original → dato real. **Nunca se commitea.** Vive en `data/raw/` (gitignored) y en el Volume de Databricks.
3. `docs/specs/*.md` → qué implementar en cada etapa y cómo se acepta.
4. `docs/decisions/ADR-*.md` → por qué. Si una decisión cambia, se actualiza el ADR **antes** que el código.

## Entorno (crítico)

- **Databricks Community Edition fue retirado el 1/1/2026.** Se usa **Databricks Free Edition** (ver ADR-001).
- Free Edition: **solo serverless**, Unity Catalog, salida a internet restringida a dominios confiables (la verificación con LinkedIn puede habilitarla).
- En serverless **no usar**: RDD API, `spark.sparkContext`/`sc`, `.cache()`/`.persist()`, configs de cluster, rutas `dbfs:/` root. Usar tablas UC y Volumes.
- Sin Databricks Jobs ni DLT/Lakeflow pipelines (restricción del challenge). Orquestación = `dbutils.notebook.run()` desde `notebooks/00_run_pipeline.py`.
- **Todas** las tablas persistentes en Delta (managed tables UC).
- Desarrollo local en VS Code: lógica en `src/iol_pipeline/` como funciones PySpark puras, testeadas con `pytest` + Spark local + delta-spark. Databricks se usa para correr y generar evidencia.
- **Runtime de Free Edition [MEDIDO, 2026-11-01]:** `client.6.5`, Spark 4.2.0, Python 3.12.3. Local corre pyspark 4.0.5 y Python 3.11: los tests validan la lógica, no el comportamiento específico de Spark 4.2 (ADR-001).
- **Entorno local: Windows nativo (PowerShell)**, sin WSL. Verificado el 2026-10-31 con `pytest` (4 passed, test de Delta MERGE incluido):

  | Componente | Versión / ubicación |
  |---|---|
  | Python (venv) | 3.11.9, `.venv` creado con `py -3.11` |
  | JDK | Temurin 17.0.20.1, `C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot` |
  | pyspark / delta-spark | 4.0.5 / 4.0.1 (jars de **Hadoop 3.4.1**) |
  | winutils | `C:\hadoop\bin\winutils.exe` + `hadoop.dll`, build `hadoop-3.4.0-win10-x64` de [kontext-tech/winutils](https://github.com/kontext-tech/winutils) |

  - **Por qué hace falta winutils:** sin `HADOOP_HOME`, Spark ni siquiera arranca. Falla en `SparkContext.addFile` → `FileUtil.chmod` → `Shell.getWinUtilsPath`.
  - **Origen del build:** no existe winutils oficial para 3.4.1, así que se usa el más cercano. Es un binario de terceros; los SHA-256 quedaron registrados en `PROGRESS.md`.
  - **`PYSPARK_PYTHON`** tiene que apuntar al Python del venv. Si no, Spark lanza el `python3` del sistema (3.14) y falla por versión distinta.
  - **Variables:** se setean **por sesión de terminal**, no como variables globales de Windows (ver §Comandos).
  - **pytest y Spark:** si Spark+Delta no arranca, la fixture `spark` de `tests/conftest.py` **falla**. pytest lo reporta como `ERROR at setup` y el mensaje lista qué revisar (JAVA_HOME, HADOOP_HOME/winutils, PATH, PYSPARK_PYTHON). Esos tests solo se saltean si `IOL_SKIP_SPARK_TESTS=1` (§Comandos), y el skip lo dice explícitamente.

## Naming

- Catálogo: parámetro `CATALOG` (default `workspace`). Schemas: `iol_bronze`, `iol_silver`, `iol_gold`, `iol_ops`.
- Volume de landing: `/Volumes/{CATALOG}/iol_bronze/landing/` (subcarpetas `trades/`, `quotes/`, `ai/`).
- Tablas: `snake_case`. Hechos `fact_*`, dimensiones `dim_*`, operativas en `iol_ops`.
- Columnas de negocio en inglés en Silver/Gold (`trade_id`, `side`, `qty`, `price`). Bronze conserva nombres originales.
- Toda la configuración (umbrales, rutas, versiones de regla) en `src/iol_pipeline/config.py`. **Ningún número mágico en notebooks.**

## Estructura

```
notebooks/        # Databricks source (.py con "# Databricks notebook source"), numerados por orden de ejecución
src/iol_pipeline/ # lógica testeable (bronze, quality, calendar, instruments, quotes, gold, monitoring, ai)
tools/quotes_fetcher/  # descarga de cotizaciones FUERA de Databricks (plan B por egress)
sql/              # q1..q5 de negocio, una consulta por archivo
prompts/          # prompts versionados (vN) de los componentes de IA
tests/unit|integration
docs/specs/       # especificación por etapa (LEER antes de implementar)
docs/decisions/   # ADRs
scripts/          # profiling local y muestra ofuscada
data/             # solo sample ofuscado + README; raw/ y quotes_raw/ gitignored
```

En notebooks, importar `src` con:
```python
import sys, os
sys.path.insert(0, os.path.abspath("../src"))
```

## Hechos del dataset ya verificados (notebook 01 en Free Edition, 2026-11-01)

- 100.000 filas, 0 nulos, 0 `id_transaccion` duplicados, 57.655 clientes, 1.445 símbolos.
- `fecha` ISO-8601 UTC (`...Z`). 5 canales: App Mobile 66.438, Sitio Web Desktop 28.952, Sitio Web Responsive 2.651, API 1.688, IOLnet 271.
- 0 operaciones en domingo y 0 en Carnaval (16–17/02). Sábados: 12.498 (UTC). Lunes empieza ~10 UTC, sábado termina ~19 UTC → **hipótesis: sábado = cola de la sesión del viernes** (ADR-003). La hora del día no es confiable (distribución casi uniforme 24 h).
- **Precio de bonos por 1 VN** (AL30 ≈ 876 ARS, AL30D ≈ 0,607 USD). Las fuentes de mercado suelen cotizar por 100 VN → normalizar escala (ADR-005).
- Sufijo D/C **no** es regla: `YPFD`, `CARC`, `C`, `BA.C`, `AMD`, `HD`, `BA37D` son ARS. 471 símbolos USD, 279 con base ARS al quitar el sufijo. Mapeo = tabla explícita (ADR-004).
- Extremos: `T30A7` qty 1.503.210.011 a precio 0,0101 cuando la mediana del propio símbolo es 1,08 → **posible error de escala**. 13 filas con precio a ×50 o ×1/50 de la mediana de su símbolo [MEDIDO]. 8 de esas 13 son de IOLnet y las 8 tienen precio que redondea a 0,01 o 1,00 (`T30A7` es 0,0101) [MEDIDO]. Que sea un valor por defecto de carga es [HIPÓTESIS]. Outliers se evalúan sobre importe y por tipo de activo.
- **Opciones:** ~1.824 operaciones en ~231 símbolos de opciones (`GFGC…` call / `GFGV…` put sobre GGAL, `YPFC…`), con descripción = símbolo. Son un `asset_type` propio (`OPCION`): primas muy volátiles y cobertura de cotizaciones esperable baja.
- Clientes: p50=1, p75=2, p90=3, p95=4, p99=10 operaciones; 40.841 con una sola operación. 1.741 operan por más de un canal.
- Top 20 símbolos = 38,9% de operaciones.

## Reglas duras (no negociables)

1. **Ninguna fila se descarta.** Toda fila de Bronze existe en Silver y en `fact_trades`, con `quality_status` y `quality_flags`. Los filtros se hacen en vistas/consultas, nunca borrando.
2. **Idempotencia:** reejecutar cualquier etapa con el mismo input deja las tablas idénticas (conteos y hashes). Nada de `append` ciego: `MERGE` con clave determinista.
3. **No mezclar monedas:** nunca sumar ARS y USD. Importes siempre con `currency_code` al lado.
4. **No look-ahead:** cotización usada ≤ fecha efectiva de la operación. Segmentos de cliente calculados solo con información hasta el snapshot.
5. **No inventar:** ni datos, ni cobertura, ni métricas, ni disponibilidad de APIs. Si algo no se ejecutó, se dice "no ejecutado".
6. **No commitear:** CSV original, credenciales, tokens, `.env`. Secretos vía `dbutils.secrets` o variables de entorno locales.
7. **No sobrescribir silenciosamente:** conflictos (mismo ID, distinto contenido) van a `iol_ops.quality_events` y se preservan.
8. **Hipótesis ≠ hechos:** en docs y outputs, etiquetar `[MEDIDO]`, `[HIPÓTESIS]`, `[SUPUESTO]`.

## Cómo trabajar (por etapa)

Usá `/implement-stage <NN>` (ver `.claude/commands/`). Para cada etapa:
1. Leé `docs/specs/NN_*.md` y los ADR que referencia.
2. Listá requisitos del PDF que cubre (IDs `R-xx` de `docs/plan/requisitos_pdf.md`).
3. Proponé archivos a tocar. Cambio mínimo completo.
4. Implementá lógica en `src/`, tests en `tests/`, notebook fino que llama a `src/`.
5. Corré `pytest` y reportá resultado real.
6. Verificá los criterios de aceptación del spec uno por uno.
7. Actualizá `docs/plan/PROGRESS.md` (qué quedó, qué falta, qué se verificó en Databricks).

No avances a la etapa siguiente con criterios de aceptación pendientes salvo que lo diga yo.

## Comandos

PowerShell (Windows nativo), desde la raíz del repo.

```powershell
# Una vez por sesión de terminal (no se tocan variables globales de Windows)
$env:JAVA_HOME      = "C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot"
$env:HADOOP_HOME    = "C:\hadoop"                    # contiene bin\winutils.exe y bin\hadoop.dll
$env:PATH           = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:PATH"
$env:PYSPARK_PYTHON = (Resolve-Path .\.venv\Scripts\python.exe).Path
.\.venv\Scripts\Activate.ps1                         # si lo bloquea: Set-ExecutionPolicy -Scope CurrentUser RemoteSigned

# Primera vez (o para recrear el venv)
py -3.11 -m venv .venv
python -m pip install -r requirements-dev.txt        # con el venv activado

pytest -q -rA                      # unit + integration; -rA muestra PASSED/SKIPPED por test
pytest -q tests/unit               # rápido

# Saltear A PROPÓSITO los tests que usan Spark (p. ej. sin JDK a mano). Solo el valor "1" saltea.
$env:IOL_SKIP_SPARK_TESTS = "1"; pytest -q -rA     # -> SKIPPED "salteados deliberadamente"
Remove-Item Env:IOL_SKIP_SPARK_TESTS               # volver al modo normal
python scripts/profile_local.py data/raw/data-set-challenge-5-.csv   # reproduce hallazgos
python scripts/make_obfuscated_sample.py data/raw/data-set-challenge-5-.csv data/sample_obfuscated.csv --salt $env:SAMPLE_SALT
python -m tools.quotes_fetcher.fetch --provider <p> --symbols-file <f> --from 2025-12-15 --to 2026-03-13
```

## Definición de terminado (global)

- Tests verdes localmente.
- Notebook corrido en Free Edition con outputs visibles (tablas de calidad, conteos, gates).
- Métricas registradas en `iol_ops.pipeline_metrics`.
- ADR/spec actualizado si cambió una decisión.
- Sin TODOs silenciosos: todo pendiente va a `PROGRESS.md`.

## Estilo de código

- Python 3.11+, type hints, funciones puras que reciben/devuelven DataFrames.
- Sin abstracciones especulativas. Una interfaz solo donde hay ≥2 implementaciones reales (ej. `QuoteProvider`).
- SQL de negocio en `sql/`, comentado con definición de métrica y supuestos.
- Mensajes de commit en español, imperativo, una etapa por PR/commit lógico.

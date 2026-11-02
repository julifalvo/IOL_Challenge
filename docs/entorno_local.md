# Entorno local

Lógica en `src/iol_pipeline/` como funciones PySpark puras, testeadas con `pytest`, Spark local y delta-spark, sin cluster. Databricks se usa para correr y generar evidencia.

**Windows nativo (PowerShell)**, sin WSL. Verificado el 2026-10-31:

| Componente | Versión / ubicación |
|---|---|
| Python (venv) | 3.11.9, `.venv` creado con `py -3.11` |
| JDK | Temurin 17.0.20.1, `C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot` |
| pyspark / delta-spark | 4.0.5 / 4.0.1 (jars de **Hadoop 3.4.1**) |
| winutils | `C:\hadoop\bin\winutils.exe` + `hadoop.dll`, build `hadoop-3.4.0-win10-x64` de [kontext-tech/winutils](https://github.com/kontext-tech/winutils) |

- **Runtime de Free Edition [MEDIDO, 2026-11-01]:** `client.6.5`, Spark 4.2.0, Python 3.12.3. Los tests locales validan la lógica, no el comportamiento específico de Spark 4.2 (ADR-001).
- **Por qué hace falta winutils:** sin `HADOOP_HOME`, Spark ni siquiera arranca. Falla en `SparkContext.addFile` → `FileUtil.chmod` → `Shell.getWinUtilsPath`.
- **Origen del build:** no existe winutils oficial para 3.4.1, así que se usa el más cercano. Es un binario de terceros; los SHA-256 están en `docs/plan/PROGRESS.md`.
- **`PYSPARK_PYTHON`** tiene que apuntar al Python del venv. Si no, Spark lanza el `python3` del sistema y falla por versión distinta.
- **Variables:** se setean por sesión de terminal, no como variables globales de Windows.
- **pytest y Spark:** si Spark y Delta no arrancan, la fixture `spark` de `tests/conftest.py` falla. pytest lo reporta como `ERROR at setup`, con la lista de qué revisar. Esos tests solo se saltean si `IOL_SKIP_SPARK_TESTS=1`, y el skip lo dice explícitamente.

## Comandos

Desde la raíz del repo:

```powershell
# Una vez por sesión de terminal
$env:JAVA_HOME      = "C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot"
$env:HADOOP_HOME    = "C:\hadoop"                    # contiene bin\winutils.exe y bin\hadoop.dll
$env:PATH           = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:PATH"
$env:PYSPARK_PYTHON = (Resolve-Path .\.venv\Scripts\python.exe).Path
.\.venv\Scripts\Activate.ps1                         # si lo bloquea: Set-ExecutionPolicy -Scope CurrentUser RemoteSigned

# Primera vez, o para recrear el venv
py -3.11 -m venv .venv
python -m pip install -r requirements-dev.txt        # con el venv activado

pytest -q -rA                       # unit + integration
pytest -q -m "not integration"      # sin los de integración

# Saltear a propósito los tests que usan Spark. Solo el valor "1" saltea.
$env:IOL_SKIP_SPARK_TESTS = "1"; pytest -q -rA
Remove-Item Env:IOL_SKIP_SPARK_TESTS

python scripts/profile_local.py data/raw/data-set-challenge-5-.csv   # reproduce hallazgos
python scripts/make_obfuscated_sample.py data/raw/data-set-challenge-5-.csv data/sample_obfuscated.csv --salt $env:SAMPLE_SALT
python -m tools.quotes_fetcher.fetch --provider <p> --symbols-file <f> --from 2025-12-15 --to 2026-03-13
```

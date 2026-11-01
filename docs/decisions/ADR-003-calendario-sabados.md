# ADR-003 — Calendario de mercado, operaciones en sábado y fecha efectiva

- **Estado:** propuesto
- **Requisitos:** R-09, R-19

## Contexto (profiling local, recalcular en notebook 01)
- 10.255 operaciones caen en sábado en hora local (12.498 en UTC). 0 en domingo.
- 0 operaciones el 16/02 y 17/02 (Carnaval): **el dataset respeta feriados**.
- Por hora UTC: los lunes no hay operaciones antes de ~10 UTC; los sábados se cortan ~19 UTC. La hora está repartida casi uniformemente en 24 h, incompatible con la rueda de BYMA (11–17 hs).

## Interpretación `[HIPÓTESIS]`
Los registros de sábado son la cola de la jornada operativa del viernes (registro diferido/procesamiento nocturno o timestamp desplazado), no operaciones ejecutadas en sábado. La hora del día no representa el instante de ejecución.

## Decisión
- No descartar ni corregir el timestamp original.
- `effective_trade_date` = fecha local si es día hábil; si es sábado → último día hábil previo. Flag `DQ010_SATURDAY` (WARNING, entra a métricas).
- Domingo o feriado → ERROR (`DQ011`, `DQ012`), no se reasigna: no hay mecanismo que lo explique.
- Fecha fuera del rango de `MARKET_HOLIDAYS` → `effective_trade_date` queda en null y ERROR (`DQ014`): sin calendario no se sabe si el día previo fue hábil. Se amplía el calendario, no se adivina.
- Toda la analítica y el matching de cotizaciones usan `effective_trade_date`.
- El análisis intradiario queda fuera de alcance; `DQ013_OUTSIDE_SESSION` solo como métrica INFO.

## Calendario de feriados bursátiles (`config.MARKET_HOLIDAYS`)
- **Regla** [SUPUESTO]: feriado bursátil = feriado nacional (inamovible o trasladable). Los "días no laborables" no cuentan. No se contrastó contra un calendario publicado por BYMA.
- **Evidencia a favor** [MEDIDO, CSV completo, 2026-11-01]: entre 02/01 y 13/03, los únicos días hábiles sin operaciones son 16/02 y 17/02, los dos feriados de ese rango. Las otras 4 fechas caen fuera del rango de los trades: solo afectan a `dim_date` y a la imputación de cotizaciones.
- **Rango:** el de `dim_date`, 2025-12-01..2026-03-31.
- **Fuente oficial:** calendario de Jefatura de Gabinete, [argentina.gob.ar/jefatura/feriados-nacionales-2026](https://www.argentina.gob.ar/jefatura/feriados-nacionales-2026). Los datos salen de los archivos `holidays-2025-es.json` y `holidays-2026-es.json`, consultados el 2026-10-31.
- **Resultado:** 08/12/2025, 25/12/2025, 01/01/2026, 16/02/2026, 17/02/2026 y 24/03/2026.
- **Excluidos a propósito:**
  - 20/03/2026: no laborable por Ramadán, aplica solo a quienes profesan esa religión.
  - 23/03/2026: no laborable con fines turísticos. BYMA operó ese día, sin liquidación porque no hubo actividad bancaria. Hay cierre registrado en [Rava](https://www.rava.com/cotizaciones/historico/byma/2026/03/23) [MEDIDO, fuente secundaria].
- **Trade-off:** si algún día no laborable hubiera tenido el mercado cerrado, aparecería como "día sin cotización" (imputación de ADR-009), no como anomalía de calidad. Dentro del rango de los trades (02/01–13/03) no hay ninguno.

## Alternativas
| Opción | Por qué no |
|---|---|
| Marcar sábados como inválidos y excluirlos | Saca ~10% de las operaciones por una anomalía con explicación operativa plausible |
| Asignarlos al lunes siguiente | Usaría información de precio posterior (look-ahead) |
| Usar fecha UTC | Mezcla jornadas; la operatoria es local |

## Cuándo revisar
Si IOL confirma la semántica de `fecha` (ejecución vs registro vs liquidación). **Pregunta para la entrevista.**

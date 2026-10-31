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
- Toda la analítica y el matching de cotizaciones usan `effective_trade_date`.
- El análisis intradiario queda fuera de alcance; `DQ013_OUTSIDE_SESSION` solo como métrica INFO.

## Alternativas
| Opción | Por qué no |
|---|---|
| Marcar sábados como inválidos y excluirlos | Saca ~10% de las operaciones por una anomalía con explicación operativa plausible |
| Asignarlos al lunes siguiente | Usaría información de precio posterior (look-ahead) |
| Usar fecha UTC | Mezcla jornadas; la operatoria es local |

## Cuándo revisar
Si IOL confirma la semántica de `fecha` (ejecución vs registro vs liquidación). **Pregunta para la entrevista.**

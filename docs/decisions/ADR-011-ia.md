# ADR-011 — IA aplicada: enriquecimiento de instrumentos (principal) y resumen de EDA (secundario)

- **Estado:** propuesto
- **Requisitos:** R-28, R-29, R-30

## Decisión
- **Principal:** clasificar y enriquecer instrumentos con LLM, solo para lo que las reglas no resuelven, con validación por schema, contraste con reglas, caché por hash y accuracy medida sobre 60 símbolos etiquetados.
- **Secundario:** resumen ejecutivo del EDA a partir de métricas agregadas, con verificación de que cada número citado existe en el input.

## Alcance después de las reglas [MEDIDO, 2026-11-01]
Las reglas y la lista de acciones de ADR-004 dejan solo 10 símbolos y 371 operaciones en `NEEDS_REVIEW`. El mayor aporte de la IA pasa a ser lo que ninguna regla puede inferir: emisor, sector y ticker del subyacente de 598 CEDEARs, más esos 10 casos.

## Por qué el principal es clasificación y no el resumen
Está **dentro** del pipeline: mejora `dim_instrument`, el mapeo a la fuente de cotización y los cortes del análisis. El resumen es consumo; útil, pero no cambia la calidad del dato.

## Integración (completar con smoke test)
| Modo | Disponible | Notas |
|---|---|---|
| `ai_query` en Databricks | sí [MEDIDO] 2026-11-01 | modelo: `databricks-meta-llama-3-3-70b-instruct`; `ai_query` con el prompt `Respond with the single word: pong` respondió `pong`: prueba conectividad, no calidad |
| API externa (offline batch → Volume) | no evaluado | innecesario mientras `ai_query` funcione |
| Manual en claude.ai con prompt versionado | sí | válido según PDF |

### Endpoints de serving disponibles en Free Edition [MEDIDO, 2026-11-01]
`databricks-bge-large-en`, `databricks-deepseek-v4-flash-0731`, `databricks-gemma-3-12b`, `databricks-gpt-oss-120b`, `databricks-gpt-oss-20b`, `databricks-gte-large-en`, `databricks-llama-4-maverick`, `databricks-meta-llama-3-1-8b-instruct`, `databricks-meta-llama-3-3-70b-instruct`, `databricks-qwen3-embedding-0-6b`, `databricks-qwen3-next-80b-a3b-instruct`, `databricks-qwen35-122b-a10b`.

- El modelo de la etapa 09 se elige ahí, con la muestra etiquetada. Llama 3.3 70B solo se probó con ese prompt de conectividad: su calidad para clasificar no está medida.
- `config.SMOKE_AI_MODEL` deja de ser [SUPUESTO]: el endpoint existe y respondió.

## Privacidad
Solo metadatos de instrumentos y agregados. Ningún `id_cliente`, `id_transaccion` ni fila transaccional sale del workspace.

## Limitaciones
Fecha de corte de conocimiento del modelo, no determinismo (mitigado con temperatura 0 + caché + versión de prompt), alucinación de emisores/tickers (mitigado con enums, reglas y muestra evaluada). La IA nunca decide calidad ni excluye filas.

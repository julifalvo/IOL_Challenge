# ADR-011 — IA aplicada: enriquecimiento de instrumentos (principal) y resumen de EDA (secundario)

- **Estado:** propuesto
- **Requisitos:** R-28, R-29, R-30

## Decisión
- **Principal:** clasificar y enriquecer instrumentos con LLM, solo para lo que las reglas no resuelven, con validación por schema, contraste con reglas, caché por hash y accuracy medida sobre 60 símbolos etiquetados.
- **Secundario:** resumen ejecutivo del EDA a partir de métricas agregadas, con verificación de que cada número citado existe en el input.

## Alcance después de las reglas [MEDIDO, 2026-11-01]
Las reglas y la lista de acciones de ADR-004 dejan solo 10 símbolos y 371 operaciones en `NEEDS_REVIEW`. El mayor aporte de la IA pasa a ser lo que ninguna regla puede inferir: emisor, sector y ticker del subyacente de 598 CEDEARs, más esos 10 casos.

## Política de uso [etapa 09]
- Una consulta por subyacente, sin opciones. Las variantes reciben el mismo resultado.
- Solo se usa una salida `VALID`. `REJECTED` no cumple el schema, `CONFLICT` contradice a una regla y gana la regla, `LOW_CONFIDENCE` no alcanza `config.AI_MIN_CONFIDENCE` [SUPUESTO] y `FAILED` es un lote sin respuesta legible, que se reintenta en otra corrida.
- Un `CONFLICT` descarta toda la respuesta de ese instrumento, también emisor y sector: si erró el tipo, el resto no es confiable.

## Evaluación
- A ciegas: el modelo no recibe `rule_asset_type` ni los hermanos. Si los viera, los copiaría y la accuracy saldría inflada.
- La muestra de 60 símbolos cubre todos los tipos salvo opciones, que no se consultan, e incluye símbolos en `NEEDS_REVIEW`.
- **Límite de la muestra:** las etiquetas las escribió Claude Code, sin revisión humana independiente. Mide la coincidencia con esas etiquetas. Parte de la muestra también tiene el contraste de las listas de data912, y lo demás se apoya en la descripción del dataset. Las etiquetas no se tocaron después de ver los resultados.

### Elección del modelo [MEDIDO, Free Edition, 2026-11-01]
| Modelo | `asset_type` (60) | Ticker (22 CEDEARs) | Error principal |
|---|---|---|---|
| `databricks-qwen3-next-80b-a3b-instruct` | 54 (90,0%) | 21 (95,5%) | Cupones PBI como letras |
| `databricks-gpt-oss-120b` | 50 (83,3%) | 21 (95,5%) | ONs como subsoberanos u `OTRO` |
| `databricks-meta-llama-3-3-70b-instruct` | 47 (78,3%) | 21 (95,5%) | Las 8 ONs como subsoberanos |

- Se usa Qwen. Con 60 símbolos la diferencia es orientativa; el error de ONs de los otros dos es sistemático.
- El ticker no separa a los modelos, y copiar el símbolo ya acierta 20 de 22 (90,9%), contra 21 de 22 de los modelos. La muestra casi no mide esta capacidad: solo tiene 2 casos no triviales.
- Issuer y sector no se evaluaron contra etiquetas.

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

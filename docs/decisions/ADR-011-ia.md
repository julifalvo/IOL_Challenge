# ADR-011 — IA aplicada: enriquecimiento de instrumentos (principal) y resumen de EDA (secundario)

- **Estado:** propuesto
- **Requisitos:** R-28, R-29, R-30

## Decisión
- **Principal:** clasificar y enriquecer instrumentos con LLM, solo para lo que las reglas no resuelven, con validación por schema, contraste con reglas, caché por hash y accuracy medida sobre 60 símbolos etiquetados.
- **Secundario:** resumen ejecutivo del EDA a partir de métricas agregadas, con verificación de que cada número citado existe en el input.

## Por qué el principal es clasificación y no el resumen
Está **dentro** del pipeline: mejora `dim_instrument`, el mapeo a la fuente de cotización y los cortes del análisis. El resumen es consumo; útil, pero no cambia la calidad del dato.

## Integración (completar con smoke test)
| Modo | Disponible | Notas |
|---|---|---|
| `ai_query` en Databricks | | modelo: |
| API externa (offline batch → Volume) | | |
| Manual en claude.ai con prompt versionado | sí | válido según PDF |

## Privacidad
Solo metadatos de instrumentos y agregados. Ningún `id_cliente`, `id_transaccion` ni fila transaccional sale del workspace.

## Limitaciones
Fecha de corte de conocimiento del modelo, no determinismo (mitigado con temperatura 0 + caché + versión de prompt), alucinación de emisores/tickers (mitigado con enums, reglas y muestra evaluada). La IA nunca decide calidad ni excluye filas.

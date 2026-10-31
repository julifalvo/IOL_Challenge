# ADR-012 — Optimizaciones de performance

- **Estado:** propuesto
- **Requisitos:** R-25

## Decisión
- Liquid clustering en `fact_trades` y `fact_trade_market` por `(effective_trade_date_key, instrument_sk)`; en `dim_client` por `client_id`.
- `OPTIMIZE` después de cada carga Gold; `ANALYZE TABLE ... COMPUTE STATISTICS FOR ALL COLUMNS`.
- Sin particionado físico por fecha ni Z-ORDER: liquid clustering los reemplaza y a este volumen las particiones crearían archivos chicos.
- Sin cache (no disponible en serverless; además el patrón es batch).

## Por qué esas columnas
q1, q2 y q5 filtran/agrupan por instrumento; q3 y q4 por período; el join point-in-time de SCD2 por cliente y fecha.

## Evidencia `[MEDIR]`
| Consulta | Escala | Antes (s) | Después (s) | Archivos leídos antes/después |
|---|---|---|---|---|
| q1 | 100x | | | |
| q2 | 100x | | | |

A 1x se espera diferencia nula o mínima; se reporta igual como argumento de que la optimización se justifica por escala, no por el dataset actual.

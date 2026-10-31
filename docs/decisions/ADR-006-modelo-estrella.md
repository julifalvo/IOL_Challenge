# ADR-006 — Modelo dimensional en estrella con hecho de mercado separado

- **Estado:** propuesto
- **Requisitos:** R-03, R-11, R-12

## Decisión
- Estrella con `fact_trades` (granularidad: una operación) y dimensiones `dim_client` (SCD2), `dim_instrument` (SCD1), `dim_channel`, `dim_date`, `dim_currency`.
- `fact_market_quotes` (instrumento × día × proveedor) y `fact_trade_market` (1:1 con trades) **separados** de `fact_trades`.
- `fact_trades` contiene las 100.000 filas, incluidas las ERROR, con `is_analytics_eligible`.

## Por qué
- Las preguntas de negocio son agregaciones por cliente, instrumento, canal y mes → estrella es el patrón natural y fácil de consultar para Analytics.
- Separar el enriquecimiento de mercado permite re-bajar o cambiar el proveedor de cotizaciones sin reescribir trades, y versionar el método de matching (`match_method`).
- Mantener todas las filas en el hecho cumple trazabilidad; el filtro es una columna, no una pérdida.

## Alternativas
| Opción | Por qué no |
|---|---|
| One Big Table desnormalizada | Rápida para consultar, pero el SCD2 de cliente obliga a reescribir toda la tabla en cada cambio |
| Data Vault | Sobredimensionado para 1 fuente y un equipo de Analytics consumidor |
| Columnas de mercado dentro de `fact_trades` | Acopla dos ciclos de vida distintos |

## Por qué SCD1 en instrumentos
Los atributos de instrumento son de referencia (corregimos clasificaciones, no cambian en el tiempo). La trazabilidad de cambios de mapeo se cubre con `mapping_version` y git de overrides.

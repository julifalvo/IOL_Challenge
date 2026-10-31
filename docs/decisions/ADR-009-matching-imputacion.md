# ADR-009 — Benchmark de mercado, matching temporal e imputación

- **Estado:** propuesto
- **Requisitos:** R-16, R-19

## Decisión
- **Benchmark = cierre diario** del mismo símbolo y moneda, en la escala del trade. Se etiqueta como "benchmark de cierre", no "precio al instante": la hora del dataset no es confiable (ADR-003) y las fuentes gratuitas no dan intradiario histórico.
- **Fecha:** `effective_trade_date` (sábado → viernes).
- **Imputación** para días sin cotización: último cierre previo con antigüedad ≤ 3 días hábiles (`PRIOR_DAY_IMPUTED`, se guarda fecha usada y antigüedad). Más allá → `UNAVAILABLE`. Nunca cotización posterior.
- **"Por encima del mercado":** `price > close × (1 + τ)`. τ inicial 0,5% `[SUPUESTO]`; calibrar con la mediana del rango diario relativo `(high − low)/close` por `asset_type` y documentar el valor final. Si hay high/low, reportar además `position_vs_range` (por encima del máximo del día = señal fuerte).
- **Lado:** comprar por encima es desfavorable para el cliente; vender por encima es favorable (`is_unfavorable_for_client`).

## Por qué 3 días hábiles
Cubre fin de semana largo + un feriado sin arrastrar precios de una semana atrás en instrumentos volátiles. Parámetro en config; la cobertura reporta cuántas operaciones dependen de imputación.

## Alternativas
| Opción | Por qué no |
|---|---|
| Interpolar entre cierre anterior y posterior | Usa información futura |
| Promedio de la semana | Diluye movimientos reales |
| Comparar contra el subyacente en USD × FX (CEDEARs) | Requiere ratio de conversión y FX; posible mejora futura documentada |
| τ = 0 | Con benchmark de cierre, ~50% de las operaciones quedaría "por encima" por puro ruido intradiario |

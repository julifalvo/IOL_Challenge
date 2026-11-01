# ADR-009 — Benchmark de mercado, matching temporal e imputación

- **Estado:** aceptado (2026-11-01, τ calibrada con la cobertura completa de la etapa 05)
- **Requisitos:** R-16, R-19

## Decisión
- **Benchmark = cierre diario** del mismo símbolo y moneda, en la escala del trade. Se etiqueta como "benchmark de cierre", no "precio al instante": la hora del dataset no es confiable (ADR-003) y las fuentes gratuitas no dan intradiario histórico.
- **Fecha:** `effective_trade_date` (sábado → viernes).
- **Imputación** para días sin cotización: último cierre previo con antigüedad ≤ 3 días hábiles (`PRIOR_DAY_IMPUTED`, se guarda fecha usada y antigüedad). Más allá → `UNAVAILABLE`. Nunca cotización posterior.
- **"Por encima del mercado":** `price > close × (1 + τ)`, con τ por `asset_type`, igual a la mitad de la mediana del rango diario relativo `(high − low) / close` de ese tipo. Si hay high/low del mismo día, se reporta además `position_vs_range` (por encima del máximo del día = señal fuerte).
- **Lado:** comprar por encima es desfavorable para el cliente; vender por encima es favorable (`is_unfavorable_for_client`).

## Calibración de τ [MEDIDO, 2026-11-01; `config.ABOVE_MARKET_TOLERANCE_BY_TYPE`]
| `asset_type` | Rango diario mediano | τ |
|---|---:|---:|
| ACCION_LOCAL | 5,2% | 2,6% |
| CEDEAR | 2,7% a 3,1%, según proveedor | 1,5% |
| BONO_SOBERANO | 1,6% | 0,8% |
| BONO_SUBSOBERANO | 2,6% | 1,3% |
| LETRA_LECAP_BONCAP | 1,25% | 0,6% |
| ON | 1,8% | 0,9% |
| WARRANT | 7,6% | 3,8% |

- **Por qué la mitad del rango:** el benchmark es el cierre, y la hora de la operación no es confiable. Una operación normal puede estar en cualquier punto del día, así que su distancia al cierre es del orden de medio rango sin que haya nada raro.
- **Por qué no el 0,5% inicial:** en acciones daba 43% de las operaciones "por encima" y en CEDEARs 39%, casi todo ruido intradiario [MEDIDO en local; que sea ruido es HIPÓTESIS].
- **τ no es un umbral de sobreprecio.** Sigue habiendo operaciones "por encima" por puro azar. Por eso la consulta por cliente de la etapa 08 compara contra la tasa base con un test binomial.
- Tipos sin datos, como opciones: se deja el 0,5% de `config.ABOVE_MARKET_TOLERANCE`. No se usa mientras no tengan cotizaciones.

## Sesgo de la fuente y comparabilidad
Una operación sin serie válida por sesgo queda `SOURCE_BIASED`, o `SCALE_SUSPECT` si es por escala; no `UNAVAILABLE`. Sigue en el denominador de la cobertura: "no hay dato" y "hay dato, pero no sirve" son problemas distintos, y los dos se ven en `iol_ops.quote_coverage`.

## Días anómalos
El 08/01 data912 ya baja 13% en AL30 y AL30D, y el dataset sigue en el nivel anterior: 314 operaciones con +14% de desvío, que un día después desaparece [MEDIDO, ADR-008]. Hay 25 pares de símbolo y día con al menos 5 operaciones y mediana a más de 10% del cierre, con 572 operaciones en total [MEDIDO en local]. **No se corrigen ni se excluyen:** un filtro con ese criterio también borraría movimientos reales, como pueden ser los de `BIOX` en febrero, y una lista de fechas sería ajustar a mano. Quedan como una limitación del benchmark de cierre. Si la consulta por cliente de la etapa 08 sale dominada por esos días, se revisa ahí.

## Por qué 3 días hábiles
Cubre fin de semana largo + un feriado sin arrastrar precios de una semana atrás en instrumentos volátiles. Parámetro en config; la cobertura reporta cuántas operaciones dependen de imputación.

## Alternativas
| Opción | Por qué no |
|---|---|
| Interpolar entre cierre anterior y posterior | Usa información futura |
| Promedio de la semana | Diluye movimientos reales |
| Comparar contra el subyacente en USD × FX (CEDEARs) | Requiere ratio de conversión y FX; posible mejora futura documentada |
| τ = 0 | Con benchmark de cierre, ~50% de las operaciones quedaría "por encima" por puro ruido intradiario |

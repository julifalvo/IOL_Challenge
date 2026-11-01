# ADR-004 — Mapeo explícito de instrumentos y variantes de liquidación

- **Estado:** propuesto
- **Requisitos:** R-22, R-17

## Contexto
- 1.445 símbolos; 471 en USD, de los cuales 279 tienen un símbolo base en ARS al quitar el sufijo.
- Falsos positivos del sufijo: `YPFD`, `CARC`, `C`, `BA.C`, `AMD`, `HD`, `BA37D`, `BB37D` son símbolos en ARS.
- ONs en USD sin base ARS (`YM34D`, `TLCOD`, …).
- `AAPLD` (≈13,44) y `AAPLC` (≈13,19) tienen precios distintos: MEP y cable no son intercambiables.
- 3 símbolos con >1 descripción (AZN, IRS2W, X15Y6).

## Decisión
Tabla de mapeo explícita y versionada (`iol_ops.instrument_mapping`) construida por capas: reglas determinísticas → IA para casos ambiguos → overrides manuales en git. Una fila por símbolo, con la descripción y la moneda más frecuentes. La variante de liquidación se asigna **solo** si la moneda es USD y existe el símbolo base en ARS con el mismo subyacente. Cada variante es un instrumento distinto con su propia cotización; nunca se compara AL30D contra AL30.

## Reglas [MEDIDO sobre el CSV completo, 2026-11-01]
- **Acciones locales:** una lista de tickers de data912 guardada en `mapping/local_stocks.csv`, que se actualiza con `tools/quotes_fetcher/refresh_local_stocks.py`. Las descripciones no permiten reconocerlas ("YPF", "Aluar"). De sus 96 símbolos, 87 están en el dataset (18.155 operaciones), y ninguna descripción la contradice. Es una foto: lo que falta sigue a la IA.
- **Opciones:** símbolo = descripción y patrón con strike decimal (`COMC47.0AB`, `COMC102.AB`). Son 295 símbolos y 2.016 operaciones, no 231 y 1.824. El subyacente sale de un prefijo en `config.OPTION_UNDERLYING`; un prefijo desconocido no se adivina.
- **Variante:** USD, sufijo D o C (también `.D`, como `BMA.D`), y base en ARS con la misma descripción o el mismo tipo. Una variante sin tipo hereda el de su base.

**Resultado** [MEDIDO, Spark local]: 1.445 símbolos mapeados. `NEEDS_REVIEW` baja de 178 símbolos y 19.241 operaciones a 10 símbolos y 371 operaciones. El MEP implícito da ≈1.447 y el CCL ≈1.488; de 281 variantes, solo `TEAMD` (2.208, 3 operaciones) queda fuera de 1.000–2.000.

## Limitaciones
- Las ON en USD como `YM34D` quedan `USD_OTHER`, aunque tengan un símbolo en pesos `YM34O`: el sufijo de la base no sigue la regla. No afecta precios, porque cada símbolo tiene su propia cotización; solo la etiqueta. Se puede corregir con reglas para ON si hace falta.
- Hay variantes con otro formato (`TGSUD` de `TGSU2`, `GOGLD` de `GOOGL`) que quedan `USD_OTHER`, salvo override.

## Alternativas
| Opción | Por qué no |
|---|---|
| Parsear sufijo D/C | Falla con YPFD, C, BA.C, bonos provinciales |
| Agrupar por descripción | La descripción no distingue variantes y tiene duplicados/truncados |
| Convertir todo a ARS con un tipo de cambio | Introduce un FX que no está en el dataset y oculta la brecha MEP/CCL |

## Cómo agregar un símbolo nuevo
Aparece como `DQ030_INSTRUMENT_UNMAPPED` → correr notebook 04 (reglas + IA) → si queda ambiguo, agregar override → rerun idempotente.

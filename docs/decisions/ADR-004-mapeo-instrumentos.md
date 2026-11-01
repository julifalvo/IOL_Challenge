# ADR-004 — Mapeo explícito de instrumentos y variantes de liquidación

- **Estado:** propuesto
- **Requisitos:** R-22, R-17

## Contexto
- 1.445 símbolos; 471 en USD, de los cuales 279 tienen un símbolo base en ARS al quitar el sufijo.
- Falsos positivos del sufijo: `YPFD`, `CARC`, `C`, `BA.C`, `AMD`, `HD`, `BA37D`, `BB37D` son símbolos en ARS.
- Muchos símbolos en USD no tienen base por sufijo, pero sí un símbolo en ARS con la misma descripción: `YM34D` y `YM34O`, `TGSUD` y `TGSU2`.
- `AAPLD` (≈13,44) y `AAPLC` (≈13,19) tienen precios distintos: MEP y cable no son intercambiables.
- 3 símbolos con >1 descripción (AZN, IRS2W, X15Y6).

## Decisión
Tabla de mapeo explícita y versionada (`iol_ops.instrument_mapping`) construida por capas: reglas determinísticas → IA para casos ambiguos → overrides manuales en git. Una fila por símbolo, con la descripción y la moneda más frecuentes. La variante de liquidación se asigna **solo** si la moneda es USD, el símbolo termina en D o C, y hay un símbolo en ARS con el mismo subyacente: por sufijo, o por ser el único con la misma descripción. Cada variante es un instrumento distinto con su propia cotización; nunca se compara AL30D contra AL30.

## Reglas [MEDIDO sobre el CSV completo, 2026-11-01]
- **Acciones locales:** una lista de tickers de data912 guardada en `mapping/local_stocks.csv`, que se actualiza con `tools/quotes_fetcher/refresh_local_stocks.py`. Las descripciones no permiten reconocerlas ("YPF", "Aluar"). De sus 96 símbolos, 87 están en el dataset (18.155 operaciones), y ninguna descripción la contradice. Es una foto: lo que falta sigue a la IA.
- **Opciones:** símbolo = descripción y patrón con strike decimal (`COMC47.0AB`, `COMC102.AB`). Son 295 símbolos y 2.016 operaciones, no 231 y 1.824. El subyacente sale de un prefijo en `config.OPTION_UNDERLYING`; un prefijo desconocido no se adivina.
- **Variante por sufijo:** USD, sufijo D o C (también `.D`, como `BMA.D`), y base en ARS con la misma descripción o el mismo tipo.
- **Variante por descripción:** USD, sufijo D o C, sin base por sufijo, y **un único** símbolo en ARS con la misma descripción. Con dos o más no se elige: queda `USD_OTHER`, para no inventar una relación.
- Una variante sin tipo hereda el de su base.
- **Control:** el cambio implícito de cada variante, en el notebook 04, con `config.VARIANT_FX_RANGE` [SUPUESTO]. "Mismo tipo" es más laxo que "mismo subyacente": dos CEDEARs distintos se podrían unir, y este control lo mostraría.
- **Orden de las capas:** reglas → IA → override de tipo → proveedor → demás overrides. El proveedor sale del tipo final, así que un tipo corregido a mano o por IA también recibe cotizaciones. La IA solo completa `NEEDS_REVIEW` y campos vacíos.
- **Overrides:** se validan al leerlos y fallan si un valor no es válido o el símbolo no existe.
- **Validación independiente:** el notebook 04 compara la clasificación con las listas por tipo de data912. La de acciones no cuenta como independiente: es la misma que usa la regla.

**Resultado** [MEDIDO en Free Edition]: 1.445 símbolos mapeados. `NEEDS_REVIEW` baja de 178 símbolos y 19.241 operaciones a 10 símbolos y 371 operaciones. Hay 403 variantes: 374 `USD_MEP` y 29 `USD_CCL`; 68 símbolos quedan `USD_OTHER`. 122 de las variantes salen de la regla por descripción, con cambio implícito de 1.191 a 1.645. De las 403, solo `TEAMD` (2.208, 3 operaciones) queda fuera de 1.000–2.000.

**Validación contra data912** [MEDIDO, 2026-11-01]: de 930 símbolos en sus listas independientes (CEDEARs, bonos, letras, ON), 923 coinciden, 7 quedan en `NEEDS_REVIEW` y 0 están mal clasificados.

## Limitaciones
- Un símbolo con otra descripción y otro formato (`GOGLD` de `GOOGL`) queda `USD_OTHER`, salvo override. No afecta precios, porque cada símbolo tiene su propia cotización; solo la etiqueta.
- `DQ032` no controla lo que no tiene fecha en la descripción (por ejemplo "Step Up 2030"): 45 bonos, letras y ON [MEDIDO].
- La lista de acciones y las listas de validación son de hoy, no del período del dataset.

## Alternativas
| Opción | Por qué no |
|---|---|
| Parsear sufijo D/C | Falla con YPFD, C, BA.C, bonos provinciales |
| Agrupar por descripción | La descripción no distingue variantes y tiene duplicados/truncados |
| Convertir todo a ARS con un tipo de cambio | Introduce un FX que no está en el dataset y oculta la brecha MEP/CCL |

## Cómo agregar un símbolo nuevo
Aparece como `DQ030_INSTRUMENT_UNMAPPED` → correr notebook 04 (reglas + IA) → si queda ambiguo, agregar override → rerun idempotente.

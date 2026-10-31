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
Tabla de mapeo explícita y versionada (`iol_ops.instrument_mapping`) construida por capas: reglas determinísticas → IA para casos ambiguos → overrides manuales en git. La variante de liquidación se asigna **solo** si la moneda es USD y existe el símbolo base en ARS con el mismo subyacente. Cada variante es un instrumento distinto con su propia cotización; nunca se compara AL30D contra AL30.

## Alternativas
| Opción | Por qué no |
|---|---|
| Parsear sufijo D/C | Falla con YPFD, C, BA.C, bonos provinciales |
| Agrupar por descripción | La descripción no distingue variantes y tiene duplicados/truncados |
| Convertir todo a ARS con un tipo de cambio | Introduce un FX que no está en el dataset y oculta la brecha MEP/CCL |

## Cómo agregar un símbolo nuevo
Aparece como `DQ030_INSTRUMENT_UNMAPPED` → correr notebook 04 (reglas + IA) → si queda ambiguo, agregar override → rerun idempotente.

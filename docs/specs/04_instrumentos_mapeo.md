# Spec 04 — Instrumentos, variantes de liquidación y mapeo (Task 03/04)

**Requisitos:** R-22 (y base de R-16, R-17) · **ADR:** 004, 005

## Problema
1.445 símbolos, descripciones sucias, variantes de liquidación (AL30 / AL30D / AL30C) y falsos positivos de sufijo (YPFD, CARC, C, BA.C, HD, AMD, BA37D en ARS). El mapeo a la fuente de cotización y a la escala de precio tiene que ser **explícito, versionado y auditable**.

## Diseño en 3 capas (de más confiable a menos)
1. **Reglas determinísticas** (`classify_by_rules`):
   - Opciones primero: símbolo con patrón `^[A-Z]{3,5}[CV]\d{3,6}[A-Z]{0,2}$` y descripción = símbolo → OPCION (C = call, V = put; subyacente = prefijo, ej. GFG → GGAL). ~231 símbolos.
   - `asset_type` por patrones de descripción: empieza con "Cedear" → CEDEAR; contiene "Lecap/Boncap/Lt Rep Arg/Letra" → LETRA_LECAP_BONCAP; "Bono/Bonos Rep/Tesoro/Tasa Dual/Bopreal" → BONO_SOBERANO; "Pcia/Prov/Provincia/Ciudad" → BONO_SUBSOBERANO; "ON/Oblig" → ON; "Warrant" → WARRANT; resto → `NEEDS_REVIEW`.
   - `settlement_variant`: **solo** si `currency = USD` y existe el símbolo base en ARS con la misma descripción (o mismo `underlying_key`): sufijo D → USD_MEP, C → USD_CCL. Si USD sin base ARS (ONs en dólares como YM34D, TLCOD) → USD_OTHER. ARS → ARS siempre, aunque termine en D/C.
   - `underlying_key`: símbolo base si la variante se validó; si no, el propio símbolo.
   - `maturity_date`: regex `dd/mm/aa(aa)` en descripción.
2. **IA** (spec 09) para `NEEDS_REVIEW` y para `issuer`, `sector`, `underlying_ticker_foreign` (CEDEARs).
3. **Overrides manuales** `src/iol_pipeline/mapping/overrides.csv` (`symbol, field, value, reason, author, date`): gana sobre todo. Se versiona en git.

Resultado → `iol_ops.instrument_mapping` → `iol_gold.dim_instrument` (SCD1, `MERGE` por `symbol`).

## Mapeo a proveedor (`provider_symbol`) y escala
- Una función por proveedor `to_provider_symbol(symbol, asset_type, settlement_variant)`; ejemplo yfinance: `AL30 → AL30.BA`, `AL30D → AL30D.BA`, `AAPL → AAPL.BA` (verificar cada uno en el spike de spec 05; lo que no se verifica queda `UNVERIFIED`).
- `price_scale_factor` por (provider, asset_type): bonos/letras cotizados por 100 VN en el proveedor → 0,01; acciones/CEDEARs → 1. **Se valida empíricamente**, no se asume: sanity check de spec 05.

## Tests
- `YPFD` ARS → ACCION_LOCAL (si descripción "YPF" y regla de acciones/override), `settlement_variant=ARS`.
- `AL30D` USD con base `AL30` ARS → USD_MEP, `underlying_key=AL30`.
- `C` (Citigroup) ARS → CEDEAR, `settlement_variant=ARS`.
- `YM34D` USD sin base ARS → USD_OTHER.
- Override pisa regla.
- Cada símbolo del dataset tiene exactamente una fila en el mapping.

## Salida visible
1. Conteo por `asset_type` × `currency` × `asset_type_source`.
2. Tabla de variantes: `underlying_key` con ≥2 variantes (AL30/AL30D/AL30C, AAPL/AAPLD/AAPLC…), con precios medianos → MEP/CCL implícitos.
3. Lista de falsos positivos de sufijo resueltos.
4. `NEEDS_REVIEW` restantes (debería ser 0 después de IA + overrides).

## Criterios de aceptación
- [ ] 1.445 símbolos mapeados, `asset_type` sin `NEEDS_REVIEW`.
- [ ] ADR-004 documenta reglas, casos borde y cómo agregar un símbolo nuevo.
- [ ] Tests verdes.

# ADR-005 — Escala de precios (por 1 VN vs por 100 VN) y errores de escala

- **Estado:** propuesto
- **Requisitos:** R-09, R-16

## Contexto
- En el dataset, los bonos cotizan **por 1 VN**: AL30 ≈ 876 ARS, AL30D ≈ 0,607 USD, GD30D ≈ 0,618, AE38D ≈ 0,79. El MEP implícito (≈1.440) es consistente.
- Las fuentes de mercado suelen publicar bonos por 100 VN.
- Caso extremo: `T30A7` qty 1.503.210.011 a precio 0,0101 (mediana del símbolo: 1,08) y `T31Y7` qty 1.427.140.785 a 0,0100 (mediana 1,02). Las 5 mayores cantidades son de IOLnet en letras/bonos del Tesoro.
- 13 filas tienen precio a ×50 o ×1/50 de la mediana de su símbolo [MEDIDO]. 8 de ellas son de IOLnet y las 8 tienen precio que redondea a 0,01 o 1,00 [MEDIDO]: `SATL` 1,0046, `TGNO4` 0,9999, `METR` 0,9997, `T31Y7` 0,0100, `AL30` 0,0100 (mediana 876), `DNC7D` 0,0100, `T13F6` 0,0100 y `T30A7` 0,0101. Que corresponda a un valor por defecto en la carga de IOLnet es [HIPÓTESIS], a confirmar con el dueño del dato.

## Decisión
1. `gross_amount = qty × price` en la escala del dataset (por 1 VN). Se documenta en el diccionario.
2. `dim_instrument.price_scale_factor` lleva el precio del proveedor a esa escala.
3. **Sanity check automático** por símbolo: mediana de `price_trade / close` en [0,5; 2] o `SCALE_SUSPECT`.
4. `DQ021_POSSIBLE_SCALE_ERROR` (ERROR) cuando el precio de un trade está a ×50 o ×1/50 de la mediana del símbolo: no entra a métricas de importe ni a comparación con mercado; se conserva y se reporta.
5. Outliers (`DQ020`) se evalúan sobre log(importe) por (asset_type, moneda), no sobre cantidad.

## Por qué no "corregir" multiplicando por 100
No sabemos si el error está en precio, en cantidad o en ambos (el importe puede ser correcto). Corregir sin confirmación del origen sería inventar datos. Se marca y se escala al dueño del dato (canal IOLnet).

## Pregunta para la entrevista
¿IOLnet carga cantidades en otra unidad para letras? ¿El importe liquidado está disponible para validar?

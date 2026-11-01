# ADR-008 — Fuente de cotizaciones y modo de adquisición

- **Estado:** aceptado (spike medido desde local y desde Free Edition, 2026-11-01); a confirmar con la cobertura de la etapa 05
- **Requisitos:** R-17, R-18, R-20

## Contexto
- El PDF da libertad de fuente y pide justificarla para el mercado local.
- Free Edition restringe la salida a internet; no hay lista pública de dominios permitidos.

## Egress desde Free Edition (smoke test, 2026-11-01)
`requests.get` a la raíz de cada dominio desde serverless (`smoke_run_id = d96b79bc-…`). Criterio vigente: HTTP 403, 407 y 429 son `INCONCLUSO`, porque también los puede devolver un intermediario; otra respuesta HTTP es OK; error de conexión o timeout es FAIL.

| Proveedor | URL probada | Respuesta [MEDIDO] |
|---|---|---|
| API IOL | `https://api.invertironline.com` | HTTP 200, `Server: cloudflare` |
| BYMA Open Data | `https://open.bymadata.com.ar` | HTTP 200, `Server: nginx` |
| data912 | `https://data912.com` | HTTP 200, `Server: cloudflare` |
| Yahoo Finance | `https://query1.finance.yahoo.com` | HTTP 429 (Too Many Requests), `Server: ATS`: `INCONCLUSO` |

Qué prueba y qué no:
- **Prueba:** IOL, BYMA y data912 respondieron HTTP 200.
- **No prueba:** que los endpoints de datos devuelvan series. Solo se pidió la raíz, sin autenticación ni parámetros. Eso lo mide el spike, abajo.
- **Yahoo:** la raíz da 429, pero el endpoint de datos respondió desde serverless: 11 de 16 símbolos con filas [MEDIDO]. Que sea rate limiting por IP compartida es [HIPÓTESIS]. Que aguante muchos símbolos no está medido.
- El fetcher local (`tools/quotes_fetcher`) deja de ser necesario por egress. Sigue como plan B por rate limiting o términos de uso.

## Spike (8 símbolos del spec 05 + 8 CEDEARs para medir sesgo; 2025-12-15..2026-03-13)

Se obtiene con `python -m tools.quotes_fetcher.spike <csv>`, y desde Free Edition con `notebooks/00_quotes_spike.py`. Las 48 combinaciones de proveedor y símbolo dan las mismas filas y la misma mediana de cierre en los dos lados [MEDIDO, 2026-11-01]. No se guardan datos crudos.

| Proveedor | Desde Free Edition [MEDIDO, 2026-11-01] | Desde local [MEDIDO, 2026-11-01] | Cobertura de los 8 del spec | OHLC | Escala | Límites / términos | Notas |
|---|---|---|---|---|---|---|---|
| API IOL (serie histórica) | dominio OK; datos no probados | HTTP 401 sin credenciales | **no ejecutado** | no ejecutado | no ejecutado | no evaluado | requiere credenciales personales; no se usan |
| BYMA Open Data | OK | OK | 8/8, 60 días cada uno | sí | ver hallazgos | no evaluado | sufijo ` 24HS`; sin él o con ` 48HS` no devuelve datos |
| data912 | OK | OK | 5/8; faltan AAPLD, T30J6 y YM34D | sí | ×100 en bonos, ×1 en el resto | sin términos publicados | proyecto personal, ver hallazgos |
| Yahoo Finance (`.BA`) | OK en el endpoint de datos; la raíz da HTTP 429 | OK | 3/8; solo AAPL, AAPLD e YPFD | sí | ver hallazgos | no evaluado | sin bonos ni letras |

**Mediana de la relación diaria: precio de operación del día / cierre del mismo día** [MEDIDO, 2026-11-01]. Lo esperable es ≈0,01 en bonos y letras cotizados por 100 VN, y ≈1 en el resto:

| Símbolo | data912 | BYMA | Yahoo |
|---|---:|---:|---:|
| AL30 | 0,0100 | **0,0115** | — |
| AL30D | 0,0100 | **0,0115** | — |
| AL30C | 0,0100 | **0,0115** | — |
| T30J6 | — | 0,0100 | — |
| YM34D | — | **0,0104** | — |
| YPFD | 1,0009 | **10,0088** | **10,0088** |
| AAPL | 1,0004 | 1,0040 | 1,0015 |
| MSFT | 1,0001 | **1,0086** | 1,0001 |
| SPY | 1,0006 | **3,0259** | **3,0024** |
| NVDA | 1,0029 | 1,0047 | 1,0024 |
| GOOGL | 1,0011 | 1,0037 | 1,0018 |
| MELI | 1,0016 | 1,0013 | 1,0013 |
| AMZN | 1,0021 | 1,0016 | 1,0016 |
| AAPLD | — | 1,0018 | 0,9994 |
| MSFTD | — | 1,0042 | 0,9965 |
| SPYD | — | **3,0243** | **3,0009** |

En un activo sin sesgo la relación diaria igual se dispersa: p10 y p90 quedan en torno a ±1,5–2% en CEDEARs y acciones, porque se compara el precio operado contra el cierre.

### Hallazgos del spike [MEDIDO]
1. **BYMA da los bonos soberanos ajustados.** Día a día, BYMA / data912 en AL30 es constante en 0,7628 hasta el 07/01 y en 0,8715 desde el 08/01. Se ve igual en AL30D. Que sea un ajuste retroactivo por el pago de renta y amortización es [HIPÓTESIS].
   - **No lo detecta el sanity check de ADR-005:** con factor 0,01, la relación da 1,15, dentro del rango [0,5; 2]. Usarla marcaría casi todas las operaciones de AL30 como "por encima del mercado" por un sesgo de la fuente.
2. **BYMA y Yahoo dan algunas series en otra escala:** `YPFD` a ×0,1, y SPY y SPYD a ×1/3, contra data912 y el dataset. El sanity check sí los detecta. Un factor por (proveedor, `asset_type`) no alcanza, porque AAPL en esos mismos proveedores da ×1: el factor tiene que ser por símbolo.
3. **data912 sigue al dataset casi al centavo.** En AL30, del 05/01 al 12/01 el cierre / 100 y la mediana diaria difieren menos de 0,3%, salvo un día.
4. **El 08/01 el proveedor y el dataset no coinciden en AL30 ni en AL30D.** data912 ya baja 13% (874,5) y el dataset sigue en 1.000,83; el 09/01 vuelven a coincidir. Es el único día de 49 con relación fuera de ≈0,01. En ese día, comparar contra el cierre da un desvío falso de 14%. Que sea la fecha de corte de renta o amortización es [HIPÓTESIS].
5. **BYMA también ajusta CEDEARs.** Sus cierres dan una relación mayor que data912 en MSFT (+0,86% contra +0,01%), NVDA, AAPL y GOOGL. El umbral de "por encima del mercado" es 0,5% (ADR-009): el sesgo solo ya lo supera en MSFT. BYMA / Yahoo da 1,0000 en MELI y AMZN, y es casi constante en los demás (NVDA: 0,9976 a 0,9977). Que sea un ajuste por dividendos es [HIPÓTESIS].
6. **YM34D, la única ON probada, da 4% de sesgo con BYMA** (0,0104; p10 y p90: 0,0103 y 0,0108), su única fuente. T30J6, una letra, da 0,0100 limpio. Que sea ajuste por renta es [HIPÓTESIS]; con un solo símbolo no se puede generalizar a todas las ONs.
7. **data912 se declara proyecto personal.** Su descripción dice que es para fines educativos, que cualquier parecido con datos reales es coincidencia y que se puede dar de baja. No publica términos de uso ni garantías.

**Límite del spike:** son 16 de 1.445 símbolos. La cobertura y la escala del resto se miden en la etapa 05.

## Decisión (aceptada el 2026-11-01; a confirmar con la cobertura de la etapa 05)
| `asset_type` | Primario | Fallback | Por qué |
|---|---|---|---|
| Bonos soberanos | data912 | ninguno | BYMA los da ajustados; Yahoo no los tiene |
| Acciones locales | data912 | Yahoo o BYMA, solo con escala validada por símbolo | `YPFD` está a ×0,1 en los otros dos |
| CEDEARs en ARS | data912 | Yahoo, solo con escala validada por símbolo | BYMA da un sesgo comparable al umbral de la métrica; Yahoo da SPY a ×1/3 |
| CEDEARs en USD (variantes D/C) | **a decidir en la etapa 05**, entre Yahoo y BYMA | el otro | data912 no las tiene. En las 3 probadas ninguna es claramente mejor, y las dos dan SPYD a ×1/3 |
| Letras | BYMA | ninguno | Es el único con esas series; T30J6 da limpio |
| ONs | BYMA, con sesgo a medir | ninguno | Es el único con esas series. En la etapa 05, excluir de la comparación con mercado las series con sesgo medible |

**Trade-off:** el criterio 3 pedía fuente oficial antes que agregador, y para bonos, acciones y CEDEARs en ARS gana un agregador personal porque la serie oficial no sirve como precio de mercado sin desajustar. Eso concentra el riesgo en data912. Si deja de estar disponible, los bonos soberanos quedan sin cobertura, y acciones y CEDEARs pasan a Yahoo o BYMA. Gold pasa a YELLOW (G08, G09), no a RED.

**API de IOL:** no evaluada. Requiere credenciales personales y se decidió no usarlas en este proyecto (2026-11-01). Podría ser una mejor opción si da los bonos sin ajustar; queda como mejora futura.

- Modo: `api` desde Databricks: los 3 proveedores devuelven datos desde serverless [MEDIDO, 16 símbolos]. Si aparecen límites con los 1.445 símbolos, `tools/quotes_fetcher` local → Volume → `quotes_raw` con `FileReplayProvider`. Mismo contrato y misma tabla `quote_requests` en ambos modos.

## Criterios de elección
1. Cobertura de variantes D/C y de soberanos (mayor parte de AL30 = 13.448 operaciones).
2. Histórico diario OHLC para el período.
3. Fuente oficial o cercana al mercado > agregador.
4. Términos de uso compatibles con un repo público (no se publican datos crudos del proveedor si los términos no lo permiten; se publica cobertura y agregados).

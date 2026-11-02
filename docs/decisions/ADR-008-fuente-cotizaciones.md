# ADR-008 — Fuente de cotizaciones y modo de adquisición

- **Estado:** aceptado (spike del 2026-11-01, confirmado con la cobertura completa de la etapa 05)
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

**Límite del spike:** son 16 de 1.445 símbolos. La cobertura y la escala del resto están abajo.

## Medición completa (etapa 05, los 1.445 símbolos)

Descarga desde local, 2025-12-15..2026-03-13 [MEDIDO, 2026-11-01]. "Con filas" es el símbolo con al menos una cotización. El sesgo es la mediana por símbolo de `|relación / factor − 1|`, con al menos 5 días comparables.

| `asset_type` | Símbolos | Operaciones | data912: con filas | Yahoo: con filas | BYMA: con filas | Sesgo data912 | Sesgo Yahoo | Sesgo BYMA |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| CEDEAR | 599 | 54.054 | 156 | 499 | 596 | 0,19% | 0,25% | 0,90% |
| BONO_SOBERANO | 98 | 18.288 | 37 | — | 98 | 0,12% | — | 3,09% |
| ACCION_LOCAL | 88 | 18.166 | 62 | 85 | 88 | 0,28% | 0,44% | 1,11% |
| LETRA_LECAP_BONCAP | 28 | 4.516 | 0 | — | 28 | — | — | 0,09% |
| ON | 315 | 2.551 | 0 | — | 315 | — | — | 4,32% |
| OPCION | 295 | 2.016 | — | — | 0 | — | — | — |
| BONO_SUBSOBERANO | 19 | 313 | 6 | — | 19 | 0,29% | — | 13,13% |
| WARRANT | 2 | 95 | — | — | 2 | — | — | 1,71% |
| OTRO | 1 | 1 | — | — | 0 | — | — | — |

- **El sesgo de BYMA es general y no solo de bonos:** mediana 1,35% y p90 7,43%, contra 0,22% y 0,77% en data912, y 0,28% y 1,03% en Yahoo. Confirma los hallazgos 1, 5 y 6 del spike. Que venga de ajustes por renta y dividendos sigue siendo [HIPÓTESIS].
- **Yahoo y BYMA comparten factores:** 570 de 583 series de Yahoo dan ×1; el resto ×10, ×1/3, ×3 y ×100, casi siempre iguales en BYMA.
- **Yahoo da HTTP 400** en AXIA, EAD, LND, MDLZD y TEFO. BYMA y data912 no dieron errores.
- **Opciones: 0 filas de 295 pedidas** a BYMA. No hay fuente gratuita probada para ellas.

## Decisión
**Una cadena de prioridad por tipo, fijada de antemano** (`config.QUOTE_PROVIDERS_BY_TYPE`). De cada símbolo se usa la primera serie de su cadena que pasa la validación de ADR-005, en dos pasadas: primero las `OK`, y si no hay ninguna, una `THIN` (ver abajo).

| `asset_type` | Cadena | Por qué |
|---|---|---|
| CEDEARs y acciones | data912 → Yahoo → BYMA | Es el orden de menor a mayor sesgo medido. BYMA queda último, por si es el único con la serie |
| Bonos soberanos y subsoberanos | data912 → BYMA | Yahoo no los tiene; BYMA los da ajustados, y se usa solo si la serie pasa la validación |
| Letras | BYMA | Es el único con esas series, y con poco sesgo: 0,09% |
| ONs | BYMA | Es el único con esas series. Con 4,32% de sesgo mediano, la mayoría queda excluida |
| Opciones, warrants, otros | BYMA | Es el único que se probó. Opciones y otros quedan sin datos, y siguen en el denominador |

**Por qué la prioridad se fija antes y no se elige la serie que más se parece a las operaciones:** elegir por parecido sesgaría la métrica de "por encima del mercado", porque siempre ganaría la serie que más la achica. La validación solo excluye.

**Estados de una serie** (`iol_ops.quote_series`), con la mediana de `precio operado / cierre crudo` por día:

| Estado | Condición | ¿Se usa? |
|---|---|---|
| `OK` | al menos `QUOTE_MIN_DAYS` (5) días comparables y sesgo dentro de la tolerancia | sí |
| `THIN` | menos de 5 días comparables, y sesgo dentro de la tolerancia | solo si el símbolo no tiene ninguna `OK` |
| `BIASED` | sesgo mayor a la tolerancia de su tipo, con cualquier cantidad de días | no |
| `SCALE_SUSPECT` | sin un factor candidato que deje la relación en [0,5; 2] | no |
| `NO_OVERLAP` | tiene filas, pero ningún día con operaciones | no |
| `NO_DATA` | sin filas | no |

- **5 días:** con menos, la mediana es más que nada ruido del precio operado contra el cierre. Por eso `THIN` queda como último recurso y se informa aparte.
- **Un `THIN` con sesgo grande se excluye igual:** 189 de 311 series de ON tienen menos de 5 días, y su tipo da 4,32%. Dejarlas pasar por tener pocos días daría cobertura sobre un benchmark que probablemente esté sesgado.

**Trade-off:** el criterio 3 pedía fuente oficial antes que agregador, y gana un agregador personal porque la serie oficial no sirve como precio de mercado sin desajustar. Eso concentra el riesgo en data912. Si deja de estar disponible, los símbolos pasan solos a Yahoo o BYMA en la corrida siguiente, si validan; los bonos soberanos, en su mayoría, no van a validar. Gold pasa a YELLOW (G08, G09), no a RED.

## Resultado (Free Edition, 2026-11-01) [MEDIDO]
- **Series:** 2.936 candidatas. Elegidas: data912 252, Yahoo 339 y BYMA 147, en total 593 `OK` y 145 `THIN`. No elegibles: 684 `BIASED` (BYMA 621, Yahoo 55, data912 8) y 14 `SCALE_SUSPECT`.
- **Cobertura:** 93,8% de las operaciones elegibles. 2.051 operaciones sin dato, y 4.180 excluidas por validación. El detalle está en `docs/quote_coverage.md`.
- **Por API desde Free Edition:** 600 pedidos del primer lote, con 3 errores (BYMA 1, Yahoo 2), unos 10 minutos por lote de 200 por proveedor. El registro de esos 3 se pisó al completarlos desde el Volume: `quote_requests` guarda el último intento de cada pedido. La corrida completa se estima en unos 80 minutos, y **no se ejecutó entera**: el resto salió del Volume. Los 199 pedidos a BYMA sin error de ese lote volvieron vacíos, igual que los mismos símbolos en local, así que BYMA por API solo está probado con filas en el spike.

**Mapeo inicial en `config.PROVIDER_RULES`:** la etapa 04 corre antes y deja cada símbolo `UNVERIFIED`. La etapa 05 pisa proveedor, símbolo y factor con la serie elegida, y deja `MAPPED`, o `UNSUPPORTED` si no hay ninguna.

**API de IOL:** no evaluada. Requiere credenciales personales y se decidió no usarlas en este proyecto (2026-11-01). Podría ser una mejor opción si da los bonos sin ajustar; queda como mejora futura.

- **Modo:** el widget `quotes_source` del notebook 05 elige `api`, desde Databricks, o `volume`: `tools/quotes_fetcher` local → Volume → `quotes_raw` con `FileReplayProvider`. Mismo contrato y misma tabla `quote_requests` en ambos. Lo pedido sin error no se repite, así que una corrida en `volume` completa exactamente lo que `api` dejó con error.

## Criterios de elección
1. Cobertura de variantes D/C y de soberanos (mayor parte de AL30 = 13.448 operaciones).
2. Histórico diario OHLC para el período.
3. Fuente oficial o cercana al mercado > agregador.
4. Términos de uso compatibles con un repo público (no se publican datos crudos del proveedor si los términos no lo permiten; se publica cobertura y agregados).

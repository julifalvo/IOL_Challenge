# ADR-008 — Fuente de cotizaciones y modo de adquisición

- **Estado:** propuesto (spike medido desde local; falta correrlo desde Free Edition)
- **Requisitos:** R-17, R-18, R-20

## Contexto
- El PDF da libertad de fuente y pide justificarla para el mercado local.
- Free Edition restringe la salida a internet; no hay lista pública de dominios permitidos.

## Egress desde Free Edition (smoke test, 2026-11-01)
`requests.get` a la raíz de cada dominio desde serverless (`smoke_run_id = 1cc453e0-…`). Criterio vigente: HTTP 403, 407 y 429 son `INCONCLUSO`, porque también los puede devolver un intermediario; otra respuesta HTTP es OK; error de conexión o timeout es FAIL.

| Proveedor | URL probada | Respuesta [MEDIDO] |
|---|---|---|
| API IOL | `https://api.invertironline.com` | HTTP 200 |
| BYMA Open Data | `https://open.bymadata.com.ar` | HTTP 200 |
| data912 | `https://data912.com` | HTTP 200 |
| Yahoo Finance | `https://query1.finance.yahoo.com` | HTTP 429 (Too Many Requests) |

Qué prueba y qué no:
- **Prueba:** IOL, BYMA y data912 respondieron HTTP 200, así que el modo `api` desde Databricks es viable a nivel red para esos tres.
- **No prueba:** que los endpoints de datos devuelvan series para los símbolos del challenge. Solo se pidió la raíz, sin autenticación ni parámetros. Eso se mide en el spike.
- **Yahoo:** inconcluso. El 429 indica rate limiting, probablemente por IP compartida de serverless [HIPÓTESIS], pero esta corrida no guardó quién respondió. Que se pueda usar desde Databricks sigue sin verificar.
- **Esta corrida usó el criterio anterior** y registró el 429 como OK (9/9). `00_smoke_test` ahora guarda también el header `Server`; falta volver a correrlo.
- Por ahora el fetcher local (`tools/quotes_fetcher`) deja de ser necesario por egress. Sigue como plan B por rate limiting o términos de uso.

## Spike (8 símbolos: AL30, AL30D, AL30C, AAPL, AAPLD, YPFD, T30J6, YM34D; 2025-12-15..2026-03-13)

Se obtiene con `python -m tools.quotes_fetcher.spike <csv>`. Desde Free Edition: `notebooks/00_quotes_spike.py`, **no ejecutado**. No se guardan datos crudos.

| Proveedor | Desde Free Edition | Desde local [MEDIDO, 2026-11-01] | Cobertura de los 8 | OHLC | Escala | Límites / términos | Notas |
|---|---|---|---|---|---|---|---|
| API IOL (serie histórica) | dominio OK | HTTP 401 sin credenciales | **no ejecutado** | no ejecutado | no ejecutado | no evaluado | requiere credenciales personales; no se usan |
| BYMA Open Data | dominio OK | OK | 8/8, 60 días cada uno | sí | ver hallazgos | no evaluado | sufijo ` 24HS`; sin él o con ` 48HS` no devuelve datos |
| data912 | dominio OK | OK | 5/8; faltan AAPLD, T30J6 y YM34D | sí | ×100 en bonos, ×1 en el resto | sin términos publicados | proyecto personal, ver hallazgos |
| Yahoo Finance (`.BA`) | inconcluso, HTTP 429 | OK | 3/8; solo AAPL, AAPLD e YPFD | sí | ver hallazgos | no evaluado | sin bonos ni letras |

**Mediana del precio de operación / mediana del cierre** [MEDIDO, 2025-12-15..2026-03-13]. Lo esperable es ≈0,01 en bonos cotizados por 100 VN y ≈1 en el resto:

| Símbolo | data912 | BYMA | Yahoo |
|---|---:|---:|---:|
| AL30 | 0,0100 | **0,0115** | — |
| AL30D | 0,0100 | **0,0115** | — |
| AL30C | 0,0100 | **0,0115** | — |
| AAPL | 0,9970 | 0,9987 | 0,9965 |
| AAPLD | — | 0,9904 | 0,9886 |
| YPFD | 0,9977 | **9,9883** | **9,9883** |
| T30J6 | — | 0,0101 | — |
| YM34D | — | 0,0105 | — |

### Hallazgos del spike [MEDIDO]
1. **BYMA da los bonos soberanos ajustados.** Día a día, BYMA / data912 en AL30 es constante en 0,7628 hasta el 07/01 y en 0,8715 desde el 08/01. Se ve igual en AL30D. Que sea un ajuste retroactivo por el pago de renta y amortización es [HIPÓTESIS].
   - **No lo detecta el sanity check de ADR-005:** con factor 0,01, la relación da 1,15, dentro del rango [0,5; 2]. Usarla marcaría casi todas las operaciones de AL30 como "por encima del mercado" por un sesgo de la fuente.
2. **BYMA y Yahoo dan `YPFD` a ×0,1:** mediana de cierre 5.466,25, contra 54.725 en data912 y 54.598 en el dataset. El sanity check sí lo detecta. Un factor por (proveedor, `asset_type`), como pide el spec 04, no alcanza para este caso, porque AAPL en esos mismos proveedores da ×1.
3. **data912 sigue al dataset casi al centavo.** En AL30, del 05/01 al 12/01 el cierre / 100 y la mediana diaria difieren menos de 0,3%, salvo un día.
4. **El 08/01 el proveedor y el dataset no coinciden en AL30 ni en AL30D.** data912 ya baja 13% (874,5) y el dataset sigue en 1.000,83; el 09/01 vuelven a coincidir. Es el único día de 49 con relación fuera de ≈0,01. En ese día, comparar contra el cierre da un desvío falso de 14%. Que sea la fecha de corte de renta o amortización es [HIPÓTESIS].
5. **data912 se declara proyecto personal.** Su descripción dice que es para fines educativos, que cualquier parecido con datos reales es coincidencia y que se puede dar de baja. No publica términos de uso ni garantías.

**Límite del spike:** son 8 símbolos. La cobertura y la escala del resto se miden en la etapa 05.

## Decisión (propuesta, a confirmar con la corrida desde Free Edition y la etapa 05)
| `asset_type` | Primario | Fallback | Por qué |
|---|---|---|---|
| Bonos soberanos | data912 | ninguno | BYMA los da ajustados; Yahoo no los tiene |
| Acciones locales | data912 | BYMA o Yahoo, solo con escala validada | `YPFD` está a ×0,1 en los otros dos |
| CEDEARs | BYMA | data912 o Yahoo | BYMA es la fuente oficial y cubre las variantes D/C |
| Letras y ONs | BYMA | ninguno | Es el único con esas series; YM34D da 0,0105, a revisar |

**Trade-off:** el criterio 3 pedía fuente oficial antes que agregador, y para bonos gana un agregador personal porque la serie oficial no sirve como precio de mercado sin desajustar. Si data912 deja de estar disponible, los bonos soberanos quedan sin cobertura y Gold pasa a YELLOW (G08, G09), no a RED.

**API de IOL:** no evaluada. Requiere credenciales personales y se decidió no usarlas en este proyecto (2026-11-01). Podría ser una mejor opción si da los bonos sin ajustar; queda como mejora futura.

- Modo: `api` desde Databricks si hay egress; si no, `tools/quotes_fetcher` local → Volume → `quotes_raw` con `FileReplayProvider`. Mismo contrato y misma tabla `quote_requests` en ambos modos.

## Criterios de elección
1. Cobertura de variantes D/C y de soberanos (mayor parte de AL30 = 13.448 operaciones).
2. Histórico diario OHLC para el período.
3. Fuente oficial o cercana al mercado > agregador.
4. Términos de uso compatibles con un repo público (no se publican datos crudos del proveedor si los términos no lo permiten; se publica cobertura y agregados).

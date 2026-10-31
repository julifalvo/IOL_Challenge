# Fixtures de test

## `trades_small.csv`

Son 40 filas **sintéticas** con el contrato `trades_v1`: mismas 10 columnas, mismo formato de fecha (`...000Z`), precios con 4 decimales y comillas cuando la descripción tiene coma.

- Los IDs son inventados y se reconocen a simple vista (`CLIFX…`, `TXNfixture…`). No hay ninguna fila copiada del CSV original.
- Los símbolos, descripciones y órdenes de magnitud de precio imitan al dataset real, para que las reglas de calidad y de mapeo se ejerciten igual.
- Hora local = UTC−3 (Argentina no tiene horario de verano).

Cada caso está verificado por `tests/unit/test_fixture_cases.py`.

| Caso | Filas (`id_transaccion`) | Regla / ADR que ejercita |
|---|---|---|
| Sábado local | `…007` (sáb 10/01 12:00), `…025` (sáb 28/02, fin de mes), `…035` (sáb 31/01, fin de mes) | DQ010, ADR-003 (fecha efectiva = viernes); cortes SCD2 |
| Sábado UTC pero viernes local | `…008` (10/01 01:30 UTC = vie 09/01 22:30 local) | ADR-003: la fecha de negocio es la local |
| Feriado bursátil | `…009` (lun 16/02, Carnaval) | DQ012 (el dataset real tiene 0 casos) |
| Domingo | `…010` (dom 22/02) | DQ011 |
| Escala anómala tipo T30A7 | `…011` (T30A7 a 0,0101 vs ~1,08, IOLnet) | DQ021 ERROR, ADR-005 |
| Precio ≈0,01 / 1,00 en IOLnet | `…015` (AL30 a 0,0100), `…029` (GGAL a 1,0000) | DQ021, [HIPÓTESIS] valor por defecto (ADR-005) |
| Escala anómala en opción | `…022` (GFGC72000F a 2,00 vs ~148) | DQ021 WARNING en `OPCION` |
| YPFD (ARS terminado en D) | `…004`, `…027`, `…035` | ADR-004: el sufijo no es regla |
| Otros ARS terminados en D/C | `…005` `CARC`, `…006` `BA.C`, `…036` `CARC` | ADR-004 |
| AL30 / AL30D / AL30C | `…001`, `…002`, `…003` (y otras) | ADR-004 variantes de liquidación, ADR-005 escala por 1 VN |
| ID repetido, mismo contenido | `TXNfixture000001` (filas 1 y 16, idénticas) | ADR-002: se inserta una sola vez |
| ID repetido, distinto contenido | `TXNfixture000002` (filas 2 y 17, precio 0,6073 vs 0,6100) | DQ004, ADR-002: ambas versiones se conservan |
| Descripción con coma (entre comillas) | `…018`, `…019`, `…039` | parseo CSV |
| Fuera de rueda 11–17 hs | `…019` (00:00 local), `…034` (07:33 local) | DQ013 INFO |
| Campo vacío | `…030` (`id_cliente` vacío) | DQ002 CRITICAL |
| Fecha no parseable | `…031` (`2026-02-30`) | DQ003 CRITICAL |
| Precio 0 | `…032` | DQ006 ERROR |
| Moneda fuera de dominio | `…033` (`EUR`) | DQ007 ERROR |
| Los 5 canales y cliente multicanal | `CLIFX000001` (App Mobile + Sitio Web Responsive) | `dim_channel`, `preferred_channel` (ADR-007) |
| Primer día y día de corte | `…034` (02/01), `…026` (13/03) | `DATA_CUTOFF` |

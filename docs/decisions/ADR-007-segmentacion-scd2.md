# ADR-007 — Segmentación de clientes y SCD2 por snapshots mensuales

- **Estado:** propuesto
- **Requisitos:** R-13, R-14, R-15

## Contexto
- El dataset solo tiene `id_cliente`: no hay atributos personales. No se inventan.
- Distribución muy asimétrica: p50=1, p75=2, p90=3, p95=4, p99=10 operaciones por cliente; 40.841 de 57.655 (71%) operan una sola vez; máximo 676.
- 1.741 clientes usan más de un canal.

## Decisión
Versionar 4 atributos **operativos derivados**, acumulados hasta cada snapshot mensual (31/01, 28/02, 13/03): `activity_segment`, `preferred_channel`, `dominant_asset_type`, `currency_profile`.

`activity_segment` por operaciones acumuladas: 1 OCASIONAL · 2–3 RECURRENTE · 4–9 ACTIVO · ≥10 MUY_ACTIVO.
- Cortes en la cola (p75, p90–p95, p99) porque cuartiles colapsarían al 71% en un solo grupo sin poder de discriminación.
- Cortes **congelados** en `seg_v1`: si se recalcularan cada run, un cliente cambiaría de segmento sin cambiar su comportamiento.

`preferred_channel` = `channel_code` con más operaciones acumuladas hasta el corte. Desempate determinista entre canales empatados en el máximo:
1. el canal de la operación más reciente (mayor `trade_ts_utc` ≤ corte) entre los empatados;
2. si persiste el empate (misma marca de tiempo), orden alfabético ascendente de `channel_code`.

Así el mismo input da siempre el mismo canal, sin depender del orden de filas ni del motor (pandas/Spark). En la versión inicial es el canal de la primera operación.

Vigencia: la versión del cierre del mes M rige desde el día siguiente (`valid_to` exclusivo). Versión inicial desde la primera operación, con atributos de esa sola operación. Así ningún trade se asocia a un atributo calculado con información posterior.

## Alternativas
| Opción | Por qué no |
|---|---|
| Fixture sintético de cambios | Demuestra la mecánica pero no un caso real; se usa solo en tests |
| Segmento por ventana móvil (últimos 30 días) | Más cambios, pero oscila mucho con 71% de una operación; menos interpretable |
| Snapshot diario | Correcto en producción; a 3 meses el mensual es suficiente para demostrar y más legible |
| Segmentar por importe | Mezcla monedas y escalas de VN; requiere FX |

## Evidencia `[MEDIR]`
Matriz de transición ene→feb→mar y cliente demostrado en notebook 06.

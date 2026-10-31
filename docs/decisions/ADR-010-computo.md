# ADR-010 — Comparativa de configuraciones de cómputo productivas

- **Estado:** propuesto (completar con benchmark de spec 01 Parte C)
- **Requisitos:** R-05

## Escenarios de volumen `[SUPUESTO — validar con IOL]`
| Escala | Operaciones | Uso |
|---|---|---|
| 1x | 100k (dataset) | Desarrollo |
| 10x | 1M | Trimestre de un broker mediano / reproceso |
| 100x | 10M | Reproceso histórico multianual |

## Medición en Free Edition `[MEDIDO]`
| Etapa | 1x (s) | 10x (s) | 100x (s) | Bytes leídos 100x | Notas |
|---|---|---|---|---|---|
| Bronze | | | | | |
| Silver | | | | | |
| Gold (dims + facts + SCD2) | | | | | |
| Matching cotizaciones | | | | | |
| q1..q5 | | | | | |

## Configuraciones productivas
| | A. Serverless Jobs | B. Job cluster clásico chico | C. Job cluster clásico Photon + autoscaling |
|---|---|---|---|
| Composición | Gestionado | 1 driver + 2 workers, workers spot | 2–8 workers, Photon |
| Arranque | Segundos | Minutos | Minutos |
| Costo idle | 0 | 0 (efímero) | 0 (efímero) |
| $/DBU (lista, fecha: ___) | | | |
| DBU/h estimadas | | | |
| Costo por run 1x / 10x / 100x `[ESTIMADO]` | | | |
| Mejor para | Batch diario corto, equipo chico | Volumen estable, costo mínimo | Reprocesos pesados, joins grandes |
| Riesgos | Menos control, $/DBU mayor | Interrupción spot, tuning manual | Photon caro si no reduce tiempo |

Consumo analítico: SQL Warehouse serverless separado del pipeline (no mezclar costo de ingesta con costo de consulta).

## Fórmula
`costo_run = (DBU/h × $/DBU + costo_VM/h [solo clásico]) × duración_h`. Fuente de precios con fecha de consulta y link.

## Recomendación (completar con números)
Regla de decisión, ej.: "A mientras el batch diario dure < N min; C para reprocesos ≥10x si Photon reduce el tiempo más de X%".

# Spec 08 — EDA y consultas de negocio (Task 06, 6 pts)

**Requisitos:** R-26, R-27 · **ADR:** 009

Regla: **cada consulta termina con 3 líneas en markdown: Hallazgo / Implicancia operativa / Limitaciones.** Responder las 5. Precondición: `gold_status != RED`; si YELLOW, mostrar el disclaimer correspondiente arriba de q1, q2, q5.

Base común: vista `iol_gold.v_trades_market` = `fact_trades` ⨝ `fact_trade_market` ⨝ dims, filtrada a `is_analytics_eligible`. Las comparaciones contra mercado usan solo `is_comparable`.

## q1 — Clientes que operaron consistentemente por encima del mercado (`sql/q1_clients_above_market.sql`)
- Separar **compras** (pagar por encima es desfavorable) de **ventas** (vender por encima es favorable). Reportar ambas; la pregunta literal se responde con compras.
- Criterio: cliente con ≥5 trades comparables del mismo lado y ≥70% `is_above_market`. Agregar **test binomial** contra la tasa base del lado (p0 = % global por encima): marcar `significant = p < 0,05`. Sin el test, con n=5 el 70% es ruido.
- Salida: cliente, lado, n, % por encima, desvío medio y mediano, instrumentos (top 3 por n) y canal preferido.
- Implicancia esperada (a validar): best execution / alertas de órdenes a precio de mercado sin límite; si se concentran en un canal o tipo de orden, ajuste de UX.
- Limitación: con 71% de clientes de una sola operación, el universo con n≥5 es chico → reportarlo.

## q2 — Instrumentos con mayor desvío promedio (`sql/q2_instrument_deviation.sql`)
- Por instrumento con n comparables ≥ 20: desvío firmado medio, desvío absoluto medio y **mediano**, p90 absoluto, n, `asset_type`, % imputados.
- Ranking por mediana absoluta (robusto); mostrar también media para evidenciar colas. Excluir `SCALE_SUSPECT`.
- Lectura por `asset_type`: instrumentos ilíquidos/ONs con benchmark de cierre tienen más desvío estructural (ADR-009).
- Implicancia: dónde el benchmark de cierre no sirve (necesita intradiario) o dónde hay riesgo de ejecución.

## q3 — Proporción por canal mes a mes (`sql/q3_channel_mix_monthly.sql`)
- % de cada canal sobre el total del mes (window `sum() over (partition by month)`), más operaciones por **día hábil** del mes (ene 21, feb 18, mar 10 hasta el 13) para comparar marzo parcial.
- Delta en puntos porcentuales vs mes anterior.
- Implicancia: tendencia de migración a mobile / uso de API; capacidad y priorización de canal.

## q4 — Retención de la cohorte de enero (`sql/q4_january_retention.sql`)
- Cohorte = clientes con ≥1 operación en enero (fecha efectiva).
- % que operó en febrero; % que operó en marzo (1–13); % en ambos; % en alguno.
- Ajuste por período parcial: además, "marzo normalizado" = retención observada comparada contra retención feb a igual cantidad de días hábiles (feb hasta su 10º día hábil). Etiquetar.
- Cortar por `activity_segment` de enero y canal: ¿los OCASIONALES vuelven menos?
- Implicancia: activación post-primera operación (71% una sola operación).

## q5 — Canal y probabilidad de comprar por encima del mercado (`sql/q5_channel_vs_above_market.sql` + celda Python)
- Universo: compras comparables.
- SQL: por canal, n, % `is_above_market`, IC 95% Wilson.
- Python (scipy): chi-cuadrado de independencia canal × above; V de Cramér. Estratificado por `asset_type` (tabla por estrato) para controlar mezcla de instrumentos: si el efecto desaparece dentro de estratos, es composición, no canal.
- Si `statsmodels` está disponible: regresión logística `above ~ channel + asset_type + currency`.
- IOLnet (271 ops) y API: IC anchos → decirlo.
- Implicancia: correlación, no causalidad; si persiste controlando por instrumento, revisar tipo de orden por defecto del canal.

## EDA adicional (hallazgos con implicancia, al menos 3)
Candidatos a verificar con outputs:
- Sábados como cola de la sesión del viernes (impacto: corte de día operativo en reportes).
- MEP/CCL implícito diario desde AL30/AL30D/AL30C y GD30/GD30D vs operaciones en USD (impacto: monitoreo de brecha y arbitraje entre variantes).
- Concentración: top 20 símbolos = 38,9% de operaciones; Pareto de clientes (impacto: priorizar cobertura de datos de mercado y atención).
- Operaciones por día hábil por mes (tendencia de actividad).
- Errores de escala concentrados en IOLnet + LECAP/BONCAP (impacto: control en el origen de carga de ese canal).

## Criterios de aceptación
- [ ] 5 archivos SQL comentados (definición, supuestos) y ejecutados con outputs.
- [ ] Cada consulta con Hallazgo / Implicancia / Limitaciones escritos a partir del output real.
- [ ] Métricas agregadas exportadas a `iol_ops.eda_metrics` (JSON) para la IA (spec 09).

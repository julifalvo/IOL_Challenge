---
description: Revisión crítica de una etapa implementada (como reviewer senior)
argument-hint: <número de spec>
---

Revisá la etapa $ARGUMENTS como un reviewer senior de un equipo de datos financieros. No modifiques código: devolvé hallazgos.

Buscá específicamente:
- Filas que se pierden o se duplican (joins, filtros, MERGE mal condicionado).
- Idempotencia: ¿qué pasa si se corre dos veces? ¿y si se corre con un archivo nuevo?
- Look-ahead / leakage temporal (cotizaciones futuras, segmentos calculados con datos posteriores).
- Mezcla de monedas o de variantes de liquidación; escala de precio (por 1 VN vs por 100 VN).
- Uso de APIs no soportadas en serverless (RDD, sparkContext, cache, dbfs root).
- Números mágicos fuera de `config.py`.
- Supuestos no documentados en ADR.
- Tests que no prueban lo que dicen probar.
- Diferencias entre el spec y lo implementado.

Formato: tabla con severidad (alta/media/baja), archivo:línea, problema, propuesta. Al final, veredicto: listo / listo con cambios menores / no listo.

# Consultas de negocio (spec 08)

| Archivo | Pregunta del PDF |
|---|---|
| `q1_clients_above_market.sql` | ¿Qué clientes operaron consistentemente por encima del precio de mercado? ¿En qué instrumentos? |
| `q2_instrument_deviation.sql` | ¿Cuáles son los instrumentos con mayor desvío promedio entre precio operado y de mercado? |
| `q3_channel_mix_monthly.sql` | ¿Cómo evolucionó la proporción de operaciones por canal mes a mes? |
| `q4_january_retention.sql` | De los clientes que operaron en enero, ¿qué % volvió a operar en febrero y en marzo? |
| `q5_channel_vs_above_market.sql` | ¿Existe correlación entre el canal de origen y la probabilidad de comprar por encima del mercado? |

Cada archivo empieza con un comentario: definición de la métrica, universo, supuestos, parámetros (desde `config.py`).

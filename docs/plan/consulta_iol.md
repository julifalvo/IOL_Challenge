# Consulta técnica a IOL (enviar el día 1)

**Asunto:** Consulta técnica — Challenge Senior Data Engineer: entorno Databricks

Hola, ¿cómo están?

Estoy arrancando el challenge de Data Engineering y quería confirmar dos puntos de entorno antes de avanzar:

1. **Databricks Community Edition fue retirado por Databricks el 1/1/2026** y reemplazado por **Databricks Free Edition** (gratuito, cómputo serverless, Unity Catalog). Mi idea es desarrollar en Free Edition respetando el espíritu de las restricciones: sin cómputo pago, sin Databricks Jobs ni pipelines declarativos (DLT/Lakeflow), Delta en todas las tablas y orquestación con `dbutils.notebook.run()`. ¿Les parece correcto?

2. **Free Edition restringe la salida a internet** a un conjunto de dominios. Si la API de cotizaciones no es accesible desde el workspace, propongo descargar las cotizaciones con un proceso separado (mismo código, mismo contrato de datos), subir el resultado crudo a un Volume e ingerirlo a Delta dentro del pipeline, dejando trazabilidad de cada request. ¿Es aceptable ese enfoque?

Si prefieren otro criterio, lo adopto y lo documento en el README.

Gracias,
Julián

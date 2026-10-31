# ARCHITECTURE.md

> Documento técnico para el equipo que mantiene el pipeline. Estructura definida en `docs/specs/01_profiling_y_arquitectura.md` (Parte B). Completar a medida que cada etapa se cierra; todo número debe salir de un output.

## 1. Contexto y objetivos no funcionales
## 2. Diagrama de flujo
## 3. Capas y contratos
## 4. Modelo de datos (por qué estrella, granularidades, SCD2)
## 5. Estrategia de escalabilidad
### 5.1 Ingesta incremental
### 5.2 Clustering y layout físico
### 5.3 SCD2 incremental
### 5.4 Cotizaciones incrementales y caché
### 5.5 Reprocesamiento
### 5.6 Escenarios de volumen
## 6. Cómputo: comparativa y recomendación (ADR-010)
## 7. Operación
### 7.1 Correr el pipeline
### 7.2 Reprocesar un rango o un archivo
### 7.3 Agregar o corregir un instrumento
### 7.4 Qué hacer ante YELLOW / RED
### 7.5 Rotar credenciales del proveedor de cotizaciones
## 8. Limitaciones conocidas

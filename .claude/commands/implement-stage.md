---
description: Implementa una etapa del pipeline a partir de su spec
argument-hint: <número de spec, ej. 02>
---

Implementá la etapa $ARGUMENTS.

1. Leé `CLAUDE.md`, el spec en `docs/specs/` cuyo nombre empieza con $ARGUMENTS, los ADR que referencia, `docs/data_model.md` y `docs/quality_rules.md` si aplica.
2. Leé `docs/plan/PROGRESS.md` y verificá que las etapas de las que depende estén terminadas. Si no, frená y decime qué falta.
3. Antes de escribir código, respondé en un bloque corto:
   - Requisitos R-xx que cubre.
   - Archivos que vas a crear o modificar.
   - Decisiones que no están en el spec (si las hay) y su trade-off. Si alguna contradice un ADR, frená y preguntame.
4. Implementá: lógica en `src/iol_pipeline/`, tests en `tests/`, notebook fino en `notebooks/`.
5. Corré `pytest -q` y mostrá el resultado real. Si algo falla, arreglalo; no marques como hecho con tests rojos.
6. Recorré los criterios de aceptación del spec uno por uno: ✅ verificado localmente / ⏳ requiere Databricks / ❌ pendiente.
7. Actualizá `docs/plan/PROGRESS.md` y, si cambió una decisión, el ADR.
8. Decime exactamente qué tengo que correr en Databricks y qué output tengo que mirar para cerrar los ⏳.

Respetá las reglas duras de `CLAUDE.md`. No inventes métricas ni resultados de Databricks.

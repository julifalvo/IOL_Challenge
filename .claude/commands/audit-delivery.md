---
description: Auditoría final contra los requisitos del PDF antes de entregar
---

Auditá la entrega completa.

1. Recorré `docs/plan/requisitos_pdf.md` y para cada R-xx indicá: evidencia (archivo + sección/celda), estado (✅/⚠️/❌), qué falta.
2. Verificá que cada número del README aparezca en un output de `notebooks_executed/` o en un archivo de `docs/`. Listá los que no tengan respaldo.
3. Buscá en todo el repo y en el historial de git: el CSV original (nombre y patrones `TXN`/`CLI` en volumen), tokens, API keys, `.env`, emails. Reportá cualquier hallazgo.
4. Listá `[HIPÓTESIS]`, `[SUPUESTO]` y `TODO` restantes y verificá que estén en la sección de limitaciones del README.
5. Verificá que los notebooks en `notebooks_executed/` tengan outputs (celdas con `outputs` no vacíos) y el mismo `pipeline_run_id`.
6. Simulá un clon limpio: seguí el README de reproducción paso a paso y señalá cualquier paso ambiguo o faltante.

Devolvé un checklist final priorizado de lo que hay que arreglar antes de entregar.

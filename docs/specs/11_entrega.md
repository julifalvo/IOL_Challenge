# Spec 11 — Entrega

**Requisitos:** R-33..R-36 y auditoría de R-01..R-39

## Repo público
- Estructura final (notebooks numerados = orden de ejecución evidente):
```
README.md  ARCHITECTURE.md
notebooks/                 # fuente .py (Databricks source)
notebooks_executed/        # .ipynb exportados CON outputs (lo que mira el evaluador)
src/iol_pipeline/  tools/  sql/  prompts/  tests/  scripts/
docs/  (data_model, quality_rules, quality_gates, quote_coverage, decisions/, specs/)
data/README.md  data/sample_obfuscated.csv
reports/pytest.xml
```
- `docs/plan/` puede quedar (muestra método) pero revisar que no tenga nada privado. El PDF del challenge **no** se sube si IOL no lo autoriza.

## Notebooks con outputs
- Exportar cada notebook del run final desde Databricks como `.ipynb` (File → Export → IPython Notebook) a `notebooks_executed/`.
- Verificar abriendo en GitHub que se ven tablas y gráficos.
- Todos del **mismo `pipeline_run_id`** (se imprime en la primera celda).

## Dataset
- `data/sample_obfuscated.csv`: generado con `scripts/make_obfuscated_sample.py` (5.000 filas estratificadas por mes × canal × moneda, IDs re-hasheados con HMAC + salt no publicado, incluye casos de sábado, escala y variantes).
- `data/README.md`: el dataset completo no se publica; cómo colocar el original en el Volume; cómo correr con el sample.

## README
Usar `README.md` (esqueleto ya creado) y completar **solo con resultados del run final**.

## Auditoría final
- Recorrer `docs/plan/requisitos_pdf.md`: cada R-xx con archivo + celda de evidencia.
- Buscar en el repo: CSV original, tokens, emails, `.env` → 0.
- Buscar `TODO`, `[HIPÓTESIS]` sin resolver → listados en "Limitaciones".
- Clonar el repo en limpio y seguir el README de reproducción paso a paso.

## Criterios de aceptación
- [ ] Repo público, clon limpio reproduce con el sample.
- [ ] Todos los R-xx con evidencia.
- [ ] Sin datos originales ni secretos (`git log` incluido).

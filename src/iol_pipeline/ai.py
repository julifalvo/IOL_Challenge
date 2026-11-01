"""Enriquecimiento de instrumentos con IA (ADR-011, spec 09). Solo metadatos de instrumentos: nunca datos de clientes.

La IA no decide calidad ni excluye filas. Su salida se valida contra un schema cerrado, y solo se usa lo válido.
"""
from __future__ import annotations

import csv
import hashlib
import json
import re
import time
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from iol_pipeline import config, io

REPO_ROOT = Path(__file__).resolve().parents[2]
PROMPTS_DIR = REPO_ROOT / "prompts"
GOLD_LABELS_PATH = REPO_ROOT / "tests" / "fixtures" / "instrument_gold_labels.csv"
COMPONENT_ENRICHMENT = "instrument_enrichment"
COMPONENT_EVALUATION = "instrument_evaluation"
NEEDS_REVIEW = "NEEDS_REVIEW"
VALID, CONFLICT, LOW_CONFIDENCE, REJECTED, FAILED = "VALID", "CONFLICT", "LOW_CONFIDENCE", "REJECTED", "FAILED"
TICKER_PATTERN = re.compile(r"^[A-Z0-9.\-]{1,12}$")

RUNS_SCHEMA = ("ai_run_id string, pipeline_run_id string, component string, prompt_version string, model string, symbol string, "
               "input_hash string, output_json string, validation_status string, error string, latency_ms long, created_at timestamp")
RUN_COLUMNS = [c.split()[0] for c in RUNS_SCHEMA.split(", ")]
ENRICHMENT_SCHEMA = ("symbol string, asset_type string, issuer string, sector string, underlying_ticker_foreign string, "
                     "confidence double, model string, prompt_version string, ai_run_id string, is_active boolean")

# Recibe los prompts y devuelve, en el mismo orden, (texto, error). Inyectable para testear sin un modelo.
CallModel = Callable[[list[str]], list[tuple[str | None, str | None]]]


def load_prompt(version: str | None = None) -> tuple[str, str]:
    """Devuelve (system, plantilla de usuario) del prompt versionado en `prompts/`."""
    text = (PROMPTS_DIR / f"{version or config.AI_PROMPT_VERSION}.md").read_text(encoding="utf-8")
    match = re.search(r"## System\n(.*?)\n## User[^\n]*\n(.*?)\n## ", text, re.S)
    if not match:
        raise ValueError(f"El prompt {version or config.AI_PROMPT_VERSION} no tiene las secciones System y User")
    return match.group(1).strip(), match.group(2).strip()


def build_prompt(system: str, user_template: str, items: list[dict]) -> str:
    return f"{system}\n\n{user_template.replace('{{instruments_json}}', json.dumps(items, ensure_ascii=False))}"


def input_hash(item: dict, prompt_version: str, model: str) -> str:
    """Clave de caché: el mismo instrumento, con el mismo prompt y modelo, no vuelve a consultarse."""
    payload = json.dumps(item, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(f"{payload}|{prompt_version}|{model}".encode("utf-8")).hexdigest()


def build_inputs(mapping_rows: list[dict]) -> list[dict]:
    """Una consulta por subyacente, sin opciones: su subyacente ya se conoce. Las variantes reciben el mismo resultado.

    `rule_asset_type` sale del tipo que dieron las reglas o los overrides. Un tipo que ya puso la IA no cuenta: si
    contara, el input cambiaría después de cada corrida y la caché nunca serviría.
    """
    groups: dict[str, list[dict]] = {}
    for row in mapping_rows:
        if row["asset_type"] != "OPCION":
            groups.setdefault(row["underlying_key"], []).append(row)

    items = []
    for underlying, rows in sorted(groups.items()):
        lead = next((r for r in rows if r["symbol"] == underlying), min(rows, key=lambda r: r["symbol"]))
        rule_type = NEEDS_REVIEW if lead["asset_type_source"] == "AI" else lead["asset_type"]
        items.append({"symbol": lead["symbol"], "description": lead["description_canonical"], "currency": lead["currency_code"],
                      "sibling_symbols": sorted(r["symbol"] for r in rows if r["symbol"] != lead["symbol"]), "rule_asset_type": rule_type})
    return items


def _text_from_response(response: str) -> str:
    """Algunos modelos devuelven una lista de bloques con el razonamiento aparte: se usa solo el texto."""
    try:
        blocks = json.loads(response)
    except (TypeError, ValueError):
        return response
    if isinstance(blocks, list) and blocks and all(isinstance(b, dict) and "type" in b for b in blocks):
        return "".join(str(b.get("text", "")) for b in blocks if b.get("type") == "text")
    return response


def parse_response(response: str) -> list[dict]:
    """Extrae el arreglo JSON de la respuesta, aunque venga entre marcas de código. Falla si no hay uno."""
    text = _text_from_response(response).strip()
    start, end = text.find("["), text.rfind("]")
    if start == -1 or end <= start:
        raise ValueError("La respuesta no tiene un arreglo JSON")
    parsed = json.loads(text[start:end + 1])
    if not isinstance(parsed, list) or not all(isinstance(item, dict) for item in parsed):
        raise ValueError("La respuesta no es un arreglo de objetos")
    return parsed


def validate_output(item: dict, output: dict) -> tuple[str, str | None]:
    """Devuelve (estado, motivo). REJECTED no cumple el schema; CONFLICT contradice a una regla; LOW_CONFIDENCE no alcanza el mínimo."""
    if output.get("symbol") != item["symbol"]:
        return REJECTED, "symbol distinto al de entrada"
    if output.get("asset_type") not in config.ASSET_TYPES:
        return REJECTED, f"asset_type fuera del enum: {output.get('asset_type')!r}"
    sector = output.get("sector")
    if sector is not None and sector not in config.AI_SECTORS:
        return REJECTED, f"sector fuera del enum: {sector!r}"
    confidence = output.get("confidence")
    if isinstance(confidence, bool) or not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
        return REJECTED, f"confidence inválida: {confidence!r}"
    ticker = output.get("underlying_ticker_foreign")
    if ticker is not None and (not isinstance(ticker, str) or not TICKER_PATTERN.match(ticker)):
        return REJECTED, f"ticker inválido: {ticker!r}"
    issuer = output.get("issuer")
    if issuer is not None and (not isinstance(issuer, str) or not issuer.strip() or len(issuer) > config.AI_MAX_ISSUER_CHARS):
        return REJECTED, "issuer inválido"
    rule_type = item.get("rule_asset_type", NEEDS_REVIEW)
    if rule_type != NEEDS_REVIEW and output["asset_type"] != rule_type:
        return CONFLICT, f"la regla dice {rule_type} y la IA {output['asset_type']}: gana la regla"
    if confidence < config.AI_MIN_CONFIDENCE:
        return LOW_CONFIDENCE, f"confianza {confidence}"
    return VALID, None


def enrich(items: list[dict], call_model: CallModel, model: str, component: str, pipeline_run_id: str, cached: set[str],
           prompt_version: str | None = None, batch_size: int | None = None, now: datetime | None = None) -> list[dict]:
    """Consulta solo lo que no está en `cached`, en lotes, y devuelve una fila de `ai_runs` por instrumento consultado.

    Un lote que falla o no se puede leer queda FAILED para esos instrumentos, sin cortar el resto, y se reintenta en otra corrida.
    """
    version = prompt_version or config.AI_PROMPT_VERSION
    size = batch_size or config.AI_BATCH_SIZE
    created_at = now or datetime.now(timezone.utc)
    pending = [item for item in items if input_hash(item, version, model) not in cached]
    if not pending:
        return []
    batches = [pending[i:i + size] for i in range(0, len(pending), size)]
    system, user_template = load_prompt(version)
    started = time.monotonic()
    responses = call_model([build_prompt(system, user_template, batch) for batch in batches])
    latency_ms = int((time.monotonic() - started) * 1000 / len(batches))

    def record(item: dict, status: str, output: dict | None, error: str | None) -> dict:
        digest = input_hash(item, version, model)
        return {"ai_run_id": f"{component}:{digest}", "pipeline_run_id": pipeline_run_id, "component": component, "prompt_version": version, "model": model,
                "symbol": item["symbol"], "input_hash": digest, "output_json": json.dumps(output, ensure_ascii=False, sort_keys=True) if output is not None else None,
                "validation_status": status, "error": error[:config.RUN_ERROR_MAX_CHARS] if error else None, "latency_ms": latency_ms, "created_at": created_at}

    records = []
    for batch, (text, error) in zip(batches, responses):
        try:
            if text is None:
                raise ValueError(error or "El modelo no devolvió respuesta")
            outputs = {str(o.get("symbol")): o for o in parse_response(text)}
        except (ValueError, TypeError) as exc:
            records += [record(item, FAILED, None, str(exc)) for item in batch]
            continue
        for item in batch:
            output = outputs.get(item["symbol"])
            if output is None:
                records.append(record(item, FAILED, None, "El modelo omitió este instrumento"))
            else:
                status, reason = validate_output(item, output)
                records.append(record(item, status, output, reason))
    return records


def enrichment_rows(items: list[dict], runs: list[dict], model: str, prompt_version: str | None = None) -> list[tuple]:
    """Filas de `instrument_enrichment`: solo lo VALID del modelo actual, propagado a las variantes del subyacente."""
    version = prompt_version or config.AI_PROMPT_VERSION
    by_hash = {r["input_hash"]: r for r in runs if r["validation_status"] == VALID}
    rows = []
    for item in items:
        run = by_hash.get(input_hash(item, version, model))
        if run:
            output = json.loads(run["output_json"])
            for symbol in [item["symbol"], *item["sibling_symbols"]]:
                rows.append((symbol, output["asset_type"], output.get("issuer"), output.get("sector"), output.get("underlying_ticker_foreign"),
                             float(output["confidence"]), model, version, run["ai_run_id"], True))
    return rows


def _write_runs(spark: SparkSession, records: list[dict], runs_table: str) -> None:
    if records:
        df = spark.createDataFrame([tuple(r[c] for c in RUN_COLUMNS) for r in records], RUNS_SCHEMA)
        io.write_delta_upsert(spark, df, runs_table, ["ai_run_id"], ["output_json", "validation_status", "error"])


def _runs(spark: SparkSession, runs_table: str, component: str, model: str) -> list[dict]:
    if not spark.catalog.tableExists(runs_table):
        return []
    rows = spark.table(runs_table).where((F.col("component") == component) & (F.col("model") == model)).collect()
    return [r.asDict() for r in rows]


def _cached_hashes(runs: list[dict]) -> set[str]:
    """Un FAILED no cuenta como caché: se reintenta."""
    return {r["input_hash"] for r in runs if r["validation_status"] != FAILED}


def run_enrichment(spark: SparkSession, mapping_table: str, runs_table: str, enrichment_table: str, call_model: CallModel, pipeline_run_id: str = "",
                   model: str | None = None) -> dict:
    """Enriquece lo que falte y deja `instrument_enrichment` lista. Después hay que reconstruir el mapeo (`instruments.run_instruments`)."""
    model = model or config.AI_MODEL
    columns = ["symbol", "currency_code", "description_canonical", "asset_type", "asset_type_source", "underlying_key"]
    items = build_inputs([r.asDict() for r in spark.table(mapping_table).select(*columns).collect()])
    previous = _runs(spark, runs_table, COMPONENT_ENRICHMENT, model)
    new = enrich(items, call_model, model, COMPONENT_ENRICHMENT, pipeline_run_id, _cached_hashes(previous))
    _write_runs(spark, new, runs_table)

    runs = _runs(spark, runs_table, COMPONENT_ENRICHMENT, model)
    rows = enrichment_rows(items, runs, model)
    result = io.write_delta_upsert(spark, spark.createDataFrame(rows, ENRICHMENT_SCHEMA), enrichment_table, ["symbol"],
                                   ["asset_type", "issuer", "sector", "underlying_ticker_foreign", "confidence", "model", "prompt_version", "ai_run_id", "is_active"],
                                   deactivate_column="is_active")
    statuses: dict[str, int] = {}
    current = {input_hash(item, config.AI_PROMPT_VERSION, model) for item in items}
    for run in runs:
        if run["input_hash"] in current:
            statuses[run["validation_status"]] = statuses.get(run["validation_status"], 0) + 1
    return {"instruments": len(items), "consulted": len(new), "statuses": statuses, "enriched_symbols": len(rows), **{f"enrichment_{k}": v for k, v in result.items()}}


# --- Evaluación contra la muestra etiquetada -------------------------------------------
def read_gold_labels(path: Path = GOLD_LABELS_PATH) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def blind_items(labels: list[dict]) -> list[dict]:
    """Sin el tipo de las reglas ni los hermanos: si los viera, el modelo los copiaría y la accuracy saldría inflada."""
    return [{"symbol": row["symbol"], "description": row["description"], "currency": row["currency"], "sibling_symbols": [], "rule_asset_type": NEEDS_REVIEW}
            for row in labels]


def _same_ticker(a: str | None, b: str | None) -> bool:
    return re.sub(r"[.\-]", "", (a or "").upper()) == re.sub(r"[.\-]", "", (b or "").upper())


def score(labels: list[dict], runs: list[dict], model: str, prompt_version: str | None = None) -> dict:
    """Accuracy de asset_type sobre toda la muestra, y de ticker sobre los CEDEARs. Lo rechazado o fallido cuenta como error."""
    version = prompt_version or config.AI_PROMPT_VERSION
    by_hash = {r["input_hash"]: r for r in runs}
    items = {i["symbol"]: i for i in blind_items(labels)}
    right_type = right_ticker = cedears = unusable = 0
    misses = []
    for label in labels:
        run = by_hash.get(input_hash(items[label["symbol"]], version, model))
        usable = run is not None and run["validation_status"] in (VALID, LOW_CONFIDENCE)
        output = json.loads(run["output_json"]) if usable else {}
        unusable += 0 if usable else 1
        if output.get("asset_type") == label["asset_type"]:
            right_type += 1
        else:
            misses.append(f"{label['symbol']}: {output.get('asset_type')} (esperado {label['asset_type']})")
        if label["asset_type"] == "CEDEAR":
            cedears += 1
            if _same_ticker(output.get("underlying_ticker_foreign"), label["underlying_ticker_foreign"]):
                right_ticker += 1
            elif usable:
                misses.append(f"{label['symbol']}: ticker {output.get('underlying_ticker_foreign')} (esperado {label['underlying_ticker_foreign']})")
    return {"model": model, "labeled": len(labels), "asset_type_accuracy": round(right_type / len(labels), 4), "cedears": cedears,
            "ticker_accuracy": round(right_ticker / cedears, 4) if cedears else None, "unusable": unusable, "misses": "; ".join(misses)}


def run_evaluation(spark: SparkSession, runs_table: str, models: tuple[str, ...], call_model_for: Callable[[str], CallModel], pipeline_run_id: str = "") -> list[dict]:
    """Evalúa cada modelo a ciegas. Queda en caché: repetir la evaluación no vuelve a consultar."""
    labels = read_gold_labels()
    items = blind_items(labels)
    results = []
    for model in models:
        previous = _runs(spark, runs_table, COMPONENT_EVALUATION, model)
        _write_runs(spark, enrich(items, call_model_for(model), model, COMPONENT_EVALUATION, pipeline_run_id, _cached_hashes(previous)), runs_table)
        results.append(score(labels, _runs(spark, runs_table, COMPONENT_EVALUATION, model), model))
    return results


def ai_query_caller(spark: SparkSession, model: str) -> CallModel:
    """`ai_query` de Databricks con temperatura 0. Un error de un lote vuelve como texto y no corta los demás."""
    def call(prompts: list[str]) -> list[tuple[str | None, str | None]]:
        params = f"named_struct('temperature', CAST({config.AI_TEMPERATURE} AS DOUBLE), 'max_tokens', {config.AI_MAX_TOKENS})"
        expression = f"ai_query('{model}', prompt, modelParameters => {params}, failOnError => false) AS response"
        rows = spark.createDataFrame(list(enumerate(prompts)), "position int, prompt string").selectExpr("position", expression).collect()
        by_position = {r["position"]: r["response"] for r in rows}
        return [(by_position[i]["result"], by_position[i]["errorMessage"]) for i in range(len(prompts))]
    return call


# --- Salidas del notebook --------------------------------------------------------------
def status_summary(runs: DataFrame, component: str = COMPONENT_ENRICHMENT) -> DataFrame:
    return runs.where(F.col("component") == component).groupBy("model", "validation_status").count().orderBy("model", "validation_status")


def problem_examples(runs: DataFrame, component: str = COMPONENT_ENRICHMENT) -> DataFrame:
    """Lo que la validación no aceptó, con el motivo: lo que muestra que la validación hace algo."""
    return (runs.where((F.col("component") == component) & (F.col("validation_status") != VALID))
            .select("symbol", "validation_status", "error").orderBy("validation_status", "symbol"))

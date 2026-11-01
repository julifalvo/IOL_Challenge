"""Del mapeo al enriquecimiento y de vuelta al mapeo, con un modelo simulado (spec 09)."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from iol_pipeline import ai, bronze, config, instruments, silver

pytestmark = pytest.mark.integration
EMPTY_OVERRIDES = Path(__file__).parents[1] / "fixtures" / "overrides_empty.csv"  # los overrides reales nombran símbolos que estos datos no tienen


def _answer(item, asset_type=None):
    return {"symbol": item["symbol"], "asset_type": asset_type or (item["rule_asset_type"] if item["rule_asset_type"] != "NEEDS_REVIEW" else "OTRO"),
            "issuer": f"Emisor {item['symbol']}", "sector": "OTRO", "underlying_ticker_foreign": None, "maturity_date": None, "is_etf": False, "confidence": 0.9, "rationale": "x"}


def _model(counter, conflict_symbol=None):
    def call(prompts):
        counter["calls"] += len(prompts)
        responses = []
        for prompt in prompts:
            items = json.loads(prompt[prompt.index('[{"symbol"'):prompt.rindex("}]") + 2])
            responses.append((json.dumps([_answer(i, "ON" if i["symbol"] == conflict_symbol else None) for i in items]), None))
        return responses
    return call


@pytest.fixture()
def tables(spark, schema, trades_small_csv):
    t = {n: f"{schema}.{n}" for n in ("trades_raw", "ingestion_runs", "trades", "quality_baselines", "quality_events", "instrument_mapping", "ai_runs", "instrument_enrichment")}
    bronze.ingest_trades(spark, str(trades_small_csv), t["trades_raw"], t["ingestion_runs"])
    silver.run_silver(spark, t["trades_raw"], t["trades"], t["quality_baselines"], t["quality_events"], "p1")
    instruments.run_instruments(spark, t["trades"], t["instrument_mapping"], overrides_path=EMPTY_OVERRIDES)
    return t


def test_enrichment_reaches_the_mapping_is_cached_and_stays_stable(spark, tables):
    counter = {"calls": 0}
    first = ai.run_enrichment(spark, tables["instrument_mapping"], tables["ai_runs"], tables["instrument_enrichment"], _model(counter, "AAPL"), "p1", "modelo")
    assert first["consulted"] == first["instruments"] > 0
    assert first["statuses"][ai.CONFLICT] == 1
    calls_after_first = counter["calls"]

    instruments.run_instruments(spark, tables["trades"], tables["instrument_mapping"], overrides_path=EMPTY_OVERRIDES, enrichment_table=tables["instrument_enrichment"])
    rows = {r.symbol: r for r in spark.table(tables["instrument_mapping"]).collect()}
    assert rows["AAPL"].asset_type == "CEDEAR" and rows["AAPL"].issuer is None  # contradijo a la regla: no se usa nada de esa respuesta
    assert rows["MSFT"].issuer == "Emisor MSFT" and rows["MSFT"].asset_type_source == "RULE"  # completa campos, no pisa el tipo
    assert rows["AL30D"].issuer == "Emisor AL30"  # la variante recibe el resultado de su subyacente

    second = ai.run_enrichment(spark, tables["instrument_mapping"], tables["ai_runs"], tables["instrument_enrichment"], _model(counter, "AAPL"), "p2", "modelo")
    assert second["consulted"] == 0 and counter["calls"] == calls_after_first  # la caché evita la llamada
    assert second["enrichment_rows_inserted"] == second["enrichment_rows_updated"] == 0
    again = instruments.run_instruments(spark, tables["trades"], tables["instrument_mapping"], overrides_path=EMPTY_OVERRIDES, enrichment_table=tables["instrument_enrichment"])
    assert again["rows_updated"] == again["rows_inserted"] == 0


def test_needs_review_is_classified_and_stays_stable_after_the_mapping_changes(spark, tables):
    row = spark.createDataFrame([("NUEVO", "Instrumento Raro", "ARS", "NEEDS_REVIEW", "RULE", "NUEVO")],
                                "symbol string, currency_code string, description_canonical string, asset_type string, asset_type_source string, underlying_key string")
    mapping = f"{tables['ai_runs']}_mapeo"
    row.write.format("delta").saveAsTable(mapping)
    counter = {"calls": 0}

    ai.run_enrichment(spark, mapping, tables["ai_runs"], tables["instrument_enrichment"], _model(counter), "p1", "modelo")
    enriched = spark.table(tables["instrument_enrichment"]).first()
    assert (enriched.symbol, enriched.asset_type) == ("NUEVO", "OTRO")

    spark.sql(f"UPDATE {mapping} SET asset_type = 'OTRO', asset_type_source = 'AI'")
    ai.run_enrichment(spark, mapping, tables["ai_runs"], tables["instrument_enrichment"], _model(counter), "p2", "modelo")
    assert counter["calls"] == 1  # el tipo que puso la IA no cambia el input


def test_failed_batch_is_retried_next_run(spark, tables):
    def failing(prompts):
        return [(None, "límite de cuota")] * len(prompts)

    first = ai.run_enrichment(spark, tables["instrument_mapping"], tables["ai_runs"], tables["instrument_enrichment"], failing, "p1", "modelo")
    assert set(first["statuses"]) == {ai.FAILED} and first["enriched_symbols"] == 0

    counter = {"calls": 0}
    second = ai.run_enrichment(spark, tables["instrument_mapping"], tables["ai_runs"], tables["instrument_enrichment"], _model(counter), "p2", "modelo")
    assert second["consulted"] == second["instruments"]
    assert set(second["statuses"]) == {ai.VALID}
    assert spark.table(tables["ai_runs"]).where("validation_status = 'FAILED'").count() == 0  # el reintento reemplaza la fila


def test_evaluation_is_cached_per_model(spark, tables, monkeypatch):
    counter = {"calls": 0}
    labels = ai.read_gold_labels()[:3]
    monkeypatch.setattr(ai, "read_gold_labels", lambda: labels)

    first = ai.run_evaluation(spark, tables["ai_runs"], ("m1", "m2"), lambda model: _model(counter), "p1")
    calls = counter["calls"]
    second = ai.run_evaluation(spark, tables["ai_runs"], ("m1", "m2"), lambda model: _model(counter), "p2")

    assert calls == 2 and counter["calls"] == calls
    assert [r["model"] for r in first] == ["m1", "m2"]
    assert first == second
    assert first[0]["labeled"] == 3


def test_config_knows_the_prompt_and_models():
    assert config.AI_MODEL in config.AI_EVAL_MODELS
    assert (ai.PROMPTS_DIR / f"{config.AI_PROMPT_VERSION}.md").is_file()

"""Validación de la salida de la IA, caché y evaluación. Sin Spark ni modelo: la llamada se inyecta."""
from __future__ import annotations

import json

import pytest

from iol_pipeline import ai, config

ITEM = {"symbol": "D30A6", "description": "D30a6 Vto 30/04/2026", "currency": "ARS", "sibling_symbols": [], "rule_asset_type": "NEEDS_REVIEW"}
OUTPUT = {"symbol": "D30A6", "asset_type": "LETRA_LECAP_BONCAP", "issuer": "Tesoro Nacional", "sector": "SOBERANO", "underlying_ticker_foreign": None,
          "maturity_date": "2026-04-30", "is_etf": False, "confidence": 0.9, "rationale": "Letra"}
MODEL = "modelo"


def _row(symbol, asset_type="CEDEAR", underlying=None, source="RULE", currency="ARS"):
    return {"symbol": symbol, "currency_code": currency, "description_canonical": f"Desc {symbol}", "asset_type": asset_type, "asset_type_source": source, "underlying_key": underlying or symbol}


def test_prompt_of_the_repository_has_its_sections():
    system, user = ai.load_prompt()
    assert "Respondé SOLO con JSON" in system
    assert "{{instruments_json}}" in user
    assert "Resultado esperado" not in user


def test_prompt_contains_only_the_items_sent():
    prompt = ai.build_prompt("SYS", "Clasificá: {{instruments_json}}", [ITEM])
    assert prompt.startswith("SYS") and "D30A6" in prompt and "{{" not in prompt


def test_inputs_have_no_client_data_by_construction():
    assert set(ai.build_inputs([_row("AAPL")])[0]) == {"symbol", "description", "currency", "sibling_symbols", "rule_asset_type"}


def test_inputs_group_variants_and_skip_options():
    rows = [_row("AL30", "BONO_SOBERANO"), _row("AL30D", "BONO_SOBERANO", "AL30", currency="USD"), _row("AL30C", "BONO_SOBERANO", "AL30", currency="USD"), _row("GFGC1", "OPCION", "GGAL")]
    items = ai.build_inputs(rows)
    assert [i["symbol"] for i in items] == ["AL30"]
    assert items[0]["sibling_symbols"] == ["AL30C", "AL30D"]


def test_input_does_not_change_after_the_ai_classified_it():
    before = ai.build_inputs([_row("D30A6", "NEEDS_REVIEW")])
    after = ai.build_inputs([_row("D30A6", "LETRA_LECAP_BONCAP", source="AI")])
    assert before == after  # si cambiara, la caché nunca serviría


def test_manual_type_counts_as_rule():
    assert ai.build_inputs([_row("D30A6", "OTRO", source="MANUAL")])[0]["rule_asset_type"] == "OTRO"


def test_hash_changes_with_prompt_model_and_input():
    base = ai.input_hash(ITEM, "v1", MODEL)
    assert base == ai.input_hash(dict(ITEM), "v1", MODEL)
    assert base != ai.input_hash(ITEM, "v2", MODEL)
    assert base != ai.input_hash(ITEM, "v1", "otro")
    assert base != ai.input_hash({**ITEM, "description": "x"}, "v1", MODEL)


@pytest.mark.parametrize("response", [
    json.dumps([OUTPUT]),
    "```json\n" + json.dumps([OUTPUT]) + "\n```",
    "Acá va:\n" + json.dumps([OUTPUT]) + "\nListo.",
    json.dumps([{"type": "reasoning", "summary": [{"text": "pienso"}]}, {"type": "text", "text": json.dumps([OUTPUT])}]),
])
def test_parse_accepts_the_usual_shapes(response):
    assert ai.parse_response(response)[0]["symbol"] == "D30A6"


@pytest.mark.parametrize("response", ["sin json", "[1, 2]", "[{\"symbol\": ", ""])
def test_parse_rejects_what_is_not_an_array_of_objects(response):
    with pytest.raises(ValueError):
        ai.parse_response(response)


def test_valid_output():
    assert ai.validate_output(ITEM, OUTPUT) == (ai.VALID, None)


@pytest.mark.parametrize(("change", "message"), [
    ({"symbol": "OTRO"}, "symbol"),
    ({"asset_type": "ACCION"}, "asset_type"),
    ({"asset_type": "NEEDS_REVIEW"}, "asset_type"),
    ({"sector": "BANANAS"}, "sector"),
    ({"confidence": 1.5}, "confidence"),
    ({"confidence": "alta"}, "confidence"),
    ({"confidence": True}, "confidence"),
    ({"confidence": None}, "confidence"),
    ({"underlying_ticker_foreign": "no es ticker"}, "ticker"),
    ({"issuer": "x" * 500}, "issuer"),
    ({"issuer": "  "}, "issuer"),
])
def test_invalid_output_is_rejected(change, message):
    status, reason = ai.validate_output(ITEM, {**OUTPUT, **change})
    assert status == ai.REJECTED
    assert message in reason


def test_contradicting_a_rule_is_a_conflict():
    status, reason = ai.validate_output({**ITEM, "rule_asset_type": "ON"}, OUTPUT)
    assert status == ai.CONFLICT and "gana la regla" in reason


def test_same_type_as_the_rule_is_valid():
    assert ai.validate_output({**ITEM, "rule_asset_type": "LETRA_LECAP_BONCAP"}, OUTPUT)[0] == ai.VALID


def test_low_confidence_is_not_valid():
    assert ai.validate_output(ITEM, {**OUTPUT, "confidence": config.AI_MIN_CONFIDENCE - 0.01})[0] == ai.LOW_CONFIDENCE


def _fake(responses=None, calls=None):
    def call(prompts):
        if calls is not None:
            calls.extend(prompts)
        return responses if responses is not None else [(json.dumps([OUTPUT]), None)] * len(prompts)
    return call


def test_enrich_validates_and_records_without_a_real_model():
    records = ai.enrich([ITEM], _fake(), MODEL, ai.COMPONENT_ENRICHMENT, "p1", set())
    assert records[0]["validation_status"] == ai.VALID
    assert records[0]["model"] == MODEL and records[0]["prompt_version"] == config.AI_PROMPT_VERSION
    assert records[0]["ai_run_id"].startswith(ai.COMPONENT_ENRICHMENT)
    assert json.loads(records[0]["output_json"])["issuer"] == "Tesoro Nacional"


def test_enrich_does_not_call_for_what_is_cached():
    calls = []
    cached = {ai.input_hash(ITEM, config.AI_PROMPT_VERSION, MODEL)}
    assert ai.enrich([ITEM], _fake(calls=calls), MODEL, ai.COMPONENT_ENRICHMENT, "p1", cached) == []
    assert calls == []


def test_enrich_splits_in_batches():
    items = [{**ITEM, "symbol": f"S{i}"} for i in range(5)]
    calls = []
    ai.enrich(items, _fake(responses=[(None, "error")] * 3, calls=calls), MODEL, ai.COMPONENT_ENRICHMENT, "p1", set(), batch_size=2)
    assert len(calls) == 3


@pytest.mark.parametrize("response", [(None, "límite de cuota"), ("no es json", None), ("[]", None)])
def test_a_failed_batch_is_recorded_and_does_not_raise(response):
    records = ai.enrich([ITEM], _fake(responses=[response]), MODEL, ai.COMPONENT_ENRICHMENT, "p1", set())
    assert records[0]["validation_status"] == ai.FAILED and records[0]["error"]
    assert records[0]["output_json"] is None


def test_failed_is_not_cached_and_the_rest_is():
    runs = [{"input_hash": "a", "validation_status": ai.FAILED}, {"input_hash": "b", "validation_status": ai.REJECTED}, {"input_hash": "c", "validation_status": ai.VALID}]
    assert ai._cached_hashes(runs) == {"b", "c"}


def test_enrichment_rows_only_use_valid_and_reach_the_variants():
    item = {**ITEM, "sibling_symbols": ["D30A6D"]}
    good = ai.enrich([item], _fake(), MODEL, ai.COMPONENT_ENRICHMENT, "p1", set())
    rows = ai.enrichment_rows([item], good, MODEL)
    assert [r[0] for r in rows] == ["D30A6", "D30A6D"]
    assert rows[0][1:4] == ("LETRA_LECAP_BONCAP", "Tesoro Nacional", "SOBERANO")

    conflict = {**item, "rule_asset_type": "ON"}
    bad = ai.enrich([conflict], _fake(), MODEL, ai.COMPONENT_ENRICHMENT, "p1", set())
    assert ai.enrichment_rows([conflict], bad, MODEL) == []


def test_gold_labels_are_a_stratified_sample_of_sixty():
    labels = ai.read_gold_labels()
    assert len(labels) == 60 == len({row["symbol"] for row in labels})
    assert {row["asset_type"] for row in labels} <= set(config.ASSET_TYPES)
    assert len({row["asset_type"] for row in labels}) >= 7
    assert all(row["underlying_ticker_foreign"] for row in labels if row["asset_type"] == "CEDEAR")
    assert not any(row["underlying_ticker_foreign"] for row in labels if row["asset_type"] != "CEDEAR")


def test_evaluation_is_blind():
    item = ai.blind_items([{"symbol": "AAPL", "description": "Cedear Apple", "currency": "ARS"}])[0]
    assert item["rule_asset_type"] == "NEEDS_REVIEW" and item["sibling_symbols"] == []


def test_score_counts_right_wrong_and_unusable():
    labels = [
        {"symbol": "AAPL", "description": "a", "currency": "ARS", "asset_type": "CEDEAR", "underlying_ticker_foreign": "AAPL"},
        {"symbol": "BRKB", "description": "b", "currency": "ARS", "asset_type": "CEDEAR", "underlying_ticker_foreign": "BRK.B"},
        {"symbol": "YPFD", "description": "c", "currency": "ARS", "asset_type": "ACCION_LOCAL", "underlying_ticker_foreign": ""},
        {"symbol": "AL30", "description": "d", "currency": "ARS", "asset_type": "BONO_SOBERANO", "underlying_ticker_foreign": ""},
    ]
    outputs = [{"symbol": "AAPL", "asset_type": "CEDEAR", "underlying_ticker_foreign": "AAPL", "confidence": 0.9},
               {"symbol": "BRKB", "asset_type": "CEDEAR", "underlying_ticker_foreign": "BRK-B", "confidence": 0.9},
               {"symbol": "YPFD", "asset_type": "ON", "underlying_ticker_foreign": None, "confidence": 0.9}]
    runs = ai.enrich(ai.blind_items(labels)[:3], _fake(responses=[(json.dumps(outputs), None)]), MODEL, ai.COMPONENT_EVALUATION, "p1", set())
    result = ai.score(labels, runs, MODEL)
    assert result["asset_type_accuracy"] == 0.5  # YPFD mal, y AL30 sin respuesta
    assert result["ticker_accuracy"] == 1.0  # BRK-B y BRK.B son el mismo ticker
    assert result["ticker_baseline_symbol"] == 1.0  # BRKB también coincide con BRK.B: copiar el símbolo acierta los dos
    assert result["unusable"] == 1
    assert "YPFD" in result["misses"] and "AL30" in result["misses"]

"""Validación por serie y elección de proveedor (ADR-005, ADR-008). Sin Spark."""
from __future__ import annotations

import pytest

from iol_pipeline import config, quotes


@pytest.mark.parametrize(("asset_type", "ratio", "factor", "status"), [
    ("BONO_SOBERANO", 0.01, 0.01, quotes.OK),          # 87.600 por 100 VN contra 876
    ("BONO_SOBERANO", 0.0115, 0.01, quotes.BIASED),    # AL30 en BYMA: dentro de [0,5; 2] pero sesgado
    ("ACCION_LOCAL", 10.0088, 10.0, quotes.OK),        # YPFD a ×0,1
    ("CEDEAR", 3.0024, 3.0, quotes.OK),                # SPY a ×1/3
    ("CEDEAR", 0.3325, 1 / 3, quotes.OK),
    ("CEDEAR", 1.0086, 1.0, quotes.OK),                # MSFT en BYMA: dentro de la tolerancia de CEDEARs
    ("BONO_SOBERANO", 1.0086, 1.0, quotes.BIASED),     # el mismo sesgo en un bono ya supera su tolerancia
    ("CEDEAR", 25.0, None, quotes.SCALE_SUSPECT),      # HUT: ningún candidato lo deja en [0,5; 2]
])
def test_factor_and_status(asset_type, ratio, factor, status):
    result = quotes.assess_series(asset_type, 40, 30, ratio)
    assert result["series_status"] == status
    assert result["price_scale_factor"] == (pytest.approx(factor) if factor else None)


def test_factor_is_discrete_so_it_cannot_absorb_a_premium():
    """Una relación de 1,4 no se convierte en factor 1,4: el sesgo tiene que verse."""
    result = quotes.assess_series("CEDEAR", 40, 30, 1.4)
    assert result["price_scale_factor"] == 1.0
    assert result["bias_pct"] == pytest.approx(0.4)
    assert result["series_status"] == quotes.BIASED


@pytest.mark.parametrize(("rows", "days", "ratio", "status"), [
    (0, 0, None, quotes.NO_DATA),
    (30, 0, None, quotes.NO_OVERLAP),
    (30, config.QUOTE_MIN_DAYS - 1, 1.001, quotes.THIN),
    (30, config.QUOTE_MIN_DAYS, 1.001, quotes.OK),
    (30, 1, 1.05, quotes.BIASED),  # con pocos días y un sesgo grande no pasa como THIN
])
def test_states_by_data_available(rows, days, ratio, status):
    assert quotes.assess_series("CEDEAR", rows, days, ratio)["series_status"] == status


def test_tolerance_of_a_type_without_measurement_is_the_default():
    assert quotes.tolerance_for("OPCION") == config.ABOVE_MARKET_TOLERANCE


def _candidates(*providers, asset_type="CEDEAR", symbol="AAA"):
    return [{"symbol": symbol, "asset_type": asset_type, "provider": p, "provider_symbol": symbol, "priority": i} for i, p in enumerate(providers)]


def _stat(provider, ratio, days=30, symbol="AAA"):
    return {"provider": provider, "symbol": symbol, "quote_rows": 40, "days_compared": days, "ratio_p10": ratio, "ratio_p50": ratio, "ratio_p90": ratio}


def _selected(rows):
    return [r["provider"] for r in rows if r["is_selected"]]


def test_priority_wins_over_the_series_closest_to_the_trades():
    """Si ganara la más parecida, la métrica de "por encima del mercado" se achicaría sola."""
    rows = quotes.assess_all(_candidates("data912", "yahoo"), [_stat("data912", 1.012), _stat("yahoo", 1.0001)])
    assert _selected(rows) == ["data912"]


def test_falls_back_when_the_first_series_does_not_validate():
    rows = quotes.assess_all(_candidates("data912", "yahoo", "byma_open_data"), [_stat("data912", 1.20), _stat("yahoo", 1.001), _stat("byma_open_data", 1.001)])
    assert _selected(rows) == ["yahoo"]


def test_a_thin_series_is_used_only_without_an_ok_one():
    with_ok = quotes.assess_all(_candidates("data912", "yahoo"), [_stat("data912", 1.001, days=2), _stat("yahoo", 1.001)])
    assert _selected(with_ok) == ["yahoo"]
    without_ok = quotes.assess_all(_candidates("data912", "yahoo"), [_stat("data912", 1.001, days=2), _stat("yahoo", 1.20)])
    assert _selected(without_ok) == ["data912"]


def test_nothing_selected_when_no_series_validates():
    rows = quotes.assess_all(_candidates("byma_open_data", asset_type="ON"), [_stat("byma_open_data", 0.0104)])
    assert _selected(rows) == []
    assert rows[0]["series_status"] == quotes.BIASED


def test_a_provider_without_rows_counts_as_no_data():
    rows = quotes.assess_all(_candidates("data912", "yahoo"), [_stat("yahoo", 1.0)])
    assert {r["provider"]: r["series_status"] for r in rows} == {"data912": quotes.NO_DATA, "yahoo": quotes.OK}
    assert _selected(rows) == ["yahoo"]


def test_candidates_follow_the_chain_of_the_type():
    rows = quotes.candidate_series([{"symbol": "AL30", "asset_type": "BONO_SOBERANO"}, {"symbol": "X", "asset_type": "NEEDS_REVIEW"}])
    assert [(r["provider"], r["provider_symbol"], r["priority"]) for r in rows] == [("data912", "AL30", 0), ("byma_open_data", "AL30 24HS", 1)]


def test_every_type_has_a_chain_and_data912_knows_where_to_look():
    assert set(config.QUOTE_PROVIDERS_BY_TYPE) == set(config.ASSET_TYPES)
    for asset_type, chain in config.QUOTE_PROVIDERS_BY_TYPE.items():
        if "data912" in chain:
            assert asset_type in config.DATA912_KIND_BY_TYPE

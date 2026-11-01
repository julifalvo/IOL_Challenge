"""Clasificación de instrumentos, variantes de liquidación y mapeo a proveedor (ADR-004, ADR-005, ADR-008).

Capas, de menos a más autoridad: reglas determinísticas, IA para lo que quede en NEEDS_REVIEW (spec 09), la validación
de cotizaciones para proveedor y escala (spec 05), y overrides manuales que ganan sobre todo. Hay una fila por símbolo.
"""
from __future__ import annotations

import csv
from datetime import date, datetime, timezone
from functools import lru_cache
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from iol_pipeline import config, io

MAPPING_DIR = Path(__file__).parent / "mapping"
OVERRIDES_PATH = MAPPING_DIR / "overrides.csv"
LOCAL_STOCKS_PATH = MAPPING_DIR / "local_stocks.csv"
OVERRIDE_COLUMNS = ["symbol", "field", "value", "reason", "author", "date"]

OPTION_PATTERN = r"^[A-Z]{3,5}[CV]\d{1,6}(\.\d*)?[A-Z]{0,2}$"
OPTION_PREFIX_PATTERN = r"^([A-Z]{3,5})[CV]\d"
MATURITY_PATTERN = r"(\d{1,2})/(\d{1,2})/(\d{4}|\d{2})\b"
VARIANT_SUFFIX_PATTERN = r"\.?[DC]$"
NEEDS_REVIEW = "NEEDS_REVIEW"

MAPPING_COLUMNS = [
    "symbol", "currency_code", "description_canonical", "asset_type", "asset_type_source", "underlying_key", "settlement_variant",
    "price_scale_factor", "maturity_date", "issuer", "sector", "underlying_ticker_foreign", "provider", "provider_symbol",
    "mapping_status", "mapping_version", "updated_at",
]
OVERRIDE_TYPES = {"asset_type": "string", "underlying_key": "string", "settlement_variant": "string", "price_scale_factor": "double",
                  "maturity_date": "date", "issuer": "string", "sector": "string", "underlying_ticker_foreign": "string",
                  "provider": "string", "provider_symbol": "string", "mapping_status": "string"}


@lru_cache(maxsize=1)
def local_stocks() -> tuple[str, ...]:
    """Acciones locales según data912 (tools/quotes_fetcher/refresh_local_stocks.py). Es una foto: lo que falta sigue a la IA."""
    with LOCAL_STOCKS_PATH.open(newline="", encoding="utf-8") as fh:
        return tuple(row["symbol"] for row in csv.DictReader(fh))


def asset_type_by_rules(symbol: Column, description: Column) -> Column:
    """Tipo de activo por símbolo y descripción. Lo que no matchea queda NEEDS_REVIEW para la IA (spec 09).

    Provincial antes que soberano: "Bono Pcia" contiene "bono". Las ON sin la palabra "ON" van después de los
    bonos, porque un bono también puede decir "REGS". La lista de acciones va última: la descripción manda.
    """
    desc = F.lower(description)
    return (F.when(symbol.rlike(OPTION_PATTERN) & (symbol == description), "OPCION")
            .when(desc.startswith("cedear"), "CEDEAR")
            .when(desc.rlike(r"lecap|boncap|lelink|\blt rep|\blt p\b|\bl\. tes|letra"), "LETRA_LECAP_BONCAP")
            .when(desc.rlike("warrant"), "WARRANT")
            .when(desc.rlike(r"pcia|prov\.|provincia|ciudad"), "BONO_SUBSOBERANO")
            .when(desc.rlike(r"\bon\b|oblig"), "ON")
            .when(desc.rlike("bono|bonos|bontes|tesoro|tasa dual|bopreal"), "BONO_SOBERANO")
            .when(desc.rlike(r"\bo n\b|\bregs\b.*\d%|\bcl(ase|\.).*\bvto"), "ON")
            .when(symbol.isin(list(local_stocks())), "ACCION_LOCAL")
            .otherwise(NEEDS_REVIEW))


def maturity_date(description: Column) -> Column:
    """Vencimiento `d/m/aa(aa)` de la descripción. Null si no hay, o si la fecha no existe."""
    day = F.lpad(F.regexp_extract(description, MATURITY_PATTERN, 1), 2, "0")
    month = F.lpad(F.regexp_extract(description, MATURITY_PATTERN, 2), 2, "0")
    year = F.regexp_extract(description, MATURITY_PATTERN, 3)
    full_year = F.when(F.length(year) == 2, F.concat(F.lit("20"), year)).otherwise(year)
    text = F.concat_ws("-", full_year, month, day)
    return F.to_date(F.try_to_timestamp(F.when(year != "", text), F.lit("yyyy-MM-dd")))


def symbol_catalog(trades: DataFrame) -> DataFrame:
    """Una fila por símbolo: la descripción y la moneda más frecuentes, con desempate alfabético.

    Espera `symbol`, `description_raw` y `currency_code`. Agrega `description_count`, la base de DQ031.
    """
    named = trades.where(F.col("symbol").isNotNull())

    def most_frequent(column: str, alias: str) -> DataFrame:
        counts = named.where(F.col(column).isNotNull()).groupBy("symbol", column).count()
        order = Window.partitionBy("symbol").orderBy(F.col("count").desc(), F.col(column))
        return counts.withColumn("_rank", F.row_number().over(order)).where("_rank = 1").select("symbol", F.col(column).alias(alias))

    descriptions = named.groupBy("symbol").agg(F.countDistinct("description_raw").alias("description_count"))
    return (descriptions.join(most_frequent("description_raw", "description_canonical"), "symbol", "left")
            .join(most_frequent("currency_code", "currency_code"), "symbol", "left"))


def classify_by_rules(catalog: DataFrame) -> DataFrame:
    """Agrega asset_type, underlying_key, settlement_variant y maturity_date.

    Variante solo si es USD, termina en D o C, y existe el símbolo base en ARS con la misma descripción o el mismo
    tipo, o un único símbolo en ARS con la misma descripción. Con dos o más no se elige: sería inventar una relación.
    El sufijo solo no alcanza: `YPFD`, `C` y `BA.C` están en pesos. Una variante sin tipo hereda el de su base.
    """
    typed = catalog.withColumn("_type", asset_type_by_rules(F.col("symbol"), F.coalesce("description_canonical", F.lit(""))))
    bases = typed.where(F.col("currency_code") == "ARS").select(
        F.col("symbol").alias("_base"), F.col("_type").alias("_base_type"), F.lower("description_canonical").alias("_base_description"))
    is_usd = F.col("currency_code") == "USD"
    candidate = F.when(is_usd & F.col("symbol").rlike(r"[DC]$"), F.regexp_replace("symbol", VARIANT_SUFFIX_PATTERN, ""))
    siblings = (bases.where(F.col("_base_description").isNotNull()).groupBy(F.col("_base_description").alias("_own_description"))
                .agg(F.count("*").alias("_siblings"), F.first("_base").alias("_sibling"), F.first("_base_type").alias("_sibling_type"))
                .where("_siblings = 1").drop("_siblings"))
    joined = (typed.withColumn("_base", candidate).withColumn("_own_description", F.lower("description_canonical"))
              .join(bases, "_base", "left").join(siblings, "_own_description", "left"))
    same_underlying = ((F.col("_own_description") == F.col("_base_description"))
                       | ((F.col("_type") == F.col("_base_type")) & (F.col("_type") != NEEDS_REVIEW)))
    by_suffix = F.coalesce(F.col("_base_type").isNotNull() & same_underlying, F.lit(False))
    by_description = candidate.isNotNull() & ~by_suffix & F.col("_sibling").isNotNull()
    is_variant = by_suffix | by_description
    base_symbol = F.when(by_suffix, F.col("_base")).when(by_description, F.col("_sibling"))
    base_type = F.when(by_suffix, F.col("_base_type")).when(by_description, F.col("_sibling_type"))

    option_underlying = F.try_element_at(
        F.create_map(*[F.lit(v) for pair in config.OPTION_UNDERLYING.items() for v in pair]),
        F.regexp_extract("symbol", OPTION_PREFIX_PATTERN, 1))
    return (joined
            .withColumn("asset_type", F.when(is_variant & (F.col("_type") == NEEDS_REVIEW), base_type).otherwise(F.col("_type")))
            .withColumn("settlement_variant",
                        F.when(F.col("currency_code") == "ARS", "ARS")
                        .when(is_usd & is_variant & F.col("symbol").endswith("D"), "USD_MEP")
                        .when(is_usd & is_variant & F.col("symbol").endswith("C"), "USD_CCL")
                        .when(is_usd, "USD_OTHER")
                        .otherwise("UNKNOWN"))
            .withColumn("underlying_key",
                        F.when(is_variant, base_symbol)
                        .when(F.col("_type") == "OPCION", F.coalesce(option_underlying, F.col("symbol")))
                        .otherwise(F.col("symbol")))
            .withColumn("maturity_date", maturity_date(F.coalesce("description_canonical", F.lit(""))))
            .drop("_base", "_base_type", "_base_description", "_type", "_own_description", "_sibling", "_sibling_type"))


def with_provider(classified: DataFrame) -> DataFrame:
    """Agrega provider, provider_symbol, price_scale_factor y mapping_status, según ADR-008.

    Es el mapeo inicial: todo queda UNVERIFIED hasta que la etapa 05 lo valide por símbolo.
    """
    provider = F.lit(None).cast("string")
    is_usd = F.col("currency_code") == "USD"
    for asset_type, usd, name in reversed(config.PROVIDER_RULES):
        match = F.col("asset_type") == asset_type
        if usd is not None:
            match = match & (is_usd if usd else ~is_usd)
        provider = F.when(match, F.lit(name)).otherwise(provider)

    per_100 = F.col("asset_type").isin(list(config.ASSET_TYPES_PER_100_NOMINAL))
    return (classified
            .withColumn("provider", provider)
            .withColumn("provider_symbol",
                        F.when(F.col("provider") == "data912", F.col("symbol"))
                        .when(F.col("provider") == "byma_open_data", F.concat("symbol", F.lit(config.BYMA_SYMBOL_SUFFIX)))
                        .when(F.col("provider") == "yahoo", F.concat("symbol", F.lit(config.YAHOO_SYMBOL_SUFFIX))))
            .withColumn("price_scale_factor", F.when(per_100, F.lit(config.PER_100_NOMINAL_FACTOR)).otherwise(F.lit(1.0)))
            .withColumn("mapping_status",
                        F.when(F.col("provider").isNotNull(), "UNVERIFIED")
                        .when(F.col("asset_type") == NEEDS_REVIEW, "UNMAPPED")
                        .otherwise("UNSUPPORTED")))


def apply_quote_series(mapping: DataFrame, series: DataFrame | None) -> DataFrame:
    """Pisa el mapeo inicial con lo que la etapa 05 midió (ADR-008): la serie elegida, o `UNSUPPORTED` si se evaluó y ninguna sirve.

    Lo que todavía no se evaluó queda como estaba. Sin esto, reejecutar la etapa 04 borraría la validación.
    """
    if series is None:
        return mapping
    active = series.where("is_active")
    evaluated = active.select("symbol").distinct().withColumn("_evaluated", F.lit(True))
    chosen = active.where("is_selected").select("symbol", F.col("provider").alias("_provider"), F.col("provider_symbol").alias("_provider_symbol"), F.col("price_scale_factor").alias("_factor"))
    joined = mapping.join(evaluated, "symbol", "left").join(chosen, "symbol", "left")
    is_evaluated = F.col("_evaluated").isNotNull()
    return (joined
            .withColumn("provider", F.when(is_evaluated, F.col("_provider")).otherwise(F.col("provider")))
            .withColumn("provider_symbol", F.when(is_evaluated, F.col("_provider_symbol")).otherwise(F.col("provider_symbol")))
            .withColumn("price_scale_factor", F.coalesce(F.col("_factor"), F.col("price_scale_factor")))
            .withColumn("mapping_status", F.when(F.col("_provider").isNotNull(), "MAPPED").when(is_evaluated, "UNSUPPORTED").otherwise(F.col("mapping_status")))
            .drop("_evaluated", "_provider", "_provider_symbol", "_factor"))


def _validate_override(field: str, symbol: str, value: str) -> None:
    allowed = {"asset_type": config.ASSET_TYPES, "settlement_variant": config.SETTLEMENT_VARIANTS, "mapping_status": config.MAPPING_STATUSES}.get(field)
    if not value.strip():
        raise ValueError(f"Override de {field} vacío para {symbol}")
    if allowed is not None and value not in allowed:
        raise ValueError(f"Override inválido para {symbol}: {field} = {value!r}, se esperaba uno de {allowed}")
    try:
        if field == "price_scale_factor" and float(value) <= 0:
            raise ValueError("debe ser positivo")
        if field == "maturity_date":
            date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Override inválido para {symbol}: {field} = {value!r} ({exc})") from exc


def read_overrides(path: Path = OVERRIDES_PATH) -> dict[str, dict[str, str]]:
    """Campo -> símbolo -> valor. Falla ante un campo, un valor o un símbolo repetido inválidos: un override mal escrito
    no puede pasar callado. Que el símbolo exista se controla en `build_mapping`."""
    overrides: dict[str, dict[str, str]] = {}
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames != OVERRIDE_COLUMNS:
            raise ValueError(f"{path}: columnas {reader.fieldnames}, se esperaba {OVERRIDE_COLUMNS}")
        for row in reader:
            field, symbol = row["field"], row["symbol"]
            if field not in OVERRIDE_TYPES:
                raise ValueError(f"{path}: campo desconocido {field!r} para {symbol}")
            if symbol in overrides.setdefault(field, {}):
                raise ValueError(f"{path}: {symbol} tiene {field} repetido")
            _validate_override(field, symbol, row["value"])
            overrides[field][symbol] = row["value"]
    return overrides


def apply_overrides(mapping: DataFrame, overrides: dict[str, dict[str, str]]) -> DataFrame:
    """Los overrides ganan sobre todo. Un asset_type pisado queda con asset_type_source = MANUAL."""
    result = mapping
    for field, by_symbol in overrides.items():
        values = F.create_map(*[F.lit(v) for pair in by_symbol.items() for v in pair])
        override = F.try_element_at(values, F.col("symbol")).try_cast(OVERRIDE_TYPES[field])
        if field == "asset_type":
            result = result.withColumn("asset_type_source", F.when(override.isNotNull(), "MANUAL").otherwise(F.col("asset_type_source")))
        result = result.withColumn(field, F.coalesce(override, F.col(field)))
    return result


def apply_enrichment(classified: DataFrame, enrichment: DataFrame | None) -> DataFrame:
    """Agrega issuer, sector y ticker de la IA. La IA no pisa reglas: su tipo solo vale para lo que está en NEEDS_REVIEW."""
    base = classified.withColumn("asset_type_source", F.lit("RULE"))
    if enrichment is None:
        return (base.withColumn("issuer", F.lit(None).cast("string")).withColumn("sector", F.lit(None).cast("string"))
                .withColumn("underlying_ticker_foreign", F.lit(None).cast("string")))
    ai = enrichment.select("symbol", F.col("asset_type").alias("_ai_type"), "issuer", "sector", "underlying_ticker_foreign")
    from_ai = (F.col("asset_type") == NEEDS_REVIEW) & F.col("_ai_type").isNotNull()
    return (base.join(ai, "symbol", "left")
            .withColumn("asset_type_source", F.when(from_ai, "AI").otherwise(F.col("asset_type_source")))
            .withColumn("asset_type", F.when(from_ai, F.col("_ai_type")).otherwise(F.col("asset_type")))
            .drop("_ai_type"))


def _check_override_symbols(catalog: DataFrame, overrides: dict[str, dict[str, str]]) -> None:
    wanted = {symbol for by_symbol in overrides.values() for symbol in by_symbol}
    if wanted:
        found = {r[0] for r in catalog.where(F.col("symbol").isin(list(wanted))).select("symbol").collect()}
        if wanted - found:
            raise ValueError(f"Overrides de símbolos que no existen: {sorted(wanted - found)}")


def build_mapping(trades: DataFrame, overrides: dict[str, dict[str, str]], updated_at: datetime, enrichment: DataFrame | None = None,
                  quote_series: DataFrame | None = None) -> DataFrame:
    """Una fila por símbolo, lista para `iol_ops.instrument_mapping`.

    Orden: reglas, IA, override de tipo, proveedor, validación de cotizaciones, demás overrides. El proveedor sale del
    tipo final: un tipo corregido a mano o por IA también tiene que recibir cotizaciones.
    """
    catalog = symbol_catalog(trades)
    _check_override_symbols(catalog, overrides)
    type_overrides = {field: values for field, values in overrides.items() if field == "asset_type"}
    other_overrides = {field: values for field, values in overrides.items() if field != "asset_type"}
    typed = apply_overrides(apply_enrichment(classify_by_rules(catalog), enrichment), type_overrides)
    return (apply_overrides(apply_quote_series(with_provider(typed), quote_series), other_overrides)
            .withColumn("mapping_version", F.lit(config.MAPPING_VERSION))
            .withColumn("updated_at", F.lit(updated_at))
            .select(*MAPPING_COLUMNS))


def run_instruments(spark: SparkSession, silver_table: str, mapping_table: str, overrides_path: Path = OVERRIDES_PATH,
                    enrichment_table: str | None = None, quote_series_table: str | None = None) -> dict[str, int]:
    """Reconstruye el mapeo desde Silver. Con el mismo input no toca nada, ni siquiera `updated_at`.

    `enrichment_table` (spec 09) y `quote_series_table` (spec 05) son salidas de etapas posteriores, si existen: sin ellas, cada corrida desharía su aporte.
    """
    enrichment = spark.table(enrichment_table).where("is_active") if enrichment_table and spark.catalog.tableExists(enrichment_table) else None
    series = spark.table(quote_series_table) if quote_series_table and spark.catalog.tableExists(quote_series_table) else None
    mapping = build_mapping(spark.table(silver_table), read_overrides(overrides_path), datetime.now(timezone.utc), enrichment, series)
    compare = [c for c in MAPPING_COLUMNS if c not in ("symbol", "updated_at")]
    result = io.write_delta_upsert(spark, mapping, mapping_table, ["symbol"], compare)
    return {**result, "symbols": spark.table(mapping_table).count()}


# --- Salidas del notebook ------------------------------------------------------------
def type_summary(mapping: DataFrame) -> DataFrame:
    return (mapping.groupBy("asset_type", "currency_code", "asset_type_source").agg(F.count("*").alias("symbols"))
            .orderBy("asset_type", "currency_code", "asset_type_source"))


def variants_with_implied_fx(mapping: DataFrame, trades: DataFrame) -> DataFrame:
    """Subyacentes con 2 o más variantes y sus medianas. `implied_fx` = mediana en ARS / mediana de la variante: MEP o CCL implícito."""
    groups = (mapping.where("asset_type != 'OPCION'").groupBy("underlying_key").agg(F.count("*").alias("variants")).where("variants >= 2")
              .join(mapping.where("asset_type != 'OPCION'"), "underlying_key"))
    prices = trades.where("price > 0").groupBy("symbol").agg(F.median(F.col("price").cast("double")).alias("median_price"), F.count("*").alias("ops"))
    base_price = prices.select(F.col("symbol").alias("underlying_key"), F.col("median_price").alias("_base_price"))
    return (groups.join(prices, "symbol", "left").join(base_price, "underlying_key", "left")
            .withColumn("implied_fx", F.when(F.col("settlement_variant").startswith("USD"), F.round(F.col("_base_price") / F.col("median_price"), 2)))
            .select("underlying_key", "symbol", "currency_code", "settlement_variant", "asset_type", "median_price", "implied_fx", "ops")
            .orderBy(F.col("underlying_key"), F.col("symbol")))


def variants_fx_out_of_range(variants: DataFrame) -> DataFrame:
    """Variantes con un cambio implícito fuera de `config.VARIANT_FX_RANGE`. Con pocas operaciones suele ser iliquidez; si no, revisar."""
    low, high = config.VARIANT_FX_RANGE
    return variants.where((F.col("implied_fx") < low) | (F.col("implied_fx") > high)).orderBy(F.col("ops").desc())


def agreement_with_provider_lists(spark: SparkSession, mapping: DataFrame, get: Callable[..., Any]) -> DataFrame:
    """Compara la clasificación con las listas por tipo de data912. Una lista que no se puede bajar se informa y no corta nada."""
    types = {r.symbol: r.asset_type for r in mapping.select("symbol", "asset_type").collect()}
    rows = []
    for name, expected in config.DATA912_LIST_TYPES.items():
        independent = name not in config.DATA912_NOT_INDEPENDENT_LISTS
        try:
            response = get(config.DATA912_LIVE_URL.format(name=name), timeout=config.HTTP_TIMEOUT_S)
            listed = {row["symbol"] for row in response.json()}
        except Exception as exc:
            rows.append((name, independent, 0, 0, 0, 0, None, f"{type(exc).__name__}: {str(exc)[:config.SMOKE_ERROR_MAX_CHARS]}"))
            continue
        found = sorted(listed & set(types))
        disagree = [s for s in found if types[s] not in expected and types[s] != NEEDS_REVIEW]
        pending = sum(1 for s in found if types[s] == NEEDS_REVIEW)
        rows.append((name, independent, len(found), len(found) - len(disagree) - pending, pending, len(disagree),
                     ", ".join(disagree[:config.DISAGREEMENT_EXAMPLES]), None))
    return spark.createDataFrame(rows, "list_name string, independent boolean, symbols_in_dataset long, agree long, needs_review long, disagree long, disagree_examples string, error string")


def suffix_false_positives(mapping: DataFrame) -> DataFrame:
    """Símbolos en ARS que terminan en D o C: el sufijo solo los habría marcado como variantes en dólares."""
    return (mapping.where("currency_code = 'ARS' AND symbol RLIKE '[DC]$'")
            .select("symbol", "description_canonical", "asset_type", "settlement_variant").orderBy("symbol"))


def needs_review(mapping: DataFrame) -> DataFrame:
    return mapping.where(F.col("asset_type") == NEEDS_REVIEW).select("symbol", "currency_code", "description_canonical", "mapping_status").orderBy("symbol")

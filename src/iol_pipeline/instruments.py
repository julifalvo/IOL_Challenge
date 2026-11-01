"""Clasificación de instrumentos, variantes de liquidación y mapeo a proveedor (ADR-004, ADR-005, ADR-008).

Tres capas, de más a menos confiable en cuanto a quién decide: reglas determinísticas, IA para lo que quede
en NEEDS_REVIEW (spec 09), y overrides manuales que ganan sobre todo. Hay una fila por símbolo.
"""
from __future__ import annotations

import csv
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

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
    tipo. El sufijo solo no alcanza: `YPFD`, `C` y `BA.C` están en pesos. Una variante sin tipo hereda el de su base.
    """
    typed = catalog.withColumn("_type", asset_type_by_rules(F.col("symbol"), F.coalesce("description_canonical", F.lit(""))))
    bases = typed.where(F.col("currency_code") == "ARS").select(
        F.col("symbol").alias("_base"), F.col("_type").alias("_base_type"), F.lower("description_canonical").alias("_base_description"))
    is_usd = F.col("currency_code") == "USD"
    candidate = F.when(is_usd & F.col("symbol").rlike(r"[DC]$"), F.regexp_replace("symbol", VARIANT_SUFFIX_PATTERN, ""))
    joined = typed.withColumn("_base", candidate).join(bases, "_base", "left")
    same_underlying = ((F.lower("description_canonical") == F.col("_base_description"))
                       | ((F.col("_type") == F.col("_base_type")) & (F.col("_type") != NEEDS_REVIEW)))
    is_variant = F.col("_base_type").isNotNull() & same_underlying

    option_underlying = F.try_element_at(
        F.create_map(*[F.lit(v) for pair in config.OPTION_UNDERLYING.items() for v in pair]),
        F.regexp_extract("symbol", OPTION_PREFIX_PATTERN, 1))
    return (joined
            .withColumn("asset_type", F.when(is_variant & (F.col("_type") == NEEDS_REVIEW), F.col("_base_type")).otherwise(F.col("_type")))
            .withColumn("settlement_variant",
                        F.when(F.col("currency_code") == "ARS", "ARS")
                        .when(is_usd & is_variant & F.col("symbol").endswith("D"), "USD_MEP")
                        .when(is_usd & is_variant & F.col("symbol").endswith("C"), "USD_CCL")
                        .when(is_usd, "USD_OTHER")
                        .otherwise("UNKNOWN"))
            .withColumn("underlying_key",
                        F.when(is_variant, F.col("_base"))
                        .when(F.col("_type") == "OPCION", F.coalesce(option_underlying, F.col("symbol")))
                        .otherwise(F.col("symbol")))
            .withColumn("maturity_date", maturity_date(F.coalesce("description_canonical", F.lit(""))))
            .drop("_base", "_base_type", "_base_description", "_type"))


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


def read_overrides(path: Path = OVERRIDES_PATH) -> dict[str, dict[str, str]]:
    """Campo -> símbolo -> valor. Falla ante un campo desconocido o repetido: un override mal escrito no puede pasar callado."""
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


def build_mapping(trades: DataFrame, overrides: dict[str, dict[str, str]], updated_at: datetime) -> DataFrame:
    """Una fila por símbolo, lista para `iol_ops.instrument_mapping`. La IA completa issuer, sector y ticker en el spec 09."""
    base = (with_provider(classify_by_rules(symbol_catalog(trades)))
            .withColumn("asset_type_source", F.lit("RULE"))
            .withColumn("issuer", F.lit(None).cast("string"))
            .withColumn("sector", F.lit(None).cast("string"))
            .withColumn("underlying_ticker_foreign", F.lit(None).cast("string"))
            .withColumn("mapping_version", F.lit(config.MAPPING_VERSION))
            .withColumn("updated_at", F.lit(updated_at)))
    return apply_overrides(base, overrides).select(*MAPPING_COLUMNS)


def run_instruments(spark: SparkSession, silver_table: str, mapping_table: str, overrides_path: Path = OVERRIDES_PATH) -> dict[str, int]:
    """Reconstruye el mapeo desde Silver. Con el mismo input no toca nada, ni siquiera `updated_at`."""
    mapping = build_mapping(spark.table(silver_table), read_overrides(overrides_path), datetime.now(timezone.utc))
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


def suffix_false_positives(mapping: DataFrame) -> DataFrame:
    """Símbolos en ARS que terminan en D o C: el sufijo solo los habría marcado como variantes en dólares."""
    return (mapping.where("currency_code = 'ARS' AND symbol RLIKE '[DC]$'")
            .select("symbol", "description_canonical", "asset_type", "settlement_variant").orderBy("symbol"))


def needs_review(mapping: DataFrame) -> DataFrame:
    return mapping.where(F.col("asset_type") == NEEDS_REVIEW).select("symbol", "currency_code", "description_canonical", "mapping_status").orderBy("symbol")

"""Reglas determinísticas de instrumentos (ADR-004). El spec 04 completa el mapeo; acá está lo que Silver necesita."""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

OPTION_PATTERN = r"^[A-Z]{3,5}[CV]\d{3,6}[A-Z]{0,2}$"
MATURITY_PATTERN = r"(\d{2})/(\d{2})/(\d{4}|\d{2})\b"


def asset_type_by_rules(symbol: Column, description: Column) -> Column:
    """Tipo de activo por símbolo y descripción. Lo que no matchea queda NEEDS_REVIEW para la IA (spec 09).

    Provincial antes que soberano: "Bono Pcia" contiene "bono".
    """
    desc = F.lower(description)
    return (F.when(symbol.rlike(OPTION_PATTERN) & (symbol == description), "OPCION")
            .when(desc.startswith("cedear"), "CEDEAR")
            .when(desc.rlike(r"lecap|boncap|\blt rep|letra"), "LETRA_LECAP_BONCAP")
            .when(desc.rlike("warrant"), "WARRANT")
            .when(desc.rlike(r"pcia|prov\.|provincia|ciudad"), "BONO_SUBSOBERANO")
            .when(desc.rlike(r"\bon\b|oblig"), "ON")
            .when(desc.rlike("bono|bonos|tesoro|tasa dual|bopreal"), "BONO_SOBERANO")
            .otherwise("NEEDS_REVIEW"))


def maturity_date(description: Column) -> Column:
    """Vencimiento `dd/mm/aa(aa)` de la descripción. Null si no hay, o si la fecha no existe."""
    day = F.regexp_extract(description, MATURITY_PATTERN, 1)
    month = F.regexp_extract(description, MATURITY_PATTERN, 2)
    year = F.regexp_extract(description, MATURITY_PATTERN, 3)
    full_year = F.when(F.length(year) == 2, F.concat(F.lit("20"), year)).otherwise(year)
    text = F.concat_ws("-", full_year, month, day)
    return F.to_date(F.try_to_timestamp(F.when(day != "", text), F.lit("yyyy-MM-dd")))

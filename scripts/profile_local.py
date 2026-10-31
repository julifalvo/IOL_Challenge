"""Profiling local del CSV del challenge (pandas).

Reproduce los hallazgos que motivan los ADR. No reemplaza al notebook 01 (que
recalcula en Spark y deja outputs en Databricks); sirve para iterar rápido en
local y para que Claude Code tenga los números a mano.

Uso:
    python scripts/profile_local.py data/raw/data-set-challenge-5-.csv
"""
from __future__ import annotations

import re
import sys

import pandas as pd

TZ = "America/Argentina/Buenos_Aires"
MARKET_HOLIDAYS = {"2026-01-01", "2026-02-16", "2026-02-17"}  # verificar contra calendario BYMA


def section(title: str) -> None:
    print(f"\n## {title}\n")


OPTION_RE = re.compile(r"^[A-Z]{3,5}[CV]\d{3,6}[A-Z]{0,2}$")


def asset_type_by_rules(symbol: str, desc: str) -> str:
    if OPTION_RE.match(symbol) and symbol == desc:
        return "OPCION"
    s = desc.lower()
    if s.startswith("cedear"):
        return "CEDEAR"
    if re.search(r"lecap|boncap|\blt rep|letra", s):
        return "LETRA_LECAP_BONCAP"
    if re.search(r"warrant", s):
        return "WARRANT"
    if re.search(r"pcia|prov\.|provincia|ciudad", s):
        return "BONO_SUBSOBERANO"
    if re.search(r"\bon\b|oblig", s):
        return "ON"
    if re.search(r"bono|bonos|tesoro|tasa dual|bopreal", s):
        return "BONO_SOBERANO"
    return "NEEDS_REVIEW"


def main(path: str) -> None:
    df = pd.read_csv(path, dtype={"cantidad": "int64", "precio": "float64"})
    ts = pd.to_datetime(df["fecha"], utc=True, errors="coerce")
    local = ts.dt.tz_convert(TZ)
    df["date_local"] = local.dt.strftime("%Y-%m-%d")

    section("1. Generales")
    print(f"filas={len(df):,}  columnas={df.shape[1]}")
    print(f"nulos por columna: {df.isna().sum().to_dict()}")
    print(f"timestamps no parseables: {ts.isna().sum()}")
    print(f"id_transaccion duplicados: {df['id_transaccion'].duplicated().sum()}")
    print(f"timestamps repetidos: {df['fecha'].duplicated().sum()}")
    print(f"clientes={df['id_cliente'].nunique():,}  simbolos={df['simbolo_titulo'].nunique():,}  "
          f"descripciones={df['descripcion_titulo'].nunique():,}")
    print(f"rango local: {local.min()} -> {local.max()}")
    print(f"tipoTran: {df['tipoTran'].value_counts().to_dict()}")
    print(f"moneda: {df['moneda'].value_counts().to_dict()}")

    section("2. Calendario (ADR-003)")
    names = ["lun", "mar", "mie", "jue", "vie", "sab", "dom"]
    dow_utc = ts.dt.day_of_week.value_counts().reindex(range(7), fill_value=0)
    dow_loc = local.dt.day_of_week.value_counts().reindex(range(7), fill_value=0)
    print("dia   UTC     local")
    for i, n in enumerate(names):
        print(f"{n}  {dow_utc[i]:>6,}  {dow_loc[i]:>6,}")
    bdays = pd.bdate_range(local.min().date(), local.max().date())
    present = set(df["date_local"])
    print("días hábiles (lun-vie) sin operaciones:",
          [str(d.date()) for d in bdays if str(d.date()) not in present])
    print("operaciones en feriados de config:",
          int(df["date_local"].isin(MARKET_HOLIDAYS).sum()))
    hours = pd.DataFrame({"dow": ts.dt.day_of_week, "h": ts.dt.hour})
    for d, label in [(0, "lunes"), (5, "sábado")]:
        h = hours[hours.dow == d].h
        print(f"{label}: hora UTC min={h.min()} max={h.max()}")
    session = local.dt.hour.between(11, 16)
    print(f"% dentro de rueda local 11-17 hs: {session.mean():.1%}")

    section("3. Canales")
    print(df["origen"].value_counts().to_dict())
    multi = (df.groupby("id_cliente")["origen"].nunique() > 1).sum()
    print(f"clientes multicanal: {multi:,}")

    section("4. Clientes (ADR-007)")
    per = df.groupby("id_cliente").size()
    print(f"percentiles ops/cliente: {per.quantile([.5,.75,.9,.95,.99]).to_dict()}  max={per.max()}")
    print(f"clientes con 1 operación: {(per == 1).sum():,} ({(per == 1).mean():.1%})")

    section("5. Instrumentos y variantes (ADR-004)")
    df["asset_type_rule"] = [asset_type_by_rules(s, d) for s, d in zip(df["simbolo_titulo"], df["descripcion_titulo"])]
    print(pd.crosstab(df["asset_type_rule"], df["moneda"], margins=True).to_string())
    syms = df[["simbolo_titulo", "moneda"]].drop_duplicates()
    ars = set(syms.loc[syms.moneda == "ARS", "simbolo_titulo"])
    usd = syms.loc[syms.moneda == "USD", "simbolo_titulo"]
    with_base = [s for s in usd if s[:-1] in ars]
    print(f"símbolos USD={len(usd)}  con base ARS al quitar sufijo={len(with_base)}")
    false_pos = sorted(s for s in ars if s[-1] in "DC")
    print(f"símbolos ARS terminados en D/C ({len(false_pos)}): {false_pos}")
    multi_desc = df.groupby("simbolo_titulo")["descripcion_titulo"].nunique()
    print(f"símbolos con >1 descripción: {sorted(multi_desc[multi_desc > 1].index)}")
    top20 = df["simbolo_titulo"].value_counts().head(20)
    print(f"top 20 símbolos = {top20.sum() / len(df):.1%} de operaciones: {top20.to_dict()}")

    section("6. Escala de precios (ADR-005)")
    for group in [("AL30", "AL30D", "AL30C"), ("GD30", "GD30D"), ("AAPL", "AAPLD", "AAPLC")]:
        med = {s: round(float(df.loc[df.simbolo_titulo == s, "precio"].median()), 4)
               for s in group if (df.simbolo_titulo == s).any()}
        print(med)
    al30 = df.loc[df.simbolo_titulo == "AL30", "precio"].median()
    al30d = df.loc[df.simbolo_titulo == "AL30D", "precio"].median()
    if al30 and al30d:
        print(f"MEP implícito (mediana AL30/AL30D): {al30 / al30d:,.1f}")

    section("7. Extremos (DQ020/DQ021)")
    df["amount"] = df["cantidad"] * df["precio"]
    cols = ["simbolo_titulo", "descripcion_titulo", "origen", "moneda", "cantidad", "precio", "amount"]
    print("top 5 por cantidad:\n", df.nlargest(5, "cantidad")[cols].to_string(index=False))
    med_sym = df.groupby("simbolo_titulo")["precio"].transform("median")
    ratio = df["precio"] / med_sym
    scale = df[(ratio < 0.02) | (ratio > 50)]
    print(f"\nfilas con precio a x50 o x1/50 de la mediana del símbolo: {len(scale)}")
    print(scale.groupby(["origen", "asset_type_rule"]).size().to_string())
    io = df[df.origen == "IOLnet"]
    placeholder = ((io.precio.round(2) == 1.0) | (io.precio == 0.01)).sum()
    print(f"IOLnet con precio ≈1,00 o 0,01: {placeholder} de {len(io)}")
    print(f"\ncantidad/precio <= 0: {(df.cantidad <= 0).sum()} / {(df.precio <= 0).sum()}")

    section("8. Actividad por mes")
    month = local.dt.month
    ops = month.value_counts().sort_index()
    for m, n in ops.items():
        bd = len([d for d in bdays if d.month == m and str(d.date()) in present])
        print(f"mes {m}: {n:,} ops, {bd} días hábiles con datos, {n / bd:,.0f} ops/día hábil")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")  # la consola de Windows (cp1252) no imprime "≈"
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])

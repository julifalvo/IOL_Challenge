"""Genera una muestra ofuscada y publicable del dataset.

- Estratifica por mes x canal x moneda.
- Fuerza la inclusión de casos que el pipeline debe resolver: sábados,
  variantes de liquidación y posibles errores de escala.
- Reemplaza id_cliente e id_transaccion con HMAC-SHA256(salt) conservando el
  formato (CLI + 8 hex, TXN + 13 alfanum) y la relación cliente<->operaciones.
- Conserva datos de mercado (símbolo, descripción, moneda, canal, fecha,
  cantidad, precio): no son datos personales.

Uso:
    python scripts/make_obfuscated_sample.py <csv_original> <csv_salida> --salt "$SAMPLE_SALT" [--n 5000] [--seed 42]
"""
from __future__ import annotations

import argparse
import hashlib
import hmac
import string

import pandas as pd

FORCED_SYMBOLS = ["AL30", "AL30D", "AL30C", "AAPL", "AAPLD", "AAPLC", "YPFD", "C", "BA.C", "GD30", "GD30D"]
ALNUM = string.ascii_lowercase + string.digits


def pseudonym(value: str, salt: str, prefix: str, length: int, alphabet: str) -> str:
    digest = hmac.new(salt.encode(), value.encode(), hashlib.sha256).digest()
    n = int.from_bytes(digest, "big")
    out = []
    for _ in range(length):
        n, r = divmod(n, len(alphabet))
        out.append(alphabet[r])
    return prefix + "".join(out)


def build_sample(df: pd.DataFrame, n: int, seed: int) -> pd.DataFrame:
    ts = pd.to_datetime(df["fecha"], utc=True).dt.tz_convert("America/Argentina/Buenos_Aires")
    df = df.assign(_month=ts.dt.month, _dow=ts.dt.day_of_week)

    forced = []
    for sym in FORCED_SYMBOLS:
        forced.append(df[df.simbolo_titulo == sym].sample(min(10, (df.simbolo_titulo == sym).sum()), random_state=seed))
    forced.append(df[df._dow == 5].sample(50, random_state=seed))  # sábados
    med = df.groupby("simbolo_titulo")["precio"].transform("median")
    ratio = df["precio"] / med
    forced.append(df[(ratio < 0.02) | (ratio > 50)].head(20))  # posibles errores de escala
    forced.append(df.nlargest(5, "cantidad"))
    forced_df = pd.concat(forced).drop_duplicates("id_transaccion")

    remaining = max(n - len(forced_df), 0)
    pool = df[~df.id_transaccion.isin(forced_df.id_transaccion)]
    frac = remaining / len(pool)
    strat = pd.concat(
        g.sample(max(1, round(len(g) * frac)), random_state=seed)
        for _, g in pool.groupby(["_month", "origen", "moneda"])
    )
    sample = pd.concat([forced_df, strat]).drop_duplicates("id_transaccion")
    return sample.drop(columns=["_month", "_dow"]).sort_values("fecha").reset_index(drop=True)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("source")
    p.add_argument("target")
    p.add_argument("--salt", required=True, help="secreto; NO commitear")
    p.add_argument("--n", type=int, default=5000)
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args()

    df = pd.read_csv(a.source, dtype=str, keep_default_na=False)
    df["cantidad"] = df["cantidad"].astype("int64")
    df["precio"] = df["precio"].astype("float64")
    s = build_sample(df, a.n, a.seed)
    s["id_cliente"] = s["id_cliente"].map(lambda v: pseudonym(v, a.salt, "CLI", 8, "0123456789ABCDEF"))
    s["id_transaccion"] = s["id_transaccion"].map(lambda v: pseudonym(v, a.salt, "TXN", 13, ALNUM))
    assert s["id_transaccion"].is_unique, "colisión de pseudónimos de transacción"
    s.to_csv(a.target, index=False)
    print(f"muestra: {len(s):,} filas, {s.id_cliente.nunique():,} clientes, {s.simbolo_titulo.nunique():,} símbolos -> {a.target}")


if __name__ == "__main__":
    main()

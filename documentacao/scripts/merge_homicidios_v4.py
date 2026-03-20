#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Faz merge da base v4 com a consolidacao de homicidios municipais de 2022 via codigo IBGE.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
V4_FILE = BASE_DIR / "prata" / "processamento" / "merge_v4.csv"
HOMICIDIOS_FILE = BASE_DIR / "prata" / "pre_merge" / "homicidios_municipais_2022.csv"
OUTPUT_FILE = (
    BASE_DIR
    / "prata"
    / "processamento"
    / "merge_v5.csv"
)


def normalizar_codigo(valor: object) -> pd.NA | str:
    if pd.isna(valor):
        return pd.NA

    texto = str(valor).strip()
    if texto.endswith(".0"):
        texto = texto[:-2]
    return texto or pd.NA


def carregar_v4() -> pd.DataFrame:
    df = pd.read_csv(V4_FILE)
    if "Cód." not in df.columns:
        raise ValueError("Coluna 'Cód.' nao encontrada na base v4.")

    df["_codigo_merge"] = df["Cód."].map(normalizar_codigo)
    return df


def carregar_homicidios() -> pd.DataFrame:
    df = pd.read_csv(HOMICIDIOS_FILE)
    colunas = ["Cód_IBGE", "vitimas_homicidio_2022"]
    faltantes = [col for col in colunas if col not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas ausentes no arquivo de homicidios: {faltantes}")

    base = df[colunas].copy()
    base["_codigo_merge"] = base["Cód_IBGE"].map(normalizar_codigo)
    base["vitimas_homicidio_2022"] = pd.to_numeric(
        base["vitimas_homicidio_2022"], errors="coerce"
    ).fillna(0)

    # Consolida novamente por codigo para evitar multiplicacao de linhas por variacoes de nome.
    base = (
        base.groupby("_codigo_merge", as_index=False)["vitimas_homicidio_2022"]
        .sum()
        .copy()
    )
    return base


def main() -> int:
    v4 = carregar_v4()
    homicidios = carregar_homicidios()

    resultado = v4.merge(homicidios, on="_codigo_merge", how="left", indicator=True)
    correspondencias = resultado["_merge"].eq("both").sum()
    resultado["vitimas_homicidio_2022"] = resultado["vitimas_homicidio_2022"].fillna(0)
    resultado = resultado.drop(columns=["_codigo_merge", "_merge"])

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print(f"Arquivo gerado: {OUTPUT_FILE}")
    print(f"Linhas na base final: {len(resultado)}")
    print(f"Municipios com valor de homicidios apos merge: {correspondencias}")
    print(f"Municipios sem correspondencia na base de homicidios: {len(resultado) - correspondencias}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

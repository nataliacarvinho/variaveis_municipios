#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Faz merge da tabela 9584 em valores absolutos com a base v7 via codigo do municipio.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
V7_FILE = BASE_DIR / "prata" / "processamento" / "merge_v7.csv"
TABELA9584_FILE = BASE_DIR / "bronze" / "tabela9584.csv"
OUTPUT_FILE = BASE_DIR / "prata" / "processamento" / "merge_v8.csv"

RENOMEAR_COLUNAS = {
    "Via pavimentada - Existe": "Via pavimentada - Existe (N)",
    "Existência de iluminação pública - Existe": "Existência de iluminação pública - Existe (N)",
    "Existência de calçada / passeio - Existe": "Existência de calçada / passeio - Existe (N)",
}


def normalizar_codigo(valor: object) -> pd.NA | str:
    if pd.isna(valor):
        return pd.NA

    texto = str(valor).strip()
    if texto.endswith(".0"):
        texto = texto[:-2]
    return texto or pd.NA


def carregar_v7() -> pd.DataFrame:
    df = pd.read_csv(V7_FILE)
    if "Cód." not in df.columns:
        raise ValueError("Coluna 'Cód.' nao encontrada na base v7.")

    df["_codigo_merge"] = df["Cód."].map(normalizar_codigo)
    return df


def carregar_tabela9584() -> pd.DataFrame:
    df = pd.read_csv(TABELA9584_FILE, skiprows=5)

    colunas_necessarias = ["Cód."] + list(RENOMEAR_COLUNAS.keys())
    faltantes = [col for col in colunas_necessarias if col not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas ausentes na tabela 9584: {faltantes}")

    df = df[colunas_necessarias].copy()
    df["Cód."] = pd.to_numeric(df["Cód."], errors="coerce")
    df = df[df["Cód."].notna()].copy()
    df = df[df["Cód."].ne(0)].copy()

    for coluna in RENOMEAR_COLUNAS:
        df[coluna] = pd.to_numeric(df[coluna], errors="coerce")

    df = df.rename(columns=RENOMEAR_COLUNAS)
    df["_codigo_merge"] = df["Cód."].map(normalizar_codigo)
    df = df.drop(columns=["Cód."])

    duplicados = df["_codigo_merge"].dropna().duplicated().sum()
    if duplicados:
        raise ValueError(f"Foram encontrados {duplicados} codigos duplicados na tabela 9584 absoluta.")

    return df


def main() -> int:
    v7 = carregar_v7()
    tabela9584 = carregar_tabela9584()

    resultado = v7.merge(tabela9584, on="_codigo_merge", how="left")
    primeira_coluna = list(RENOMEAR_COLUNAS.values())[0]
    correspondencias = resultado[primeira_coluna].notna().sum()
    resultado = resultado.drop(columns=["_codigo_merge"])

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print(f"Arquivo gerado: {OUTPUT_FILE}")
    print(f"Linhas na base final: {len(resultado)}")
    print(f"Municipios com correspondencia na tabela 9584 absoluta: {correspondencias}")
    print(f"Municipios sem correspondencia na tabela 9584 absoluta: {len(resultado) - correspondencias}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

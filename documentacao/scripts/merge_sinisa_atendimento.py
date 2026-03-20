#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Faz merge da aba Atendimento do SINISA com a base municipal final via codigo do municipio.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
ATENDIMENTO_CSV = (
    BASE_DIR
    / "prata"
    / "pre_merge"
    / "sinisa_esgoto_base_municipal"
    / "sinisa_esgoto_indicadores_base_municipal_2023_v2__atendimento_processado.csv"
)
BASE_MUNICIPAL_CSV = (
    BASE_DIR
    / "prata"
    / "processamento"
    / "merge_v3.csv"
)
OUTPUT_CSV = (
    BASE_DIR
    / "prata"
    / "processamento"
    / "merge_v4.csv"
)

COLUNAS_ATENDIMENTO = [
    "Atendimento da população total com rede coletora de esgoto",
    "Atendimento da população urbana com rede coletora de esgoto",
    "Atendimento dos domicílios totais com rede coletora de esgoto",
    "Atendimento dos domicílios urbanos com rede coletora de esgoto",
    "Atendimento dos domicílios totais com coleta e tratamento de esgoto",
    "Atendimento dos domicílios urbanos com coleta e tratamento de esgoto",
]


def normalizar_codigo(valor: object) -> pd.NA | str:
    if pd.isna(valor):
        return pd.NA

    texto = str(valor).strip()
    if texto.endswith(".0"):
        texto = texto[:-2]
    return texto or pd.NA


def carregar_atendimento() -> pd.DataFrame:
    df = pd.read_csv(ATENDIMENTO_CSV, header=7, skiprows=[8, 9])
    df.columns = [str(col).replace("\n", " ").strip() for col in df.columns]

    colunas_necessarias = ["Codigo do IBGE"] + COLUNAS_ATENDIMENTO
    faltantes = [col for col in colunas_necessarias if col not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas ausentes no arquivo de atendimento: {faltantes}")

    df = df[colunas_necessarias].copy()
    df["_codigo_merge"] = df["Codigo do IBGE"].map(normalizar_codigo)
    df = df.drop(columns=["Codigo do IBGE"])
    return df


def carregar_base() -> pd.DataFrame:
    df = pd.read_csv(BASE_MUNICIPAL_CSV)
    if "Cód." not in df.columns:
        raise ValueError("Coluna 'Cód.' nao encontrada na base municipal final.")

    df["_codigo_merge"] = df["Cód."].map(normalizar_codigo)
    return df


def main() -> int:
    atendimento = carregar_atendimento()
    base = carregar_base()

    duplicados = atendimento["_codigo_merge"].dropna().duplicated().sum()
    if duplicados:
        raise ValueError(f"Foram encontrados {duplicados} codigos duplicados em atendimento.")

    resultado = base.merge(atendimento, on="_codigo_merge", how="left")
    resultado = resultado.drop(columns=["_codigo_merge"])
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")

    correspondencias = resultado[COLUNAS_ATENDIMENTO[0]].notna().sum()
    print(f"Arquivo gerado: {OUTPUT_CSV}")
    print(f"Linhas na base final: {len(resultado)}")
    print(f"Municipios com correspondencia no atendimento: {correspondencias}")
    print(f"Municipios sem correspondencia no atendimento: {len(resultado) - correspondencias}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

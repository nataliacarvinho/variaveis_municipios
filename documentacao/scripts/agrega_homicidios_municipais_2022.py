#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Consolida os CSVs de indicadores de seguranca publica em uma base anual de homicidios por municipio para 2022.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_DIR = BASE_DIR / "prata" / "pre_merge" / "indicadores_seguranca_publica_municipal"
OUTPUT_FILE = BASE_DIR / "prata" / "pre_merge" / "homicidios_municipais_2022.csv"


COLUNAS_ESPERADAS = [
    "Cód_IBGE",
    "Município",
    "Sigla UF",
    "Região",
    "Mês/Ano",
    "Vítimas",
]


def carregar_bases() -> pd.DataFrame:
    arquivos = sorted(INPUT_DIR.glob("*.csv"))
    if not arquivos:
        raise FileNotFoundError(f"Nenhum CSV encontrado em: {INPUT_DIR}")

    dfs: list[pd.DataFrame] = []
    for arquivo in arquivos:
        df = pd.read_csv(arquivo)
        faltantes = [col for col in COLUNAS_ESPERADAS if col not in df.columns]
        if faltantes:
            raise ValueError(f"Colunas ausentes em {arquivo.name}: {faltantes}")
        dfs.append(df[COLUNAS_ESPERADAS].copy())

    return pd.concat(dfs, ignore_index=True)


def preparar_base(df: pd.DataFrame) -> pd.DataFrame:
    base = df.copy()
    base["Mês/Ano"] = pd.to_datetime(base["Mês/Ano"], errors="coerce")
    base["Vítimas"] = pd.to_numeric(base["Vítimas"], errors="coerce").fillna(0)
    base["Cód_IBGE"] = base["Cód_IBGE"].astype("Int64")
    base = base[base["Mês/Ano"].dt.year == 2022].copy()
    return base


def agregar_municipios(df: pd.DataFrame) -> pd.DataFrame:
    resultado = (
        df.groupby(["Cód_IBGE", "Município", "Sigla UF", "Região"], as_index=False)["Vítimas"]
        .sum()
        .rename(columns={"Vítimas": "vitimas_homicidio_2022"})
        .sort_values(["Sigla UF", "Município"])
        .reset_index(drop=True)
    )
    return resultado


def main() -> int:
    base = carregar_bases()
    base_2022 = preparar_base(base)
    resultado = agregar_municipios(base_2022)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print(f"Linhas lidas: {len(base)}")
    print(f"Linhas filtradas para 2022: {len(base_2022)}")
    print(f"Municipios consolidados: {len(resultado)}")
    print(f"Arquivo gerado: {OUTPUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

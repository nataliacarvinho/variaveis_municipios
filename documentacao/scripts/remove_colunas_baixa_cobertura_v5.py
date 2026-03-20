#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera a v6 removendo colunas de baixa cobertura nacional da base v5.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_FILE = (
    BASE_DIR
    / "prata"
    / "processamento"
    / "merge_v5.csv"
)
OUTPUT_FILE = (
    BASE_DIR
    / "prata"
    / "processamento"
    / "merge_v6.csv"
)

COLUNAS_REMOVER = [
    "taxa_escolarizacao_2024",
    "Valor Receita Prevista",
    "Valor Receita Realizada",
]


def main() -> int:
    df = pd.read_csv(INPUT_FILE)

    faltantes = [col for col in COLUNAS_REMOVER if col not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas ausentes na v5: {faltantes}")

    resultado = df.drop(columns=COLUNAS_REMOVER)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print(f"Arquivo gerado: {OUTPUT_FILE}")
    print(f"Linhas na base final: {len(resultado)}")
    print(f"Colunas removidas: {', '.join(COLUNAS_REMOVER)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

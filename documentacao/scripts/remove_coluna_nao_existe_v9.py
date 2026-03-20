#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gera a v10 removendo a coluna de iluminacao publica "Nao existe" da base v9.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_FILE = BASE_DIR / "prata" / "processamento" / "merge_v9.csv"
OUTPUT_FILE = BASE_DIR / "prata" / "processamento" / "merge_v10.csv"

COLUNA_REMOVER = "Existência de iluminação pública - Não existe (%)"


def main() -> int:
    df = pd.read_csv(INPUT_FILE)

    if COLUNA_REMOVER not in df.columns:
        raise ValueError(f"Coluna ausente na v9: {COLUNA_REMOVER}")

    resultado = df.drop(columns=[COLUNA_REMOVER])
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print(f"Arquivo gerado: {OUTPUT_FILE}")
    print(f"Linhas na base final: {len(resultado)}")
    print(f"Coluna removida: {COLUNA_REMOVER}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

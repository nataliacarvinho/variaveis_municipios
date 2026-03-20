#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Exporta cada aba das planilhas de esgoto da pasta Base Municipal para um CSV separado.

Os arquivos de saida recebem nomes normalizados e o sufixo `_processado`.
"""

from __future__ import annotations

import re
import unicodedata
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_DIR = (
    BASE_DIR
    / "bronze"
    / "sinisa_esgoto_planilhas_2023_v2"
    / "esgoto_base_municipal"
)
OUTPUT_DIR = BASE_DIR / "prata" / "pre_merge" / "sinisa_esgoto_base_municipal"


def slugify(texto: str) -> str:
    texto = unicodedata.normalize("NFKD", str(texto))
    texto = "".join(ch for ch in texto if not unicodedata.combining(ch))
    texto = texto.lower()
    texto = re.sub(r"[^a-z0-9]+", "_", texto)
    return re.sub(r"_+", "_", texto).strip("_")


def exportar_planilha(caminho_xlsx: Path) -> list[Path]:
    planilha = pd.ExcelFile(caminho_xlsx)
    prefixo = slugify(caminho_xlsx.stem)
    arquivos_gerados: list[Path] = []

    for aba in planilha.sheet_names:
        df = pd.read_excel(caminho_xlsx, sheet_name=aba)
        nome_saida = f"{prefixo}__{slugify(aba)}_processado.csv"
        caminho_saida = OUTPUT_DIR / nome_saida
        df.to_csv(caminho_saida, index=False, encoding="utf-8")
        arquivos_gerados.append(caminho_saida)

    return arquivos_gerados


def main() -> int:
    if not INPUT_DIR.exists():
        raise FileNotFoundError(f"Pasta de entrada nao encontrada: {INPUT_DIR}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    total = 0
    for caminho_xlsx in sorted(INPUT_DIR.glob("*.xlsx")):
        print(f"\nProcessando: {caminho_xlsx.name}")
        for arquivo in exportar_planilha(caminho_xlsx):
            print(f"  - Gerado: {arquivo.name}")
            total += 1

    print(f"\nTotal de CSVs gerados: {total}")
    print(f"Pasta de saida: {OUTPUT_DIR.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

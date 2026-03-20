#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Exporta cada aba da planilha de indicadores de seguranca publica municipal para um CSV separado.
"""

from __future__ import annotations

import re
import unicodedata
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_FILE = BASE_DIR / "bronze" / "indicadoressegurancapublicamunic.xlsx"
OUTPUT_DIR = BASE_DIR / "prata" / "pre_merge" / "indicadores_seguranca_publica_municipal"


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
        nome_saida = f"{prefixo}__{slugify(aba)}.csv"
        caminho_saida = OUTPUT_DIR / nome_saida
        df.to_csv(caminho_saida, index=False, encoding="utf-8")
        arquivos_gerados.append(caminho_saida)

    return arquivos_gerados


def main() -> int:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Arquivo de entrada nao encontrado: {INPUT_FILE}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    arquivos = exportar_planilha(INPUT_FILE)
    print(f"Arquivo processado: {INPUT_FILE.name}")
    print(f"Total de CSVs gerados: {len(arquivos)}")
    print(f"Pasta de saida: {OUTPUT_DIR.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

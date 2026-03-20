#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Faz merge da tabela 9584 de iluminacao com a base v8, descartando a coluna duplicada ja existente.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
V8_FILE = BASE_DIR / "prata" / "processamento" / "merge_v8.csv"
TABELA9584_FILE = BASE_DIR / "bronze" / "tabela9584 (1).csv"
OUTPUT_FILE = BASE_DIR / "prata" / "processamento" / "merge_v9.csv"

COLUNA_DUPLICADA = "Existência de iluminação pública - Existe (%)"
COLUNA_ORIGEM_DUPLICADA = "Existência de iluminação pública - Existe"
COLUNA_ORIGEM_NOVA = "Existência de iluminação pública - Não existe"
COLUNA_NOVA = "Existência de iluminação pública - Não existe (%)"

COLUNAS_ORDENADAS_POR_TEMA = [
    "Via pavimentada - Existe (%)",
    "Via pavimentada - Existe (N)",
    "Existência de iluminação pública - Existe (%)",
    "Existência de iluminação pública - Não existe (%)",
    "Existência de iluminação pública - Existe (N)",
    "Existência de calçada / passeio - Existe (%)",
    "Existência de calçada / passeio - Existe (N)",
]


def normalizar_codigo(valor: object) -> pd.NA | str:
    if pd.isna(valor):
        return pd.NA

    texto = str(valor).strip()
    if texto.endswith(".0"):
        texto = texto[:-2]
    return texto or pd.NA


def carregar_v8() -> pd.DataFrame:
    df = pd.read_csv(V8_FILE)
    if "Cód." not in df.columns:
        raise ValueError("Coluna 'Cód.' nao encontrada na base v8.")

    if COLUNA_DUPLICADA not in df.columns:
        raise ValueError(f"Coluna esperada nao encontrada na v8: {COLUNA_DUPLICADA}")

    df["_codigo_merge"] = df["Cód."].map(normalizar_codigo)
    return df


def carregar_tabela() -> pd.DataFrame:
    df = pd.read_csv(TABELA9584_FILE, skiprows=5)
    colunas_necessarias = ["Cód.", COLUNA_ORIGEM_DUPLICADA, COLUNA_ORIGEM_NOVA]
    faltantes = [col for col in colunas_necessarias if col not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas ausentes na tabela 9584 (1): {faltantes}")

    df = df[colunas_necessarias].copy()
    df["Cód."] = pd.to_numeric(df["Cód."], errors="coerce")
    df = df[df["Cód."].notna()].copy()
    df = df[df["Cód."].ne(0)].copy()

    df[COLUNA_ORIGEM_DUPLICADA] = pd.to_numeric(df[COLUNA_ORIGEM_DUPLICADA], errors="coerce")
    df[COLUNA_ORIGEM_NOVA] = pd.to_numeric(df[COLUNA_ORIGEM_NOVA], errors="coerce")
    df["_codigo_merge"] = df["Cód."].map(normalizar_codigo)

    duplicados = df["_codigo_merge"].dropna().duplicated().sum()
    if duplicados:
        raise ValueError(f"Foram encontrados {duplicados} codigos duplicados na tabela 9584 (1).")

    # Mantem apenas a coluna nova; a coluna "Existe" ja esta presente na v8.
    df = df.rename(columns={COLUNA_ORIGEM_NOVA: COLUNA_NOVA})
    return df[["_codigo_merge", COLUNA_NOVA]].copy()


def main() -> int:
    v8 = carregar_v8()
    tabela = carregar_tabela()

    resultado = v8.merge(tabela, on="_codigo_merge", how="left")
    correspondencias = resultado[COLUNA_NOVA].notna().sum()
    resultado = resultado.drop(columns=["_codigo_merge"])

    colunas_presentes = [col for col in COLUNAS_ORDENADAS_POR_TEMA if col in resultado.columns]
    colunas_restantes = [col for col in resultado.columns if col not in colunas_presentes]
    resultado = resultado[colunas_restantes + colunas_presentes]

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print(f"Arquivo gerado: {OUTPUT_FILE}")
    print(f"Linhas na base final: {len(resultado)}")
    print(f"Municipios com correspondencia na tabela 9584 (1): {correspondencias}")
    print(f"Municipios sem correspondencia na tabela 9584 (1): {len(resultado) - correspondencias}")
    print(f"Coluna duplicada descartada: {COLUNA_DUPLICADA}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

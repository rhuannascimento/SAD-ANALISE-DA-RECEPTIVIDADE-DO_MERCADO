#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script para extrair CNAEs únicos de um arquivo CSV.

Este script lê o arquivo rais-combinado.csv e extrai as colunas 'CNAE' e 'ID CNAE',
salvando apenas os registros com IDs CNAE únicos em um novo arquivo CSV.
"""

import pandas as pd
import os
from pathlib import Path


def extrair_cnaes_unicos(arquivo_entrada: str, arquivo_saida: str) -> None:
    """
    Extrai CNAEs únicos de um arquivo CSV.

    Args:
        arquivo_entrada: Caminho para o arquivo CSV de entrada
        arquivo_saida: Caminho para o arquivo CSV de saída
    """
    print(f"Lendo arquivo: {arquivo_entrada}")

    df = pd.read_csv(arquivo_entrada, usecols=["CNAE", "ID CNAE"], sep=";")

    df_unicos = df.drop_duplicates(subset=["ID CNAE"])

    df_unicos = df_unicos.sort_values(by=["ID CNAE"])

    df_unicos.to_csv(arquivo_saida, index=False)

    print(f"Arquivo salvo: {arquivo_saida}")
    print(f"Total de CNAEs únicos extraídos: {len(df_unicos)}")


def main():
    """Função principal do script."""

    arquivo_entrada = "./data/rais-combinado.csv"
    arquivo_saida = "cnaes_unicos.csv"

    extrair_cnaes_unicos(str(arquivo_entrada), str(arquivo_saida))


if __name__ == "__main__":
    main()

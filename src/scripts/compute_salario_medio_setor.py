#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Agrega o campo "Salário Médio" por Ano e SETOR a partir de
`data/rais_com_cnaes_setor.csv` e escreve `data/salario_medio_por_setor.csv`.

- Entrada: CSV separado por `;` com colunas contendo Ano, SETOR e Salário Médio.
- Saída: CSV separado por `;` com colunas: ano;setor;salario_mediana

O script é preparado para arquivos grandes: faz leitura em streaming e acumula listas
de valores por chave (ano,setor) para calcular a mediana no final. Isso assume que
o número de combinações (ano,setor) é bem menor que o número de linhas; caso contrário
podemos adaptar para um algoritmo streaming por chave (dois heaps por chave).
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Tuple
import statistics

# logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("compute_salario_medio_setor")


def open_text(path: Path, encoding: str = "utf-8"):
    try:
        return path.open("r", encoding=encoding, newline="")
    except UnicodeDecodeError:
        logger.warning("Falha ao abrir %s com %s; tentando latin-1", path, encoding)
        return path.open("r", encoding="latin-1", newline="")


def normalize_header(h: str) -> str:
    return h.strip().lower()


def choose_column(fieldnames, candidates):
    norm_map = {normalize_header(fn): fn for fn in fieldnames}
    for cand in candidates:
        real = norm_map.get(normalize_header(cand))
        if real:
            return real
    return None


def parse_decimal(value: str) -> float:
    """Converte string numérica comum brasileira/inglesa para float.
    Tenta '1234.56', '1.234,56', '1234,56' e variações.
    Retorna 0.0 em caso de vazio ou falha.
    """
    if value is None:
        return 0.0
    s = value.strip()
    if s == "":
        return 0.0
    # remove possíveis aspas
    s = s.replace('"', "")
    # primeiro tenta forma direta com ponto decimal
    try:
        return float(s)
    except ValueError:
        pass
    # substitui vírgula por ponto (caso 1234,56)
    try:
        return float(s.replace(",", "."))
    except ValueError:
        pass
    # tenta remover separadores de milhar (pontos) e trocar vírgula por ponto
    try:
        return float(s.replace(".", "").replace(",", "."))
    except ValueError:
        logger.debug("Não foi possível converter valor numérico: %s", value)
        return 0.0


def aggregate_median(
    rais_path: Path, report_every: int = 100_000, include_zeros: bool = False
) -> Dict[Tuple[str, str], float]:
    """Agrega listas do campo Salário Médio por (ano, setor) e retorna a mediana por chave."""
    logger.info("Agregando mediana do Salário Médio por Ano+SETOR de: %s", rais_path)
    bins = defaultdict(list)  # type: ignore

    with open_text(rais_path) as fh:
        reader = csv.DictReader(fh, delimiter=";")
        if reader.fieldnames is None:
            raise ValueError(f"Arquivo {rais_path} não tem header reconhecível")

        ano_col = choose_column(reader.fieldnames, ("Ano", "ano", "ANO"))
        setor_col = choose_column(reader.fieldnames, ("SETOR", "setor"))
        salario_col = choose_column(
            reader.fieldnames,
            (
                "Salário Médio",
                "Salario Medio",
                "salario medio",
                "salario_medio",
                "Salário Medio",
            ),
        )

        if not ano_col or not setor_col or not salario_col:
            logger.error(
                "Colunas esperadas não encontradas. Encontradas: %s", reader.fieldnames
            )
            raise SystemExit(1)

        processed = 0
        for row in reader:
            processed += 1
            ano = row.get(ano_col, "").strip()
            setor = row.get(setor_col, "").strip()
            salario_raw = row.get(salario_col, "")
            if not ano or not setor:
                # ignora linhas sem ano ou setor
                continue

            salario = parse_decimal(salario_raw)
            # por padrão ignoramos salários iguais a zero (muitos registros nulos/zerados)
            if (not include_zeros) and salario == 0.0:
                continue
            key = (ano, setor)
            bins[key].append(salario)

            if processed % report_every == 0:
                logger.info(
                    "Linhas processadas: %d; chaves distintas: %d", processed, len(bins)
                )

    # calcula medianas
    medians: Dict[Tuple[str, str], float] = {}
    for key, values in bins.items():
        if not values:
            medians[key] = 0.0
            continue
        try:
            medians[key] = float(statistics.median(values))
        except Exception:
            # fallback: sort and compute manually
            vals = sorted(values)
            n = len(vals)
            mid = n // 2
            if n % 2 == 1:
                medians[key] = float(vals[mid])
            else:
                medians[key] = float((vals[mid - 1] + vals[mid]) / 2.0)

    logger.info("Agregação concluída. Chaves: %d", len(medians))
    return medians


def write_output(out_path: Path, means: Dict[Tuple[str, str], float]):
    logger.info("Escrevendo resultado em: %s", out_path)
    with out_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, delimiter=";")
        writer.writerow(["ano", "setor", "salario_medio"])
        for (ano, setor), avg in sorted(
            means.items(), key=lambda kv: (kv[0][0], kv[0][1])
        ):
            writer.writerow([ano, setor, f"{avg:.6f}"])


def parse_args():
    p = argparse.ArgumentParser(
        description="Calcula salário médio por setor/ano a partir de rais_com_cnaes_setor"
    )
    p.add_argument(
        "--rais", required=True, help="Arquivo rais_com_cnaes_setor.csv (separador ;)"
    )
    p.add_argument("--out", required=True, help="Arquivo de saída (separador ;)")
    p.add_argument(
        "--report-every", type=int, default=100_000, help="Frequência de log em linhas"
    )
    p.add_argument(
        "--include-zeros",
        action="store_true",
        help="Incluir salários iguais a zero ao calcular a mediana",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    rais_path = Path(args.rais)
    out_path = Path(args.out)

    if not rais_path.exists():
        logger.error("Arquivo rais não encontrado: %s", rais_path)
        raise SystemExit(1)

    medians = aggregate_median(
        rais_path, report_every=args.report_every, include_zeros=args.include_zeros
    )
    # Reusa write_output, mas altera header para 'salario_mediana'
    logger.info("Escrevendo resultado em: %s", out_path)
    with out_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, delimiter=";")
        writer.writerow(["ano", "setor", "salario_mediana"])
        for (ano, setor), avg in sorted(
            medians.items(), key=lambda kv: (kv[0][0], kv[0][1])
        ):
            writer.writerow([ano, setor, f"{avg:.6f}"])

    logger.info("Arquivo gerado: %s (linhas: %d)", out_path, len(medians))


if __name__ == "__main__":
    main()

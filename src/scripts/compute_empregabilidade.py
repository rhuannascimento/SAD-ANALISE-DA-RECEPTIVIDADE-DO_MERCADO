#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Agrega `data/rais_com_cnaes_setor.csv` por Ano e SETOR e calcula a empregabilidade:

  empregabilidade = empregados / (empregados + desempregados)
  desempregados = empregados * (taxa_desocupacao / (100 - taxa_desocupacao))

A taxa por ano vem de `data/desocupacao.json`.
Saída: CSV separado por `;` com colunas: ano;setor;empregabilidade

O script é preparado para arquivos grandes (agregação em dicionário em memória; há uma entrada por combinação ano+setor).
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, Mapping, Optional, Tuple


# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("compute_empregabilidade")


def open_text(path: Path, encoding: str = "utf-8"):
    try:
        return path.open("r", encoding=encoding, newline="")
    except UnicodeDecodeError:
        logger.warning("Falha ao abrir %s com %s; tentando latin-1", path, encoding)
        return path.open("r", encoding="latin-1", newline="")


def normalize_header(h: str) -> str:
    return h.strip().lower()


def choose_column(
    fieldnames: Iterable[str], candidates: Iterable[str]
) -> Optional[str]:
    norm_map = {normalize_header(fn): fn for fn in fieldnames}
    for cand in candidates:
        real = norm_map.get(normalize_header(cand))
        if real:
            return real
    return None


def load_desocupacao(path: Path) -> Dict[str, float]:
    logger.info("Carregando taxas de desocupação: %s", path)
    with open_text(path) as fh:
        data = json.load(fh)
    # keys may be strings like "2019"; ensure floats
    result: Dict[str, float] = {}
    for k, v in data.items():
        try:
            result[str(k).strip()] = float(v)
        except Exception:
            logger.warning("Valor inválido para taxa de desocupação ano=%s: %s", k, v)
    logger.info("Taxas carregadas para anos: %s", sorted(result.keys()))
    return result


def aggregate_empregos(
    rais_path: Path, report_every: int = 100_000
) -> Dict[Tuple[str, str], float]:
    logger.info("Agregando empregados por Ano+SETOR de: %s", rais_path)
    counts: Dict[Tuple[str, str], float] = defaultdict(float)
    with open_text(rais_path) as fh:
        reader = csv.DictReader(fh, delimiter=";")
        if reader.fieldnames is None:
            raise ValueError(f"Arquivo {rais_path} não tem header reconhecível")

        ano_col = choose_column(reader.fieldnames, ("Ano", "ano", "ANO"))
        setor_col = choose_column(reader.fieldnames, ("SETOR", "setor"))
        num_emp_col = choose_column(
            reader.fieldnames,
            (
                "Número de empregos",
                "Numero de empregos",
                "numero de empregos",
                "numero_de_empregos",
                "num_empregos",
            ),
        )

        if ano_col is None or setor_col is None or num_emp_col is None:
            logger.error(
                "Colunas esperadas não encontradas no arquivo rais_com_cnaes_setor.csv. Encontradas: %s",
                reader.fieldnames,
            )
            raise SystemExit(1)

        processed = 0
        for row in reader:
            processed += 1
            ano = row.get(ano_col, "").strip()
            setor = row.get(setor_col, "").strip()
            num_emp_raw = row.get(num_emp_col, "").strip().replace('"', "")
            if not ano or not setor or not num_emp_raw:
                # ignore incomplete rows but log occasionally
                if processed % (report_every * 10) == 0:
                    logger.warning(
                        "Linhas incompletas encontradas (ex.: ano/setor/num_emp vazio) na linha %d",
                        processed,
                    )
                continue
            try:
                num_emp = float(num_emp_raw.replace(",", "."))
            except ValueError:
                # tenta remover possíveis separadores de milhar
                try:
                    num_emp = float(num_emp_raw.replace(".", "").replace(",", "."))
                except ValueError:
                    logger.debug(
                        "Não foi possível converter número de empregos: %s (linha %d)",
                        num_emp_raw,
                        processed,
                    )
                    continue

            counts[(ano, setor)] += num_emp
            if processed % report_every == 0:
                logger.info(
                    "Linhas processadas: %d; chaves distintas: %d",
                    processed,
                    len(counts),
                )

    logger.info("Agregação concluída. Total de chaves (ano,setor): %d", len(counts))
    return counts


def calc_empregabilidade(empregados: float, taxa: float) -> float:
    # protege contra taxa >= 100
    if taxa >= 100.0:
        logger.warning(
            "Taxa de desocupação >= 100%% (%s). Retornando empregabilidade 0.0", taxa
        )
        return 0.0
    # desempregados = empregados * (taxa/(100 - taxa))
    denom = 100.0 - taxa
    if denom == 0:
        logger.warning("Denominador zero para taxa %s; retornando 0", taxa)
        return 0.0
    desempregados = empregados * (taxa / denom)
    total = empregados + desempregados
    if total == 0:
        return 0.0
    return empregados / total


def write_output(out_path: Path, results: Dict[Tuple[str, str], float]):
    with out_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, delimiter=";")
        writer.writerow(["ano", "setor", "empregabilidade"])
        # sort by ano then setor for deterministic output
        for (ano, setor), empreg in sorted(
            results.items(), key=lambda kv: (kv[0][0], kv[0][1])
        ):
            writer.writerow([ano, setor, f"{empreg:.6f}"])


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Calcula empregabilidade por ano e setor a partir de rais_com_cnaes_setor e desocupacao.json"
    )
    p.add_argument(
        "--rais", required=True, help="Arquivo rais_com_cnaes_setor.csv (separador ;)"
    )
    p.add_argument("--desemp", required=True, help="Arquivo desocupacao.json")
    p.add_argument("--out", required=True, help="Arquivo de saída (separador ;)")
    p.add_argument(
        "--report-every", type=int, default=100_000, help="Frequência de log em linhas"
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    rais_path = Path(args.rais)
    desemp_path = Path(args.desemp)
    out_path = Path(args.out)

    if not rais_path.exists():
        logger.error("Arquivo rais não encontrado: %s", rais_path)
        raise SystemExit(1)
    if not desemp_path.exists():
        logger.error("Arquivo desocupacao não encontrado: %s", desemp_path)
        raise SystemExit(1)

    taxas = load_desocupacao(desemp_path)
    counts = aggregate_empregos(rais_path, report_every=args.report_every)

    results: Dict[Tuple[str, str], float] = {}
    missing_taxas = set()
    for (ano, setor), empregados in counts.items():
        taxa = taxas.get(str(ano))
        if taxa is None:
            missing_taxas.add(ano)
            # pulamos esta chave para manter consistência
            continue
        empreg = calc_empregabilidade(empregados, taxa)
        results[(ano, setor)] = empreg

    if missing_taxas:
        logger.warning(
            "Taxas ausentes para anos: %s. Essas combinações foram ignoradas.",
            sorted(missing_taxas),
        )

    write_output(out_path, results)
    logger.info("Arquivo gerado: %s (linhas: %d)", out_path, len(results))


if __name__ == "__main__":
    main()

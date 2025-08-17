#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Normaliza duas colunas em um CSV para o intervalo [0,1] usando min/max do próprio arquivo.
Adiciona duas colunas: `demanda_normalizada` e `salario_mediana_normalizado`.

Uso:
  python normalize_salario_demanda.py --in data/salario_medio_por_setor.csv --out data/salario_medio_por_setor_normalizado.csv

O script é tolerante a formatos numéricos (vírgula/ponto) e preserva o separador `;`.
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from pathlib import Path
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("normalize_salario_demanda")


def open_text(path: Path, encoding: str = "utf-8"):
    try:
        return path.open("r", encoding=encoding, newline="")
    except UnicodeDecodeError:
        logger.warning("Falha ao abrir %s com %s; tentando latin-1", path, encoding)
        return path.open("r", encoding="latin-1", newline="")


def parse_decimal(value: Optional[str]) -> float:
    if value is None:
        return 0.0
    s = str(value).strip()
    if s == "":
        return 0.0
    s = s.replace('"', "")
    try:
        return float(s)
    except ValueError:
        try:
            return float(s.replace(".", "").replace(",", "."))
        except Exception:
            logger.debug("Não foi possível converter para float: %s", value)
            return 0.0


def normalize(value: float, minv: float, maxv: float) -> float:
    if maxv == minv:
        return 0.0
    return (value - minv) / (maxv - minv)


def process(
    in_path: Path,
    out_path: Path,
    demanda_col_candidates=("demanda", "Demanda"),
    salario_col_candidates=("salario_mediana", "salario_medio", "salario_mediana"),
):
    logger.info("Lendo arquivo: %s", in_path)
    # primeira passada: determinar min/max por setor (streaming, sem manter todas as linhas)
    min_dem_by_sector: dict = {}
    max_dem_by_sector: dict = {}
    min_sal_by_sector: dict = {}
    max_sal_by_sector: dict = {}

    # detecta colunas reais
    with open_text(in_path) as fh:
        reader = csv.DictReader(fh, delimiter=";")
        if reader.fieldnames is None:
            raise ValueError("Arquivo sem header reconhecível")

        def choose(fieldnames, candidates):
            norm = {c.strip().lower(): c for c in fieldnames}
            for cand in candidates:
                v = norm.get(cand.strip().lower())
                if v:
                    return v
            return None

        ano_field = choose(reader.fieldnames, ("ano",))
        setor_field = choose(reader.fieldnames, ("setor",))
        demanda_col = choose(reader.fieldnames, demanda_col_candidates)
        salario_col = choose(reader.fieldnames, salario_col_candidates)

        if demanda_col is None or salario_col is None or setor_field is None:
            logger.error(
                "Colunas esperadas não encontradas. Header: %s", reader.fieldnames
            )
            raise SystemExit(1)

        processed = 0
        for row in reader:
            processed += 1
            setor = (row.get(setor_field) or "").strip()
            if not setor:
                continue
            dem = parse_decimal(row.get(demanda_col))
            sal = parse_decimal(row.get(salario_col))

            # demanda
            prev_min = min_dem_by_sector.get(setor)
            prev_max = max_dem_by_sector.get(setor)
            if prev_min is None or dem < prev_min:
                min_dem_by_sector[setor] = dem
            if prev_max is None or dem > prev_max:
                max_dem_by_sector[setor] = dem

            # salario
            prev_min_s = min_sal_by_sector.get(setor)
            prev_max_s = max_sal_by_sector.get(setor)
            if prev_min_s is None or sal < prev_min_s:
                min_sal_by_sector[setor] = sal
            if prev_max_s is None or sal > prev_max_s:
                max_sal_by_sector[setor] = sal

            if processed % 500_000 == 0:
                logger.info(
                    "Primeira passada: linhas processadas: %d; setores encontrados: %d",
                    processed,
                    len(min_dem_by_sector),
                )

    logger.info("Min/max por setor calculados. Setores: %d", len(min_dem_by_sector))

    # segunda passada: reabre e escreve arquivo com colunas extras
    with (
        open_text(in_path) as fh_in,
        out_path.open("w", encoding="utf-8", newline="") as fh_out,
    ):
        reader2 = csv.DictReader(fh_in, delimiter=";")
        out_fieldnames = list(reader2.fieldnames) + [
            "demanda_normalizada",
            "salario_mediana_normalizado",
        ]
        writer = csv.DictWriter(fh_out, fieldnames=out_fieldnames, delimiter=";")
        writer.writeheader()

        processed = 0
        for row in reader2:
            processed += 1
            setor = (row.get(setor_field) or "").strip()
            dem = parse_decimal(row.get(demanda_col))
            sal = parse_decimal(row.get(salario_col))

            min_dem = min_dem_by_sector.get(setor, 0.0)
            max_dem = max_dem_by_sector.get(setor, min_dem)
            min_sal = min_sal_by_sector.get(setor, 0.0)
            max_sal = max_sal_by_sector.get(setor, min_sal)

            row["demanda_normalizada"] = f"{normalize(dem, min_dem, max_dem):.6f}"
            row["salario_mediana_normalizado"] = (
                f"{normalize(sal, min_sal, max_sal):.6f}"
            )
            writer.writerow(row)

            if processed % 500_000 == 0:
                logger.info("Segunda passada: linhas processadas: %d", processed)

    logger.info("Arquivo salvo: %s", out_path)


def parse_args():
    p = argparse.ArgumentParser(
        description="Normaliza demanda e salario_mediana em um CSV (0..1)"
    )
    p.add_argument(
        "--in", required=True, dest="infile", help="Arquivo de entrada (separador ;)"
    )
    p.add_argument("--out", required=True, help="Arquivo de saída (separador ;)")
    return p.parse_args()


def main():
    args = parse_args()
    in_path = Path(args.infile)
    out_path = Path(args.out)
    if not in_path.exists():
        logger.error("Arquivo não encontrado: %s", in_path)
        raise SystemExit(1)
    process(in_path, out_path)


if __name__ == "__main__":
    main()

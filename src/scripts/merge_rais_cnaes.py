#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Faz o "join" entre `data/rais-combinado.csv` e `data/cnaes_unicos.csv` por "ID CNAE" de forma streaming
(efficient e com baixo uso de memória) e salva o resultado em CSV separado por `;`.

Saída terá as colunas (nessa ordem):
  - Ano
  - ID CNAE
  - CNAE
  - Massa Salarial
  - Salário Médio
  - Número de empregos
  - Ganho de Oportunidade
  - SETOR

Pressupostos razoáveis:
  - `data/rais-combinado.csv` é separado por `;` (como informado)
  - `data/cnaes_unicos.csv` é separado por `,`
  - Arquivos podem ser muito grandes, por isso o rais é lido em streaming linha-a-linha.

Uso:
  python merge_rais_cnaes.py --rais data/rais-combinado.csv --cnaes data/cnaes_unicos.csv --out data/rais_com_cnaes_setor.csv

O script tenta abrir arquivos em UTF-8 e, em caso de falha, em latin-1.
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from pathlib import Path
from typing import Dict, Iterable, Mapping, Optional, Tuple


# --- Configuração de logging -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("merge_rais_cnaes")


# --- Utilities ---------------------------------------------------------------


def open_text(path: Path, encoding: str = "utf-8"):
    """Abre um arquivo de texto tentando UTF-8 e caindo para latin-1 se necessário."""
    try:
        return path.open("r", encoding=encoding, newline="")
    except UnicodeDecodeError:
        logger.warning("Falha ao abrir %s com %s; tentando latin-1", path, encoding)
        return path.open("r", encoding="latin-1", newline="")


def normalize_header(h: str) -> str:
    """Normaliza nomes de colunas para comparação (lower, sem espaços extras)."""
    return h.strip().lower()


def choose_column(
    fieldnames: Iterable[str], candidates: Iterable[str]
) -> Optional[str]:
    """Dado um iterável de nomes reais e candidatos (em ord. de preferência), retorna o nome real correspondente."""
    norm_map = {normalize_header(fn): fn for fn in fieldnames}
    for cand in candidates:
        real = norm_map.get(normalize_header(cand))
        if real:
            return real
    return None


# --- Carregamento do mapa SETOR por ID CNAE ---------------------------------


def load_cnaes_unicos(
    path: Path,
    id_cnae_candidates: Tuple[str, ...] = ("ID CNAE", "id_cnae", "id cnae"),
    setor_candidates: Tuple[str, ...] = ("SETOR", "setor"),
) -> Dict[str, str]:
    """
    Carrega `cnaes_unicos.csv` em memória como um mapa de ID CNAE -> SETOR.

    Assumimos que este arquivo é razoavelmente pequeno (comparado ao rais) e pode caber em memória.
    Se for grande, essa função ainda funciona, mas pode ser adaptada para usar um banco leve (sqlite) em alternativa.
    """
    path = Path(path)
    logger.info("Carregando CNAEs únicos de: %s", path)

    with open_text(path) as fh:
        # for cnaes_unicos, o separador informado foi ","
        reader = csv.DictReader(fh, delimiter=",")
        if reader.fieldnames is None:
            raise ValueError(f"Arquivo {path} não tem header reconhecível")

        id_col = choose_column(reader.fieldnames, id_cnae_candidates)
        setor_col = choose_column(reader.fieldnames, setor_candidates)

        if id_col is None:
            raise ValueError(
                f"Não foi possível localizar coluna ID CNAE em {path}; encontrados: {reader.fieldnames}"
            )
        if setor_col is None:
            raise ValueError(
                f"Não foi possível localizar coluna SETOR em {path}; encontrados: {reader.fieldnames}"
            )

        mapping: Dict[str, str] = {}
        total = 0
        for row in reader:
            total += 1
            key = row.get(id_col, "").strip()
            if not key:
                continue
            mapping[key] = row.get(setor_col, "").strip()

    logger.info("CNAEs carregados: %d", len(mapping))
    return mapping


# --- Merge streaming --------------------------------------------------------


def stream_merge(
    rais_path: Path,
    cnaes_map: Mapping[str, str],
    out_path: Path,
    rais_delimiter: str = ";",
    out_delimiter: str = ";",
    report_every: int = 100_000,
) -> None:
    """
    Percorre `rais_path` linha-a-linha, anexa a coluna SETOR a partir de `cnaes_map` e grava em `out_path`.

    O arquivo de saída será separado por `out_delimiter`.
    """
    rais_path = Path(rais_path)
    out_path = Path(out_path)

    logger.info("Iniciando merge: %s -> %s", rais_path, out_path)

    with (
        open_text(rais_path) as rin,
        out_path.open("w", encoding="utf-8", newline="") as wout,
    ):
        reader = csv.DictReader(rin, delimiter=rais_delimiter)
        if reader.fieldnames is None:
            raise ValueError(f"Arquivo {rais_path} não tem header reconhecível")

        # Mapeamentos de nomes esperados (candidatos) para localizar colunas no rais
        col_candidates = {
            "ano": ("Ano", "ano", "ANo", "ANO"),
            "id_cnae": ("ID CNAE", "id_cnae", "id cnae", "idcnae"),
            "cnae": ("CNAE", "cnae"),
            "massa_salarial": ("Massa Salarial", "massa_salarial", "massa salarial"),
            "salario_medio": (
                "Salário Médio",
                "Salario Medio",
                "salario medio",
                "salario_medio",
            ),
            "num_empregos": (
                "Número de empregos",
                "Numero de empregos",
                "numero de empregos",
                "numero_de_empregos",
                "numero_de_empregos",
            ),
            "ganho_oportunidade": (
                "Ganho de Oportunidade",
                "Ganho de oportunidade",
                "ganho oportunidade",
                "ganho_oportunidade",
            ),
        }

        # Escolhe as colunas reais presentes no arquivo rais
        chosen: Dict[str, str] = {}
        for key, candidates in col_candidates.items():
            col = choose_column(reader.fieldnames, candidates)
            if col is None:
                logger.error(
                    "Coluna esperada não encontrada no rais: %s (candidatos: %s)",
                    key,
                    candidates,
                )
                # Não levantamos aqui; permitimos continuidade e usaremos valores vazios
                chosen[key] = None  # type: ignore
            else:
                chosen[key] = col

        # Ordem de saída requerida
        out_fieldnames = [
            "Ano",
            "ID CNAE",
            "CNAE",
            "Massa Salarial",
            "Salário Médio",
            "Número de empregos",
            "Ganho de Oportunidade",
            "SETOR",
        ]

        writer = csv.DictWriter(
            wout, fieldnames=out_fieldnames, delimiter=out_delimiter
        )
        writer.writeheader()

        processed = 0
        written = 0

        for row in reader:
            processed += 1

            # Extrai campos com tolerância à ausência
            def get_col(name_key: str) -> str:
                colname = chosen.get(name_key)
                if not colname:
                    return ""
                return row.get(colname, "").strip()

            id_cnae = get_col("id_cnae")
            setor = cnaes_map.get(id_cnae, "") if id_cnae else ""

            out_row = {
                "Ano": get_col("ano"),
                "ID CNAE": id_cnae,
                "CNAE": get_col("cnae"),
                "Massa Salarial": get_col("massa_salarial"),
                "Salário Médio": get_col("salario_medio"),
                "Número de empregos": get_col("num_empregos"),
                "Ganho de Oportunidade": get_col("ganho_oportunidade"),
                "SETOR": setor,
            }

            writer.writerow(out_row)
            written += 1

            if processed % report_every == 0:
                logger.info("Linhas processadas: %d (escritas: %d)", processed, written)

    logger.info(
        "Merge concluído. Linhas processadas: %d, escritas: %d", processed, written
    )


# --- CLI --------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Merge rais-combinado.csv com cnaes_unicos.csv por ID CNAE (streaming)"
    )

    p.add_argument(
        "--rais", required=True, help="Caminho para rais-combinado.csv (separador ;)"
    )
    p.add_argument(
        "--cnaes", required=True, help="Caminho para cnaes_unicos.csv (separador ,)"
    )
    p.add_argument(
        "--out", required=True, help="Arquivo de saída (será separado por ;)"
    )
    p.add_argument(
        "--report-every",
        type=int,
        default=100_000,
        help="Frequência de log em linhas (default: 100000)",
    )

    return p.parse_args()


def main() -> None:
    args = parse_args()

    rais_path = Path(args.rais)
    cnaes_path = Path(args.cnaes)
    out_path = Path(args.out)

    if not rais_path.exists():
        logger.error("Arquivo rais não encontrado: %s", rais_path)
        raise SystemExit(1)
    if not cnaes_path.exists():
        logger.error("Arquivo cnaes_unicos não encontrado: %s", cnaes_path)
        raise SystemExit(1)

    cnaes_map = load_cnaes_unicos(cnaes_path)
    stream_merge(
        rais_path,
        cnaes_map,
        out_path,
        rais_delimiter=";",
        out_delimiter=";",
        report_every=args.report_every,
    )


if __name__ == "__main__":
    main()

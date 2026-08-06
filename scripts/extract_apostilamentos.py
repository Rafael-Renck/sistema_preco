#!/usr/bin/env python3
"""Extrai apostilamentos PDF → XLSX (aba Itens compatível com import DTP)."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import apostilamento_extract as extract


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extrai itens/condições de PDFs de apostilamento para Excel."
    )
    parser.add_argument(
        "pasta",
        nargs="?",
        default=None,
        help="Pasta com PDFs (ex.: JULHO → entrada_apostilamentos/JULHO).",
    )
    parser.add_argument(
        "--saida",
        default=None,
        help="Pasta de saída. Padrão: APOSTILAMENTOS/<pasta>.",
    )
    args = parser.parse_args()

    gerados = extract.executar_batch(pasta_entrada=args.pasta, pasta_saida=args.saida)
    if not gerados:
        print("Nenhuma planilha gerada.", file=sys.stderr)
        return 1

    print(f"Planilhas geradas: {len(gerados)}")
    for path in gerados:
        print(f"  {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

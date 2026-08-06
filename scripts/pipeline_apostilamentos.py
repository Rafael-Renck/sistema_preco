#!/usr/bin/env python3
"""Pipeline: extrai PDFs de apostilamento e opcionalmente importa no banco (DTP)."""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import apostilamento_extract as extract


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Extrai apostilamentos (PDF→XLSX) e, com --importar, "
            "carrega na tabela DTP via import_apostilamentos_dtp.py."
        )
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
        help="Pasta de saída XLSX. Padrão: APOSTILAMENTOS/<pasta>.",
    )
    parser.add_argument(
        "--importar",
        action="store_true",
        help="Após extrair, importa os XLSX no banco (DATABASE_URL).",
    )
    parser.add_argument(
        "--tabela-id",
        type=int,
        default=41,
        help="ID da tabela DTP destino (default: 41).",
    )
    parser.add_argument(
        "--operadora-id",
        type=int,
        default=1,
        help="Operadora (default: 1 = MPF).",
    )
    parser.add_argument(
        "--substituir-prestador",
        action="store_true",
        help="Remove itens existentes do prestador antes de inserir.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Na importação, só simula (não grava).",
    )
    args = parser.parse_args()

    gerados = extract.executar_batch(pasta_entrada=args.pasta, pasta_saida=args.saida)
    if not gerados:
        print("Nenhuma planilha gerada — abortando.", file=sys.stderr)
        return 1

    print(f"Extração OK: {len(gerados)} planilha(s)")
    for path in gerados:
        print(f"  {path}")

    if not args.importar:
        print("\nPara importar no banco, rode de novo com --importar")
        print("  ou: python scripts/import_apostilamentos_dtp.py --dir APOSTILAMENTOS")
        return 0

    # Importa só a pasta de saída desta execução (não o lote inteiro histórico)
    saida_dir = gerados[0].parent
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "import_apostilamentos_dtp.py"),
        "--dir",
        str(saida_dir),
        "--tabela-id",
        str(args.tabela_id),
        "--operadora-id",
        str(args.operadora_id),
    ]
    if args.substituir_prestador:
        cmd.append("--substituir-prestador")
    if args.dry_run:
        cmd.append("--dry-run")

    print("\nImportando…")
    print(" ", " ".join(cmd))
    result = subprocess.run(cmd, cwd=str(ROOT))
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())

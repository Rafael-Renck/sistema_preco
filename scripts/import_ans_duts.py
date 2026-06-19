#!/usr/bin/env python3
"""Baixa o Anexo II (DUTs) da ANS e importa para o banco."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import rol_import


def main() -> int:
    parser = argparse.ArgumentParser(description='Importa DUTs do Anexo II oficial da ANS.')
    parser.add_argument(
        '--pdf',
        type=Path,
        help='Usar PDF local em vez de baixar da ANS.',
    )
    parser.add_argument(
        '--url',
        help='URL alternativa do PDF (padrão: Anexo II consolidado no gov.br).',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Apenas extrai e lista DUTs, sem gravar no banco.',
    )
    args = parser.parse_args()

    if args.pdf:
        data = args.pdf.read_bytes()
        rows, erros = rol_import.parse_anexo_ii_pdf(data)
        source = str(args.pdf)
    else:
        print('Baixando Anexo II da ANS…')
        rows, erros = rol_import.fetch_and_parse_ans_anexo_ii(url=args.url)
        source = args.url or rol_import.ANS_ANEXO_II_PDF_URL

    if erros:
        for msg in erros:
            print(f'AVISO: {msg}', file=sys.stderr)

    if not rows:
        print('Nenhuma DUT extraída.', file=sys.stderr)
        return 1

    print(f'Fonte: {source}')
    print(f'DUTs extraídas: {len(rows)}')
    print(f'  Primeira: {rows[0].numero} — {rows[0].titulo[:70]}')
    print(f'  Última:   {rows[-1].numero} — {rows[-1].titulo[:70]}')

    if args.dry_run:
        return 0

    from app import RolDut, app, db

    with app.app_context():
        stats = rol_import.import_duts_to_db(db.session, {'RolDut': RolDut}, rows)
        db.session.commit()
        print(
            f'Importação concluída: {stats.processados} processada(s); '
            f'{stats.criados} nova(s); {stats.atualizados} atualizada(s).'
        )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

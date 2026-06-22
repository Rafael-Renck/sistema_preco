#!/usr/bin/env python3
"""Gera CBHPM precificada (formato importação) + tabelas de Porte por ano."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import cbhpm_comunicado_import
import cbhpm_pricing
import cbhpm_rn_import
import cbhpm_years


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Monta XLSX CBHPM precificada (2021–2026) a partir dos comunicados oficiais.',
    )
    parser.add_argument(
        '--pdf',
        type=Path,
        default=ROOT / 'CBHPM_2022_Portal_do_Faturamento_Hospitalar.pdf',
    )
    parser.add_argument('--rn-dir', type=Path, default=ROOT / 'docs' / 'CBHPM' / 'rn')
    parser.add_argument('--catalog-dir', type=Path, default=ROOT / 'tmp' / 'cbhpm')
    parser.add_argument('--out-dir', type=Path, default=ROOT / 'tmp' / 'cbhpm' / 'precificada')
    parser.add_argument('--years', default='2021-2026')
    parser.add_argument(
        '--faixa',
        choices=['original', 'faixa_i', 'faixa_ii', 'faixa_iii'],
        default='original',
        help='Faixa de porte do comunicado. A partir de 2023, "original" aplica reajuste INPC sobre o ano anterior.',
    )
    parser.add_argument(
        '--filme-unit',
        type=str,
        default='',
        help='Sobrescreve valor m² CBR do filme (R$). Padrão: tabela CBR por ano.',
    )
    parser.add_argument('--rebuild-catalog', action='store_true', help='Regenerar catálogos fatores antes de precificar.')
    args = parser.parse_args()

    if ',' in args.years:
        years = [int(y.strip()) for y in args.years.split(',')]
    else:
        start, end = args.years.split('-', 1)
        years = list(range(int(start), int(end) + 1))

    filme_override = cbhpm_comunicado_import.parse_money_br(args.filme_unit) if args.filme_unit else None
    args.out_dir.mkdir(parents=True, exist_ok=True)

    if args.rebuild_catalog:
        if not args.pdf.is_file():
            print(f'PDF 2022 não encontrado: {args.pdf}', file=sys.stderr)
            return 1
        catalogs = cbhpm_years.build_all_year_catalogs(args.pdf, args.rn_dir, years=years)
        args.catalog_dir.mkdir(parents=True, exist_ok=True)
        for year in years:
            cbhpm_years.write_year_catalog_xlsx(catalogs[year], args.catalog_dir / f'CBHPM_{year}.xlsx')
    else:
        catalogs = {}
        for year in years:
            path = args.catalog_dir / f'CBHPM_{year}.xlsx'
            if not path.is_file():
                print(f'Catálogo não encontrado: {path}. Use --rebuild-catalog.', file=sys.stderr)
                return 1
            catalogs[year] = cbhpm_rn_import.load_catalog_from_xlsx(path)

    comunicados = cbhpm_comunicado_import.load_all_comunicados(ROOT, years, faixa=args.faixa)
    summary: dict[str, dict] = {}

    for year in years:
        catalog = catalogs[year]
        com = comunicados[year]
        filme_m2 = filme_override or cbhpm_comunicado_import.filme_m2_for_year(year)
        priced_rows = cbhpm_pricing.build_priced_catalog(
            catalog,
            com,
            filme_unit=filme_m2,
            ano=year,
        )
        porte_rows = cbhpm_comunicado_import.portes_to_rows(com)
        porte_an_rows = [
            {'porte_an': r['porte_an'], 'valor': r['valor']}
            for r in cbhpm_comunicado_import.porte_anestesico_rows(com)
        ]

        priced_path = args.out_dir / f'CBHPM_{year}_import.xlsx'
        porte_path = args.out_dir / f'Portes_{year}_{args.faixa}.xlsx'
        porte_an_path = args.out_dir / f'PorteAnestesico_{year}_{args.faixa}.xlsx'

        cbhpm_rn_import._write_dict_rows_xlsx(
            priced_rows,
            priced_path,
            fieldnames=cbhpm_pricing.CBHPM_IMPORT_FIELDS,
            sheet_title=f'CBHPM {year}',
        )
        cbhpm_rn_import._write_dict_rows_xlsx(
            porte_rows,
            porte_path,
            fieldnames=cbhpm_pricing.PORTE_IMPORT_FIELDS,
            sheet_title=f'Portes {year}',
        )
        cbhpm_rn_import._write_dict_rows_xlsx(
            porte_an_rows,
            porte_an_path,
            fieldnames=cbhpm_pricing.PORTE_ANESTESICO_IMPORT_FIELDS,
            sheet_title=f'Porte An {year}',
        )

        com_porte = sum(1 for r in priced_rows if r.get('valor_porte'))
        com_uco = sum(1 for r in priced_rows if r.get('total_uco'))
        com_filme = sum(1 for r in priced_rows if r.get('total_filme'))
        com_an = sum(1 for r in priced_rows if r.get('valor_porte_anestesico'))
        com_sub = sum(1 for r in priced_rows if r.get('subtotal'))
        summary[str(year)] = {
            'periodo_comunicado': com.periodo,
            'faixa': com.faixa,
            'uco_valor': cbhpm_comunicado_import.format_money_br(com.uco_valor),
            'filme_m2_cbr': cbhpm_comunicado_import.format_money_br(filme_m2),
            'inpc_pct': cbhpm_comunicado_import.format_money_br(com.inpc_pct) if com.inpc_pct else '',
            'portes_carregados': len(com.portes),
            'fonte': com.fonte,
            'observacao': com.observacao,
            'procedimentos': len(priced_rows),
            'com_valor_porte': com_porte,
            'com_total_uco': com_uco,
            'com_total_filme': com_filme,
            'com_porte_anestesico': com_an,
            'com_subtotal': com_sub,
            'arquivo_import': priced_path.name,
            'arquivo_portes': porte_path.name,
            'arquivo_porte_anestesico': porte_an_path.name,
        }
        print(
            f'CBHPM {year}: {len(priced_rows)} proc | UCO R$ {summary[str(year)]["uco_valor"]} | '
            f'filme m² R$ {summary[str(year)]["filme_m2_cbr"]} | '
            f'porte {com_porte} | filme {com_filme} | anest {com_an} -> {priced_path.name}'
        )
        if com.observacao:
            print(f'  obs: {com.observacao}')

    meta_path = args.out_dir / 'cbhpm_precificada_resumo.json'
    meta_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding='utf-8')
    print(f'Resumo: {meta_path}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

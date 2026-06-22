"""Extração de valores de Porte e UCO dos Comunicados Oficiais CBHPM."""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Literal

try:
    import fitz
except ImportError:  # pragma: no cover
    fitz = None


FaixaPorte = Literal['original', 'faixa_i', 'faixa_ii', 'faixa_iii']

PORTE_CODE_RE = re.compile(r'^(\d{1,2}[ABC])$')
ALL_PORTES = tuple(
    f'{n}{s}'
    for n in range(1, 15)
    for s in ('A', 'B', 'C')
)

# Comunicado 2021-2022 (PNG) — faixa única, UCO R$ 24,24
PORTES_2021_2022: dict[str, str] = {
    '1A': '27,00', '1B': '74,58', '1C': '106,83',
    '2A': '158,30', '2B': '249,14', '2C': '339,66',
    '3A': '486,63', '3B': '633,61', '3C': '780,58',
    '4A': '927,55', '4B': '1.074,52', '4C': '1.221,49',
    '5A': '1.368,45', '5B': '1.515,43', '5C': '1.662,40',
    '6A': '1.809,37', '6B': '1.956,34', '6C': '2.103,31',
    '7A': '2.250,29', '7B': '2.397,26', '7C': '2.544,23',
    '8A': '2.691,20', '8B': '2.838,16', '8C': '2.985,13',
    '9A': '3.132,11', '9B': '3.279,08', '9C': '3.426,05',
    '10A': '3.573,02', '10B': '3.719,99', '10C': '3.866,96',
    '11A': '4.013,94', '11B': '4.160,90', '11C': '4.307,87',
    '12A': '4.454,84', '12B': '4.601,81', '12C': '4.748,78',
    '13A': '4.895,76', '13B': '5.042,73', '13C': '5.189,70',
    '14A': '5.336,67', '14B': '5.483,64', '14C': '5.630,60',
}

COMUNICADO_BY_YEAR: dict[int, dict[str, str]] = {
    2021: {'pdf': 'Comunicado-CBHPM-2020-2021.pdf', 'periodo': '2020-2021'},
    2022: {'png': 'Comunicado-CBHPM-2021-2022.png', 'periodo': '2021-2022'},
    2023: {'pdf': 'COMUNICADO-CBHPM-2022_2023.pdf', 'periodo': '2022-2023'},
    2024: {'pdf': 'COMUNICADO-CBHPM-2023_2024.pdf', 'periodo': '2023-2024'},
    2025: {'pdf': 'COMUNICADO-CBHPM-2024_2025.pdf', 'periodo': '2024-2025'},
    2026: {'pdf': 'COMUNICADO-CBHPM-2025_2026_.pdf', 'periodo': '2025-2026'},
}

# A partir de 2023 o comunicado AMB passou a publicar multifaixa; a coluna "Original"
# é a curva de 2003 (muito abaixo da vigência 2021-2022). Para continuidade com os
# comunicados anteriores, reajustamos os portes do ano anterior pelo INPC do PDF.
INPC_CHAIN_FROM_YEAR = 2023

DEFAULT_INPC_BY_YEAR: dict[int, Decimal] = {
    2025: Decimal('4.42'),
}

# Valor m² documentação radiológica (CBR), vigente a partir de 1º de abril.
FILME_M2_CBR: dict[int, Decimal] = {
    2021: Decimal('31.59'),
    2022: Decimal('34.92'),
    2023: Decimal('36.88'),
    2024: Decimal('38.54'),
    2025: Decimal('40.49'),
    2026: Decimal('42.17'),
}

# De-para porte anestésico → porte CBHPM (CBHPM 2022, Instruções Gerais — Anestesiologia).
PORTE_ANESTESICO_MAP: dict[str, str] = {
    '1': '3A',
    '2': '3C',
    '3': '4C',
    '4': '6B',
    '5': '7C',
    '6': '9B',
    '7': '10C',
    '8': '12A',
}


@dataclass
class CbhpmComunicadoValores:
    ano: int
    periodo: str
    faixa: FaixaPorte
    uco_valor: Decimal
    portes: dict[str, Decimal] = field(default_factory=dict)
    inpc_pct: Decimal | None = None
    fonte: str = ''
    observacao: str = ''


def _normalize_space(text: str) -> str:
    text = unicodedata.normalize('NFKC', text or '')
    return re.sub(r'\s+', ' ', text).strip()


def parse_money_br(value: str | None) -> Decimal | None:
    if value is None:
        return None
    text = _normalize_space(str(value)).replace('R$', '').replace(' ', '')
    if not text or text in {'–', '-', '—', '*'}:
        return None
    text = text.replace('.', '').replace(',', '.')
    try:
        return Decimal(text)
    except Exception:
        return None


def format_money_br(value: Decimal | None) -> str:
    if value is None:
        return ''
    s = f'{value:.2f}'.replace('.', ',')
    if ',' in s:
        whole, frac = s.split(',', 1)
        if len(whole) > 3:
            whole = re.sub(r'(?<=\d)(?=(\d{3})+(?!\d))', '.', whole)
        return f'{whole},{frac}'
    return s


def _parse_uco_from_text(text: str) -> Decimal | None:
    for pattern in (
        r'1\s+UCO\s*=\s*R\$\s*([\d\.,]+)',
        r'UCO\s*=\s*R\$\s*([\d\.,]+)',
        r'estabelecida\s+1\s+UCO\s*=\s*R\$\s*([\d\.,]+)',
    ):
        m = re.search(pattern, text, re.I)
        if m:
            return parse_money_br(m.group(1))
    return None


def _parse_inpc_from_text(text: str) -> Decimal | None:
    m = re.search(r'INPC/IBGE.*?([\d,\.]+)\s*%', text, re.I | re.S)
    if not m:
        m = re.search(r'índice de\s+([\d,\.]+)\s*%', text, re.I)
    if not m:
        m = re.search(r'INPC\s*\n?\s*([\d,\.]+)\s*%', text, re.I)
    return parse_money_br(m.group(1)) if m else None


def _parse_portes_single_column(text: str) -> dict[str, Decimal]:
    portes: dict[str, Decimal] = {}
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    i = 0
    while i < len(lines):
        m = PORTE_CODE_RE.match(lines[i])
        if not m:
            i += 1
            continue
        code = m.group(1)
        for j in range(i + 1, min(i + 5, len(lines))):
            if PORTE_CODE_RE.match(lines[j]):
                break
            chunk = lines[j].replace('R$', '').strip()
            if re.match(r'^[\d\.,]+$', chunk):
                val = parse_money_br(chunk)
                if val is not None:
                    portes[code] = val
                break
        i += 1
    return portes


def _parse_portes_multifaixa(text: str, faixa: FaixaPorte) -> dict[str, Decimal]:
    faixa_idx = {'original': 0, 'faixa_i': 1, 'faixa_ii': 2, 'faixa_iii': 3}[faixa]
    idx = re.search(r'ORIGINAL', text, re.I)
    if not idx:
        return _parse_portes_single_column(text)
    chunk = text[idx.start():]
    lines = [line.strip() for line in chunk.splitlines() if line.strip()]
    portes: dict[str, Decimal] = {}
    i = 0
    while i < len(lines):
        m = PORTE_CODE_RE.match(lines[i])
        if not m:
            i += 1
            continue
        code = m.group(1)
        values: list[Decimal] = []
        for j in range(i + 1, min(i + 12, len(lines))):
            if PORTE_CODE_RE.match(lines[j]):
                break
            line = lines[j]
            if line.upper() in {'UCO', 'INPC'}:
                break
            if line.startswith('R$') or re.match(r'^[\d\.,]+$', line.replace('R$', '').strip()):
                val = parse_money_br(re.sub(r'^R\$', '', line))
                if val is not None:
                    values.append(val)
        if len(values) > faixa_idx:
            portes[code] = values[faixa_idx]
        i += 1
    return portes


def _derive_portes_from_previous(
    base: CbhpmComunicadoValores,
    *,
    inpc_pct: Decimal,
    uco_valor: Decimal | None = None,
) -> dict[str, Decimal]:
    factor = Decimal('1') + (inpc_pct / Decimal('100'))
    portes = {code: (val * factor).quantize(Decimal('0.01')) for code, val in base.portes.items()}
    return portes


def parse_comunicado_file(
    path: Path,
    *,
    ano: int,
    periodo: str,
    faixa: FaixaPorte = 'original',
) -> CbhpmComunicadoValores:
    path = Path(path)
    suffix = path.suffix.lower()
    if suffix == '.png':
        portes = {code: parse_money_br(val) for code, val in PORTES_2021_2022.items()}
        portes = {k: v for k, v in portes.items() if v is not None}
        return CbhpmComunicadoValores(
            ano=ano,
            periodo=periodo,
            faixa=faixa,
            uco_valor=parse_money_br('24,24') or Decimal('0'),
            portes=portes,
            inpc_pct=parse_money_br('10,78'),
            fonte=str(path),
            observacao='Valores transcritos do Comunicado 2021-2022 (PNG).',
        )

    if fitz is None:
        raise ValueError('PyMuPDF não instalado.')
    text = '\n'.join(page.get_text() or '' for page in fitz.open(str(path)))
    uco = _parse_uco_from_text(text)
    inpc = _parse_inpc_from_text(text)
    if '2020' in path.name or '2020-2021' in periodo:
        portes = _parse_portes_single_column(text)
    else:
        portes = _parse_portes_multifaixa(text, faixa)
    return CbhpmComunicadoValores(
        ano=ano,
        periodo=periodo,
        faixa=faixa,
        uco_valor=uco or Decimal('0'),
        portes=portes,
        inpc_pct=inpc,
        fonte=str(path),
    )


def _default_inpc_for_year(year: int) -> Decimal:
    return DEFAULT_INPC_BY_YEAR.get(year, Decimal('4.51'))


def _apply_inpc_porte_chain(
    result: CbhpmComunicadoValores,
    prev: CbhpmComunicadoValores,
    *,
    inpc_pct: Decimal,
    motivo: str,
) -> None:
    if prev.portes:
        result.portes = _derive_portes_from_previous(prev, inpc_pct=inpc_pct)
    if prev.uco_valor and result.uco_valor <= 0:
        result.uco_valor = (
            prev.uco_valor * (Decimal('1') + inpc_pct / Decimal('100'))
        ).quantize(Decimal('0.01'))
    result.inpc_pct = inpc_pct
    result.observacao = motivo


def load_comunicado_for_year(
    year: int,
    root: Path,
    *,
    faixa: FaixaPorte = 'original',
) -> CbhpmComunicadoValores:
    meta = COMUNICADO_BY_YEAR.get(year)
    if not meta:
        raise ValueError(f'Ano sem comunicado mapeado: {year}')

    search_dirs = [
        root,
        root / 'docs' / 'CBHPM' / 'comunicados',
    ]
    filename = meta.get('pdf') or meta.get('png')
    path = None
    for base in search_dirs:
        candidate = base / filename
        if candidate.is_file():
            path = candidate
            break
    if path is None:
        raise FileNotFoundError(f'Comunicado não encontrado para {year}: {filename}')

    result = parse_comunicado_file(
        path,
        ano=year,
        periodo=meta['periodo'],
        faixa=faixa,
    )

    prev = None
    if year > 2021:
        prev = load_comunicado_for_year(year - 1, root, faixa=faixa)

    use_inpc_chain = year >= INPC_CHAIN_FROM_YEAR and faixa == 'original' and prev is not None
    if use_inpc_chain:
        inpc = result.inpc_pct or _default_inpc_for_year(year)
        _apply_inpc_porte_chain(
            result,
            prev,
            inpc_pct=inpc,
            motivo=(
                f'Portes reajustados por INPC {format_money_br(inpc)}% sobre {year - 1} '
                f'({prev.periodo}); continuidade com comunicado {year - 2}-{year - 1}.'
            ),
        )
        return result

    if len(result.portes) < 40 and prev is not None:
        inpc = result.inpc_pct or _default_inpc_for_year(year)
        _apply_inpc_porte_chain(
            result,
            prev,
            inpc_pct=inpc,
            motivo=(
                f'Valores derivados do comunicado {year - 1} com INPC {format_money_br(inpc)}% '
                f'(PDF {path.name} sem tabela extraível).'
            ),
        )
    return result


def load_all_comunicados(
    root: Path,
    years: list[int] | None = None,
    *,
    faixa: FaixaPorte = 'original',
) -> dict[int, CbhpmComunicadoValores]:
    years = years or list(COMUNICADO_BY_YEAR)
    out: dict[int, CbhpmComunicadoValores] = {}
    for year in years:
        out[year] = load_comunicado_for_year(year, root, faixa=faixa)
    return out


def portes_to_rows(comunicado: CbhpmComunicadoValores) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for code in ALL_PORTES:
        val = comunicado.portes.get(code)
        rows.append({
            'porte': code,
            'valor': format_money_br(val),
        })
    return rows


def filme_m2_for_year(year: int) -> Decimal | None:
    return FILME_M2_CBR.get(year)


def porte_anestesico_rows(comunicado: CbhpmComunicadoValores) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for an_code, porte_code in PORTE_ANESTESICO_MAP.items():
        val = comunicado.portes.get(porte_code)
        rows.append({
            'porte_an': an_code,
            'porte_cbhpm': porte_code,
            'valor': format_money_br(val),
        })
    return rows

#!/usr/bin/env python3
"""
Script de importação em lote para SIMPRO e Brasíndice.

MODO INTERATIVO (recomendado):
    python scripts/batch_import.py

MODO LINHA DE COMANDO:
    # Importar todos os arquivos SIMPRO da pasta
    python scripts/batch_import.py --tipo SIMPRO --diretorio testes/

    # Dry-run (apenas mostra o que seria feito)
    python scripts/batch_import.py --tipo SIMPRO --diretorio testes/ --dry-run

    # Importar Brasíndice
    python scripts/batch_import.py --tipo BRAS --diretorio testes/
"""

import argparse
import json
import re
import sys
import os
from pathlib import Path
from decimal import Decimal, InvalidOperation
from datetime import datetime

# Adiciona o diretório raiz ao path para importar o app
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

# Mapeamento de alíquotas para UFs
ALIQUOTA_UF_MAP = {
    "17": ["DF", "ES", "MT", "MS", "RS", "SC"],
    "18": ["AP", "MG", "SP"],
    "19": ["AC", "AL", "GO", "PA", "SE"],
    "19.5": ["PR", "RO"],
    "20": ["AL", "AM", "CE", "PB", "RN", "RR", "TO"],
    "20.5": ["BA", "PE"],
    "22": ["RJ"],
    "22.5": ["PI"],
    "23": ["MA"]
}


def carregar_config_aliquotas(config_path: Path) -> dict:
    """Carrega o mapeamento de alíquotas para UFs do arquivo JSON."""
    if not config_path.exists():
        return ALIQUOTA_UF_MAP
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        aliquotas = config.get('aliquotas', {})
        result = {}
        for aliq, data in aliquotas.items():
            ufs = data.get('ufs', [])
            if 'TODO' not in ufs:
                result[aliq] = ufs
        return result if result else ALIQUOTA_UF_MAP
    except:
        return ALIQUOTA_UF_MAP


def extrair_info_do_nome(nome_arquivo: str) -> dict:
    """
    Extrai informações do nome do arquivo SIMPRO.
    
    Padrões suportados:
    - MSG 51 2025 18.TXT → versão: 2025-51, alíquota: 18
    - MSG 51 2025 17.5.TXT → versão: 2025-51, alíquota: 17.5
    """
    info = {
        'versao': None,
        'aliquota': None,
        'edicao': None,
        'ano': None,
        'detectado': False
    }
    
    # Padrão: MSG {num} {ano} {aliquota}.TXT
    match = re.search(r'MSG\s*(\d+)\s*(\d{4})\s*(\d+(?:\.\d+)?)\s*\.TXT', nome_arquivo, re.IGNORECASE)
    if match:
        info['edicao'] = match.group(1)
        info['ano'] = match.group(2)
        info['aliquota'] = match.group(3)
        info['versao'] = f"{info['ano']}-{info['edicao'].zfill(2)}"
        info['detectado'] = True
        return info
    
    # Padrão alternativo: apenas número no final
    match = re.search(r'(\d+(?:\.\d+)?)\s*\.TXT$', nome_arquivo, re.IGNORECASE)
    if match:
        info['aliquota'] = match.group(1)
        info['versao'] = datetime.now().strftime('%Y-%m')
    
    return info


def extrair_edicao_brasindice(nome_arquivo: str) -> str | None:
    """Extrai a edição do nome do arquivo Brasíndice."""
    match = re.search(r'Edi[çc][aã]o\s+(\d+)', nome_arquivo, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def listar_arquivos_simpro(diretorio: Path) -> list[Path]:
    """Lista arquivos SIMPRO no diretório."""
    arquivos = []
    patterns = ['*.TXT', '*.txt']
    
    for pattern in patterns:
        for arquivo in diretorio.glob(pattern):
            nome = arquivo.name.upper()
            if nome.startswith('MSG') or re.match(r'^\d+\.TXT$', nome):
                if arquivo not in arquivos:
                    arquivos.append(arquivo)
    
    return sorted(arquivos, key=lambda x: x.name)


def listar_arquivos_brasindice(diretorio: Path) -> list[Path]:
    """Lista arquivos Brasíndice no diretório."""
    arquivos = []
    
    for arquivo in diretorio.glob('*.txt'):
        nome = arquivo.name.lower()
        if 'brasindice' in nome or 'bras' in nome:
            arquivos.append(arquivo)
        elif 'medicamento' in nome or 'materiais' in nome or 'parenterais' in nome:
            arquivos.append(arquivo)
    
    return sorted(arquivos, key=lambda x: x.name)


def print_header(text: str):
    """Imprime cabeçalho formatado."""
    print(f"\n{'='*60}")
    print(f"  {text}")
    print(f"{'='*60}")


def print_table(headers: list, rows: list):
    """Imprime tabela formatada."""
    # Calcula larguras
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))
    
    # Cabeçalho
    header_line = " | ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    separator = "-+-".join("-" * w for w in widths)
    
    print(f"\n{header_line}")
    print(separator)
    
    # Linhas
    for row in rows:
        line = " | ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row))
        print(line)


def modo_interativo():
    """Modo interativo para seleção de arquivos."""
    print_header("IMPORTAÇÃO EM LOTE - MODO INTERATIVO")
    
    # Carrega configuração
    config_path = ROOT_DIR / 'testes' / 'config' / 'aliquota_uf_map.json'
    aliquota_map = carregar_config_aliquotas(config_path)
    
    # Lista diretórios disponíveis
    testes_dir = ROOT_DIR / 'testes'
    
    print("\n📂 Diretório padrão: testes/")
    diretorio = input("   Outro diretório? (Enter para usar padrão): ").strip()
    
    if diretorio:
        diretorio = Path(diretorio)
        if not diretorio.is_absolute():
            diretorio = ROOT_DIR / diretorio
    else:
        diretorio = testes_dir
    
    if not diretorio.exists():
        print(f"❌ Diretório não encontrado: {diretorio}")
        return
    
    # Detecta arquivos
    arquivos_simpro = listar_arquivos_simpro(diretorio)
    arquivos_bras = listar_arquivos_brasindice(diretorio)
    
    print(f"\n📦 Encontrados: {len(arquivos_simpro)} SIMPRO, {len(arquivos_bras)} Brasíndice")
    
    if not arquivos_simpro and not arquivos_bras:
        print("❌ Nenhum arquivo encontrado para importar.")
        return
    
    # Menu de seleção
    print("\n🔧 O que deseja importar?")
    print("   1. Todos os arquivos SIMPRO")
    print("   2. Todos os arquivos Brasíndice")
    print("   3. Selecionar arquivos individualmente")
    print("   4. Ver preview de todos os arquivos")
    print("   0. Sair")
    
    escolha = input("\nEscolha: ").strip()
    
    if escolha == '0':
        return
    
    arquivos_para_importar = []
    
    if escolha == '1':
        for arq in arquivos_simpro:
            info = extrair_info_do_nome(arq.name)
            ufs = aliquota_map.get(info['aliquota'], [])
            arquivos_para_importar.append({
                'arquivo': arq,
                'tipo': 'SIMPRO',
                'info': info,
                'ufs': ufs
            })
    
    elif escolha == '2':
        for arq in arquivos_bras:
            edicao = extrair_edicao_brasindice(arq.name)
            arquivos_para_importar.append({
                'arquivo': arq,
                'tipo': 'BRAS',
                'info': {'versao': f"ED-{edicao}" if edicao else datetime.now().strftime('%Y-%m')},
                'ufs': []
            })
    
    elif escolha in ('3', '4'):
        todos_arquivos = []
        for arq in arquivos_simpro:
            info = extrair_info_do_nome(arq.name)
            ufs = aliquota_map.get(info['aliquota'], [])
            todos_arquivos.append({
                'arquivo': arq,
                'tipo': 'SIMPRO',
                'info': info,
                'ufs': ufs
            })
        for arq in arquivos_bras:
            edicao = extrair_edicao_brasindice(arq.name)
            todos_arquivos.append({
                'arquivo': arq,
                'tipo': 'BRAS',
                'info': {'versao': f"ED-{edicao}" if edicao else datetime.now().strftime('%Y-%m')},
                'ufs': []
            })
        
        # Mostra preview
        print_header("PREVIEW DOS ARQUIVOS")
        
        headers = ["#", "Tipo", "Arquivo", "Versão", "Alíquota", "UFs", "Status"]
        rows = []
        
        for i, item in enumerate(todos_arquivos, 1):
            info = item['info']
            status = "✓ Auto" if info.get('detectado') else "? Verificar"
            rows.append([
                str(i),
                item['tipo'],
                item['arquivo'].name[:30],
                info.get('versao', '-'),
                info.get('aliquota', '-'),
                ', '.join(item['ufs'][:3]) + ('...' if len(item['ufs']) > 3 else '') if item['ufs'] else '-',
                status
            ])
        
        print_table(headers, rows)
        
        if escolha == '4':
            input("\nPressione Enter para continuar...")
            return modo_interativo()
        
        # Seleção
        print("\n📋 Selecione os arquivos (ex: 1,2,5 ou 1-5 ou 'todos'):")
        selecao = input("   Seleção: ").strip().lower()
        
        if selecao == 'todos':
            arquivos_para_importar = todos_arquivos
        else:
            indices = set()
            for parte in selecao.split(','):
                if '-' in parte:
                    inicio, fim = parte.split('-')
                    indices.update(range(int(inicio), int(fim) + 1))
                else:
                    indices.add(int(parte))
            
            for idx in sorted(indices):
                if 1 <= idx <= len(todos_arquivos):
                    arquivos_para_importar.append(todos_arquivos[idx - 1])
    
    else:
        print("❌ Opção inválida.")
        return
    
    if not arquivos_para_importar:
        print("❌ Nenhum arquivo selecionado.")
        return
    
    # Confirmação
    print(f"\n✅ {len(arquivos_para_importar)} arquivo(s) selecionado(s) para importação.")
    confirma = input("   Confirma a importação? (s/N): ").strip().lower()
    
    if confirma != 's':
        print("❌ Importação cancelada.")
        return
    
    # Executa importação
    executar_importacao(arquivos_para_importar)


def executar_importacao(arquivos: list):
    """Executa a importação dos arquivos."""
    print_header("EXECUTANDO IMPORTAÇÃO")
    
    try:
        from app import app, db, _import_simpro, _import_bras
    except ImportError as e:
        print(f"❌ Erro ao importar módulos: {e}")
        print("   Execute o script da pasta raiz do projeto.")
        return
    
    mapa_path = ROOT_DIR / 'testes' / 'mapa.json'
    map_config = {}
    
    if mapa_path.exists():
        try:
            content = mapa_path.read_text(encoding='utf-8')
            # Remove comentários
            content = re.sub(r'//.*$', '', content, flags=re.MULTILINE)
            content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
            map_config = json.loads(content)
        except Exception as e:
            print(f"⚠️  Erro ao carregar mapa.json: {e}")
    
    total = len(arquivos)
    sucesso = 0
    falha = 0
    
    with app.app_context():
        for i, item in enumerate(arquivos, 1):
            arquivo = item['arquivo']
            tipo = item['tipo']
            info = item['info']
            ufs = item['ufs']
            
            print(f"\n[{i}/{total}] 📄 {arquivo.name}")
            print(f"         Tipo: {tipo} | Versão: {info.get('versao', '-')} | Alíquota: {info.get('aliquota', '-')}%")
            if ufs:
                print(f"         UFs: {', '.join(ufs)}")
            
            try:
                if tipo == 'SIMPRO':
                    if not map_config:
                        print("         ❌ Mapa de configuração não encontrado")
                        falha += 1
                        continue
                    
                    aliquota_value = None
                    if info.get('aliquota'):
                        try:
                            aliquota_value = Decimal(info['aliquota'])
                        except:
                            pass
                    
                    result = _import_simpro(
                        file_path=arquivo,
                        versao=info.get('versao', ''),
                        fmt='fixed',
                        map_config=map_config,
                        encoding=map_config.get('encoding', 'latin-1'),
                        truncate=False,
                        uf_default=ufs[0] if ufs else None,
                        uf_values=ufs,
                        aliquota_default=aliquota_value,
                    )
                    
                    print(f"         ✅ Importado: {result['linhas_materializadas']} itens")
                    sucesso += 1
                
                elif tipo == 'BRAS':
                    result = _import_bras(
                        file_path=arquivo,
                        versao=info.get('versao', ''),
                        data_ref=None,
                        fmt='delimited',
                        delimiter=',',
                        quotechar='"',
                        line_terminator='\n',
                        skip_header=False,
                        encoding='latin-1',
                        map_config={},
                        truncate=False,
                        uf_default=ufs[0] if ufs else None,
                        uf_values=ufs if ufs else None,
                        aliquota_default=None,
                    )
                    
                    print(f"         ✅ Importado: {result['linhas_materializadas']} itens")
                    sucesso += 1
                
            except Exception as e:
                print(f"         ❌ Erro: {e}")
                falha += 1
    
    # Resumo
    print_header("RESUMO DA IMPORTAÇÃO")
    print(f"   Total: {total}")
    print(f"   ✅ Sucesso: {sucesso}")
    print(f"   ❌ Falhas: {falha}")


def main():
    parser = argparse.ArgumentParser(
        description='Importação em lote de arquivos SIMPRO e Brasíndice',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--tipo', '-t',
        choices=['SIMPRO', 'BRAS', 'AUTO'],
        help='Tipo de arquivo a importar'
    )
    
    parser.add_argument(
        '--diretorio', '-d',
        type=Path,
        help='Diretório contendo os arquivos a importar'
    )
    
    parser.add_argument(
        '--arquivo', '-a',
        type=Path,
        help='Arquivo específico a importar'
    )
    
    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help='Apenas mostra o que seria feito, sem importar'
    )
    
    parser.add_argument(
        '--interativo', '-i',
        action='store_true',
        help='Modo interativo (padrão se nenhum argumento for passado)'
    )
    
    args = parser.parse_args()
    
    # Se nenhum argumento relevante, usa modo interativo
    if not args.tipo and not args.diretorio and not args.arquivo:
        modo_interativo()
        return
    
    # Modo linha de comando
    if not args.diretorio and not args.arquivo:
        parser.error('Informe --diretorio ou --arquivo')
    
    config_path = ROOT_DIR / 'testes' / 'config' / 'aliquota_uf_map.json'
    aliquota_map = carregar_config_aliquotas(config_path)
    
    arquivos_para_importar = []
    
    if args.arquivo:
        arquivo = args.arquivo.resolve()
        if not arquivo.exists():
            print(f"❌ Arquivo não encontrado: {arquivo}")
            sys.exit(1)
        
        info = extrair_info_do_nome(arquivo.name)
        tipo = args.tipo or ('SIMPRO' if arquivo.name.upper().startswith('MSG') else 'BRAS')
        ufs = aliquota_map.get(info.get('aliquota', ''), [])
        
        arquivos_para_importar.append({
            'arquivo': arquivo,
            'tipo': tipo,
            'info': info,
            'ufs': ufs
        })
    
    elif args.diretorio:
        diretorio = args.diretorio.resolve()
        if not diretorio.exists():
            print(f"❌ Diretório não encontrado: {diretorio}")
            sys.exit(1)
        
        if args.tipo in ('SIMPRO', 'AUTO', None):
            for arq in listar_arquivos_simpro(diretorio):
                info = extrair_info_do_nome(arq.name)
                ufs = aliquota_map.get(info.get('aliquota', ''), [])
                arquivos_para_importar.append({
                    'arquivo': arq,
                    'tipo': 'SIMPRO',
                    'info': info,
                    'ufs': ufs
                })
        
        if args.tipo in ('BRAS', 'AUTO', None):
            for arq in listar_arquivos_brasindice(diretorio):
                edicao = extrair_edicao_brasindice(arq.name)
                arquivos_para_importar.append({
                    'arquivo': arq,
                    'tipo': 'BRAS',
                    'info': {'versao': f"ED-{edicao}" if edicao else datetime.now().strftime('%Y-%m')},
                    'ufs': []
                })
    
    if not arquivos_para_importar:
        print("❌ Nenhum arquivo encontrado para importar.")
        sys.exit(1)
    
    # Preview
    print_header(f"ARQUIVOS ENCONTRADOS ({len(arquivos_para_importar)})")
    
    for item in arquivos_para_importar:
        info = item['info']
        status = "✓" if info.get('detectado') else "?"
        print(f"  {status} {item['tipo']:6} | {item['arquivo'].name}")
        print(f"           Versão: {info.get('versao', '-')} | Alíquota: {info.get('aliquota', '-')}% | UFs: {', '.join(item['ufs']) or '-'}")
    
    if args.dry_run:
        print("\n🔍 Modo dry-run: nenhum arquivo foi importado.")
        sys.exit(0)
    
    executar_importacao(arquivos_para_importar)


if __name__ == '__main__':
    main()

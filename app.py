import os
import time
import csv
import math
import re
from pathlib import Path

import click
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    jsonify,
    session,
    send_file,
    g,
    abort,
    flash,
)
import json
from flask_sqlalchemy import SQLAlchemy
import pymysql
from dotenv import load_dotenv
from functools import wraps
from sqlalchemy import text, or_, func
from sqlalchemy.dialects.mysql import insert as mysql_insert
from werkzeug.utils import secure_filename
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import unicodedata
import html
import hashlib
from datetime import date, datetime
from uuid import uuid4
from flask import make_response
import io
import tempfile
import xlsxwriter
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
# --- 1. CONFIGURAÇÃO INICIAL ---
# Inicializa a aplicação Flask
load_dotenv()

app = Flask(__name__, template_folder='templates', static_folder='static')

# Chave de sessão (ajuste em produção via variável de ambiente)
app.secret_key = os.getenv('SECRET_KEY', 'dev-secret-key')

# Configuração da conexão com o banco de dados MySQL
# Formato: mysql+pymysql://<usuario>:<senha>@<host>/<nome_do_banco>
# Para o XAMPP padrão, o usuário é 'root' e a senha é vazia.
# DATABASE_URL pode vir do Docker Compose. Fallback para dev local.
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv(
    'DATABASE_URL', 'mysql+pymysql://root:@localhost/operadora_saude'
)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Inicializa o SQLAlchemy para interagir com o banco de dados
db = SQLAlchemy(app)


# --- 1.1 Autorização/Session helpers ---
def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get('user_id'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return wrapper


def admin_required(f):
    @wraps(f)
    @login_required
    def wrapper(*args, **kwargs):
        if session.get('perfil') != 'adm':
            return redirect(url_for('consulta_comparar'))
        return f(*args, **kwargs)
    return wrapper




def _store_history_entry(entry: dict):
    entry = dict(entry or {})
    if not entry:
        return
    entry.setdefault('type', 'generic')
    entry.setdefault('label', 'Consulta recente')
    entry.setdefault('timestamp', datetime.now().strftime('%d/%m %H:%M'))
    if 'id' not in entry:
        entry['id'] = uuid4().hex[:8]
    if 'url_fragment' not in entry:
        entry['url_fragment'] = f"sim_hist={entry['id']}"
    if 'signature' not in entry:
        entry['signature'] = f"{entry.get('type', 'generic')}:{entry['id']}"
    history = session.get('sim_history') or []
    history = [h for h in history if h.get('signature') != entry['signature']]
    history.insert(0, entry)
    session['sim_history'] = history[:5]
    session.pop('ultima_simulacao_url', None)
    session.pop('ultima_simulacao_label', None)
    session.modified = True


@app.context_processor
def inject_session():
    history_raw = session.get('sim_history') or []
    history = []
    for item in history_raw:
        entry = dict(item or {})
        if 'url_fragment' not in entry and entry.get('url'):
            entry['url_fragment'] = entry['url']
        if not entry.get('label'):
            entry['label'] = 'Consulta recente'
        if 'id' not in entry:
            entry['id'] = uuid4().hex[:8]
        if 'signature' not in entry:
            entry['signature'] = f"legacy:{entry.get('url_fragment', entry['id'])}"
        history.append(entry)
    if not history:
        legacy_url = session.get('ultima_simulacao_url')
        if legacy_url:
            history = [{
                'id': uuid4().hex[:8],
                'type': 'compare',
                'url_fragment': legacy_url,
                'label': session.get('ultima_simulacao_label') or 'Consulta recente',
                'timestamp': '',
                'signature': f"legacy:{legacy_url}",
            }]
    last = history[0] if history else {}
    return {
        "session_perfil": session.get('perfil'),
        "session_nome": session.get('nome'),
        "session_last_simulation_url": last.get('url_fragment'),
        "session_last_simulation_label": last.get('label'),
        "session_sim_history": history,
    }


# --- 2. DEFINIÇÃO DOS MODELOS (TABELAS) ---
# Cada classe representa uma tabela no banco de dados.

class Usuario(db.Model):
    __tablename__ = 'usuarios'
    id = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(255), nullable=False)
    email = db.Column(db.String(255), unique=True, nullable=False)
    senha = db.Column(db.String(255), nullable=False) # Em um projeto real, usaríamos hash!
    perfil = db.Column(db.String(50), nullable=False)

class Operadora(db.Model):
    __tablename__ = 'operadoras'
    id = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(255), nullable=False)
    uf = db.Column(db.String(2), nullable=True)
    cnpj = db.Column(db.String(20), nullable=True)
    status = db.Column(db.String(50), nullable=False)
    # Relacionamento: uma operadora pode ter várias tabelas de preços
    tabelas = db.relationship('Tabela', backref='operadora', lazy=True)

class Tabela(db.Model):
    __tablename__ = 'tabelas'
    id = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(255), nullable=False)
    data_vigencia = db.Column(db.Date, nullable=True)
    prestador = db.Column(db.String(255), nullable=True)
    tipo_tabela = db.Column(db.String(50), nullable=True)
    uf = db.Column(db.String(2), nullable=True)
    uco_valor = db.Column(db.Numeric(12, 2), nullable=True)
    # Chave estrangeira para ligar à tabela de operadoras
    id_operadora = db.Column(db.Integer, db.ForeignKey('operadoras.id'), nullable=False)
    # Relacionamento: uma tabela contém vários procedimentos
    procedimentos = db.relationship('Procedimento', backref='tabela', lazy=True)

class Procedimento(db.Model):
    __tablename__ = 'procedimentos'
    id = db.Column(db.Integer, primary_key=True)
    codigo = db.Column(db.String(100), nullable=False)
    descricao = db.Column(db.String(500), nullable=False)
    valor = db.Column(db.Numeric(10, 2), nullable=False)
    prestador = db.Column(db.String(255), nullable=True)
    uf = db.Column(db.String(2), nullable=True)
    # Chave estrangeira para ligar à tabela de preços
    id_tabela = db.Column(db.Integer, db.ForeignKey('tabelas.id'), nullable=False)


class CBHPMItem(db.Model):
    __tablename__ = 'cbhpm_itens'
    id = db.Column(db.Integer, primary_key=True)
    # básicos
    codigo = db.Column(db.String(100), nullable=False)
    procedimento = db.Column(db.String(500), nullable=False)
    uf = db.Column(db.String(2), nullable=True)
    # porte cirúrgico
    porte = db.Column(db.String(50), nullable=True)
    fracao_porte = db.Column(db.Numeric(10, 2), nullable=True)
    valor_porte = db.Column(db.Numeric(12, 2), nullable=True)
    total_porte = db.Column(db.Numeric(12, 2), nullable=True)
    # incidências e filme
    incidencias = db.Column(db.String(255), nullable=True)
    filme = db.Column(db.Numeric(12, 2), nullable=True)
    total_filme = db.Column(db.Numeric(12, 2), nullable=True)
    # uco
    uco = db.Column(db.Numeric(12, 2), nullable=True)
    total_uco = db.Column(db.Numeric(12, 2), nullable=True)
    # anestesia
    porte_anestesico = db.Column(db.String(50), nullable=True)
    valor_porte_anestesico = db.Column(db.Numeric(12, 2), nullable=True)
    total_porte_anestesico = db.Column(db.Numeric(12, 2), nullable=True)
    # auxiliares
    numero_auxiliares = db.Column(db.Integer, nullable=True)
    total_auxiliares = db.Column(db.Numeric(12, 2), nullable=True)
    total_1_aux = db.Column(db.Numeric(12, 2), nullable=True)
    total_2_aux = db.Column(db.Numeric(12, 2), nullable=True)
    total_3_aux = db.Column(db.Numeric(12, 2), nullable=True)
    total_4_aux = db.Column(db.Numeric(12, 2), nullable=True)
    # subtotal
    subtotal = db.Column(db.Numeric(12, 2), nullable=True)
    # vínculo
    id_tabela = db.Column(db.Integer, db.ForeignKey('tabelas.id'), nullable=False)


class CbhpmTeto(db.Model):
    __tablename__ = 'cbhpm_teto'

    codigo = db.Column(db.String(20), primary_key=True)
    descricao = db.Column(db.String(255), nullable=False)
    valor_total = db.Column(db.Numeric(15, 2), nullable=False)
    updated_at = db.Column(
        db.TIMESTAMP,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
        server_onupdate=text('CURRENT_TIMESTAMP'),
    )

    __table_args__ = (
        db.Index('idx_cbhpm_teto_descricao', 'descricao'),
    )


class BrasRaw(db.Model):
    __tablename__ = 'bras_raw'

    id = db.Column(db.BigInteger, primary_key=True)
    arquivo = db.Column(db.String(255), nullable=False)
    linha_num = db.Column(db.Integer, nullable=False)
    col01 = db.Column(db.String(255))
    col02 = db.Column(db.String(255))
    col03 = db.Column(db.String(255))
    col04 = db.Column(db.String(255))
    col05 = db.Column(db.String(255))
    col06 = db.Column(db.String(255))
    col07 = db.Column(db.String(255))
    col08 = db.Column(db.String(255))
    col09 = db.Column(db.String(255))
    col10 = db.Column(db.String(255))
    col11 = db.Column(db.String(255))
    col12 = db.Column(db.String(255))
    col13 = db.Column(db.String(255))
    col14 = db.Column(db.String(255))
    col15 = db.Column(db.String(255))
    col16 = db.Column(db.String(255))
    col17 = db.Column(db.String(255))
    col18 = db.Column(db.String(255))
    col19 = db.Column(db.String(255))
    col20 = db.Column(db.String(255))
    col21 = db.Column(db.String(255))
    col22 = db.Column(db.String(255))
    col23 = db.Column(db.String(255))
    imported_at = db.Column(
        db.DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
    )

    __table_args__ = (
        db.Index('idx_bras_raw_arquivo', 'arquivo'),
        db.Index('idx_bras_raw_col17', 'col17'),
        db.Index('idx_bras_raw_col03', 'col03'),
        db.Index('idx_bras_raw_col06', 'col06'),
    )


class BrasFixedStage(db.Model):
    __tablename__ = 'bras_fixed_stage'

    id = db.Column(db.BigInteger, primary_key=True)
    arquivo = db.Column(db.String(255), nullable=False)
    linha_num = db.Column(db.Integer, nullable=False)
    linha = db.Column(db.Text, nullable=False)
    imported_at = db.Column(
        db.DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
    )

    __table_args__ = (
        db.Index('idx_bras_fixed_arquivo', 'arquivo'),
    )


class BrasItemNormalized(db.Model):
    __tablename__ = 'bras_item_n'

    id = db.Column(db.BigInteger, primary_key=True)
    arquivo = db.Column(db.String(255), nullable=False)
    linha_num = db.Column(db.Integer, nullable=False)
    laboratorio_codigo = db.Column(db.String(50), nullable=True)
    laboratorio_nome = db.Column(db.String(255), index=True, nullable=True)
    produto_codigo = db.Column(db.String(50), index=True, nullable=True)
    produto_nome = db.Column(db.String(255), index=True, nullable=True)
    apresentacao_codigo = db.Column(db.String(50), nullable=True)
    apresentacao_descricao = db.Column(db.String(255), index=True, nullable=True)
    ean = db.Column(db.String(20), index=True, nullable=True)
    registro_anvisa = db.Column(db.String(50), index=True, nullable=True)
    edicao = db.Column(db.String(50), index=True, nullable=True)
    preco_pmc_pacote = db.Column(db.Numeric(15, 4), nullable=True)
    preco_pfb_pacote = db.Column(db.Numeric(15, 4), nullable=True)
    preco_pmc_unit = db.Column(db.Numeric(15, 4), nullable=True)
    preco_pfb_unit = db.Column(db.Numeric(15, 4), nullable=True)
    aliquota_ou_ipi = db.Column(db.Numeric(15, 4), nullable=True)
    quantidade_embalagem = db.Column(db.Integer, nullable=True)
    imported_at = db.Column(db.DateTime, nullable=True)

    __table_args__ = (
        db.Index('idx_bras_item_n_ean', 'ean'),
        db.Index('idx_bras_item_n_prod', 'produto_codigo'),
        db.Index('idx_bras_item_n_desc', 'produto_nome', 'apresentacao_descricao'),
        db.Index('idx_bras_item_n_anvisa', 'registro_anvisa'),
        db.Index('idx_bras_item_n_edicao', 'edicao'),
    )


class SimproItem(db.Model):
    __tablename__ = 'simpro_item'

    id = db.Column(db.Integer, primary_key=True)
    tuss = db.Column(db.String(50), index=True, nullable=True)
    tiss = db.Column(db.String(50), index=True, nullable=True)
    anvisa = db.Column(db.String(50), index=True, nullable=True)
    descricao = db.Column(db.String(500), nullable=False, index=True)
    preco = db.Column(db.Numeric(12, 4), nullable=True)
    aliquota = db.Column(db.Numeric(12, 4), nullable=True)
    fabricante = db.Column(db.String(255), nullable=True)
    versao_tabela = db.Column(db.String(100), nullable=True)
    data_atualizacao = db.Column(db.Date, nullable=True)
    uf_referencia = db.Column(db.String(5), nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
        server_onupdate=text('CURRENT_TIMESTAMP'),
    )


class SimproFixedStage(db.Model):
    __tablename__ = 'simpro_fixed_stage'

    id = db.Column(db.BigInteger, primary_key=True)
    arquivo = db.Column(db.String(255), nullable=False)
    linha_num = db.Column(db.Integer, nullable=False)
    linha = db.Column(db.Text, nullable=False)
    imported_at = db.Column(
        db.DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
    )

    __table_args__ = (
        db.Index('idx_simpro_fixed_arquivo', 'arquivo'),
    )


class SimproItemNormalized(db.Model):
    __tablename__ = 'simpro_item_norm'

    id = db.Column(db.BigInteger, primary_key=True)
    arquivo = db.Column(db.String(255), nullable=False)
    linha_num = db.Column(db.Integer, nullable=False)
    codigo = db.Column(db.String(20), index=True, nullable=False)
    codigo_alt = db.Column(db.String(20), index=True, nullable=True)
    descricao = db.Column(db.String(255), index=True, nullable=False)
    data_ref = db.Column(db.Date, nullable=True)
    tipo_reg = db.Column(db.String(4), nullable=True)
    preco1 = db.Column(db.Numeric(15, 4), nullable=True)
    preco2 = db.Column(db.Numeric(15, 4), nullable=True)
    preco3 = db.Column(db.Numeric(15, 4), nullable=True)
    preco4 = db.Column(db.Numeric(15, 4), nullable=True)
    unidade = db.Column(db.String(16), nullable=True)
    qtd_unidade = db.Column(db.Integer, nullable=True)
    fabricante = db.Column(db.String(80), nullable=True)
    anvisa = db.Column(db.String(20), index=True, nullable=True)
    validade_anvisa = db.Column(db.Date, nullable=True)
    ean = db.Column(db.String(32), index=True, nullable=True)
    situacao = db.Column(db.String(40), nullable=True)
    versao = db.Column(db.String(100), nullable=True)
    uf_referencia = db.Column(db.String(5), nullable=True)
    imported_at = db.Column(
        db.DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
    )

    __table_args__ = (
        db.Index('idx_simpro_item_norm_desc', 'descricao'),
        db.Index('idx_simpro_item_norm_ean', 'ean'),
        db.Index('idx_simpro_item_norm_anvisa', 'anvisa'),
        db.Index('idx_simpro_item_norm_versao', 'versao'),
    )


class InsumoIndex(db.Model):
    __tablename__ = 'insumos_index'

    origem = db.Column(db.Enum('BRAS', 'SIMPRO', name='insumo_origem'), primary_key=True)
    item_id = db.Column(db.Integer, primary_key=True)
    tuss = db.Column(db.String(50), index=True, nullable=True)
    tiss = db.Column(db.String(50), index=True, nullable=True)
    descricao = db.Column(db.String(500), index=True, nullable=True)
    preco = db.Column(db.Numeric(12, 4), nullable=True)
    aliquota = db.Column(db.Numeric(12, 4), nullable=True)
    fabricante = db.Column(db.String(255), nullable=True)
    anvisa = db.Column(db.String(50), index=True, nullable=True)
    versao_tabela = db.Column(db.String(100), nullable=True)
    data_atualizacao = db.Column(db.Date, nullable=True)
    uf_referencia = db.Column(db.String(5), nullable=True)
    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        server_default=text('CURRENT_TIMESTAMP'),
        server_onupdate=text('CURRENT_TIMESTAMP'),
    )


BRAS_DEFAULT_COLUMNS = ['tuss', 'tiss', 'anvisa', 'descricao', 'preco', 'fabricante', 'aliquota']
SIMPRO_DEFAULT_COLUMNS = [
    'codigo', 'codigo_alt', 'descricao', 'data_ref', 'tipo_reg',
    'preco1', 'preco2', 'preco3', 'preco4', 'unidade', 'qtd_unidade',
    'fabricante', 'anvisa', 'validade_anvisa', 'ean', 'situacao'
]
DECIMAL_FIELDS = {'preco', 'aliquota'}
DATE_FIELDS = {'data_atualizacao'}
DEFAULT_IMPORT_ENCODINGS = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']
TETO_PREVIEW_DIR = Path(tempfile.gettempdir()) / 'cbhpm_teto_previews'
BRAS_RAW_DEFAULT_COLUMNS = [
    'col01', 'col02', 'col03', 'col04', 'col05', 'col06', 'col07', 'col08', 'col09', 'col10',
    'col11', 'col12', 'col13', 'col14', 'col15', 'col16', 'col17', 'col18', 'col19', 'col20',
    'col21', 'col22', 'col23'
]


def _clean_decimal_expression(column: str) -> str:
    sanitized = f"REPLACE(REPLACE(REPLACE({column}, '.', ''), ' ', ''), ',', '.')"
    integer_part = f"SUBSTRING_INDEX({sanitized}, '.', 1)"
    scale_expr = f"GREATEST(CHAR_LENGTH({integer_part}) - 8, 2)"
    return (
        "CAST(\n"
        "    CASE\n"
        f"        WHEN {column} IS NULL THEN NULL\n"
        f"        WHEN {sanitized} = '' THEN NULL\n"
        f"        WHEN {sanitized} NOT REGEXP '^[0-9]+(\\.[0-9]+)?$' THEN NULL\n"
        f"        WHEN CHAR_LENGTH({sanitized}) > 32 THEN NULL\n"
        f"        ELSE (CAST({sanitized} AS DECIMAL(38,6)) / POW(10, {scale_expr}))\n"
        "    END AS DECIMAL(15,4)\n"
        ")"
    )


def _build_bras_item_view_sql() -> str:
    preco_pmc = _clean_decimal_expression('r.col07')
    preco_pfb = _clean_decimal_expression('r.col08')

    return (
        "CREATE OR REPLACE VIEW bras_item_v AS\n"
        "SELECT\n"
        "    r.id,\n"
        "    r.arquivo,\n"
        "    r.linha_num,\n"
        "    r.col01 AS laboratorio_codigo,\n"
        "    r.col02 AS laboratorio_nome,\n"
        "    r.col20 AS produto_codigo,\n"
        "    r.col04 AS produto_nome,\n"
        "    r.col18 AS apresentacao_codigo,\n"
        "    r.col06 AS apresentacao_descricao,\n"
        "    r.col17 AS ean,\n"
        "    r.col22 AS registro_anvisa,\n"
        "    r.col14 AS edicao,\n"
        f"    {preco_pmc} AS preco_pmc_pacote,\n"
        f"    {preco_pfb} AS preco_pfb_pacote,\n"
        f"    {preco_pmc} AS preco_pmc_unit,\n"
        f"    {preco_pfb} AS preco_pfb_unit,\n"
        "    NULL AS aliquota_ou_ipi,\n"
        "    NULL AS quantidade_embalagem,\n"
        "    r.imported_at\n"
        "FROM bras_raw r\n"
    )


BRAS_ITEM_VIEW_SQL = _build_bras_item_view_sql()


def _normalize_column_token(name: str | None) -> str:
    if name is None:
        return ''
    token = str(name).strip().strip('"').strip("'")
    if not token:
        return ''
    normalized = unicodedata.normalize('NFKD', token)
    normalized = ''.join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = normalized.replace('-', '_').replace(' ', '_')
    normalized = re.sub(r'[^0-9a-zA-Z_]', '', normalized)
    return normalized.lower()


def _columns_valid_for_model(model_cls, columns: list[str]) -> bool:
    if not columns:
        return False
    valid = {col.name for col in model_cls.__table__.columns}
    for col in columns:
        if not col or col not in valid:
            return False
    return True


def _build_encoding_list(primary: str | None) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()

    def _add(candidate: str | None) -> None:
        if not candidate:
            return
        normalized = candidate.strip()
        if not normalized:
            return
        key = normalized.lower()
        if key in seen:
            return
        seen.add(key)
        ordered.append(normalized)

    _add(primary)
    for fallback in DEFAULT_IMPORT_ENCODINGS:
        _add(fallback)
    return ordered


MYSQL_CHARSET_MAP = {
    'utf-8-sig': 'utf8mb4',
    'utf8-sig': 'utf8mb4',
    'utf-8': 'utf8mb4',
    'utf8': 'utf8mb4',
    'utf8mb4': 'utf8mb4',
    'latin-1': 'latin1',
    'latin1': 'latin1',
    'iso-8859-1': 'latin1',
    'cp1252': 'cp1252',
}


def _encoding_to_mysql_charset(encoding: str | None) -> str:
    if not encoding:
        return 'utf8mb4'
    return MYSQL_CHARSET_MAP.get(encoding.lower(), 'utf8mb4')


def _sql_escape_literal(value: str) -> str:
    escaped = value.replace('\\', r'\\').replace("'", r"\'")
    return f"'{escaped}'"


def _encode_line_terminator(value: str | None) -> str:
    if not value:
        return '\n'
    return value.encode('unicode_escape').decode('ascii')


def _delete_existing_bras_records(arquivo_label: str | None, truncate: bool) -> None:
    if truncate:
        db.session.execute(text("DELETE FROM insumos_index WHERE origem = 'BRAS'"))
        db.session.execute(text('TRUNCATE TABLE bras_item_n'))
        db.session.execute(text('TRUNCATE TABLE bras_raw'))
        db.session.execute(text('TRUNCATE TABLE bras_fixed_stage'))
        db.session.commit()
        return

    if not arquivo_label:
        return

    params = {'arquivo': arquivo_label}
    db.session.execute(
        text(
            "DELETE FROM insumos_index "
            "WHERE origem = 'BRAS' AND item_id IN (SELECT id FROM bras_item_n WHERE arquivo = :arquivo)"
        ),
        params,
    )
    db.session.execute(text('DELETE FROM bras_item_n WHERE arquivo = :arquivo'), params)
    db.session.execute(text('DELETE FROM bras_raw WHERE arquivo = :arquivo'), params)
    db.session.execute(text('DELETE FROM bras_fixed_stage WHERE arquivo = :arquivo'), params)
    db.session.commit()


def _delete_existing_simpro_records(arquivo_label: str | None, truncate: bool) -> None:
    if truncate:
        db.session.execute(text("DELETE FROM insumos_index WHERE origem = 'SIMPRO'"))
        db.session.execute(text('TRUNCATE TABLE simpro_item_norm'))
        db.session.execute(text('TRUNCATE TABLE simpro_fixed_stage'))
        db.session.commit()
        return

    if not arquivo_label:
        return

    params = {'arquivo': arquivo_label}
    db.session.execute(
        text(
            "DELETE FROM insumos_index "
            "WHERE origem = 'SIMPRO' AND item_id IN (SELECT id FROM simpro_item_norm WHERE arquivo = :arquivo)"
        ),
        params,
    )
    db.session.execute(text('DELETE FROM simpro_item_norm WHERE arquivo = :arquivo'), params)
    db.session.execute(text('DELETE FROM simpro_fixed_stage WHERE arquivo = :arquivo'), params)
    db.session.commit()


def _bras_load_data_delimited(
    *,
    file_path: Path,
    delimiter: str,
    quotechar: str | None,
    line_terminator: str,
    skip_header: bool,
    encoding: str | None,
    arquivo_label: str,
) -> int:
    charset = _encoding_to_mysql_charset(encoding)
    delimiter_lit = _sql_escape_literal(delimiter)
    line_term_lit = _sql_escape_literal(_encode_line_terminator(line_terminator))
    file_literal = _sql_escape_literal(str(file_path))
    arquivo_literal = _sql_escape_literal(arquivo_label)
    quote_clause = ''
    if quotechar:
        quote_clause = f"OPTIONALLY ENCLOSED BY {_sql_escape_literal(quotechar)}\n"

    ignore_clause = 'IGNORE 1 LINES\n' if skip_header else ''

    bindings = [f"@col{idx:02d}" for idx in range(1, 24)]
    set_lines = [
        f"col{idx:02d} = NULLIF(@col{idx:02d}, '')"
        for idx in range(1, 24)
    ]
    set_lines.append(f"arquivo = {arquivo_literal}")
    set_lines.append("linha_num = (@row := @row + 1)")
    set_clause = ',\n        '.join(set_lines)

    load_stmt = (
        f"LOAD DATA LOCAL INFILE {file_literal}\n"
        "INTO TABLE bras_raw\n"
        f"CHARACTER SET {charset}\n"
        f"FIELDS TERMINATED BY {delimiter_lit}\n"
        f"{quote_clause}"
        f"LINES TERMINATED BY {line_term_lit}\n"
        f"{ignore_clause}"
        f"({', '.join(bindings)})\n"
        f"SET {set_clause}"
    )

    with db.engine.begin() as conn:
        conn.exec_driver_sql('SET @row := 0')
        result = conn.exec_driver_sql(load_stmt)
        return result.rowcount or 0


def _bras_csv_fallback(
    *,
    file_path: Path,
    delimiter: str,
    quotechar: str | None,
    skip_header: bool,
    encodings: list[str],
    arquivo_label: str,
) -> int:
    for enc in encodings:
        try:
            with file_path.open('r', encoding=enc, newline='') as handle:
                reader = csv.reader(handle, delimiter=delimiter, quotechar=quotechar or '"')
                rows: list[dict] = []
                for idx, raw in enumerate(reader, start=1):
                    if skip_header and idx == 1:
                        continue
                    values = (raw or [])[:23]
                    values += [''] * (23 - len(values))
                    mapping = {
                        'arquivo': arquivo_label,
                        'linha_num': len(rows) + 1,
                        **{f'col{pos:02d}': (val.strip() or None) if isinstance(val, str) else None for pos, val in enumerate(values, start=1)}
                    }
                    rows.append(mapping)
            if rows:
                db.session.bulk_insert_mappings(BrasRaw, rows)
                db.session.commit()
                return len(rows)
            return 0
        except UnicodeDecodeError:
            db.session.rollback()
            continue
    raise click.ClickException('Não foi possível decodificar o arquivo com as codificações testadas.')


def _stage_simpro_fixed(
    *,
    file_path: Path,
    map_config: dict,
    encoding: str | None,
    arquivo_label: str,
) -> int:
    encodings = _build_encoding_list(encoding)
    inserted = 0
    for enc in encodings:
        try:
            rows: list[dict] = []
            with file_path.open('r', encoding=enc, newline='') as handle:
                for idx, raw_line in enumerate(handle, start=1):
                    line = raw_line.rstrip('\r\n')
                    rows.append({
                        'arquivo': arquivo_label,
                        'linha_num': idx,
                        'linha': line,
                    })
            if rows:
                db.session.bulk_insert_mappings(SimproFixedStage, rows)
                db.session.commit()
                inserted = len(rows)
                break
        except UnicodeDecodeError:
            db.session.rollback()
            continue
    if not inserted:
        raise click.ClickException('Não foi possível decodificar o arquivo de largura fixa do SIMPRO.')
    return inserted


def _parse_fixed_date(value: str | None, fmt: str | None) -> date | None:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    fmt = (fmt or 'DDMMYYYY').upper()
    python_fmt = fmt.replace('YYYY', '%Y').replace('YY', '%y').replace('MM', '%m').replace('DD', '%d')
    try:
        return datetime.strptime(value, python_fmt).date()
    except ValueError:
        return None


def _sanitize_numeric(value: str) -> str:
    return ''.join(ch for ch in value if ch.isdigit() or ch in ',.-')


def _auto_scale_decimal(value: Decimal | None, *, max_integer_digits: int = 8, min_fraction_digits: int = 2) -> Decimal | None:
    if value is None:
        return None
    if not isinstance(value, Decimal):
        value = Decimal(str(value))

    sign = -1 if value < 0 else 1
    magnitude = value.copy_abs()
    if not magnitude:
        return value.quantize(Decimal('0.01'))

    digits_tuple = magnitude.normalize().as_tuple()
    digits_len = len(digits_tuple.digits)
    exponent = digits_tuple.exponent
    integer_digits = digits_len + exponent
    if integer_digits < 0:
        integer_digits = 0

    scale_power = max(integer_digits - max_integer_digits, 0)
    if scale_power > 0:
        magnitude = magnitude / (Decimal(10) ** scale_power)

    while magnitude >= Decimal('1000'):
        magnitude = magnitude / Decimal('10')

    quantize_pattern = '0.' + ('0' * max(min_fraction_digits, 4))
    return (magnitude * sign).quantize(Decimal(quantize_pattern))


def _materialize_simpro_items(
    *,
    arquivo_label: str,
    map_config: dict,
    versao: str,
    uf_default: str | None,
) -> int:
    columns_cfg = map_config.get('columns') or []
    if not columns_cfg:
        raise click.ClickException('Mapa SIMPRO precisa definir "columns".')

    decimal_divisor = Decimal(str(map_config.get('decimal_divisor') or '1'))
    rows = (
        SimproFixedStage.query
        .filter_by(arquivo=arquivo_label)
        .order_by(SimproFixedStage.linha_num.asc())
        .all()
    )

    parsed_rows: list[dict] = []
    for stage in rows:
        line = stage.linha or ''
        record: dict[str, object | None] = {
            'id': stage.id,
            'arquivo': stage.arquivo,
            'linha_num': stage.linha_num,
            'versao': versao,
            'uf_referencia': uf_default,
            'imported_at': stage.imported_at,
        }
        for cfg in columns_cfg:
            name = (cfg.get('name') or '').strip()
            if not name:
                continue
            start = max(int(cfg.get('start', 1)) - 1, 0)
            length = max(int(cfg.get('length', 0)), 0)
            raw_value = line[start:start + length]
            if cfg.get('strip') and isinstance(cfg['strip'], (list, tuple)):
                for ch in cfg['strip']:
                    raw_value = raw_value.replace(str(ch), '')
            value = raw_value.strip()
            if not value:
                record[name] = None
                continue

            value_type = (cfg.get('type') or '').lower()
            if value_type == 'decimal':
                coerced = _coerce_decimal(_sanitize_numeric(value))
                if coerced is None:
                    record[name] = None
                else:
                    divisor = Decimal(str(cfg.get('divide_by') or decimal_divisor)) or Decimal('1')
                    scaled = Decimal(coerced) / divisor
                    record[name] = _auto_scale_decimal(scaled)
            elif value_type == 'date':
                record[name] = _parse_fixed_date(value, cfg.get('date_fmt'))
            elif value_type == 'int':
                digits = ''.join(ch for ch in value if ch.isdigit() or ch == '-')
                try:
                    record[name] = int(digits) if digits else None
                except ValueError:
                    record[name] = None
            else:
                record[name] = value
        parsed_rows.append(record)

    if not parsed_rows:
        return 0

    db.session.bulk_insert_mappings(SimproItemNormalized, parsed_rows)
    db.session.commit()
    return len(parsed_rows)
def _stage_bras_delimited(
    *,
    file_path: Path,
    delimiter: str,
    quotechar: str | None,
    line_terminator: str,
    skip_header: bool,
    encoding: str | None,
    arquivo_label: str,
    use_load_data: bool,
) -> int:
    encodings = _build_encoding_list(encoding)
    inserted = 0
    if use_load_data:
        try:
            inserted = _bras_load_data_delimited(
                file_path=file_path,
                delimiter=delimiter,
                quotechar=quotechar,
                line_terminator=line_terminator,
                skip_header=skip_header,
                encoding=encodings[0],
                arquivo_label=arquivo_label,
            )
        except Exception as exc:
            db.session.rollback()
            app.logger.warning('LOAD DATA falhou (%s); usando fallback Python.', exc)
            inserted = 0

    if not inserted:
        inserted = _bras_csv_fallback(
            file_path=file_path,
            delimiter=delimiter,
            quotechar=quotechar,
            skip_header=skip_header,
            encodings=encodings,
            arquivo_label=arquivo_label,
        )
    return inserted


def _stage_bras_fixed(
    *,
    file_path: Path,
    map_config: dict,
    encoding: str | None,
    line_terminator: str,
    arquivo_label: str,
) -> int:
    columns_cfg = map_config.get('columns') or []
    if not columns_cfg:
        raise click.ClickException('Arquivo de mapeamento precisa definir "columns".')

    encodings = _build_encoding_list(encoding)
    inserted = 0
    for enc in encodings:
        try:
            rows_stage: list[dict] = []
            rows_raw: list[dict] = []
            with file_path.open('r', encoding=enc, newline='') as handle:
                for idx, raw_line in enumerate(handle, start=1):
                    line = raw_line.rstrip('\r\n')
                    rows_stage.append({'arquivo': arquivo_label, 'linha_num': idx, 'linha': line})
                    mapping = {'arquivo': arquivo_label, 'linha_num': idx}
                    for col in columns_cfg:
                        name = col.get('name')
                        start = int(col.get('start', 1)) - 1
                        length = int(col.get('length', 0))
                        if not name or length <= 0:
                            continue
                        snippet = line[start:start + length]
                        mapping[name] = snippet.strip() or None
                    rows_raw.append(mapping)
            if rows_stage:
                db.session.bulk_insert_mappings(BrasFixedStage, rows_stage)
            if rows_raw:
                db.session.bulk_insert_mappings(BrasRaw, rows_raw)
            db.session.commit()
            inserted = len(rows_raw)
            break
        except UnicodeDecodeError:
            db.session.rollback()
            continue
    if not inserted:
        raise click.ClickException('Não foi possível decodificar o arquivo de largura fixa.')
    return inserted


def _ensure_bras_item_view_exists() -> None:
    with db.engine.begin() as conn:
        conn.exec_driver_sql(BRAS_ITEM_VIEW_SQL)


def _materialize_bras_items(arquivo_label: str | None) -> int:
    _ensure_bras_item_view_exists()
    params: dict[str, str] = {}
    where_clause = ''
    if arquivo_label:
        params['arquivo'] = arquivo_label
        where_clause = 'WHERE arquivo = :arquivo'

    insert_sql = text(
        """
        INSERT INTO bras_item_n (
            id, arquivo, linha_num,
            laboratorio_codigo, laboratorio_nome,
            produto_codigo, produto_nome,
            apresentacao_codigo, apresentacao_descricao,
            ean, registro_anvisa, edicao,
            preco_pmc_pacote, preco_pfb_pacote, preco_pmc_unit, preco_pfb_unit,
            aliquota_ou_ipi, quantidade_embalagem, imported_at
        )
        SELECT
            id, arquivo, linha_num,
            laboratorio_codigo, laboratorio_nome,
            produto_codigo, produto_nome,
            apresentacao_codigo, apresentacao_descricao,
            ean, registro_anvisa, edicao,
            preco_pmc_pacote, preco_pfb_pacote, preco_pmc_unit, preco_pfb_unit,
            aliquota_ou_ipi, quantidade_embalagem, imported_at
        FROM bras_item_v
        {where_clause}
        ON DUPLICATE KEY UPDATE
            arquivo = VALUES(arquivo),
            linha_num = VALUES(linha_num),
            laboratorio_codigo = VALUES(laboratorio_codigo),
            laboratorio_nome = VALUES(laboratorio_nome),
            produto_codigo = VALUES(produto_codigo),
            produto_nome = VALUES(produto_nome),
            apresentacao_codigo = VALUES(apresentacao_codigo),
            apresentacao_descricao = VALUES(apresentacao_descricao),
            ean = VALUES(ean),
            registro_anvisa = VALUES(registro_anvisa),
            edicao = VALUES(edicao),
            preco_pmc_pacote = VALUES(preco_pmc_pacote),
            preco_pfb_pacote = VALUES(preco_pfb_pacote),
            preco_pmc_unit = VALUES(preco_pmc_unit),
            preco_pfb_unit = VALUES(preco_pfb_unit),
            aliquota_ou_ipi = VALUES(aliquota_ou_ipi),
            quantidade_embalagem = VALUES(quantidade_embalagem),
            imported_at = VALUES(imported_at)
        """.replace('{where_clause}', where_clause)
    )

    result = db.session.execute(insert_sql, params)
    db.session.commit()
    return result.rowcount or 0


def _sync_bras_insumo_index(
    arquivo_label: str | None,
    *,
    uf_default: str | None = None,
    aliquota_default: Decimal | None = None,
) -> None:
    params: dict[str, object] = {}
    where_clause = ''
    if arquivo_label:
        params['arquivo'] = arquivo_label
        where_clause = 'WHERE arquivo = :arquivo'
    params['uf_default'] = uf_default
    params['aliquota_default'] = aliquota_default

    upsert_sql = text(
        """
        INSERT INTO insumos_index (
            origem, item_id, tuss, tiss, descricao, preco, aliquota,
            fabricante, anvisa, versao_tabela, data_atualizacao,
            uf_referencia, updated_at
        )
        SELECT
            'BRAS' AS origem,
            n.id AS item_id,
            n.produto_codigo AS tuss,
            n.apresentacao_codigo AS tiss,
            TRIM(CONCAT_WS(' • ', NULLIF(n.produto_nome, ''), NULLIF(n.apresentacao_descricao, ''))) AS descricao,
            COALESCE(n.preco_pmc_unit, n.preco_pmc_pacote, n.preco_pfb_unit, n.preco_pfb_pacote) AS preco,
            COALESCE(n.aliquota_ou_ipi, :aliquota_default) AS aliquota,
            n.laboratorio_nome AS fabricante,
            n.registro_anvisa AS anvisa,
            COALESCE(n.edicao, n.arquivo) AS versao_tabela,
            NULL AS data_atualizacao,
            COALESCE(:uf_default, NULL) AS uf_referencia,
            NOW() AS updated_at
        FROM bras_item_n n
        {where_clause}
        ON DUPLICATE KEY UPDATE
            tuss = VALUES(tuss),
            tiss = VALUES(tiss),
            descricao = VALUES(descricao),
            preco = VALUES(preco),
            aliquota = VALUES(aliquota),
            fabricante = VALUES(fabricante),
            anvisa = VALUES(anvisa),
            versao_tabela = VALUES(versao_tabela),
            data_atualizacao = VALUES(data_atualizacao),
            uf_referencia = VALUES(uf_referencia),
            updated_at = VALUES(updated_at)
        """.replace('{where_clause}', where_clause)
    )

    db.session.execute(upsert_sql, params)
    db.session.commit()


def _sync_simpro_insumo_index(
    arquivo_label: str | None,
    *,
    uf_default: str | None = None,
    aliquota_default: Decimal | None = None,
) -> None:
    params: dict[str, object] = {}
    where_clause = ''
    if arquivo_label:
        params['arquivo'] = arquivo_label
        where_clause = 'WHERE arquivo = :arquivo'
    params['uf_default'] = uf_default
    params['aliquota_default'] = aliquota_default

    upsert_sql = text(
        """
        INSERT INTO insumos_index (
            origem, item_id, tuss, tiss, descricao, preco, aliquota,
            fabricante, anvisa, versao_tabela, data_atualizacao,
            uf_referencia, updated_at
        )
        SELECT
            'SIMPRO' AS origem,
            n.id AS item_id,
            n.codigo AS tuss,
            n.codigo_alt AS tiss,
            n.descricao AS descricao,
            COALESCE(n.preco2, n.preco1, n.preco3, n.preco4) AS preco,
            :aliquota_default AS aliquota,
            n.fabricante AS fabricante,
            n.anvisa AS anvisa,
            COALESCE(n.versao, n.arquivo) AS versao_tabela,
            n.data_ref AS data_atualizacao,
            COALESCE(n.uf_referencia, :uf_default) AS uf_referencia,
            NOW() AS updated_at
        FROM simpro_item_norm n
        {where_clause}
        ON DUPLICATE KEY UPDATE
            tuss = VALUES(tuss),
            tiss = VALUES(tiss),
            descricao = VALUES(descricao),
            preco = VALUES(preco),
            aliquota = VALUES(aliquota),
            fabricante = VALUES(fabricante),
            anvisa = VALUES(anvisa),
            versao_tabela = VALUES(versao_tabela),
            data_atualizacao = VALUES(data_atualizacao),
            uf_referencia = VALUES(uf_referencia),
            updated_at = VALUES(updated_at)
        """.replace('{where_clause}', where_clause)
    )

    db.session.execute(upsert_sql, params)
    db.session.commit()


def _import_bras(
    *,
    file_path: Path,
    versao: str,
    data_ref: str | None,
    fmt: str,
    delimiter: str,
    quotechar: str | None,
    line_terminator: str,
    skip_header: bool,
    encoding: str | None,
    map_config: dict,
    truncate: bool,
    uf_default: str | None = None,
    aliquota_default: Decimal | None = None,
) -> dict:
    del data_ref
    arquivo_label = map_config.get('arquivo') or versao or file_path.name
    if uf_default and not map_config.get('arquivo'):
        arquivo_label = f"{arquivo_label}_{uf_default.upper()}"

    _delete_existing_bras_records(arquivo_label, truncate)

    inserted = 0
    if fmt == 'delimited':
        inserted = _stage_bras_delimited(
            file_path=file_path,
            delimiter=delimiter,
            quotechar=quotechar,
            line_terminator=line_terminator,
            skip_header=skip_header,
            encoding=encoding,
            arquivo_label=arquivo_label,
            use_load_data=not map_config.get('disable_load_data', False),
        )
    else:
        inserted = _stage_bras_fixed(
            file_path=file_path,
            map_config=map_config,
            encoding=encoding,
            line_terminator=line_terminator,
            arquivo_label=arquivo_label,
        )

    materialized = _materialize_bras_items(arquivo_label if not truncate else None)
    _sync_bras_insumo_index(
        arquivo_label if not truncate else None,
        uf_default=uf_default,
        aliquota_default=aliquota_default,
    )

    return {
        'arquivo': arquivo_label,
        'linhas_raw': inserted,
        'linhas_materializadas': materialized,
    }


def _import_simpro(
    *,
    file_path: Path,
    versao: str,
    fmt: str,
    map_config: dict,
    encoding: str | None,
    truncate: bool,
    uf_default: str | None,
    aliquota_default: Decimal | None,
) -> dict:
    if fmt != 'fixed':
        raise click.ClickException('Importação SIMPRO suporta apenas formato de largura fixa no momento.')

    arquivo_label = map_config.get('arquivo') or versao or file_path.name
    if uf_default and not map_config.get('arquivo'):
        arquivo_label = f"{arquivo_label}_{uf_default.upper()}"

    _delete_existing_simpro_records(arquivo_label, truncate)

    inserted = _stage_simpro_fixed(
        file_path=file_path,
        map_config=map_config,
        encoding=encoding,
        arquivo_label=arquivo_label,
    )

    materialized = _materialize_simpro_items(
        arquivo_label=arquivo_label,
        map_config=map_config,
        versao=versao,
        uf_default=uf_default,
    )

    _sync_simpro_insumo_index(
        arquivo_label if not truncate else None,
        uf_default=uf_default,
        aliquota_default=aliquota_default,
    )

    return {
        'arquivo': arquivo_label,
        'linhas_raw': inserted,
        'linhas_materializadas': materialized,
    }


DECIMAL_SANITIZE_RE = re.compile(r'[^0-9,\.-]')


def _coerce_decimal(value: str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return format(value, 'f')
    if isinstance(value, (int, float)):
        try:
            decimal_value = Decimal(str(value))
        except (InvalidOperation, ValueError):
            return None
        return format(decimal_value, 'f')

    raw = str(value).strip()
    if not raw:
        return None

    raw = raw.replace('\xa0', '').replace(' ', '')
    if raw.endswith('-') and raw.count('-') == 1:
        raw = '-' + raw[:-1]
    raw = DECIMAL_SANITIZE_RE.sub('', raw)

    if not raw or raw in {'-', '.', ',', '-.', '-,'}:
        return None
    if raw.count('-') > 1 or (raw[0] == '-' and '-' in raw[1:]):
        return None

    negative = raw.startswith('-')
    if negative:
        raw = raw[1:]

    if not raw:
        return None

    decimal_sep = None
    if ',' in raw and '.' in raw:
        last_comma = raw.rfind(',')
        last_dot = raw.rfind('.')
        sep_index = max(last_comma, last_dot)
        decimal_sep = raw[sep_index]
        integer_part = raw[:sep_index].replace(',', '').replace('.', '')
        fractional_part = raw[sep_index + 1:]
    else:
        sep = None
        if ',' in raw:
            sep = ','
        elif '.' in raw:
            sep = '.'

        if sep is not None:
            occurrences = [idx for idx, ch in enumerate(raw) if ch == sep]
            last_idx = occurrences[-1]
            decimals_len = len(raw) - last_idx - 1
            if 0 < decimals_len <= 6:
                decimal_sep = sep
                integer_part = raw[:last_idx].replace(',', '').replace('.', '')
                fractional_part = raw[last_idx + 1:]
            else:
                integer_part = raw.replace(',', '').replace('.', '')
                fractional_part = ''
        else:
            integer_part = raw
            fractional_part = ''

    if decimal_sep is None:
        normalized = integer_part
    else:
        normalized = f"{integer_part}.{fractional_part}" if fractional_part else integer_part

    if negative:
        normalized = f"-{normalized}"

    if not normalized or normalized in {'-', '.', '-.'}:
        return None

    try:
        decimal_value = Decimal(normalized)
    except (InvalidOperation, ValueError):
        return None

    return format(decimal_value, 'f')


def _coerce_date(value: str | None) -> date | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y'):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _normalize_delimiter(delimiter: str) -> str:
    if delimiter.lower() in {'\t', 'tab'}:
        return '\t'
    return delimiter


def _resolve_columns(config_columns: list[str] | None, default_columns: list[str], header: list[str] | None) -> list[str]:
    if config_columns:
        sanitized = [_normalize_column_token(col) for col in config_columns]
        cols = [col for col in sanitized if col]
        return cols or default_columns
    if header:
        sanitized = [_normalize_column_token(h) for h in header]
        cols = [col for col in sanitized if col]
        return cols or default_columns
    return default_columns


def _parse_positive_int(value: str | None, default: int, *, minimum: int = 1, maximum: int | None = None) -> int:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError):
        return default
    if parsed < minimum:
        return minimum
    if maximum is not None and parsed > maximum:
        return maximum
    return parsed


def _decimal_to_string(value: Decimal | None, precision: int = 4) -> str | None:
    if value is None:
        return None
    quantize_target = Decimal('1').scaleb(-precision)
    try:
        normalized = value.quantize(quantize_target)
    except (InvalidOperation, ValueError):
        normalized = value
    normalized = normalized.normalize()
    as_str = format(normalized, 'f')
    if '.' in as_str:
        as_str = as_str.rstrip('0').rstrip('.')
    return as_str

def _ensure_teto_preview_dir() -> Path:
    directory = TETO_PREVIEW_DIR
    try:
        directory.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    return directory


def _format_brl(value: Decimal | None) -> str:
    if value is None:
        return ''
    try:
        quantized = Decimal(value).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
        return ''
    formatted = format(quantized, ',.2f')
    return 'R$ ' + formatted.replace(',', 'X').replace('.', ',').replace('X', '.')


def _store_teto_preview(payload: dict) -> str:
    directory = _ensure_teto_preview_dir()
    token = uuid4().hex
    rows = []
    for row in payload.get('rows', []):
        value = row.get('valor_total')
        if value is not None and not isinstance(value, str):
            try:
                value = format(Decimal(value), 'f')
            except (InvalidOperation, ValueError):
                value = None
        rows.append({
            'codigo': row.get('codigo'),
            'descricao': row.get('descricao'),
            'valor_total': value,
            'row_number': row.get('row_number'),
        })
    data = {
        'rows': rows,
        'meta': payload.get('meta', {}),
        'errors': payload.get('errors', []),
    }
    file_path = directory / f'{token}.json'
    file_path.write_text(json.dumps(data, ensure_ascii=False))
    return token


def _load_teto_preview(token: str) -> dict | None:
    if not token:
        return None
    file_path = _ensure_teto_preview_dir() / f'{token}.json'
    if not file_path.exists():
        return None
    try:
        raw = json.loads(file_path.read_text(encoding='utf-8'))
    except (json.JSONDecodeError, OSError):
        return None
    rows: list[dict] = []
    for row in raw.get('rows', []):
        valor = row.get('valor_total')
        try:
            valor_decimal = Decimal(str(valor)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if valor is not None else None
        except (InvalidOperation, ValueError):
            valor_decimal = None
        rows.append({
            'codigo': row.get('codigo'),
            'descricao': row.get('descricao'),
            'valor_total': valor_decimal,
            'row_number': row.get('row_number'),
        })
    return {
        'rows': rows,
        'meta': raw.get('meta', {}),
        'errors': raw.get('errors', []),
        'token': token,
    }


def _discard_teto_preview(token: str) -> None:
    if not token:
        return
    file_path = _ensure_teto_preview_dir() / f'{token}.json'
    try:
        file_path.unlink(missing_ok=True)
    except Exception:
        pass


def _read_teto_rows_from_csv(file_path: Path) -> list[tuple[int, dict[str, object]]]:
    encodings = DEFAULT_IMPORT_ENCODINGS + ['utf-8']
    for enc in encodings:
        try:
            with file_path.open('r', encoding=enc, newline='') as handle:
                first_line = handle.readline()
                if not first_line:
                    return []
                delimiter = ';' if first_line.count(';') >= first_line.count(',') else ','
                handle.seek(0)
                reader = csv.reader(handle, delimiter=delimiter)
                try:
                    header = next(reader)
                except StopIteration:
                    return []
                headers_norm = [_norm_header(h) for h in header]
                rows: list[tuple[int, dict[str, object]]] = []
                for row_idx, raw_row in enumerate(reader, start=2):
                    values: dict[str, object] = {}
                    for col_idx, key in enumerate(headers_norm):
                        if not key:
                            continue
                        value = raw_row[col_idx] if col_idx < len(raw_row) else ''
                        if isinstance(value, str):
                            value = value.strip()
                        values[key] = value
                    rows.append((row_idx, values))
                return rows
        except UnicodeDecodeError:
            continue
    raise click.ClickException('Não foi possível decodificar o arquivo CSV (UTF-8/Latin-1).')


def _read_teto_rows_from_xlsx(file_path: Path) -> list[tuple[int, dict[str, object]]]:
    try:
        from openpyxl import load_workbook
    except Exception as exc:
        raise click.ClickException('Dependência openpyxl não disponível para ler arquivos XLSX.') from exc
    wb = load_workbook(file_path, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return []
    headers = [str(cell).strip() if cell is not None else '' for cell in rows[0]]
    headers_norm = [_norm_header(h) for h in headers]
    parsed: list[tuple[int, dict[str, object]]] = []
    for idx, line in enumerate(rows[1:], start=2):
        values: dict[str, object] = {}
        for col_idx, key in enumerate(headers_norm):
            if not key:
                continue
            value = line[col_idx] if col_idx < len(line) else None
            if isinstance(value, str):
                value = value.strip()
            values[key] = value
        parsed.append((idx, values))
    return parsed


def _parse_teto_import_file(file_path: Path) -> dict:
    ext = file_path.suffix.lower()
    if ext in {'.csv', '.txt'}:
        raw_rows = _read_teto_rows_from_csv(file_path)
    elif ext in {'.xlsx'}:
        raw_rows = _read_teto_rows_from_xlsx(file_path)
    else:
        raise click.ClickException('Formato não suportado. Envie um arquivo CSV ou XLSX.')

    records: dict[str, dict[str, object]] = {}
    order: list[str] = []
    errors: list[str] = []
    total_input = 0
    duplicate_count = 0

    for row_number, row in raw_rows:
        total_input += 1
        codigo_raw = row.get('codigo') or row.get('codigoprocedimento')
        codigo = str(codigo_raw or '').strip().upper()
        if not codigo:
            errors.append(f"Linha {row_number}: campo 'codigo' é obrigatório.")
            continue

        descricao_raw = row.get('descricao') or row.get('procedimento')
        descricao = str(descricao_raw or '').strip()
        if not descricao:
            errors.append(f"Linha {row_number} ({codigo}): campo 'descricao' é obrigatório.")
            continue

        valor_raw = row.get('valor_total') or row.get('valortotal') or row.get('valor')
        valor_str = _coerce_decimal(valor_raw if valor_raw is not None else '')
        if valor_str is None:
            errors.append(f"Linha {row_number} ({codigo}): valor_total inválido.")
            continue
        try:
            valor_decimal = Decimal(valor_str)
        except (InvalidOperation, ValueError):
            errors.append(f"Linha {row_number} ({codigo}): valor_total inválido.")
            continue
        if valor_decimal <= Decimal('0'):
            errors.append(f"Linha {row_number} ({codigo}): valor_total deve ser maior que zero.")
            continue
        valor_decimal = valor_decimal.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

        record = {
            'codigo': codigo,
            'descricao': descricao,
            'valor_total': valor_decimal,
            'row_number': row_number,
        }
        if codigo in records:
            duplicate_count += 1
            try:
                order.remove(codigo)
            except ValueError:
                pass
        order.append(codigo)
        records[codigo] = record

    final_rows = [records[cod] for cod in order]
    return {
        'rows': final_rows,
        'errors': errors,
        'total_input': total_input,
        'valid_count': len(final_rows),
        'duplicate_count': duplicate_count,
    }




def _serialize_insumo_index(item: 'InsumoIndex', *, preco_pmc: Decimal | None = None, preco_pfb: Decimal | None = None) -> dict:
    return {
        'origem': item.origem,
        'item_id': item.item_id,
        'tuss': item.tuss,
        'tiss': item.tiss,
        'descricao': item.descricao,
        'preco': _decimal_to_string(item.preco),
        'preco_pmc': _decimal_to_string(preco_pmc if preco_pmc is not None else item.preco),
        'preco_pfb': _decimal_to_string(preco_pfb if preco_pfb is not None else item.preco),
        'aliquota': _decimal_to_string(item.aliquota),
        'fabricante': item.fabricante,
        'anvisa': item.anvisa,
        'versao_tabela': item.versao_tabela,
        'data_atualizacao': item.data_atualizacao.isoformat() if isinstance(item.data_atualizacao, date) else None,
        'updated_at': item.updated_at.isoformat() if isinstance(item.updated_at, datetime) else None,
        'uf_referencia': item.uf_referencia,
    }


def _serialize_insumo_detail(
    origem: str,
    item: BrasItemNormalized | SimproItemNormalized | SimproItem,
    index_entry: InsumoIndex | None = None,
) -> dict:
    index_aliquota = _decimal_to_string(index_entry.aliquota) if index_entry else None
    index_uf = index_entry.uf_referencia if index_entry else None
    index_data = index_entry.data_atualizacao.isoformat() if isinstance(getattr(index_entry, 'data_atualizacao', None), date) else None
    index_created = index_entry.updated_at.isoformat() if isinstance(getattr(index_entry, 'updated_at', None), datetime) else None
    if isinstance(item, BrasItemNormalized):
        descricao = item.produto_nome or ''
        if item.apresentacao_descricao:
            descricao = f"{descricao} • {item.apresentacao_descricao}" if descricao else item.apresentacao_descricao
        return {
            'origem': 'BRAS',
            'item_id': item.id,
            'tuss': item.produto_codigo,
            'tiss': item.apresentacao_codigo,
            'anvisa': item.registro_anvisa,
            'descricao': descricao,
            'preco': _decimal_to_string(item.preco_pmc_unit) or _decimal_to_string(item.preco_pmc_pacote),
            'preco_pmc': _decimal_to_string(item.preco_pmc_unit) or _decimal_to_string(item.preco_pmc_pacote),
            'preco_pfb': _decimal_to_string(item.preco_pfb_unit) or _decimal_to_string(item.preco_pfb_pacote),
            'aliquota': _decimal_to_string(item.aliquota_ou_ipi) or index_aliquota,
            'fabricante': item.laboratorio_nome,
            'versao_tabela': item.edicao or item.arquivo,
            'data_atualizacao': index_data,
            'updated_at': (item.imported_at.isoformat() if isinstance(item.imported_at, datetime) else index_created),
            'created_at': None,
            'uf_referencia': index_uf,
            'arquivo': item.arquivo,
            'linha_num': item.linha_num,
            'preco_pmc_pacote': _decimal_to_string(item.preco_pmc_pacote),
            'preco_pfb_pacote': _decimal_to_string(item.preco_pfb_pacote),
            'preco_pfb_unit': _decimal_to_string(item.preco_pfb_unit),
            'quantidade_embalagem': item.quantidade_embalagem,
        }

    if isinstance(item, SimproItemNormalized):
        preco_candidates = [item.preco2, item.preco1, item.preco3, item.preco4]
        preco_effective = next((p for p in preco_candidates if p is not None), None)
        return {
            'origem': 'SIMPRO',
            'item_id': item.id,
            'tuss': item.codigo,
            'tiss': item.codigo_alt,
            'anvisa': item.anvisa,
            'descricao': item.descricao,
            'preco': _decimal_to_string(preco_effective),
            'preco_pmc': None,
            'preco_pfb': _decimal_to_string(preco_effective),
            'aliquota': index_aliquota,
            'fabricante': item.fabricante,
            'versao_tabela': item.versao or item.arquivo,
            'data_atualizacao': item.data_ref.isoformat() if isinstance(item.data_ref, date) else index_data,
            'updated_at': item.imported_at.isoformat() if isinstance(item.imported_at, datetime) else index_created,
            'created_at': None,
            'uf_referencia': item.uf_referencia or index_uf,
            'situacao': item.situacao,
            'validade_anvisa': item.validade_anvisa.isoformat() if isinstance(item.validade_anvisa, date) else None,
            'ean': item.ean,
        }

    if isinstance(item, SimproItem):  # fallback legacy
        return {
            'origem': origem,
            'item_id': item.id,
            'tuss': item.tuss,
            'tiss': item.tiss,
            'anvisa': item.anvisa,
            'descricao': item.descricao,
            'preco': _decimal_to_string(item.preco),
            'preco_pmc': _decimal_to_string(item.preco),
            'preco_pfb': _decimal_to_string(item.preco),
            'aliquota': _decimal_to_string(item.aliquota),
            'fabricante': item.fabricante,
            'versao_tabela': item.versao_tabela,
            'data_atualizacao': item.data_atualizacao.isoformat() if isinstance(item.data_atualizacao, date) else None,
            'updated_at': item.updated_at.isoformat() if isinstance(item.updated_at, datetime) else None,
            'created_at': item.created_at.isoformat() if isinstance(item.created_at, datetime) else None,
            'uf_referencia': item.uf_referencia,
        }

    return {
        'origem': origem,
        'item_id': item.id,
        'tuss': None,
        'tiss': None,
        'anvisa': None,
        'descricao': '',
        'preco': None,
        'preco_pmc': None,
        'preco_pfb': None,
        'aliquota': None,
        'fabricante': None,
        'versao_tabela': None,
        'data_atualizacao': None,
        'updated_at': None,
        'created_at': None,
        'uf_referencia': None,
    }


def _extract_insumo_filters(args) -> dict:
    origem = (args.get('origem') or '').strip().upper()
    uf_ref = (args.get('uf_referencia') or args.get('uf') or '').strip().upper()
    aliquota_raw = (args.get('aliquota') or '').strip()
    aliquota_filter = _coerce_decimal(aliquota_raw) if aliquota_raw else None
    aliquota_value = Decimal(aliquota_filter) if aliquota_filter is not None else None
    filters = {
        'origem': origem if origem in {'BRAS', 'SIMPRO'} else None,
        'tuss': (args.get('tuss') or '').strip() or None,
        'tiss': (args.get('tiss') or '').strip() or None,
        'anvisa': (args.get('anvisa') or '').strip() or None,
        'fabricante': (args.get('fabricante') or '').strip() or None,
        'versao_tabela': (args.get('versao_tabela') or '').strip() or None,
        'uf_referencia': uf_ref or None,
        'aliquota': aliquota_value,
    }
    q = (args.get('q') or '').strip()
    tokens = [token.lower() for token in re.split(r'\s+', q) if token]
    filters['tokens'] = tokens[:6]
    filters['raw_q'] = q
    return filters


def _apply_insumo_filters(query, filters: dict):
    origem = filters.get('origem')
    if origem:
        query = query.filter(InsumoIndex.origem == origem)

    if filters.get('tuss'):
        query = query.filter(InsumoIndex.tuss == filters['tuss'])
    if filters.get('tiss'):
        query = query.filter(InsumoIndex.tiss == filters['tiss'])
    if filters.get('anvisa'):
        query = query.filter(InsumoIndex.anvisa == filters['anvisa'])
    if filters.get('fabricante'):
        fabricante = filters['fabricante'].lower()
        query = query.filter(func.lower(InsumoIndex.fabricante).like(f"%{fabricante}%"))
    if filters.get('versao_tabela'):
        query = query.filter(InsumoIndex.versao_tabela == filters['versao_tabela'])
    if filters.get('uf_referencia'):
        query = query.filter(func.upper(InsumoIndex.uf_referencia) == filters['uf_referencia'])
    if filters.get('aliquota') is not None:
        query = query.filter(InsumoIndex.aliquota == filters['aliquota'])

    tokens = filters.get('tokens') or []
    for token in tokens:
        pattern = f"%{token}%"
        query = query.filter(
            or_(
                func.lower(InsumoIndex.descricao).like(pattern),
                func.lower(InsumoIndex.fabricante).like(pattern),
                func.lower(func.coalesce(InsumoIndex.tuss, '')).like(pattern),
                func.lower(func.coalesce(InsumoIndex.tiss, '')).like(pattern),
                func.lower(func.coalesce(InsumoIndex.anvisa, '')).like(pattern),
            )
        )

    return query


def _insumo_summary(model_cls) -> dict:
    total = db.session.query(func.count(model_cls.id)).scalar() or 0

    updated_column = None
    for candidate in ('updated_at', 'imported_at'):
        updated_column = getattr(model_cls, candidate, None)
        if updated_column is not None:
            break
    last_updated = db.session.query(func.max(updated_column)).scalar() if updated_column is not None else None

    data_column = getattr(model_cls, 'data_atualizacao', None)
    if data_column is None:
        data_column = getattr(model_cls, 'data_ref', None)
    last_data = db.session.query(func.max(data_column)).scalar() if data_column is not None else None

    version_column = None
    for candidate in ('versao_tabela', 'versao', 'edicao', 'arquivo'):
        version_column = getattr(model_cls, candidate, None)
        if version_column is not None:
            break

    latest_version = None
    if version_column is not None:
        latest_version = (
            db.session.query(version_column)
            .filter(version_column.isnot(None))
            .order_by(version_column.desc())
            .limit(1)
            .scalar()
        )

    return {
        'total': int(total),
        'last_updated': last_updated,
        'last_data_ref': last_data,
        'latest_version': latest_version,
    }


def _insumo_distinct_versions(model_cls) -> list[str]:
    version_column = None
    for candidate in ('versao_tabela', 'versao', 'edicao', 'arquivo'):
        version_column = getattr(model_cls, candidate, None)
        if version_column is not None:
            break

    if version_column is None:
        return []

    rows = (
        db.session.query(version_column)
        .filter(version_column.isnot(None))
        .distinct()
        .order_by(version_column)
        .all()
    )
    return [row[0] for row in rows if row[0]]


def _get_teto_map(codigos: list[str]) -> dict[str, 'CbhpmTeto']:
    unique_codes = {str(c or '').strip().upper() for c in codigos if str(c or '').strip()}
    if not unique_codes:
        return {}
    rows = CbhpmTeto.query.filter(CbhpmTeto.codigo.in_(unique_codes)).all()
    return {row.codigo.upper(): row for row in rows}


def _load_data_local_infile(engine, table_name: str, columns: list[str], file_path: Path, delimiter: str,
                             quotechar: str | None, skip_lines: int, extra_assignments: dict[str, str | None],
                             charset: str | None) -> int:
    bindings = [f"@col{idx}" for idx in range(len(columns))]
    set_clauses: list[str] = []
    params: dict[str, str | None] = {'file_path': str(file_path), 'delimiter': delimiter}
    if quotechar:
        params['enclosed'] = quotechar

    for name, binding in zip(columns, bindings):
        if name in DECIMAL_FIELDS:
            normalized = f"REPLACE({binding}, ',', '.')"
            set_clauses.append(
                f"{name} = CASE WHEN {normalized} REGEXP '^-?[0-9]+(\\.[0-9]+)?$' "
                f"THEN NULLIF({normalized}, '') ELSE NULL END"
            )
        else:
            set_clauses.append(f"{name} = NULLIF({binding}, '')")

    for key, value in extra_assignments.items():
        param_key = f"extra_{key}"
        set_clauses.append(f"{key} = :{param_key}")
        params[param_key] = value

    sql_parts = [
        "LOAD DATA LOCAL INFILE :file_path",
        f"INTO TABLE {table_name}",
        "FIELDS TERMINATED BY :delimiter",
    ]
    if charset:
        sql_parts.insert(2, f"CHARACTER SET {charset}")
    if quotechar:
        sql_parts.append("OPTIONALLY ENCLOSED BY :enclosed")
    sql_parts.append("LINES TERMINATED BY '\n'")
    if skip_lines:
        sql_parts.append(f"IGNORE {skip_lines} LINES")
    sql_parts.append(f"({', '.join(bindings)})")
    sql_parts.append(f"SET {', '.join(set_clauses)}")
    sql = "\n".join(sql_parts)

    with engine.begin() as conn:
        result = conn.exec_driver_sql(sql, params)
        return result.rowcount or 0


def _fallback_delimited(model_cls, columns: list[str], file_path: Path, delimiter: str, quotechar: str | None,
                        skip_header: bool, extra_assignments: dict[str, object | None],
                        encodings: list[str]) -> int:
    delimiter = delimiter or ';'
    quotechar = quotechar or '"'
    tried: list[str] = []

    for encoding in encodings:
        try:
            created = 0
            with file_path.open('r', encoding=encoding, newline='') as fh:
                reader = csv.reader(fh, delimiter=delimiter, quotechar=quotechar)
                if skip_header:
                    next(reader, None)
                rows = []
                for raw_row in reader:
                    record: dict[str, object | None] = {}
                    for idx, col in enumerate(columns):
                        value = raw_row[idx] if idx < len(raw_row) else ''
                        value = value.strip() if isinstance(value, str) else value
                        if not value:
                            record[col] = None
                        elif col in DECIMAL_FIELDS:
                            coerced = _coerce_decimal(value)
                            record[col] = Decimal(coerced) if coerced is not None else None
                        elif col in DATE_FIELDS:
                            record[col] = _coerce_date(value)
                        else:
                            record[col] = value
                    record.update(extra_assignments)
                    rows.append(model_cls(**record))
                if rows:
                    db.session.bulk_save_objects(rows)
                    db.session.commit()
                    created = len(rows)
            return created
        except UnicodeDecodeError:
            tried.append(encoding)
            db.session.rollback()
            continue

    tried_display = ', '.join(tried) if tried else 'utf-8'
    raise click.ClickException(
        f'Não foi possível decodificar o arquivo com as codificações testadas ({tried_display}). '
        'Informe a codificação correta ou converta o arquivo para UTF-8.'
    )


def _handle_delimited_import(*, model_cls, table_name: str, file_path: Path, versao: str,
                             data_ref: date | None, delimiter: str, quotechar: str | None,
                             columns_cfg: list[str] | None, skip_header: bool, use_load_data: bool,
                             truncate: bool, encoding: str | None,
                             extra_assignments: dict[str, object | None]) -> int:
    if truncate:
        db.session.query(model_cls).delete(synchronize_session=False)
        db.session.commit()

    encodings = _build_encoding_list(encoding)
    chosen_encoding = encodings[0] if encodings else 'utf-8-sig'
    header: list[str] | None = None
    if skip_header:
        effective_delimiter = delimiter or ';'
        effective_quotechar = (quotechar or '"') if quotechar is not None else '"'
        raw_header: list[str] = []
        try:
            for encoding_option in encodings:
                try:
                    with file_path.open('r', encoding=encoding_option, newline='') as fh:
                        reader = csv.reader(fh, delimiter=effective_delimiter, quotechar=effective_quotechar)
                        raw_header = next(reader, [])
                        chosen_encoding = encoding_option
                        break
                except UnicodeDecodeError:
                    db.session.rollback()
                    raw_header = []
                    continue
        except Exception:
            raw_header = []

        header = [_normalize_column_token(h) for h in raw_header]
        if not _columns_valid_for_model(model_cls, header):
            header = None
            skip_header = False

    default_cols = BRAS_DEFAULT_COLUMNS if table_name == 'bras_item' else SIMPRO_DEFAULT_COLUMNS
    columns = _resolve_columns(columns_cfg, default_cols, header)

    has_decimal_columns = any(col in DECIMAL_FIELDS for col in columns)
    if has_decimal_columns:
        use_load_data = False

    inserted = 0
    if use_load_data:
        try:
            inserted = _load_data_local_infile(
                db.engine,
                table_name,
                columns,
                file_path,
                delimiter,
                quotechar,
                1 if skip_header else 0,
                {k: (_decimal_to_string(v) if isinstance(v, (Decimal, float)) else (v.isoformat() if isinstance(v, date) else v)) for k, v in extra_assignments.items()},
                _encoding_to_mysql_charset(chosen_encoding),
            )
        except Exception:
            db.session.rollback()
            inserted = 0

    if not inserted:
        inserted = _fallback_delimited(
            model_cls,
            columns,
            file_path,
            delimiter,
            quotechar,
            skip_header,
            extra_assignments,
            encodings,
        )
    return inserted


def _handle_fixed_import(*, model_cls, file_path: Path, versao: str, data_ref: date | None,
                         map_config: dict, truncate: bool,
                         extra_assignments: dict[str, object | None]) -> int:
    columns_cfg = map_config.get('columns') or []
    if not columns_cfg:
        raise click.ClickException('Arquivo de mapeamento precisa definir "columns".')
    if truncate:
        db.session.query(model_cls).delete(synchronize_session=False)
        db.session.commit()

    extra_assignments_local = dict(extra_assignments)

    rows = []
    encoding = map_config.get('encoding', 'utf-8-sig')
    multiplier = Decimal(str(map_config.get('decimal_divisor', '100')))
    with file_path.open('r', encoding=encoding) as fh:
        for raw_line in fh:
            record: dict[str, object | None] = {}
            for cfg in columns_cfg:
                name = cfg.get('name')
                if not name:
                    continue
                start = int(cfg.get('start', 1)) - 1
                length = int(cfg.get('length', 0))
                value = raw_line[start:start + length].strip()
                divides_by = cfg.get('divide_by')
                if divides_by:
                    try:
                        divisor = Decimal(str(divides_by))
                    except Exception:
                        divisor = multiplier
                else:
                    divisor = multiplier if name in DECIMAL_FIELDS or cfg.get('type') == 'decimal' else Decimal('1')

                if not value:
                    record[name] = None
                elif name in DECIMAL_FIELDS or cfg.get('type') == 'decimal':
                    coerced = _coerce_decimal(value)
                    if coerced is None:
                        record[name] = None
                    else:
                        record[name] = (Decimal(coerced) / divisor) if divisor else Decimal(coerced)
                elif name in DATE_FIELDS or cfg.get('type') == 'date':
                    record[name] = _coerce_date(value)
                else:
                    record[name] = value
            record.update(extra_assignments_local)
            rows.append(model_cls(**record))
    if rows:
        db.session.bulk_save_objects(rows)
        db.session.commit()
    return len(rows)


def _run_insumo_import(resource: str, model_cls, table_name: str, file_path: Path, versao: str,
                       data_str: str | None, fmt: str, delimiter: str, quotechar: str | None,
                       map_path: Path | None, no_header: bool, truncate: bool, encoding: str | None,
                       uf_referencia: str | None, aliquota: Decimal | None) -> None:
    file_path = file_path.resolve()
    if not file_path.exists():
        raise click.ClickException(f'Arquivo não encontrado: {file_path}')

    data_ref = _coerce_date(data_str)
    map_config: dict = {}
    if map_path:
        try:
            map_config = json.loads(map_path.read_text(encoding='utf-8'))
        except json.JSONDecodeError as exc:
            raise click.ClickException(f'Não foi possível ler o arquivo de mapeamento: {exc}') from exc
        if not isinstance(map_config, dict):
            raise click.ClickException('Arquivo de mapeamento deve conter um objeto JSON na raiz.')

    delimiter = _normalize_delimiter(map_config.get('delimiter', delimiter)) if fmt == 'delimited' else delimiter
    quotechar_cfg = map_config.get('quotechar') if fmt == 'delimited' else None
    if quotechar_cfg is not None:
        quotechar = quotechar_cfg
    if quotechar is not None and not str(quotechar).strip():
        quotechar = None

    encoding_cfg = map_config.get('encoding') if fmt == 'delimited' else map_config.get('encoding')
    if isinstance(encoding_cfg, str) and encoding_cfg.strip():
        encoding = encoding_cfg.strip()
    elif isinstance(encoding_cfg, list) and encoding_cfg:
        encoding = str(encoding_cfg[0]).strip() or encoding

    skip_header = map_config.get('skip_header') if fmt == 'delimited' and 'skip_header' in map_config else (not no_header)
    columns_cfg = map_config.get('columns') if fmt == 'delimited' else map_config.get('columns')

    base_assignments: dict[str, object | None] = {
        'versao_tabela': versao,
        'data_atualizacao': data_ref,
    }
    if uf_referencia:
        base_assignments['uf_referencia'] = uf_referencia
    if aliquota is not None:
        base_assignments['aliquota'] = aliquota

    extra_from_map = map_config.get('extra') if isinstance(map_config.get('extra'), dict) else {}

    merged_assignments = dict(extra_from_map)
    merged_assignments.update(base_assignments)

    for key, value in list(merged_assignments.items()):
        if key in DECIMAL_FIELDS and value is not None:
            if isinstance(value, Decimal):
                continue
            if isinstance(value, (int, float)):
                merged_assignments[key] = Decimal(str(value))
            else:
                coerced = _coerce_decimal(str(value))
                merged_assignments[key] = Decimal(coerced) if coerced is not None else None

    if fmt == 'delimited':
        _handle_delimited_import(
            model_cls=model_cls,
            table_name=table_name,
            file_path=file_path,
            versao=versao,
            data_ref=data_ref,
            delimiter=_normalize_delimiter(delimiter or ';'),
            quotechar=quotechar,
            columns_cfg=columns_cfg,
            skip_header=bool(skip_header),
            use_load_data=not map_config.get('disable_load_data', False),
            truncate=truncate,
            encoding=encoding,
            extra_assignments=merged_assignments,
        )
    else:
        if not map_path:
            raise click.ClickException('Formato fixed requer arquivo de mapeamento (--map).')
        _handle_fixed_import(
            model_cls=model_cls,
            file_path=file_path,
            versao=versao,
            data_ref=data_ref,
            map_config=map_config,
            truncate=truncate,
            extra_assignments=merged_assignments,
        )


def _common_import_options(func):
    func = click.option('--truncate', is_flag=True, default=False, help='Limpa a tabela antes de importar.')(func)
    func = click.option('--no-header', is_flag=True, default=False, help='Arquivo sem cabeçalho (delimited).')(func)
    func = click.option('--map', 'map_path', type=click.Path(exists=True, dir_okay=False, path_type=Path), help='Arquivo JSON com configuração.')(func)
    func = click.option('--quotechar', default='"', show_default=True, help='Delimitador de texto (apenas delimited).')(func)
    func = click.option('--delimiter', default=';', show_default=True, help='Delimitador (apenas delimited).')(func)
    func = click.option('--lines-terminated', 'lines_terminated', default='\n', show_default=True, help='Terminador de linha do arquivo.')(func)
    func = click.option('--encoding', default=None, help='Codificação do arquivo (tenta auto se omitido).')(func)
    func = click.option('--uf', 'uf_referencia', default=None, help='UF de referência da tabela importada.')(func)
    func = click.option('--aliquota', default=None, help='Alíquota associada à tabela (percentual).')(func)
    func = click.option('--format', 'fmt', type=click.Choice(['delimited', 'fixed']), default='delimited', show_default=True)(func)
    func = click.option('--data', 'data_str', required=False, help='Data de atualização (YYYY-MM-DD).')(func)
    func = click.option('--versao', required=True, help='Versão de referência da tabela.')(func)
    func = click.option('--file', 'file_path', type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)(func)
    return func


@app.cli.command('bras:import')
@_common_import_options
def bras_import(file_path: Path, versao: str, data_str: str | None, fmt: str, delimiter: str,
                quotechar: str, map_path: Path | None, no_header: bool, truncate: bool,
                encoding: str | None, uf_referencia: str | None, aliquota: str | None,
                lines_terminated: str) -> None:
    """Importa arquivo da Brasíndice (pipeline staging + materialização)."""
    uf_value = (uf_referencia or '').strip().upper() or None
    aliquota_value: Decimal | None = None
    if aliquota:
        aliquota_str = _coerce_decimal(aliquota)
        if aliquota_str is None:
            raise click.ClickException('Valor de alíquota inválido.')
        aliquota_value = Decimal(aliquota_str)

    file_path = file_path.resolve()
    if not file_path.exists():
        raise click.ClickException(f'Arquivo não encontrado: {file_path}')

    map_config: dict = {}
    if map_path:
        try:
            map_config = json.loads(map_path.read_text(encoding='utf-8'))
        except json.JSONDecodeError as exc:
            raise click.ClickException(f'Não foi possível ler o arquivo de mapeamento: {exc}') from exc
        if not isinstance(map_config, dict):
            raise click.ClickException('Arquivo de mapeamento deve conter um objeto JSON na raiz.')

    delimiter = map_config.get('delimiter', delimiter) if fmt == 'delimited' else delimiter
    quote_cfg = map_config.get('quotechar') if fmt == 'delimited' else None
    if quote_cfg is not None:
        quotechar = quote_cfg
    if quotechar is not None and not str(quotechar).strip():
        quotechar = None

    encoding_cfg = map_config.get('encoding')
    if isinstance(encoding_cfg, str) and encoding_cfg.strip():
        encoding = encoding_cfg.strip()

    line_cfg = map_config.get('lines_terminated') or map_config.get('line_terminator')
    if line_cfg:
        lines_terminated = line_cfg

    skip_header_cfg = map_config.get('skip_header') if 'skip_header' in map_config else None
    skip_header = bool(skip_header_cfg) if skip_header_cfg is not None else (not no_header)

    result = _import_bras(
        file_path=file_path,
        versao=versao,
        data_ref=data_str,
        fmt=fmt,
        delimiter=_normalize_delimiter(delimiter) if fmt == 'delimited' else delimiter,
        quotechar=quotechar,
        line_terminator=lines_terminated or '\n',
        skip_header=skip_header,
        encoding=encoding,
        map_config=map_config,
        truncate=truncate,
        uf_default=uf_value,
        aliquota_default=aliquota_value,
    )

    click.echo(f"Brasíndice importado: arquivo={result['arquivo']} linhas_raw={result['linhas_raw']} materializadas={result['linhas_materializadas']}")


@app.cli.command('simpro:import')
@_common_import_options
def simpro_import(file_path: Path, versao: str, data_str: str | None, fmt: str, delimiter: str,
                  quotechar: str, map_path: Path | None, no_header: bool, truncate: bool,
                  encoding: str | None, uf_referencia: str | None, aliquota: str | None,
                  lines_terminated: str) -> None:
    """Importa arquivo do SIMPRO."""
    del lines_terminated
    uf_value = (uf_referencia or '').strip().upper() or None
    aliquota_value: Decimal | None = None
    if aliquota:
        aliquota_str = _coerce_decimal(aliquota)
        if aliquota_str is None:
            raise click.ClickException('Valor de alíquota inválido.')
        aliquota_value = Decimal(aliquota_str)

    if fmt != 'fixed':
        raise click.ClickException('Importação SIMPRO suporta apenas arquivos de largura fixa.')

    file_path = file_path.resolve()
    if not file_path.exists():
        raise click.ClickException(f'Arquivo não encontrado: {file_path}')

    map_config: dict = {}
    if map_path:
        try:
            map_config = json.loads(map_path.read_text(encoding='utf-8'))
        except json.JSONDecodeError as exc:
            raise click.ClickException(f'Não foi possível ler o arquivo de mapeamento: {exc}') from exc
        if not isinstance(map_config, dict):
            raise click.ClickException('Arquivo de mapeamento deve conter um objeto JSON na raiz.')
    if not map_config:
        raise click.ClickException('Informe um mapa JSON contendo as posições do arquivo SIMPRO.')

    encoding_cfg = map_config.get('encoding')
    if isinstance(encoding_cfg, str) and encoding_cfg.strip():
        encoding = encoding_cfg.strip()

    result = _import_simpro(
        file_path=file_path,
        versao=versao,
        fmt=fmt,
        map_config=map_config,
        encoding=encoding,
        truncate=truncate,
        uf_default=uf_value,
        aliquota_default=aliquota_value,
    )

    click.echo(
        f"Importação SIMPRO concluída: arquivo={result['arquivo']} linhas_raw={result['linhas_raw']} "
        f"materializadas={result['linhas_materializadas']}"
    )


class CBHPMRuleSet(db.Model):
    __tablename__ = 'cbhpm_rulesets'
    id = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(255), nullable=False)
    versao = db.Column(db.String(50), nullable=True)
    descricao = db.Column(db.Text, nullable=True)
    ativo = db.Column(db.Boolean, nullable=False, default=False)
    regras = db.Column(db.JSON, nullable=False, default=dict)
    criado_em = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    atualizado_em = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)


DEFAULT_CBHPM_RULES = {
    "descricao": "Regras base CBHPM",
    "porte": {
        "reducoes_simultaneos": [1.0, 0.5, 0.3, 0.2]
    },
    "auxiliares": {
        "percentuais": [0.3, 0.2, 0.1, 0.1],
        "max_por_porte": {
            "0": 0,
            "1": 0,
            "2": 1,
            "3": 2,
            "4": 2,
            "5": 3,
            "6": 3,
            "default": 2
        }
    },
    "uco": {"multiplicador": 1.0},
    "filme": {"multiplicador": 1.0}
}

class PorteValorItem(db.Model):
    __tablename__ = 'porte_valores'
    id = db.Column(db.Integer, primary_key=True)
    porte = db.Column(db.String(50), nullable=False)
    valor = db.Column(db.Numeric(12, 2), nullable=False)
    uf = db.Column(db.String(2), nullable=True)
    id_tabela = db.Column(db.Integer, db.ForeignKey('tabelas.id'), nullable=False)


class PorteAnestesicoValorItem(db.Model):
    __tablename__ = 'porte_anestesico_valores'
    id = db.Column(db.Integer, primary_key=True)
    porte_an = db.Column(db.String(50), nullable=False)
    valor = db.Column(db.Numeric(12, 2), nullable=False)
    uf = db.Column(db.String(2), nullable=True)
    id_tabela = db.Column(db.Integer, db.ForeignKey('tabelas.id'), nullable=False)


# --- 3. ROTAS (PÁGINAS) ---

@app.route('/')
@login_required
def dashboard():
    return render_template('index.html')


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form.get('email')
        senha = request.form.get('senha')
        usuario = Usuario.query.filter_by(email=email, senha=senha).first()
        if usuario:
            session['user_id'] = usuario.id
            session['perfil']  = usuario.perfil
            session['nome']    = usuario.nome
            return redirect(url_for('dashboard'))

        # falha: mantém layout limpo
        return render_template('login.html', erro='Credenciais inválidas', hide_chrome=True)

    # GET: layout limpo
    return render_template('login.html', hide_chrome=True)



@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))



@app.route('/consulta-comparar')
@login_required
def consulta_comparar():
    restore_cbhpm_payload = None
    history_id = request.args.get('sim_hist')
    if history_id:
        history = session.get('sim_history') or []
        entry = next((item for item in history if str(item.get('id')) == str(history_id)), None)
        if entry:
            if entry.get('type') == 'cbhpm' and entry.get('payload'):
                restore_cbhpm_payload = entry.get('payload')
            elif entry.get('type') == 'compare' and entry.get('url_fragment'):
                target = entry.get('url_fragment') or ''
                if target:
                    return redirect(f"{url_for('consulta_comparar')}?{target}")

    q = request.args.get('q', '').strip()
    search_code = None
    search_text = None
    if ' - ' in q:
        parts = q.split(' - ', 1)
        search_code = parts[0].strip()
        search_text = parts[1].strip() if len(parts) > 1 else ''

    tabela_nome = request.args.get('tabela_nome')
    selected_uf = request.args.get('uf') or ''
    selected_prestadores = request.args.getlist('prestadores')
    selected_versoes = request.args.getlist('versoes')
    show_results = (request.args.get('run') == '1')

    procs_raw = request.args.getlist('procedimentos')  # ex.: ['10101012', '10101020 - ...']
    codigos = []
    for s in procs_raw:
        s = (s or '').strip()
        if not s:
            continue
        codigos.append(s.split(' - ', 1)[0].strip())
    if q.isdigit():
        codigos.append(q)
    codigos = list(dict.fromkeys(codigos))

    nomes = [r[0] for r in db.session.query(Tabela.nome).distinct().order_by(Tabela.nome).all()]
    if not tabela_nome and len(nomes) == 1:
        tabela_nome = nomes[0]

    is_cbhpm = False
    if tabela_nome:
        is_cbhpm = db.session.query(CBHPMItem.id)            .join(Tabela, CBHPMItem.id_tabela == Tabela.id)            .filter(Tabela.nome == tabela_nome).first() is not None

    prestadores_disp, versoes_disp = [], []
    if tabela_nome:
        if is_cbhpm:
            versoes_disp = [r[0] for r in db.session.query(Tabela.nome)
                              .filter(Tabela.tipo_tabela == 'cbhpm')
                              .distinct().order_by(Tabela.nome).all()]
        else:
            q_prest = db.session.query(Procedimento.prestador)                .join(Tabela, Procedimento.id_tabela == Tabela.id)                .filter(Tabela.nome == tabela_nome)
            if selected_uf:
                q_prest = q_prest.filter(or_(Tabela.uf == selected_uf, Procedimento.uf == selected_uf))
            prestadores_disp = [r[0] for r in q_prest
                .filter((Procedimento.prestador.isnot(None)) & (Procedimento.prestador != ''))
                .distinct().order_by(Procedimento.prestador).all()]

    columns = (selected_versoes or versoes_disp) if is_cbhpm else (selected_prestadores or prestadores_disp)

    rows = []
    if show_results and tabela_nome:
        data = {}
        if is_cbhpm:
            targets = columns or []
            for ver in targets:
                qv = db.session.query(
                        CBHPMItem.codigo, CBHPMItem.procedimento,
                        CBHPMItem.subtotal, CBHPMItem.total_porte, CBHPMItem.valor_porte,
                        CBHPMItem.total_uco, CBHPMItem.uco, CBHPMItem.total_filme, CBHPMItem.filme
                    )                    .join(Tabela, CBHPMItem.id_tabela == Tabela.id)                    .filter(Tabela.nome == ver, Tabela.tipo_tabela == 'cbhpm')
                if selected_uf:
                    qv = qv.filter(or_(Tabela.uf == selected_uf, CBHPMItem.uf == selected_uf))

                if codigos:
                    qv = qv.filter(CBHPMItem.codigo.in_(codigos))
                elif q:
                    if search_code:
                        qv = qv.filter(or_(
                            CBHPMItem.codigo == search_code,
                            CBHPMItem.codigo.ilike(f"{search_code}%"),
                            (CBHPMItem.procedimento.ilike(f"%{search_text}%") if search_text else False)
                        ))
                    else:
                        like = f"%{q}%"
                        qv = qv.filter(or_(CBHPMItem.codigo.ilike(like), CBHPMItem.procedimento.ilike(like)))

                for cod, desc, sub, tp, vp, tu, u, tf, f in qv.all():
                    v = sub or tp or vp or tu or u or tf or f
                    entry = data.setdefault(cod, {"descricao": desc, "values": {}})
                    entry["values"][ver] = v
        else:
            query = db.session.query(Procedimento, Procedimento.prestador)                .join(Tabela, Procedimento.id_tabela == Tabela.id)                .filter(Tabela.nome == tabela_nome)
            if selected_uf:
                query = query.filter(or_(Tabela.uf == selected_uf, Procedimento.uf == selected_uf))
            if selected_prestadores:
                query = query.filter(Procedimento.prestador.in_(selected_prestadores))

            if codigos:
                query = query.filter(Procedimento.codigo.in_(codigos))
            elif q:
                if search_code:
                    query = query.filter(or_(
                        Procedimento.codigo == search_code,
                        Procedimento.codigo.ilike(f"{search_code}%"),
                        (Procedimento.descricao.ilike(f"%{search_text}%") if search_text else False)
                    ))
                else:
                    like = f"%{q}%"
                    query = query.filter(or_(Procedimento.codigo.ilike(like), Procedimento.descricao.ilike(like)))

            prestadores_usados = set()
            for proc, prest in query.all():
                prest = prest or '-'
                prestadores_usados.add(prest)
                entry = data.setdefault(proc.codigo, {"descricao": proc.descricao, "values": {}})
                entry["values"][prest] = proc.valor
            if not selected_prestadores and prestadores_usados:
                columns = sorted(list(prestadores_usados))

        for codigo in sorted(data.keys()):
            item = data[codigo]
            values = [item["values"].get(p) for p in columns]
            numeric = [v for v in values if v is not None]
            min_v = min(numeric) if numeric else None
            max_v = max(numeric) if numeric else None
            avg_v = (sum(numeric) / len(numeric)) if numeric else None
            rows.append({
                "codigo": codigo,
                "descricao": item["descricao"],
                "values": values,
                "min": min_v, "max": max_v, "avg": avg_v, "count": len(numeric)
            })

    porte_list = [t.nome for t in Tabela.query.filter_by(tipo_tabela='porte').order_by(Tabela.nome).all()]
    porte_an_list = [t.nome for t in Tabela.query.filter_by(tipo_tabela='porte_anestesico').order_by(Tabela.nome).all()]
    dtp_list = [t.nome for t in Tabela.query.filter_by(tipo_tabela='diarias_taxas_pacotes').order_by(Tabela.nome).all()]
    cbhpm_list_all = [r[0] for r in db.session.query(Tabela.nome).filter(Tabela.tipo_tabela=='cbhpm').distinct().order_by(Tabela.nome).all()]
    ruleset_dict, ruleset_model = _get_active_cbhpm_ruleset(return_model=True)
    rules_meta = {
        'nome': ruleset_model.nome if ruleset_model else 'Padrão',
        'versao': ruleset_model.versao if ruleset_model else None,
        'descricao': ruleset_model.descricao if ruleset_model else None,
        'id': ruleset_model.id if ruleset_model else None,
    }

    if show_results and tabela_nome:
        query_bytes = request.query_string or b''
        if query_bytes:
            try:
                query_str = query_bytes.decode('utf-8', 'ignore')
            except Exception:
                query_str = query_bytes.decode('latin-1', 'ignore')
            label_parts = []
            clean_table = unicodedata.normalize('NFKD', tabela_nome).encode('ascii', 'ignore').decode() if tabela_nome else ''
            if clean_table:
                label_parts.append(clean_table)
            if codigos:
                snippet = ', '.join(codigos[:3])
                if len(codigos) > 3:
                    snippet += ', ...'
                snippet = unicodedata.normalize('NFKD', snippet).encode('ascii', 'ignore').decode()
                label_parts.append(f'codigos {snippet}')
            elif q:
                clean_q = unicodedata.normalize('NFKD', q).encode('ascii', 'ignore').decode()
                label_parts.append(f'busca {clean_q}')
            label = ' | '.join(filter(None, label_parts)) or (clean_table or 'Consulta recente')
            entry_id = hashlib.md5(query_str.encode('utf-8')).hexdigest()[:10] if query_str else uuid4().hex[:8]
            _store_history_entry({
                'type': 'compare',
                'id': entry_id,
                'signature': f'compare:{query_str}',
                'url_fragment': query_str,
                'label': label[:80],
                'timestamp': datetime.now().strftime('%d/%m %H:%M'),
            })

    return render_template(
        'consulta-comparar.html',
        nomes=nomes, tabela_nome=tabela_nome,
        prestadores_disp=prestadores_disp, selected_prestadores=selected_prestadores,
        versoes_disp=versoes_disp, selected_versoes=selected_versoes,
        UFS=BR_UFS, selected_uf=selected_uf, q=q,
        columns=columns, rows=rows, show_results=show_results, is_cbhpm=is_cbhpm,
        porte_list=porte_list, porte_an_list=porte_an_list,
        cbhpm_list_all=cbhpm_list_all, dtp_list=dtp_list,
        restore_cbhpm_payload=restore_cbhpm_payload,
        cbhpm_rules_info=rules_meta
    )


def _compute_simulacao_cbhpm(data):
    data = data or {}

    ruleset_dict, ruleset_model = _get_active_cbhpm_ruleset(return_model=True)
    rules_meta = {
        'nome': ruleset_model.nome if ruleset_model else 'Padrao',
        'versao': ruleset_model.versao if ruleset_model else None,
        'descricao': ruleset_model.descricao if ruleset_model else None,
        'id': ruleset_model.id if ruleset_model else None,
    }

    quantize_money = Decimal('0.01')

    via_pct_map_raw = data.get('via_entrada_pcts')
    via_pct_map: dict[str, Decimal] = {}
    if isinstance(via_pct_map_raw, dict):
        for key, value in via_pct_map_raw.items():
            key_norm = str(key or '').strip()
            if not key_norm:
                continue
            pct = _as_decimal(value)
            if pct is None:
                continue
            try:
                pct = max(Decimal('0'), min(Decimal('100'), pct))
            except Exception:
                continue
            via_pct_map[key_norm] = pct
    if '__default__' not in via_pct_map:
        via_pct_map['__default__'] = Decimal('100')
    applied_via_map: dict[str, Decimal] = {}

    def apply_via_entrada(breakdown: dict | None, code_key: str | None):
        if not breakdown:
            return breakdown
        normalized = str(code_key or '').strip().upper() or '__default__'
        pct = via_pct_map.get(normalized, via_pct_map.get('__default__', Decimal('100')))
        applied_via_map[normalized] = pct
        factor = (pct / Decimal('100')) if pct is not None else Decimal('1')
        breakdown['via_entrada_pct'] = pct
        breakdown['via_entrada_factor'] = factor

        total_porte_original = _as_decimal(breakdown.get('total_porte'))
        total_filme = _as_decimal(breakdown.get('total_filme'))
        total_uco = _as_decimal(breakdown.get('total_uco'))
        total_an = _as_decimal(breakdown.get('total_porte_an'))
        total_aux = _as_decimal(breakdown.get('total_auxiliares'))
        total_original = _sum_decimals([total_porte_original, total_filme, total_uco, total_an, total_aux])
        breakdown['total_original'] = total_original

        reduced_porte = total_porte_original
        if total_porte_original is not None:
            try:
                reduced_porte = (total_porte_original * factor).quantize(quantize_money, rounding=ROUND_HALF_UP)
            except (InvalidOperation, ValueError):
                reduced_porte = total_porte_original
        breakdown['total_porte'] = reduced_porte

        total_reduzido = _sum_decimals([reduced_porte, total_filme, total_uco, total_an, total_aux])
        breakdown['total_reduzido'] = total_reduzido
        breakdown['total_final'] = total_reduzido
        breakdown['total'] = total_reduzido
        return breakdown

    codigo = (data.get('codigo') or '').strip()
    codigos = data.get('codigos') or []
    if isinstance(codigos, list):
        codigos = [str(c or '').split(' - ', 1)[0].strip() for c in codigos if (c or '')]

    dtp_raw = data.get('dtp_items') or []
    dtp_map = {}
    for item in dtp_raw:
        code = str(item.get('codigo') or '').strip()
        if not code:
            continue
        desc = (item.get('descricao') or '').strip()
        valor = _as_decimal(item.get('valor'))
        tabela_nome = (item.get('tabela_nome') or item.get('tabela') or '').strip()
        uf_item = (item.get('uf') or '').strip()
        dtp_map[code] = {
            'codigo': code,
            'descricao': desc,
            'valor': valor,
            'tabela_nome': tabela_nome,
            'uf': uf_item,
        }

    codigos = list(dict.fromkeys(codigos))
    dtp_items = list(dtp_map.values())
    if dtp_items:
        codigos = [c for c in codigos if c not in dtp_map]

    uf = (data.get('uf') or '').strip() or None
    versao = data.get('versao')
    porte_tab_name = data.get('porte_tab')
    porte_an_tab_name = data.get('porte_an_tab')
    uco_valor_in = _as_decimal(data.get('uco_valor'))
    filme_valor_in = _as_decimal(data.get('filme_valor'))
    incid_in = _as_decimal(data.get('incidencias'))
    aj_porte_pct = _as_decimal(data.get('ajuste_porte_pct')) or Decimal('0')
    aj_an_pct = _as_decimal(data.get('ajuste_porte_an_pct')) or Decimal('0')

    if not codigo and not codigos and not dtp_items:
        return {"error": 'Informe "codigo" ou a lista "codigos".'}, 400

    item = None
    t_ref = None

    target_code = codigo or (codigos[0] if codigos else '')
    if target_code:
        q = (db.session.query(CBHPMItem, Tabela)
             .join(Tabela, CBHPMItem.id_tabela == Tabela.id))
        if versao:
            q = q.filter(Tabela.nome == versao)
        q = q.filter(or_(CBHPMItem.codigo == target_code,
                         CBHPMItem.codigo.ilike(f"{target_code}%")))
        if uf:
            q = q.filter(or_(CBHPMItem.uf == uf, Tabela.uf == uf))
        row = q.first()
        if row:
            item, t_ref = row[0], row[1]

    if not t_ref and versao:
        t_ref = Tabela.query.filter_by(nome=versao).first()

    if not t_ref:
        t_ref = Tabela.query.filter_by(tipo_tabela='cbhpm').first()

    if not t_ref:
        op = Operadora.query.first()
        t_ref = Tabela(nome='SIMULACAO', id_operadora=(op.id if op else 1))

    if uco_valor_in is not None:
        t_ref.uco_valor = uco_valor_in

    fracao_override = _as_decimal(data.get('fracao_porte'))

    base = CBHPMItem(
        codigo=codigo,
        procedimento=(item.procedimento if item else (data.get('descricao') or '')),
        porte=(item.porte if item else data.get('porte')),
        fracao_porte=(fracao_override if fracao_override is not None else (item.fracao_porte if item else None)),
        valor_porte=(item.valor_porte if item else _as_decimal(data.get('valor_porte'))),
        total_porte=(item.total_porte if item else None),
        filme=(item.filme if item else filme_valor_in),
        incidencias=(item.incidencias if item else incid_in),
        total_filme=(item.total_filme if item else None),
        uco=(item.uco if item else _as_decimal(data.get('uco_qtd'))),
        total_uco=(item.total_uco if item else None),
        porte_anestesico=(item.porte_anestesico if item else data.get('porte_an')),
        valor_porte_anestesico=(item.valor_porte_anestesico if item else _as_decimal(data.get('valor_porte_an'))),
        total_porte_anestesico=(item.total_porte_anestesico if item else None),
        numero_auxiliares=(item.numero_auxiliares if item is not None else data.get('numero_auxiliares')),
        total_auxiliares=(item.total_auxiliares if item else None),
        total_1_aux=(item.total_1_aux if item else _as_decimal(data.get('total_1_aux'))),
        total_2_aux=(item.total_2_aux if item else _as_decimal(data.get('total_2_aux'))),
        total_3_aux=(item.total_3_aux if item else _as_decimal(data.get('total_3_aux'))),
        total_4_aux=(item.total_4_aux if item else _as_decimal(data.get('total_4_aux'))),
    )

    base._fracao_input = fracao_override

    if porte_tab_name:
        base.valor_porte = None
        base.total_porte = None
    if porte_an_tab_name:
        base.valor_porte_anestesico = None
        base.total_porte_anestesico = None
    if filme_valor_in is not None:
        base.filme = filme_valor_in
        base.total_filme = None
    if uco_valor_in is not None:
        base.total_uco = None

    porte_hint = porte_tab_name or (t_ref.nome if t_ref else None)
    porte_an_hint = porte_an_tab_name or (t_ref.nome if t_ref else None)

    if codigos or dtp_items:
        itens = []
        cbhpm_results = []
        teto_alerts: list[dict] = []
        d0 = Decimal('0')

        def to_decimal(value):
            val = _as_decimal(value)
            return val if val is not None else d0

        if codigos:
            for cod in codigos:
                it_item = None
                if versao or cod:
                    qit = (db.session.query(CBHPMItem, Tabela)
                           .join(Tabela, CBHPMItem.id_tabela == Tabela.id))
                    if versao:
                        qit = qit.filter(Tabela.nome == versao)
                    if cod:
                        qit = qit.filter(or_(CBHPMItem.codigo == cod, CBHPMItem.codigo.ilike(f"{cod}%")))
                    if uf:
                        qit = qit.filter(or_(CBHPMItem.uf == uf, Tabela.uf == uf))
                    rowi = qit.first()
                    if rowi:
                        it_item = rowi[0]

                base_i = CBHPMItem(
                    codigo=cod,
                    procedimento=(it_item.procedimento if it_item else ''),
                    porte=(it_item.porte if it_item else None),
                    fracao_porte=(fracao_override if fracao_override is not None else (it_item.fracao_porte if it_item else None)),
                    valor_porte=(it_item.valor_porte if it_item else None),
                    total_porte=(it_item.total_porte if it_item else None),
                    filme=(it_item.filme if it_item else filme_valor_in),
                    incidencias=(it_item.incidencias if it_item else incid_in),
                    total_filme=(it_item.total_filme if it_item else None),
                    uco=(it_item.uco if it_item else None),
                    total_uco=(it_item.total_uco if it_item else None),
                    porte_anestesico=(it_item.porte_anestesico if it_item else None),
                    valor_porte_anestesico=(it_item.valor_porte_anestesico if it_item else None),
                    total_porte_anestesico=(it_item.total_porte_anestesico if it_item else None),
                    numero_auxiliares=(it_item.numero_auxiliares if it_item else None),
                    total_auxiliares=(it_item.total_auxiliares if it_item else None),
                    total_1_aux=(it_item.total_1_aux if it_item else None),
                    total_2_aux=(it_item.total_2_aux if it_item else None),
                    total_3_aux=(it_item.total_3_aux if it_item else None),
                    total_4_aux=(it_item.total_4_aux if it_item else None),
                )
                base_i._fracao_input = fracao_override

                if porte_tab_name:
                    base_i.valor_porte = None
                    base_i.total_porte = None
                if porte_an_tab_name:
                    base_i.valor_porte_anestesico = None
                    base_i.total_porte_anestesico = None
                if filme_valor_in is not None:
                    base_i.filme = filme_valor_in
                    base_i.total_filme = None
                if uco_valor_in is not None:
                    base_i.total_uco = None

                br = compute_cbhpm_breakdown(
                    base_i, t_ref,
                    porte_hint=porte_hint, porte_an_hint=porte_an_hint,
                    ajuste_porte_pct=aj_porte_pct, ajuste_porte_an_pct=aj_an_pct,
                    rules=ruleset_dict
                )
                br = apply_via_entrada(br, cod)
                item_out = {k: _stringify_for_output(v) for k, v in br.items()}
                if br.get('applied_rules'):
                    item_out['applied_rules'] = _stringify_for_output(br['applied_rules'])
                if br.get('total_original') is not None:
                    item_out['total_original'] = _stringify_for_output(br.get('total_original'))
                if br.get('total_final') is not None:
                    item_out['total_final'] = _stringify_for_output(br.get('total_final'))
                item_out['percentual_via'] = _stringify_for_output(br.get('via_entrada_pct')) if br.get('via_entrada_pct') is not None else item_out.get('percentual_via')
                item_out.update({'codigo': cod, 'descricao': base_i.procedimento, 'origem': 'cbhpm'})
                cbhpm_results.append({
                    'payload': item_out,
                    'totals': {
                        'total_porte': to_decimal(br.get('total_porte')),
                        'total_filme': to_decimal(br.get('total_filme')),
                        'total_uco': to_decimal(br.get('total_uco')),
                        'total_porte_an': to_decimal(br.get('total_porte_an')),
                        'total_auxiliares': to_decimal(br.get('total_auxiliares')),
                        'total': to_decimal(br.get('total')),
                        'total_original': to_decimal(br.get('total_original')),
                        'total_final': to_decimal(br.get('total_final')),
                        'via_pct': to_decimal(br.get('via_entrada_pct')),
                    }
                })

        reducoes = (ruleset_dict.get('porte') or {}).get('reducoes_simultaneos') or []
        if reducoes and len(cbhpm_results) > 1:
            ordered = sorted(
                enumerate(cbhpm_results),
                key=lambda pair: pair[1]['totals']['total_porte'],
                reverse=True
            )
            for rank, (idx, entry) in enumerate(ordered):
                original = entry['totals']['total_porte']
                if original <= d0:
                    continue
                factor_raw = reducoes[min(rank, len(reducoes) - 1)]
                try:
                    factor = Decimal(str(factor_raw))
                except (InvalidOperation, ValueError):
                    continue
                if factor > Decimal('5'):
                    factor = factor / Decimal('100')
                if factor > Decimal('1'):
                    factor = Decimal('1')
                if factor < Decimal('0'):
                    factor = Decimal('0')
                adjusted = original * factor
                if adjusted == original:
                    continue
                delta = original - adjusted
                entry['totals']['total_porte'] = adjusted
                entry['totals']['total'] = entry['totals']['total'] - delta
                payload_entry = entry['payload']
                payload_entry['total_porte'] = str(adjusted)
                payload_entry['total'] = str(entry['totals']['total'])
                applied = list(payload_entry.get('applied_rules') or [])
                applied.append({
                    'component': 'porte',
                    'rule': 'reducoes_simultaneos',
                    'ordem': rank + 1,
                    'fator': str(factor),
                    'reduzido_de': str(original),
                    'reduzido_para': str(adjusted),
                })
                payload_entry['applied_rules'] = applied

        if cbhpm_results:
            codes_to_check = [entry['payload'].get('codigo') for entry in cbhpm_results]
            teto_map = _get_teto_map(codes_to_check)
            for entry in cbhpm_results:
                payload_entry = entry['payload']
                codigo_item = (payload_entry.get('codigo') or '').strip().upper()
                if not codigo_item:
                    continue
                teto_row = teto_map.get(codigo_item)
                if not teto_row:
                    continue
                teto_val = _as_decimal(teto_row.valor_total)
                calc_total = entry['totals'].get('total_final') or entry['totals'].get('total')
                if calc_total is None:
                    calc_total = _as_decimal(payload_entry.get('total_final') or payload_entry.get('total'))
                excedido = False
                excedente = None
                if teto_val is not None and calc_total is not None:
                    diff = (calc_total - teto_val).quantize(quantize_money, rounding=ROUND_HALF_UP)
                    if diff > Decimal('0'):
                        excedido = True
                        excedente = diff
                payload_entry['teto_valor_total'] = _stringify_for_output(teto_val) if teto_val is not None else None
                payload_entry['teto_descricao'] = teto_row.descricao
                payload_entry['teto_excedente'] = _stringify_for_output(excedente) if excedente is not None else None
                payload_entry['teto_excedido'] = excedido
                payload_entry['teto_status'] = 'ULTRAPASSA' if excedido else 'OK'
                if excedido:
                    teto_alerts.append({
                        'codigo': payload_entry.get('codigo'),
                        'descricao': payload_entry.get('descricao'),
                        'total_calculado': _stringify_for_output(calc_total),
                        'teto_valor_total': _stringify_for_output(teto_val),
                        'excedente': _stringify_for_output(excedente),
                        'descricao_teto': teto_row.descricao,
                    })

        itens.extend([entry['payload'] for entry in cbhpm_results])

        for meta in dtp_items:
            val = meta.get('valor')
            if val is None:
                val = d0
            elif not isinstance(val, Decimal):
                val = _as_decimal(val) or d0
            itens.append({
                'codigo': meta.get('codigo'),
                'descricao': meta.get('descricao'),
                'total_porte': '0',
                'total_filme': '0',
                'total_uco': '0',
                'total_porte_an': '0',
                'total_auxiliares': '0',
                'total': str(val),
                'total_original': str(val),
                'total_final': str(val),
                'percentual_aplicado': '100',
                'origem': 'dtp',
                'tabela_origem': meta.get('tabela_nome'),
                'uf_origem': meta.get('uf'),
                'auxiliares_detalhe': [],
            })

        sum_porte = sum(to_decimal(item.get('total_porte')) for item in itens)
        sum_filme = sum(to_decimal(item.get('total_filme')) for item in itens)
        sum_uco = sum(to_decimal(item.get('total_uco')) for item in itens)
        sum_an = sum(to_decimal(item.get('total_porte_an')) for item in itens)
        sum_aux = sum(to_decimal(item.get('total_auxiliares')) for item in itens)
        sum_total = sum(to_decimal(item.get('total')) for item in itens)
        sum_total_original = sum(to_decimal(item.get('total_original')) for item in itens)
        sum_total_final = sum(to_decimal(item.get('total_final') or item.get('total')) for item in itens)

        via_out_map = {
            key: str(value)
            for key, value in applied_via_map.items()
            if key != '__default__'
        }

        payload_agregado = {
            'itens': itens,
            'total_porte': str(sum_porte),
            'total_filme': str(sum_filme),
            'total_uco': str(sum_uco),
            'total_porte_an': str(sum_an),
            'total_auxiliares': str(sum_aux),
            'total': str(sum_total_final if sum_total_final is not None else sum_total),
            'total_original': str(sum_total_original),
            'total_final': str(sum_total_final if sum_total_final is not None else sum_total),
            'porte_tabela_usada': (porte_tab_name or _resolve_porte_tabela_nome(t_ref.id_operadora, t_ref.uf, porte_hint, None)),
            'porte_an_tabela_usada': (porte_an_tab_name or _resolve_porte_an_tabela_nome(t_ref.id_operadora, t_ref.uf, porte_an_hint, None)),
            'uco_valor': str(t_ref.uco_valor) if getattr(t_ref, 'uco_valor', None) is not None else None,
            'versao_base': versao,
            'ajuste_porte_pct': str(aj_porte_pct),
            'ajuste_porte_an_pct': str(aj_an_pct),
            'via_entrada_pcts': via_out_map,
            'cbhpm_rules_info': rules_meta,
            'teto_alertas': teto_alerts,
            'teto_status': 'ULTRAPASSA' if teto_alerts else 'OK',
        }
        return payload_agregado, 200

    breakdown = compute_cbhpm_breakdown(
        base, t_ref,
        porte_hint=porte_hint, porte_an_hint=porte_an_hint,
        ajuste_porte_pct=aj_porte_pct, ajuste_porte_an_pct=aj_an_pct,
        rules=ruleset_dict
    )
    breakdown = apply_via_entrada(breakdown, codigo)
    resp = {k: _stringify_for_output(v) for k, v in breakdown.items()}
    resp.update({
        'codigo': codigo,
        'descricao': base.procedimento,
        'uco_valor': str(t_ref.uco_valor) if getattr(t_ref, 'uco_valor', None) is not None else None,
        'versao_base': versao,
        'porte_tabela_usada': (porte_tab_name or _resolve_porte_tabela_nome(t_ref.id_operadora, t_ref.uf, porte_hint, base.porte)),
        'porte_an_tabela_usada': (porte_an_tab_name or _resolve_porte_an_tabela_nome(t_ref.id_operadora, t_ref.uf, porte_an_hint, base.porte_anestesico)),
        'ajuste_porte_pct': str(aj_porte_pct),
        'ajuste_porte_an_pct': str(aj_an_pct),
        'via_entrada_pct': str(breakdown.get('via_entrada_pct')) if breakdown.get('via_entrada_pct') is not None else None,
        'via_entrada_pcts': {
            key: str(value)
            for key, value in applied_via_map.items()
            if key != '__default__'
        },
        'cbhpm_rules_info': rules_meta,
    })

    if codigo:
        teto_row = _get_teto_map([codigo]).get(codigo.strip().upper())
        if teto_row:
            teto_val = _as_decimal(teto_row.valor_total)
            calc_total = _as_decimal(resp.get('total_final') or resp.get('total'))
            excedido = False
            excedente = None
            if teto_val is not None and calc_total is not None:
                diff = (calc_total - teto_val).quantize(quantize_money, rounding=ROUND_HALF_UP)
                if diff > Decimal('0'):
                    excedido = True
                    excedente = diff
            resp['teto_valor_total'] = _stringify_for_output(teto_val) if teto_val is not None else None
            resp['teto_descricao'] = teto_row.descricao
            resp['teto_excedente'] = _stringify_for_output(excedente) if excedente is not None else None
            resp['teto_excedido'] = excedido
            resp['teto_status'] = 'ULTRAPASSA' if excedido else 'OK'
            resp['teto_alertas'] = [{
                'codigo': codigo,
                'descricao': resp.get('descricao'),
                'total_calculado': _stringify_for_output(calc_total) if calc_total is not None else None,
                'teto_valor_total': _stringify_for_output(teto_val) if teto_val is not None else None,
                'excedente': _stringify_for_output(excedente) if excedente is not None else None,
                'descricao_teto': teto_row.descricao,
            }] if excedido else []
        else:
            resp['teto_alertas'] = []
            resp['teto_status'] = 'OK'
    return resp, 200



@app.route('/api/simulacao_cbhpm', methods=['POST'])
@login_required
def api_simulacao_cbhpm():
    data = request.get_json(force=True, silent=True) or {}
    payload, status = _compute_simulacao_cbhpm(data)
    if status == 200:
        restore_payload = {
            'codigo': data.get('codigo') or '',
            'codigos': list(data.get('codigos') or []),
            'dtp_items': list(data.get('dtp_items') or []),
            'uf': data.get('uf') or '',
            'versao': data.get('versao') or '',
            'porte_tab': data.get('porte_tab') or '',
            'porte_an_tab': data.get('porte_an_tab') or '',
            'uco_valor': data.get('uco_valor') or '',
            'filme_valor': data.get('filme_valor') or '',
            'incidencias': data.get('incidencias') or '',
            'via_entrada_pcts': data.get('via_entrada_pcts') or {},
            'via_entrada_pct': data.get('via_entrada_pct') or '',
            'ajuste_porte_pct': data.get('ajuste_porte_pct') or '',
            'ajuste_porte_an_pct': data.get('ajuste_porte_an_pct') or '',
        }
        label_parts = []
        codigo_label = (restore_payload.get('codigo') or '').strip()
        if codigo_label:
            label_parts.append(codigo_label)
        codes_list = restore_payload.get('codigos') or []
        if codes_list:
            snippet = ', '.join([str(c) for c in codes_list[:2]])
            if len(codes_list) > 2:
                snippet += ', ...'
            label_parts.append(f"codigos {snippet}")
        versao_label = (restore_payload.get('versao') or '').strip()
        if versao_label:
            label_parts.append(versao_label)
        label_raw = ' | '.join(filter(None, label_parts)) or 'Simulacao CBHPM'
        label = unicodedata.normalize('NFKD', label_raw).encode('ascii', 'ignore').decode()
        signature_source = json.dumps({'type': 'cbhpm', 'payload': restore_payload}, sort_keys=True).encode('utf-8')
        entry_id = hashlib.md5(signature_source).hexdigest()[:10]
        _store_history_entry({
            'type': 'cbhpm',
            'id': entry_id,
            'signature': f'cbhpm:{entry_id}',
            'url_fragment': f'sim_hist={entry_id}',
            'label': label[:80],
            'timestamp': datetime.now().strftime('%d/%m %H:%M'),
            'payload': restore_payload,
        })
    return jsonify(payload), status


@app.route('/api/simulacao_cbhpm/pdf', methods=['POST'])
@login_required
def export_simulacao_pdf():
    data = request.get_json(force=True, silent=True) or {}
    payload, status = _compute_simulacao_cbhpm(data)
    if status != 200:
        return jsonify(payload), status

    rules_meta = payload.get('cbhpm_rules_info') or {}
    if not rules_meta:
        _, rules_model = _get_active_cbhpm_ruleset(return_model=True)
        rules_meta = {
            'nome': getattr(rules_model, 'nome', 'Padrao'),
            'versao': getattr(rules_model, 'versao', None),
            'descricao': getattr(rules_model, 'descricao', None),
            'id': getattr(rules_model, 'id', None),
        }

    def fmt_brl(value):
        if value in (None, '', 'None'):
            return '-'
        try:
            val = Decimal(str(value))
        except (InvalidOperation, ValueError):
            return str(value)
        formatted = f"{val:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
        return f"R$ {formatted}"

    def fmt_pct(value):
        if value in (None, '', 'None'):
            return '-'
        try:
            val = Decimal(str(value))
        except (InvalidOperation, ValueError):
            return str(value)
        return f"{val:.2f}%".replace('.', ',')

    def fmt_text(value, placeholder='-'):
        if value in (None, '', 'None'):
            return placeholder
        return str(value)

    logo_max_height = 18 * mm

    def load_logo_bytes(cache: dict[str, bytes] = {}) -> bytes | None:

        def _bitmap_to_bytes(path_logo: str):
            try:
                from PIL import Image as PILImage  # type: ignore
            except ImportError:
                return None
            try:
                with PILImage.open(path_logo) as opened:
                    opened.load()
                    if opened.mode not in ('RGB', 'RGBA'):
                        opened = opened.convert('RGBA')
                    if opened.mode == 'RGBA':
                        white_bg = PILImage.new('RGBA', opened.size, (255, 255, 255, 255))
                        opened = PILImage.alpha_composite(white_bg, opened)
                    buffer = io.BytesIO()
                    opened.convert('RGB').save(buffer, format='PNG')
                    return buffer.getvalue()
            except Exception:
                return None

        def _svg_to_bytes(path_logo: str):
            try:
                from svglib.svglib import svg2rlg  # type: ignore
                from reportlab.graphics import renderPM  # type: ignore
            except ImportError:
                return None
            try:
                drawing = svg2rlg(path_logo)
            except Exception:
                return None
            if not drawing or not getattr(drawing, 'height', None):
                return None
            try:
                scale = logo_max_height / float(drawing.height)
                drawing.scale(scale, scale)
                drawing.width = drawing.width * scale
                drawing.height = drawing.height * scale
                return renderPM.drawToString(drawing, fmt='PNG')
            except Exception:
                return None

        def _smart_to_bytes(path_logo: str):
            try:
                with open(path_logo, 'rb') as fh:
                    head = fh.read(256).lstrip()
            except OSError:
                return None
            if head.startswith(b'<svg'):
                return _svg_to_bytes(path_logo)
            return _bitmap_to_bytes(path_logo)

        candidates = [
            ('logo-pdf.svg', _svg_to_bytes),
            ('logo-pdf.png', _smart_to_bytes),
            ('logo-menu.png', _bitmap_to_bytes),
            ('logo-header.png', _bitmap_to_bytes),
            ('logo-login.png', _bitmap_to_bytes),
        ]

        for filename, loader in candidates:
            path_logo = os.path.join(app.root_path, 'static', filename)
            if not os.path.exists(path_logo):
                continue

            cache_key = os.path.abspath(path_logo)
            if cache_key not in cache:
                cache[cache_key] = loader(path_logo) or b''

            data = cache.get(cache_key) or b''
            if data:
                setattr(load_logo_bytes, 'last_static_name', filename)
                return data

        return None

    generated_at = datetime.now().strftime('%d/%m/%Y %H:%M')
    logo_bytes = load_logo_bytes()
    logo_static_name = getattr(load_logo_bytes, 'last_static_name', None)
    logo_static_path = f'static/{logo_static_name}' if logo_static_name else None
    logo_uri = None
    if logo_bytes:
        import base64
        logo_uri = f"data:image/png;base64,{base64.b64encode(logo_bytes).decode('ascii')}"

    meta_rows = []
    meta_rows.append({'label': 'UF', 'value': fmt_text(data.get('uf') or 'Todos')})
    meta_rows.append({'label': 'Versão referência', 'value': fmt_text(payload.get('versao_base') or data.get('versao') or '-')})
    meta_rows.append({'label': 'Tabela de Porte', 'value': fmt_text(payload.get('porte_tabela_usada'))})
    meta_rows.append({'label': 'Tabela Porte AN', 'value': fmt_text(payload.get('porte_an_tabela_usada'))})
    meta_rows.append({'label': 'Ajuste Porte %', 'value': fmt_pct(payload.get('ajuste_porte_pct'))})
    meta_rows.append({'label': 'Ajuste Porte AN %', 'value': fmt_pct(payload.get('ajuste_porte_an_pct'))})
    meta_rows.append({'label': 'Valor UCO', 'value': fmt_brl(payload.get('uco_valor'))})
    meta_rows.append({'label': 'Incidências', 'value': fmt_text(data.get('incidencias') or '-')})

    requested_codes = []
    if isinstance(data.get('codigos'), list):
        requested_codes.extend([str(c).strip() for c in data.get('codigos') if c])
    if data.get('codigo'):
        requested_codes.append(str(data.get('codigo')).strip())
    codes_from_payload = []
    if isinstance(payload.get('itens'), list):
        codes_from_payload.extend([it.get('codigo') for it in payload['itens'] if it.get('codigo')])
    if payload.get('codigo'):
        codes_from_payload.append(payload.get('codigo'))
    merged_codes = []
    for code in requested_codes + codes_from_payload:
        if code and code not in merged_codes:
            merged_codes.append(code)

    if payload.get('descricao'):
        meta_rows.append({'label': 'Procedimento base', 'value': fmt_text(payload.get('descricao'))})
    if payload.get('itens'):
        meta_rows.append({'label': 'Quantidade de itens', 'value': str(len(payload['itens']))})

    limit = 12
    codes_preview = ', '.join(merged_codes[:limit]) if merged_codes else ''
    codes_extra = max(len(merged_codes) - limit, 0) if merged_codes else 0

    itens = payload.get('itens') or []
    itens_rows = []
    for item in itens:
        aux_raw = item.get('auxiliares_detalhe') or []
        aux_detail = []
        for det in aux_raw:
            aux_detail.append({
                'indice': det.get('indice'),
                'percentual': fmt_pct(det.get('percentual_pct')),
                'valor': fmt_brl(det.get('valor')),
            })
        itens_rows.append({
            'codigo': fmt_text(item.get('codigo')),
            'descricao': fmt_text(item.get('descricao')),
            'total_porte': fmt_brl(item.get('total_porte')),
            'total_filme': fmt_brl(item.get('total_filme')),
            'total_uco': fmt_brl(item.get('total_uco')),
            'total_porte_an': fmt_brl(item.get('total_porte_an')),
            'total_auxiliares': fmt_brl(item.get('total_auxiliares')),
            'total': fmt_brl(item.get('total')),
            'auxiliares_detalhe': aux_detail,
            'auxiliares_qtd': len(aux_detail),
        })

    fallback = {
        'codigo': fmt_text(payload.get('codigo') or data.get('codigo') or '-'),
        'descricao': fmt_text(payload.get('descricao') or ''),
    }

    summary_labels = [
        ('Total Porte', fmt_brl(payload.get('total_porte'))),
        ('Total Filme', fmt_brl(payload.get('total_filme'))),
        ('Total UCO', fmt_brl(payload.get('total_uco'))),
        ('Total Porte AN', fmt_brl(payload.get('total_porte_an'))),
        ('Total Auxiliares', fmt_brl(payload.get('total_auxiliares'))),
        ('Total Geral', fmt_brl(payload.get('total'))),
    ]

    summary_rows = [
        {
            'label': label,
            'value': value,
            'highlight': False,
        }
        for label, value in summary_labels
    ]
    if summary_rows:
        summary_rows[-1]['highlight'] = True

    context = {
        'generated_at': generated_at,
        'logo_data_uri': logo_uri,
        'logo_height_mm': 18,
        'logo_static_path': logo_static_path,
        'meta_rows': meta_rows,
        'codes_preview': codes_preview,
        'codes_extra': codes_extra,
        'itens': itens_rows,
        'fallback': fallback,
        'summary_rows': summary_rows,
        'logo_bytes': logo_bytes,
    }
    context['cbhpm_rules_info'] = rules_meta

    html_output = render_template('simulacao_cbhpm_pdf.html', **context)

    def render_reportlab_pdf(ctx: dict) -> bytes:
        buffer_rl = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer_rl,
            pagesize=A4,
            leftMargin=25 * mm,
            rightMargin=25 * mm,
            topMargin=30 * mm,
            bottomMargin=20 * mm,
        )

        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            'PDFTitle',
            parent=styles['Title'],
            fontSize=18,
            leading=22,
            textColor=colors.HexColor('#0f172a'),
            alignment=2,
        )
        header_info_style = ParagraphStyle(
            'PDFHeaderInfo',
            parent=styles['Normal'],
            fontSize=9,
            textColor=colors.HexColor('#475569'),
            alignment=2,
        )
        meta_label_style = ParagraphStyle(
            'MetaLabel', parent=styles['Normal'], fontSize=9, textColor=colors.HexColor('#475569'), leading=12
        )
        meta_value_style = ParagraphStyle(
            'MetaValue', parent=styles['Normal'], fontSize=10, textColor=colors.HexColor('#111827'), leading=12
        )
        table_header_style = ParagraphStyle(
            'TableHeader', parent=styles['Normal'], alignment=1, fontSize=10, textColor=colors.white, leading=12
        )
        table_text_style = ParagraphStyle(
            'TableText', parent=styles['Normal'], fontSize=9, textColor=colors.HexColor('#111827'), leading=12
        )

        story = []
        logo_img = None
        if ctx.get('logo_bytes'):
            try:
                logo_img = Image(io.BytesIO(ctx['logo_bytes']))
                scale = logo_max_height / logo_img.imageHeight
                logo_img.drawHeight = logo_max_height
                logo_img.drawWidth = logo_img.imageWidth * scale
                logo_img.hAlign = 'LEFT'
            except Exception:
                logo_img = None

        header_title = Paragraph('<b>Relatório de Simulação CBHPM</b>', title_style)
        header_info = Paragraph(f"Gerado em {html.escape(ctx['generated_at'])}", header_info_style)
        if logo_img:
            logo_width = logo_img.drawWidth + 6
            col_widths = [logo_width, doc.width - logo_width]
        else:
            col_widths = [doc.width * 0.25, doc.width * 0.75]
        header_data = [
            [logo_img if logo_img else '', header_title],
            ['', header_info],
        ]
        header_table = Table(header_data, colWidths=col_widths)
        header_table.setStyle(
            TableStyle([
                ('SPAN', (0, 0), (0, 1)),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
                ('ALIGN', (1, 1), (1, 1), 'RIGHT'),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('TOPPADDING', (0, 0), (-1, -1), 2),
            ])
        )
        story.append(header_table)
        story.append(Spacer(0, 16))

        meta_rows_ctx = ctx.get('meta_rows') or []
        if meta_rows_ctx:
            rows = []
            for i in range(0, len(meta_rows_ctx), 2):
                left = meta_rows_ctx[i]
                right = meta_rows_ctx[i + 1] if i + 1 < len(meta_rows_ctx) else {'label': '', 'value': ''}
                rows.append([
                    Paragraph(f"<b>{html.escape(str(left['label']))}</b>", meta_label_style),
                    Paragraph(html.escape(str(left['value'])), meta_value_style),
                    Paragraph(f"<b>{html.escape(str(right['label']))}</b>", meta_label_style) if right['label'] else '',
                    Paragraph(html.escape(str(right['value'])), meta_value_style) if right['label'] else '',
                ])
            meta_table = Table(rows, colWidths=[doc.width * 0.18, doc.width * 0.32, doc.width * 0.18, doc.width * 0.32])
            meta_table.setStyle(
                TableStyle([
                    ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#f8fafc')),
                    ('BOX', (0, 0), (-1, -1), 0.5, colors.HexColor('#cbd5e1')),
                    ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.HexColor('#e2e8f0')),
                    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                    ('LEFTPADDING', (0, 0), (-1, -1), 6),
                    ('RIGHTPADDING', (0, 0), (-1, -1), 6),
                    ('TOPPADDING', (0, 0), (-1, -1), 4),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
                ])
            )
            story.append(meta_table)
            story.append(Spacer(0, 12))

        if ctx.get('codes_preview'):
            codes_text = f"<b>Códigos selecionados:</b> {html.escape(ctx['codes_preview'])}"
            if ctx.get('codes_extra'):
                codes_text += f"<br/><font size=9 color='#475569'>+{ctx['codes_extra']} código(s) adicional(is) não exibido(s).</font>"
            story.append(Paragraph(codes_text, meta_value_style))
            story.append(Spacer(0, 10))

        itens_ctx = ctx.get('itens') or []
        if itens_ctx:
            table_data = [[
                Paragraph('<b>Código</b>', table_header_style),
                Paragraph('<b>Descrição</b>', table_header_style),
                Paragraph('<b>Total Porte</b>', table_header_style),
                Paragraph('<b>Total Filme</b>', table_header_style),
                Paragraph('<b>Total UCO</b>', table_header_style),
                Paragraph('<b>Total Porte AN</b>', table_header_style),
                Paragraph('<b>Auxiliares</b>', table_header_style),
                Paragraph('<b>Total</b>', table_header_style),
            ]]
            for item in itens_ctx:
                table_data.append([
                    Paragraph(html.escape(item['codigo']), table_text_style),
                    Paragraph(html.escape(item['descricao']), table_text_style),
                    Paragraph(item['total_porte'], table_text_style),
                    Paragraph(item['total_filme'], table_text_style),
                    Paragraph(item['total_uco'], table_text_style),
                    Paragraph(item['total_porte_an'], table_text_style),
                    Paragraph(item['total_auxiliares'], table_text_style),
                    Paragraph(item['total'], table_text_style),
                ])
            col_widths = [
                doc.width * 0.1,
                doc.width * 0.4,
                doc.width * 0.1,
                doc.width * 0.1,
                doc.width * 0.1,
                doc.width * 0.1,
                doc.width * 0.1,
                doc.width * 0.0 + 40,
            ]
            resultados_table = Table(table_data, colWidths=col_widths, repeatRows=1)
            resultados_table.setStyle(
                TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0f766e')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                    ('ALIGN', (2, 1), (-1, -1), 'RIGHT'),
                    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f1f5f9')]),
                    ('GRID', (0, 0), (-1, -1), 0.25, colors.HexColor('#cbd5e1')),
                    ('FONTSIZE', (0, 0), (-1, -1), 9),
                    ('TOPPADDING', (0, 0), (-1, 0), 6),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
                ])
            )
            story.append(resultados_table)
            story.append(Spacer(0, 12))
        else:
            fallback_ctx = ctx.get('fallback') or {}
            story.append(Paragraph(
                f"<b>Código:</b> {html.escape(fallback_ctx.get('codigo', '-'))}",
                meta_value_style,
            ))
            if fallback_ctx.get('descricao'):
                story.append(Paragraph(f"<b>Descrição:</b> {html.escape(fallback_ctx['descricao'])}", meta_value_style))
            story.append(Spacer(0, 10))

        summary_rows_ctx = ctx.get('summary_rows') or []
        if summary_rows_ctx:
            rows = []
            highlight_map = []
            for i in range(0, len(summary_rows_ctx), 2):
                left = summary_rows_ctx[i]
                right = summary_rows_ctx[i + 1] if i + 1 < len(summary_rows_ctx) else {'label': '', 'value': '', 'highlight': False}
                highlight_map.append(bool(left.get('highlight') or right.get('highlight')))
                rows.append([
                    Paragraph(f"<b>{html.escape(left['label'])}</b>", meta_label_style),
                    Paragraph(html.escape(left['value']), meta_value_style),
                    Paragraph(f"<b>{html.escape(right['label'])}</b>", meta_label_style) if right['label'] else '',
                    Paragraph(html.escape(right['value']), meta_value_style) if right['label'] else '',
                ])
            summary_table = Table(rows, colWidths=[doc.width * 0.2, doc.width * 0.3, doc.width * 0.2, doc.width * 0.3])
            summary_table.setStyle(
                TableStyle([
                    ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#ecfeff')),
                    ('BOX', (0, 0), (-1, -1), 0.6, colors.HexColor('#0891b2')),
                    ('INNERGRID', (0, 0), (-1, -1), 0.3, colors.HexColor('#bae6fd')),
                    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                    ('LEFTPADDING', (0, 0), (-1, -1), 6),
                    ('RIGHTPADDING', (0, 0), (-1, -1), 6),
                    ('TOPPADDING', (0, 0), (-1, -1), 4),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
                ])
            )
            for idx, highlight in enumerate(highlight_map):
                if highlight:
                    summary_table.setStyle(
                        TableStyle([
                            ('BACKGROUND', (0, idx), (-1, idx), colors.HexColor('#0f766e')),
                            ('TEXTCOLOR', (0, idx), (-1, idx), colors.white),
                            ('FONTNAME', (0, idx), (-1, idx), 'Helvetica-Bold'),
                        ])
                    )
            story.append(summary_table)

        def _footer(canvas_obj, doc_obj):
            canvas_obj.saveState()
            canvas_obj.setStrokeColor(colors.HexColor('#cbd5e1'))
            canvas_obj.setLineWidth(0.5)
            canvas_obj.line(doc_obj.leftMargin, 15, doc_obj.leftMargin + doc_obj.width, 15)
            canvas_obj.setFont('Helvetica', 9)
            canvas_obj.setFillColor(colors.HexColor('#475569'))
            canvas_obj.drawString(doc_obj.leftMargin, 5, f"Sistema de Simulação • Página {doc_obj.page}")
            canvas_obj.restoreState()

        doc.build(story, onFirstPage=_footer, onLaterPages=_footer)
        buffer_rl.seek(0)
        return buffer_rl.getvalue()

    try:
        from weasyprint import HTML  # type: ignore

        pdf_bytes = HTML(string=html_output, base_url=app.root_path).write_pdf()
    except Exception as exc:
        app.logger.warning('WeasyPrint indisponível (%s); usando ReportLab fallback.', exc)
        pdf_bytes = render_reportlab_pdf(context)

    buffer = io.BytesIO(pdf_bytes)
    buffer.seek(0)
    return send_file(buffer, mimetype='application/pdf', as_attachment=True, download_name='simulacao.pdf')


@app.route('/api/simulacao_cbhpm/xlsx', methods=['POST'])
@login_required
def export_simulacao_xlsx():
    data = request.get_json(force=True, silent=True) or {}
    payload, status = _compute_simulacao_cbhpm(data)
    if status != 200:
        return jsonify(payload), status

    def to_number(value):
        try:
            return float(value)
        except (TypeError, ValueError, InvalidOperation):
            return 0.0

    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet("Simulacao")

    bold = workbook.add_format({'bold': True})
    money = workbook.add_format({'num_format': 'R$ #,##0.00'})

    headers = [
        "Codigo",
        "Descricao",
        "Total Porte",
        "Total Filme",
        "Total UCO",
        "Total Porte AN",
        "Total Auxiliares",
        "Via Entrada",
        "Total",
        "Teto",
        "Excedente",
    ]
    for col, header in enumerate(headers):
        worksheet.write(0, col, header, bold)

    row_idx = 1
    itens = payload.get('itens') or []
    if itens:
        for item in itens:
            worksheet.write(row_idx, 0, item.get('codigo'))
            worksheet.write(row_idx, 1, item.get('descricao') or '')
            worksheet.write_number(row_idx, 2, to_number(item.get('total_porte')), money)
            worksheet.write_number(row_idx, 3, to_number(item.get('total_filme')), money)
            worksheet.write_number(row_idx, 4, to_number(item.get('total_uco')), money)
            worksheet.write_number(row_idx, 5, to_number(item.get('total_porte_an')), money)
            worksheet.write_number(row_idx, 6, to_number(item.get('total_auxiliares')), money)
            worksheet.write(row_idx, 7, item.get('via_entrada_pct') or '-')
            worksheet.write_number(row_idx, 8, to_number(item.get('total')), money)
            teto_value = item.get('teto_valor_total')
            excedente_value = item.get('teto_excedente')
            if teto_value not in (None, '', 'None'):
                worksheet.write_number(row_idx, 9, to_number(teto_value), money)
            else:
                worksheet.write_blank(row_idx, 9, None)
            if excedente_value not in (None, '', 'None'):
                worksheet.write_number(row_idx, 10, to_number(excedente_value), money)
            else:
                worksheet.write_blank(row_idx, 10, None)
            row_idx += 1
    else:
        worksheet.write(row_idx, 0, payload.get('codigo'))
        worksheet.write(row_idx, 1, payload.get('descricao') or '')
        worksheet.write_number(row_idx, 2, to_number(payload.get('total_porte')), money)
        worksheet.write_number(row_idx, 3, to_number(payload.get('total_filme')), money)
        worksheet.write_number(row_idx, 4, to_number(payload.get('total_uco')), money)
        worksheet.write_number(row_idx, 5, to_number(payload.get('total_porte_an')), money)
        worksheet.write_number(row_idx, 6, to_number(payload.get('total_auxiliares')), money)
        worksheet.write(row_idx, 7, payload.get('via_entrada_pct') or (payload.get('via_entrada_summary') or '-'))
        worksheet.write_number(row_idx, 8, to_number(payload.get('total')), money)
        teto_value = payload.get('teto_valor_total')
        excedente_value = payload.get('teto_excedente')
        if teto_value not in (None, '', 'None'):
            worksheet.write_number(row_idx, 9, to_number(teto_value), money)
        if excedente_value not in (None, '', 'None'):
            worksheet.write_number(row_idx, 10, to_number(excedente_value), money)
        row_idx += 1

    worksheet.write(row_idx + 1, 7, 'TOTAL GERAL', bold)
    worksheet.write_number(row_idx + 1, 8, to_number(payload.get('total')), money)

    workbook.close()
    output.seek(0)

    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name='simulacao.xlsx'
    )

@app.route('/api/simulacao_dtp')
@login_required
def api_simulacao_dtp():
    """Pesquisa itens em 'Diárias, Taxas e Pacotes' por tabela e termo (código ou descrição).
    Parâmetros: tabela_nome (obrig.), q (código ou parte da descrição), uf (opcional)
    """
    tabela_nome = request.args.get('tabela_nome') or ''
    q = (request.args.get('q') or '').strip()
    uf = (request.args.get('uf') or '').strip() or None
    if not tabela_nome:
        return jsonify({'itens': [], 'total': '0'})
    t = Tabela.query.filter_by(nome=tabela_nome, tipo_tabela='diarias_taxas_pacotes').first()
    if not t:
        return jsonify({'itens': [], 'total': '0'})
    query = db.session.query(Procedimento).filter(Procedimento.id_tabela == t.id)
    if uf:
        query = query.filter(or_(Procedimento.uf == uf, t.uf == uf))
    if q:
        like = f"%{q}%"
        query = query.filter(or_(Procedimento.codigo == q, Procedimento.codigo.ilike(f"{q}%"), Procedimento.descricao.ilike(like)))
    rows = query.order_by(Procedimento.codigo).limit(200).all()
    itens = [{'codigo': r.codigo, 'descricao': r.descricao, 'valor': (str(r.valor) if r.valor is not None else None)} for r in rows]
    total = sum([_as_decimal(r.valor) or Decimal('0') for r in rows])
    return jsonify({'itens': itens, 'total': str(total)})


@app.route('/api/prestadores_por_codigo')
@login_required
def api_prestadores_por_codigo():
    """Retorna a lista de prestadores que possuem o código informado
    dentro da tabela selecionada e UF opcional.
    Parâmetros: tabela_nome, codigo, uf (opcional)
    """
    tabela_nome = request.args.get('tabela_nome')
    codigo = (request.args.get('codigo') or '').strip()
    uf = request.args.get('uf')
    if not tabela_nome or not codigo:
        return jsonify([])

    # Extrai somente o código caso venha no formato "codigo - descricao"
    if ' - ' in codigo:
        codigo = codigo.split(' - ', 1)[0].strip()

    q = db.session.query(Procedimento.prestador).join(Tabela, Procedimento.id_tabela == Tabela.id)
    q = q.filter(Tabela.nome == tabela_nome)
    if uf:
        q = q.filter(or_(Tabela.uf == uf, Procedimento.uf == uf))
    # Match por igualdade ou prefixo
    q = q.filter(or_(Procedimento.codigo == codigo, Procedimento.codigo.ilike(f"{codigo}%")))
    q = q.filter((Procedimento.prestador.isnot(None)) & (Procedimento.prestador != ''))
    prestadores = [r[0] for r in q.distinct().order_by(Procedimento.prestador).all()]
    return jsonify(prestadores)


@app.route('/api/versoes_por_codigo')
@login_required
def api_versoes_por_codigo():
    tabela_nome = request.args.get('tabela_nome')
    codigo = (request.args.get('codigo') or '').strip()
    uf = request.args.get('uf')
    if not codigo:
        return jsonify([])
    if ' - ' in codigo:
        codigo = codigo.split(' - ', 1)[0].strip()
    qv = db.session.query(Tabela.nome).join(CBHPMItem, CBHPMItem.id_tabela == Tabela.id).filter(Tabela.tipo_tabela == 'cbhpm')
    if uf:
        qv = qv.filter(or_(Tabela.uf == uf, CBHPMItem.uf == uf))
    qv = qv.filter(or_(CBHPMItem.codigo == codigo, CBHPMItem.codigo.ilike(f"{codigo}%")))
    versoes = [r[0] for r in qv.distinct().order_by(Tabela.nome).all()]
    return jsonify(versoes)


@app.route('/gerenciar-usuarios')
@admin_required
def gerenciar_usuarios():
    usuarios = Usuario.query.all()
    return render_template('gerenciar-usuarios.html', usuarios=usuarios)


@app.route('/gerenciar-operadoras')
@admin_required
def gerenciar_operadoras():
    operadoras = Operadora.query.all()
    return render_template('gerenciar-operadoras.html', operadoras=operadoras)


@app.route('/gerenciar-tabelas')
@admin_required
def gerenciar_tabelas():
    tabelas = Tabela.query.all()
    operadoras = Operadora.query.all()
    cbhpm_tabelas = Tabela.query.filter_by(tipo_tabela='cbhpm').order_by(Tabela.nome).all()
    return render_template('gerenciar-tabelas.html', tabelas=tabelas, operadoras=operadoras, UFS=BR_UFS, cbhpm_tabelas=cbhpm_tabelas)


@app.route('/admin/tetos')
@admin_required
def admin_tetos():
    per_page = 25
    try:
        page = int(request.args.get('page', 1) or 1)
    except (TypeError, ValueError):
        page = 1
    page = max(page, 1)
    search = (request.args.get('q') or '').strip()
    query = CbhpmTeto.query
    if search:
        like = f"%{search}%"
        query = query.filter(or_(CbhpmTeto.codigo.ilike(like), CbhpmTeto.descricao.ilike(like)))
    total = query.count()
    tetos = (
        query.order_by(CbhpmTeto.codigo.asc())
        .offset((page - 1) * per_page)
        .limit(per_page)
        .all()
    )
    pages = max((total + per_page - 1) // per_page, 1) if total else 1

    preview_token = (request.args.get('preview_token') or '').strip()
    preview_payload = None
    if preview_token:
        preview_payload = _load_teto_preview(preview_token)
        if not preview_payload:
            flash('Pré-visualização expirada ou inválida. Envie o arquivo novamente.', 'warning')
            return redirect(url_for('admin_tetos'))

    return render_template(
        'admin_tetos.html',
        tetos=tetos,
        total=total,
        page=page,
        per_page=per_page,
        pages=pages,
        q=search,
        preview=preview_payload,
        format_brl=_format_brl,
    )


@app.route('/admin/tetos/import', methods=['POST'])
@admin_required
def admin_tetos_import():
    confirm_token = (request.form.get('token') or '').strip()
    if confirm_token:
        preview_payload = _load_teto_preview(confirm_token)
        if not preview_payload or not preview_payload.get('rows'):
            _discard_teto_preview(confirm_token)
            flash('Pré-visualização expirada ou vazia. Envie o arquivo novamente.', 'warning')
            return redirect(url_for('admin_tetos'))
        rows = [row for row in preview_payload['rows'] if row.get('codigo') and row.get('valor_total') is not None]
        codes = [row['codigo'] for row in rows]
        existing: set[str] = set()
        if codes:
            existing = {c for (c,) in db.session.query(CbhpmTeto.codigo).filter(CbhpmTeto.codigo.in_(codes)).all()}
        if rows:
            insert_rows = [
                {
                    'codigo': row['codigo'],
                    'descricao': row['descricao'],
                    'valor_total': row['valor_total'],
                }
                for row in rows
            ]
            stmt = mysql_insert(CbhpmTeto.__table__).values(insert_rows)
            upsert_stmt = stmt.on_duplicate_key_update(
                descricao=stmt.inserted.descricao,
                valor_total=stmt.inserted.valor_total,
                updated_at=text('CURRENT_TIMESTAMP'),
            )
            db.session.execute(upsert_stmt)
            db.session.commit()
        inserted = len([code for code in codes if code not in existing])
        updated = len(codes) - inserted
        error_count = len(preview_payload.get('errors', []))
        app.logger.info(
            'Importação CBHPM teto concluída: total=%s, inseridos=%s, atualizados=%s, erros=%s',
            len(codes), inserted, updated, error_count
        )
        flash(f'Importação concluída: {len(codes)} registro(s), {inserted} inserido(s), {updated} atualizado(s).', 'success')
        _discard_teto_preview(confirm_token)
        return redirect(url_for('admin_tetos'))

    upload = request.files.get('arquivo')
    if not upload or not upload.filename:
        flash('Selecione um arquivo CSV ou XLSX para importar.', 'danger')
        return redirect(url_for('admin_tetos'))

    suffix = Path(upload.filename).suffix or '.csv'
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        upload.save(tmp)
        temp_path = Path(tmp.name)
    try:
        parsed = _parse_teto_import_file(temp_path)
    finally:
        temp_path.unlink(missing_ok=True)

    if not parsed['rows']:
        if parsed['errors']:
            for message in parsed['errors'][:10]:
                flash(message, 'warning')
        flash('Nenhum registro válido encontrado no arquivo.', 'warning')
        return redirect(url_for('admin_tetos'))

    meta = {
        'filename': upload.filename,
        'total_input': parsed['total_input'],
        'valid_count': parsed['valid_count'],
        'duplicate_count': parsed['duplicate_count'],
        'error_count': len(parsed['errors']),
        'generated_at': datetime.utcnow().isoformat(),
    }
    token = _store_teto_preview({'rows': parsed['rows'], 'meta': meta, 'errors': parsed['errors']})
    if parsed['errors']:
        flash(f"Pré-visualização gerada com {parsed['valid_count']} registro(s) válido(s) e {len(parsed['errors'])} aviso(s).", 'warning')
    else:
        flash(f"{parsed['valid_count']} registro(s) prontos para importação.", 'success')
    return redirect(url_for('admin_tetos', preview_token=token))


@app.route('/admin/tetos/template.csv')
@admin_required
def admin_tetos_template_download():
    output = io.StringIO()
    writer = csv.writer(output, delimiter=';')
    writer.writerow(['codigo', 'descricao', 'valor_total'])
    writer.writerow(['12345', 'Procedimento exemplo', '1234,56'])
    response = make_response(output.getvalue())
    response.headers['Content-Type'] = 'text/csv; charset=utf-8'
    response.headers['Content-Disposition'] = 'attachment; filename=teto_template.csv'
    return response


@app.route('/admin/tetos/<codigo>/delete', methods=['POST'])
@admin_required
def admin_tetos_delete(codigo: str):
    codigo_norm = (codigo or '').strip().upper()
    if not codigo_norm:
        flash('Código inválido.', 'danger')
        return redirect(url_for('admin_tetos'))
    row = CbhpmTeto.query.get(codigo_norm)
    if not row:
        flash('Registro não encontrado.', 'warning')
        return redirect(url_for('admin_tetos'))
    db.session.delete(row)
    db.session.commit()
    flash(f'Teto {codigo_norm} removido com sucesso.', 'success')
    return redirect(url_for('admin_tetos'))


@app.route('/cbhpm/regras')
@admin_required
def cbhpm_rules():
    status = request.args.get('status')
    rulesets = (
        CBHPMRuleSet.query
        .order_by(CBHPMRuleSet.ativo.desc(), CBHPMRuleSet.atualizado_em.desc())
        .all()
    )
    return render_template(
        'cbhpm_rules_list.html',
        rulesets=rulesets,
        status=status,
        default_rules=DEFAULT_CBHPM_RULES
    )


@app.route('/cbhpm/regras/nova', methods=['GET', 'POST'])
@admin_required
def cbhpm_rules_new():
    default_rules = _clone_default_cbhpm_rules()
    error = None
    form_data = {
        'nome': '',
        'versao': '',
        'descricao': '',
        'ativo': False,
        'regras': json.dumps(default_rules, indent=2, ensure_ascii=False),
        'regras_dict': default_rules
    }
    if request.method == 'POST':
        nome = (request.form.get('nome') or '').strip()
        versao = (request.form.get('versao') or '').strip()
        descricao = (request.form.get('descricao') or '').strip()
        ativo = request.form.get('ativo') == 'on'
        regras_raw = (request.form.get('regras') or '').strip()
        parsed_rules = None
        try:
            parsed_rules = json.loads(regras_raw or '{}')
            if not isinstance(parsed_rules, dict):
                raise ValueError('Estrutura deve ser um objeto JSON.')
        except Exception as exc:
            error = f'JSON invalido: {exc}'
        regras_json = parsed_rules if isinstance(parsed_rules, dict) else {}
        if not nome:
            error = 'Informe um nome para a regra.'
        if not error:
            try:
                if ativo:
                    CBHPMRuleSet.query.filter(CBHPMRuleSet.ativo.is_(True)).update({'ativo': False}, synchronize_session=False)
                ruleset = CBHPMRuleSet(
                    nome=nome,
                    versao=versao or None,
                    descricao=descricao or None,
                    ativo=ativo,
                    regras=regras_json
                )
                db.session.add(ruleset)
                db.session.commit()
                return redirect(url_for('cbhpm_rules', status='created'))
            except Exception as exc:
                db.session.rollback()
                error = f'Erro ao gravar: {exc}'
        form_data.update({
            'nome': nome,
            'versao': versao,
            'descricao': descricao,
            'ativo': ativo,
            'regras': regras_raw or '',
            'regras_dict': regras_json if isinstance(parsed_rules, dict) else form_data.get('regras_dict')
        })
    return render_template('cbhpm_rules_form.html', ruleset=None, form_data=form_data, error=error)


@app.route('/cbhpm/regras/<int:ruleset_id>/editar', methods=['GET', 'POST'])
@admin_required
def cbhpm_rules_edit(ruleset_id: int):
    ruleset = CBHPMRuleSet.query.get_or_404(ruleset_id)
    error = None
    if request.method == 'POST':
        nome = (request.form.get('nome') or '').strip()
        versao = (request.form.get('versao') or '').strip()
        descricao = (request.form.get('descricao') or '').strip()
        ativo = request.form.get('ativo') == 'on'
        regras_raw = (request.form.get('regras') or '').strip()
        parsed_rules = None
        try:
            parsed_rules = json.loads(regras_raw or '{}')
            if not isinstance(parsed_rules, dict):
                raise ValueError('Estrutura deve ser um objeto JSON.')
        except Exception as exc:
            error = f'JSON invalido: {exc}'
        regras_json = parsed_rules if isinstance(parsed_rules, dict) else {}
        if not nome:
            error = 'Informe um nome para a regra.'
        if not error:
            try:
                ruleset.nome = nome
                ruleset.versao = versao or None
                ruleset.descricao = descricao or None
                ruleset.regras = regras_json
                if ativo:
                    CBHPMRuleSet.query.filter(
                        CBHPMRuleSet.id != ruleset.id,
                        CBHPMRuleSet.ativo.is_(True)
                    ).update({'ativo': False}, synchronize_session=False)
                ruleset.ativo = ativo
                db.session.commit()
                return redirect(url_for('cbhpm_rules', status='updated'))
            except Exception as exc:
                db.session.rollback()
                error = f'Erro ao gravar: {exc}'
        form_data = {
            'nome': nome,
            'versao': versao,
            'descricao': descricao,
            'ativo': ativo,
            'regras': regras_raw or '',
            'regras_dict': regras_json if isinstance(parsed_rules, dict) else (ruleset.regras if isinstance(ruleset.regras, dict) else {})
        }
        return render_template('cbhpm_rules_form.html', ruleset=ruleset, form_data=form_data, error=error)
    current_rules = ruleset.regras if isinstance(ruleset.regras, dict) else {}
    form_data = {
        'nome': ruleset.nome,
        'versao': ruleset.versao or '',
        'descricao': ruleset.descricao or '',
        'ativo': bool(ruleset.ativo),
        'regras': json.dumps(current_rules, indent=2, ensure_ascii=False),
        'regras_dict': current_rules
    }
    return render_template('cbhpm_rules_form.html', ruleset=ruleset, form_data=form_data, error=error)


@app.route('/cbhpm/regras/<int:ruleset_id>/ativar', methods=['POST'])
@admin_required
def cbhpm_rules_activate(ruleset_id: int):
    try:
        ruleset = CBHPMRuleSet.query.get_or_404(ruleset_id)
        CBHPMRuleSet.query.filter(
            CBHPMRuleSet.id != ruleset.id,
            CBHPMRuleSet.ativo.is_(True)
        ).update({'ativo': False}, synchronize_session=False)
        ruleset.ativo = True
        db.session.commit()
    except Exception:
        db.session.rollback()
    return redirect(url_for('cbhpm_rules', status='activated'))


# --- 4. APIs básicas (CRUD mínimo) ---

# Operadoras
@app.route('/api/operadoras', methods=['GET', 'POST'])
@admin_required
def api_operadoras():
    if request.method == 'GET':
        data = [
            {"id": o.id, "nome": o.nome, "cnpj": o.cnpj, "status": o.status}
            for o in Operadora.query.all()
        ]
        return jsonify(data)
    payload = request.json or {}
    o = Operadora(nome=payload.get('nome'), cnpj=payload.get('cnpj'), status=payload.get('status', 'Ativa'))
    db.session.add(o)
    db.session.commit()
    return jsonify({"id": o.id}), 201


@app.route('/api/operadoras/<int:oid>', methods=['PUT', 'DELETE'])
@admin_required
def api_operadora_item(oid):
    o = Operadora.query.get_or_404(oid)
    if request.method == 'PUT':
        payload = request.json or {}
        o.nome = payload.get('nome', o.nome)
        o.cnpj = payload.get('cnpj', o.cnpj)
        o.status = payload.get('status', o.status)
        db.session.commit()
        return jsonify({"ok": True})
    db.session.delete(o)
    db.session.commit()
    return jsonify({"ok": True})


# --- 5. Inicialização ---
def ensure_db(max_retries: int = 20, delay_seconds: int = 3):
    """Cria as tabelas com tentativas/retry para aguardar o MySQL.
    Útil quando o container web inicia antes do banco estar pronto.
    """
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            with app.app_context():
                db.create_all()
                # Tentativa de migração leve para acrescentar colunas caso já exista a tabela
                try:
                    db.session.execute(text("ALTER TABLE tabelas ADD COLUMN prestador VARCHAR(255) NULL"))
                    db.session.commit()
                except Exception:
                    db.session.rollback()
                try:
                    db.session.execute(text("ALTER TABLE tabelas ADD COLUMN tipo_tabela VARCHAR(50) NULL"))
                    db.session.commit()
                except Exception:
                    db.session.rollback()
                try:
                    db.session.execute(text("ALTER TABLE tabelas ADD COLUMN uf VARCHAR(2) NULL"))
                    db.session.commit()
                except Exception:
                    db.session.rollback()
                try:
                    db.session.execute(text("ALTER TABLE tabelas ADD COLUMN uco_valor DECIMAL(12,2) NULL"))
                    db.session.commit()
                except Exception:
                    db.session.rollback()
                try:
                    db.session.execute(text("ALTER TABLE bras_item ADD COLUMN tipo_preco VARCHAR(50) NULL"))
                    db.session.commit()
                except Exception:
                    db.session.rollback()
                try:
                    db.session.execute(text("ALTER TABLE bras_item ADD COLUMN ean VARCHAR(64) NULL"))
                    db.session.commit()
                except Exception:
                    db.session.rollback()
                # Migração leve: acrescentar coluna UF em operadoras
                try:
                    db.session.execute(text("ALTER TABLE operadoras ADD COLUMN uf VARCHAR(2) NULL"))
                    db.session.commit()
                except Exception:
                    db.session.rollback()
                # Migração leve: acrescentar colunas em procedimentos
                try:
                    db.session.execute(text("ALTER TABLE procedimentos ADD COLUMN prestador VARCHAR(255) NULL"))
                    db.session.commit()
                except Exception:
                    db.session.rollback()
                try:
                    db.session.execute(text("ALTER TABLE procedimentos ADD COLUMN uf VARCHAR(2) NULL"))
                    db.session.commit()
                except Exception:
                    db.session.rollback()
                # Garante criação da tabela CBHPM (se ainda não existir)
                db.create_all()
                # Semeia um usuário admin padrão se não existir nenhum usuário
                try:
                    if db.session.query(Usuario).count() == 0:
                        admin_email = os.getenv('ADMIN_EMAIL', 'admin@local')
                        admin_senha = os.getenv('ADMIN_PASSWORD', 'admin123')
                        admin_nome = os.getenv('ADMIN_NAME', 'Administrador')
                        db.session.add(Usuario(nome=admin_nome, email=admin_email, senha=admin_senha, perfil='adm'))
                        db.session.commit()
                        print(f"[init] Usuário admin criado: {admin_email} / senha padrão")
                except Exception:
                    db.session.rollback()

                print(f"[init] Banco pronto após {attempt} tentativa(s).")
                try:
                    if db.session.query(CBHPMRuleSet).count() == 0:
                        regras_default = json.loads(json.dumps(DEFAULT_CBHPM_RULES))
                        ruleset = CBHPMRuleSet(
                            nome='CBHPM Padrão',
                            versao='Base',
                            descricao='Criada automaticamente',
                            ativo=True,
                            regras=regras_default
                        )
                        db.session.add(ruleset)
                        db.session.commit()
                except Exception:
                    db.session.rollback()
                return
        except Exception as e:
            last_err = e
            print(f"[init] MySQL indisponível (tentativa {attempt}/{max_retries}). Aguardando {delay_seconds}s...")
            time.sleep(delay_seconds)
    # Se esgotar
    raise last_err


ensure_db()


# --- 6. Usuários (UI) ---
@app.route('/usuarios/novo', methods=['GET', 'POST'])
@admin_required
def usuario_novo():
    if request.method == 'POST':
        nome = request.form.get('nome')
        email = request.form.get('email')
        senha = request.form.get('senha')
        perfil = request.form.get('perfil')
        if not all([nome, email, senha, perfil]):
            return render_template('usuario-form.html', erro='Preencha todos os campos', modo='novo', form=request.form)
        if Usuario.query.filter_by(email=email).first():
            return render_template('usuario-form.html', erro='E-mail já cadastrado', modo='novo', form=request.form)
        u = Usuario(nome=nome, email=email, senha=senha, perfil=perfil)
        db.session.add(u)
        db.session.commit()
        return redirect(url_for('gerenciar_usuarios'))
    return render_template('usuario-form.html', modo='novo')


@app.route('/usuarios/<int:uid>/editar', methods=['GET', 'POST'])
@admin_required
def usuario_editar(uid):
    u = Usuario.query.get_or_404(uid)
    if request.method == 'POST':
        u.nome = request.form.get('nome') or u.nome
        u.email = request.form.get('email') or u.email
        new_senha = request.form.get('senha')
        if new_senha:
            u.senha = new_senha
        u.perfil = request.form.get('perfil') or u.perfil
        db.session.commit()
        return redirect(url_for('gerenciar_usuarios'))
    return render_template('usuario-form.html', modo='editar', usuario=u)


# --- 7. Operadoras (UI) ---
BR_UFS = [
    'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO'
]


@app.route('/operadoras/nova', methods=['GET', 'POST'])
@admin_required
def operadora_nova():
    if request.method == 'POST':
        nome = request.form.get('nome')
        uf = request.form.get('uf')
        cnpj = request.form.get('cnpj')
        if not nome or not uf:
            return render_template('operadora-form.html', erro='Nome e UF são obrigatórios', modo='nova', form=request.form, UFS=BR_UFS)
        o = Operadora(nome=nome, uf=uf, cnpj=cnpj, status='Ativa')
        db.session.add(o)
        db.session.commit()
        return redirect(url_for('gerenciar_operadoras'))
    return render_template('operadora-form.html', modo='nova', UFS=BR_UFS)


@app.route('/operadoras/<int:oid>/editar', methods=['GET', 'POST'])
@admin_required
def operadora_editar(oid):
    o = Operadora.query.get_or_404(oid)
    if request.method == 'POST':
        o.nome = request.form.get('nome') or o.nome
        o.uf = request.form.get('uf') or o.uf
        o.cnpj = request.form.get('cnpj') or o.cnpj
        db.session.commit()
        return redirect(url_for('gerenciar_operadoras'))
    return render_template('operadora-form.html', modo='editar', operadora=o, UFS=BR_UFS)


# --- 8. Importação de Tabelas ---
def _norm_header(s: str) -> str:
    s = (s or '').strip()
    s = ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
    s = s.lower()
    s = s.replace(' ', '').replace('-', '').replace('_', '')
    return s


def _parse_money(v) -> Decimal:
    if v is None:
        return Decimal('0')
    if isinstance(v, (int, float, Decimal)):
        return Decimal(str(v))
    s = str(v).strip()
    if not s:
        return Decimal('0')
    s = s.replace('R$', '').replace(' ', '')
    s = s.replace('.', '')  # milhar
    s = s.replace(',', '.')  # decimal
    try:
        return Decimal(s)
    except InvalidOperation:
        return Decimal('0')


def _as_decimal(v):
    try:
        if v is None:
            return None
        if isinstance(v, (int, float, Decimal)):
            return Decimal(str(v))
        s = str(v).strip()
        if s == '' or s == '-':
            return None
        # If it uses comma as decimal separator or has currency, treat as money string
        if (',' in s) or ('R$' in s):
            return _parse_money(s)
        # Otherwise, assume dot-decimal and parse directly (to avoid stripping the dot)
        try:
            return Decimal(s)
        except InvalidOperation:
            return _parse_money(s)
    except Exception:
        setattr(load_logo_bytes, 'last_static_name', None)
        return None


def _sum_decimals(values):
    total = Decimal('0')
    found = False
    for v in values:
        dv = _as_decimal(v)
        if dv is not None:
            total += dv
            found = True
    return (total if found else None)


def _stringify_for_output(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, list):
        return [_stringify_for_output(v) for v in value]
    if isinstance(value, dict):
        return {k: _stringify_for_output(v) for k, v in value.items()}
    return value


def _lookup_porte_valor(operadora_id, uf, nome_hint, porte_codigo):
    if not porte_codigo:
        return None
    q = Tabela.query.filter(Tabela.tipo_tabela == 'porte', Tabela.id_operadora == operadora_id)
    if uf:
        q = q.filter(Tabela.uf == uf)
    # Se existir alguma com nome próximo ao hint, prefere
    if nome_hint:
        cand = q.filter(Tabela.nome.ilike(f"%{nome_hint}%")).order_by(Tabela.data_vigencia.is_(None), Tabela.data_vigencia.desc()).first()
        if cand:
            pv = PorteValorItem.query.filter_by(id_tabela=cand.id, porte=str(porte_codigo)).first()
            if pv:
                return pv.valor
    # Fallback: mais recente
    cand = q.order_by(Tabela.data_vigencia.is_(None), Tabela.data_vigencia.desc()).first()
    if cand:
        pv = PorteValorItem.query.filter_by(id_tabela=cand.id, porte=str(porte_codigo)).first()
        if pv:
            return pv.valor
    return None


def _lookup_porte_an_valor(operadora_id, uf, nome_hint, porte_an):
    if not porte_an:
        return None
    q = Tabela.query.filter(Tabela.tipo_tabela == 'porte_anestesico', Tabela.id_operadora == operadora_id)
    if uf:
        q = q.filter(Tabela.uf == uf)
    if nome_hint:
        cand = q.filter(Tabela.nome.ilike(f"%{nome_hint}%")).order_by(Tabela.data_vigencia.is_(None), Tabela.data_vigencia.desc()).first()
        if cand:
            pv = PorteAnestesicoValorItem.query.filter_by(id_tabela=cand.id, porte_an=str(porte_an)).first()
            if pv:
                return pv.valor
    cand = q.order_by(Tabela.data_vigencia.is_(None), Tabela.data_vigencia.desc()).first()
    if cand:
        pv = PorteAnestesicoValorItem.query.filter_by(id_tabela=cand.id, porte_an=str(porte_an)).first()
        if pv:
            return pv.valor
    return None


def _resolve_porte_tabela_nome(operadora_id, uf, nome_hint, porte_codigo):
    """Resolve o nome da tabela de Porte utilizada seguindo a mesma heurística
    de _lookup_porte_valor (preferindo por hint; fallback mais recente)."""
    if not porte_codigo:
        return None
    q = Tabela.query.filter(Tabela.tipo_tabela == 'porte', Tabela.id_operadora == operadora_id)
    if uf:
        q = q.filter(Tabela.uf == uf)
    if nome_hint:
        cand = q.filter(Tabela.nome.ilike(f"%{nome_hint}%")).order_by(Tabela.data_vigencia.is_(None), Tabela.data_vigencia.desc()).first()
        if cand:
            pv = PorteValorItem.query.filter_by(id_tabela=cand.id, porte=str(porte_codigo)).first()
            if pv:
                return cand.nome
    cand = q.order_by(Tabela.data_vigencia.is_(None), Tabela.data_vigencia.desc()).first()
    if cand:
        pv = PorteValorItem.query.filter_by(id_tabela=cand.id, porte=str(porte_codigo)).first()
        if pv:
            return cand.nome
    return None


def _resolve_porte_an_tabela_nome(operadora_id, uf, nome_hint, porte_an):
    """Resolve o nome da tabela de Porte Anestésico utilizada seguindo a mesma
    heurística de _lookup_porte_an_valor."""
    if not porte_an:
        return None
    q = Tabela.query.filter(Tabela.tipo_tabela == 'porte_anestesico', Tabela.id_operadora == operadora_id)
    if uf:
        q = q.filter(Tabela.uf == uf)
    if nome_hint:
        cand = q.filter(Tabela.nome.ilike(f"%{nome_hint}%")).order_by(Tabela.data_vigencia.is_(None), Tabela.data_vigencia.desc()).first()
        if cand:
            pv = PorteAnestesicoValorItem.query.filter_by(id_tabela=cand.id, porte_an=str(porte_an)).first()
            if pv:
                return cand.nome
    cand = q.order_by(Tabela.data_vigencia.is_(None), Tabela.data_vigencia.desc()).first()
    if cand:
        pv = PorteAnestesicoValorItem.query.filter_by(id_tabela=cand.id, porte_an=str(porte_an)).first()
        if pv:
            return cand.nome
    return None



def _clone_default_cbhpm_rules():
    return json.loads(json.dumps(DEFAULT_CBHPM_RULES))


def _get_active_cbhpm_ruleset(return_model: bool = False):
    ruleset = None
    try:
        ruleset = (
            CBHPMRuleSet.query
            .filter(CBHPMRuleSet.ativo.is_(True))
            .order_by(CBHPMRuleSet.atualizado_em.desc())
            .first()
        )
        if not ruleset:
            ruleset = (
                CBHPMRuleSet.query
                .order_by(CBHPMRuleSet.atualizado_em.desc())
                .first()
            )
    except Exception:
        db.session.rollback()
        ruleset = None
    data = _clone_default_cbhpm_rules()
    if ruleset and isinstance(ruleset.regras, dict) and ruleset.regras:
        try:
            data = json.loads(json.dumps(ruleset.regras))
        except Exception:
            data = _clone_default_cbhpm_rules()
    if return_model:
        return data, ruleset
    return data


def _apply_ruleset_to_breakdown(item: CBHPMItem, tabela_ref: Tabela, breakdown: dict, rules: dict | None):
    result = dict(breakdown or {})
    applied = []

    total_porte = _as_decimal(result.get('total_porte'))
    if total_porte is not None:
        aux_cfg = (rules or {}).get('auxiliares') or {}
        current_aux = _as_decimal(result.get('total_auxiliares'))
        aux_count_raw = getattr(item, 'numero_auxiliares', None)
        explicit_no_aux = False
        if aux_count_raw is not None:
            try:
                explicit_no_aux = Decimal(str(aux_count_raw)) == Decimal('0')
            except (InvalidOperation, ValueError):
                explicit_no_aux = False
        aux_details = []
        if (current_aux is None or current_aux == Decimal('0')) and aux_cfg.get('percentuais') and not explicit_no_aux:
            percentuais = aux_cfg.get('percentuais') or []
            try:
                aux_count = int(aux_count_raw) if aux_count_raw is not None else None
            except (TypeError, ValueError):
                aux_count = None
            max_por_porte = aux_cfg.get('max_por_porte') or {}
            porte_key = str(getattr(item, 'porte', '') or '').strip()
            max_aux = max_por_porte.get(porte_key, max_por_porte.get('default'))
            try:
                max_aux = int(max_aux) if max_aux is not None else None
            except (TypeError, ValueError):
                max_aux = None
            if aux_count is None:
                aux_count = max_aux if max_aux is not None else len(percentuais)
            elif max_aux is not None:
                aux_count = min(aux_count, max_aux)
            aux_count = max(aux_count or 0, 0)
            if aux_count and percentuais:
                computed = Decimal('0')
                for idx in range(aux_count):
                    perc = percentuais[min(idx, len(percentuais) - 1)]
                    perc = Decimal(str(perc))
                    if perc > 1:
                        perc = perc / Decimal('100')
                    if perc <= 0:
                        continue
                    value_aux = total_porte * perc
                    computed += value_aux
                    perc_display = perc * Decimal('100')
                    aux_details.append({
                        'indice': idx + 1,
                        'percentual_pct': str(perc_display),
                        'valor': value_aux
                    })
                if computed > 0:
                    result['total_auxiliares'] = computed
                    result['auxiliares_detalhe'] = aux_details
                    applied.append({
                        'component': 'auxiliares',
                        'rule': 'percentuais',
                        'quantidade': aux_count
                    })
        elif explicit_no_aux:
            result['total_auxiliares'] = Decimal('0')
            result['auxiliares_detalhe'] = []
    if 'auxiliares_detalhe' not in result or result['auxiliares_detalhe'] is None:
        aux_existing = []
        for idx, attr in enumerate(['total_1_aux', 'total_2_aux', 'total_3_aux', 'total_4_aux'], start=1):
            val = _as_decimal(getattr(item, attr, None))
            if val is not None and val != Decimal('0'):
                aux_existing.append({
                    'indice': idx,
                    'percentual_pct': None,
                    'valor': val
                })
        if aux_existing:
            result['auxiliares_detalhe'] = aux_existing
    result['total_porte'] = _as_decimal(result.get('total_porte'))
    result['total_filme'] = _as_decimal(result.get('total_filme'))
    result['total_uco'] = _as_decimal(result.get('total_uco'))
    result['total_porte_an'] = _as_decimal(result.get('total_porte_an'))
    result['total_auxiliares'] = _as_decimal(result.get('total_auxiliares'))

    multipliers = [
        ('total_porte', (rules or {}).get('porte'), 'porte'),
        ('total_filme', (rules or {}).get('filme'), 'filme'),
        ('total_uco', (rules or {}).get('uco'), 'uco'),
        ('total_porte_an', (rules or {}).get('porte_an'), 'porte_an'),
    ]
    for key, cfg, comp_name in multipliers:
        if not cfg:
            continue
        factor_raw = cfg.get('multiplicador') if isinstance(cfg, dict) else None
        if factor_raw in (None, '', 'None'):
            continue
        try:
            factor = Decimal(str(factor_raw))
        except (InvalidOperation, ValueError):
            continue
        if factor > Decimal('5'):
            factor = factor / Decimal('100')
        if factor < Decimal('0'):
            factor = Decimal('0')
        current = result.get(key)
        if current is None:
            continue
        new_value = current * factor
        if new_value == current:
            continue
        result[key] = new_value
        applied.append({
            'component': comp_name,
            'rule': 'multiplicador',
            'fator': str(factor)
        })

    result['total'] = _sum_decimals([
        result.get('total_porte'),
        result.get('total_filme'),
        result.get('total_uco'),
        result.get('total_porte_an'),
        result.get('total_auxiliares'),
    ])
    if applied:
        result['applied_rules'] = applied
    return result

def compute_cbhpm_total(item: CBHPMItem, tabela_ref: Tabela, porte_hint: str | None = None, porte_an_hint: str | None = None,
                        ajuste_porte_pct: Decimal | None = None, ajuste_porte_an_pct: Decimal | None = None, rules: dict | None = None):
    breakdown = compute_cbhpm_breakdown(
        item,
        tabela_ref,
        porte_hint=porte_hint,
        porte_an_hint=porte_an_hint,
        ajuste_porte_pct=ajuste_porte_pct,
        ajuste_porte_an_pct=ajuste_porte_an_pct,
        rules=rules,
    )
    return breakdown.get('total')


def compute_cbhpm_breakdown(item: CBHPMItem, tabela_ref: Tabela, porte_hint: str | None = None, porte_an_hint: str | None = None,
                            ajuste_porte_pct: Decimal | None = None, ajuste_porte_an_pct: Decimal | None = None, rules: dict | None = None):
    valor_porte = _as_decimal(item.valor_porte)
    if valor_porte is None:
        valor_porte = _lookup_porte_valor(tabela_ref.id_operadora, tabela_ref.uf, (porte_hint or tabela_ref.nome), item.porte)
    fracao_input = getattr(item, '_fracao_input', None)
    fracao = _as_decimal(fracao_input) if fracao_input is not None else _as_decimal(item.fracao_porte)
    if fracao is None or fracao <= Decimal('0'):
        fracao = Decimal('1')
    elif fracao_input is None and fracao < Decimal('1'):
        fracao = Decimal('1')
    total_porte = None
    if valor_porte is not None:
        total_porte = (valor_porte * fracao)
    if total_porte is None:
        total_porte = _as_decimal(item.total_porte)
    if total_porte is not None and ajuste_porte_pct:
        total_porte = total_porte * (Decimal('1') + (ajuste_porte_pct/Decimal('100')))

    total_filme = _as_decimal(item.total_filme)
    if total_filme is None:
        filme = _as_decimal(item.filme)
        incid = _as_decimal(item.incidencias)
        if filme is not None:
            total_filme = (filme * (incid or Decimal('1')))

    total_uco = _as_decimal(item.total_uco)
    if total_uco is None:
        uco_qtd = _as_decimal(item.uco)
        uco_val = _as_decimal(tabela_ref.uco_valor)
        if uco_qtd is not None and uco_val is not None:
            total_uco = (uco_qtd * uco_val)

    valor_an = _as_decimal(item.valor_porte_anestesico)
    if valor_an is None:
        valor_an = _lookup_porte_an_valor(tabela_ref.id_operadora, tabela_ref.uf, (porte_an_hint or tabela_ref.nome), item.porte_anestesico)
    total_an = _as_decimal(item.total_porte_anestesico)
    if total_an is not None and ajuste_porte_an_pct:
        total_an = total_an * (Decimal('1') + (ajuste_porte_an_pct/Decimal('100')))
    if total_an is None and valor_an is not None:
        total_an = valor_an

    total_aux = _as_decimal(item.total_auxiliares)
    if total_aux is None:
        total_aux = _sum_decimals([item.total_1_aux, item.total_2_aux, item.total_3_aux, item.total_4_aux])

    breakdown = {
        'total_porte': total_porte,
        'total_filme': total_filme,
        'total_uco': total_uco,
        'total_porte_an': total_an,
        'total_auxiliares': total_aux,
        'total': _sum_decimals([total_porte, total_filme, total_uco, total_an, total_aux]),
    }
    ruleset_dict = rules or _get_active_cbhpm_ruleset()
    breakdown = _apply_ruleset_to_breakdown(item, tabela_ref, breakdown, ruleset_dict)
    return breakdown

@app.route('/tabelas/importar/diarias-taxas-pacotes', methods=['POST'])
@admin_required
def importar_diarias_taxas_pacotes():
    file = request.files.get('arquivo')
    nome_tabela = request.form.get('nome_tabela')
    prestador = request.form.get('prestador')
    uf = request.form.get('uf')
    data_vigencia = request.form.get('data_vigencia')  # YYYY-MM-DD
    operadora_id = request.form.get('operadora_id')
    substituir = request.form.get('substituir') in ('on', 'true', '1', 'yes', 'sim', 'true')

    if not file or not nome_tabela or not operadora_id:
        return redirect(url_for('gerenciar_tabelas'))

    # A criação de Tabelas ocorrerá após a leitura do arquivo, podendo ser
    # uma por prestador (e UF) quando não informado no formulário.

    filename = secure_filename(file.filename or '')
    ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''

    linhas = []
    if ext == 'csv':
        content = file.read().decode('utf-8-sig', errors='ignore').splitlines()
        if not content:
            return redirect(url_for('gerenciar_tabelas'))
        headers = [h.strip() for h in content[0].split(',')]
        keys = [_norm_header(h) for h in headers]
        for row in content[1:]:
            cols = row.split(',')
            item = {keys[i]: (cols[i].strip() if i < len(cols) else '') for i in range(len(keys))}
            linhas.append(item)
    elif ext == 'xlsx':
        try:
            from openpyxl import load_workbook
        except Exception:
            return redirect(url_for('gerenciar_tabelas'))
        wb = load_workbook(file, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            return redirect(url_for('gerenciar_tabelas'))
        headers = [str(h) if h is not None else '' for h in rows[0]]
        keys = [_norm_header(h) for h in headers]
        for r in rows[1:]:
            item = {keys[i]: (r[i] if i < len(keys) else None) for i in range(len(keys))}
            linhas.append(item)
    else:
        db.session.rollback()
        return redirect(url_for('gerenciar_tabelas'))

    # Importação consolidada: cria uma única Tabela e grava o prestador/UF por item
    if True:
        if substituir:
            subq = db.session.query(Tabela.id).filter(
                Tabela.nome == nome_tabela,
                Tabela.id_operadora == int(operadora_id)
            )
            db.session.query(Procedimento).filter(Procedimento.id_tabela.in_(subq)).delete(synchronize_session=False)
            db.session.query(Tabela).filter(
                Tabela.nome == nome_tabela,
                Tabela.id_operadora == int(operadora_id)
            ).delete(synchronize_session=False)
            db.session.flush()

        tab = Tabela(
            nome=nome_tabela,
            prestador=None,
            tipo_tabela='diarias_taxas_pacotes',
            uf=uf,
            id_operadora=int(operadora_id)
        )
        if data_vigencia:
            try:
                tab.data_vigencia = date.fromisoformat(data_vigencia)
            except Exception:
                pass
        db.session.add(tab)
        db.session.flush()

        for item in linhas:
            codigo = item.get('codigo') or item.get('cod')
            descricao = item.get('descricao') or item.get('descriçao') or item.get('descrição')
            valor = _parse_money(item.get('valor'))
            if not codigo or not descricao:
                continue
            prest_item = item.get('prestador') or item.get('fornecedor') or item.get('credenciado') or prestador
            prest_item = str(prest_item).strip() if prest_item is not None else None
            uf_item = (item.get('uf') or uf)
            uf_item = str(uf_item).strip() if uf_item else None
            db.session.add(Procedimento(
                codigo=str(codigo),
                descricao=str(descricao),
                valor=valor,
                prestador=prest_item or None,
                uf=uf_item or None,
                id_tabela=tab.id
            ))

        db.session.commit()
        return redirect(url_for('gerenciar_tabelas'))

    if prestador:
        # Se solicitado, remove tabelas existentes com o mesmo nome/operadora/UF/prestador
        if substituir:
            db.session.query(Procedimento).filter(
                Procedimento.id_tabela.in_(db.session.query(Tabela.id).filter(
                    Tabela.nome == nome_tabela,
                    Tabela.id_operadora == int(operadora_id),
                    (Tabela.uf == uf) if uf else True,
                    Tabela.prestador == prestador,
                ))
            ).delete(synchronize_session=False)
            db.session.query(Tabela).filter(
                Tabela.nome == nome_tabela,
                Tabela.id_operadora == int(operadora_id),
                (Tabela.uf == uf) if uf else True,
                Tabela.prestador == prestador,
            ).delete(synchronize_session=False)
            db.session.flush()
        # Importa tudo em uma única tabela usando o prestador do formulário
        tab = Tabela(
            nome=nome_tabela,
            prestador=prestador,
            tipo_tabela='diarias_taxas_pacotes',
            uf=uf,
            id_operadora=int(operadora_id)
        )
        if data_vigencia:
            try:
                tab.data_vigencia = date.fromisoformat(data_vigencia)
            except Exception:
                pass
        db.session.add(tab)
        db.session.flush()
        for item in linhas:
            codigo = item.get('codigo') or item.get('cod')
            descricao = item.get('descricao') or item.get('descriçao') or item.get('descrição')
            valor = _parse_money(item.get('valor'))
            if not codigo or not descricao:
                continue
            db.session.add(Procedimento(codigo=str(codigo), descricao=str(descricao), valor=valor, id_tabela=tab.id))
    else:
        # Agrupa por prestador (e UF) vindos do arquivo
        grupos = {}
        for item in linhas:
            codigo = item.get('codigo') or item.get('cod')
            descricao = item.get('descricao') or item.get('descriçao') or item.get('descrição')
            valor = _parse_money(item.get('valor'))
            if not codigo or not descricao:
                continue
            prest = item.get('prestador') or item.get('fornecedor') or item.get('credenciado') or ''
            prest = str(prest).strip() if prest is not None else ''
            uf_row = (item.get('uf') or uf or '').strip() if item.get('uf') is not None else (uf or '')
            nome_arq = item.get('tabela') or nome_tabela
            key = (prest or '-'), (uf_row or '')
            bucket = grupos.setdefault(key, {"nome": nome_arq or nome_tabela, "items": []})
            bucket["items"].append((str(codigo), str(descricao), valor))

        # Se solicitado, remove tabelas existentes com os nomes que serão criados
        if substituir and grupos:
            nomes_alvo = {g["nome"] for g in grupos.values()}
            subq = db.session.query(Tabela.id).filter(
                Tabela.id_operadora == int(operadora_id),
                Tabela.nome.in_(list(nomes_alvo))
            )
            db.session.query(Procedimento).filter(Procedimento.id_tabela.in_(subq)).delete(synchronize_session=False)
            db.session.query(Tabela).filter(
                Tabela.id_operadora == int(operadora_id),
                Tabela.nome.in_(list(nomes_alvo))
            ).delete(synchronize_session=False)
            db.session.flush()

        for (prest_key, uf_key), bucket in grupos.items():
            tab = Tabela(
                nome=bucket["nome"],
                prestador=None if prest_key == '-' else prest_key,
                tipo_tabela='diarias_taxas_pacotes',
                uf=(uf_key or None),
                id_operadora=int(operadora_id)
            )
            if data_vigencia:
                try:
                    tab.data_vigencia = date.fromisoformat(data_vigencia)
                except Exception:
                    pass
            db.session.add(tab)
            db.session.flush()
            for codigo, descricao, valor in bucket["items"]:
                db.session.add(Procedimento(codigo=codigo, descricao=descricao, valor=valor, id_tabela=tab.id))

    db.session.commit()
    return redirect(url_for('gerenciar_tabelas'))


@app.route('/tabelas/<int:tid>/excluir', methods=['POST'])
@admin_required
def tabela_excluir(tid):
    t = Tabela.query.get_or_404(tid)
    # Remove itens vinculados e depois a tabela
    db.session.query(Procedimento).filter_by(id_tabela=tid).delete(synchronize_session=False)
    db.session.delete(t)
    db.session.commit()
    return redirect(url_for('gerenciar_tabelas'))


@app.route('/tabelas/uco/definir', methods=['POST'])
@admin_required
def definir_uco_cbhpm():
    updated = 0
    form = request.form or {}
    for key, value in form.items():
        if not key.startswith('uco_'):
            continue
        try:
            tid = int(key.split('_', 1)[1])
        except Exception:
            continue
        tab = Tabela.query.get(tid)
        if not tab or tab.tipo_tabela != 'cbhpm':
            continue
        v = (value or '').strip()
        tab.uco_valor = _parse_money(v) if v else None
        updated += 1
    if updated:
        db.session.commit()
    return redirect(url_for('gerenciar_tabelas'))


@app.route('/tabelas/importar/porte', methods=['POST'])
@admin_required
def importar_porte():
    file = request.files.get('arquivo')
    nome_tabela = request.form.get('nome_tabela')
    uf = request.form.get('uf')
    data_vigencia = request.form.get('data_vigencia')
    operadora_id = request.form.get('operadora_id')
    substituir = request.form.get('substituir') in ('on', 'true', '1', 'yes', 'sim', 'true')

    if not file or not nome_tabela or not operadora_id:
        return redirect(url_for('gerenciar_tabelas'))

    if substituir:
        subq = db.session.query(Tabela.id).filter(Tabela.nome == nome_tabela, Tabela.id_operadora == int(operadora_id), Tabela.tipo_tabela == 'porte')
        db.session.query(PorteValorItem).filter(PorteValorItem.id_tabela.in_(subq)).delete(synchronize_session=False)
        db.session.query(Tabela).filter(Tabela.id.in_(subq)).delete(synchronize_session=False)
        db.session.flush()

    tab = Tabela(nome=nome_tabela, prestador=None, tipo_tabela='porte', uf=uf, id_operadora=int(operadora_id))
    if data_vigencia:
        try:
            tab.data_vigencia = date.fromisoformat(data_vigencia)
        except Exception:
            pass
    db.session.add(tab)
    db.session.flush()

    filename = secure_filename(file.filename or '')
    ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
    linhas = []
    if ext == 'csv':
        content = file.read().decode('utf-8-sig', errors='ignore').splitlines()
        if not content:
            return redirect(url_for('gerenciar_tabelas'))
        headers = [h.strip() for h in content[0].split(',')]
        keys = [_norm_header(h) for h in headers]
        for row in content[1:]:
            cols = row.split(',')
            linhas.append({keys[i]: (cols[i].strip() if i < len(cols) else '') for i in range(len(keys))})
    elif ext == 'xlsx':
        from openpyxl import load_workbook
        wb = load_workbook(file, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            return redirect(url_for('gerenciar_tabelas'))
        headers = [str(h) if h is not None else '' for h in rows[0]]
        keys = [_norm_header(h) for h in headers]
        for r in rows[1:]:
            linhas.append({keys[i]: (r[i] if i < len(keys) else None) for i in range(len(keys))})
    else:
        return redirect(url_for('gerenciar_tabelas'))

    for row in linhas:
        porte = row.get('porte') or row.get('portevalor') or row.get('portecodigo')
        if not porte:
            continue
        valor = _parse_money(row.get('valor'))
        db.session.add(PorteValorItem(porte=str(porte), valor=valor, uf=uf, id_tabela=tab.id))

    db.session.commit()
    return redirect(url_for('gerenciar_tabelas'))


@app.route('/tabelas/importar/porte-anestesico', methods=['POST'])
@admin_required
def importar_porte_anestesico():
    file = request.files.get('arquivo')
    nome_tabela = request.form.get('nome_tabela')
    uf = request.form.get('uf')
    data_vigencia = request.form.get('data_vigencia')
    operadora_id = request.form.get('operadora_id')
    substituir = request.form.get('substituir') in ('on', 'true', '1', 'yes', 'sim', 'true')

    if not file or not nome_tabela or not operadora_id:
        return redirect(url_for('gerenciar_tabelas'))

    if substituir:
        subq = db.session.query(Tabela.id).filter(Tabela.nome == nome_tabela, Tabela.id_operadora == int(operadora_id), Tabela.tipo_tabela == 'porte_anestesico')
        db.session.query(PorteAnestesicoValorItem).filter(PorteAnestesicoValorItem.id_tabela.in_(subq)).delete(synchronize_session=False)
        db.session.query(Tabela).filter(Tabela.id.in_(subq)).delete(synchronize_session=False)
        db.session.flush()

    tab = Tabela(nome=nome_tabela, prestador=None, tipo_tabela='porte_anestesico', uf=uf, id_operadora=int(operadora_id))
    if data_vigencia:
        try:
            tab.data_vigencia = date.fromisoformat(data_vigencia)
        except Exception:
            pass
    db.session.add(tab)
    db.session.flush()

    filename = secure_filename(file.filename or '')
    ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
    linhas = []
    if ext == 'csv':
        content = file.read().decode('utf-8-sig', errors='ignore').splitlines()
        if not content:
            return redirect(url_for('gerenciar_tabelas'))
        headers = [h.strip() for h in content[0].split(',')]
        keys = [_norm_header(h) for h in headers]
        for row in content[1:]:
            cols = row.split(',')
            linhas.append({keys[i]: (cols[i].strip() if i < len(cols) else '') for i in range(len(keys))})
    elif ext == 'xlsx':
        from openpyxl import load_workbook
        wb = load_workbook(file, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            return redirect(url_for('gerenciar_tabelas'))
        headers = [str(h) if h is not None else '' for h in rows[0]]
        keys = [_norm_header(h) for h in headers]
        for r in rows[1:]:
            linhas.append({keys[i]: (r[i] if i < len(keys) else None) for i in range(len(keys))})
    else:
        return redirect(url_for('gerenciar_tabelas'))

    for row in linhas:
        porte_an = row.get('portean') or row.get('porteanestesico') or row.get('porte_an') or row.get('porte an')
        if not porte_an:
            continue
        valor = _parse_money(row.get('valor'))
        db.session.add(PorteAnestesicoValorItem(porte_an=str(porte_an), valor=valor, uf=uf, id_tabela=tab.id))

    db.session.commit()
    return redirect(url_for('gerenciar_tabelas'))


@app.route('/tabelas/importar/cbhpm', methods=['POST'])
@admin_required
def importar_cbhpm():
    file = request.files.get('arquivo')
    nome_tabela = request.form.get('nome_tabela')
    uf = request.form.get('uf')
    data_vigencia = request.form.get('data_vigencia')
    operadora_id = request.form.get('operadora_id')
    substituir = request.form.get('substituir') in ('on', 'true', '1', 'yes', 'sim', 'true')

    if not file or not nome_tabela or not operadora_id:
        return redirect(url_for('gerenciar_tabelas'))

    # Substituição
    if substituir:
        subq = db.session.query(Tabela.id).filter(
            Tabela.nome == nome_tabela,
            Tabela.id_operadora == int(operadora_id),
            Tabela.tipo_tabela == 'cbhpm'
        )
        db.session.query(CBHPMItem).filter(CBHPMItem.id_tabela.in_(subq)).delete(synchronize_session=False)
        db.session.query(Tabela).filter(
            Tabela.nome == nome_tabela,
            Tabela.id_operadora == int(operadora_id),
            Tabela.tipo_tabela == 'cbhpm'
        ).delete(synchronize_session=False)
        db.session.flush()

    # Cria Tabela
    tab = Tabela(
        nome=nome_tabela,
        prestador=None,
        tipo_tabela='cbhpm',
        uf=uf,
        id_operadora=int(operadora_id)
    )
    if data_vigencia:
        try:
            tab.data_vigencia = date.fromisoformat(data_vigencia)
        except Exception:
            pass
    db.session.add(tab)
    db.session.flush()

    filename = secure_filename(file.filename or '')
    ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''

    # Leitura
    linhas = []
    keys = []
    if ext == 'csv':
        content = file.read().decode('utf-8-sig', errors='ignore').splitlines()
        if not content:
            return redirect(url_for('gerenciar_tabelas'))
        headers = [h.strip() for h in content[0].split(',')]
        keys = [_norm_header(h) for h in headers]
        for row in content[1:]:
            cols = row.split(',')
            item = {keys[i]: (cols[i].strip() if i < len(cols) else '') for i in range(len(keys))}
            linhas.append(item)
    elif ext == 'xlsx':
        try:
            from openpyxl import load_workbook
        except Exception:
            return redirect(url_for('gerenciar_tabelas'))
        wb = load_workbook(file, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            return redirect(url_for('gerenciar_tabelas'))
        headers = [str(h) if h is not None else '' for h in rows[0]]
        keys = [_norm_header(h) for h in headers]
        for r in rows[1:]:
            item = {keys[i]: (r[i] if i < len(keys) else None) for i in range(len(keys))}
            linhas.append(item)
    else:
        db.session.rollback()
        return redirect(url_for('gerenciar_tabelas'))

    # Campos esperados (normalizados)
    def g(d, *names):
        for n in names:
            v = d.get(n)
            if v not in (None, ''):
                return v
        return None

    def dec(v):
        return _parse_money(v) if v not in (None, '') else None

    def intval(v):
        try:
            return int(str(v).strip()) if v not in (None, '') else None
        except Exception:
            return None

    for row in linhas:
        codigo = str(g(row, 'codigo')) if g(row, 'codigo') is not None else None
        descricao = str(g(row, 'procedimento', 'descricao')) if g(row, 'procedimento', 'descricao') is not None else ''
        if not codigo:
            continue
        item = CBHPMItem(
            codigo=codigo,
            procedimento=descricao,
            uf=uf,
            porte=str(g(row, 'porte')) if g(row, 'porte') is not None else None,
            fracao_porte=dec(g(row, 'fracaoporte', 'fraçãoporte')),
            valor_porte=dec(g(row, 'valorporte', 'valor_do_porte')),
            total_porte=dec(g(row, 'totalporte')),
            incidencias=str(g(row, 'incidencias', 'incidências')) if g(row, 'incidencias', 'incidências') is not None else None,
            filme=dec(g(row, 'filme')),
            total_filme=dec(g(row, 'totalfilme')),
            uco=dec(g(row, 'uco')),
            total_uco=dec(g(row, 'totaluco')),
            porte_anestesico=str(g(row, 'porteanestesico', 'porteanestésico')) if g(row, 'porteanestesico', 'porteanestésico') is not None else None,
            valor_porte_anestesico=dec(g(row, 'valorporteanestesico', 'valorporteanestésico')),
            total_porte_anestesico=dec(g(row, 'totalporteanestesico', 'totalporteanestésico')),
            numero_auxiliares=intval(g(row, 'numero_de_auxiliares', 'numerodeauxiliares')),
            total_auxiliares=dec(g(row, 'totalauxiliares')),
            total_1_aux=dec(g(row, 'total1oauxiliar', 'total1ºauxiliar', 'total1auxiliar')),
            total_2_aux=dec(g(row, 'total2oauxiliar', 'total2ºauxiliar', 'total2auxiliar')),
            total_3_aux=dec(g(row, 'total3oauxiliar', 'total3ºauxiliar', 'total3auxiliar')),
            total_4_aux=dec(g(row, 'total4oauxiliar', 'total4ºauxiliar', 'total4auxiliar')),
            subtotal=dec(g(row, 'subtotal')),
            id_tabela=tab.id,
        )
        db.session.add(item)

    db.session.commit()
    return redirect(url_for('gerenciar_tabelas'))


# --- 9. Visualização de Itens da Tabela ---
@app.template_filter('brl')
def brl(value):
    try:
        d = Decimal(value)
    except Exception:
        return value
    s = f"{d:,.2f}"
    return f"R$ {s}".replace(",", "X").replace(".", ",").replace("X", ".")


@app.template_filter('date_br')
def date_br(value):
    try:
        return value.strftime('%d/%m/%Y') if value else '-'
    except Exception:
        return str(value) if value else '-'


@app.route('/tabelas/<int:tid>/itens')
@admin_required
def tabela_itens(tid):
    tabela = Tabela.query.get_or_404(tid)
    q = request.args.get('q', '').strip()
    # Se for CBHPM, lista a partir da tabela específica
    if tabela.tipo_tabela == 'cbhpm':
        query = CBHPMItem.query.filter_by(id_tabela=tid)
        if q:
            like = f"%{q}%"
            query = query.filter(
                (CBHPMItem.codigo.ilike(like)) | (CBHPMItem.procedimento.ilike(like))
            )
        rows = query.order_by(CBHPMItem.codigo).all()
        # Mapeia para o formato consumido pelo template (codigo, descricao, valor)
        itens = []
        for r in rows:
            val = r.subtotal
            if val in (None, Decimal('0')):
                val = compute_cbhpm_total(r, tabela)
            itens.append({
                'codigo': r.codigo,
                'descricao': r.procedimento,
                'valor': val,
            })
        return render_template('tabela-itens.html', tabela=tabela, itens=itens, q=q)

    if tabela.tipo_tabela == 'porte':
        query = PorteValorItem.query.filter_by(id_tabela=tid)
        if q:
            like = f"%{q}%"
            query = query.filter(PorteValorItem.porte.ilike(like))
        rows = query.order_by(PorteValorItem.porte).all()
        itens = [{'porte': r.porte, 'valor': r.valor, 'uf': r.uf} for r in rows]
        return render_template('tabela-porte-itens.html', tabela=tabela, itens=itens, q=q, label='Porte')

    if tabela.tipo_tabela == 'porte_anestesico':
        query = PorteAnestesicoValorItem.query.filter_by(id_tabela=tid)
        if q:
            like = f"%{q}%"
            query = query.filter(PorteAnestesicoValorItem.porte_an.ilike(like))
        rows = query.order_by(PorteAnestesicoValorItem.porte_an).all()
        itens = [{'porte': r.porte_an, 'valor': r.valor, 'uf': r.uf} for r in rows]
        return render_template('tabela-porte-itens.html', tabela=tabela, itens=itens, q=q, label='Porte AN')

    # Default: procedimentos comuns
    query = Procedimento.query.filter_by(id_tabela=tid)
    if q:
        like = f"%{q}%"
        query = query.filter(
            (Procedimento.codigo.ilike(like)) | (Procedimento.descricao.ilike(like))
        )
    itens = query.order_by(Procedimento.codigo).all()
    return render_template('tabela-itens.html', tabela=tabela, itens=itens, q=q)


@app.route('/insumos/search')
@login_required
def insumos_search():
    page = _parse_positive_int(request.args.get('page'), 1, maximum=500)
    per_page = _parse_positive_int(request.args.get('per_page'), 50, maximum=500)

    filters = _extract_insumo_filters(request.args)
    query = _apply_insumo_filters(InsumoIndex.query, filters)
    query = query.order_by(InsumoIndex.descricao.asc(), InsumoIndex.item_id.asc())

    total = query.count()
    items = (
        query
        .offset((page - 1) * per_page)
        .limit(per_page)
        .all()
    )

    bras_map: dict[int, BrasItemNormalized] = {}
    bras_ids = [item.item_id for item in items if item.origem == 'BRAS']
    if bras_ids:
        bras_rows = BrasItemNormalized.query.filter(BrasItemNormalized.id.in_(bras_ids)).all()
        bras_map = {row.id: row for row in bras_rows}

    simpro_map: dict[int, SimproItemNormalized] = {}
    simpro_ids = [item.item_id for item in items if item.origem == 'SIMPRO']
    if simpro_ids:
        simpro_rows = SimproItemNormalized.query.filter(SimproItemNormalized.id.in_(simpro_ids)).all()
        simpro_map = {row.id: row for row in simpro_rows}

    serialized: list[dict] = []
    for item in items:
        if item.origem == 'BRAS':
            bras = bras_map.get(item.item_id)
            if bras:
                preco_pmc = bras.preco_pmc_unit or bras.preco_pmc_pacote
                preco_pfb = bras.preco_pfb_unit or bras.preco_pfb_pacote
            else:
                preco_pmc = preco_pfb = None
            serialized.append(_serialize_insumo_index(item, preco_pmc=preco_pmc, preco_pfb=preco_pfb))
        elif item.origem == 'SIMPRO':
            simpro = simpro_map.get(item.item_id)
            if simpro:
                preco_pmc = None
                preco_pfb = simpro.preco2 or simpro.preco1 or simpro.preco3 or simpro.preco4
            else:
                preco_pmc = None
                preco_pfb = None
            serialized.append(_serialize_insumo_index(item, preco_pmc=preco_pmc, preco_pfb=preco_pfb))
        else:
            serialized.append(_serialize_insumo_index(item))

    payload = {
        'items': serialized,
        'pagination': {
            'page': page,
            'per_page': per_page,
            'total': total,
            'pages': math.ceil(total / per_page) if total else 0,
        }
    }
    return jsonify(payload)


@app.route('/insumos/<origem>/<int:item_id>')
@login_required
def insumo_detail(origem: str, item_id: int):
    origem = (origem or '').upper()
    if origem not in {'BRAS', 'SIMPRO'}:
        abort(404)

    model = BrasItemNormalized if origem == 'BRAS' else SimproItemNormalized
    item = model.query.get(item_id)
    if not item:
        abort(404)

    index_entry = InsumoIndex.query.filter_by(origem=origem, item_id=item_id).first()
    return jsonify(_serialize_insumo_detail(origem, item, index_entry=index_entry))


@app.route('/insumos')
@login_required
def insumos_dashboard():
    bras_summary = _insumo_summary(BrasItemNormalized)
    simpro_summary = _insumo_summary(SimproItemNormalized)
    bras_versions = _insumo_distinct_versions(BrasItemNormalized)
    simpro_versions = _insumo_distinct_versions(SimproItemNormalized)
    versions = sorted(set(bras_versions + simpro_versions))

    return render_template(
        'insumos_index.html',
        bras_summary=bras_summary,
        simpro_summary=simpro_summary,
        bras_versions=bras_versions,
        simpro_versions=simpro_versions,
        versions=versions,
        is_admin=(session.get('perfil') == 'adm'),
        UFS=BR_UFS,
    )


@app.route('/insumos/export/xlsx')
@login_required
def insumos_export_xlsx():
    filters = _extract_insumo_filters(request.args)
    query = _apply_insumo_filters(InsumoIndex.query, filters)
    query = query.order_by(InsumoIndex.descricao.asc(), InsumoIndex.item_id.asc())

    limit = _parse_positive_int(request.args.get('limit'), 5000, maximum=20000)
    rows = query.limit(limit).all()

    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet('Insumos')

    header_fmt = workbook.add_format({'bold': True, 'bg_color': '#0EA5E9', 'font_color': '#ffffff'})
    money_fmt = workbook.add_format({'num_format': '#,##0.0000'})

    headers = ['Origem', 'TUSS', 'TISS', 'ANVISA', 'Descrição', 'PMC', 'PFB', 'Alíquota', 'Fabricante', 'UF', 'Versão', 'Data Atualização', 'Atualizado em']
    for col, title in enumerate(headers):
        worksheet.write(0, col, title, header_fmt)

    bras_map: dict[int, BrasItemNormalized] = {}
    bras_ids = [item.item_id for item in rows if item.origem == 'BRAS']
    if bras_ids:
        bras_rows = BrasItemNormalized.query.filter(BrasItemNormalized.id.in_(bras_ids)).all()
        bras_map = {row.id: row for row in bras_rows}

    simpro_map: dict[int, SimproItemNormalized] = {}
    simpro_ids = [item.item_id for item in rows if item.origem == 'SIMPRO']
    if simpro_ids:
        simpro_rows = SimproItemNormalized.query.filter(SimproItemNormalized.id.in_(simpro_ids)).all()
        simpro_map = {row.id: row for row in simpro_rows}

    for row_idx, item in enumerate(rows, start=1):
        worksheet.write(row_idx, 0, item.origem)
        worksheet.write(row_idx, 1, item.tuss or '')
        worksheet.write(row_idx, 2, item.tiss or '')
        worksheet.write(row_idx, 3, item.anvisa or '')
        worksheet.write(row_idx, 4, item.descricao or '')
        preco_pmc = item.preco
        preco_pfb = item.preco
        if item.origem == 'BRAS':
            bras = bras_map.get(item.item_id)
            if bras:
                preco_pmc = bras.preco_pmc_unit or bras.preco_pmc_pacote
                preco_pfb = bras.preco_pfb_unit or bras.preco_pfb_pacote
        elif item.origem == 'SIMPRO':
            simpro = simpro_map.get(item.item_id)
            if simpro:
                preco_pmc = None
                preco_pfb = simpro.preco2 or simpro.preco1 or simpro.preco3 or simpro.preco4
            else:
                preco_pmc = None
                preco_pfb = None
        if preco_pmc is not None:
            worksheet.write_number(row_idx, 5, float(preco_pmc), money_fmt)
        else:
            worksheet.write_blank(row_idx, 5, None)
        if preco_pfb is not None:
            worksheet.write_number(row_idx, 6, float(preco_pfb), money_fmt)
        else:
            worksheet.write_blank(row_idx, 6, None)
        if item.aliquota is not None:
            worksheet.write_number(row_idx, 7, float(item.aliquota), money_fmt)
        else:
            worksheet.write_blank(row_idx, 7, None)
        worksheet.write(row_idx, 8, item.fabricante or '')
        worksheet.write(row_idx, 9, item.uf_referencia or '')
        worksheet.write(row_idx, 10, item.versao_tabela or '')
        worksheet.write(row_idx, 11, item.data_atualizacao.isoformat() if isinstance(item.data_atualizacao, date) else '')
        worksheet.write(row_idx, 12, item.updated_at.isoformat(sep=' ') if isinstance(item.updated_at, datetime) else '')

    worksheet.autofilter(0, 0, max(len(rows), 1), len(headers) - 1)
    worksheet.freeze_panes(1, 0)

    workbook.close()
    output.seek(0)
    stamp = datetime.now().strftime('%Y%m%d_%H%M')
    filename = f'insumos_{stamp}.xlsx'
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=filename,
    )


@app.route('/insumos/import', methods=['POST'])
@admin_required
def insumos_import():
    return_to = (request.form.get('return_to') or '').strip()
    redirect_endpoint = 'gerenciar_tabelas' if return_to == 'gerenciar_tabelas' else 'insumos_dashboard'
    def _go_back():
        return redirect(url_for(redirect_endpoint))

    origem = (request.form.get('origem') or '').upper()
    if origem not in {'BRAS', 'SIMPRO'}:
        flash('Origem inválida para importação.', 'danger')
        return _go_back()

    upload = request.files.get('arquivo')
    if not upload or not upload.filename:
        flash('Selecione um arquivo TXT/CSV para importar.', 'danger')
        return _go_back()

    fmt = (request.form.get('format') or 'delimited').lower()
    delimiter = request.form.get('delimiter') or ';'
    quotechar = request.form.get('quotechar') or '"'
    versao = (request.form.get('versao') or '').strip()
    data_ref = (request.form.get('data_atualizacao') or '').strip() or None
    no_header = request.form.get('no_header') == 'on'
    truncate = request.form.get('truncate') == 'on'
    encoding = (request.form.get('encoding') or '').strip() or None
    uf_values = [uf.strip().upper() for uf in request.form.getlist('uf') if uf and uf.strip()]
    if not uf_values:
        fallback_uf = (request.form.get('uf') or request.form.get('uf_referencia') or '').strip().upper()
        if fallback_uf:
            uf_values = [fallback_uf]
    seen_ufs: set[str] = set()
    uf_values = [uf for uf in uf_values if not (uf in seen_ufs or seen_ufs.add(uf))]
    if not uf_values:
        flash('Selecione pelo menos uma UF para importar.', 'danger')
        return _go_back()
    invalid_ufs = [uf for uf in uf_values if uf not in BR_UFS]
    if invalid_ufs:
        flash(f"UF inválida informada: {', '.join(invalid_ufs)}", 'danger')
        return _go_back()
    aliquota_input = (request.form.get('aliquota') or '').strip() or None
    aliquota_value: Decimal | None = None
    if aliquota_input:
        aliquota_str = _coerce_decimal(aliquota_input)
        if aliquota_str is None:
            flash('Informe uma alíquota válida (use números, ponto ou vírgula).', 'danger')
            return _go_back()
        aliquota_value = Decimal(aliquota_str)

    if not versao:
        flash('Informe a versão de referência da tabela.', 'danger')
        return _go_back()

    map_upload = request.files.get('map_config')
    map_temp_path: Path | None = None
    file_temp_path: Path | None = None

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=Path(upload.filename).suffix) as tmp_file:
            upload.save(tmp_file)
            file_temp_path = Path(tmp_file.name)

        if map_upload and map_upload.filename:
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(map_upload.filename).suffix or '.json') as tmp_map:
                map_upload.save(tmp_map)
                map_temp_path = Path(tmp_map.name)

        map_config: dict = {}
        if map_temp_path:
            try:
                map_config = json.loads(map_temp_path.read_text(encoding='utf-8'))
            except json.JSONDecodeError as exc:
                flash(f'Erro ao ler o mapa: {exc}', 'danger')
                return _go_back()
            if not isinstance(map_config, dict):
                flash('Arquivo de mapeamento deve conter um objeto JSON.', 'danger')
                return _go_back()

        if origem == 'BRAS':
            if fmt == 'fixed' and not map_config.get('columns'):
                flash('Envie um arquivo de mapeamento contendo "columns" para largura fixa.', 'danger')
                return _go_back()

            line_cfg = map_config.get('lines_terminated') or map_config.get('line_terminator')
            lines_terminated = line_cfg or (request.form.get('lines_terminated') or '\n')

            encoding_cfg = map_config.get('encoding')
            if isinstance(encoding_cfg, str) and encoding_cfg.strip():
                encoding = encoding_cfg.strip()

            skip_header_cfg = map_config.get('skip_header') if 'skip_header' in map_config else None
            skip_header = bool(skip_header_cfg) if skip_header_cfg is not None else (not no_header)

            delimiter_cfg = map_config.get('delimiter') if fmt == 'delimited' else None
            if delimiter_cfg:
                delimiter = delimiter_cfg
            quote_cfg = map_config.get('quotechar') if fmt == 'delimited' else None
            if quote_cfg is not None:
                quotechar = quote_cfg
            if quotechar is not None and not str(quotechar).strip():
                quotechar = None

            import_results: list[dict[str, object]] = []
            for idx, uf in enumerate(uf_values):
                result = _import_bras(
                    file_path=file_temp_path,
                    versao=versao,
                    data_ref=data_ref,
                    fmt=fmt,
                    delimiter=_normalize_delimiter(delimiter) if fmt == 'delimited' else delimiter,
                    quotechar=quotechar,
                    line_terminator=lines_terminated or '\n',
                    skip_header=skip_header,
                    encoding=encoding,
                    map_config=map_config,
                    truncate=truncate if idx == 0 else False,
                    uf_default=uf,
                    aliquota_default=aliquota_value,
                )
                result['uf'] = uf
                import_results.append(result)
            if import_results:
                if len(import_results) == 1:
                    res = import_results[0]
                    flash(
                        f"Importação BRAS concluída (UF {res['uf']} -> {res['arquivo']} | "
                        f"{res['linhas_raw']} linhas brutas, {res['linhas_materializadas']} materializadas).",
                        'success'
                    )
                else:
                    resumo = '; '.join(
                        f"{res['uf']} -> {res['arquivo']} ({res['linhas_raw']} brutas / {res['linhas_materializadas']} materializadas)"
                        for res in import_results
                    )
                    flash(
                        f"Importação BRAS concluída para {len(import_results)} UFs: {resumo}.",
                        'success'
                    )
        else:
            if fmt != 'fixed':
                flash('Importação SIMPRO suporta apenas arquivos de largura fixa.', 'danger')
                return _go_back()
            if not map_config:
                flash('Envie um arquivo de mapeamento para importação de largura fixa.', 'danger')
                return _go_back()

            import_results: list[dict[str, object]] = []
            for idx, uf in enumerate(uf_values):
                result = _import_simpro(
                    file_path=file_temp_path,
                    versao=versao,
                    fmt=fmt,
                    map_config=map_config,
                    encoding=encoding,
                    truncate=truncate if idx == 0 else False,
                    uf_default=uf,
                    aliquota_default=aliquota_value,
                )
                result['uf'] = uf
                import_results.append(result)
            if import_results:
                if len(import_results) == 1:
                    res = import_results[0]
                    flash(
                        f"Importação SIMPRO concluída (UF {res['uf']} -> {res['arquivo']} | "
                        f"{res['linhas_raw']} linhas brutas, {res['linhas_materializadas']} materializadas).",
                        'success'
                    )
                else:
                    resumo = '; '.join(
                        f"{res['uf']} -> {res['arquivo']} ({res['linhas_raw']} brutas / {res['linhas_materializadas']} materializadas)"
                        for res in import_results
                    )
                    flash(
                        f"Importação SIMPRO concluída para {len(import_results)} UFs: {resumo}.",
                        'success'
                    )
    except click.ClickException as exc:
        flash(str(exc), 'danger')
    except Exception as exc:  # noqa: BLE001
        flash(f'Erro inesperado ao importar: {exc}', 'danger')
    finally:
        if file_temp_path and file_temp_path.exists():
            file_temp_path.unlink(missing_ok=True)
        if map_temp_path and map_temp_path.exists():
            map_temp_path.unlink(missing_ok=True)

    return _go_back()

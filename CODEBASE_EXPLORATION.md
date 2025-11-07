# Sistema de Preços - Codebase Exploration Report

## Overview
**Project**: Sistema de Preços (Price Management System)  
**Technology Stack**: Flask (Python), MySQL, SQLAlchemy ORM  
**App Entry Point**: `/home/rafaelrenck/code/sistema_precos/app.py` (11,402 lines)  
**Total Routes**: 63 endpoints  
**Total Database Models**: 28 models  

---

## 1. DATABASE MODELS & RELATIONSHIPS

### Core Business Models

#### Users & Access Control
- **Usuario** (`usuarios` table)
  - Fields: id, nome, email, senha, perfil, acesso_insumos, acesso_consulta, acesso_contratos, acesso_tuss_rol, must_reset_senha, senha_atualizada_em, failed_login_attempts, locked_until, last_logout_at
  - Relationships: operadoras (many-to-many via usuario_operadoras), senhas_historico
  - Perfis: adm, adm de contrato, operadora

- **UsuarioSenhaHistorico** (`usuario_senhas_historico` table)
  - Tracks password history for security (PASSWORD_HISTORY_SIZE=5, PASSWORD_EXPIRATION_DAYS=90)
  - Fields: id, usuario_id, senha_hash, criada_em

- **AuditLog** (`audit_logs` table)
  - Comprehensive audit trail for all system actions
  - Fields: id, usuario_id, email_alvo, evento, ip, detalhes, criado_em

#### Health Insurance Operators
- **Operadora** (`operadoras` table)
  - Fields: id, nome, uf, cnpj, status
  - Relationships: tabelas (price tables), contratos (contracts), procedimentos (procedures), tetos_cbhpm (CBHPM ceilings)

- **ContractSummary** (`contratos_resumo` table)
  - Fields: id, prestador, tabela_honorarios, tabela_portes, valor_uco, inflator_deflator, filme_radiologico, observacoes, operadora_id, created_at, updated_at

#### Price Tables & Procedures
- **Tabela** (`tabelas` table)
  - Fields: id, nome, data_vigencia, prestador, tipo_tabela, uf, uco_valor, id_operadora
  - Relationships: procedimentos, cbhpm_itens, porte_valores, porte_anestesico_valores

- **Procedimento** (`procedimentos` table) - DTP (Diárias, Taxas, Pacotes)
  - Fields: id, codigo, descricao, valor, prestador, uf, id_tabela, operadora_id
  - Multi-operadora support

- **CBHPMItem** (`cbhpm_itens` table)
  - Comprehensive surgical procedure pricing model
  - Fields: codigo, procedimento, uf, porte, fracao_porte, valor_porte, total_porte, incidencias, filme, total_filme, uco, total_uco, porte_anestesico, valor_porte_anestesico, total_porte_anestesico, numero_auxiliares, total_auxiliares (1-4 aux), subtotal, id_tabela

- **CbhpmTeto** (`cbhpm_teto` table)
  - CBHPM pricing ceilings per operator
  - Primary Key: (codigo, operadora_id)
  - Fields: descricao, valor_total, updated_at

- **PorteValorItem** (`porte_valores` table)
  - Surgical porte (complexity) values
  - Fields: porte, valor, uf, id_tabela

- **PorteAnestesicoValorItem** (`porte_anestesico_valores` table)
  - Anesthesia porte values
  - Fields: porte_an, valor, uf, id_tabela

#### CBHPM Rules Engine
- **CBHPMRuleSet** (`cbhpm_rulesets` table)
  - Custom pricing rules for CBHPM calculations
  - Fields: id, nome, versao, descricao, ativo, regras (JSON), criado_em, atualizado_em
  - Default rules include reductions for simultaneous procedures, auxiliary percentages, UCO multiplier, film multiplier

#### TUSS/ROL Management
- **TussRolCorrelacao** (`tuss_rol_correlacoes` table)
  - TUSS (Tabela de Atos e Serviços de Saúde) and ROL (Referência de Orientação da Lei) codes
  - Fields: id, codigo (unique), descricao, consta_rol (boolean), atualizado_em
  - Indexes on codigo and consta_rol flag

---

## 2. INSUMOS (SUPPLIES/MATERIALS) DATA MODELS

### Import & Staging
- **ImportJob** (`insumo_import_jobs` table)
  - Tracks async import jobs for supplies
  - Status values: PENDING, RUNNING, SUCCESS, FAILED
  - Fields: id, origem (BRAS/SIMPRO), original_filename, data_path, status, message, total_linhas, linhas_materializadas, versao, aliquota, uf_list, params (JSON), created_at, started_at, finished_at

#### BRAS (Brazilian Supplies) Import Pipeline
- **Lote** (`lote` table)
  - Batch/Lot of BRAS import
  - Status: PENDENTE, VALIDADO, REPROVADO, PUBLICADO
  - Fields: id, fornecedor, aliquota_bp, periodo, sequencia, arquivo_label, hash_arquivo, total_itens, status, validado_em, publicado_em, created_at, updated_at

- **Publicacao** (`publicacao` table)
  - Published versions of BRAS data
  - Fields: id, fornecedor, aliquota_bp, periodo, sequencia, lote_id, publicado_em, etag_versao, criado_em

- **LinhaHash** (`linha_hash` table)
  - Row-level deduplication for BRAS imports
  - Fields: id, lote_id, item_chave, hash_linha, payload_snapshot, created_at, updated_at

- **BrasRaw** (`bras_raw` table)
  - Raw imported BRAS data (23 generic columns: col01-col23)
  - Fields: id, arquivo, linha_num, col01-col23, imported_at
  - Key columns: col01=lab_code, col02=lab_name, col04=product_name, col06=presentation, col07=price_pmc, col13=price_pfb, col17=EAN, col22=ANVISA, col20=product_code, col18=presentation_code

- **BrasFixedStage** (`bras_fixed_stage` table)
  - Stage table for fixed-width BRAS files
  - Fields: id, arquivo, linha_num, linha (full text), imported_at

- **BrasItemNormalized** (`bras_item_n` table)
  - Normalized BRAS data
  - Fields: laboratorio_codigo, laboratorio_nome, produto_codigo, produto_nome, apresentacao_codigo, apresentacao_descricao, ean, registro_anvisa, edicao, preco_pmc_pacote, preco_pfb_pacote, preco_pmc_unit, preco_pfb_unit, aliquota_ou_ipi, quantidade_embalagem, imported_at

#### SIMPRO (Sistema de Informações de Preços) Import Pipeline
- **SimproFixedStage** (`simpro_fixed_stage` table)
  - Stage table for fixed-width SIMPRO files
  - Fields: id, arquivo, linha_num, linha (full text), imported_at

- **SimproItemNormalized** (`simpro_item_norm` table)
  - Normalized SIMPRO data
  - Fields: codigo, codigo_alt, descricao, data_ref, tipo_reg, preco1-4, unidade, qtd_unidade, fabricante, anvisa, validade_anvisa, ean, situacao, versao, uf_referencia, tuss_prefix, tuss_numero, status_final, imported_at
  - Complex indexing on: codigo_interno, descricao, ean, anvisa, versao, tuss_numero

- **SimproItem** (`simpro_item` table)
  - Unified SIMPRO items (possibly materialized view or consolidated)
  - Fields: id, tuss, tiss, anvisa, descricao, preco, aliquota, fabricante, versao_tabela, data_atualizacao, uf_referencia, created_at, updated_at

### Catalog & Index Management
- **UfAliquota** (`uf_aliquota` table)
  - Tax rates (aliquota) per state per date
  - Primary Key: (uf, valid_from)
  - Fields: aliquota_bp, valid_to, is_current, created_at, updated_at

- **CatalogoBrasindice** (`mv_catalogo_vigente_brasindice` - MATERIALIZED VIEW)
  - Current active BRAS catalog by state/period
  - Key fields: uf, aliquota_bp, periodo, sequencia, item_id, produto_codigo, apresentacao_descricao, ean, registro_anvisa, preco_pmc_unit, preco_pfb_unit, preco_pmc_pacote, preco_pfb_pacote, laboratorio_nome, edicao, etag_versao

- **CatalogoSimpro** (`mv_catalogo_vigente_simpro` - MATERIALIZED VIEW)
  - Current active SIMPRO catalog by state/period
  - Key fields: uf, aliquota_bp, periodo, item_id, codigo, codigo_alt, descricao, preco1-4, qtd_unidade, fabricante, anvisa, validade_anvisa, ean, situacao, etag_versao

- **InsumoIndex** (`insumos_index` table)
  - Unified search index for all supplies (BRAS + SIMPRO)
  - Primary Key: (origem, item_id)
  - Fields: tuss, tiss, descricao, preco, aliquota, fabricante, anvisa, versao_tabela, data_atualizacao, uf_referencia, updated_at

- **InsumoContextoClinico** (`insumo_contexto_clinico` table)
  - Clinical context for supplies (DRG associations, usage frequency)
  - Fields: id, origem (BRAS/SIMPRO), item_id, drg, procedimento_codigo, procedimento_descricao, frequencia_relativa, custo_procedimento, substitutos_raw, narrativa, created_at, updated_at

---

## 3. API ENDPOINTS STRUCTURE

### Authentication Routes
- `GET/POST /login` - User login
- `GET /logout` - User logout
- `GET /health` - Health check
- `GET/POST /minha-senha` - Change password

### Main Feature Routes
- `GET /` - Dashboard (requires login)
- `GET /consulta-comparar` - Price comparison tool

### CBHPM Simulation APIs
- `POST /api/simulacao_cbhpm` - CBHPM procedure pricing calculation
- `POST /api/simulacao_cbhpm/pdf` - Export simulation to PDF
- `POST /api/simulacao_cbhpm/xlsx` - Export simulation to Excel
- `GET /api/simulacao_dtp` - DTP simulation

### Data Lookup APIs (Smart Filters)
- `GET /api/prestadores_por_codigo` - Providers by code
- `GET /api/versoes_por_codigo` - Table versions by code
- `GET /api/tabela-info/<int:table_id>` - Table information
- `GET /api/prestadores/<int:table_id>` - Providers in table
- `GET /api/versoes/<int:table_id>` - Versions of table
- `GET /api/tabelas-list` - List all tables
- `GET /api/dtp-codigos/<int:table_id>` - DTP codes in table
- `GET /api/dtp-prestadores/<int:table_id>` - DTP providers
- `GET /api/procedimentos/suggest` - Procedure autocomplete
- `GET /api/cbhpm/detalhe` - CBHPM detail lookup

### TUSS/ROL Management
- `GET /api/tuss-rol` - Get TUSS/ROL data
- `GET /api/tuss-rol/<codigo>` - Get specific TUSS/ROL code
- `GET/POST /admin/tuss-rol` - Admin TUSS/ROL management
- `GET/POST /tuss-rol` - User TUSS/ROL management

### Admin Management Routes
- `GET /gerenciar-usuarios` - User management page
- `GET /gerenciar-operadoras` - Operator management page
- `GET /gerenciar-tabelas` - Table management page
- `GET /admin/audit-trail` - Audit log viewing

### Operator Management APIs
- `GET/POST /api/operadoras` - List/create operators
- `PUT/DELETE /api/operadoras/<int:oid>` - Update/delete operator
- `GET/POST /operadoras/nova` - New operator form/submit
- `GET/POST /operadoras/<int:oid>/editar` - Edit operator form/submit

### User Management APIs
- `GET/POST /usuarios/novo` - New user form/submit
- `GET/POST /usuarios/<int:uid>/editar` - Edit user form/submit

### Contract Management Routes
- `GET/POST /contratos-resumo` - Contract summary management
- `POST /contratos-resumo/<int:cid>/excluir` - Delete contract

### CBHPM Ceiling (Teto) Management
- `GET /admin/tetos` - CBHPM ceilings admin page
- `POST /admin/tetos/import` - Import teto CSV
- `GET /admin/tetos/template.csv` - Download teto template
- `POST /admin/tetos/copy` - Copy teto between operators
- `POST /admin/tetos/<codigo>/delete` - Delete teto

### Table Import Routes
- `POST /tabelas/importar/diarias-taxas-pacotes` - Import DTP
- `POST /tabelas/importar/porte` - Import porte values
- `POST /tabelas/importar/porte-anestesico` - Import anesthesia porte values
- `POST /tabelas/importar/cbhpm` - Import CBHPM items
- `POST /admin/procedimentos/copy` - Copy procedures between tables
- `POST /tabelas/uco/definir` - Set UCO value
- `GET /tabelas/<int:tid>/itens` - View table items
- `POST /tabelas/<int:tid>/excluir` - Delete table

### Supplies (Insumos) Management
- `GET /insumos` - Main supplies page
- `GET /insumos/search` - Search supplies
- `GET /api/insumos/suggest` - Autocomplete supplies
- `GET /insumos/<origem>/<int:item_id>` - View supply details (BRAS or SIMPRO)
- `POST /insumos/<origem>/<int:item_id>/contexto` - Add clinical context
- `GET /insumos/export/xlsx` - Export supplies to Excel
- `GET/POST /insumos/aliquotas` - Tax rate management
- `POST /insumos/import` - Import supplies (BRAS/SIMPRO)
- `GET /insumos/import/jobs` - List import jobs
- `GET /insumos/import/jobs/<job_id>` - Get import job status

---

## 4. AVAILABLE DATA FOR ANALYTICS

### Financial/Pricing Data
- **Procedures & Pricing**: CBHPM codes with complex pricing (porte, anesthesia, auxiliaries, film, UCO)
- **DTP Pricing**: Diárias (daily rates), Taxas (fees), Pacotes (packages)
- **Price Comparisons**: Multiple operators, multiple table versions
- **Ceiling Prices**: CBHPM teto (maximum) prices per operator
- **UCO Values**: Unit Cost Value (Valor de Unidade de Custo) per table
- **Tax Rates**: Aliquota BP (Brazilian Pharmacopoeia tax) by state and date

### Supply Chain Data
- **BRAS Supplies**: Lab products with EAN, ANVISA registration, pricing (PMC unit/package, PFB unit/package)
- **SIMPRO Supplies**: Medical supplies with TUSS codes, multiple price points, ANVISA validity
- **Catalogs**: Current effective catalogs by state, period, and sequence
- **Supply Pricing**: 4 price tiers available for SIMPRO items

### Usage & Context Data
- **Clinical Context**: DRG associations, procedure frequency, procedure costs, substitute items
- **TUSS/ROL Mapping**: Service codes and whether they're in insurance coverage
- **Operator-Specific Data**: Operator names, UF, CNPJ, contracts per operator

### User & Access Data
- **User Profiles**: Operators, contracts admins, system admins with feature flags
- **Audit Trail**: All system actions with user, IP, timestamp, event type
- **Access Controls**: Per-user feature access (insumos, consulta, contratos, tuss_rol)

### Import & Metadata
- **Import Jobs**: Job status, completion %, version, aliquota, affected states
- **Batch Tracking**: Lote status, publication tracking, file hashes
- **Data Versions**: Table versions, SIMPRO versioning, BRAS edition tracking
- **Timestamps**: Creation, update, import, publication times on all records

---

## 5. EXISTING ANALYTICS/REPORTING CODE

### Export Functions
- `export_simulacao_pdf()` - Generates PDF reports for CBHPM simulations
  - Uses ReportLab library for PDF creation
  - Includes summary tables and financial breakdowns
  - Supports highlight formatting

- `export_simulacao_xlsx()` - Generates Excel exports for CBHPM simulations
  - Uses xlsxwriter for Excel creation
  - Multi-sheet support for detailed data

### Data Summarization
- `_insumo_summary(model_cls)` - Summary statistics for supplies
  - Cached summary data (5-minute TTL)
  - Count of items by type/status

### Cache Management
- TTL caches for performance:
  - `_insumo_cache` (1000 items, 5 minutes)
  - `_teto_cache` (500 items, 10 minutes)
  - `_rol_cache` (2000 items, 15 minutes)

### Audit & History Tracking
- Complete audit trail with `_register_audit()` function
- Audit log table includes: usuario, email_alvo, evento, ip, detalhes (JSON)
- Password history for compliance

---

## 6. FLASK APP CONFIGURATION

### Database Configuration
```python
DATABASE_URL = 'mysql+pymysql://root:@localhost/operadora_saude'
SQLALCHEMY_TRACK_MODIFICATIONS = False

# Connection Pool Settings (optimized for ~25 concurrent users):
- pool_size: 10
- max_overflow: 20 (total 30 connections)
- pool_recycle: 3600 (recycle after 1 hour)
- pool_pre_ping: True (verify connections before use)
- pool_timeout: 30 seconds
```

### Security Settings
```python
SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key')
PASSWORD_MIN_LENGTH = 10
PASSWORD_EXPIRATION_DAYS = 90
PASSWORD_HISTORY_SIZE = 5
MAX_FAILED_LOGIN_ATTEMPTS = 5
ACCOUNT_LOCK_MINUTES = 15
SESSION_LIFETIME_MINUTES = 120
```

### Session & Request Context
- `login_required` decorator for protected routes
- `admin_required` decorator for admin-only routes
- `feature_required` decorator for feature flags
- `g.current_user` for request-scoped user
- Session injection via context processor

### Decorators & Middleware
- Password policy enforcement (uppercase, lowercase, digit, special char)
- Password history checking to prevent reuse
- Account lockout after failed login attempts
- Session invalidation on password change
- Request context for IP logging

---

## 7. ROUTE STRUCTURE & ORGANIZATION

### Protected Routes Hierarchy
```
/ (Dashboard) - login_required
├── /consulta-comparar (Price comparison)
├── /insumos/* (Supply management)
├── /contratos-resumo (Contract management)
├── /tuss-rol (TUSS/ROL lookup - user accessible)
└── /admin/* (Admin panel)
    ├── /admin/tetos (Ceiling management)
    ├── /admin/tuss-rol (TUSS/ROL admin)
    ├── /admin/audit-trail
    ├── /gerenciar-usuarios
    ├── /gerenciar-operadoras
    └── /gerenciar-tabelas
```

### API vs Page Routes
- **Page Routes**: Return HTML templates, user-facing
- **API Routes**: Return JSON, usually start with `/api/`
- **Import Routes**: Handle file uploads and async jobs

---

## 8. KEY TECHNICAL DETAILS

### Multi-Operator Support
- All primary entities (Operadora, Tabela, Procedimento, ContractSummary, CBHPMTeto) support multi-operator scenarios
- Usuario can access multiple operadoras via many-to-many relationship
- Session maintains operadora_id and operadora_nome

### Data Import Pipeline (BRAS/SIMPRO)
1. **Raw Stage**: Store raw data as-is (BrasRaw, SimproFixedStage)
2. **Normalize**: Extract and clean data (BrasItemNormalized, SimproItemNormalized)
3. **Catalog**: Create current-period views (CatalogoBrasindice, CatalogoSimpro)
4. **Index**: Unified search index (InsumoIndex)
5. **Track**: Job status and audit trail (ImportJob, AuditLog)

### Calculation Engines
- **CBHPM**: Complex rule-based calculation with configurable rules (CBHPMRuleSet)
- **DTP**: Simple table lookups
- **Teto**: Maximum ceiling enforcement
- **Aliquota**: State-based tax rate application

### Type Safety & Validation
- Decimal precision: Most financial values are `Numeric(12,2)` or `Numeric(15,4)`
- Enum types for status (LoteStatus, ImportJobStatus)
- Index columns for frequently queried fields
- Check constraints for non-negative values

---

## 9. DEPENDENCIES & LIBRARIES

**Key Python Packages:**
- Flask (web framework)
- SQLAlchemy + Flask-SQLAlchemy (ORM)
- PyMySQL (MySQL driver)
- python-dotenv (environment configuration)
- Werkzeug (security, file handling)
- ReportLab (PDF generation)
- XlsxWriter (Excel export)
- cachetools (TTL caching)

**JavaScript Modules:**
- `/static/js/core/main.js` - Core application logic
- `/static/js/core/api.js` - API communication layer
- `/static/js/modules/consulta-comparar.js` - Price comparison module
- `/static/js/modules/auth.js` - Authentication handling
- `/static/js/autocomplete.js` - Autocomplete functionality

---

## 10. POTENTIAL ANALYTICS OPPORTUNITIES

1. **Pricing Analytics**
   - Price trends across operators and table versions
   - Procedure cost composition (porte vs UCO vs film vs auxiliaries)
   - Price variance by state (UF)

2. **Supply Chain Analytics**
   - Supplier/manufacturer analysis from BRAS/SIMPRO data
   - Product availability by region
   - Price point distribution

3. **Usage Analytics**
   - Most commonly searched procedures
   - User access patterns
   - Feature usage (consulta vs insumos vs contratos)

4. **Compliance Analytics**
   - TUSS/ROL coverage rates
   - Contract compliance (teto adherence)
   - Audit trail analysis

5. **Import Analytics**
   - Data quality metrics from imports
   - Update frequency and coverage
   - Version tracking and change detection

6. **Performance Analytics**
   - Query performance by operator/table size
   - User session patterns
   - System capacity utilization


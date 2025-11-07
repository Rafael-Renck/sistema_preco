# Database Schema Quick Reference

## Core Entity Relationships

```
┌─────────────────────────────────────────────────────────────────┐
│                      USER & SECURITY                            │
├─────────────────────────────────────────────────────────────────┤
│
├─ Usuario (usuarios)
│  ├─ perfil: adm, adm de contrato, operadora
│  ├─ operadoras [M-N] via usuario_operadoras
│  ├─ senhas_historico [1-N] UsuarioSenhaHistorico
│  ├─ acesso_insumos, acesso_consulta, acesso_contratos, acesso_tuss_rol
│  └─ failed_login_attempts, locked_until (security fields)
│
├─ UsuarioSenhaHistorico (usuario_senhas_historico)
│  └─ usuario_id [FK]
│
└─ AuditLog (audit_logs)
   └─ usuario_id [FK]
```

## Price Management Structure

```
┌──────────────────────────────────────────────────────────────────┐
│                  PRICE TABLES & PROCEDURES                       │
├──────────────────────────────────────────────────────────────────┤
│
Operadora (operadoras)
├─ 1 ──→ [N] Tabela (tabelas)
│         ├─ uco_valor: Unit Cost Value
│         ├─ 1 ──→ [N] Procedimento (procedimentos) [DTP]
│         │         ├─ codigo, descricao, valor
│         │         └─ operadora_id
│         │
│         ├─ 1 ──→ [N] CBHPMItem (cbhpm_itens)
│         │         ├─ codigo (procedure code)
│         │         ├─ porte (surgical complexity)
│         │         ├─ porte_anestesico (anesthesia)
│         │         ├─ numero_auxiliares (1-4)
│         │         ├─ valores: porte, filme, uco, auxiliares
│         │         └─ subtotal (calculated)
│         │
│         ├─ 1 ──→ [N] PorteValorItem (porte_valores)
│         │         └─ porte, valor, uf
│         │
│         └─ 1 ──→ [N] PorteAnestesicoValorItem (porte_anestesico_valores)
│                   └─ porte_an, valor, uf
│
├─ 1 ──→ [N] ContractSummary (contratos_resumo)
│         ├─ tabela_honorarios, tabela_portes
│         ├─ valor_uco, inflator_deflator
│         └─ filme_radiologico
│
└─ 1 ──→ [N] CbhpmTeto (cbhpm_teto)
          ├─ Primary Key: (codigo, operadora_id)
          └─ valor_total (maximum price)
```

## CBHPM Rules & Calculations

```
CBHPMRuleSet (cbhpm_rulesets)
├─ ativo (boolean - which rule set is active)
├─ regras (JSON)
│  ├─ descricao: Rule description
│  ├─ porte
│  │  └─ reducoes_simultaneos: [1.0, 0.5, 0.3, 0.2]
│  ├─ auxiliares
│  │  ├─ percentuais: [0.3, 0.2, 0.1, 0.1]
│  │  └─ max_por_porte: {0: 0, 1: 0, 2: 1, 3: 2, ...}
│  ├─ uco
│  │  └─ multiplicador: 1.0
│  └─ filme
│     └─ multiplicador: 1.0
└─ criado_em, atualizado_em

TUSS/ROL Codes (tuss_rol_correlacoes)
├─ codigo (TUSS service code) - UNIQUE
├─ descricao
└─ consta_rol (in insurance coverage?)
```

## Supply Chain (Insumos) - BRAS Pipeline

```
┌──────────────────────────────────────────────────────────────────┐
│               BRAS IMPORT & NORMALIZATION PIPELINE                │
├──────────────────────────────────────────────────────────────────┤

STAGE 1: RAW DATA
├─ BrasRaw (bras_raw)
│  ├─ col01: laboratorio_codigo
│  ├─ col02: laboratorio_nome
│  ├─ col04: produto_nome
│  ├─ col06: apresentacao_descricao
│  ├─ col07: preco_pmc (Price Medical Company)
│  ├─ col13: preco_pfb (Price Factory Base)
│  ├─ col17: ean (barcode)
│  ├─ col18: apresentacao_codigo
│  ├─ col20: produto_codigo
│  ├─ col22: registro_anvisa (ANVISA registration)
│  ├─ col14: edicao (edition)
│  ├─ arquivo, linha_num, imported_at
│  └─ [23 generic columns: col01-col23]
│
└─ BrasFixedStage (bras_fixed_stage) - for fixed-width files
   └─ Raw line text for processing

STAGE 2: NORMALIZATION
└─ BrasItemNormalized (bras_item_n)
   ├─ laboratorio_codigo, laboratorio_nome
   ├─ produto_codigo, produto_nome
   ├─ apresentacao_codigo, apresentacao_descricao
   ├─ ean, registro_anvisa, edicao
   ├─ preco_pmc_unit, preco_pfb_unit
   ├─ preco_pmc_pacote, preco_pfb_pacote
   ├─ aliquota_ou_ipi, quantidade_embalagem
   ├─ arquivo, linha_num, imported_at
   └─ [Indexes on: ean, produto_codigo, produto_nome, registro_anvisa, edicao]

STAGE 3: CATALOG (MATERIALIZED VIEW)
└─ CatalogoBrasindice (mv_catalogo_vigente_brasindice)
   ├─ uf, aliquota_bp, periodo, sequencia
   ├─ item_id (reference to normalized item)
   ├─ All normalized fields above
   ├─ etag_versao (for change tracking)
   ├─ etag_catalogo (catalog tracking)
   └─ [Current effective catalog for each state/period]

STAGE 4: UNIFIED INDEX
└─ InsumoIndex (insumos_index) [Key: origem + item_id]
   ├─ origem: 'BRAS' (enum)
   ├─ item_id: reference to normalized
   ├─ tuss, tiss, descricao (searchable)
   ├─ preco, aliquota, fabricante, anvisa
   ├─ versao_tabela, data_atualizacao, uf_referencia
   └─ updated_at
```

## Supply Chain (Insumos) - SIMPRO Pipeline

```
┌──────────────────────────────────────────────────────────────────┐
│               SIMPRO IMPORT & NORMALIZATION PIPELINE              │
├──────────────────────────────────────────────────────────────────┤

STAGE 1: RAW DATA
└─ SimproFixedStage (simpro_fixed_stage)
   ├─ arquivo, linha_num
   └─ linha (raw line text)

STAGE 2: NORMALIZATION
└─ SimproItemNormalized (simpro_item_norm)
   ├─ codigo, codigo_alt (alternative code)
   ├─ descricao
   ├─ data_ref (reference date)
   ├─ tipo_reg (record type)
   ├─ preco1, preco2, preco3, preco4 (4 price tiers)
   ├─ unidade, qtd_unidade
   ├─ fabricante (manufacturer)
   ├─ anvisa (ANVISA registration), validade_anvisa
   ├─ ean (barcode)
   ├─ situacao (status)
   ├─ versao (table version)
   ├─ tuss_prefix, tuss_numero (TUSS code parts)
   ├─ status_final (final status)
   ├─ uf_referencia, arquivo, linha_num
   └─ [Indexes: codigo, codigo_alt, descricao, anvisa, versao, tuss_numero]

STAGE 3: UNIFIED ITEMS
└─ SimproItem (simpro_item) - Consolidated/materialized
   ├─ tuss, tiss (service codes)
   ├─ anvisa, descricao
   ├─ preco, aliquota
   ├─ fabricante, versao_tabela, data_atualizacao
   └─ uf_referencia

STAGE 4: CATALOG (MATERIALIZED VIEW)
└─ CatalogoSimpro (mv_catalogo_vigente_simpro)
   ├─ uf, aliquota_bp, periodo, sequencia
   ├─ item_id (reference to normalized)
   ├─ codigo, codigo_alt, descricao, data_ref
   ├─ preco1-4, qtd_unidade, fabricante
   ├─ anvisa, validade_anvisa, ean, situacao
   ├─ etag_versao (version tracking)
   └─ etag_catalogo (catalog tracking)

STAGE 5: UNIFIED INDEX
└─ InsumoIndex (insumos_index) [Key: origem + item_id]
   ├─ origem: 'SIMPRO' (enum)
   ├─ item_id: reference to normalized
   ├─ tuss, tiss, descricao (searchable)
   ├─ preco, aliquota, fabricante, anvisa
   ├─ versao_tabela, data_atualizacao, uf_referencia
   └─ updated_at
```

## Clinical Context & Usage Data

```
InsumoContextoClinico (insumo_contexto_clinico)
├─ origem: 'BRAS' | 'SIMPRO'
├─ item_id: reference to insumo
├─ drg (Diagnosis-Related Group)
├─ procedimento_codigo, procedimento_descricao
├─ frequencia_relativa (usage frequency)
├─ custo_procedimento (procedure cost)
├─ substitutos_raw (JSON list of substitute items)
├─ narrativa (clinical notes)
├─ created_at, updated_at
└─ [Index on: origem + item_id]
```

## Tax & Aliquota Management

```
UfAliquota (uf_aliquota)
├─ Primary Key: (uf, valid_from)
├─ aliquota_bp (tax percentage as integer)
├─ valid_to (end date)
├─ is_current (current active rate)
├─ created_at, updated_at
└─ [Index on: uf + is_current]
```

## Import Job Tracking

```
ImportJob (insumo_import_jobs)
├─ id (UUID string)
├─ origem: 'BRAS' | 'SIMPRO'
├─ original_filename
├─ data_path (temporary file location)
├─ status: PENDING | RUNNING | SUCCESS | FAILED
├─ message (status message, max 500 chars)
├─ total_linhas (expected line count)
├─ linhas_materializadas (processed count)
├─ versao (version string)
├─ aliquota (tax rate used)
├─ uf_list (comma-separated states)
├─ params (JSON - processing parameters)
├─ created_at, started_at, finished_at
└─ [Indexes on: created_at, status]

BRAS Batch Tracking:
├─ Lote (lote) - Batch info
│  ├─ id, fornecedor, aliquota_bp, periodo, sequencia
│  ├─ status: PENDENTE | VALIDADO | REPROVADO | PUBLICADO
│  ├─ arquivo_label, hash_arquivo, total_itens
│  ├─ validado_em, publicado_em
│  └─ [Unique constraint: fornecedor + aliquota_bp + periodo + sequencia]
│
├─ Publicacao (publicacao) - Published versions
│  ├─ id, fornecedor, aliquota_bp, periodo, sequencia
│  ├─ lote_id [FK]
│  ├─ publicado_em, etag_versao
│  └─ [Unique constraint: fornecedor + aliquota_bp + periodo + sequencia]
│
└─ LinhaHash (linha_hash) - Row deduplication
   ├─ id, lote_id [FK]
   ├─ item_chave (unique key for row)
   ├─ hash_linha (hash of row content)
   ├─ payload_snapshot (full row JSON)
   └─ [Unique constraint: lote_id + item_chave]
```

## Model Count Summary

- **28 Database Models** (db.Model classes)
- **3 Enum Classes** (Status enums)
- **8 User/Security Models** (Usuario, UsuarioSenhaHistorico, AuditLog, etc.)
- **7 Price Management Models** (Tabela, Procedimento, CBHPMItem, etc.)
- **15 Supply Chain Models** (BRAS/SIMPRO pipelines, catalogs, indexes)
- **Multiple Foreign Key Relationships**
- **Comprehensive Indexing** for performance optimization

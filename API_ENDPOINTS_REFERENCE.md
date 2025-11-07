# API Endpoints Reference Guide

## Authentication & Session Management

| Endpoint | Method | Auth | Purpose |
|----------|--------|------|---------|
| `/login` | GET | None | Display login form |
| `/login` | POST | None | Process login (email, senha) |
| `/logout` | GET | Required | Logout user |
| `/minha-senha` | GET | Required | Change password form |
| `/minha-senha` | POST | Required | Process password change |
| `/health` | GET | None | Health check endpoint |

## Dashboard & Main Features

| Endpoint | Method | Auth | Purpose |
|----------|--------|------|---------|
| `/` | GET | Required | Main dashboard |
| `/consulta-comparar` | GET | Required | Price comparison page |

## CBHPM Simulation Endpoints

| Endpoint | Method | Auth | Purpose |
|----------|--------|------|---------|
| `/api/simulacao_cbhpm` | POST | Required | Calculate CBHPM procedure pricing |
| `/api/simulacao_cbhpm/pdf` | POST | Required | Export CBHPM simulation to PDF |
| `/api/simulacao_cbhpm/xlsx` | POST | Required | Export CBHPM simulation to Excel |
| `/api/simulacao_dtp` | GET | Required | Get DTP simulation data |

## Smart Filters & Data Lookup APIs

### Table & Provider Information
| Endpoint | Method | Auth | Parameters | Purpose |
|----------|--------|------|------------|---------|
| `/api/tabelas-list` | GET | Required | (optional query params) | List all price tables |
| `/api/tabela-info/<id>` | GET | Required | id: table ID | Get table details |
| `/api/prestadores/<id>` | GET | Required | id: table ID | List providers in table |
| `/api/versoes/<id>` | GET | Required | id: table ID | List table versions |
| `/api/prestadores_por_codigo` | GET | Required | codigo: procedure code | Get providers by code |
| `/api/versoes_por_codigo` | GET | Required | codigo: procedure code | Get versions by code |

### Procedure Data
| Endpoint | Method | Auth | Parameters | Purpose |
|----------|--------|------|------------|---------|
| `/api/procedimentos/suggest` | GET | Required | q: search query | Autocomplete procedures |
| `/api/dtp-codigos/<id>` | GET | Required | id: table ID | List DTP codes |
| `/api/dtp-prestadores/<id>` | GET | Required | id: table ID | List DTP providers |
| `/api/cbhpm/detalhe` | GET | Required | codigo: CBHPM code | Get CBHPM details |

## TUSS/ROL Management

| Endpoint | Method | Auth | Role | Purpose |
|----------|--------|------|------|---------|
| `/api/tuss-rol` | GET | Required | Any | Get TUSS/ROL codes |
| `/api/tuss-rol/<codigo>` | GET | Required | Any | Get specific TUSS code |
| `/tuss-rol` | GET | Required | User | Display TUSS lookup form |
| `/tuss-rol` | POST | Required | User | Search TUSS codes |
| `/admin/tuss-rol` | GET | Required | Admin | TUSS management page |
| `/admin/tuss-rol` | POST | Required | Admin | Update TUSS codes |

## Operator Management

| Endpoint | Method | Auth | Role | Purpose |
|----------|--------|------|------|---------|
| `/api/operadoras` | GET | Required | Admin | List operators |
| `/api/operadoras` | POST | Required | Admin | Create operator |
| `/api/operadoras/<id>` | PUT | Required | Admin | Update operator |
| `/api/operadoras/<id>` | DELETE | Required | Admin | Delete operator |
| `/operadoras/nova` | GET | Required | Admin | New operator form |
| `/operadoras/nova` | POST | Required | Admin | Submit new operator |
| `/operadoras/<id>/editar` | GET | Required | Admin | Edit operator form |
| `/operadoras/<id>/editar` | POST | Required | Admin | Submit operator edit |
| `/gerenciar-operadoras` | GET | Required | Admin | Operator management page |

## User Management

| Endpoint | Method | Auth | Role | Purpose |
|----------|--------|------|------|---------|
| `/usuarios/novo` | GET | Required | Admin | New user form |
| `/usuarios/novo` | POST | Required | Admin | Create user |
| `/usuarios/<id>/editar` | GET | Required | Admin | Edit user form |
| `/usuarios/<id>/editar` | POST | Required | Admin | Update user |
| `/gerenciar-usuarios` | GET | Required | Admin | User management page |

## Contract Management

| Endpoint | Method | Auth | Role | Purpose |
|----------|--------|------|------|---------|
| `/contratos-resumo` | GET | Required | Permitted | View contracts |
| `/contratos-resumo` | POST | Required | Permitted | Create/update contract |
| `/contratos-resumo/<id>/excluir` | POST | Required | Permitted | Delete contract |

## CBHPM Ceiling (Teto) Management

| Endpoint | Method | Auth | Role | Purpose |
|----------|--------|------|------|---------|
| `/admin/tetos` | GET | Required | Admin | View ceilings page |
| `/admin/tetos/template.csv` | GET | Required | Admin | Download CSV template |
| `/admin/tetos/import` | POST | Required | Admin | Import teto CSV |
| `/admin/tetos/copy` | POST | Required | Admin | Copy teto between operators |
| `/admin/tetos/<codigo>/delete` | POST | Required | Admin | Delete specific teto |

## Table Management

| Endpoint | Method | Auth | Role | Purpose |
|----------|--------|------|------|---------|
| `/gerenciar-tabelas` | GET | Required | Admin | Table management page |
| `/tabelas/<id>/itens` | GET | Required | Admin | View table items |
| `/tabelas/<id>/excluir` | POST | Required | Admin | Delete table |
| `/tabelas/uco/definir` | POST | Required | Admin | Set UCO value |

### Table Import Endpoints
| Endpoint | Method | Auth | Role | File Format | Purpose |
|----------|--------|------|------|-------------|---------|
| `/tabelas/importar/diarias-taxas-pacotes` | POST | Required | Admin | CSV | Import DTP |
| `/tabelas/importar/porte` | POST | Required | Admin | CSV | Import porte values |
| `/tabelas/importar/porte-anestesico` | POST | Required | Admin | CSV | Import anesthesia porte |
| `/tabelas/importar/cbhpm` | POST | Required | Admin | CSV | Import CBHPM items |
| `/admin/procedimentos/copy` | POST | Required | Admin | JSON | Copy procedures |

## Supplies (Insumos) Management

### Browse & Search
| Endpoint | Method | Auth | Parameters | Purpose |
|----------|--------|------|------------|---------|
| `/insumos` | GET | Required | q: search, origem: BRAS/SIMPRO | Main supplies page |
| `/insumos/search` | GET | Required | q: search, origem | Advanced search |
| `/api/insumos/suggest` | GET | Required | q: search, origem | Autocomplete supplies |
| `/insumos/<origem>/<id>` | GET | Required | origem: BRAS/SIMPRO, id: item ID | View supply details |

### Clinical Context
| Endpoint | Method | Auth | Purpose |
|----------|--------|------|---------|
| `/insumos/<origem>/<id>/contexto` | POST | Required | Add clinical context |

### Import & Management
| Endpoint | Method | Auth | Role | Purpose |
|----------|--------|------|------|---------|
| `/insumos/import` | POST | Required | Admin | Import BRAS/SIMPRO file |
| `/insumos/import/jobs` | GET | Required | Admin | List import jobs |
| `/insumos/import/jobs/<id>` | GET | Required | Admin | Get job status/details |
| `/insumos/export/xlsx` | GET | Required | Admin | Export supplies to Excel |
| `/insumos/aliquotas` | GET | Required | Admin | View tax rates |
| `/insumos/aliquotas` | POST | Required | Admin | Update tax rates |

## CBHPM Rules Management

| Endpoint | Method | Auth | Role | Purpose |
|----------|--------|------|------|---------|
| `/cbhpm/regras` | GET | Required | Admin | List rule sets |
| `/cbhpm/regras/nova` | GET | Required | Admin | New rule set form |
| `/cbhpm/regras/nova` | POST | Required | Admin | Create rule set |
| `/cbhpm/regras/<id>/editar` | GET | Required | Admin | Edit rule set form |
| `/cbhpm/regras/<id>/editar` | POST | Required | Admin | Update rule set |
| `/cbhpm/regras/<id>/ativar` | POST | Required | Admin | Activate rule set |

## Audit & Administrative

| Endpoint | Method | Auth | Role | Purpose |
|----------|--------|------|------|---------|
| `/admin/audit-trail` | GET | Required | Admin | View audit logs |

---

## Common Request Parameters

### Search/Filter Parameters
- `q` - Search query string
- `origem` - Data origin: 'BRAS' or 'SIMPRO'
- `codigo` - Procedure/item code
- `uf` - State code (2 chars)
- `operadora_id` - Operator ID

### Pagination Parameters
- `page` - Page number (1-indexed)
- `limit` - Items per page
- `offset` - Number of items to skip

### Sort Parameters
- `sort_by` - Field to sort by
- `sort_dir` - Sort direction: 'asc' or 'desc'

---

## Response Status Codes

| Code | Meaning |
|------|---------|
| 200 | Success |
| 201 | Created |
| 204 | No content |
| 400 | Bad request (validation error) |
| 401 | Unauthorized (login required) |
| 403 | Forbidden (insufficient permissions) |
| 404 | Not found |
| 500 | Server error |

---

## Authentication & Authorization

### Login Decorators
- `@login_required` - Must be logged in
- `@admin_required` - Must be admin user
- `@feature_required(feature_key)` - Must have feature enabled

### User Roles
- `adm` - Full administrator
- `adm de contrato` - Contract administrator
- `operadora` - Operator user

### Feature Flags (Per User)
- `acesso_insumos` - Can access supplies module
- `acesso_consulta` - Can access comparison module
- `acesso_contratos` - Can access contracts module
- `acesso_tuss_rol` - Can access TUSS/ROL lookup

---

## Import Job Status Values

| Status | Meaning |
|--------|---------|
| PENDING | Awaiting processing |
| RUNNING | Currently processing |
| SUCCESS | Completed successfully |
| FAILED | Failed with error |

---

## Error Response Format

```json
{
  "error": "Error message",
  "details": "Additional details",
  "code": "ERROR_CODE"
}
```

---

## CBHPM Calculation Parameters

When calling `/api/simulacao_cbhpm`, typically POST body includes:
- `codigo_cbhpm` - Procedure code
- `tabela_id` - Price table ID
- `operadora_id` - Operator ID
- `uf` - State (for rule application)
- (Optional) `ruleset_id` - Specific rule set to apply
- (Optional) Custom values for porte, film, UCO, auxiliaries

---

## Rate Limiting & Caching

- Insumo cache: 1000 items, 5-minute TTL
- Teto cache: 500 items, 10-minute TTL
- ROL cache: 2000 items, 15-minute TTL

---

## File Upload Specifications

### CBHPM Import
- Format: CSV/Excel
- Required columns: codigo, porte, valor_porte, etc.
- Max file size: Configured in Flask

### BRAS Import
- Format: CSV (delimited or fixed-width)
- Columns: 23 columns (col01-col23)
- Encoding: Auto-detected (utf-8, utf-8-sig, latin-1, cp1252)

### SIMPRO Import
- Format: CSV (fixed-width)
- Columns: codigo, descricao, preco1-4, etc.
- Encoding: Auto-detected

### Teto Import
- Format: CSV
- Columns: codigo, descricao, valor_total
- Template available at: `/admin/tetos/template.csv`


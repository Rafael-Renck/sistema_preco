# Codebase Exploration - Complete Summary

This document provides a comprehensive overview of the **Sistema de Preços** codebase. Three detailed reference documents have been created:

## Core Documentation Files (Just Created)

### 1. CODEBASE_EXPLORATION.md (19 KB)
**Comprehensive Overview of the Entire System**

This is the main exploration document covering:
- **Database Models & Relationships**: All 28 models with field definitions
- **Insumos (Supplies) Data Models**: BRAS and SIMPRO import pipelines with 4-stage processing
- **API Endpoints Structure**: All 63 endpoints organized by feature
- **Available Data for Analytics**: Financial, supply chain, usage, and metadata
- **Existing Analytics/Reporting Code**: PDF/Excel exports, data summarization
- **Flask Configuration**: Database, security, and session settings
- **Route Structure**: Hierarchy and organization of all endpoints
- **Key Technical Details**: Multi-operator support, calculation engines, type safety
- **Potential Analytics Opportunities**: 6 categories for data insights

### 2. DATABASE_SCHEMA_REFERENCE.md (11 KB)
**Visual Entity Relationships & Data Structure**

Contains:
- **ASCII Diagrams** showing entity relationships
- **Price Management Structure**: Operadora → Tabela → Procedimento → CBHPMItem hierarchy
- **BRAS Import Pipeline**: 4-stage normalization process (Raw → Normalized → Catalog → Index)
- **SIMPRO Import Pipeline**: 5-stage processing for medical supplies
- **Clinical Context Data**: DRG associations and usage frequency
- **Tax & Aliquota Management**: State-based tax rates by date
- **Import Job Tracking**: Batch tracking and row-level deduplication
- **Model Count Summary**: Quick reference to model quantities

### 3. API_ENDPOINTS_REFERENCE.md (11 KB)
**Complete REST API Endpoint Reference**

Includes:
- **Authentication Routes**: Login, logout, password change, health check
- **Dashboard & Main Features**: Dashboard and comparison tools
- **CBHPM Simulation Endpoints**: Price calculations and PDF/Excel exports
- **Smart Filters APIs**: Table lookup, provider info, procedure search
- **TUSS/ROL Management**: Service codes and insurance coverage
- **Administrator Endpoints**: Operator, user, contract management
- **Supplies Management**: Import, search, export functionality
- **CBHPM Rules Management**: Custom pricing rule sets
- **Audit & Administrative**: Audit log viewing
- **Request Parameters**: Search, pagination, sorting conventions
- **Status Codes & Error Formats**: Standard HTTP responses
- **File Upload Specifications**: CBHPM, BRAS, SIMPRO, and Teto formats

---

## Key Findings Summary

### Architecture Overview

**Framework**: Flask (Python) with SQLAlchemy ORM  
**Database**: MySQL with PyMySQL driver  
**Frontend**: HTML/CSS/JavaScript with Bootstrap  
**Authentication**: Session-based with password hashing

**Scale**: 
- 28 database models
- 63 API endpoints
- Support for 25 concurrent users
- TTL caching for performance

### Primary Data Entities

#### Business Entities
1. **Operadoras** (Insurance operators) - Multi-operator support
2. **Tabelas** (Price tables) - CBHPM, DTP, porte values
3. **Procedimentos** (Procedures) - DTP (Daily, Tax, Package rates)
4. **CBHPMItens** (Surgical procedures) - Complex pricing with components
5. **Contratos** (Contracts) - Contract summaries per operator

#### Supply Chain
1. **BRAS Data** - Pharmaceutical products (EAN, ANVISA, pricing)
2. **SIMPRO Data** - Medical supplies (TUSS codes, multiple price tiers)
3. **Catalogs** - Materialized views for current active data by state/period
4. **InsumoIndex** - Unified search index across BRAS and SIMPRO

#### Administrative
1. **Usuarios** - Users with roles (adm, contract admin, operator)
2. **AuditLog** - Complete system action history
3. **TussRolCodigos** - Service/procedure code reference
4. **CBHPMRuleSet** - Custom pricing calculation rules

### Key Features

#### 1. Price Comparison & Simulation
- Compare CBHPM codes across operators and table versions
- Simulate costs with complex pricing rules
- Export to PDF and Excel formats
- DTP (diárias, taxas, pacotes) lookups

#### 2. Supply Management (Insumos)
- Import BRAS (pharmaceutical) and SIMPRO (medical supplies) data
- Advanced search with autocomplete
- Clinical context attachment (DRG, usage frequency)
- Tax rate (aliquota) management by state and date
- Export functionality

#### 3. Administrative Control
- Multi-operator management
- User management with feature flags
- Contract management
- CBHPM pricing ceiling (teto) management
- Audit trail tracking

#### 4. Smart Filters
- Dynamic procedure/provider lookups
- Table version management
- Code-based searching
- State-based filtering

### Data Available for Analytics

#### Financial Data
- Procedure pricing with component breakdown (porte, anesthesia, film, UCO, auxiliaries)
- Multiple price tiers per supply item
- Price comparisons across operators and time periods
- Ceiling/maximum prices (teto)

#### Clinical Data
- DRG (Diagnosis-Related Group) associations
- Procedure frequency in treatment plans
- Substitute item recommendations
- TUSS/ROL insurance coverage mapping

#### Operational Data
- Import job tracking and status
- Data version tracking
- Update frequency and coverage by state
- User access patterns (audit trail)

#### Administrative Data
- User login attempts and lockouts
- Feature access per user
- Contract details per operator
- System action history

### Technical Highlights

#### Security Features
- Password hashing (pbkdf2:sha256)
- Password history enforcement (prevents reuse)
- Password expiration (90 days)
- Account lockout after failed attempts (5 attempts → 15 min lock)
- Session invalidation on logout
- Comprehensive audit logging with IP tracking

#### Performance Optimizations
- Connection pooling (10 base + 20 overflow = 30 total)
- TTL caching at multiple levels
- Strategic database indexing
- Query optimization for frequently accessed data
- Prepared statements for SQL injection prevention

#### Data Integrity
- Foreign key constraints
- Unique constraints on critical fields
- Check constraints for valid data ranges
- Transaction support for multi-step operations
- Row-level deduplication for imports (LinhaHash)

---

## File Structure

```
/home/rafaelrenck/code/sistema_precos/
├── app.py                              (11,402 lines - Main Flask app)
├── CODEBASE_EXPLORATION.md             (This exploration report)
├── DATABASE_SCHEMA_REFERENCE.md        (Entity relationships)
├── API_ENDPOINTS_REFERENCE.md          (REST API catalog)
│
├── static/
│   ├── js/
│   │   ├── core/
│   │   │   ├── main.js
│   │   │   ├── api.js
│   │   │   └── utils.js
│   │   └── modules/
│   │       ├── consulta-comparar.js
│   │       ├── auth.js
│   │       └── modal.js
│   └── css/
│       ├── modern-design.css
│       ├── components-modal.css
│       └── others...
│
├── templates/
│   ├── index.html                      (Dashboard)
│   ├── consulta-comparar.html          (Price comparison)
│   ├── insumos.html                    (Supplies management)
│   ├── contratos_resumo.html           (Contracts)
│   ├── login.html
│   └── components/
│       └── (Reusable UI components)
│
├── migrations/
│   └── versions/                       (Database migration history)
│
└── tests/
    ├── test_simulacao.py
    ├── test_insumos.py
    └── conftest.py
```

---

## Quick Start for Development

### Understanding the Database Layer
1. Start with `DATABASE_SCHEMA_REFERENCE.md` to understand entity relationships
2. Review the model definitions in `app.py` (lines 305-4534)
3. Look at specific models for your feature area

### Working with APIs
1. Find your endpoint in `API_ENDPOINTS_REFERENCE.md`
2. Check required parameters and response format
3. Look at the corresponding route handler in `app.py`

### Adding Analytics
1. Review "Available Data for Analytics" in `CODEBASE_EXPLORATION.md`
2. Decide which models to query
3. Use the export functions (`export_simulacao_pdf`, `export_simulacao_xlsx`) as templates
4. Create new routes following existing patterns

### Debugging Issues
1. Check the database schema first
2. Verify API endpoint requirements
3. Review audit logs at `/admin/audit-trail`
4. Look at import job status at `/insumos/import/jobs`

---

## Important Models to Know

### Core Models
- **Usuario**: User with roles and feature access
- **Operadora**: Insurance company with multiple tables
- **Tabela**: Price table (CBHPM, DTP, etc.)
- **Procedimento**: Procedure codes with values (DTP)
- **CBHPMItem**: Surgical procedures with complex pricing

### Supply Chain Models
- **InsumoIndex**: Unified search across BRAS and SIMPRO
- **CatalogoBrasindice**: Current BRAS catalog by state/period
- **CatalogoSimpro**: Current SIMPRO catalog by state/period
- **ImportJob**: Track async imports
- **InsumoContextoClinico**: Clinical usage context

### Management Models
- **AuditLog**: Complete audit trail
- **CBHPMRuleSet**: Custom pricing rules (JSON)
- **CbhpmTeto**: Pricing ceilings per operator
- **TussRolCorrelacao**: Service code reference

---

## Next Steps for Analytics Development

Based on the exploration, here are recommended areas for analytics:

1. **Pricing Analytics Dashboard**
   - Average procedure costs by operator
   - Price trends over time
   - Variance analysis across states

2. **Supply Chain Analytics**
   - Supplier/manufacturer analysis
   - Product availability by region
   - Price point comparisons

3. **Usage Analytics**
   - Most searched procedures
   - User access patterns
   - Feature adoption rates

4. **Compliance Analytics**
   - TUSS/ROL coverage rates
   - Teto adherence tracking
   - Contract compliance monitoring

5. **Data Quality Analytics**
   - Import success rates
   - Data completeness by source
   - Update frequency tracking

---

## Document Links

For detailed information, please refer to:

1. **CODEBASE_EXPLORATION.md** - Complete system overview
2. **DATABASE_SCHEMA_REFERENCE.md** - Visual entity relationships
3. **API_ENDPOINTS_REFERENCE.md** - REST API catalog

These three documents together provide a complete understanding of the Sistema de Preços codebase.

---

**Exploration Date**: November 7, 2025  
**Codebase Version**: Current main branch  
**Total Lines of Code**: 11,402 (app.py)  
**Total Models**: 28  
**Total Endpoints**: 63  


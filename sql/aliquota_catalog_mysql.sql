-- ---------------------------------------------------------------------------
-- Catálogo por alíquota – estrutura auxiliar (MySQL 8.x)
-- ---------------------------------------------------------------------------

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;
SET FOREIGN_KEY_CHECKS = 0;

DROP VIEW IF EXISTS mv_catalogo_vigente_simpro;
DROP VIEW IF EXISTS mv_catalogo_vigente_brasindice;
DROP VIEW IF EXISTS vw_canon_simpro;
DROP VIEW IF EXISTS vw_canon_brasindice;
DROP VIEW IF EXISTS vw_aliquota_vigente;
DROP VIEW IF EXISTS vw_cadastro_aliquota;
DROP TABLE IF EXISTS linha_hash;
DROP TABLE IF EXISTS publicacao;
DROP TABLE IF EXISTS lote;
DROP TABLE IF EXISTS uf_aliquota;

CREATE TABLE uf_aliquota (
    uf            CHAR(2)        NOT NULL,
    valid_from    DATE           NOT NULL,
    valid_to      DATE           DEFAULT NULL,
    aliquota_bp   INT            NOT NULL CHECK (aliquota_bp >= 0),
    is_current    TINYINT(1)     NOT NULL DEFAULT 1,
    created_at    DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (uf, valid_from)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE lote (
    id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    fornecedor    VARCHAR(50)     NOT NULL,
    aliquota_bp   INT              NOT NULL,
    periodo       CHAR(6)          NOT NULL,
    sequencia     SMALLINT         NOT NULL,
    arquivo_label VARCHAR(255)     NOT NULL,
    hash_arquivo  VARCHAR(128)     DEFAULT NULL,
    total_itens   INT              DEFAULT NULL,
    status        ENUM('PENDENTE','VALIDADO','REPROVADO','PUBLICADO') NOT NULL DEFAULT 'PENDENTE',
    validado_em   DATETIME         DEFAULT NULL,
    publicado_em  DATETIME         DEFAULT NULL,
    created_at    DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_lote_identidade (fornecedor, aliquota_bp, periodo, sequencia)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE publicacao (
    id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    fornecedor    VARCHAR(50)     NOT NULL,
    aliquota_bp   INT              NOT NULL,
    periodo       CHAR(6)          NOT NULL,
    sequencia     SMALLINT         NOT NULL,
    lote_id       BIGINT UNSIGNED  NOT NULL,
    publicado_em  DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    etag_versao   VARCHAR(128)     NOT NULL,
    criado_em     DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_publicacao_identidade (fornecedor, aliquota_bp, periodo, sequencia),
    KEY idx_publicacao_fornecedor (fornecedor, aliquota_bp),
    CONSTRAINT fk_publicacao_lote FOREIGN KEY (lote_id) REFERENCES lote(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE linha_hash (
    id               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    lote_id          BIGINT UNSIGNED NOT NULL,
    item_chave       VARCHAR(255)    NOT NULL,
    hash_linha       VARCHAR(128)    NOT NULL,
    payload_snapshot LONGTEXT        NULL,
    created_at       DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_linha_hash_item (lote_id, item_chave),
    KEY idx_linha_hash_lote (lote_id),
    CONSTRAINT fk_linha_hash_lote FOREIGN KEY (lote_id) REFERENCES lote(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_uf_aliquota_current ON uf_aliquota(uf, is_current);
CREATE INDEX idx_lote_status ON lote(status);
CREATE INDEX idx_lote_arquivo_label ON lote(arquivo_label);

CREATE OR REPLACE VIEW vw_cadastro_aliquota AS
SELECT uf, aliquota_bp, valid_from, valid_to, is_current
  FROM uf_aliquota;

CREATE OR REPLACE VIEW vw_aliquota_vigente AS
SELECT uf, aliquota_bp, valid_from, valid_to, is_current
  FROM uf_aliquota
 WHERE is_current = 1;

CREATE OR REPLACE VIEW vw_canon_brasindice AS
SELECT
    CAST('BRASINDICE' AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci AS fornecedor,
    p.aliquota_bp,
    p.periodo,
    p.sequencia,
    p.etag_versao,
    b.id AS item_id,
    b.produto_codigo,
    b.apresentacao_codigo,
    b.produto_nome,
    b.apresentacao_descricao,
    b.ean,
    b.registro_anvisa,
    b.preco_pmc_unit,
    b.preco_pfb_unit,
    b.preco_pmc_pacote,
    b.preco_pfb_pacote,
    b.laboratorio_nome,
    b.edicao,
    b.imported_at
FROM publicacao p
JOIN lote l              ON l.id = p.lote_id
JOIN bras_item_n b       ON b.arquivo COLLATE utf8mb4_unicode_ci = l.arquivo_label
WHERE p.fornecedor = CAST('BRASINDICE' AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
  AND p.publicado_em = (
        SELECT MAX(p2.publicado_em)
          FROM publicacao p2
         WHERE p2.fornecedor = p.fornecedor
           AND p2.aliquota_bp = p.aliquota_bp
    );

CREATE OR REPLACE VIEW vw_canon_simpro AS
SELECT
    CAST('SIMPRO' AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci AS fornecedor,
    p.aliquota_bp,
    p.periodo,
    p.sequencia,
    p.etag_versao,
    s.id AS item_id,
    s.codigo,
    s.codigo_alt,
    s.descricao,
    s.data_ref,
    s.preco1,
    s.preco2,
    s.preco3,
    s.preco4,
    s.qtd_unidade,
    s.fabricante,
    s.anvisa,
    s.validade_anvisa,
    s.ean,
    s.situacao,
    s.imported_at
FROM publicacao p
JOIN lote l              ON l.id = p.lote_id
JOIN simpro_item_norm s  ON s.arquivo COLLATE utf8mb4_unicode_ci = l.arquivo_label
WHERE p.fornecedor = CAST('SIMPRO' AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
  AND p.publicado_em = (
        SELECT MAX(p2.publicado_em)
          FROM publicacao p2
         WHERE p2.fornecedor = p.fornecedor
           AND p2.aliquota_bp = p.aliquota_bp
    );

CREATE OR REPLACE VIEW mv_catalogo_vigente_brasindice AS
SELECT
    ua.uf,
    c.aliquota_bp,
    ua.valid_from,
    ua.valid_to,
    c.periodo,
    c.sequencia,
    c.etag_versao,
    c.item_id,
    c.produto_codigo,
    c.apresentacao_codigo,
    c.produto_nome,
    c.apresentacao_descricao,
    c.ean,
    c.registro_anvisa,
    c.preco_pmc_unit,
    c.preco_pfb_unit,
    c.preco_pmc_pacote,
    c.preco_pfb_pacote,
    c.laboratorio_nome,
    c.edicao,
    c.imported_at,
    CONCAT(CAST('BRASINDICE:' AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci, ua.uf, ':', c.etag_versao) AS etag_catalogo
FROM vw_aliquota_vigente ua
JOIN vw_canon_brasindice c ON c.aliquota_bp = ua.aliquota_bp;

CREATE OR REPLACE VIEW mv_catalogo_vigente_simpro AS
SELECT
    ua.uf,
    c.aliquota_bp,
    ua.valid_from,
    ua.valid_to,
    c.periodo,
    c.sequencia,
    c.etag_versao,
    c.item_id,
    c.codigo,
    c.codigo_alt,
    c.descricao,
    c.data_ref,
    c.preco1,
    c.preco2,
    c.preco3,
    c.preco4,
    c.qtd_unidade,
    c.fabricante,
    c.anvisa,
    c.validade_anvisa,
    c.ean,
    c.situacao,
    c.imported_at,
    CONCAT(CAST('SIMPRO:' AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci, ua.uf, ':', c.etag_versao) AS etag_catalogo
FROM vw_aliquota_vigente ua
JOIN vw_canon_simpro c ON c.aliquota_bp = ua.aliquota_bp;

INSERT INTO uf_aliquota (uf, valid_from, valid_to, aliquota_bp, is_current)
VALUES
    ('AC', '2024-01-01', NULL, 1700, 1),
    ('AL', '2024-01-01', NULL, 1900, 1),
    ('AM', '2024-01-01', NULL, 1800, 1),
    ('AP', '2024-01-01', NULL, 1700, 1),
    ('BA', '2024-01-01', NULL, 1950, 1),
    ('CE', '2024-01-01', NULL, 2000, 1),
    ('DF', '2024-01-01', NULL, 1800, 1),
    ('ES', '2024-01-01', NULL, 1700, 1),
    ('GO', '2024-01-01', NULL, 1900, 1),
    ('MA', '2024-01-01', NULL, 1700, 1),
    ('MG', '2024-01-01', NULL, 1900, 1),
    ('MS', '2024-01-01', NULL, 1700, 1),
    ('MT', '2024-01-01', NULL, 1700, 1),
    ('PA', '2024-01-01', NULL, 1700, 1),
    ('PB', '2024-01-01', NULL, 1900, 1),
    ('PE', '2024-01-01', NULL, 2000, 1),
    ('PI', '2024-01-01', NULL, 1900, 1),
    ('PR', '2024-01-01', NULL, 1900, 1),
    ('RJ', '2024-01-01', NULL, 2000, 1),
    ('RN', '2024-01-01', NULL, 1900, 1),
    ('RO', '2024-01-01', NULL, 1700, 1),
    ('RR', '2024-01-01', NULL, 1700, 1),
    ('RS', '2024-01-01', NULL, 1900, 1),
    ('SC', '2024-01-01', NULL, 1900, 1),
    ('SE', '2024-01-01', NULL, 1900, 1),
    ('SP', '2024-01-01', NULL, 2000, 1),
    ('TO', '2024-01-01', NULL, 1700, 1)
ON DUPLICATE KEY UPDATE
    aliquota_bp = VALUES(aliquota_bp),
    valid_to    = VALUES(valid_to),
    is_current  = VALUES(is_current),
    updated_at  = CURRENT_TIMESTAMP;

SET FOREIGN_KEY_CHECKS = 1;

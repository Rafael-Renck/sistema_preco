-- SIMPRO Fixed-Width Import Pipeline
-- -----------------------------------
-- Configuração completa para importar um arquivo TXT de largura fixa
-- (sem delimitadores) para o MySQL 8.x.
-- O pipeline segue as etapas:
--   [A] staging bruto
--   [B] tabela normalizada
--   [C] LOAD DATA
--   [D] normalização via INSERT ... SELECT
--   [E] rotinas de reimportação
--   [F] consultas de referência

/* ------------------------------------------------------------------ */
/* [A] Criação da tabela de staging (linha crua)                      */
/* ------------------------------------------------------------------ */
CREATE TABLE IF NOT EXISTS bras_fixed_stage (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    arquivo VARCHAR(255) NOT NULL,
    linha_num INT NOT NULL,
    linha TEXT NOT NULL,
    imported_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_bras_fixed_stage_arquivo (arquivo)
) ENGINE=InnoDB DEFAULT CHARSET = utf8mb4;

/* ------------------------------------------------------------------ */
/* [B] Criação da tabela normalizada com colunas tipadas               */
/* ------------------------------------------------------------------ */
CREATE TABLE IF NOT EXISTS bras_item_norm (
    id BIGINT UNSIGNED NOT NULL,
    codigo_item VARCHAR(10) NOT NULL,
    codigo_item_alt VARCHAR(10) NULL,
    descricao VARCHAR(255) NOT NULL,
    data_ref DATE NULL,
    tipo_reg CHAR(1) NULL,
    preco1 DECIMAL(15,2) NULL,
    preco2 DECIMAL(15,2) NULL,
    preco3 DECIMAL(15,2) NULL,
    preco4 DECIMAL(15,2) NULL,
    unidade VARCHAR(8) NULL,
    qtd_unidade INT NULL,
    fabricante VARCHAR(40) NULL,
    registro_anvisa VARCHAR(20) NULL,
    validade_anvisa DATE NULL,
    ean VARCHAR(20) NULL,
    situacao VARCHAR(20) NULL,
    codigo_extra_1 VARCHAR(30) NULL,
    codigo_extra_2 VARCHAR(30) NULL,
    grupo_grem VARCHAR(20) NULL,
    arquivo VARCHAR(255) NOT NULL,
    imported_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_bras_item_norm_codigo (codigo_item),
    KEY idx_bras_item_norm_descricao (descricao),
    KEY idx_bras_item_norm_ean (ean),
    KEY idx_bras_item_norm_anvisa (registro_anvisa)
) ENGINE=InnoDB DEFAULT CHARSET = utf8mb4;

/* ------------------------------------------------------------------ */
/* [C] Script LOAD DATA para carregar a staging                        */
/* ------------------------------------------------------------------ */
-- Ajuste os parâmetros abaixo para o arquivo a ser importado.
SET @arquivo := '392025.TXT';
SET @path    := '/caminho/para/392025.TXT';
SET @charset := 'latin1';   -- use 'utf8mb4' se o arquivo estiver em UTF-8
SET @lines   := '\n';       -- use '\r\n' para arquivos gerados no Windows

-- Reinicia o contador de linhas antes do LOAD DATA
SET @row := 0;

LOAD DATA LOCAL INFILE @path
INTO TABLE bras_fixed_stage
CHARACTER SET @charset
FIELDS TERMINATED BY '\t'        -- ignorado (sem delimitadores reais)
LINES TERMINATED BY @lines
(@linha)
SET arquivo   = @arquivo,
    linha_num = (@row := @row + 1),
    linha     = @linha;

/* ------------------------------------------------------------------ */
/* [D] Normalização: explode a linha fixa em colunas tipadas           */
/* ------------------------------------------------------------------ */
INSERT INTO bras_item_norm (
    id,
    codigo_item,
    codigo_item_alt,
    descricao,
    data_ref,
    tipo_reg,
    preco1,
    preco2,
    preco3,
    preco4,
    unidade,
    qtd_unidade,
    fabricante,
    registro_anvisa,
    validade_anvisa,
    ean,
    situacao,
    codigo_extra_1,
    codigo_extra_2,
    grupo_grem,
    arquivo,
    imported_at
)
SELECT
    s.id,
    TRIM(SUBSTRING(s.linha,   1, 10))                                      AS codigo_item,
    NULLIF(TRIM(SUBSTRING(s.linha, 16, 10)), '')                           AS codigo_item_alt,
    RTRIM(SUBSTRING(s.linha,  30, 92))                                     AS descricao,       -- 30–121
    NULLIF(STR_TO_DATE(SUBSTRING(s.linha, 123, 8), '%d%m%Y'), '0000-00-00') AS data_ref,
    NULLIF(SUBSTRING(s.linha, 131, 1), '')                                 AS tipo_reg,
    CAST(SUBSTRING(s.linha, 132, 12) AS UNSIGNED) / 100.0                  AS preco1,
    CAST(SUBSTRING(s.linha, 144, 12) AS UNSIGNED) / 100.0                  AS preco2,
    CAST(SUBSTRING(s.linha, 156, 12) AS UNSIGNED) / 100.0                  AS preco3,
    CAST(SUBSTRING(s.linha, 168, 12) AS UNSIGNED) / 100.0                  AS preco4,
    NULLIF(RTRIM(SUBSTRING(s.linha, 200,  8)), '')                         AS unidade,
    NULLIF(CAST(SUBSTRING(s.linha, 209,  6) AS UNSIGNED), 0)               AS qtd_unidade,
    NULLIF(RTRIM(SUBSTRING(s.linha, 230, 24)), '')                         AS fabricante,
    NULLIF(RTRIM(SUBSTRING(s.linha, 280, 20)), '')                         AS registro_anvisa,
    NULLIF(STR_TO_DATE(SUBSTRING(s.linha, 301, 8), '%d%m%Y'), '0000-00-00') AS validade_anvisa,
    NULLIF(TRIM(REPLACE(SUBSTRING(s.linha, 310, 16), '+', '')), '')        AS ean,
    NULLIF(RTRIM(SUBSTRING(s.linha, 330, 20)), '')                         AS situacao,
    NULLIF(RTRIM(SUBSTRING(s.linha, 360, 20)), '')                         AS codigo_extra_1,
    NULLIF(RTRIM(SUBSTRING(s.linha, 380, 20)), '')                         AS codigo_extra_2,
    NULLIF(RTRIM(SUBSTRING(s.linha, 400, 20)), '')                         AS grupo_grem,
    s.arquivo,
    s.imported_at
FROM bras_fixed_stage AS s
WHERE s.arquivo = @arquivo;

/* ------------------------------------------------------------------ */
/* [E] Rotinas de reimportação                                        */
/* ------------------------------------------------------------------ */
-- Reimportar apenas um arquivo específico
DELETE FROM bras_item_norm   WHERE arquivo = @arquivo;
DELETE FROM bras_fixed_stage WHERE arquivo = @arquivo;
-- Em seguida execute novamente as etapas [C] e [D].

-- Limpeza total das tabelas
TRUNCATE TABLE bras_item_norm;
TRUNCATE TABLE bras_fixed_stage;

/* ------------------------------------------------------------------ */
/* [F] Consultas de referência                                        */
/* ------------------------------------------------------------------ */
-- Busca por EAN específico
SELECT *
FROM bras_item_norm
WHERE ean = '7891234567890'
LIMIT 1;

-- Busca textual por descrição
SELECT codigo_item, descricao, fabricante, preco1, preco2
FROM bras_item_norm
WHERE descricao LIKE '%AMOXICILINA%';

-- Itens com situação "VIGENTE"
SELECT descricao, situacao
FROM bras_item_norm
WHERE situacao = 'VIGENTE'
LIMIT 50;

/* ------------------------------------------------------------------ */
/* Observações                                                        */
/* ------------------------------------------------------------------ */
-- * Ajuste as posições/length de SUBSTRING caso o layout do TXT mude.
-- * Se algum preço vier com quatro casas decimais, troque "/ 100.0" por "/ 10000.0".
-- * Garanta que o servidor MySQL aceite LOAD DATA LOCAL INFILE; Caso não aceite,
--   substitua a etapa [C] por um script auxiliar (ex.: Python) que faça INSERT em lote.
-- * Após grandes importações, considere ANALYZE TABLE para atualizar estatísticas.

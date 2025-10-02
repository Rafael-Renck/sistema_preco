-- ---------------------------------------------------------------------------
-- Catálogo por alíquota – estrutura auxiliar para Brasíndice / SIMPRO
-- Banco alvo: PostgreSQL 13+
-- ---------------------------------------------------------------------------

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'lote_status') THEN
    CREATE TYPE lote_status AS ENUM ('PENDENTE','VALIDADO','REPROVADO','PUBLICADO');
  END IF;
END;$$;

CREATE TABLE IF NOT EXISTS uf_aliquota (
    uf             VARCHAR(2)   NOT NULL,
    valid_from     DATE         NOT NULL,
    valid_to       DATE         NULL,
    aliquota_bp    INTEGER      NOT NULL CHECK (aliquota_bp >= 0),
    is_current     BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT pk_uf_aliquota PRIMARY KEY (uf, valid_from)
);

CREATE TABLE IF NOT EXISTS lote (
    id             BIGSERIAL      PRIMARY KEY,
    fornecedor     VARCHAR(50)    NOT NULL,
    aliquota_bp    INTEGER        NOT NULL CHECK (aliquota_bp >= 0),
    periodo        CHAR(6)        NOT NULL,
    sequencia      SMALLINT       NOT NULL CHECK (sequencia IN (1,2)),
    arquivo_label  VARCHAR(255)   NOT NULL,
    hash_arquivo   VARCHAR(128)   NULL,
    total_itens    INTEGER        NULL,
    status         lote_status    NOT NULL DEFAULT 'PENDENTE',
    validado_em    TIMESTAMPTZ    NULL,
    publicado_em   TIMESTAMPTZ    NULL,
    created_at     TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ    NOT NULL DEFAULT now(),
    CONSTRAINT uq_lote_identidade UNIQUE (fornecedor, aliquota_bp, periodo, sequencia)
);

CREATE TABLE IF NOT EXISTS publicacao (
    id             BIGSERIAL     PRIMARY KEY,
    fornecedor     VARCHAR(50)   NOT NULL,
    aliquota_bp    INTEGER       NOT NULL,
    periodo        CHAR(6)       NOT NULL,
    sequencia      SMALLINT      NOT NULL,
    lote_id        BIGINT        NOT NULL REFERENCES lote(id) ON DELETE CASCADE,
    publicado_em   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    etag_versao    VARCHAR(128)  NOT NULL,
    criado_em      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT uq_publicacao_identidade UNIQUE (fornecedor, aliquota_bp, periodo, sequencia)
);

CREATE TABLE IF NOT EXISTS linha_hash (
    id               BIGSERIAL    PRIMARY KEY,
    lote_id          BIGINT       NOT NULL REFERENCES lote(id) ON DELETE CASCADE,
    item_chave       VARCHAR(255) NOT NULL,
    hash_linha       VARCHAR(128) NOT NULL,
    payload_snapshot TEXT         NULL,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_linha_hash UNIQUE (lote_id, item_chave)
);

CREATE INDEX IF NOT EXISTS idx_uf_aliquota_current ON uf_aliquota(uf, is_current);
CREATE INDEX IF NOT EXISTS idx_lote_status ON lote(status);
CREATE INDEX IF NOT EXISTS idx_lote_arquivo_label ON lote(arquivo_label);
CREATE INDEX IF NOT EXISTS idx_publicacao_fornecedor ON publicacao(fornecedor, aliquota_bp);
CREATE INDEX IF NOT EXISTS idx_linha_hash_lote ON linha_hash(lote_id);

-- ---------------------------------------------------------------------------
-- View de manutenção da vigência (CRUD amigável via INSTEAD OF trigger)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW vw_cadastro_aliquota AS
SELECT
    uf,
    aliquota_bp,
    valid_from,
    valid_to,
    is_current
FROM uf_aliquota
ORDER BY uf, valid_from DESC;

CREATE OR REPLACE FUNCTION trg_vw_cadastro_aliquota_upsert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_valid_from  DATE;
    v_valid_to    DATE;
BEGIN
    v_valid_from := COALESCE(NEW.valid_from, CURRENT_DATE);
    v_valid_to   := NEW.valid_to;
    IF v_valid_to IS NOT NULL AND v_valid_to < v_valid_from THEN
        RAISE EXCEPTION 'valid_to (% ) não pode ser anterior a valid_from (%).', v_valid_to, v_valid_from;
    END IF;

    IF TG_OP = 'INSERT' THEN
        -- impedir sobreposição de vigência
        PERFORM 1
          FROM uf_aliquota
         WHERE uf = NEW.uf
           AND daterange(valid_from, COALESCE(valid_to, 'infinity'::date), '[]') &&
               daterange(v_valid_from, COALESCE(v_valid_to, 'infinity'::date), '[]');
        IF FOUND THEN
            RAISE EXCEPTION 'Já existe alíquota vigente para % no intervalo informado.', NEW.uf;
        END IF;

        IF COALESCE(NEW.is_current, TRUE) THEN
            UPDATE uf_aliquota
               SET is_current = FALSE,
                   valid_to   = COALESCE(valid_to, v_valid_from - 1)
             WHERE uf = NEW.uf
               AND is_current;
        END IF;

        INSERT INTO uf_aliquota AS dst (uf, valid_from, valid_to, aliquota_bp, is_current)
        VALUES (NEW.uf, v_valid_from, v_valid_to, NEW.aliquota_bp,
                COALESCE(NEW.is_current, v_valid_to IS NULL OR v_valid_to >= CURRENT_DATE))
        ON CONFLICT (uf, valid_from)
        DO UPDATE SET
            valid_to   = EXCLUDED.valid_to,
            aliquota_bp = EXCLUDED.aliquota_bp,
            is_current = EXCLUDED.is_current,
            updated_at = now();
        RETURN NEW;

    ELSIF TG_OP = 'UPDATE' THEN
        IF NEW.valid_from <> OLD.valid_from THEN
            RAISE EXCEPTION 'valid_from é imutável; remova e cadastre novamente.';
        END IF;

        PERFORM 1
          FROM uf_aliquota
         WHERE uf = NEW.uf
           AND valid_from <> NEW.valid_from
           AND daterange(valid_from, COALESCE(valid_to, 'infinity'::date), '[]') &&
               daterange(v_valid_from, COALESCE(v_valid_to, 'infinity'::date), '[]');
        IF FOUND THEN
            RAISE EXCEPTION 'Atualização criaria sobreposição de vigência para %.', NEW.uf;
        END IF;

        UPDATE uf_aliquota
           SET valid_to    = v_valid_to,
               aliquota_bp = NEW.aliquota_bp,
               is_current  = COALESCE(NEW.is_current, (v_valid_to IS NULL OR v_valid_to >= CURRENT_DATE)),
               updated_at  = now()
         WHERE uf = NEW.uf
           AND valid_from = NEW.valid_from;
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_vw_cadastro_aliquota ON vw_cadastro_aliquota;
CREATE TRIGGER trg_vw_cadastro_aliquota
    INSTEAD OF INSERT OR UPDATE ON vw_cadastro_aliquota
    FOR EACH ROW EXECUTE FUNCTION trg_vw_cadastro_aliquota_upsert();

-- ---------------------------------------------------------------------------
-- View de alíquota vigente (uma linha por UF)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW vw_aliquota_vigente AS
SELECT DISTINCT ON (uf)
    uf,
    aliquota_bp,
    valid_from,
    valid_to,
    is_current
FROM uf_aliquota
WHERE is_current
ORDER BY uf, valid_from DESC;

-- ---------------------------------------------------------------------------
-- Funções utilitárias para hashing e sincronização dos lotes
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fn_supplier_payload(
    p_fornecedor TEXT,
    p_arquivo_label TEXT
) RETURNS TABLE(item_chave TEXT, linha_hash TEXT, payload_text TEXT) AS $$
    SELECT * FROM (
        SELECT
            CONCAT_WS('|',
                      COALESCE(b.produto_codigo,''),
                      COALESCE(b.apresentacao_codigo,''),
                      COALESCE(b.ean,'')) AS item_chave,
            encode(digest(convert_to(payload::text, 'UTF8'), 'sha256'), 'hex')     AS linha_hash,
            payload::text                                                        AS payload_text
        FROM (
            SELECT
                jsonb_build_object(
                    'produto_codigo', b.produto_codigo,
                    'apresentacao_codigo', b.apresentacao_codigo,
                    'ean', b.ean,
                    'registro_anvisa', b.registro_anvisa,
                    'preco_pmc_unit', b.preco_pmc_unit,
                    'preco_pfb_unit', b.preco_pfb_unit,
                    'preco_pmc_pacote', b.preco_pmc_pacote,
                    'preco_pfb_pacote', b.preco_pfb_pacote,
                    'laboratorio_nome', b.laboratorio_nome,
                    'edicao', b.edicao,
                    'linha_num', b.linha_num
                ) AS payload,
                b.produto_codigo,
                b.apresentacao_codigo,
                b.ean
            FROM bras_item_n b
            WHERE upper(p_fornecedor) = 'BRASINDICE'
              AND b.arquivo = p_arquivo_label
        ) s
        UNION ALL
        SELECT
            CONCAT_WS('|', COALESCE(s.codigo,''), COALESCE(s.ean,''))                AS item_chave,
            encode(digest(convert_to(payload::text, 'UTF8'), 'sha256'), 'hex')       AS linha_hash,
            payload::text                                                           AS payload_text
        FROM (
            SELECT
                jsonb_build_object(
                    'codigo', s.codigo,
                    'codigo_alt', s.codigo_alt,
                    'descricao', s.descricao,
                    'data_ref', s.data_ref,
                    'tipo_reg', s.tipo_reg,
                    'preco1', s.preco1,
                    'preco2', s.preco2,
                    'preco3', s.preco3,
                    'preco4', s.preco4,
                    'fabricante', s.fabricante,
                    'anvisa', s.anvisa,
                    'validade_anvisa', s.validade_anvisa,
                    'ean', s.ean,
                    'situacao', s.situacao,
                    'linha_num', s.linha_num
                ) AS payload,
                s.codigo,
                s.ean
            FROM simpro_item_norm s
            WHERE upper(p_fornecedor) = 'SIMPRO'
              AND s.arquivo = p_arquivo_label
        ) s
    ) payloads;
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION ingestir_arquivo(
    p_fornecedor    TEXT,
    p_aliquota_bp   INTEGER,
    p_periodo       CHAR(6),
    p_sequencia     SMALLINT,
    p_arquivo_label TEXT,
    p_hash_arquivo  TEXT DEFAULT NULL
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_fornecedor TEXT := upper(p_fornecedor);
    v_lote_id    BIGINT;
    v_total      INTEGER;
    v_hash       TEXT;
BEGIN
    CREATE TEMP TABLE tmp_linha_hash ON COMMIT DROP AS
    SELECT *
      FROM fn_supplier_payload(v_fornecedor, p_arquivo_label)
     ORDER BY item_chave;

    SELECT COUNT(*) INTO v_total FROM tmp_linha_hash;
    IF v_total = 0 THEN
        RAISE EXCEPTION 'Nenhum item encontrado para o fornecedor % e arquivo %.', v_fornecedor, p_arquivo_label;
    END IF;

    SELECT encode(digest(convert_to(string_agg(item_chave || ':' || linha_hash, '||'), 'UTF8'), 'sha256'), 'hex')
      INTO v_hash
      FROM tmp_linha_hash;

    IF p_hash_arquivo IS NOT NULL THEN
        v_hash := p_hash_arquivo;
    END IF;

    INSERT INTO lote AS l (fornecedor, aliquota_bp, periodo, sequencia, arquivo_label,
                           hash_arquivo, total_itens, status, validado_em)
    VALUES (v_fornecedor, p_aliquota_bp, p_periodo, p_sequencia, p_arquivo_label,
            v_hash, v_total, 'VALIDADO', now())
    ON CONFLICT (fornecedor, aliquota_bp, periodo, sequencia)
    DO UPDATE SET
        arquivo_label = EXCLUDED.arquivo_label,
        hash_arquivo  = EXCLUDED.hash_arquivo,
        total_itens   = EXCLUDED.total_itens,
        status        = 'VALIDADO',
        validado_em   = now(),
        updated_at    = now()
    RETURNING id INTO v_lote_id;

    INSERT INTO linha_hash AS lh (lote_id, item_chave, hash_linha, payload_snapshot, created_at, updated_at)
    SELECT v_lote_id, item_chave, linha_hash, payload_text, now(), now()
      FROM tmp_linha_hash
    ON CONFLICT (lote_id, item_chave)
    DO UPDATE SET
        hash_linha       = EXCLUDED.hash_linha,
        payload_snapshot = EXCLUDED.payload_snapshot,
        updated_at       = now();

    DELETE FROM linha_hash
     WHERE lote_id = v_lote_id
       AND item_chave NOT IN (SELECT item_chave FROM tmp_linha_hash);

    RETURN v_lote_id;
END;
$$;

CREATE OR REPLACE FUNCTION publicar_lote(
    p_fornecedor  TEXT,
    p_aliquota_bp INTEGER,
    p_periodo     CHAR(6),
    p_sequencia   SMALLINT
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_fornecedor TEXT := upper(p_fornecedor);
    v_lote       lote;
    v_publicacao_id BIGINT;
BEGIN
    SELECT * INTO v_lote
      FROM lote
     WHERE fornecedor = v_fornecedor
       AND aliquota_bp = p_aliquota_bp
       AND periodo = p_periodo
       AND sequencia = p_sequencia;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Lote não encontrado (% %, %/%).', v_fornecedor, p_aliquota_bp, p_periodo, p_sequencia;
    END IF;

    IF v_lote.status NOT IN ('VALIDADO','PUBLICADO') THEN
        RAISE EXCEPTION 'Lote % está em status %; publique apenas lotes validados.', v_lote.id, v_lote.status;
    END IF;

    IF v_lote.status = 'PUBLICADO' THEN
        SELECT id INTO v_publicacao_id
          FROM publicacao
         WHERE lote_id = v_lote.id
         ORDER BY publicado_em DESC
         LIMIT 1;
        RETURN COALESCE(v_publicacao_id, v_lote.id);
    END IF;

    UPDATE lote
       SET status = 'PUBLICADO',
           publicado_em = now(),
           updated_at = now()
     WHERE id = v_lote.id;

    INSERT INTO publicacao (fornecedor, aliquota_bp, periodo, sequencia, lote_id, etag_versao)
    VALUES (v_fornecedor, p_aliquota_bp, p_periodo, p_sequencia,
            v_lote.id, concat_ws('-', v_fornecedor, p_periodo, p_sequencia))
    RETURNING id INTO v_publicacao_id;

    PERFORM _refresh_materialized_catalogs(v_fornecedor);
    RETURN v_publicacao_id;
END;
$$;

CREATE OR REPLACE FUNCTION _refresh_materialized_catalogs(p_fornecedor TEXT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_view TEXT;
BEGIN
    FOR v_view IN SELECT unnest(ARRAY[
        CASE WHEN upper(p_fornecedor) = 'BRASINDICE' THEN 'mv_catalogo_vigente_brasindice' END,
        CASE WHEN upper(p_fornecedor) = 'SIMPRO' THEN 'mv_catalogo_vigente_simpro' END
    ]) LOOP
        EXIT WHEN v_view IS NULL;
        BEGIN
            EXECUTE format('REFRESH MATERIALIZED VIEW IF EXISTS %I', v_view);
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Falha ao atualizar %: %', v_view, SQLERRM;
        END;
    END LOOP;
END;
$$;

-- ---------------------------------------------------------------------------
-- Views canônicas: colapso por alíquota (sem duplicar por UF)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW vw_canon_brasindice AS
WITH last_pub AS (
    SELECT DISTINCT ON (p.aliquota_bp)
        p.aliquota_bp,
        p.periodo,
        p.sequencia,
        p.etag_versao,
        l.arquivo_label
    FROM publicacao p
    JOIN lote l ON l.id = p.lote_id
    WHERE p.fornecedor = 'BRASINDICE'
    ORDER BY p.aliquota_bp, p.periodo DESC, p.sequencia DESC, p.publicado_em DESC
)
SELECT
    'BRASINDICE'::TEXT AS fornecedor,
    lp.aliquota_bp,
    lp.periodo,
    lp.sequencia,
    lp.etag_versao,
    b.id                AS item_id,
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
FROM bras_item_n b
JOIN last_pub lp ON lp.arquivo_label = b.arquivo;

CREATE OR REPLACE VIEW vw_canon_simpro AS
WITH last_pub AS (
    SELECT DISTINCT ON (p.aliquota_bp)
        p.aliquota_bp,
        p.periodo,
        p.sequencia,
        p.etag_versao,
        l.arquivo_label
    FROM publicacao p
    JOIN lote l ON l.id = p.lote_id
    WHERE p.fornecedor = 'SIMPRO'
    ORDER BY p.aliquota_bp, p.periodo DESC, p.sequencia DESC, p.publicado_em DESC
)
SELECT
    'SIMPRO'::TEXT AS fornecedor,
    lp.aliquota_bp,
    lp.periodo,
    lp.sequencia,
    lp.etag_versao,
    s.id                AS item_id,
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
FROM simpro_item_norm s
JOIN last_pub lp ON lp.arquivo_label = s.arquivo;

-- ---------------------------------------------------------------------------
-- Catálogo vigente por UF (materialized views) – carregar com REFRESH
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_catalogo_vigente_brasindice AS
SELECT
    ua.uf,
    b.aliquota_bp,
    b.periodo,
    b.sequencia,
    b.etag_versao,
    b.item_id,
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
    b.imported_at,
    concat('BRASINDICE', ':', ua.uf, ':', b.etag_versao) AS etag_catalogo
FROM vw_aliquota_vigente ua
JOIN vw_canon_brasindice b ON b.aliquota_bp = ua.aliquota_bp
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_catalogo_vigente_simpro AS
SELECT
    ua.uf,
    s.aliquota_bp,
    s.periodo,
    s.sequencia,
    s.etag_versao,
    s.item_id,
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
    s.imported_at,
    concat('SIMPRO', ':', ua.uf, ':', s.etag_versao) AS etag_catalogo
FROM vw_aliquota_vigente ua
JOIN vw_canon_simpro s ON s.aliquota_bp = ua.aliquota_bp
WITH NO DATA;

-- ---------------------------------------------------------------------------
-- Dados iniciais de UF → alíquota (ajuste conforme necessidade)
-- ---------------------------------------------------------------------------

INSERT INTO uf_aliquota (uf, valid_from, valid_to, aliquota_bp, is_current)
VALUES
    ('AC', '2024-01-01', NULL, 1700, TRUE),
    ('AL', '2024-01-01', NULL, 1900, TRUE),
    ('AM', '2024-01-01', NULL, 1800, TRUE),
    ('AP', '2024-01-01', NULL, 1700, TRUE),
    ('BA', '2024-01-01', NULL, 1950, TRUE),
    ('CE', '2024-01-01', NULL, 2000, TRUE),
    ('DF', '2024-01-01', NULL, 1800, TRUE),
    ('ES', '2024-01-01', NULL, 1700, TRUE),
    ('GO', '2024-01-01', NULL, 1900, TRUE),
    ('MA', '2024-01-01', NULL, 1700, TRUE),
    ('MG', '2024-01-01', NULL, 1900, TRUE),
    ('MS', '2024-01-01', NULL, 1700, TRUE),
    ('MT', '2024-01-01', NULL, 1700, TRUE),
    ('PA', '2024-01-01', NULL, 1700, TRUE),
    ('PB', '2024-01-01', NULL, 1900, TRUE),
    ('PE', '2024-01-01', NULL, 2000, TRUE),
    ('PI', '2024-01-01', NULL, 1900, TRUE),
    ('PR', '2024-01-01', NULL, 1900, TRUE),
    ('RJ', '2024-01-01', NULL, 2000, TRUE),
    ('RN', '2024-01-01', NULL, 1900, TRUE),
    ('RO', '2024-01-01', NULL, 1700, TRUE),
    ('RR', '2024-01-01', NULL, 1700, TRUE),
    ('RS', '2024-01-01', NULL, 1900, TRUE),
    ('SC', '2024-01-01', NULL, 1900, TRUE),
    ('SE', '2024-01-01', NULL, 1900, TRUE),
    ('SP', '2024-01-01', NULL, 2000, TRUE),
    ('TO', '2024-01-01', NULL, 1700, TRUE)
ON CONFLICT (uf, valid_from) DO NOTHING;

COMMIT;

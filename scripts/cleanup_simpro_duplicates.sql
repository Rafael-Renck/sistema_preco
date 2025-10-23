-- Script para limpar duplicatas antigas do SIMPRO
-- USO: Execute ANTES de fazer novas importações
-- ATENÇÃO: Faça backup antes de executar!

-- 1. Ver quantas duplicatas existem atualmente
SELECT
    'DUPLICATAS ATUAIS' as status,
    COUNT(*) as total_registros,
    COUNT(DISTINCT CONCAT(codigo, descricao, preco2)) as registros_unicos,
    COUNT(*) - COUNT(DISTINCT CONCAT(codigo, descricao, preco2)) as duplicatas
FROM simpro_item_norm
WHERE versao = '2025-41';

-- 2. Ver detalhes das duplicatas por arquivo
SELECT
    arquivo,
    COUNT(*) as total_linhas,
    uf_referencia
FROM simpro_item_norm
WHERE versao = '2025-41'
GROUP BY arquivo, uf_referencia
ORDER BY arquivo;

-- 3. BACKUP: Criar tabela temporária com dados atuais
CREATE TABLE IF NOT EXISTS simpro_item_norm_backup AS
SELECT * FROM simpro_item_norm
WHERE versao = '2025-41';

SELECT 'Backup criado: simpro_item_norm_backup' as status;

-- 4. ESTRATÉGIA DE LIMPEZA:
-- Manter apenas 1 registro por (codigo, descricao, preco) e consolidar as UFs

-- 4a. Criar tabela temporária com registros consolidados
CREATE TEMPORARY TABLE simpro_consolidated AS
SELECT
    MIN(id) as id_manter,
    arquivo,
    codigo,
    descricao,
    preco1,
    preco2,
    preco3,
    preco4,
    fabricante,
    anvisa,
    versao,
    data_ref,
    -- Consolida todas as UFs em uma única string
    GROUP_CONCAT(DISTINCT uf_referencia ORDER BY uf_referencia SEPARATOR ',') as uf_consolidado,
    COUNT(*) as total_duplicatas
FROM simpro_item_norm
WHERE versao = '2025-41'
GROUP BY
    codigo,
    descricao,
    preco1,
    preco2,
    fabricante,
    anvisa,
    versao,
    data_ref;

-- 4b. Ver estatísticas da consolidação
SELECT
    'DEPOIS DA CONSOLIDACAO' as status,
    COUNT(*) as registros_unicos,
    SUM(total_duplicatas) as total_original,
    SUM(total_duplicatas) - COUNT(*) as duplicatas_removidas
FROM simpro_consolidated;

-- 5. OPCIONAL: Deletar duplicatas (DESCOMENTE PARA EXECUTAR)
/*
-- ATENÇÃO: Isto vai DELETAR registros! Faça backup antes!

-- 5a. Deletar todos os registros antigos
DELETE FROM simpro_item_norm
WHERE versao = '2025-41';

-- 5b. Inserir registros consolidados
INSERT INTO simpro_item_norm (
    arquivo, linha_num, codigo, descricao,
    preco1, preco2, preco3, preco4,
    fabricante, anvisa, versao, data_ref,
    uf_referencia, imported_at
)
SELECT
    sc.arquivo,
    0 as linha_num,  -- Será resetado
    sc.codigo,
    sc.descricao,
    sc.preco1,
    sc.preco2,
    sc.preco3,
    sc.preco4,
    sc.fabricante,
    sc.anvisa,
    sc.versao,
    sc.data_ref,
    sc.uf_consolidado,
    NOW() as imported_at
FROM simpro_consolidated sc;

-- 5c. Atualizar InsumoIndex
DELETE FROM insumos_index
WHERE origem = 'SIMPRO' AND versao_tabela = '2025-41';

-- Recriar índice será feito automaticamente na próxima sincronização
SELECT 'Limpeza concluída! Execute nova importação para recriar InsumoIndex' as status;
*/

-- 6. VERIFICAÇÃO FINAL
SELECT
    'VERIFICACAO FINAL' as status,
    COUNT(*) as registros_atuais
FROM simpro_item_norm
WHERE versao = '2025-41';

-- 7. Limpar tabela temporária
DROP TEMPORARY TABLE IF EXISTS simpro_consolidated;

-- INSTRUÇÕES:
-- 1. Faça backup do banco: mysqldump -u root -p operadora_saude > backup_$(date +%Y%m%d).sql
-- 2. Execute este script SEM as linhas comentadas primeiro (apenas visualização)
-- 3. Se os números estiverem corretos, DESCOMENTE a seção 5 e execute novamente
-- 4. Faça uma nova importação SIMPRO com a correção aplicada

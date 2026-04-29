-- Índice FULLTEXT para acelerar busca por termo (campo q) na tela Simpro & Brasíndice.
-- Execute UMA VEZ no MySQL/MariaDB de produção (ajuste o nome do schema se necessário).
--
-- Depois defina no ambiente da aplicação:
--   INSUMOS_FULLTEXT_SEARCH=1
--
-- Observação: palavras muito curtas podem ser ignoradas pelo servidor conforme ft_min_token_size.

ALTER TABLE insumos_index
  ADD FULLTEXT INDEX ft_insumo_desc_fab (descricao, fabricante);

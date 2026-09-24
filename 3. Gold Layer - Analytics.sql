-- ============================================================
-- GOLD LAYER - Modelo Analítico e Agregações
-- Dimensões: dim_cliente, dim_data, dim_produto, dim_tipo_suporte
-- Fatos: fact_vendas, fact_suporte
-- Gold tables: vendas por produto, ranking clientes, suporte por tipo
-- View: vw_cliente_360
-- ============================================================

-- ============================================================
-- DIMENSÕES
-- ============================================================

-- dim_cliente
CREATE OR REPLACE TABLE desafio_grupo1.gold.dim_cliente AS
SELECT
  id_cliente,
  nome,
  sexo,
  data_nascimento,
  cidade,
  estado,
  data_cadastro
FROM desafio_grupo1.silver.silver_clientes;

-- dim_data: derivada das datas de venda
CREATE OR REPLACE TABLE desafio_grupo1.gold.dim_data AS
SELECT DISTINCT
  data_venda AS data,
  YEAR(data_venda) AS ano,
  MONTH(data_venda) AS mes,
  QUARTER(data_venda) AS trimestre,
  DATE_FORMAT(data_venda, 'MMMM') AS nome_mes,
  DAYOFWEEK(data_venda) AS dia_semana,
  DATE_FORMAT(data_venda, 'EEEE') AS nome_dia_semana
FROM desafio_grupo1.silver.silver_vendas;

-- dim_produto
CREATE OR REPLACE TABLE desafio_grupo1.gold.dim_produto AS
SELECT
  ROW_NUMBER() OVER (ORDER BY produto) AS id_produto,
  produto
FROM (SELECT DISTINCT produto FROM desafio_grupo1.silver.silver_vendas);

-- dim_tipo_suporte
CREATE OR REPLACE TABLE desafio_grupo1.gold.dim_tipo_suporte AS
SELECT
  ROW_NUMBER() OVER (ORDER BY tipo_problema) AS id_tipo_suporte,
  tipo_problema
FROM (SELECT DISTINCT tipo_problema FROM desafio_grupo1.silver.silver_suporte);

-- ============================================================
-- FATOS
-- ============================================================

-- fact_vendas: junta vendas com dim_produto
CREATE OR REPLACE TABLE desafio_grupo1.gold.fact_vendas AS
SELECT
  v.id_venda,
  v.id_cliente,
  p.id_produto,
  v.data_venda,
  v.quantidade,
  v.valor_total
FROM desafio_grupo1.silver.silver_vendas v
LEFT JOIN desafio_grupo1.gold.dim_produto p ON v.produto = p.produto;

-- fact_suporte: junta suporte com dim_tipo_suporte
CREATE OR REPLACE TABLE desafio_grupo1.gold.fact_suporte AS
SELECT
  s.id_interacao,
  s.id_cliente,
  ts.id_tipo_suporte,
  s.canal,
  s.tempo_resolucao,
  s.satisfacao_cliente
FROM desafio_grupo1.silver.silver_suporte s
LEFT JOIN desafio_grupo1.gold.dim_tipo_suporte ts ON s.tipo_problema = ts.tipo_problema;

-- ============================================================
-- GOLD TABLES - Agregações de Negócio
-- ============================================================

-- Gold comercial: vendas por produto e período (ano-mês)
CREATE OR REPLACE TABLE desafio_grupo1.gold.gold_vendas_produto AS
SELECT
  YEAR(v.data_venda) AS ano,
  MONTH(v.data_venda) AS mes,
  p.produto,
  COUNT(v.id_venda) AS qtd_vendas,
  SUM(v.quantidade) AS qtd_itens,
  SUM(v.valor_total) AS receita_total,
  AVG(v.valor_total) AS ticket_medio
FROM desafio_grupo1.silver.silver_vendas v
LEFT JOIN desafio_grupo1.gold.dim_produto p ON v.produto = p.produto
GROUP BY YEAR(v.data_venda), MONTH(v.data_venda), p.produto
ORDER BY ano, mes, produto;

-- Gold relacionamento: ranking de clientes por receita
CREATE OR REPLACE TABLE desafio_grupo1.gold.gold_ranking_clientes AS
SELECT
  c.id_cliente,
  c.nome,
  c.estado,
  COUNT(v.id_venda) AS qtd_vendas,
  SUM(v.valor_total) AS receita_total,
  AVG(v.valor_total) AS ticket_medio,
  RANK() OVER (ORDER BY SUM(v.valor_total) DESC) AS ranking_receita
FROM desafio_grupo1.silver.silver_clientes c
INNER JOIN desafio_grupo1.silver.silver_vendas v ON c.id_cliente = v.id_cliente
GROUP BY c.id_cliente, c.nome, c.estado
ORDER BY ranking_receita;

-- Gold suporte: chamados por tipo de problema e canal
CREATE OR REPLACE TABLE desafio_grupo1.gold.gold_suporte_tipo AS
SELECT
  s.tipo_problema,
  s.canal,
  COUNT(s.id_interacao) AS qtd_chamados,
  AVG(s.tempo_resolucao) AS tempo_medio_resolucao,
  AVG(s.satisfacao_cliente) AS satisfacao_media
FROM desafio_grupo1.silver.silver_suporte s
GROUP BY s.tipo_problema, s.canal
ORDER BY tipo_problema, canal;

-- ============================================================
-- VIEW INTEGRADA - vw_cliente_360
-- Conecta cadastro, vendas e suporte em uma visão 360°
-- ============================================================
CREATE OR REPLACE VIEW desafio_grupo1.gold.vw_cliente_360 AS
SELECT
  c.id_cliente,
  c.nome,
  c.sexo,
  c.data_nascimento,
  c.cidade,
  c.estado,
  c.data_cadastro,
  COALESCE(v.qtd_vendas, 0) AS qtd_vendas,
  COALESCE(v.receita_total, 0) AS receita_total,
  COALESCE(v.ticket_medio, 0) AS ticket_medio,
  COALESCE(s.qtd_chamados, 0) AS qtd_chamados,
  COALESCE(s.tempo_medio_resolucao, 0) AS tempo_medio_resolucao,
  COALESCE(s.satisfacao_media, 0) AS satisfacao_media
FROM desafio_grupo1.silver.silver_clientes c
LEFT JOIN (
  SELECT
    id_cliente,
    COUNT(*) AS qtd_vendas,
    SUM(valor_total) AS receita_total,
    AVG(valor_total) AS ticket_medio
  FROM desafio_grupo1.silver.silver_vendas
  GROUP BY id_cliente
) v ON c.id_cliente = v.id_cliente
LEFT JOIN (
  SELECT
    id_cliente,
    COUNT(*) AS qtd_chamados,
    AVG(tempo_resolucao) AS tempo_medio_resolucao,
    AVG(satisfacao_cliente) AS satisfacao_media
  FROM desafio_grupo1.silver.silver_suporte
  GROUP BY id_cliente
) s ON c.id_cliente = s.id_cliente;

-- ============================================================
-- VERIFICAÇÃO DA CAMADA GOLD
-- ============================================================
SELECT 'dim_cliente' AS tabela, COUNT(*) AS total FROM desafio_grupo1.gold.dim_cliente
UNION ALL SELECT 'dim_data', COUNT(*) FROM desafio_grupo1.gold.dim_data
UNION ALL SELECT 'dim_produto', COUNT(*) FROM desafio_grupo1.gold.dim_produto
UNION ALL SELECT 'dim_tipo_suporte', COUNT(*) FROM desafio_grupo1.gold.dim_tipo_suporte
UNION ALL SELECT 'fact_vendas', COUNT(*) FROM desafio_grupo1.gold.fact_vendas
UNION ALL SELECT 'fact_suporte', COUNT(*) FROM desafio_grupo1.gold.fact_suporte
UNION ALL SELECT 'gold_vendas_produto', COUNT(*) FROM desafio_grupo1.gold.gold_vendas_produto
UNION ALL SELECT 'gold_ranking_clientes', COUNT(*) FROM desafio_grupo1.gold.gold_ranking_clientes
UNION ALL SELECT 'gold_suporte_tipo', COUNT(*) FROM desafio_grupo1.gold.gold_suporte_tipo;

-- Amostra da vw_cliente_360
SELECT * FROM desafio_grupo1.gold.vw_cliente_360 LIMIT 10;
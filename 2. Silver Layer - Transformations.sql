-- ============================================================
-- SILVER LAYER - Transformações e Limpeza
-- Aplica: tipagem, padronização (TRIM/UPPER), tratamento de datas,
-- deduplicação (ROW_NUMBER), tratamento de nulos e valores inválidos,
-- integridade referencial com silver_clientes
-- ============================================================

-- 1. silver_clientes: padronização de texto, dedup por id_cliente
CREATE OR REPLACE TABLE desafio_grupo1.silver.dim_clientes AS
WITH dedup AS (
  SELECT
    CAST(id_cliente AS INT) AS id_cliente,
    TRIM(nome) AS nome,
    TRIM(sexo) AS sexo,
    date_format(data_nascimento, 'dd-MM-yyyy') AS data_nascimento,    
    date_format(data_cadastro, 'dd-MM-yyyy') AS data_cadastro,
    ROW_NUMBER() OVER (PARTITION BY id_cliente ORDER BY updated DESC) AS rn
  FROM desafio_grupo1.bronze.bronze_clientes
  WHERE id_cliente IS NOT NULL
    AND TRIM(nome) IS NOT NULL
    AND data_nascimento IS NOT NULL
    AND data_cadastro IS NOT NULL
)
SELECT
  id_cliente,
  nome,
  sexo,
  data_nascimento,
  data_cadastro,
  CURRENT_TIMESTAMP() AS updated
FROM dedup
WHERE rn = 1;

-- 1.2 dim_geografia: dimensão geográfica (cidade/estado) extraída dos clientes
CREATE OR REPLACE TABLE desafio_grupo1.silver.dim_geografia AS
WITH distinct_geo AS (
  SELECT DISTINCT
    TRIM(cidade) AS cidade,
    TRIM(estado) AS estado
  FROM desafio_grupo1.bronze.bronze_clientes
  WHERE cidade IS NOT NULL
    AND estado IS NOT NULL
)
SELECT
  ROW_NUMBER() OVER (ORDER BY cidade, estado) AS id_geografia,
  cidade,
  estado
FROM distinct_geo;

-- 1.3 dim_tipo_suporte: dimensão de tipos de problema de suporte
CREATE OR REPLACE TABLE desafio_grupo1.silver.dim_tipo_suporte AS
WITH distinct_tipo AS (
  SELECT DISTINCT
    TRIM(tipo_problema) AS tipo_problema
  FROM desafio_grupo1.bronze.bronze_suporte
  WHERE tipo_problema IS NOT NULL
)
SELECT
  ROW_NUMBER() OVER (ORDER BY tipo_problema) AS id_tipo_suporte,
  tipo_problema
FROM distinct_tipo;

-- 1.4 dim_produto: dimensão de produtos extraída das vendas
CREATE OR REPLACE TABLE desafio_grupo1.silver.dim_produto AS
WITH distinct_produto AS (
  SELECT DISTINCT
    TRIM(produto) AS produto
  FROM desafio_grupo1.bronze.bronze_vendas
  WHERE produto IS NOT NULL
)
SELECT
  ROW_NUMBER() OVER (ORDER BY produto) AS id_produto,
  produto
FROM distinct_produto;

-- 2. silver_vendas: dedup por id_venda, filtro de valores válidos, integridade referencial
CREATE OR REPLACE TABLE desafio_grupo1.silver.fat_vendas AS
WITH dedup AS (
  SELECT
    CAST(id_venda AS INT) AS id_venda,
    CAST(id_cliente AS INT) AS id_cliente,
    DATE_FORMAT(data_venda, 'dd-MM-yyyy') AS data_venda,
    TRIM(produto) AS produto,
    CAST(quantidade AS INT) AS quantidade,
    CONCAT('R$ ', CAST(valor_total AS DECIMAL(10,2))) AS valor_total,
    ROW_NUMBER() OVER (PARTITION BY id_venda ORDER BY updated DESC) AS rn
  FROM desafio_grupo1.bronze.bronze_vendas
  WHERE id_venda IS NOT NULL
    AND id_cliente IS NOT NULL
    AND quantidade > 0
    AND valor_total > 0
    AND data_venda IS NOT NULL
)
SELECT
  v.id_venda,
  v.id_cliente,
  v.data_venda,
  p.id_produto,
  v.produto,
  v.quantidade,
  v.valor_total,
  CURRENT_TIMESTAMP() AS updated
FROM dedup v
INNER JOIN desafio_grupo1.silver.dim_clientes c ON v.id_cliente = c.id_cliente
INNER JOIN desafio_grupo1.silver.dim_produto p ON v.produto = p.produto
WHERE v.rn = 1;

-- 3. silver_suporte: dedup por id_interacao, filtro de valores válidos, integridade referencial
CREATE OR REPLACE TABLE desafio_grupo1.silver.fat_suporte AS
WITH dedup AS (
  SELECT
    CAST(id_interacao AS INT) AS id_interacao,
    CAST(id_cliente AS INT) AS id_cliente,
    TRIM(canal) AS canal,
    TRIM(tipo_problema) AS tipo_problema,
    CAST(tempo_resolucao AS INT) AS tempo_resolucao,
    FLOOR(tempo_resolucao / 60) AS horas,
    tempo_resolucao % 60 AS minutos,
    ROW_NUMBER() OVER (PARTITION BY id_interacao ORDER BY updated DESC) AS rn
  FROM desafio_grupo1.bronze.bronze_suporte
  WHERE id_interacao IS NOT NULL
    AND id_cliente IS NOT NULL
)
SELECT
  s.id_interacao,
  s.id_cliente,
  s.canal,
  t.id_tipo_suporte,
  s.tipo_problema,
  s.tempo_resolucao,
  s.horas,
  s.minutos,
  CURRENT_TIMESTAMP() AS updated
FROM dedup s
INNER JOIN desafio_grupo1.silver.dim_clientes c ON s.id_cliente = c.id_cliente
INNER JOIN desafio_grupo1.silver.dim_tipo_suporte t ON s.tipo_problema = t.tipo_problema
WHERE s.rn = 1;

-- ============================================================
-- VERIFICAÇÃO DA CAMADA SILVER
-- ============================================================
SELECT 'dim_clientes' AS tabela, COUNT(*) AS total_registros FROM desafio_grupo1.silver.dim_clientes
UNION ALL
SELECT 'dim_geografia', COUNT(*) FROM desafio_grupo1.silver.dim_geografia
UNION ALL
SELECT 'dim_tipo_suporte', COUNT(*) FROM desafio_grupo1.silver.dim_tipo_suporte
UNION ALL
SELECT 'dim_produto', COUNT(*) FROM desafio_grupo1.silver.dim_produto
UNION ALL
SELECT 'fat_vendas', COUNT(*) FROM desafio_grupo1.silver.fat_vendas
UNION ALL
SELECT 'fat_suporte', COUNT(*) FROM desafio_grupo1.silver.fat_suporte;

-- Verificação de integridade referencial (deve retornar 0 em ambos)
SELECT 'vendas_sem_cliente' AS verificacao, COUNT(*) AS qtd
FROM desafio_grupo1.silver.fat_vendas v
LEFT JOIN desafio_grupo1.silver.dim_clientes c ON v.id_cliente = c.id_cliente
WHERE c.id_cliente IS NULL
UNION ALL
SELECT 'vendas_sem_produto', COUNT(*)
FROM desafio_grupo1.silver.fat_vendas v
LEFT JOIN desafio_grupo1.silver.dim_produto p ON v.id_produto = p.id_produto
WHERE p.id_produto IS NULL
UNION ALL
SELECT 'suporte_sem_cliente', COUNT(*)
FROM desafio_grupo1.silver.fat_suporte s
LEFT JOIN desafio_grupo1.silver.dim_clientes c ON s.id_cliente = c.id_cliente
WHERE c.id_cliente IS NULL
UNION ALL
SELECT 'suporte_sem_tipo', COUNT(*)
FROM desafio_grupo1.silver.fat_suporte s
LEFT JOIN desafio_grupo1.silver.dim_tipo_suporte t ON s.id_tipo_suporte = t.id_tipo_suporte
WHERE t.id_tipo_suporte IS NULL;
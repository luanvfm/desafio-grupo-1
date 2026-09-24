-- ============================================================
-- SILVER LAYER - Transformações e Limpeza
-- Aplica: tipagem, padronização (TRIM/UPPER), tratamento de datas,
-- deduplicação (ROW_NUMBER), tratamento de nulos e valores inválidos,
-- integridade referencial com silver_clientes
-- ============================================================

-- 1. silver_clientes: padronização de texto, dedup por id_cliente
CREATE OR REPLACE TABLE desafio_grupo1.silver.silver_clientes AS
WITH dedup AS (
  SELECT
    id_cliente,
    TRIM(nome) AS nome,
    TRIM(sexo) AS sexo,
    data_nascimento,
    TRIM(cidade) AS cidade,
    UPPER(TRIM(estado)) AS estado,
    data_cadastro,
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
  cidade,
  estado,
  data_cadastro,
  CURRENT_TIMESTAMP() AS updated
FROM dedup
WHERE rn = 1;

-- 2. silver_vendas: dedup por id_venda, filtro de valores válidos, integridade referencial
CREATE OR REPLACE TABLE desafio_grupo1.silver.silver_vendas AS
WITH dedup AS (
  SELECT
    id_venda,
    id_cliente,
    data_venda,
    TRIM(produto) AS produto,
    quantidade,
    valor_total,
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
  v.produto,
  v.quantidade,
  v.valor_total,
  CURRENT_TIMESTAMP() AS updated
FROM dedup v
INNER JOIN desafio_grupo1.silver.silver_clientes c ON v.id_cliente = c.id_cliente
WHERE v.rn = 1;

-- 3. silver_suporte: dedup por id_interacao, filtro de valores válidos, integridade referencial
CREATE OR REPLACE TABLE desafio_grupo1.silver.silver_suporte AS
WITH dedup AS (
  SELECT
    id_interacao,
    id_cliente,
    TRIM(canal) AS canal,
    TRIM(tipo_problema) AS tipo_problema,
    tempo_resolucao,
    satisfacao_cliente,
    ROW_NUMBER() OVER (PARTITION BY id_interacao ORDER BY updated DESC) AS rn
  FROM desafio_grupo1.bronze.bronze_suporte
  WHERE id_interacao IS NOT NULL
    AND id_cliente IS NOT NULL
    AND satisfacao_cliente BETWEEN 1 AND 5
    AND tempo_resolucao >= 0
)
SELECT
  s.id_interacao,
  s.id_cliente,
  s.canal,
  s.tipo_problema,
  s.tempo_resolucao,
  s.satisfacao_cliente,
  CURRENT_TIMESTAMP() AS updated
FROM dedup s
INNER JOIN desafio_grupo1.silver.silver_clientes c ON s.id_cliente = c.id_cliente
WHERE s.rn = 1;

-- ============================================================
-- VERIFICAÇÃO DA CAMADA SILVER
-- ============================================================
SELECT 'silver_clientes' AS tabela, COUNT(*) AS total_registros FROM desafio_grupo1.silver.silver_clientes
UNION ALL
SELECT 'silver_vendas', COUNT(*) FROM desafio_grupo1.silver.silver_vendas
UNION ALL
SELECT 'silver_suporte', COUNT(*) FROM desafio_grupo1.silver.silver_suporte;

-- Verificação de integridade referencial (deve retornar 0 em ambos)
SELECT 'vendas_sem_cliente' AS verificacao, COUNT(*) AS qtd
FROM desafio_grupo1.silver.silver_vendas v
LEFT JOIN desafio_grupo1.silver.silver_clientes c ON v.id_cliente = c.id_cliente
WHERE c.id_cliente IS NULL
UNION ALL
SELECT 'suporte_sem_cliente', COUNT(*)
FROM desafio_grupo1.silver.silver_suporte s
LEFT JOIN desafio_grupo1.silver.silver_clientes c ON s.id_cliente = c.id_cliente
WHERE c.id_cliente IS NULL;
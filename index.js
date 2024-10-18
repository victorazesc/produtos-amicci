const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const dotenv = require('dotenv');

// Carrega as variáveis de ambiente do arquivo .env
dotenv.config();

// Configura o cliente do BigQuery
const bigquery = new BigQuery({
  projectId: 'prod-amicci-lake', // Substitua pelo ID do seu projeto
  keyFilename: './prod-amicci-lake-f3f34d57cfb8.json' // Substitua pelo caminho para o seu arquivo de credenciais
});

const app = express();
const PORT = process.env.PORT || 3002;

// Middleware para tratar o body das requisições como JSON
app.use(express.json());

// Função auxiliar para buscar fornecedores por categoria
const buscarPorCategoria = async (category_name) => {
  const query = `
    WITH CategoriasComoCabelos AS (
      SELECT
        c.name AS Categoria,
        (1 - \`prod-amicci-lake.curated_aws_api_seller.LEVENSHTEIN\`(LOWER(c.name), LOWER('${category_name}')) / GREATEST(LENGTH(c.name), LENGTH('${category_name}'))) AS similaridade
      FROM
        \`prod-amicci-lake.curated_aws_api_seller.api_produto_gr\` b
      LEFT JOIN
        \`prod-amicci-lake.curated_aws_api_mercadologico.api_classificationv2_gr\` c ON b.market_level = c.id
      WHERE
        c.name IS NOT NULL
      GROUP BY
        c.name
    )
    SELECT
      d.id,
      d.company_name AS Razao_social,
      d.nickname AS Fornecedor,
      f.name AS Plano,
      a.variant_name AS Variante,
      b.name AS Produto,
      c.name AS Categoria,
      STRING_AGG(h.name, ', ') AS Estados
    FROM 
      \`prod-amicci-lake.curated_aws_api_seller.api_seller_gr\` d
    LEFT JOIN 
      \`prod-amicci-lake.curated_aws_api_seller.api_produto_gr\` b ON b.seller_id = d.id
    LEFT JOIN 
      \`prod-amicci-lake.curated_aws_api_seller.api_productvariant_gr\` a ON b.id = a.product_id
    LEFT JOIN 
      \`prod-amicci-lake.curated_aws_api_mercadologico.api_classificationv2_gr\` c ON b.market_level = c.id
    LEFT JOIN 
      \`prod-amicci-lake.curated_aws_api_seller.api_sellerplan_gr\` e ON e.seller_id = d.id
    LEFT JOIN 
      \`prod-amicci-lake.curated_aws_api_seller.api_plan_gr\` f ON f.id = e.plan_id
    LEFT JOIN 
      \`prod-amicci-lake.curated_aws_api_seller.api_seller_coverage_gr\` g ON d.id = g.seller_id
    LEFT JOIN
      \`prod-amicci-lake.curated_aws_api_seller.api_uf_gr\` h ON g.uf_id = h.id
    JOIN 
      CategoriasComoCabelos cc ON c.name = cc.Categoria
    WHERE 
      cc.similaridade > 0.8  -- Filtra apenas categorias com mais de 80% de similaridade
      AND d.id NOT IN (25, 1761, 2, 2078, 2070, 4382)
    GROUP BY 
      d.id, d.company_name, d.nickname, f.name, a.variant_name, b.name, c.name;
  `;
  
  const [rows] = await bigquery.query({ query });
  return rows;
};

// Função auxiliar para buscar produtos pelo nome se a categoria não for encontrada
const buscarProdutoPorNome = async (product_name) => {
  const query = `
      WITH ProdutosComoCafe AS (
      SELECT
        b.name AS Produto,
        c.name AS Categoria,
      \`prod-amicci-lake.curated_aws_api_seller.LEVENSHTEIN\`(LOWER(b.name), LOWER('${product_name.toLowerCase()}')) AS distancia, 
        (1 - \`prod-amicci-lake.curated_aws_api_seller.LEVENSHTEIN\`(LOWER(b.name), LOWER('${product_name.toLowerCase()}')) / GREATEST(LENGTH(b.name), LENGTH('${product_name.toLowerCase()}'))) AS similaridade
      FROM
        \`prod-amicci-lake.curated_aws_api_seller.api_produto_gr\` b
      LEFT JOIN
        \`prod-amicci-lake.curated_aws_api_mercadologico.api_classificationv2_gr\` c ON b.market_level = c.id
      WHERE
        b.name IS NOT NULL
    )
    SELECT
      Produto,
      Categoria
    FROM 
      ProdutosComoCafe
    WHERE 
      similaridade > 0.7  -- Ajuste a porcentagem de similaridade conforme necessário
    ORDER BY 
      similaridade DESC
    LIMIT 1;
  `;
  
  const [rows] = await bigquery.query({ query });
  return rows.length > 0 ? rows[0] : null;
};

// Rota para buscar categorias
app.get('/suppliers/category/:category_name', async (req, res) => {
  try {
    const { category_name } = req.params;

    // Primeiro tenta buscar pela categoria
    let suppliers = await buscarPorCategoria(category_name);

    // Se não encontrar fornecedores pela categoria, busca pelo nome do produto
    if (suppliers.length === 0) {
      const produto = await buscarProdutoPorNome(category_name);

      if (produto) {
        // Se encontrar um produto, busca novamente usando a categoria encontrada do produto
        suppliers = await buscarPorCategoria(produto.Categoria);
      }
    }

    // Se não encontrar nada, retorna um 404
    if (suppliers.length === 0) {
      return res.status(404).send('Nenhum fornecedor encontrado.');
    }

    // Envia os fornecedores encontrados como resposta
    res.json(suppliers);
  } catch (err) {
    console.error('Erro ao buscar fornecedores:', err.message);
    res.status(500).send('Erro ao buscar fornecedores');
  }
});

// Rota simples para teste
app.get('/', (req, res) => {
  res.send('API conectada ao BigQuery');
});

// Inicia o servidor
app.listen(PORT, () => {
  console.log(`API rodando na porta ${PORT}`);
});
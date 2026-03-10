


WITH silver_data AS (
    SELECT * FROM read_csv_auto('s3://data-lake/silver/products_silver.csv')
)

SELECT 
    category AS categoria,
    COUNT(id) AS total_de_produtos,
    ROUND(AVG(price), 2) AS preco_medio
FROM silver_data
GROUP BY category
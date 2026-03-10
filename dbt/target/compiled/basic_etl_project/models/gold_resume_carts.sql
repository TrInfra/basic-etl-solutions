

WITH silver_carts AS (
    SELECT * FROM read_csv_auto('s3://data-lake/silver/carts_silver.csv')
)

SELECT 
    userId AS id_usuario,
    COUNT(DISTINCT id) AS total_carrinhos_abertos,
    SUM(quantity) AS total_itens_comprados
FROM silver_carts
GROUP BY userId
ORDER BY total_itens_comprados DESC
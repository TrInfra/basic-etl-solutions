{{ config(
    materialized='external',
    location='s3://data-lake/gold/resumo_usuarios.parquet'
) }}

WITH silver_users AS (
    SELECT * FROM read_csv_auto('s3://data-lake/silver/users_silver.csv')
)

SELECT 
    city AS cidade,
    COUNT(id) AS total_usuarios
FROM silver_users
GROUP BY city
ORDER BY total_usuarios DESC

      create or replace view "duckdb"."main"."gold_resume_carts__dbt_int" as (
        select * from read_parquet('s3://data-lake/gold/resumo_carrinhos.parquet', union_by_name=False)
        -- if relation is empty, filter by all columns having null values
        
      );
    
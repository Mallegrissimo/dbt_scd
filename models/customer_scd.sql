{{ config(
    materialized='scd',
    unique_key='id',
    updated_at='updated',
    scd1_columns=['category']
  ) }}
SELECT 1 AS id , 'A' AS name, 'A1' AS value, 'A' AS category, '2023-04-01' AS updated, '1' AS batch UNION ALL
SELECT 2 AS id , 'B' AS name, 'B1' AS value, 'B' AS category, '2023-04-01' AS updated, '1' AS batch UNION ALL
SELECT 3 AS id , 'C' AS name, 'C1' AS value, 'C' AS category, '2023-04-01' AS updated, '1' AS batch UNION ALL
SELECT 4 AS id , 'D' AS name, 'D1' AS value, 'D' AS category, '2023-04-01' AS updated, '1' AS batch UNION ALL
SELECT 1 AS id , 'A' AS name, 'A1' AS value, 'A' AS category, '2023-04-01' AS updated, '2' AS batch UNION ALL
SELECT 2 AS id , 'B' AS name, 'B1' AS value, 'BX' AS category, '2023-04-02' AS updated, '2' AS batch UNION ALL
SELECT 3 AS id , 'C' AS name, 'C1' AS value, 'C' AS category, '2023-04-01' AS updated, '2' AS batch UNION ALL
SELECT 4 AS id , 'D' AS name, 'D1' AS value, 'D' AS category, '2023-04-01' AS updated, '2' AS batch UNION ALL
SELECT 1 AS id , 'A' AS name, 'A1' AS value, 'A' AS category, '2023-04-01' AS updated, '3' AS batch UNION ALL
SELECT 2 AS id , 'B' AS name, 'B2' AS value, 'BX' AS category, '2023-04-03' AS updated, '3' AS batch UNION ALL
SELECT 3 AS id , 'C' AS name, 'C2' AS value, 'C' AS category, '2023-04-03' AS updated, '3' AS batch UNION ALL
SELECT 4 AS id , 'D' AS name, 'D1' AS value, 'D' AS category, '2023-04-01' AS updated, '3' AS batch UNION ALL
SELECT 1 AS id , 'A' AS name, 'A1' AS value, 'A' AS category, '2023-04-01' AS updated, '4' AS batch UNION ALL
SELECT 2 AS id , 'B' AS name, 'B2' AS value, 'BX' AS category, '2023-04-03' AS updated, '4' AS batch UNION ALL
SELECT 3 AS id , 'C' AS name, 'C2' AS value, 'C' AS category, '2023-04-03' AS updated, '4' AS batch UNION ALL
SELECT 4 AS id , 'D' AS name, 'D2' AS value, 'DX' AS category, '2023-04-04' AS updated, '4' AS batch UNION ALL
SELECT 2 AS id , 'B' AS name, 'B2' AS value, 'BX' AS category, '2023-04-03' AS updated, '5' AS batch UNION ALL
SELECT 3 AS id , 'C' AS name, 'C2' AS value, 'C' AS category, '2023-04-03' AS updated, '5' AS batch UNION ALL
SELECT 4 AS id , 'D' AS name, 'D3' AS value, 'DX' AS category, '2023-04-05' AS updated, '5' AS batch UNION ALL
SELECT 1 AS id , 'A' AS name, 'A1' AS value, 'A' AS category, '2023-04-01' AS updated, '6' AS batch UNION ALL
SELECT 2 AS id , 'B' AS name, 'B2' AS value, 'BZ' AS category, '2023-04-06' AS updated, '6' AS batch UNION ALL
SELECT 3 AS id , 'C' AS name, 'C2' AS value, 'C2' AS category, '2023-04-10' AS updated, '6' AS batch UNION ALL
SELECT 4 AS id , 'D' AS name, 'D2' AS value, 'DX' AS category, '2023-04-10' AS updated, '6' AS batch UNION ALL
SELECT 5 AS id , 'E' AS name, 'E1' AS value, 'E' AS category, '2023-04-10' AS updated, '6' AS batch  
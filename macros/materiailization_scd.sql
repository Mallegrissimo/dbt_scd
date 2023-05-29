{%- materialization scd, default -%}
  {%- set target_relation = this %}
  {%- set unique_key = config.get('unique_key', []) -%}
  {%- set unique_keys = [unique_key] if unique_key is string else unique_key %}
  {%- set unique_key_alias = config.get('unique_key_alias', 'UNIQUE_KEY') -%}
  {%- set updated_at = config.get('updated_at', 'UPDATED_AT') -%}
  {%- set scd1_columns = config.get('scd1_columns', []) -%}
  {%- set scd2_columns = config.get('scd2_columns', []) -%}
  {%- set etl_is_current = var('dbt_scd__col_name__etl_is_current', 'ETL_IS_CURRENT') -%}
  {%- set etl_valid_from = var('dbt_scd__col_name__etl_valid_from', 'ETL_VALID_FROM') -%}
  {%- set etl_valid_to = var('dbt_scd__col_name__etl_valid_to', 'ETL_VALID_TO') -%}
  {%- set hash1 = var('dbt_scd__col_name__hash1', 'HASH1') -%}
  {%- set hash2 = var('dbt_scd__col_name__hash2', 'HASH2') -%}
  {%- set record_default_end_date = var('dbt_scd__record_default_end_date', '9999-12-31') %}
  {%- set etl_columns = [etl_is_current, etl_valid_from, etl_valid_to] -%}
  {%- set hash_columns = [hash1|upper, hash2|upper] if scd1_columns|length > 0 else [hash2|upper] -%}
  {%- set hash_columns_with_etl_columns = hash_columns + etl_columns -%}

  {%- set scd_settings = {'unique_keys': unique_keys
                          , 'unique_key_alias': unique_key_alias
                          , 'updated_at': updated_at
                          , 'scd1_columns': scd1_columns
                          , 'scd2_columns': scd2_columns
                          , 'etl_is_current': etl_is_current
                          , 'etl_valid_from': etl_valid_from
                          , 'etl_valid_to': etl_valid_to
                          , 'hash1': hash1
                          , 'hash2': hash2
                          , 'record_default_end_date': record_default_end_date
                          , 'etl_columns': etl_columns
                          , 'hash_columns': hash_columns
                          , 'hash_columns_with_etl_columns': hash_columns_with_etl_columns} -%}
  
  {%- do log('scd_settings: '~ scd_settings, info=True) -%}

  {%- set temp_identifier = target_relation.identifier ~ '_dbt_tmp' %}
  {%- set temp_relation = target_relation.incorporate(path= {'identifier': temp_identifier}, type = 'view') -%}
  {%- set existing_relation = load_relation(this) -%}
  {%- set target_relation_columns = adapter.get_columns_in_relation(target_relation) -%}

  
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {%- set temp_relation_sql = get_create_view_as_sql(temp_relation, sql) -%}
  {#%- do log('temp_relation_sql: '~ temp_relation_sql, info=True) -%#}
  -- create a view then retrieve columns from views
  {%- do run_query(temp_relation_sql) -%}

  {%- set temp_relation_columns = adapter.get_columns_in_relation(temp_relation) -%}
  {#%- do log('temp_relation_columns: '~ temp_relation_columns, info=True) -%#}

  -- Update the target table with the incremental logic
  {% if existing_relation is none or should_full_refresh()%}
    {%- do log('Full load:', info=True) -%}
    {%- set build_full_load_sql = _build_full_load(temp_relation, temp_relation_columns, scd_settings) -%}
    {#%- do log('build_full_load_sql: '~ build_full_load_sql, info=True) -%#}

    -- If target table does not exist, create it from the temporary table
    {%- call statement('main') -%}
      {%- set build_sql = create_table_as(False, target_relation, build_full_load_sql) -%}
      {%- do log('build_sql: '~ build_sql, info=True) -%}
      {{ build_sql }}
    {%- endcall -%}
  {% else %}
    {%- do log('Delta load:', info=True) -%}
    -- If target table exists, merge it with the temporary table
    {{ run_hooks(pre_hooks) }}
    -- Generate a merge SQL statement
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    {%- set temp_relation_columns = adapter.get_columns_in_relation(temp_relation) -%}

    {%- set merge_sql = _build_merge_sql(target_relation, target_relation_columns, temp_relation, scd_settings) -%}
    {%- set insert_sql = _build_insert_sql(target_relation, target_relation_columns, temp_relation, scd_settings) -%}


    -- Execute the merge SQL statement
    {%- call statement('main') -%}
      -- endate scd 2 records, insert new records 
      {{ merge_sql }}
      ;

      -- insert active record for each endated records
      {{ insert_sql }}
      ;
    {%- endcall -%}


  {% endif %}
  
  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {%- do adapter.commit() -%}
  
  -- Drop the temporary table
  {{ adapter.drop_relation(temp_relation) }}

  -- Return the target relation
  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

{%- macro _build_full_load(temp_relation, temp_relation_columns, scd_settings) -%}
  {%- set temp_relation_cols_csv = temp_relation_columns | map(attribute='quoted') | join(', ') -%}
  {%- set unique_keys_csv = scd_settings.unique_keys |join(', ') -%}
  {%- set scd1_columns_csv = quoted(scd_settings.scd1_columns) -%}
  {%- set scd2_columns_csv = quoted(scd_settings.scd2_columns) -%}
  {%- set is_scd1_enabled = scd_settings.scd1_columns|length > 0  -%}

WITH src as (
SELECT *
  , ROW_NUMBER() OVER (PARTITION BY {{ unique_keys_csv }} order by {{ quoted(scd_settings.updated_at) }} DESC) AS rnk
  {{- ', ' ~ hash(scd_settings.scd1_columns, scd_settings.hash1) if is_scd1_enabled }}
  , {{ hash(scd_settings.scd2_columns, scd_settings.hash2) }}
  FROM ({{ temp_relation }}
  )
)
SELECT  {{ temp_relation_cols_csv }}
  {{- ', ' ~  quoted(scd_settings.hash1) if is_scd1_enabled -}}
  , {{ quoted(scd_settings.hash2) }}
  , CASE rnk WHEN 1 THEN 1 ELSE 0 END AS {{ quoted(scd_settings.etl_is_current) }}
  , {{ quoted(scd_settings.updated_at) }}             AS {{ quoted(scd_settings.etl_valid_from) }}
  , LEAD({{ quoted(scd_settings.updated_at) }}, 1, '{{ scd_settings.record_default_end_date }}') OVER (PARTITION BY {{ unique_keys_csv }} ORDER BY {{ quoted(scd_settings.updated_at) }})  
                                      AS {{ quoted(scd_settings.etl_valid_to) }}
FROM src
{%- endmacro -%}

{%- macro _build_merge_sql(target_relation, target_relation_columns, temp_relation, scd_settings) -%}
  {%- set unique_keys_csv = scd_settings.unique_keys|join(', ') -%}
  {%- set scd1_columns_csv = quoted(scd_settings.scd1_columns) -%}
  {%- set scd2_columns_csv = quoted(scd_settings.scd2_columns) -%}
  {%- set is_scd1_enabled = scd_settings.scd1_columns|length > 0  -%}
  {%- set relation_cols_csv = quoted(target_relation_columns | map(attribute='column') | list)-%}
  {%- set target_relation_columns_subset = target_relation_columns| map(attribute='column') | list | reject('in', scd_settings.hash_columns_with_etl_columns|map('upper')|list) | list -%}
  {%- set target_relation_columns_subset_csv = quoted(target_relation_columns_subset, 'src') -%}

MERGE INTO {{ target_relation }}  tgt
USING (
SELECT *
  , ROW_NUMBER() OVER (PARTITION BY {{ unique_keys_csv }} ORDER BY {{ quoted(scd_settings.updated_at) }} DESC) AS rnk
  {{- ', ' ~ hash(scd_settings.scd1_columns, scd_settings.hash1) if is_scd1_enabled }}
  , {{ hash(scd_settings.scd2_columns, scd_settings.hash2) }}
  FROM ({{ temp_relation }}
  )
) AS src 
ON {%- for unique_key in scd_settings.unique_keys %}
  {{ 'AND ' if not loop.first -}} 
  src.{{ quoted(unique_key)}} = tgt.{{ quoted(unique_key)}}
  {%- endfor %}
  AND tgt.{{ quoted(scd_settings.etl_is_current) }} = 1 
WHEN MATCHED AND src.{{ quoted(scd_settings.hash2) }} <> tgt.{{ quoted(scd_settings.hash2) }} THEN
    UPDATE SET tgt.{{ quoted(scd_settings.etl_is_current) }} = 0
      , tgt.{{ quoted(scd_settings.etl_valid_to) }} = src.{{ quoted(scd_settings.updated_at) }}
  {%- if is_scd1_enabled -%}
WHEN MATCHED AND src.{{ quoted(scd_settings.hash1) }} <> tgt.{{ quoted(scd_settings.hash1) }}   THEN
    UPDATE SET tgt.{{ quoted(scd_settings.updated_at) }} = src.{{ quoted(scd_settings.updated_at) }}
      , tgt.{{ quoted(scd_settings.hash1) }} = src.{{ quoted(scd_settings.hash1) }}
      {% for item in scd_settings.scd1_columns -%}
      , tgt.{{quoted(item)}} = src.{{quoted(item)}} 
      {{- ', ' if not loop.last }}
      {%- endfor %}
  {%- endif %}
WHEN NOT MATCHED THEN
    INSERT ({{ relation_cols_csv }}) 
    VALUES({{ target_relation_columns_subset_csv }}, {{ quoted(scd_settings.hash2, 'src') }},1, {{ quoted(scd_settings.updated_at, 'src') }}, '{{ scd_settings.record_default_end_date }}')

{%- endmacro -%}

{%- macro _build_insert_sql(target_relation, target_relation_columns, temp_relation, scd_settings) -%}
  {%- set relation_cols_csv = target_relation_columns | map(attribute='quoted') | join(', ') -%}
  {%- set unique_keys_csv = scd_settings.unique_keys|join(', ') -%}
 
  {%- set scd1_columns_csv = quoted(scd_settings.scd1_columns) -%}
  {%- set scd2_columns_csv = quoted(scd_settings.scd2_columns) -%}
  {%- set is_scd1_enabled = scd_settings.scd1_columns|length > 0  -%}
  {%- set target_relation_columns_subset = target_relation_columns| map(attribute='column') | list | reject('in', scd_settings.hash_columns_with_etl_columns|map('upper')|list) | list -%}
  {#%- set target_relation_columns_subset = target_relation_columns_subset | reject('in', [scd_settings.hash1]|map('upper')|list) | list -%#}
  {%- set target_relation_columns_subset_csv_w_prefix = quoted(target_relation_columns_subset, 'src') -%}

INSERT INTO {{ target_relation }} ({{ quoted(target_relation_columns_subset) }}, {{  quoted(scd_settings.hash2) }}, {{  quoted(scd_settings.etl_is_current) }}, {{  quoted(scd_settings.etl_valid_from) }}, {{  quoted(scd_settings.etl_valid_to) }})
WITH src as ( 
SELECT *
  , ROW_NUMBER() OVER (PARTITION BY {{ unique_keys_csv }} ORDER BY {{ quoted(scd_settings.updated_at) }} DESC) AS rnk
  {{- ', ' ~hash(scd1_columns, hash1) if is_scd1_enabled }}
  , {{ hash(scd_settings.scd2_columns, scd_settings.hash2) }}
  FROM ({{ temp_relation }}
  )
)  
, tgt as (
  SELECT *
    ,ROW_NUMBER()  OVER(PARTITION BY {{ unique_keys_csv }} ORDER BY s.{{ quoted(scd_settings.etl_valid_from) }} DESC) rnk 
  FROM {{ target_relation }} s
  QUALIFY rnk = 1
)
SELECT {{ target_relation_columns_subset_csv_w_prefix }}, {{ quoted(scd_settings.hash2, 'src') }},1, {{ quoted(scd_settings.updated_at, 'src') }}, '{{ scd_settings.record_default_end_date }}'
FROM src
  JOIN tgt 
    ON 
    {%- for unique_key in scd_settings.unique_keys %}
    {{ 'AND ' if not loop.first -}} 
    src.{{ quoted(unique_key)}} = tgt.{{ quoted(unique_key)}} 
    {%- endfor %}
    AND src.{{ quoted(scd_settings.updated_at) }} = tgt.{{ quoted(scd_settings.etl_valid_to) }}

{%- endmacro -%}
{%- materialization scd, default -%}
  {%- set target_relation = this %}
  {%- set unique_key = config.get('unique_key', 'unique_key') -%}
  {%- set updated = config.get('updated', 'updated') -%}
  {%- set scd1_columns = config.get('scd1_columns', []) -%}
  {%- set scd2_columns = config.get('scd2_columns', []) -%}
  {%- set etl_is_current = config.get('etl_is_current', 'etl_is_current') -%}
  {%- set etl_valid_from = config.get('etl_valid_from', 'etl_valid_from') -%}
  {%- set etl_valid_to = config.get('etl_valid_to', 'etl_valid_to') -%}
  {%- set etl_columns = [etl_is_current, etl_valid_from, etl_valid_to] -%}
  {%- set hash_columns = ['hash1', 'hash2'] -%}
  {%- set hash_columns_with_etl_columns = hash_columns + etl_columns -%}

  {%- set scd_settings = {'unique_key': unique_key
                          , 'updated': updated
                          , 'scd1_columns': scd1_columns
                          , 'scd2_columns': scd2_columns
                          , 'etl_is_current': etl_is_current
                          , 'etl_valid_from': etl_valid_from
                          , 'etl_valid_to': etl_valid_to
                          , 'etl_columns': etl_columns
                          , 'hash_columns': hash_columns
                          , 'hash_columns_with_etl_columns': hash_columns_with_etl_columns} -%}
  
  {%- do log('scd_settings: '~ scd_settings, info=True) -%}

  {%- set tmp_identifier = target_relation.identifier ~ "_dbt_tmp" %}
  {%- set tmp_relation = target_relation.incorporate(path= {"identifier": tmp_identifier, "schema": config.get('temp_schema', default=target_relation.schema)}) -%}
  {%- set existing_relation = load_relation(this) -%}
  {%- set target_relation_columns = adapter.get_columns_in_relation(target_relation) -%}
  {%- set to_drop = [] -%}
  
  
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {%- set temp_relation_sql = get_create_view_as_sql(tmp_relation, sql) -%}
  {%- do log('temp_relation_sql: '~ temp_relation_sql, info=True) -%}
  -- create a view then retrieve columns from views
  {%- do run_query(temp_relation_sql) -%}

  {%- set temp_relation_columns = adapter.get_columns_in_relation(tmp_relation) -%}
  {%- do log('temp_relation_columns: '~ temp_relation_columns, info=True) -%}




  -- Update the target table with the incremental logic
  {% if existing_relation is none or should_full_refresh()%}
    {%- do log('Full load:', info=True) -%}
    {%- set build_full_load_sql = _build_full_load(temp_relation, temp_relation_columns, sql, scd_settings) -%}
    {%- do log('build_full_load_sql: '~ build_full_load_sql, info=True) -%}

    -- If target table does not exist, create it from the temporary table
    {%- do log('begin of main call', info=True) -%}
    {%- call statement('main') -%}
      {%- set build_sql = create_table_as(False, target_relation, build_full_load_sql) -%}
      {%- do log('build_sql: '~ build_sql, info=True) -%}
      {{ build_sql }}
    {%- endcall -%}
    {%- do log('end of main call', info=True) -%}
  {% else %}
    {%- do log('Delta load:', info=True) -%}
    -- If target table exists, merge it with the temporary table
    {{ run_hooks(pre_hooks) }}
    -- Generate a merge SQL statement
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    {%- set temp_relation_columns = adapter.get_columns_in_relation(tmp_relation) -%}

    {%- set merge_sql = _build_merge_sql(target_relation, target_relation_columns, sql, scd_settings) -%}
    {%- set insert_sql = _build_insert_sql(target_relation, target_relation_columns, sql, scd_settings) -%}


    -- Execute the merge SQL statement
    {%- call statement('main') -%}
      -- endate scd 2 records, insert new records 
      {{ merge_sql }};
      -- insert active record for each endated records
      {{ insert_sql }};
    {%- endcall -%}


  {% endif %}
  
  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {%- do adapter.commit() -%}
  
  -- Drop the temporary table
  {{ adapter.drop_relation(tmp_relation) }}

  -- Return the target relation
  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

{%- macro _build_full_load(tmp_relation, temp_relation_columns, sql, scd_settings) -%}
  {%- set temp_relation_cols_csv = temp_relation_columns | map(attribute='quoted') | join(', ') -%}
  {%- set updated = scd_settings.get('updated') -%}
  {%- set unique_key = scd_settings.get('unique_key') -%}
  {%- set etl_is_current = scd_settings.get('etl_is_current') -%}
  {%- set etl_valid_from = scd_settings.get('etl_valid_from') -%}
  {%- set etl_valid_to = scd_settings.get('etl_valid_to') -%}
  {%- set scd1_columns = scd_settings.get('scd1_columns') -%}
  {%- set scd2_columns = scd_settings.get('scd2_columns') -%}
  {%- set scd1_columns_csv = quoted(scd1_columns) -%}
  {%- set scd2_columns_csv = quoted(scd2_columns) -%}

WITH src as (
SELECT *
  , ROW_NUMBER() OVER (PARTITION BY {{ quoted(unique_key) }} order by {{ quoted(updated) }} DESC) AS rnk
  , CONCAT_WS('||', {{ scd1_columns_csv }})   AS hash1
  , CONCAT_WS('||', {{ scd2_columns_csv }})   AS hash2
  FROM ({{ sql }})
)
SELECT  {{ temp_relation_cols_csv }}
  , hash1
  , hash2
  , CASE rnk WHEN 1 THEN 1 ELSE 0 END AS {{ etl_is_current }}
  , {{ quoted(updated) }}             AS {{ etl_valid_from }}
  , LEAD({{ quoted(updated) }}, 1, '9999-12-31') OVER (PARTITION BY {{ quoted(unique_key) }} ORDER BY {{ quoted(updated) }})  
                                      AS {{ etl_valid_to }}
FROM src
{%- endmacro -%}

{%- macro _build_merge_sql(target_relation, target_relation_columns, sql, scd_settings) -%}
  {%- set relation_cols_csv = target_relation_columns | map(attribute='quoted') | join(', ') -%}
  {%- set updated = scd_settings.get('updated') -%}
  {%- set unique_key = scd_settings.get('unique_key') -%}
  {%- set etl_is_current = scd_settings.get('etl_is_current') -%}
  {%- set etl_valid_from = scd_settings.get('etl_valid_from') -%}
  {%- set etl_valid_to = scd_settings.get('etl_valid_to') -%}
  {%- set scd1_columns = scd_settings.get('scd1_columns') -%}
  {%- set scd2_columns = scd_settings.get('scd2_columns') -%}
  {%- set scd1_columns_csv = quoted(scd1_columns) -%}
  {%- set scd2_columns_csv = quoted(scd2_columns) -%}
  {%- set etl_columns = scd_settings.get('etl_columns') -%}
  {%- set target_relation_columns_subset = target_relation_columns| map(attribute="column") | list | reject('in', etl_columns|map('upper')|list) | list -%}
  {%- set target_relation_columns_subset_csv = quoted(target_relation_columns_subset, 'src') -%}

MERGE INTO {{ target_relation }}  tgt
USING (
SELECT *
  , ROW_NUMBER() OVER (PARTITION BY {{ quoted(unique_key) }} ORDER BY {{ quoted(updated) }} DESC) AS rnk
  , CONCAT_WS('||', {{ scd1_columns_csv }})   AS hash1
  , CONCAT_WS('||', {{ scd2_columns_csv }})   AS hash2
  FROM ({{ sql }})
) AS src ON src.{{ quoted(unique_key)}} = tgt.{{ quoted(unique_key)}} AND tgt.{{ quoted(etl_is_current) }} = 1 
WHEN MATCHED AND src.hash2 <> tgt.hash2 THEN
    UPDATE SET tgt.{{ etl_is_current }} = 0
      , tgt.{{ etl_valid_to }} = src.{{ quoted(updated) }}
WHEN MATCHED AND src.hash1 <> tgt.hash1   THEN
    UPDATE SET tgt.{{ quoted(updated) }} = src.{{ quoted(updated) }}
      , tgt.hash1 = src.hash1
      {% for item in scd1_columns -%}
      , tgt.{{quoted(item)}} = src.{{quoted(item)}} {%- if not loop.last -%}, {%- endif -%}
      {%- endfor %}
WHEN NOT MATCHED THEN
    INSERT ({{ relation_cols_csv }}) VALUES({{ target_relation_columns_subset_csv }}, 1, src.{{ quoted(updated) }}, '9999-12-31')

{%- endmacro -%}

{%- macro _build_insert_sql(target_relation, target_relation_columns, sql, scd_settings) -%}
  {%- set relation_cols_csv = target_relation_columns | map(attribute='quoted') | join(', ') -%}
  {%- set updated = scd_settings.get('updated') -%}
  {%- set unique_key = scd_settings.get('unique_key') -%}
  {%- set etl_is_current = scd_settings.get('etl_is_current') -%}
  {%- set etl_valid_from = scd_settings.get('etl_valid_from') -%}
  {%- set etl_valid_to = scd_settings.get('etl_valid_to') -%}
  {%- set scd1_columns = scd_settings.get('scd1_columns') -%}
  {%- set scd2_columns = scd_settings.get('scd2_columns') -%}
  {%- set scd1_columns_csv = quoted(scd1_columns) -%}
  {%- set scd2_columns_csv = quoted(scd2_columns) -%}
  {%- set target_relation_columns_subset = target_relation_columns| map(attribute="column") | list | reject('in', [etl_is_current|upper, etl_valid_from|upper, etl_valid_to|upper]) | list -%}
  {%- set target_relation_columns_subset_csv_w_prefix = quoted(target_relation_columns_subset, 'src') -%}

INSERT INTO {{ target_relation }} ({{ quoted(target_relation_columns_subset) }}, {{  quoted(etl_is_current_row) }}, {{  quoted(etl_valid_from) }}, {{  quoted(etl_valid_to) }})
WITH src as ( 
  {{sql}}
)  
, tgt as (
  SELECT *
    ,ROW_NUMBER()  OVER(PARTITION BY s.{{ quoted(unique_key) }} ORDER BY s.{{ quoted(etl_valid_from) }} DESC) rnk 
  FROM {{ target_relation }} s
  QUALIFY rnk = 1
)
SELECT {{ target_relation_columns_subset_csv_w_prefix }}, 1, src.{{ quoted(updated) }}, '9999-12-31'
FROM src
  JOIN tgt 
    ON src.{{ quoted(unique_key) }} = tgt.{{ quoted(unique_key) }}
        AND src.{{ quoted(updated) }} = tgt.{{ quoted(etl_valid_to) }}
;

{%- endmacro -%}

{%- macro quoted(args, col_prefix='') -%}
  {%- set items = [args] if args is string  else args  -%}
  {%- for item in items -%}
    {{ col_prefix~'.' if col_prefix else ''}}"{{item|upper}}"
    {%- if not loop.last  -%}, {% endif-%}
  {%- endfor -%}
{%- endmacro -%}
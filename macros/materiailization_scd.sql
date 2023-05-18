{%- materialization scd, default -%}
  {%- set target_relation = this %}
  {%- set bk = config.get('unique_key', 'bk') -%}
  {%- set updated = config.get('updated', 'updated') -%}
  {%- set scd1_columns = config.get('scd1_columns', none) -%}
  {%- set scd2_columns = config.get('scd2_columns', none) -%}
  {%- set etl_is_current = config.get('etl_is_current', 'etl_is_current') -%}
  {%- set etl_valid_from = config.get('etl_valid_from', 'etl_valid_from') -%}
  {%- set etl_valid_to = config.get('etl_valid_to', 'etl_valid_to') -%}

  {%- set tmp_identifier = "temp_" ~ target_relation.identifier %}
  {%- set tmp_relation = target_relation.incorporate(path= {"identifier": tmp_identifier, "schema": config.get('temp_schema', default=target_relation.schema)}) -%}
  {%- set existing_relation = load_relation(this) -%}

  -- Create a temporary table with the snapshot logic
  {%- call statement('main') -%}
    {%- do log('source_sql: '~ sql, info=True) -%}
    {{ create_table_as(False, tmp_relation, sql) }}
  {%- endcall -%}

  -- Update the target table with the incremental logic
  {% if existing_relation is none or should_full_refresh() %}
    -- If target table does not exist, create it from the temporary table
    {{ run_hooks(pre_hooks) }}
    {{ adapter.rename_relation(tmp_relation, target_relation) }}
    {{ run_hooks(post_hooks) }}
  {% else %}
    -- If target table exists, merge it with the temporary table
    {{ run_hooks(pre_hooks) }}

    -- Get the unique key and the SCD type 1 columns from the config
    {%- set unique_key = config.get('unique_key') -%}
    {%- set scd1_columns = config.get('scd1_columns') -%}

    -- Generate a merge SQL statement
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    {%- set src_columns = adapter.get_columns_in_relation(tmp_relation) -%}
    {%- set src_cols_csv = src_columns | map(attribute='quoted') | join(', ') -%}

    {% set matching_condition %}
      target.{{ unique_key }} = source.{{ unique_key }}
      and target.dbt_valid_to is null
      and source.dbt_valid_to is null
    {% endset %}

    {% set update_condition %}
      target.{{ unique_key }} = source.{{ unique_key }}
      and target.dbt_valid_to is null
      and source.dbt_valid_to is not null
    {% endset %}

    {% set insert_condition %}
      source.{{ unique_key }} not in (select {{ unique_key }} from {{ target_relation }})
      and source.dbt_valid_to is null
    {% endset %}

    {% set update_actions %}
      {%- for column in scd1_columns %}
        target.{{ column }} = source.{{ column }},
      {%- endfor %}
        target.dbt_valid_to = source.dbt_valid_to
    {% endset %}

    {% set insert_values %}
      ({{ src_cols_csv }})
      select {{ src_cols_csv }}
      from {{ tmp_relation }}
      where {{ insert_condition }}
    {% endset %}

    {% set sql %}
      merge into {{ target_relation }} as target
      using {{ tmp_relation }} as source
      on {{ matching_condition }}
      when matched then update set {{ update_actions }}
      when not matched then insert {{ insert_values }};
    {% endset %}

    -- Execute the merge SQL statement
    {%- call statement('main') -%}
      {{ sql }}
    {%- endcall -%}

    -- Drop the temporary table
    {{ adapter.drop_relation(tmp_relation) }}

    {{ run_hooks(post_hooks) }}
  {% endif %}

  -- Return the target relation
  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
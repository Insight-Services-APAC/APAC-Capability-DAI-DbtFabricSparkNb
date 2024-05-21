{% macro fabricspark__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    {# create schema if not exists {{relation}} #}
    /*FABRICSPARKNB_ALERT: Schema Does NOT exist and automatic schema creation in Fabric Lakehouse not allowed. Please create the schema {{relation}} manually*/ select 1
  {% endcall %}
{% endmacro %}

{% macro fabricspark__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    {# drop schema if exists {{ relation }} cascade #}
    /*FABRICSPARKNB_ALERT: Drop schema in Fabric Lakehouse not allowed. Please drop the schema {{relation}} manually*/ select 1
  {%- endcall -%}
{% endmacro %}

{% macro fabricspark__list_schemas(database) -%}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    show databases 
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro fabricspark__generate_database_name(custom_database_name=none, node=none) -%}
  {% do return(None) %}
{%- endmacro %}
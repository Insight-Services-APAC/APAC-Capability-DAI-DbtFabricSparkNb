
{% snapshot cf_customercategories_snapshot  %}

{{
    config(
  
      target_schema='lh_conformed',
      unique_key='CustomerCategoryID',

      strategy='timestamp',
      updated_at='updated_at',
      file_format="delta"
    )
}}


select * from {{ ref('cf_customercategories') }}

{% endsnapshot %}
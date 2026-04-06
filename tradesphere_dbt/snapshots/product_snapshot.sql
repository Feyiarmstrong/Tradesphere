
-- SCD Type 2 snapshot for products
-- Tracks when product price changes over time

{% snapshot product_snapshot %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='product_id',
        strategy='check',
        check_cols=['product_price', 'product_status']
    )
}}

SELECT * FROM {{ ref('stg_products') }}

{% endsnapshot %}
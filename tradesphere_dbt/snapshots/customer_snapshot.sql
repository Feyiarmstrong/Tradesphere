-- SCD Type 2 snapshot for customers
-- Tracks when customer address or segment changes over time

{% snapshot customer_snapshot %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='customer_id',
        strategy='check',
        check_cols=['city', 'state', 'country', 'customer_segment']
    )
}}

SELECT * FROM {{ ref('stg_customers') }}

{% endsnapshot %}

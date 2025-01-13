{{
    config(
        materialized='table'
    )
}}

with kontantstotte_meta_data as (
  select pk_ks_meta_data, kafka_offset, kafka_mottatt_dato, kafka_topic, kafka_partisjon, melding from {{ source ('fam_ks', 'fam_ks_meta_data') }}
    where kafka_offset in (71235,71236)

)

select * from kontantstotte_meta_data
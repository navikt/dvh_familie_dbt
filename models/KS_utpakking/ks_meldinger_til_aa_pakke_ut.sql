{{
    config(
        materialized='table'
    )
}}

with kontantstotte_meta_data as (
  select pk_ks_meta_data, kafka_offset, kafka_mottatt_dato, kafka_topic, kafka_partisjon, melding from {{ source ('fam_ks', 'fam_ks_meta_data') }}
    --where kafka_offset in (112,113,115)
    where kafka_mottatt_dato >= sysdate - 30 and kafka_offset not in (
      select kafka_offset from {{ source ('fam_ks', 'fam_ks_fagsak') }})
)

select * from kontantstotte_meta_data
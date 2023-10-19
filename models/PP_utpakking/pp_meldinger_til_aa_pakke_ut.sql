{{
    config(
        materialized='table'
    )
}}

with pleiepenger_meta_data as (
  select pk_pp_meta_data, kafka_offset, kafka_mottatt_dato, kafka_topic, KAFKA_PARTITION, melding from {{ source ('fam_pp', 'fam_pp_meta_data') }}
    where kafka_mottatt_dato >= sysdate - 30 and kafka_offset not in (
      select kafka_offset from {{ source ('fam_pp', 'fam_pp_fagsak') }})
)

select * from pleiepenger_meta_data
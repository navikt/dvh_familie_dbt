{{
    config(
        materialized='table'
    )
}}

with ensligForsorger_meta_data as (
  select pk_ef_meta_data, kafka_offset, kafka_mottatt_dato, kafka_topic, kafka_partition, melding from {{ source ('fam_ef', 'fam_ef_meta_data') }}
    where kafka_offset = 73510--73509--73509--73512--73607 --72554--72232 --73315
    --where kafka_mottatt_dato >= sysdate - 30 and kafka_offset not in (
      --select kafka_offset from {{ source ('fam_ef', 'fam_ef_fagsak') }})
)

select * from ensligForsorger_meta_data
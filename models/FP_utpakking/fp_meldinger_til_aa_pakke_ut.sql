{{
    config(
        materialized='table'
    )
}}

with fp_meta_data as (
  select pk_fp_meta_data, kafka_offset, kafka_mottatt_dato, melding from {{ source ('fam_fp', 'fam_fp_meta_data') }}
  --where kafka_offset = 1
  where kafka_mottatt_dato >= sysdate - 30
  and kafka_offset not in (select distinct kafka_offset from {{ source('fam_fp','json_fam_fp_fagsak') }})
)

select * from fp_meta_data

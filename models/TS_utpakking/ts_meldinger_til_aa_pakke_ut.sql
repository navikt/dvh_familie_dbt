{{
    config(
        materialized='table'
    )
}}

with ts_meta_data as (
  select pk_ts_meta_data, melding, ekstern_behandling_id, endret_tid from {{ source ('fam_ef', 'fam_ts_meta_data') }}
    where endret_tid > nvl( (select max(endret_tid) from {{ source ('fam_ef', 'fam_ts_fagsak') }}), endret_tid-1 )
    --where opprettet_tid >= sysdate - 30 and ekstern_behandling_id not in (
      --select ekstern_behandling_id from {{ source ('fam_ef', 'fam_ts_fagsak') }})
        --and endret_tid <= (select max(endret_tid) from {{ this }})
)

select * from ts_meta_data

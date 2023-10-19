{{
    config(
        materialized='incremental'
    )
}}

with pp_meta_data as (
  select * from {{ref ('pp_meldinger_til_aa_pakke_ut')}}
),

pp_fagsak AS (
  SELECT * FROM {{ ref ('fam_pp_fagsak') }}
),

pre_final as (
select * from pp_meta_data,
  json_table(melding, '$'
    columns (
      nested             path '$.relasjon[*]' columns (
      kode               varchar2 path '$.kode'
      ,fom               date path '$.fom'
      ,tom               date path '$.tom'
      )
    )
  ) j
  where kode is not null
),

final as (
  select
    p.kode
    ,p.fom DATO_FOM
    ,p.tom DATO_TOM
    ,f.pk_pp_fagsak as FK_PP_FAGSAK
  from pre_final p
  join pp_fagsak f
  on p.kafka_offset = f.kafka_offset
)

select
  dvh_fampp_kafka.hibernate_sequence.nextval as PK_PP_RELASJONER
  ,DATO_FOM
  ,DATO_TOM
  ,KODE
  ,FK_PP_FAGSAK
  ,localtimestamp as LASTET_DATO
from final
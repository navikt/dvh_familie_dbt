{{
    config(
        materialized='incremental'
    )
}}

with pp_meta_data as (
  select * from {{ref ('pp_meldinger_til_aa_pakke_ut')}}
),

pp_perioder as (
  select * from {{ref ('fam_pp_perioder')}}
),

pre_final as (
  select * from pp_meta_data,
  json_table(melding, '$'
    columns (
      nested path '$.perioder[*]' columns (
      dato_fom         date path '$.fom'
      ,dato_tom         date path '$.tom'
      , nested path '$.årsaker[*]' columns (
        aarsak varchar2 path '$[*]'
        )
      )
    )
  ) j
  where aarsak is not null
),

final as (
  select
    aarsak
    ,pk_pp_perioder as FK_PP_PERIODER
  from pre_final p
  join pp_perioder perioder
  on p.kafka_offset = perioder.kafka_offset
  where p.dato_fom = perioder.dato_fom
  and p.dato_tom = perioder.dato_tom
)

select
  dvh_fampp_kafka.hibernate_sequence.nextval as PK_PP_PERIODE_AARSAK
  ,AARSAK
  ,FK_PP_PERIODER
  ,localtimestamp as LASTET_DATO
from final

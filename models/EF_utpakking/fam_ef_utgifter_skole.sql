{{
    config(
        materialized='incremental'
    )
}}

with ef_meta_data as (
  select * from {{ref ('meldinger_til_aa_pakke_ut')}}
),

vedtaksperioder_skole AS (
  SELECT * FROM {{ ref ('fam_ef_vedtaksperioder_skole') }}
),

kolonner as (
  select * from ef_meta_data,
  json_table(melding, '$?(@.stønadstype == "SKOLEPENGER")'
    COLUMNS (
      nested path '$.vedtaksperioder[*]' columns (
      nested path '$.utgifter[*]' columns (
      utgiftsdato        varchar2 path '$.utgiftsdato'
      ,utgiftsbelop       varchar2 path '$.utgiftsbeløp'
      ,utbetaltbelop      varchar2 path '$.utbetaltBeløp'
        )
      )
      )
    ) j
),

pre_final as (
  select
    p.UTGIFTSDATO,
    p.UTGIFTSBELOP,
    p.UTBETALTBELOP,
    p.kafka_offset,
    PK_EF_VEDTAKSPERIODER_SKOLE as FK_EF_VEDTAKSPERIODER_SKOLE
  from kolonner p
  join vedtaksperioder_skole b
  on p.kafka_offset = b.kafka_offset
  and to_date(p.UTGIFTSDATO, 'yyyy-mm-dd') between b.dato_fra and b.dato_til
),

final as (
  select
    UTGIFTSDATO,
    UTGIFTSBELOP,
    UTBETALTBELOP,
    KAFKA_OFFSET,
    FK_EF_VEDTAKSPERIODER_SKOLE
  From
  pre_final
)

select
  dvh_famef_kafka.hibernate_sequence.nextval as PK_EF_UTGIFTER_SKOLE,
  to_date(UTGIFTSDATO, 'yyyy-mm-dd') UTGIFTSDATO,
  UTGIFTSBELOP,
  UTBETALTBELOP,
  localtimestamp AS LASTET_DATO,
  FK_EF_VEDTAKSPERIODER_SKOLE,
  kafka_offset
from final

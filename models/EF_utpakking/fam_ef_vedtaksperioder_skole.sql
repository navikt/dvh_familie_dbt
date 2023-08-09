{{
    config(
        materialized='incremental'
    )
}}

with ef_meta_data as (
  select * from {{ref ('meldinger_til_aa_pakke_ut')}}
),

ef_fagsak AS (
  SELECT * FROM {{ ref ('fam_ef_fagsak') }}
),

kolonner as (
select * from ef_meta_data,
  json_table(melding, '$?(@.stønadstype == "SKOLEPENGER")'
    COLUMNS (
      nested                    path '$.vedtaksperioder[*]' columns (
      skoleaar                  varchar2 path '$.skoleår'
      ,maks_sats_for_skoleaar    varchar2 path '$.maksSatsForSkoleår'
      ,nested                   path '$.perioder[*]' columns (
        studietype              path '$.studietype',
        dato_fra                path '$.datoFra',
        dato_til                path '$.datoTil',
        studiebelastning        path '$.studiebelastning'
      )
      )
    )
  ) j
),

final as (
  select
    p.SKOLEAAR,
    p.MAKS_SATS_FOR_SKOLEAAR,
    p.STUDIETYPE,
    p.DATO_FRA,
    p.DATO_TIL,
    p.STUDIEBELASTNING,
    p.kafka_offset,
    pk_EF_FAGSAK as FK_EF_FAGSAK
  from kolonner p
  join ef_fagsak b
  on p.kafka_offset = b.kafka_offset
)

select
  dvh_famef_kafka.hibernate_sequence.nextval as PK_EF_VEDTAKSPERIODER_SKOLE,
  SKOLEAAR,
  MAKS_SATS_FOR_SKOLEAAR,
  localtimestamp AS lastet_dato,
  FK_EF_FAGSAK,
  STUDIETYPE,
  to_date(DATO_FRA, 'yyyy-mm-dd') DATO_FRA,
  to_date(DATO_TIL, 'yyyy-mm-dd') DATO_TIL,
  STUDIEBELASTNING,
  kafka_offset
from final

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
      , nested path '$.utbetalingsgrader[*]' columns (
         arbeidsforhold_aktorid varchar2 path '$.arbeidsforhold.aktørId'
        ,arbeidsforhold_id      varchar2 path '$.arbeidsforhold.arbeidsforholdId'
        ,arbeidsforhold_orgnr   varchar2 path '$.arbeidsforhold.organisasjonsnummer'
        ,arbeidsforhold_type    varchar2 path '$.arbeidsforhold.type'
        ,dagsats                varchar2 path '$.dagsats'
        ,faktisk_arbeidstid     varchar2 path '$.faktiskArbeidstid'
        ,normal_arbeidstid      varchar2 path '$.normalArbeidstid'
        ,utbetalingsgrad        varchar2 path '$.utbetalingsgrad'
        ,bruker_er_mottaker     varchar2 path '$.brukerErMottaker'
        )
      )
    )
  ) j
),

final as (
  select
    p.arbeidsforhold_aktorid
    ,p.arbeidsforhold_id
    ,p.arbeidsforhold_orgnr
    ,p.arbeidsforhold_type
    ,p.dagsats
    ,p.faktisk_arbeidstid
    ,p.normal_arbeidstid
    ,p.utbetalingsgrad
    ,p.bruker_er_mottaker
    ,perioder.pk_pp_perioder as FK_PP_PERIODER
  from pre_final p
  join pp_perioder perioder
  on p.kafka_offset = perioder.kafka_offset
  where p.dato_fom = perioder.dato_fom
  and p.dato_tom = perioder.dato_tom
)

select
  dvh_fampp_kafka.hibernate_sequence.nextval as PK_PP_PERIODE_UTBET_GRADER
  ,ARBEIDSFORHOLD_AKTORID
  ,ARBEIDSFORHOLD_ID
  ,ARBEIDSFORHOLD_ORGNR
  ,ARBEIDSFORHOLD_TYPE
  ,DAGSATS
  ,null as DELYTELSE_ID_DIREKTE
  ,null as DELYTELSE_ID_REFUSJON
  ,NORMAL_ARBEIDSTID
  ,FAKTISK_ARBEIDSTID
  ,UTBETALINGSGRAD
  ,FK_PP_PERIODER
  ,BRUKER_ER_MOTTAKER
  ,localtimestamp as LASTET_DATO
from final



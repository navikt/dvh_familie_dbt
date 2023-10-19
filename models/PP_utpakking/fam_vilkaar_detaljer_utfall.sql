{{
    config(
        materialized='incremental'
    )
}}

with pp_meta_data as (
  select * from {{ref ('pp_meldinger_til_aa_pakke_ut')}}
),

pp_periode_inngangsvilkaar as (
  select * from {{ref ('fam_pp_periode_inngangsvilkaar')}}
),

pre_final as (
  select * from pp_meta_data,
  json_table(melding, '$'
    columns (
      nested path '$.perioder[*]' columns (
      dato_fom          date path '$.fom'
      ,dato_tom         date path '$.tom'
      ,nested           path '$.inngangsvilkår[*]' columns (
      utfall            varchar2 path '$.utfall'
      ,vilkaar          varchar2 path '$.vilkår',
      nested            path '$.detaljertUtfall[*]' columns (
      GJELDER_KRAVSTILLER             varchar2 path '$.gjelderKravstiller',
      GJELDER_AKTIVITET_TYPE          varchar2 path '$.gjelderAktivitetType',
      GJELDER_ORGANISASJONSNUMMER     varchar2 path '$.gjelderOrganisasjonsnummer',
      GJELDER_AKTOR_ID                varchar2 path '$.gjelderAktørId',
      GJELDER_ARBEIDSFORHOLD_ID       varchar2 path '$.gjelderArbeidsforholdId',
      det_utfall                      varchar2 path '$.utfall'
          )
        )
      )
    )
  ) j
  where GJELDER_ORGANISASJONSNUMMER is not null and GJELDER_AKTIVITET_TYPE is not null
),

final as (
  select p.*,
  pk_pp_periode_inngangsvilkaar as FK_PP_PERIODE_INNGANGSVILKAAR
  from pre_final p
  join pp_periode_inngangsvilkaar inn
  on p.kafka_offset = inn.kafka_offset
  and p.DATO_FOM = inn.DATO_FOM
  AND p.DATO_TOM = inn.DATO_TOM
  and p.utfall = inn.utfall
  and p.vilkaar = inn.vilkaar
)

select
  dvh_fampp_kafka.hibernate_sequence.nextval as PK_VILKAAR_DETALJERT_UTFALL
  ,GJELDER_KRAVSTILLER
  ,GJELDER_AKTIVITET_TYPE
  ,GJELDER_ORGANISASJONSNUMMER
  ,GJELDER_AKTOR_ID
  ,GJELDER_ARBEIDSFORHOLD_ID
  ,DET_UTFALL
  ,localtimestamp as LASTET_DATO
  ,FK_PP_PERIODE_INNGANGSVILKAAR
from final
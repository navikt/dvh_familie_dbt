{{
    config(
        materialized='incremental'
    )
}}
with ef_meta_data as (
  select pk_ef_meta_data, kafka_offset, kafka_mottatt_dato, melding from {{ source ('fam_ef', 'fam_ef_meta_data') }}
  where kafka_mottatt_dato between to_timestamp('{{ var("dag_interval_start") }}', 'yyyy-mm-dd hh24:mi:ss')
  and to_timestamp('{{ var("dag_interval_end") }}', 'yyyy-mm-dd hh24:mi:ss')
),

pre_final as (
select * from ef_meta_data,
  json_table(melding, '$'
    COLUMNS (
          fagsak_id                       VARCHAR2 PATH '$.fagsakId'
         ,behandlings_id                  VARCHAR2 PATH '$.behandlingsId'
         ,relatert_behandlings_id         VARCHAR2 PATH '$.relatertBehandlingId'
         ,adressebeskyttelse              VARCHAR2 PATH '$.adressebeskyttelse'
         ,behandling_type                 VARCHAR2 PATH '$.behandlingType'
         ,behandling_aarsak               VARCHAR2 PATH '$.behandlingÅrsak'
         ,vedtaks_status                  VARCHAR2 PATH '$.vedtaksStatus'
         ,stonadstype                     VARCHAR2 PATH '$.stønadstype'
         ,person_ident                    VARCHAR2 PATH '$.person.personIdent'
         ,aktivitetsplikt_inntreffer_dato VARCHAR2 PATH '$.aktivitetskrav.aktivitetspliktInntrefferDato'
         ,har_sagt_opp_arbeidsforhold     VARCHAR2 PATH '$.aktivitetskrav.harSagtOppArbeidsforhold'
         ,funksjonell_id                  VARCHAR2 PATH '$.funksjonellId'
         ,vedtaks_tidspunkt               VARCHAR2 PATH '$.tidspunktVedtak'
         ,aktivitetsvilkaar_barnetilsyn   VARCHAR2 PATH '$.aktivitetskrav'
         ,vedtaksbegrunnelse_skole        VARCHAR2 PATH '$.vedtaksbegrunnelse'
         ,krav_motatt                     VARCHAR2 PATH '$.kravMottatt'
         ,årsak_revurderings_kilde        VARCHAR2 PATH '$.årsakRevurdering.opplysningskilde'
         ,revurderings_årsak              VARCHAR2 PATH '$.årsakRevurdering.årsak'
         )
        ) j
),

final as (
  select
     p.fagsak_id
    ,p.behandlings_id
    ,p.relatert_behandlings_id
    ,p.adressebeskyttelse
    ,p.behandling_type
    ,p.behandling_aarsak
    ,p.vedtaks_status
    ,p.stonadstype
    ,nvl(ident.fk_person1) as fk_person1
    ,p.aktivitetsplikt_inntreffer_dato
    ,p.har_sagt_opp_arbeidsforhold
    ,p.funksjonell_id
    ,p.vedtaks_tidspunkt
    ,p.aktivitetsvilkaar_barnetilsyn
    ,p.vedtaksbegrunnelse_skole
    ,p.krav_motatt
    ,p.årsak_revurderings_kilde
    ,p.revurderings_årsak
    /*
    ,CASE
      WHEN LENGTH(tidspunkt_vedtak) = 25 THEN CAST(to_timestamp_tz(tidspunkt_vedtak, 'yyyy-mm-dd"T"hh24:mi:ss TZH:TZM') AT TIME ZONE 'Europe/Belgrade' AS TIMESTAMP)
      ELSE CAST(to_timestamp_tz(tidspunkt_vedtak, 'FXYYYY-MM-DD"T"HH24:MI:SS.FXFF3TZH:TZM') AT TIME ZONE 'Europe/Belgrade' AS TIMESTAMP)
      END tidspunkt_vedtak
    */
    ,p.pk_ef_meta_data as fk_ef_meta_data
  from pre_final p
  left outer join dt_person.ident_off_id_til_fk_person1 ident
  on p.person_ident = ident.off_id
  and p.kafka_mottatt_dato between ident.gyldig_fra_dato and ident.gyldig_til_dato
  and ident.skjermet_kode = 0
)

select dvh_famef_kafka.hibernate_sequence.nextval as PK_EF_FAGSAK
      ,fk_ef_meta_data
      ,fagsak_id
      ,behandlings_id
      ,relatert_behandlings_id
      ,adressebeskyttelse
      ,behandling_type
      ,behandling_aarsak
      ,vedtaks_status
      ,stonadstype
      ,fk_person1
      ,aktivitetsplikt_inntreffer_dato
      ,har_sagt_opp_arbeidsforhold
      ,funksjonell_id
      ,vedtaks_tidspunkt
      ,aktivitetsvilkaar_barnetilsyn
      ,vedtaksbegrunnelse_skole
      ,krav_motatt
      ,årsak_revurderings_kilde
      ,revurderings_årsak
      ,kafka_offset
      ,kildesystem
      ,lastet_dato
      ,kafka_mottatt_dato
from final
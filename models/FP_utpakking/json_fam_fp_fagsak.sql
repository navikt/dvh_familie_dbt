{{
    config(
        materialized='incremental'
    )
}}

with fp_meta_data as (
  select * from {{ref ('fp_meldinger_til_aa_pakke_ut')}}
),

pre_final as (
  select *
  from fp_meta_data
      ,json_table(melding, '$'
        COLUMNS (
          saksnummer                 VARCHAR2 PATH '$.saksnummer'
         ,fagsak_id                  VARCHAR2 PATH '$.fagsakId'
         ,ytelse_type                VARCHAR2 PATH '$.ytelseType'
         ,lov_versjon                VARCHAR2 PATH '$.lovVersjon'
         ,behandling_uuid            VARCHAR2 PATH '$.behandlingUuid'
         ,forrige_behandling_uuid    VARCHAR2 PATH '$.forrigeBehandlingUuid'
         ,soknadsdato                VARCHAR2 PATH '$.søknadsdato'
         ,skjaeringstidspunkt        VARCHAR2 PATH '$.skjæringstidspunkt'
         ,vedtakstidspunkt           VARCHAR2 PATH '$.vedtakstidspunkt'
         ,vedtaksresultat            VARCHAR2 PATH '$.vedtaksresultat'
         ,vilkaar_ikke_oppfylt       VARCHAR2 PATH '$.vilkårIkkeOppfylt'
         ,soker_aktor_id             VARCHAR2 PATH '$.søker'
         ,sokers_rolle               VARCHAR2 PATH '$.søkersRolle'
         ,utlands_tilsnitt           VARCHAR2 PATH '$.utlandsTilsnitt'
         ,annen_forelder_aktor_id    VARCHAR2 PATH '$.annenForelder.aktørId'
         ,annen_forelder_saksnummer  VARCHAR2 PATH '$.annenForelder.saksnummer'
         ,annen_forelder_ytelse_type VARCHAR2 PATH '$.annenForelder.ytelseType'
         ,annen_forelder_saksrolle   VARCHAR2 PATH '$.annenForelder.saksrolle'
         ,termindato                 VARCHAR2 PATH '$.familieHendelse.termindato'
         ,adopsjonsdato              VARCHAR2 PATH '$.familieHendelse.adopsjonsdato'
         ,antall_barn                VARCHAR2 PATH '$.familieHendelse.antallBarn'
         ,fodselsdato                VARCHAR2 PATH '$.familieHendelse.barn[0].fødselsdato'
         ,hendelse_type              VARCHAR2 PATH '$.familieHendelse.hendelseType'
         ,utbetalingsreferanse       VARCHAR2 PATH '$.utbetalingsreferanse'
         ,behandling_id              VARCHAR2 PATH '$.behandlingId'
         ,engangsstonad_innvilget    VARCHAR2 PATH '$.engangsstønadInnvilget'
         ,dekningsgrad               VARCHAR2 PATH '$.foreldrepengerRettigheter.dekningsgrad'
         ,rettighet_type             VARCHAR2 PATH '$.foreldrepengerRettigheter.rettighetType'
         ,flerbarnsdager             VARCHAR2 PATH '$.foreldrepengerRettigheter.flerbarnsdager'
         ) ) j
),

final as (
  select
     p.saksnummer
    ,p.fagsak_id
    ,p.ytelse_type
    ,p.lov_versjon
    ,p.behandling_uuid
    ,p.forrige_behandling_uuid
    ,to_date(p.soknadsdato, 'yyyy-mm-dd') as soknadsdato
    ,to_date(p.skjaeringstidspunkt, 'yyyy-mm-dd') as skjaeringstidspunkt
    ,CASE
      WHEN LENGTH(vedtakstidspunkt) = 25 THEN CAST(to_timestamp_tz(vedtakstidspunkt, 'yyyy-mm-dd"T"hh24:mi:ss TZH:TZM') AT TIME ZONE 'Europe/Belgrade' AS TIMESTAMP)
      ELSE CAST(to_timestamp_tz(vedtakstidspunkt, 'YYYY-MM-DD"T"HH24:MI:SS.FF3 TZH:TZM') AT TIME ZONE 'Europe/Belgrade' AS TIMESTAMP)
      END vedtakstidspunkt
    ,p.vedtaksresultat
    ,p.vilkaar_ikke_oppfylt
    ,p.soker_aktor_id
    ,p.sokers_rolle
    ,p.utlands_tilsnitt
    ,p.annen_forelder_aktor_id
    ,p.annen_forelder_saksnummer
    ,p.annen_forelder_ytelse_type
    ,p.annen_forelder_saksrolle
    ,to_date(p.termindato, 'yyyy-mm-dd') as termindato
    ,to_date(p.adopsjonsdato, 'yyyy-mm-dd') as adopsjonsdato
    ,p.antall_barn
    ,to_date(p.fodselsdato, 'yyyy-mm-dd') as fodselsdato
    ,p.hendelse_type
    ,p.utbetalingsreferanse
    ,p.behandling_id
    ,p.engangsstonad_innvilget
    ,p.dekningsgrad
    ,p.rettighet_type
    ,p.flerbarnsdager
    ,p.pk_fp_meta_data as fk_fp_meta_data
    ,p.kafka_offset
    ,p.kafka_mottatt_dato
  from pre_final p
)

select
     dvh_fam_fp.fam_fp_seq.nextval as pk_fp_fagsak
    ,saksnummer
    ,fagsak_id
    ,ytelse_type
    ,lov_versjon
    ,behandling_uuid
    ,forrige_behandling_uuid
    ,soknadsdato
    ,skjaeringstidspunkt
    ,vedtakstidspunkt
    ,vedtaksresultat
    ,vilkaar_ikke_oppfylt
    ,soker_aktor_id
    ,ident.fk_person1 as soker_fk_person1
    ,sokers_rolle
    ,utlands_tilsnitt
    ,annen_forelder_aktor_id
    ,ident_annen.fk_person1 as annen_forelder_fk_person1
    ,annen_forelder_saksnummer
    ,annen_forelder_ytelse_type
    ,annen_forelder_saksrolle
    ,termindato
    ,adopsjonsdato
    ,antall_barn
    ,fodselsdato
    ,hendelse_type
    ,utbetalingsreferanse
    ,behandling_id
    ,engangsstonad_innvilget
    ,dekningsgrad
    ,rettighet_type
    ,flerbarnsdager
    ,fk_fp_meta_data
    ,kafka_offset
    ,kafka_mottatt_dato
    ,localtimestamp as lastet_dato
from final

left join dt_person.ident_aktor_til_fk_person1_ikke_skjermet ident
on final.soker_aktor_id = ident.aktor_id
and trunc(final.vedtakstidspunkt, 'dd') between ident.gyldig_fra_dato and ident.gyldig_til_dato

left join dt_person.ident_aktor_til_fk_person1_ikke_skjermet ident_annen
on final.annen_forelder_aktor_id = ident_annen.aktor_id
and trunc(final.vedtakstidspunkt, 'dd') between ident_annen.gyldig_fra_dato and ident_annen.gyldig_til_dato

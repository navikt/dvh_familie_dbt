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
          saksnummer                 VARCHAR2(255) PATH '$.saksnummer'
         ,fagsak_id                  VARCHAR2(255) PATH '$.fagsakId'
         ,ytelse_type                VARCHAR2(255) PATH '$.ytelseType'
         ,lov_versjon                VARCHAR2(255) PATH '$.lovVersjon'
         ,behandling_uuid            VARCHAR2(255) PATH '$.behandlingUuid'
         ,forrige_behandling_uuid    VARCHAR2(255) PATH '$.forrigeBehandlingUuid'
         ,revurdering_aarsak         VARCHAR2(255) PATH '$.revurderingÅrsak'
         ,soknadsdato                VARCHAR2(255) PATH '$.søknadsdato'
         ,skjaeringstidspunkt        VARCHAR2(255) PATH '$.skjæringstidspunkt'
         ,vedtakstidspunkt           VARCHAR2(255) PATH '$.vedtakstidspunkt'
         ,vedtaksresultat            VARCHAR2(255) PATH '$.vedtaksresultat'
         ,vilkaar_ikke_oppfylt       VARCHAR2(255) PATH '$.vilkårIkkeOppfylt'
         ,soker_aktor_id             VARCHAR2(255) PATH '$.søker'
         ,saksrolle                  VARCHAR2(255) PATH '$.saksrolle'
         ,utlands_tilsnitt           VARCHAR2(255) PATH '$.utlandsTilsnitt'
         ,annen_forelder_aktor_id    VARCHAR2(255) PATH '$.annenForelder.aktørId'
         ,annen_forelder_saksnummer  VARCHAR2(255) PATH '$.annenForelder.saksnummer'
         ,annen_forelder_ytelse_type VARCHAR2(255) PATH '$.annenForelder.ytelseType'
         ,annen_forelder_saksrolle   VARCHAR2(255) PATH '$.annenForelder.saksrolle'
         ,termindato                 VARCHAR2(255) PATH '$.familieHendelse.termindato'
         ,adopsjonsdato              VARCHAR2(255) PATH '$.familieHendelse.adopsjonsdato'
         ,antall_barn                VARCHAR2(255) PATH '$.familieHendelse.antallBarn'
         ,fodselsdato                VARCHAR2(255) PATH '$.familieHendelse.barn[0].fødselsdato'
         ,hendelse_type              VARCHAR2(255) PATH '$.familieHendelse.hendelseType'
         ,utbetalingsreferanse       VARCHAR2(255) PATH '$.utbetalingsreferanse'
         ,behandling_id              VARCHAR2(255) PATH '$.behandlingId'
         ,engangsstonad_innvilget    VARCHAR2(255) PATH '$.engangsstønadInnvilget'
         ,dekningsgrad               VARCHAR2(255) PATH '$.foreldrepengerRettigheter.dekningsgrad'
         ,rettighet_type             VARCHAR2(255) PATH '$.foreldrepengerRettigheter.rettighetType'
         ,nested PATH '$.stønadsutvidelser[*]' COLUMNS (
            type  VARCHAR2(255) PATH '$.type'
           ,dager NUMBER        PATH '$.dager'
          )
         ) ) j
),

pre_final_dager as
(
  select kafka_offset, j.saksnummer, j.behandling_uuid, j.type, j.dager
  from fp_meta_data
      ,json_table(melding, '$'
        COLUMNS (
          saksnummer                 VARCHAR2(255) PATH '$.saksnummer'
         ,behandling_uuid            VARCHAR2(255) PATH '$.behandlingUuid'
         ,nested PATH '$.foreldrepengerRettigheter.stønadsutvidelser[*]' COLUMNS (
            type  VARCHAR2(255) PATH '$.type'
           ,dager NUMBER        PATH '$.dager'
        ) ) ) j
),

final as (
  select distinct
     p.saksnummer
    ,p.fagsak_id
    ,p.ytelse_type
    ,p.lov_versjon
    ,p.behandling_uuid
    ,p.forrige_behandling_uuid
    ,p.revurdering_aarsak
    ,to_date(p.soknadsdato, 'yyyy-mm-dd') as soknadsdato
    ,to_date(p.skjaeringstidspunkt, 'yyyy-mm-dd') as skjaeringstidspunkt
    ,CASE
      WHEN LENGTH(vedtakstidspunkt) = 25 THEN CAST(to_timestamp_tz(vedtakstidspunkt, 'yyyy-mm-dd"T"hh24:mi:ss TZH:TZM') AT TIME ZONE 'Europe/Belgrade' AS TIMESTAMP)
      ELSE CAST(to_timestamp_tz(vedtakstidspunkt, 'YYYY-MM-DD"T"HH24:MI:SS.FF3 TZH:TZM') AT TIME ZONE 'Europe/Belgrade' AS TIMESTAMP)
      END vedtakstidspunkt
    ,p.vedtaksresultat
    ,p.vilkaar_ikke_oppfylt
    ,p.soker_aktor_id
    ,p.saksrolle
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
    ,flerbarnsdager.dager as flerbarnsdager
    ,prematurdager.dager as prematurdager
    ,p.pk_fp_meta_data as fk_fp_meta_data
    ,p.kafka_offset
    ,p.kafka_mottatt_dato
    ,p.kafka_partition
  from pre_final p

  left join pre_final_dager flerbarnsdager
  on p.kafka_offset = flerbarnsdager.kafka_offset
  and p.saksnummer = flerbarnsdager.saksnummer
  and p.behandling_uuid = flerbarnsdager.behandling_uuid
  and flerbarnsdager.type = 'FLERBARNSDAGER'

  left join pre_final_dager prematurdager
  on p.kafka_offset = prematurdager.kafka_offset
  and p.saksnummer = prematurdager.saksnummer
  and p.behandling_uuid = prematurdager.behandling_uuid
  and prematurdager.type = 'PREMATURDAGER'
)

select
     dvh_fam_fp.fam_fp_seq.nextval as pk_fp_fagsak
    ,saksnummer
    ,fagsak_id
    ,ytelse_type
    ,lov_versjon
    ,behandling_uuid
    ,forrige_behandling_uuid
    ,revurdering_aarsak
    ,soknadsdato
    ,skjaeringstidspunkt
    ,vedtakstidspunkt
    ,vedtaksresultat
    ,vilkaar_ikke_oppfylt
    ,soker_aktor_id
    ,ident.fk_person1 as soker_fk_person1
    ,saksrolle
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
    ,prematurdager
    ,kafka_partition
from final

join dt_person.ident_aktor_til_fk_person1_ikke_skjermet ident
on final.soker_aktor_id = ident.aktor_id
and trunc(final.vedtakstidspunkt, 'dd') between ident.gyldig_fra_dato and ident.gyldig_til_dato

join dt_person.ident_aktor_til_fk_person1_ikke_skjermet ident_annen
on final.annen_forelder_aktor_id = ident_annen.aktor_id
and trunc(final.vedtakstidspunkt, 'dd') between ident_annen.gyldig_fra_dato and ident_annen.gyldig_til_dato

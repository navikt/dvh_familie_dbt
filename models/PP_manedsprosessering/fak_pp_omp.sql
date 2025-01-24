{{
    config(
        materialized='table',
        schema='pp_omp'
    )
}}

with perioder as
(
  select *
  from {{ source('fam_pp', 'fam_pp_perioder') }}
),

tid as
(
  select *
  from {{ source('dt_kodeverk', 'dim_tid') }}
),

fagsak as
(
  select *
  from {{ source('fam_pp', 'fam_pp_fagsak') }}
),

vedtak1 as
(
  select fagsak.pk_pp_fagsak, fagsak.saksnummer, fagsak.fk_person1_mottaker, fagsak.utbetalingsreferanse
        ,fagsak.vedtaks_tidspunkt
        ,perioder.pk_pp_perioder, perioder1.dato_fom, perioder1.dato_tom
        ,tid.dato
        ,row_number() over (partition by fagsak.fk_person1_mottaker, tid.dato
                            order by fagsak.vedtaks_tidspunkt desc) nr
  from perioder

  join tid
  on tid.dato between perioder.dato_fom and perioder1.dato_tom
  and tid.dim_nivaa = 1
  and tid.gyldig_flagg = 1
  and tid.dato between to_date('01.12.2023','dd.mm.yyyy') and to_date('31.12.2023','dd.mm.yyyy')

  join fagsak
  on perioder.fk_pp_fagsak = fagsak.pk_pp_fagsak
  and fagsak.ytelse_type = 'OMP'
  and fagsak.fk_person1_mottaker != -1
  and fagsak.vedtaks_tidspunkt < sysdate

  where perioder.utfall = 'OPPFYLT'
)
--select * from perioder order by fk_person1_mottaker, saksnummer, dato_fom, dato_tom, dato;
--select * from fam_pp_fagsak where fk_person1_mottaker = '1036351612';
,

utbet as
(
  select *
  from {{ source('fam_pp', 'fam_pp_periode_utbet_grader') }}
),

vedtak2 as
(
  select vedtak1.*
        ,utbet.pk_pp_periode_utbet_grader, utbet.dagsats, utbet.utbetalingsgrad, utbet.bruker_er_mottaker
        ,utbet.aktivitet_status, utbet.arbeidsforhold_type
  from vedtak1

  join utbet
  on vedtak1.pk_pp_perioder = utbet.fk_pp_perioder
  and utbet.dagsats > 0

  where vedtak1.nr = 1
)
--select * from utbet order by fk_person1_mottaker, saksnummer, dato_fom, dato_tom, dato;
,

agg as
(
  select fk_person1_mottaker, dato_fom, dato_tom, saksnummer, utbetalingsreferanse
        ,count(distinct dato) as antall_dager
        ,sum(dagsats) as belop
        ,sum(utbetalingsgrad/100) as forbrukte_dager
  from vedtak2
  group by fk_person1_mottaker, dato_fom, dato_tom, saksnummer, utbetalingsreferanse
)
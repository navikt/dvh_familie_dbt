{{
    config(
        materialized='table'
    )
}}

with kjonn as (
     select *
     from {{ source('bt_statistikk_bank_dvh_fam_bt', 'statistikk_bank_mottaker_kjonn') }}
)
,

alder_gruppe as (
     select *
     from {{ source('bt_statistikk_bank_dvh_fam_bt', 'statistikk_bank_mottaker_alder_gruppe') }}
)
,

periode as (
     select aar, aar_kvartal, kvartal, kvartal_besk, forste_dato_i_perioden, siste_dato_i_perioden
     from {{ source('bt_statistikk_bank_dt_kodeverk', 'dim_tid') }}
     where gyldig_flagg = 1
     and dim_nivaa = 4 --På kvartal nivå
     and aar >= 2014
)
,

mottaker as (
     select *
     from {{ source('bt_statistikk_bank_dvh_fam_bt', 'fam_bt_mottaker') }}
     where (statusk != 4 and stat_aarmnd <= 202212) --Publisert statistikk(nav.no) til og med 2022, har data fra Infotrygd, og Institusjon(statusk=4) ble filtrert vekk.
     or stat_aarmnd >= 202301 --Statistikk fra og med 2023, inkluderer Institusjon.
)
,

mottaker_kjonn_alder as (
     select
          periode.aar
         ,periode.aar_kvartal
         ,periode.kvartal
         ,periode.kvartal_besk
         ,mottaker.stat_aarmnd
         ,kjonn.kjonn_besk
         ,alder_gruppe.alder_gruppe_besk
         ,count(distinct case when barn.fk_person1 is null then mottaker.fk_person1 end) antall --Ekskludere som barn selv er mottaker
         ,count(distinct case when barn.fk_person1 is not null then mottaker.fk_person1 end) antall_mottaker_barn --Barn selv er mottaker

     from mottaker

     join alder_gruppe
     on mottaker.alder between alder_gruppe.alder_fra_og_med and alder_gruppe.alder_til_og_med

     join kjonn
     on mottaker.kjonn = kjonn.kjonn

     left join
     (
          select stat_aarmnd, fk_person1
          from {{ source('bt_statistikk_bank_dvh_fam_bt', 'fam_bt_barn') }}
          where gyldig_flagg = 1
          and fk_person1 = fkb_person1 --Barn selv er mottaker
          group by stat_aarmnd, fk_person1
     ) barn
     on mottaker.fk_person1 = barn.fk_person1
     and mottaker.stat_aarmnd = barn.stat_aarmnd

     join periode
     on mottaker.stat_aarmnd = to_char(periode.siste_dato_i_perioden, 'yyyymm') --Siste måned i kvartal

     where mottaker.gyldig_flagg = 1
     and mottaker.belop > 0 -- Etterbetalinger telles ikke

     group by
          periode.aar
         ,periode.aar_kvartal
         ,periode.kvartal
         ,periode.kvartal_besk
         ,mottaker.stat_aarmnd
         ,kjonn.kjonn_besk
         ,alder_gruppe.alder_gruppe_besk
)
,

--Summere opp alle aldersgruppe og alle kjonn per periode. kjonn_besk='I alt' og alder_gruppe_besk='I alt'.
alt_sum as (
     select
          aar
         ,aar_kvartal
         ,kvartal
         ,kvartal_besk
         ,stat_aarmnd
         ,kjonn.kjonn_besk
         ,alder_gruppe.alder_gruppe_besk
         ,sum(antall) antall
         ,sum(antall_mottaker_barn) antall_mottaker_barn
         ,round(100,1) as prosent
     from mottaker_kjonn_alder

     join alder_gruppe
     on alder_gruppe.alder_fra_og_med = -1 --I alt

     join kjonn
     on kjonn.kjonn_kode = -1 --I alt

     group by
          aar
         ,aar_kvartal
         ,kvartal
         ,kvartal_besk
         ,stat_aarmnd
         ,kjonn.kjonn_besk
         ,alder_gruppe.alder_gruppe_besk
)
,

--Summere opp alle kjonn per aldersgruppe og per periode. kjonn_besk='I alt'.
--Regn ut prosent per aldersgruppe av total antall
alt_alder_gruppe as (
     select
          mottaker_kjonn_alder.aar
         ,mottaker_kjonn_alder.aar_kvartal
         ,mottaker_kjonn_alder.kvartal
         ,mottaker_kjonn_alder.kvartal_besk
         ,mottaker_kjonn_alder.stat_aarmnd
         ,kjonn.kjonn_besk
         ,mottaker_kjonn_alder.alder_gruppe_besk
         ,sum(mottaker_kjonn_alder.antall) antall
         ,sum(mottaker_kjonn_alder.antall_mottaker_barn) antall_mottaker_barn
         ,case when alt_sum.antall != 0 then round(sum(mottaker_kjonn_alder.antall)/alt_sum.antall*100,1) end prosent
     from mottaker_kjonn_alder

     join kjonn
     on kjonn.kjonn_kode = -1 --I alt

     join alt_sum
     on alt_sum.stat_aarmnd = mottaker_kjonn_alder.stat_aarmnd --Regn ut prosent for hver aldersgruppe av total antall av kjonn_besk='I alt'.

     group by
          mottaker_kjonn_alder.aar
         ,mottaker_kjonn_alder.aar_kvartal
         ,mottaker_kjonn_alder.kvartal
         ,mottaker_kjonn_alder.kvartal_besk
         ,mottaker_kjonn_alder.stat_aarmnd
         ,kjonn.kjonn_besk
         ,mottaker_kjonn_alder.alder_gruppe_besk
         ,alt_sum.antall
)
,

--Summere opp alle aldersgruppe. alder_gruppe_besk='I alt'.
kjonn_sum as (
     select
          aar
         ,aar_kvartal
         ,kvartal
         ,kvartal_besk
         ,stat_aarmnd
         ,kjonn_besk
         ,alder_gruppe.alder_gruppe_besk
         ,sum(antall) antall
         ,sum(antall_mottaker_barn) antall_mottaker_barn
         ,round(100,1) as prosent
     from mottaker_kjonn_alder

     join alder_gruppe
     on alder_gruppe.alder_fra_og_med = -1 --I alt

     group by
          aar
         ,aar_kvartal
         ,kvartal
         ,kvartal_besk
         ,stat_aarmnd
         ,kjonn_besk
         ,alder_gruppe.alder_gruppe_besk
)
,

--Regn ut prosent per aldersgruppe av total antall per kjonn
kjonn_alder_prosent as (
     select
          mottaker_kjonn_alder.*
         ,case when kjonn_sum.antall != 0 then round(mottaker_kjonn_alder.antall/kjonn_sum.antall*100,1) end prosent
     from mottaker_kjonn_alder

     join kjonn_sum
     on mottaker_kjonn_alder.stat_aarmnd = kjonn_sum.stat_aarmnd
     and mottaker_kjonn_alder.kjonn_besk = kjonn_sum.kjonn_besk
)
,

resultat as (
     select
          aar
         ,aar_kvartal
         ,kvartal
         ,kvartal_besk
         ,stat_aarmnd
         ,kjonn_besk
         ,alder_gruppe_besk
         ,antall
         ,antall_mottaker_barn
         ,prosent
     from alt_sum

     union all
     select
          aar
         ,aar_kvartal
         ,kvartal
         ,kvartal_besk
         ,stat_aarmnd
         ,kjonn_besk
         ,alder_gruppe_besk
         ,antall
         ,antall_mottaker_barn
         ,prosent
     from alt_alder_gruppe

     union all
     select
          aar
         ,aar_kvartal
         ,kvartal
         ,kvartal_besk
         ,stat_aarmnd
         ,kjonn_besk
         ,alder_gruppe_besk
         ,antall
         ,antall_mottaker_barn
         ,prosent
     from kjonn_sum

     union all
     select
          aar
         ,aar_kvartal
         ,kvartal
         ,kvartal_besk
         ,stat_aarmnd
         ,kjonn_besk
         ,alder_gruppe_besk
         ,antall
         ,antall_mottaker_barn
         ,prosent
     from kjonn_alder_prosent
)

select
     aar
    ,aar_kvartal
    ,kvartal
    ,kvartal_besk
    ,stat_aarmnd
    ,kjonn_besk
    ,alder_gruppe_besk
    ,antall
    ,antall_mottaker_barn
    ,prosent
    ,current_timestamp as lastet_dato
    ,current_timestamp as oppdatert_dato
from resultat


--Last opp kun ny periode siden siste periode fra tabellen
--Tidligste periode fra tabellen er 201401
{% if is_incremental() %}

where stat_aarmnd > (select coalesce(max(stat_aarmnd), 201400) from {{ this }})

{% endif %}
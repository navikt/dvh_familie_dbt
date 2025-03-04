{{
    config(
        materialized='incremental'
    )
}}

with kjonn as (
     select *
     from {{ source('statistikk_bank_dvh_fam_bt', 'statistikk_bank_mottaker_kjonn') }}
)
,

alder_gruppe as (
     select *
     from {{ source('statistikk_bank_dvh_fam_bt', 'statistikk_bank_mottaker_alder_gruppe') }}
)
,

periode as (
     select aar, aar_kvartal, kvartal, kvartal_besk, forste_dato_i_perioden, siste_dato_i_perioden
     from {{ source('statistikk_bank_dt_kodeverk', 'dim_tid') }}
     where gyldig_flagg = 1
     and dim_nivaa = 4 --På kvartal nivå
     and aar >= 2014
)
,

mottaker as (
     select *
     from {{ source('statistikk_bank_dvh_fam_bt', 'fam_bt_mottaker') }}
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
          from {{ source('statistikk_bank_dvh_fam_bt', 'fam_bt_barn') }}
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
--Summere opp alle alders gruppe
mottaker_kjonn_sum as (
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
--Summere opp alle kjonn
mottaker_alt_alder_gruppe as (
     select
          aar
         ,aar_kvartal
         ,kvartal
         ,kvartal_besk
         ,stat_aarmnd
         ,kjonn.kjonn_besk
         ,alder_gruppe_besk
         ,sum(antall) antall
         ,sum(antall_mottaker_barn) antall_mottaker_barn
     from mottaker_kjonn_alder

     join kjonn
     on kjonn.kjonn_kode = -1 --I alt

     group by
          aar
         ,aar_kvartal
         ,kvartal
         ,kvartal_besk
         ,stat_aarmnd
         ,kjonn.kjonn_besk
         ,alder_gruppe_besk
)
,
--Summere opp alle alders gruppe og alle kjonn
mottaker_alt_sum as (
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
     from mottaker_alt_sum

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
     from mottaker_alt_alder_gruppe

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
     from mottaker_kjonn_sum

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
     from mottaker_kjonn_alder
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
    ,current_timestamp as lastet_dato
    ,current_timestamp as oppdatert_dato
from resultat


--Last opp kun ny periode siden siste periode fra tabellen
--Tidligste periode fra tabellen er 201401
{% if is_incremental() %}

where stat_aarmnd > (select coalesce(max(stat_aarmnd), 201400) from {{ this }})

{% endif %}
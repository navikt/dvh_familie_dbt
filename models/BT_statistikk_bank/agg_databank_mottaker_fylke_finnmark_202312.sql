{{
  config(
    materialized='table'
  )
}}

with geografi as (
    select
        pk_dim_geografi
       ,navarende_fylke_nr
       ,gtverdi
       ,gyldig_fra_dato
       ,gyldig_til_dato
       ,kommune_navn
       ,fylke_nr
    from {{ source('bt_statistikk_bank_dt_kodeverk', 'dim_geografi') }}
)
,
--Dagens fylke
navarende_fylke as
(
    select distinct nåværende_fylke_nr_navn, nåværende_fylke_nr, nåværende_fylkenavn
    from {{ source('bt_statistikk_bank_dt_kodeverk', 'dim_geografi_fylke') }}
    where trunc(sysdate, 'dd') between funk_gyldig_fra_dato and funk_gyldig_til_dato
    and nåværende_fylke_nr not in (21, 22, 23, 97) --Svalbard, Jan Mayen, Kontinentalsokkelen, Svalbard og øvrige områder
)
,

periode as (
     select aar, aar_kvartal, kvartal, kvartal_besk, forste_dato_i_perioden, siste_dato_i_perioden
     from {{ source('bt_statistikk_bank_dt_kodeverk', 'dim_tid') }}
     where gyldig_flagg = 1
     and dim_nivaa = 4 --På kvartal nivå
     and aar >= 2015 --Ikke ta med årene før 2015
     and aar_kvartal = 202304
)
,

mottaker as (
     select *
     from {{ source('bt_statistikk_bank_dvh_fam_bt', 'fam_bt_mottaker') }}
     where (
               (statusk != 4 and stat_aarmnd <= 202212) --Publisert statistikk(nav.no) til og med 2022, har data fra Infotrygd, og Institusjon(statusk=4) ble filtrert vekk.
               or stat_aarmnd >= 202301 --Statistikk fra og med 2023, inkluderer Institusjon.
           )
     and gyldig_flagg = 1
     and belop > 0 -- Etterbetalinger telles ikke
)
,

--Hent ut fylkenr basert på fremmednøkkel til geografi
mottaker_geografi as (
    select
        periode.aar
       ,periode.aar_kvartal
       ,periode.kvartal
       ,periode.kvartal_besk
       ,mottaker.stat_aarmnd
       ,mottaker.fk_person1
       ,mottaker.fk_dim_geografi_bosted
       ,mottaker.mottaker_gt_verdi
       ,case when barn.fk_person1 is not null then 1 else 0 end barn_selv_mottaker_flagg
       ,geografi.navarende_fylke_nr
       ,geografi.kommune_navn
       ,geografi.fylke_nr
    from mottaker

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

    left join geografi
    on mottaker.fk_dim_geografi_bosted = geografi.pk_dim_geografi
)
--select * from mottaker_geografi where kommune_navn = 'Lunner';
,

--Hent ut fylkenr basert på gtverdi for de som har Ukjent nåværende fylkenr etter forrige steg
mottaker_ukjent_gtverdi as
(
    select
        mottaker_geografi.fk_person1
       ,mottaker_geografi.aar
       ,mottaker_geografi.aar_kvartal
       ,mottaker_geografi.kvartal
       ,mottaker_geografi.kvartal_besk
       ,mottaker_geografi.stat_aarmnd
       ,mottaker_geografi.barn_selv_mottaker_flagg
       ,mottaker_geografi.mottaker_gt_verdi
       ,dim_land.land_iso_3_kode
       ,case when dim_land.land_iso_3_kode is not null then '98'
             else gt_verdi.navarende_fylke_nr
        end navarende_fylke_nr --Når det er landskode på gtverdi, tilhører det Utland(fylkenr=98)
       ,gt_verdi.gtverdi
    from mottaker_geografi

    left outer join
    (
        select distinct land_iso_3_kode
        from dt_kodeverk.dim_land
    ) dim_land
    on mottaker_geografi.mottaker_gt_verdi = dim_land.land_iso_3_kode

    left join
    (
        select gtverdi
              ,max(navarende_fylke_nr) keep (dense_rank first order by gyldig_fra_dato desc) navarende_fylke_nr
        from geografi
        group by gtverdi
    ) gt_verdi
    on mottaker_geografi.mottaker_gt_verdi = gt_verdi.gtverdi

    where mottaker_geografi.navarende_fylke_nr = 'Ukjent'
)
--select * from mottaker_ukjent_gtverdi;
,

mottaker_alle as
(
    select
        fk_person1
       ,aar
       ,aar_kvartal
       ,kvartal
       ,kvartal_besk
       ,stat_aarmnd
       ,barn_selv_mottaker_flagg
       ,navarende_fylke_nr
    from mottaker_geografi
    where navarende_fylke_nr != 'Ukjent' or navarende_fylke_nr is null

    union all
    select
        fk_person1
       ,aar
       ,aar_kvartal
       ,kvartal
       ,kvartal_besk
       ,stat_aarmnd
       ,barn_selv_mottaker_flagg
       ,navarende_fylke_nr
    from mottaker_ukjent_gtverdi
)
select *
from mottaker_alle
where navarende_fylke_nr = 56 --56 Finnmark - Finnmárku - Finmarkku

--Last opp kun ny periode siden siste periode fra tabellen
--Tidligste periode fra tabellen er 201501
{% if is_incremental() %}

where stat_aarmnd > (select coalesce(max(stat_aarmnd), 201500) from {{ this }})

{% endif %}
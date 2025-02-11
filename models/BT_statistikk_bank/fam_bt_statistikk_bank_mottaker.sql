{{
    config(
        materialized='table'
    )
}}

with mottaker_kjonn as (
  select periode.aar
       , mottaker.kjonn
       , alder_gruppe.alder_gruppe_besk
       , count(distinct case when barn.fk_person1 is null then mottaker.fk_person1 end) antall --Ekskludere som barn selv er mottaker

  from {{ source('statistikk_bank_dvh_fam_bt', 'fam_bt_mottaker') }} mottaker

  join {{ ref('statistikk_bank_mottaker_alder_gruppe') }} alder_gruppe
  on mottaker.alder between alder_gruppe.alder_fra_og_med and alder_gruppe.alder_til_og_med

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

  join {{ ref('statistikk_bank_mottaker_periode') }} periode
  on mottaker.stat_aarmnd = periode.aar_maaned

  where mottaker.gyldig_flagg = 1
  and mottaker.belop > 0 -- Etterbetalinger telles ikke

  group by periode.aar, mottaker.kjonn, alder_gruppe.alder_gruppe_besk
  order by periode.aar, mottaker.kjonn, alder_gruppe.alder_gruppe_besk
)
,
mottaker_kjonn_sum as (
  select kjonn
       , aar
       , 'ALT' as alder_gruppe_besk
       , sum(antall)  antall
  from mottaker_kjonn
  group by kjonn, aar
)
,
mottaker_alt_alder_gruppe as (
  select 'ALT' as kjonn
       , mottaker.aar
       , mottaker.alder_gruppe_besk
       , sum(mottaker.antall) antall
  from mottaker_kjonn mottaker
  group by mottaker.aar, mottaker.alder_gruppe_besk
)
,
mottaker_alt_sum as (
  select 'ALT' as kjonn
       , aar
       , 'ALT' as alder_gruppe_besk
       , sum(antall)  antall
  from mottaker_alt_alder_gruppe
  group by aar
)

select kjonn
     , aar
     , alder_gruppe_besk
     , antall
from mottaker_alt_sum

union all
select kjonn
     , aar
     , alder_gruppe_besk
     , antall
from mottaker_alt_alder_gruppe

union all
select kjonn
     , aar
     , alder_gruppe_besk
     , antall
from mottaker_kjonn_sum

union all
select kjonn
     , aar
     , alder_gruppe_besk
     , antall
from mottaker_kjonn




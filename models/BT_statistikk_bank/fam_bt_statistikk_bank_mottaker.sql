{{
    config(
        materialized='table'
    )
}}

with mottaker as (
  select substr(mottaker.stat_aarmnd, 1, 4) aar
       , mottaker.kjonn
       , alder_gruppe.alder_gruppe_besk
       , count(distinct case when barn.fk_person1 is null then mottaker.fk_person1 end) antall --Ekskludere barn selv er mottaker
  from {{ source('dvh_fam_bt', 'fam_bt_mottaker') }} mottaker

  join {{ source('dvh_fam_bt', 'fam_bt_statistikk_alder_gruppe') }} alder_gruppe
  on mottaker.alder between alder_gruppe.alder_fra_og_med and alder_gruppe.alder_til_og_med

  left join
  (
    select stat_aarmnd, fk_person1
    from {{ source('dvh_fam_bt', 'fam_bt_barn') }}
    where gyldig_flagg = 1
    and fk_person1 = fkb_person1 --Barn selv er mottaker
    group by stat_aarmnd, fk_person1
   ) barn
  on mottaker.fk_person1 = barn.fk_person1
  and mottaker.stat_aarmnd = barn.stat_aarmnd

  where mottaker.gyldig_flagg = 1
  and substr(mottaker.stat_aarmnd, 1, 4) between 2015 and 2024
  and substr(mottaker.stat_aarmnd, 5, 2) = 12
  --and mottaker.belop > mottaker.belope -- Etterbetalinger telles ikke
  and mottaker.belop > 0 -- Etterbetalinger telles ikke

  group by mottaker.stat_aarmnd, mottaker.kjonn, alder_gruppe.alder_gruppe_besk
  order by mottaker.kjonn, mottaker.stat_aarmnd, alder_gruppe.alder_gruppe_besk
)
select *
from mottaker
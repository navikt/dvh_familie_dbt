{{
    config(
        materialized='table'
    )
}}

with mottaker as (
  select substr(mottaker.stat_aarmnd, 1, 4) aar
       , mottaker.kjonn
       , alder_gruppe.alder_gruppe_besk
       , count(1) antall
  from {{ source('bt_statistikk_bank', 'fam_bt_mottaker') }} mottaker

  join {{ source('bt_statistikk_bank', 'fam_bt_statistikk_alder_gruppe') }} alder_gruppe
  on mottaker.alder between alder_gruppe.alder_fra_og_med and alder_gruppe.alder_til_og_med

  where mottaker.gyldig_flagg = 1
  and substr(mottaker.stat_aarmnd, 1, 4) between 2015 and 2024
  and substr(mottaker.stat_aarmnd, 5, 2) = 12

  group by mottaker.stat_aarmnd, mottaker.kjonn, alder_gruppe.alder_gruppe_besk
  order by mottaker.kjonn, mottaker.stat_aarmnd, alder_gruppe.alder_gruppe_besk
)
select *
from mottaker
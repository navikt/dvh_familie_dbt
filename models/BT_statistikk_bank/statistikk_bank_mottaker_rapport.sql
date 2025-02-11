{{
    config(
        materialized='table'
    )
}}

with header as (
  select 'Kjønn og alder' as felt_besk
       , 0 as rapport_rekkefolge_kjonn
       , 0 as rapport_rekkefolge_alder_gruppe
       , 2015 as aar_2015
       , 2016 as aar_2016
       , 2017 as aar_2017
       , 2018 as aar_2018
       , 2019 as aar_2019
       , 2020 as aar_2020
       , 2021 as aar_2021
       , 2022 as aar_2022
       , 2023 as aar_2023
       , 2024 as aar_2024
  from dual
)
,
final as (
  select *
  from header

  union
  select case when kjonn.kjonn = 'ALT' and alder_gruppe.alder_gruppe_besk = 'ALT' then kjonn.kjonn_besk
              when kjonn.kjonn != 'ALT' and alder_gruppe.alder_gruppe_besk = 'ALT' then kjonn.kjonn_besk
              else alder_gruppe.alder_gruppe_besk
         end felt_besk
       , kjonn.rapport_rekkefolge as rapport_rekkefolge_kjonn
       , alder_gruppe.rapport_rekkefolge as rapport_rekkefolge_alder_gruppe
       , max(case when mottaker.aar = 2015 then mottaker.antall end) as aar_2015
       , max(case when mottaker.aar = 2016 then mottaker.antall end) as aar_2016
       , max(case when mottaker.aar = 2017 then mottaker.antall end) as aar_2017
       , max(case when mottaker.aar = 2018 then mottaker.antall end) as aar_2018
       , max(case when mottaker.aar = 2019 then mottaker.antall end) as aar_2019
       , max(case when mottaker.aar = 2020 then mottaker.antall end) as aar_2020
       , max(case when mottaker.aar = 2021 then mottaker.antall end) as aar_2021
       , max(case when mottaker.aar = 2022 then mottaker.antall end) as aar_2022
       , max(case when mottaker.aar = 2023 then mottaker.antall end) as aar_2023
       , max(case when mottaker.aar = 2024 then mottaker.antall end) as aar_2024
  from {{ ref('fam_bt_statistikk_bank_mottaker') }} mottaker

  join {{ ref('statistikk_bank_mottaker_kjonn') }} kjonn
  on mottaker.kjonn = kjonn.kjonn

  join {{ ref('statistikk_bank_mottaker_alder_gruppe') }} alder_gruppe
  on mottaker.alder_gruppe_besk = alder_gruppe.alder_gruppe_besk

  group by kjonn.kjonn, kjonn.kjonn_besk, kjonn.rapport_rekkefolge, alder_gruppe.alder_gruppe_besk
         , alder_gruppe.rapport_rekkefolge
)

select felt_besk
     , aar_2015
     , aar_2016
     , aar_2017
     , aar_2018
     , aar_2019
     , aar_2020
     , aar_2021
     , aar_2022
     , aar_2023
     , aar_2024
from final
order by rapport_rekkefolge_kjonn, rapport_rekkefolge_alder_gruppe
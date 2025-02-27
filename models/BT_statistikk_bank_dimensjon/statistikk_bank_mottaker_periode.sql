{{
    config(
        materialized='table'
    )
}}

with aar as (
  select 2014 + level as aar
  from dual
  connect by level <= 10
)
,

aar_maaned as (
  select aar.aar, tid.aar_maaned
  from aar

  join {{ source('statistikk_bank_dt_kodeverk', 'dim_tid') }} tid
  on aar.aar = tid.aar
  and tid.gyldig_flagg = 1
  and tid.dim_nivaa = 3
  and tid.maaned = 12
)

select aar
     , aar_maaned
from aar_maaned
order by aar_maaned
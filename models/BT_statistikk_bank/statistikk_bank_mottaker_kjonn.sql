{{
    config(
        materialized='table'
    )
}}

with kjonn as (
  select 'K' as kjonn, 'Kvinner' as kjonn_besk, 0 as kjonn_kode, 3 as rapport_rekkefolge from dual

  union all
  select 'M' as kjonn, 'Menn' as kjonn_besk, 1 as kjonn_kode, 2 as rapport_rekkefolge from dual

  union all
  select 'ALT' as kjonn, 'I alt' as kjonn_besk, -1 as kjonn_kode, 1 as rapport_rekkefolge from dual
)

select kjonn
     , kjonn_besk
     , kjonn_kode
     , rapport_rekkefolge
from kjonn
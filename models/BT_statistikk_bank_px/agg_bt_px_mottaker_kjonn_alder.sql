{{
    config(
        materialized='table'
    )
}}

with data as (
  select *
  from {{ ref('v_agg_bt_px_mottaker_kjonn_alder') }}
)

select kjonn_besk
      ,sortering_kjonn
      ,alder_gruppe_besk
      ,sortering_alder_gruppe
      ,statistikkvariabel
      ,aar
      ,aar_kvartal
      ,kvartal
      ,kvartal_besk
      ,px_data
from data
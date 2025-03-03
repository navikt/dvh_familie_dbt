{{
    config(
        materialized='incremental',
        unique_key='aar'
    )
}}

with data as (
  select *
  from {{ ref('v_agg_bt_px_mottaker_kjonn_alder') }}
)

select kjonn_besk
      ,alder_gruppe_besk
      ,aar
      ,statistikkvariabel
      ,px_data
from data
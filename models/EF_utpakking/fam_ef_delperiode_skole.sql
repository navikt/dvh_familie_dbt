{{
    config(
        materialized='incremental'
    )
}}

with ef_meta_data as (
  select * from {{ref ('meldinger_til_aa_pakke_ut')}}
),


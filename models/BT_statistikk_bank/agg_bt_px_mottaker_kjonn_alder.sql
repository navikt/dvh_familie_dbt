{{
    config(
        materialized='view'
    )
}}

with alle_kjonn_alder as (
    select kjonn.kjonn_besk
          ,alder_gruppe.alder_gruppe_besk
          ,periode.aar
    from {{ source('statistikk_bank_dvh_fam_bt', 'statistikk_bank_mottaker_kjonn') }} kjonn
    cross join {{ source('statistikk_bank_bt', 'statistikk_bank_mottaker_alder_gruppe') }} alder_gruppe
    cross join {{ source('statistikk_bank_bt', 'statistikk_bank_mottaker_periode') }} periode
)
,
fakta_tall as (
    select mottaker.*
          ,kjonn.kjonn_besk
    from {{ source('statistikk_bank_bt', 'agg_bt_statistikk_bank_mottaker') }} mottaker
    join {{ source('statistikk_bank_bt', 'statistikk_bank_mottaker_kjonn')}} kjonn
    on mottaker.kjonn = kjonn.kjonn
)
,
resultat as (
    select alle_kjonn_alder.*
          ,fakta_tall.antall
    from fakta_tall
    full outer join alle_kjonn_alder
    on fakta_tall.kjonn_besk = alle_kjonn_alder.kjonn_besk
    and fakta_tall.aar = alle_kjonn_alder.aar
    and fakta_tall.alder_gruppe_besk = alle_kjonn_alder.alder_gruppe_besk
)
select *
from resultat
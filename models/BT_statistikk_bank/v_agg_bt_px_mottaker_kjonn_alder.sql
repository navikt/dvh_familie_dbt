{{
    config(
        materialized='view'
    )
}}

--Hent ut alle kjonn og alder som finnes i dimensjonstabell
with dim_alle_kjonn_alder as (
    select kjonn.kjonn_besk
          ,kjonn.rapport_rekkefolge kjonn_rapport_rekkefolge
          ,alder_gruppe.alder_gruppe_besk
          ,alder_gruppe.rapport_rekkefolge alder_rapport_rekkefolge
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
    select dim.kjonn_besk
          ,dim.alder_gruppe_besk
          ,dim.aar
          ,'antall' as statistikkvariabel
          ,fakta_tall.antall as px_data
    from fakta_tall
    full outer join dim_alle_kjonn_alder dim
    on fakta_tall.kjonn_besk = dim.kjonn_besk
    and fakta_tall.aar = dim.aar
    and fakta_tall.alder_gruppe_besk = dim.alder_gruppe_besk
)
select *
from resultat
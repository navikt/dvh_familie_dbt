{{
    config(
        materialized='view'
    )
}}

--Hent ut alle kjonn og alder som finnes i dimensjonstabell
with dim_alle_kjonn_alder as (
    select
        kjonn.kjonn_besk
       ,kjonn.rapport_rekkefolge sortering_kjonn
       ,alder_gruppe.alder_gruppe_besk
       ,alder_gruppe.rapport_rekkefolge sortering_alder_gruppe
    from {{ source('statistikk_bank_dvh_fam_bt', 'statistikk_bank_mottaker_kjonn') }} kjonn
    cross join {{ source('statistikk_bank_bt', 'statistikk_bank_mottaker_alder_gruppe') }} alder_gruppe
)
,

fakta_tall as (
    select mottaker.*
    from {{ source('statistikk_bank_bt', 'agg_bt_statistikk_bank_mottaker') }} mottaker
)
,

resultat as (
    select
        dim.kjonn_besk
       ,dim.sortering_kjonn
       ,dim.alder_gruppe_besk
       ,dim.sortering_alder_gruppe
       ,'antall' as statistikkvariabel
       ,fakta_tall.aar
       ,fakta_tall.aar_kvartal
       ,fakta_tall.kvartal
       ,fakta_tall.kvartal_besk
       ,fakta_tall.antall as px_data
    from fakta_tall
    full outer join dim_alle_kjonn_alder dim
    on fakta_tall.kjonn_besk = dim.kjonn_besk
    and fakta_tall.alder_gruppe_besk = dim.alder_gruppe_besk
)
select *
from resultat
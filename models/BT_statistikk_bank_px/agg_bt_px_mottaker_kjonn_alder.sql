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
    from {{ source('bt_statistikk_bank_px_dvh_fam_bt', 'statistikk_bank_mottaker_kjonn') }} kjonn
    cross join {{ source('bt_statistikk_bank_px_dvh_fam_bt', 'statistikk_bank_mottaker_alder_gruppe') }} alder_gruppe
)
,

mottaker as (
    select *
    from {{ source('bt_statistikk_bank_px_dvh_fam_bt', 'agg_bt_statistikk_bank_mottaker') }}
)
,

resultat as (
    --Antall mottaker
    select
        dim.kjonn_besk
       ,dim.sortering_kjonn
       ,dim.alder_gruppe_besk
       ,dim.sortering_alder_gruppe
       ,'antall' as statistikkvariabel
       ,mottaker.aar
       ,mottaker.aar_kvartal
       ,mottaker.kvartal
       ,mottaker.kvartal_besk
       ,mottaker.antall as px_data
    from mottaker
    full outer join dim_alle_kjonn_alder dim
    on mottaker.kjonn_besk = dim.kjonn_besk
    and mottaker.alder_gruppe_besk = dim.alder_gruppe_besk

    --Prosent av antall mottaker
    union all
    select
        dim.kjonn_besk
       ,dim.sortering_kjonn
       ,dim.alder_gruppe_besk
       ,dim.sortering_alder_gruppe
       ,'prosent' as statistikkvariabel
       ,mottaker.aar
       ,mottaker.aar_kvartal
       ,mottaker.kvartal
       ,mottaker.kvartal_besk
       ,mottaker.prosent as px_data
    from mottaker
    full outer join dim_alle_kjonn_alder dim
    on mottaker.kjonn_besk = dim.kjonn_besk
    and mottaker.alder_gruppe_besk = dim.alder_gruppe_besk
)
select *
from resultat
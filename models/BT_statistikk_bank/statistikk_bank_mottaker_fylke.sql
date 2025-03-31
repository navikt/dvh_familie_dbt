{{
    config(
        materialized='table'
    )
}}

with fylke as (
    select distinct nåværende_fylke_nr_navn, nåværende_fylke_nr, nåværende_fylkenavn
    from {{ source('bt_statistikk_bank_dt_kodeverk', 'dim_geografi_fylke') }}
    where trunc(sysdate, 'dd') between funk_gyldig_fra_dato and funk_gyldig_til_dato
    and nåværende_fylke_nr not in (21, 22, 23, 97) --Svalbard, Jan Mayen, Kontinentalsokkelen, Svalbard og øvrige områder
)

select nåværende_fylke_nr_navn
      ,nåværende_fylke_nr
      ,nåværende_fylkenavn
from fylke

union all
select '-01 I alt' as nåværende_fylke_nr_navn
      ,'-01' as nåværende_fylke_nr
      ,'I alt' as nåværende_fylkenavn
from dual

order by nåværende_fylke_nr

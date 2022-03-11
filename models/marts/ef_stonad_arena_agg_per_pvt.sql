with ef_stonad_arena_agg_pr_person_ as (
  select * from {{ref ('ef_stonad_arena_agg_pr_person')}}
),

hoy_lav as (
  select fodselsnummer_gjeldende,
    max(antblav) as antblav,
    max(antbhoy) as antbhoy
  from ef_stonad_arena_agg_pr_person_
  group by fodselsnummer_gjeldende
),

trans_ef_arena as (
  select * from
    (
      select periode,
        stonad_kode,
        postert_belop,
        alder,
        kommune_nr,
        bydel_nr,
        kjonn_kode,
        maalgruppe_kode,
        maalgruppe_navn,
        statsborgerskap,
        fodeland,
        sivilstatus_kode,
        barn_under_18_antall,
        inntekt_siste_beraar,
        inntekt_3_siste_beraar,
        fodselsnummer_gjeldende
      from ef_stonad_arena_agg_pr_person_
    )
pivot(max(postert_belop) for stonad_kode
IN ('TSOBOUTG','TSODAGREIS','TSOFLYTT','TSOLMIDLER','TSOREISAKT','TSOREISARB','TSOREISOBL','TSOTILBARN','TSOTILFAM'))

),

final as (
  select trans_ef_arena.*, hoy_lav.antblav, hoy_lav.antbhoy
  from trans_ef_arena join hoy_lav
  on trans_ef_arena.fodselsnummer_gjeldende = hoy_lav.fodselsnummer_gjeldende
  order by trans_ef_arena.fodselsnummer_gjeldende
)

select * from final
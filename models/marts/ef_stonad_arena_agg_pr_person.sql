with ef_stonad_arena_ as (
  select * from {{ref ('ef_stonad_arena')}}
),

final as (
  select periode,
    alder,
    STONAD_kode,
    kommune_nr,
    bydel_nr,
    kjonn_kode,
    maalgruppe_kode,
    maalgruppe_navn,
    statsborgerskap,
    fodeland,
    sivilstatus_kode,
    max(antblav) as antblav,
    max(antbhoy) as antbhoy,
    barn_under_18_antall,
    inntekt_siste_beraar,
    inntekt_3_siste_beraar,
    fodselsnummer_gjeldende,
	  sum(postert_belop) as postert_belop

  from ef_stonad_arena_
  group by periode,
    alder,
    STONAD_kode,
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

  order by periode,
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
)

select * from final


with fak_stonad_ as (
    select * from {{source ('arena_stonad', 'fak_stonad')}}
),

dim_omraade as(
    select  pk_dim_f_stonad_omraade, stonad_kode
    from {{source ('arena_stonad', 'dim_f_stonad_omraade')}}
    where stonad_kode in ('TSOBOUTG','TSODAGREIS','TSOFLYTT','TSOLMIDLER','TSOREISAKT',
    'TSOREISARB','TSOREISOBL','TSOTILBARN','TSOTILFAM')
),

person as (
    select * from {{source ('arena_stonad', 'dim_person')}}
),

person_kontaktinfo as (
    select * from {{source ('dt_person_arena', 'dim_person_kontaktinfo')}}
),

dim_kjonn_ as (
    select * from {{source ('arena_stonad','dim_kjonn')}}
),

dim_alder_ as (
    select * from {{source ('arena_stonad','dim_alder')}}
),

dim_maalgruppe_type_ as (
    select pk_dim_maalgruppe_type, maalgruppe_kode, maalgruppe_navn_scd1
    from {{source ('arena_stonad', 'dim_maalgruppe_type')}}
    where maalgruppe_kode in ('ENSFORUTD','ENSFORARBS','TIDLFAMPL','GJENEKUTD','GJENEKARBS')
),

dim_vedtak_postering_ as (
    select * from {{source ('arena_stonad','dim_vedtak_postering')}}
),

dim_geo as (
    select * from {{source ('arena_stonad','dim_geografi')}}
),

final as (
    select {{var ("periode")}} as periode,
		fs.lk_postering_id,
		ald.alder,
		so.STONAD_kode,
		geo.kommune_nr,
		geo.bydel_nr,
		kjonn.kjonn_kode,
		maalt.maalgruppe_kode,
		maalt.maalgruppe_navn_scd1 as maalgruppe_navn,
		per.statsborgerskap,
		per.fodeland,
		per.sivilstatus_kode,
		per.barn_under_18_antall,
		vp.antblav,
		vp.antbhoy,
		fs.postert_dato,
		fs.fk_dim_tid_postering_fra_dato as postering_fra_dato,
		fs.fk_dim_tid_postering_til_dato as postering_til_dato,
		fs.inntekt_siste_beraar,
		fs.inntekt_3_siste_beraar,
		dtp.fodselsnummer_gjeldende,
	    /*fs.stonadberett_aktivitet_flagg,*/
		fs.postert_belop

    from fak_stonad_ fs
    join dim_omraade so
    on fs.fk_dim_f_stonad_omraade = so.pk_dim_f_stonad_omraade

    join dim_maalgruppe_type_ maalt
    on fs.fk_dim_maalgruppe_type = maalt.pk_dim_maalgruppe_type

    join person per
    on fs.fk_dim_person = per.pk_dim_person

    join person_kontaktinfo dtp
    on per.fk_person1 = dtp.fk_person1

    join dim_kjonn_ kjonn
    on fs.fk_dim_kjonn = kjonn.pk_dim_kjonn

    join dim_alder_ ald
    on fs.fk_dim_alder = ald.pk_dim_alder

    join dim_vedtak_postering_ vp
    on fs.fk_dim_vedtak_postering = vp.pk_dim_vedtak_postering

    join dim_geo geo
    on fs.fk_dim_geografi_bosted = geo.pk_dim_geografi
    where trunc(fs.postert_dato) >= to_date({{var ("periode")}}||'01','yyyymmdd')
    and trunc(fs.postert_dato) <= last_day(to_date({{var ("periode")}}||'01','yyyymmdd'))
)

select * from final


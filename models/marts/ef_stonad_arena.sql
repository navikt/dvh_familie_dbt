
with fak_stonad1 as (
    select * from {{source ('arena_stonad', 'fak_stonad')}}
),

dim_maalgruppe_type1 as (
    select * from {{source ('arena_stonad', 'dim_maalgruppe_type')}}
),

person_ident as (
    select * from {{source ('dt_person_arena', 'dvh_person_ident_off_id_ikke_skjermet')}}
),

person as (
    select * from {{source ('dt_person_arena', 'dim_person')}}
),

fam_ef_arena_dim_maalgruppe as(
  select pk_dim_maalgruppe_type, maalgruppe_kode, maalgruppe_navn_scd1 
  from {{source ('arena_stonad', 'dim_maalgruppe_type')}}
  where maalgruppe_kode in ('ENSFORUTD','ENSFORARBS','TIDLFAMPL','GJENEKUTD','GJENEKARBS') 
),

fam_ef_arena_dim_omraade as(
    select  pk_dim_f_stonad_omraade, stonad_kode
    from {{source ('arena_stonad', 'DIM_F_STONAD_OMRAADE')}}
    where stonad_kode in ('TSOBOUTG','TSODAGREIS','TSOFLYTT','TSOLMIDLER','TSOREISAKT',
    'TSOREISARB','TSOREISOBL','TSOTILBARN','TSOTILFAM')
),

final as (
    select {{var ("periode")}} as periode, 
            fs.lk_postering_id,
            /*
            ald.alder,
            so.stonad_kode,
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
            */
            ident.off_id as person_id_off,

            

            --dkp.ident_type,--???
            /*fs.stonadberett_aktivitet_flagg,*/
            fs.postert_belop	 
      from fak_stonad1 fs 
      
      --join dim_omraade so
      --on fs.fk_dim_f_stonad_omraade = so.pk_dim_f_stonad_omraade
      
      join dim_maalgruppe_type1 maalt 
      on fs.fk_dim_maalgruppe_type = maalt.pk_dim_maalgruppe_type
      
      
      join person per
      on fs.fk_dim_person = per.pk_dim_person 
      
      join person_ident ident
      on per.fk_person1 = ident.fk_person1
      --and ident.gyldig_flagg = 1--????
      /*
      join dt_kodeverk.dim_kjonn kjonn
      on fs.fk_dim_kjonn = kjonn.pk_dim_kjonn
      

      join dt_kodeverk.dim_alder ald
      on fs.fk_dim_alder = ald.pk_dim_alder

      join dt_p.dim_vedtak_postering vp
      on fs.fk_dim_vedtak_postering = vp.pk_dim_vedtak_postering
    		
      join dt_kodeverk.dim_geografi geo
      on fs.fk_dim_geografi_bosted = geo.pk_dim_geografi
      */
      where trunc(fs.postert_dato) >= to_date({{var ("periode")}}||'01','yyyymmdd') 
      and trunc(fs.postert_dato) <= last_day(to_date({{var ("periode")}}||'01','yyyymmdd'))
)

select * from final



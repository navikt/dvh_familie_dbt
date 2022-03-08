with stg_fam_ef_arena_dim_maalgruppe as(
  select * from DT_p.dim_maalgruppe_type
),

final as (
  select pk_dim_maalgruppe_type, maalgruppe_kode, maalgruppe_navn_scd1 from stg_fam_ef_arena_dim_maalgruppe
  where maalgruppe_kode in ('ENSFORUTD','ENSFORARBS','TIDLFAMPL','GJENEKUTD','GJENEKARBS')
)

select * from final
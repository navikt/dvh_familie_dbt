with fam_ef_arena_dim_omraade as(
  select * from {{source ('arena_stonad', 'DIM_F_STONAD_OMRAADE')}}
),

final as (
  select pk_dim_f_stonad_omraade, stonad_kode
  from fam_ef_arena_dim_omraade
  where stonad_kode in ('TSOBOUTG','TSODAGREIS','TSOFLYTT','TSOLMIDLER','TSOREISAKT',
  'TSOREISARB','TSOREISOBL','TSOTILBARN','TSOTILFAM')
)

select * from final



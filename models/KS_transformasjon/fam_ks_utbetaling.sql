with kafka_ny_losning as (
  select kafka_offset , melding from {{ source ('fam_ks', 'fam_ks_meta_data') }}
),

pre_final as (
select *  from kafka_ny_losning,
  json_table(melding, '$'
    columns(
        fagsak_id  path  '$.fagsakId',
          nested path '$.utbetalingsperioder[*]'
          columns(
            hjemmel path '$.hjemmel',
            utbetalt_per_mnd path '$.utbetaltPerMnd',
            stonad_fom     path '$.stønadFom',
            stonad_tom     path '$.stønadTom'
        )
      )
    ) j
),

final as (
select
  kafka_offset,
  hjemmel,
  utbetalt_per_mnd,
  stonad_fom,
  stonad_tom,
  fagsak_id as fk_fam_ks_fagsak
from pre_final
)

select * from final
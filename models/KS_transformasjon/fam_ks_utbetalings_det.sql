with kafka_ny_losning as (
  select kafka_offset , melding from {{ source ('fam_ks', 'fam_ks_meta_data') }}
),

pre_final as (
select *  from kafka_ny_losning,
json_table(melding, '$.utbetalingsperioder.utbetalingsDetaljer[*]'
  columns (
    klassekode path '$.klassekode',
    utbetalt_per_mnd path '$.utbetaltPerMnd',
    delytelse_id     path '$.delytelseId'
    )
  ) j
),

final as (
select
kafka_offset, klassekode, utbetalt_per_mnd, delytelse_id
from pre_final
)

select * from final
with kafka_ny_losning as (
  select * from {{ source ('fam_ks', 'fam_ks_meta_data') }}
),

final as (
select
  --dvh_fam_ks.HIBERNATE_SEQUENCE.nextval pk_fam_ks_fagsak,
  k.kafka_offset,
  k.melding.utbetalingsperioder.Hjemmel as Hjemmel,
  k.melding.utbetalingsperioder.utbetaltPerMnd,
  k.melding.utbetalingsperioder.stønadFom Stonad_Fom,
  k.melding.utbetalingsperioder.stønadTom Stonad_Tom
from
  kafka_ny_losning k
)

select * from final
{{ config(
    tags=["ks_kafka_test"]
) }}

with kafka_ny_losning as (
  select * from {{ source ('fam_ks', 'FAM_KS_META_DATA') }}
),


final as (
select
k.kafka_offset,
k.kafka_topic,
k.kafka_partisjon,
k.kafka_hash,
k.melding.fagsakId,
k.melding.behandlingId,
k.melding.relatertBehandlingId,
k.melding.tidspunktVedtak,
k.melding.person.rolle,
k.melding.person.statsborgerskap
from kafka_ny_losning k
)

select * from final
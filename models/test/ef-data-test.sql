{{ config(
  tags = ["ef_kafka_test"]
) }}

with kafka_ny_losning as (
  select * from {{ source ('fam_ef', 'kafka_ny_løsning_test') }}
),




final as (
select
k.kafka_offset,
k.kafka_topic,
k.kafka_partisjon,
k.kafka_hash,
k.kafka_message.fagsakId,
k.kafka_message.behandlingId,
k.kafka_message.relatertBehandlingId,
k.kafka_message.tidspunktVedtak
from kafka_ny_losning k
)



select * from final
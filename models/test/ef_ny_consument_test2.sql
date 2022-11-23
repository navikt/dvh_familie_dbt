{{ config(
    tags=["ef_kafka_test"]
) }}

with kafka_ny_løsning_2 as (
  select * from {{ source ('fam_ef', 'kafka_ny_løsning_test') }}
),


final as (
select
k.kafka_offset,
k.kafka_message.fagsakId,
k.kafka_message.behandlingId,
k.kafka_message.tidspunktVedtak
from kafka_ny_løsning_2 k
)

select * from final

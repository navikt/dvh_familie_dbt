{{ config(
    tags=["ks_kafka_test"]
) }}

with kafka_ny_losning as (
  select * from {{ source ('fam_ks', 'fam_ks_meta_data') }}
),


final as (
select
k.kafka_offset,
k.kafka_topic,
k.kafka_partisjon,
k.kafka_hash,
k.melding.fagsakId as fagsakId,
k.melding.behandlingId as behandlingId,
k.melding.relatertBehandlingId as relatertBehandlingId,
k.melding.tidspunktVedtak as tidspunktVedtak,
k.melding.person.rolle as rolle
from kafka_ny_losning k
)

select * from final
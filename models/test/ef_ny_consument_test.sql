with kafka_ny_løsning as (
  select * from {{ source ('fam_ef', 'kafka_ny_løsning_test') }}
),

final as (
select
k.kafka_offset,
k.kafka_topic,
k.kafka_partisjon,
k.kafka_hash
j.fagsakId,
j.behandlingId,
j.relatertBehandlingId,
j.tidspunktVedtak,
sysdate lastet_dato
from kafka_ny_løsning k,
json_table(to_clob(kafka_message), '$'
  columns(
        fagsakId PATH '$.fagsakId',
        behandlingId PATH '$.behandlingId',
        relatertBehandlingId PATH '$.relatertBehandlingId',
        tidspunktVedtak PATH '$.tidspunktVedtak')
        ) j )

select * from final

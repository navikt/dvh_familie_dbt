{{ config(
  tags = ["ef_kafka_test"]
) }}

WITH kafka_ny_losning AS (

  SELECT
    *
  FROM
    {{ source (
      'fam_ef',
      'kafka_ny_løsning_test'
    ) }}
),
FINAL AS (
  SELECT
    k.kafka_offset,
    k.kafka_topic,
    k.kafka_partisjon,
    k.kafka_hash,
    k.kafka_message.fagsakId,
    k.kafka_message.behandlingId,
    k.kafka_message.relatertBehandlingId,
    k.kafka_message.tidspunktVedtak
  FROM
    kafka_ny_l ø sning k
)
SELECT
  *
FROM
  FINAL

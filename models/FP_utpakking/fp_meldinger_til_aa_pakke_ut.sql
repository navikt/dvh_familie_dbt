{{
    config(
        materialized='table'
    )
}}

with fp_meta_data as (
  select A.pk_fp_meta_data, A.kafka_offset, A.kafka_mottatt_dato, A.kafka_partition, melding,B.KAFKA_OFFSET C from dvh_fam_fp.fam_fp_meta_data A
  LEFT OUTER JOIN dvh_fam_fp.json_fam_fp_fagsak B ON
  A.KAFKA_OFFSET=B.KAFKA_OFFSET AND
  A.KAFKA_PARTITION=B.KAFKA_PARTITION
  where A.kafka_mottatt_dato >= sysdate - 30
AND B.KAFKA_OFFSET IS NULL
)
select * from fp_meta_data

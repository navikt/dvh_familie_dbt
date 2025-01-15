{{
    config(
        materialized='incremental'
    )
}}

with bb_meta_data as (
  select * from {{ref ('bb_meldinger_til_aa_pakke_ut')}}
),

pre_final as (
select * from bb_meta_data,
  json_table(melding, '$'
    COLUMNS (
          VEDTAKS_ID    VARCHAR2 PATH '$.vedtaksid'
          --,BEHANDLINGS_ID NUMBER PATH '$.' Denne mangler!
          ,VEDTAKSTIDSPUNKT VARCHAR2 PATH '$.vedtakstidspunkt'
          ,BEHANDLINGS_TYPE VARCHAR2 PATH '$.type'
          ,FNR_KRAVHAVER VARCHAR2 PATH '$.kravhaver'
          ,FNR_MOTTAKER VARCHAR2 PATH '$.mottaker'
          )
        ) j
)
,

final as (
  select
    VEDTAKS_ID
    ,BEHANDLINGS_TYPE
    ,FNR_KRAVHAVER
    ,FNR_MOTTAKER
    ,CASE
      WHEN LENGTH(VEDTAKSTIDSPUNKT) = 25 THEN CAST(to_timestamp_tz(VEDTAKSTIDSPUNKT, 'yyyy-mm-dd"T"hh24:mi:ss TZH:TZM') AT TIME ZONE 'Europe/Belgrade' AS TIMESTAMP)
      ELSE CAST(to_timestamp_tz(VEDTAKSTIDSPUNKT, 'YYYY-MM-DD"T"HH24:MI:SS.ff') AT TIME ZONE 'Europe/Belgrade' AS TIMESTAMP)
      END VEDTAKSTIDSPUNKT
    ,pk_bb_meta_data as fk_bb_meta_data
    ,kafka_offset
  from pre_final
)

select dvh_fam_bb.DVH_FAMBB_KAFKA.nextval as pk_bb_fagsak
    ,VEDTAKS_ID
    ,-1 AS BEHANDLINGS_ID
    ,VEDTAKSTIDSPUNKT
    ,BEHANDLINGS_TYPE
    ,FNR_KRAVHAVER
    ,FNR_MOTTAKER
    ,-1 AS FK_PERSON1_KRAVHAVER
    ,-1 AS FK_PERSON1_MOTTAKER
    ,fk_bb_meta_data
    ,kafka_offset
    ,localtimestamp as lastet_dato
    ,localtimestamp as OPPDATERT_DATO
from final
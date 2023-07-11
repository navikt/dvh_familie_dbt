{{
    config(
        materialized='incremental'
    )
}}

with barnetrygd_meta_data as (
  select pk_bt_meta_data, kafka_offset, kafka_mottatt_dato, melding from {{ source ('fam_bt', 'fam_bt_meta_data') }}
  where kafka_mottatt_dato between to_timestamp('{{ var("dag_interval_start") }}', 'yyyy-mm-dd hh24:mi:ss')
  and to_timestamp('{{ var("dag_interval_end") }}', 'yyyy-mm-dd hh24:mi:ss')
),

bt_fagsak AS (
  SELECT * FROM {{ ref ('fam_bt_fagsak') }}
),

pre_final as (
select * from barnetrygd_meta_data,
  json_table(melding, '$'
    COLUMNS (
      NESTED             PATH '$.kompetanseperioder[*]'
      COLUMNS (
      tom                            VARCHAR2 PATH '$.tom'
      ,fom                            VARCHAR2 PATH '$.fom'
      ,sokersaktivitet                VARCHAR2 PATH '$.sokersaktivitet'
      ,sokers_aktivitetsland          VARCHAR2 PATH '$.sokersAktivitetsland'
      ,annenforelder_aktivitet        VARCHAR2 PATH '$.annenForeldersAktivitet'
      ,annenforelder_aktivitetsland   VARCHAR2 PATH '$.annenForeldersAktivitetsland'
      ,barnets_bostedsland            VARCHAR2 PATH '$.barnetsBostedsland'
      ,kompetanse_Resultat            VARCHAR2 PATH '$.resultat'
    ))
    )j
  ),

final as (
  select p.*,
  f.pk_BT_FAGSAK as FK_BT_FAGSAK
  from pre_final p
  join bt_fagsak f
  on p.kafka_offset = f.kafka_offset
  where p.fom is not null
)

select
  --ROWNUM as PK_BT_KOMPETANSE_PERIODER,
  dvh_fambt_kafka.hibernate_sequence.nextval as PK_BT_KOMPETANSE_PERIODER,
  FOM,
  TOM,
  SOKERSAKTIVITET,
  ANNENFORELDER_AKTIVITET,
  ANNENFORELDER_AKTIVITETSLAND,
  KOMPETANSE_RESULTAT,
  BARNETS_BOSTEDSLAND,
  localtimestamp as LASTET_DATO,
  FK_BT_FAGSAK,
  SOKERS_AKTIVITETSLAND,
  KAFKA_MOTTATT_DATO,
  KAFKA_OFFSET
from final

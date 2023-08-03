{{
    config(
        materialized='incremental'
    )
}}

with barnetrygd_meta_data as (
  select * from {{ref ('meldinger_til_aa_pakke_ut')}}
  --select pk_bt_meta_data, kafka_offset, kafka_mottatt_dato, melding from {{ source ('fam_bt', 'fam_bt_meta_data') }}
  --where kafka_mottatt_dato >= sysdate-30
),

bt_person AS (
  SELECT * FROM {{ ref ('fam_bt_person') }}
),

statsborgerskap_soker as (
select * from barnetrygd_meta_data,
  json_table(melding, '$'
    COLUMNS (
    person_ident             VARCHAR2 PATH '$.personV2[*].personIdent'
    ,statsborgerskap_soker          VARCHAR2 PATH '$.personV2[*].statsborgerskap[*]'
    )
  ) j
),

statsborgerskap_barn as (
select * from barnetrygd_meta_data,
  json_table(melding, '$'
    COLUMNS (
    NESTED                    PATH '$.utbetalingsperioderV2[*]'
    COLUMNS(
    NESTED                   PATH '$.utbetalingsDetaljer[*]'
    COLUMNS (
    person_ident_barn             VARCHAR2 PATH '$.person.personIdent'
    ,statsborgerskap_barn          VARCHAR2 PATH '$.person.statsborgerskap[*]'
      )
    )
  )
  )j
),

pre_final_soker as (
  select
    person_ident
    ,statsborgerskap_soker as statsborgerskap
    ,nvl(b.fk_person1, -1) fk_person1
    ,kafka_offset
    ,KAFKA_MOTTATT_DATO
  from
    statsborgerskap_soker
  left outer join dt_person.ident_off_id_til_fk_person1 b on
    person_ident=b.off_id
    and b.gyldig_fra_dato<=kafka_mottatt_dato
    and b.gyldig_til_dato>=kafka_mottatt_dato
    and b.skjermet_kode=0
),

pre_final_barn as (
  select
    person_ident_barn
    ,statsborgerskap_barn as statsborgerskap
    ,nvl(b.fk_person1, -1) fk_person1
    ,kafka_offset
    ,KAFKA_MOTTATT_DATO
  from
    statsborgerskap_barn
  left outer join dt_person.ident_off_id_til_fk_person1 b on
    person_ident_barn=b.off_id
    and b.gyldig_fra_dato<=kafka_mottatt_dato
    and b.gyldig_til_dato>=kafka_mottatt_dato
    and b.skjermet_kode=0
),

pre_final as (
  select * from pre_final_soker
  union
  select * from pre_final_barn
),

final as (
  select
  p.statsborgerskap
  ,p.kafka_mottatt_dato
  ,p.kafka_offset
  ,p.fk_person1
  ,per.pk_bt_person as FK_BT_PERSON
  from pre_final p
  join bt_person per
  on p.fk_person1 = per.fk_person1
  and p.kafka_offset = per.kafka_offset
)

select
  --ROWNUM as pk_statsborgerskap
  dvh_fambt_kafka.hibernate_sequence.nextval as pk_statsborgerskap
  ,statsborgerskap
  ,localtimestamp AS lastet_dato
  ,FK_BT_PERSON
  ,kafka_mottatt_dato
from final




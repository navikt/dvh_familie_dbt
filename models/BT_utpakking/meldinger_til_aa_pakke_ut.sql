with barnetrygd_meta_data as (
  select pk_bt_meta_data, kafka_offset, kafka_mottatt_dato, melding from {{ source ('fam_bt', 'fam_bt_meta_data') }}
    where kafka_mottatt_dato >= sysdate - 30 and kafka_offset not in (
      select kafka_offset from {{ source ('fam_bt', 'fam_bt_fagsak') }})
)

select * from barnetrygd_meta_data


{{
    config(
        materialized='incremental'
    )
}}

with kafka_ny_losning as (
  select kafka_offset, kafka_mottatt_dato, melding from {{ source ('fam_ks', 'fam_ks_meta_data') }}
  where kafka_mottatt_dato between to_timestamp('{{ var("dag_interval_start") }}', 'yyyy-mm-dd hh24:mi:ss')
  and to_timestamp('{{ var("dag_interval_end") }}', 'yyyy-mm-dd hh24:mi:ss')
),

pre_final as (
select *  from kafka_ny_losning,
  json_table(melding, '$'
    columns(
        behandlings_id  path  '$.behandlingsId',
          nested path '$.utbetalingsperioder[*]'
          columns(
            hjemmel path '$.hjemmel',
            utbetalt_per_mnd path '$.utbetaltPerMnd',
            stonad_fom     path '$.stønadFom',
            stonad_tom     path '$.stønadTom'
        )
      )
    ) j
),

final as (
select
  behandlings_id || stonad_fom || stonad_tom as pk_ks_utbetaling,
  kafka_offset,
  hjemmel,
  utbetalt_per_mnd,
  to_date(stonad_fom, 'yyyy-mm-dd') stonad_fom,
  to_date(stonad_tom,'yyyy-mm-dd') stonad_tom,
  kafka_mottatt_dato,
  sysdate lastet_dato,
  behandlings_id as fk_ks_fagsak
from pre_final
)

select * from final


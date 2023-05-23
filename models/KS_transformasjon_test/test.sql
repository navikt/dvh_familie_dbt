{{
    config(
        materialized='table',
        unique_key='pk_ks_utbetaling',
        pre_hook='{{generate_sequences()}}'

    )
}}

with kafka_ny_losning as (
  select kafka_offset, kafka_mottatt_dato, melding from {{ source ('fam_ks', 'fam_ks_meta_data') }}
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
  --behandlings_id || stonad_fom || stonad_tom as ppk_ks_utbetaling,
  kafka_offset,
  hjemmel,
  CAST(utbetalt_per_mnd AS NUMBER) utbetalt_per_mnd,
  to_date(stonad_fom, 'yyyy-mm-dd') stonad_fom,
  to_date(stonad_tom,'yyyy-mm-dd') stonad_tom,
  kafka_mottatt_dato,
  CAST(sysdate AS TIMESTAMP) lastet_dato,
  CAST(behandlings_id AS NUMBER) as fk_ks_fagsak
from pre_final
)

select
  /*{{ dbt_utils.generate_surrogate_key(['behandlings_id', 'stonad_fom', 'stonad_tom']) }} as pk_ks_utbetaling,*/
  {{ increment_sequence() }} as pppk_ks_utbetaling,
  kafka_offset,
  hjemmel,
  utbetalt_per_mnd,
  stonad_fom,
  stonad_tom,
  kafka_mottatt_dato,
  lastet_dato,
  fk_ks_fagsak
 from final




{{
    config(
        materialized='incremental',
        unique_key='pk_ks_utbetaling',
        on_schema_change='append_new_columns'
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
  behandlings_id || stonad_fom || stonad_tom as pk_ks_utbetaling,
  kafka_offset,
  hjemmel,
  utbetalt_per_mnd,
  stonad_fom,
  stonad_tom,
  kafka_mottatt_dato,
  sysdate lastet_dato,
  behandlings_id as fk_fam_ks_fagsak
from pre_final
{% if is_incremental() %}

  where kafka_mottatt_dato > (select max(kafka_mottatt_dato) from {{ this }})

{% endif %}
)

select * from final

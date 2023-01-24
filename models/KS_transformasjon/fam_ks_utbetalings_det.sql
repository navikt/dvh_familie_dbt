{{
    config(
        materialized='incremental',
        unique_key='pk_fam_ks_utbet_det',
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
            stonad_fom     path '$.stønadFom',
            stonad_tom     path '$.stønadTom',
            nested path '$.utbetalingsDetaljer[*]'
            columns(
              klassekode path '$.klassekode',
              utbetalt_per_mnd path '$.utbetaltPrMnd',
              delytelse_id     path '$.delytelseId',
              nested path '$.person'
                columns(
                  person_ident path '$.personIdent',
                  rolle path '$.rolle',
                  bosteds_land path '$.bostedsland',
                  delingsprosent_ytelse path '$.delingsprosentYtelse'
                )
            ))
      )
  ) j
),

final as (
select
delytelse_id as pk_fam_ks_utbet_det,
kafka_offset,
klassekode,
utbetalt_per_mnd,
delytelse_id,
person_ident,
rolle,
bosteds_land,
delingsprosent_ytelse,
kafka_mottatt_dato,
sysdate lastet_dato,
behandlings_id || stonad_fom || stonad_tom as fk_ks_utbetaling
from pre_final
{% if is_incremental() %}

  where kafka_mottatt_dato > (select max(kafka_mottatt_dato) from {{ this }})

{% endif %}
)

select * from final


{{
    config(
        materialized='incremental'
    )
}}

with ks_meta_data as (
  select * from {{ref ('ks_meldinger_til_aa_pakke_ut')}}
),

pre_final as (
select *  from ks_meta_data,
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
)

select
  kafka_offset,
  to_date(stonad_fom, 'yyyy-mm-dd') stonad_fom,
  to_date(stonad_tom,'yyyy-mm-dd') stonad_tom,
  kafka_mottatt_dato,
  sysdate lastet_dato,
  utbetalt_per_mnd,
  behandlings_id as fk_ks_fagsak,
  behandlings_id || stonad_fom || stonad_tom as pk_ks_utbetaling,
  hjemmel
from pre_final




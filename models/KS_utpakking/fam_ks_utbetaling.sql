{{
    config(
        materialized='incremental'
    )
}}

with ks_meta_data as (
  select * from {{ref ('ks_meldinger_til_aa_pakke_ut')}}
),

ks_fagsak as (
  select * from {{ref('fam_ks_fagsak')}}
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
),

final as (
select
  to_number(replace(behandlings_id || stonad_fom || stonad_tom, '-', '')) as pk_ks_utbetaling,
  kafka_offset,
  hjemmel,
  utbetalt_per_mnd,
  to_date(stonad_fom, 'yyyy-mm-dd') stonad_fom,
  to_date(stonad_tom,'yyyy-mm-dd') stonad_tom,
  --kafka_mottatt_dato,
  sysdate lastet_dato,
  ks_fagsak.pk_ks_fagsak as fk_ks_fagsak
from pre_final
join ks_fagsak
on to_number(pre_final.behandlings_id) = ks_fagsak.pk_ks_fagsak
)

select * from final


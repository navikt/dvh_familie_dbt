{{
    config(
        materialized='incremental',
        unique_key='pk_ks_utbet_det'
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
          hjemmel        path '$.hjemmel',
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
behandlings_id || stonad_fom || stonad_tom || delytelse_ID as pk_ks_utbet_det,
kafka_offset,
klassekode,
utbetalt_per_mnd,
delytelse_id,
hjemmel,
person_ident,
nvl(b.fk_person1, -1) fk_person1_barn,
rolle,
to_date(stonad_fom, 'yyyy-mm-dd') stonad_fom,
to_date(stonad_tom,'yyyy-mm-dd') stonad_tom,
bosteds_land,
delingsprosent_ytelse,
kafka_mottatt_dato,
sysdate lastet_dato,
behandlings_id as fk_ks_fagsak,
behandlings_id || stonad_fom || stonad_tom as fk_ks_utbetaling
from
  pre_final
left outer join dt_person.ident_off_id_til_fk_person1 b on
  pre_final.person_ident=b.off_id
  and b.gyldig_fra_dato<=pre_final.kafka_mottatt_dato
  and b.gyldig_til_dato>=kafka_mottatt_dato
  and b.skjermet_kode=0
)

select
  pk_ks_utbet_det,
  kafka_offset,
  hjemmel,
  utbetalt_per_mnd,
  kafka_mottatt_dato,
  lastet_dato,
  delytelse_id,
  fk_person1_barn,
  fk_ks_utbetaling,
  fk_ks_fagsak
from final

{% if is_incremental() %}

  where kafka_mottatt_dato > (select max(kafka_mottatt_dato) from {{ this }}) and utbetalt_per_mnd is not null

{% endif %}

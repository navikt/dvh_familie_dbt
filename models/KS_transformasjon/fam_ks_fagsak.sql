/*
{{
    config(
        materialized='incremental',
        unique_key='pk_ks_fagsak'
    )
}}
*/

with kafka_ny_losning as (
  select pk_ks_meta_data, kafka_offset, kafka_mottatt_dato, melding from {{ source ('fam_ks', 'fam_ks_meta_data') }}
),

pre_final as (
select * from kafka_ny_losning,
  json_table(melding, '$'
    columns(
      fagsak_id  path  '$.fagsakId',
      behandlings_id  path '$.behandlingsId',
      tidspunkt_vedtak  path '$.tidspunktVedtak',
      kategori  path '$.kategori',
      behandling_type  path '$.behandlingType',
      funksjonell_id  path '$.funksjonellId',
      behandling_aarsak  path '$.behandlingÅrsak',
        nested path '$.person'
          columns(
            person_ident path '$.personIdent',
            rolle path '$.rolle',
            bosteds_land path '$.bostedsland',
            delingsprosent_ytelse path '$.delingsprosentYtelse'
        )
      )
    ) j
),

final as (
  select
    behandlings_id as pk_ks_fagsak,
    kafka_offset,
    fagsak_id,
    behandlings_id,
    tidspunkt_vedtak,
    kategori,
    behandling_type,
    funksjonell_id,
    behandling_aarsak,
    person_ident,
    nvl(b.fk_person1, -1) fk_person1,
    rolle,
    bosteds_land,
    delingsprosent_ytelse,
    sysdate lastet_dato,
    kafka_mottatt_dato,
    pk_ks_meta_data as fk_ks_meta_data
  from
    pre_final
  left outer join dt_person.ident_off_id_til_fk_person1 b on
    pre_final.person_ident=b.off_id
    and b.gyldig_fra_dato<=pre_final.kafka_mottatt_dato
    and b.gyldig_til_dato>=kafka_mottatt_dato
    and b.skjermet_kode=0
)

select
  pk_ks_fagsak,
  kafka_offset,
  fagsak_id,
  behandlings_id,
  tidspunkt_vedtak,
  kategori,
  behandling_type,
  funksjonell_id,
  behandling_aarsak,
  fk_person1,
  rolle,
  bosteds_land,
  delingsprosent_ytelse,
  lastet_dato,
  kafka_mottatt_dato,
  fk_ks_meta_data
from final

/*
{% if is_incremental() %}

  where kafka_mottatt_dato > (select max(kafka_mottatt_dato) from {{ this }})

{% endif %}
*/



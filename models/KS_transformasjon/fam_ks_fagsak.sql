with kafka_ny_losning as (
  select * from {{ source ('fam_ks', 'fam_ks_meta_data') }}
),

final as (
select
  --dvh_fam_ks.HIBERNATE_SEQUENCE.nextval pk_fam_ks_fagsak,
  k.melding.fagsakId as pk_fam_ks_fagsak,
  k.kafka_offset,
  k.melding.fagsakId as fagsak_id,
  k.melding.behandlingsId as behandlings_id,
  k.melding.tidspunktVedtak as tidspunkt_vedtak,
  k.melding.kategori as kategori,
  k.melding.behandlingType as behandling_type,
  k.melding.funksjonellId as funksjonell_id,
  k.melding.behandlingÅrsak as behandling_aarsak,
  k.melding.person.personIdent as person_ident,
  k.melding.person.rolle,
  k.pk_ks_meta_data as fk_ks_meta_data
from
  kafka_ny_losning k
)

select * from final
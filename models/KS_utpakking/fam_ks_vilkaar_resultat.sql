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
  select * from
  (
    select *  from ks_meta_data,
      json_table(melding, '$'
        columns(
          behandlings_id  path  '$.behandlingsId',
          nested          path '$.vilkårResultater[*]'
          columns(
            resultat              path '$.resultat',
            antall_timer          path '$.antallTimer',
            periode_fom           path '$.periodeFom',
            periode_tom           path '$.periodeTom',
            ident                 path '$.ident',
            vilkaar_type          path '$.vilkårType'
            )
          )
        ) j
  )
  where vilkaar_type is not null
  --where json_value (melding, '$.vilkårResultater.size()' )> 0
  --where json_exists(melding, '$.vilkårResultater.vilkårType')
),

final as (
  Select
  pre_final.behandlings_id as fk_ks_fagsak,
  resultat,
  ident,
  antall_timer,
  to_date(periode_fom, 'yyyy-mm-dd') periode_fom,
  to_date(periode_tom, 'yyyy-mm-dd') periode_tom,
  nvl(b.fk_person1, -1) fk_person1,
  vilkaar_type
from
  pre_final
left outer join dt_person.ident_off_id_til_fk_person1 b on
  pre_final.ident=b.off_id
  and b.gyldig_fra_dato<=pre_final.kafka_mottatt_dato
  and b.gyldig_til_dato>=kafka_mottatt_dato
  and b.skjermet_kode=0
)

SELECT
  dvh_fam_ks.hibernate_sequence.nextval as PK_KS_VILKAAR_RESULTAT,
  RESULTAT,
  ANTALL_TIMER,
  PERIODE_FOM,
  PERIODE_TOM,
  case when FK_PERSON1 = -1 then IDENT
      else cast(null as varchar2(11))
  end IDENT,
  FK_PERSON1,
  VILKAAR_TYPE,
  localtimestamp AS LASTET_DATO,
  fk_ks_fagsak
from final


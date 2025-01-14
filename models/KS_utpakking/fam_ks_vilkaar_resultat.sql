{{
    config(
        materialized='incremental'
    )
}}

with ks_meta_data as (
  select * from {{ref ('ks_meldinger_til_aa_pakke_ut')}}
),

pre_final as (
  select * from
  (
    select *  from ks_meta_data,
      json_table(melding, '$'
        columns(
          behandlings_id  number(38,0) path  '$.behandlingsId',
          nested          path '$.vilkårResultater[*]'
          columns(
            resultat              varchar2(255) path '$.resultat',
            antall_timer          varchar2(255) path '$.antallTimer',
            periode_fom           varchar2(255) path '$.periodeFom',
            periode_tom           varchar2(255) path '$.periodeTom',
            ident                 varchar2(255) path '$.ident',
            vilkaar_type          varchar2(255) path '$.vilkårType'
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
  to_number(pre_final.behandlings_id) as fk_ks_fagsak,
  resultat,
  ident,
  replace(antall_timer, '.', ',') antall_timer,
  replace(antall_timer, '.', ',') antall_timer2,
  replace(antall_timer, '.', ',') antall_timer3,
  --case when antall_timer is not null
  --  then to_number(antall_timer)
  --else antall_timer
  --end antall_timer,
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
  0 as ANTALL_TIMER,
  PERIODE_FOM,
  PERIODE_TOM,
  case when FK_PERSON1 = -1 then IDENT
      else cast(null as varchar2(11))
  end IDENT,
  FK_PERSON1,
  VILKAAR_TYPE,
  localtimestamp AS LASTET_DATO,
  fk_ks_fagsak,
  antall_timer2,
  antall_timer3
from final
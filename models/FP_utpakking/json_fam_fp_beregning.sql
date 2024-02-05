{{
    config(
        materialized='incremental'
    )
}}

with fp_meta_data as (
  select * from {{ref ('fp_meldinger_til_aa_pakke_ut')}}
),

fp_fagsak as (
  select kafka_offset, saksnummer, fagsak_id, behandling_uuid, pk_fp_fagsak from {{ ref('json_fam_fp_fagsak') }}
),

pre_final as (
  select fp_meta_data.kafka_offset, j.*
  from fp_meta_data
      ,json_table(melding, '$' COLUMNS (
          saksnummer      VARCHAR2(255) PATH '$.saksnummer'
         ,fagsak_id       VARCHAR2(255) PATH '$.fagsakId'
         ,behandling_uuid VARCHAR2(255) PATH '$.behandlingUuid'
         ,nested PATH '$.beregning' COLUMNS (
            grunnbelop         VARCHAR2(255) PATH '$.grunnbeløp'
           ,aarsbelop_brutto   VARCHAR2(255) PATH '$.årsbeløp.brutto'
           ,aarsbelop_avkortet VARCHAR2(255) PATH '$.årsbeløp.avkortet'
           ,aarsbelop_redusert VARCHAR2(255) PATH '$.årsbeløp.redusert'
           ,aarsbelop_dagsats  VARCHAR2(255) PATH '$.årsbeløp.dagsats'
           ,nested PATH '$.andeler[*]' COLUMNS (
              andeler_aktivitet          VARCHAR2(255) PATH '$.aktivitet'
             ,andeler_arbeidsgiver       VARCHAR2(255) PATH '$.arbeidsgiver'
             ,andeler_aarsbelop_brutto   VARCHAR2(255) PATH '$.årsbeløp.brutto'
             ,andeler_aarsbelop_avkortet VARCHAR2(255) PATH '$.årsbeløp.avkortet'
             ,andeler_aarsbelop_redusert VARCHAR2(255) PATH '$.årsbeløp.redusert'
             ,andeler_aarsbelop_dagsats  VARCHAR2(255) PATH '$.årsbeløp.dagsats'
           ) ) ) ) j
  where j.grunnbelop is not null
),

final as (
  select
    to_number(p.grunnbelop) grunnbelop
   ,to_number(p.aarsbelop_brutto) aarsbelop_brutto
   ,to_number(p.aarsbelop_avkortet) aarsbelop_avkortet
   ,to_number(p.aarsbelop_redusert) aarsbelop_redusert
   ,to_number(p.aarsbelop_dagsats) aarsbelop_dagsats
   ,p.andeler_aktivitet
   ,p.andeler_arbeidsgiver
   ,to_number(p.andeler_aarsbelop_brutto) andeler_aarsbelop_brutto
   ,to_number(p.andeler_aarsbelop_avkortet) andeler_aarsbelop_avkortet
   ,to_number(p.andeler_aarsbelop_redusert) andeler_aarsbelop_redusert
   ,to_number(p.andeler_aarsbelop_dagsats) andeler_aarsbelop_dagsats
   ,fp_fagsak.pk_fp_fagsak as fk_fp_fagsak
   ,p.kafka_offset
  from pre_final p
  join fp_fagsak
  on fp_fagsak.kafka_offset = p.kafka_offset
  and fp_fagsak.saksnummer = p.saksnummer
  and fp_fagsak.fagsak_id = p.fagsak_id
  and fp_fagsak.behandling_uuid = p.behandling_uuid
)

select
     dvh_fam_fp.fam_fp_seq.nextval as pk_fp_beregning
    ,grunnbelop
    ,aarsbelop_brutto
    ,aarsbelop_avkortet
    ,aarsbelop_redusert
    ,aarsbelop_dagsats
    ,andeler_aktivitet
    ,andeler_arbeidsgiver
    ,andeler_aarsbelop_brutto
    ,andeler_aarsbelop_avkortet
    ,andeler_aarsbelop_redusert
    ,andeler_aarsbelop_dagsats
    ,fk_fp_fagsak
    ,kafka_offset
    ,localtimestamp as lastet_dato
from final

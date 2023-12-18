{{
    config(
        materialized='incremental'
    )
}}

with fp_meta_data as (
  select * from {{ref ('fp_meldinger_til_aa_pakke_ut')}}
),

fp_fagsak as (
  select saksnummer, fagsak_id, behandling_uuid from {{ ref('json_fam_fp_fagsak') }}
),

pre_final as (
  select *
  from fp_meta_data
      ,json_table(melding, '$'
        COLUMNS (
          saksnummer                 VARCHAR2 PATH '$.saksnummer'
         ,fagsak_id                  VARCHAR2 PATH '$.fagsakId'
         ,behandling_uuid            VARCHAR2 PATH '$.behandlingUuid'
         ,nested PATH '$.uttaksperioder[*]'
          COLUMNS (
            seq_i_perioder FOR ORDINALITY
           ,fom                      VARCHAR2 PATH '$.fom'
           ,tom                      VARCHAR2 PATH '$.tom'
           ,type                     VARCHAR2 PATH '$.type'
           ,stonadskonto_type        VARCHAR2 PATH '$.stønadskontoType'
           ,rettighet_type           VARCHAR2 PATH '$.rettighetType'
           ,forklaring               VARCHAR2 PATH '$.forklaring'
           ,er_utbetaling            VARCHAR2 PATH '$.erUtbetaling'
           ,virkedager               VARCHAR2 PATH '$.virkedager'
           ,trekkdager               VARCHAR2 PATH '$.trekkdager'
           ,gradering_aktivitet_type VARCHAR2 PATH '$.gradering.aktivitetType'
           ,gradering_arbeidsprosent VARCHAR2 PATH '$.gradering.arbeidsprosent'
           ,samtidig_uttak_prosent   VARCHAR2 PATH '$.samtidigUttakProsent'
          ) ) ) j
  join fp_fagsak
  on fp_fagsak.saksnummer = j.saksnummer
  and fp_fagsak.fagsak_id = j.fagsak_id
  and fp_fagsak.behandling_uuid = j.behandling_uuid
  where j.fom is not null
),

final as (
  select
    seq_i_perioder
   ,to_date(p.fom, 'yyyy-mm-dd') as fom
   ,to_date(p.tom, 'yyyy-mm-dd') as tom
   ,p.type
   ,p.stonadskonto_type
   ,p.rettighet_type
   ,p.forklaring
   ,p.er_utbetaling
   ,p.virkedager
   ,p.trekkdager
   ,p.gradering_aktivitet_type
   ,p.gradering_arbeidsprosent
   ,p.samtidig_uttak_prosent
   ,p.KAFKA_OFFSET
  from pre_final p
)

select
     dvh_fam_fp.fam_fp_seq.nextval as pk_fp_uttaksperioder
    ,seq_i_perioder
    ,fom
    ,tom
    ,type
    ,stonadskonto_type
    ,rettighet_type
    ,forklaring
    ,er_utbetaling
    ,virkedager
    ,trekkdager
    ,gradering_aktivitet_type
    ,gradering_arbeidsprosent
    ,samtidig_uttak_prosent
    ,KAFKA_OFFSET
    ,localtimestamp as LASTET_DATO
from final
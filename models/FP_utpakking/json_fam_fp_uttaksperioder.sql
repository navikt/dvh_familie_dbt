{{
    config(
        materialized='incremental'
    )
}}

with fp_meta_data as (
  select * from {{ref ('fp_meldinger_til_aa_pakke_ut')}}
),

fp_fagsak as (
  select saksnummer, fagsak_id, behandling_uuid, pk_fp_fagsak from {{ ref('json_fam_fp_fagsak') }}
),

pre_final as (
  select fp_meta_data.kafka_offset, j.*
        ,fp_fagsak.pk_fp_fagsak
  from fp_meta_data
      ,json_table(melding, '$' COLUMNS (
          saksnummer                 VARCHAR2 PATH '$.saksnummer'
         ,fagsak_id                  VARCHAR2 PATH '$.fagsakId'
         ,behandling_uuid            VARCHAR2 PATH '$.behandlingUuid'
         ,nested PATH '$.uttaksperioder[*]' COLUMNS (
            seq_i_array           FOR ORDINALITY
           ,fom                      VARCHAR2 PATH '$.fom'
           ,tom                      VARCHAR2 PATH '$.tom'
           ,type                     VARCHAR2 PATH '$.type'
           ,stonadskonto_type        VARCHAR2 PATH '$.stønadskontoType'
           ,rettighet_type           VARCHAR2 PATH '$.rettighetType'
           ,forklaring               VARCHAR2 PATH '$.forklaring'
           ,soknadsdato              VARCHAR2 PATH '$.søknadsDato'
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
    p.seq_i_array
   ,to_date(p.fom, 'yyyy-mm-dd') as fom
   ,to_date(p.tom, 'yyyy-mm-dd') as tom
   ,p.type
   ,p.stonadskonto_type
   ,p.rettighet_type
   ,p.forklaring
   ,to_date(p.soknadsdato, 'yyyy-mm-dd') as soknadsdato
   ,p.er_utbetaling
   ,p.virkedager
   ,p.trekkdager
   ,p.gradering_aktivitet_type
   ,p.gradering_arbeidsprosent
   ,p.samtidig_uttak_prosent
   ,p.pk_fp_fagsak as fk_fp_fagsak
   ,p.kafka_offset
  from pre_final p
)

select
     dvh_fam_fp.fam_fp_seq.nextval as pk_fp_uttaksperioder
    ,seq_i_array
    ,fom
    ,tom
    ,type
    ,stonadskonto_type
    ,rettighet_type
    ,forklaring
    ,soknadsdato
    ,er_utbetaling
    ,to_number(replace(virkedager, '.', ',')) virkedager
    ,to_number(replace(trekkdager, '.', ',')) trekkdager
    ,gradering_aktivitet_type
    ,to_number(replace(gradering_arbeidsprosent, '.', ',')) gradering_arbeidsprosent
    ,to_number(replace(samtidig_uttak_prosent, '.', ',')) samtidig_uttak_prosent
    ,fk_fp_fagsak
    ,kafka_offset
    ,localtimestamp as lastet_dato
from final

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
          saksnummer                       VARCHAR2 PATH '$.saksnummer'
         ,fagsak_id                        VARCHAR2 PATH '$.fagsakId'
         ,behandling_uuid                  VARCHAR2 PATH '$.behandlingUuid'
         ,nested PATH '$.utbetalingssperioder[*]' COLUMNS (
            seq_i_array                 FOR ORDINALITY
           ,fom                            VARCHAR2 PATH '$.fom'
           ,tom                            VARCHAR2 PATH '$.tom'
           ,klasse_kode                    VARCHAR2 PATH '$.klasseKode'
           ,arbeidsgiver                   VARCHAR2 PATH '$.arbeidsgiver'
           ,dagsats                        VARCHAR2 PATH '$.dagsats'
           ,dagsats_fra_beregningsgrunnlag VARCHAR2 PATH '$.dagsatsFraBeregningsgrunnlag'
           ,utbetalingsgrad                VARCHAR2 PATH '$.utbetalingsgrad'
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
   ,p.klasse_kode
   ,p.arbeidsgiver
   ,p.dagsats
   ,p.dagsats_fra_beregningsgrunnlag
   ,p.utbetalingsgrad
   ,p.pk_fp_fagsak as fk_fp_fagsak
   ,p.kafka_offset
  from pre_final p
)

select
     dvh_fam_fp.fam_fp_seq.nextval as pk_fp_utbetalingssperioder
    ,seq_i_array
    ,fom
    ,tom
    ,klasse_kode
    ,arbeidsgiver
    ,dagsats
    ,dagsats_fra_beregningsgrunnlag
    ,utbetalingsgrad
    ,fk_fp_fagsak
    ,kafka_offset
    ,localtimestamp as lastet_dato
from final
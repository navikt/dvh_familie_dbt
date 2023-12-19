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
          saksnummer      VARCHAR2 PATH '$.saksnummer'
         ,fagsak_id       VARCHAR2 PATH '$.fagsakId'
         ,behandling_uuid VARCHAR2 PATH '$.behandlingUuid'
         ,nested PATH '$.familieHendelse.barn[*]' COLUMNS (
            seq_i_array    FOR ORDINALITY
           ,barn_aktor_id  VARCHAR2 PATH '$.aktørId'
           ,fodselsdato    VARCHAR2 PATH '$.fødselsdato'
           ,dodsdato       VARCHAR2 PATH '$.dødsdato'
          ) ) ) j
  join fp_fagsak
  on fp_fagsak.saksnummer = j.saksnummer
  and fp_fagsak.fagsak_id = j.fagsak_id
  and fp_fagsak.behandling_uuid = j.behandling_uuid
  where j.barn_aktor_id is not null
),

final as (
  select
    p.seq_i_array
   ,p.barn_aktor_id
   ,to_date(p.fodselsdato, 'yyyy-mm-dd') as fodselsdato
   ,to_date(p.dodsdato, 'yyyy-mm-dd') as dodsdato
   ,p.pk_fp_fagsak as fk_fp_fagsak
   ,p.kafka_offset
  from pre_final p
)

select
     dvh_fam_fp.fam_fp_seq.nextval as pk_fp_familie_hendelse
    ,seq_i_array
    ,barn_aktor_id
    ,fodselsdato
    ,dodsdato
    ,fk_fp_fagsak
    ,kafka_offset
    ,localtimestamp as lastet_dato
from final
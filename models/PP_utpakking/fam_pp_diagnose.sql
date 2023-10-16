{{
    config(
        materialized='incremental'
    )
}}

with pp_meta_data as (
  select * from {{ref ('pp_meldinger_til_aa_pakke_ut')}}
),

pp_fagsak AS (
  SELECT * FROM {{ ref ('fam_pp_fagsak') }}
),

pre_final as (
select * from pp_meta_data,
  json_table(melding, '$'
    columns (
      vedtaks_tidspunkt  varchar2 path '$.vedtakstidspunkt'
      ,nested             path '$.diagnosekoder[*]' columns (
      kode               varchar2 path '$.kode'
      ,type              varchar2 path '$.type'
      )
    )
  ) j
),

pre_final_dim_diagnose as (
  select
    p.*
    ,d.pk_dim_diagnose as fk_dim_diagnose
  from pre_final p
  join dt_p.dim_diagnose d
  on p.kode = d.diagnose_kode
  and p.type = upper(d.diagnose_tabell)
  and p.vedtaks_tidspunkt between d.gyldig_fra_dato and d.gyldig_til_dato
),

final as (
  select
    p.kode
    ,p.type
    ,p.fk_dim_diagnose
    ,f.pk_pp_fagsak as FK_PP_FAGSAK
  from pre_final_dim_diagnose p
  join pp_fagsak f
  on p.kafka_offset = f.kafka_offset
)

select
  dvh_fampp_kafka.hibernate_sequence.nextval as PK_PP_DIAGNOSE
  ,KODE
  ,TYPE
  ,FK_PP_FAGSAK
  ,localtimestamp AS LASTET_DATO
  ,FK_DIM_DIAGNOSE
from final
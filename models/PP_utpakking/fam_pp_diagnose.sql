{{
    config(
        materialized='incremental'
    )
}}

with pp_meta_data as (
  select * from {{ref ('pp_meldinger_til_aa_pakke_ut')}}
),

pp_fagsak as (
  select * from {{ ref ('fam_pp_fagsak') }}
),

pre_final as (
select * from pp_meta_data,
  json_table(melding, '$'
    columns (
      vedtaks_tidspunkt  varchar2 path '$.vedtakstidspunkt'
     ,nested path '$.diagnosekoder[*]' columns (
      kode varchar2 path '$.kode'
     ,type varchar2 path '$.type'
      )
    )
  ) j
  where kode is not null
),

pre_final2 as (
  select p.kode
        ,p.type
        --,p.vedtaks_tidspunkt
        ,f.pk_pp_fagsak as fk_pp_fagsak
        ,f.forrige_behandlings_id, f.behandlings_id, f.saksnummer
        ,f.vedtaks_tidspunkt
  from pre_final p
  join pp_fagsak f
  on p.kafka_offset = f.kafka_offset
),

ny_diagnose as
(
  select ny.saksnummer, ny.behandlings_id, ny.vedtaks_tidspunkt, ny.kode, ny.type, ny.fk_pp_fagsak
  from pre_final2 ny
  left outer join
  (
    select fagsak.saksnummer, fagsak.behandlings_id, fagsak.forrige_behandlings_id, fagsak.vedtaks_tidspunkt
          ,diagnose.kode, diagnose.type
    from {{ source('fam_pp', 'fam_pp_diagnose') }} diagnose
    join {{ source('fam_pp', 'fam_pp_fagsak') }} fagsak
    on diagnose.fk_pp_fagsak = fagsak.pk_pp_fagsak

    union all
    select saksnummer, behandlings_id, forrige_behandlings_id, vedtaks_tidspunkt
          ,kode, type
    from pre_final2
  ) gml
  on gml.saksnummer = ny.saksnummer
  and gml.vedtaks_tidspunkt < ny.vedtaks_tidspunkt
  and ny.kode = gml.kode
  and ny.type = gml.type
  where ny.forrige_behandlings_id is not null
  and gml.kode is null
),

final as
(
  select ny.*
        ,case when ny.forrige_behandlings_id is not null and ny_diagnose.kode is not null then 'J'
              when ny.forrige_behandlings_id is null then 'J'
              else 'N'
         end siste_diagnose_flagg
  from pre_final2 ny
  left join ny_diagnose
  on ny.fk_pp_fagsak = ny_diagnose.fk_pp_fagsak
  and ny.kode = ny_diagnose.kode
  and ny.type = ny_diagnose.type
)

select
  dvh_fampp_kafka.hibernate_sequence.nextval as pk_pp_diagnose
 ,kode
 ,type
 ,fk_pp_fagsak
 ,localtimestamp as lastet_dato
 ,cast(null as varchar2(4)) as fk_dim_diagnose
 ,siste_diagnose_flagg
from final
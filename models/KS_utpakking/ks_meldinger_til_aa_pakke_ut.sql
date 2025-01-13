{{
    config(
        materialized='incremental'
    )
}}

with kontantstotte_meta_data as (
  select pk_ks_meta_data, kafka_offset, kafka_mottatt_dato, kafka_topic, kafka_partisjon, melding
  from {{ source ('fam_ks', 'fam_ks_meta_data') }}
  where pk_ks_meta_data in
  (
    select fk_ks_meta_data
    from {{ source ('fam_ks', 'fam_ks_fagsak') }}
    where pk_ks_fagsak not in
    (
      select fk_ks_fagsak
      from {{ source ('fam_ks', 'fam_ks_vilkaar_resultat') }}
    )
  )

)

select * from kontantstotte_meta_data
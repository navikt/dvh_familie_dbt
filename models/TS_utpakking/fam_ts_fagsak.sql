{{
    config(
        materialized='incremental'
    )
}}

with ts_meta_data as (
  select pk_ts_meta_data, melding  from {{ref ('ts_meldinger_til_aa_pakke_ut')}}
),

pre_final as (
select * from ts_meta_data,
  json_table(melding, '$'
    COLUMNS (
      id                                VARCHAR2 PATH '$.id',
      fagsak_id                         VARCHAR2 PATH '$.fagsak_Id',
      behandling_id                     VARCHAR2 PATH '$.behandling_id',
      ekstern_fagsak_id                 VARCHAR2 PATH '$.ekstern_fagsak_id',
      ekstern_behandling_id             NUMBER PATH '$.ekstern_behandling_id',
      relatert_behandling_id            VARCHAR2 PATH '$.relatert_behandling_id',
      adressebeskyttelse                VARCHAR2 PATH '$.adressebeskyttelse',
      tidspunkt_vedtak                  TIMESTAMP PATH '$.tidspunkt_vedtak',
      person                            VARCHAR2 PATH '$.person',
      behandling_type                   VARCHAR2 PATH '$.behandling_type',
      behandling_arsak                  VARCHAR2 PATH '$.behandling_arsak',
      vedtak_resultat                   VARCHAR2 PATH '$.vedtak_resultat',
      stonadstype                       VARCHAR2 PATH '$.stonadstype',
      krav_mottatt                      DATE PATH '$.krav_mottatt'
    )
  ) j
),
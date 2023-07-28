{{
    config(
        materialized='incremental'
    )
}}
with ef_meta_data as (
  select pk_bt_meta_data, kafka_offset, kafka_mottatt_dato, melding from {{ source ('fam_ef', 'fam_ef_meta_data') }}
  where kafka_mottatt_dato between to_timestamp('{{ var("dag_interval_start") }}', 'yyyy-mm-dd hh24:mi:ss')
  and to_timestamp('{{ var("dag_interval_end") }}', 'yyyy-mm-dd hh24:mi:ss')
),

bt_person AS (
  SELECT * FROM {{ ref ('fam_bt_person') }}
),

pre_final as (
select * from ef_meta_data,
  json_table(melding, '$'
    COLUMNS (
          -- behandling_opprinnelse    VARCHAR2 PATH '$.behandlingOpprinnelse'
         --,behandling_type           VARCHAR2 PATH '$.behandlingTypeV2'
         --,
          fagsak_id                       VARCHAR2 PATH '$.fagsakId'
         ,behandlings_id                  VARCHAR2 PATH '$.behandlingsId'
         ,relatert_behandlings_id         VARCHAR2 PATH '$.relatertBehandlingId'
         ,adressebeskyttelse              VARCHAR2 PATH '$.adressebeskyttelse'
         ,behandling_type                 VARCHAR2 PATH '$.behandlingType'
         ,behandling_aarsak               VARCHAR2 PATH '$.behandlingÅrsak'
         ,vedtaks_status                  VARCHAR2 PATH '$.vedtaksStatus'
         ,stonadstype                     VARCHAR2 PATH '$.stønadstype'
         ,person_ident                    VARCHAR2 PATH '$.personIdent'
         ,aktivitetsplikt_inntreffer_dato VARCHAR2 PATH '$.aktivitetspliktInntrefferDato'
         ,har_sagt_opp_arbeidsforhold     VARCHAR2 PATH '$.harSagtOppArbeidsforhold'
         ,funksjonell_id                  VARCHAR2 PATH '$.funksjonellId'
         ,vedtaks_tidspunkt               VARCHAR2 PATH '$.vedtaksTidspunkt'
         ,vedtaksbegrunnelse_skole        VARCHAR2 PATH '$.vedtaksbegrunnelse'
         )
        ) j
),

final as (
  select
    p.behandling_opprinnelse
    ,p.behandling_type
    ,p.fagsak_id
    ,p.behandlings_id
    ,p.fagsak_type
    ,CASE
      WHEN LENGTH(tidspunkt_vedtak) = 25 THEN CAST(to_timestamp_tz(tidspunkt_vedtak, 'yyyy-mm-dd"T"hh24:mi:ss TZH:TZM') AT TIME ZONE 'Europe/Belgrade' AS TIMESTAMP)
      ELSE CAST(to_timestamp_tz(tidspunkt_vedtak, 'FXYYYY-MM-DD"T"HH24:MI:SS.FXFF3TZH:TZM') AT TIME ZONE 'Europe/Belgrade' AS TIMESTAMP)
      END tidspunkt_vedtak
    ,CASE
      WHEN p.enslig_forsørger = 'false' THEN '0'
      ELSE '1'
      END AS enslig_forsørger
    ,p.kategori
    ,p.underkategori
    ,'BT' AS kildesystem
    ,sysdate AS lastet_dato
    ,p.funksjonell_id
    ,p.behandling_Årsak
    ,p.person_ident
    ,p.kafka_offset
    ,p.kafka_mottatt_dato
    ,pk_bt_meta_data as FK_BT_META_DATA
    ,per.pk_bt_person as FK_BT_PERSON
  from pre_final p
  join bt_person per
  on p.kafka_offset = per.kafka_offset
  where per.soker_flagg = 1
)

select
    dvh_fambt_kafka.hibernate_sequence.nextval as PK_BT_FAGSAK
    ,FK_BT_PERSON
    ,FK_BT_META_DATA
    ,BEHANDLING_OPPRINNELSE
    ,BEHANDLING_TYPE
    ,FAGSAK_ID
    ,FUNKSJONELL_ID
    ,BEHANDLINGS_ID
    ,TIDSPUNKT_VEDTAK
    ,ENSLIG_FORSØRGER
    ,KATEGORI
    ,UNDERKATEGORI
    ,KAFKA_OFFSET
    ,KILDESYSTEM
    ,LASTET_DATO
    ,BEHANDLING_ÅRSAK
    ,FAGSAK_TYPE
    ,kafka_mottatt_dato
from final

PK_EF_FAGSAK
FK_EF_META_DATA
FAGSAK_ID
BEHANDLINGS_ID
RELATERT_BEHANDLINGS_ID
ADRESSEBESKYTTELSE
FK_PERSON1
BEHANDLING_TYPE
BEHANDLINGS_AARSAK
VEDTAKS_STATUS
STONADSTYPE
PERSON_IDENT
AKTIVITETSPLIKT_INNTREFFER_DATO
HAR_SAGT_OPP_ARBEIDSFORHOLD
FUNKSJONELL_ID
VEDTAKS_TIDSPUNKT
KAFKA_TOPIC
KAFKA_OFFSET
KAFKA_PARTITION
LASTET_DATO
FAGSAK_ID_GML
BEHANDLINGS_ID_GML
AKTIVITETSVILKAAR_BARNETILSYN
VEDTAKSBEGRUNNELSE_SKOLE
KRAV_MOTTATT
ÅRSAK_REVURDERINGS_KILDE
REVURDERINGS_ÅRSAK
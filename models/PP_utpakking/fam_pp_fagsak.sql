{{
    config(
        materialized='incremental'
    )
}}

with pp_meta_data as (
  select * from {{ref ('pp_meldinger_til_aa_pakke_ut')}}
),

pre_final as (
select * from pp_meta_data,
  json_table(melding, '$'
    COLUMNS (
      behandlings_id          varchar2 path '$.behandlingUuid'
      ,pleietrengende         varchar2 path '$.pleietrengende'
      ,saksnummer             varchar2 path '$.saksnummer'
      ,soker                  varchar2 path '$.søker'
      ,utbetalingsreferanse   varchar2 path '$.utbetalingsreferanse'
      ,ytelse_type            varchar2 path '$.ytelseType'
      ,vedtaks_tidspunkt      varchar2 path '$.vedtakstidspunkt'
      ,forrige_behandlings_id varchar2 path '$.forrigeBehandlingUuid'
    )
  ) j
),

mottaker_final as (
  select
    p.behandlings_id
    ,p.pleietrengende
    ,p.saksnummer
    ,p.soker
    ,p.utbetalingsreferanse
    ,p.ytelse_type
    ,p.kafka_offset
    ,p.kafka_topic
    ,p.kafka_mottatt_dato
    ,nvl(ident.fk_person1, -1) as FK_PERSON1_MOTTAKER
    ,p.kafka_partition
    ,p.pk_pp_meta_data as fk_pp_metadata
    ,CASE
    WHEN LENGTH(p.vedtaks_tidspunkt) = 25 THEN CAST(to_timestamp_tz(p.vedtaks_tidspunkt, 'yyyy-mm-dd"T"hh24:mi:ss TZH:TZM') AT TIME ZONE 'Europe/Belgrade' AS TIMESTAMP)
    WHEN '.' NOT IN p.vedtaks_tidspunkt THEN CAST(to_timestamp_tz(p.vedtaks_tidspunkt, 'yyyy-mm-dd"T"hh24:mi:ss TZH:TZM') AT TIME ZONE 'Europe/Belgrade' AS TIMESTAMP)
    ELSE CAST(to_timestamp_tz(p.vedtaks_tidspunkt, 'FXYYYY-MM-DD"T"HH24:MI:SS.FXFFTZH:TZM') AT TIME ZONE 'Europe/Belgrade' AS TIMESTAMP)
    END vedtaks_tidspunkt
    ,p.forrige_behandlings_id
  from pre_final p
  left outer join dt_person.ident_off_id_til_fk_person1 ident
  on p.soker = ident.off_id
  and p.kafka_mottatt_dato between ident.gyldig_fra_dato and ident.gyldig_til_dato
  and ident.skjermet_kode = 0
),

pleietrengende_final as(
  select m.*
    ,nvl(ident.fk_person1, -1) as FK_PERSON1_PLEIETRENGENDE
  from mottaker_final m
  left outer join dt_person.ident_off_id_til_fk_person1 ident
  on m.pleietrengende = ident.off_id
  and m.kafka_mottatt_dato between ident.gyldig_fra_dato and ident.gyldig_til_dato
  and ident.skjermet_kode = 0
)

select
  dvh_fampp_kafka.hibernate_sequence.nextval as PK_pp_FAGSAK
  ,behandlings_id
  ,FORRIGE_BEHANDLINGS_ID
  ,FK_PERSON1_MOTTAKER
  ,FK_PERSON1_PLEIETRENGENDE
  ,KAFKA_OFFSET
  ,KAFKA_PARTITION
  ,KAFKA_TOPIC
  ,localtimestamp as lastet_dato
  ,case when FK_PERSON1_PLEIETRENGENDE = -1 then pleietrengende
    else cast(null as varchar2(11))
  end pleietrengende
  ,saksnummer
  ,case when FK_PERSON1_MOTTAKER = -1 then soker
    else cast(null as varchar2(11))
  end soker
  ,utbetalingsreferanse
  ,ytelse_type
  ,VEDTAKS_TIDSPUNKT
  ,fk_pp_metadata
from pleietrengende_final








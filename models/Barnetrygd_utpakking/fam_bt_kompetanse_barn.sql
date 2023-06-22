{{
    config(
        materialized='incremental'
    )
}}

with barnetrygd_meta_data as (
  select pk_bt_meta_data, kafka_offset, kafka_mottatt_dato, melding from {{ source ('fam_bt', 'fam_bt_meta_data') }}
  where kafka_mottatt_dato between '{{ var ("dag_interval_start") }}' and '{{ var ("dag_interval_end") }}'

  --{% if is_incremental() %}

  --where kafka_mottatt_dato > (select max(kafka_mottatt_dato) from {{ this }})

  --{% endif %}

),

bt_komp_barn AS (
  SELECT * FROM {{ ref ('fam_bt_kompetanse_perioder') }}
),

bt_fagsak AS (
  SELECT * FROM {{ ref ('fam_bt_fagsak') }}
),

pre_final as (
select * from barnetrygd_meta_data,
  json_table(melding, '$'
    COLUMNS (
        nested path '$.kompetanseperioder[*]'
    COLUMNS (
         tom                            VARCHAR2 PATH '$.tom'
        ,fom                            VARCHAR2 PATH '$.fom'
        ,kompetanse_Resultat            VARCHAR2 PATH '$.resultat'
        ,nested path '$.barnsIdenter[*]'
         columns (
         personidentbarn   varchar2 path '$[*]'
         )
         )
        )
    ) j
),

joining_pre_final as (
  select
    personidentbarn,
    nvl(b.fk_person1, -1) fk_person1,
    tom,
    fom,
    kompetanse_Resultat,
    kafka_offset,
    kafka_mottatt_dato
  from
    pre_final
  left outer join dt_person.ident_off_id_til_fk_person1 b on
    pre_final.personidentbarn=b.off_id
    and b.gyldig_fra_dato<=pre_final.kafka_mottatt_dato
    and b.gyldig_til_dato>=kafka_mottatt_dato
    and b.skjermet_kode=0
),

final as (
  select
    j.fk_person1,
    j.fom,
    j.tom,
    j.kafka_offset,
    j.kompetanse_Resultat,
    j.kafka_mottatt_dato,
    k.pK_BT_KOMPETANSE_PERIODER as fK_BT_KOMPETANSE_PERIODER--,
  from joining_pre_final j
  join bt_komp_barn k
  on j.fom = k.fom and j.kompetanse_Resultat = k.kompetanse_Resultat
  and j.kafka_offset = k.kafka_offset
)

select
  --ROWNUM as PK_BT_KOMPETANSE_BARN,
  dvh_fambt_kafka.hibernate_sequence.nextval as PK_BT_KOMPETANSE_BARN,
  FK_BT_KOMPETANSE_PERIODER,
  FK_PERSON1,
  kafka_mottatt_dato
from final




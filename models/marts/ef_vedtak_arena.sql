/*
with dim_maalgruppe_type_ AS (
SELECT
pk_dim_maalgruppe_type,
maalgruppe_kode,
maalgruppe_navn_scd1
FROM dvh_fam_ef.stg_fam_ef_arena_dim_maalgruppe
WHERE
maalgruppe_kode IN ('ENSFORUTD', 'ENSFORARBS', 'TIDLFAMPL', 'GJENEKUTD', 'GJENEKARBS')
),

dim_omraade AS(
SELECT pk_dim_f_stonad_omraade, stonad_kode, stonad_navn FROM dvh_fam_ef.stg_fam_ef_arena_dim_omraade
WHERE
stonad_kode IN ('TSOBOUTG', 'TSODAGREIS', 'TSOFLYTT', 'TSOLMIDLER', 'TSOREISAKT', 'TSOREISARB', 'TSOREISOBL', 'TSOTILBARN', 'TSOTILFAM')
),

fak_vedtak_fakta as (
select * from dvh_fam_ef.stg_fam_ef_arena_fak_vedtak_fakta
where VEDTAK_FAKTA_VERDI_DATO BETWEEN to_date(202201||'01','yyyymmdd') and last_day(to_date(202201||'01','yyyymmdd'))
AND VEDTAK_FAKTA_KODE in ('INNVF','FDATO','TDATO')
),

fak_arena_sak_vedtak as (
select * from dvh_fam_ef.stg_fam_ef_arena_fak_sak_vedtak
where postering_postert_dato BETWEEN to_date(202201||'01','yyyymmdd') and last_day(to_date(202201||'01','yyyymmdd'))
),

person_kontaktinfo AS (
SELECT * FROM dvh_fam_ef.stg_fam_ef_arena_dim_person_k_info
),

dim_geo AS (
SELECT * FROM dvh_fam_ef.stg_fam_ef_arena_dim_geo
),

fak_vilkaar_vurdering as (
select * from dvh_fam_ef.stg_fam_ef_arena_fak_vilkaar_vurdering
),

dim_vilkaar_type as (
select * from dvh_fam_ef.stg_fam_ef_arena_dim_vilkaar_type
),

pre_final as (
SELECT
FASV.LK_VEDTAK_ID,
MKID.FODSELSNUMMER_GJELDENDE as "FNR",
--VFAKTA_INNFV.VEDTAK_FAKTA_KODE,
FASV.FK_PERSON1,
G.KOMMUNE_NR,
G.BYDEL_NR,
FASV.VEDTAK_SAK_RESULTAT_KODE,
OMR.STONAD_KODE,
FASV.STONADBERETT_AKTIVITET_FLAGG,
FASV.AAR,
FASV.SAK_STATUS_KODE,
OMR.STONAD_NAVN,
MAALGRP.MAALGRUPPE_KODE,
MAALGRP.MAALGRUPPE_NAVN_SCD1 as "MAALGRUPPE_NAVN",
VFAKTA_INNFV.VEDTAK_FAKTA_VERDI_DATO as "VEDTAK_DATO",
VILKVURD.VILKAAR_KODE,
VILKVURD.VILKAAR_STATUS_KODE,
VILKTYPE.VILKAAR_NAVN_SCD1 as "VILKAAR_NAVN",
FASV.VEDTAK_SAK_TYPE_KODE,
FASV.VEDTAK_BEHANDLING_STATUS,
VFAKTA_FDATO.VEDTAK_FAKTA_VERDI_DATO AS "GYLDIG_FRA_DATO",
VFAKTA_TDATO.VEDTAK_FAKTA_VERDI_DATO AS "GYLDIG_TIL_DATO",
'ARENA' kildesystem,
sysdate lastet_dato


FROM fak_arena_sak_vedtak FASV
join fak_vedtak_fakta VFAKTA_INNFV
ON FASV.LK_VEDTAK_ID = VFAKTA_INNFV.LK_VEDTAK_ID
and VFAKTA_INNFV.VEDTAK_FAKTA_KODE in ('INNVF')

inner JOIN dim_maalgruppe_type_ maalgrp
ON FASV.FK_DIM_MAALGRUPPE_TYPE = MAALGRP.PK_DIM_MAALGRUPPE_TYPE

inner JOIN person_kontaktinfo mkid
ON FASV.FK_PERSON1 = MKID.FK_PERSON1

inner JOIN dim_geo G
ON FASV.FK_DIM_GEOGRAFI_BOSTED = G.PK_DIM_GEOGRAFI

inner JOIN dim_omraade OMR
ON FASV.FK_DIM_F_STONAD_OMRAADE = OMR.PK_DIM_F_STONAD_OMRAADE

inner JOIN fak_vilkaar_vurdering vilkvurd
ON FASV.LK_VEDTAK_ID = VILKVURD.LK_VEDTAK_ID

inner JOIN dim_vilkaar_type vilktype
ON VILKVURD.FK_DIM_VILKAAR_TYPE = VILKTYPE.PK_DIM_VILKAAR_TYPE

inner JOIN fak_vedtak_fakta VFAKTA_FDATO
ON FASV.LK_VEDTAK_ID = VFAKTA_FDATO.LK_VEDTAK_ID
AND VFAKTA_FDATO.VEDTAK_FAKTA_KODE = 'FDATO'

LEFT OUTER JOIN fak_vedtak_fakta VFAKTA_TDATO
ON FASV.LK_VEDTAK_ID = VFAKTA_TDATO.LK_VEDTAK_ID
and VFAKTA_TDATO.VEDTAK_FAKTA_KODE = 'TDATO'
),


final as (
	select * from pre_final

)

select * from final

*/


with dim_maalgruppe_type_ AS (
    SELECT
      pk_dim_maalgruppe_type,
      maalgruppe_kode,
      maalgruppe_navn_scd1
    FROM {{ ref ('stg_fam_ef_arena_dim_maalgruppe') }}
    WHERE
      maalgruppe_kode IN ('ENSFORUTD', 'ENSFORARBS', 'TIDLFAMPL', 'GJENEKUTD', 'GJENEKARBS')
),

dim_omraade AS(
    SELECT pk_dim_f_stonad_omraade, stonad_kode, stonad_navn FROM {{ ref ('stg_fam_ef_arena_dim_omraade') }}
    WHERE
      stonad_kode IN ('TSOBOUTG', 'TSODAGREIS', 'TSOFLYTT', 'TSOLMIDLER', 'TSOREISAKT', 'TSOREISARB', 'TSOREISOBL', 'TSOTILBARN', 'TSOTILFAM')
),

fak_vedtak_fakta as (
  select * from {{ ref ('stg_fam_ef_arena_fak_vedtak_fakta')}}
),

fak_arena_sak_vedtak as (
  select * from {{ ref ('stg_fam_ef_arena_fak_sak_vedtak')}}
),

person AS (
    SELECT * FROM {{ ref ('stg_fam_ef_arena_dim_person') }}
),
person_kontaktinfo AS (
    SELECT * FROM {{ ref ('stg_fam_ef_arena_dim_person_k_info') }}
),

dim_geo AS (
    SELECT * FROM {{ ref ('stg_fam_ef_arena_dim_geo') }}
),

fak_vilkaar_vurdering as (
  select * from {{ ref ('stg_fam_ef_arena_fak_vilkaar_vurdering')}}
),

dim_vilkaar_type as (
  select * from {{ ref ('stg_fam_ef_arena_dim_vilkaar_type')}}
),

final as (
      SELECT
			{{ var ("periode") }} AS PERIODE,
		  FASV.LK_VEDTAK_ID,
		  MKID.FODSELSNUMMER_GJELDENDE as "FNR",
      PERS.FK_PERSON1,
		  G.KOMMUNE_NR,
		  G.BYDEL_NR,
		  FASV.VEDTAK_SAK_RESULTAT_KODE,
		  OMR.STONAD_KODE,
		  FASV.STONADBERETT_AKTIVITET_FLAGG,
		  FASV.AAR,
		  FASV.SAK_STATUS_KODE,
		  OMR.STONAD_NAVN,
		  MAALGRP.MAALGRUPPE_KODE,
		  MAALGRP.MAALGRUPPE_NAVN_SCD1 as "MAALGRUPPE_NAVN",
		  VFAKTA_INNFV.VEDTAK_FAKTA_VERDI_DATO as "VEDTAK_DATO",
		  VILKVURD.VILKAAR_KODE,
		  VILKVURD.VILKAAR_STATUS_KODE,
		  VILKTYPE.VILKAAR_NAVN_SCD1 as "VILKAAR_NAVN",
		  FASV.VEDTAK_SAK_TYPE_KODE,
		  FASV.VEDTAK_BEHANDLING_STATUS,
		  VFAKTA_FDATO.VEDTAK_FAKTA_VERDI_DATO AS "GYLDIG_FRA_DATO",
		  VFAKTA_TDATO.VEDTAK_FAKTA_VERDI_DATO AS "GYLDIG_TIL_DATO",
      'DBT_ARENA' kildesystem,
      sysdate lastet_dato

	FROM  fak_vedtak_fakta VFAKTA_INNFV
	JOIN fak_arena_sak_vedtak FASV
	ON FASV.LK_VEDTAK_ID = VFAKTA_INNFV.LK_VEDTAK_ID

	JOIN dim_maalgruppe_type_ maalgrp
	ON FASV.FK_DIM_MAALGRUPPE_TYPE = MAALGRP.PK_DIM_MAALGRUPPE_TYPE

	JOIN person PERS
	ON FASV.FK_DIM_PERSON = PERS.PK_DIM_PERSON

	JOIN person_kontaktinfo mkid
	ON PERS.FK_PERSON1 = MKID.FK_PERSON1

	JOIN dim_geo G
	ON PERS.FK_DIM_GEOGRAFI_BOSTED = G.PK_DIM_GEOGRAFI

	JOIN dim_omraade OMR
	ON FASV.FK_DIM_F_STONAD_OMRAADE = OMR.PK_DIM_F_STONAD_OMRAADE

	JOIN fak_vilkaar_vurdering vilkvurd
	ON FASV.LK_VEDTAK_ID = VILKVURD.LK_VEDTAK_ID

	JOIN dim_vilkaar_type vilktype
	ON VILKVURD.FK_DIM_VILKAAR_TYPE = VILKTYPE.PK_DIM_VILKAAR_TYPE

  JOIN fak_vedtak_fakta VFAKTA_FDATO
	ON FASV.LK_VEDTAK_ID = VFAKTA_FDATO.LK_VEDTAK_ID
	AND VFAKTA_FDATO.VEDTAK_FAKTA_KODE = 'FDATO'

	LEFT OUTER JOIN fak_vedtak_fakta VFAKTA_TDATO
	ON FASV.LK_VEDTAK_ID = VFAKTA_TDATO.LK_VEDTAK_ID
  and VFAKTA_TDATO.VEDTAK_FAKTA_KODE = 'TDATO'

  WHERE
	VFAKTA_INNFV.VEDTAK_FAKTA_KODE = 'INNVF'
	AND VFAKTA_INNFV.VEDTAK_FAKTA_VERDI_DATO BETWEEN to_date({{ var ("periode") }}||'01','yyyymmdd') and last_day(to_date({{ var ("periode") }}||'01','yyyymmdd'))
	and FASV.GYLDIG_TIL_DATO = to_date('31.12.9999')

)


select

--PK_FAM_EF_VEDTAK_ARENA,
FK_PERSON1,
PERIODE,
LK_VEDTAK_ID,
KOMMUNE_NR,
BYDEL_NR,
VEDTAK_SAK_RESULTAT_KODE,
STONAD_KODE,
STONADBERETT_AKTIVITET_FLAGG,
AAR,
SAK_STATUS_KODE,
STONAD_NAVN,
MAALGRUPPE_KODE,
MAALGRUPPE_NAVN,
VEDTAK_DATO,
VILKAAR_KODE,
VILKAAR_STATUS_KODE,
VILKAAR_NAVN,
VEDTAK_SAK_TYPE_KODE,
VEDTAK_BEHANDLING_STATUS,
GYLDIG_FRA_DATO,
GYLDIG_TIL_DATO,
KILDESYSTEM,
LASTET_DATO

from final
order by fk_person1


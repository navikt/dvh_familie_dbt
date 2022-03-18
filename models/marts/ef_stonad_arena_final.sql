{{ config(materialized='incremental') }}
WITH ef_stonad_arena_agg_per_pvt_ AS (
  SELECT * FROM {{ ref ('ef_stonad_arena_agg_per_pvt') }}
),
ef_stonad_arena_ AS (
  SELECT * FROM {{ ref ('ef_stonad_arena') }}
),
barntrygd_data AS (
  SELECT * FROM {{ source ('fam_bt', 'fam_bt_barn') }}
),
ikke_skjermet_person_kontakt_info AS (
  SELECT * FROM {{ source ('dt_person_arena','dvh_person_ident_off_id_ikke_skjermet') }}
),

k67 as (
    SELECT fk_person1, count(*) FROM ikke_skjermet_person_kontakt_info
    group by fk_person1
),

legg_til_utdanningsstonad AS (
  SELECT

    fk_person1,
    periode,
    alder,
    kommune_nr,
    bydel_nr,
    kjonn_kode,
    maalgruppe_kode,
    maalgruppe_navn,
    statsborgerskap,
    fodeland,
    sivilstatus_kode,
    antblav,
    antbhoy,
    barn_under_18_antall,
    inntekt_siste_beraar,
    inntekt_3_siste_beraar,

    CASE
      WHEN maalgruppe_kode IN ('ENSFORUTD','TIDLFAMPL','GJENEKUTD') THEN COALESCE(tsoboutg,0) + COALESCE(tsodagreis,0) + COALESCE(tsolmidler,0) + COALESCE(tsoreisakt,0) + COALESCE(tsoreisarb,0) + COALESCE(tsoreisobl,0)
    END AS utdstonad,

    tsotilbarn,
    tsolmidler,
    tsoboutg,
    tsodagreis,
    tsoreisobl,
    tsoflytt,
    tsoreisakt,
    tsoreisarb,
    tsotilfam
  FROM
    ef_stonad_arena_agg_per_pvt_
),
barn_data_koloner AS (
  SELECT
    ef_stonad_arena_.fk_person1,
    MIN(NVL(CASE WHEN FLOOR(months_between(LAST_DAY(TO_DATE(ef_stonad_arena_.periode, 'yyyymm')),LAST_DAY(TO_DATE(bt_d.fodsel_aar_barn || bt_d.fodsel_mnd_barn,'yyyymm'))) / 12) < 0 THEN 0
    ELSE FLOOR(months_between(LAST_DAY(TO_DATE(ef_stonad_arena_.periode, 'yyyymm')),LAST_DAY(TO_DATE(bt_d.fodsel_aar_barn || bt_d.fodsel_mnd_barn,'yyyymm'))) / 12)
    END,0)) ybarn,

    COUNT(DISTINCT bt_d.fkb_person1) antbarn,

    COUNT(DISTINCT CASE WHEN FLOOR(months_between(LAST_DAY(TO_DATE(ef_stonad_arena_.periode, 'yyyymm')),LAST_DAY(TO_DATE(bt_d.fodsel_aar_barn || bt_d.fodsel_mnd_barn,'yyyymm'))) / 12) < 1 THEN bt_d.fkb_person1
    ELSE NULL
    END) antbu1,

    COUNT(DISTINCT CASE WHEN FLOOR(months_between(LAST_DAY(TO_DATE(ef_stonad_arena_.periode, 'yyyymm')),LAST_DAY(TO_DATE(bt_d.fodsel_aar_barn || bt_d.fodsel_mnd_barn,'yyyymm'))) / 12) < 3 THEN bt_d.fkb_person1
    ELSE NULL
    END) antbu3,

    COUNT( DISTINCT CASE WHEN FLOOR(months_between(LAST_DAY(TO_DATE(ef_stonad_arena_.periode, 'yyyymm')),LAST_DAY( TO_DATE(bt_d.fodsel_aar_barn || bt_d.fodsel_mnd_barn,'yyyymm')) ) / 12) < 8 THEN bt_d.fkb_person1
    ELSE NULL
    END) antbu8,

    COUNT( DISTINCT CASE WHEN FLOOR( months_between(LAST_DAY(TO_DATE(ef_stonad_arena_.periode, 'yyyymm')),LAST_DAY(TO_DATE( bt_d.fodsel_aar_barn || bt_d.fodsel_mnd_barn,'yyyymm' ))) / 12 ) < 10 THEN bt_d.fkb_person1
    ELSE NULL
    END) antbu10,

    COUNT( DISTINCT CASE WHEN FLOOR(months_between(LAST_DAY(TO_DATE(ef_stonad_arena_.periode, 'yyyymm')),LAST_DAY(TO_DATE(bt_d.fodsel_aar_barn || bt_d.fodsel_mnd_barn,'yyyymm'))) / 12) < 18 THEN bt_d.fkb_person1
    ELSE NULL
    END) antbu18

  FROM

    ef_stonad_arena_
    LEFT JOIN barntrygd_data bt_d
    ON ef_stonad_arena_.fk_person1 = bt_d.fk_person1
    AND ef_stonad_arena_.periode = bt_d.stat_aarmnd
  GROUP BY
    ef_stonad_arena_.fk_person1
),

final AS (
  SELECT
    utdstnd.*,
    bdk.antbarn,
    bdk.ybarn,
    bdk.antbu1,
    bdk.antbu3,
    bdk.antbu8,
    bdk.antbu10,
    bdk.antbu18,
    'ARENA' as kildesystem,
    sysdate lastet_dato
  FROM
    legg_til_utdanningsstonad utdstnd
    JOIN barn_data_koloner bdk
    ON utdstnd.fk_person1 = bdk.fk_person1
    JOIN k67
    ON utdstnd.fk_person1 = k67.fk_person1
  ORDER BY
    utdstnd.fk_person1
)

SELECT
  *
FROM final

where periode > (select NVL(max(periode),0) from {{ this }})

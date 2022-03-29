WITH ef_stonad_arena_agg_pr_person_ AS (
  SELECT * FROM {{ ref ('ef_stonad_arena_agg_pr_person') }}
),

hoy_lav AS (
  SELECT
    fodselsnummer_gjeldende,
    MAX(antblav) AS antblav,
    MAX(antbhoy) AS antbhoy
  FROM
    ef_stonad_arena_agg_pr_person_
  GROUP BY
    fodselsnummer_gjeldende
),
trans_ef_arena AS (
  SELECT
    *
  FROM
    (
      SELECT
        periode,
        stonad_kode,
        postert_belop,
        alder,
        kommune_nr,
        bydel_nr,
        pk_dim_geografi,
        kjonn_kode,
        maalgruppe_kode,
        maalgruppe_navn,
        statsborgerskap,
        fodeland,
        sivilstatus_kode,
        barn_under_18_antall,
        inntekt_siste_beraar,
        inntekt_3_siste_beraar,
        fodselsnummer_gjeldende,
        fk_dim_person,
        fk_person1
      FROM
        ef_stonad_arena_agg_pr_person_
    ) pivot(MAX(postert_belop) for stonad_kode IN ('TSOBOUTG' AS tsoboutg, 'TSODAGREIS' AS tsodagreis, 'TSOFLYTT' AS tsoflytt, 'TSOLMIDLER' AS tsolmidler, 'TSOREISAKT' AS tsoreisakt, 'TSOREISARB' AS tsoreisarb, 'TSOREISOBL' AS tsoreisobl, 'TSOTILBARN' AS tsotilbarn, 'TSOTILFAM' AS tsotilfam))
),
FINAL AS (
  SELECT
    trans_ef_arena.*,
    hoy_lav.antblav,
    hoy_lav.antbhoy
  FROM
    trans_ef_arena
    JOIN hoy_lav
    ON trans_ef_arena.fodselsnummer_gjeldende = hoy_lav.fodselsnummer_gjeldende
  ORDER BY
    trans_ef_arena.fodselsnummer_gjeldende
)
SELECT
  *
FROM
  FINAL

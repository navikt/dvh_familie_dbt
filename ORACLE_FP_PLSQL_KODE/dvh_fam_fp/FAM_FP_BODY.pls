CREATE OR REPLACE PACKAGE BODY FAM_FP AS

  FUNCTION DIM_TID_ANTALL(
    P_IN_TID_FOM IN NUMBER,
    P_IN_TID_TOM IN NUMBER
  ) RETURN NUMBER AS
    V_DIM_TID_ANTALL NUMBER := 0;
  BEGIN
    SELECT
      COUNT(1) INTO V_DIM_TID_ANTALL
    FROM
      DT_KODEVERK.DIM_TID
    WHERE
      DAG_I_UKE < 6
      AND DIM_NIVAA = 1
      AND GYLDIG_FLAGG = 1
      AND PK_DIM_TID BETWEEN P_IN_TID_FOM AND P_IN_TID_TOM;
    RETURN V_DIM_TID_ANTALL;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN 0;
  END;

  PROCEDURE FAM_FP_STATISTIKK_MAANED(
    P_IN_VEDTAK_TOM IN VARCHAR2,
    P_IN_RAPPORT_DATO IN VARCHAR2,
    P_IN_FORSKYVNINGER IN NUMBER,
    P_IN_GYLDIG_FLAGG IN NUMBER DEFAULT 0,
    P_IN_PERIODE_TYPE IN VARCHAR2 DEFAULT 'M',
    P_OUT_ERROR OUT VARCHAR2
  ) AS
    CURSOR CUR_PERIODE(P_RAPPORT_DATO IN VARCHAR2, P_FORSKYVNINGER IN NUMBER, P_TID_FOM IN VARCHAR2, P_TID_TOM IN VARCHAR2, P_BUDSJETT IN VARCHAR2) IS
      WITH FAGSAK AS (
        SELECT
          FAGSAK_ID,
          MAX(BEHANDLINGSTEMA)                                                   AS BEHANDLINGSTEMA,
          MAX(FAGSAKANNENFORELDER_ID)                                            AS ANNENFORELDERFAGSAK_ID,
          MAX(TRANS_ID) KEEP(DENSE_RANK FIRST ORDER BY FUNKSJONELL_TID DESC)     AS MAX_TRANS_ID,
          MAX(SOEKNADSDATO) KEEP(DENSE_RANK FIRST ORDER BY FUNKSJONELL_TID DESC) AS SOKNADSDATO,
          MIN(SOEKNADSDATO)                                                      AS FORSTE_SOKNADSDATO,
          MIN(VEDTAKSDATO)                                                       AS FORSTE_VEDTAKSDATO,
          MAX(FUNKSJONELL_TID)                                                   AS FUNKSJONELL_TID,
          MAX(VEDTAKSDATO)                                                       AS SISTE_VEDTAKSDATO,
          P_IN_PERIODE_TYPE                                                      AS PERIODE,
          LAST_DAY(TO_DATE(P_RAPPORT_DATO, 'yyyymm')) + P_FORSKYVNINGER          AS MAX_VEDTAKSDATO
        FROM
          DVH_FAM_FP.FAM_FP_FAGSAK
        WHERE
          FAM_FP_FAGSAK.FUNKSJONELL_TID <= LAST_DAY(TO_DATE(P_RAPPORT_DATO, 'yyyymm')) + P_FORSKYVNINGER
        GROUP BY
          FAGSAK_ID
      ), TERMIN AS (
        SELECT
          FAGSAK_ID,
          MAX(TERMINDATO)            TERMINDATO,
          MAX(FOEDSELSDATO)          FOEDSELSDATO,
          MAX(ANTALL_BARN_TERMIN)    ANTALL_BARN_TERMIN,
          MAX(ANTALL_BARN_FOEDSEL)   ANTALL_BARN_FOEDSEL,
          MAX(FOEDSELSDATO_ADOPSJON) FOEDSELSDATO_ADOPSJON,
          MAX(ANTALL_BARN_ADOPSJON) ANTALL_BARN_ADOPSJON
        FROM
          (
            SELECT
              FAM_FP_FAGSAK.FAGSAK_ID,
              MAX(FODSEL.TERMINDATO)              TERMINDATO,
              MAX(FODSEL.FOEDSELSDATO)            FOEDSELSDATO,
              MAX(FODSEL.ANTALL_BARN_FOEDSEL)     ANTALL_BARN_FOEDSEL,
              MAX(FODSEL.ANTALL_BARN_TERMIN)      ANTALL_BARN_TERMIN,
              MAX(ADOPSJON.FOEDSELSDATO_ADOPSJON) FOEDSELSDATO_ADOPSJON,
              COUNT(ADOPSJON.TRANS_ID)            ANTALL_BARN_ADOPSJON
            FROM
              DVH_FAM_FP.FAM_FP_FAGSAK
              LEFT JOIN DVH_FAM_FP.FAM_FP_FODSELTERMIN FODSEL
              ON FODSEL.FAGSAK_ID = FAM_FP_FAGSAK.FAGSAK_ID
              AND UPPER(FAM_FP_FAGSAK.BEHANDLINGSTEMA) = 'FORP_FODS'
              LEFT JOIN DVH_FAM_FP.FAM_FP_FODSELTERMIN ADOPSJON
              ON ADOPSJON.FAGSAK_ID = FAM_FP_FAGSAK.FAGSAK_ID
              AND ADOPSJON.TRANS_ID = FAM_FP_FAGSAK.TRANS_ID
              AND UPPER(FAM_FP_FAGSAK.BEHANDLINGSTEMA) = 'FORP_ADOP'
            GROUP BY
              FAM_FP_FAGSAK.FAGSAK_ID,
              FAM_FP_FAGSAK.TRANS_ID
          )
        GROUP BY
          FAGSAK_ID
      ), FK_PERSON1 AS (
        SELECT
          PERSON.PERSON,
          PERSON.FAGSAK_ID,
          MAX(PERSON.BEHANDLINGSTEMA)                                                                             AS BEHANDLINGSTEMA,
          PERSON.MAX_TRANS_ID,
          MAX(PERSON.ANNENFORELDERFAGSAK_ID)                                                                      AS ANNENFORELDERFAGSAK_ID,
          PERSON.AKTOER_ID,
          MAX(PERSON.KJONN)                                                                                       AS KJONN,
          MAX(PERSON_67_VASKET.FK_PERSON1) KEEP (DENSE_RANK FIRST ORDER BY PERSON_67_VASKET.GYLDIG_FRA_DATO DESC) AS FK_PERSON1,
          MAX(FOEDSELSDATO)                                                                                       AS FOEDSELSDATO,
          MAX(SIVILSTAND)                                                                                         AS SIVILSTAND,
          MAX(STATSBORGERSKAP)                                                                                    AS STATSBORGERSKAP
        FROM
          (
            SELECT
              'MOTTAKER'                                AS PERSON,
              FAGSAK.FAGSAK_ID,
              FAGSAK.BEHANDLINGSTEMA,
              FAGSAK.MAX_TRANS_ID,
              FAGSAK.ANNENFORELDERFAGSAK_ID,
              FAM_FP_PERSONOPPLYSNINGER.AKTOER_ID,
              FAM_FP_PERSONOPPLYSNINGER.KJONN,
              FAM_FP_PERSONOPPLYSNINGER.FOEDSELSDATO,
              FAM_FP_PERSONOPPLYSNINGER.SIVILSTAND,
              FAM_FP_PERSONOPPLYSNINGER.STATSBORGERSKAP
            FROM
              DVH_FAM_FP.FAM_FP_PERSONOPPLYSNINGER
              JOIN FAGSAK
              ON FAM_FP_PERSONOPPLYSNINGER.TRANS_ID = FAGSAK.MAX_TRANS_ID UNION ALL
              SELECT
                'BARN'                                    AS PERSON,
                FAGSAK.FAGSAK_ID,
                MAX(FAGSAK.BEHANDLINGSTEMA)               AS BEHANDLINGSTEMA,
                FAGSAK.MAX_TRANS_ID,
                MAX(FAGSAK.ANNENFORELDERFAGSAK_ID)        ANNENFORELDERFAGSAK_ID,
                MAX(FAM_FP_FAMILIEHENDELSE.TIL_AKTOER_ID) AS AKTOER_ID,
                MAX(FAM_FP_FAMILIEHENDELSE.KJOENN)        AS KJONN,
                NULL                                      AS FOEDSELSDATO,
                NULL                                      AS SIVILSTAND,
                NULL                                      AS STATSBORGERSKAP
              FROM
                DVH_FAM_FP.FAM_FP_FAMILIEHENDELSE
                JOIN FAGSAK
                ON FAM_FP_FAMILIEHENDELSE.FAGSAK_ID = FAGSAK.FAGSAK_ID
              WHERE
                UPPER(FAM_FP_FAMILIEHENDELSE.RELASJON) = 'BARN'
              GROUP BY
                FAGSAK.FAGSAK_ID, FAGSAK.MAX_TRANS_ID
          )                                    PERSON
          JOIN DT_PERSON.DVH_PERSON_IDENT_AKTOR_IKKE_SKJERMET PERSON_67_VASKET
          ON PERSON_67_VASKET.AKTOR_ID = PERSON.AKTOER_ID
          AND TO_DATE(P_TID_TOM, 'yyyymmdd') BETWEEN PERSON_67_VASKET.GYLDIG_FRA_DATO
          AND PERSON_67_VASKET.GYLDIG_TIL_DATO
        GROUP BY
          PERSON.PERSON, PERSON.FAGSAK_ID, PERSON.MAX_TRANS_ID, PERSON.AKTOER_ID
      ), BARN AS (
        SELECT
          FAGSAK_ID,
          LISTAGG(FK_PERSON1, ',') WITHIN GROUP (ORDER BY FK_PERSON1) AS FK_PERSON1_BARN
        FROM
          FK_PERSON1
        WHERE
          PERSON = 'BARN'
        GROUP BY
          FAGSAK_ID
      ), MOTTAKER AS (
        SELECT
          FK_PERSON1.FAGSAK_ID,
          FK_PERSON1.BEHANDLINGSTEMA,
          FK_PERSON1.MAX_TRANS_ID,
          FK_PERSON1.ANNENFORELDERFAGSAK_ID,
          FK_PERSON1.AKTOER_ID,
          FK_PERSON1.KJONN,
          FK_PERSON1.FK_PERSON1                       AS FK_PERSON1_MOTTAKER,
          EXTRACT(YEAR FROM FK_PERSON1.FOEDSELSDATO) AS MOTTAKER_FODSELS_AAR,
          EXTRACT(MONTH FROM FK_PERSON1.FOEDSELSDATO) AS MOTTAKER_FODSELS_MND,
          FK_PERSON1.SIVILSTAND,
          FK_PERSON1.STATSBORGERSKAP,
          BARN.FK_PERSON1_BARN,
          TERMIN.TERMINDATO,
          TERMIN.FOEDSELSDATO,
          TERMIN.ANTALL_BARN_TERMIN,
          TERMIN.ANTALL_BARN_FOEDSEL,
          TERMIN.FOEDSELSDATO_ADOPSJON,
          TERMIN.ANTALL_BARN_ADOPSJON
        FROM
          FK_PERSON1
          LEFT JOIN BARN
          ON BARN.FAGSAK_ID = FK_PERSON1.FAGSAK_ID
          LEFT JOIN TERMIN
          ON FK_PERSON1.FAGSAK_ID = TERMIN.FAGSAK_ID
        WHERE
          FK_PERSON1.PERSON = 'MOTTAKER'
      ), ADOPSJON AS (
        SELECT
          FAM_FP_VILKAAR.FAGSAK_ID,
          MAX(FAM_FP_VILKAAR.OMSORGS_OVERTAKELSESDATO) AS ADOPSJONSDATO,
          MAX(FAM_FP_VILKAAR.EKTEFELLES_BARN)          AS STEBARNSADOPSJON
        FROM
          FAGSAK
          JOIN DVH_FAM_FP.FAM_FP_VILKAAR
          ON FAGSAK.FAGSAK_ID = FAM_FP_VILKAAR.FAGSAK_ID
        WHERE
          FAGSAK.BEHANDLINGSTEMA = 'FORP_ADOP'
        GROUP BY
          FAM_FP_VILKAAR.FAGSAK_ID
      ), EOS AS (
        SELECT
          A.TRANS_ID,
          CASE
            WHEN UPPER(ER_BORGER_AV_EU_EOS) = 'TRUE' THEN
              'J'
            WHEN UPPER(ER_BORGER_AV_EU_EOS) = 'FALSE' THEN
              'N'
            ELSE
              NULL
          END EOS_SAK
        FROM
          (
            SELECT
              FAM_FP_VILKAAR.TRANS_ID,
              MAX(FAM_FP_VILKAAR.ER_BORGER_AV_EU_EOS) AS ER_BORGER_AV_EU_EOS
            FROM
              FAGSAK
              JOIN DVH_FAM_FP.FAM_FP_VILKAAR
              ON FAGSAK.MAX_TRANS_ID = FAM_FP_VILKAAR.TRANS_ID
              AND LENGTH(FAM_FP_VILKAAR.PERSON_STATUS) > 0
            GROUP BY
              FAM_FP_VILKAAR.TRANS_ID
          ) A
      ), ANNENFORELDERFAGSAK AS (
        SELECT
          ANNENFORELDERFAGSAK.*,
          MOTTAKER.FK_PERSON1_MOTTAKER AS FK_PERSON1_ANNEN_PART
        FROM
          (
            SELECT
              FAGSAK_ID,
              MAX_TRANS_ID,
              MAX(ANNENFORELDERFAGSAK_ID) AS ANNENFORELDERFAGSAK_ID
            FROM
              (
                SELECT
                  FORELDER1.FAGSAK_ID,
                  FORELDER1.MAX_TRANS_ID,
                  NVL(FORELDER1.ANNENFORELDERFAGSAK_ID, FORELDER2.FAGSAK_ID) AS ANNENFORELDERFAGSAK_ID
                FROM
                  MOTTAKER FORELDER1
                  JOIN MOTTAKER FORELDER2
                  ON FORELDER1.FK_PERSON1_BARN = FORELDER2.FK_PERSON1_BARN
                  AND FORELDER1.FK_PERSON1_MOTTAKER != FORELDER2.FK_PERSON1_MOTTAKER
              )
            GROUP BY
              FAGSAK_ID,
              MAX_TRANS_ID
          )        ANNENFORELDERFAGSAK
          JOIN MOTTAKER
          ON ANNENFORELDERFAGSAK.ANNENFORELDERFAGSAK_ID = MOTTAKER.FAGSAK_ID
      ), TID AS (
        SELECT
          PK_DIM_TID,
          DATO,
          AAR,
          HALVAAR,
          KVARTAL,
          AAR_MAANED
        FROM
          DT_KODEVERK.DIM_TID
        WHERE
          DAG_I_UKE < 6
          AND DIM_NIVAA = 1
          AND GYLDIG_FLAGG = 1
          AND PK_DIM_TID BETWEEN P_TID_FOM AND P_TID_TOM
          AND ((P_BUDSJETT = 'A'
          AND PK_DIM_TID <= TO_CHAR(LAST_DAY(TO_DATE(P_RAPPORT_DATO, 'yyyymm')), 'yyyymmdd'))
          OR P_BUDSJETT = 'B')
      ), UTTAK AS (
        SELECT
          UTTAK.TRANS_ID,
          UTTAK.TREKKONTO,
          UTTAK.UTTAK_ARBEID_TYPE,
          UTTAK.VIRKSOMHET,
          UTTAK.UTBETALINGSPROSENT,
          UTTAK.GRADERING_INNVILGET,
          UTTAK.GRADERING,
          UTTAK.ARBEIDSTIDSPROSENT,
          UTTAK.SAMTIDIG_UTTAK,
          UTTAK.PERIODE_RESULTAT_AARSAK,
          UTTAK.FOM                                      AS UTTAK_FOM,
          UTTAK.TOM                                      AS UTTAK_TOM,
          UTTAK.TREKKDAGER,
          FAGSAK.FAGSAK_ID,
          FAGSAK.PERIODE,
          FAGSAK.FUNKSJONELL_TID,
          FAGSAK.FORSTE_VEDTAKSDATO,
          FAGSAK.SISTE_VEDTAKSDATO,
          FAGSAK.MAX_VEDTAKSDATO,
          FAGSAK.FORSTE_SOKNADSDATO,
          FAGSAK.SOKNADSDATO,
          FAM_FP_TREKKONTO.PK_FAM_FP_TREKKONTO,
          AARSAK_UTTAK.PK_FAM_FP_PERIODE_RESULTAT_AARSAK,
          UTTAK.ARBEIDSFORHOLD_ID,
          UTTAK.GRADERINGSDAGER,
          FAM_FP_UTTAK_FORDELINGSPER.MORS_AKTIVITET
        FROM
          DVH_FAM_FP.FAM_FP_UTTAK_RES_PER_AKTIV UTTAK
          JOIN FAGSAK
          ON FAGSAK.MAX_TRANS_ID = UTTAK.TRANS_ID LEFT JOIN DVH_FAM_FP.FAM_FP_TREKKONTO
          ON UPPER(UTTAK.TREKKONTO) = FAM_FP_TREKKONTO.TREKKONTO
          LEFT JOIN (
            SELECT
              AARSAK_UTTAK,
              MAX(PK_FAM_FP_PERIODE_RESULTAT_AARSAK) AS PK_FAM_FP_PERIODE_RESULTAT_AARSAK
            FROM
              DVH_FAM_FP.FAM_FP_PERIODE_RESULTAT_AARSAK
            GROUP BY
              AARSAK_UTTAK
          ) AARSAK_UTTAK
          ON UPPER(UTTAK.PERIODE_RESULTAT_AARSAK) = AARSAK_UTTAK.AARSAK_UTTAK
          LEFT JOIN DVH_FAM_FP.FAM_FP_UTTAK_FORDELINGSPER
          ON FAM_FP_UTTAK_FORDELINGSPER.TRANS_ID = UTTAK.TRANS_ID
          AND UTTAK.FOM BETWEEN FAM_FP_UTTAK_FORDELINGSPER.FOM
          AND FAM_FP_UTTAK_FORDELINGSPER.TOM
          AND UPPER(UTTAK.TREKKONTO) = UPPER(FAM_FP_UTTAK_FORDELINGSPER.PERIODE_TYPE)
          AND LENGTH(FAM_FP_UTTAK_FORDELINGSPER.MORS_AKTIVITET) > 1
        WHERE
          UTTAK.UTBETALINGSPROSENT > 0
      ), STONADSDAGER_KVOTE AS (
        SELECT
          UTTAK.*,
          TID1.PK_DIM_TID AS FK_DIM_TID_MIN_DATO_KVOTE,
          TID2.PK_DIM_TID AS FK_DIM_TID_MAX_DATO_KVOTE
        FROM
          (
            SELECT
              FAGSAK_ID,
              TRANS_ID,
              TREKKONTO,
              VIRKSOMHET,
              UTTAK_ARBEID_TYPE,
              SUM(TREKKDAGER)   AS STONADSDAGER_KVOTE,
              MIN(UTTAK_FOM)    AS MIN_UTTAK_FOM,
              MAX(UTTAK_TOM)    AS MAX_UTTAK_TOM
            FROM
              (
                SELECT
                  FAGSAK_ID,
                  TRANS_ID,
                  UTTAK_FOM,
                  UTTAK_TOM,
                  TREKKONTO,
                  VIRKSOMHET,
                  UTTAK_ARBEID_TYPE,
                  MAX(TREKKDAGER)   AS TREKKDAGER
                FROM
                  UTTAK
                GROUP BY
                  FAGSAK_ID,
                  TRANS_ID,
                  UTTAK_FOM,
                  UTTAK_TOM,
                  TREKKONTO,
                  VIRKSOMHET,
                  UTTAK_ARBEID_TYPE
              ) A
            GROUP BY
              FAGSAK_ID,
              TRANS_ID,
              TREKKONTO,
              VIRKSOMHET,
              UTTAK_ARBEID_TYPE
          )                   UTTAK
          JOIN DT_KODEVERK.DIM_TID TID1
          ON TID1.DIM_NIVAA = 1
          AND TID1.DATO = TRUNC(UTTAK.MIN_UTTAK_FOM, 'dd') JOIN DT_KODEVERK.DIM_TID TID2
          ON TID2.DIM_NIVAA = 1
          AND TID2.DATO = TRUNC(UTTAK.MAX_UTTAK_TOM,
          'dd')
      ), UTTAK_DAGER AS (
        SELECT
          UTTAK.*,
          TID.PK_DIM_TID,
          TID.DATO,
          TID.AAR,
          TID.HALVAAR,
          TID.KVARTAL,
          TID.AAR_MAANED
        FROM
          UTTAK
          JOIN TID
          ON TID.DATO BETWEEN UTTAK.UTTAK_FOM
          AND UTTAK.UTTAK_TOM
      ), ALENEOMSORG AS (
        SELECT
          UTTAK.FAGSAK_ID,
          UTTAK.UTTAK_FOM
        FROM
          UTTAK
          JOIN DVH_FAM_FP.FAM_FP_DOKUMENTASJONSPERIODER DOK1
          ON DOK1.FAGSAK_ID = UTTAK.FAGSAK_ID
          AND UTTAK.UTTAK_FOM >= DOK1.FOM
          AND DOK1.DOKUMENTASJON_TYPE IN ('ALENEOMSORG', 'ALENEOMSORG_OVERFØRING') LEFT JOIN DVH_FAM_FP.FAM_FP_DOKUMENTASJONSPERIODER DOK2
          ON DOK1.FAGSAK_ID = DOK2.FAGSAK_ID
          AND UTTAK.UTTAK_FOM >= DOK2.FOM
          AND DOK1.TRANS_ID < DOK2.TRANS_ID
          AND DOK2.DOKUMENTASJON_TYPE = 'ANNEN_FORELDER_HAR_RETT'
          AND DOK2.FAGSAK_ID IS NULL
        GROUP BY
          UTTAK.FAGSAK_ID,
          UTTAK.UTTAK_FOM
      ), BEREGNINGSGRUNNLAG AS (
        SELECT
          FAGSAK_ID,
          TRANS_ID,
          VIRKSOMHETSNUMMER,
          MAX(STATUS_OG_ANDEL_BRUTTO)         AS STATUS_OG_ANDEL_BRUTTO,
          MAX(STATUS_OG_ANDEL_AVKORTET)       AS STATUS_OG_ANDEL_AVKORTET,
          FOM                                 AS BEREGNINGSGRUNNLAG_FOM,
          TOM                                 AS BEREGNINGSGRUNNLAG_TOM,
          MAX(DEKNINGSGRAD)                   AS DEKNINGSGRAD,
          MAX(DAGSATS)                        AS DAGSATS,
          DAGSATS_BRUKER,
          DAGSATS_ARBEIDSGIVER,
          DAGSATS_BRUKER+DAGSATS_ARBEIDSGIVER DAGSATS_VIRKSOMHET,
          MAX(STATUS_OG_ANDEL_INNTEKTSKAT)    AS STATUS_OG_ANDEL_INNTEKTSKAT,
          AKTIVITET_STATUS,
          MAX(BRUTTO)                         AS BRUTTO_INNTEKT,
          MAX(AVKORTET)                       AS AVKORTET_INNTEKT,
          COUNT(1)                            AS ANTALL_BEREGNINGSGRUNNLAG
        FROM
          DVH_FAM_FP.FAM_FP_BEREGNINGSGRUNNLAG
        GROUP BY
          FAGSAK_ID,
          TRANS_ID,
          VIRKSOMHETSNUMMER,
          FOM,
          TOM,
          AKTIVITET_STATUS,
          DAGSATS_BRUKER,
          DAGSATS_ARBEIDSGIVER
      ), BEREGNINGSGRUNNLAG_DETALJ AS (
        SELECT
          UTTAK_DAGER.*,
          STONADSDAGER_KVOTE.STONADSDAGER_KVOTE,
          STONADSDAGER_KVOTE.MIN_UTTAK_FOM,
          STONADSDAGER_KVOTE.MAX_UTTAK_TOM,
          STONADSDAGER_KVOTE.FK_DIM_TID_MIN_DATO_KVOTE,
          STONADSDAGER_KVOTE.FK_DIM_TID_MAX_DATO_KVOTE,
          BEREG.STATUS_OG_ANDEL_BRUTTO,
          BEREG.STATUS_OG_ANDEL_AVKORTET,
          BEREG.BEREGNINGSGRUNNLAG_FOM,
          BEREG.DEKNINGSGRAD,
          BEREG.BEREGNINGSGRUNNLAG_TOM,
          BEREG.DAGSATS,
          BEREG.DAGSATS_BRUKER,
          BEREG.DAGSATS_ARBEIDSGIVER,
          BEREG.DAGSATS_VIRKSOMHET,
          BEREG.STATUS_OG_ANDEL_INNTEKTSKAT,
          BEREG.AKTIVITET_STATUS,
          BEREG.BRUTTO_INNTEKT,
          BEREG.AVKORTET_INNTEKT,
          BEREG.DAGSATS*UTTAK_DAGER.UTBETALINGSPROSENT/100 AS DAGSATS_ERST,
          BEREG.ANTALL_BEREGNINGSGRUNNLAG
        FROM
          BEREGNINGSGRUNNLAG                        BEREG
          JOIN UTTAK_DAGER
          ON UTTAK_DAGER.TRANS_ID = BEREG.TRANS_ID
          AND NVL(UTTAK_DAGER.VIRKSOMHET, 'X') = NVL(BEREG.VIRKSOMHETSNUMMER, 'X')
          AND BEREG.BEREGNINGSGRUNNLAG_FOM <= UTTAK_DAGER.DATO
          AND NVL(BEREG.BEREGNINGSGRUNNLAG_TOM, TO_DATE('20991201', 'YYYYMMDD')) >= UTTAK_DAGER.DATO LEFT JOIN STONADSDAGER_KVOTE
          ON UTTAK_DAGER.TRANS_ID = STONADSDAGER_KVOTE.TRANS_ID
          AND UTTAK_DAGER.TREKKONTO = STONADSDAGER_KVOTE.TREKKONTO
          AND NVL(UTTAK_DAGER.VIRKSOMHET,
          'X') = NVL(STONADSDAGER_KVOTE.VIRKSOMHET,
          'X')
          AND UTTAK_DAGER.UTTAK_ARBEID_TYPE = STONADSDAGER_KVOTE.UTTAK_ARBEID_TYPE
          JOIN DVH_FAM_FP.FAM_FP_UTTAK_AKTIVITET_MAPPING UTTAK_MAPPING
          ON UTTAK_DAGER.UTTAK_ARBEID_TYPE = UTTAK_MAPPING.UTTAK_ARBEID
          AND BEREG.AKTIVITET_STATUS = UTTAK_MAPPING.AKTIVITET_STATUS
        WHERE
          BEREG.DAGSATS_BRUKER + BEREG.DAGSATS_ARBEIDSGIVER != 0
      ), BEREGNINGSGRUNNLAG_AGG AS (
        SELECT
          A.*,
          DAGER_ERST*DAGSATS_VIRKSOMHET/DAGSATS*ANTALL_BEREGNINGSGRUNNLAG                                                                                                                    TILFELLE_ERST,
          DAGER_ERST*ROUND(UTBETALINGSPROSENT/100*DAGSATS_VIRKSOMHET)                                                                                                                        BELOP,
          ROUND(UTBETALINGSPROSENT/100*DAGSATS_VIRKSOMHET-0.5)                                                                                                                               DAGSATS_REDUSERT,
          CASE
            WHEN PERIODE_RESULTAT_AARSAK IN (2004, 2033) THEN
              'N'
            WHEN TREKKONTO IN ('FEDREKVOTE', 'FELLESPERIODE', 'MØDREKVOTE') THEN
              'J'
            WHEN TREKKONTO = 'FORELDREPENGER' THEN
              'N'
          END MOR_RETTIGHET
        FROM
          (
            SELECT
              FAGSAK_ID,
              TRANS_ID,
              TREKKONTO,
              TREKKDAGER,
              STONADSDAGER_KVOTE,
              UTTAK_ARBEID_TYPE,
              AAR,
              HALVAAR,
              KVARTAL,
              AAR_MAANED,
              UTTAK_FOM,
              UTTAK_TOM,
              SUM(DAGSATS_VIRKSOMHET/DAGSATS*
                CASE
                  WHEN ((UPPER(GRADERING_INNVILGET) ='TRUE'
                  AND UPPER(GRADERING)='TRUE')
                  OR UPPER(SAMTIDIG_UTTAK)='TRUE') THEN
                    (100-ARBEIDSTIDSPROSENT)/100
                  ELSE
                    1.0
                END )                                DAGER_ERST2,
              MAX(ARBEIDSTIDSPROSENT)                AS ARBEIDSTIDSPROSENT,
              COUNT(DISTINCT PK_DIM_TID)             DAGER_ERST,
 --count(distinct pk_dim_tid)*dagsats_virksomhet/dagsats tilfelle_erst,
              MIN(BEREGNINGSGRUNNLAG_FOM)            BEREGNINGSGRUNNLAG_FOM,
              MAX(BEREGNINGSGRUNNLAG_TOM)            BEREGNINGSGRUNNLAG_TOM,
              DEKNINGSGRAD,
 --count(distinct pk_dim_tid)*
 --      round(utbetalingsprosent/100*dagsats_virksomhet-0.5) belop,
              DAGSATS_BRUKER,
              DAGSATS_ARBEIDSGIVER,
              DAGSATS_VIRKSOMHET,
              VIRKSOMHET,
              PERIODE_RESULTAT_AARSAK,
              DAGSATS,
              DAGSATS_ERST, --dagsats_virksomhet,
              UTBETALINGSPROSENT                     GRADERINGSPROSENT,
              STATUS_OG_ANDEL_INNTEKTSKAT,
              AKTIVITET_STATUS,
              BRUTTO_INNTEKT,
              AVKORTET_INNTEKT,
              STATUS_OG_ANDEL_BRUTTO,
              STATUS_OG_ANDEL_AVKORTET,
 --round(utbetalingsprosent/100*dagsats_virksomhet-0.5) dagsats_redusert,
              UTBETALINGSPROSENT,
              MIN(PK_DIM_TID)                        PK_DIM_TID_DATO_UTBET_FOM,
              MAX(PK_DIM_TID)                        PK_DIM_TID_DATO_UTBET_TOM,
              FUNKSJONELL_TID,
              FORSTE_VEDTAKSDATO,
              SISTE_VEDTAKSDATO,
              MAX_VEDTAKSDATO,
              PERIODE,
              MAX(FORSTE_SOKNADSDATO)                AS FORSTE_SOKNADSDATO,
              MAX(SOKNADSDATO)                       AS SOKNADSDATO,
              SAMTIDIG_UTTAK,
              GRADERING,
              GRADERING_INNVILGET,
              MIN_UTTAK_FOM,
              MAX_UTTAK_TOM,
              FK_DIM_TID_MIN_DATO_KVOTE,
              FK_DIM_TID_MAX_DATO_KVOTE,
              MAX(PK_FAM_FP_TREKKONTO)               AS PK_FAM_FP_TREKKONTO,
              MAX(PK_FAM_FP_PERIODE_RESULTAT_AARSAK) AS PK_FAM_FP_PERIODE_RESULTAT_AARSAK,
              ANTALL_BEREGNINGSGRUNNLAG,
              MAX(GRADERINGSDAGER)                   AS GRADERINGSDAGER,
              MAX(MORS_AKTIVITET)                    AS MORS_AKTIVITET
            FROM
              BEREGNINGSGRUNNLAG_DETALJ
            GROUP BY
              FAGSAK_ID,
              TRANS_ID,
              TREKKONTO,
              TREKKDAGER,
              STONADSDAGER_KVOTE,
              UTTAK_ARBEID_TYPE,
              AAR,
              HALVAAR,
              KVARTAL,
              AAR_MAANED,
              UTTAK_FOM,
              UTTAK_TOM,
              DEKNINGSGRAD,
              VIRKSOMHET,
              UTBETALINGSPROSENT,
              PERIODE_RESULTAT_AARSAK,
              DAGSATS,
              DAGSATS_ERST,
              DAGSATS_BRUKER,
              DAGSATS_ARBEIDSGIVER,
              DAGSATS_VIRKSOMHET,
              UTBETALINGSPROSENT,
              STATUS_OG_ANDEL_INNTEKTSKAT,
              AKTIVITET_STATUS,
              BRUTTO_INNTEKT,
              AVKORTET_INNTEKT,
              STATUS_OG_ANDEL_BRUTTO,
              STATUS_OG_ANDEL_AVKORTET,
              FUNKSJONELL_TID,
              FORSTE_VEDTAKSDATO,
              SISTE_VEDTAKSDATO,
              MAX_VEDTAKSDATO,
              PERIODE,
              SAMTIDIG_UTTAK,
              GRADERING,
              GRADERING_INNVILGET,
              MIN_UTTAK_FOM,
              MAX_UTTAK_TOM,
              FK_DIM_TID_MIN_DATO_KVOTE,
              FK_DIM_TID_MAX_DATO_KVOTE,
              ANTALL_BEREGNINGSGRUNNLAG
          ) A
      ), GRUNNLAG AS (
        SELECT
          BEREGNINGSGRUNNLAG_AGG.*,
          SYSDATE                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      AS LASTET_DATO,
          MOTTAKER.BEHANDLINGSTEMA,
          MOTTAKER.MAX_TRANS_ID,
          MOTTAKER.FK_PERSON1_MOTTAKER,
          MOTTAKER.KJONN,
          MOTTAKER.FK_PERSON1_BARN,
          MOTTAKER.TERMINDATO,
          MOTTAKER.FOEDSELSDATO,
          MOTTAKER.ANTALL_BARN_TERMIN,
          MOTTAKER.ANTALL_BARN_FOEDSEL,
          MOTTAKER.FOEDSELSDATO_ADOPSJON,
          MOTTAKER.ANTALL_BARN_ADOPSJON,
          MOTTAKER.MOTTAKER_FODSELS_AAR,
          MOTTAKER.MOTTAKER_FODSELS_MND,
          SUBSTR(P_TID_FOM, 1, 4) - MOTTAKER.MOTTAKER_FODSELS_AAR                                                                                                                                                                                                                                                                                                                                                                                                                                                      AS MOTTAKER_ALDER,
          MOTTAKER.SIVILSTAND,
          MOTTAKER.STATSBORGERSKAP,
          DIM_PERSON.PK_DIM_PERSON,
          DIM_PERSON.BOSTED_KOMMUNE_NR,
          DIM_PERSON.FK_DIM_SIVILSTATUS,
          DIM_GEOGRAFI.PK_DIM_GEOGRAFI,
          DIM_GEOGRAFI.BYDEL_KOMMUNE_NR,
          DIM_GEOGRAFI.KOMMUNE_NR,
          DIM_GEOGRAFI.KOMMUNE_NAVN,
          DIM_GEOGRAFI.BYDEL_NR,
          DIM_GEOGRAFI.BYDEL_NAVN,
          ANNENFORELDERFAGSAK.ANNENFORELDERFAGSAK_ID,
          ANNENFORELDERFAGSAK.FK_PERSON1_ANNEN_PART,
          FAM_FP_UTTAK_FP_KONTOER.MAX_DAGER                                                                                                                                                                                                                                                                                                                                                                                                                                                                            MAX_STONADSDAGER_KONTO,
          CASE
            WHEN ALENEOMSORG.FAGSAK_ID IS NOT NULL THEN
              'J'
            ELSE
              NULL
          END AS                                                                                                                                                                                                                                                                                                                                                                                                                                           ALENEOMSORG,
          CASE
            WHEN BEHANDLINGSTEMA = 'FORP_FODS' THEN
              '214'
            WHEN BEHANDLINGSTEMA = 'FORP_ADOP' THEN
              '216'
          END AS                                                                                                                                                                                                                                                                                                                                                                                                         HOVEDKONTONR,
          CASE
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='ARBEIDSTAKER' AND DAGSATS_ARBEIDSGIVER/DAGSATS*100<=50 THEN
              '1000'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='ARBEIDSTAKER' AND DAGSATS_ARBEIDSGIVER/DAGSATS*100>50 THEN
              '8020'
 --when status_og_andel_inntektskat='ARBEIDSTAKER' then '1000'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='JORDBRUKER' THEN
              '5210'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='SJØMANN' THEN
              '1300'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='SELVSTENDIG_NÆRINGSDRIVENDE' THEN
              '5010'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='DAGPENGER' THEN
              '1200'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='ARBEIDSTAKER_UTEN_FERIEPENGER' THEN
              '1000'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='FISKER' THEN
              '5300'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='DAGMAMMA' THEN
              '5110'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='FRILANSER' THEN
              '1100'
          END AS UNDERKONTONR,
          CASE
            WHEN RETT_TIL_MØDREKVOTE.TRANS_ID IS NULL THEN
              'N'
            ELSE
              'J'
          END AS                                                                                                                                                                                                                                                                                                                                                                                                                                         RETT_TIL_MØDREKVOTE,
          CASE
            WHEN RETT_TIL_FEDREKVOTE.TRANS_ID IS NULL THEN
              'N'
            ELSE
              'J'
          END AS                                                                                                                                                                                                                                                                                                                                                                                                                                         RETT_TIL_FEDREKVOTE,
          FLERBARNSDAGER.FLERBARNSDAGER,
          ROUND(DAGSATS_ARBEIDSGIVER/DAGSATS*100, 0)                                                                                                                                                                                                                                                                                                                                                                                                                                                                   AS ANDEL_AV_REFUSJON,
          ADOPSJON.ADOPSJONSDATO,
          ADOPSJON.STEBARNSADOPSJON,
          EOS.EOS_SAK
        FROM
          BEREGNINGSGRUNNLAG_AGG
          LEFT JOIN MOTTAKER
          ON BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID = MOTTAKER.FAGSAK_ID
          AND BEREGNINGSGRUNNLAG_AGG.TRANS_ID = MOTTAKER.MAX_TRANS_ID
          LEFT JOIN ANNENFORELDERFAGSAK
          ON BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID = ANNENFORELDERFAGSAK.FAGSAK_ID
          AND BEREGNINGSGRUNNLAG_AGG.TRANS_ID = ANNENFORELDERFAGSAK.MAX_TRANS_ID
          LEFT JOIN DVH_FAM_FP.FAM_FP_UTTAK_FP_KONTOER
          ON BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID = FAM_FP_UTTAK_FP_KONTOER.FAGSAK_ID
          AND MOTTAKER.MAX_TRANS_ID = FAM_FP_UTTAK_FP_KONTOER.TRANS_ID
 --AND UPPER(REGEXP_REPLACE(grunnlag_drp1.TREKKONTO, '_|-|[[:space:]]', '')) = UPPER(REGEXP_REPLACE(FAM_FP_Uttak_FP_Kontoer.STOENADSKONTOTYPE, '_|-|[[:space:]]', ''))
          AND UPPER(REPLACE(BEREGNINGSGRUNNLAG_AGG.TREKKONTO,
          '_',
          '')) = UPPER(REPLACE(FAM_FP_UTTAK_FP_KONTOER.STOENADSKONTOTYPE,
          ' ',
          ''))
          LEFT JOIN DT_PERSON.DIM_PERSON
          ON MOTTAKER.FK_PERSON1_MOTTAKER = DIM_PERSON.FK_PERSON1
 --and beregningsgrunnlag_agg.uttak_tom between dim_person.gyldig_fra_dato and dim_person.gyldig_til_dato
          AND TO_DATE(BEREGNINGSGRUNNLAG_AGG.PK_DIM_TID_DATO_UTBET_TOM,
          'yyyymmdd') BETWEEN DIM_PERSON.GYLDIG_FRA_DATO
          AND DIM_PERSON.GYLDIG_TIL_DATO
          LEFT JOIN DT_KODEVERK.DIM_GEOGRAFI
          ON DIM_PERSON.FK_DIM_GEOGRAFI_BOSTED = DIM_GEOGRAFI.PK_DIM_GEOGRAFI
          LEFT JOIN ALENEOMSORG
          ON ALENEOMSORG.FAGSAK_ID = BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID
          AND ALENEOMSORG.UTTAK_FOM = BEREGNINGSGRUNNLAG_AGG.UTTAK_FOM
          LEFT JOIN (
            SELECT
              TRANS_ID
            FROM
              DVH_FAM_FP.FAM_FP_UTTAK_FP_KONTOER
            WHERE
              UPPER(STOENADSKONTOTYPE) = 'MØDREKVOTE'
            GROUP BY
              TRANS_ID
          ) RETT_TIL_MØDREKVOTE
          ON RETT_TIL_MØDREKVOTE.TRANS_ID = BEREGNINGSGRUNNLAG_AGG.TRANS_ID
          LEFT JOIN (
            SELECT
              TRANS_ID
            FROM
              DVH_FAM_FP.FAM_FP_UTTAK_FP_KONTOER
            WHERE
              UPPER(STOENADSKONTOTYPE) = 'FEDREKVOTE'
            GROUP BY
              TRANS_ID
          ) RETT_TIL_FEDREKVOTE
          ON RETT_TIL_FEDREKVOTE.TRANS_ID = BEREGNINGSGRUNNLAG_AGG.TRANS_ID
          LEFT JOIN (
            SELECT
              TRANS_ID,
              MAX(MAX_DAGER) AS FLERBARNSDAGER
            FROM
              DVH_FAM_FP.FAM_FP_UTTAK_FP_KONTOER
            WHERE
              UPPER(STOENADSKONTOTYPE) = 'FLERBARNSDAGER'
            GROUP BY
              TRANS_ID
          ) FLERBARNSDAGER
          ON FLERBARNSDAGER.TRANS_ID = BEREGNINGSGRUNNLAG_AGG.TRANS_ID
          LEFT JOIN ADOPSJON
          ON BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID = ADOPSJON.FAGSAK_ID
          LEFT JOIN EOS
          ON BEREGNINGSGRUNNLAG_AGG.TRANS_ID = EOS.TRANS_ID
      )
      SELECT /*+ PARALLEL(8) */
        *
 --from uttak_dager
      FROM
        GRUNNLAG
      ORDER BY
        FAGSAK_ID
 --where fagsak_id in (1035184)
;
    V_TID_FOM                      VARCHAR2(8) := NULL;
    V_TID_TOM                      VARCHAR2(8) := NULL;
    V_COMMIT                       NUMBER := 0;
    V_ERROR_MELDING                VARCHAR2(1000) := NULL;
    V_DIM_TID_ANTALL               NUMBER := 0;
    V_UTBETALINGSPROSENT_KALKULERT NUMBER := 0;
    V_BUDSJETT                     VARCHAR2(5);
  BEGIN
    V_TID_FOM := P_IN_VEDTAK_TOM
                 || '01';
    V_TID_TOM := TO_CHAR(LAST_DAY(TO_DATE(P_IN_VEDTAK_TOM, 'yyyymm')), 'yyyymmdd');
    IF TO_DATE(P_IN_VEDTAK_TOM, 'yyyymm') <= TO_DATE(P_IN_RAPPORT_DATO, 'yyyymm') THEN
      V_BUDSJETT := 'A';
    ELSE
      V_BUDSJETT := 'B';
    END IF;
 --dbms_output.put_line(v_tid_fom||v_tid_tom);--TEST!!!
    FOR REC_PERIODE IN CUR_PERIODE(P_IN_RAPPORT_DATO, P_IN_FORSKYVNINGER, V_TID_FOM, V_TID_TOM, V_BUDSJETT) LOOP
      V_DIM_TID_ANTALL := 0;
      V_UTBETALINGSPROSENT_KALKULERT := 0;
      V_DIM_TID_ANTALL := DIM_TID_ANTALL(TO_NUMBER(TO_CHAR(REC_PERIODE.UTTAK_FOM, 'yyyymmdd')), TO_NUMBER(TO_CHAR(REC_PERIODE.UTTAK_TOM, 'yyyymmdd')));
      IF V_DIM_TID_ANTALL != 0 THEN
        V_UTBETALINGSPROSENT_KALKULERT := ROUND(REC_PERIODE.TREKKDAGER/V_DIM_TID_ANTALL*100, 2);
      ELSE
        V_UTBETALINGSPROSENT_KALKULERT := 0;
      END IF;
      BEGIN
        INSERT INTO DVH_FAM_FP.FAK_FAM_FP_VEDTAK_UTBETALING (
          FAGSAK_ID,
          TRANS_ID,
          BEHANDLINGSTEMA,
          TREKKONTO,
          STONADSDAGER_KVOTE,
          UTTAK_ARBEID_TYPE,
          AAR,
          HALVAAR,
          KVARTAL,
          AAR_MAANED,
          RAPPORT_PERIODE,
          UTTAK_FOM,
          UTTAK_TOM,
          DAGER_ERST,
          BEREGNINGSGRUNNLAG_FOM,
          BEREGNINGSGRUNNLAG_TOM,
          DEKNINGSGRAD,
          DAGSATS_BRUKER,
          DAGSATS_ARBEIDSGIVER,
          VIRKSOMHET,
          PERIODE_RESULTAT_AARSAK,
          DAGSATS,
          GRADERINGSPROSENT,
          STATUS_OG_ANDEL_INNTEKTSKAT,
          AKTIVITET_STATUS,
          BRUTTO_INNTEKT,
          AVKORTET_INNTEKT,
          STATUS_OG_ANDEL_BRUTTO,
          STATUS_OG_ANDEL_AVKORTET,
          UTBETALINGSPROSENT,
          FK_DIM_TID_DATO_UTBET_FOM,
          FK_DIM_TID_DATO_UTBET_TOM,
          FUNKSJONELL_TID,
          FORSTE_VEDTAKSDATO,
          VEDTAKSDATO,
          MAX_VEDTAKSDATO,
          PERIODE_TYPE,
          TILFELLE_ERST,
          BELOP,
          DAGSATS_REDUSERT,
          LASTET_DATO,
          MAX_TRANS_ID,
          FK_PERSON1_MOTTAKER,
          FK_PERSON1_ANNEN_PART,
          KJONN,
          FK_PERSON1_BARN,
          TERMINDATO,
          FOEDSELSDATO,
          ANTALL_BARN_TERMIN,
          ANTALL_BARN_FOEDSEL,
          FOEDSELSDATO_ADOPSJON,
          ANTALL_BARN_ADOPSJON,
          ANNENFORELDERFAGSAK_ID,
          MAX_STONADSDAGER_KONTO,
          FK_DIM_PERSON,
          BOSTED_KOMMUNE_NR,
          FK_DIM_GEOGRAFI,
          BYDEL_KOMMUNE_NR,
          KOMMUNE_NR,
          KOMMUNE_NAVN,
          BYDEL_NR,
          BYDEL_NAVN,
          ALENEOMSORG,
          HOVEDKONTONR,
          UNDERKONTONR,
          MOTTAKER_FODSELS_AAR,
          MOTTAKER_FODSELS_MND,
          MOTTAKER_ALDER,
          RETT_TIL_FEDREKVOTE,
          RETT_TIL_MODREKVOTE,
          DAGSATS_ERST,
          TREKKDAGER,
          SAMTIDIG_UTTAK,
          GRADERING,
          GRADERING_INNVILGET,
          ANTALL_DAGER_PERIODE,
          FLERBARNSDAGER,
          UTBETALINGSPROSENT_KALKULERT,
          MIN_UTTAK_FOM,
          MAX_UTTAK_TOM,
          FK_FAM_FP_TREKKONTO,
          FK_FAM_FP_PERIODE_RESULTAT_AARSAK,
          SIVILSTATUS,
          FK_DIM_SIVILSTATUS,
          ANTALL_BEREGNINGSGRUNNLAG,
          GRADERINGSDAGER,
          FK_DIM_TID_MIN_DATO_KVOTE,
          FK_DIM_TID_MAX_DATO_KVOTE,
          ADOPSJONSDATO,
          STEBARNSADOPSJON,
          EOS_SAK,
          MOR_RETTIGHET,
          STATSBORGERSKAP,
          ARBEIDSTIDSPROSENT,
          MORS_AKTIVITET,
          GYLDIG_FLAGG,
          ANDEL_AV_REFUSJON,
          FORSTE_SOKNADSDATO,
          SOKNADSDATO,
          BUDSJETT
        ) VALUES (
          REC_PERIODE.FAGSAK_ID,
          REC_PERIODE.TRANS_ID,
          REC_PERIODE.BEHANDLINGSTEMA,
          REC_PERIODE.TREKKONTO,
          REC_PERIODE.STONADSDAGER_KVOTE,
          REC_PERIODE.UTTAK_ARBEID_TYPE,
          REC_PERIODE.AAR,
          REC_PERIODE.HALVAAR,
          REC_PERIODE.KVARTAL,
          REC_PERIODE.AAR_MAANED,
          P_IN_RAPPORT_DATO,
          REC_PERIODE.UTTAK_FOM,
          REC_PERIODE.UTTAK_TOM,
          REC_PERIODE.DAGER_ERST,
          REC_PERIODE.BEREGNINGSGRUNNLAG_FOM,
          REC_PERIODE.BEREGNINGSGRUNNLAG_TOM,
          REC_PERIODE.DEKNINGSGRAD,
          REC_PERIODE.DAGSATS_BRUKER,
          REC_PERIODE.DAGSATS_ARBEIDSGIVER,
          REC_PERIODE.VIRKSOMHET,
          REC_PERIODE.PERIODE_RESULTAT_AARSAK,
          REC_PERIODE.DAGSATS,
          REC_PERIODE.GRADERINGSPROSENT,
          REC_PERIODE.STATUS_OG_ANDEL_INNTEKTSKAT,
          REC_PERIODE.AKTIVITET_STATUS,
          REC_PERIODE.BRUTTO_INNTEKT,
          REC_PERIODE.AVKORTET_INNTEKT,
          REC_PERIODE.STATUS_OG_ANDEL_BRUTTO,
          REC_PERIODE.STATUS_OG_ANDEL_AVKORTET,
          REC_PERIODE.UTBETALINGSPROSENT,
          REC_PERIODE.PK_DIM_TID_DATO_UTBET_FOM,
          REC_PERIODE.PK_DIM_TID_DATO_UTBET_TOM,
          REC_PERIODE.FUNKSJONELL_TID,
          REC_PERIODE.FORSTE_VEDTAKSDATO,
          REC_PERIODE.SISTE_VEDTAKSDATO,
          REC_PERIODE.MAX_VEDTAKSDATO,
          REC_PERIODE.PERIODE,
          REC_PERIODE.TILFELLE_ERST,
          REC_PERIODE.BELOP,
          REC_PERIODE.DAGSATS_REDUSERT,
          REC_PERIODE.LASTET_DATO,
          REC_PERIODE.MAX_TRANS_ID,
          REC_PERIODE.FK_PERSON1_MOTTAKER,
          REC_PERIODE.FK_PERSON1_ANNEN_PART,
          REC_PERIODE.KJONN,
          REC_PERIODE.FK_PERSON1_BARN,
          REC_PERIODE.TERMINDATO,
          REC_PERIODE.FOEDSELSDATO,
          REC_PERIODE.ANTALL_BARN_TERMIN,
          REC_PERIODE.ANTALL_BARN_FOEDSEL,
          REC_PERIODE.FOEDSELSDATO_ADOPSJON,
          REC_PERIODE.ANTALL_BARN_ADOPSJON,
          REC_PERIODE.ANNENFORELDERFAGSAK_ID,
          REC_PERIODE.MAX_STONADSDAGER_KONTO,
          REC_PERIODE.PK_DIM_PERSON,
          REC_PERIODE.BOSTED_KOMMUNE_NR,
          REC_PERIODE.PK_DIM_GEOGRAFI,
          REC_PERIODE.BYDEL_KOMMUNE_NR,
          REC_PERIODE.KOMMUNE_NR,
          REC_PERIODE.KOMMUNE_NAVN,
          REC_PERIODE.BYDEL_NR,
          REC_PERIODE.BYDEL_NAVN,
          REC_PERIODE.ALENEOMSORG,
          REC_PERIODE.HOVEDKONTONR,
          REC_PERIODE.UNDERKONTONR,
          REC_PERIODE.MOTTAKER_FODSELS_AAR,
          REC_PERIODE.MOTTAKER_FODSELS_MND,
          REC_PERIODE.MOTTAKER_ALDER,
          REC_PERIODE.RETT_TIL_FEDREKVOTE,
          REC_PERIODE.RETT_TIL_MØDREKVOTE,
          REC_PERIODE.DAGSATS_ERST,
          REC_PERIODE.TREKKDAGER,
          REC_PERIODE.SAMTIDIG_UTTAK,
          REC_PERIODE.GRADERING,
          REC_PERIODE.GRADERING_INNVILGET,
          V_DIM_TID_ANTALL,
          REC_PERIODE.FLERBARNSDAGER,
          V_UTBETALINGSPROSENT_KALKULERT,
          REC_PERIODE.MIN_UTTAK_FOM,
          REC_PERIODE.MAX_UTTAK_TOM,
          REC_PERIODE.PK_FAM_FP_TREKKONTO,
          REC_PERIODE.PK_FAM_FP_PERIODE_RESULTAT_AARSAK,
          REC_PERIODE.SIVILSTAND,
          REC_PERIODE.FK_DIM_SIVILSTATUS,
          REC_PERIODE.ANTALL_BEREGNINGSGRUNNLAG,
          REC_PERIODE.GRADERINGSDAGER,
          REC_PERIODE.FK_DIM_TID_MIN_DATO_KVOTE,
          REC_PERIODE.FK_DIM_TID_MAX_DATO_KVOTE,
          REC_PERIODE.ADOPSJONSDATO,
          REC_PERIODE.STEBARNSADOPSJON,
          REC_PERIODE.EOS_SAK,
          REC_PERIODE.MOR_RETTIGHET,
          REC_PERIODE.STATSBORGERSKAP,
          REC_PERIODE.ARBEIDSTIDSPROSENT,
          REC_PERIODE.MORS_AKTIVITET,
          P_IN_GYLDIG_FLAGG,
          REC_PERIODE.ANDEL_AV_REFUSJON,
          REC_PERIODE.FORSTE_SOKNADSDATO,
          REC_PERIODE.SOKNADSDATO,
          V_BUDSJETT
        );
        V_COMMIT := V_COMMIT + 1;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK;
          V_ERROR_MELDING := SUBSTR(SQLCODE
                                    || ' '
                                    || SQLERRM, 1, 1000);
          INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
            MIN_LASTET_DATO,
            ID,
            ERROR_MSG,
            OPPRETTET_TID,
            KILDE
          ) VALUES(
            NULL,
            REC_PERIODE.FAGSAK_ID,
            V_ERROR_MELDING,
            SYSDATE,
            'FAM_FP_STATISTIKK_MAANED:INSERT'
          );
          COMMIT;
          P_OUT_ERROR := SUBSTR(P_OUT_ERROR
                                || V_ERROR_MELDING, 1, 1000);
      END;
      IF V_COMMIT > 100000 THEN
        COMMIT;
        V_COMMIT := 0;
      END IF;
    END LOOP;
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_ERROR_MELDING := SUBSTR(SQLCODE
                                || ' '
                                || SQLERRM, 1, 1000);
      INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
        MIN_LASTET_DATO,
        ID,
        ERROR_MSG,
        OPPRETTET_TID,
        KILDE
      ) VALUES(
        NULL,
        NULL,
        V_ERROR_MELDING,
        SYSDATE,
        'FAM_FP_STATISTIKK_MAANED'
      );
      COMMIT;
      P_OUT_ERROR := SUBSTR(P_OUT_ERROR
                            || V_ERROR_MELDING, 1, 1000);
  END FAM_FP_STATISTIKK_MAANED;

  PROCEDURE FAM_FP_STATISTIKK_KVARTAL(
    P_IN_VEDTAK_TOM IN VARCHAR2,
    P_IN_RAPPORT_DATO IN VARCHAR2,
    P_IN_FORSKYVNINGER IN NUMBER,
    P_IN_GYLDIG_FLAGG IN NUMBER DEFAULT 0,
    P_IN_PERIODE_TYPE IN VARCHAR2 DEFAULT 'K',
    P_OUT_ERROR OUT VARCHAR2
  ) AS
    CURSOR CUR_PERIODE(P_RAPPORT_DATO IN VARCHAR2, P_FORSKYVNINGER IN NUMBER, P_TID_FOM IN VARCHAR2, P_TID_TOM IN VARCHAR2) IS
      WITH FAGSAK AS (
        SELECT
          FAGSAK_ID,
          MAX(BEHANDLINGSTEMA)                                                   AS BEHANDLINGSTEMA,
          MAX(FAGSAKANNENFORELDER_ID)                                            AS ANNENFORELDERFAGSAK_ID,
          MAX(TRANS_ID) KEEP(DENSE_RANK FIRST ORDER BY FUNKSJONELL_TID DESC)     AS MAX_TRANS_ID,
          MAX(SOEKNADSDATO) KEEP(DENSE_RANK FIRST ORDER BY FUNKSJONELL_TID DESC) AS SOKNADSDATO,
          MIN(SOEKNADSDATO)                                                      AS FORSTE_SOKNADSDATO,
          MIN(VEDTAKSDATO)                                                       AS FORSTE_VEDTAKSDATO,
          MAX(FUNKSJONELL_TID)                                                   AS FUNKSJONELL_TID,
          MAX(VEDTAKSDATO)                                                       AS SISTE_VEDTAKSDATO,
          P_IN_PERIODE_TYPE                                                      AS PERIODE,
          LAST_DAY(TO_DATE(P_RAPPORT_DATO, 'yyyymm')) + P_FORSKYVNINGER          AS MAX_VEDTAKSDATO
        FROM
          DVH_FAM_FP.FAM_FP_FAGSAK
        WHERE
          FAM_FP_FAGSAK.FUNKSJONELL_TID <= LAST_DAY(TO_DATE(P_RAPPORT_DATO, 'yyyymm')) + P_FORSKYVNINGER
        GROUP BY
          FAGSAK_ID
      ), TERMIN AS (
        SELECT
          FAGSAK_ID,
          MAX(TERMINDATO)            TERMINDATO,
          MAX(FOEDSELSDATO)          FOEDSELSDATO,
          MAX(ANTALL_BARN_TERMIN)    ANTALL_BARN_TERMIN,
          MAX(ANTALL_BARN_FOEDSEL)   ANTALL_BARN_FOEDSEL,
          MAX(FOEDSELSDATO_ADOPSJON) FOEDSELSDATO_ADOPSJON,
          MAX(ANTALL_BARN_ADOPSJON) ANTALL_BARN_ADOPSJON
        FROM
          (
            SELECT
              FAM_FP_FAGSAK.FAGSAK_ID,
              MAX(FODSEL.TERMINDATO)              TERMINDATO,
              MAX(FODSEL.FOEDSELSDATO)            FOEDSELSDATO,
              MAX(FODSEL.ANTALL_BARN_FOEDSEL)     ANTALL_BARN_FOEDSEL,
              MAX(FODSEL.ANTALL_BARN_TERMIN)      ANTALL_BARN_TERMIN,
              MAX(ADOPSJON.FOEDSELSDATO_ADOPSJON) FOEDSELSDATO_ADOPSJON,
              COUNT(ADOPSJON.TRANS_ID)            ANTALL_BARN_ADOPSJON
            FROM
              DVH_FAM_FP.FAM_FP_FAGSAK
              LEFT JOIN DVH_FAM_FP.FAM_FP_FODSELTERMIN FODSEL
              ON FODSEL.FAGSAK_ID = FAM_FP_FAGSAK.FAGSAK_ID
              AND UPPER(FAM_FP_FAGSAK.BEHANDLINGSTEMA) = 'FORP_FODS'
              LEFT JOIN DVH_FAM_FP.FAM_FP_FODSELTERMIN ADOPSJON
              ON ADOPSJON.FAGSAK_ID = FAM_FP_FAGSAK.FAGSAK_ID
              AND ADOPSJON.TRANS_ID = FAM_FP_FAGSAK.TRANS_ID
              AND UPPER(FAM_FP_FAGSAK.BEHANDLINGSTEMA) = 'FORP_ADOP'
            GROUP BY
              FAM_FP_FAGSAK.FAGSAK_ID,
              FAM_FP_FAGSAK.TRANS_ID
          )
        GROUP BY
          FAGSAK_ID
      ), FK_PERSON1 AS (
        SELECT
          PERSON.PERSON,
          PERSON.FAGSAK_ID,
          MAX(PERSON.BEHANDLINGSTEMA)                                                                             AS BEHANDLINGSTEMA,
          PERSON.MAX_TRANS_ID,
          MAX(PERSON.ANNENFORELDERFAGSAK_ID)                                                                      AS ANNENFORELDERFAGSAK_ID,
          PERSON.AKTOER_ID,
          MAX(PERSON.KJONN)                                                                                       AS KJONN,
          MAX(PERSON_67_VASKET.FK_PERSON1) KEEP (DENSE_RANK FIRST ORDER BY PERSON_67_VASKET.GYLDIG_FRA_DATO DESC) AS FK_PERSON1,
          MAX(FOEDSELSDATO)                                                                                       AS FOEDSELSDATO,
          MAX(SIVILSTAND)                                                                                         AS SIVILSTAND,
          MAX(STATSBORGERSKAP)                                                                                    AS STATSBORGERSKAP
        FROM
          (
            SELECT
              'MOTTAKER'                                AS PERSON,
              FAGSAK.FAGSAK_ID,
              FAGSAK.BEHANDLINGSTEMA,
              FAGSAK.MAX_TRANS_ID,
              FAGSAK.ANNENFORELDERFAGSAK_ID,
              FAM_FP_PERSONOPPLYSNINGER.AKTOER_ID,
              FAM_FP_PERSONOPPLYSNINGER.KJONN,
              FAM_FP_PERSONOPPLYSNINGER.FOEDSELSDATO,
              FAM_FP_PERSONOPPLYSNINGER.SIVILSTAND,
              FAM_FP_PERSONOPPLYSNINGER.STATSBORGERSKAP
            FROM
              DVH_FAM_FP.FAM_FP_PERSONOPPLYSNINGER
              JOIN FAGSAK
              ON FAM_FP_PERSONOPPLYSNINGER.TRANS_ID = FAGSAK.MAX_TRANS_ID UNION ALL
              SELECT
                'BARN'                                    AS PERSON,
                FAGSAK.FAGSAK_ID,
                MAX(FAGSAK.BEHANDLINGSTEMA)               AS BEHANDLINGSTEMA,
                FAGSAK.MAX_TRANS_ID,
                MAX(FAGSAK.ANNENFORELDERFAGSAK_ID)        ANNENFORELDERFAGSAK_ID,
                MAX(FAM_FP_FAMILIEHENDELSE.TIL_AKTOER_ID) AS AKTOER_ID,
                MAX(FAM_FP_FAMILIEHENDELSE.KJOENN)        AS KJONN,
                NULL                                      AS FOEDSELSDATO,
                NULL                                      AS SIVILSTAND,
                NULL                                      AS STATSBORGERSKAP
              FROM
                DVH_FAM_FP.FAM_FP_FAMILIEHENDELSE
                JOIN FAGSAK
                ON FAM_FP_FAMILIEHENDELSE.FAGSAK_ID = FAGSAK.FAGSAK_ID
              WHERE
                UPPER(FAM_FP_FAMILIEHENDELSE.RELASJON) = 'BARN'
              GROUP BY
                FAGSAK.FAGSAK_ID, FAGSAK.MAX_TRANS_ID
          )                                    PERSON
          JOIN DT_PERSON.DVH_PERSON_IDENT_AKTOR_IKKE_SKJERMET PERSON_67_VASKET
          ON PERSON_67_VASKET.AKTOR_ID = PERSON.AKTOER_ID
          AND TO_DATE(P_TID_TOM, 'yyyymmdd') BETWEEN PERSON_67_VASKET.GYLDIG_FRA_DATO
          AND PERSON_67_VASKET.GYLDIG_TIL_DATO
        GROUP BY
          PERSON.PERSON, PERSON.FAGSAK_ID, PERSON.MAX_TRANS_ID, PERSON.AKTOER_ID
      ), BARN AS (
        SELECT
          FAGSAK_ID,
          LISTAGG(FK_PERSON1, ',') WITHIN GROUP (ORDER BY FK_PERSON1) AS FK_PERSON1_BARN
        FROM
          FK_PERSON1
        WHERE
          PERSON = 'BARN'
        GROUP BY
          FAGSAK_ID
      ), MOTTAKER AS (
        SELECT
          FK_PERSON1.FAGSAK_ID,
          FK_PERSON1.BEHANDLINGSTEMA,
          FK_PERSON1.MAX_TRANS_ID,
          FK_PERSON1.ANNENFORELDERFAGSAK_ID,
          FK_PERSON1.AKTOER_ID,
          FK_PERSON1.KJONN,
          FK_PERSON1.FK_PERSON1                       AS FK_PERSON1_MOTTAKER,
          EXTRACT(YEAR FROM FK_PERSON1.FOEDSELSDATO) AS MOTTAKER_FODSELS_AAR,
          EXTRACT(MONTH FROM FK_PERSON1.FOEDSELSDATO) AS MOTTAKER_FODSELS_MND,
          FK_PERSON1.SIVILSTAND,
          FK_PERSON1.STATSBORGERSKAP,
          BARN.FK_PERSON1_BARN,
          TERMIN.TERMINDATO,
          TERMIN.FOEDSELSDATO,
          TERMIN.ANTALL_BARN_TERMIN,
          TERMIN.ANTALL_BARN_FOEDSEL,
          TERMIN.FOEDSELSDATO_ADOPSJON,
          TERMIN.ANTALL_BARN_ADOPSJON
        FROM
          FK_PERSON1
          LEFT JOIN BARN
          ON BARN.FAGSAK_ID = FK_PERSON1.FAGSAK_ID
          LEFT JOIN TERMIN
          ON FK_PERSON1.FAGSAK_ID = TERMIN.FAGSAK_ID
        WHERE
          FK_PERSON1.PERSON = 'MOTTAKER'
      ), ADOPSJON AS (
        SELECT
          FAM_FP_VILKAAR.FAGSAK_ID,
          MAX(FAM_FP_VILKAAR.OMSORGS_OVERTAKELSESDATO) AS ADOPSJONSDATO,
          MAX(FAM_FP_VILKAAR.EKTEFELLES_BARN)          AS STEBARNSADOPSJON
        FROM
          FAGSAK
          JOIN DVH_FAM_FP.FAM_FP_VILKAAR
          ON FAGSAK.FAGSAK_ID = FAM_FP_VILKAAR.FAGSAK_ID
        WHERE
          FAGSAK.BEHANDLINGSTEMA = 'FORP_ADOP'
        GROUP BY
          FAM_FP_VILKAAR.FAGSAK_ID
      ), EOS AS (
        SELECT
          A.TRANS_ID,
          CASE
            WHEN UPPER(ER_BORGER_AV_EU_EOS) = 'TRUE' THEN
              'J'
            WHEN UPPER(ER_BORGER_AV_EU_EOS) = 'FALSE' THEN
              'N'
            ELSE
              NULL
          END EOS_SAK
        FROM
          (
            SELECT
              FAM_FP_VILKAAR.TRANS_ID,
              MAX(FAM_FP_VILKAAR.ER_BORGER_AV_EU_EOS) AS ER_BORGER_AV_EU_EOS
            FROM
              FAGSAK
              JOIN DVH_FAM_FP.FAM_FP_VILKAAR
              ON FAGSAK.MAX_TRANS_ID = FAM_FP_VILKAAR.TRANS_ID
              AND LENGTH(FAM_FP_VILKAAR.PERSON_STATUS) > 0
            GROUP BY
              FAM_FP_VILKAAR.TRANS_ID
          ) A
      ), ANNENFORELDERFAGSAK AS (
        SELECT
          ANNENFORELDERFAGSAK.*,
          MOTTAKER.FK_PERSON1_MOTTAKER AS FK_PERSON1_ANNEN_PART
        FROM
          (
            SELECT
              FAGSAK_ID,
              MAX_TRANS_ID,
              MAX(ANNENFORELDERFAGSAK_ID) AS ANNENFORELDERFAGSAK_ID
            FROM
              (
                SELECT
                  FORELDER1.FAGSAK_ID,
                  FORELDER1.MAX_TRANS_ID,
                  NVL(FORELDER1.ANNENFORELDERFAGSAK_ID, FORELDER2.FAGSAK_ID) AS ANNENFORELDERFAGSAK_ID
                FROM
                  MOTTAKER FORELDER1
                  JOIN MOTTAKER FORELDER2
                  ON FORELDER1.FK_PERSON1_BARN = FORELDER2.FK_PERSON1_BARN
                  AND FORELDER1.FK_PERSON1_MOTTAKER != FORELDER2.FK_PERSON1_MOTTAKER
              )
            GROUP BY
              FAGSAK_ID,
              MAX_TRANS_ID
          )        ANNENFORELDERFAGSAK
          JOIN MOTTAKER
          ON ANNENFORELDERFAGSAK.ANNENFORELDERFAGSAK_ID = MOTTAKER.FAGSAK_ID
      ), TID AS (
        SELECT
          PK_DIM_TID,
          DATO,
          AAR,
          HALVAAR,
          KVARTAL,
          AAR_MAANED
        FROM
          DT_KODEVERK.DIM_TID
        WHERE
          DAG_I_UKE < 6
          AND DIM_NIVAA = 1
          AND GYLDIG_FLAGG = 1
          AND PK_DIM_TID BETWEEN P_TID_FOM AND P_TID_TOM
          AND PK_DIM_TID <= TO_CHAR(LAST_DAY(TO_DATE(P_RAPPORT_DATO, 'yyyymm')), 'yyyymmdd')
      ), UTTAK AS (
        SELECT
          UTTAK.TRANS_ID,
          UTTAK.TREKKONTO,
          UTTAK.UTTAK_ARBEID_TYPE,
          UTTAK.VIRKSOMHET,
          UTTAK.UTBETALINGSPROSENT,
          UTTAK.GRADERING_INNVILGET,
          UTTAK.GRADERING,
          UTTAK.ARBEIDSTIDSPROSENT,
          UTTAK.SAMTIDIG_UTTAK,
          UTTAK.PERIODE_RESULTAT_AARSAK,
          UTTAK.FOM                                      AS UTTAK_FOM,
          UTTAK.TOM                                      AS UTTAK_TOM,
          UTTAK.TREKKDAGER,
          FAGSAK.FAGSAK_ID,
          FAGSAK.PERIODE,
          FAGSAK.FUNKSJONELL_TID,
          FAGSAK.FORSTE_VEDTAKSDATO,
          FAGSAK.SISTE_VEDTAKSDATO,
          FAGSAK.MAX_VEDTAKSDATO,
          FAGSAK.FORSTE_SOKNADSDATO,
          FAGSAK.SOKNADSDATO,
          FAM_FP_TREKKONTO.PK_FAM_FP_TREKKONTO,
          AARSAK_UTTAK.PK_FAM_FP_PERIODE_RESULTAT_AARSAK,
          UTTAK.ARBEIDSFORHOLD_ID,
          UTTAK.GRADERINGSDAGER,
          FAM_FP_UTTAK_FORDELINGSPER.MORS_AKTIVITET
        FROM
          DVH_FAM_FP.FAM_FP_UTTAK_RES_PER_AKTIV UTTAK
          JOIN FAGSAK
          ON FAGSAK.MAX_TRANS_ID = UTTAK.TRANS_ID LEFT JOIN DVH_FAM_FP.FAM_FP_TREKKONTO
          ON UPPER(UTTAK.TREKKONTO) = FAM_FP_TREKKONTO.TREKKONTO
          LEFT JOIN (
            SELECT
              AARSAK_UTTAK,
              MAX(PK_FAM_FP_PERIODE_RESULTAT_AARSAK) AS PK_FAM_FP_PERIODE_RESULTAT_AARSAK
            FROM
              DVH_FAM_FP.FAM_FP_PERIODE_RESULTAT_AARSAK
            GROUP BY
              AARSAK_UTTAK
          ) AARSAK_UTTAK
          ON UPPER(UTTAK.PERIODE_RESULTAT_AARSAK) = AARSAK_UTTAK.AARSAK_UTTAK
          LEFT JOIN DVH_FAM_FP.FAM_FP_UTTAK_FORDELINGSPER
          ON FAM_FP_UTTAK_FORDELINGSPER.TRANS_ID = UTTAK.TRANS_ID
          AND UTTAK.FOM BETWEEN FAM_FP_UTTAK_FORDELINGSPER.FOM
          AND FAM_FP_UTTAK_FORDELINGSPER.TOM
          AND UPPER(UTTAK.TREKKONTO) = UPPER(FAM_FP_UTTAK_FORDELINGSPER.PERIODE_TYPE)
          AND LENGTH(FAM_FP_UTTAK_FORDELINGSPER.MORS_AKTIVITET) > 1
        WHERE
          UTTAK.UTBETALINGSPROSENT > 0
      ), STONADSDAGER_KVOTE AS (
        SELECT
          UTTAK.*,
          TID1.PK_DIM_TID AS FK_DIM_TID_MIN_DATO_KVOTE,
          TID2.PK_DIM_TID AS FK_DIM_TID_MAX_DATO_KVOTE
        FROM
          (
            SELECT
              FAGSAK_ID,
              TRANS_ID,
              TREKKONTO,
              VIRKSOMHET,
              UTTAK_ARBEID_TYPE,
              SUM(TREKKDAGER)   AS STONADSDAGER_KVOTE,
              MIN(UTTAK_FOM)    AS MIN_UTTAK_FOM,
              MAX(UTTAK_TOM)    AS MAX_UTTAK_TOM
            FROM
              (
                SELECT
                  FAGSAK_ID,
                  TRANS_ID,
                  UTTAK_FOM,
                  UTTAK_TOM,
                  TREKKONTO,
                  VIRKSOMHET,
                  UTTAK_ARBEID_TYPE,
                  MAX(TREKKDAGER)   AS TREKKDAGER
                FROM
                  UTTAK
                GROUP BY
                  FAGSAK_ID,
                  TRANS_ID,
                  UTTAK_FOM,
                  UTTAK_TOM,
                  TREKKONTO,
                  VIRKSOMHET,
                  UTTAK_ARBEID_TYPE
              ) A
            GROUP BY
              FAGSAK_ID,
              TRANS_ID,
              TREKKONTO,
              VIRKSOMHET,
              UTTAK_ARBEID_TYPE
          )                   UTTAK
          JOIN DT_KODEVERK.DIM_TID TID1
          ON TID1.DIM_NIVAA = 1
          AND TID1.DATO = TRUNC(UTTAK.MIN_UTTAK_FOM, 'dd') JOIN DT_KODEVERK.DIM_TID TID2
          ON TID2.DIM_NIVAA = 1
          AND TID2.DATO = TRUNC(UTTAK.MAX_UTTAK_TOM,
          'dd')
      ), UTTAK_DAGER AS (
        SELECT
          UTTAK.*,
          TID.PK_DIM_TID,
          TID.DATO,
          TID.AAR,
          TID.HALVAAR,
          TID.KVARTAL,
          TID.AAR_MAANED
        FROM
          UTTAK
          JOIN TID
          ON TID.DATO BETWEEN UTTAK.UTTAK_FOM
          AND UTTAK.UTTAK_TOM
      ), ALENEOMSORG AS (
        SELECT
          UTTAK.FAGSAK_ID,
          UTTAK.UTTAK_FOM
        FROM
          UTTAK
          JOIN DVH_FAM_FP.FAM_FP_DOKUMENTASJONSPERIODER DOK1
          ON DOK1.FAGSAK_ID = UTTAK.FAGSAK_ID
          AND UTTAK.UTTAK_FOM >= DOK1.FOM
          AND DOK1.DOKUMENTASJON_TYPE IN ('ALENEOMSORG', 'ALENEOMSORG_OVERFØRING') LEFT JOIN DVH_FAM_FP.FAM_FP_DOKUMENTASJONSPERIODER DOK2
          ON DOK1.FAGSAK_ID = DOK2.FAGSAK_ID
          AND UTTAK.UTTAK_FOM >= DOK2.FOM
          AND DOK1.TRANS_ID < DOK2.TRANS_ID
          AND DOK2.DOKUMENTASJON_TYPE = 'ANNEN_FORELDER_HAR_RETT'
          AND DOK2.FAGSAK_ID IS NULL
        GROUP BY
          UTTAK.FAGSAK_ID,
          UTTAK.UTTAK_FOM
      ), BEREGNINGSGRUNNLAG AS (
        SELECT
          FAGSAK_ID,
          TRANS_ID,
          VIRKSOMHETSNUMMER,
          MAX(STATUS_OG_ANDEL_BRUTTO)         AS STATUS_OG_ANDEL_BRUTTO,
          MAX(STATUS_OG_ANDEL_AVKORTET)       AS STATUS_OG_ANDEL_AVKORTET,
          FOM                                 AS BEREGNINGSGRUNNLAG_FOM,
          TOM                                 AS BEREGNINGSGRUNNLAG_TOM,
          MAX(DEKNINGSGRAD)                   AS DEKNINGSGRAD,
          MAX(DAGSATS)                        AS DAGSATS,
          DAGSATS_BRUKER,
          DAGSATS_ARBEIDSGIVER,
          DAGSATS_BRUKER+DAGSATS_ARBEIDSGIVER DAGSATS_VIRKSOMHET,
          MAX(STATUS_OG_ANDEL_INNTEKTSKAT)    AS STATUS_OG_ANDEL_INNTEKTSKAT,
          AKTIVITET_STATUS,
          MAX(BRUTTO)                         AS BRUTTO_INNTEKT,
          MAX(AVKORTET)                       AS AVKORTET_INNTEKT,
          COUNT(1)                            AS ANTALL_BEREGNINGSGRUNNLAG
        FROM
          DVH_FAM_FP.FAM_FP_BEREGNINGSGRUNNLAG
        GROUP BY
          FAGSAK_ID,
          TRANS_ID,
          VIRKSOMHETSNUMMER,
          FOM,
          TOM,
          AKTIVITET_STATUS,
          DAGSATS_BRUKER,
          DAGSATS_ARBEIDSGIVER
      ), BEREGNINGSGRUNNLAG_DETALJ AS (
        SELECT
          UTTAK_DAGER.*,
          STONADSDAGER_KVOTE.STONADSDAGER_KVOTE,
          STONADSDAGER_KVOTE.MIN_UTTAK_FOM,
          STONADSDAGER_KVOTE.MAX_UTTAK_TOM,
          STONADSDAGER_KVOTE.FK_DIM_TID_MIN_DATO_KVOTE,
          STONADSDAGER_KVOTE.FK_DIM_TID_MAX_DATO_KVOTE,
          BEREG.STATUS_OG_ANDEL_BRUTTO,
          BEREG.STATUS_OG_ANDEL_AVKORTET,
          BEREG.BEREGNINGSGRUNNLAG_FOM,
          BEREG.DEKNINGSGRAD,
          BEREG.BEREGNINGSGRUNNLAG_TOM,
          BEREG.DAGSATS,
          BEREG.DAGSATS_BRUKER,
          BEREG.DAGSATS_ARBEIDSGIVER,
          BEREG.DAGSATS_VIRKSOMHET,
          BEREG.STATUS_OG_ANDEL_INNTEKTSKAT,
          BEREG.AKTIVITET_STATUS,
          BEREG.BRUTTO_INNTEKT,
          BEREG.AVKORTET_INNTEKT,
          BEREG.DAGSATS*UTTAK_DAGER.UTBETALINGSPROSENT/100 AS DAGSATS_ERST,
          BEREG.ANTALL_BEREGNINGSGRUNNLAG
        FROM
          BEREGNINGSGRUNNLAG                        BEREG
          JOIN UTTAK_DAGER
          ON UTTAK_DAGER.TRANS_ID = BEREG.TRANS_ID
          AND NVL(UTTAK_DAGER.VIRKSOMHET, 'X') = NVL(BEREG.VIRKSOMHETSNUMMER, 'X')
          AND BEREG.BEREGNINGSGRUNNLAG_FOM <= UTTAK_DAGER.DATO
          AND NVL(BEREG.BEREGNINGSGRUNNLAG_TOM, TO_DATE('20991201', 'YYYYMMDD')) >= UTTAK_DAGER.DATO LEFT JOIN STONADSDAGER_KVOTE
          ON UTTAK_DAGER.TRANS_ID = STONADSDAGER_KVOTE.TRANS_ID
          AND UTTAK_DAGER.TREKKONTO = STONADSDAGER_KVOTE.TREKKONTO
          AND NVL(UTTAK_DAGER.VIRKSOMHET,
          'X') = NVL(STONADSDAGER_KVOTE.VIRKSOMHET,
          'X')
          AND UTTAK_DAGER.UTTAK_ARBEID_TYPE = STONADSDAGER_KVOTE.UTTAK_ARBEID_TYPE
          JOIN DVH_FAM_FP.FAM_FP_UTTAK_AKTIVITET_MAPPING UTTAK_MAPPING
          ON UTTAK_DAGER.UTTAK_ARBEID_TYPE = UTTAK_MAPPING.UTTAK_ARBEID
          AND BEREG.AKTIVITET_STATUS = UTTAK_MAPPING.AKTIVITET_STATUS
        WHERE
          BEREG.DAGSATS_BRUKER + BEREG.DAGSATS_ARBEIDSGIVER != 0
      ), BEREGNINGSGRUNNLAG_AGG AS (
        SELECT
          A.*,
          DAGER_ERST*DAGSATS_VIRKSOMHET/DAGSATS*ANTALL_BEREGNINGSGRUNNLAG                                                                                                                    TILFELLE_ERST,
          DAGER_ERST*ROUND(UTBETALINGSPROSENT/100*DAGSATS_VIRKSOMHET)                                                                                                                        BELOP,
          ROUND(UTBETALINGSPROSENT/100*DAGSATS_VIRKSOMHET-0.5)                                                                                                                               DAGSATS_REDUSERT,
          CASE
            WHEN PERIODE_RESULTAT_AARSAK IN (2004, 2033) THEN
              'N'
            WHEN TREKKONTO IN ('FEDREKVOTE', 'FELLESPERIODE', 'MØDREKVOTE') THEN
              'J'
            WHEN TREKKONTO = 'FORELDREPENGER' THEN
              'N'
          END MOR_RETTIGHET
        FROM
          (
            SELECT
              FAGSAK_ID,
              TRANS_ID,
              TREKKONTO,
              TREKKDAGER,
              STONADSDAGER_KVOTE,
              UTTAK_ARBEID_TYPE,
              AAR,
              HALVAAR,
              KVARTAL --, aar_maaned
,
              UTTAK_FOM,
              UTTAK_TOM,
              SUM(DAGSATS_VIRKSOMHET/DAGSATS*
                CASE
                  WHEN ((UPPER(GRADERING_INNVILGET) ='TRUE'
                  AND UPPER(GRADERING)='TRUE')
                  OR UPPER(SAMTIDIG_UTTAK)='TRUE') THEN
                    (100-ARBEIDSTIDSPROSENT)/100
                  ELSE
                    1.0
                END )                                DAGER_ERST2,
              MAX(ARBEIDSTIDSPROSENT)                AS ARBEIDSTIDSPROSENT,
              COUNT(DISTINCT PK_DIM_TID)             DAGER_ERST,
 --count(distinct pk_dim_tid)*dagsats_virksomhet/dagsats tilfelle_erst,
              MIN(BEREGNINGSGRUNNLAG_FOM)            BEREGNINGSGRUNNLAG_FOM,
              MAX(BEREGNINGSGRUNNLAG_TOM)            BEREGNINGSGRUNNLAG_TOM,
              DEKNINGSGRAD,
 --count(distinct pk_dim_tid)*
 --      round(utbetalingsprosent/100*dagsats_virksomhet-0.5) belop,
              DAGSATS_BRUKER,
              DAGSATS_ARBEIDSGIVER,
              DAGSATS_VIRKSOMHET,
              VIRKSOMHET,
              PERIODE_RESULTAT_AARSAK,
              DAGSATS,
              DAGSATS_ERST, --dagsats_virksomhet,
              UTBETALINGSPROSENT                     GRADERINGSPROSENT,
              STATUS_OG_ANDEL_INNTEKTSKAT,
              AKTIVITET_STATUS,
              BRUTTO_INNTEKT,
              AVKORTET_INNTEKT,
              STATUS_OG_ANDEL_BRUTTO,
              STATUS_OG_ANDEL_AVKORTET,
 --round(utbetalingsprosent/100*dagsats_virksomhet-0.5) dagsats_redusert,
              UTBETALINGSPROSENT,
              MIN(PK_DIM_TID)                        PK_DIM_TID_DATO_UTBET_FOM,
              MAX(PK_DIM_TID)                        PK_DIM_TID_DATO_UTBET_TOM,
              FUNKSJONELL_TID,
              FORSTE_VEDTAKSDATO,
              SISTE_VEDTAKSDATO,
              MAX_VEDTAKSDATO,
              PERIODE,
              MAX(FORSTE_SOKNADSDATO)                AS FORSTE_SOKNADSDATO,
              MAX(SOKNADSDATO)                       AS SOKNADSDATO,
              SAMTIDIG_UTTAK,
              GRADERING,
              GRADERING_INNVILGET,
              MIN_UTTAK_FOM,
              MAX_UTTAK_TOM,
              FK_DIM_TID_MIN_DATO_KVOTE,
              FK_DIM_TID_MAX_DATO_KVOTE,
              MAX(PK_FAM_FP_TREKKONTO)               AS PK_FAM_FP_TREKKONTO,
              MAX(PK_FAM_FP_PERIODE_RESULTAT_AARSAK) AS PK_FAM_FP_PERIODE_RESULTAT_AARSAK,
              ANTALL_BEREGNINGSGRUNNLAG,
              MAX(GRADERINGSDAGER)                   AS GRADERINGSDAGER,
              MAX(MORS_AKTIVITET)                    AS MORS_AKTIVITET
            FROM
              BEREGNINGSGRUNNLAG_DETALJ
            GROUP BY
              FAGSAK_ID,
              TRANS_ID,
              TREKKONTO,
              TREKKDAGER,
              STONADSDAGER_KVOTE,
              UTTAK_ARBEID_TYPE,
              AAR,
              HALVAAR,
              KVARTAL --, aar_maaned
,
              UTTAK_FOM,
              UTTAK_TOM,
              DEKNINGSGRAD,
              VIRKSOMHET,
              UTBETALINGSPROSENT,
              PERIODE_RESULTAT_AARSAK,
              DAGSATS,
              DAGSATS_ERST,
              DAGSATS_BRUKER,
              DAGSATS_ARBEIDSGIVER,
              DAGSATS_VIRKSOMHET,
              UTBETALINGSPROSENT,
              STATUS_OG_ANDEL_INNTEKTSKAT,
              AKTIVITET_STATUS,
              BRUTTO_INNTEKT,
              AVKORTET_INNTEKT,
              STATUS_OG_ANDEL_BRUTTO,
              STATUS_OG_ANDEL_AVKORTET,
              FUNKSJONELL_TID,
              FORSTE_VEDTAKSDATO,
              SISTE_VEDTAKSDATO,
              MAX_VEDTAKSDATO,
              PERIODE,
              SAMTIDIG_UTTAK,
              GRADERING,
              GRADERING_INNVILGET,
              MIN_UTTAK_FOM,
              MAX_UTTAK_TOM,
              FK_DIM_TID_MIN_DATO_KVOTE,
              FK_DIM_TID_MAX_DATO_KVOTE,
              ANTALL_BEREGNINGSGRUNNLAG
          ) A
      ), GRUNNLAG AS (
        SELECT
          BEREGNINGSGRUNNLAG_AGG.*,
          SYSDATE                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      AS LASTET_DATO,
          MOTTAKER.BEHANDLINGSTEMA,
          MOTTAKER.MAX_TRANS_ID,
          MOTTAKER.FK_PERSON1_MOTTAKER,
          MOTTAKER.KJONN,
          MOTTAKER.FK_PERSON1_BARN,
          MOTTAKER.TERMINDATO,
          MOTTAKER.FOEDSELSDATO,
          MOTTAKER.ANTALL_BARN_TERMIN,
          MOTTAKER.ANTALL_BARN_FOEDSEL,
          MOTTAKER.FOEDSELSDATO_ADOPSJON,
          MOTTAKER.ANTALL_BARN_ADOPSJON,
          MOTTAKER.MOTTAKER_FODSELS_AAR,
          MOTTAKER.MOTTAKER_FODSELS_MND,
          SUBSTR(P_TID_FOM, 1, 4) - MOTTAKER.MOTTAKER_FODSELS_AAR                                                                                                                                                                                                                                                                                                                                                                                                                                                      AS MOTTAKER_ALDER,
          MOTTAKER.SIVILSTAND,
          MOTTAKER.STATSBORGERSKAP,
          DIM_PERSON.PK_DIM_PERSON,
          DIM_PERSON.BOSTED_KOMMUNE_NR,
          DIM_PERSON.FK_DIM_SIVILSTATUS,
          DIM_GEOGRAFI.PK_DIM_GEOGRAFI,
          DIM_GEOGRAFI.BYDEL_KOMMUNE_NR,
          DIM_GEOGRAFI.KOMMUNE_NR,
          DIM_GEOGRAFI.KOMMUNE_NAVN,
          DIM_GEOGRAFI.BYDEL_NR,
          DIM_GEOGRAFI.BYDEL_NAVN,
          ANNENFORELDERFAGSAK.ANNENFORELDERFAGSAK_ID,
          ANNENFORELDERFAGSAK.FK_PERSON1_ANNEN_PART,
          FAM_FP_UTTAK_FP_KONTOER.MAX_DAGER                                                                                                                                                                                                                                                                                                                                                                                                                                                                            MAX_STONADSDAGER_KONTO,
          CASE
            WHEN ALENEOMSORG.FAGSAK_ID IS NOT NULL THEN
              'J'
            ELSE
              NULL
          END AS                                                                                                                                                                                                                                                                                                                                                                                                                                           ALENEOMSORG,
          CASE
            WHEN BEHANDLINGSTEMA = 'FORP_FODS' THEN
              '214'
            WHEN BEHANDLINGSTEMA = 'FORP_ADOP' THEN
              '216'
          END AS                                                                                                                                                                                                                                                                                                                                                                                                         HOVEDKONTONR,
          CASE
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='ARBEIDSTAKER' AND DAGSATS_ARBEIDSGIVER/DAGSATS*100<=50 THEN
              '1000'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='ARBEIDSTAKER' AND DAGSATS_ARBEIDSGIVER/DAGSATS*100>50 THEN
              '8020'
 --when status_og_andel_inntektskat='ARBEIDSTAKER' then '1000'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='JORDBRUKER' THEN
              '5210'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='SJØMANN' THEN
              '1300'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='SELVSTENDIG_NÆRINGSDRIVENDE' THEN
              '5010'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='DAGPENGER' THEN
              '1200'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='ARBEIDSTAKER_UTEN_FERIEPENGER' THEN
              '1000'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='FISKER' THEN
              '5300'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='DAGMAMMA' THEN
              '5110'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='FRILANSER' THEN
              '1100'
          END AS UNDERKONTONR,
          ROUND(DAGSATS_ARBEIDSGIVER/DAGSATS*100, 0)                                                                                                                                                                                                                                                                                                                                                                                                                                                                   AS ANDEL_AV_REFUSJON,
          CASE
            WHEN RETT_TIL_MØDREKVOTE.TRANS_ID IS NULL THEN
              'N'
            ELSE
              'J'
          END AS                                                                                                                                                                                                                                                                                                                                                                                                                                         RETT_TIL_MØDREKVOTE,
          CASE
            WHEN RETT_TIL_FEDREKVOTE.TRANS_ID IS NULL THEN
              'N'
            ELSE
              'J'
          END AS                                                                                                                                                                                                                                                                                                                                                                                                                                         RETT_TIL_FEDREKVOTE,
          FLERBARNSDAGER.FLERBARNSDAGER,
          ADOPSJON.ADOPSJONSDATO,
          ADOPSJON.STEBARNSADOPSJON,
          EOS.EOS_SAK
        FROM
          BEREGNINGSGRUNNLAG_AGG
          LEFT JOIN MOTTAKER
          ON BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID = MOTTAKER.FAGSAK_ID
          AND BEREGNINGSGRUNNLAG_AGG.TRANS_ID = MOTTAKER.MAX_TRANS_ID
          LEFT JOIN ANNENFORELDERFAGSAK
          ON BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID = ANNENFORELDERFAGSAK.FAGSAK_ID
          AND BEREGNINGSGRUNNLAG_AGG.TRANS_ID = ANNENFORELDERFAGSAK.MAX_TRANS_ID
          LEFT JOIN DVH_FAM_FP.FAM_FP_UTTAK_FP_KONTOER
          ON BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID = FAM_FP_UTTAK_FP_KONTOER.FAGSAK_ID
          AND MOTTAKER.MAX_TRANS_ID = FAM_FP_UTTAK_FP_KONTOER.TRANS_ID
 --AND UPPER(REGEXP_REPLACE(grunnlag_drp1.TREKKONTO, '_|-|[[:space:]]', '')) = UPPER(REGEXP_REPLACE(FAM_FP_Uttak_FP_Kontoer.STOENADSKONTOTYPE, '_|-|[[:space:]]', ''))
          AND UPPER(REPLACE(BEREGNINGSGRUNNLAG_AGG.TREKKONTO,
          '_',
          '')) = UPPER(REPLACE(FAM_FP_UTTAK_FP_KONTOER.STOENADSKONTOTYPE,
          ' ',
          ''))
          LEFT JOIN DT_PERSON.DIM_PERSON
          ON MOTTAKER.FK_PERSON1_MOTTAKER = DIM_PERSON.FK_PERSON1
 --and beregningsgrunnlag_agg.uttak_tom between dim_person.gyldig_fra_dato and dim_person.gyldig_til_dato
          AND TO_DATE(BEREGNINGSGRUNNLAG_AGG.PK_DIM_TID_DATO_UTBET_TOM,
          'yyyymmdd') BETWEEN DIM_PERSON.GYLDIG_FRA_DATO
          AND DIM_PERSON.GYLDIG_TIL_DATO
          LEFT JOIN DT_KODEVERK.DIM_GEOGRAFI
          ON DIM_PERSON.FK_DIM_GEOGRAFI_BOSTED = DIM_GEOGRAFI.PK_DIM_GEOGRAFI
          LEFT JOIN ALENEOMSORG
          ON ALENEOMSORG.FAGSAK_ID = BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID
          AND ALENEOMSORG.UTTAK_FOM = BEREGNINGSGRUNNLAG_AGG.UTTAK_FOM
          LEFT JOIN (
            SELECT
              TRANS_ID
            FROM
              DVH_FAM_FP.FAM_FP_UTTAK_FP_KONTOER
            WHERE
              UPPER(STOENADSKONTOTYPE) = 'MØDREKVOTE'
            GROUP BY
              TRANS_ID
          ) RETT_TIL_MØDREKVOTE
          ON RETT_TIL_MØDREKVOTE.TRANS_ID = BEREGNINGSGRUNNLAG_AGG.TRANS_ID
          LEFT JOIN (
            SELECT
              TRANS_ID
            FROM
              DVH_FAM_FP.FAM_FP_UTTAK_FP_KONTOER
            WHERE
              UPPER(STOENADSKONTOTYPE) = 'FEDREKVOTE'
            GROUP BY
              TRANS_ID
          ) RETT_TIL_FEDREKVOTE
          ON RETT_TIL_FEDREKVOTE.TRANS_ID = BEREGNINGSGRUNNLAG_AGG.TRANS_ID
          LEFT JOIN (
            SELECT
              TRANS_ID,
              MAX(MAX_DAGER) AS FLERBARNSDAGER
            FROM
              DVH_FAM_FP.FAM_FP_UTTAK_FP_KONTOER
            WHERE
              UPPER(STOENADSKONTOTYPE) = 'FLERBARNSDAGER'
            GROUP BY
              TRANS_ID
          ) FLERBARNSDAGER
          ON FLERBARNSDAGER.TRANS_ID = BEREGNINGSGRUNNLAG_AGG.TRANS_ID
          LEFT JOIN ADOPSJON
          ON BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID = ADOPSJON.FAGSAK_ID
          LEFT JOIN EOS
          ON BEREGNINGSGRUNNLAG_AGG.TRANS_ID = EOS.TRANS_ID
      )
      SELECT /*+ PARALLEL(8) */
        *
 --from uttak_dager
      FROM
        GRUNNLAG
      WHERE
        FAGSAK_ID NOT IN (1679117)
 --where fagsak_id in (1035184)
;
    V_TID_FOM                      VARCHAR2(8) := NULL;
    V_TID_TOM                      VARCHAR2(8) := NULL;
    V_COMMIT                       NUMBER := 0;
    V_ERROR_MELDING                VARCHAR2(1000) := NULL;
    V_DIM_TID_ANTALL               NUMBER := 0;
    V_UTBETALINGSPROSENT_KALKULERT NUMBER := 0;
  BEGIN
    V_TID_FOM := SUBSTR(P_IN_VEDTAK_TOM, 1, 4)
                 || SUBSTR(P_IN_VEDTAK_TOM, 5, 6)-2
                                                  || '01';
    V_TID_TOM := TO_CHAR(LAST_DAY(TO_DATE(P_IN_VEDTAK_TOM, 'yyyymm')), 'yyyymmdd');
 --dbms_output.put_line(v_tid_fom||v_tid_tom);--TEST!!!
    FOR REC_PERIODE IN CUR_PERIODE(P_IN_RAPPORT_DATO, P_IN_FORSKYVNINGER, V_TID_FOM, V_TID_TOM) LOOP
      V_DIM_TID_ANTALL := 0;
      V_UTBETALINGSPROSENT_KALKULERT := 0;
      V_DIM_TID_ANTALL := DIM_TID_ANTALL(TO_NUMBER(TO_CHAR(REC_PERIODE.UTTAK_FOM, 'yyyymmdd')), TO_NUMBER(TO_CHAR(REC_PERIODE.UTTAK_TOM, 'yyyymmdd')));
      IF V_DIM_TID_ANTALL != 0 THEN
        V_UTBETALINGSPROSENT_KALKULERT := ROUND(REC_PERIODE.TREKKDAGER/V_DIM_TID_ANTALL*100, 2);
      ELSE
        V_UTBETALINGSPROSENT_KALKULERT := 0;
      END IF;
      BEGIN
        INSERT INTO DVH_FAM_FP.FAK_FAM_FP_VEDTAK_UTBETALING (
          FAGSAK_ID,
          TRANS_ID,
          BEHANDLINGSTEMA,
          TREKKONTO,
          STONADSDAGER_KVOTE,
          UTTAK_ARBEID_TYPE,
          AAR,
          HALVAAR,
          KVARTAL --, aar_maaned
,
          RAPPORT_PERIODE,
          UTTAK_FOM,
          UTTAK_TOM,
          DAGER_ERST,
          BEREGNINGSGRUNNLAG_FOM,
          BEREGNINGSGRUNNLAG_TOM,
          DEKNINGSGRAD,
          DAGSATS_BRUKER,
          DAGSATS_ARBEIDSGIVER,
          VIRKSOMHET,
          PERIODE_RESULTAT_AARSAK,
          DAGSATS,
          GRADERINGSPROSENT,
          STATUS_OG_ANDEL_INNTEKTSKAT,
          AKTIVITET_STATUS,
          BRUTTO_INNTEKT,
          AVKORTET_INNTEKT,
          STATUS_OG_ANDEL_BRUTTO,
          STATUS_OG_ANDEL_AVKORTET,
          UTBETALINGSPROSENT,
          FK_DIM_TID_DATO_UTBET_FOM,
          FK_DIM_TID_DATO_UTBET_TOM,
          FUNKSJONELL_TID,
          FORSTE_VEDTAKSDATO,
          VEDTAKSDATO,
          MAX_VEDTAKSDATO,
          PERIODE_TYPE,
          TILFELLE_ERST,
          BELOP,
          DAGSATS_REDUSERT,
          LASTET_DATO,
          MAX_TRANS_ID,
          FK_PERSON1_MOTTAKER,
          FK_PERSON1_ANNEN_PART,
          KJONN,
          FK_PERSON1_BARN,
          TERMINDATO,
          FOEDSELSDATO,
          ANTALL_BARN_TERMIN,
          ANTALL_BARN_FOEDSEL,
          FOEDSELSDATO_ADOPSJON,
          ANTALL_BARN_ADOPSJON,
          ANNENFORELDERFAGSAK_ID,
          MAX_STONADSDAGER_KONTO,
          FK_DIM_PERSON,
          BOSTED_KOMMUNE_NR,
          FK_DIM_GEOGRAFI,
          BYDEL_KOMMUNE_NR,
          KOMMUNE_NR,
          KOMMUNE_NAVN,
          BYDEL_NR,
          BYDEL_NAVN,
          ALENEOMSORG,
          HOVEDKONTONR,
          UNDERKONTONR,
          MOTTAKER_FODSELS_AAR,
          MOTTAKER_FODSELS_MND,
          MOTTAKER_ALDER,
          RETT_TIL_FEDREKVOTE,
          RETT_TIL_MODREKVOTE,
          DAGSATS_ERST,
          TREKKDAGER,
          SAMTIDIG_UTTAK,
          GRADERING,
          GRADERING_INNVILGET,
          ANTALL_DAGER_PERIODE,
          FLERBARNSDAGER,
          UTBETALINGSPROSENT_KALKULERT,
          MIN_UTTAK_FOM,
          MAX_UTTAK_TOM,
          FK_FAM_FP_TREKKONTO,
          FK_FAM_FP_PERIODE_RESULTAT_AARSAK,
          SIVILSTATUS,
          FK_DIM_SIVILSTATUS,
          ANTALL_BEREGNINGSGRUNNLAG,
          GRADERINGSDAGER,
          FK_DIM_TID_MIN_DATO_KVOTE,
          FK_DIM_TID_MAX_DATO_KVOTE,
          ADOPSJONSDATO,
          STEBARNSADOPSJON,
          EOS_SAK,
          MOR_RETTIGHET,
          STATSBORGERSKAP,
          ARBEIDSTIDSPROSENT,
          MORS_AKTIVITET,
          GYLDIG_FLAGG,
          ANDEL_AV_REFUSJON,
          FORSTE_SOKNADSDATO,
          SOKNADSDATO
        ) VALUES (
          REC_PERIODE.FAGSAK_ID,
          REC_PERIODE.TRANS_ID,
          REC_PERIODE.BEHANDLINGSTEMA,
          REC_PERIODE.TREKKONTO,
          REC_PERIODE.STONADSDAGER_KVOTE,
          REC_PERIODE.UTTAK_ARBEID_TYPE,
          REC_PERIODE.AAR,
          REC_PERIODE.HALVAAR,
          REC_PERIODE.KVARTAL --, rec_periode.aar_maaned
,
          P_IN_RAPPORT_DATO,
          REC_PERIODE.UTTAK_FOM,
          REC_PERIODE.UTTAK_TOM,
          REC_PERIODE.DAGER_ERST,
          REC_PERIODE.BEREGNINGSGRUNNLAG_FOM,
          REC_PERIODE.BEREGNINGSGRUNNLAG_TOM,
          REC_PERIODE.DEKNINGSGRAD,
          REC_PERIODE.DAGSATS_BRUKER,
          REC_PERIODE.DAGSATS_ARBEIDSGIVER,
          REC_PERIODE.VIRKSOMHET,
          REC_PERIODE.PERIODE_RESULTAT_AARSAK,
          REC_PERIODE.DAGSATS,
          REC_PERIODE.GRADERINGSPROSENT,
          REC_PERIODE.STATUS_OG_ANDEL_INNTEKTSKAT,
          REC_PERIODE.AKTIVITET_STATUS,
          REC_PERIODE.BRUTTO_INNTEKT,
          REC_PERIODE.AVKORTET_INNTEKT,
          REC_PERIODE.STATUS_OG_ANDEL_BRUTTO,
          REC_PERIODE.STATUS_OG_ANDEL_AVKORTET,
          REC_PERIODE.UTBETALINGSPROSENT,
          REC_PERIODE.PK_DIM_TID_DATO_UTBET_FOM,
          REC_PERIODE.PK_DIM_TID_DATO_UTBET_TOM,
          REC_PERIODE.FUNKSJONELL_TID,
          REC_PERIODE.FORSTE_VEDTAKSDATO,
          REC_PERIODE.SISTE_VEDTAKSDATO,
          REC_PERIODE.MAX_VEDTAKSDATO,
          REC_PERIODE.PERIODE,
          REC_PERIODE.TILFELLE_ERST,
          REC_PERIODE.BELOP,
          REC_PERIODE.DAGSATS_REDUSERT,
          REC_PERIODE.LASTET_DATO,
          REC_PERIODE.MAX_TRANS_ID,
          REC_PERIODE.FK_PERSON1_MOTTAKER,
          REC_PERIODE.FK_PERSON1_ANNEN_PART,
          REC_PERIODE.KJONN,
          REC_PERIODE.FK_PERSON1_BARN,
          REC_PERIODE.TERMINDATO,
          REC_PERIODE.FOEDSELSDATO,
          REC_PERIODE.ANTALL_BARN_TERMIN,
          REC_PERIODE.ANTALL_BARN_FOEDSEL,
          REC_PERIODE.FOEDSELSDATO_ADOPSJON,
          REC_PERIODE.ANTALL_BARN_ADOPSJON,
          REC_PERIODE.ANNENFORELDERFAGSAK_ID,
          REC_PERIODE.MAX_STONADSDAGER_KONTO,
          REC_PERIODE.PK_DIM_PERSON,
          REC_PERIODE.BOSTED_KOMMUNE_NR,
          REC_PERIODE.PK_DIM_GEOGRAFI,
          REC_PERIODE.BYDEL_KOMMUNE_NR,
          REC_PERIODE.KOMMUNE_NR,
          REC_PERIODE.KOMMUNE_NAVN,
          REC_PERIODE.BYDEL_NR,
          REC_PERIODE.BYDEL_NAVN,
          REC_PERIODE.ALENEOMSORG,
          REC_PERIODE.HOVEDKONTONR,
          REC_PERIODE.UNDERKONTONR,
          REC_PERIODE.MOTTAKER_FODSELS_AAR,
          REC_PERIODE.MOTTAKER_FODSELS_MND,
          REC_PERIODE.MOTTAKER_ALDER,
          REC_PERIODE.RETT_TIL_FEDREKVOTE,
          REC_PERIODE.RETT_TIL_MØDREKVOTE,
          REC_PERIODE.DAGSATS_ERST,
          REC_PERIODE.TREKKDAGER,
          REC_PERIODE.SAMTIDIG_UTTAK,
          REC_PERIODE.GRADERING,
          REC_PERIODE.GRADERING_INNVILGET,
          V_DIM_TID_ANTALL,
          REC_PERIODE.FLERBARNSDAGER,
          V_UTBETALINGSPROSENT_KALKULERT,
          REC_PERIODE.MIN_UTTAK_FOM,
          REC_PERIODE.MAX_UTTAK_TOM,
          REC_PERIODE.PK_FAM_FP_TREKKONTO,
          REC_PERIODE.PK_FAM_FP_PERIODE_RESULTAT_AARSAK,
          REC_PERIODE.SIVILSTAND,
          REC_PERIODE.FK_DIM_SIVILSTATUS,
          REC_PERIODE.ANTALL_BEREGNINGSGRUNNLAG,
          REC_PERIODE.GRADERINGSDAGER,
          REC_PERIODE.FK_DIM_TID_MIN_DATO_KVOTE,
          REC_PERIODE.FK_DIM_TID_MAX_DATO_KVOTE,
          REC_PERIODE.ADOPSJONSDATO,
          REC_PERIODE.STEBARNSADOPSJON,
          REC_PERIODE.EOS_SAK,
          REC_PERIODE.MOR_RETTIGHET,
          REC_PERIODE.STATSBORGERSKAP,
          REC_PERIODE.ARBEIDSTIDSPROSENT,
          REC_PERIODE.MORS_AKTIVITET,
          P_IN_GYLDIG_FLAGG,
          REC_PERIODE.ANDEL_AV_REFUSJON,
          REC_PERIODE.FORSTE_SOKNADSDATO,
          REC_PERIODE.SOKNADSDATO
        );
        V_COMMIT := V_COMMIT + 1;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK;
          V_ERROR_MELDING := SUBSTR(SQLCODE
                                    || ' '
                                    || SQLERRM, 1, 1000);
          INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
            MIN_LASTET_DATO,
            ID,
            ERROR_MSG,
            OPPRETTET_TID,
            KILDE
          ) VALUES(
            NULL,
            REC_PERIODE.FAGSAK_ID,
            V_ERROR_MELDING,
            SYSDATE,
            'FAM_FP_STATISTIKK_KVARTAL:INSERT'
          );
          COMMIT;
          P_OUT_ERROR := SUBSTR(P_OUT_ERROR
                                || V_ERROR_MELDING, 1, 1000);
      END;
      IF V_COMMIT > 100000 THEN
        COMMIT;
        V_COMMIT := 0;
      END IF;
    END LOOP;
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_ERROR_MELDING := SUBSTR(SQLCODE
                                || ' '
                                || SQLERRM, 1, 1000);
      INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
        MIN_LASTET_DATO,
        ID,
        ERROR_MSG,
        OPPRETTET_TID,
        KILDE
      ) VALUES(
        NULL,
        NULL,
        V_ERROR_MELDING,
        SYSDATE,
        'FAM_FP_STATISTIKK_KVARTAL'
      );
      COMMIT;
      P_OUT_ERROR := SUBSTR(P_OUT_ERROR
                            || V_ERROR_MELDING, 1, 1000);
  END FAM_FP_STATISTIKK_KVARTAL;

  PROCEDURE FAM_FP_STATISTIKK_HALVAAR(
    P_IN_VEDTAK_TOM IN VARCHAR2,
    P_IN_RAPPORT_DATO IN VARCHAR2,
    P_IN_FORSKYVNINGER IN NUMBER,
    P_IN_GYLDIG_FLAGG IN NUMBER DEFAULT 0,
    P_IN_PERIODE_TYPE IN VARCHAR2 DEFAULT 'H',
    P_OUT_ERROR OUT VARCHAR2
  ) AS
    CURSOR CUR_PERIODE(P_RAPPORT_DATO IN VARCHAR2, P_FORSKYVNINGER IN NUMBER, P_TID_FOM IN VARCHAR2, P_TID_TOM IN VARCHAR2) IS
      WITH FAGSAK AS (
        SELECT
          FAGSAK_ID,
          MAX(BEHANDLINGSTEMA)                                                   AS BEHANDLINGSTEMA,
          MAX(FAGSAKANNENFORELDER_ID)                                            AS ANNENFORELDERFAGSAK_ID,
          MAX(TRANS_ID) KEEP(DENSE_RANK FIRST ORDER BY FUNKSJONELL_TID DESC)     AS MAX_TRANS_ID,
          MAX(SOEKNADSDATO) KEEP(DENSE_RANK FIRST ORDER BY FUNKSJONELL_TID DESC) AS SOKNADSDATO,
          MIN(SOEKNADSDATO)                                                      AS FORSTE_SOKNADSDATO,
          MIN(VEDTAKSDATO)                                                       AS FORSTE_VEDTAKSDATO,
          MAX(FUNKSJONELL_TID)                                                   AS FUNKSJONELL_TID,
          MAX(VEDTAKSDATO)                                                       AS SISTE_VEDTAKSDATO,
          P_IN_PERIODE_TYPE                                                      AS PERIODE,
          LAST_DAY(TO_DATE(P_RAPPORT_DATO, 'yyyymm')) + P_FORSKYVNINGER          AS MAX_VEDTAKSDATO
        FROM
          DVH_FAM_FP.FAM_FP_FAGSAK
        WHERE
          FAM_FP_FAGSAK.FUNKSJONELL_TID <= LAST_DAY(TO_DATE(P_RAPPORT_DATO, 'yyyymm')) + P_FORSKYVNINGER
        GROUP BY
          FAGSAK_ID
      ), TERMIN AS (
        SELECT
          FAGSAK_ID,
          MAX(TERMINDATO)            TERMINDATO,
          MAX(FOEDSELSDATO)          FOEDSELSDATO,
          MAX(ANTALL_BARN_TERMIN)    ANTALL_BARN_TERMIN,
          MAX(ANTALL_BARN_FOEDSEL)   ANTALL_BARN_FOEDSEL,
          MAX(FOEDSELSDATO_ADOPSJON) FOEDSELSDATO_ADOPSJON,
          MAX(ANTALL_BARN_ADOPSJON) ANTALL_BARN_ADOPSJON
        FROM
          (
            SELECT
              FAM_FP_FAGSAK.FAGSAK_ID,
              MAX(FODSEL.TERMINDATO)              TERMINDATO,
              MAX(FODSEL.FOEDSELSDATO)            FOEDSELSDATO,
              MAX(FODSEL.ANTALL_BARN_FOEDSEL)     ANTALL_BARN_FOEDSEL,
              MAX(FODSEL.ANTALL_BARN_TERMIN)      ANTALL_BARN_TERMIN,
              MAX(ADOPSJON.FOEDSELSDATO_ADOPSJON) FOEDSELSDATO_ADOPSJON,
              COUNT(ADOPSJON.TRANS_ID)            ANTALL_BARN_ADOPSJON
            FROM
              DVH_FAM_FP.FAM_FP_FAGSAK
              LEFT JOIN DVH_FAM_FP.FAM_FP_FODSELTERMIN FODSEL
              ON FODSEL.FAGSAK_ID = FAM_FP_FAGSAK.FAGSAK_ID
              AND UPPER(FAM_FP_FAGSAK.BEHANDLINGSTEMA) = 'FORP_FODS'
              LEFT JOIN DVH_FAM_FP.FAM_FP_FODSELTERMIN ADOPSJON
              ON ADOPSJON.FAGSAK_ID = FAM_FP_FAGSAK.FAGSAK_ID
              AND ADOPSJON.TRANS_ID = FAM_FP_FAGSAK.TRANS_ID
              AND UPPER(FAM_FP_FAGSAK.BEHANDLINGSTEMA) = 'FORP_ADOP'
            GROUP BY
              FAM_FP_FAGSAK.FAGSAK_ID,
              FAM_FP_FAGSAK.TRANS_ID
          )
        GROUP BY
          FAGSAK_ID
      ), FK_PERSON1 AS (
        SELECT
          PERSON.PERSON,
          PERSON.FAGSAK_ID,
          MAX(PERSON.BEHANDLINGSTEMA)                                                                             AS BEHANDLINGSTEMA,
          PERSON.MAX_TRANS_ID,
          MAX(PERSON.ANNENFORELDERFAGSAK_ID)                                                                      AS ANNENFORELDERFAGSAK_ID,
          PERSON.AKTOER_ID,
          MAX(PERSON.KJONN)                                                                                       AS KJONN,
          MAX(PERSON_67_VASKET.FK_PERSON1) KEEP (DENSE_RANK FIRST ORDER BY PERSON_67_VASKET.GYLDIG_FRA_DATO DESC) AS FK_PERSON1,
          MAX(FOEDSELSDATO)                                                                                       AS FOEDSELSDATO,
          MAX(SIVILSTAND)                                                                                         AS SIVILSTAND,
          MAX(STATSBORGERSKAP)                                                                                    AS STATSBORGERSKAP
        FROM
          (
            SELECT
              'MOTTAKER'                                AS PERSON,
              FAGSAK.FAGSAK_ID,
              FAGSAK.BEHANDLINGSTEMA,
              FAGSAK.MAX_TRANS_ID,
              FAGSAK.ANNENFORELDERFAGSAK_ID,
              FAM_FP_PERSONOPPLYSNINGER.AKTOER_ID,
              FAM_FP_PERSONOPPLYSNINGER.KJONN,
              FAM_FP_PERSONOPPLYSNINGER.FOEDSELSDATO,
              FAM_FP_PERSONOPPLYSNINGER.SIVILSTAND,
              FAM_FP_PERSONOPPLYSNINGER.STATSBORGERSKAP
            FROM
              DVH_FAM_FP.FAM_FP_PERSONOPPLYSNINGER
              JOIN FAGSAK
              ON FAM_FP_PERSONOPPLYSNINGER.TRANS_ID = FAGSAK.MAX_TRANS_ID UNION ALL
              SELECT
                'BARN'                                    AS PERSON,
                FAGSAK.FAGSAK_ID,
                MAX(FAGSAK.BEHANDLINGSTEMA)               AS BEHANDLINGSTEMA,
                FAGSAK.MAX_TRANS_ID,
                MAX(FAGSAK.ANNENFORELDERFAGSAK_ID)        ANNENFORELDERFAGSAK_ID,
                MAX(FAM_FP_FAMILIEHENDELSE.TIL_AKTOER_ID) AS AKTOER_ID,
                MAX(FAM_FP_FAMILIEHENDELSE.KJOENN)        AS KJONN,
                NULL                                      AS FOEDSELSDATO,
                NULL                                      AS SIVILSTAND,
                NULL                                      AS STATSBORGERSKAP
              FROM
                DVH_FAM_FP.FAM_FP_FAMILIEHENDELSE
                JOIN FAGSAK
                ON FAM_FP_FAMILIEHENDELSE.FAGSAK_ID = FAGSAK.FAGSAK_ID
              WHERE
                UPPER(FAM_FP_FAMILIEHENDELSE.RELASJON) = 'BARN'
              GROUP BY
                FAGSAK.FAGSAK_ID, FAGSAK.MAX_TRANS_ID
          )                                    PERSON
          JOIN DT_PERSON.DVH_PERSON_IDENT_AKTOR_IKKE_SKJERMET PERSON_67_VASKET
 --join fk_person.fam_fp_person_67_vasket person_67_vasket
          ON PERSON_67_VASKET.AKTOR_ID = PERSON.AKTOER_ID
          AND TO_DATE(P_TID_TOM, 'yyyymmdd') BETWEEN PERSON_67_VASKET.GYLDIG_FRA_DATO
          AND PERSON_67_VASKET.GYLDIG_TIL_DATO
 --on person_67_vasket.lk_person_id_kilde_num = person.aktoer_id
        GROUP BY
          PERSON.PERSON, PERSON.FAGSAK_ID, PERSON.MAX_TRANS_ID, PERSON.AKTOER_ID
      ), BARN AS (
        SELECT
          FAGSAK_ID,
          LISTAGG(FK_PERSON1, ',') WITHIN GROUP (ORDER BY FK_PERSON1) AS FK_PERSON1_BARN
        FROM
          FK_PERSON1
        WHERE
          PERSON = 'BARN'
        GROUP BY
          FAGSAK_ID
      ), MOTTAKER AS (
        SELECT
          FK_PERSON1.FAGSAK_ID,
          FK_PERSON1.BEHANDLINGSTEMA,
          FK_PERSON1.MAX_TRANS_ID,
          FK_PERSON1.ANNENFORELDERFAGSAK_ID,
          FK_PERSON1.AKTOER_ID,
          FK_PERSON1.KJONN,
          FK_PERSON1.FK_PERSON1                       AS FK_PERSON1_MOTTAKER,
          EXTRACT(YEAR FROM FK_PERSON1.FOEDSELSDATO) AS MOTTAKER_FODSELS_AAR,
          EXTRACT(MONTH FROM FK_PERSON1.FOEDSELSDATO) AS MOTTAKER_FODSELS_MND,
          FK_PERSON1.SIVILSTAND,
          FK_PERSON1.STATSBORGERSKAP,
          BARN.FK_PERSON1_BARN,
          TERMIN.TERMINDATO,
          TERMIN.FOEDSELSDATO,
          TERMIN.ANTALL_BARN_TERMIN,
          TERMIN.ANTALL_BARN_FOEDSEL,
          TERMIN.FOEDSELSDATO_ADOPSJON,
          TERMIN.ANTALL_BARN_ADOPSJON
        FROM
          FK_PERSON1
          LEFT JOIN BARN
          ON BARN.FAGSAK_ID = FK_PERSON1.FAGSAK_ID
          LEFT JOIN TERMIN
          ON FK_PERSON1.FAGSAK_ID = TERMIN.FAGSAK_ID
        WHERE
          FK_PERSON1.PERSON = 'MOTTAKER'
      ), ADOPSJON AS (
        SELECT
          FAM_FP_VILKAAR.FAGSAK_ID,
          MAX(FAM_FP_VILKAAR.OMSORGS_OVERTAKELSESDATO) AS ADOPSJONSDATO,
          MAX(FAM_FP_VILKAAR.EKTEFELLES_BARN)          AS STEBARNSADOPSJON
        FROM
          FAGSAK
          JOIN DVH_FAM_FP.FAM_FP_VILKAAR
          ON FAGSAK.FAGSAK_ID = FAM_FP_VILKAAR.FAGSAK_ID
        WHERE
          FAGSAK.BEHANDLINGSTEMA = 'FORP_ADOP'
        GROUP BY
          FAM_FP_VILKAAR.FAGSAK_ID
      ), EOS AS (
        SELECT
          A.TRANS_ID,
          CASE
            WHEN UPPER(ER_BORGER_AV_EU_EOS) = 'TRUE' THEN
              'J'
            WHEN UPPER(ER_BORGER_AV_EU_EOS) = 'FALSE' THEN
              'N'
            ELSE
              NULL
          END EOS_SAK
        FROM
          (
            SELECT
              FAM_FP_VILKAAR.TRANS_ID,
              MAX(FAM_FP_VILKAAR.ER_BORGER_AV_EU_EOS) AS ER_BORGER_AV_EU_EOS
            FROM
              FAGSAK
              JOIN DVH_FAM_FP.FAM_FP_VILKAAR
              ON FAGSAK.MAX_TRANS_ID = FAM_FP_VILKAAR.TRANS_ID
              AND LENGTH(FAM_FP_VILKAAR.PERSON_STATUS) > 0
            GROUP BY
              FAM_FP_VILKAAR.TRANS_ID
          ) A
      ), ANNENFORELDERFAGSAK AS (
        SELECT
          ANNENFORELDERFAGSAK.*,
          MOTTAKER.FK_PERSON1_MOTTAKER AS FK_PERSON1_ANNEN_PART
        FROM
          (
            SELECT
              FAGSAK_ID,
              MAX_TRANS_ID,
              MAX(ANNENFORELDERFAGSAK_ID) AS ANNENFORELDERFAGSAK_ID
            FROM
              (
                SELECT
                  FORELDER1.FAGSAK_ID,
                  FORELDER1.MAX_TRANS_ID,
                  NVL(FORELDER1.ANNENFORELDERFAGSAK_ID, FORELDER2.FAGSAK_ID) AS ANNENFORELDERFAGSAK_ID
                FROM
                  MOTTAKER FORELDER1
                  JOIN MOTTAKER FORELDER2
                  ON FORELDER1.FK_PERSON1_BARN = FORELDER2.FK_PERSON1_BARN
                  AND FORELDER1.FK_PERSON1_MOTTAKER != FORELDER2.FK_PERSON1_MOTTAKER
              )
            GROUP BY
              FAGSAK_ID,
              MAX_TRANS_ID
          )        ANNENFORELDERFAGSAK
          JOIN MOTTAKER
          ON ANNENFORELDERFAGSAK.ANNENFORELDERFAGSAK_ID = MOTTAKER.FAGSAK_ID
      ), TID AS (
        SELECT
          PK_DIM_TID,
          DATO,
          AAR,
          HALVAAR,
          KVARTAL,
          AAR_MAANED
        FROM
          DT_KODEVERK.DIM_TID
        WHERE
          DAG_I_UKE < 6
          AND DIM_NIVAA = 1
          AND GYLDIG_FLAGG = 1
          AND PK_DIM_TID BETWEEN P_TID_FOM AND P_TID_TOM
          AND PK_DIM_TID <= TO_CHAR(LAST_DAY(TO_DATE(P_RAPPORT_DATO, 'yyyymm')), 'yyyymmdd')
      ), UTTAK AS (
        SELECT
          UTTAK.TRANS_ID,
          UTTAK.TREKKONTO,
          UTTAK.UTTAK_ARBEID_TYPE,
          UTTAK.VIRKSOMHET,
          UTTAK.UTBETALINGSPROSENT,
          UTTAK.GRADERING_INNVILGET,
          UTTAK.GRADERING,
          UTTAK.ARBEIDSTIDSPROSENT,
          UTTAK.SAMTIDIG_UTTAK,
          UTTAK.PERIODE_RESULTAT_AARSAK,
          UTTAK.FOM                                      AS UTTAK_FOM,
          UTTAK.TOM                                      AS UTTAK_TOM,
          UTTAK.TREKKDAGER,
          FAGSAK.FAGSAK_ID,
          FAGSAK.PERIODE,
          FAGSAK.FUNKSJONELL_TID,
          FAGSAK.FORSTE_VEDTAKSDATO,
          FAGSAK.SISTE_VEDTAKSDATO,
          FAGSAK.MAX_VEDTAKSDATO,
          FAGSAK.FORSTE_SOKNADSDATO,
          FAGSAK.SOKNADSDATO,
          FAM_FP_TREKKONTO.PK_FAM_FP_TREKKONTO,
          AARSAK_UTTAK.PK_FAM_FP_PERIODE_RESULTAT_AARSAK,
          UTTAK.ARBEIDSFORHOLD_ID,
          UTTAK.GRADERINGSDAGER,
          FAM_FP_UTTAK_FORDELINGSPER.MORS_AKTIVITET
        FROM
          DVH_FAM_FP.FAM_FP_UTTAK_RES_PER_AKTIV UTTAK
          JOIN FAGSAK
          ON FAGSAK.MAX_TRANS_ID = UTTAK.TRANS_ID LEFT JOIN DVH_FAM_FP.FAM_FP_TREKKONTO
          ON UPPER(UTTAK.TREKKONTO) = FAM_FP_TREKKONTO.TREKKONTO
          LEFT JOIN (
            SELECT
              AARSAK_UTTAK,
              MAX(PK_FAM_FP_PERIODE_RESULTAT_AARSAK) AS PK_FAM_FP_PERIODE_RESULTAT_AARSAK
            FROM
              DVH_FAM_FP.FAM_FP_PERIODE_RESULTAT_AARSAK
            GROUP BY
              AARSAK_UTTAK
          ) AARSAK_UTTAK
          ON UPPER(UTTAK.PERIODE_RESULTAT_AARSAK) = AARSAK_UTTAK.AARSAK_UTTAK
          LEFT JOIN DVH_FAM_FP.FAM_FP_UTTAK_FORDELINGSPER
          ON FAM_FP_UTTAK_FORDELINGSPER.TRANS_ID = UTTAK.TRANS_ID
          AND UTTAK.FOM BETWEEN FAM_FP_UTTAK_FORDELINGSPER.FOM
          AND FAM_FP_UTTAK_FORDELINGSPER.TOM
          AND UPPER(UTTAK.TREKKONTO) = UPPER(FAM_FP_UTTAK_FORDELINGSPER.PERIODE_TYPE)
          AND LENGTH(FAM_FP_UTTAK_FORDELINGSPER.MORS_AKTIVITET) > 1
        WHERE
          UTTAK.UTBETALINGSPROSENT > 0
      ), STONADSDAGER_KVOTE AS (
        SELECT
          UTTAK.*,
          TID1.PK_DIM_TID AS FK_DIM_TID_MIN_DATO_KVOTE,
          TID2.PK_DIM_TID AS FK_DIM_TID_MAX_DATO_KVOTE
        FROM
          (
            SELECT
              FAGSAK_ID,
              TRANS_ID,
              TREKKONTO,
              VIRKSOMHET,
              UTTAK_ARBEID_TYPE,
              SUM(TREKKDAGER)   AS STONADSDAGER_KVOTE,
              MIN(UTTAK_FOM)    AS MIN_UTTAK_FOM,
              MAX(UTTAK_TOM)    AS MAX_UTTAK_TOM
            FROM
              (
                SELECT
                  FAGSAK_ID,
                  TRANS_ID,
                  UTTAK_FOM,
                  UTTAK_TOM,
                  TREKKONTO,
                  VIRKSOMHET,
                  UTTAK_ARBEID_TYPE,
                  MAX(TREKKDAGER)   AS TREKKDAGER
                FROM
                  UTTAK
                GROUP BY
                  FAGSAK_ID,
                  TRANS_ID,
                  UTTAK_FOM,
                  UTTAK_TOM,
                  TREKKONTO,
                  VIRKSOMHET,
                  UTTAK_ARBEID_TYPE
              ) A
            GROUP BY
              FAGSAK_ID,
              TRANS_ID,
              TREKKONTO,
              VIRKSOMHET,
              UTTAK_ARBEID_TYPE
          )                   UTTAK
          JOIN DT_KODEVERK.DIM_TID TID1
          ON TID1.DIM_NIVAA = 1
          AND TID1.DATO = TRUNC(UTTAK.MIN_UTTAK_FOM, 'dd') JOIN DT_KODEVERK.DIM_TID TID2
          ON TID2.DIM_NIVAA = 1
          AND TID2.DATO = TRUNC(UTTAK.MAX_UTTAK_TOM,
          'dd')
      ), UTTAK_DAGER AS (
        SELECT
          UTTAK.*,
          TID.PK_DIM_TID,
          TID.DATO,
          TID.AAR,
          TID.HALVAAR,
          TID.KVARTAL,
          TID.AAR_MAANED
        FROM
          UTTAK
          JOIN TID
          ON TID.DATO BETWEEN UTTAK.UTTAK_FOM
          AND UTTAK.UTTAK_TOM
      ), ALENEOMSORG AS (
        SELECT
          UTTAK.FAGSAK_ID,
          UTTAK.UTTAK_FOM
        FROM
          UTTAK
          JOIN DVH_FAM_FP.FAM_FP_DOKUMENTASJONSPERIODER DOK1
          ON DOK1.FAGSAK_ID = UTTAK.FAGSAK_ID
          AND UTTAK.UTTAK_FOM >= DOK1.FOM
          AND DOK1.DOKUMENTASJON_TYPE IN ('ALENEOMSORG', 'ALENEOMSORG_OVERFØRING') LEFT JOIN DVH_FAM_FP.FAM_FP_DOKUMENTASJONSPERIODER DOK2
          ON DOK1.FAGSAK_ID = DOK2.FAGSAK_ID
          AND UTTAK.UTTAK_FOM >= DOK2.FOM
          AND DOK1.TRANS_ID < DOK2.TRANS_ID
          AND DOK2.DOKUMENTASJON_TYPE = 'ANNEN_FORELDER_HAR_RETT'
          AND DOK2.FAGSAK_ID IS NULL
        GROUP BY
          UTTAK.FAGSAK_ID,
          UTTAK.UTTAK_FOM
      ), BEREGNINGSGRUNNLAG AS (
        SELECT
          FAGSAK_ID,
          TRANS_ID,
          VIRKSOMHETSNUMMER,
          MAX(STATUS_OG_ANDEL_BRUTTO)         AS STATUS_OG_ANDEL_BRUTTO,
          MAX(STATUS_OG_ANDEL_AVKORTET)       AS STATUS_OG_ANDEL_AVKORTET,
          FOM                                 AS BEREGNINGSGRUNNLAG_FOM,
          TOM                                 AS BEREGNINGSGRUNNLAG_TOM,
          MAX(DEKNINGSGRAD)                   AS DEKNINGSGRAD,
          MAX(DAGSATS)                        AS DAGSATS,
          DAGSATS_BRUKER,
          DAGSATS_ARBEIDSGIVER,
          DAGSATS_BRUKER+DAGSATS_ARBEIDSGIVER DAGSATS_VIRKSOMHET,
          MAX(STATUS_OG_ANDEL_INNTEKTSKAT)    AS STATUS_OG_ANDEL_INNTEKTSKAT,
          AKTIVITET_STATUS,
          MAX(BRUTTO)                         AS BRUTTO_INNTEKT,
          MAX(AVKORTET)                       AS AVKORTET_INNTEKT,
          COUNT(1)                            AS ANTALL_BEREGNINGSGRUNNLAG
        FROM
          DVH_FAM_FP.FAM_FP_BEREGNINGSGRUNNLAG
        GROUP BY
          FAGSAK_ID,
          TRANS_ID,
          VIRKSOMHETSNUMMER,
          FOM,
          TOM,
          AKTIVITET_STATUS,
          DAGSATS_BRUKER,
          DAGSATS_ARBEIDSGIVER
      ), BEREGNINGSGRUNNLAG_DETALJ AS (
        SELECT
          UTTAK_DAGER.*,
          STONADSDAGER_KVOTE.STONADSDAGER_KVOTE,
          STONADSDAGER_KVOTE.MIN_UTTAK_FOM,
          STONADSDAGER_KVOTE.MAX_UTTAK_TOM,
          STONADSDAGER_KVOTE.FK_DIM_TID_MIN_DATO_KVOTE,
          STONADSDAGER_KVOTE.FK_DIM_TID_MAX_DATO_KVOTE,
          BEREG.STATUS_OG_ANDEL_BRUTTO,
          BEREG.STATUS_OG_ANDEL_AVKORTET,
          BEREG.BEREGNINGSGRUNNLAG_FOM,
          BEREG.DEKNINGSGRAD,
          BEREG.BEREGNINGSGRUNNLAG_TOM,
          BEREG.DAGSATS,
          BEREG.DAGSATS_BRUKER,
          BEREG.DAGSATS_ARBEIDSGIVER,
          BEREG.DAGSATS_VIRKSOMHET,
          BEREG.STATUS_OG_ANDEL_INNTEKTSKAT,
          BEREG.AKTIVITET_STATUS,
          BEREG.BRUTTO_INNTEKT,
          BEREG.AVKORTET_INNTEKT,
          BEREG.DAGSATS*UTTAK_DAGER.UTBETALINGSPROSENT/100 AS DAGSATS_ERST,
          BEREG.ANTALL_BEREGNINGSGRUNNLAG
        FROM
          BEREGNINGSGRUNNLAG                        BEREG
          JOIN UTTAK_DAGER
          ON UTTAK_DAGER.TRANS_ID = BEREG.TRANS_ID
          AND NVL(UTTAK_DAGER.VIRKSOMHET, 'X') = NVL(BEREG.VIRKSOMHETSNUMMER, 'X')
          AND BEREG.BEREGNINGSGRUNNLAG_FOM <= UTTAK_DAGER.DATO
          AND NVL(BEREG.BEREGNINGSGRUNNLAG_TOM, TO_DATE('20991201', 'YYYYMMDD')) >= UTTAK_DAGER.DATO LEFT JOIN STONADSDAGER_KVOTE
          ON UTTAK_DAGER.TRANS_ID = STONADSDAGER_KVOTE.TRANS_ID
          AND UTTAK_DAGER.TREKKONTO = STONADSDAGER_KVOTE.TREKKONTO
          AND NVL(UTTAK_DAGER.VIRKSOMHET,
          'X') = NVL(STONADSDAGER_KVOTE.VIRKSOMHET,
          'X')
          AND UTTAK_DAGER.UTTAK_ARBEID_TYPE = STONADSDAGER_KVOTE.UTTAK_ARBEID_TYPE
          JOIN DVH_FAM_FP.FAM_FP_UTTAK_AKTIVITET_MAPPING UTTAK_MAPPING
          ON UTTAK_DAGER.UTTAK_ARBEID_TYPE = UTTAK_MAPPING.UTTAK_ARBEID
          AND BEREG.AKTIVITET_STATUS = UTTAK_MAPPING.AKTIVITET_STATUS
        WHERE
          BEREG.DAGSATS_BRUKER + BEREG.DAGSATS_ARBEIDSGIVER != 0
      ), BEREGNINGSGRUNNLAG_AGG AS (
        SELECT
          A.*,
          DAGER_ERST*DAGSATS_VIRKSOMHET/DAGSATS*ANTALL_BEREGNINGSGRUNNLAG                                                                                                                    TILFELLE_ERST,
          DAGER_ERST*ROUND(UTBETALINGSPROSENT/100*DAGSATS_VIRKSOMHET)                                                                                                                        BELOP,
          ROUND(UTBETALINGSPROSENT/100*DAGSATS_VIRKSOMHET-0.5)                                                                                                                               DAGSATS_REDUSERT,
          CASE
            WHEN PERIODE_RESULTAT_AARSAK IN (2004, 2033) THEN
              'N'
            WHEN TREKKONTO IN ('FEDREKVOTE', 'FELLESPERIODE', 'MØDREKVOTE') THEN
              'J'
            WHEN TREKKONTO = 'FORELDREPENGER' THEN
              'N'
          END MOR_RETTIGHET
        FROM
          (
            SELECT
              FAGSAK_ID,
              TRANS_ID,
              TREKKONTO,
              TREKKDAGER,
              STONADSDAGER_KVOTE,
              UTTAK_ARBEID_TYPE,
              AAR,
              HALVAAR --, kvartal, aar_maaned
,
              UTTAK_FOM,
              UTTAK_TOM,
              SUM(DAGSATS_VIRKSOMHET/DAGSATS*
                CASE
                  WHEN ((UPPER(GRADERING_INNVILGET) ='TRUE'
                  AND UPPER(GRADERING)='TRUE')
                  OR UPPER(SAMTIDIG_UTTAK)='TRUE') THEN
                    (100-ARBEIDSTIDSPROSENT)/100
                  ELSE
                    1.0
                END )                                DAGER_ERST2,
              MAX(ARBEIDSTIDSPROSENT)                AS ARBEIDSTIDSPROSENT,
              COUNT(DISTINCT PK_DIM_TID)             DAGER_ERST,
 --count(distinct pk_dim_tid)*dagsats_virksomhet/dagsats tilfelle_erst,
              MIN(BEREGNINGSGRUNNLAG_FOM)            BEREGNINGSGRUNNLAG_FOM,
              MAX(BEREGNINGSGRUNNLAG_TOM)            BEREGNINGSGRUNNLAG_TOM,
              DEKNINGSGRAD,
 --count(distinct pk_dim_tid)*
 --      round(utbetalingsprosent/100*dagsats_virksomhet-0.5) belop,
              DAGSATS_BRUKER,
              DAGSATS_ARBEIDSGIVER,
              DAGSATS_VIRKSOMHET,
              VIRKSOMHET,
              PERIODE_RESULTAT_AARSAK,
              DAGSATS,
              DAGSATS_ERST, --dagsats_virksomhet,
              UTBETALINGSPROSENT                     GRADERINGSPROSENT,
              STATUS_OG_ANDEL_INNTEKTSKAT,
              AKTIVITET_STATUS,
              BRUTTO_INNTEKT,
              AVKORTET_INNTEKT,
              STATUS_OG_ANDEL_BRUTTO,
              STATUS_OG_ANDEL_AVKORTET,
 --round(utbetalingsprosent/100*dagsats_virksomhet-0.5) dagsats_redusert,
              UTBETALINGSPROSENT,
              MIN(PK_DIM_TID)                        PK_DIM_TID_DATO_UTBET_FOM,
              MAX(PK_DIM_TID)                        PK_DIM_TID_DATO_UTBET_TOM,
              FUNKSJONELL_TID,
              FORSTE_VEDTAKSDATO,
              SISTE_VEDTAKSDATO,
              MAX_VEDTAKSDATO,
              PERIODE,
              MAX(FORSTE_SOKNADSDATO)                AS FORSTE_SOKNADSDATO,
              MAX(SOKNADSDATO)                       AS SOKNADSDATO,
              SAMTIDIG_UTTAK,
              GRADERING,
              GRADERING_INNVILGET,
              MIN_UTTAK_FOM,
              MAX_UTTAK_TOM,
              FK_DIM_TID_MIN_DATO_KVOTE,
              FK_DIM_TID_MAX_DATO_KVOTE,
              MAX(PK_FAM_FP_TREKKONTO)               AS PK_FAM_FP_TREKKONTO,
              MAX(PK_FAM_FP_PERIODE_RESULTAT_AARSAK) AS PK_FAM_FP_PERIODE_RESULTAT_AARSAK,
              ANTALL_BEREGNINGSGRUNNLAG,
              MAX(GRADERINGSDAGER)                   AS GRADERINGSDAGER,
              MAX(MORS_AKTIVITET)                    AS MORS_AKTIVITET
            FROM
              BEREGNINGSGRUNNLAG_DETALJ
            GROUP BY
              FAGSAK_ID,
              TRANS_ID,
              TREKKONTO,
              TREKKDAGER,
              STONADSDAGER_KVOTE,
              UTTAK_ARBEID_TYPE,
              AAR,
              HALVAAR --, kvartal, aar_maaned
,
              UTTAK_FOM,
              UTTAK_TOM,
              DEKNINGSGRAD,
              VIRKSOMHET,
              UTBETALINGSPROSENT,
              PERIODE_RESULTAT_AARSAK,
              DAGSATS,
              DAGSATS_ERST,
              DAGSATS_BRUKER,
              DAGSATS_ARBEIDSGIVER,
              DAGSATS_VIRKSOMHET,
              UTBETALINGSPROSENT,
              STATUS_OG_ANDEL_INNTEKTSKAT,
              AKTIVITET_STATUS,
              BRUTTO_INNTEKT,
              AVKORTET_INNTEKT,
              STATUS_OG_ANDEL_BRUTTO,
              STATUS_OG_ANDEL_AVKORTET,
              FUNKSJONELL_TID,
              FORSTE_VEDTAKSDATO,
              SISTE_VEDTAKSDATO,
              MAX_VEDTAKSDATO,
              PERIODE,
              SAMTIDIG_UTTAK,
              GRADERING,
              GRADERING_INNVILGET,
              MIN_UTTAK_FOM,
              MAX_UTTAK_TOM,
              FK_DIM_TID_MIN_DATO_KVOTE,
              FK_DIM_TID_MAX_DATO_KVOTE,
              ANTALL_BEREGNINGSGRUNNLAG
          ) A
      ), GRUNNLAG AS (
        SELECT
          BEREGNINGSGRUNNLAG_AGG.*,
          SYSDATE                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      AS LASTET_DATO,
          MOTTAKER.BEHANDLINGSTEMA,
          MOTTAKER.MAX_TRANS_ID,
          MOTTAKER.FK_PERSON1_MOTTAKER,
          MOTTAKER.KJONN,
          MOTTAKER.FK_PERSON1_BARN,
          MOTTAKER.TERMINDATO,
          MOTTAKER.FOEDSELSDATO,
          MOTTAKER.ANTALL_BARN_TERMIN,
          MOTTAKER.ANTALL_BARN_FOEDSEL,
          MOTTAKER.FOEDSELSDATO_ADOPSJON,
          MOTTAKER.ANTALL_BARN_ADOPSJON,
          MOTTAKER.MOTTAKER_FODSELS_AAR,
          MOTTAKER.MOTTAKER_FODSELS_MND,
          SUBSTR(P_TID_FOM, 1, 4) - MOTTAKER.MOTTAKER_FODSELS_AAR                                                                                                                                                                                                                                                                                                                                                                                                                                                      AS MOTTAKER_ALDER,
          MOTTAKER.SIVILSTAND,
          MOTTAKER.STATSBORGERSKAP,
          DIM_PERSON.PK_DIM_PERSON,
          DIM_PERSON.BOSTED_KOMMUNE_NR,
          DIM_PERSON.FK_DIM_SIVILSTATUS,
          DIM_GEOGRAFI.PK_DIM_GEOGRAFI,
          DIM_GEOGRAFI.BYDEL_KOMMUNE_NR,
          DIM_GEOGRAFI.KOMMUNE_NR,
          DIM_GEOGRAFI.KOMMUNE_NAVN,
          DIM_GEOGRAFI.BYDEL_NR,
          DIM_GEOGRAFI.BYDEL_NAVN,
          ANNENFORELDERFAGSAK.ANNENFORELDERFAGSAK_ID,
          ANNENFORELDERFAGSAK.FK_PERSON1_ANNEN_PART,
          FAM_FP_UTTAK_FP_KONTOER.MAX_DAGER                                                                                                                                                                                                                                                                                                                                                                                                                                                                            MAX_STONADSDAGER_KONTO,
          CASE
            WHEN ALENEOMSORG.FAGSAK_ID IS NOT NULL THEN
              'J'
            ELSE
              NULL
          END AS                                                                                                                                                                                                                                                                                                                                                                                                                                           ALENEOMSORG,
          CASE
            WHEN BEHANDLINGSTEMA = 'FORP_FODS' THEN
              '214'
            WHEN BEHANDLINGSTEMA = 'FORP_ADOP' THEN
              '216'
          END AS                                                                                                                                                                                                                                                                                                                                                                                                         HOVEDKONTONR,
          CASE
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='ARBEIDSTAKER' AND DAGSATS_ARBEIDSGIVER/DAGSATS*100<=50 THEN
              '1000'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='ARBEIDSTAKER' AND DAGSATS_ARBEIDSGIVER/DAGSATS*100>50 THEN
              '8020'
 --when status_og_andel_inntektskat='ARBEIDSTAKER' then '1000'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='JORDBRUKER' THEN
              '5210'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='SJØMANN' THEN
              '1300'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='SELVSTENDIG_NÆRINGSDRIVENDE' THEN
              '5010'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='DAGPENGER' THEN
              '1200'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='ARBEIDSTAKER_UTEN_FERIEPENGER' THEN
              '1000'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='FISKER' THEN
              '5300'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='DAGMAMMA' THEN
              '5110'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='FRILANSER' THEN
              '1100'
          END AS UNDERKONTONR,
          ROUND(DAGSATS_ARBEIDSGIVER/DAGSATS*100, 0)                                                                                                                                                                                                                                                                                                                                                                                                                                                                   AS ANDEL_AV_REFUSJON,
          CASE
            WHEN RETT_TIL_MØDREKVOTE.TRANS_ID IS NULL THEN
              'N'
            ELSE
              'J'
          END AS                                                                                                                                                                                                                                                                                                                                                                                                                                         RETT_TIL_MØDREKVOTE,
          CASE
            WHEN RETT_TIL_FEDREKVOTE.TRANS_ID IS NULL THEN
              'N'
            ELSE
              'J'
          END AS                                                                                                                                                                                                                                                                                                                                                                                                                                         RETT_TIL_FEDREKVOTE,
          FLERBARNSDAGER.FLERBARNSDAGER,
          ADOPSJON.ADOPSJONSDATO,
          ADOPSJON.STEBARNSADOPSJON,
          EOS.EOS_SAK
        FROM
          BEREGNINGSGRUNNLAG_AGG
          LEFT JOIN MOTTAKER
          ON BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID = MOTTAKER.FAGSAK_ID
          AND BEREGNINGSGRUNNLAG_AGG.TRANS_ID = MOTTAKER.MAX_TRANS_ID
          LEFT JOIN ANNENFORELDERFAGSAK
          ON BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID = ANNENFORELDERFAGSAK.FAGSAK_ID
          AND BEREGNINGSGRUNNLAG_AGG.TRANS_ID = ANNENFORELDERFAGSAK.MAX_TRANS_ID
          LEFT JOIN DVH_FAM_FP.FAM_FP_UTTAK_FP_KONTOER
          ON BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID = FAM_FP_UTTAK_FP_KONTOER.FAGSAK_ID
          AND MOTTAKER.MAX_TRANS_ID = FAM_FP_UTTAK_FP_KONTOER.TRANS_ID
 --AND UPPER(REGEXP_REPLACE(grunnlag_drp1.TREKKONTO, '_|-|[[:space:]]', '')) = UPPER(REGEXP_REPLACE(FAM_FP_Uttak_FP_Kontoer.STOENADSKONTOTYPE, '_|-|[[:space:]]', ''))
          AND UPPER(REPLACE(BEREGNINGSGRUNNLAG_AGG.TREKKONTO,
          '_',
          '')) = UPPER(REPLACE(FAM_FP_UTTAK_FP_KONTOER.STOENADSKONTOTYPE,
          ' ',
          ''))
          LEFT JOIN DT_PERSON.DIM_PERSON
          ON MOTTAKER.FK_PERSON1_MOTTAKER = DIM_PERSON.FK_PERSON1
 --and beregningsgrunnlag_agg.uttak_tom between dim_person.gyldig_fra_dato and dim_person.gyldig_til_dato
          AND TO_DATE(BEREGNINGSGRUNNLAG_AGG.PK_DIM_TID_DATO_UTBET_TOM,
          'yyyymmdd') BETWEEN DIM_PERSON.GYLDIG_FRA_DATO
          AND DIM_PERSON.GYLDIG_TIL_DATO
          LEFT JOIN DT_KODEVERK.DIM_GEOGRAFI
          ON DIM_PERSON.FK_DIM_GEOGRAFI_BOSTED = DIM_GEOGRAFI.PK_DIM_GEOGRAFI
          LEFT JOIN ALENEOMSORG
          ON ALENEOMSORG.FAGSAK_ID = BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID
          AND ALENEOMSORG.UTTAK_FOM = BEREGNINGSGRUNNLAG_AGG.UTTAK_FOM
          LEFT JOIN (
            SELECT
              TRANS_ID
            FROM
              DVH_FAM_FP.FAM_FP_UTTAK_FP_KONTOER
            WHERE
              UPPER(STOENADSKONTOTYPE) = 'MØDREKVOTE'
            GROUP BY
              TRANS_ID
          ) RETT_TIL_MØDREKVOTE
          ON RETT_TIL_MØDREKVOTE.TRANS_ID = BEREGNINGSGRUNNLAG_AGG.TRANS_ID
          LEFT JOIN (
            SELECT
              TRANS_ID
            FROM
              DVH_FAM_FP.FAM_FP_UTTAK_FP_KONTOER
            WHERE
              UPPER(STOENADSKONTOTYPE) = 'FEDREKVOTE'
            GROUP BY
              TRANS_ID
          ) RETT_TIL_FEDREKVOTE
          ON RETT_TIL_FEDREKVOTE.TRANS_ID = BEREGNINGSGRUNNLAG_AGG.TRANS_ID
          LEFT JOIN (
            SELECT
              TRANS_ID,
              MAX(MAX_DAGER) AS FLERBARNSDAGER
            FROM
              DVH_FAM_FP.FAM_FP_UTTAK_FP_KONTOER
            WHERE
              UPPER(STOENADSKONTOTYPE) = 'FLERBARNSDAGER'
            GROUP BY
              TRANS_ID
          ) FLERBARNSDAGER
          ON FLERBARNSDAGER.TRANS_ID = BEREGNINGSGRUNNLAG_AGG.TRANS_ID
          LEFT JOIN ADOPSJON
          ON BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID = ADOPSJON.FAGSAK_ID
          LEFT JOIN EOS
          ON BEREGNINGSGRUNNLAG_AGG.TRANS_ID = EOS.TRANS_ID
      )
      SELECT /*+ PARALLEL(8) */
        *
 --from uttak_dager
      FROM
        GRUNNLAG
      WHERE
        FAGSAK_ID NOT IN (1679117)
 --where fagsak_id in (1035184)
;
    V_TID_FOM                      VARCHAR2(8) := NULL;
    V_TID_TOM                      VARCHAR2(8) := NULL;
    V_COMMIT                       NUMBER := 0;
    V_ERROR_MELDING                VARCHAR2(1000) := NULL;
    V_DIM_TID_ANTALL               NUMBER := 0;
    V_UTBETALINGSPROSENT_KALKULERT NUMBER := 0;
  BEGIN
    V_TID_FOM := SUBSTR(P_IN_VEDTAK_TOM, 1, 4)
                 || SUBSTR(P_IN_VEDTAK_TOM, 5, 6)-5
                                                  || '01';
    V_TID_TOM := TO_CHAR(LAST_DAY(TO_DATE(P_IN_VEDTAK_TOM, 'yyyymm')), 'yyyymmdd');
 --dbms_output.put_line(v_tid_fom||v_tid_tom);--TEST!!!
    FOR REC_PERIODE IN CUR_PERIODE(P_IN_RAPPORT_DATO, P_IN_FORSKYVNINGER, V_TID_FOM, V_TID_TOM) LOOP
      V_DIM_TID_ANTALL := 0;
      V_UTBETALINGSPROSENT_KALKULERT := 0;
      V_DIM_TID_ANTALL := DIM_TID_ANTALL(TO_NUMBER(TO_CHAR(REC_PERIODE.UTTAK_FOM, 'yyyymmdd')), TO_NUMBER(TO_CHAR(REC_PERIODE.UTTAK_TOM, 'yyyymmdd')));
      IF V_DIM_TID_ANTALL != 0 THEN
        V_UTBETALINGSPROSENT_KALKULERT := ROUND(REC_PERIODE.TREKKDAGER/V_DIM_TID_ANTALL*100, 2);
      ELSE
        V_UTBETALINGSPROSENT_KALKULERT := 0;
      END IF;
      BEGIN
        INSERT INTO DVH_FAM_FP.FAK_FAM_FP_VEDTAK_UTBETALING (
          FAGSAK_ID,
          TRANS_ID,
          BEHANDLINGSTEMA,
          TREKKONTO,
          STONADSDAGER_KVOTE,
          UTTAK_ARBEID_TYPE,
          AAR,
          HALVAAR, --AAR_MAANED,
          RAPPORT_PERIODE,
          UTTAK_FOM,
          UTTAK_TOM,
          DAGER_ERST,
          BEREGNINGSGRUNNLAG_FOM,
          BEREGNINGSGRUNNLAG_TOM,
          DEKNINGSGRAD,
          DAGSATS_BRUKER,
          DAGSATS_ARBEIDSGIVER,
          VIRKSOMHET,
          PERIODE_RESULTAT_AARSAK,
          DAGSATS,
          GRADERINGSPROSENT,
          STATUS_OG_ANDEL_INNTEKTSKAT,
          AKTIVITET_STATUS,
          BRUTTO_INNTEKT,
          AVKORTET_INNTEKT,
          STATUS_OG_ANDEL_BRUTTO,
          STATUS_OG_ANDEL_AVKORTET,
          UTBETALINGSPROSENT,
          FK_DIM_TID_DATO_UTBET_FOM,
          FK_DIM_TID_DATO_UTBET_TOM,
          FUNKSJONELL_TID,
          FORSTE_VEDTAKSDATO,
          VEDTAKSDATO,
          MAX_VEDTAKSDATO,
          PERIODE_TYPE,
          TILFELLE_ERST,
          BELOP,
          DAGSATS_REDUSERT,
          LASTET_DATO,
          MAX_TRANS_ID,
          FK_PERSON1_MOTTAKER,
          FK_PERSON1_ANNEN_PART,
          KJONN,
          FK_PERSON1_BARN,
          TERMINDATO,
          FOEDSELSDATO,
          ANTALL_BARN_TERMIN,
          ANTALL_BARN_FOEDSEL,
          FOEDSELSDATO_ADOPSJON,
          ANTALL_BARN_ADOPSJON,
          ANNENFORELDERFAGSAK_ID,
          MAX_STONADSDAGER_KONTO,
          FK_DIM_PERSON,
          BOSTED_KOMMUNE_NR,
          FK_DIM_GEOGRAFI,
          BYDEL_KOMMUNE_NR,
          KOMMUNE_NR,
          KOMMUNE_NAVN,
          BYDEL_NR,
          BYDEL_NAVN,
          ALENEOMSORG,
          HOVEDKONTONR,
          UNDERKONTONR,
          MOTTAKER_FODSELS_AAR,
          MOTTAKER_FODSELS_MND,
          MOTTAKER_ALDER,
          RETT_TIL_FEDREKVOTE,
          RETT_TIL_MODREKVOTE,
          DAGSATS_ERST,
          TREKKDAGER,
          SAMTIDIG_UTTAK,
          GRADERING,
          GRADERING_INNVILGET,
          ANTALL_DAGER_PERIODE,
          FLERBARNSDAGER,
          UTBETALINGSPROSENT_KALKULERT,
          MIN_UTTAK_FOM,
          MAX_UTTAK_TOM,
          FK_FAM_FP_TREKKONTO,
          FK_FAM_FP_PERIODE_RESULTAT_AARSAK,
          SIVILSTATUS,
          FK_DIM_SIVILSTATUS,
          ANTALL_BEREGNINGSGRUNNLAG,
          GRADERINGSDAGER,
          FK_DIM_TID_MIN_DATO_KVOTE,
          FK_DIM_TID_MAX_DATO_KVOTE,
          ADOPSJONSDATO,
          STEBARNSADOPSJON,
          EOS_SAK,
          MOR_RETTIGHET,
          STATSBORGERSKAP,
          ARBEIDSTIDSPROSENT,
          MORS_AKTIVITET,
          GYLDIG_FLAGG,
          ANDEL_AV_REFUSJON,
          FORSTE_SOKNADSDATO,
          SOKNADSDATO
        ) VALUES (
          REC_PERIODE.FAGSAK_ID,
          REC_PERIODE.TRANS_ID,
          REC_PERIODE.BEHANDLINGSTEMA,
          REC_PERIODE.TREKKONTO,
          REC_PERIODE.STONADSDAGER_KVOTE,
          REC_PERIODE.UTTAK_ARBEID_TYPE,
          REC_PERIODE.AAR,
          REC_PERIODE.HALVAAR --, AAR_MAANED
,
          P_IN_RAPPORT_DATO,
          REC_PERIODE.UTTAK_FOM,
          REC_PERIODE.UTTAK_TOM,
          REC_PERIODE.DAGER_ERST,
          REC_PERIODE.BEREGNINGSGRUNNLAG_FOM,
          REC_PERIODE.BEREGNINGSGRUNNLAG_TOM,
          REC_PERIODE.DEKNINGSGRAD,
          REC_PERIODE.DAGSATS_BRUKER,
          REC_PERIODE.DAGSATS_ARBEIDSGIVER,
          REC_PERIODE.VIRKSOMHET,
          REC_PERIODE.PERIODE_RESULTAT_AARSAK,
          REC_PERIODE.DAGSATS,
          REC_PERIODE.GRADERINGSPROSENT,
          REC_PERIODE.STATUS_OG_ANDEL_INNTEKTSKAT,
          REC_PERIODE.AKTIVITET_STATUS,
          REC_PERIODE.BRUTTO_INNTEKT,
          REC_PERIODE.AVKORTET_INNTEKT,
          REC_PERIODE.STATUS_OG_ANDEL_BRUTTO,
          REC_PERIODE.STATUS_OG_ANDEL_AVKORTET,
          REC_PERIODE.UTBETALINGSPROSENT,
          REC_PERIODE.PK_DIM_TID_DATO_UTBET_FOM,
          REC_PERIODE.PK_DIM_TID_DATO_UTBET_TOM,
          REC_PERIODE.FUNKSJONELL_TID,
          REC_PERIODE.FORSTE_VEDTAKSDATO,
          REC_PERIODE.SISTE_VEDTAKSDATO,
          REC_PERIODE.MAX_VEDTAKSDATO,
          REC_PERIODE.PERIODE,
          REC_PERIODE.TILFELLE_ERST,
          REC_PERIODE.BELOP,
          REC_PERIODE.DAGSATS_REDUSERT,
          REC_PERIODE.LASTET_DATO,
          REC_PERIODE.MAX_TRANS_ID,
          REC_PERIODE.FK_PERSON1_MOTTAKER,
          REC_PERIODE.FK_PERSON1_ANNEN_PART,
          REC_PERIODE.KJONN,
          REC_PERIODE.FK_PERSON1_BARN,
          REC_PERIODE.TERMINDATO,
          REC_PERIODE.FOEDSELSDATO,
          REC_PERIODE.ANTALL_BARN_TERMIN,
          REC_PERIODE.ANTALL_BARN_FOEDSEL,
          REC_PERIODE.FOEDSELSDATO_ADOPSJON,
          REC_PERIODE.ANTALL_BARN_ADOPSJON,
          REC_PERIODE.ANNENFORELDERFAGSAK_ID,
          REC_PERIODE.MAX_STONADSDAGER_KONTO,
          REC_PERIODE.PK_DIM_PERSON,
          REC_PERIODE.BOSTED_KOMMUNE_NR,
          REC_PERIODE.PK_DIM_GEOGRAFI,
          REC_PERIODE.BYDEL_KOMMUNE_NR,
          REC_PERIODE.KOMMUNE_NR,
          REC_PERIODE.KOMMUNE_NAVN,
          REC_PERIODE.BYDEL_NR,
          REC_PERIODE.BYDEL_NAVN,
          REC_PERIODE.ALENEOMSORG,
          REC_PERIODE.HOVEDKONTONR,
          REC_PERIODE.UNDERKONTONR,
          REC_PERIODE.MOTTAKER_FODSELS_AAR,
          REC_PERIODE.MOTTAKER_FODSELS_MND,
          REC_PERIODE.MOTTAKER_ALDER,
          REC_PERIODE.RETT_TIL_FEDREKVOTE,
          REC_PERIODE.RETT_TIL_MØDREKVOTE,
          REC_PERIODE.DAGSATS_ERST,
          REC_PERIODE.TREKKDAGER,
          REC_PERIODE.SAMTIDIG_UTTAK,
          REC_PERIODE.GRADERING,
          REC_PERIODE.GRADERING_INNVILGET,
          V_DIM_TID_ANTALL,
          REC_PERIODE.FLERBARNSDAGER,
          V_UTBETALINGSPROSENT_KALKULERT,
          REC_PERIODE.MIN_UTTAK_FOM,
          REC_PERIODE.MAX_UTTAK_TOM,
          REC_PERIODE.PK_FAM_FP_TREKKONTO,
          REC_PERIODE.PK_FAM_FP_PERIODE_RESULTAT_AARSAK,
          REC_PERIODE.SIVILSTAND,
          REC_PERIODE.FK_DIM_SIVILSTATUS,
          REC_PERIODE.ANTALL_BEREGNINGSGRUNNLAG,
          REC_PERIODE.GRADERINGSDAGER,
          REC_PERIODE.FK_DIM_TID_MIN_DATO_KVOTE,
          REC_PERIODE.FK_DIM_TID_MAX_DATO_KVOTE,
          REC_PERIODE.ADOPSJONSDATO,
          REC_PERIODE.STEBARNSADOPSJON,
          REC_PERIODE.EOS_SAK,
          REC_PERIODE.MOR_RETTIGHET,
          REC_PERIODE.STATSBORGERSKAP,
          REC_PERIODE.ARBEIDSTIDSPROSENT,
          REC_PERIODE.MORS_AKTIVITET,
          P_IN_GYLDIG_FLAGG,
          REC_PERIODE.ANDEL_AV_REFUSJON,
          REC_PERIODE.FORSTE_SOKNADSDATO,
          REC_PERIODE.SOKNADSDATO
        );
        V_COMMIT := V_COMMIT + 1;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK;
          V_ERROR_MELDING := SUBSTR(SQLCODE
                                    || ' '
                                    || SQLERRM, 1, 1000);
          INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
            MIN_LASTET_DATO,
            ID,
            ERROR_MSG,
            OPPRETTET_TID,
            KILDE
          ) VALUES(
            NULL,
            REC_PERIODE.FAGSAK_ID,
            V_ERROR_MELDING,
            SYSDATE,
            'FAM_FP_STATISTIKK_HALVAAR:INSERT'
          );
          COMMIT;
          P_OUT_ERROR := SUBSTR(P_OUT_ERROR
                                || V_ERROR_MELDING, 1, 1000);
      END;
      IF V_COMMIT > 100000 THEN
        COMMIT;
        V_COMMIT := 0;
      END IF;
    END LOOP;
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_ERROR_MELDING := SUBSTR(SQLCODE
                                || ' '
                                || SQLERRM, 1, 1000);
      INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
        MIN_LASTET_DATO,
        ID,
        ERROR_MSG,
        OPPRETTET_TID,
        KILDE
      ) VALUES(
        NULL,
        NULL,
        V_ERROR_MELDING,
        SYSDATE,
        'FAM_FP_STATISTIKK_HALVAAR'
      );
      COMMIT;
      P_OUT_ERROR := SUBSTR(P_OUT_ERROR
                            || V_ERROR_MELDING, 1, 1000);
  END FAM_FP_STATISTIKK_HALVAAR;

  PROCEDURE FAM_FP_STATISTIKK_S(
    P_IN_VEDTAK_TOM IN VARCHAR2,
    P_IN_RAPPORT_DATO IN VARCHAR2,
    P_IN_FORSKYVNINGER IN NUMBER,
    P_IN_GYLDIG_FLAGG IN NUMBER DEFAULT 0,
    P_IN_PERIODE_TYPE IN VARCHAR2 DEFAULT 'S',
    P_OUT_ERROR OUT VARCHAR2
  ) AS
    CURSOR CUR_PERIODE(P_RAPPORT_DATO IN VARCHAR2, P_FORSKYVNINGER IN NUMBER, P_TID_FOM IN VARCHAR2, P_TID_TOM IN VARCHAR2) IS
      WITH FAGSAK AS (
        SELECT
          FAGSAK_ID,
          MAX(BEHANDLINGSTEMA)                                                   AS BEHANDLINGSTEMA,
          MAX(FAGSAKANNENFORELDER_ID)                                            AS ANNENFORELDERFAGSAK_ID,
          MAX(TRANS_ID) KEEP(DENSE_RANK FIRST ORDER BY FUNKSJONELL_TID DESC)     AS MAX_TRANS_ID,
          MAX(SOEKNADSDATO) KEEP(DENSE_RANK FIRST ORDER BY FUNKSJONELL_TID DESC) AS SOKNADSDATO,
          MIN(SOEKNADSDATO)                                                      AS FORSTE_SOKNADSDATO,
          MIN(VEDTAKSDATO)                                                       AS FORSTE_VEDTAKSDATO,
          MAX(FUNKSJONELL_TID)                                                   AS FUNKSJONELL_TID,
          MAX(VEDTAKSDATO)                                                       AS SISTE_VEDTAKSDATO,
          P_IN_PERIODE_TYPE                                                      AS PERIODE,
          LAST_DAY(TO_DATE(P_RAPPORT_DATO, 'yyyymm')) + P_FORSKYVNINGER          AS MAX_VEDTAKSDATO
        FROM
          DVH_FAM_FP.FAM_FP_FAGSAK
        WHERE
          FAM_FP_FAGSAK.FUNKSJONELL_TID <= LAST_DAY(TO_DATE(P_RAPPORT_DATO, 'yyyymm')) + P_FORSKYVNINGER
        GROUP BY
          FAGSAK_ID
      ), TERMIN AS (
        SELECT
          FAGSAK_ID,
          MAX(TERMINDATO)            TERMINDATO,
          MAX(FOEDSELSDATO)          FOEDSELSDATO,
          MAX(ANTALL_BARN_TERMIN)    ANTALL_BARN_TERMIN,
          MAX(ANTALL_BARN_FOEDSEL)   ANTALL_BARN_FOEDSEL,
          MAX(FOEDSELSDATO_ADOPSJON) FOEDSELSDATO_ADOPSJON,
          MAX(ANTALL_BARN_ADOPSJON) ANTALL_BARN_ADOPSJON
        FROM
          (
            SELECT
              FAM_FP_FAGSAK.FAGSAK_ID,
              MAX(FODSEL.TERMINDATO)              TERMINDATO,
              MAX(FODSEL.FOEDSELSDATO)            FOEDSELSDATO,
              MAX(FODSEL.ANTALL_BARN_FOEDSEL)     ANTALL_BARN_FOEDSEL,
              MAX(FODSEL.ANTALL_BARN_TERMIN)      ANTALL_BARN_TERMIN,
              MAX(ADOPSJON.FOEDSELSDATO_ADOPSJON) FOEDSELSDATO_ADOPSJON,
              COUNT(ADOPSJON.TRANS_ID)            ANTALL_BARN_ADOPSJON
            FROM
              DVH_FAM_FP.FAM_FP_FAGSAK
              LEFT JOIN DVH_FAM_FP.FAM_FP_FODSELTERMIN FODSEL
              ON FODSEL.FAGSAK_ID = FAM_FP_FAGSAK.FAGSAK_ID
              AND UPPER(FAM_FP_FAGSAK.BEHANDLINGSTEMA) = 'FORP_FODS'
              LEFT JOIN DVH_FAM_FP.FAM_FP_FODSELTERMIN ADOPSJON
              ON ADOPSJON.FAGSAK_ID = FAM_FP_FAGSAK.FAGSAK_ID
              AND ADOPSJON.TRANS_ID = FAM_FP_FAGSAK.TRANS_ID
              AND UPPER(FAM_FP_FAGSAK.BEHANDLINGSTEMA) = 'FORP_ADOP'
            GROUP BY
              FAM_FP_FAGSAK.FAGSAK_ID,
              FAM_FP_FAGSAK.TRANS_ID
          )
        GROUP BY
          FAGSAK_ID
      ), FK_PERSON1 AS (
        SELECT
          PERSON.PERSON,
          PERSON.FAGSAK_ID,
          MAX(PERSON.BEHANDLINGSTEMA)                                                                             AS BEHANDLINGSTEMA,
          PERSON.MAX_TRANS_ID,
          MAX(PERSON.ANNENFORELDERFAGSAK_ID)                                                                      AS ANNENFORELDERFAGSAK_ID,
          PERSON.AKTOER_ID,
          MAX(PERSON.KJONN)                                                                                       AS KJONN,
          MAX(PERSON_67_VASKET.FK_PERSON1) KEEP (DENSE_RANK FIRST ORDER BY PERSON_67_VASKET.GYLDIG_FRA_DATO DESC) AS FK_PERSON1,
          MAX(FOEDSELSDATO)                                                                                       AS FOEDSELSDATO,
          MAX(SIVILSTAND)                                                                                         AS SIVILSTAND,
          MAX(STATSBORGERSKAP)                                                                                    AS STATSBORGERSKAP
        FROM
          (
            SELECT
              'MOTTAKER'                                AS PERSON,
              FAGSAK.FAGSAK_ID,
              FAGSAK.BEHANDLINGSTEMA,
              FAGSAK.MAX_TRANS_ID,
              FAGSAK.ANNENFORELDERFAGSAK_ID,
              FAM_FP_PERSONOPPLYSNINGER.AKTOER_ID,
              FAM_FP_PERSONOPPLYSNINGER.KJONN,
              FAM_FP_PERSONOPPLYSNINGER.FOEDSELSDATO,
              FAM_FP_PERSONOPPLYSNINGER.SIVILSTAND,
              FAM_FP_PERSONOPPLYSNINGER.STATSBORGERSKAP
            FROM
              DVH_FAM_FP.FAM_FP_PERSONOPPLYSNINGER
              JOIN FAGSAK
              ON FAM_FP_PERSONOPPLYSNINGER.TRANS_ID = FAGSAK.MAX_TRANS_ID UNION ALL
              SELECT
                'BARN'                                    AS PERSON,
                FAGSAK.FAGSAK_ID,
                MAX(FAGSAK.BEHANDLINGSTEMA)               AS BEHANDLINGSTEMA,
                FAGSAK.MAX_TRANS_ID,
                MAX(FAGSAK.ANNENFORELDERFAGSAK_ID)        ANNENFORELDERFAGSAK_ID,
                MAX(FAM_FP_FAMILIEHENDELSE.TIL_AKTOER_ID) AS AKTOER_ID,
                MAX(FAM_FP_FAMILIEHENDELSE.KJOENN)        AS KJONN,
                NULL                                      AS FOEDSELSDATO,
                NULL                                      AS SIVILSTAND,
                NULL                                      AS STATSBORGERSKAP
              FROM
                DVH_FAM_FP.FAM_FP_FAMILIEHENDELSE
                JOIN FAGSAK
                ON FAM_FP_FAMILIEHENDELSE.FAGSAK_ID = FAGSAK.FAGSAK_ID
              WHERE
                UPPER(FAM_FP_FAMILIEHENDELSE.RELASJON) = 'BARN'
              GROUP BY
                FAGSAK.FAGSAK_ID, FAGSAK.MAX_TRANS_ID
          )                                    PERSON
          JOIN DT_PERSON.DVH_PERSON_IDENT_AKTOR_IKKE_SKJERMET PERSON_67_VASKET
          ON PERSON_67_VASKET.AKTOR_ID = PERSON.AKTOER_ID
          AND TO_DATE(P_TID_TOM, 'yyyymmdd') BETWEEN PERSON_67_VASKET.GYLDIG_FRA_DATO
          AND PERSON_67_VASKET.GYLDIG_TIL_DATO
        GROUP BY
          PERSON.PERSON, PERSON.FAGSAK_ID, PERSON.MAX_TRANS_ID, PERSON.AKTOER_ID
      ), BARN AS (
        SELECT
          FAGSAK_ID,
          LISTAGG(FK_PERSON1, ',') WITHIN GROUP (ORDER BY FK_PERSON1) AS FK_PERSON1_BARN
        FROM
          FK_PERSON1
        WHERE
          PERSON = 'BARN'
        GROUP BY
          FAGSAK_ID
      ), MOTTAKER AS (
        SELECT
          FK_PERSON1.FAGSAK_ID,
          FK_PERSON1.BEHANDLINGSTEMA,
          FK_PERSON1.MAX_TRANS_ID,
          FK_PERSON1.ANNENFORELDERFAGSAK_ID,
          FK_PERSON1.AKTOER_ID,
          FK_PERSON1.KJONN,
          FK_PERSON1.FK_PERSON1                       AS FK_PERSON1_MOTTAKER,
          EXTRACT(YEAR FROM FK_PERSON1.FOEDSELSDATO) AS MOTTAKER_FODSELS_AAR,
          EXTRACT(MONTH FROM FK_PERSON1.FOEDSELSDATO) AS MOTTAKER_FODSELS_MND,
          FK_PERSON1.SIVILSTAND,
          FK_PERSON1.STATSBORGERSKAP,
          BARN.FK_PERSON1_BARN,
          TERMIN.TERMINDATO,
          TERMIN.FOEDSELSDATO,
          TERMIN.ANTALL_BARN_TERMIN,
          TERMIN.ANTALL_BARN_FOEDSEL,
          TERMIN.FOEDSELSDATO_ADOPSJON,
          TERMIN.ANTALL_BARN_ADOPSJON
        FROM
          FK_PERSON1
          LEFT JOIN BARN
          ON BARN.FAGSAK_ID = FK_PERSON1.FAGSAK_ID
          LEFT JOIN TERMIN
          ON FK_PERSON1.FAGSAK_ID = TERMIN.FAGSAK_ID
        WHERE
          FK_PERSON1.PERSON = 'MOTTAKER'
      ), ADOPSJON AS (
        SELECT
          FAM_FP_VILKAAR.FAGSAK_ID,
          MAX(FAM_FP_VILKAAR.OMSORGS_OVERTAKELSESDATO) AS ADOPSJONSDATO,
          MAX(FAM_FP_VILKAAR.EKTEFELLES_BARN)          AS STEBARNSADOPSJON
        FROM
          FAGSAK
          JOIN DVH_FAM_FP.FAM_FP_VILKAAR
          ON FAGSAK.FAGSAK_ID = FAM_FP_VILKAAR.FAGSAK_ID
        WHERE
          FAGSAK.BEHANDLINGSTEMA = 'FORP_ADOP'
        GROUP BY
          FAM_FP_VILKAAR.FAGSAK_ID
      ), EOS AS (
        SELECT
          A.TRANS_ID,
          CASE
            WHEN UPPER(ER_BORGER_AV_EU_EOS) = 'TRUE' THEN
              'J'
            WHEN UPPER(ER_BORGER_AV_EU_EOS) = 'FALSE' THEN
              'N'
            ELSE
              NULL
          END EOS_SAK
        FROM
          (
            SELECT
              FAM_FP_VILKAAR.TRANS_ID,
              MAX(FAM_FP_VILKAAR.ER_BORGER_AV_EU_EOS) AS ER_BORGER_AV_EU_EOS
            FROM
              FAGSAK
              JOIN DVH_FAM_FP.FAM_FP_VILKAAR
              ON FAGSAK.MAX_TRANS_ID = FAM_FP_VILKAAR.TRANS_ID
              AND LENGTH(FAM_FP_VILKAAR.PERSON_STATUS) > 0
            GROUP BY
              FAM_FP_VILKAAR.TRANS_ID
          ) A
      ), ANNENFORELDERFAGSAK AS (
        SELECT
          ANNENFORELDERFAGSAK.*,
          MOTTAKER.FK_PERSON1_MOTTAKER AS FK_PERSON1_ANNEN_PART
        FROM
          (
            SELECT
              FAGSAK_ID,
              MAX_TRANS_ID,
              MAX(ANNENFORELDERFAGSAK_ID) AS ANNENFORELDERFAGSAK_ID
            FROM
              (
                SELECT
                  FORELDER1.FAGSAK_ID,
                  FORELDER1.MAX_TRANS_ID,
                  NVL(FORELDER1.ANNENFORELDERFAGSAK_ID, FORELDER2.FAGSAK_ID) AS ANNENFORELDERFAGSAK_ID
                FROM
                  MOTTAKER FORELDER1
                  JOIN MOTTAKER FORELDER2
                  ON FORELDER1.FK_PERSON1_BARN = FORELDER2.FK_PERSON1_BARN
                  AND FORELDER1.FK_PERSON1_MOTTAKER != FORELDER2.FK_PERSON1_MOTTAKER
              )
            GROUP BY
              FAGSAK_ID,
              MAX_TRANS_ID
          )        ANNENFORELDERFAGSAK
          JOIN MOTTAKER
          ON ANNENFORELDERFAGSAK.ANNENFORELDERFAGSAK_ID = MOTTAKER.FAGSAK_ID
      ), TID AS (
        SELECT
          PK_DIM_TID,
          DATO,
          AAR,
          HALVAAR,
          KVARTAL,
          AAR_MAANED
        FROM
          DT_KODEVERK.DIM_TID
        WHERE
          DAG_I_UKE < 6
          AND DIM_NIVAA = 1
          AND GYLDIG_FLAGG = 1
          AND PK_DIM_TID BETWEEN P_TID_FOM AND P_TID_TOM
          AND PK_DIM_TID <= TO_CHAR(LAST_DAY(TO_DATE(P_RAPPORT_DATO, 'yyyymm')), 'yyyymmdd')
      ), UTTAK AS (
        SELECT
          UTTAK.TRANS_ID,
          UTTAK.TREKKONTO,
          UTTAK.UTTAK_ARBEID_TYPE,
          UTTAK.VIRKSOMHET,
          UTTAK.UTBETALINGSPROSENT,
          UTTAK.GRADERING_INNVILGET,
          UTTAK.GRADERING,
          UTTAK.ARBEIDSTIDSPROSENT,
          UTTAK.SAMTIDIG_UTTAK,
          UTTAK.PERIODE_RESULTAT_AARSAK,
          UTTAK.FOM                                      AS UTTAK_FOM,
          UTTAK.TOM                                      AS UTTAK_TOM,
          UTTAK.TREKKDAGER,
          FAGSAK.FAGSAK_ID,
          FAGSAK.PERIODE,
          FAGSAK.FUNKSJONELL_TID,
          FAGSAK.FORSTE_VEDTAKSDATO,
          FAGSAK.SISTE_VEDTAKSDATO,
          FAGSAK.MAX_VEDTAKSDATO,
          FAGSAK.FORSTE_SOKNADSDATO,
          FAGSAK.SOKNADSDATO,
          FAM_FP_TREKKONTO.PK_FAM_FP_TREKKONTO,
          AARSAK_UTTAK.PK_FAM_FP_PERIODE_RESULTAT_AARSAK,
          UTTAK.ARBEIDSFORHOLD_ID,
          UTTAK.GRADERINGSDAGER,
          FAM_FP_UTTAK_FORDELINGSPER.MORS_AKTIVITET
        FROM
          DVH_FAM_FP.FAM_FP_UTTAK_RES_PER_AKTIV UTTAK
          JOIN FAGSAK
          ON FAGSAK.MAX_TRANS_ID = UTTAK.TRANS_ID LEFT JOIN DVH_FAM_FP.FAM_FP_TREKKONTO
          ON UPPER(UTTAK.TREKKONTO) = FAM_FP_TREKKONTO.TREKKONTO
          LEFT JOIN (
            SELECT
              AARSAK_UTTAK,
              MAX(PK_FAM_FP_PERIODE_RESULTAT_AARSAK) AS PK_FAM_FP_PERIODE_RESULTAT_AARSAK
            FROM
              DVH_FAM_FP.FAM_FP_PERIODE_RESULTAT_AARSAK
            GROUP BY
              AARSAK_UTTAK
          ) AARSAK_UTTAK
          ON UPPER(UTTAK.PERIODE_RESULTAT_AARSAK) = AARSAK_UTTAK.AARSAK_UTTAK
          LEFT JOIN DVH_FAM_FP.FAM_FP_UTTAK_FORDELINGSPER
          ON FAM_FP_UTTAK_FORDELINGSPER.TRANS_ID = UTTAK.TRANS_ID
          AND UTTAK.FOM BETWEEN FAM_FP_UTTAK_FORDELINGSPER.FOM
          AND FAM_FP_UTTAK_FORDELINGSPER.TOM
          AND UPPER(UTTAK.TREKKONTO) = UPPER(FAM_FP_UTTAK_FORDELINGSPER.PERIODE_TYPE)
          AND LENGTH(FAM_FP_UTTAK_FORDELINGSPER.MORS_AKTIVITET) > 1
        WHERE
          UTTAK.UTBETALINGSPROSENT > 0
      ), STONADSDAGER_KVOTE AS (
        SELECT
          UTTAK.*,
          TID1.PK_DIM_TID AS FK_DIM_TID_MIN_DATO_KVOTE,
          TID2.PK_DIM_TID AS FK_DIM_TID_MAX_DATO_KVOTE
        FROM
          (
            SELECT
              FAGSAK_ID,
              TRANS_ID,
              TREKKONTO,
              VIRKSOMHET,
              UTTAK_ARBEID_TYPE,
              SUM(TREKKDAGER)   AS STONADSDAGER_KVOTE,
              MIN(UTTAK_FOM)    AS MIN_UTTAK_FOM,
              MAX(UTTAK_TOM)    AS MAX_UTTAK_TOM
            FROM
              (
                SELECT
                  FAGSAK_ID,
                  TRANS_ID,
                  UTTAK_FOM,
                  UTTAK_TOM,
                  TREKKONTO,
                  VIRKSOMHET,
                  UTTAK_ARBEID_TYPE,
                  MAX(TREKKDAGER)   AS TREKKDAGER
                FROM
                  UTTAK
                GROUP BY
                  FAGSAK_ID,
                  TRANS_ID,
                  UTTAK_FOM,
                  UTTAK_TOM,
                  TREKKONTO,
                  VIRKSOMHET,
                  UTTAK_ARBEID_TYPE
              ) A
            GROUP BY
              FAGSAK_ID,
              TRANS_ID,
              TREKKONTO,
              VIRKSOMHET,
              UTTAK_ARBEID_TYPE
          )                   UTTAK
          JOIN DT_KODEVERK.DIM_TID TID1
          ON TID1.DIM_NIVAA = 1
          AND TID1.DATO = TRUNC(UTTAK.MIN_UTTAK_FOM, 'dd') JOIN DT_KODEVERK.DIM_TID TID2
          ON TID2.DIM_NIVAA = 1
          AND TID2.DATO = TRUNC(UTTAK.MAX_UTTAK_TOM,
          'dd')
      ), UTTAK_DAGER AS (
        SELECT
          UTTAK.*,
          TID.PK_DIM_TID,
          TID.DATO,
          TID.AAR,
          TID.HALVAAR,
          TID.KVARTAL,
          TID.AAR_MAANED
        FROM
          UTTAK
          JOIN TID
          ON TID.DATO BETWEEN UTTAK.UTTAK_FOM
          AND UTTAK.UTTAK_TOM
      ), ALENEOMSORG AS (
        SELECT
          UTTAK.FAGSAK_ID,
          UTTAK.UTTAK_FOM
        FROM
          UTTAK
          JOIN DVH_FAM_FP.FAM_FP_DOKUMENTASJONSPERIODER DOK1
          ON DOK1.FAGSAK_ID = UTTAK.FAGSAK_ID
          AND UTTAK.UTTAK_FOM >= DOK1.FOM
          AND DOK1.DOKUMENTASJON_TYPE IN ('ALENEOMSORG', 'ALENEOMSORG_OVERFØRING') LEFT JOIN DVH_FAM_FP.FAM_FP_DOKUMENTASJONSPERIODER DOK2
          ON DOK1.FAGSAK_ID = DOK2.FAGSAK_ID
          AND UTTAK.UTTAK_FOM >= DOK2.FOM
          AND DOK1.TRANS_ID < DOK2.TRANS_ID
          AND DOK2.DOKUMENTASJON_TYPE = 'ANNEN_FORELDER_HAR_RETT'
          AND DOK2.FAGSAK_ID IS NULL
        GROUP BY
          UTTAK.FAGSAK_ID,
          UTTAK.UTTAK_FOM
      ), BEREGNINGSGRUNNLAG AS (
        SELECT
          FAGSAK_ID,
          TRANS_ID,
          VIRKSOMHETSNUMMER,
          MAX(STATUS_OG_ANDEL_BRUTTO)         AS STATUS_OG_ANDEL_BRUTTO,
          MAX(STATUS_OG_ANDEL_AVKORTET)       AS STATUS_OG_ANDEL_AVKORTET,
          FOM                                 AS BEREGNINGSGRUNNLAG_FOM,
          TOM                                 AS BEREGNINGSGRUNNLAG_TOM,
          MAX(DEKNINGSGRAD)                   AS DEKNINGSGRAD,
          MAX(DAGSATS)                        AS DAGSATS,
          DAGSATS_BRUKER,
          DAGSATS_ARBEIDSGIVER,
          DAGSATS_BRUKER+DAGSATS_ARBEIDSGIVER DAGSATS_VIRKSOMHET,
          MAX(STATUS_OG_ANDEL_INNTEKTSKAT)    AS STATUS_OG_ANDEL_INNTEKTSKAT,
          AKTIVITET_STATUS,
          MAX(BRUTTO)                         AS BRUTTO_INNTEKT,
          MAX(AVKORTET)                       AS AVKORTET_INNTEKT,
          COUNT(1)                            AS ANTALL_BEREGNINGSGRUNNLAG
        FROM
          DVH_FAM_FP.FAM_FP_BEREGNINGSGRUNNLAG
        GROUP BY
          FAGSAK_ID,
          TRANS_ID,
          VIRKSOMHETSNUMMER,
          FOM,
          TOM,
          AKTIVITET_STATUS,
          DAGSATS_BRUKER,
          DAGSATS_ARBEIDSGIVER
      ), BEREGNINGSGRUNNLAG_DETALJ AS (
        SELECT
          UTTAK_DAGER.*,
          STONADSDAGER_KVOTE.STONADSDAGER_KVOTE,
          STONADSDAGER_KVOTE.MIN_UTTAK_FOM,
          STONADSDAGER_KVOTE.MAX_UTTAK_TOM,
          STONADSDAGER_KVOTE.FK_DIM_TID_MIN_DATO_KVOTE,
          STONADSDAGER_KVOTE.FK_DIM_TID_MAX_DATO_KVOTE,
          BEREG.STATUS_OG_ANDEL_BRUTTO,
          BEREG.STATUS_OG_ANDEL_AVKORTET,
          BEREG.BEREGNINGSGRUNNLAG_FOM,
          BEREG.DEKNINGSGRAD,
          BEREG.BEREGNINGSGRUNNLAG_TOM,
          BEREG.DAGSATS,
          BEREG.DAGSATS_BRUKER,
          BEREG.DAGSATS_ARBEIDSGIVER,
          BEREG.DAGSATS_VIRKSOMHET,
          BEREG.STATUS_OG_ANDEL_INNTEKTSKAT,
          BEREG.AKTIVITET_STATUS,
          BEREG.BRUTTO_INNTEKT,
          BEREG.AVKORTET_INNTEKT,
          BEREG.DAGSATS*UTTAK_DAGER.UTBETALINGSPROSENT/100 AS DAGSATS_ERST,
          BEREG.ANTALL_BEREGNINGSGRUNNLAG
        FROM
          BEREGNINGSGRUNNLAG                        BEREG
          JOIN UTTAK_DAGER
          ON UTTAK_DAGER.TRANS_ID = BEREG.TRANS_ID
          AND NVL(UTTAK_DAGER.VIRKSOMHET, 'X') = NVL(BEREG.VIRKSOMHETSNUMMER, 'X')
          AND BEREG.BEREGNINGSGRUNNLAG_FOM <= UTTAK_DAGER.DATO
          AND NVL(BEREG.BEREGNINGSGRUNNLAG_TOM, TO_DATE('20991201', 'YYYYMMDD')) >= UTTAK_DAGER.DATO LEFT JOIN STONADSDAGER_KVOTE
          ON UTTAK_DAGER.TRANS_ID = STONADSDAGER_KVOTE.TRANS_ID
          AND UTTAK_DAGER.TREKKONTO = STONADSDAGER_KVOTE.TREKKONTO
          AND NVL(UTTAK_DAGER.VIRKSOMHET,
          'X') = NVL(STONADSDAGER_KVOTE.VIRKSOMHET,
          'X')
          AND UTTAK_DAGER.UTTAK_ARBEID_TYPE = STONADSDAGER_KVOTE.UTTAK_ARBEID_TYPE
          JOIN DVH_FAM_FP.FAM_FP_UTTAK_AKTIVITET_MAPPING UTTAK_MAPPING
          ON UTTAK_DAGER.UTTAK_ARBEID_TYPE = UTTAK_MAPPING.UTTAK_ARBEID
          AND BEREG.AKTIVITET_STATUS = UTTAK_MAPPING.AKTIVITET_STATUS
        WHERE
          BEREG.DAGSATS_BRUKER + BEREG.DAGSATS_ARBEIDSGIVER != 0
      ), BEREGNINGSGRUNNLAG_AGG AS (
        SELECT
          A.*,
          DAGER_ERST*DAGSATS_VIRKSOMHET/DAGSATS*ANTALL_BEREGNINGSGRUNNLAG                                                                                                                    TILFELLE_ERST,
          DAGER_ERST*ROUND(UTBETALINGSPROSENT/100*DAGSATS_VIRKSOMHET)                                                                                                                        BELOP,
          ROUND(UTBETALINGSPROSENT/100*DAGSATS_VIRKSOMHET-0.5)                                                                                                                               DAGSATS_REDUSERT,
          CASE
            WHEN PERIODE_RESULTAT_AARSAK IN (2004, 2033) THEN
              'N'
            WHEN TREKKONTO IN ('FEDREKVOTE', 'FELLESPERIODE', 'MØDREKVOTE') THEN
              'J'
            WHEN TREKKONTO = 'FORELDREPENGER' THEN
              'N'
          END MOR_RETTIGHET
        FROM
          (
            SELECT
              FAGSAK_ID,
              TRANS_ID,
              TREKKONTO,
              TREKKDAGER,
              STONADSDAGER_KVOTE,
              UTTAK_ARBEID_TYPE,
              AAR --, halvaar, kvartal, aar_maaned
,
              UTTAK_FOM,
              UTTAK_TOM,
              SUM(DAGSATS_VIRKSOMHET/DAGSATS*
                CASE
                  WHEN ((UPPER(GRADERING_INNVILGET) ='TRUE'
                  AND UPPER(GRADERING)='TRUE')
                  OR UPPER(SAMTIDIG_UTTAK)='TRUE') THEN
                    (100-ARBEIDSTIDSPROSENT)/100
                  ELSE
                    1.0
                END )                                DAGER_ERST2,
              MAX(ARBEIDSTIDSPROSENT)                AS ARBEIDSTIDSPROSENT,
              COUNT(DISTINCT PK_DIM_TID)             DAGER_ERST,
 --count(distinct pk_dim_tid)*dagsats_virksomhet/dagsats tilfelle_erst,
              MIN(BEREGNINGSGRUNNLAG_FOM)            BEREGNINGSGRUNNLAG_FOM,
              MAX(BEREGNINGSGRUNNLAG_TOM)            BEREGNINGSGRUNNLAG_TOM,
              DEKNINGSGRAD,
 --count(distinct pk_dim_tid)*
 --      round(utbetalingsprosent/100*dagsats_virksomhet-0.5) belop,
              DAGSATS_BRUKER,
              DAGSATS_ARBEIDSGIVER,
              DAGSATS_VIRKSOMHET,
              VIRKSOMHET,
              PERIODE_RESULTAT_AARSAK,
              DAGSATS,
              DAGSATS_ERST, --dagsats_virksomhet,
              UTBETALINGSPROSENT                     GRADERINGSPROSENT,
              STATUS_OG_ANDEL_INNTEKTSKAT,
              AKTIVITET_STATUS,
              BRUTTO_INNTEKT,
              AVKORTET_INNTEKT,
              STATUS_OG_ANDEL_BRUTTO,
              STATUS_OG_ANDEL_AVKORTET,
 --round(utbetalingsprosent/100*dagsats_virksomhet-0.5) dagsats_redusert,
              UTBETALINGSPROSENT,
              MIN(PK_DIM_TID)                        PK_DIM_TID_DATO_UTBET_FOM,
              MAX(PK_DIM_TID)                        PK_DIM_TID_DATO_UTBET_TOM,
              FUNKSJONELL_TID,
              FORSTE_VEDTAKSDATO,
              SISTE_VEDTAKSDATO,
              MAX_VEDTAKSDATO,
              PERIODE,
              MAX(FORSTE_SOKNADSDATO)                AS FORSTE_SOKNADSDATO,
              MAX(SOKNADSDATO)                       AS SOKNADSDATO,
              SAMTIDIG_UTTAK,
              GRADERING,
              GRADERING_INNVILGET,
              MIN_UTTAK_FOM,
              MAX_UTTAK_TOM,
              FK_DIM_TID_MIN_DATO_KVOTE,
              FK_DIM_TID_MAX_DATO_KVOTE,
              MAX(PK_FAM_FP_TREKKONTO)               AS PK_FAM_FP_TREKKONTO,
              MAX(PK_FAM_FP_PERIODE_RESULTAT_AARSAK) AS PK_FAM_FP_PERIODE_RESULTAT_AARSAK,
              ANTALL_BEREGNINGSGRUNNLAG,
              MAX(GRADERINGSDAGER)                   AS GRADERINGSDAGER,
              MAX(MORS_AKTIVITET)                    AS MORS_AKTIVITET
            FROM
              BEREGNINGSGRUNNLAG_DETALJ
            GROUP BY
              FAGSAK_ID,
              TRANS_ID,
              TREKKONTO,
              TREKKDAGER,
              STONADSDAGER_KVOTE,
              UTTAK_ARBEID_TYPE,
              AAR --, halvaar, kvartal, aar_maaned
,
              UTTAK_FOM,
              UTTAK_TOM,
              DEKNINGSGRAD,
              VIRKSOMHET,
              UTBETALINGSPROSENT,
              PERIODE_RESULTAT_AARSAK,
              DAGSATS,
              DAGSATS_ERST,
              DAGSATS_BRUKER,
              DAGSATS_ARBEIDSGIVER,
              DAGSATS_VIRKSOMHET,
              UTBETALINGSPROSENT,
              STATUS_OG_ANDEL_INNTEKTSKAT,
              AKTIVITET_STATUS,
              BRUTTO_INNTEKT,
              AVKORTET_INNTEKT,
              STATUS_OG_ANDEL_BRUTTO,
              STATUS_OG_ANDEL_AVKORTET,
              FUNKSJONELL_TID,
              FORSTE_VEDTAKSDATO,
              SISTE_VEDTAKSDATO,
              MAX_VEDTAKSDATO,
              PERIODE,
              SAMTIDIG_UTTAK,
              GRADERING,
              GRADERING_INNVILGET,
              MIN_UTTAK_FOM,
              MAX_UTTAK_TOM,
              FK_DIM_TID_MIN_DATO_KVOTE,
              FK_DIM_TID_MAX_DATO_KVOTE,
              ANTALL_BEREGNINGSGRUNNLAG
          ) A
      ), GRUNNLAG AS (
        SELECT
          BEREGNINGSGRUNNLAG_AGG.*,
          SYSDATE                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      AS LASTET_DATO,
          MOTTAKER.BEHANDLINGSTEMA,
          MOTTAKER.MAX_TRANS_ID,
          MOTTAKER.FK_PERSON1_MOTTAKER,
          MOTTAKER.KJONN,
          MOTTAKER.FK_PERSON1_BARN,
          MOTTAKER.TERMINDATO,
          MOTTAKER.FOEDSELSDATO,
          MOTTAKER.ANTALL_BARN_TERMIN,
          MOTTAKER.ANTALL_BARN_FOEDSEL,
          MOTTAKER.FOEDSELSDATO_ADOPSJON,
          MOTTAKER.ANTALL_BARN_ADOPSJON,
          MOTTAKER.MOTTAKER_FODSELS_AAR,
          MOTTAKER.MOTTAKER_FODSELS_MND,
          SUBSTR(P_TID_FOM, 1, 4) - MOTTAKER.MOTTAKER_FODSELS_AAR                                                                                                                                                                                                                                                                                                                                                                                                                                                      AS MOTTAKER_ALDER,
          MOTTAKER.SIVILSTAND,
          MOTTAKER.STATSBORGERSKAP,
          DIM_PERSON.PK_DIM_PERSON,
          DIM_PERSON.BOSTED_KOMMUNE_NR,
          DIM_PERSON.FK_DIM_SIVILSTATUS,
          DIM_GEOGRAFI.PK_DIM_GEOGRAFI,
          DIM_GEOGRAFI.BYDEL_KOMMUNE_NR,
          DIM_GEOGRAFI.KOMMUNE_NR,
          DIM_GEOGRAFI.KOMMUNE_NAVN,
          DIM_GEOGRAFI.BYDEL_NR,
          DIM_GEOGRAFI.BYDEL_NAVN,
          ANNENFORELDERFAGSAK.ANNENFORELDERFAGSAK_ID,
          ANNENFORELDERFAGSAK.FK_PERSON1_ANNEN_PART,
          FAM_FP_UTTAK_FP_KONTOER.MAX_DAGER                                                                                                                                                                                                                                                                                                                                                                                                                                                                            MAX_STONADSDAGER_KONTO,
          CASE
            WHEN ALENEOMSORG.FAGSAK_ID IS NOT NULL THEN
              'J'
            ELSE
              NULL
          END AS                                                                                                                                                                                                                                                                                                                                                                                                                                           ALENEOMSORG,
          CASE
            WHEN BEHANDLINGSTEMA = 'FORP_FODS' THEN
              '214'
            WHEN BEHANDLINGSTEMA = 'FORP_ADOP' THEN
              '216'
          END AS                                                                                                                                                                                                                                                                                                                                                                                                         HOVEDKONTONR,
          CASE
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='ARBEIDSTAKER' AND DAGSATS_ARBEIDSGIVER/DAGSATS*100<=50 THEN
              '1000'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='ARBEIDSTAKER' AND DAGSATS_ARBEIDSGIVER/DAGSATS*100>50 THEN
              '8020'
 --when status_og_andel_inntektskat='ARBEIDSTAKER' then '1000'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='JORDBRUKER' THEN
              '5210'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='SJØMANN' THEN
              '1300'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='SELVSTENDIG_NÆRINGSDRIVENDE' THEN
              '5010'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='DAGPENGER' THEN
              '1200'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='ARBEIDSTAKER_UTEN_FERIEPENGER' THEN
              '1000'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='FISKER' THEN
              '5300'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='DAGMAMMA' THEN
              '5110'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='FRILANSER' THEN
              '1100'
          END AS UNDERKONTONR,
          ROUND(DAGSATS_ARBEIDSGIVER/DAGSATS*100, 0)                                                                                                                                                                                                                                                                                                                                                                                                                                                                   AS ANDEL_AV_REFUSJON,
          CASE
            WHEN RETT_TIL_MØDREKVOTE.TRANS_ID IS NULL THEN
              'N'
            ELSE
              'J'
          END AS                                                                                                                                                                                                                                                                                                                                                                                                                                         RETT_TIL_MØDREKVOTE,
          CASE
            WHEN RETT_TIL_FEDREKVOTE.TRANS_ID IS NULL THEN
              'N'
            ELSE
              'J'
          END AS                                                                                                                                                                                                                                                                                                                                                                                                                                         RETT_TIL_FEDREKVOTE,
          FLERBARNSDAGER.FLERBARNSDAGER,
          ADOPSJON.ADOPSJONSDATO,
          ADOPSJON.STEBARNSADOPSJON,
          EOS.EOS_SAK
        FROM
          BEREGNINGSGRUNNLAG_AGG
          LEFT JOIN MOTTAKER
          ON BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID = MOTTAKER.FAGSAK_ID
          AND BEREGNINGSGRUNNLAG_AGG.TRANS_ID = MOTTAKER.MAX_TRANS_ID
          LEFT JOIN ANNENFORELDERFAGSAK
          ON BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID = ANNENFORELDERFAGSAK.FAGSAK_ID
          AND BEREGNINGSGRUNNLAG_AGG.TRANS_ID = ANNENFORELDERFAGSAK.MAX_TRANS_ID
          LEFT JOIN DVH_FAM_FP.FAM_FP_UTTAK_FP_KONTOER
          ON BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID = FAM_FP_UTTAK_FP_KONTOER.FAGSAK_ID
          AND MOTTAKER.MAX_TRANS_ID = FAM_FP_UTTAK_FP_KONTOER.TRANS_ID
 --AND UPPER(REGEXP_REPLACE(grunnlag_drp1.TREKKONTO, '_|-|[[:space:]]', '')) = UPPER(REGEXP_REPLACE(FAM_FP_Uttak_FP_Kontoer.STOENADSKONTOTYPE, '_|-|[[:space:]]', ''))
          AND UPPER(REPLACE(BEREGNINGSGRUNNLAG_AGG.TREKKONTO,
          '_',
          '')) = UPPER(REPLACE(FAM_FP_UTTAK_FP_KONTOER.STOENADSKONTOTYPE,
          ' ',
          ''))
          LEFT JOIN DT_PERSON.DIM_PERSON
          ON MOTTAKER.FK_PERSON1_MOTTAKER = DIM_PERSON.FK_PERSON1
 --and beregningsgrunnlag_agg.uttak_tom between dim_person.gyldig_fra_dato and dim_person.gyldig_til_dato
          AND TO_DATE(BEREGNINGSGRUNNLAG_AGG.PK_DIM_TID_DATO_UTBET_TOM,
          'yyyymmdd') BETWEEN DIM_PERSON.GYLDIG_FRA_DATO
          AND DIM_PERSON.GYLDIG_TIL_DATO
          LEFT JOIN DT_KODEVERK.DIM_GEOGRAFI
          ON DIM_PERSON.FK_DIM_GEOGRAFI_BOSTED = DIM_GEOGRAFI.PK_DIM_GEOGRAFI
          LEFT JOIN ALENEOMSORG
          ON ALENEOMSORG.FAGSAK_ID = BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID
          AND ALENEOMSORG.UTTAK_FOM = BEREGNINGSGRUNNLAG_AGG.UTTAK_FOM
          LEFT JOIN (
            SELECT
              TRANS_ID
            FROM
              DVH_FAM_FP.FAM_FP_UTTAK_FP_KONTOER
            WHERE
              UPPER(STOENADSKONTOTYPE) = 'MØDREKVOTE'
            GROUP BY
              TRANS_ID
          ) RETT_TIL_MØDREKVOTE
          ON RETT_TIL_MØDREKVOTE.TRANS_ID = BEREGNINGSGRUNNLAG_AGG.TRANS_ID
          LEFT JOIN (
            SELECT
              TRANS_ID
            FROM
              DVH_FAM_FP.FAM_FP_UTTAK_FP_KONTOER
            WHERE
              UPPER(STOENADSKONTOTYPE) = 'FEDREKVOTE'
            GROUP BY
              TRANS_ID
          ) RETT_TIL_FEDREKVOTE
          ON RETT_TIL_FEDREKVOTE.TRANS_ID = BEREGNINGSGRUNNLAG_AGG.TRANS_ID
          LEFT JOIN (
            SELECT
              TRANS_ID,
              MAX(MAX_DAGER) AS FLERBARNSDAGER
            FROM
              DVH_FAM_FP.FAM_FP_UTTAK_FP_KONTOER
            WHERE
              UPPER(STOENADSKONTOTYPE) = 'FLERBARNSDAGER'
            GROUP BY
              TRANS_ID
          ) FLERBARNSDAGER
          ON FLERBARNSDAGER.TRANS_ID = BEREGNINGSGRUNNLAG_AGG.TRANS_ID
          LEFT JOIN ADOPSJON
          ON BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID = ADOPSJON.FAGSAK_ID
          LEFT JOIN EOS
          ON BEREGNINGSGRUNNLAG_AGG.TRANS_ID = EOS.TRANS_ID
      )
      SELECT /*+ PARALLEL(8) */
        *
 --from uttak_dager
      FROM
        GRUNNLAG
      WHERE
        FAGSAK_ID NOT IN (1679117)
 --where fagsak_id in (1035184)
;
    V_TID_FOM                      VARCHAR2(8) := NULL;
    V_TID_TOM                      VARCHAR2(8) := NULL;
    V_COMMIT                       NUMBER := 0;
    V_ERROR_MELDING                VARCHAR2(1000) := NULL;
    V_DIM_TID_ANTALL               NUMBER := 0;
    V_UTBETALINGSPROSENT_KALKULERT NUMBER := 0;
  BEGIN
    V_TID_FOM := SUBSTR(P_IN_VEDTAK_TOM, 1, 4)
                 || '0101';
    V_TID_TOM := TO_CHAR(LAST_DAY(TO_DATE(P_IN_VEDTAK_TOM, 'yyyymm')), 'yyyymmdd');
 --dbms_output.put_line(v_tid_fom||v_tid_tom);--TEST!!!
    FOR REC_PERIODE IN CUR_PERIODE(P_IN_RAPPORT_DATO, P_IN_FORSKYVNINGER, V_TID_FOM, V_TID_TOM) LOOP
      V_DIM_TID_ANTALL := 0;
      V_UTBETALINGSPROSENT_KALKULERT := 0;
      V_DIM_TID_ANTALL := DIM_TID_ANTALL(TO_NUMBER(TO_CHAR(REC_PERIODE.UTTAK_FOM, 'yyyymmdd')), TO_NUMBER(TO_CHAR(REC_PERIODE.UTTAK_TOM, 'yyyymmdd')));
      IF V_DIM_TID_ANTALL != 0 THEN
        V_UTBETALINGSPROSENT_KALKULERT := ROUND(REC_PERIODE.TREKKDAGER/V_DIM_TID_ANTALL*100, 2);
      ELSE
        V_UTBETALINGSPROSENT_KALKULERT := 0;
      END IF;
      BEGIN
        INSERT INTO DVH_FAM_FP.FAK_FAM_FP_VEDTAK_UTBETALING (
          FAGSAK_ID,
          TRANS_ID,
          BEHANDLINGSTEMA,
          TREKKONTO,
          STONADSDAGER_KVOTE,
          UTTAK_ARBEID_TYPE,
          AAR --, halvaar, kvartal, aar_maaned
,
          RAPPORT_PERIODE,
          UTTAK_FOM,
          UTTAK_TOM,
          DAGER_ERST,
          BEREGNINGSGRUNNLAG_FOM,
          BEREGNINGSGRUNNLAG_TOM,
          DEKNINGSGRAD,
          DAGSATS_BRUKER,
          DAGSATS_ARBEIDSGIVER,
          VIRKSOMHET,
          PERIODE_RESULTAT_AARSAK,
          DAGSATS,
          GRADERINGSPROSENT,
          STATUS_OG_ANDEL_INNTEKTSKAT,
          AKTIVITET_STATUS,
          BRUTTO_INNTEKT,
          AVKORTET_INNTEKT,
          STATUS_OG_ANDEL_BRUTTO,
          STATUS_OG_ANDEL_AVKORTET,
          UTBETALINGSPROSENT,
          FK_DIM_TID_DATO_UTBET_FOM,
          FK_DIM_TID_DATO_UTBET_TOM,
          FUNKSJONELL_TID,
          FORSTE_VEDTAKSDATO,
          VEDTAKSDATO,
          MAX_VEDTAKSDATO,
          PERIODE_TYPE,
          TILFELLE_ERST,
          BELOP,
          DAGSATS_REDUSERT,
          LASTET_DATO,
          MAX_TRANS_ID,
          FK_PERSON1_MOTTAKER,
          FK_PERSON1_ANNEN_PART,
          KJONN,
          FK_PERSON1_BARN,
          TERMINDATO,
          FOEDSELSDATO,
          ANTALL_BARN_TERMIN,
          ANTALL_BARN_FOEDSEL,
          FOEDSELSDATO_ADOPSJON,
          ANTALL_BARN_ADOPSJON,
          ANNENFORELDERFAGSAK_ID,
          MAX_STONADSDAGER_KONTO,
          FK_DIM_PERSON,
          BOSTED_KOMMUNE_NR,
          FK_DIM_GEOGRAFI,
          BYDEL_KOMMUNE_NR,
          KOMMUNE_NR,
          KOMMUNE_NAVN,
          BYDEL_NR,
          BYDEL_NAVN,
          ALENEOMSORG,
          HOVEDKONTONR,
          UNDERKONTONR,
          MOTTAKER_FODSELS_AAR,
          MOTTAKER_FODSELS_MND,
          MOTTAKER_ALDER,
          RETT_TIL_FEDREKVOTE,
          RETT_TIL_MODREKVOTE,
          DAGSATS_ERST,
          TREKKDAGER,
          SAMTIDIG_UTTAK,
          GRADERING,
          GRADERING_INNVILGET,
          ANTALL_DAGER_PERIODE,
          FLERBARNSDAGER,
          UTBETALINGSPROSENT_KALKULERT,
          MIN_UTTAK_FOM,
          MAX_UTTAK_TOM,
          FK_FAM_FP_TREKKONTO,
          FK_FAM_FP_PERIODE_RESULTAT_AARSAK,
          SIVILSTATUS,
          FK_DIM_SIVILSTATUS,
          ANTALL_BEREGNINGSGRUNNLAG,
          GRADERINGSDAGER,
          FK_DIM_TID_MIN_DATO_KVOTE,
          FK_DIM_TID_MAX_DATO_KVOTE,
          ADOPSJONSDATO,
          STEBARNSADOPSJON,
          EOS_SAK,
          MOR_RETTIGHET,
          STATSBORGERSKAP,
          ARBEIDSTIDSPROSENT,
          MORS_AKTIVITET,
          GYLDIG_FLAGG,
          ANDEL_AV_REFUSJON,
          FORSTE_SOKNADSDATO,
          SOKNADSDATO
        ) VALUES (
          REC_PERIODE.FAGSAK_ID,
          REC_PERIODE.TRANS_ID,
          REC_PERIODE.BEHANDLINGSTEMA,
          REC_PERIODE.TREKKONTO,
          REC_PERIODE.STONADSDAGER_KVOTE,
          REC_PERIODE.UTTAK_ARBEID_TYPE,
          REC_PERIODE.AAR --, rec_periode.halvaar, rec_periode.kvartal, rec_periode.aar_maaned
,
          P_IN_RAPPORT_DATO,
          REC_PERIODE.UTTAK_FOM,
          REC_PERIODE.UTTAK_TOM,
          REC_PERIODE.DAGER_ERST,
          REC_PERIODE.BEREGNINGSGRUNNLAG_FOM,
          REC_PERIODE.BEREGNINGSGRUNNLAG_TOM,
          REC_PERIODE.DEKNINGSGRAD,
          REC_PERIODE.DAGSATS_BRUKER,
          REC_PERIODE.DAGSATS_ARBEIDSGIVER,
          REC_PERIODE.VIRKSOMHET,
          REC_PERIODE.PERIODE_RESULTAT_AARSAK,
          REC_PERIODE.DAGSATS,
          REC_PERIODE.GRADERINGSPROSENT,
          REC_PERIODE.STATUS_OG_ANDEL_INNTEKTSKAT,
          REC_PERIODE.AKTIVITET_STATUS,
          REC_PERIODE.BRUTTO_INNTEKT,
          REC_PERIODE.AVKORTET_INNTEKT,
          REC_PERIODE.STATUS_OG_ANDEL_BRUTTO,
          REC_PERIODE.STATUS_OG_ANDEL_AVKORTET,
          REC_PERIODE.UTBETALINGSPROSENT,
          REC_PERIODE.PK_DIM_TID_DATO_UTBET_FOM,
          REC_PERIODE.PK_DIM_TID_DATO_UTBET_TOM,
          REC_PERIODE.FUNKSJONELL_TID,
          REC_PERIODE.FORSTE_VEDTAKSDATO,
          REC_PERIODE.SISTE_VEDTAKSDATO,
          REC_PERIODE.MAX_VEDTAKSDATO,
          REC_PERIODE.PERIODE,
          REC_PERIODE.TILFELLE_ERST,
          REC_PERIODE.BELOP,
          REC_PERIODE.DAGSATS_REDUSERT,
          REC_PERIODE.LASTET_DATO,
          REC_PERIODE.MAX_TRANS_ID,
          REC_PERIODE.FK_PERSON1_MOTTAKER,
          REC_PERIODE.FK_PERSON1_ANNEN_PART,
          REC_PERIODE.KJONN,
          REC_PERIODE.FK_PERSON1_BARN,
          REC_PERIODE.TERMINDATO,
          REC_PERIODE.FOEDSELSDATO,
          REC_PERIODE.ANTALL_BARN_TERMIN,
          REC_PERIODE.ANTALL_BARN_FOEDSEL,
          REC_PERIODE.FOEDSELSDATO_ADOPSJON,
          REC_PERIODE.ANTALL_BARN_ADOPSJON,
          REC_PERIODE.ANNENFORELDERFAGSAK_ID,
          REC_PERIODE.MAX_STONADSDAGER_KONTO,
          REC_PERIODE.PK_DIM_PERSON,
          REC_PERIODE.BOSTED_KOMMUNE_NR,
          REC_PERIODE.PK_DIM_GEOGRAFI,
          REC_PERIODE.BYDEL_KOMMUNE_NR,
          REC_PERIODE.KOMMUNE_NR,
          REC_PERIODE.KOMMUNE_NAVN,
          REC_PERIODE.BYDEL_NR,
          REC_PERIODE.BYDEL_NAVN,
          REC_PERIODE.ALENEOMSORG,
          REC_PERIODE.HOVEDKONTONR,
          REC_PERIODE.UNDERKONTONR,
          REC_PERIODE.MOTTAKER_FODSELS_AAR,
          REC_PERIODE.MOTTAKER_FODSELS_MND,
          REC_PERIODE.MOTTAKER_ALDER,
          REC_PERIODE.RETT_TIL_FEDREKVOTE,
          REC_PERIODE.RETT_TIL_MØDREKVOTE,
          REC_PERIODE.DAGSATS_ERST,
          REC_PERIODE.TREKKDAGER,
          REC_PERIODE.SAMTIDIG_UTTAK,
          REC_PERIODE.GRADERING,
          REC_PERIODE.GRADERING_INNVILGET,
          V_DIM_TID_ANTALL,
          REC_PERIODE.FLERBARNSDAGER,
          V_UTBETALINGSPROSENT_KALKULERT,
          REC_PERIODE.MIN_UTTAK_FOM,
          REC_PERIODE.MAX_UTTAK_TOM,
          REC_PERIODE.PK_FAM_FP_TREKKONTO,
          REC_PERIODE.PK_FAM_FP_PERIODE_RESULTAT_AARSAK,
          REC_PERIODE.SIVILSTAND,
          REC_PERIODE.FK_DIM_SIVILSTATUS,
          REC_PERIODE.ANTALL_BEREGNINGSGRUNNLAG,
          REC_PERIODE.GRADERINGSDAGER,
          REC_PERIODE.FK_DIM_TID_MIN_DATO_KVOTE,
          REC_PERIODE.FK_DIM_TID_MAX_DATO_KVOTE,
          REC_PERIODE.ADOPSJONSDATO,
          REC_PERIODE.STEBARNSADOPSJON,
          REC_PERIODE.EOS_SAK,
          REC_PERIODE.MOR_RETTIGHET,
          REC_PERIODE.STATSBORGERSKAP,
          REC_PERIODE.ARBEIDSTIDSPROSENT,
          REC_PERIODE.MORS_AKTIVITET,
          P_IN_GYLDIG_FLAGG,
          REC_PERIODE.ANDEL_AV_REFUSJON,
          REC_PERIODE.FORSTE_SOKNADSDATO,
          REC_PERIODE.SOKNADSDATO
        );
        V_COMMIT := V_COMMIT + 1;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK;
          V_ERROR_MELDING := SUBSTR(SQLCODE
                                    || ' '
                                    || SQLERRM, 1, 1000);
          INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
            MIN_LASTET_DATO,
            ID,
            ERROR_MSG,
            OPPRETTET_TID,
            KILDE
          ) VALUES(
            NULL,
            REC_PERIODE.FAGSAK_ID,
            V_ERROR_MELDING,
            SYSDATE,
            'FAM_FP_STATISTIKK_S:INSERT'
          );
          COMMIT;
          P_OUT_ERROR := SUBSTR(P_OUT_ERROR
                                || V_ERROR_MELDING, 1, 1000);
      END;
      IF V_COMMIT > 100000 THEN
        COMMIT;
        V_COMMIT := 0;
      END IF;
    END LOOP;
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_ERROR_MELDING := SUBSTR(SQLCODE
                                || ' '
                                || SQLERRM, 1, 1000);
      INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
        MIN_LASTET_DATO,
        ID,
        ERROR_MSG,
        OPPRETTET_TID,
        KILDE
      ) VALUES(
        NULL,
        NULL,
        V_ERROR_MELDING,
        SYSDATE,
        'FAM_FP_STATISTIKK_S'
      );
      COMMIT;
      P_OUT_ERROR := SUBSTR(P_OUT_ERROR
                            || V_ERROR_MELDING, 1, 1000);
  END FAM_FP_STATISTIKK_S;

  PROCEDURE FAM_FP_STATISTIKK_AAR(
    P_IN_VEDTAK_TOM IN VARCHAR2,
    P_IN_RAPPORT_DATO IN VARCHAR2,
    P_IN_FORSKYVNINGER IN NUMBER,
    P_IN_GYLDIG_FLAGG IN NUMBER DEFAULT 0,
    P_IN_PERIODE_TYPE IN VARCHAR2 DEFAULT 'A',
    P_OUT_ERROR OUT VARCHAR2
  ) AS
    CURSOR CUR_PERIODE(P_RAPPORT_DATO IN VARCHAR2, P_FORSKYVNINGER IN NUMBER, P_TID_FOM IN VARCHAR2, P_TID_TOM IN VARCHAR2) IS
      WITH FAGSAK AS (
        SELECT
          FAGSAK_ID,
          MAX(BEHANDLINGSTEMA)                                                   AS BEHANDLINGSTEMA,
          MAX(FAGSAKANNENFORELDER_ID)                                            AS ANNENFORELDERFAGSAK_ID,
          MAX(TRANS_ID) KEEP(DENSE_RANK FIRST ORDER BY FUNKSJONELL_TID DESC)     AS MAX_TRANS_ID,
          MAX(SOEKNADSDATO) KEEP(DENSE_RANK FIRST ORDER BY FUNKSJONELL_TID DESC) AS SOKNADSDATO,
          MIN(SOEKNADSDATO)                                                      AS FORSTE_SOKNADSDATO,
          MIN(VEDTAKSDATO)                                                       AS FORSTE_VEDTAKSDATO,
          MAX(FUNKSJONELL_TID)                                                   AS FUNKSJONELL_TID,
          MAX(VEDTAKSDATO)                                                       AS SISTE_VEDTAKSDATO,
          P_IN_PERIODE_TYPE                                                      AS PERIODE,
          LAST_DAY(TO_DATE(P_RAPPORT_DATO, 'yyyymm')) + P_FORSKYVNINGER          AS MAX_VEDTAKSDATO
        FROM
          DVH_FAM_FP.FAM_FP_FAGSAK
        WHERE
          FAM_FP_FAGSAK.FUNKSJONELL_TID <= LAST_DAY(TO_DATE(P_RAPPORT_DATO, 'yyyymm')) + P_FORSKYVNINGER
        GROUP BY
          FAGSAK_ID
      ), TERMIN AS (
        SELECT
          FAGSAK_ID,
          MAX(TERMINDATO)            TERMINDATO,
          MAX(FOEDSELSDATO)          FOEDSELSDATO,
          MAX(ANTALL_BARN_TERMIN)    ANTALL_BARN_TERMIN,
          MAX(ANTALL_BARN_FOEDSEL)   ANTALL_BARN_FOEDSEL,
          MAX(FOEDSELSDATO_ADOPSJON) FOEDSELSDATO_ADOPSJON,
          MAX(ANTALL_BARN_ADOPSJON) ANTALL_BARN_ADOPSJON
        FROM
          (
            SELECT
              FAM_FP_FAGSAK.FAGSAK_ID,
              MAX(FODSEL.TERMINDATO)              TERMINDATO,
              MAX(FODSEL.FOEDSELSDATO)            FOEDSELSDATO,
              MAX(FODSEL.ANTALL_BARN_FOEDSEL)     ANTALL_BARN_FOEDSEL,
              MAX(FODSEL.ANTALL_BARN_TERMIN)      ANTALL_BARN_TERMIN,
              MAX(ADOPSJON.FOEDSELSDATO_ADOPSJON) FOEDSELSDATO_ADOPSJON,
              COUNT(ADOPSJON.TRANS_ID)            ANTALL_BARN_ADOPSJON
            FROM
              DVH_FAM_FP.FAM_FP_FAGSAK
              LEFT JOIN DVH_FAM_FP.FAM_FP_FODSELTERMIN FODSEL
              ON FODSEL.FAGSAK_ID = FAM_FP_FAGSAK.FAGSAK_ID
              AND UPPER(FAM_FP_FAGSAK.BEHANDLINGSTEMA) = 'FORP_FODS'
              LEFT JOIN DVH_FAM_FP.FAM_FP_FODSELTERMIN ADOPSJON
              ON ADOPSJON.FAGSAK_ID = FAM_FP_FAGSAK.FAGSAK_ID
              AND ADOPSJON.TRANS_ID = FAM_FP_FAGSAK.TRANS_ID
              AND UPPER(FAM_FP_FAGSAK.BEHANDLINGSTEMA) = 'FORP_ADOP'
            GROUP BY
              FAM_FP_FAGSAK.FAGSAK_ID,
              FAM_FP_FAGSAK.TRANS_ID
          )
        GROUP BY
          FAGSAK_ID
      ), FK_PERSON1 AS (
        SELECT
          PERSON.PERSON,
          PERSON.FAGSAK_ID,
          MAX(PERSON.BEHANDLINGSTEMA)                                                                             AS BEHANDLINGSTEMA,
          PERSON.MAX_TRANS_ID,
          MAX(PERSON.ANNENFORELDERFAGSAK_ID)                                                                      AS ANNENFORELDERFAGSAK_ID,
          PERSON.AKTOER_ID,
          MAX(PERSON.KJONN)                                                                                       AS KJONN,
          MAX(PERSON_67_VASKET.FK_PERSON1) KEEP (DENSE_RANK FIRST ORDER BY PERSON_67_VASKET.GYLDIG_FRA_DATO DESC) AS FK_PERSON1,
          MAX(FOEDSELSDATO)                                                                                       AS FOEDSELSDATO,
          MAX(SIVILSTAND)                                                                                         AS SIVILSTAND,
          MAX(STATSBORGERSKAP)                                                                                    AS STATSBORGERSKAP
        FROM
          (
            SELECT
              'MOTTAKER'                                AS PERSON,
              FAGSAK.FAGSAK_ID,
              FAGSAK.BEHANDLINGSTEMA,
              FAGSAK.MAX_TRANS_ID,
              FAGSAK.ANNENFORELDERFAGSAK_ID,
              FAM_FP_PERSONOPPLYSNINGER.AKTOER_ID,
              FAM_FP_PERSONOPPLYSNINGER.KJONN,
              FAM_FP_PERSONOPPLYSNINGER.FOEDSELSDATO,
              FAM_FP_PERSONOPPLYSNINGER.SIVILSTAND,
              FAM_FP_PERSONOPPLYSNINGER.STATSBORGERSKAP
            FROM
              DVH_FAM_FP.FAM_FP_PERSONOPPLYSNINGER
              JOIN FAGSAK
              ON FAM_FP_PERSONOPPLYSNINGER.TRANS_ID = FAGSAK.MAX_TRANS_ID UNION ALL
              SELECT
                'BARN'                                    AS PERSON,
                FAGSAK.FAGSAK_ID,
                MAX(FAGSAK.BEHANDLINGSTEMA)               AS BEHANDLINGSTEMA,
                FAGSAK.MAX_TRANS_ID,
                MAX(FAGSAK.ANNENFORELDERFAGSAK_ID)        ANNENFORELDERFAGSAK_ID,
                MAX(FAM_FP_FAMILIEHENDELSE.TIL_AKTOER_ID) AS AKTOER_ID,
                MAX(FAM_FP_FAMILIEHENDELSE.KJOENN)        AS KJONN,
                NULL                                      AS FOEDSELSDATO,
                NULL                                      AS SIVILSTAND,
                NULL                                      AS STATSBORGERSKAP
              FROM
                DVH_FAM_FP.FAM_FP_FAMILIEHENDELSE
                JOIN FAGSAK
                ON FAM_FP_FAMILIEHENDELSE.FAGSAK_ID = FAGSAK.FAGSAK_ID
              WHERE
                UPPER(FAM_FP_FAMILIEHENDELSE.RELASJON) = 'BARN'
              GROUP BY
                FAGSAK.FAGSAK_ID, FAGSAK.MAX_TRANS_ID
          )                                    PERSON
          JOIN DT_PERSON.DVH_PERSON_IDENT_AKTOR_IKKE_SKJERMET PERSON_67_VASKET
          ON PERSON_67_VASKET.AKTOR_ID = PERSON.AKTOER_ID
          AND TO_DATE(P_TID_TOM, 'yyyymmdd') BETWEEN PERSON_67_VASKET.GYLDIG_FRA_DATO
          AND PERSON_67_VASKET.GYLDIG_TIL_DATO
        GROUP BY
          PERSON.PERSON, PERSON.FAGSAK_ID, PERSON.MAX_TRANS_ID, PERSON.AKTOER_ID
      ), BARN AS (
        SELECT
          FAGSAK_ID,
          LISTAGG(FK_PERSON1, ',') WITHIN GROUP (ORDER BY FK_PERSON1) AS FK_PERSON1_BARN
        FROM
          FK_PERSON1
        WHERE
          PERSON = 'BARN'
        GROUP BY
          FAGSAK_ID
      ), MOTTAKER AS (
        SELECT
          FK_PERSON1.FAGSAK_ID,
          FK_PERSON1.BEHANDLINGSTEMA,
          FK_PERSON1.MAX_TRANS_ID,
          FK_PERSON1.ANNENFORELDERFAGSAK_ID,
          FK_PERSON1.AKTOER_ID,
          FK_PERSON1.KJONN,
          FK_PERSON1.FK_PERSON1                       AS FK_PERSON1_MOTTAKER,
          EXTRACT(YEAR FROM FK_PERSON1.FOEDSELSDATO) AS MOTTAKER_FODSELS_AAR,
          EXTRACT(MONTH FROM FK_PERSON1.FOEDSELSDATO) AS MOTTAKER_FODSELS_MND,
          FK_PERSON1.SIVILSTAND,
          FK_PERSON1.STATSBORGERSKAP,
          BARN.FK_PERSON1_BARN,
          TERMIN.TERMINDATO,
          TERMIN.FOEDSELSDATO,
          TERMIN.ANTALL_BARN_TERMIN,
          TERMIN.ANTALL_BARN_FOEDSEL,
          TERMIN.FOEDSELSDATO_ADOPSJON,
          TERMIN.ANTALL_BARN_ADOPSJON
        FROM
          FK_PERSON1
          LEFT JOIN BARN
          ON BARN.FAGSAK_ID = FK_PERSON1.FAGSAK_ID
          LEFT JOIN TERMIN
          ON FK_PERSON1.FAGSAK_ID = TERMIN.FAGSAK_ID
        WHERE
          FK_PERSON1.PERSON = 'MOTTAKER'
      ), ADOPSJON AS (
        SELECT
          FAM_FP_VILKAAR.FAGSAK_ID,
          MAX(FAM_FP_VILKAAR.OMSORGS_OVERTAKELSESDATO) AS ADOPSJONSDATO,
          MAX(FAM_FP_VILKAAR.EKTEFELLES_BARN)          AS STEBARNSADOPSJON
        FROM
          FAGSAK
          JOIN DVH_FAM_FP.FAM_FP_VILKAAR
          ON FAGSAK.FAGSAK_ID = FAM_FP_VILKAAR.FAGSAK_ID
        WHERE
          FAGSAK.BEHANDLINGSTEMA = 'FORP_ADOP'
        GROUP BY
          FAM_FP_VILKAAR.FAGSAK_ID
      ), EOS AS (
        SELECT
          A.TRANS_ID,
          CASE
            WHEN UPPER(ER_BORGER_AV_EU_EOS) = 'TRUE' THEN
              'J'
            WHEN UPPER(ER_BORGER_AV_EU_EOS) = 'FALSE' THEN
              'N'
            ELSE
              NULL
          END EOS_SAK
        FROM
          (
            SELECT
              FAM_FP_VILKAAR.TRANS_ID,
              MAX(FAM_FP_VILKAAR.ER_BORGER_AV_EU_EOS) AS ER_BORGER_AV_EU_EOS
            FROM
              FAGSAK
              JOIN DVH_FAM_FP.FAM_FP_VILKAAR
              ON FAGSAK.MAX_TRANS_ID = FAM_FP_VILKAAR.TRANS_ID
              AND LENGTH(FAM_FP_VILKAAR.PERSON_STATUS) > 0
            GROUP BY
              FAM_FP_VILKAAR.TRANS_ID
          ) A
      ), ANNENFORELDERFAGSAK AS (
        SELECT
          ANNENFORELDERFAGSAK.*,
          MOTTAKER.FK_PERSON1_MOTTAKER AS FK_PERSON1_ANNEN_PART
        FROM
          (
            SELECT
              FAGSAK_ID,
              MAX_TRANS_ID,
              MAX(ANNENFORELDERFAGSAK_ID) AS ANNENFORELDERFAGSAK_ID
            FROM
              (
                SELECT
                  FORELDER1.FAGSAK_ID,
                  FORELDER1.MAX_TRANS_ID,
                  NVL(FORELDER1.ANNENFORELDERFAGSAK_ID, FORELDER2.FAGSAK_ID) AS ANNENFORELDERFAGSAK_ID
                FROM
                  MOTTAKER FORELDER1
                  JOIN MOTTAKER FORELDER2
                  ON FORELDER1.FK_PERSON1_BARN = FORELDER2.FK_PERSON1_BARN
                  AND FORELDER1.FK_PERSON1_MOTTAKER != FORELDER2.FK_PERSON1_MOTTAKER
              )
            GROUP BY
              FAGSAK_ID,
              MAX_TRANS_ID
          )        ANNENFORELDERFAGSAK
          JOIN MOTTAKER
          ON ANNENFORELDERFAGSAK.ANNENFORELDERFAGSAK_ID = MOTTAKER.FAGSAK_ID
      ), TID AS (
        SELECT
          PK_DIM_TID,
          DATO,
          AAR,
          HALVAAR,
          KVARTAL,
          AAR_MAANED
        FROM
          DT_KODEVERK.DIM_TID
        WHERE
          DAG_I_UKE < 6
          AND DIM_NIVAA = 1
          AND GYLDIG_FLAGG = 1
          AND PK_DIM_TID BETWEEN P_TID_FOM AND P_TID_TOM
          AND PK_DIM_TID <= TO_CHAR(LAST_DAY(TO_DATE(P_RAPPORT_DATO, 'yyyymm')), 'yyyymmdd')
      ), UTTAK AS (
        SELECT
          UTTAK.TRANS_ID,
          UTTAK.TREKKONTO,
          UTTAK.UTTAK_ARBEID_TYPE,
          UTTAK.VIRKSOMHET,
          UTTAK.UTBETALINGSPROSENT,
          UTTAK.GRADERING_INNVILGET,
          UTTAK.GRADERING,
          UTTAK.ARBEIDSTIDSPROSENT,
          UTTAK.SAMTIDIG_UTTAK,
          UTTAK.PERIODE_RESULTAT_AARSAK,
          UTTAK.FOM                                      AS UTTAK_FOM,
          UTTAK.TOM                                      AS UTTAK_TOM,
          UTTAK.TREKKDAGER,
          FAGSAK.FAGSAK_ID,
          FAGSAK.PERIODE,
          FAGSAK.FUNKSJONELL_TID,
          FAGSAK.FORSTE_VEDTAKSDATO,
          FAGSAK.SISTE_VEDTAKSDATO,
          FAGSAK.MAX_VEDTAKSDATO,
          FAGSAK.FORSTE_SOKNADSDATO,
          FAGSAK.SOKNADSDATO,
          FAM_FP_TREKKONTO.PK_FAM_FP_TREKKONTO,
          AARSAK_UTTAK.PK_FAM_FP_PERIODE_RESULTAT_AARSAK,
          UTTAK.ARBEIDSFORHOLD_ID,
          UTTAK.GRADERINGSDAGER,
          FAM_FP_UTTAK_FORDELINGSPER.MORS_AKTIVITET
        FROM
          DVH_FAM_FP.FAM_FP_UTTAK_RES_PER_AKTIV UTTAK
          JOIN FAGSAK
          ON FAGSAK.MAX_TRANS_ID = UTTAK.TRANS_ID LEFT JOIN DVH_FAM_FP.FAM_FP_TREKKONTO
          ON UPPER(UTTAK.TREKKONTO) = FAM_FP_TREKKONTO.TREKKONTO
          LEFT JOIN (
            SELECT
              AARSAK_UTTAK,
              MAX(PK_FAM_FP_PERIODE_RESULTAT_AARSAK) AS PK_FAM_FP_PERIODE_RESULTAT_AARSAK
            FROM
              DVH_FAM_FP.FAM_FP_PERIODE_RESULTAT_AARSAK
            GROUP BY
              AARSAK_UTTAK
          ) AARSAK_UTTAK
          ON UPPER(UTTAK.PERIODE_RESULTAT_AARSAK) = AARSAK_UTTAK.AARSAK_UTTAK
          LEFT JOIN DVH_FAM_FP.FAM_FP_UTTAK_FORDELINGSPER
          ON FAM_FP_UTTAK_FORDELINGSPER.TRANS_ID = UTTAK.TRANS_ID
          AND UTTAK.FOM BETWEEN FAM_FP_UTTAK_FORDELINGSPER.FOM
          AND FAM_FP_UTTAK_FORDELINGSPER.TOM
          AND UPPER(UTTAK.TREKKONTO) = UPPER(FAM_FP_UTTAK_FORDELINGSPER.PERIODE_TYPE)
          AND LENGTH(FAM_FP_UTTAK_FORDELINGSPER.MORS_AKTIVITET) > 1
        WHERE
          UTTAK.UTBETALINGSPROSENT > 0
      ), STONADSDAGER_KVOTE AS (
        SELECT
          UTTAK.*,
          TID1.PK_DIM_TID AS FK_DIM_TID_MIN_DATO_KVOTE,
          TID2.PK_DIM_TID AS FK_DIM_TID_MAX_DATO_KVOTE
        FROM
          (
            SELECT
              FAGSAK_ID,
              TRANS_ID,
              TREKKONTO,
              VIRKSOMHET,
              UTTAK_ARBEID_TYPE,
              SUM(TREKKDAGER)   AS STONADSDAGER_KVOTE,
              MIN(UTTAK_FOM)    AS MIN_UTTAK_FOM,
              MAX(UTTAK_TOM)    AS MAX_UTTAK_TOM
            FROM
              (
                SELECT
                  FAGSAK_ID,
                  TRANS_ID,
                  UTTAK_FOM,
                  UTTAK_TOM,
                  TREKKONTO,
                  VIRKSOMHET,
                  UTTAK_ARBEID_TYPE,
                  MAX(TREKKDAGER)   AS TREKKDAGER
                FROM
                  UTTAK
                GROUP BY
                  FAGSAK_ID,
                  TRANS_ID,
                  UTTAK_FOM,
                  UTTAK_TOM,
                  TREKKONTO,
                  VIRKSOMHET,
                  UTTAK_ARBEID_TYPE
              ) A
            GROUP BY
              FAGSAK_ID,
              TRANS_ID,
              TREKKONTO,
              VIRKSOMHET,
              UTTAK_ARBEID_TYPE
          )                   UTTAK
          JOIN DT_KODEVERK.DIM_TID TID1
          ON TID1.DIM_NIVAA = 1
          AND TID1.DATO = TRUNC(UTTAK.MIN_UTTAK_FOM, 'dd') JOIN DT_KODEVERK.DIM_TID TID2
          ON TID2.DIM_NIVAA = 1
          AND TID2.DATO = TRUNC(UTTAK.MAX_UTTAK_TOM,
          'dd')
      ), UTTAK_DAGER AS (
        SELECT
          UTTAK.*,
          TID.PK_DIM_TID,
          TID.DATO,
          TID.AAR,
          TID.HALVAAR,
          TID.KVARTAL,
          TID.AAR_MAANED
        FROM
          UTTAK
          JOIN TID
          ON TID.DATO BETWEEN UTTAK.UTTAK_FOM
          AND UTTAK.UTTAK_TOM
      ), ALENEOMSORG AS (
        SELECT
          UTTAK.FAGSAK_ID,
          UTTAK.UTTAK_FOM
        FROM
          UTTAK
          JOIN DVH_FAM_FP.FAM_FP_DOKUMENTASJONSPERIODER DOK1
          ON DOK1.FAGSAK_ID = UTTAK.FAGSAK_ID
          AND UTTAK.UTTAK_FOM >= DOK1.FOM
          AND DOK1.DOKUMENTASJON_TYPE IN ('ALENEOMSORG', 'ALENEOMSORG_OVERFØRING') LEFT JOIN DVH_FAM_FP.FAM_FP_DOKUMENTASJONSPERIODER DOK2
          ON DOK1.FAGSAK_ID = DOK2.FAGSAK_ID
          AND UTTAK.UTTAK_FOM >= DOK2.FOM
          AND DOK1.TRANS_ID < DOK2.TRANS_ID
          AND DOK2.DOKUMENTASJON_TYPE = 'ANNEN_FORELDER_HAR_RETT'
          AND DOK2.FAGSAK_ID IS NULL
        GROUP BY
          UTTAK.FAGSAK_ID,
          UTTAK.UTTAK_FOM
      ), BEREGNINGSGRUNNLAG AS (
        SELECT
          FAGSAK_ID,
          TRANS_ID,
          VIRKSOMHETSNUMMER,
          MAX(STATUS_OG_ANDEL_BRUTTO)         AS STATUS_OG_ANDEL_BRUTTO,
          MAX(STATUS_OG_ANDEL_AVKORTET)       AS STATUS_OG_ANDEL_AVKORTET,
          FOM                                 AS BEREGNINGSGRUNNLAG_FOM,
          TOM                                 AS BEREGNINGSGRUNNLAG_TOM,
          MAX(DEKNINGSGRAD)                   AS DEKNINGSGRAD,
          MAX(DAGSATS)                        AS DAGSATS,
          DAGSATS_BRUKER,
          DAGSATS_ARBEIDSGIVER,
          DAGSATS_BRUKER+DAGSATS_ARBEIDSGIVER DAGSATS_VIRKSOMHET,
          MAX(STATUS_OG_ANDEL_INNTEKTSKAT)    AS STATUS_OG_ANDEL_INNTEKTSKAT,
          AKTIVITET_STATUS,
          MAX(BRUTTO)                         AS BRUTTO_INNTEKT,
          MAX(AVKORTET)                       AS AVKORTET_INNTEKT,
          COUNT(1)                            AS ANTALL_BEREGNINGSGRUNNLAG
        FROM
          DVH_FAM_FP.FAM_FP_BEREGNINGSGRUNNLAG
        GROUP BY
          FAGSAK_ID,
          TRANS_ID,
          VIRKSOMHETSNUMMER,
          FOM,
          TOM,
          AKTIVITET_STATUS,
          DAGSATS_BRUKER,
          DAGSATS_ARBEIDSGIVER
      ), BEREGNINGSGRUNNLAG_DETALJ AS (
        SELECT
          UTTAK_DAGER.*,
          STONADSDAGER_KVOTE.STONADSDAGER_KVOTE,
          STONADSDAGER_KVOTE.MIN_UTTAK_FOM,
          STONADSDAGER_KVOTE.MAX_UTTAK_TOM,
          STONADSDAGER_KVOTE.FK_DIM_TID_MIN_DATO_KVOTE,
          STONADSDAGER_KVOTE.FK_DIM_TID_MAX_DATO_KVOTE,
          BEREG.STATUS_OG_ANDEL_BRUTTO,
          BEREG.STATUS_OG_ANDEL_AVKORTET,
          BEREG.BEREGNINGSGRUNNLAG_FOM,
          BEREG.DEKNINGSGRAD,
          BEREG.BEREGNINGSGRUNNLAG_TOM,
          BEREG.DAGSATS,
          BEREG.DAGSATS_BRUKER,
          BEREG.DAGSATS_ARBEIDSGIVER,
          BEREG.DAGSATS_VIRKSOMHET,
          BEREG.STATUS_OG_ANDEL_INNTEKTSKAT,
          BEREG.AKTIVITET_STATUS,
          BEREG.BRUTTO_INNTEKT,
          BEREG.AVKORTET_INNTEKT,
          BEREG.DAGSATS*UTTAK_DAGER.UTBETALINGSPROSENT/100 AS DAGSATS_ERST,
          BEREG.ANTALL_BEREGNINGSGRUNNLAG
        FROM
          BEREGNINGSGRUNNLAG                        BEREG
          JOIN UTTAK_DAGER
          ON UTTAK_DAGER.TRANS_ID = BEREG.TRANS_ID
          AND NVL(UTTAK_DAGER.VIRKSOMHET, 'X') = NVL(BEREG.VIRKSOMHETSNUMMER, 'X')
          AND BEREG.BEREGNINGSGRUNNLAG_FOM <= UTTAK_DAGER.DATO
          AND NVL(BEREG.BEREGNINGSGRUNNLAG_TOM, TO_DATE('20991201', 'YYYYMMDD')) >= UTTAK_DAGER.DATO LEFT JOIN STONADSDAGER_KVOTE
          ON UTTAK_DAGER.TRANS_ID = STONADSDAGER_KVOTE.TRANS_ID
          AND UTTAK_DAGER.TREKKONTO = STONADSDAGER_KVOTE.TREKKONTO
          AND NVL(UTTAK_DAGER.VIRKSOMHET,
          'X') = NVL(STONADSDAGER_KVOTE.VIRKSOMHET,
          'X')
          AND UTTAK_DAGER.UTTAK_ARBEID_TYPE = STONADSDAGER_KVOTE.UTTAK_ARBEID_TYPE
          JOIN DVH_FAM_FP.FAM_FP_UTTAK_AKTIVITET_MAPPING UTTAK_MAPPING
          ON UTTAK_DAGER.UTTAK_ARBEID_TYPE = UTTAK_MAPPING.UTTAK_ARBEID
          AND BEREG.AKTIVITET_STATUS = UTTAK_MAPPING.AKTIVITET_STATUS
        WHERE
          BEREG.DAGSATS_BRUKER + BEREG.DAGSATS_ARBEIDSGIVER != 0
      ), BEREGNINGSGRUNNLAG_AGG AS (
        SELECT
          A.*,
          DAGER_ERST*DAGSATS_VIRKSOMHET/DAGSATS*ANTALL_BEREGNINGSGRUNNLAG                                                                                                                    TILFELLE_ERST,
          DAGER_ERST*ROUND(UTBETALINGSPROSENT/100*DAGSATS_VIRKSOMHET)                                                                                                                        BELOP,
          ROUND(UTBETALINGSPROSENT/100*DAGSATS_VIRKSOMHET-0.5)                                                                                                                               DAGSATS_REDUSERT,
          CASE
            WHEN PERIODE_RESULTAT_AARSAK IN (2004, 2033) THEN
              'N'
            WHEN TREKKONTO IN ('FEDREKVOTE', 'FELLESPERIODE', 'MØDREKVOTE') THEN
              'J'
            WHEN TREKKONTO = 'FORELDREPENGER' THEN
              'N'
          END MOR_RETTIGHET
        FROM
          (
            SELECT
              FAGSAK_ID,
              TRANS_ID,
              TREKKONTO,
              TREKKDAGER,
              STONADSDAGER_KVOTE,
              UTTAK_ARBEID_TYPE,
              AAR --, halvaar--, kvartal, aar_maaned
,
              UTTAK_FOM,
              UTTAK_TOM,
              SUM(DAGSATS_VIRKSOMHET/DAGSATS*
                CASE
                  WHEN ((UPPER(GRADERING_INNVILGET) ='TRUE'
                  AND UPPER(GRADERING)='TRUE')
                  OR UPPER(SAMTIDIG_UTTAK)='TRUE') THEN
                    (100-ARBEIDSTIDSPROSENT)/100
                  ELSE
                    1.0
                END )                                DAGER_ERST2,
              MAX(ARBEIDSTIDSPROSENT)                AS ARBEIDSTIDSPROSENT,
              COUNT(DISTINCT PK_DIM_TID)             DAGER_ERST,
 --count(distinct pk_dim_tid)*dagsats_virksomhet/dagsats tilfelle_erst,
              MIN(BEREGNINGSGRUNNLAG_FOM)            BEREGNINGSGRUNNLAG_FOM,
              MAX(BEREGNINGSGRUNNLAG_TOM)            BEREGNINGSGRUNNLAG_TOM,
              DEKNINGSGRAD,
 --count(distinct pk_dim_tid)*
 --      round(utbetalingsprosent/100*dagsats_virksomhet-0.5) belop,
              DAGSATS_BRUKER,
              DAGSATS_ARBEIDSGIVER,
              DAGSATS_VIRKSOMHET,
              VIRKSOMHET,
              PERIODE_RESULTAT_AARSAK,
              DAGSATS,
              DAGSATS_ERST, --dagsats_virksomhet,
              UTBETALINGSPROSENT                     GRADERINGSPROSENT,
              STATUS_OG_ANDEL_INNTEKTSKAT,
              AKTIVITET_STATUS,
              BRUTTO_INNTEKT,
              AVKORTET_INNTEKT,
              STATUS_OG_ANDEL_BRUTTO,
              STATUS_OG_ANDEL_AVKORTET,
 --round(utbetalingsprosent/100*dagsats_virksomhet-0.5) dagsats_redusert,
              UTBETALINGSPROSENT,
              MIN(PK_DIM_TID)                        PK_DIM_TID_DATO_UTBET_FOM,
              MAX(PK_DIM_TID)                        PK_DIM_TID_DATO_UTBET_TOM,
              FUNKSJONELL_TID,
              FORSTE_VEDTAKSDATO,
              SISTE_VEDTAKSDATO,
              MAX_VEDTAKSDATO,
              PERIODE,
              MAX(FORSTE_SOKNADSDATO)                AS FORSTE_SOKNADSDATO,
              MAX(SOKNADSDATO)                       AS SOKNADSDATO,
              SAMTIDIG_UTTAK,
              GRADERING,
              GRADERING_INNVILGET,
              MIN_UTTAK_FOM,
              MAX_UTTAK_TOM,
              FK_DIM_TID_MIN_DATO_KVOTE,
              FK_DIM_TID_MAX_DATO_KVOTE,
              MAX(PK_FAM_FP_TREKKONTO)               AS PK_FAM_FP_TREKKONTO,
              MAX(PK_FAM_FP_PERIODE_RESULTAT_AARSAK) AS PK_FAM_FP_PERIODE_RESULTAT_AARSAK,
              ANTALL_BEREGNINGSGRUNNLAG,
              MAX(GRADERINGSDAGER)                   AS GRADERINGSDAGER,
              MAX(MORS_AKTIVITET)                    AS MORS_AKTIVITET
            FROM
              BEREGNINGSGRUNNLAG_DETALJ
            GROUP BY
              FAGSAK_ID,
              TRANS_ID,
              TREKKONTO,
              TREKKDAGER,
              STONADSDAGER_KVOTE,
              UTTAK_ARBEID_TYPE,
              AAR --, halvaar--, kvartal, aar_maaned
,
              UTTAK_FOM,
              UTTAK_TOM,
              DEKNINGSGRAD,
              VIRKSOMHET,
              UTBETALINGSPROSENT,
              PERIODE_RESULTAT_AARSAK,
              DAGSATS,
              DAGSATS_ERST,
              DAGSATS_BRUKER,
              DAGSATS_ARBEIDSGIVER,
              DAGSATS_VIRKSOMHET,
              UTBETALINGSPROSENT,
              STATUS_OG_ANDEL_INNTEKTSKAT,
              AKTIVITET_STATUS,
              BRUTTO_INNTEKT,
              AVKORTET_INNTEKT,
              STATUS_OG_ANDEL_BRUTTO,
              STATUS_OG_ANDEL_AVKORTET,
              FUNKSJONELL_TID,
              FORSTE_VEDTAKSDATO,
              SISTE_VEDTAKSDATO,
              MAX_VEDTAKSDATO,
              PERIODE,
              SAMTIDIG_UTTAK,
              GRADERING,
              GRADERING_INNVILGET,
              MIN_UTTAK_FOM,
              MAX_UTTAK_TOM,
              FK_DIM_TID_MIN_DATO_KVOTE,
              FK_DIM_TID_MAX_DATO_KVOTE,
              ANTALL_BEREGNINGSGRUNNLAG
          ) A
      ), GRUNNLAG AS (
        SELECT
          BEREGNINGSGRUNNLAG_AGG.*,
          SYSDATE                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      AS LASTET_DATO,
          MOTTAKER.BEHANDLINGSTEMA,
          MOTTAKER.MAX_TRANS_ID,
          MOTTAKER.FK_PERSON1_MOTTAKER,
          MOTTAKER.KJONN,
          MOTTAKER.FK_PERSON1_BARN,
          MOTTAKER.TERMINDATO,
          MOTTAKER.FOEDSELSDATO,
          MOTTAKER.ANTALL_BARN_TERMIN,
          MOTTAKER.ANTALL_BARN_FOEDSEL,
          MOTTAKER.FOEDSELSDATO_ADOPSJON,
          MOTTAKER.ANTALL_BARN_ADOPSJON,
          MOTTAKER.MOTTAKER_FODSELS_AAR,
          MOTTAKER.MOTTAKER_FODSELS_MND,
          SUBSTR(P_TID_FOM, 1, 4) - MOTTAKER.MOTTAKER_FODSELS_AAR                                                                                                                                                                                                                                                                                                                                                                                                                                                      AS MOTTAKER_ALDER,
          MOTTAKER.SIVILSTAND,
          MOTTAKER.STATSBORGERSKAP,
          DIM_PERSON.PK_DIM_PERSON,
          DIM_PERSON.BOSTED_KOMMUNE_NR,
          DIM_PERSON.FK_DIM_SIVILSTATUS,
          DIM_GEOGRAFI.PK_DIM_GEOGRAFI,
          DIM_GEOGRAFI.BYDEL_KOMMUNE_NR,
          DIM_GEOGRAFI.KOMMUNE_NR,
          DIM_GEOGRAFI.KOMMUNE_NAVN,
          DIM_GEOGRAFI.BYDEL_NR,
          DIM_GEOGRAFI.BYDEL_NAVN,
          ANNENFORELDERFAGSAK.ANNENFORELDERFAGSAK_ID,
          ANNENFORELDERFAGSAK.FK_PERSON1_ANNEN_PART,
          FAM_FP_UTTAK_FP_KONTOER.MAX_DAGER                                                                                                                                                                                                                                                                                                                                                                                                                                                                            MAX_STONADSDAGER_KONTO,
          CASE
            WHEN ALENEOMSORG.FAGSAK_ID IS NOT NULL THEN
              'J'
            ELSE
              NULL
          END AS                                                                                                                                                                                                                                                                                                                                                                                                                                           ALENEOMSORG,
          CASE
            WHEN BEHANDLINGSTEMA = 'FORP_FODS' THEN
              '214'
            WHEN BEHANDLINGSTEMA = 'FORP_ADOP' THEN
              '216'
          END AS                                                                                                                                                                                                                                                                                                                                                                                                         HOVEDKONTONR,
          CASE
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='ARBEIDSTAKER' AND DAGSATS_ARBEIDSGIVER/DAGSATS*100<=50 THEN
              '1000'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='ARBEIDSTAKER' AND DAGSATS_ARBEIDSGIVER/DAGSATS*100>50 THEN
              '8020'
 --when status_og_andel_inntektskat='ARBEIDSTAKER' then '1000'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='JORDBRUKER' THEN
              '5210'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='SJØMANN' THEN
              '1300'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='SELVSTENDIG_NÆRINGSDRIVENDE' THEN
              '5010'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='DAGPENGER' THEN
              '1200'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='ARBEIDSTAKER_UTEN_FERIEPENGER' THEN
              '1000'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='FISKER' THEN
              '5300'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='DAGMAMMA' THEN
              '5110'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='FRILANSER' THEN
              '1100'
          END AS UNDERKONTONR,
          ROUND(DAGSATS_ARBEIDSGIVER/DAGSATS*100, 0)                                                                                                                                                                                                                                                                                                                                                                                                                                                                   AS ANDEL_AV_REFUSJON,
          CASE
            WHEN RETT_TIL_MØDREKVOTE.TRANS_ID IS NULL THEN
              'N'
            ELSE
              'J'
          END AS                                                                                                                                                                                                                                                                                                                                                                                                                                         RETT_TIL_MØDREKVOTE,
          CASE
            WHEN RETT_TIL_FEDREKVOTE.TRANS_ID IS NULL THEN
              'N'
            ELSE
              'J'
          END AS                                                                                                                                                                                                                                                                                                                                                                                                                                         RETT_TIL_FEDREKVOTE,
          FLERBARNSDAGER.FLERBARNSDAGER,
          ADOPSJON.ADOPSJONSDATO,
          ADOPSJON.STEBARNSADOPSJON,
          EOS.EOS_SAK
        FROM
          BEREGNINGSGRUNNLAG_AGG
          LEFT JOIN MOTTAKER
          ON BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID = MOTTAKER.FAGSAK_ID
          AND BEREGNINGSGRUNNLAG_AGG.TRANS_ID = MOTTAKER.MAX_TRANS_ID
          LEFT JOIN ANNENFORELDERFAGSAK
          ON BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID = ANNENFORELDERFAGSAK.FAGSAK_ID
          AND BEREGNINGSGRUNNLAG_AGG.TRANS_ID = ANNENFORELDERFAGSAK.MAX_TRANS_ID
          LEFT JOIN DVH_FAM_FP.FAM_FP_UTTAK_FP_KONTOER
          ON BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID = FAM_FP_UTTAK_FP_KONTOER.FAGSAK_ID
          AND MOTTAKER.MAX_TRANS_ID = FAM_FP_UTTAK_FP_KONTOER.TRANS_ID
 --AND UPPER(REGEXP_REPLACE(grunnlag_drp1.TREKKONTO, '_|-|[[:space:]]', '')) = UPPER(REGEXP_REPLACE(FAM_FP_Uttak_FP_Kontoer.STOENADSKONTOTYPE, '_|-|[[:space:]]', ''))
          AND UPPER(REPLACE(BEREGNINGSGRUNNLAG_AGG.TREKKONTO,
          '_',
          '')) = UPPER(REPLACE(FAM_FP_UTTAK_FP_KONTOER.STOENADSKONTOTYPE,
          ' ',
          ''))
          LEFT JOIN DT_PERSON.DIM_PERSON
          ON MOTTAKER.FK_PERSON1_MOTTAKER = DIM_PERSON.FK_PERSON1
 --and beregningsgrunnlag_agg.uttak_tom between dim_person.gyldig_fra_dato and dim_person.gyldig_til_dato
          AND TO_DATE(BEREGNINGSGRUNNLAG_AGG.PK_DIM_TID_DATO_UTBET_TOM,
          'yyyymmdd') BETWEEN DIM_PERSON.GYLDIG_FRA_DATO
          AND DIM_PERSON.GYLDIG_TIL_DATO
          LEFT JOIN DT_KODEVERK.DIM_GEOGRAFI
          ON DIM_PERSON.FK_DIM_GEOGRAFI_BOSTED = DIM_GEOGRAFI.PK_DIM_GEOGRAFI
          LEFT JOIN ALENEOMSORG
          ON ALENEOMSORG.FAGSAK_ID = BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID
          AND ALENEOMSORG.UTTAK_FOM = BEREGNINGSGRUNNLAG_AGG.UTTAK_FOM
          LEFT JOIN (
            SELECT
              TRANS_ID
            FROM
              DVH_FAM_FP.FAM_FP_UTTAK_FP_KONTOER
            WHERE
              UPPER(STOENADSKONTOTYPE) = 'MØDREKVOTE'
            GROUP BY
              TRANS_ID
          ) RETT_TIL_MØDREKVOTE
          ON RETT_TIL_MØDREKVOTE.TRANS_ID = BEREGNINGSGRUNNLAG_AGG.TRANS_ID
          LEFT JOIN (
            SELECT
              TRANS_ID
            FROM
              DVH_FAM_FP.FAM_FP_UTTAK_FP_KONTOER
            WHERE
              UPPER(STOENADSKONTOTYPE) = 'FEDREKVOTE'
            GROUP BY
              TRANS_ID
          ) RETT_TIL_FEDREKVOTE
          ON RETT_TIL_FEDREKVOTE.TRANS_ID = BEREGNINGSGRUNNLAG_AGG.TRANS_ID
          LEFT JOIN (
            SELECT
              TRANS_ID,
              MAX(MAX_DAGER) AS FLERBARNSDAGER
            FROM
              DVH_FAM_FP.FAM_FP_UTTAK_FP_KONTOER
            WHERE
              UPPER(STOENADSKONTOTYPE) = 'FLERBARNSDAGER'
            GROUP BY
              TRANS_ID
          ) FLERBARNSDAGER
          ON FLERBARNSDAGER.TRANS_ID = BEREGNINGSGRUNNLAG_AGG.TRANS_ID
          LEFT JOIN ADOPSJON
          ON BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID = ADOPSJON.FAGSAK_ID
          LEFT JOIN EOS
          ON BEREGNINGSGRUNNLAG_AGG.TRANS_ID = EOS.TRANS_ID
      )
      SELECT /*+ PARALLEL(8) */
        *
 --from uttak_dager
      FROM
        GRUNNLAG
      WHERE
        FAGSAK_ID NOT IN (1679117)
 --where fagsak_id in (1035184)
;
    V_TID_FOM                      VARCHAR2(8) := NULL;
    V_TID_TOM                      VARCHAR2(8) := NULL;
    V_COMMIT                       NUMBER := 0;
    V_ERROR_MELDING                VARCHAR2(1000) := NULL;
    V_DIM_TID_ANTALL               NUMBER := 0;
    V_UTBETALINGSPROSENT_KALKULERT NUMBER := 0;
  BEGIN
    V_TID_FOM := SUBSTR(P_IN_VEDTAK_TOM, 1, 4)
                 || '0101';
    V_TID_TOM := SUBSTR(P_IN_VEDTAK_TOM, 1, 4)
                 || '1231';
 --dbms_output.put_line(v_tid_fom||v_tid_tom);--TEST!!!
    FOR REC_PERIODE IN CUR_PERIODE(P_IN_RAPPORT_DATO, P_IN_FORSKYVNINGER, V_TID_FOM, V_TID_TOM) LOOP
      V_DIM_TID_ANTALL := 0;
      V_UTBETALINGSPROSENT_KALKULERT := 0;
      V_DIM_TID_ANTALL := DIM_TID_ANTALL(TO_NUMBER(TO_CHAR(REC_PERIODE.UTTAK_FOM, 'yyyymmdd')), TO_NUMBER(TO_CHAR(REC_PERIODE.UTTAK_TOM, 'yyyymmdd')));
      IF V_DIM_TID_ANTALL != 0 THEN
        V_UTBETALINGSPROSENT_KALKULERT := ROUND(REC_PERIODE.TREKKDAGER/V_DIM_TID_ANTALL*100, 2);
      ELSE
        V_UTBETALINGSPROSENT_KALKULERT := 0;
      END IF;
 --dbms_output.put_line(v_dim_tid_antall);
      BEGIN
        INSERT INTO DVH_FAM_FP.FAK_FAM_FP_VEDTAK_UTBETALING (
          FAGSAK_ID,
          TRANS_ID,
          BEHANDLINGSTEMA,
          TREKKONTO,
          STONADSDAGER_KVOTE,
          UTTAK_ARBEID_TYPE,
          AAR --, halvaar, --AAR_MAANED,
,
          RAPPORT_PERIODE,
          UTTAK_FOM,
          UTTAK_TOM,
          DAGER_ERST,
          BEREGNINGSGRUNNLAG_FOM,
          BEREGNINGSGRUNNLAG_TOM,
          DEKNINGSGRAD,
          DAGSATS_BRUKER,
          DAGSATS_ARBEIDSGIVER,
          VIRKSOMHET,
          PERIODE_RESULTAT_AARSAK,
          DAGSATS,
          GRADERINGSPROSENT,
          STATUS_OG_ANDEL_INNTEKTSKAT,
          AKTIVITET_STATUS,
          BRUTTO_INNTEKT,
          AVKORTET_INNTEKT,
          STATUS_OG_ANDEL_BRUTTO,
          STATUS_OG_ANDEL_AVKORTET,
          UTBETALINGSPROSENT,
          FK_DIM_TID_DATO_UTBET_FOM,
          FK_DIM_TID_DATO_UTBET_TOM,
          FUNKSJONELL_TID,
          FORSTE_VEDTAKSDATO,
          VEDTAKSDATO,
          MAX_VEDTAKSDATO,
          PERIODE_TYPE,
          TILFELLE_ERST,
          BELOP,
          DAGSATS_REDUSERT,
          LASTET_DATO,
          MAX_TRANS_ID,
          FK_PERSON1_MOTTAKER,
          FK_PERSON1_ANNEN_PART,
          KJONN,
          FK_PERSON1_BARN,
          TERMINDATO,
          FOEDSELSDATO,
          ANTALL_BARN_TERMIN,
          ANTALL_BARN_FOEDSEL,
          FOEDSELSDATO_ADOPSJON,
          ANTALL_BARN_ADOPSJON,
          ANNENFORELDERFAGSAK_ID,
          MAX_STONADSDAGER_KONTO,
          FK_DIM_PERSON,
          BOSTED_KOMMUNE_NR,
          FK_DIM_GEOGRAFI,
          BYDEL_KOMMUNE_NR,
          KOMMUNE_NR,
          KOMMUNE_NAVN,
          BYDEL_NR,
          BYDEL_NAVN,
          ALENEOMSORG,
          HOVEDKONTONR,
          UNDERKONTONR,
          MOTTAKER_FODSELS_AAR,
          MOTTAKER_FODSELS_MND,
          MOTTAKER_ALDER,
          RETT_TIL_FEDREKVOTE,
          RETT_TIL_MODREKVOTE,
          DAGSATS_ERST,
          TREKKDAGER,
          SAMTIDIG_UTTAK,
          GRADERING,
          GRADERING_INNVILGET,
          ANTALL_DAGER_PERIODE,
          FLERBARNSDAGER,
          UTBETALINGSPROSENT_KALKULERT,
          MIN_UTTAK_FOM,
          MAX_UTTAK_TOM,
          FK_FAM_FP_TREKKONTO,
          FK_FAM_FP_PERIODE_RESULTAT_AARSAK,
          SIVILSTATUS,
          FK_DIM_SIVILSTATUS,
          ANTALL_BEREGNINGSGRUNNLAG,
          GRADERINGSDAGER,
          FK_DIM_TID_MIN_DATO_KVOTE,
          FK_DIM_TID_MAX_DATO_KVOTE,
          ADOPSJONSDATO,
          STEBARNSADOPSJON,
          EOS_SAK,
          MOR_RETTIGHET,
          STATSBORGERSKAP,
          ARBEIDSTIDSPROSENT,
          MORS_AKTIVITET,
          GYLDIG_FLAGG,
          ANDEL_AV_REFUSJON,
          FORSTE_SOKNADSDATO,
          SOKNADSDATO
        ) VALUES (
          REC_PERIODE.FAGSAK_ID,
          REC_PERIODE.TRANS_ID,
          REC_PERIODE.BEHANDLINGSTEMA,
          REC_PERIODE.TREKKONTO,
          REC_PERIODE.STONADSDAGER_KVOTE,
          REC_PERIODE.UTTAK_ARBEID_TYPE,
          REC_PERIODE.AAR --, rec_periode.halvaar--, AAR_MAANED
,
          P_IN_RAPPORT_DATO,
          REC_PERIODE.UTTAK_FOM,
          REC_PERIODE.UTTAK_TOM,
          REC_PERIODE.DAGER_ERST,
          REC_PERIODE.BEREGNINGSGRUNNLAG_FOM,
          REC_PERIODE.BEREGNINGSGRUNNLAG_TOM,
          REC_PERIODE.DEKNINGSGRAD,
          REC_PERIODE.DAGSATS_BRUKER,
          REC_PERIODE.DAGSATS_ARBEIDSGIVER,
          REC_PERIODE.VIRKSOMHET,
          REC_PERIODE.PERIODE_RESULTAT_AARSAK,
          REC_PERIODE.DAGSATS,
          REC_PERIODE.GRADERINGSPROSENT,
          REC_PERIODE.STATUS_OG_ANDEL_INNTEKTSKAT,
          REC_PERIODE.AKTIVITET_STATUS,
          REC_PERIODE.BRUTTO_INNTEKT,
          REC_PERIODE.AVKORTET_INNTEKT,
          REC_PERIODE.STATUS_OG_ANDEL_BRUTTO,
          REC_PERIODE.STATUS_OG_ANDEL_AVKORTET,
          REC_PERIODE.UTBETALINGSPROSENT,
          REC_PERIODE.PK_DIM_TID_DATO_UTBET_FOM,
          REC_PERIODE.PK_DIM_TID_DATO_UTBET_TOM,
          REC_PERIODE.FUNKSJONELL_TID,
          REC_PERIODE.FORSTE_VEDTAKSDATO,
          REC_PERIODE.SISTE_VEDTAKSDATO,
          REC_PERIODE.MAX_VEDTAKSDATO,
          REC_PERIODE.PERIODE,
          REC_PERIODE.TILFELLE_ERST,
          REC_PERIODE.BELOP,
          REC_PERIODE.DAGSATS_REDUSERT,
          REC_PERIODE.LASTET_DATO,
          REC_PERIODE.MAX_TRANS_ID,
          REC_PERIODE.FK_PERSON1_MOTTAKER,
          REC_PERIODE.FK_PERSON1_ANNEN_PART,
          REC_PERIODE.KJONN,
          REC_PERIODE.FK_PERSON1_BARN,
          REC_PERIODE.TERMINDATO,
          REC_PERIODE.FOEDSELSDATO,
          REC_PERIODE.ANTALL_BARN_TERMIN,
          REC_PERIODE.ANTALL_BARN_FOEDSEL,
          REC_PERIODE.FOEDSELSDATO_ADOPSJON,
          REC_PERIODE.ANTALL_BARN_ADOPSJON,
          REC_PERIODE.ANNENFORELDERFAGSAK_ID,
          REC_PERIODE.MAX_STONADSDAGER_KONTO,
          REC_PERIODE.PK_DIM_PERSON,
          REC_PERIODE.BOSTED_KOMMUNE_NR,
          REC_PERIODE.PK_DIM_GEOGRAFI,
          REC_PERIODE.BYDEL_KOMMUNE_NR,
          REC_PERIODE.KOMMUNE_NR,
          REC_PERIODE.KOMMUNE_NAVN,
          REC_PERIODE.BYDEL_NR,
          REC_PERIODE.BYDEL_NAVN,
          REC_PERIODE.ALENEOMSORG,
          REC_PERIODE.HOVEDKONTONR,
          REC_PERIODE.UNDERKONTONR,
          REC_PERIODE.MOTTAKER_FODSELS_AAR,
          REC_PERIODE.MOTTAKER_FODSELS_MND,
          REC_PERIODE.MOTTAKER_ALDER,
          REC_PERIODE.RETT_TIL_FEDREKVOTE,
          REC_PERIODE.RETT_TIL_MØDREKVOTE,
          REC_PERIODE.DAGSATS_ERST,
          REC_PERIODE.TREKKDAGER,
          REC_PERIODE.SAMTIDIG_UTTAK,
          REC_PERIODE.GRADERING,
          REC_PERIODE.GRADERING_INNVILGET,
          V_DIM_TID_ANTALL,
          REC_PERIODE.FLERBARNSDAGER,
          V_UTBETALINGSPROSENT_KALKULERT,
          REC_PERIODE.MIN_UTTAK_FOM,
          REC_PERIODE.MAX_UTTAK_TOM,
          REC_PERIODE.PK_FAM_FP_TREKKONTO,
          REC_PERIODE.PK_FAM_FP_PERIODE_RESULTAT_AARSAK,
          REC_PERIODE.SIVILSTAND,
          REC_PERIODE.FK_DIM_SIVILSTATUS,
          REC_PERIODE.ANTALL_BEREGNINGSGRUNNLAG,
          REC_PERIODE.GRADERINGSDAGER,
          REC_PERIODE.FK_DIM_TID_MIN_DATO_KVOTE,
          REC_PERIODE.FK_DIM_TID_MAX_DATO_KVOTE,
          REC_PERIODE.ADOPSJONSDATO,
          REC_PERIODE.STEBARNSADOPSJON,
          REC_PERIODE.EOS_SAK,
          REC_PERIODE.MOR_RETTIGHET,
          REC_PERIODE.STATSBORGERSKAP,
          REC_PERIODE.ARBEIDSTIDSPROSENT,
          REC_PERIODE.MORS_AKTIVITET,
          P_IN_GYLDIG_FLAGG,
          REC_PERIODE.ANDEL_AV_REFUSJON,
          REC_PERIODE.FORSTE_SOKNADSDATO,
          REC_PERIODE.SOKNADSDATO
        );
        V_COMMIT := V_COMMIT + 1;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK;
          V_ERROR_MELDING := SUBSTR(SQLCODE
                                    || ' '
                                    || SQLERRM, 1, 1000);
          INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
            MIN_LASTET_DATO,
            ID,
            ERROR_MSG,
            OPPRETTET_TID,
            KILDE
          ) VALUES(
            NULL,
            REC_PERIODE.FAGSAK_ID,
            V_ERROR_MELDING,
            SYSDATE,
            'FAM_FP_STATISTIKK_AAR:INSERT'
          );
          COMMIT;
          P_OUT_ERROR := SUBSTR(P_OUT_ERROR
                                || V_ERROR_MELDING, 1, 1000);
      END;
      IF V_COMMIT > 100000 THEN
        COMMIT;
        V_COMMIT := 0;
      END IF;
    END LOOP;
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_ERROR_MELDING := SUBSTR(SQLCODE
                                || ' '
                                || SQLERRM, 1, 1000);
      INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
        MIN_LASTET_DATO,
        ID,
        ERROR_MSG,
        OPPRETTET_TID,
        KILDE
      ) VALUES(
        NULL,
        NULL,
        V_ERROR_MELDING,
        SYSDATE,
        'FAM_FP_STATISTIKK_AAR'
      );
      COMMIT;
      P_OUT_ERROR := SUBSTR(P_OUT_ERROR
                            || V_ERROR_MELDING, 1, 1000);
  END FAM_FP_STATISTIKK_AAR;

  PROCEDURE FAM_FP_STATISTIKK_AAR_MND(
    P_IN_VEDTAK_TOM IN VARCHAR2,
    P_IN_RAPPORT_DATO IN VARCHAR2,
    P_IN_FORSKYVNINGER IN NUMBER,
    P_IN_GYLDIG_FLAGG IN NUMBER DEFAULT 0,
    P_IN_PERIODE_TYPE IN VARCHAR2 DEFAULT 'A',
    P_OUT_ERROR OUT VARCHAR2
  ) AS
    CURSOR CUR_PERIODE(P_RAPPORT_DATO IN VARCHAR2, P_FORSKYVNINGER IN NUMBER, P_TID_FOM IN VARCHAR2, P_TID_TOM IN VARCHAR2) IS
      WITH FAGSAK AS (
        SELECT
          FAGSAK_ID,
          MAX(BEHANDLINGSTEMA)                                                   AS BEHANDLINGSTEMA,
          MAX(FAGSAKANNENFORELDER_ID)                                            AS ANNENFORELDERFAGSAK_ID,
          MAX(TRANS_ID) KEEP(DENSE_RANK FIRST ORDER BY FUNKSJONELL_TID DESC)     AS MAX_TRANS_ID,
          MAX(SOEKNADSDATO) KEEP(DENSE_RANK FIRST ORDER BY FUNKSJONELL_TID DESC) AS SOKNADSDATO,
          MIN(SOEKNADSDATO)                                                      AS FORSTE_SOKNADSDATO,
          MIN(VEDTAKSDATO)                                                       AS FORSTE_VEDTAKSDATO,
          MAX(FUNKSJONELL_TID)                                                   AS FUNKSJONELL_TID,
          MAX(VEDTAKSDATO)                                                       AS SISTE_VEDTAKSDATO,
          P_IN_PERIODE_TYPE                                                      AS PERIODE,
          LAST_DAY(TO_DATE(P_RAPPORT_DATO, 'yyyymm')) + P_FORSKYVNINGER          AS MAX_VEDTAKSDATO
        FROM
          DVH_FAM_FP.FAM_FP_FAGSAK
        WHERE
          FAM_FP_FAGSAK.FUNKSJONELL_TID <= LAST_DAY(TO_DATE(P_RAPPORT_DATO, 'yyyymm')) + P_FORSKYVNINGER
        GROUP BY
          FAGSAK_ID
      ), TERMIN AS (
        SELECT
          FAGSAK_ID,
          MAX(TERMINDATO)            TERMINDATO,
          MAX(FOEDSELSDATO)          FOEDSELSDATO,
          MAX(ANTALL_BARN_TERMIN)    ANTALL_BARN_TERMIN,
          MAX(ANTALL_BARN_FOEDSEL)   ANTALL_BARN_FOEDSEL,
          MAX(FOEDSELSDATO_ADOPSJON) FOEDSELSDATO_ADOPSJON,
          MAX(ANTALL_BARN_ADOPSJON) ANTALL_BARN_ADOPSJON
        FROM
          (
            SELECT
              FAM_FP_FAGSAK.FAGSAK_ID,
              MAX(FODSEL.TERMINDATO)              TERMINDATO,
              MAX(FODSEL.FOEDSELSDATO)            FOEDSELSDATO,
              MAX(FODSEL.ANTALL_BARN_FOEDSEL)     ANTALL_BARN_FOEDSEL,
              MAX(FODSEL.ANTALL_BARN_TERMIN)      ANTALL_BARN_TERMIN,
              MAX(ADOPSJON.FOEDSELSDATO_ADOPSJON) FOEDSELSDATO_ADOPSJON,
              COUNT(ADOPSJON.TRANS_ID)            ANTALL_BARN_ADOPSJON
            FROM
              DVH_FAM_FP.FAM_FP_FAGSAK
              LEFT JOIN DVH_FAM_FP.FAM_FP_FODSELTERMIN FODSEL
              ON FODSEL.FAGSAK_ID = FAM_FP_FAGSAK.FAGSAK_ID
              AND UPPER(FAM_FP_FAGSAK.BEHANDLINGSTEMA) = 'FORP_FODS'
              LEFT JOIN DVH_FAM_FP.FAM_FP_FODSELTERMIN ADOPSJON
              ON ADOPSJON.FAGSAK_ID = FAM_FP_FAGSAK.FAGSAK_ID
              AND ADOPSJON.TRANS_ID = FAM_FP_FAGSAK.TRANS_ID
              AND UPPER(FAM_FP_FAGSAK.BEHANDLINGSTEMA) = 'FORP_ADOP'
            GROUP BY
              FAM_FP_FAGSAK.FAGSAK_ID,
              FAM_FP_FAGSAK.TRANS_ID
          )
        GROUP BY
          FAGSAK_ID
      ), FK_PERSON1 AS (
        SELECT
          PERSON.PERSON,
          PERSON.FAGSAK_ID,
          MAX(PERSON.BEHANDLINGSTEMA)                                                                             AS BEHANDLINGSTEMA,
          PERSON.MAX_TRANS_ID,
          MAX(PERSON.ANNENFORELDERFAGSAK_ID)                                                                      AS ANNENFORELDERFAGSAK_ID,
          PERSON.AKTOER_ID,
          MAX(PERSON.KJONN)                                                                                       AS KJONN,
          MAX(PERSON_67_VASKET.FK_PERSON1) KEEP (DENSE_RANK FIRST ORDER BY PERSON_67_VASKET.GYLDIG_FRA_DATO DESC) AS FK_PERSON1,
          MAX(FOEDSELSDATO)                                                                                       AS FOEDSELSDATO,
          MAX(SIVILSTAND)                                                                                         AS SIVILSTAND,
          MAX(STATSBORGERSKAP)                                                                                    AS STATSBORGERSKAP
        FROM
          (
            SELECT
              'MOTTAKER'                                AS PERSON,
              FAGSAK.FAGSAK_ID,
              FAGSAK.BEHANDLINGSTEMA,
              FAGSAK.MAX_TRANS_ID,
              FAGSAK.ANNENFORELDERFAGSAK_ID,
              FAM_FP_PERSONOPPLYSNINGER.AKTOER_ID,
              FAM_FP_PERSONOPPLYSNINGER.KJONN,
              FAM_FP_PERSONOPPLYSNINGER.FOEDSELSDATO,
              FAM_FP_PERSONOPPLYSNINGER.SIVILSTAND,
              FAM_FP_PERSONOPPLYSNINGER.STATSBORGERSKAP
            FROM
              DVH_FAM_FP.FAM_FP_PERSONOPPLYSNINGER
              JOIN FAGSAK
              ON FAM_FP_PERSONOPPLYSNINGER.TRANS_ID = FAGSAK.MAX_TRANS_ID UNION ALL
              SELECT
                'BARN'                                    AS PERSON,
                FAGSAK.FAGSAK_ID,
                MAX(FAGSAK.BEHANDLINGSTEMA)               AS BEHANDLINGSTEMA,
                FAGSAK.MAX_TRANS_ID,
                MAX(FAGSAK.ANNENFORELDERFAGSAK_ID)        ANNENFORELDERFAGSAK_ID,
                MAX(FAM_FP_FAMILIEHENDELSE.TIL_AKTOER_ID) AS AKTOER_ID,
                MAX(FAM_FP_FAMILIEHENDELSE.KJOENN)        AS KJONN,
                NULL                                      AS FOEDSELSDATO,
                NULL                                      AS SIVILSTAND,
                NULL                                      AS STATSBORGERSKAP
              FROM
                DVH_FAM_FP.FAM_FP_FAMILIEHENDELSE
                JOIN FAGSAK
                ON FAM_FP_FAMILIEHENDELSE.FAGSAK_ID = FAGSAK.FAGSAK_ID
              WHERE
                UPPER(FAM_FP_FAMILIEHENDELSE.RELASJON) = 'BARN'
              GROUP BY
                FAGSAK.FAGSAK_ID, FAGSAK.MAX_TRANS_ID
          )                                    PERSON
          JOIN DT_PERSON.DVH_PERSON_IDENT_AKTOR_IKKE_SKJERMET PERSON_67_VASKET
          ON PERSON_67_VASKET.AKTOR_ID = PERSON.AKTOER_ID
          AND TO_DATE(P_TID_TOM, 'yyyymmdd') BETWEEN PERSON_67_VASKET.GYLDIG_FRA_DATO
          AND PERSON_67_VASKET.GYLDIG_TIL_DATO
        GROUP BY
          PERSON.PERSON, PERSON.FAGSAK_ID, PERSON.MAX_TRANS_ID, PERSON.AKTOER_ID
      ), BARN AS (
        SELECT
          FAGSAK_ID,
          LISTAGG(FK_PERSON1, ',') WITHIN GROUP (ORDER BY FK_PERSON1) AS FK_PERSON1_BARN
        FROM
          FK_PERSON1
        WHERE
          PERSON = 'BARN'
        GROUP BY
          FAGSAK_ID
      ), MOTTAKER AS (
        SELECT
          FK_PERSON1.FAGSAK_ID,
          FK_PERSON1.BEHANDLINGSTEMA,
          FK_PERSON1.MAX_TRANS_ID,
          FK_PERSON1.ANNENFORELDERFAGSAK_ID,
          FK_PERSON1.AKTOER_ID,
          FK_PERSON1.KJONN,
          FK_PERSON1.FK_PERSON1                       AS FK_PERSON1_MOTTAKER,
          EXTRACT(YEAR FROM FK_PERSON1.FOEDSELSDATO) AS MOTTAKER_FODSELS_AAR,
          EXTRACT(MONTH FROM FK_PERSON1.FOEDSELSDATO) AS MOTTAKER_FODSELS_MND,
          FK_PERSON1.SIVILSTAND,
          FK_PERSON1.STATSBORGERSKAP,
          BARN.FK_PERSON1_BARN,
          TERMIN.TERMINDATO,
          TERMIN.FOEDSELSDATO,
          TERMIN.ANTALL_BARN_TERMIN,
          TERMIN.ANTALL_BARN_FOEDSEL,
          TERMIN.FOEDSELSDATO_ADOPSJON,
          TERMIN.ANTALL_BARN_ADOPSJON
        FROM
          FK_PERSON1
          LEFT JOIN BARN
          ON BARN.FAGSAK_ID = FK_PERSON1.FAGSAK_ID
          LEFT JOIN TERMIN
          ON FK_PERSON1.FAGSAK_ID = TERMIN.FAGSAK_ID
        WHERE
          FK_PERSON1.PERSON = 'MOTTAKER'
      ), ADOPSJON AS (
        SELECT
          FAM_FP_VILKAAR.FAGSAK_ID,
          MAX(FAM_FP_VILKAAR.OMSORGS_OVERTAKELSESDATO) AS ADOPSJONSDATO,
          MAX(FAM_FP_VILKAAR.EKTEFELLES_BARN)          AS STEBARNSADOPSJON
        FROM
          FAGSAK
          JOIN DVH_FAM_FP.FAM_FP_VILKAAR
          ON FAGSAK.FAGSAK_ID = FAM_FP_VILKAAR.FAGSAK_ID
        WHERE
          FAGSAK.BEHANDLINGSTEMA = 'FORP_ADOP'
        GROUP BY
          FAM_FP_VILKAAR.FAGSAK_ID
      ), EOS AS (
        SELECT
          A.TRANS_ID,
          CASE
            WHEN UPPER(ER_BORGER_AV_EU_EOS) = 'TRUE' THEN
              'J'
            WHEN UPPER(ER_BORGER_AV_EU_EOS) = 'FALSE' THEN
              'N'
            ELSE
              NULL
          END EOS_SAK
        FROM
          (
            SELECT
              FAM_FP_VILKAAR.TRANS_ID,
              MAX(FAM_FP_VILKAAR.ER_BORGER_AV_EU_EOS) AS ER_BORGER_AV_EU_EOS
            FROM
              FAGSAK
              JOIN DVH_FAM_FP.FAM_FP_VILKAAR
              ON FAGSAK.MAX_TRANS_ID = FAM_FP_VILKAAR.TRANS_ID
              AND LENGTH(FAM_FP_VILKAAR.PERSON_STATUS) > 0
            GROUP BY
              FAM_FP_VILKAAR.TRANS_ID
          ) A
      ), ANNENFORELDERFAGSAK AS (
        SELECT
          ANNENFORELDERFAGSAK.*,
          MOTTAKER.FK_PERSON1_MOTTAKER AS FK_PERSON1_ANNEN_PART
        FROM
          (
            SELECT
              FAGSAK_ID,
              MAX_TRANS_ID,
              MAX(ANNENFORELDERFAGSAK_ID) AS ANNENFORELDERFAGSAK_ID
            FROM
              (
                SELECT
                  FORELDER1.FAGSAK_ID,
                  FORELDER1.MAX_TRANS_ID,
                  NVL(FORELDER1.ANNENFORELDERFAGSAK_ID, FORELDER2.FAGSAK_ID) AS ANNENFORELDERFAGSAK_ID
                FROM
                  MOTTAKER FORELDER1
                  JOIN MOTTAKER FORELDER2
                  ON FORELDER1.FK_PERSON1_BARN = FORELDER2.FK_PERSON1_BARN
                  AND FORELDER1.FK_PERSON1_MOTTAKER != FORELDER2.FK_PERSON1_MOTTAKER
              )
            GROUP BY
              FAGSAK_ID,
              MAX_TRANS_ID
          )        ANNENFORELDERFAGSAK
          JOIN MOTTAKER
          ON ANNENFORELDERFAGSAK.ANNENFORELDERFAGSAK_ID = MOTTAKER.FAGSAK_ID
      ), TID AS (
        SELECT
          PK_DIM_TID,
          DATO,
          AAR,
          HALVAAR,
          KVARTAL,
          AAR_MAANED
        FROM
          DT_KODEVERK.DIM_TID
        WHERE
          DAG_I_UKE < 6
          AND DIM_NIVAA = 1
          AND GYLDIG_FLAGG = 1
          AND PK_DIM_TID BETWEEN P_TID_FOM AND P_TID_TOM
 --and pk_dim_tid <= to_char(last_day(to_date(p_rapport_dato,'yyyymm')),'yyyymmdd')
      ), UTTAK AS (
        SELECT
          UTTAK.TRANS_ID,
          UTTAK.TREKKONTO,
          UTTAK.UTTAK_ARBEID_TYPE,
          UTTAK.VIRKSOMHET,
          UTTAK.UTBETALINGSPROSENT,
          UTTAK.GRADERING_INNVILGET,
          UTTAK.GRADERING,
          UTTAK.ARBEIDSTIDSPROSENT,
          UTTAK.SAMTIDIG_UTTAK,
          UTTAK.PERIODE_RESULTAT_AARSAK,
          UTTAK.FOM                                      AS UTTAK_FOM,
          UTTAK.TOM                                      AS UTTAK_TOM,
          UTTAK.TREKKDAGER,
          FAGSAK.FAGSAK_ID,
          FAGSAK.PERIODE,
          FAGSAK.FUNKSJONELL_TID,
          FAGSAK.FORSTE_VEDTAKSDATO,
          FAGSAK.SISTE_VEDTAKSDATO,
          FAGSAK.MAX_VEDTAKSDATO,
          FAGSAK.FORSTE_SOKNADSDATO,
          FAGSAK.SOKNADSDATO,
          FAM_FP_TREKKONTO.PK_FAM_FP_TREKKONTO,
          AARSAK_UTTAK.PK_FAM_FP_PERIODE_RESULTAT_AARSAK,
          UTTAK.ARBEIDSFORHOLD_ID,
          UTTAK.GRADERINGSDAGER,
          FAM_FP_UTTAK_FORDELINGSPER.MORS_AKTIVITET
        FROM
          DVH_FAM_FP.FAM_FP_UTTAK_RES_PER_AKTIV UTTAK
          JOIN FAGSAK
          ON FAGSAK.MAX_TRANS_ID = UTTAK.TRANS_ID LEFT JOIN DVH_FAM_FP.FAM_FP_TREKKONTO
          ON UPPER(UTTAK.TREKKONTO) = FAM_FP_TREKKONTO.TREKKONTO
          LEFT JOIN (
            SELECT
              AARSAK_UTTAK,
              MAX(PK_FAM_FP_PERIODE_RESULTAT_AARSAK) AS PK_FAM_FP_PERIODE_RESULTAT_AARSAK
            FROM
              DVH_FAM_FP.FAM_FP_PERIODE_RESULTAT_AARSAK
            GROUP BY
              AARSAK_UTTAK
          ) AARSAK_UTTAK
          ON UPPER(UTTAK.PERIODE_RESULTAT_AARSAK) = AARSAK_UTTAK.AARSAK_UTTAK
          LEFT JOIN DVH_FAM_FP.FAM_FP_UTTAK_FORDELINGSPER
          ON FAM_FP_UTTAK_FORDELINGSPER.TRANS_ID = UTTAK.TRANS_ID
          AND UTTAK.FOM BETWEEN FAM_FP_UTTAK_FORDELINGSPER.FOM
          AND FAM_FP_UTTAK_FORDELINGSPER.TOM
          AND UPPER(UTTAK.TREKKONTO) = UPPER(FAM_FP_UTTAK_FORDELINGSPER.PERIODE_TYPE)
          AND LENGTH(FAM_FP_UTTAK_FORDELINGSPER.MORS_AKTIVITET) > 1
        WHERE
          UTTAK.UTBETALINGSPROSENT > 0
      ), STONADSDAGER_KVOTE AS (
        SELECT
          UTTAK.*,
          TID1.PK_DIM_TID AS FK_DIM_TID_MIN_DATO_KVOTE,
          TID2.PK_DIM_TID AS FK_DIM_TID_MAX_DATO_KVOTE
        FROM
          (
            SELECT
              FAGSAK_ID,
              TRANS_ID,
              TREKKONTO,
              VIRKSOMHET,
              UTTAK_ARBEID_TYPE,
              SUM(TREKKDAGER)   AS STONADSDAGER_KVOTE,
              MIN(UTTAK_FOM)    AS MIN_UTTAK_FOM,
              MAX(UTTAK_TOM)    AS MAX_UTTAK_TOM
            FROM
              (
                SELECT
                  FAGSAK_ID,
                  TRANS_ID,
                  UTTAK_FOM,
                  UTTAK_TOM,
                  TREKKONTO,
                  VIRKSOMHET,
                  UTTAK_ARBEID_TYPE,
                  MAX(TREKKDAGER)   AS TREKKDAGER
                FROM
                  UTTAK
                GROUP BY
                  FAGSAK_ID,
                  TRANS_ID,
                  UTTAK_FOM,
                  UTTAK_TOM,
                  TREKKONTO,
                  VIRKSOMHET,
                  UTTAK_ARBEID_TYPE
              ) A
            GROUP BY
              FAGSAK_ID,
              TRANS_ID,
              TREKKONTO,
              VIRKSOMHET,
              UTTAK_ARBEID_TYPE
          )                   UTTAK
          JOIN DT_KODEVERK.DIM_TID TID1
          ON TID1.DIM_NIVAA = 1
          AND TID1.DATO = TRUNC(UTTAK.MIN_UTTAK_FOM, 'dd') JOIN DT_KODEVERK.DIM_TID TID2
          ON TID2.DIM_NIVAA = 1
          AND TID2.DATO = TRUNC(UTTAK.MAX_UTTAK_TOM,
          'dd')
      ), UTTAK_DAGER AS (
        SELECT
          UTTAK.*,
          TID.PK_DIM_TID,
          TID.DATO,
          TID.AAR,
          TID.HALVAAR,
          TID.KVARTAL,
          TID.AAR_MAANED
        FROM
          UTTAK
          JOIN TID
          ON TID.DATO BETWEEN UTTAK.UTTAK_FOM
          AND UTTAK.UTTAK_TOM
      ), ALENEOMSORG AS (
        SELECT
          UTTAK.FAGSAK_ID,
          UTTAK.UTTAK_FOM
        FROM
          UTTAK
          JOIN DVH_FAM_FP.FAM_FP_DOKUMENTASJONSPERIODER DOK1
          ON DOK1.FAGSAK_ID = UTTAK.FAGSAK_ID
          AND UTTAK.UTTAK_FOM >= DOK1.FOM
          AND DOK1.DOKUMENTASJON_TYPE IN ('ALENEOMSORG', 'ALENEOMSORG_OVERFØRING') LEFT JOIN DVH_FAM_FP.FAM_FP_DOKUMENTASJONSPERIODER DOK2
          ON DOK1.FAGSAK_ID = DOK2.FAGSAK_ID
          AND UTTAK.UTTAK_FOM >= DOK2.FOM
          AND DOK1.TRANS_ID < DOK2.TRANS_ID
          AND DOK2.DOKUMENTASJON_TYPE = 'ANNEN_FORELDER_HAR_RETT'
          AND DOK2.FAGSAK_ID IS NULL
        GROUP BY
          UTTAK.FAGSAK_ID,
          UTTAK.UTTAK_FOM
      ), BEREGNINGSGRUNNLAG AS (
        SELECT
          FAGSAK_ID,
          TRANS_ID,
          VIRKSOMHETSNUMMER,
          MAX(STATUS_OG_ANDEL_BRUTTO)         AS STATUS_OG_ANDEL_BRUTTO,
          MAX(STATUS_OG_ANDEL_AVKORTET)       AS STATUS_OG_ANDEL_AVKORTET,
          FOM                                 AS BEREGNINGSGRUNNLAG_FOM,
          TOM                                 AS BEREGNINGSGRUNNLAG_TOM,
          MAX(DEKNINGSGRAD)                   AS DEKNINGSGRAD,
          MAX(DAGSATS)                        AS DAGSATS,
          DAGSATS_BRUKER,
          DAGSATS_ARBEIDSGIVER,
          DAGSATS_BRUKER+DAGSATS_ARBEIDSGIVER DAGSATS_VIRKSOMHET,
          MAX(STATUS_OG_ANDEL_INNTEKTSKAT)    AS STATUS_OG_ANDEL_INNTEKTSKAT,
          AKTIVITET_STATUS,
          MAX(BRUTTO)                         AS BRUTTO_INNTEKT,
          MAX(AVKORTET)                       AS AVKORTET_INNTEKT,
          COUNT(1)                            AS ANTALL_BEREGNINGSGRUNNLAG
        FROM
          DVH_FAM_FP.FAM_FP_BEREGNINGSGRUNNLAG
        GROUP BY
          FAGSAK_ID,
          TRANS_ID,
          VIRKSOMHETSNUMMER,
          FOM,
          TOM,
          AKTIVITET_STATUS,
          DAGSATS_BRUKER,
          DAGSATS_ARBEIDSGIVER
      ), BEREGNINGSGRUNNLAG_DETALJ AS (
        SELECT
          UTTAK_DAGER.*,
          STONADSDAGER_KVOTE.STONADSDAGER_KVOTE,
          STONADSDAGER_KVOTE.MIN_UTTAK_FOM,
          STONADSDAGER_KVOTE.MAX_UTTAK_TOM,
          STONADSDAGER_KVOTE.FK_DIM_TID_MIN_DATO_KVOTE,
          STONADSDAGER_KVOTE.FK_DIM_TID_MAX_DATO_KVOTE,
          BEREG.STATUS_OG_ANDEL_BRUTTO,
          BEREG.STATUS_OG_ANDEL_AVKORTET,
          BEREG.BEREGNINGSGRUNNLAG_FOM,
          BEREG.DEKNINGSGRAD,
          BEREG.BEREGNINGSGRUNNLAG_TOM,
          BEREG.DAGSATS,
          BEREG.DAGSATS_BRUKER,
          BEREG.DAGSATS_ARBEIDSGIVER,
          BEREG.DAGSATS_VIRKSOMHET,
          BEREG.STATUS_OG_ANDEL_INNTEKTSKAT,
          BEREG.AKTIVITET_STATUS,
          BEREG.BRUTTO_INNTEKT,
          BEREG.AVKORTET_INNTEKT,
          BEREG.DAGSATS*UTTAK_DAGER.UTBETALINGSPROSENT/100 AS DAGSATS_ERST,
          BEREG.ANTALL_BEREGNINGSGRUNNLAG
        FROM
          BEREGNINGSGRUNNLAG                        BEREG
          JOIN UTTAK_DAGER
          ON UTTAK_DAGER.TRANS_ID = BEREG.TRANS_ID
          AND NVL(UTTAK_DAGER.VIRKSOMHET, 'X') = NVL(BEREG.VIRKSOMHETSNUMMER, 'X')
          AND BEREG.BEREGNINGSGRUNNLAG_FOM <= UTTAK_DAGER.DATO
          AND NVL(BEREG.BEREGNINGSGRUNNLAG_TOM, TO_DATE('20991201', 'YYYYMMDD')) >= UTTAK_DAGER.DATO LEFT JOIN STONADSDAGER_KVOTE
          ON UTTAK_DAGER.TRANS_ID = STONADSDAGER_KVOTE.TRANS_ID
          AND UTTAK_DAGER.TREKKONTO = STONADSDAGER_KVOTE.TREKKONTO
          AND NVL(UTTAK_DAGER.VIRKSOMHET,
          'X') = NVL(STONADSDAGER_KVOTE.VIRKSOMHET,
          'X')
          AND UTTAK_DAGER.UTTAK_ARBEID_TYPE = STONADSDAGER_KVOTE.UTTAK_ARBEID_TYPE
          JOIN DVH_FAM_FP.FAM_FP_UTTAK_AKTIVITET_MAPPING UTTAK_MAPPING
          ON UTTAK_DAGER.UTTAK_ARBEID_TYPE = UTTAK_MAPPING.UTTAK_ARBEID
          AND BEREG.AKTIVITET_STATUS = UTTAK_MAPPING.AKTIVITET_STATUS
        WHERE
          BEREG.DAGSATS_BRUKER + BEREG.DAGSATS_ARBEIDSGIVER != 0
      ), BEREGNINGSGRUNNLAG_AGG AS (
        SELECT
          A.*,
          DAGER_ERST*DAGSATS_VIRKSOMHET/DAGSATS*ANTALL_BEREGNINGSGRUNNLAG                                                                                                                    TILFELLE_ERST,
          DAGER_ERST*ROUND(UTBETALINGSPROSENT/100*DAGSATS_VIRKSOMHET)                                                                                                                        BELOP,
          ROUND(UTBETALINGSPROSENT/100*DAGSATS_VIRKSOMHET-0.5)                                                                                                                               DAGSATS_REDUSERT,
          CASE
            WHEN PERIODE_RESULTAT_AARSAK IN (2004, 2033) THEN
              'N'
            WHEN TREKKONTO IN ('FEDREKVOTE', 'FELLESPERIODE', 'MØDREKVOTE') THEN
              'J'
            WHEN TREKKONTO = 'FORELDREPENGER' THEN
              'N'
          END MOR_RETTIGHET
        FROM
          (
            SELECT
              FAGSAK_ID,
              TRANS_ID,
              TREKKONTO,
              TREKKDAGER,
              STONADSDAGER_KVOTE,
              UTTAK_ARBEID_TYPE,
              AAR,
              HALVAAR,
              KVARTAL,
              AAR_MAANED,
              UTTAK_FOM,
              UTTAK_TOM,
              SUM(DAGSATS_VIRKSOMHET/DAGSATS*
                CASE
                  WHEN ((UPPER(GRADERING_INNVILGET) ='TRUE'
                  AND UPPER(GRADERING)='TRUE')
                  OR UPPER(SAMTIDIG_UTTAK)='TRUE') THEN
                    (100-ARBEIDSTIDSPROSENT)/100
                  ELSE
                    1.0
                END )                                DAGER_ERST2,
              MAX(ARBEIDSTIDSPROSENT)                AS ARBEIDSTIDSPROSENT,
              COUNT(DISTINCT PK_DIM_TID)             DAGER_ERST,
 --count(distinct pk_dim_tid)*dagsats_virksomhet/dagsats tilfelle_erst,
              MIN(BEREGNINGSGRUNNLAG_FOM)            BEREGNINGSGRUNNLAG_FOM,
              MAX(BEREGNINGSGRUNNLAG_TOM)            BEREGNINGSGRUNNLAG_TOM,
              DEKNINGSGRAD,
 --count(distinct pk_dim_tid)*
 --      round(utbetalingsprosent/100*dagsats_virksomhet-0.5) belop,
              DAGSATS_BRUKER,
              DAGSATS_ARBEIDSGIVER,
              DAGSATS_VIRKSOMHET,
              VIRKSOMHET,
              PERIODE_RESULTAT_AARSAK,
              DAGSATS,
              DAGSATS_ERST, --dagsats_virksomhet,
              UTBETALINGSPROSENT                     GRADERINGSPROSENT,
              STATUS_OG_ANDEL_INNTEKTSKAT,
              AKTIVITET_STATUS,
              BRUTTO_INNTEKT,
              AVKORTET_INNTEKT,
              STATUS_OG_ANDEL_BRUTTO,
              STATUS_OG_ANDEL_AVKORTET,
 --round(utbetalingsprosent/100*dagsats_virksomhet-0.5) dagsats_redusert,
              UTBETALINGSPROSENT,
              MIN(PK_DIM_TID)                        PK_DIM_TID_DATO_UTBET_FOM,
              MAX(PK_DIM_TID)                        PK_DIM_TID_DATO_UTBET_TOM,
              FUNKSJONELL_TID,
              FORSTE_VEDTAKSDATO,
              SISTE_VEDTAKSDATO,
              MAX_VEDTAKSDATO,
              PERIODE,
              MAX(FORSTE_SOKNADSDATO)                AS FORSTE_SOKNADSDATO,
              MAX(SOKNADSDATO)                       AS SOKNADSDATO,
              SAMTIDIG_UTTAK,
              GRADERING,
              GRADERING_INNVILGET,
              MIN_UTTAK_FOM,
              MAX_UTTAK_TOM,
              FK_DIM_TID_MIN_DATO_KVOTE,
              FK_DIM_TID_MAX_DATO_KVOTE,
              MAX(PK_FAM_FP_TREKKONTO)               AS PK_FAM_FP_TREKKONTO,
              MAX(PK_FAM_FP_PERIODE_RESULTAT_AARSAK) AS PK_FAM_FP_PERIODE_RESULTAT_AARSAK,
              ANTALL_BEREGNINGSGRUNNLAG,
              MAX(GRADERINGSDAGER)                   AS GRADERINGSDAGER,
              MAX(MORS_AKTIVITET)                    AS MORS_AKTIVITET
            FROM
              BEREGNINGSGRUNNLAG_DETALJ
            GROUP BY
              FAGSAK_ID,
              TRANS_ID,
              TREKKONTO,
              TREKKDAGER,
              STONADSDAGER_KVOTE,
              UTTAK_ARBEID_TYPE,
              AAR,
              HALVAAR,
              KVARTAL,
              AAR_MAANED,
              UTTAK_FOM,
              UTTAK_TOM,
              DEKNINGSGRAD,
              VIRKSOMHET,
              UTBETALINGSPROSENT,
              PERIODE_RESULTAT_AARSAK,
              DAGSATS,
              DAGSATS_ERST,
              DAGSATS_BRUKER,
              DAGSATS_ARBEIDSGIVER,
              DAGSATS_VIRKSOMHET,
              UTBETALINGSPROSENT,
              STATUS_OG_ANDEL_INNTEKTSKAT,
              AKTIVITET_STATUS,
              BRUTTO_INNTEKT,
              AVKORTET_INNTEKT,
              STATUS_OG_ANDEL_BRUTTO,
              STATUS_OG_ANDEL_AVKORTET,
              FUNKSJONELL_TID,
              FORSTE_VEDTAKSDATO,
              SISTE_VEDTAKSDATO,
              MAX_VEDTAKSDATO,
              PERIODE,
              SAMTIDIG_UTTAK,
              GRADERING,
              GRADERING_INNVILGET,
              MIN_UTTAK_FOM,
              MAX_UTTAK_TOM,
              FK_DIM_TID_MIN_DATO_KVOTE,
              FK_DIM_TID_MAX_DATO_KVOTE,
              ANTALL_BEREGNINGSGRUNNLAG
          ) A
      ), GRUNNLAG AS (
        SELECT
          BEREGNINGSGRUNNLAG_AGG.*,
          SYSDATE                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      AS LASTET_DATO,
          CASE
            WHEN BEREGNINGSGRUNNLAG_AGG.AAR_MAANED > TO_NUMBER(P_RAPPORT_DATO) THEN
              'B'
            ELSE
              'A'
          END BUDSJETT                                                                                                                                                                                                                                                                                                                                                                                                                ,
          MOTTAKER.BEHANDLINGSTEMA,
          MOTTAKER.MAX_TRANS_ID,
          MOTTAKER.FK_PERSON1_MOTTAKER,
          MOTTAKER.KJONN,
          MOTTAKER.FK_PERSON1_BARN,
          MOTTAKER.TERMINDATO,
          MOTTAKER.FOEDSELSDATO,
          MOTTAKER.ANTALL_BARN_TERMIN,
          MOTTAKER.ANTALL_BARN_FOEDSEL,
          MOTTAKER.FOEDSELSDATO_ADOPSJON,
          MOTTAKER.ANTALL_BARN_ADOPSJON,
          MOTTAKER.MOTTAKER_FODSELS_AAR,
          MOTTAKER.MOTTAKER_FODSELS_MND,
          SUBSTR(P_TID_FOM, 1, 4) - MOTTAKER.MOTTAKER_FODSELS_AAR                                                                                                                                                                                                                                                                                                                                                                                                                                                      AS MOTTAKER_ALDER,
          MOTTAKER.SIVILSTAND,
          MOTTAKER.STATSBORGERSKAP,
          DIM_PERSON.PK_DIM_PERSON,
          DIM_PERSON.BOSTED_KOMMUNE_NR,
          DIM_PERSON.FK_DIM_SIVILSTATUS,
          DIM_GEOGRAFI.PK_DIM_GEOGRAFI,
          DIM_GEOGRAFI.BYDEL_KOMMUNE_NR,
          DIM_GEOGRAFI.KOMMUNE_NR,
          DIM_GEOGRAFI.KOMMUNE_NAVN,
          DIM_GEOGRAFI.BYDEL_NR,
          DIM_GEOGRAFI.BYDEL_NAVN,
          ANNENFORELDERFAGSAK.ANNENFORELDERFAGSAK_ID,
          ANNENFORELDERFAGSAK.FK_PERSON1_ANNEN_PART,
          FAM_FP_UTTAK_FP_KONTOER.MAX_DAGER                                                                                                                                                                                                                                                                                                                                                                                                                                                                            MAX_STONADSDAGER_KONTO,
          CASE
            WHEN ALENEOMSORG.FAGSAK_ID IS NOT NULL THEN
              'J'
            ELSE
              NULL
          END AS                                                                                                                                                                                                                                                                                                                                                                                                                                           ALENEOMSORG,
          CASE
            WHEN BEHANDLINGSTEMA = 'FORP_FODS' THEN
              '214'
            WHEN BEHANDLINGSTEMA = 'FORP_ADOP' THEN
              '216'
          END AS                                                                                                                                                                                                                                                                                                                                                                                                         HOVEDKONTONR,
          CASE
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='ARBEIDSTAKER' AND DAGSATS_ARBEIDSGIVER/DAGSATS*100<=50 THEN
              '1000'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='ARBEIDSTAKER' AND DAGSATS_ARBEIDSGIVER/DAGSATS*100>50 THEN
              '8020'
 --when status_og_andel_inntektskat='ARBEIDSTAKER' then '1000'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='JORDBRUKER' THEN
              '5210'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='SJØMANN' THEN
              '1300'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='SELVSTENDIG_NÆRINGSDRIVENDE' THEN
              '5010'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='DAGPENGER' THEN
              '1200'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='ARBEIDSTAKER_UTEN_FERIEPENGER' THEN
              '1000'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='FISKER' THEN
              '5300'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='DAGMAMMA' THEN
              '5110'
            WHEN STATUS_OG_ANDEL_INNTEKTSKAT='FRILANSER' THEN
              '1100'
          END AS UNDERKONTONR,
          ROUND(DAGSATS_ARBEIDSGIVER/DAGSATS*100, 0)                                                                                                                                                                                                                                                                                                                                                                                                                                                                   AS ANDEL_AV_REFUSJON,
          CASE
            WHEN RETT_TIL_MØDREKVOTE.TRANS_ID IS NULL THEN
              'N'
            ELSE
              'J'
          END AS                                                                                                                                                                                                                                                                                                                                                                                                                                         RETT_TIL_MØDREKVOTE,
          CASE
            WHEN RETT_TIL_FEDREKVOTE.TRANS_ID IS NULL THEN
              'N'
            ELSE
              'J'
          END AS                                                                                                                                                                                                                                                                                                                                                                                                                                         RETT_TIL_FEDREKVOTE,
          FLERBARNSDAGER.FLERBARNSDAGER,
          ADOPSJON.ADOPSJONSDATO,
          ADOPSJON.STEBARNSADOPSJON,
          EOS.EOS_SAK
        FROM
          BEREGNINGSGRUNNLAG_AGG
          LEFT JOIN MOTTAKER
          ON BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID = MOTTAKER.FAGSAK_ID
          AND BEREGNINGSGRUNNLAG_AGG.TRANS_ID = MOTTAKER.MAX_TRANS_ID
          LEFT JOIN ANNENFORELDERFAGSAK
          ON BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID = ANNENFORELDERFAGSAK.FAGSAK_ID
          AND BEREGNINGSGRUNNLAG_AGG.TRANS_ID = ANNENFORELDERFAGSAK.MAX_TRANS_ID
          LEFT JOIN DVH_FAM_FP.FAM_FP_UTTAK_FP_KONTOER
          ON BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID = FAM_FP_UTTAK_FP_KONTOER.FAGSAK_ID
          AND MOTTAKER.MAX_TRANS_ID = FAM_FP_UTTAK_FP_KONTOER.TRANS_ID
 --AND UPPER(REGEXP_REPLACE(grunnlag_drp1.TREKKONTO, '_|-|[[:space:]]', '')) = UPPER(REGEXP_REPLACE(FAM_FP_Uttak_FP_Kontoer.STOENADSKONTOTYPE, '_|-|[[:space:]]', ''))
          AND UPPER(REPLACE(BEREGNINGSGRUNNLAG_AGG.TREKKONTO,
          '_',
          '')) = UPPER(REPLACE(FAM_FP_UTTAK_FP_KONTOER.STOENADSKONTOTYPE,
          ' ',
          ''))
          LEFT JOIN DT_PERSON.DIM_PERSON
          ON MOTTAKER.FK_PERSON1_MOTTAKER = DIM_PERSON.FK_PERSON1
 --and beregningsgrunnlag_agg.uttak_tom between dim_person.gyldig_fra_dato and dim_person.gyldig_til_dato
          AND TO_DATE(BEREGNINGSGRUNNLAG_AGG.PK_DIM_TID_DATO_UTBET_TOM,
          'yyyymmdd') BETWEEN DIM_PERSON.GYLDIG_FRA_DATO
          AND DIM_PERSON.GYLDIG_TIL_DATO
          LEFT JOIN DT_KODEVERK.DIM_GEOGRAFI
          ON DIM_PERSON.FK_DIM_GEOGRAFI_BOSTED = DIM_GEOGRAFI.PK_DIM_GEOGRAFI
          LEFT JOIN ALENEOMSORG
          ON ALENEOMSORG.FAGSAK_ID = BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID
          AND ALENEOMSORG.UTTAK_FOM = BEREGNINGSGRUNNLAG_AGG.UTTAK_FOM
          LEFT JOIN (
            SELECT
              TRANS_ID
            FROM
              DVH_FAM_FP.FAM_FP_UTTAK_FP_KONTOER
            WHERE
              UPPER(STOENADSKONTOTYPE) = 'MØDREKVOTE'
            GROUP BY
              TRANS_ID
          ) RETT_TIL_MØDREKVOTE
          ON RETT_TIL_MØDREKVOTE.TRANS_ID = BEREGNINGSGRUNNLAG_AGG.TRANS_ID
          LEFT JOIN (
            SELECT
              TRANS_ID
            FROM
              DVH_FAM_FP.FAM_FP_UTTAK_FP_KONTOER
            WHERE
              UPPER(STOENADSKONTOTYPE) = 'FEDREKVOTE'
            GROUP BY
              TRANS_ID
          ) RETT_TIL_FEDREKVOTE
          ON RETT_TIL_FEDREKVOTE.TRANS_ID = BEREGNINGSGRUNNLAG_AGG.TRANS_ID
          LEFT JOIN (
            SELECT
              TRANS_ID,
              MAX(MAX_DAGER) AS FLERBARNSDAGER
            FROM
              DVH_FAM_FP.FAM_FP_UTTAK_FP_KONTOER
            WHERE
              UPPER(STOENADSKONTOTYPE) = 'FLERBARNSDAGER'
            GROUP BY
              TRANS_ID
          ) FLERBARNSDAGER
          ON FLERBARNSDAGER.TRANS_ID = BEREGNINGSGRUNNLAG_AGG.TRANS_ID
          LEFT JOIN ADOPSJON
          ON BEREGNINGSGRUNNLAG_AGG.FAGSAK_ID = ADOPSJON.FAGSAK_ID
          LEFT JOIN EOS
          ON BEREGNINGSGRUNNLAG_AGG.TRANS_ID = EOS.TRANS_ID
      )
      SELECT /*+ PARALLEL(8) */
        * --Fjern parallel hint hvis man får feilmelding ved kjøring
 --from uttak_dager
      FROM
        GRUNNLAG --grunnlag
      WHERE
        FAGSAK_ID NOT IN (1679117)
 --where fagsak_id in (1035184)
;
    V_TID_FOM                      VARCHAR2(8) := NULL;
    V_TID_TOM                      VARCHAR2(8) := NULL;
    V_COMMIT                       NUMBER := 0;
    V_ERROR_MELDING                VARCHAR2(1000) := NULL;
    V_DIM_TID_ANTALL               NUMBER := 0;
    V_UTBETALINGSPROSENT_KALKULERT NUMBER := 0;
  BEGIN
    V_TID_FOM := SUBSTR(P_IN_VEDTAK_TOM, 1, 4)
                 || '0101';
    V_TID_TOM := SUBSTR(P_IN_VEDTAK_TOM, 1, 4)
                 || '1231';
 --dbms_output.put_line(v_tid_fom||v_tid_tom);--TEST!!!
    FOR REC_PERIODE IN CUR_PERIODE(P_IN_RAPPORT_DATO, P_IN_FORSKYVNINGER, V_TID_FOM, V_TID_TOM) LOOP
      V_DIM_TID_ANTALL := 0;
      V_UTBETALINGSPROSENT_KALKULERT := 0;
      V_DIM_TID_ANTALL := DIM_TID_ANTALL(TO_NUMBER(TO_CHAR(REC_PERIODE.UTTAK_FOM, 'yyyymmdd')), TO_NUMBER(TO_CHAR(REC_PERIODE.UTTAK_TOM, 'yyyymmdd')));
      IF V_DIM_TID_ANTALL != 0 THEN
        V_UTBETALINGSPROSENT_KALKULERT := ROUND(REC_PERIODE.TREKKDAGER/V_DIM_TID_ANTALL*100, 2);
      ELSE
        V_UTBETALINGSPROSENT_KALKULERT := 0;
      END IF;
 --dbms_output.put_line(v_dim_tid_antall);
      BEGIN
        INSERT INTO DVH_FAM_FP.FAK_FAM_FP_VEDTAK_UTBETALING (
          FAGSAK_ID,
          TRANS_ID,
          BEHANDLINGSTEMA,
          TREKKONTO,
          STONADSDAGER_KVOTE,
          UTTAK_ARBEID_TYPE,
          AAR,
          HALVAAR,
          AAR_MAANED,
          RAPPORT_PERIODE,
          UTTAK_FOM,
          UTTAK_TOM,
          DAGER_ERST,
          BEREGNINGSGRUNNLAG_FOM,
          BEREGNINGSGRUNNLAG_TOM,
          DEKNINGSGRAD,
          DAGSATS_BRUKER,
          DAGSATS_ARBEIDSGIVER,
          VIRKSOMHET,
          PERIODE_RESULTAT_AARSAK,
          DAGSATS,
          GRADERINGSPROSENT,
          STATUS_OG_ANDEL_INNTEKTSKAT,
          AKTIVITET_STATUS,
          BRUTTO_INNTEKT,
          AVKORTET_INNTEKT,
          STATUS_OG_ANDEL_BRUTTO,
          STATUS_OG_ANDEL_AVKORTET,
          UTBETALINGSPROSENT,
          FK_DIM_TID_DATO_UTBET_FOM,
          FK_DIM_TID_DATO_UTBET_TOM,
          FUNKSJONELL_TID,
          FORSTE_VEDTAKSDATO,
          VEDTAKSDATO,
          MAX_VEDTAKSDATO,
          PERIODE_TYPE,
          TILFELLE_ERST,
          BELOP,
          DAGSATS_REDUSERT,
          LASTET_DATO,
          MAX_TRANS_ID,
          FK_PERSON1_MOTTAKER,
          FK_PERSON1_ANNEN_PART,
          KJONN,
          FK_PERSON1_BARN,
          TERMINDATO,
          FOEDSELSDATO,
          ANTALL_BARN_TERMIN,
          ANTALL_BARN_FOEDSEL,
          FOEDSELSDATO_ADOPSJON,
          ANTALL_BARN_ADOPSJON,
          ANNENFORELDERFAGSAK_ID,
          MAX_STONADSDAGER_KONTO,
          FK_DIM_PERSON,
          BOSTED_KOMMUNE_NR,
          FK_DIM_GEOGRAFI,
          BYDEL_KOMMUNE_NR,
          KOMMUNE_NR,
          KOMMUNE_NAVN,
          BYDEL_NR,
          BYDEL_NAVN,
          ALENEOMSORG,
          HOVEDKONTONR,
          UNDERKONTONR,
          MOTTAKER_FODSELS_AAR,
          MOTTAKER_FODSELS_MND,
          MOTTAKER_ALDER,
          RETT_TIL_FEDREKVOTE,
          RETT_TIL_MODREKVOTE,
          DAGSATS_ERST,
          TREKKDAGER,
          SAMTIDIG_UTTAK,
          GRADERING,
          GRADERING_INNVILGET,
          ANTALL_DAGER_PERIODE,
          FLERBARNSDAGER,
          UTBETALINGSPROSENT_KALKULERT,
          MIN_UTTAK_FOM,
          MAX_UTTAK_TOM,
          FK_FAM_FP_TREKKONTO,
          FK_FAM_FP_PERIODE_RESULTAT_AARSAK,
          SIVILSTATUS,
          FK_DIM_SIVILSTATUS,
          ANTALL_BEREGNINGSGRUNNLAG,
          GRADERINGSDAGER,
          FK_DIM_TID_MIN_DATO_KVOTE,
          FK_DIM_TID_MAX_DATO_KVOTE,
          ADOPSJONSDATO,
          STEBARNSADOPSJON,
          EOS_SAK,
          MOR_RETTIGHET,
          STATSBORGERSKAP,
          ARBEIDSTIDSPROSENT,
          MORS_AKTIVITET,
          GYLDIG_FLAGG,
          ANDEL_AV_REFUSJON,
          FORSTE_SOKNADSDATO,
          SOKNADSDATO,
          BUDSJETT
        ) VALUES (
          REC_PERIODE.FAGSAK_ID,
          REC_PERIODE.TRANS_ID,
          REC_PERIODE.BEHANDLINGSTEMA,
          REC_PERIODE.TREKKONTO,
          REC_PERIODE.STONADSDAGER_KVOTE,
          REC_PERIODE.UTTAK_ARBEID_TYPE,
          REC_PERIODE.AAR,
          REC_PERIODE.HALVAAR,
          REC_PERIODE.AAR_MAANED,
          P_IN_RAPPORT_DATO,
          REC_PERIODE.UTTAK_FOM,
          REC_PERIODE.UTTAK_TOM,
          REC_PERIODE.DAGER_ERST,
          REC_PERIODE.BEREGNINGSGRUNNLAG_FOM,
          REC_PERIODE.BEREGNINGSGRUNNLAG_TOM,
          REC_PERIODE.DEKNINGSGRAD,
          REC_PERIODE.DAGSATS_BRUKER,
          REC_PERIODE.DAGSATS_ARBEIDSGIVER,
          REC_PERIODE.VIRKSOMHET,
          REC_PERIODE.PERIODE_RESULTAT_AARSAK,
          REC_PERIODE.DAGSATS,
          REC_PERIODE.GRADERINGSPROSENT,
          REC_PERIODE.STATUS_OG_ANDEL_INNTEKTSKAT,
          REC_PERIODE.AKTIVITET_STATUS,
          REC_PERIODE.BRUTTO_INNTEKT,
          REC_PERIODE.AVKORTET_INNTEKT,
          REC_PERIODE.STATUS_OG_ANDEL_BRUTTO,
          REC_PERIODE.STATUS_OG_ANDEL_AVKORTET,
          REC_PERIODE.UTBETALINGSPROSENT,
          REC_PERIODE.PK_DIM_TID_DATO_UTBET_FOM,
          REC_PERIODE.PK_DIM_TID_DATO_UTBET_TOM,
          REC_PERIODE.FUNKSJONELL_TID,
          REC_PERIODE.FORSTE_VEDTAKSDATO,
          REC_PERIODE.SISTE_VEDTAKSDATO,
          REC_PERIODE.MAX_VEDTAKSDATO,
          REC_PERIODE.PERIODE,
          REC_PERIODE.TILFELLE_ERST,
          REC_PERIODE.BELOP,
          REC_PERIODE.DAGSATS_REDUSERT,
          SYSDATE --rec_periode.lastet_dato
,
          REC_PERIODE.MAX_TRANS_ID,
          REC_PERIODE.FK_PERSON1_MOTTAKER,
          REC_PERIODE.FK_PERSON1_ANNEN_PART,
          REC_PERIODE.KJONN,
          REC_PERIODE.FK_PERSON1_BARN,
          REC_PERIODE.TERMINDATO,
          REC_PERIODE.FOEDSELSDATO,
          REC_PERIODE.ANTALL_BARN_TERMIN,
          REC_PERIODE.ANTALL_BARN_FOEDSEL,
          REC_PERIODE.FOEDSELSDATO_ADOPSJON,
          REC_PERIODE.ANTALL_BARN_ADOPSJON,
          REC_PERIODE.ANNENFORELDERFAGSAK_ID,
          REC_PERIODE.MAX_STONADSDAGER_KONTO,
          REC_PERIODE.PK_DIM_PERSON,
          REC_PERIODE.BOSTED_KOMMUNE_NR,
          REC_PERIODE.PK_DIM_GEOGRAFI,
          REC_PERIODE.BYDEL_KOMMUNE_NR,
          REC_PERIODE.KOMMUNE_NR,
          REC_PERIODE.KOMMUNE_NAVN,
          REC_PERIODE.BYDEL_NR,
          REC_PERIODE.BYDEL_NAVN,
          REC_PERIODE.ALENEOMSORG,
          REC_PERIODE.HOVEDKONTONR,
          REC_PERIODE.UNDERKONTONR,
          REC_PERIODE.MOTTAKER_FODSELS_AAR,
          REC_PERIODE.MOTTAKER_FODSELS_MND,
          REC_PERIODE.MOTTAKER_ALDER,
          REC_PERIODE.RETT_TIL_FEDREKVOTE,
          REC_PERIODE.RETT_TIL_MØDREKVOTE,
          REC_PERIODE.DAGSATS_ERST,
          REC_PERIODE.TREKKDAGER,
          REC_PERIODE.SAMTIDIG_UTTAK,
          REC_PERIODE.GRADERING,
          REC_PERIODE.GRADERING_INNVILGET,
          V_DIM_TID_ANTALL,
          REC_PERIODE.FLERBARNSDAGER,
          V_UTBETALINGSPROSENT_KALKULERT,
          REC_PERIODE.MIN_UTTAK_FOM,
          REC_PERIODE.MAX_UTTAK_TOM,
          REC_PERIODE.PK_FAM_FP_TREKKONTO,
          REC_PERIODE.PK_FAM_FP_PERIODE_RESULTAT_AARSAK,
          REC_PERIODE.SIVILSTAND,
          REC_PERIODE.FK_DIM_SIVILSTATUS,
          REC_PERIODE.ANTALL_BEREGNINGSGRUNNLAG,
          REC_PERIODE.GRADERINGSDAGER,
          REC_PERIODE.FK_DIM_TID_MIN_DATO_KVOTE,
          REC_PERIODE.FK_DIM_TID_MAX_DATO_KVOTE,
          REC_PERIODE.ADOPSJONSDATO,
          REC_PERIODE.STEBARNSADOPSJON,
          REC_PERIODE.EOS_SAK,
          REC_PERIODE.MOR_RETTIGHET,
          REC_PERIODE.STATSBORGERSKAP,
          REC_PERIODE.ARBEIDSTIDSPROSENT,
          REC_PERIODE.MORS_AKTIVITET,
          P_IN_GYLDIG_FLAGG,
          REC_PERIODE.ANDEL_AV_REFUSJON,
          REC_PERIODE.FORSTE_SOKNADSDATO,
          REC_PERIODE.SOKNADSDATO,
          REC_PERIODE.BUDSJETT
        );
        V_COMMIT := V_COMMIT + 1;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK;
          V_ERROR_MELDING := SUBSTR(SQLCODE
                                    || ' '
                                    || SQLERRM, 1, 1000);
          INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
            MIN_LASTET_DATO,
            ID,
            ERROR_MSG,
            OPPRETTET_TID,
            KILDE
          ) VALUES(
            NULL,
            REC_PERIODE.FAGSAK_ID,
            V_ERROR_MELDING,
            SYSDATE,
            'FAM_FP_STATISTIKK_AAR:INSERT'
          );
          COMMIT;
          P_OUT_ERROR := SUBSTR(P_OUT_ERROR
                                || V_ERROR_MELDING, 1, 1000);
      END;
      IF V_COMMIT > 100000 THEN
        COMMIT;
        V_COMMIT := 0;
      END IF;
    END LOOP;
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      V_ERROR_MELDING := SUBSTR(SQLCODE
                                || ' '
                                || SQLERRM, 1, 1000);
      INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
        MIN_LASTET_DATO,
        ID,
        ERROR_MSG,
        OPPRETTET_TID,
        KILDE
      ) VALUES(
        NULL,
        NULL,
        V_ERROR_MELDING,
        SYSDATE,
        'FAM_FP_STATISTIKK_AAR'
      );
      COMMIT;
      P_OUT_ERROR := SUBSTR(P_OUT_ERROR
                            || V_ERROR_MELDING, 1, 1000);
  END FAM_FP_STATISTIKK_AAR_MND;
END FAM_FP;
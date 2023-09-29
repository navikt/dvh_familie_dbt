CREATE OR REPLACE PACKAGE BODY FAM_BT AS

  PROCEDURE FAM_BT_INFOTRYGD_MOTTAKER_UPDATE(
    P_IN_PERIOD IN VARCHAR2,
    P_ERROR_MELDING OUT VARCHAR2
  ) AS
 --v_aar_start VARCHAR2(6):= substr(p_in_period,1,4) || '01';
    V_STOREDATE     DATE := SYSDATE;
    L_ERROR_MELDING VARCHAR2(1000);
    L_COMMIT        NUMBER := 0;
    CURSOR CUR_MOTTAKER IS
      SELECT /*+ PARALLEL(8) */
        INFOTRYGD.FK_PERSON1,
        INFOTRYGD.PK_BT_MOTTAKER,
        NVL(MAX(DIM_PERSON_MOTTAKER.BOSTED_KOMMUNE_NR), -1)      AS BOSTED_KOMMUNE_NR,
        NVL(MAX(DIM_GEOGRAFI.BYDEL_KOMMUNE_NR), -1)              AS BYDEL_KOMMUNE_NR,
        NVL(MAX(DIM_PERSON_MOTTAKER.PK_DIM_PERSON), -1)          AS FK_DIM_PERSON,
        NVL(MAX(DIM_PERSON_MOTTAKER.FK_DIM_GEOGRAFI_BOSTED), -1) AS FK_DIM_GEOGRAFI_BOSTED
      FROM
        DVH_FAM_BT.FAM_BT_MOTTAKER INFOTRYGD
        LEFT OUTER JOIN DT_KODEVERK.DIM_TID FAM_BT_PERIODE
        ON INFOTRYGD.STAT_AARMND = FAM_BT_PERIODE.AAR_MAANED
        AND FAM_BT_PERIODE.DIM_NIVAA = 3
        AND FAM_BT_PERIODE.GYLDIG_FLAGG = 1
        LEFT OUTER JOIN DT_PERSON.DIM_PERSON DIM_PERSON_MOTTAKER
        ON DIM_PERSON_MOTTAKER.FK_PERSON1 = INFOTRYGD.FK_PERSON1
        AND DIM_PERSON_MOTTAKER.GYLDIG_FRA_DATO <= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
        AND DIM_PERSON_MOTTAKER.GYLDIG_TIL_DATO >= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
        LEFT OUTER JOIN DT_KODEVERK.DIM_GEOGRAFI DIM_GEOGRAFI
        ON DIM_PERSON_MOTTAKER.FK_DIM_GEOGRAFI_BOSTED = DIM_GEOGRAFI.PK_DIM_GEOGRAFI
      WHERE
        INFOTRYGD.KILDE = 'INFOTRYGD'
        AND INFOTRYGD.STAT_AARMND = P_IN_PERIOD
        AND INFOTRYGD.FK_DIM_GEOGRAFI_BOSTED IS NULL
      GROUP BY
        INFOTRYGD.FK_PERSON1, INFOTRYGD.PK_BT_MOTTAKER;
  BEGIN
    FOR REC_MOTTAKER IN CUR_MOTTAKER LOOP
      BEGIN
        UPDATE DVH_FAM_BT.FAM_BT_MOTTAKER
        SET
          BOSTED_KOMMUNE_NR = REC_MOTTAKER.BOSTED_KOMMUNE_NR,
          BOSTED_BYDEL_KOMMUNE_NR = REC_MOTTAKER.BYDEL_KOMMUNE_NR,
          FK_DIM_PERSON = REC_MOTTAKER.FK_DIM_PERSON,
          FK_DIM_GEOGRAFI_BOSTED = REC_MOTTAKER.FK_DIM_GEOGRAFI_BOSTED
        WHERE
          PK_BT_MOTTAKER = REC_MOTTAKER.PK_BT_MOTTAKER;
        L_COMMIT := L_COMMIT + 1;
      EXCEPTION
        WHEN OTHERS THEN
          L_ERROR_MELDING := SUBSTR(SQLCODE
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
            REC_MOTTAKER.PK_BT_MOTTAKER,
            L_ERROR_MELDING,
            V_STOREDATE,
            'FAM_BT_INFOTRYGD_MOTTAKER_UPDATE1'
          );
          L_COMMIT := L_COMMIT + 1; --Gå videre til neste rekord
          P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                                                  || L_ERROR_MELDING, 1, 1000);
          L_ERROR_MELDING := NULL;
      END;
      IF L_COMMIT >= 100000 THEN
        COMMIT;
        L_COMMIT := 0;
      END IF;
    END LOOP;
    COMMIT;
    IF L_ERROR_MELDING IS NOT NULL THEN
      INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
        ID,
        ERROR_MSG,
        OPPRETTET_TID,
        KILDE
      ) VALUES(
        NULL,
        L_ERROR_MELDING,
        V_STOREDATE,
        'FAM_BT_INFOTRYGD_MOTTAKER_UPDATE2'
      );
      COMMIT;
      P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                || L_ERROR_MELDING, 1, 1000);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      L_ERROR_MELDING := SUBSTR(SQLCODE
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
        L_ERROR_MELDING,
        V_STOREDATE,
        'FAM_BT_INFOTRYGD_MOTTAKER_UPDATE3'
      );
      COMMIT;
      P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                || L_ERROR_MELDING, 1, 1000);
  END FAM_BT_INFOTRYGD_MOTTAKER_UPDATE;

  PROCEDURE FAM_BT_MOTTAKER_INSERT_2(
    P_IN_PERIOD IN VARCHAR2,
    P_IN_GYLDIG_FLAGG IN NUMBER DEFAULT 0,
    P_ERROR_MELDING OUT VARCHAR2
  ) AS
    V_AAR_START     VARCHAR2(6):= SUBSTR(P_IN_PERIOD, 1, 4)
                                  || '01';
    V_KILDE         VARCHAR2(10) := 'BT';
 --v_storedate DATE := sysdate;
    L_ERROR_MELDING VARCHAR2(1000);
    L_COMMIT        NUMBER := 0;
    CURSOR CUR_MOTTAKER IS
      SELECT /*+ PARALLEL(8) */
        DATA.*,
        BELOPE + BELOPHIE_B                                                                                                            AS BELOPHIE,
        BELOP + BELOPHIT_B                                                                                                             AS BELOPHIT,
        BELOP_UTVIDET + BELOPHIT_UTVIDET_B                                                                                             AS BELOPHIT_UTVIDET,
        CASE
          WHEN UNDERKATEGORI = 'ORDINÆR' THEN
            1
          WHEN UNDERKATEGORI = 'UTVIDET' THEN
            2
          WHEN UNDERKATEGORI = 'INSTITUSJON' THEN
            4
        END STATUSK
      FROM
        (
          SELECT
            UR.GJELDER_MOTTAKER,
 --to_char(ur.posteringsdato,'YYYYMM') periode,
            SUM(UR.BELOP_SIGN)                                                                         BELOP,
            DIM_KJONN_MOTTAKER.KJONN_KODE                                                              KJONN,
            SUM(
              CASE
                WHEN TO_CHAR(UR.POSTERINGSDATO, 'YYYYMM') > TO_CHAR(UR.DATO_UTBET_FOM, 'YYYYMM') THEN
                  UR.BELOP_SIGN
                ELSE
                  0.0
              END)                                                                                     BELOPE,
            EXTRACT( YEAR FROM DIM_PERSON_MOTTAKER.FODT_DATO)                                          FODSEL_AAR,
            TO_CHAR(DIM_PERSON_MOTTAKER.FODT_DATO, 'MM')                                               FODSEL_MND,
            DIM_PERSON_MOTTAKER.TKNR,
            DIM_PERSON_MOTTAKER.BOSTED_KOMMUNE_NR,
            DIM_GEOGRAFI.BYDEL_KOMMUNE_NR,
            FAGSAK.FAGSAK_ID,
 --fagsak.fagsak_type,
            MAX(FAGSAK.PK_BT_FAGSAK) KEEP (DENSE_RANK FIRST ORDER BY FAGSAK.TIDSPUNKT_VEDTAK DESC)     AS PK_BT_FAGSAK,
            MAX(FAGSAK.BEHANDLING_ÅRSAK) KEEP (DENSE_RANK FIRST ORDER BY FAGSAK.TIDSPUNKT_VEDTAK DESC) AS BEHANDLING_ÅRSAK,
            MAX(FAGSAK.UNDERKATEGORI) KEEP (DENSE_RANK FIRST ORDER BY FAGSAK.TIDSPUNKT_VEDTAK DESC)    AS UNDERKATEGORI,
 --fagsak.enslig_forsørger,
            DIM_PERSON_MOTTAKER.PK_DIM_PERSON,
            DIM_PERSON_MOTTAKER.FK_DIM_KJONN,
            DIM_PERSON_MOTTAKER.FK_DIM_SIVILSTATUS,
            DIM_PERSON_MOTTAKER.FK_DIM_LAND_FODT,
            DIM_PERSON_MOTTAKER.FK_DIM_GEOGRAFI_BOSTED,
            DIM_PERSON_MOTTAKER.FK_DIM_LAND_STATSBORGERSKAP,
            FAM_BT_PERIODE.PK_DIM_TID,
            FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN,
            DIM_ALDER.PK_DIM_ALDER,
            BARN.FKBY_PERSON1,
            BARN.ANTBARN,
            BARN.DELINGSPROSENT_YTELSE,
            TRUNC(MONTHS_BETWEEN(FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN, DIM_PERSON_BARN.FODT_DATO)/12) ALDERYB,
            DIM_LAND_BARN.LAND_SSB_KODE                                                                SSB_KODE,
            NVL(BELOP.BELOPHIE, 0)                                                                     BELOPHIE_B,
            NVL(BELOP.BELOPHIT, 0)                                                                     BELOPHIT_B,
            SUM((
              CASE
                WHEN UTVIDET_BARNETRYGD.DELYTELSE_ID IS NOT NULL THEN
                  1
                ELSE
                  0
              END) * UR.BELOP_SIGN)                                                                    BELOP_UTVIDET,
            NVL(BELOPHIT_UTVIDET.BELOPHIT_UTVIDET, 0)                                                  BELOPHIT_UTVIDET_B
          FROM
            DVH_FAM_BT.FAM_BT_UR_UTBETALING UR
            LEFT JOIN DVH_FAM_BT.FAM_BT_FAGSAK FAGSAK
            ON FAGSAK.BEHANDLINGS_ID = UR.HENVISNING
            LEFT OUTER JOIN DT_KODEVERK.DIM_TID FAM_BT_PERIODE
            ON TO_CHAR(UR.POSTERINGSDATO,
            'YYYYMM') = FAM_BT_PERIODE.AAR_MAANED
            AND FAM_BT_PERIODE.DIM_NIVAA = 3
            AND FAM_BT_PERIODE.GYLDIG_FLAGG = 1
            LEFT OUTER JOIN DT_PERSON.DIM_PERSON DIM_PERSON_MOTTAKER
            ON DIM_PERSON_MOTTAKER.FK_PERSON1 = UR.GJELDER_MOTTAKER
            AND DIM_PERSON_MOTTAKER.GYLDIG_FRA_DATO <= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
            AND DIM_PERSON_MOTTAKER.GYLDIG_TIL_DATO >= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
            LEFT OUTER JOIN DT_KODEVERK.DIM_GEOGRAFI DIM_GEOGRAFI
            ON DIM_PERSON_MOTTAKER.FK_DIM_GEOGRAFI_BOSTED = DIM_GEOGRAFI.PK_DIM_GEOGRAFI
            LEFT OUTER JOIN DT_KODEVERK.DIM_KJONN DIM_KJONN_MOTTAKER
            ON DIM_PERSON_MOTTAKER.FK_DIM_KJONN = DIM_KJONN_MOTTAKER.PK_DIM_KJONN
            LEFT OUTER JOIN DT_KODEVERK.DIM_ALDER
            ON FLOOR(MONTHS_BETWEEN(SYSDATE,
            DIM_PERSON_MOTTAKER.FODT_DATO)/12) = DIM_ALDER.ALDER
            AND DIM_ALDER.GYLDIG_FRA_DATO <= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
            AND DIM_ALDER.GYLDIG_TIL_DATO >= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
            LEFT JOIN (
              SELECT
                FK_PERSON1,
                SUBSTR(MAX(FODSEL_AAR_BARN
                           ||FODSEL_MND_BARN
                           ||'-'
                           ||FKB_PERSON1), 8) FKBY_PERSON1,
                MAX(DELINGSPROSENT_YTELSE)    AS DELINGSPROSENT_YTELSE,
                COUNT(DISTINCT FKB_PERSON1)   ANTBARN
              FROM
                DVH_FAM_BT.FAM_BT_BARN
              WHERE
                KILDE = V_KILDE
                AND STAT_AARMND = P_IN_PERIOD
                AND GYLDIG_FLAGG = P_IN_GYLDIG_FLAGG
              GROUP BY
                FK_PERSON1
            ) BARN
            ON UR.GJELDER_MOTTAKER = BARN.FK_PERSON1
            LEFT JOIN DT_PERSON.DIM_PERSON DIM_PERSON_BARN
            ON DIM_PERSON_BARN.FK_PERSON1 = BARN.FKBY_PERSON1
            AND DIM_PERSON_BARN.GYLDIG_FRA_DATO <= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
            AND DIM_PERSON_BARN.GYLDIG_TIL_DATO >= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
            LEFT OUTER JOIN DT_KODEVERK.DIM_LAND DIM_LAND_BARN
            ON DIM_PERSON_BARN.FK_DIM_LAND_BOSTED = DIM_LAND_BARN.PK_DIM_LAND
 --AND dim_land.gyldig_fra_dato <= fam_bt_periode.siste_dato_i_perioden
 --AND dim_land.gyldig_til_dato >= fam_bt_periode.siste_dato_i_perioden
            LEFT JOIN (
              SELECT
                FK_PERSON1,
                SUM(NVL(BELOP, 0)) BELOPHIT,
                SUM(NVL(BELOPE, 0)) BELOPHIE
              FROM
                DVH_FAM_BT.FAM_BT_MOTTAKER
              WHERE
                STAT_AARMND >= V_AAR_START
                AND STAT_AARMND < P_IN_PERIOD
                AND GYLDIG_FLAGG = P_IN_GYLDIG_FLAGG
              GROUP BY
                FK_PERSON1
            ) BELOP
            ON UR.GJELDER_MOTTAKER = BELOP.FK_PERSON1
 --Utvidet barnetrygd
            LEFT JOIN (
              SELECT
                UTBET.BEHANDLINGS_ID,
                UTBET.DELYTELSE_ID,
                UTBETALING.STØNAD_FOM,
                UTBETALING.STØNAD_TOM
              FROM
                DVH_FAM_BT.FAM_BT_UTBET_DET  UTBET
                JOIN DVH_FAM_BT.FAM_BT_PERSON PERSON
                ON UTBET.FK_BT_PERSON = PERSON.PK_BT_PERSON
                AND PERSON.ROLLE = 'SØKER' JOIN DVH_FAM_BT.FAM_BT_UTBETALING UTBETALING
                ON UTBET.FK_BT_UTBETALING = UTBETALING.PK_BT_UTBETALING
                JOIN DVH_FAM_BT.FAM_BT_FAGSAK FAGSAK
                ON UTBETALING.BEHANDLINGS_ID = FAGSAK.BEHANDLINGS_ID
                AND FAGSAK.ENSLIG_FORSØRGER = 1
            ) UTVIDET_BARNETRYGD
            ON UR.HENVISNING = UTVIDET_BARNETRYGD.BEHANDLINGS_ID
            AND UR.DELYTELSE_ID = UTVIDET_BARNETRYGD.DELYTELSE_ID
            AND UTVIDET_BARNETRYGD.STØNAD_FOM <= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
            AND UTVIDET_BARNETRYGD.STØNAD_TOM >= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
            LEFT JOIN (
              SELECT
                FK_PERSON1,
                SUM(NVL(BELOP_UTVIDET, 0)) BELOPHIT_UTVIDET
              FROM
                DVH_FAM_BT.FAM_BT_MOTTAKER
              WHERE
                STAT_AARMND >= V_AAR_START
                AND STAT_AARMND < P_IN_PERIOD
                AND GYLDIG_FLAGG = P_IN_GYLDIG_FLAGG
              GROUP BY
                FK_PERSON1
            ) BELOPHIT_UTVIDET
            ON UR.GJELDER_MOTTAKER = BELOPHIT_UTVIDET.FK_PERSON1
          WHERE
            UR.HOVEDKONTONR = 800 --in (800, 214, 216, 215)-- = 800 Test!!!
            AND TO_CHAR(UR.POSTERINGSDATO, 'YYYYMM') = P_IN_PERIOD
 --AND LENGTH(ur.delytelse_id) < 14
          GROUP BY
            UR.GJELDER_MOTTAKER, TO_CHAR(UR.POSTERINGSDATO, 'YYYYMM'), DIM_KJONN_MOTTAKER.KJONN_KODE, FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN, EXTRACT( YEAR FROM DIM_PERSON_MOTTAKER.FODT_DATO), TO_CHAR(DIM_PERSON_MOTTAKER.FODT_DATO, 'MM'), DIM_PERSON_MOTTAKER.TKNR, DIM_PERSON_MOTTAKER.BOSTED_KOMMUNE_NR, DIM_GEOGRAFI.KOMMUNE_NR, DIM_GEOGRAFI.BYDEL_KOMMUNE_NR, FAGSAK.FAGSAK_ID, DIM_PERSON_MOTTAKER.PK_DIM_PERSON, DIM_PERSON_MOTTAKER.FK_DIM_KJONN, DIM_PERSON_MOTTAKER.FK_DIM_SIVILSTATUS, DIM_PERSON_MOTTAKER.FK_DIM_LAND_FODT, DIM_PERSON_MOTTAKER.FK_DIM_GEOGRAFI_BOSTED, DIM_PERSON_MOTTAKER.FK_DIM_LAND_STATSBORGERSKAP, FAM_BT_PERIODE.PK_DIM_TID, DIM_ALDER.PK_DIM_ALDER, BARN.FKBY_PERSON1, BARN.ANTBARN, BARN.DELINGSPROSENT_YTELSE, TRUNC(MONTHS_BETWEEN(FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN, DIM_PERSON_BARN.FODT_DATO)/12), DIM_LAND_BARN.LAND_SSB_KODE, NVL(BELOP.BELOPHIE, 0), NVL(BELOP.BELOPHIT, 0), NVL(BELOPHIT_UTVIDET.BELOPHIT_UTVIDET, 0)
          HAVING
            SUM(UR.BELOP_SIGN) != 0
        ) DATA;
  BEGIN
 -- Slett mottakere dvh_fam_fp.fam_bt_mottaker_hist for aktuell periode
    BEGIN
      DELETE FROM DVH_FAM_BT.FAM_BT_MOTTAKER --r152241.test_drp
      WHERE
        KILDE = V_KILDE
        AND STAT_AARMND= P_IN_PERIOD
        AND GYLDIG_FLAGG = P_IN_GYLDIG_FLAGG;
      COMMIT;
    EXCEPTION
      WHEN OTHERS THEN
        L_ERROR_MELDING := SUBSTR(SQLCODE
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
          L_ERROR_MELDING,
          SYSDATE,
          'FAM_BT_MOTTAKER_INSERT_WITH1'
        );
        COMMIT;
        P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                  || L_ERROR_MELDING, 1, 1000);
        L_ERROR_MELDING := NULL;
    END;
    FOR REC_MOTTAKER IN CUR_MOTTAKER LOOP
 --INSERT INTO dvh_fam_fp.fam_bt_mottaker
      BEGIN
        INSERT INTO DVH_FAM_BT.FAM_BT_MOTTAKER --R152241.test_drp
 (
          FK_PERSON1,
          BELOP,
          KJONN,
          BELOPE,
          FODSEL_AAR,
          FODSEL_MND,
          TKNR,
          BOSTED_KOMMUNE_NR,
          BOSTED_BYDEL_KOMMUNE_NR,
          FAGSAK_ID,
          FK_DIM_PERSON,
          FK_DIM_KJONN,
          FK_DIM_SIVILSTATUS,
          FK_DIM_LAND_FODT,
          FK_DIM_GEOGRAFI_BOSTED,
          FK_DIM_LAND_STATSBORGERSKAP,
          FK_DIM_TID_MND,
          FK_DIM_ALDER,
          FKBY_PERSON1,
          ANTBARN,
          BELOPHIE,
          BELOPHIT,
          YBARN,
          EOYBLAND,
          KILDE,
          STAT_AARMND,
          LASTET_DATO,
          PROSENT,
          STATUSK,
          BELOP_UTVIDET,
          BELOPHIT_UTVIDET,
          FK_BT_FAGSAK,
          BEHANDLING_ÅRSAK,
          GYLDIG_FLAGG
        ) VALUES (
          REC_MOTTAKER.GJELDER_MOTTAKER,
          REC_MOTTAKER.BELOP,
          REC_MOTTAKER.KJONN,
          REC_MOTTAKER.BELOPE,
          REC_MOTTAKER.FODSEL_AAR,
          REC_MOTTAKER.FODSEL_MND,
          REC_MOTTAKER.TKNR,
          REC_MOTTAKER.BOSTED_KOMMUNE_NR,
          REC_MOTTAKER.BYDEL_KOMMUNE_NR,
          REC_MOTTAKER.FAGSAK_ID,
          REC_MOTTAKER.PK_DIM_PERSON,
          REC_MOTTAKER.FK_DIM_KJONN,
          REC_MOTTAKER.FK_DIM_SIVILSTATUS,
          REC_MOTTAKER.FK_DIM_LAND_FODT,
          REC_MOTTAKER.FK_DIM_GEOGRAFI_BOSTED,
          REC_MOTTAKER.FK_DIM_LAND_STATSBORGERSKAP,
          REC_MOTTAKER.PK_DIM_TID,
          REC_MOTTAKER.PK_DIM_ALDER,
          REC_MOTTAKER.FKBY_PERSON1,
          REC_MOTTAKER.ANTBARN,
          REC_MOTTAKER.BELOPHIE,
          REC_MOTTAKER.BELOPHIT,
          REC_MOTTAKER.ALDERYB,
          REC_MOTTAKER.SSB_KODE,
          V_KILDE,
          P_IN_PERIOD,
          SYSDATE,
          REC_MOTTAKER.DELINGSPROSENT_YTELSE,
          REC_MOTTAKER.STATUSK,
          REC_MOTTAKER.BELOP_UTVIDET,
          REC_MOTTAKER.BELOPHIT_UTVIDET,
          REC_MOTTAKER.PK_BT_FAGSAK,
          REC_MOTTAKER.BEHANDLING_ÅRSAK,
          P_IN_GYLDIG_FLAGG
        );
        L_COMMIT := L_COMMIT + 1;
      EXCEPTION
        WHEN OTHERS THEN
          L_ERROR_MELDING := SUBSTR(SQLCODE
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
            REC_MOTTAKER.FAGSAK_ID,
            L_ERROR_MELDING,
            SYSDATE,
            'FAM_BT_MOTTAKER_INSERT_WITH2'
          );
          L_COMMIT := L_COMMIT + 1; --Gå videre til neste rekord
          P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                                                  || L_ERROR_MELDING, 1, 1000);
          L_ERROR_MELDING := NULL;
      END;
      IF L_COMMIT >= 100000 THEN
        COMMIT;
        L_COMMIT := 0;
      END IF;
    END LOOP;
    COMMIT;
    IF L_ERROR_MELDING IS NOT NULL THEN
      INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
        ID,
        ERROR_MSG,
        OPPRETTET_TID,
        KILDE
      ) VALUES(
        NULL,
        L_ERROR_MELDING,
        SYSDATE,
        'FAM_BT_MOTTAKER_INSERT_WITH3'
      );
      COMMIT;
      P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                || L_ERROR_MELDING, 1, 1000);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      L_ERROR_MELDING := SUBSTR(SQLCODE
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
        L_ERROR_MELDING,
        SYSDATE,
        'FAM_BT_MOTTAKER_INSERT_WITH4'
      );
      COMMIT;
      P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                || L_ERROR_MELDING, 1, 1000);
  END FAM_BT_MOTTAKER_INSERT_2;

  PROCEDURE FAM_BT_MOTTAKER_INSERT(
    P_IN_PERIOD IN VARCHAR2,
    P_IN_GYLDIG_FLAGG IN NUMBER DEFAULT 0,
    P_ERROR_MELDING OUT VARCHAR2
  ) AS
    V_AAR_START     VARCHAR2(6):= SUBSTR(P_IN_PERIOD, 1, 4)
                                  || '01';
    V_KILDE         VARCHAR2(10) := 'BT';
 --v_storedate DATE := sysdate;
    L_ERROR_MELDING VARCHAR2(1000);
    L_COMMIT        NUMBER := 0;
    CURSOR CUR_MOTTAKER IS
      SELECT /*+ PARALLEL(8) */
        DATA.*,
        BELOPE + BELOPHIE_B                                                                                                          AS BELOPHIE,
        BELOP + BELOPHIT_B                                                                                                           AS BELOPHIT,
        BELOP_UTVIDET + BELOPHIT_UTVIDET_B                                                                                           AS BELOPHIT_UTVIDET,
        CASE
          WHEN FAGSAK_TYPE = 'INSTITUSJON' THEN
            4
          WHEN UNDERKATEGORI = 'ORDINÆR' THEN
            1
          WHEN UNDERKATEGORI = 'UTVIDET' THEN
            2
        END STATUSK
      FROM
        (
          SELECT
            UR.GJELDER_MOTTAKER,
 --to_char(ur.posteringsdato,'YYYYMM') periode,
            SUM(UR.BELOP_SIGN)                                                                         BELOP,
            DIM_KJONN_MOTTAKER.KJONN_KODE                                                              KJONN,
            SUM(
              CASE
                WHEN TO_CHAR(UR.POSTERINGSDATO, 'YYYYMM') > TO_CHAR(UR.DATO_UTBET_FOM, 'YYYYMM') THEN
                  UR.BELOP_SIGN
                ELSE
                  0.0
              END)                                                                                     BELOPE,
            EXTRACT( YEAR FROM DIM_PERSON_MOTTAKER.FODT_DATO)                                          FODSEL_AAR,
            TO_CHAR(DIM_PERSON_MOTTAKER.FODT_DATO, 'MM')                                               FODSEL_MND,
            DIM_PERSON_MOTTAKER.TKNR,
            DIM_PERSON_MOTTAKER.BOSTED_KOMMUNE_NR,
            DIM_GEOGRAFI.BYDEL_KOMMUNE_NR,
            FAGSAK.FAGSAK_ID,
            MAX(FAGSAK.PK_BT_FAGSAK) KEEP (DENSE_RANK FIRST ORDER BY FAGSAK.TIDSPUNKT_VEDTAK DESC)     AS PK_BT_FAGSAK,
            MAX(FAGSAK.BEHANDLING_ÅRSAK) KEEP (DENSE_RANK FIRST ORDER BY FAGSAK.TIDSPUNKT_VEDTAK DESC) AS BEHANDLING_ÅRSAK,
            MAX(FAGSAK.UNDERKATEGORI) KEEP (DENSE_RANK FIRST ORDER BY FAGSAK.TIDSPUNKT_VEDTAK DESC)    AS UNDERKATEGORI,
            MAX(FAGSAK.FAGSAK_TYPE) KEEP (DENSE_RANK FIRST ORDER BY FAGSAK.TIDSPUNKT_VEDTAK DESC)      AS FAGSAK_TYPE,
 --fagsak.enslig_forsørger,
            DIM_PERSON_MOTTAKER.PK_DIM_PERSON,
            DIM_PERSON_MOTTAKER.FK_DIM_KJONN,
            DIM_PERSON_MOTTAKER.FK_DIM_SIVILSTATUS,
            DIM_PERSON_MOTTAKER.FK_DIM_LAND_FODT,
            DIM_PERSON_MOTTAKER.FK_DIM_GEOGRAFI_BOSTED,
            DIM_PERSON_MOTTAKER.FK_DIM_LAND_STATSBORGERSKAP,
            FAM_BT_PERIODE.PK_DIM_TID,
            FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN,
            DIM_ALDER.PK_DIM_ALDER,
            BARN.FKBY_PERSON1,
            BARN.ANTBARN,
            BARN.DELINGSPROSENT_YTELSE,
            TRUNC(MONTHS_BETWEEN(FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN, DIM_PERSON_BARN.FODT_DATO)/12) ALDERYB,
            DIM_LAND_BARN.LAND_SSB_KODE                                                                SSB_KODE,
            NVL(BELOP.BELOPHIE, 0)                                                                     BELOPHIE_B,
            NVL(BELOP.BELOPHIT, 0)                                                                     BELOPHIT_B,
            SUM((
              CASE
                WHEN UTVIDET_BARNETRYGD.DELYTELSE_ID IS NOT NULL THEN
                  1
                ELSE
                  0
              END) * UR.BELOP_SIGN)                                                                    BELOP_UTVIDET,
            NVL(BELOPHIT_UTVIDET.BELOPHIT_UTVIDET, 0)                                                  BELOPHIT_UTVIDET_B,
            MAX(FAM_BT_KOMPETANSE_PERIODER.SOKERSAKTIVITET)                                            SOKERS_AKTIVITET,
            MAX(FAM_BT_KOMPETANSE_PERIODER.ANNENFORELDER_AKTIVITET)                                    ANNENFORELDER_AKTIVITET,
            MAX(FAM_BT_KOMPETANSE_PERIODER.KOMPETANSE_RESULTAT)                                        KOMPETANSE_RESULTAT,
            MIN(
              CASE
                WHEN FAM_BT_KOMPETANSE_PERIODER.KOMPETANSE_RESULTAT= 'NORGE_ER_PRIMÆRLAND' THEN
                  '000'
                WHEN FAM_BT_KOMPETANSE_PERIODER.KOMPETANSE_RESULTAT= 'NORGE_ER_SEKUNDÆRLAND' THEN
                  DIM_LAND_ANNEN_FORELDER_AKTIVITET.LAND_SSB_KODE
                ELSE
                  NULL
              END)                                                                                     EOKLAND,
            MIN(
              CASE
                WHEN FAM_BT_KOMPETANSE_PERIODER.KOMPETANSE_RESULTAT= 'NORGE_ER_PRIMÆRLAND' THEN
                  DIM_LAND_ANNEN_FORELDER_AKTIVITET.LAND_SSB_KODE
                WHEN FAM_BT_KOMPETANSE_PERIODER.KOMPETANSE_RESULTAT= 'NORGE_ER_SEKUNDÆRLAND' THEN
                  '000'
                ELSE
                  NULL
              END)                                                                                     EODLAND,
            DIM_PERSON_MOTTAKER.GT_VERDI                                                               PERSON_GT_VERDI,
            MAX(FAM_BT_KOMPETANSE_PERIODER.BARNETS_BOSTEDSLAND)                                        BARNETS_BOSTEDSLAND,
            MAX(FAM_BT_KOMPETANSE_PERIODER.PK_BT_KOMPETANSE_PERIODER)                                  FK_BT_KOMPETANSE_PERIODER
          FROM
            DVH_FAM_BT.FAM_BT_UR_UTBETALING       UR
 --  dvh_fam_bt.FAM_BT_UR_202302 ur
            LEFT JOIN DVH_FAM_BT.FAM_BT_FAGSAK FAGSAK
            ON FAGSAK.BEHANDLINGS_ID = UR.HENVISNING
            LEFT OUTER JOIN DT_KODEVERK.DIM_TID FAM_BT_PERIODE
            ON TO_CHAR(UR.POSTERINGSDATO,
            'YYYYMM') = FAM_BT_PERIODE.AAR_MAANED
            AND FAM_BT_PERIODE.DIM_NIVAA = 3
            AND FAM_BT_PERIODE.GYLDIG_FLAGG = 1
            LEFT OUTER JOIN DT_PERSON.DIM_PERSON DIM_PERSON_MOTTAKER
            ON DIM_PERSON_MOTTAKER.FK_PERSON1 = UR.GJELDER_MOTTAKER
            AND DIM_PERSON_MOTTAKER.GYLDIG_FRA_DATO <= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
            AND DIM_PERSON_MOTTAKER.GYLDIG_TIL_DATO >= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
            LEFT OUTER JOIN DT_KODEVERK.DIM_GEOGRAFI DIM_GEOGRAFI
            ON DIM_PERSON_MOTTAKER.FK_DIM_GEOGRAFI_BOSTED = DIM_GEOGRAFI.PK_DIM_GEOGRAFI
            LEFT OUTER JOIN DT_KODEVERK.DIM_KJONN DIM_KJONN_MOTTAKER
            ON DIM_PERSON_MOTTAKER.FK_DIM_KJONN = DIM_KJONN_MOTTAKER.PK_DIM_KJONN
            LEFT OUTER JOIN DT_KODEVERK.DIM_ALDER
            ON FLOOR(MONTHS_BETWEEN(SYSDATE,
            DIM_PERSON_MOTTAKER.FODT_DATO)/12) = DIM_ALDER.ALDER
            AND DIM_ALDER.GYLDIG_FRA_DATO <= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
            AND DIM_ALDER.GYLDIG_TIL_DATO >= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
            LEFT OUTER JOIN --dvh_fam_bt.fam_bt_kompetanse_perioder
            (
              SELECT
                MAX(PK_BT_KOMPETANSE_PERIODER) PK_BT_KOMPETANSE_PERIODER,
                FOM,
                TOM,
 -- sokersaktivitet,
 -- annenforelder_aktivitet,
 -- annenforelder_aktivitetsland,
 -- kompetanse_resultat,
 -- barnets_bostedsland,
                FK_BT_FAGSAK --,
 -- sokers_aktivitetsland
              FROM
                DVH_FAM_BT.FAM_BT_KOMPETANSE_PERIODER
              GROUP BY
                FOM,
                TOM,
 --sokersaktivitet,
 --annenforelder_aktivitet,
 --annenforelder_aktivitetsland,
 --kompetanse_resultat,
 --barnets_bostedsland,
                FK_BT_FAGSAK --,
 --sokers_aktivitetsland
            ) KOMPETANSE_PERIODER
            ON FAGSAK.PK_BT_FAGSAK=KOMPETANSE_PERIODER.FK_BT_FAGSAK
            AND FAGSAK.KATEGORI='EØS'
            AND UR.DATO_UTBET_FOM>=TO_DATE(KOMPETANSE_PERIODER.FOM,
            'YYYY-MM')
            AND UR.DATO_UTBET_FOM<=TO_DATE(NVL(KOMPETANSE_PERIODER.TOM,
            '2099-12'),
            'YYYY-MM')
            LEFT OUTER JOIN DVH_FAM_BT.FAM_BT_KOMPETANSE_PERIODER
            ON KOMPETANSE_PERIODER.PK_BT_KOMPETANSE_PERIODER = DVH_FAM_BT.FAM_BT_KOMPETANSE_PERIODER.PK_BT_KOMPETANSE_PERIODER
            LEFT OUTER JOIN DT_KODEVERK.DIM_LAND DIM_LAND_ANNEN_FORELDER_AKTIVITET
            ON DIM_LAND_ANNEN_FORELDER_AKTIVITET.LAND_ISO_2_KODE=FAM_BT_KOMPETANSE_PERIODER.ANNENFORELDER_AKTIVITETSLAND
            AND DIM_LAND_ANNEN_FORELDER_AKTIVITET.GYLDIG_FLAGG=1
            AND DIM_LAND_ANNEN_FORELDER_AKTIVITET.LAND_ISO_3_KODE!='ESC'
            LEFT JOIN (
              SELECT
                FK_PERSON1,
                SUBSTR(MAX(FODSEL_AAR_BARN
                           ||FODSEL_MND_BARN
                           ||'-'
                           ||FKB_PERSON1), 8) FKBY_PERSON1,
                MAX(DELINGSPROSENT_YTELSE)    AS DELINGSPROSENT_YTELSE,
                COUNT(DISTINCT FKB_PERSON1)   ANTBARN
              FROM
                DVH_FAM_BT.FAM_BT_BARN
              WHERE
                KILDE = V_KILDE
                AND STAT_AARMND = P_IN_PERIOD
                AND GYLDIG_FLAGG = P_IN_GYLDIG_FLAGG
              GROUP BY
                FK_PERSON1
            ) BARN
            ON UR.GJELDER_MOTTAKER = BARN.FK_PERSON1
            LEFT JOIN DT_PERSON.DIM_PERSON DIM_PERSON_BARN
            ON DIM_PERSON_BARN.FK_PERSON1 = BARN.FKBY_PERSON1
            AND DIM_PERSON_BARN.GYLDIG_FRA_DATO <= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
            AND DIM_PERSON_BARN.GYLDIG_TIL_DATO >= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
            LEFT OUTER JOIN DT_KODEVERK.DIM_LAND DIM_LAND_BARN
            ON DIM_PERSON_BARN.FK_DIM_LAND_BOSTED = DIM_LAND_BARN.PK_DIM_LAND
 --AND dim_land.gyldig_fra_dato <= fam_bt_periode.siste_dato_i_perioden
 --AND dim_land.gyldig_til_dato >= fam_bt_periode.siste_dato_i_perioden
            LEFT JOIN (
              SELECT
                FK_PERSON1,
                SUM(NVL(BELOP, 0)) BELOPHIT,
                SUM(NVL(BELOPE, 0)) BELOPHIE
              FROM
                DVH_FAM_BT.FAM_BT_MOTTAKER
              WHERE
                STAT_AARMND >= V_AAR_START
                AND STAT_AARMND < P_IN_PERIOD
                AND GYLDIG_FLAGG = P_IN_GYLDIG_FLAGG
              GROUP BY
                FK_PERSON1
            ) BELOP
            ON UR.GJELDER_MOTTAKER = BELOP.FK_PERSON1
 --Utvidet barnetrygd
            LEFT JOIN (
              SELECT
                UTBET.BEHANDLINGS_ID,
                UTBET.DELYTELSE_ID,
                UTBETALING.STØNAD_FOM,
                UTBETALING.STØNAD_TOM
              FROM
                DVH_FAM_BT.FAM_BT_UTBET_DET  UTBET
                JOIN DVH_FAM_BT.FAM_BT_PERSON PERSON
                ON UTBET.FK_BT_PERSON = PERSON.PK_BT_PERSON
                AND PERSON.ROLLE = 'SØKER' JOIN DVH_FAM_BT.FAM_BT_UTBETALING UTBETALING
                ON UTBET.FK_BT_UTBETALING = UTBETALING.PK_BT_UTBETALING
                JOIN DVH_FAM_BT.FAM_BT_FAGSAK FAGSAK
                ON UTBETALING.BEHANDLINGS_ID = FAGSAK.BEHANDLINGS_ID
                AND FAGSAK.ENSLIG_FORSØRGER = 1
              GROUP BY
                UTBET.BEHANDLINGS_ID, UTBET.DELYTELSE_ID, UTBETALING.STØNAD_FOM, UTBETALING.STØNAD_TOM
            ) UTVIDET_BARNETRYGD
            ON UR.HENVISNING = UTVIDET_BARNETRYGD.BEHANDLINGS_ID
            AND UR.DELYTELSE_ID = UTVIDET_BARNETRYGD.DELYTELSE_ID
            AND UTVIDET_BARNETRYGD.STØNAD_FOM <= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
            AND UTVIDET_BARNETRYGD.STØNAD_TOM >= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
            LEFT JOIN (
              SELECT
                FK_PERSON1,
                SUM(NVL(BELOP_UTVIDET, 0)) BELOPHIT_UTVIDET
              FROM
                DVH_FAM_BT.FAM_BT_MOTTAKER
              WHERE
                STAT_AARMND >= V_AAR_START
                AND STAT_AARMND < P_IN_PERIOD
                AND GYLDIG_FLAGG = P_IN_GYLDIG_FLAGG
              GROUP BY
                FK_PERSON1
            ) BELOPHIT_UTVIDET
            ON UR.GJELDER_MOTTAKER = BELOPHIT_UTVIDET.FK_PERSON1
          WHERE
            UR.HOVEDKONTONR = 800 --in (800, 214, 216, 215)-- = 800 Test!!!
            AND TO_CHAR(UR.POSTERINGSDATO, 'YYYYMM') = P_IN_PERIOD
 --AND SLETT=0
 --AND UR.FK_PERSON1=1073261799
 --AND LENGTH(ur.delytelse_id) < 14
          GROUP BY
            UR.GJELDER_MOTTAKER, TO_CHAR(UR.POSTERINGSDATO, 'YYYYMM'), DIM_KJONN_MOTTAKER.KJONN_KODE, FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN, EXTRACT( YEAR FROM DIM_PERSON_MOTTAKER.FODT_DATO), TO_CHAR(DIM_PERSON_MOTTAKER.FODT_DATO, 'MM'), DIM_PERSON_MOTTAKER.TKNR, DIM_PERSON_MOTTAKER.BOSTED_KOMMUNE_NR, DIM_GEOGRAFI.KOMMUNE_NR, DIM_GEOGRAFI.BYDEL_KOMMUNE_NR, FAGSAK.FAGSAK_ID, DIM_PERSON_MOTTAKER.PK_DIM_PERSON, DIM_PERSON_MOTTAKER.FK_DIM_KJONN, DIM_PERSON_MOTTAKER.FK_DIM_SIVILSTATUS, DIM_PERSON_MOTTAKER.FK_DIM_LAND_FODT, DIM_PERSON_MOTTAKER.FK_DIM_GEOGRAFI_BOSTED, DIM_PERSON_MOTTAKER.FK_DIM_LAND_STATSBORGERSKAP, FAM_BT_PERIODE.PK_DIM_TID, DIM_ALDER.PK_DIM_ALDER, BARN.FKBY_PERSON1, BARN.ANTBARN, BARN.DELINGSPROSENT_YTELSE, TRUNC(MONTHS_BETWEEN(FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN, DIM_PERSON_BARN.FODT_DATO)/12), DIM_LAND_BARN.LAND_SSB_KODE, NVL(BELOP.BELOPHIE, 0), NVL(BELOP.BELOPHIT, 0), NVL(BELOPHIT_UTVIDET.BELOPHIT_UTVIDET, 0), DIM_PERSON_MOTTAKER.GT_VERDI
          HAVING
            SUM(UR.BELOP_SIGN) != 0
        ) DATA;
  BEGIN
 -- Slett mottakere dvh_fam_fp.fam_bt_mottaker_hist for aktuell periode
    BEGIN
      DELETE FROM DVH_FAM_BT.FAM_BT_MOTTAKER --r152241.test_drp
      WHERE
        KILDE = V_KILDE
        AND STAT_AARMND= P_IN_PERIOD
        AND GYLDIG_FLAGG = P_IN_GYLDIG_FLAGG;
      COMMIT;
    EXCEPTION
      WHEN OTHERS THEN
        L_ERROR_MELDING := SUBSTR(SQLCODE
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
          L_ERROR_MELDING,
          SYSDATE,
          'FAM_BT_MOTTAKER_INSERT_WITH1'
        );
        COMMIT;
        P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                  || L_ERROR_MELDING, 1, 1000);
        L_ERROR_MELDING := NULL;
    END;
    FOR REC_MOTTAKER IN CUR_MOTTAKER LOOP
 --INSERT INTO dvh_fam_fp.fam_bt_mottaker
      BEGIN
        INSERT INTO DVH_FAM_BT.FAM_BT_MOTTAKER --R152241.test_drp
 (
          FK_PERSON1,
          BELOP,
          KJONN,
          BELOPE,
          FODSEL_AAR,
          FODSEL_MND,
          TKNR,
          BOSTED_KOMMUNE_NR,
          BOSTED_BYDEL_KOMMUNE_NR,
          FAGSAK_ID,
          FK_DIM_PERSON,
          FK_DIM_KJONN,
          FK_DIM_SIVILSTATUS,
          FK_DIM_LAND_FODT,
          FK_DIM_GEOGRAFI_BOSTED,
          FK_DIM_LAND_STATSBORGERSKAP,
          FK_DIM_TID_MND,
          FK_DIM_ALDER,
          FKBY_PERSON1,
          ANTBARN,
          BELOPHIE,
          BELOPHIT,
          YBARN,
          EOYBLAND,
          KILDE,
          STAT_AARMND,
          LASTET_DATO,
          PROSENT,
          STATUSK,
          BELOP_UTVIDET,
          BELOPHIT_UTVIDET,
          FK_BT_FAGSAK,
          BEHANDLING_ÅRSAK,
          GYLDIG_FLAGG,
          EOKLAND,
          EODLAND
 --,SOKERS_AKTIVITET
 --,ANNENFORELDER_AKTIVITET
 --,KOMPETANSE_RESULTAT
 --,BARNETS_BOSTEDSLAND
,
          PERSON_GT_VERDI,
          FK_BT_KOMPETANSE_PERIODER
        ) VALUES (
          REC_MOTTAKER.GJELDER_MOTTAKER,
          REC_MOTTAKER.BELOP,
          REC_MOTTAKER.KJONN,
          REC_MOTTAKER.BELOPE,
          REC_MOTTAKER.FODSEL_AAR,
          REC_MOTTAKER.FODSEL_MND,
          REC_MOTTAKER.TKNR,
          REC_MOTTAKER.BOSTED_KOMMUNE_NR,
          REC_MOTTAKER.BYDEL_KOMMUNE_NR,
          REC_MOTTAKER.FAGSAK_ID,
          REC_MOTTAKER.PK_DIM_PERSON,
          REC_MOTTAKER.FK_DIM_KJONN,
          REC_MOTTAKER.FK_DIM_SIVILSTATUS,
          REC_MOTTAKER.FK_DIM_LAND_FODT,
          REC_MOTTAKER.FK_DIM_GEOGRAFI_BOSTED,
          REC_MOTTAKER.FK_DIM_LAND_STATSBORGERSKAP,
          REC_MOTTAKER.PK_DIM_TID,
          REC_MOTTAKER.PK_DIM_ALDER,
          REC_MOTTAKER.FKBY_PERSON1,
          REC_MOTTAKER.ANTBARN,
          REC_MOTTAKER.BELOPHIE,
          REC_MOTTAKER.BELOPHIT,
          REC_MOTTAKER.ALDERYB,
          REC_MOTTAKER.SSB_KODE,
          V_KILDE,
          P_IN_PERIOD,
          SYSDATE,
          REC_MOTTAKER.DELINGSPROSENT_YTELSE,
          REC_MOTTAKER.STATUSK,
          REC_MOTTAKER.BELOP_UTVIDET,
          REC_MOTTAKER.BELOPHIT_UTVIDET,
          REC_MOTTAKER.PK_BT_FAGSAK,
          REC_MOTTAKER.BEHANDLING_ÅRSAK,
          P_IN_GYLDIG_FLAGG,
          REC_MOTTAKER.EOKLAND,
          REC_MOTTAKER.EODLAND
 --,rec_mottaker.sokers_aktivitet
 --,rec_mottaker.annenforelder_aktivitet
 --,rec_mottaker.kompetanse_resultat
 --,rec_mottaker.barnets_bostedsland
,
          REC_MOTTAKER.PERSON_GT_VERDI,
          REC_MOTTAKER.FK_BT_KOMPETANSE_PERIODER
        );
        L_COMMIT := L_COMMIT + 1;
      EXCEPTION
        WHEN OTHERS THEN
          L_ERROR_MELDING := SUBSTR(SQLCODE
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
            REC_MOTTAKER.FAGSAK_ID,
            L_ERROR_MELDING,
            SYSDATE,
            'FAM_BT_MOTTAKER_INSERT_WITH2'
          );
          L_COMMIT := L_COMMIT + 1; --Gå videre til neste rekord
          P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                                                  || L_ERROR_MELDING, 1, 1000);
          L_ERROR_MELDING := NULL;
      END;
      IF L_COMMIT >= 100000 THEN
        COMMIT;
        L_COMMIT := 0;
      END IF;
    END LOOP;
    COMMIT;
    IF L_ERROR_MELDING IS NOT NULL THEN
      INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
        ID,
        ERROR_MSG,
        OPPRETTET_TID,
        KILDE
      ) VALUES(
        NULL,
        L_ERROR_MELDING,
        SYSDATE,
        'FAM_BT_MOTTAKER_INSERT_WITH3'
      );
      COMMIT;
      P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                || L_ERROR_MELDING, 1, 1000);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      L_ERROR_MELDING := SUBSTR(SQLCODE
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
        L_ERROR_MELDING,
        SYSDATE,
        'FAM_BT_MOTTAKER_INSERT_WITH4'
      );
      COMMIT;
      P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                || L_ERROR_MELDING, 1, 1000);
  END FAM_BT_MOTTAKER_INSERT;

  PROCEDURE FAM_BT_BARN_INSERT_BCK(
    P_IN_PERIOD IN VARCHAR2,
    P_ERROR_MELDING OUT VARCHAR2
  ) AS
    V_KILDE         VARCHAR2(10) := 'BT';
    V_STOREDATE     DATE := SYSDATE;
    L_ERROR_MELDING VARCHAR2(1000);
    L_COMMIT        NUMBER := 0;
    CURSOR CUR_BARN(V_IN_PERIOD VARCHAR2) IS
      SELECT /*+ PARALLEL(8) */
        BARN.FK_PERSON1                                   FKB_PERSON1,
        BARN.DELINGSPROSENT_YTELSE,
        DIM_PERSON_MOTTAKER.FK_PERSON1,
        NULL                                              INST,
        EXTRACT (YEAR FROM DIM_PERSON_MOTTAKER.FODT_DATO) FODSEL_AAR,
        TO_CHAR(DIM_PERSON_MOTTAKER.FODT_DATO, 'MM')      FODSEL_MND,
        DIM_KJONN_MOTTAKER.KJONN_KODE                     KJONN,
        EXTRACT (YEAR FROM DIM_PERSON_BARN.FODT_DATO)     FODSEL_AAR_BARN,
        TO_CHAR(DIM_PERSON_BARN.FODT_DATO, 'MM')          FODSEL_MND_BARN,
        DIM_KJONN_BARN.KJONN_KODE                         KJONN_BARN,
        FAM_BT_PERIODE.AAR_MAANED                         STAT_AARMND,
        FAGSAK.PK_BT_FAGSAK                               FK_BT_FAGSAK
 --,fagsak.fagsak_id
      FROM
        DVH_FAM_BT.FAM_BT_UR_UTBETALING UR
        JOIN DVH_FAM_BT.FAM_BT_UTBET_DET DET
        ON UR.DELYTELSE_ID = DET.DELYTELSE_ID
 --and det.delytelse_id = 10364905
        JOIN DVH_FAM_BT.FAM_BT_UTBETALING PERUT
        ON PERUT.PK_BT_UTBETALING = DET.FK_BT_UTBETALING
        AND PERUT.STØNAD_FOM <= UR.DATO_UTBET_FOM
        AND PERUT.STØNAD_TOM >= UR.DATO_UTBET_TOM
        JOIN DVH_FAM_BT.FAM_BT_PERSON BARN
        ON BARN.PK_BT_PERSON = DET.FK_BT_PERSON
        AND BARN.ROLLE = 'BARN' JOIN DVH_FAM_BT.FAM_BT_FAGSAK FAGSAK
        ON FAGSAK.PK_BT_FAGSAK = PERUT.FK_BT_FAGSAK
        AND FAGSAK.BEHANDLINGS_ID = UR.HENVISNING
        LEFT OUTER JOIN DT_KODEVERK.DIM_TID FAM_BT_PERIODE
        ON TO_CHAR(UR.POSTERINGSDATO,
        'YYYYMM') = FAM_BT_PERIODE.AAR_MAANED
        AND FAM_BT_PERIODE.DIM_NIVAA = 3
        AND FAM_BT_PERIODE.GYLDIG_FLAGG = 1
        LEFT OUTER JOIN DT_PERSON.DIM_PERSON DIM_PERSON_BARN
        ON DIM_PERSON_BARN.FK_PERSON1 = BARN.FK_PERSON1
        AND DIM_PERSON_BARN.GYLDIG_FRA_DATO <= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
        AND DIM_PERSON_BARN.GYLDIG_TIL_DATO >= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
        LEFT OUTER JOIN DT_KODEVERK.DIM_KJONN DIM_KJONN_BARN
        ON DIM_PERSON_BARN.FK_DIM_KJONN = DIM_KJONN_BARN.PK_DIM_KJONN
 --AND dim_kjonn_barn.gyldig_fra_dato < fam_bt_periode.siste_dato_i_perioden
 --AND dim_kjonn_barn.gyldig_til_dato > fam_bt_periode.siste_dato_i_perioden
        JOIN DT_PERSON.DIM_PERSON DIM_PERSON_MOTTAKER
        ON DIM_PERSON_MOTTAKER.FK_PERSON1 = UR.GJELDER_MOTTAKER
        AND DIM_PERSON_MOTTAKER.GYLDIG_FRA_DATO <= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
        AND DIM_PERSON_MOTTAKER.GYLDIG_TIL_DATO >= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN LEFT OUTER JOIN DT_KODEVERK.DIM_KJONN DIM_KJONN_MOTTAKER
        ON DIM_PERSON_MOTTAKER.FK_DIM_KJONN = DIM_KJONN_MOTTAKER.PK_DIM_KJONN
 --AND dim_kjonn_mottaker.gyldig_fra_dato < fam_bt_periode.siste_dato_i_perioden
 --AND dim_kjonn_mottaker.gyldig_til_dato > fam_bt_periode.siste_dato_i_perioden
      WHERE
        UR.HOVEDKONTONR = 800
 --AND ur.status != 11
        AND TO_CHAR(UR.POSTERINGSDATO, 'YYYYMM') = V_IN_PERIOD
 --AND LENGTH(ur.delytelse_id) < 14
      GROUP BY
        BARN.FK_PERSON1, BARN.DELINGSPROSENT_YTELSE, DIM_PERSON_MOTTAKER.FK_PERSON1, EXTRACT (YEAR FROM DIM_PERSON_MOTTAKER.FODT_DATO), TO_CHAR(DIM_PERSON_MOTTAKER.FODT_DATO, 'MM'), DIM_KJONN_MOTTAKER.KJONN_KODE, EXTRACT (YEAR FROM DIM_PERSON_BARN.FODT_DATO), TO_CHAR(DIM_PERSON_BARN.FODT_DATO, 'MM'), DIM_KJONN_BARN.KJONN_KODE, FAM_BT_PERIODE.AAR_MAANED, FAGSAK.PK_BT_FAGSAK;
  BEGIN
 -- Slett data i dvh_fam_fp.fam_bt_barn for aktiuell periode (egen prosedyre)
    BEGIN
      DELETE FROM DVH_FAM_BT.FAM_BT_BARN
      WHERE
        KILDE = V_KILDE
        AND STAT_AARMND = P_IN_PERIOD;
      COMMIT;
    EXCEPTION
      WHEN OTHERS THEN
        L_ERROR_MELDING := SQLCODE
                           || ' '
                           || SQLERRM;
        INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
          MIN_LASTET_DATO,
          ID,
          ERROR_MSG,
          OPPRETTET_TID,
          KILDE
        ) VALUES(
          NULL,
          NULL,
          L_ERROR_MELDING,
          V_STOREDATE,
          'FAM_BT_BARN_INSERT1'
        );
        COMMIT;
        P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                  || L_ERROR_MELDING, 1, 1000);
    END;
 -- Patch opp fk_person1
    BEGIN
      MERGE INTO DVH_FAM_BT.FAM_BT_PERSON USING (
        SELECT
          OFF_ID,
          MAX(FK_PERSON1) KEEP (DENSE_RANK FIRST ORDER BY GYLDIG_FRA_DATO DESC) AS FK_PERSON1
        FROM
          DT_PERSON.DVH_PERSON_IDENT_OFF_ID_IKKE_SKJERMET
        GROUP BY
          OFF_ID
      ) PERSON_67_VASKET ON (PERSON_67_VASKET.OFF_ID = FAM_BT_PERSON.PERSON_IDENT) WHEN MATCHED THEN UPDATE SET FAM_BT_PERSON.FK_PERSON1 = PERSON_67_VASKET.FK_PERSON1 WHERE FAM_BT_PERSON.FK_PERSON1 = -1;
      COMMIT;
    EXCEPTION
      WHEN OTHERS THEN
        L_ERROR_MELDING := SQLCODE
                           || ' '
                           || SQLERRM;
        INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
          MIN_LASTET_DATO,
          ID,
          ERROR_MSG,
          OPPRETTET_TID,
          KILDE
        ) VALUES(
          NULL,
          NULL,
          L_ERROR_MELDING,
          V_STOREDATE,
          'FAM_BT_BARN_INSERT: Patch opp fk_person1'
        );
        COMMIT;
        P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                  || L_ERROR_MELDING, 1, 1000);
    END;
 -- Slett person_ident
    BEGIN
      UPDATE DVH_FAM_BT.FAM_BT_PERSON
      SET
        PERSON_IDENT = NULL
      WHERE
        FK_PERSON1 != -1
        AND PERSON_IDENT IS NOT NULL;
      COMMIT;
    EXCEPTION
      WHEN OTHERS THEN
        L_ERROR_MELDING := SQLCODE
                           || ' '
                           || SQLERRM;
        INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
          MIN_LASTET_DATO,
          ID,
          ERROR_MSG,
          OPPRETTET_TID,
          KILDE
        ) VALUES(
          NULL,
          NULL,
          L_ERROR_MELDING,
          V_STOREDATE,
          'FAM_BT_BARN_INSERT: Slett person_ident'
        );
        COMMIT;
        P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                  || L_ERROR_MELDING, 1, 1000);
    END;
    FOR REC_BARN IN CUR_BARN(P_IN_PERIOD) LOOP
 --dbms_output.put_line(rec_barn.fkb_person1);--Test
      BEGIN
        INSERT INTO DVH_FAM_BT.FAM_BT_BARN (
          FKB_PERSON1,
          FK_PERSON1,
          INST,
          FODSEL_AAR,
          FODSEL_MND,
          KJONN,
          FODSEL_AAR_BARN,
          FODSEL_MND_BARN,
          KJONN_BARN,
          DELINGSPROSENT_YTELSE,
          STAT_AARMND,
          KILDE,
          LASTET_DATO,
          FK_BT_FAGSAK
        ) VALUES (
          REC_BARN.FKB_PERSON1,
          REC_BARN.FK_PERSON1,
          REC_BARN.INST,
          REC_BARN.FODSEL_AAR,
          REC_BARN.FODSEL_MND,
          REC_BARN.KJONN,
          REC_BARN.FODSEL_AAR_BARN,
          REC_BARN.FODSEL_MND_BARN,
          REC_BARN.KJONN_BARN,
          REC_BARN.DELINGSPROSENT_YTELSE,
          REC_BARN.STAT_AARMND,
          V_KILDE,
          SYSDATE, --v_storedate
          REC_BARN.FK_BT_FAGSAK
        );
        L_COMMIT := L_COMMIT + 1;
      EXCEPTION
        WHEN OTHERS THEN
          L_ERROR_MELDING := SQLCODE
                             || ' '
                             || SQLERRM;
          INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
            MIN_LASTET_DATO,
            ID,
            ERROR_MSG,
            OPPRETTET_TID,
            KILDE
          ) VALUES(
            NULL,
            REC_BARN.FKB_PERSON1,
            L_ERROR_MELDING,
            V_STOREDATE,
            'FAM_BT_BARN_INSERT2'
          );
          L_COMMIT := L_COMMIT + 1; --Gå videre til neste rekord
          P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                                                  || L_ERROR_MELDING, 1, 1000);
      END;
 --rollback;--Test
      IF L_COMMIT >= 100000 THEN
        COMMIT;
        L_COMMIT := 0;
      END IF;
    END LOOP;
    COMMIT; --commit til slutt
    IF L_ERROR_MELDING IS NOT NULL THEN
      INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
        ID,
        ERROR_MSG,
        OPPRETTET_TID,
        KILDE
      ) VALUES(
        NULL,
        L_ERROR_MELDING,
        V_STOREDATE,
        'FAM_BT_BARN_INSERT3'
      );
      COMMIT;
      P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                || L_ERROR_MELDING, 1, 1000);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      L_ERROR_MELDING := SQLCODE
                         || ' '
                         || SQLERRM;
      INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
        MIN_LASTET_DATO,
        ID,
        ERROR_MSG,
        OPPRETTET_TID,
        KILDE
      ) VALUES(
        NULL,
        NULL,
        L_ERROR_MELDING,
        V_STOREDATE,
        'FAM_BT_BARN_INSERT4'
      );
      COMMIT;
      P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                || L_ERROR_MELDING, 1, 1000);
  END FAM_BT_BARN_INSERT_BCK;

  PROCEDURE FAM_BT_BARN_INSERT(
    P_IN_PERIOD IN VARCHAR2,
    P_IN_GYLDIG_FLAGG IN NUMBER DEFAULT 0,
    P_ERROR_MELDING OUT VARCHAR2
  ) AS
    V_KILDE         VARCHAR2(10) := 'BT';
    V_STOREDATE     DATE := SYSDATE;
    L_ERROR_MELDING VARCHAR2(1000);
    L_COMMIT        NUMBER := 0;
    CURSOR CUR_BARN(V_IN_PERIOD VARCHAR2) IS
      WITH UR_VEDTAK1 AS (
        SELECT /*+ PARALLEL(8) */
          UR.GJELDER_MOTTAKER,
          UR.DATO_UTBET_FOM,
          UR.DATO_UTBET_TOM,
          UR.POSTERINGSDATO,
          UR.HENVISNING,
          UR.BELOP_SIGN,
          UR.DELYTELSE_ID,
          DET.FK_BT_PERSON,
          DET.BEHANDLINGS_ID,
          UTBETALING.FK_BT_FAGSAK,
          BARN.FK_PERSON1,
          BARN.DELINGSPROSENT_YTELSE,
          FAGSAK.FAGSAK_ID
        FROM
          DVH_FAM_BT.FAM_BT_UR_UTBETALING UR
          JOIN DVH_FAM_BT.FAM_BT_UTBET_DET DET
          ON UR.DELYTELSE_ID = DET.DELYTELSE_ID JOIN DVH_FAM_BT.FAM_BT_UTBETALING UTBETALING
          ON DET.FK_BT_UTBETALING = UTBETALING.PK_BT_UTBETALING
          AND UTBETALING.STØNAD_FOM <= UR.DATO_UTBET_FOM
          AND UTBETALING.STØNAD_TOM >= UR.DATO_UTBET_TOM
          JOIN DVH_FAM_BT.FAM_BT_FAGSAK FAGSAK
          ON UTBETALING.FK_BT_FAGSAK = FAGSAK.PK_BT_FAGSAK JOIN DVH_FAM_BT.FAM_BT_PERSON BARN
          ON BARN.PK_BT_PERSON = DET.FK_BT_PERSON
          AND BARN.ROLLE = 'BARN'
        WHERE
          TO_CHAR(UR.POSTERINGSDATO, 'YYYYMM') = V_IN_PERIOD
      ), UR_VEDTAK2 AS (
        SELECT
          UR_VEDTAK1.*
        FROM
          UR_VEDTAK1
          JOIN (
            SELECT
              UR_VEDTAK1.DELYTELSE_ID,
              MAX(UR_VEDTAK1.BEHANDLINGS_ID) KEEP (DENSE_RANK FIRST ORDER BY FAGSAK.TIDSPUNKT_VEDTAK DESC) AS SISTE_VERSJON
            FROM
              UR_VEDTAK1
              JOIN DVH_FAM_BT.FAM_BT_FAGSAK FAGSAK
              ON UR_VEDTAK1.FAGSAK_ID = FAGSAK.FAGSAK_ID
              AND TO_CHAR(FAGSAK.TIDSPUNKT_VEDTAK, 'yyyymmdd') <= TO_CHAR(UR_VEDTAK1.POSTERINGSDATO, 'yyyymmdd')
            GROUP BY
              UR_VEDTAK1.DELYTELSE_ID
          ) SISTE
          ON UR_VEDTAK1.DELYTELSE_ID = SISTE.DELYTELSE_ID
          AND UR_VEDTAK1.BEHANDLINGS_ID = SISTE.SISTE_VERSJON
      ), UR AS (
        SELECT /*+ PARALLEL(8) */
          GJELDER_MOTTAKER,
          FK_PERSON1,
          MAX(FK_BT_FAGSAK)                                                               AS FK_BT_FAGSAK,
          MAX(DATO_UTBET_TOM)                                                             AS MAX_DATO_UTBET_TOM,
          MAX(POSTERINGSDATO)                                                             AS MAX_POSTERINGSDATO,
          MAX(FK_BT_PERSON) KEEP (DENSE_RANK FIRST ORDER BY DATO_UTBET_TOM DESC)          AS MAX_FK_BT_PERSON,
          MAX(DELINGSPROSENT_YTELSE) KEEP (DENSE_RANK FIRST ORDER BY DATO_UTBET_TOM DESC) AS MAX_DELINGSPROSENT_YTELSE,
          MAX(HENVISNING) KEEP (DENSE_RANK FIRST ORDER BY HENVISNING DESC)                AS HENVISNING,
          SUM(
            CASE
              WHEN POSTERINGSDATO BETWEEN DATO_UTBET_FOM AND DATO_UTBET_TOM THEN
                BELOP_SIGN
              ELSE
                0
            END)                                                                          AS BELOP,
          SUM(
            CASE
              WHEN POSTERINGSDATO > DATO_UTBET_TOM THEN
                BELOP_SIGN
              ELSE
                0
            END)                                                                          AS BELOPE
        FROM
          UR_VEDTAK2
        GROUP BY
          TO_CHAR(POSTERINGSDATO,
          'YYYYMM'),
          GJELDER_MOTTAKER,
          FK_PERSON1
        HAVING
          SUM(BELOP_SIGN) != 0
      ), VEDTAK AS (
        SELECT /*+ PARALLEL(8) */
          UR.FK_PERSON1                                     AS FKB_PERSON1,
          UR.MAX_DELINGSPROSENT_YTELSE                      AS DELINGSPROSENT_YTELSE,
          UR.FK_BT_FAGSAK,
          DIM_PERSON_MOTTAKER.FK_PERSON1,
          NULL                                              INST,
          EXTRACT (YEAR FROM DIM_PERSON_MOTTAKER.FODT_DATO) FODSEL_AAR,
          TO_CHAR(DIM_PERSON_MOTTAKER.FODT_DATO, 'MM')      FODSEL_MND,
          DIM_KJONN_MOTTAKER.KJONN_KODE                     KJONN,
          EXTRACT (YEAR FROM DIM_PERSON_BARN.FODT_DATO)     FODSEL_AAR_BARN,
          TO_CHAR(DIM_PERSON_BARN.FODT_DATO, 'MM')          FODSEL_MND_BARN,
          DIM_KJONN_BARN.KJONN_KODE                         KJONN_BARN,
          FAM_BT_PERIODE.AAR_MAANED                         STAT_AARMND,
          DIM_PERSON_BARN.PK_DIM_PERSON                     FK_DIM_PERSON_BARN,
          BELOP,
          BELOPE
        FROM
          UR
          JOIN DT_KODEVERK.DIM_TID FAM_BT_PERIODE
          ON TO_CHAR(UR.MAX_POSTERINGSDATO, 'YYYYMM') = FAM_BT_PERIODE.AAR_MAANED
          AND FAM_BT_PERIODE.DIM_NIVAA = 3
          AND FAM_BT_PERIODE.GYLDIG_FLAGG = 1 LEFT OUTER JOIN DT_PERSON.DIM_PERSON DIM_PERSON_BARN
          ON DIM_PERSON_BARN.FK_PERSON1 = UR.FK_PERSON1
          AND DIM_PERSON_BARN.GYLDIG_FRA_DATO <= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
          AND DIM_PERSON_BARN.GYLDIG_TIL_DATO >= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
          LEFT OUTER JOIN DT_KODEVERK.DIM_KJONN DIM_KJONN_BARN
          ON DIM_PERSON_BARN.FK_DIM_KJONN = DIM_KJONN_BARN.PK_DIM_KJONN
          LEFT OUTER JOIN DT_PERSON.DIM_PERSON DIM_PERSON_MOTTAKER
          ON DIM_PERSON_MOTTAKER.FK_PERSON1 = UR.GJELDER_MOTTAKER
          AND DIM_PERSON_MOTTAKER.GYLDIG_FRA_DATO <= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
          AND DIM_PERSON_MOTTAKER.GYLDIG_TIL_DATO >= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
          LEFT OUTER JOIN DT_KODEVERK.DIM_KJONN DIM_KJONN_MOTTAKER
          ON DIM_PERSON_MOTTAKER.FK_DIM_KJONN = DIM_KJONN_MOTTAKER.PK_DIM_KJONN
        GROUP BY
          UR.FK_PERSON1,
          UR.MAX_DELINGSPROSENT_YTELSE,
          UR.FK_BT_FAGSAK,
          DIM_PERSON_MOTTAKER.FK_PERSON1,
          DIM_PERSON_MOTTAKER.FODT_DATO,
          DIM_PERSON_BARN.FODT_DATO,
          DIM_KJONN_MOTTAKER.KJONN_KODE,
          DIM_KJONN_BARN.KJONN_KODE,
          FAM_BT_PERIODE.AAR_MAANED,
          DIM_PERSON_BARN.PK_DIM_PERSON,
          BELOP,
          BELOPE
      )
      SELECT /*+ PARALLEL(8) */
        VEDTAK.*
      FROM
        VEDTAK;
  BEGIN
 -- Slett data i dvh_fam_fp.fam_bt_barn for aktiuell periode (egen prosedyre)
    BEGIN
      DELETE FROM DVH_FAM_BT.FAM_BT_BARN
      WHERE
        KILDE = V_KILDE
        AND STAT_AARMND = P_IN_PERIOD
        AND GYLDIG_FLAGG = P_IN_GYLDIG_FLAGG;
      COMMIT;
    EXCEPTION
      WHEN OTHERS THEN
        L_ERROR_MELDING := SQLCODE
                           || ' '
                           || SQLERRM;
        INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
          MIN_LASTET_DATO,
          ID,
          ERROR_MSG,
          OPPRETTET_TID,
          KILDE
        ) VALUES(
          NULL,
          NULL,
          L_ERROR_MELDING,
          V_STOREDATE,
          'FAM_BT_BARN_INSERT1'
        );
        COMMIT;
        P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                  || L_ERROR_MELDING, 1, 1000);
    END;
    FOR REC_BARN IN CUR_BARN(P_IN_PERIOD) LOOP
 --dbms_output.put_line(rec_barn.fkb_person1);--Test
      BEGIN
        INSERT INTO DVH_FAM_BT.FAM_BT_BARN (
          FKB_PERSON1,
          FK_PERSON1,
          INST,
          FODSEL_AAR,
          FODSEL_MND,
          KJONN,
          FODSEL_AAR_BARN,
          FODSEL_MND_BARN,
          KJONN_BARN,
          DELINGSPROSENT_YTELSE,
          STAT_AARMND,
          KILDE,
          LASTET_DATO,
          GYLDIG_FLAGG,
          FK_BT_FAGSAK,
          FK_DIM_PERSON_BARN,
          BELOP,
          BELOPE
        ) VALUES (
          REC_BARN.FKB_PERSON1,
          REC_BARN.FK_PERSON1,
          REC_BARN.INST,
          REC_BARN.FODSEL_AAR,
          REC_BARN.FODSEL_MND,
          REC_BARN.KJONN,
          REC_BARN.FODSEL_AAR_BARN,
          REC_BARN.FODSEL_MND_BARN,
          REC_BARN.KJONN_BARN,
          REC_BARN.DELINGSPROSENT_YTELSE,
          REC_BARN.STAT_AARMND,
          V_KILDE,
          SYSDATE --v_storedate
,
          P_IN_GYLDIG_FLAGG,
          REC_BARN.FK_BT_FAGSAK,
          REC_BARN.FK_DIM_PERSON_BARN,
          REC_BARN.BELOP,
          REC_BARN.BELOPE
        );
        L_COMMIT := L_COMMIT + 1;
      EXCEPTION
        WHEN OTHERS THEN
          L_ERROR_MELDING := SQLCODE
                             || ' '
                             || SQLERRM;
          INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
            MIN_LASTET_DATO,
            ID,
            ERROR_MSG,
            OPPRETTET_TID,
            KILDE
          ) VALUES(
            NULL,
            REC_BARN.FKB_PERSON1,
            L_ERROR_MELDING,
            V_STOREDATE,
            'FAM_BT_BARN_INSERT2'
          );
          L_COMMIT := L_COMMIT + 1; --Gå videre til neste rekord
          P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                                                  || L_ERROR_MELDING, 1, 1000);
      END;
 --rollback;--Test
      IF L_COMMIT >= 100000 THEN
        COMMIT;
        L_COMMIT := 0;
      END IF;
    END LOOP;
    COMMIT; --commit til slutt
    IF L_ERROR_MELDING IS NOT NULL THEN
      INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
        ID,
        ERROR_MSG,
        OPPRETTET_TID,
        KILDE
      ) VALUES(
        NULL,
        L_ERROR_MELDING,
        V_STOREDATE,
        'FAM_BT_BARN_INSERT3'
      );
      COMMIT;
      P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                || L_ERROR_MELDING, 1, 1000);
    END IF;
    MERGE INTO DVH_FAM_BT.FAM_BT_BARN USING (
      SELECT
        A.PK_BT_BARN,
        A.STAT_AARMND,
        A.FK_PERSON1,
        A.FKB_PERSON1,
        D.PK_BT_KOMPETANSE_PERIODER                                           FK_BT_KOMPETANSE_PERIODER
      FROM
        DVH_FAM_BT.FAM_BT_BARN                          A
        JOIN DVH_FAM_BT.FAM_BT_KOMPETANSE_PERIODER D
        ON A.FK_BT_FAGSAK=D.FK_BT_FAGSAK
 --AND D.PK_BT_KOMPETANSE_PERIODER=C.FK_BT_KOMPETANSE_PERIODER
        AND TO_DATE(A.STAT_AARMND, 'YYYYMM')>=TO_DATE(D.FOM, 'YYYY-MM')
        AND TO_DATE(A.STAT_AARMND, 'YYYYMM')<=TO_DATE(NVL(D.TOM, '2099-12'), 'YYYY-MM') JOIN DVH_FAM_BT.FAM_BT_KOMPETANSE_BARN C
        ON A.FKB_PERSON1=C.FK_PERSON1
        AND D.PK_BT_KOMPETANSE_PERIODER=C.FK_BT_KOMPETANSE_PERIODER
      WHERE
        A.STAT_AARMND=P_IN_PERIOD
        AND A.GYLDIG_FLAGG=P_IN_GYLDIG_FLAGG
    ) BARN ON (FAM_BT_BARN.PK_BT_BARN=BARN.PK_BT_BARN) WHEN MATCHED THEN UPDATE SET FAM_BT_BARN.FK_BT_KOMPETANSE_PERIODER = BARN.FK_BT_KOMPETANSE_PERIODER WHERE BARN.STAT_AARMND=P_IN_PERIOD;
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      L_ERROR_MELDING := SQLCODE
                         || ' '
                         || SQLERRM;
      INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
        MIN_LASTET_DATO,
        ID,
        ERROR_MSG,
        OPPRETTET_TID,
        KILDE
      ) VALUES(
        NULL,
        NULL,
        L_ERROR_MELDING,
        V_STOREDATE,
        'FAM_BT_BARN_INSERT4'
      );
      COMMIT;
      P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                || L_ERROR_MELDING, 1, 1000);
  END FAM_BT_BARN_INSERT;

  PROCEDURE FAM_BT_MOTTAKER_INSERT_BCK(
    P_IN_PERIOD IN VARCHAR2,
    P_ERROR_MELDING OUT VARCHAR2
  ) AS
    V_KILDE         VARCHAR2(10) := 'BT';
    V_STOREDATE     DATE := SYSDATE;
    V_AAR_START     VARCHAR2(6):= SUBSTR(P_IN_PERIOD, 1, 4)
                                  || '01';
    V_FK_PERSON1_YB VARCHAR2(40);
    V_FK_PERSON1_EB VARCHAR2(40);
    V_ANT_BARN      NUMBER;
    V_BELOPHIE      NUMBER;
    V_BELOPHIT      NUMBER;
    V_ALDERYB       NUMBER;
    V_SSB_KODE      VARCHAR2(10);
    L_ERROR_MELDING VARCHAR2(1000);
    L_COMMIT        NUMBER := 0;
    CURSOR CUR_MOTTAKER(V_IN_PERIOD VARCHAR2) IS
      SELECT /*+ PARALLEL(8) */
        GJELDER_MOTTAKER,
        TO_CHAR(POSTERINGSDATO, 'YYYYMM')                 PERIODE,
        SUM(BELOP_SIGN)                                   BELOP, --, DELYTELSE_ID
        DIM_KJONN_MOTTAKER.KJONN_KODE                     KJONN,
        SUM(
          CASE
            WHEN TO_CHAR(POSTERINGSDATO, 'YYYYMM') > TO_CHAR(DATO_UTBET_FOM, 'YYYYMM') THEN
              BELOP_SIGN
            ELSE
              0.0
          END)                                            BELOPE,
        FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN,
        EXTRACT( YEAR FROM DIM_PERSON_MOTTAKER.FODT_DATO) FODSEL_AAR,
        TO_CHAR(DIM_PERSON_MOTTAKER.FODT_DATO, 'MM')      FODSEL_MND,
 --nvl(DIM_GEOGRAFI.KOMMUNE_NR, DIM_PERSON_MOTTAKER.BOSTED_KOMMUNE_NR) KOMMUNE_NR
        DIM_PERSON_MOTTAKER.TKNR,
        DIM_PERSON_MOTTAKER.BOSTED_KOMMUNE_NR,
        DIM_GEOGRAFI.KOMMUNE_NR,
 --dim_geografi.bydel_nr,
        DIM_GEOGRAFI.BYDEL_KOMMUNE_NR,
        FAGSAK.FAGSAK_ID,
        DIM_PERSON_MOTTAKER.PK_DIM_PERSON,
        DIM_PERSON_MOTTAKER.FK_DIM_KJONN,
        DIM_PERSON_MOTTAKER.FK_DIM_SIVILSTATUS,
        DIM_PERSON_MOTTAKER.FK_DIM_LAND_FODT,
        DIM_PERSON_MOTTAKER.FK_DIM_GEOGRAFI_BOSTED,
        DIM_PERSON_MOTTAKER.FK_DIM_LAND_STATSBORGERSKAP,
        FAM_BT_PERIODE.PK_DIM_TID,
        DIM_ALDER.PK_DIM_ALDER
      FROM
        DVH_FAM_BT.FAM_BT_UR_UTBETALING UR
        LEFT JOIN DVH_FAM_BT.FAM_BT_FAGSAK FAGSAK
        ON FAGSAK.BEHANDLINGS_ID = UR.HENVISNING
        LEFT OUTER JOIN DT_KODEVERK.DIM_TID FAM_BT_PERIODE
        ON TO_CHAR(UR.POSTERINGSDATO,
        'YYYYMM') = FAM_BT_PERIODE.AAR_MAANED
        AND FAM_BT_PERIODE.DIM_NIVAA = 3
        AND FAM_BT_PERIODE.GYLDIG_FLAGG = 1
        LEFT OUTER JOIN DT_PERSON.DIM_PERSON DIM_PERSON_MOTTAKER
        ON DIM_PERSON_MOTTAKER.FK_PERSON1 = UR.GJELDER_MOTTAKER
        AND DIM_PERSON_MOTTAKER.GYLDIG_FRA_DATO <= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
        AND DIM_PERSON_MOTTAKER.GYLDIG_TIL_DATO >= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
        LEFT OUTER JOIN DT_KODEVERK.DIM_GEOGRAFI DIM_GEOGRAFI
        ON DIM_PERSON_MOTTAKER.FK_DIM_GEOGRAFI_BOSTED = DIM_GEOGRAFI.PK_DIM_GEOGRAFI
 --AND dim_geografi.gyldig_fra_dato< fam_bt_periode.siste_dato_i_perioden
 --AND dim_geografi.gyldig_til_dato> fam_bt_periode.siste_dato_i_perioden
        LEFT OUTER JOIN DT_KODEVERK.DIM_KJONN DIM_KJONN_MOTTAKER
        ON DIM_PERSON_MOTTAKER.FK_DIM_KJONN=DIM_KJONN_MOTTAKER.PK_DIM_KJONN
 --AND dim_kjonn_mottaker.gyldig_fra_dato< fam_bt_periode.siste_dato_i_perioden
 --AND dim_kjonn_mottaker.gyldig_til_dato> fam_bt_periode.siste_dato_i_perioden
        LEFT OUTER JOIN DT_KODEVERK.DIM_ALDER
        ON FLOOR(MONTHS_BETWEEN(SYSDATE,
        DIM_PERSON_MOTTAKER.FODT_DATO)/12) = DIM_ALDER.ALDER
        AND DIM_ALDER.GYLDIG_FRA_DATO <= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
        AND DIM_ALDER.GYLDIG_TIL_DATO >= FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN
      WHERE
        HOVEDKONTONR = 800
        AND TO_CHAR(POSTERINGSDATO, 'YYYYMM') = V_IN_PERIOD
        AND LENGTH(DELYTELSE_ID) < 14
      GROUP BY
        GJELDER_MOTTAKER, TO_CHAR(POSTERINGSDATO, 'YYYYMM'), DIM_KJONN_MOTTAKER.KJONN_KODE, FAM_BT_PERIODE.SISTE_DATO_I_PERIODEN, EXTRACT( YEAR FROM DIM_PERSON_MOTTAKER.FODT_DATO), TO_CHAR(DIM_PERSON_MOTTAKER.FODT_DATO, 'MM'),
 -- nvl(DIM_GEOGRAFI.KOMMUNE_NR, DIM_PERSON_MOTTAKER.BOSTED_KOMMUNE_NR)
        DIM_PERSON_MOTTAKER.TKNR, DIM_PERSON_MOTTAKER.BOSTED_KOMMUNE_NR, DIM_GEOGRAFI.KOMMUNE_NR, DIM_GEOGRAFI.BYDEL_KOMMUNE_NR, FAGSAK.FAGSAK_ID, DIM_PERSON_MOTTAKER.PK_DIM_PERSON, DIM_PERSON_MOTTAKER.FK_DIM_KJONN, DIM_PERSON_MOTTAKER.FK_DIM_SIVILSTATUS, DIM_PERSON_MOTTAKER.FK_DIM_LAND_FODT, DIM_PERSON_MOTTAKER.FK_DIM_GEOGRAFI_BOSTED, DIM_PERSON_MOTTAKER.FK_DIM_LAND_STATSBORGERSKAP, FAM_BT_PERIODE.PK_DIM_TID, DIM_ALDER.PK_DIM_ALDER;
  BEGIN
 -- Slett mottakere dvh_fam_fp.fam_bt_mottaker_hist for aktuell periode
    BEGIN
      DELETE FROM DVH_FAM_BT.FAM_BT_MOTTAKER
      WHERE
        KILDE = V_KILDE
        AND STAT_AARMND= P_IN_PERIOD;
      COMMIT;
    EXCEPTION
      WHEN OTHERS THEN
        L_ERROR_MELDING := SQLCODE
                           || ' '
                           || SQLERRM;
        INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
          MIN_LASTET_DATO,
          ID,
          ERROR_MSG,
          OPPRETTET_TID,
          KILDE
        ) VALUES(
          NULL,
          NULL,
          L_ERROR_MELDING,
          V_STOREDATE,
          'FAM_BT_MOTTAKER_INSERT1'
        );
        COMMIT;
        P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                  || L_ERROR_MELDING, 1, 1000);
    END;
    FOR REC_MOTTAKER IN CUR_MOTTAKER(P_IN_PERIOD) LOOP
 --Finn Fk_person1 for yngste og eldste barn, antall barn
      V_FK_PERSON1_YB := NULL;
      V_FK_PERSON1_EB := NULL;
      V_ANT_BARN := 0;
 --dbms_output.put_line('Antall barn'||sysdate);--Test!!!
      BEGIN
        SELECT
          SUBSTR(MIN(FODSEL_AAR_BARN
                     ||FODSEL_MND_BARN
                     ||'-'
                     ||FKB_PERSON1), 8, 40),
          SUBSTR(MAX(FODSEL_AAR_BARN
                     ||FODSEL_MND_BARN
                     ||'-'
                     ||FKB_PERSON1), 8),
          COUNT(DISTINCT FKB_PERSON1) INTO V_FK_PERSON1_EB,
          V_FK_PERSON1_YB,
          V_ANT_BARN
        FROM
          DVH_FAM_BT.FAM_BT_BARN
        WHERE
          FK_PERSON1 = REC_MOTTAKER.GJELDER_MOTTAKER
          AND KILDE = V_KILDE
          AND STAT_AARMND= P_IN_PERIOD;
 --dbms_output.put_line(rec_mottaker.gjelder_mottaker||'   '||v_fk_person1_yb);--Test
      EXCEPTION
        WHEN OTHERS THEN
          V_FK_PERSON1_EB := NULL;
          V_FK_PERSON1_YB := NULL;
          V_ANT_BARN := 0;
          L_ERROR_MELDING := SQLCODE
                             || ' '
                             || SQLERRM;
          INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
            MIN_LASTET_DATO,
            ID,
            ERROR_MSG,
            OPPRETTET_TID,
            KILDE
          ) VALUES(
            NULL,
            REC_MOTTAKER.GJELDER_MOTTAKER,
            L_ERROR_MELDING,
            V_STOREDATE,
            'FAM_BT_MOTTAKER_INSERT2'
          );
          L_COMMIT := L_COMMIT + 1;
          P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                    || L_ERROR_MELDING, 1, 1000);
      END;
 -- Kalkulere beløp utbetalt hittil i år, sum for tidligere perioder inneværende år + denne periode
 -- Kalkulere beløp etterbetalt hittil i år, sum for tidligere perioder inneværende år + denne periode
      V_BELOPHIE := 0;
      V_BELOPHIT := 0;
 --IF (v_ant_barn > 0) THEN
 --dbms_output.put_line('Beløp'||sysdate);--Test!!!
      BEGIN
        SELECT
          NVL(SUM(MOT.BELOP), 0) + NVL(REC_MOTTAKER.BELOP, 0),
 --nvl(MAX(mot.belophie),0) + nvl(rec_mottaker.belope,0)
          NVL(SUM(MOT.BELOPE), 0) + NVL(REC_MOTTAKER.BELOPE, 0) INTO V_BELOPHIT,
          V_BELOPHIE
        FROM
          DVH_FAM_BT.FAM_BT_MOTTAKER MOT
        WHERE
          MOT.STAT_AARMND >= V_AAR_START
          AND MOT.FK_PERSON1 = REC_MOTTAKER.GJELDER_MOTTAKER
          AND MOT.STAT_AARMND < P_IN_PERIOD;
      EXCEPTION
        WHEN OTHERS THEN
          V_BELOPHIT := 0;
          V_BELOPHIE := 0;
          L_ERROR_MELDING := SQLCODE
                             || ' '
                             || SQLERRM;
          INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
            MIN_LASTET_DATO,
            ID,
            ERROR_MSG,
            OPPRETTET_TID,
            KILDE
          ) VALUES(
            NULL,
            REC_MOTTAKER.GJELDER_MOTTAKER,
            L_ERROR_MELDING,
            V_STOREDATE,
            'FAM_BT_MOTTAKER_INSERT3'
          );
          L_COMMIT := L_COMMIT + 1;
          P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                    || L_ERROR_MELDING, 1, 1000);
      END;
 --END IF;
      V_ALDERYB := 0;
      V_SSB_KODE := NULL;
 -- Finn yngste barns alder, bostedsland
      IF (V_FK_PERSON1_YB IS NOT NULL) THEN
 --dbms_output.put_line('Yngste barn'||sysdate);--Test!!!
        BEGIN
          SELECT
            TRUNC(MONTHS_BETWEEN(REC_MOTTAKER.SISTE_DATO_I_PERIODEN, DIM_PERSON_BARN.FODT_DATO)/12),
            DIM_LAND.LAND_SSB_KODE INTO V_ALDERYB,
            V_SSB_KODE
          FROM
            DT_PERSON.DIM_PERSON DIM_PERSON_BARN
            LEFT OUTER JOIN DT_KODEVERK.DIM_LAND DIM_LAND
            ON DIM_PERSON_BARN.FK_DIM_LAND_BOSTED = DIM_LAND.PK_DIM_LAND
            AND DIM_LAND.GYLDIG_FRA_DATO <= REC_MOTTAKER.SISTE_DATO_I_PERIODEN
            AND DIM_LAND.GYLDIG_TIL_DATO >= REC_MOTTAKER.SISTE_DATO_I_PERIODEN
          WHERE
            DIM_PERSON_BARN.FK_PERSON1 = V_FK_PERSON1_YB
            AND DIM_PERSON_BARN.GYLDIG_FRA_DATO <= REC_MOTTAKER.SISTE_DATO_I_PERIODEN
            AND DIM_PERSON_BARN.GYLDIG_TIL_DATO >= REC_MOTTAKER.SISTE_DATO_I_PERIODEN;
        EXCEPTION
          WHEN OTHERS THEN
            V_ALDERYB := 0;
            V_SSB_KODE := NULL;
            L_ERROR_MELDING := SQLCODE
                               || ' '
                               || SQLERRM;
            INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
              MIN_LASTET_DATO,
              ID,
              ERROR_MSG,
              OPPRETTET_TID,
              KILDE
            ) VALUES(
              NULL,
              REC_MOTTAKER.GJELDER_MOTTAKER,
              L_ERROR_MELDING,
              V_STOREDATE,
              'FAM_BT_MOTTAKER_INSERT4'
            );
            L_COMMIT := L_COMMIT + 1;
            P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                      || L_ERROR_MELDING, 1, 1000);
        END;
      END IF;
 --dbms_output.put_line('Insert'||sysdate);--Test!!!
      BEGIN
        INSERT INTO DVH_FAM_BT.FAM_BT_MOTTAKER (
          FK_PERSON1,
          BELOP,
          BELOPHIE,
          EOYBLAND,
          ANTBARN,
          YBARN,
          KJONN,
          FODSEL_AAR,
          FODSEL_MND,
          BELOPHIT,
          TKNR,
          BOSTED_KOMMUNE_NR,
          BOSTED_BYDEL_KOMMUNE_NR,
          STAT_AARMND,
          KILDE,
          LASTET_DATO,
          FAGSAK_ID,
          FKBY_PERSON1,
          BELOPE,
          FK_DIM_PERSON,
          FK_DIM_ALDER,
          FK_DIM_KJONN,
          FK_DIM_TID_MND,
          FK_DIM_SIVILSTATUS,
          FK_DIM_LAND_FODT,
          FK_DIM_GEOGRAFI_BOSTED,
          FK_DIM_LAND_STATSBORGERSKAP
        ) VALUES (
          REC_MOTTAKER.GJELDER_MOTTAKER,
          REC_MOTTAKER.BELOP,
          V_BELOPHIE,
          SUBSTR(V_SSB_KODE, 1, 3),
          V_ANT_BARN,
          V_ALDERYB,
          REC_MOTTAKER.KJONN,
          REC_MOTTAKER.FODSEL_AAR,
          REC_MOTTAKER.FODSEL_MND,
          V_BELOPHIT,
 --substr(rec_mottaker.kommune_nr,1,4),
          SUBSTR(REC_MOTTAKER.TKNR, 1, 4),
          SUBSTR(REC_MOTTAKER.BOSTED_KOMMUNE_NR, 1, 10),
          SUBSTR(REC_MOTTAKER.BYDEL_KOMMUNE_NR, 1, 11),
          P_IN_PERIOD,
          V_KILDE,
          V_STOREDATE,
          REC_MOTTAKER.FAGSAK_ID,
          V_FK_PERSON1_YB,
          REC_MOTTAKER.BELOPE,
          REC_MOTTAKER.PK_DIM_PERSON,
          REC_MOTTAKER.PK_DIM_ALDER,
          REC_MOTTAKER.FK_DIM_KJONN,
          REC_MOTTAKER.PK_DIM_TID,
          REC_MOTTAKER.FK_DIM_SIVILSTATUS,
          REC_MOTTAKER.FK_DIM_LAND_FODT,
          REC_MOTTAKER.FK_DIM_GEOGRAFI_BOSTED,
          REC_MOTTAKER.FK_DIM_LAND_STATSBORGERSKAP
        );
        L_COMMIT := L_COMMIT + 1;
      EXCEPTION
        WHEN OTHERS THEN
          L_ERROR_MELDING := SQLCODE
                             || ' '
                             || SQLERRM;
          INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
            MIN_LASTET_DATO,
            ID,
            ERROR_MSG,
            OPPRETTET_TID,
            KILDE
          ) VALUES(
            NULL,
            REC_MOTTAKER.GJELDER_MOTTAKER,
            L_ERROR_MELDING,
            V_STOREDATE,
            'FAM_BT_MOTTAKER_INSERT5'
          );
          L_COMMIT := L_COMMIT + 1;
          P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                    || L_ERROR_MELDING, 1, 1000);
      END;
 --dbms_output.put_line('Commit'||sysdate);--Test!!!
      IF L_COMMIT >= 100000 THEN
        COMMIT;
        L_COMMIT := 0;
      END IF;
    END LOOP;
    COMMIT;
    IF L_ERROR_MELDING IS NOT NULL THEN
      INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
        ID,
        ERROR_MSG,
        OPPRETTET_TID,
        KILDE
      ) VALUES(
        NULL,
        L_ERROR_MELDING,
        V_STOREDATE,
        'FAM_BT_MOTTAKER_INSERT6'
      );
      COMMIT;
      P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                || L_ERROR_MELDING, 1, 1000);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      L_ERROR_MELDING := SQLCODE
                         || ' '
                         || SQLERRM;
      INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
        MIN_LASTET_DATO,
        ID,
        ERROR_MSG,
        OPPRETTET_TID,
        KILDE
      ) VALUES(
        NULL,
        NULL,
        L_ERROR_MELDING,
        V_STOREDATE,
        'FAM_BT_MOTTAKER_INSERT7'
      );
      COMMIT;
      P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                || L_ERROR_MELDING, 1, 1000);
  END FAM_BT_MOTTAKER_INSERT_BCK;

  PROCEDURE FAM_BT_SLETT_OFFSET(
    P_IN_OFFSET IN VARCHAR2,
    P_ERROR_MELDING OUT VARCHAR2
  ) AS
    V_TEMP_DML      VARCHAR2(4000);
    L_ERROR_MELDING VARCHAR2(1000);
  BEGIN
    V_TEMP_DML := 'CREATE GLOBAL TEMPORARY TABLE TEMP_TBL_SLETT
                   ON COMMIT PRESERVE ROWS
                   AS
                   SELECT DISTINCT det.pk_bt_utbet_det,
                          utb.pk_bt_utbetaling,
                          fag.pk_bt_fagsak,
                          fag.fagsak_id,
                          pers_utb.pk_bt_person pk_bt_person_utb,
                          pers_mot.pk_bt_person pk_bt_person_mot,
                          meta.pk_bt_meta_data,
                          nvl(statsborg.pk_statsborgerskap,-100) pk_statsborgerskap
                   FROM dvh_fam_bt.fam_bt_meta_data meta

                   JOIN dvh_fam_bt.fam_bt_fagsak fag ON
                   meta.pk_bt_meta_data = fag.fk_bt_meta_data

                   JOIN dvh_fam_bt.fam_bt_person pers_mot ON
                   fag.fk_bt_person = pers_mot.pk_bt_person

                   LEFT JOIN dvh_fam_bt.fam_bt_utbetaling utb ON
                   utb.fk_bt_fagsak = fag.pk_bt_fagsak

                   LEFT JOIN dvh_fam_bt.fam_bt_utbet_det det ON
                   det.fk_bt_utbetaling = utb.pk_bt_utbetaling

                   LEFT JOIN dvh_fam_bt.fam_bt_person pers_utb ON
                   det.fk_bt_person = pers_utb.pk_bt_person

                   LEFT JOIN dvh_fam_bt.fam_bt_statsborgerskap statsborg ON
                   (statsborg.fk_bt_person = pers_utb.pk_bt_person OR statsborg.fk_bt_person = pers_mot.pk_bt_person)
                   WHERE meta.kafka_offset = '
                  || P_IN_OFFSET;
 --dbms_output.put_line(v_temp_dml);
    EXECUTE IMMEDIATE V_TEMP_DML;
    BEGIN
      V_TEMP_DML := '
      DELETE FROM dvh_fam_bt.fam_bt_statsborgerskap
      WHERE pk_statsborgerskap IN (SELECT DISTINCT pk_statsborgerskap FROM TEMP_TBL_SLETT)';
 --dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE V_TEMP_DML;
      V_TEMP_DML := '
      DELETE FROM dvh_fam_bt.fam_bt_utbet_det
      WHERE pk_bt_utbet_det IN (SELECT DISTINCT pk_bt_utbet_det FROM TEMP_TBL_SLETT)';
 --dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE V_TEMP_DML;
      V_TEMP_DML := '
      DELETE FROM dvh_fam_bt.fam_bt_utbetaling
      WHERE pk_bt_utbetaling IN (SELECT DISTINCT pk_bt_utbetaling FROM TEMP_TBL_SLETT)';
 --dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE V_TEMP_DML;
      V_TEMP_DML := '
      DELETE FROM dvh_fam_bt.fam_bt_fagsak
      WHERE pk_bt_fagsak IN (SELECT DISTINCT pk_bt_fagsak FROM TEMP_TBL_SLETT)';
 --dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE V_TEMP_DML;
      V_TEMP_DML := '
      DELETE FROM dvh_fam_bt.fam_bt_person
      WHERE pk_bt_person IN (SELECT DISTINCT pk_bt_person_utb FROM TEMP_TBL_SLETT)';
 --dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE V_TEMP_DML;
      V_TEMP_DML := '
      DELETE FROM dvh_fam_bt.fam_bt_person
      WHERE pk_bt_person IN (SELECT DISTINCT pk_bt_person_mot FROM TEMP_TBL_SLETT)';
 --dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE V_TEMP_DML;
      COMMIT; --Commit på alle
    EXCEPTION
      WHEN OTHERS THEN
        L_ERROR_MELDING := SQLCODE
                           || ' '
                           || SQLERRM;
        ROLLBACK;
        INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
          MIN_LASTET_DATO,
          ID,
          ERROR_MSG,
          OPPRETTET_TID,
          KILDE
        ) VALUES(
          NULL,
          NULL,
          L_ERROR_MELDING,
          SYSDATE,
          'FAM_BT_SLETT_OFFSET1'
        );
        COMMIT;
        P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                  || L_ERROR_MELDING, 1, 1000);
    END;
 --ROLLBACK;--Test
    V_TEMP_DML := 'TRUNCATE TABLE TEMP_TBL_SLETT';
 --dbms_output.put_line(v_temp_dml);
    EXECUTE IMMEDIATE V_TEMP_DML;
    V_TEMP_DML := 'DROP TABLE TEMP_TBL_SLETT';
 --dbms_output.put_line(v_temp_dml);
    EXECUTE IMMEDIATE V_TEMP_DML;
 --COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      L_ERROR_MELDING := SQLCODE
                         || ' '
                         || SQLERRM;
      INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
        MIN_LASTET_DATO,
        ID,
        ERROR_MSG,
        OPPRETTET_TID,
        KILDE
      ) VALUES(
        NULL,
        NULL,
        L_ERROR_MELDING,
        SYSDATE,
        'FAM_BT_SLETT_OFFSET2'
      );
      COMMIT;
      P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                || L_ERROR_MELDING, 1, 1000);
  END FAM_BT_SLETT_OFFSET;

  PROCEDURE FAM_BT2_SLETT_OFFSET(
    P_IN_OFFSET IN VARCHAR2,
    P_ERROR_MELDING OUT VARCHAR2
  ) AS
    V_TEMP_DML      VARCHAR2(4000);
    L_ERROR_MELDING VARCHAR2(1000);
  BEGIN
    V_TEMP_DML := 'CREATE GLOBAL TEMPORARY TABLE TEMP_TBL_SLETT
                   ON COMMIT PRESERVE ROWS
                   AS
                   SELECT DISTINCT det.pk_bt_utbet_det,
                          utb.pk_bt_utbetaling,
                          kBarn.pk_bt_kompetanse_barn,
                          kPerioder.pk_bt_kompetanse_perioder,
                          fag.pk_bt_fagsak,
                          fag.fagsak_id,
                          pers_utb.pk_bt_person pk_bt_person_utb,
                          pers_mot.pk_bt_person pk_bt_person_mot,
                          meta.pk_bt_meta_data,
                          nvl(statsborg.pk_statsborgerskap,-100) pk_statsborgerskap
                   FROM dvh_fam_bt.fam_bt_meta_data meta

                   JOIN dvh_fam_bt.fam_bt_fagsak fag ON
                   meta.pk_bt_meta_data = fag.fk_bt_meta_data

                   JOIN dvh_fam_bt.fam_bt_person pers_mot ON
                   fag.fk_bt_person = pers_mot.pk_bt_person

                   LEFT JOIN dvh_fam_bt.fam_bt_utbetaling utb ON
                   utb.fk_bt_fagsak = fag.pk_bt_fagsak

                   LEFT JOIN dvh_fam_bt.fam_bt_utbet_det det ON
                   det.fk_bt_utbetaling = utb.pk_bt_utbetaling

                   LEFT JOIN dvh_fam_bt.fam_bt_kompetanse_perioder kPerioder ON
                   kPerioder.fk_bt_fagsak = fag.pk_bt_fagsak

                   LEFT JOIN dvh_fam_bt.fam_bt_kompetanse_barn kBarn ON
                   kBarn.fk_bt_kompetanse_perioder = kPerioder.pk_bt_kompetanse_perioder

                   LEFT JOIN dvh_fam_bt.fam_bt_person pers_utb ON
                   det.fk_bt_person = pers_utb.pk_bt_person

                   LEFT JOIN dvh_fam_bt.fam_bt_statsborgerskap statsborg ON
                   (statsborg.fk_bt_person = pers_utb.pk_bt_person OR statsborg.fk_bt_person = pers_mot.pk_bt_person)
                   where meta.kafka_offset = '
                  || P_IN_OFFSET
                  || ' AND meta.kafka_topic = ''teamfamilie.aapen-barnetrygd-vedtak-v2'' ';
 --dbms_output.put_line(v_temp_dml);
    EXECUTE IMMEDIATE V_TEMP_DML;
    BEGIN
      V_TEMP_DML := '
      DELETE FROM dvh_fam_bt.fam_bt_statsborgerskap
      WHERE pk_statsborgerskap IN (SELECT DISTINCT pk_statsborgerskap FROM TEMP_TBL_SLETT)';
 --dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE V_TEMP_DML;
      V_TEMP_DML := '
      DELETE FROM dvh_fam_bt.fam_bt_utbet_det
      WHERE pk_bt_utbet_det IN (SELECT DISTINCT pk_bt_utbet_det FROM TEMP_TBL_SLETT)';
 --dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE V_TEMP_DML;
      V_TEMP_DML := '
      DELETE FROM dvh_fam_bt.fam_bt_utbetaling
      WHERE pk_bt_utbetaling IN (SELECT DISTINCT pk_bt_utbetaling FROM TEMP_TBL_SLETT)';
 --dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE V_TEMP_DML;
      V_TEMP_DML := '
      DELETE FROM dvh_fam_bt.fam_bt_kompetanse_barn
      WHERE pk_bt_kompetanse_barn IN (SELECT DISTINCT pk_bt_kompetanse_barn FROM TEMP_TBL_SLETT)';
 --dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE V_TEMP_DML;
      V_TEMP_DML := '
      DELETE FROM dvh_fam_bt.fam_bt_kompetanse_perioder
      WHERE pk_bt_kompetanse_perioder IN (SELECT DISTINCT pk_bt_kompetanse_perioder FROM TEMP_TBL_SLETT)';
 --dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE V_TEMP_DML;
      V_TEMP_DML := '
      DELETE FROM dvh_fam_bt.fam_bt_fagsak
      WHERE pk_bt_fagsak IN (SELECT DISTINCT pk_bt_fagsak FROM TEMP_TBL_SLETT)';
      DBMS_OUTPUT.PUT_LINE(V_TEMP_DML);
      EXECUTE IMMEDIATE V_TEMP_DML;
      V_TEMP_DML := '
      DELETE FROM dvh_fam_bt.fam_bt_person
      WHERE pk_bt_person IN (SELECT DISTINCT pk_bt_person_utb FROM TEMP_TBL_SLETT)';
 --dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE V_TEMP_DML;
      V_TEMP_DML := '
      DELETE FROM dvh_fam_bt.fam_bt_person
      WHERE pk_bt_person IN (SELECT DISTINCT pk_bt_person_mot FROM TEMP_TBL_SLETT)';
 --dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE V_TEMP_DML;
      COMMIT; --Commit på alle
    EXCEPTION
      WHEN OTHERS THEN
        L_ERROR_MELDING := SQLCODE
                           || ' '
                           || SQLERRM;
        ROLLBACK;
        INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
          MIN_LASTET_DATO,
          ID,
          ERROR_MSG,
          OPPRETTET_TID,
          KILDE
        ) VALUES(
          NULL,
          NULL,
          L_ERROR_MELDING,
          SYSDATE,
          'FAM_BT_SLETT_OFFSET1'
        );
        COMMIT;
        P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                  || L_ERROR_MELDING, 1, 1000);
    END;
 --ROLLBACK;--Test
    V_TEMP_DML := 'TRUNCATE TABLE TEMP_TBL_SLETT';
 --dbms_output.put_line(v_temp_dml);
    EXECUTE IMMEDIATE V_TEMP_DML;
    V_TEMP_DML := 'DROP TABLE TEMP_TBL_SLETT';
 --dbms_output.put_line(v_temp_dml);
    EXECUTE IMMEDIATE V_TEMP_DML;
 --COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      L_ERROR_MELDING := SQLCODE
                         || ' '
                         || SQLERRM;
      INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
        MIN_LASTET_DATO,
        ID,
        ERROR_MSG,
        OPPRETTET_TID,
        KILDE
      ) VALUES(
        NULL,
        NULL,
        L_ERROR_MELDING,
        SYSDATE,
        'FAM_BT_SLETT_OFFSET2'
      );
      COMMIT;
      P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                || L_ERROR_MELDING, 1, 1000);
  END FAM_BT2_SLETT_OFFSET;

  PROCEDURE FAM_BT_UTPAKKING_OFFSET(
    P_IN_OFFSET IN NUMBER,
    P_ERROR_MELDING OUT VARCHAR2
  ) AS
 --v_person_ident VARCHAR2(20);
    V_FK_PERSON1_SOKER NUMBER;
    V_FK_PERSON1_UTB   NUMBER;
    V_PK_BT_UTBETALING NUMBER;
    V_PK_BT_UTBET_DET  NUMBER;
    V_PK_PERSON_SOKER  NUMBER;
    V_PK_PERSON_UTB    NUMBER;
    V_PK_FAGSAK        NUMBER;
    V_STOREDATE        DATE := SYSDATE;
    L_ERROR_MELDING    VARCHAR2(4000);
    CURSOR CUR_BT_FAGSAK(P_OFFSET NUMBER) IS
      WITH JDATA AS (
        SELECT
          KAFKA_OFFSET,
          MELDING         AS DOC,
          PK_BT_META_DATA
        FROM
          DVH_FAM_BT.FAM_BT_META_DATA
        WHERE
          KAFKA_OFFSET = P_OFFSET
 --WHERE kafka_offset=110--Test!!!
      )
      SELECT
        T.BEHANDLING_OPPRINNELSE,
        T.BEHANDLING_TYPE,
        T.FAGSAK_ID,
        T.BEHANDLINGS_ID,
        CAST(TO_TIMESTAMP_TZ(T.TIDSPUNKT_VEDTAK, 'FXYYYY-MM-DD"T"HH24:MI:SS.FXFF3TZH:TZM') AT TIME ZONE 'Europe/Belgrade' AS TIMESTAMP) AS TIDSPUNKT_VEDTAK,
        CASE
          WHEN T.ENSLIG_FORSØRGER = 'false' THEN
            '0'
          ELSE
            '1'
        END AS                                                                    ENSLIG_FORSØRGER,
        T.KATEGORI,
        T.UNDERKATEGORI,
        'BT'                                                                                                                            AS KILDESYSTEM,
        CURRENT_TIMESTAMP                                                                                                               AS LASTET_DATO,
        T.FUNKSJONELL_ID,
        T.BEHANDLINGÅRSAK,
        T.PERSON_IDENT,
        T.ROLLE,
        T.STATSBORGERSKAP,
        T.ANNENPART_BOSTEDSLAND,
        T.ANNENPART_PERSONIDENT,
        T.ANNENPART_STATSBORGERSKAP,
        T.BOSTEDSLAND,
        T.DELINGSPROSENT_OMSORG,
        T.DELINGSPROSENT_YTELSE,
        T.PRIMÆRLAND,
        T.SEKUNDÆRLAND,
        PK_BT_META_DATA,
        KAFKA_OFFSET
      FROM
        JDATA,
        JSON_TABLE ( DOC,
        '$' COLUMNS ( BEHANDLING_OPPRINNELSE VARCHAR2 PATH '$.behandlingOpprinnelse',
        BEHANDLING_TYPE VARCHAR2 PATH '$.behandlingType',
        FAGSAK_ID VARCHAR2 PATH '$.fagsakId',
        BEHANDLINGS_ID VARCHAR2 PATH '$.behandlingsId',
        TIDSPUNKT_VEDTAK VARCHAR2 PATH '$.tidspunktVedtak',
        ENSLIG_FORSØRGER VARCHAR2 PATH '$.ensligForsørger',
        KATEGORI VARCHAR2 PATH '$.kategori',
        UNDERKATEGORI VARCHAR2 PATH '$.underkategori',
        FUNKSJONELL_ID VARCHAR2 PATH '$.funksjonellId',
        PERSON_IDENT VARCHAR2 PATH '$.person[*].personIdent',
        ROLLE VARCHAR2 PATH '$.person[*].rolle',
        STATSBORGERSKAP VARCHAR2 PATH '$.person[*].statsborgerskap[*]',
        ANNENPART_BOSTEDSLAND VARCHAR2 PATH '$.person[*].annenpartBostedsland',
        ANNENPART_PERSONIDENT VARCHAR2 PATH '$.person[*].annenpartPersonident',
        ANNENPART_STATSBORGERSKAP VARCHAR2 PATH '$.person[*].annenpartStatsborgerskap',
        BOSTEDSLAND VARCHAR2 PATH '$.person[*].bostedsland',
        DELINGSPROSENT_OMSORG VARCHAR2 PATH '$.person[*].delingsprosentOmsorg',
        DELINGSPROSENT_YTELSE VARCHAR2 PATH '$.person[*].delingsprosentYtelse',
        PRIMÆRLAND VARCHAR2 PATH '$.person[*].primærland',
        SEKUNDÆRLAND VARCHAR2 PATH '$.person[*].sekundærland',
        BEHANDLINGÅRSAK VARCHAR2 PATH '$.behandlingÅrsak' ) ) T     ;
    CURSOR CUR_BT_UTBETALING(P_OFFSET NUMBER) IS
      WITH JDATA AS (
        SELECT
          KAFKA_OFFSET,
          MELDING      AS DOC
        FROM
          DVH_FAM_BT.FAM_BT_META_DATA
        WHERE
          KAFKA_OFFSET = P_OFFSET
 --WHERE kafka_offset=110--Test!!!
      )
      SELECT
        T.UTBETALT_PER_MND,
        TO_DATE(T.STØNAD_FOM, 'YYYY-MM-DD') AS STØNAD_FOM,
        TO_DATE(T.STØNAD_TOM, 'YYYY-MM-DD') AS STØNAD_TOM,
        T.HJEMMEL,
        CURRENT_TIMESTAMP                   AS LASTET_DATO,
        BEHANDLINGS_ID,
        KAFKA_OFFSET
      FROM
        JDATA,
        JSON_TABLE ( DOC,
        '$' COLUMNS ( BEHANDLINGS_ID VARCHAR2 PATH '$.behandlingsId',
        NESTED PATH '$.utbetalingsperioder[*]' COLUMNS ( UTBETALT_PER_MND VARCHAR2 PATH '$.utbetaltPerMnd',
        STØNAD_FOM VARCHAR2 PATH '$.stønadFom',
        STØNAD_TOM VARCHAR2 PATH '$.stønadTom',
        HJEMMEL VARCHAR2 PATH '$.hjemmel' )) ) T     ;
    CURSOR CUR_BT_UTBETALINGS_DETALJER(P_OFFSET NUMBER, P_FOM DATE, P_TOM DATE) IS
      WITH JDATA AS (
        SELECT
          KAFKA_OFFSET,
          MELDING      AS DOC
        FROM
          DVH_FAM_BT.FAM_BT_META_DATA
        WHERE
          KAFKA_OFFSET = P_OFFSET
 --WHERE kafka_offset = 110--Test!!!
      )
      SELECT --1 AS pk_bt_utbet_det
 --,
        S.HJEMMEL,
        S.UTBETALTPERMND,
        S.STØNADFOM,
        S.STØNADTOM,
        S.KLASSEKODE,
        S.DELYTELSE_ID,
        S.UTBETALT_PR_MND,
        S.STONAD_FOM,
        S.PERSONIDENT,
        S.ROLLE,
        S.STATSBORGERSKAP,
        S.BOSTEDSLAND,
        S.PRIMÆRLAND,
        S.SEKUNDÆRLAND,
        S.DELINGSPROSENTOMSORG     DELINGSPROSENT_OMSORG,
        S.DELINGSPROSENTYTELSE     DELINGSPROSENT_YTELSE,
        S.ANNENPARTPERSONIDENT     ANNENPART_PERSONIDENT,
        S.ANNENPARTSTATSBORGERSKAP ANNENPART_STATSBORGERSKAP,
        S.ANNENPARTBOSTEDSLAND     ANNENPART_BOSTEDSLAND,
        CURRENT_TIMESTAMP          AS LASTET_DATO,
        BEHANDLINGS_ID,
        KAFKA_OFFSET
      FROM
        JDATA,
        JSON_TABLE ( DOC,
        '$' COLUMNS ( BEHANDLINGS_ID VARCHAR2 PATH '$.behandlingsId',
        NESTED PATH '$.utbetalingsperioder[*]' COLUMNS ( HJEMMEL VARCHAR2 PATH '$.hjemmel',
        UTBETALTPERMND VARCHAR2 PATH '$.utbetaltPerMnd',
        STØNADFOM VARCHAR2 PATH '$.stønadFom',
        STØNADTOM VARCHAR2 PATH '$.stønadTom',
        JOINED_ON VARCHAR2 PATH '$.joined_on',
        NESTED PATH '$.utbetalingsDetaljer[*]' COLUMNS ( KLASSEKODE VARCHAR2 PATH '$.klassekode',
        DELYTELSE_ID VARCHAR2 PATH '$.delytelseId',
        UTBETALT_PR_MND VARCHAR2 PATH '$..utbetaltPrMnd',
        STONAD_FOM VARCHAR2 PATH '$.stonad_fom',
        PERSONIDENT VARCHAR2 PATH '$.person[*].personIdent',
        ROLLE VARCHAR2 PATH '$.person[*].rolle',
        STATSBORGERSKAP VARCHAR2 PATH '$.person[*].statsborgerskap[*]',
        BOSTEDSLAND VARCHAR2 PATH '$.person[*].bostedsland',
        PRIMÆRLAND VARCHAR2 PATH '$.person[*].primærland',
        SEKUNDÆRLAND VARCHAR2 PATH '$.person[*].sekundærland',
        DELINGSPROSENTOMSORG VARCHAR2 PATH '$.person[*].delingsprosentOmsorg',
        DELINGSPROSENTYTELSE VARCHAR2 PATH '$.person[*].delingsprosentYtelse',
        ANNENPARTPERSONIDENT VARCHAR2 PATH '$.person[*].annenpartPersonident',
        ANNENPARTSTATSBORGERSKAP VARCHAR2 PATH '$.person[*].annenpartStatsborgerskap',
        ANNENPARTBOSTEDSLAND VARCHAR2 PATH '$.person[*].annenpartBostedsland' ))) ) S
      WHERE
        TO_DATE(S.STØNADFOM, 'YYYY-MM-DD') = P_FOM
        AND TO_DATE(S.STØNADTOM, 'YYYY-MM-DD') = P_TOM
 --WHERE TO_DATE(S.stønadfom,'YYYY-MM-DD') >= TO_DATE('2020-03-01','YYYY-MM-DD')--Test!!!
 --AND TO_DATE(S.stønadtom,'YYYY-MM-DD') <= TO_DATE('2032-01-31','YYYY-MM-DD')--Test!!!
     ;
  BEGIN
 -- For alle fagsaker
    FOR REC_FAG IN CUR_BT_FAGSAK(P_IN_OFFSET) LOOP
      BEGIN
 --dbms_output.put_line(rec_fag.fagsak_id);
        V_PK_PERSON_SOKER := NULL;
        V_FK_PERSON1_SOKER := -1;
        SELECT
          DVH_FAMBT_KAFKA.HIBERNATE_SEQUENCE.NEXTVAL INTO V_PK_PERSON_SOKER
        FROM
          DUAL;
 --Hent fk_person1
        BEGIN
          SELECT
            DISTINCT PERSON_67_VASKET.FK_PERSON1 AS AK_PERSON1 INTO V_FK_PERSON1_SOKER
          FROM
            DT_PERSON.DVH_PERSON_IDENT_OFF_ID_IKKE_SKJERMET PERSON_67_VASKET
          WHERE
            PERSON_67_VASKET.OFF_ID = REC_FAG.PERSON_IDENT
            AND REC_FAG.TIDSPUNKT_VEDTAK BETWEEN PERSON_67_VASKET.GYLDIG_FRA_DATO AND PERSON_67_VASKET.GYLDIG_TIL_DATO;
        EXCEPTION
          WHEN OTHERS THEN
            V_FK_PERSON1_SOKER := -1;
            L_ERROR_MELDING := SQLCODE
                               || ' '
                               || SQLERRM;
            INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
              MIN_LASTET_DATO,
              ID,
              ERROR_MSG,
              OPPRETTET_TID,
              KILDE
            ) VALUES(
              NULL,
              REC_FAG.FAGSAK_ID,
              L_ERROR_MELDING,
              V_STOREDATE,
              'FAM_BT_UTPAKKING_OFFSET1'
            );
            COMMIT;
            P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                      || L_ERROR_MELDING, 1, 1000);
        END;
 -- Insert søker into person tabell
        INSERT INTO DVH_FAM_BT.FAM_BT_PERSON (
          PK_BT_PERSON,
          ANNENPART_BOSTEDSLAND,
          ANNENPART_PERSONIDENT,
          ANNENPART_STATSBORGERSKAP,
          BOSTEDSLAND,
          DELINGSPROSENT_OMSORG,
          DELINGSPROSENT_YTELSE,
          PERSON_IDENT,
          PRIMÆRLAND,
          ROLLE,
          SEKUNDÆRLAND,
          FK_PERSON1,
          LASTET_DATO,
          BEHANDLINGS_ID,
          KAFKA_OFFSET
        ) VALUES (
          V_PK_PERSON_SOKER,
          REC_FAG.ANNENPART_BOSTEDSLAND,
          REC_FAG.ANNENPART_PERSONIDENT,
          REC_FAG.ANNENPART_STATSBORGERSKAP,
          REC_FAG.BOSTEDSLAND,
          REC_FAG.DELINGSPROSENT_OMSORG,
          REC_FAG.DELINGSPROSENT_YTELSE,
          REC_FAG.PERSON_IDENT,
          REC_FAG.PRIMÆRLAND,
          REC_FAG.ROLLE,
          REC_FAG.SEKUNDÆRLAND,
          V_FK_PERSON1_SOKER,
          REC_FAG.LASTET_DATO,
          REC_FAG.BEHANDLINGS_ID,
          REC_FAG.KAFKA_OFFSET
        );
 -- Insert into FAGSAK tabellen
        V_PK_FAGSAK := NULL;
        SELECT
          DVH_FAMBT_KAFKA.HIBERNATE_SEQUENCE.NEXTVAL INTO V_PK_FAGSAK
        FROM
          DUAL;
        INSERT INTO DVH_FAM_BT.FAM_BT_FAGSAK (
          PK_BT_FAGSAK,
          FK_BT_PERSON,
          FK_BT_META_DATA,
          BEHANDLING_OPPRINNELSE,
          BEHANDLING_TYPE,
          FAGSAK_ID,
          BEHANDLINGS_ID,
          TIDSPUNKT_VEDTAK,
          ENSLIG_FORSØRGER,
          KATEGORI,
          UNDERKATEGORI,
          KILDESYSTEM,
          LASTET_DATO,
          FUNKSJONELL_ID,
          BEHANDLING_ÅRSAK,
          KAFKA_OFFSET
        ) VALUES (
          V_PK_FAGSAK,
          V_PK_PERSON_SOKER,
          REC_FAG.PK_BT_META_DATA,
          REC_FAG.BEHANDLING_OPPRINNELSE,
          REC_FAG.BEHANDLING_TYPE,
          REC_FAG.FAGSAK_ID,
          REC_FAG.BEHANDLINGS_ID,
          REC_FAG.TIDSPUNKT_VEDTAK,
          REC_FAG.ENSLIG_FORSØRGER,
          REC_FAG.KATEGORI,
          REC_FAG.UNDERKATEGORI,
          REC_FAG.KILDESYSTEM,
          REC_FAG.LASTET_DATO,
          REC_FAG.FUNKSJONELL_ID,
          REC_FAG.BEHANDLINGÅRSAK,
          REC_FAG.KAFKA_OFFSET
        );
 -- For alle utbetalingsperioder
        FOR REC_UTBETALING IN CUR_BT_UTBETALING(P_IN_OFFSET) LOOP
          BEGIN
 --dbms_output.put_line('Hallo z:'||rec_utbetaling.stønad_fom||','||rec_utbetaling.stønad_tom);
            V_PK_BT_UTBETALING := NULL;
            SELECT
              DVH_FAMBT_KAFKA.HIBERNATE_SEQUENCE.NEXTVAL INTO V_PK_BT_UTBETALING
            FROM
              DUAL;
 --v_pk_bt_utbetaling:=rec_utbetaling.PK_BT_UTBETALING;
            INSERT INTO DVH_FAM_BT.FAM_BT_UTBETALING (
              PK_BT_UTBETALING,
              UTBETALT_PER_MND,
              STØNAD_FOM,
              STØNAD_TOM,
              HJEMMEL,
              LASTET_DATO,
              FK_BT_FAGSAK,
              BEHANDLINGS_ID,
              KAFKA_OFFSET
            ) VALUES (
              V_PK_BT_UTBETALING,
              REC_UTBETALING.UTBETALT_PER_MND,
              REC_UTBETALING.STØNAD_FOM,
              REC_UTBETALING.STØNAD_TOM,
              REC_UTBETALING.HJEMMEL,
              REC_UTBETALING.LASTET_DATO,
              V_PK_FAGSAK,
              REC_UTBETALING.BEHANDLINGS_ID,
              REC_UTBETALING.KAFKA_OFFSET
            );
 -- For alle utbetalingsdetaljer for aktuell tidsperiode
            FOR REC_UTBET_DET IN CUR_BT_UTBETALINGS_DETALJER(P_IN_OFFSET, REC_UTBETALING.STØNAD_FOM, REC_UTBETALING.STØNAD_TOM) LOOP
              BEGIN
 --dbms_output.put_line(rec_utbet_det.personident||','||rec_utbet_det.stønadfom||'YY'||to_char(rec_utbet_det.utbetalt_pr_mnd));
 --Hent fk_person1
                V_FK_PERSON1_UTB := -1;
                BEGIN
                  SELECT
                    DISTINCT PERSON_67_VASKET.FK_PERSON1 AS AK_PERSON1 INTO V_FK_PERSON1_UTB
                  FROM
                    DT_PERSON.DVH_PERSON_IDENT_OFF_ID_IKKE_SKJERMET PERSON_67_VASKET
                  WHERE
                    PERSON_67_VASKET.OFF_ID = REC_UTBET_DET.PERSONIDENT
                    AND REC_FAG.TIDSPUNKT_VEDTAK BETWEEN PERSON_67_VASKET.GYLDIG_FRA_DATO AND PERSON_67_VASKET.GYLDIG_TIL_DATO;
                EXCEPTION
                  WHEN OTHERS THEN
                    V_FK_PERSON1_UTB := -1;
                    L_ERROR_MELDING := SQLCODE
                                       || ' '
                                       || SQLERRM;
                    INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
                      MIN_LASTET_DATO,
                      ID,
                      ERROR_MSG,
                      OPPRETTET_TID,
                      KILDE
                    ) VALUES(
                      NULL,
                      REC_UTBET_DET.BEHANDLINGS_ID,
                      L_ERROR_MELDING,
                      V_STOREDATE,
                      'FAM_BT_UTPAKKING_OFFSET2'
                    );
                    COMMIT;
                    P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                              || L_ERROR_MELDING, 1, 1000);
                END;
                V_PK_PERSON_UTB := NULL;
                SELECT
                  DVH_FAMBT_KAFKA.HIBERNATE_SEQUENCE.NEXTVAL INTO V_PK_PERSON_UTB
                FROM
                  DUAL;
 --dbms_output.put_line(v_pk_person_utb);
                BEGIN
                  INSERT INTO DVH_FAM_BT.FAM_BT_PERSON (
                    PK_BT_PERSON,
                    ANNENPART_BOSTEDSLAND,
                    ANNENPART_PERSONIDENT,
                    ANNENPART_STATSBORGERSKAP,
                    BOSTEDSLAND,
                    DELINGSPROSENT_OMSORG,
                    DELINGSPROSENT_YTELSE,
                    PERSON_IDENT,
                    PRIMÆRLAND,
                    ROLLE,
                    SEKUNDÆRLAND,
                    FK_PERSON1,
                    LASTET_DATO,
                    BEHANDLINGS_ID,
                    KAFKA_OFFSET
                  ) VALUES (
 --dvh_fam_fp.hibernate_sequence_test.NEXTVAL
                    V_PK_PERSON_UTB,
                    REC_UTBET_DET.ANNENPART_BOSTEDSLAND,
                    REC_UTBET_DET.ANNENPART_PERSONIDENT,
                    REC_UTBET_DET.ANNENPART_STATSBORGERSKAP,
                    REC_UTBET_DET.BOSTEDSLAND,
                    REC_UTBET_DET.DELINGSPROSENT_OMSORG,
                    REC_UTBET_DET.DELINGSPROSENT_YTELSE,
                    REC_UTBET_DET.PERSONIDENT,
                    REC_UTBET_DET.PRIMÆRLAND,
                    REC_UTBET_DET.ROLLE,
                    REC_UTBET_DET.SEKUNDÆRLAND
 -- ,rec_utbet_det.FK_PERSON1
,
                    V_FK_PERSON1_UTB,
                    REC_UTBET_DET.LASTET_DATO,
                    REC_UTBET_DET.BEHANDLINGS_ID,
                    REC_UTBET_DET.KAFKA_OFFSET
                  );
                EXCEPTION
                  WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE(V_PK_PERSON_UTB);
                    L_ERROR_MELDING := SQLCODE
                                       || ' '
                                       || SQLERRM;
                    INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
                      MIN_LASTET_DATO,
                      ID,
                      ERROR_MSG,
                      OPPRETTET_TID,
                      KILDE
                    ) VALUES(
                      NULL,
                      REC_UTBET_DET.BEHANDLINGS_ID,
                      L_ERROR_MELDING,
                      V_STOREDATE,
                      'FAM_BT_UTPAKKING_OFFSET3'
                    );
                    COMMIT;
                    P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                              || L_ERROR_MELDING, 1, 1000);
                END;
                BEGIN
                  INSERT INTO DVH_FAM_BT.FAM_BT_UTBET_DET (
                    PK_BT_UTBET_DET,
                    KLASSEKODE,
                    DELYTELSE_ID,
                    UTBETALT_PR_MND,
                    LASTET_DATO,
                    FK_BT_PERSON,
                    FK_BT_UTBETALING,
                    BEHANDLINGS_ID,
                    KAFKA_OFFSET
                  ) VALUES (
                    DVH_FAMBT_KAFKA.HIBERNATE_SEQUENCE.NEXTVAL
 --v_pk_bt_utbet_det
,
                    REC_UTBET_DET.KLASSEKODE,
                    REC_UTBET_DET.DELYTELSE_ID,
                    REC_UTBET_DET.UTBETALT_PR_MND,
                    REC_UTBET_DET.LASTET_DATO,
                    V_PK_PERSON_UTB,
                    V_PK_BT_UTBETALING,
                    REC_UTBET_DET.BEHANDLINGS_ID,
                    REC_UTBET_DET.KAFKA_OFFSET
                  );
                EXCEPTION
                  WHEN OTHERS THEN
                    L_ERROR_MELDING := SQLCODE
                                       || ' '
                                       || SQLERRM;
                    INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
                      MIN_LASTET_DATO,
                      ID,
                      ERROR_MSG,
                      OPPRETTET_TID,
                      KILDE
                    ) VALUES(
                      NULL,
                      REC_UTBET_DET.BEHANDLINGS_ID,
                      L_ERROR_MELDING,
                      V_STOREDATE,
                      'FAM_BT_UTPAKKING_OFFSET4'
                    );
                    COMMIT;
                    P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                              || L_ERROR_MELDING, 1, 1000);
                END;
              EXCEPTION
                WHEN OTHERS THEN
                  L_ERROR_MELDING := SQLCODE
                                     || ' '
                                     || SQLERRM;
                  INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
                    MIN_LASTET_DATO,
                    ID,
                    ERROR_MSG,
                    OPPRETTET_TID,
                    KILDE
                  ) VALUES(
                    NULL,
                    REC_FAG.FAGSAK_ID,
                    L_ERROR_MELDING,
                    V_STOREDATE,
                    'FAM_BT_UTPAKKING_OFFSET5'
                  );
                  COMMIT;
                  P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                            || L_ERROR_MELDING, 1, 1000);
              END;
            END LOOP; --Utbetalingsdetaljer
          EXCEPTION
            WHEN OTHERS THEN
              L_ERROR_MELDING := SQLCODE
                                 || ' '
                                 || SQLERRM;
              INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
                MIN_LASTET_DATO,
                ID,
                ERROR_MSG,
                OPPRETTET_TID,
                KILDE
              ) VALUES(
                NULL,
                REC_FAG.FAGSAK_ID,
                L_ERROR_MELDING,
                V_STOREDATE,
                'FAM_BT_UTPAKKING_OFFSET6'
              );
              COMMIT;
              P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                        || L_ERROR_MELDING, 1, 1000);
          END;
        END LOOP; --Utbetalinger
      EXCEPTION
        WHEN OTHERS THEN
          L_ERROR_MELDING := SQLCODE
                             || ' '
                             || SQLERRM;
          INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
            MIN_LASTET_DATO,
            ID,
            ERROR_MSG,
            OPPRETTET_TID,
            KILDE
          ) VALUES(
            NULL,
            REC_FAG.FAGSAK_ID,
            L_ERROR_MELDING,
            V_STOREDATE,
            'FAM_BT_UTPAKKING_OFFSET7'
          );
          COMMIT;
          P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                    || L_ERROR_MELDING, 1, 1000);
      END;
    END LOOP; --Fagsak
    COMMIT;
    IF L_ERROR_MELDING IS NOT NULL THEN
      INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
        ID,
        ERROR_MSG,
        OPPRETTET_TID,
        KILDE
      ) VALUES(
        NULL,
        L_ERROR_MELDING,
        V_STOREDATE,
        'FAM_BT_UTPAKKING_OFFSET8'
      );
      COMMIT;
      P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                || L_ERROR_MELDING, 1, 1000);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      L_ERROR_MELDING := SQLCODE
                         || ' '
                         || SQLERRM;
      INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
        MIN_LASTET_DATO,
        ID,
        ERROR_MSG,
        OPPRETTET_TID,
        KILDE
      ) VALUES(
        NULL,
        NULL,
        L_ERROR_MELDING,
        V_STOREDATE,
        'FAM_BT_UTPAKKING_OFFSET9'
      );
      COMMIT;
      P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                || L_ERROR_MELDING, 1, 1000);
  END FAM_BT_UTPAKKING_OFFSET;

  PROCEDURE FAM_BT2_UTPAKKING_OFFSET(
    P_IN_OFFSET IN NUMBER,
    P_ERROR_MELDING OUT VARCHAR2
  ) AS
 --v_person_ident VARCHAR2(20);
    V_FK_PERSON1_SOKER          NUMBER;
    V_FK_PERSON1_UTB            NUMBER;
    V_PK_BT_UTBETALING          NUMBER;
    V_FK_PERSON1_KOMP_BARN      NUMBER;
    V_PK_BT_UTBET_DET           NUMBER;
    V_PK_PERSON_SOKER           NUMBER;
    V_PK_PERSON_UTB             NUMBER;
    V_PK_FAGSAK                 NUMBER;
    V_PK_BT_KOMPETANSE_PERIODER NUMBER;
    V_PK_BT_KOMPETANSE_BARN     NUMBER;
    V_STOREDATE                 DATE := SYSDATE;
    L_ERROR_MELDING             VARCHAR2(4000);
    CURSOR CUR_BT_FAGSAK(P_OFFSET NUMBER) IS
      WITH JDATA AS (
        SELECT
          KAFKA_OFFSET,
          MELDING         AS DOC,
          PK_BT_META_DATA
        FROM
          DVH_FAM_BT.FAM_BT_META_DATA
        WHERE
          KAFKA_OFFSET = P_OFFSET
          AND KAFKA_TOPIC = 'teamfamilie.aapen-barnetrygd-vedtak-v2'
 --WHERE kafka_offset=110--Test!!!
      )
      SELECT
        T.BEHANDLING_OPPRINNELSE,
        T.BEHANDLING_TYPE,
        T.FAGSAK_ID,
        T.BEHANDLINGS_ID,
        T.FAGSAK_TYPE,
        CAST(TO_TIMESTAMP_TZ(T.TIDSPUNKT_VEDTAK, 'FXYYYY-MM-DD"T"HH24:MI:SS.FXFF3TZH:TZM') AT TIME ZONE 'Europe/Belgrade' AS TIMESTAMP) AS TIDSPUNKT_VEDTAK,
        CASE
          WHEN T.ENSLIG_FORSØRGER = 'false' THEN
            '0'
          ELSE
            '1'
        END AS                                                                    ENSLIG_FORSØRGER,
        T.KATEGORI,
        T.UNDERKATEGORI,
        'BT'                                                                                                                            AS KILDESYSTEM,
        CURRENT_TIMESTAMP                                                                                                               AS LASTET_DATO,
        T.FUNKSJONELL_ID,
        T.BEHANDLINGÅRSAK,
        T.PERSON_IDENT,
        T.ROLLE,
        T.STATSBORGERSKAP,
        T.ANNENPART_BOSTEDSLAND,
        T.ANNENPART_PERSONIDENT,
        T.ANNENPART_STATSBORGERSKAP,
        T.BOSTEDSLAND,
        T.DELINGSPROSENT_OMSORG,
        T.DELINGSPROSENT_YTELSE,
        T.PRIMÆRLAND,
        T.SEKUNDÆRLAND,
        PK_BT_META_DATA,
        KAFKA_OFFSET
      FROM
        JDATA,
        JSON_TABLE ( DOC,
        '$' COLUMNS ( BEHANDLING_OPPRINNELSE VARCHAR2 PATH '$.behandlingOpprinnelse',
        BEHANDLING_TYPE VARCHAR2 PATH '$.behandlingTypeV2',
        FAGSAK_ID VARCHAR2 PATH '$.fagsakId',
        BEHANDLINGS_ID VARCHAR2 PATH '$.behandlingsId',
        FAGSAK_TYPE VARCHAR2 PATH '$.fagsakType',
        TIDSPUNKT_VEDTAK VARCHAR2 PATH '$.tidspunktVedtak',
        ENSLIG_FORSØRGER VARCHAR2 PATH '$.ensligForsørger',
        KATEGORI VARCHAR2 PATH '$.kategoriV2',
        UNDERKATEGORI VARCHAR2 PATH '$.underkategoriV2',
        FUNKSJONELL_ID VARCHAR2 PATH '$.funksjonellId',
        PERSON_IDENT VARCHAR2 PATH '$.personV2[*].personIdent',
        ROLLE VARCHAR2 PATH '$.personV2[*].rolle',
        STATSBORGERSKAP VARCHAR2 PATH '$.personV2[*].statsborgerskap[*]',
        ANNENPART_BOSTEDSLAND VARCHAR2 PATH '$.personV2[*].annenpartBostedsland',
        ANNENPART_PERSONIDENT VARCHAR2 PATH '$.personV2[*].annenpartPersonident',
        ANNENPART_STATSBORGERSKAP VARCHAR2 PATH '$.personV2[*].annenpartStatsborgerskap',
        BOSTEDSLAND VARCHAR2 PATH '$.personV2[*].bostedsland',
        DELINGSPROSENT_OMSORG VARCHAR2 PATH '$.personV2[*].delingsprosentOmsorg',
        DELINGSPROSENT_YTELSE VARCHAR2 PATH '$.personV2[*].delingsprosentYtelse',
        PRIMÆRLAND VARCHAR2 PATH '$.personV2[*].primærland',
        SEKUNDÆRLAND VARCHAR2 PATH '$.personV2[*].sekundærland',
        BEHANDLINGÅRSAK VARCHAR2 PATH '$.behandlingÅrsakV2' ) ) T     ;
 ---------------------------------------------
    CURSOR CUR_BT_KOMPETANSE_PERIODER(P_OFFSET NUMBER) IS
      WITH JDATA AS (
        SELECT
          KAFKA_OFFSET,
          MELDING         AS DOC,
          PK_BT_META_DATA
        FROM
          DVH_FAM_BT.FAM_BT_META_DATA
        WHERE
          KAFKA_OFFSET = P_OFFSET
          AND KAFKA_TOPIC = 'teamfamilie.aapen-barnetrygd-vedtak-v2'
      )
      SELECT
        T.FOM,
        T.TOM,
        T.SOKERSAKTIVITET,
        T.SOKERS_AKTIVITETSLAND,
        T.ANNENFORELDER_AKTIVITET,
        T.ANNENFORELDER_AKTIVITETSLAND,
        T.BARNETS_BOSTEDSLAND,
        T.KOMPETANSE_RESULTAT,
        CURRENT_TIMESTAMP              AS LASTET_DATO
      FROM
        JDATA,
        JSON_TABLE ( DOC,
        '$' COLUMNS ( TOM VARCHAR2 PATH '$.kompetanseperioder[*].tom',
        FOM VARCHAR2 PATH '$.kompetanseperioder[*].fom',
        SOKERSAKTIVITET VARCHAR2 PATH '$.kompetanseperioder[*].sokersaktivitet',
        SOKERS_AKTIVITETSLAND VARCHAR2 PATH '$.kompetanseperioder[*].sokersAktivitetsland',
        ANNENFORELDER_AKTIVITET VARCHAR2 PATH '$.kompetanseperioder[*].annenForeldersAktivitet',
        ANNENFORELDER_AKTIVITETSLAND VARCHAR2 PATH '$.kompetanseperioder[*].annenForeldersAktivitetsland',
        BARNETS_BOSTEDSLAND VARCHAR2 PATH '$.kompetanseperioder[*].barnetsBostedsland',
        KOMPETANSE_RESULTAT VARCHAR2 PATH '$.kompetanseperioder[*].resultat' )) T     ;
 --------------------------------------------------------
    CURSOR CUR_BT_UTBETALING(P_OFFSET NUMBER) IS
      WITH JDATA AS (
        SELECT
          KAFKA_OFFSET,
          MELDING      AS DOC
        FROM
          DVH_FAM_BT.FAM_BT_META_DATA
        WHERE
          KAFKA_OFFSET = P_OFFSET
          AND KAFKA_TOPIC = 'teamfamilie.aapen-barnetrygd-vedtak-v2'
 --WHERE kafka_offset=110--Test!!!
      )
      SELECT
        T.UTBETALT_PER_MND,
        TO_DATE(T.STØNAD_FOM, 'YYYY-MM-DD') AS STØNAD_FOM,
        TO_DATE(T.STØNAD_TOM, 'YYYY-MM-DD') AS STØNAD_TOM,
        T.HJEMMEL,
        CURRENT_TIMESTAMP                   AS LASTET_DATO,
        BEHANDLINGS_ID,
        KAFKA_OFFSET
      FROM
        JDATA,
        JSON_TABLE ( DOC,
        '$' COLUMNS ( BEHANDLINGS_ID VARCHAR2 PATH '$.behandlingsId',
        NESTED PATH '$.utbetalingsperioderV2[*]' COLUMNS ( UTBETALT_PER_MND VARCHAR2 PATH '$.utbetaltPerMnd',
        STØNAD_FOM VARCHAR2 PATH '$.stønadFom',
        STØNAD_TOM VARCHAR2 PATH '$.stønadTom',
        HJEMMEL VARCHAR2 PATH '$.hjemmel' )) ) T     ;
    CURSOR CUR_BT_UTBETALINGS_DETALJER(P_OFFSET NUMBER, P_FOM DATE, P_TOM DATE) IS
      WITH JDATA AS (
        SELECT
          KAFKA_OFFSET,
          MELDING      AS DOC
        FROM
          DVH_FAM_BT.FAM_BT_META_DATA
        WHERE
          KAFKA_OFFSET = P_OFFSET
          AND KAFKA_TOPIC = 'teamfamilie.aapen-barnetrygd-vedtak-v2'
 --WHERE kafka_offset = 110--Test!!!
      )
      SELECT --1 AS pk_bt_utbet_det
 --,
        S.HJEMMEL,
        S.UTBETALTPERMND,
        S.STØNADFOM,
        S.STØNADTOM,
        S.KLASSEKODE,
        S.DELYTELSE_ID,
        S.UTBETALT_PR_MND,
        S.STONAD_FOM,
        S.PERSONIDENT,
        S.ROLLE,
        S.STATSBORGERSKAP,
        S.BOSTEDSLAND,
        S.YTELSE_TYPE
 -- ,S.primærland
 -- ,S.sekundærland
 -- ,S.delingsprosentomsorg delingsprosent_omsorg
,
        S.DELINGSPROSENTYTELSE DELINGSPROSENT_YTELSE
 --  ,S.annenpartpersonident annenpart_personident
 -- ,S.annenpartstatsborgerskap annenpart_statsborgerskap
 -- ,S.annenpartbostedsland annenpart_bostedsland
,
        CURRENT_TIMESTAMP      AS LASTET_DATO,
        BEHANDLINGS_ID,
        KAFKA_OFFSET
      FROM
        JDATA,
        JSON_TABLE ( DOC,
        '$' COLUMNS ( BEHANDLINGS_ID VARCHAR2 PATH '$.behandlingsId',
        NESTED PATH '$.utbetalingsperioderV2[*]' COLUMNS ( HJEMMEL VARCHAR2 PATH '$.hjemmel',
        UTBETALTPERMND VARCHAR2 PATH '$.utbetaltPerMnd',
        STØNADFOM VARCHAR2 PATH '$.stønadFom',
        STØNADTOM VARCHAR2 PATH '$.stønadTom',
        JOINED_ON VARCHAR2 PATH '$.joined_on',
        NESTED PATH '$.utbetalingsDetaljer[*]' COLUMNS ( KLASSEKODE VARCHAR2 PATH '$.klassekode',
        DELYTELSE_ID VARCHAR2 PATH '$.delytelseId',
        YTELSE_TYPE VARCHAR2 PATH '$.ytelseType',
        UTBETALT_PR_MND VARCHAR2 PATH '$..utbetaltPrMnd',
        STONAD_FOM VARCHAR2 PATH '$.stonad_fom',
        PERSONIDENT VARCHAR2 PATH '$.person.personIdent',
        ROLLE VARCHAR2 PATH '$.person.rolle',
        STATSBORGERSKAP VARCHAR2 PATH '$.person.statsborgerskap[*]',
        BOSTEDSLAND VARCHAR2 PATH '$.person.bostedsland'
 --,primærland                VARCHAR2 PATH '$.personV2[*].primærland'
 --,sekundærland              VARCHAR2 PATH '$.personV2[*].sekundærland'
 --,delingsprosentomsorg      VARCHAR2 PATH '$.personV2[*].delingsprosentOmsorg'
,
        DELINGSPROSENTYTELSE VARCHAR2 PATH '$.person.delingsprosentYtelse'
 -- ,annenpartpersonident      VARCHAR2 PATH '$.personV2[*].annenpartPersonident'
 -- ,annenpartstatsborgerskap  VARCHAR2 PATH '$.personV2[*].annenpartStatsborgerskap'
 -- ,annenpartbostedsland      VARCHAR2 PATH '$.personV2[*].annenpartBostedsland'
        ))) ) S
      WHERE
        TO_DATE(S.STØNADFOM, 'YYYY-MM-DD') = P_FOM
        AND TO_DATE(S.STØNADTOM, 'YYYY-MM-DD') = P_TOM
 --WHERE TO_DATE(S.stønadfom,'YYYY-MM-DD') >= TO_DATE('2020-03-01','YYYY-MM-DD')--Test!!!
 --AND TO_DATE(S.stønadtom,'YYYY-MM-DD') <= TO_DATE('2032-01-31','YYYY-MM-DD')--Test!!!
     ;
  BEGIN
 -- For alle fagsaker
    FOR REC_FAG IN CUR_BT_FAGSAK(P_IN_OFFSET) LOOP
      BEGIN
 --dbms_output.put_line(rec_fag.fagsak_id);
        V_PK_PERSON_SOKER := NULL;
        V_FK_PERSON1_SOKER := -1;
        SELECT
          DVH_FAMBT_KAFKA.HIBERNATE_SEQUENCE.NEXTVAL INTO V_PK_PERSON_SOKER
        FROM
          DUAL;
 --Hent fk_person1
        BEGIN
          SELECT
            DISTINCT PERSON_67_VASKET.FK_PERSON1 AS AK_PERSON1 INTO V_FK_PERSON1_SOKER
          FROM
            DT_PERSON.DVH_PERSON_IDENT_OFF_ID_IKKE_SKJERMET PERSON_67_VASKET
          WHERE
            PERSON_67_VASKET.OFF_ID = REC_FAG.PERSON_IDENT
            AND REC_FAG.TIDSPUNKT_VEDTAK BETWEEN PERSON_67_VASKET.GYLDIG_FRA_DATO AND PERSON_67_VASKET.GYLDIG_TIL_DATO;
        EXCEPTION
          WHEN OTHERS THEN
            V_FK_PERSON1_SOKER := -1;
            L_ERROR_MELDING := SQLCODE
                               || ' '
                               || SQLERRM;
            INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
              MIN_LASTET_DATO,
              ID,
              ERROR_MSG,
              OPPRETTET_TID,
              KILDE
            ) VALUES(
              NULL,
              REC_FAG.FAGSAK_ID,
              L_ERROR_MELDING,
              V_STOREDATE,
              'FAM_BT_UTPAKKING_OFFSET1'
            );
            COMMIT;
            P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                      || L_ERROR_MELDING, 1, 1000);
        END;
 -- Insert søker into person tabell
        INSERT INTO DVH_FAM_BT.FAM_BT_PERSON (
          PK_BT_PERSON
 --,annenpart_bostedsland
 --,annenpart_personident
 --,annenpart_statsborgerskap
,
          BOSTEDSLAND
 --,delingsprosent_omsorg
,
          DELINGSPROSENT_YTELSE,
          PERSON_IDENT
 --,primærland
,
          ROLLE
 --,sekundærland
,
          FK_PERSON1,
          LASTET_DATO,
          BEHANDLINGS_ID,
          KAFKA_OFFSET
        ) VALUES (
          V_PK_PERSON_SOKER
 --,rec_fag.annenpart_bostedsland
 --,rec_fag.annenpart_personident
 --,rec_fag.annenpart_statsborgerskap
,
          REC_FAG.BOSTEDSLAND
 --,rec_fag.delingsprosent_omsorg
,
          REC_FAG.DELINGSPROSENT_YTELSE,
          REC_FAG.PERSON_IDENT
 --,rec_fag.primærland
,
          REC_FAG.ROLLE
 --,rec_fag.sekundærland
,
          V_FK_PERSON1_SOKER,
          REC_FAG.LASTET_DATO,
          REC_FAG.BEHANDLINGS_ID,
          REC_FAG.KAFKA_OFFSET
        );
 -- Insert into FAGSAK tabellen
        V_PK_FAGSAK := NULL;
        SELECT
          DVH_FAMBT_KAFKA.HIBERNATE_SEQUENCE.NEXTVAL INTO V_PK_FAGSAK
        FROM
          DUAL;
        INSERT INTO DVH_FAM_BT.FAM_BT_FAGSAK (
          PK_BT_FAGSAK,
          FK_BT_PERSON,
          FK_BT_META_DATA,
          BEHANDLING_OPPRINNELSE,
          BEHANDLING_TYPE,
          FAGSAK_ID,
          BEHANDLINGS_ID,
          TIDSPUNKT_VEDTAK,
          ENSLIG_FORSØRGER,
          KATEGORI,
          UNDERKATEGORI,
          KILDESYSTEM,
          LASTET_DATO,
          FUNKSJONELL_ID,
          BEHANDLING_ÅRSAK,
          KAFKA_OFFSET,
          FAGSAK_TYPE
        ) VALUES (
          V_PK_FAGSAK,
          V_PK_PERSON_SOKER,
          REC_FAG.PK_BT_META_DATA,
          REC_FAG.BEHANDLING_OPPRINNELSE,
          REC_FAG.BEHANDLING_TYPE,
          REC_FAG.FAGSAK_ID,
          REC_FAG.BEHANDLINGS_ID,
          REC_FAG.TIDSPUNKT_VEDTAK,
          REC_FAG.ENSLIG_FORSØRGER,
          REC_FAG.KATEGORI,
          REC_FAG.UNDERKATEGORI,
          REC_FAG.KILDESYSTEM,
          REC_FAG.LASTET_DATO,
          REC_FAG.FUNKSJONELL_ID,
          REC_FAG.BEHANDLINGÅRSAK,
          REC_FAG.KAFKA_OFFSET,
          REC_FAG.FAGSAK_TYPE
        );
 --------------------------------------
 -- For alle Kompetanse/insert into fam_bt_kompetanse_perioder
        FOR REC_KOMPETANSE IN CUR_BT_KOMPETANSE_PERIODER(P_IN_OFFSET) LOOP
          BEGIN
            V_PK_BT_KOMPETANSE_PERIODER := NULL;
            SELECT
              DVH_FAMBT_KAFKA.HIBERNATE_SEQUENCE.NEXTVAL INTO V_PK_BT_KOMPETANSE_PERIODER
            FROM
              DUAL;
            INSERT INTO DVH_FAM_BT.FAM_BT_KOMPETANSE_PERIODER (
              PK_BT_KOMPETANSE_PERIODER,
              FOM,
              TOM,
              SOKERSAKTIVITET,
              ANNENFORELDER_AKTIVITET,
              ANNENFORELDER_AKTIVITETSLAND,
              KOMPETANSE_RESULTAT,
              BARNETS_BOSTEDSLAND,
              FK_BT_FAGSAK,
              LASTET_DATO,
              SOKERS_AKTIVITETSLAND
            ) VALUES (
              V_PK_BT_KOMPETANSE_PERIODER,
              REC_KOMPETANSE.FOM,
              REC_KOMPETANSE.TOM,
              REC_KOMPETANSE.SOKERSAKTIVITET,
              REC_KOMPETANSE.ANNENFORELDER_AKTIVITET,
              REC_KOMPETANSE.ANNENFORELDER_AKTIVITETSLAND,
              REC_KOMPETANSE.KOMPETANSE_RESULTAT,
              REC_KOMPETANSE.BARNETS_BOSTEDSLAND,
              V_PK_FAGSAK,
              REC_KOMPETANSE.LASTET_DATO,
              REC_KOMPETANSE.SOKERS_AKTIVITETSLAND
            );
          END;
 -- Insert into fam_bt_kompetanse_barn
          V_PK_BT_KOMPETANSE_BARN := NULL;
          SELECT
            DVH_FAMBT_KAFKA.HIBERNATE_SEQUENCE.NEXTVAL INTO V_PK_BT_KOMPETANSE_BARN
          FROM
            DUAL;
          INSERT INTO DVH_FAM_BT.FAM_BT_KOMPETANSE_BARN (
            PK_BT_KOMPETANSE_BARN,
            FK_BT_KOMPETANSE_PERIODER,
            FK_PERSON1
          ) VALUES (
            V_PK_BT_KOMPETANSE_BARN,
            V_PK_BT_KOMPETANSE_PERIODER,
            V_FK_PERSON1_SOKER
          );
        END LOOP;
 -----------------------------
 -- For alle utbetalingsperioder
        FOR REC_UTBETALING IN CUR_BT_UTBETALING(P_IN_OFFSET) LOOP
          BEGIN
 --dbms_output.put_line('Hallo z:'||rec_utbetaling.stønad_fom||','||rec_utbetaling.stønad_tom);
            V_PK_BT_UTBETALING := NULL;
            SELECT
              DVH_FAMBT_KAFKA.HIBERNATE_SEQUENCE.NEXTVAL INTO V_PK_BT_UTBETALING
            FROM
              DUAL;
 --v_pk_bt_utbetaling:=rec_utbetaling.PK_BT_UTBETALING;
            INSERT INTO DVH_FAM_BT.FAM_BT_UTBETALING (
              PK_BT_UTBETALING,
              UTBETALT_PER_MND,
              STØNAD_FOM,
              STØNAD_TOM,
              HJEMMEL,
              LASTET_DATO,
              FK_BT_FAGSAK,
              BEHANDLINGS_ID,
              KAFKA_OFFSET
            ) VALUES (
              V_PK_BT_UTBETALING,
              REC_UTBETALING.UTBETALT_PER_MND,
              REC_UTBETALING.STØNAD_FOM,
              REC_UTBETALING.STØNAD_TOM,
              REC_UTBETALING.HJEMMEL,
              REC_UTBETALING.LASTET_DATO,
              V_PK_FAGSAK,
              REC_UTBETALING.BEHANDLINGS_ID,
              REC_UTBETALING.KAFKA_OFFSET
            );
 -- For alle utbetalingsdetaljer for aktuell tidsperiode
            FOR REC_UTBET_DET IN CUR_BT_UTBETALINGS_DETALJER(P_IN_OFFSET, REC_UTBETALING.STØNAD_FOM, REC_UTBETALING.STØNAD_TOM) LOOP
              BEGIN
 --dbms_output.put_line(rec_utbet_det.personident||','||rec_utbet_det.stønadfom||'YY'||to_char(rec_utbet_det.utbetalt_pr_mnd));
 --Hent fk_person1
                V_FK_PERSON1_UTB := -1;
                BEGIN
                  SELECT
                    DISTINCT PERSON_67_VASKET.FK_PERSON1 AS AK_PERSON1 INTO V_FK_PERSON1_UTB
                  FROM
                    DT_PERSON.DVH_PERSON_IDENT_OFF_ID_IKKE_SKJERMET PERSON_67_VASKET
                  WHERE
                    PERSON_67_VASKET.OFF_ID = REC_UTBET_DET.PERSONIDENT
                    AND REC_FAG.TIDSPUNKT_VEDTAK BETWEEN PERSON_67_VASKET.GYLDIG_FRA_DATO AND PERSON_67_VASKET.GYLDIG_TIL_DATO;
                EXCEPTION
                  WHEN OTHERS THEN
                    V_FK_PERSON1_UTB := -1;
                    L_ERROR_MELDING := SQLCODE
                                       || ' '
                                       || SQLERRM;
                    INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
                      MIN_LASTET_DATO,
                      ID,
                      ERROR_MSG,
                      OPPRETTET_TID,
                      KILDE
                    ) VALUES(
                      NULL,
                      REC_UTBET_DET.BEHANDLINGS_ID,
                      L_ERROR_MELDING,
                      V_STOREDATE,
                      'FAM_BT_UTPAKKING_OFFSET2'
                    );
                    COMMIT;
                    P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                              || L_ERROR_MELDING, 1, 1000);
                END;
                V_PK_PERSON_UTB := NULL;
                SELECT
                  DVH_FAMBT_KAFKA.HIBERNATE_SEQUENCE.NEXTVAL INTO V_PK_PERSON_UTB
                FROM
                  DUAL;
 --dbms_output.put_line(v_pk_person_utb);
                BEGIN
                  INSERT INTO DVH_FAM_BT.FAM_BT_PERSON (
                    PK_BT_PERSON
 --,annenpart_bostedsland
 --,annenpart_personident
 --,annenpart_statsborgerskap
,
                    BOSTEDSLAND
 --,delingsprosent_omsorg
,
                    DELINGSPROSENT_YTELSE,
                    PERSON_IDENT
 --,primærland
,
                    ROLLE
 --,sekundærland
,
                    FK_PERSON1,
                    LASTET_DATO,
                    BEHANDLINGS_ID,
                    KAFKA_OFFSET
                  ) VALUES (
 --dvh_fam_fp.hibernate_sequence_test.NEXTVAL
                    V_PK_PERSON_UTB
 --,rec_utbet_det.annenpart_bostedsland
 --,rec_utbet_det.annenpart_personident
 --,rec_utbet_det.annenpart_statsborgerskap
,
                    REC_UTBET_DET.BOSTEDSLAND
 --,rec_utbet_det.delingsprosent_omsorg
,
                    REC_UTBET_DET.DELINGSPROSENT_YTELSE,
                    REC_UTBET_DET.PERSONIDENT
 --,rec_utbet_det.primærland
,
                    REC_UTBET_DET.ROLLE
 --,rec_utbet_det.sekundærland
 -- ,rec_utbet_det.FK_PERSON1
,
                    V_FK_PERSON1_UTB,
                    REC_UTBET_DET.LASTET_DATO,
                    REC_UTBET_DET.BEHANDLINGS_ID,
                    REC_UTBET_DET.KAFKA_OFFSET
                  );
                EXCEPTION
                  WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE(V_PK_PERSON_UTB);
                    L_ERROR_MELDING := SQLCODE
                                       || ' '
                                       || SQLERRM;
                    INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
                      MIN_LASTET_DATO,
                      ID,
                      ERROR_MSG,
                      OPPRETTET_TID,
                      KILDE
                    ) VALUES(
                      NULL,
                      REC_UTBET_DET.BEHANDLINGS_ID,
                      L_ERROR_MELDING,
                      V_STOREDATE,
                      'FAM_BT_UTPAKKING_OFFSET3'
                    );
                    COMMIT;
                    P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                              || L_ERROR_MELDING, 1, 1000);
                END;
                BEGIN
                  INSERT INTO DVH_FAM_BT.FAM_BT_UTBET_DET (
                    PK_BT_UTBET_DET,
                    KLASSEKODE,
                    DELYTELSE_ID,
                    UTBETALT_PR_MND,
                    LASTET_DATO,
                    FK_BT_PERSON,
                    FK_BT_UTBETALING,
                    BEHANDLINGS_ID,
                    KAFKA_OFFSET,
                    YTELSE_TYPE
                  ) VALUES (
                    DVH_FAMBT_KAFKA.HIBERNATE_SEQUENCE.NEXTVAL
 --v_pk_bt_utbet_det
,
                    REC_UTBET_DET.KLASSEKODE,
                    REC_UTBET_DET.DELYTELSE_ID,
                    REC_UTBET_DET.UTBETALT_PR_MND,
                    REC_UTBET_DET.LASTET_DATO,
                    V_PK_PERSON_UTB,
                    V_PK_BT_UTBETALING,
                    REC_UTBET_DET.BEHANDLINGS_ID,
                    REC_UTBET_DET.KAFKA_OFFSET,
                    REC_UTBET_DET.YTELSE_TYPE
                  );
                EXCEPTION
                  WHEN OTHERS THEN
                    L_ERROR_MELDING := SQLCODE
                                       || ' '
                                       || SQLERRM;
                    INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
                      MIN_LASTET_DATO,
                      ID,
                      ERROR_MSG,
                      OPPRETTET_TID,
                      KILDE
                    ) VALUES(
                      NULL,
                      REC_UTBET_DET.BEHANDLINGS_ID,
                      L_ERROR_MELDING,
                      V_STOREDATE,
                      'FAM_BT_UTPAKKING_OFFSET4'
                    );
                    COMMIT;
                    P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                              || L_ERROR_MELDING, 1, 1000);
                END;
              EXCEPTION
                WHEN OTHERS THEN
                  L_ERROR_MELDING := SQLCODE
                                     || ' '
                                     || SQLERRM;
                  INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
                    MIN_LASTET_DATO,
                    ID,
                    ERROR_MSG,
                    OPPRETTET_TID,
                    KILDE
                  ) VALUES(
                    NULL,
                    REC_FAG.FAGSAK_ID,
                    L_ERROR_MELDING,
                    V_STOREDATE,
                    'FAM_BT_UTPAKKING_OFFSET5'
                  );
                  COMMIT;
                  P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                            || L_ERROR_MELDING, 1, 1000);
              END;
            END LOOP; --Utbetalingsdetaljer
          EXCEPTION
            WHEN OTHERS THEN
              L_ERROR_MELDING := SQLCODE
                                 || ' '
                                 || SQLERRM;
              INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
                MIN_LASTET_DATO,
                ID,
                ERROR_MSG,
                OPPRETTET_TID,
                KILDE
              ) VALUES(
                NULL,
                REC_FAG.FAGSAK_ID,
                L_ERROR_MELDING,
                V_STOREDATE,
                'FAM_BT_UTPAKKING_OFFSET6'
              );
              COMMIT;
              P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                        || L_ERROR_MELDING, 1, 1000);
          END;
        END LOOP; --Utbetalinger
      EXCEPTION
        WHEN OTHERS THEN
          L_ERROR_MELDING := SQLCODE
                             || ' '
                             || SQLERRM;
          INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
            MIN_LASTET_DATO,
            ID,
            ERROR_MSG,
            OPPRETTET_TID,
            KILDE
          ) VALUES(
            NULL,
            REC_FAG.FAGSAK_ID,
            L_ERROR_MELDING,
            V_STOREDATE,
            'FAM_BT_UTPAKKING_OFFSET7'
          );
          COMMIT;
          P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                    || L_ERROR_MELDING, 1, 1000);
      END;
    END LOOP; --Fagsak
    COMMIT;
    IF L_ERROR_MELDING IS NOT NULL THEN
      INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
        ID,
        ERROR_MSG,
        OPPRETTET_TID,
        KILDE
      ) VALUES(
        NULL,
        L_ERROR_MELDING,
        V_STOREDATE,
        'FAM_BT_UTPAKKING_OFFSET8'
      );
      COMMIT;
      P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                || L_ERROR_MELDING, 1, 1000);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      L_ERROR_MELDING := SQLCODE
                         || ' '
                         || SQLERRM;
      INSERT INTO DVH_FAM_FP.FP_XML_UTBRETT_ERROR(
        MIN_LASTET_DATO,
        ID,
        ERROR_MSG,
        OPPRETTET_TID,
        KILDE
      ) VALUES(
        NULL,
        NULL,
        L_ERROR_MELDING,
        V_STOREDATE,
        'FAM_BT_UTPAKKING_OFFSET9'
      );
      COMMIT;
      P_ERROR_MELDING := SUBSTR(P_ERROR_MELDING
                                || L_ERROR_MELDING, 1, 1000);
  END FAM_BT2_UTPAKKING_OFFSET;
END FAM_BT;
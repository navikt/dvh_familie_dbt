create or replace PACKAGE BODY fam_ks AS


  PROCEDURE fam_ks_mottaker_insert(p_in_period IN VARCHAR2
                                  ,p_in_gyldig_flagg in number default 0
                                  ,p_error_melding OUT VARCHAR2) AS
    v_aar_start VARCHAR2(6):= substr(p_in_period,1,4) || '01';
    v_kilde VARCHAR2(10) := 'KS';
    --v_storedate DATE := sysdate;
    l_error_melding VARCHAR2(1000);
    v_in_period_fra NUMBER :=p_in_period||'00';
    v_in_period_til NUMBER :=p_in_period||'32';


    l_commit NUMBER := 0;

    CURSOR cur_mottaker IS SELECT ur.fk_person1,max(henvisning) behandlings_id,fagsak.fagsak_id,
        sum(belop) belop,
        SUM(CASE WHEN ur.posteringsdato between ur.DATO_UTBET_FOM AND ur.DATO_UTBET_TOM THEN BELOP ELSE 0 END) belopm,
        SUM(CASE WHEN ur.posteringsdato > ur.DATO_UTBET_TOM THEN BELOP ELSE 0 END) belope,
        MAX(utbet_det.STONAD_FOM) utbet_fom,
       --MAX(utbet_det.STONAD_FOM) utbet_fom
    --ur.DATO_UTBET_TOM utbet_tom,
        MAX(DIM_PERSON_MOTTAKER.PK_DIM_PERSON) FK_DIM_PERSON,
        max(fagsak.pk_ks_fagsak)  as fk_ks_fagsak,
        EXTRACT( YEAR FROM dim_person_mottaker.fodt_dato) fodsel_aar,
        to_char(dim_person_mottaker.fodt_dato,'MM') fodsel_mnd,
        dim_kjonn_mottaker.kjonn_kode kjonn,
        dim_person_mottaker.fk_dim_kjonn,
        dim_alder.pk_dim_alder fk_dim_alder,
        dim_person_mottaker.fk_dim_sivilstatus,
        dim_person_mottaker.fk_dim_land_fodt,
        dim_person_mottaker.fk_dim_geografi_bosted,
        dim_person_mottaker.fk_dim_land_statsborgerskap,
        fam_ks_periode.pk_dim_tid fk_dim_tid_mnd,
        count(distinct utbet_det.fk_person1) antbarn

    --keep (dense_rank first order by fagsak.tidspunkt_vedtak desc) as pk_bt_fagsak,

    FROM dvh_fam_ks.fam_ks_ur_utbetaling ur
    LEFT JOIN dvh_fam_ks.vfam_ks_fagsak fagsak
    ON fagsak.behandlings_id = ur.henvisning

     LEFT OUTER JOIN dt_kodeverk.dim_tid fam_ks_periode
        ON to_char(ur.posteringsdato,'YYYYMM') = fam_ks_periode.aar_maaned
        AND fam_ks_periode.dim_nivaa = 3
        AND fam_ks_periode.gyldig_flagg = 1

    left outer join VFAM_KS_UTBETALING_DET utbet_det ON
    ur.delytelse_id=utbet_det.delytelse_id and
    fagsak.pk_ks_fagsak=utbet_det.fk_ks_fagsak

     LEFT OUTER JOIN dt_person.dim_person dim_person_mottaker
        ON dim_person_mottaker.fk_person1 = ur.gjelder_mottaker
        AND dim_person_mottaker.gyldig_fra_dato <= fam_ks_periode.siste_dato_i_perioden
        AND dim_person_mottaker.gyldig_til_dato >= fam_ks_periode.siste_dato_i_perioden

    LEFT OUTER JOIN dt_kodeverk.dim_geografi dim_geografi
        ON dim_person_mottaker.fk_dim_geografi_bosted = dim_geografi.pk_dim_geografi

    LEFT OUTER JOIN dt_kodeverk.dim_kjonn dim_kjonn_mottaker
        ON dim_person_mottaker.fk_dim_kjonn = dim_kjonn_mottaker.pk_dim_kjonn

    LEFT OUTER JOIN dt_kodeverk.dim_alder
        ON floor(months_between(fam_ks_periode.siste_dato_i_perioden, dim_person_mottaker.fodt_dato)/12) = dim_alder.alder
        AND dim_alder.gyldig_fra_dato <= fam_ks_periode.siste_dato_i_perioden
        AND dim_alder.gyldig_til_dato >= fam_ks_periode.siste_dato_i_perioden

    WHERE --TRUNC(POSTERINGSDATO,'MM')=to_date('20230201','YYYYMMDD')
    FK_DIM_TID_DATO_POSTERT_UR between v_in_period_fra and v_in_period_til
    group by ur.fk_person1,--DATO_UTBET_FOM,DATO_UTBET_TOM,
    fagsak.fagsak_id,
    EXTRACT( YEAR FROM dim_person_mottaker.fodt_dato),
    to_char(dim_person_mottaker.fodt_dato,'MM'),
    dim_kjonn_mottaker.kjonn_kode,
    dim_person_mottaker.fk_dim_kjonn,
    dim_person_mottaker.fk_dim_sivilstatus,
    dim_person_mottaker.fk_dim_land_fodt,
    dim_person_mottaker.fk_dim_geografi_bosted,
    dim_person_mottaker.fk_dim_land_statsborgerskap,
    fam_ks_periode.pk_dim_tid,dim_alder.pk_dim_alder

    having sum(ur.belop)!=0
    ;

  BEGIN

    -- Slett mottakere dvh_fam_fp.fam_bt_mottaker_hist for aktuell periode
    BEGIN
      DELETE FROM dvh_fam_ks.FAK_FAM_KS_MOTTAKER
      WHERE kilde = v_kilde
      AND stat_aarmnd= p_in_period
      and gyldig_flagg = p_in_gyldig_flagg;
      COMMIT;
    EXCEPTION
      WHEN OTHERS THEN
        l_error_melding := substr(SQLCODE || ' ' || sqlerrm, 1, 1000);
        /*INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
        VALUES(NULL, NULL, l_error_melding, sysdate, 'FAM_KS_MOTTAKER_INSERT_WITH1');*/
        COMMIT;
        p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
        l_error_melding := NULL;
    END;

    FOR rec_mottaker IN cur_mottaker LOOP
      --INSERT INTO dvh_fam_fp.fam_bt_mottaker
      BEGIN
        INSERT INTO dvh_fam_ks.fak_fam_ks_mottaker
        (
          fk_person1_mottaker, stat_aarmnd,belop,
          belopm,belope,
          utbetfom,--utbet_tom,
          fk_dim_person,fagsak_id,fk_ks_fagsak,behandlings_id,
          fodsel_aar,fodsel_mnd,kjonn,fk_dim_alder,kilde,fk_dim_kjonn,fk_dim_tid_mnd,
          fk_dim_geografi_bosted,fk_dim_land_statsborgerskap,
          lastet_dato,gyldig_flagg,antbarn
        )
        VALUES
        (
          rec_mottaker.fk_person1, p_in_period,rec_mottaker.belop,rec_mottaker.belopm,rec_mottaker.belope,rec_mottaker.utbet_fom,
          --rec_mottaker.utbet_tom,
          rec_mottaker.fk_dim_person,rec_mottaker.fagsak_id,rec_mottaker.fk_ks_fagsak,
          rec_mottaker.behandlings_id,
          rec_mottaker.fodsel_aar,rec_mottaker.fodsel_mnd,rec_mottaker.kjonn,rec_mottaker.fk_dim_alder, v_kilde,
          rec_mottaker.fk_dim_kjonn,rec_mottaker.fk_dim_tid_mnd,rec_mottaker.fk_dim_geografi_bosted,rec_mottaker.fk_dim_land_statsborgerskap,
          sysdate,p_in_gyldig_flagg,rec_mottaker.antbarn
        );
        l_commit := l_commit + 1;
      EXCEPTION
        WHEN OTHERS THEN
          l_error_melding := substr(SQLCODE || ' ' || sqlerrm, 1, 1000);
        INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
          VALUES(NULL, rec_mottaker.fagsak_id, l_error_melding, sysdate, 'FAM_KS_MOTTAKER_INSERT_WITH2');
          l_commit := l_commit + 1;--Gå videre til neste rekord
          p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
          l_error_melding := NULL;
      END;
      IF l_commit >= 100000 THEN
          COMMIT;
          l_commit := 0;
      END IF;
    END LOOP;
    COMMIT;
    IF l_error_melding IS NOT NULL THEN
     INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(ID, error_msg, opprettet_tid, kilde)
      VALUES(NULL, l_error_melding, sysdate, 'FAM_KS_MOTTAKER_INSERT_WITH3');
      COMMIT;
      p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      l_error_melding := substr(SQLCODE || ' ' || sqlerrm, 1, 1000);
     INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
      VALUES(NULL, NULL, l_error_melding, sysdate, 'FAM_KS_MOTTAKER_INSERT_WITH4');
      COMMIT;
      p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
  END fam_ks_mottaker_insert;



  PROCEDURE fam_ks_barn_insert(p_in_period IN VARCHAR2
                                  ,p_in_gyldig_flagg in number default 0
                                  ,p_error_melding OUT VARCHAR2) AS
    v_aar_start VARCHAR2(6):= substr(p_in_period,1,4) || '01';
    v_kilde VARCHAR2(10) := 'KS';
    --v_storedate DATE := sysdate;
    l_error_melding VARCHAR2(1000);
    v_in_period_fra NUMBER :=p_in_period||'00';
    v_in_period_til NUMBER :=p_in_period||'32';


    l_commit NUMBER := 0;

    CURSOR cur_barn IS

WITH
   UR AS
      (
SELECT ur.fk_person1,
HENVISNING,delytelse_id,
fam_ks_periode.aar_maaned stat_aarmnd,
fam_ks_periode.pk_dim_tid fk_dim_tid_mnd,
fam_ks_periode.siste_dato_i_perioden,
belop,
fagsak.fagsak_id,
fagsak.pk_ks_fagsak fk_ks_fagsak
    FROM dvh_fam_ks.fam_ks_ur_utbetaling ur
     JOIN dt_kodeverk.dim_tid fam_ks_periode
        ON to_char(ur.posteringsdato,'YYYYMM') = fam_ks_periode.aar_maaned
        AND fam_ks_periode.dim_nivaa = 3
        AND fam_ks_periode.gyldig_flagg = 1

      JOIN dvh_fam_ks.fam_ks_fagsak FAGSAK ON
      ur.henvisning=FAGSAK.behandlings_id
   WHERE FK_DIM_TID_DATO_POSTERT_UR between v_in_period_fra and v_in_period_til
    )

SELECT ur.fk_person1 fk_person1_mottaker
,dim_person_mottaker.pk_dim_person,
dim_kjonn_mottaker.kjonn_kode kjonn,
dim_kjonn_mottaker.pk_dim_kjonn fk_dim_kjonn,
utbet_det.fk_person1_barn fk_person1_barn,
dim_person_barn.pk_dim_person fk_dim_person_barn,
ur.fagsak_id,
ur.fk_ks_fagsak,
dim_kjonn_barn.kjonn_kode kjonn_barn,
dim_kjonn_barn.pk_dim_kjonn fk_dim_kjonn_barn,

--,ur.delytelse_id,
sum(ur.belop) belop, stat_aarmnd
,EXTRACT( YEAR FROM dim_person_mottaker.fodt_dato) fodsel_aar
,EXTRACT (MONTH FROM dim_person_mottaker.fodt_dato) fodsel_mnd
,EXTRACT( YEAR FROM dim_person_barn.fodt_dato) fodsel_aar_barn
,EXTRACT (MONTH FROM dim_person_barn.fodt_dato) fodsel_mnd_barn
,fk_dim_tid_mnd


FROM UR


  join FAM_KS_UTBETALINGS_DET utbet_det ON
    ur.delytelse_id=utbet_det.delytelse_id and
    ur.fk_ks_fagsak=utbet_det.fk_ks_fagsak

      JOIN dt_person.dim_person dim_person_mottaker
        ON dim_person_mottaker.fk_person1 = ur.fk_person1
        AND dim_person_mottaker.gyldig_fra_dato <= ur.siste_dato_i_perioden
        AND dim_person_mottaker.gyldig_til_dato >= ur.siste_dato_i_perioden

        left outer join dt_kodeverk.dim_kjonn dim_kjonn_mottaker
        on dim_person_mottaker.fk_dim_kjonn = dim_kjonn_mottaker.pk_dim_kjonn

     LEFT OUTER JOIN dt_person.dim_person dim_person_barn
        ON dim_person_barn.fk_person1 = utbet_det.fk_person1_barn
        AND dim_person_barn.gyldig_fra_dato <= ur.siste_dato_i_perioden
        AND dim_person_barn.gyldig_til_dato >= ur.siste_dato_i_perioden

    left outer join dt_kodeverk.dim_kjonn dim_kjonn_barn
        on dim_person_barn.fk_dim_kjonn = dim_kjonn_barn.pk_dim_kjonn
--where dim_person_mottaker.fk_person1=1021056929

group by ur.fk_person1,
dim_person_mottaker.pk_dim_person,
dim_kjonn_mottaker.kjonn_kode,dim_kjonn_mottaker.pk_dim_kjonn,
utbet_det.fk_person1_barn,
dim_person_barn.pk_dim_person,
stat_aarmnd,
ur.fagsak_id,
ur.fk_ks_fagsak,
dim_kjonn_barn.kjonn_kode,stat_aarmnd
,EXTRACT( YEAR FROM dim_person_mottaker.fodt_dato),EXTRACT(MONTH FROM dim_person_mottaker.fodt_dato)
,EXTRACT( YEAR FROM dim_person_mottaker.fodt_dato)
,EXTRACT (MONTH FROM dim_person_mottaker.fodt_dato)
,EXTRACT( YEAR FROM dim_person_barn.fodt_dato)
,EXTRACT (MONTH FROM dim_person_barn.fodt_dato),dim_kjonn_barn.pk_dim_kjonn
,fk_dim_tid_mnd,dim_kjonn_barn.pk_dim_kjonn
having sum(ur.belop)>0;

  BEGIN

    -- Slett mottakere dvh_fam_fp.fam_bt_mottaker_hist for aktuell periode
    BEGIN
      DELETE FROM dvh_fam_ks.FAK_FAM_KS_MOTTAKER
      WHERE kilde = v_kilde
      AND stat_aarmnd= p_in_period
      and gyldig_flagg = p_in_gyldig_flagg;
      COMMIT;
    EXCEPTION
      WHEN OTHERS THEN
        l_error_melding := substr(SQLCODE || ' ' || sqlerrm, 1, 1000);
        /*INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
        VALUES(NULL, NULL, l_error_melding, sysdate, 'FAM_KS_MOTTAKER_INSERT_WITH1');*/
        COMMIT;
        p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
        l_error_melding := NULL;
    END;

    FOR rec_barn IN cur_barn LOOP
      --INSERT INTO dvh_fam_fp.fam_bt_mottaker
      BEGIN
       INSERT INTO dvh_fam_ks.fam_ks_barn
        (
          stat_aarmnd,
          fk_dim_tid_mnd,
          fk_person1_mottaker,
        --  fk_dim_person,
          fk_dim_kjonn,
          fodsel_aar,
          fodsel_mnd,
          kjonn,
          fk_person1_barn,
          fk_dim_person_barn,
          kjonn_barn,
          fk_dim_kjonn_barn,
          fodsel_aar_barn,
          fodsel_mnd_barn,
          fagsak_id,fk_ks_fagsak,
          kilde,
          lastet_dato
          ,gyldig_flagg
        )
        VALUES
        (
          p_in_period,
          rec_barn.fk_dim_tid_mnd,
          rec_barn.fk_person1_mottaker,
        --  rec_barn.fk_dim_person_mottaker,
         -- rec_barn.fk_dim_person,
         rec_barn.fk_dim_kjonn,
          rec_barn.fodsel_aar,
          rec_barn.fodsel_mnd,
          rec_barn.kjonn,
          rec_barn.fk_person1_barn,
          rec_barn.fk_dim_person_barn,
          rec_barn.kjonn_barn,
          rec_barn.fk_dim_kjonn_barn,
          rec_barn.fodsel_aar_barn,
          rec_barn.fodsel_mnd_barn,
          rec_barn.fagsak_id,
          rec_barn.fk_ks_fagsak,
          v_kilde,
          sysdate
          ,p_in_gyldig_flagg
        );
        l_commit := l_commit + 1;
      EXCEPTION
        WHEN OTHERS THEN
          l_error_melding := substr(SQLCODE || ' ' || sqlerrm, 1, 1000);
        /*INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
          VALUES(NULL, rec_mottaker.fagsak_id, l_error_melding, sysdate, 'FAM_KS_MOTTAKER_INSERT_WITH2');
          l_commit := l_commit + 1;--Gå videre til neste rekord
          */
          p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
          l_error_melding := NULL;
      END;
      IF l_commit >= 100000 THEN
          COMMIT;
          l_commit := 0;
      END IF;
    END LOOP;
    COMMIT;
    IF l_error_melding IS NOT NULL THEN
     INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(ID, error_msg, opprettet_tid, kilde)
      VALUES(NULL, l_error_melding, sysdate, 'FAM_KS_MOTTAKER_INSERT_WITH3');
      COMMIT;
      p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      l_error_melding := substr(SQLCODE || ' ' || sqlerrm, 1, 1000);
     INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
      VALUES(NULL, NULL, l_error_melding, sysdate, 'FAM_KS_MOTTAKER_INSERT_WITH4');
      COMMIT;
      p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
  END fam_ks_barn_insert;




END fam_ks;
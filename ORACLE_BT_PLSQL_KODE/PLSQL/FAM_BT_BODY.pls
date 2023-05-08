create or replace PACKAGE BODY                                                                                                                                                                                                                                                                                                                                                                                                                                 fam_bt AS

  PROCEDURE fam_bt_infotrygd_mottaker_update(p_in_period IN VARCHAR2, p_error_melding OUT VARCHAR2) AS
    --v_aar_start VARCHAR2(6):= substr(p_in_period,1,4) || '01';
    v_storedate DATE := sysdate;
    l_error_melding VARCHAR2(1000);

    l_commit NUMBER := 0;

    CURSOR cur_mottaker IS
      SELECT /*+ PARALLEL(8) */
             infotrygd.fk_person1, infotrygd.pk_bt_mottaker,
             nvl(MAX(dim_person_mottaker.bosted_kommune_nr),-1) AS bosted_kommune_nr,
             nvl(MAX(dim_geografi.bydel_kommune_nr),-1) AS bydel_kommune_nr,
             nvl(max(dim_person_mottaker.pk_dim_person),-1) as fk_dim_person,
             nvl(max(dim_person_mottaker.fk_dim_geografi_bosted),-1) as fk_dim_geografi_bosted
      FROM dvh_fam_bt.fam_bt_mottaker infotrygd

      LEFT OUTER JOIN dt_kodeverk.dim_tid fam_bt_periode
      ON infotrygd.stat_aarmnd = fam_bt_periode.aar_maaned
      AND fam_bt_periode.dim_nivaa = 3
      AND fam_bt_periode.gyldig_flagg = 1

      LEFT OUTER JOIN dt_person.dim_person dim_person_mottaker
      ON dim_person_mottaker.fk_person1 = infotrygd.fk_person1
      AND dim_person_mottaker.gyldig_fra_dato <= fam_bt_periode.siste_dato_i_perioden
      AND dim_person_mottaker.gyldig_til_dato >= fam_bt_periode.siste_dato_i_perioden

      LEFT OUTER JOIN dt_kodeverk.dim_geografi dim_geografi
      ON dim_person_mottaker.fk_dim_geografi_bosted = dim_geografi.pk_dim_geografi

      WHERE infotrygd.kilde = 'INFOTRYGD'
      AND infotrygd.stat_aarmnd = p_in_period
      and infotrygd.fk_dim_geografi_bosted is null
      GROUP BY infotrygd.fk_person1, infotrygd.pk_bt_mottaker;
  BEGIN
    FOR rec_mottaker IN cur_mottaker LOOP
      BEGIN
        UPDATE dvh_fam_bt.fam_bt_mottaker
        SET bosted_kommune_nr = rec_mottaker.bosted_kommune_nr,
            bosted_bydel_kommune_nr = rec_mottaker.bydel_kommune_nr,
            fk_dim_person = rec_mottaker.fk_dim_person,
            fk_dim_geografi_bosted = rec_mottaker.fk_dim_geografi_bosted
        WHERE pk_bt_mottaker = rec_mottaker.pk_bt_mottaker;
        l_commit := l_commit + 1;
      EXCEPTION
        WHEN OTHERS THEN
          l_error_melding := substr(SQLCODE || ' ' || sqlerrm, 1, 1000);
          INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
          VALUES(NULL, rec_mottaker.pk_bt_mottaker, l_error_melding, v_storedate, 'FAM_BT_INFOTRYGD_MOTTAKER_UPDATE1');
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
      VALUES(NULL, l_error_melding, v_storedate, 'FAM_BT_INFOTRYGD_MOTTAKER_UPDATE2');
      COMMIT;
      p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      l_error_melding := substr(SQLCODE || ' ' || sqlerrm, 1, 1000);
      INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
      VALUES(NULL, NULL, l_error_melding, v_storedate, 'FAM_BT_INFOTRYGD_MOTTAKER_UPDATE3');
      COMMIT;
      p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
  END fam_bt_infotrygd_mottaker_update;

  PROCEDURE fam_bt_mottaker_insert_2(p_in_period IN VARCHAR2
                                  ,p_in_gyldig_flagg in number default 0
                                  ,p_error_melding OUT VARCHAR2) AS
    v_aar_start VARCHAR2(6):= substr(p_in_period,1,4) || '01';
    v_kilde VARCHAR2(10) := 'BT';
    --v_storedate DATE := sysdate;
    l_error_melding VARCHAR2(1000);

    l_commit NUMBER := 0;

    CURSOR cur_mottaker IS
      SELECT /*+ PARALLEL(8) */ data.*,
             belope + belophie_b as belophie,
             belop + belophit_b as belophit
            ,belop_utvidet + belophit_utvidet_b as belophit_utvidet
            ,case when underkategori = 'ORDINÆR' then 1
                  when underkategori = 'UTVIDET' then 2
                  when underkategori = 'INSTITUSJON' then 4
             end statusk
      FROM
      (
        SELECT ur.gjelder_mottaker,
               --to_char(ur.posteringsdato,'YYYYMM') periode,
               SUM(ur.belop_sign) belop,
               dim_kjonn_mottaker.kjonn_kode kjonn,
               SUM(CASE WHEN to_char(ur.posteringsdato,'YYYYMM') > to_char(ur.dato_utbet_fom,'YYYYMM') THEN
                             ur.belop_sign
                        ELSE 0.0
                   END) belope,
               EXTRACT( YEAR FROM dim_person_mottaker.fodt_dato) fodsel_aar,
               to_char(dim_person_mottaker.fodt_dato,'MM') fodsel_mnd,
               dim_person_mottaker.tknr,
               dim_person_mottaker.bosted_kommune_nr,
               dim_geografi.bydel_kommune_nr,
               fagsak.fagsak_id,
               --fagsak.fagsak_type,
               max(fagsak.pk_bt_fagsak) keep (dense_rank first order by fagsak.tidspunkt_vedtak desc) as pk_bt_fagsak,
               max(fagsak.behandling_årsak) keep (dense_rank first order by fagsak.tidspunkt_vedtak desc) as behandling_årsak,
               max(fagsak.underkategori) keep (dense_rank first order by fagsak.tidspunkt_vedtak desc) as underkategori,
               --fagsak.enslig_forsørger,
               dim_person_mottaker.pk_dim_person,
               dim_person_mottaker.fk_dim_kjonn,
               dim_person_mottaker.fk_dim_sivilstatus,
               dim_person_mottaker.fk_dim_land_fodt,
               dim_person_mottaker.fk_dim_geografi_bosted,
               dim_person_mottaker.fk_dim_land_statsborgerskap,
               fam_bt_periode.pk_dim_tid,
               fam_bt_periode.siste_dato_i_perioden,
               dim_alder.pk_dim_alder,
               barn.fkby_person1,
               barn.antbarn, barn.delingsprosent_ytelse,
               TRUNC(months_between(fam_bt_periode.siste_dato_i_perioden,dim_person_barn.fodt_dato)/12) alderyb,
               dim_land_barn.land_ssb_kode ssb_kode,
               nvl(belop.belophie,0) belophie_b,
               nvl(belop.belophit,0) belophit_b
              ,sum((case when utvidet_barnetrygd.delytelse_id is not null then 1
                         else 0
                    end) * ur.belop_sign) belop_utvidet
              ,nvl(belophit_utvidet.belophit_utvidet,0) belophit_utvidet_b
        FROM dvh_fam_bt.fam_bt_ur_utbetaling ur
        LEFT JOIN dvh_fam_bt.fam_bt_fagsak fagsak
        ON fagsak.behandlings_id = ur.henvisning

        LEFT OUTER JOIN dt_kodeverk.dim_tid fam_bt_periode
        ON to_char(ur.posteringsdato,'YYYYMM') = fam_bt_periode.aar_maaned
        AND fam_bt_periode.dim_nivaa = 3
        AND fam_bt_periode.gyldig_flagg = 1

        LEFT OUTER JOIN dt_person.dim_person dim_person_mottaker
        ON dim_person_mottaker.fk_person1 = ur.gjelder_mottaker
        AND dim_person_mottaker.gyldig_fra_dato <= fam_bt_periode.siste_dato_i_perioden
        AND dim_person_mottaker.gyldig_til_dato >= fam_bt_periode.siste_dato_i_perioden
        LEFT OUTER JOIN dt_kodeverk.dim_geografi dim_geografi
        ON dim_person_mottaker.fk_dim_geografi_bosted = dim_geografi.pk_dim_geografi
        LEFT OUTER JOIN dt_kodeverk.dim_kjonn dim_kjonn_mottaker
        ON dim_person_mottaker.fk_dim_kjonn = dim_kjonn_mottaker.pk_dim_kjonn

        LEFT OUTER JOIN dt_kodeverk.dim_alder
        ON floor(months_between(sysdate, dim_person_mottaker.fodt_dato)/12) = dim_alder.alder
        AND dim_alder.gyldig_fra_dato <= fam_bt_periode.siste_dato_i_perioden
        AND dim_alder.gyldig_til_dato >= fam_bt_periode.siste_dato_i_perioden

        LEFT JOIN
        (
          SELECT fk_person1,
                 substr(MAX(fodsel_aar_barn||fodsel_mnd_barn||'-'||fkb_person1),8) fkby_person1,
                 max(delingsprosent_ytelse) as delingsprosent_ytelse,
                 COUNT(DISTINCT fkb_person1) antbarn
          FROM dvh_fam_bt.fam_bt_barn
          WHERE kilde = v_kilde
          AND stat_aarmnd = p_in_period
          and gyldig_flagg = p_in_gyldig_flagg
          GROUP BY fk_person1
        ) barn
        ON ur.gjelder_mottaker = barn.fk_person1

        LEFT JOIN dt_person.dim_person dim_person_barn
        ON dim_person_barn.fk_person1 = barn.fkby_person1
        AND dim_person_barn.gyldig_fra_dato <= fam_bt_periode.siste_dato_i_perioden
        AND dim_person_barn.gyldig_til_dato >= fam_bt_periode.siste_dato_i_perioden
        LEFT OUTER JOIN dt_kodeverk.dim_land dim_land_barn
        ON dim_person_barn.fk_dim_land_bosted = dim_land_barn.pk_dim_land
        --AND dim_land.gyldig_fra_dato <= fam_bt_periode.siste_dato_i_perioden
        --AND dim_land.gyldig_til_dato >= fam_bt_periode.siste_dato_i_perioden

        LEFT JOIN
        (
          SELECT fk_person1,
                 SUM(nvl(belop,0)) belophit,
                 SUM(nvl(belope,0)) belophie
          FROM dvh_fam_bt.fam_bt_mottaker
          WHERE stat_aarmnd >= v_aar_start
          AND stat_aarmnd < p_in_period
          and gyldig_flagg = p_in_gyldig_flagg
          GROUP BY fk_person1
        ) belop
        ON ur.gjelder_mottaker = belop.fk_person1

        --Utvidet barnetrygd
        left join
        (select utbet.behandlings_id, utbet.delytelse_id, utbetaling.stønad_fom, utbetaling.stønad_tom
         from dvh_fam_bt.fam_bt_utbet_det utbet
         join dvh_fam_bt.fam_bt_person person
         on utbet.fk_bt_person = person.pk_bt_person
         and person.rolle = 'SØKER'
         join dvh_fam_bt.fam_bt_utbetaling utbetaling
         on utbet.fk_bt_utbetaling = utbetaling.pk_bt_utbetaling
         join dvh_fam_bt.fam_bt_fagsak fagsak
         on utbetaling.behandlings_id = fagsak.behandlings_id
         and fagsak.enslig_forsørger = 1) utvidet_barnetrygd
        on ur.henvisning = utvidet_barnetrygd.behandlings_id
        and ur.delytelse_id = utvidet_barnetrygd.delytelse_id
        and utvidet_barnetrygd.stønad_fom <= fam_bt_periode.siste_dato_i_perioden
        and utvidet_barnetrygd.stønad_tom >= fam_bt_periode.siste_dato_i_perioden
        left join
        (select fk_person1,
                sum(nvl(belop_utvidet,0)) belophit_utvidet
         from dvh_fam_bt.fam_bt_mottaker
         where stat_aarmnd >= v_aar_start
         and stat_aarmnd < p_in_period
         and gyldig_flagg = p_in_gyldig_flagg
         group by fk_person1
        ) belophit_utvidet
        ON ur.gjelder_mottaker = belophit_utvidet.fk_person1

        WHERE ur.hovedkontonr = 800--in (800, 214, 216, 215)-- = 800 Test!!!
        AND to_char(ur.posteringsdato,'YYYYMM') = p_in_period
        --AND LENGTH(ur.delytelse_id) < 14
        GROUP BY ur.gjelder_mottaker, to_char(ur.posteringsdato,'YYYYMM'), dim_kjonn_mottaker.kjonn_kode,
                 fam_bt_periode.siste_dato_i_perioden, EXTRACT( YEAR FROM dim_person_mottaker.fodt_dato),
                 to_char(dim_person_mottaker.fodt_dato,'MM'), dim_person_mottaker.tknr,
                 dim_person_mottaker.bosted_kommune_nr, dim_geografi.kommune_nr,
                 dim_geografi.bydel_kommune_nr, fagsak.fagsak_id,
                 dim_person_mottaker.pk_dim_person,
                 dim_person_mottaker.fk_dim_kjonn, dim_person_mottaker.fk_dim_sivilstatus,
                 dim_person_mottaker.fk_dim_land_fodt, dim_person_mottaker.fk_dim_geografi_bosted,
                 dim_person_mottaker.fk_dim_land_statsborgerskap, fam_bt_periode.pk_dim_tid,
                 dim_alder.pk_dim_alder,barn.fkby_person1,barn.antbarn,barn.delingsprosent_ytelse,
                 TRUNC(months_between(fam_bt_periode.siste_dato_i_perioden,dim_person_barn.fodt_dato)/12),
                 dim_land_barn.land_ssb_kode,nvl(belop.belophie,0),nvl(belop.belophit,0)
                ,nvl(belophit_utvidet.belophit_utvidet,0)
        having sum(ur.belop_sign) != 0
      ) data;
  BEGIN

    -- Slett mottakere dvh_fam_fp.fam_bt_mottaker_hist for aktuell periode
    BEGIN
      DELETE FROM dvh_fam_bt.fam_bt_mottaker--r152241.test_drp
      WHERE kilde = v_kilde
      AND stat_aarmnd= p_in_period
      and gyldig_flagg = p_in_gyldig_flagg;
      COMMIT;
    EXCEPTION
      WHEN OTHERS THEN
        l_error_melding := substr(SQLCODE || ' ' || sqlerrm, 1, 1000);
        INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
        VALUES(NULL, NULL, l_error_melding, sysdate, 'FAM_BT_MOTTAKER_INSERT_WITH1');
        COMMIT;
        p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
        l_error_melding := NULL;
    END;

    FOR rec_mottaker IN cur_mottaker LOOP
      --INSERT INTO dvh_fam_fp.fam_bt_mottaker
      BEGIN
        INSERT INTO dvh_fam_bt.fam_bt_mottaker--R152241.test_drp
        (
          fk_person1, belop, kjonn, belope,
          fodsel_aar, fodsel_mnd, tknr, bosted_kommune_nr,
          bosted_bydel_kommune_nr, fagsak_id, fk_dim_person,
          fk_dim_kjonn, fk_dim_sivilstatus, fk_dim_land_fodt,
          fk_dim_geografi_bosted, fk_dim_land_statsborgerskap, fk_dim_tid_mnd,
          fk_dim_alder, fkby_person1, antbarn, belophie,
          belophit, ybarn, eoybland, kilde, stat_aarmnd, lastet_dato
         ,prosent, statusk, belop_utvidet, belophit_utvidet, fk_bt_fagsak, behandling_årsak
         ,gyldig_flagg
        )
        VALUES
        (
          rec_mottaker.gjelder_mottaker, rec_mottaker.belop, rec_mottaker.kjonn, rec_mottaker.belope,
          rec_mottaker.fodsel_aar, rec_mottaker.fodsel_mnd, rec_mottaker.tknr, rec_mottaker.bosted_kommune_nr,
          rec_mottaker.bydel_kommune_nr, rec_mottaker.fagsak_id, rec_mottaker.pk_dim_person,
          rec_mottaker.fk_dim_kjonn, rec_mottaker.fk_dim_sivilstatus, rec_mottaker.fk_dim_land_fodt,
          rec_mottaker.fk_dim_geografi_bosted, rec_mottaker.fk_dim_land_statsborgerskap, rec_mottaker.pk_dim_tid,
          rec_mottaker.pk_dim_alder, rec_mottaker.fkby_person1, rec_mottaker.antbarn, rec_mottaker.belophie,
          rec_mottaker.belophit, rec_mottaker.alderyb, rec_mottaker.ssb_kode, v_kilde, p_in_period, sysdate
         ,rec_mottaker.delingsprosent_ytelse, rec_mottaker.statusk, rec_mottaker.belop_utvidet, rec_mottaker.belophit_utvidet
         ,rec_mottaker.pk_bt_fagsak, rec_mottaker.behandling_årsak
         ,p_in_gyldig_flagg
        );
        l_commit := l_commit + 1;
      EXCEPTION
        WHEN OTHERS THEN
          l_error_melding := substr(SQLCODE || ' ' || sqlerrm, 1, 1000);
          INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
          VALUES(NULL, rec_mottaker.fagsak_id, l_error_melding, sysdate, 'FAM_BT_MOTTAKER_INSERT_WITH2');
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
      VALUES(NULL, l_error_melding, sysdate, 'FAM_BT_MOTTAKER_INSERT_WITH3');
      COMMIT;
      p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      l_error_melding := substr(SQLCODE || ' ' || sqlerrm, 1, 1000);
      INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
      VALUES(NULL, NULL, l_error_melding, sysdate, 'FAM_BT_MOTTAKER_INSERT_WITH4');
      COMMIT;
      p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
  END fam_bt_mottaker_insert_2;


  PROCEDURE fam_bt_mottaker_insert(p_in_period IN VARCHAR2
                                  ,p_in_gyldig_flagg in number default 0
                                  ,p_error_melding OUT VARCHAR2) AS
    v_aar_start VARCHAR2(6):= substr(p_in_period,1,4) || '01';
    v_kilde VARCHAR2(10) := 'BT';
    --v_storedate DATE := sysdate;
    l_error_melding VARCHAR2(1000);

    l_commit NUMBER := 0;

    CURSOR cur_mottaker IS
      SELECT /*+ PARALLEL(8) */ data.*,
             belope + belophie_b as belophie,
             belop + belophit_b as belophit
            ,belop_utvidet + belophit_utvidet_b as belophit_utvidet
            ,case when fagsak_type = 'INSTITUSJON' then 4
                  when underkategori = 'ORDINÆR' then 1
                  when underkategori = 'UTVIDET' then 2

             end statusk
      FROM
      (
        SELECT ur.gjelder_mottaker,
               --to_char(ur.posteringsdato,'YYYYMM') periode,
               SUM(ur.belop_sign) belop,
               dim_kjonn_mottaker.kjonn_kode kjonn,
               SUM(CASE WHEN to_char(ur.posteringsdato,'YYYYMM') > to_char(ur.dato_utbet_fom,'YYYYMM') THEN
                             ur.belop_sign
                        ELSE 0.0
                   END) belope,
               EXTRACT( YEAR FROM dim_person_mottaker.fodt_dato) fodsel_aar,
               to_char(dim_person_mottaker.fodt_dato,'MM') fodsel_mnd,
               dim_person_mottaker.tknr,
               dim_person_mottaker.bosted_kommune_nr,
               dim_geografi.bydel_kommune_nr,
               fagsak.fagsak_id,
               max(fagsak.pk_bt_fagsak) keep (dense_rank first order by fagsak.tidspunkt_vedtak desc) as pk_bt_fagsak,
               max(fagsak.behandling_årsak) keep (dense_rank first order by fagsak.tidspunkt_vedtak desc) as behandling_årsak,
               max(fagsak.underkategori) keep (dense_rank first order by fagsak.tidspunkt_vedtak desc) as underkategori,
               max(fagsak.fagsak_type) keep (dense_rank first order by fagsak.tidspunkt_vedtak desc) as fagsak_type,
               --fagsak.enslig_forsørger,
               dim_person_mottaker.pk_dim_person,
               dim_person_mottaker.fk_dim_kjonn,
               dim_person_mottaker.fk_dim_sivilstatus,
               dim_person_mottaker.fk_dim_land_fodt,
               dim_person_mottaker.fk_dim_geografi_bosted,
               dim_person_mottaker.fk_dim_land_statsborgerskap,
               fam_bt_periode.pk_dim_tid,
               fam_bt_periode.siste_dato_i_perioden,
               dim_alder.pk_dim_alder,
               barn.fkby_person1,
               barn.antbarn, barn.delingsprosent_ytelse,
               TRUNC(months_between(fam_bt_periode.siste_dato_i_perioden,dim_person_barn.fodt_dato)/12) alderyb,
               dim_land_barn.land_ssb_kode ssb_kode,
               nvl(belop.belophie,0) belophie_b,
               nvl(belop.belophit,0) belophit_b
              ,sum((case when utvidet_barnetrygd.delytelse_id is not null then 1
                         else 0
                    end) * ur.belop_sign) belop_utvidet
              ,nvl(belophit_utvidet.belophit_utvidet,0) belophit_utvidet_b,
               max(fam_bt_kompetanse_perioder.SOKERSAKTIVITET) sokers_aktivitet,
               max(fam_bt_kompetanse_perioder.ANNENFORELDER_AKTIVITET) annenforelder_aktivitet,
               max(fam_bt_kompetanse_perioder.KOMPETANSE_RESULTAT) kompetanse_resultat,
               min(CASE WHEN  fam_bt_kompetanse_perioder.KOMPETANSE_RESULTAT= 'NORGE_ER_PRIMÆRLAND' THEN '000'
                     WHEN  fam_bt_kompetanse_perioder.KOMPETANSE_RESULTAT= 'NORGE_ER_SEKUNDÆRLAND' THEN  DIM_LAND_ANNEN_FORELDER_AKTIVITET.LAND_SSB_KODE
                 ELSE NULL
               END) eokland,
               min(CASE WHEN fam_bt_kompetanse_perioder.KOMPETANSE_RESULTAT= 'NORGE_ER_PRIMÆRLAND' THEN  DIM_LAND_ANNEN_FORELDER_AKTIVITET.LAND_SSB_KODE
		       WHEN fam_bt_kompetanse_perioder.KOMPETANSE_RESULTAT= 'NORGE_ER_SEKUNDÆRLAND' THEN '000'
                    ELSE NULL
               END) eodland,
               dim_person_mottaker.GT_VERDI PERSON_GT_VERDI,
               MAX(fam_bt_kompetanse_perioder.BARNETS_BOSTEDSLAND) BARNETS_BOSTEDSLAND,
               MAX(fam_bt_kompetanse_perioder.PK_BT_KOMPETANSE_PERIODER) FK_BT_KOMPETANSE_PERIODER

        FROM dvh_fam_bt.fam_bt_ur_utbetaling ur
               --  dvh_fam_bt.FAM_BT_UR_202302 ur
        LEFT JOIN dvh_fam_bt.fam_bt_fagsak fagsak
        ON fagsak.behandlings_id = ur.henvisning

        LEFT OUTER JOIN dt_kodeverk.dim_tid fam_bt_periode
        ON to_char(ur.posteringsdato,'YYYYMM') = fam_bt_periode.aar_maaned
        AND fam_bt_periode.dim_nivaa = 3
        AND fam_bt_periode.gyldig_flagg = 1

        LEFT OUTER JOIN dt_person.dim_person dim_person_mottaker
        ON dim_person_mottaker.fk_person1 = ur.gjelder_mottaker
        AND dim_person_mottaker.gyldig_fra_dato <= fam_bt_periode.siste_dato_i_perioden
        AND dim_person_mottaker.gyldig_til_dato >= fam_bt_periode.siste_dato_i_perioden
        LEFT OUTER JOIN dt_kodeverk.dim_geografi dim_geografi
        ON dim_person_mottaker.fk_dim_geografi_bosted = dim_geografi.pk_dim_geografi
        LEFT OUTER JOIN dt_kodeverk.dim_kjonn dim_kjonn_mottaker
        ON dim_person_mottaker.fk_dim_kjonn = dim_kjonn_mottaker.pk_dim_kjonn

        LEFT OUTER JOIN dt_kodeverk.dim_alder
        ON floor(months_between(sysdate, dim_person_mottaker.fodt_dato)/12) = dim_alder.alder
        AND dim_alder.gyldig_fra_dato <= fam_bt_periode.siste_dato_i_perioden
        AND dim_alder.gyldig_til_dato >= fam_bt_periode.siste_dato_i_perioden

        LEFT OUTER JOIN --dvh_fam_bt.fam_bt_kompetanse_perioder
       (
         SELECT MAX(pk_bt_kompetanse_perioder) pk_bt_kompetanse_perioder ,
         fom,
         tom,
        -- sokersaktivitet,
        -- annenforelder_aktivitet,
        -- annenforelder_aktivitetsland,
        -- kompetanse_resultat,
        -- barnets_bostedsland,
         fk_bt_fagsak--,
        -- sokers_aktivitetsland
         FROM dvh_fam_bt.fam_bt_kompetanse_perioder
         group by fom,
         tom,
         --sokersaktivitet,
         --annenforelder_aktivitet,
         --annenforelder_aktivitetsland,
         --kompetanse_resultat,
         --barnets_bostedsland,
         fk_bt_fagsak--,
         --sokers_aktivitetsland

         )   kompetanse_perioder  ON
        fagsak.pk_bt_fagsak=kompetanse_perioder.FK_BT_FAGSAK AND
        fagsak.KATEGORI='EØS' AND
        ur.dato_utbet_fom>=to_Date(kompetanse_perioder.fom,'YYYY-MM')
        AND ur.dato_utbet_fom<=to_Date(nvl(kompetanse_perioder.tom,'2099-12'),'YYYY-MM')

        left outer join dvh_fam_bt.fam_bt_kompetanse_perioder on
        kompetanse_perioder.pk_bt_kompetanse_perioder = dvh_fam_bt.fam_bt_kompetanse_perioder.pk_bt_kompetanse_perioder


        LEFT OUTER JOIN dt_kodeverk.dim_land DIM_LAND_ANNEN_FORELDER_AKTIVITET ON
        DIM_LAND_ANNEN_FORELDER_AKTIVITET.LAND_ISO_2_KODE=fam_bt_kompetanse_perioder.ANNENFORELDER_AKTIVITETSLAND
        AND DIM_LAND_ANNEN_FORELDER_AKTIVITET.gyldig_flagg=1
        AND DIM_LAND_ANNEN_FORELDER_AKTIVITET.LAND_ISO_3_KODE!='ESC'

        LEFT JOIN
        (
          SELECT fk_person1,
                 substr(MAX(fodsel_aar_barn||fodsel_mnd_barn||'-'||fkb_person1),8) fkby_person1,
                 max(delingsprosent_ytelse) as delingsprosent_ytelse,
                 COUNT(DISTINCT fkb_person1) antbarn
          FROM dvh_fam_bt.fam_bt_barn
          WHERE kilde = v_kilde
          AND stat_aarmnd = p_in_period
          and gyldig_flagg = p_in_gyldig_flagg
          GROUP BY fk_person1
        ) barn
        ON ur.gjelder_mottaker = barn.fk_person1

        LEFT JOIN dt_person.dim_person dim_person_barn
        ON dim_person_barn.fk_person1 = barn.fkby_person1
        AND dim_person_barn.gyldig_fra_dato <= fam_bt_periode.siste_dato_i_perioden
        AND dim_person_barn.gyldig_til_dato >= fam_bt_periode.siste_dato_i_perioden

        LEFT OUTER JOIN dt_kodeverk.dim_land dim_land_barn
        ON dim_person_barn.fk_dim_land_bosted = dim_land_barn.pk_dim_land
        --AND dim_land.gyldig_fra_dato <= fam_bt_periode.siste_dato_i_perioden
        --AND dim_land.gyldig_til_dato >= fam_bt_periode.siste_dato_i_perioden

        LEFT JOIN
        (
          SELECT fk_person1,
                 SUM(nvl(belop,0)) belophit,
                 SUM(nvl(belope,0)) belophie
          FROM dvh_fam_bt.fam_bt_mottaker
          WHERE stat_aarmnd >= v_aar_start
          AND stat_aarmnd < p_in_period
          and gyldig_flagg = p_in_gyldig_flagg
          GROUP BY fk_person1
        ) belop
        ON ur.gjelder_mottaker = belop.fk_person1

        --Utvidet barnetrygd
        left join
        (select utbet.behandlings_id, utbet.delytelse_id, utbetaling.stønad_fom, utbetaling.stønad_tom
         from dvh_fam_bt.fam_bt_utbet_det utbet
         join dvh_fam_bt.fam_bt_person person
         on utbet.fk_bt_person = person.pk_bt_person
         and person.rolle = 'SØKER'
         join dvh_fam_bt.fam_bt_utbetaling utbetaling
         on utbet.fk_bt_utbetaling = utbetaling.pk_bt_utbetaling
         join dvh_fam_bt.fam_bt_fagsak fagsak
         on utbetaling.behandlings_id = fagsak.behandlings_id
         and fagsak.enslig_forsørger = 1) utvidet_barnetrygd
        on ur.henvisning = utvidet_barnetrygd.behandlings_id
        and ur.delytelse_id = utvidet_barnetrygd.delytelse_id
        and utvidet_barnetrygd.stønad_fom <= fam_bt_periode.siste_dato_i_perioden
        and utvidet_barnetrygd.stønad_tom >= fam_bt_periode.siste_dato_i_perioden
        left join
        (select fk_person1,
                sum(nvl(belop_utvidet,0)) belophit_utvidet
         from dvh_fam_bt.fam_bt_mottaker
         where stat_aarmnd >= v_aar_start
         and stat_aarmnd < p_in_period
         and gyldig_flagg = p_in_gyldig_flagg
         group by fk_person1
        ) belophit_utvidet
        ON ur.gjelder_mottaker = belophit_utvidet.fk_person1

        WHERE ur.hovedkontonr = 800--in (800, 214, 216, 215)-- = 800 Test!!!
        AND to_char(ur.posteringsdato,'YYYYMM') = p_in_period
        --AND SLETT=0
        --AND UR.FK_PERSON1=1073261799
        --AND LENGTH(ur.delytelse_id) < 14
        GROUP BY ur.gjelder_mottaker, to_char(ur.posteringsdato,'YYYYMM'), dim_kjonn_mottaker.kjonn_kode,
                 fam_bt_periode.siste_dato_i_perioden, EXTRACT( YEAR FROM dim_person_mottaker.fodt_dato),
                 to_char(dim_person_mottaker.fodt_dato,'MM'), dim_person_mottaker.tknr,
                 dim_person_mottaker.bosted_kommune_nr, dim_geografi.kommune_nr,
                 dim_geografi.bydel_kommune_nr, fagsak.fagsak_id
                ,dim_person_mottaker.pk_dim_person,
                 dim_person_mottaker.fk_dim_kjonn, dim_person_mottaker.fk_dim_sivilstatus,
                 dim_person_mottaker.fk_dim_land_fodt, dim_person_mottaker.fk_dim_geografi_bosted,
                 dim_person_mottaker.fk_dim_land_statsborgerskap, fam_bt_periode.pk_dim_tid,
                 dim_alder.pk_dim_alder,barn.fkby_person1,barn.antbarn,barn.delingsprosent_ytelse,
                 TRUNC(months_between(fam_bt_periode.siste_dato_i_perioden,dim_person_barn.fodt_dato)/12),
                 dim_land_barn.land_ssb_kode,nvl(belop.belophie,0),nvl(belop.belophit,0)
                ,nvl(belophit_utvidet.belophit_utvidet,0)
                ,dim_person_mottaker.GT_VERDI
        having sum(ur.belop_sign) != 0
      ) data;
  BEGIN

    -- Slett mottakere dvh_fam_fp.fam_bt_mottaker_hist for aktuell periode
    BEGIN
      DELETE FROM dvh_fam_bt.fam_bt_mottaker--r152241.test_drp
      WHERE kilde = v_kilde
      AND stat_aarmnd= p_in_period
      and gyldig_flagg = p_in_gyldig_flagg;
      COMMIT;
    EXCEPTION
      WHEN OTHERS THEN
        l_error_melding := substr(SQLCODE || ' ' || sqlerrm, 1, 1000);
        INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
        VALUES(NULL, NULL, l_error_melding, sysdate, 'FAM_BT_MOTTAKER_INSERT_WITH1');
        COMMIT;
        p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
        l_error_melding := NULL;
    END;

    FOR rec_mottaker IN cur_mottaker LOOP
      --INSERT INTO dvh_fam_fp.fam_bt_mottaker
      BEGIN
        INSERT INTO dvh_fam_bt.fam_bt_mottaker--R152241.test_drp
        (
          fk_person1, belop, kjonn, belope,
          fodsel_aar, fodsel_mnd, tknr, bosted_kommune_nr,
          bosted_bydel_kommune_nr, fagsak_id, fk_dim_person,
          fk_dim_kjonn, fk_dim_sivilstatus, fk_dim_land_fodt,
          fk_dim_geografi_bosted, fk_dim_land_statsborgerskap, fk_dim_tid_mnd,
          fk_dim_alder, fkby_person1, antbarn, belophie,
          belophit, ybarn, eoybland, kilde, stat_aarmnd, lastet_dato
         ,prosent, statusk, belop_utvidet, belophit_utvidet, fk_bt_fagsak, behandling_årsak
         ,gyldig_flagg
         ,eokland
         ,eodland
         --,SOKERS_AKTIVITET
         --,ANNENFORELDER_AKTIVITET
         --,KOMPETANSE_RESULTAT
         --,BARNETS_BOSTEDSLAND
         ,PERSON_GT_VERDI
         ,FK_BT_KOMPETANSE_PERIODER
        )
        VALUES
        (
          rec_mottaker.gjelder_mottaker, rec_mottaker.belop, rec_mottaker.kjonn, rec_mottaker.belope,
          rec_mottaker.fodsel_aar, rec_mottaker.fodsel_mnd, rec_mottaker.tknr, rec_mottaker.bosted_kommune_nr,
          rec_mottaker.bydel_kommune_nr, rec_mottaker.fagsak_id, rec_mottaker.pk_dim_person,
          rec_mottaker.fk_dim_kjonn, rec_mottaker.fk_dim_sivilstatus, rec_mottaker.fk_dim_land_fodt,
          rec_mottaker.fk_dim_geografi_bosted, rec_mottaker.fk_dim_land_statsborgerskap, rec_mottaker.pk_dim_tid,
          rec_mottaker.pk_dim_alder, rec_mottaker.fkby_person1, rec_mottaker.antbarn, rec_mottaker.belophie,
          rec_mottaker.belophit, rec_mottaker.alderyb, rec_mottaker.ssb_kode, v_kilde, p_in_period, sysdate
         ,rec_mottaker.delingsprosent_ytelse, rec_mottaker.statusk, rec_mottaker.belop_utvidet, rec_mottaker.belophit_utvidet
         ,rec_mottaker.pk_bt_fagsak, rec_mottaker.behandling_årsak
         ,p_in_gyldig_flagg
         ,rec_mottaker.eokland
         ,rec_mottaker.eodland
         --,rec_mottaker.sokers_aktivitet
         --,rec_mottaker.annenforelder_aktivitet
         --,rec_mottaker.kompetanse_resultat
         --,rec_mottaker.barnets_bostedsland
         ,rec_mottaker.person_gt_verdi
         ,rec_mottaker.fk_bt_kompetanse_perioder

        );
        l_commit := l_commit + 1;
      EXCEPTION
        WHEN OTHERS THEN
          l_error_melding := substr(SQLCODE || ' ' || sqlerrm, 1, 1000);
          INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
          VALUES(NULL, rec_mottaker.fagsak_id, l_error_melding, sysdate, 'FAM_BT_MOTTAKER_INSERT_WITH2');
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
      VALUES(NULL, l_error_melding, sysdate, 'FAM_BT_MOTTAKER_INSERT_WITH3');
      COMMIT;
      p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      l_error_melding := substr(SQLCODE || ' ' || sqlerrm, 1, 1000);
      INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
      VALUES(NULL, NULL, l_error_melding, sysdate, 'FAM_BT_MOTTAKER_INSERT_WITH4');
      COMMIT;
      p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
  END fam_bt_mottaker_insert;

  PROCEDURE fam_bt_barn_insert_bck(p_in_period IN VARCHAR2, p_error_melding OUT VARCHAR2) AS
    v_kilde VARCHAR2(10) := 'BT';
    v_storedate DATE := sysdate;
    l_error_melding VARCHAR2(1000);
    l_commit NUMBER := 0;

    CURSOR cur_barn(v_in_period VARCHAR2) IS
      SELECT /*+ PARALLEL(8) */ barn.fk_person1 fkb_person1,
             barn.delingsprosent_ytelse,
             dim_person_mottaker.fk_person1,
             NULL inst,
             EXTRACT (YEAR FROM dim_person_mottaker.fodt_dato) fodsel_aar,
             to_char(dim_person_mottaker.fodt_dato,'MM') fodsel_mnd,
             dim_kjonn_mottaker.kjonn_kode kjonn,
             EXTRACT (YEAR FROM dim_person_barn.fodt_dato) fodsel_aar_barn,
             to_char(dim_person_barn.fodt_dato,'MM') fodsel_mnd_barn,
             dim_kjonn_barn.kjonn_kode kjonn_barn,
             fam_bt_periode.aar_maaned stat_aarmnd,
             fagsak.pk_bt_fagsak fk_bt_fagsak
             --,fagsak.fagsak_id
      FROM dvh_fam_bt.fam_bt_ur_utbetaling ur
      JOIN dvh_fam_bt.fam_bt_utbet_det det ON
      ur.delytelse_id = det.delytelse_id
      --and det.delytelse_id = 10364905
      JOIN dvh_fam_bt.fam_bt_utbetaling perut ON
      perut.pk_bt_utbetaling = det.fk_bt_utbetaling
      AND perut.stønad_fom <= ur.dato_utbet_fom
      AND perut.stønad_tom >= ur.dato_utbet_tom

      JOIN dvh_fam_bt.fam_bt_person barn ON
      barn.pk_bt_person = det.fk_bt_person AND
      barn.rolle = 'BARN'

      JOIN dvh_fam_bt.fam_bt_fagsak fagsak ON
      fagsak.pk_bt_fagsak = perut.fk_bt_fagsak
      AND fagsak.behandlings_id = ur.henvisning

      LEFT OUTER JOIN dt_kodeverk.dim_tid fam_bt_periode ON
      to_char(ur.posteringsdato,'YYYYMM') = fam_bt_periode.aar_maaned
      AND fam_bt_periode.dim_nivaa = 3
      AND fam_bt_periode.gyldig_flagg = 1

      LEFT OUTER JOIN dt_person.dim_person dim_person_barn ON
      dim_person_barn.fk_person1 = barn.fk_person1
      AND dim_person_barn.gyldig_fra_dato <= fam_bt_periode.siste_dato_i_perioden
      AND dim_person_barn.gyldig_til_dato >= fam_bt_periode.siste_dato_i_perioden

      LEFT OUTER JOIN dt_kodeverk.dim_kjonn dim_kjonn_barn ON
      dim_person_barn.fk_dim_kjonn = dim_kjonn_barn.pk_dim_kjonn
      --AND dim_kjonn_barn.gyldig_fra_dato < fam_bt_periode.siste_dato_i_perioden
      --AND dim_kjonn_barn.gyldig_til_dato > fam_bt_periode.siste_dato_i_perioden

      JOIN dt_person.dim_person dim_person_mottaker ON
      dim_person_mottaker.fk_person1 = ur.gjelder_mottaker
      AND dim_person_mottaker.gyldig_fra_dato <= fam_bt_periode.siste_dato_i_perioden
      AND dim_person_mottaker.gyldig_til_dato >= fam_bt_periode.siste_dato_i_perioden

      LEFT OUTER JOIN dt_kodeverk.dim_kjonn dim_kjonn_mottaker ON
      dim_person_mottaker.fk_dim_kjonn = dim_kjonn_mottaker.pk_dim_kjonn
      --AND dim_kjonn_mottaker.gyldig_fra_dato < fam_bt_periode.siste_dato_i_perioden
      --AND dim_kjonn_mottaker.gyldig_til_dato > fam_bt_periode.siste_dato_i_perioden

      WHERE ur.hovedkontonr = 800
      --AND ur.status != 11
      AND to_char(ur.posteringsdato,'YYYYMM') = v_in_period
      --AND LENGTH(ur.delytelse_id) < 14

      GROUP BY barn.fk_person1,barn.delingsprosent_ytelse,
               dim_person_mottaker.fk_person1,
               EXTRACT (YEAR FROM dim_person_mottaker.fodt_dato),
               to_char(dim_person_mottaker.fodt_dato,'MM'),
               dim_kjonn_mottaker.kjonn_kode,
               EXTRACT (YEAR FROM dim_person_barn.fodt_dato),
               to_char(dim_person_barn.fodt_dato,'MM'),
               dim_kjonn_barn.kjonn_kode,
               fam_bt_periode.aar_maaned, fagsak.pk_bt_fagsak;
  BEGIN
    -- Slett data i dvh_fam_fp.fam_bt_barn for aktiuell periode (egen prosedyre)
    BEGIN
      DELETE FROM dvh_fam_bt.fam_bt_barn
      WHERE kilde = v_kilde
      AND stat_aarmnd = p_in_period;
      COMMIT;
    EXCEPTION
      WHEN OTHERS THEN
        l_error_melding := SQLCODE || ' ' || sqlerrm;
        INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
        VALUES(NULL, NULL, l_error_melding, v_storedate, 'FAM_BT_BARN_INSERT1');
        COMMIT;
        p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
    END;

    -- Patch opp fk_person1
    begin
      merge into dvh_fam_bt.fam_bt_person
      using (
              select off_id
                    ,max(fk_person1) keep
                               (dense_rank first order by gyldig_fra_dato desc) as fk_person1
              from dt_person.dvh_person_ident_off_id_ikke_skjermet
              group by off_id
            ) person_67_vasket
      on (person_67_vasket.off_id = fam_bt_person.person_ident)
      when matched then update set fam_bt_person.fk_person1 = person_67_vasket.fk_person1
      where fam_bt_person.fk_person1 = -1;
      commit;
    exception
      when others then
        l_error_melding := sqlcode || ' ' || sqlerrm;
        insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
        values(null, null, l_error_melding, v_storedate, 'FAM_BT_BARN_INSERT: Patch opp fk_person1');
        commit;
        p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
    end;

    -- Slett person_ident
    begin
      update dvh_fam_bt.fam_bt_person
      set person_ident = null
      where fk_person1 != -1
      and person_ident is not null;
      commit;
    exception
      when others then
        l_error_melding := sqlcode || ' ' || sqlerrm;
        insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
        values(null, null, l_error_melding, v_storedate, 'FAM_BT_BARN_INSERT: Slett person_ident');
        commit;
        p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
    end;

    FOR rec_barn IN cur_barn(p_in_period) LOOP
      --dbms_output.put_line(rec_barn.fkb_person1);--Test
      BEGIN
        INSERT INTO dvh_fam_bt.fam_bt_barn
        (
          fkb_person1,
          fk_person1,
          inst,
          fodsel_aar,
          fodsel_mnd,
          kjonn,
          fodsel_aar_barn,
          fodsel_mnd_barn,
          kjonn_barn,
          delingsprosent_ytelse,
          stat_aarmnd,
          kilde,
          lastet_dato,
          fk_bt_fagsak
        )
        VALUES
        (
          rec_barn.fkb_person1,
          rec_barn.fk_person1,
          rec_barn.inst,
          rec_barn.fodsel_aar,
          rec_barn.fodsel_mnd,
          rec_barn.kjonn,
          rec_barn.fodsel_aar_barn,
          rec_barn.fodsel_mnd_barn,
          rec_barn.kjonn_barn,
          rec_barn.delingsprosent_ytelse,
          rec_barn.stat_aarmnd,
          v_kilde,
          sysdate,--v_storedate
          rec_barn.fk_bt_fagsak
        );
        l_commit := l_commit + 1;
      EXCEPTION
        WHEN OTHERS THEN
          l_error_melding := SQLCODE || ' ' || sqlerrm;
          INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
          VALUES(NULL, rec_barn.fkb_person1, l_error_melding, v_storedate, 'FAM_BT_BARN_INSERT2');
          l_commit := l_commit + 1;--Gå videre til neste rekord
          p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
      END;
      --rollback;--Test
      IF l_commit >= 100000 THEN
        COMMIT;
        l_commit := 0;
      END IF;
    END LOOP;
    COMMIT;--commit til slutt
    IF l_error_melding IS NOT NULL THEN
      INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(ID, error_msg, opprettet_tid, kilde)
      VALUES(NULL, l_error_melding, v_storedate, 'FAM_BT_BARN_INSERT3');
      COMMIT;
      p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      l_error_melding := SQLCODE || ' ' || sqlerrm;
      INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
      VALUES(NULL, NULL, l_error_melding, v_storedate, 'FAM_BT_BARN_INSERT4');
      COMMIT;
      p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
  END fam_bt_barn_insert_bck;

  PROCEDURE fam_bt_barn_insert(p_in_period IN VARCHAR2
                              ,p_in_gyldig_flagg in number default 0
                              ,p_error_melding OUT VARCHAR2) AS
    v_kilde VARCHAR2(10) := 'BT';
    v_storedate DATE := sysdate;
    l_error_melding VARCHAR2(1000);
    l_commit NUMBER := 0;

    CURSOR cur_barn(v_in_period VARCHAR2) IS
      with ur as
      (
        select /*+ PARALLEL(8) */ ur.gjelder_mottaker
              ,max(ur.dato_utbet_tom) as max_dato_utbet_tom, max(ur.posteringsdato) as max_posteringsdato
              ,max(det.fk_bt_person) keep (dense_rank first order by ur.dato_utbet_tom desc) as max_fk_bt_person
              ,max(ur.henvisning) keep (dense_rank first order by ur.henvisning desc) as henvisning
        from dvh_fam_bt.fam_bt_ur_utbetaling ur
        join dvh_fam_bt.fam_bt_utbet_det det
        on ur.delytelse_id = det.delytelse_id
        join dvh_fam_bt.fam_bt_utbetaling utbetaling
        on det.fk_bt_utbetaling = utbetaling.pk_bt_utbetaling
        and utbetaling.stønad_fom <= ur.dato_utbet_fom
        and utbetaling.stønad_tom >= ur.dato_utbet_tom


        join dvh_fam_bt.fam_bt_person barn
        on barn.pk_bt_person = det.fk_bt_person
        and barn.rolle = 'BARN'

        where to_char(ur.posteringsdato,'YYYYMM') = v_in_period
        group by to_char(ur.posteringsdato,'YYYYMM'), ur.gjelder_mottaker, barn.fk_person1
        having sum(belop_sign) != 0
      ),
      vedtak as
      (
        select /*+ PARALLEL(8) */ barn.fk_person1 as fkb_person1, barn.delingsprosent_ytelse
              ,dim_person_mottaker.fk_person1, null inst
              ,extract (year from dim_person_mottaker.fodt_dato) fodsel_aar
              ,to_char(dim_person_mottaker.fodt_dato,'MM') fodsel_mnd
              ,dim_kjonn_mottaker.kjonn_kode kjonn
              ,extract (year from dim_person_barn.fodt_dato) fodsel_aar_barn
              ,to_char(dim_person_barn.fodt_dato,'MM') fodsel_mnd_barn
              ,dim_kjonn_barn.kjonn_kode kjonn_barn
              ,fam_bt_periode.aar_maaned stat_aarmnd
              ,dim_person_barn.PK_DIM_PERSON FK_DIM_PERSON_BARN
             ,fagsak.pk_bt_fagsak fk_bt_fagsak
        from ur

        join dvh_fam_bt.fam_bt_person barn
        on barn.pk_bt_person = ur.max_fk_bt_person


        JOIN dvh_fam_bt.fam_bt_fagsak fagsak ON
            fagsak.behandlings_id = ur.henvisning

        left outer join dt_kodeverk.dim_tid fam_bt_periode
        on to_char(ur.max_posteringsdato,'YYYYMM') = fam_bt_periode.aar_maaned
        and fam_bt_periode.dim_nivaa = 3
        and fam_bt_periode.gyldig_flagg = 1

        left outer join dt_person.dim_person dim_person_barn
        on dim_person_barn.fk_person1 = barn.fk_person1
        and dim_person_barn.gyldig_fra_dato <= fam_bt_periode.siste_dato_i_perioden
        and dim_person_barn.gyldig_til_dato >= fam_bt_periode.siste_dato_i_perioden
        left outer join dt_kodeverk.dim_kjonn dim_kjonn_barn
        on dim_person_barn.fk_dim_kjonn = dim_kjonn_barn.pk_dim_kjonn

        join dt_person.dim_person dim_person_mottaker
        on dim_person_mottaker.fk_person1 = ur.gjelder_mottaker
        and dim_person_mottaker.gyldig_fra_dato <= fam_bt_periode.siste_dato_i_perioden
        and dim_person_mottaker.gyldig_til_dato >= fam_bt_periode.siste_dato_i_perioden
        left outer join dt_kodeverk.dim_kjonn dim_kjonn_mottaker
        on dim_person_mottaker.fk_dim_kjonn = dim_kjonn_mottaker.pk_dim_kjonn


        group by barn.fk_person1, barn.delingsprosent_ytelse, dim_person_mottaker.fk_person1
                ,dim_person_mottaker.fodt_dato, dim_person_barn.fodt_dato
                ,dim_kjonn_mottaker.kjonn_kode, dim_kjonn_barn.kjonn_kode, fam_bt_periode.aar_maaned ,dim_person_barn.PK_DIM_PERSON,fagsak.pk_bt_fagsak
      )
      select /*+ PARALLEL(8) */ vedtak.*
      from vedtak;
  BEGIN
    -- Slett data i dvh_fam_fp.fam_bt_barn for aktiuell periode (egen prosedyre)
    BEGIN
      DELETE FROM dvh_fam_bt.fam_bt_barn
      WHERE kilde = v_kilde
      AND stat_aarmnd = p_in_period
      and gyldig_flagg = p_in_gyldig_flagg;
      COMMIT;
    EXCEPTION
      WHEN OTHERS THEN
        l_error_melding := SQLCODE || ' ' || sqlerrm;
        INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
        VALUES(NULL, NULL, l_error_melding, v_storedate, 'FAM_BT_BARN_INSERT1');
        COMMIT;
        p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
    END;

    -- Patch opp fk_person1
    begin
      merge into dvh_fam_bt.fam_bt_person
      using (
              select off_id
                    ,max(fk_person1) keep
                               (dense_rank first order by gyldig_fra_dato desc) as fk_person1
                    ,max(gyldig_til_dato) keep
                               (dense_rank first order by gyldig_fra_dato desc) as gyldig_til_dato
              from dt_person.dvh_person_ident_off_id_ikke_skjermet
              group by off_id
            ) person_67_vasket
      on (person_67_vasket.off_id = fam_bt_person.person_ident)
      when matched then update set fam_bt_person.fk_person1 = person_67_vasket.fk_person1
                                  ,fam_bt_person.oppdatert_dato = sysdate
      where fam_bt_person.fk_person1 = -1
      and rolle = 'BARN'
      and fam_bt_person.lastet_dato <= person_67_vasket.gyldig_til_dato;
      commit;
    exception
      when others then
        l_error_melding := sqlcode || ' ' || sqlerrm;
        insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
        values(null, null, l_error_melding, v_storedate, 'FAM_BT_BARN_INSERT: Patch opp fk_person1');
        commit;
        p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
    end;

    -- Slett person_ident
    begin
      update dvh_fam_bt.fam_bt_person
      set person_ident = null
      where fk_person1 != -1
      and person_ident is not null;
      commit;
    exception
      when others then
        l_error_melding := sqlcode || ' ' || sqlerrm;
        insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
        values(null, null, l_error_melding, v_storedate, 'FAM_BT_BARN_INSERT: Slett person_ident');
        commit;
        p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
    end;

    FOR rec_barn IN cur_barn(p_in_period) LOOP
      --dbms_output.put_line(rec_barn.fkb_person1);--Test
      BEGIN
        INSERT INTO dvh_fam_bt.fam_bt_barn
        (
          fkb_person1,
          fk_person1,
          inst,
          fodsel_aar,
          fodsel_mnd,
          kjonn,
          fodsel_aar_barn,
          fodsel_mnd_barn,
          kjonn_barn,
          delingsprosent_ytelse,
          stat_aarmnd,
          kilde,
          lastet_dato,
          gyldig_flagg,
          fk_bt_fagsak,
          fk_dim_person_barn
        )
        VALUES
        (
          rec_barn.fkb_person1,
          rec_barn.fk_person1,
          rec_barn.inst,
          rec_barn.fodsel_aar,
          rec_barn.fodsel_mnd,
          rec_barn.kjonn,
          rec_barn.fodsel_aar_barn,
          rec_barn.fodsel_mnd_barn,
          rec_barn.kjonn_barn,
          rec_barn.delingsprosent_ytelse,
          rec_barn.stat_aarmnd,
          v_kilde,
          sysdate--v_storedate
         ,p_in_gyldig_flagg,
          rec_barn.fk_bt_fagsak,
          rec_barn.fk_dim_person_barn
        );
        l_commit := l_commit + 1;
      EXCEPTION
        WHEN OTHERS THEN
          l_error_melding := SQLCODE || ' ' || sqlerrm;
          INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
          VALUES(NULL, rec_barn.fkb_person1, l_error_melding, v_storedate, 'FAM_BT_BARN_INSERT2');
          l_commit := l_commit + 1;--Gå videre til neste rekord
          p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
      END;
      --rollback;--Test
      IF l_commit >= 100000 THEN
        COMMIT;
        l_commit := 0;
      END IF;
    END LOOP;
    COMMIT;--commit til slutt
    IF l_error_melding IS NOT NULL THEN
      INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(ID, error_msg, opprettet_tid, kilde)
      VALUES(NULL, l_error_melding, v_storedate, 'FAM_BT_BARN_INSERT3');
      COMMIT;
      p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
    END IF;



    MERGE into dvh_fam_bt.FAM_BT_BARN
    USING (
    SELECT
    A.PK_BT_BARN,A.STAT_AARMND,A.FK_PERSON1,A.FKB_PERSON1,D.PK_BT_KOMPETANSE_PERIODER FK_BT_KOMPETANSE_PERIODER
    FROM dvh_fam_bt.FAM_BT_BARN A


    JOIN dvh_fam_bt.FAM_BT_KOMPETANSE_PERIODER D ON
    A.FK_BT_FAGSAK=D.FK_BT_FAGSAK
    --AND D.PK_BT_KOMPETANSE_PERIODER=C.FK_BT_KOMPETANSE_PERIODER
    AND to_Date(A.STAT_AARMND,'YYYYMM')>=to_Date(D.fom,'YYYY-MM')
    AND to_date(A.STAT_AARMND,'YYYYMM')<=to_Date(nvl(D.tom,'2099-12'),'YYYY-MM')

    JOIN dvh_fam_bt.FAM_BT_KOMPETANSE_BARN C ON
    A.FKB_PERSON1=C.FK_PERSON1
    AND D.PK_BT_KOMPETANSE_PERIODER=C.FK_BT_KOMPETANSE_PERIODER

    WHERE A.STAT_AARMND=p_in_period
    AND A.GYLDIG_FLAGG=p_in_gyldig_flagg ) BARN ON
    (FAM_BT_BARN.PK_BT_BARN=BARN.PK_BT_BARN)
    WHEN MATCHED THEN UPDATE SET FAM_BT_BARN.FK_BT_KOMPETANSE_PERIODER = BARN.FK_BT_KOMPETANSE_PERIODER
    WHERE BARN.STAT_AARMND=p_in_period;
    COMMIT;



  EXCEPTION
    WHEN OTHERS THEN
      l_error_melding := SQLCODE || ' ' || sqlerrm;
      INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
      VALUES(NULL, NULL, l_error_melding, v_storedate, 'FAM_BT_BARN_INSERT4');
      COMMIT;
      p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
  END fam_bt_barn_insert;

  PROCEDURE fam_bt_mottaker_insert_bck(p_in_period IN VARCHAR2, p_error_melding OUT VARCHAR2) AS
    v_kilde VARCHAR2(10) := 'BT';
    v_storedate DATE := sysdate;
    v_aar_start VARCHAR2(6):= substr(p_in_period,1,4) || '01';
    v_fk_person1_yb VARCHAR2(40);
    v_fk_person1_eb VARCHAR2(40);
    v_ant_barn NUMBER;
    v_belophie NUMBER;
    v_belophit NUMBER;
    v_alderyb NUMBER;
    v_ssb_kode VARCHAR2(10);

    l_error_melding VARCHAR2(1000);
    l_commit NUMBER := 0;

    CURSOR cur_mottaker(v_in_period VARCHAR2) IS
      SELECT /*+ PARALLEL(8) */gjelder_mottaker,
             to_char(posteringsdato,'YYYYMM') periode,
             SUM(belop_sign) belop,--, DELYTELSE_ID
             dim_kjonn_mottaker.kjonn_kode kjonn,
             SUM(CASE WHEN to_char(posteringsdato,'YYYYMM') > to_char(dato_utbet_fom,'YYYYMM') THEN
                      belop_sign
                      ELSE 0.0
                 END) belope,
             fam_bt_periode.siste_dato_i_perioden,
             EXTRACT( YEAR FROM dim_person_mottaker.fodt_dato) fodsel_aar,
             to_char(dim_person_mottaker.fodt_dato,'MM') fodsel_mnd,
             --nvl(DIM_GEOGRAFI.KOMMUNE_NR, DIM_PERSON_MOTTAKER.BOSTED_KOMMUNE_NR) KOMMUNE_NR
             DIM_PERSON_MOTTAKER.tknr,
             DIM_PERSON_MOTTAKER.BOSTED_KOMMUNE_NR,
             dim_geografi.kommune_nr,
             --dim_geografi.bydel_nr,
             dim_geografi.bydel_kommune_nr,
             fagsak.fagsak_id,
             dim_person_mottaker.pk_dim_person,
             dim_person_mottaker.fk_dim_kjonn,
             dim_person_mottaker.fk_dim_sivilstatus,
             dim_person_mottaker.fk_dim_land_fodt,
             dim_person_mottaker.fk_dim_geografi_bosted,
             dim_person_mottaker.fk_dim_land_statsborgerskap,
             fam_bt_periode.pk_dim_tid,
             dim_alder.pk_dim_alder
      FROM dvh_fam_bt.fam_bt_ur_utbetaling ur
      LEFT JOIN dvh_fam_bt.fam_bt_fagsak fagsak
      ON fagsak.behandlings_id = ur.henvisning

      LEFT OUTER JOIN dt_kodeverk.dim_tid fam_bt_periode ON
      to_char(ur.posteringsdato,'YYYYMM') = fam_bt_periode.aar_maaned
      AND fam_bt_periode.dim_nivaa = 3
      AND fam_bt_periode.gyldig_flagg = 1

      LEFT OUTER JOIN dt_person.dim_person dim_person_mottaker ON
      dim_person_mottaker.fk_person1 = ur.gjelder_mottaker
      AND dim_person_mottaker.gyldig_fra_dato <= fam_bt_periode.siste_dato_i_perioden
      AND dim_person_mottaker.gyldig_til_dato >= fam_bt_periode.siste_dato_i_perioden

      LEFT OUTER JOIN dt_kodeverk.dim_geografi dim_geografi ON
      dim_person_mottaker.fk_dim_geografi_bosted = dim_geografi.pk_dim_geografi
      --AND dim_geografi.gyldig_fra_dato< fam_bt_periode.siste_dato_i_perioden
      --AND dim_geografi.gyldig_til_dato> fam_bt_periode.siste_dato_i_perioden

      LEFT OUTER JOIN dt_kodeverk.dim_kjonn dim_kjonn_mottaker ON
      dim_person_mottaker.fk_dim_kjonn=dim_kjonn_mottaker.pk_dim_kjonn
      --AND dim_kjonn_mottaker.gyldig_fra_dato< fam_bt_periode.siste_dato_i_perioden
      --AND dim_kjonn_mottaker.gyldig_til_dato> fam_bt_periode.siste_dato_i_perioden

      left outer join dt_kodeverk.dim_alder
      on floor(months_between(sysdate, dim_person_mottaker.fodt_dato)/12) = dim_alder.alder
      and dim_alder.gyldig_fra_dato <= fam_bt_periode.siste_dato_i_perioden
      and dim_alder.gyldig_til_dato >= fam_bt_periode.siste_dato_i_perioden

      WHERE hovedkontonr = 800
      AND to_char(posteringsdato,'YYYYMM') = v_in_period
      AND LENGTH(delytelse_id) < 14
      GROUP BY gjelder_mottaker,
               to_char(posteringsdato,'YYYYMM'),
               dim_kjonn_mottaker.kjonn_kode,
               fam_bt_periode.siste_dato_i_perioden,
               EXTRACT( YEAR FROM dim_person_mottaker.fodt_dato),
               to_char(dim_person_mottaker.fodt_dato,'MM'),
               -- nvl(DIM_GEOGRAFI.KOMMUNE_NR, DIM_PERSON_MOTTAKER.BOSTED_KOMMUNE_NR)
               DIM_PERSON_MOTTAKER.tknr,
               DIM_PERSON_MOTTAKER.BOSTED_KOMMUNE_NR,
               dim_geografi.kommune_nr,
               dim_geografi.bydel_kommune_nr,
               fagsak.fagsak_id,
               dim_person_mottaker.pk_dim_person,
               dim_person_mottaker.fk_dim_kjonn,
               dim_person_mottaker.fk_dim_sivilstatus,
               dim_person_mottaker.fk_dim_land_fodt,
               dim_person_mottaker.fk_dim_geografi_bosted,
               dim_person_mottaker.fk_dim_land_statsborgerskap,
               fam_bt_periode.pk_dim_tid,
               dim_alder.pk_dim_alder;
  BEGIN
    -- Slett mottakere dvh_fam_fp.fam_bt_mottaker_hist for aktuell periode
    BEGIN
      DELETE FROM dvh_fam_bt.fam_bt_mottaker
      WHERE kilde = v_kilde
      AND stat_aarmnd= p_in_period;
      COMMIT;
    EXCEPTION
      WHEN OTHERS THEN
        l_error_melding := SQLCODE || ' ' || sqlerrm;
        INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
        VALUES(NULL, NULL, l_error_melding, v_storedate, 'FAM_BT_MOTTAKER_INSERT1');
        COMMIT;
        p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
    END;

    FOR rec_mottaker IN cur_mottaker(p_in_period) LOOP
      --Finn Fk_person1 for yngste og eldste barn, antall barn
      v_fk_person1_yb := null;
      v_fk_person1_eb := null;
      v_ant_barn := 0;

      --dbms_output.put_line('Antall barn'||sysdate);--Test!!!
      BEGIN
        SELECT substr(MIN(fodsel_aar_barn||fodsel_mnd_barn||'-'||fkb_person1),8,40),
               substr(MAX(fodsel_aar_barn||fodsel_mnd_barn||'-'||fkb_person1),8),
               COUNT(DISTINCT fkb_person1)
        INTO v_fk_person1_eb,
             v_fk_person1_yb,
             v_ant_barn
        FROM dvh_fam_bt.fam_bt_barn
        WHERE fk_person1 = rec_mottaker.gjelder_mottaker
        AND kilde = v_kilde
        AND stat_aarmnd= p_in_period;
        --dbms_output.put_line(rec_mottaker.gjelder_mottaker||'   '||v_fk_person1_yb);--Test
      EXCEPTION
        WHEN OTHERS THEN
          v_fk_person1_eb := NULL;
          v_fk_person1_yb := NULL;
          v_ant_barn := 0;
          l_error_melding := SQLCODE || ' ' || sqlerrm;
          INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
          VALUES(NULL, rec_mottaker.gjelder_mottaker, l_error_melding, v_storedate, 'FAM_BT_MOTTAKER_INSERT2');
          l_commit := l_commit + 1;
          p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
      END;

      -- Kalkulere beløp utbetalt hittil i år, sum for tidligere perioder inneværende år + denne periode
      -- Kalkulere beløp etterbetalt hittil i år, sum for tidligere perioder inneværende år + denne periode
      v_belophie := 0;
      v_belophit := 0;
      --IF (v_ant_barn > 0) THEN
      --dbms_output.put_line('Beløp'||sysdate);--Test!!!
        BEGIN
          SELECT nvl(SUM(mot.belop),0) + nvl(rec_mottaker.belop,0),
                 --nvl(MAX(mot.belophie),0) + nvl(rec_mottaker.belope,0)
                 nvl(SUM(mot.belope),0) + nvl(rec_mottaker.belope,0)
          INTO v_belophit,v_belophie
          FROM dvh_fam_bt.fam_bt_mottaker mot
          WHERE mot.stat_aarmnd >= v_aar_start
          AND mot.fk_person1 = rec_mottaker.gjelder_mottaker
          AND mot.stat_aarmnd < p_in_period;
        EXCEPTION
          WHEN OTHERS THEN
            v_belophit := 0;
            v_belophie := 0;
            l_error_melding := SQLCODE || ' ' || sqlerrm;
            INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
            VALUES(NULL, rec_mottaker.gjelder_mottaker, l_error_melding, v_storedate, 'FAM_BT_MOTTAKER_INSERT3');
            l_commit := l_commit + 1;
            p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
        END;
      --END IF;

      v_alderyb := 0;
      v_ssb_kode := null;
      -- Finn yngste barns alder, bostedsland
      IF (v_fk_person1_yb IS NOT NULL) THEN
      --dbms_output.put_line('Yngste barn'||sysdate);--Test!!!
        BEGIN
          SELECT TRUNC(months_between(rec_mottaker.siste_dato_i_perioden,dim_person_barn.fodt_dato)/12),
                 dim_land.land_ssb_kode
          INTO v_alderyb, v_ssb_kode
          FROM dt_person.dim_person dim_person_barn
          LEFT OUTER JOIN dt_kodeverk.dim_land dim_land ON
          dim_person_barn.fk_dim_land_bosted = dim_land.pk_dim_land
          AND dim_land.gyldig_fra_dato <= rec_mottaker.siste_dato_i_perioden
          AND dim_land.gyldig_til_dato >= rec_mottaker.siste_dato_i_perioden
          WHERE dim_person_barn.fk_person1 = v_fk_person1_yb
          AND dim_person_barn.gyldig_fra_dato <= rec_mottaker.siste_dato_i_perioden
          AND dim_person_barn.gyldig_til_dato >= rec_mottaker.siste_dato_i_perioden;
        EXCEPTION
          WHEN OTHERS THEN
            v_alderyb := 0;
            v_ssb_kode := NULL;
            l_error_melding := SQLCODE || ' ' || sqlerrm;
            INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
            VALUES(NULL, rec_mottaker.gjelder_mottaker, l_error_melding, v_storedate, 'FAM_BT_MOTTAKER_INSERT4');
            l_commit := l_commit + 1;
            p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
        END;
      END IF;
      --dbms_output.put_line('Insert'||sysdate);--Test!!!
      BEGIN
        INSERT INTO dvh_fam_bt.fam_bt_mottaker
        (
          fk_person1,
          belop,
          belophie,
          eoybland,
          antbarn,
          ybarn,
          kjonn,
          fodsel_aar,
          fodsel_mnd,
          belophit,
          tknr,
          bosted_kommune_nr,
          bosted_bydel_kommune_nr,
          stat_aarmnd,
          kilde,
          lastet_dato,
          fagsak_id,
          fkby_person1,
          belope,
          fk_dim_person,
          fk_dim_alder,
          fk_dim_kjonn,
          fk_dim_tid_mnd,
          fk_dim_sivilstatus,
          fk_dim_land_fodt,
          fk_dim_geografi_bosted,
          fk_dim_land_statsborgerskap
          )
        VALUES
        (
          rec_mottaker.gjelder_mottaker,
          rec_mottaker.belop,
          v_belophie,
          substr(v_ssb_kode,1,3),
          v_ant_barn,
          v_alderyb,
          rec_mottaker.kjonn,
          rec_mottaker.fodsel_aar,
          rec_mottaker.fodsel_mnd,
          v_belophit,
          --substr(rec_mottaker.kommune_nr,1,4),
          substr(rec_mottaker.tknr,1,4),
          substr(rec_mottaker.bosted_kommune_nr,1,10),
          substr(rec_mottaker.bydel_kommune_nr,1,11),
          p_in_period,
          v_kilde,
          v_storedate,
          rec_mottaker.fagsak_id,
          v_fk_person1_yb,
          rec_mottaker.belope,
          rec_mottaker.pk_dim_person,
          rec_mottaker.pk_dim_alder,
          rec_mottaker.fk_dim_kjonn,
          rec_mottaker.pk_dim_tid,
          rec_mottaker.fk_dim_sivilstatus,
          rec_mottaker.fk_dim_land_fodt,
          rec_mottaker.fk_dim_geografi_bosted,
          rec_mottaker.fk_dim_land_statsborgerskap
        );
        l_commit := l_commit + 1;
      EXCEPTION
          WHEN OTHERS THEN
            l_error_melding := SQLCODE || ' ' || sqlerrm;
            INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
            VALUES(NULL, rec_mottaker.gjelder_mottaker, l_error_melding, v_storedate, 'FAM_BT_MOTTAKER_INSERT5');
            l_commit := l_commit + 1;
            p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
      END;
      --dbms_output.put_line('Commit'||sysdate);--Test!!!
      IF l_commit >= 100000 THEN
        COMMIT;
        l_commit := 0;
      END IF;
    END LOOP;
    COMMIT;
    IF l_error_melding IS NOT NULL THEN
      INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(ID, error_msg, opprettet_tid, kilde)
      VALUES(NULL, l_error_melding, v_storedate, 'FAM_BT_MOTTAKER_INSERT6');
      COMMIT;
      p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      l_error_melding := SQLCODE || ' ' || sqlerrm;
      INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
      VALUES(NULL, NULL, l_error_melding, v_storedate, 'FAM_BT_MOTTAKER_INSERT7');
      COMMIT;
      p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
  END fam_bt_mottaker_insert_bck;

  PROCEDURE fam_bt_slett_offset(p_in_offset IN VARCHAR2, p_error_melding OUT VARCHAR2) AS
    v_temp_dml varchar2(4000);
    l_error_melding VARCHAR2(1000);
  BEGIN
    v_temp_dml := 'CREATE GLOBAL TEMPORARY TABLE TEMP_TBL_SLETT
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
                   WHERE meta.kafka_offset = ' || p_in_offset;
    --dbms_output.put_line(v_temp_dml);
    EXECUTE IMMEDIATE v_temp_dml;

    BEGIN
      v_temp_dml := '
      DELETE FROM dvh_fam_bt.fam_bt_statsborgerskap
      WHERE pk_statsborgerskap IN (SELECT DISTINCT pk_statsborgerskap FROM TEMP_TBL_SLETT)';
      --dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE v_temp_dml;

      v_temp_dml := '
      DELETE FROM dvh_fam_bt.fam_bt_utbet_det
      WHERE pk_bt_utbet_det IN (SELECT DISTINCT pk_bt_utbet_det FROM TEMP_TBL_SLETT)';
      --dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE v_temp_dml;

      v_temp_dml := '
      DELETE FROM dvh_fam_bt.fam_bt_utbetaling
      WHERE pk_bt_utbetaling IN (SELECT DISTINCT pk_bt_utbetaling FROM TEMP_TBL_SLETT)';
      --dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE v_temp_dml;

      v_temp_dml := '
      DELETE FROM dvh_fam_bt.fam_bt_fagsak
      WHERE pk_bt_fagsak IN (SELECT DISTINCT pk_bt_fagsak FROM TEMP_TBL_SLETT)';
      --dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE v_temp_dml;

      v_temp_dml := '
      DELETE FROM dvh_fam_bt.fam_bt_person
      WHERE pk_bt_person IN (SELECT DISTINCT pk_bt_person_utb FROM TEMP_TBL_SLETT)';
      --dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE v_temp_dml;

      v_temp_dml := '
      DELETE FROM dvh_fam_bt.fam_bt_person
      WHERE pk_bt_person IN (SELECT DISTINCT pk_bt_person_mot FROM TEMP_TBL_SLETT)';
      --dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE v_temp_dml;

      COMMIT;--Commit på alle
    EXCEPTION
      WHEN OTHERS THEN
        l_error_melding := SQLCODE || ' ' || sqlerrm;
        ROLLBACK;
        INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
        VALUES(NULL, NULL, l_error_melding, sysdate, 'FAM_BT_SLETT_OFFSET1');
        COMMIT;
        p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
    END;

    --ROLLBACK;--Test
    v_temp_dml := 'TRUNCATE TABLE TEMP_TBL_SLETT';
    --dbms_output.put_line(v_temp_dml);
    EXECUTE IMMEDIATE v_temp_dml;

    v_temp_dml := 'DROP TABLE TEMP_TBL_SLETT';
    --dbms_output.put_line(v_temp_dml);
    EXECUTE IMMEDIATE v_temp_dml;
    --COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      l_error_melding := SQLCODE || ' ' || sqlerrm;
      INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
      VALUES(NULL, NULL, l_error_melding, sysdate, 'FAM_BT_SLETT_OFFSET2');
      COMMIT;
      p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
  END fam_bt_slett_offset;

  PROCEDURE fam_bt2_slett_offset(p_in_offset IN VARCHAR2, p_error_melding OUT VARCHAR2) AS
    v_temp_dml varchar2(4000);
    l_error_melding VARCHAR2(1000);
  BEGIN
    v_temp_dml := 'CREATE GLOBAL TEMPORARY TABLE TEMP_TBL_SLETT
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
                   where meta.kafka_offset = ' || p_in_offset || ' AND meta.kafka_topic = ''teamfamilie.aapen-barnetrygd-vedtak-v2'' ' ;
    --dbms_output.put_line(v_temp_dml);
    EXECUTE IMMEDIATE v_temp_dml;

    BEGIN
      v_temp_dml := '
      DELETE FROM dvh_fam_bt.fam_bt_statsborgerskap
      WHERE pk_statsborgerskap IN (SELECT DISTINCT pk_statsborgerskap FROM TEMP_TBL_SLETT)';
      --dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE v_temp_dml;

      v_temp_dml := '
      DELETE FROM dvh_fam_bt.fam_bt_utbet_det
      WHERE pk_bt_utbet_det IN (SELECT DISTINCT pk_bt_utbet_det FROM TEMP_TBL_SLETT)';
      --dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE v_temp_dml;

      v_temp_dml := '
      DELETE FROM dvh_fam_bt.fam_bt_utbetaling
      WHERE pk_bt_utbetaling IN (SELECT DISTINCT pk_bt_utbetaling FROM TEMP_TBL_SLETT)';
      --dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE v_temp_dml;

      v_temp_dml := '
      DELETE FROM dvh_fam_bt.fam_bt_kompetanse_barn
      WHERE pk_bt_kompetanse_barn IN (SELECT DISTINCT pk_bt_kompetanse_barn FROM TEMP_TBL_SLETT)';
      --dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE v_temp_dml;

      v_temp_dml := '
      DELETE FROM dvh_fam_bt.fam_bt_kompetanse_perioder
      WHERE pk_bt_kompetanse_perioder IN (SELECT DISTINCT pk_bt_kompetanse_perioder FROM TEMP_TBL_SLETT)';
      --dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE v_temp_dml;

      v_temp_dml := '
      DELETE FROM dvh_fam_bt.fam_bt_fagsak
      WHERE pk_bt_fagsak IN (SELECT DISTINCT pk_bt_fagsak FROM TEMP_TBL_SLETT)';
      dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE v_temp_dml;

      v_temp_dml := '
      DELETE FROM dvh_fam_bt.fam_bt_person
      WHERE pk_bt_person IN (SELECT DISTINCT pk_bt_person_utb FROM TEMP_TBL_SLETT)';
      --dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE v_temp_dml;

      v_temp_dml := '
      DELETE FROM dvh_fam_bt.fam_bt_person
      WHERE pk_bt_person IN (SELECT DISTINCT pk_bt_person_mot FROM TEMP_TBL_SLETT)';
      --dbms_output.put_line(v_temp_dml);
      EXECUTE IMMEDIATE v_temp_dml;

      COMMIT;--Commit på alle
    EXCEPTION
      WHEN OTHERS THEN
        l_error_melding := SQLCODE || ' ' || sqlerrm;
        ROLLBACK;
        INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
        VALUES(NULL, NULL, l_error_melding, sysdate, 'FAM_BT_SLETT_OFFSET1');
        COMMIT;
        p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
    END;

    --ROLLBACK;--Test
    v_temp_dml := 'TRUNCATE TABLE TEMP_TBL_SLETT';
    --dbms_output.put_line(v_temp_dml);
    EXECUTE IMMEDIATE v_temp_dml;

    v_temp_dml := 'DROP TABLE TEMP_TBL_SLETT';
    --dbms_output.put_line(v_temp_dml);
    EXECUTE IMMEDIATE v_temp_dml;
    --COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      l_error_melding := SQLCODE || ' ' || sqlerrm;
      INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
      VALUES(NULL, NULL, l_error_melding, sysdate, 'FAM_BT_SLETT_OFFSET2');
      COMMIT;
      p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
  END fam_bt2_slett_offset;

  PROCEDURE fam_bt_utpakking_offset(p_in_offset IN NUMBER, p_error_melding OUT VARCHAR2) AS
  --v_person_ident VARCHAR2(20);
  v_fk_person1_soker NUMBER;
  v_fk_person1_utb NUMBER;
  v_pk_bt_utbetaling NUMBER;
  v_pk_bt_utbet_det NUMBER;
  v_pk_person_soker NUMBER;
  v_pk_person_utb NUMBER;
  v_pk_fagsak NUMBER;
  v_storedate DATE := sysdate;
  l_error_melding VARCHAR2(4000);

  CURSOR cur_bt_fagsak(p_offset NUMBER) IS
    WITH jdata AS (
      SELECT kafka_offset
            ,melding AS doc
            ,pk_bt_meta_data
      FROM dvh_fam_bt.fam_bt_meta_data
      WHERE kafka_offset = p_offset
      --WHERE kafka_offset=110--Test!!!
    )
    SELECT T.behandling_opprinnelse
          ,T.behandling_type
          ,T.fagsak_id
          ,T.behandlings_id
          ,CAST(to_timestamp_tz(T.tidspunkt_vedtak, 'FXYYYY-MM-DD"T"HH24:MI:SS.FXFF3TZH:TZM') AT TIME ZONE 'Europe/Belgrade' AS TIMESTAMP) AS tidspunkt_vedtak
          ,CASE
           WHEN T.enslig_forsørger = 'false' THEN '0'
           ELSE '1'
           END AS enslig_forsørger
          ,T.kategori
          ,T.underkategori
          ,'BT' AS kildesystem
          ,current_timestamp AS lastet_dato
          ,T.funksjonell_id
          ,T.behandlingÅrsak
          ,T.person_ident
          ,T.rolle
          ,T.statsborgerskap
          ,T.annenpart_bostedsland
          ,T.annenpart_personident
          ,T.annenpart_statsborgerskap
          ,T.bostedsland
          ,T.delingsprosent_omsorg
          ,T.delingsprosent_ytelse
          ,T.primærland
          ,T.sekundærland
          ,pk_bt_meta_data
          ,kafka_offset
    FROM jdata
        ,JSON_TABLE
        (
          doc, '$'
          COLUMNS (
          behandling_opprinnelse    VARCHAR2 PATH '$.behandlingOpprinnelse'
         ,behandling_type           VARCHAR2 PATH '$.behandlingType'
         ,fagsak_id                 VARCHAR2 PATH '$.fagsakId'
         ,behandlings_id            VARCHAR2 PATH '$.behandlingsId'
         ,tidspunkt_vedtak          VARCHAR2 PATH '$.tidspunktVedtak'
         ,enslig_forsørger          VARCHAR2 PATH '$.ensligForsørger'
         ,kategori                  VARCHAR2 PATH '$.kategori'
         ,underkategori             VARCHAR2 PATH '$.underkategori'
         ,funksjonell_id            VARCHAR2 PATH '$.funksjonellId'
         ,person_ident              VARCHAR2 PATH '$.person[*].personIdent'
         ,rolle                     VARCHAR2 PATH '$.person[*].rolle'
         ,statsborgerskap           VARCHAR2 PATH '$.person[*].statsborgerskap[*]'
         ,annenpart_bostedsland     VARCHAR2 PATH '$.person[*].annenpartBostedsland'
         ,annenpart_personident     VARCHAR2 PATH '$.person[*].annenpartPersonident'
         ,annenpart_statsborgerskap VARCHAR2 PATH '$.person[*].annenpartStatsborgerskap'
         ,bostedsland               VARCHAR2 PATH '$.person[*].bostedsland'
         ,delingsprosent_omsorg     VARCHAR2 PATH '$.person[*].delingsprosentOmsorg'
         ,delingsprosent_ytelse     VARCHAR2 PATH '$.person[*].delingsprosentYtelse'
         ,primærland                VARCHAR2 PATH '$.person[*].primærland'
         ,sekundærland              VARCHAR2 PATH '$.person[*].sekundærland'
         ,behandlingÅrsak           VARCHAR2 PATH '$.behandlingÅrsak'
         )
        ) T;

  CURSOR cur_bt_utbetaling(p_offset NUMBER) IS
    WITH jdata AS (
      SELECT kafka_offset
            ,melding AS doc
      FROM dvh_fam_bt.fam_bt_meta_data
      WHERE kafka_offset = p_offset
      --WHERE kafka_offset=110--Test!!!
    )
    SELECT T.utbetalt_per_mnd
          ,TO_DATE(T.stønad_fom, 'YYYY-MM-DD') AS stønad_fom
          ,TO_DATE(T.stønad_tom, 'YYYY-MM-DD') AS stønad_tom
          ,T.hjemmel
          ,current_timestamp AS lastet_dato
          ,behandlings_id
          ,kafka_offset
    FROM jdata
        ,JSON_TABLE
        (
         doc, '$'
         COLUMNS (
         behandlings_id     VARCHAR2 PATH '$.behandlingsId',
         NESTED             PATH '$.utbetalingsperioder[*]'
         COLUMNS (
         utbetalt_per_mnd   VARCHAR2 PATH '$.utbetaltPerMnd'
        ,stønad_fom         VARCHAR2 PATH '$.stønadFom'
        ,stønad_tom         VARCHAR2 PATH '$.stønadTom'
        ,hjemmel            VARCHAR2 PATH '$.hjemmel'
        ))
        ) T;

  CURSOR cur_bt_utbetalings_detaljer(p_offset NUMBER, p_fom DATE, p_tom DATE) IS
    WITH jdata AS (
      SELECT kafka_offset
            ,melding AS doc
      FROM dvh_fam_bt.fam_bt_meta_data
      WHERE kafka_offset = p_offset
      --WHERE kafka_offset = 110--Test!!!
    )
    SELECT --1 AS pk_bt_utbet_det
          --,
           S.hjemmel
          ,S.utbetaltpermnd
          ,S.stønadfom
          ,S.stønadtom
          ,S.klassekode
          ,S.delytelse_id
          ,S.utbetalt_pr_mnd
          ,S.stonad_fom
          ,S.personident
          ,S.rolle
          ,S.statsborgerskap
          ,S.bostedsland
          ,S.primærland
          ,S.sekundærland
          ,S.delingsprosentomsorg delingsprosent_omsorg
          ,S.delingsprosentytelse delingsprosent_ytelse
          ,S.annenpartpersonident annenpart_personident
          ,S.annenpartstatsborgerskap annenpart_statsborgerskap
          ,S.annenpartbostedsland annenpart_bostedsland
          ,current_timestamp AS lastet_dato
          ,behandlings_id
          ,kafka_offset
    FROM jdata
        ,JSON_TABLE
         (
          doc, '$'
          COLUMNS (
          behandlings_id            VARCHAR2 PATH '$.behandlingsId',
          NESTED                    PATH '$.utbetalingsperioder[*]'
          COLUMNS (
          hjemmel                   VARCHAR2 PATH '$.hjemmel'
         ,utbetaltpermnd            VARCHAR2 PATH '$.utbetaltPerMnd'
         ,stønadfom                 VARCHAR2 PATH '$.stønadFom'
         ,stønadtom                 VARCHAR2 PATH '$.stønadTom'
         ,joined_on                 VARCHAR2 PATH '$.joined_on'
         ,NESTED                    PATH '$.utbetalingsDetaljer[*]'
          COLUMNS (
          klassekode                VARCHAR2 PATH '$.klassekode'
         ,delytelse_id              VARCHAR2 PATH '$.delytelseId'
         ,utbetalt_pr_mnd           VARCHAR2 PATH '$..utbetaltPrMnd'
         ,stonad_fom                VARCHAR2 PATH '$.stonad_fom'
         ,personident               VARCHAR2 PATH '$.person[*].personIdent'
         ,rolle                     VARCHAR2 PATH '$.person[*].rolle'
         ,statsborgerskap           VARCHAR2 PATH '$.person[*].statsborgerskap[*]'
         ,bostedsland               VARCHAR2 PATH '$.person[*].bostedsland'
         ,primærland                VARCHAR2 PATH '$.person[*].primærland'
         ,sekundærland              VARCHAR2 PATH '$.person[*].sekundærland'
         ,delingsprosentomsorg      VARCHAR2 PATH '$.person[*].delingsprosentOmsorg'
         ,delingsprosentytelse      VARCHAR2 PATH '$.person[*].delingsprosentYtelse'
         ,annenpartpersonident      VARCHAR2 PATH '$.person[*].annenpartPersonident'
         ,annenpartstatsborgerskap  VARCHAR2 PATH '$.person[*].annenpartStatsborgerskap'
         ,annenpartbostedsland      VARCHAR2 PATH '$.person[*].annenpartBostedsland'
          )))
         ) S
    WHERE TO_DATE(S.stønadfom,'YYYY-MM-DD') = p_fom
    AND TO_DATE(S.stønadtom,'YYYY-MM-DD') = p_tom
    --WHERE TO_DATE(S.stønadfom,'YYYY-MM-DD') >= TO_DATE('2020-03-01','YYYY-MM-DD')--Test!!!
    --AND TO_DATE(S.stønadtom,'YYYY-MM-DD') <= TO_DATE('2032-01-31','YYYY-MM-DD')--Test!!!
    ;

  BEGIN
    -- For alle fagsaker
    FOR rec_fag IN cur_bt_fagsak(p_in_offset) LOOP
      BEGIN
        --dbms_output.put_line(rec_fag.fagsak_id);
        v_pk_person_soker := NULL;
        v_fk_person1_soker := -1;
        SELECT dvh_fambt_kafka.hibernate_sequence.NEXTVAL INTO v_pk_person_soker FROM dual;

        --Hent fk_person1
        BEGIN
          SELECT DISTINCT person_67_vasket.fk_person1 as ak_person1
          INTO v_fk_person1_soker
          FROM dt_person.dvh_person_ident_off_id_ikke_skjermet person_67_vasket
          WHERE person_67_vasket.off_id = rec_fag.person_ident
          AND rec_fag.tidspunkt_vedtak BETWEEN person_67_vasket.gyldig_fra_dato AND person_67_vasket.gyldig_til_dato;
        EXCEPTION
          WHEN OTHERS THEN
            v_fk_person1_soker := -1;
            l_error_melding := SQLCODE || ' ' || sqlerrm;
            INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
            VALUES(NULL, rec_fag.fagsak_id, l_error_melding, v_storedate, 'FAM_BT_UTPAKKING_OFFSET1');
            COMMIT;
            p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
        END;
        -- Insert søker into person tabell
        INSERT INTO dvh_fam_bt.fam_bt_person
        (
          pk_bt_person
         ,annenpart_bostedsland
         ,annenpart_personident
         ,annenpart_statsborgerskap
         ,bostedsland
         ,delingsprosent_omsorg
         ,delingsprosent_ytelse
         ,person_ident
         ,primærland
         ,rolle
         ,sekundærland
         ,fk_person1
         ,lastet_dato
         ,behandlings_id
         ,kafka_offset
        )
        VALUES
        (
          v_pk_person_soker
         ,rec_fag.annenpart_bostedsland
         ,rec_fag.annenpart_personident
         ,rec_fag.annenpart_statsborgerskap
         ,rec_fag.bostedsland
         ,rec_fag.delingsprosent_omsorg
         ,rec_fag.delingsprosent_ytelse
         ,rec_fag.person_ident
         ,rec_fag.primærland
         ,rec_fag.rolle
         ,rec_fag.sekundærland
         ,v_fk_person1_soker
         ,rec_fag.lastet_dato
         ,rec_fag.behandlings_id
         ,rec_fag.kafka_offset
        );

        -- Insert into FAGSAK tabellen
        v_pk_fagsak := NULL;
        SELECT dvh_fambt_kafka.hibernate_sequence.NEXTVAL INTO v_pk_fagsak FROM dual;
        INSERT INTO dvh_fam_bt.fam_bt_fagsak
        (
           pk_bt_fagsak
          ,fk_bt_person
          ,fk_bt_meta_data
          ,behandling_opprinnelse
          ,behandling_type
          ,fagsak_id
          ,behandlings_id
          ,tidspunkt_vedtak
          ,enslig_forsørger
          ,kategori
          ,underkategori
          ,kildesystem
          ,lastet_dato
          ,funksjonell_id
          ,behandling_Årsak
          ,kafka_offset
        )
        VALUES
        (
          v_pk_fagsak
         ,v_pk_person_soker
         ,rec_fag.pk_bt_meta_data
         ,rec_fag.behandling_opprinnelse
         ,rec_fag.behandling_type
         ,rec_fag.fagsak_id
         ,rec_fag.behandlings_id
         ,rec_fag.tidspunkt_vedtak
         ,rec_fag.enslig_forsørger
         ,rec_fag.kategori
         ,rec_fag.underkategori
         ,rec_fag.kildesystem
         ,rec_fag.lastet_dato
         ,rec_fag.funksjonell_id
         ,rec_fag.behandlingÅrsak
         ,rec_fag.kafka_offset
        );

        -- For alle utbetalingsperioder
        FOR rec_utbetaling IN cur_bt_utbetaling(p_in_offset) LOOP
          BEGIN
            --dbms_output.put_line('Hallo z:'||rec_utbetaling.stønad_fom||','||rec_utbetaling.stønad_tom);
            v_pk_bt_utbetaling := NULL;
            SELECT dvh_fambt_kafka.hibernate_sequence.NEXTVAL INTO v_pk_bt_utbetaling FROM dual;
            --v_pk_bt_utbetaling:=rec_utbetaling.PK_BT_UTBETALING;
            INSERT INTO dvh_fam_bt.fam_bt_utbetaling
            (
              pk_bt_utbetaling
             ,utbetalt_per_mnd
             ,stønad_fom
             ,stønad_tom
             ,hjemmel
             ,lastet_dato
             ,fk_bt_fagsak
             ,behandlings_id
             ,kafka_offset
            )
            VALUES
            (
              v_pk_bt_utbetaling
             ,rec_utbetaling.utbetalt_per_mnd
             ,rec_utbetaling.stønad_fom
             ,rec_utbetaling.stønad_tom
             ,rec_utbetaling.hjemmel
             ,rec_utbetaling.lastet_dato
             ,v_pk_fagsak
             ,rec_utbetaling.behandlings_id
             ,rec_utbetaling.kafka_offset
            );

            -- For alle utbetalingsdetaljer for aktuell tidsperiode
            FOR rec_utbet_det IN cur_bt_utbetalings_detaljer(p_in_offset, rec_utbetaling.stønad_fom, rec_utbetaling.stønad_tom) LOOP
              BEGIN
                --dbms_output.put_line(rec_utbet_det.personident||','||rec_utbet_det.stønadfom||'YY'||to_char(rec_utbet_det.utbetalt_pr_mnd));
                --Hent fk_person1
                v_fk_person1_utb := -1;
                BEGIN
                  SELECT DISTINCT person_67_vasket.fk_person1 as ak_person1
                  INTO v_fk_person1_utb
                  FROM dt_person.dvh_person_ident_off_id_ikke_skjermet person_67_vasket
                  WHERE person_67_vasket.off_id = rec_utbet_det.personident
                  AND rec_fag.tidspunkt_vedtak BETWEEN person_67_vasket.gyldig_fra_dato AND person_67_vasket.gyldig_til_dato;
                EXCEPTION
                  WHEN OTHERS THEN
                    v_fk_person1_utb := -1;
                    l_error_melding := SQLCODE || ' ' || sqlerrm;
                    INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
                    VALUES(NULL, rec_utbet_det.behandlings_id, l_error_melding, v_storedate, 'FAM_BT_UTPAKKING_OFFSET2');
                    COMMIT;
                    p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
                END;

                v_pk_person_utb := NULL;
                SELECT dvh_fambt_kafka.hibernate_sequence.NEXTVAL INTO v_pk_person_utb FROM dual;
                --dbms_output.put_line(v_pk_person_utb);
                BEGIN
                INSERT INTO dvh_fam_bt.fam_bt_person
                (
                  pk_bt_person
                 ,annenpart_bostedsland
                 ,annenpart_personident
                 ,annenpart_statsborgerskap
                 ,bostedsland
                 ,delingsprosent_omsorg
                 ,delingsprosent_ytelse
                 ,person_ident
                 ,primærland
                 ,rolle
                 ,sekundærland
                 ,fk_person1
                 ,lastet_dato
                 ,behandlings_id
                 ,kafka_offset
                )
                VALUES
                (
                  --dvh_fam_fp.hibernate_sequence_test.NEXTVAL
                  v_pk_person_utb
                 ,rec_utbet_det.annenpart_bostedsland
                 ,rec_utbet_det.annenpart_personident
                 ,rec_utbet_det.annenpart_statsborgerskap
                 ,rec_utbet_det.bostedsland
                 ,rec_utbet_det.delingsprosent_omsorg
                 ,rec_utbet_det.delingsprosent_ytelse
                 ,rec_utbet_det.personident
                 ,rec_utbet_det.primærland
                 ,rec_utbet_det.rolle
                 ,rec_utbet_det.sekundærland
                 -- ,rec_utbet_det.FK_PERSON1
                 ,v_fk_person1_utb
                 ,rec_utbet_det.lastet_dato
                 ,rec_utbet_det.behandlings_id
                 ,rec_utbet_det.kafka_offset
                );
                EXCEPTION
                  WHEN OTHERS THEN
                    dbms_output.put_line(v_pk_person_utb);
                    l_error_melding := SQLCODE || ' ' || sqlerrm;
                    INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
                    VALUES(NULL, rec_utbet_det.behandlings_id, l_error_melding, v_storedate, 'FAM_BT_UTPAKKING_OFFSET3');
                    COMMIT;
                    p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
                END;

                BEGIN
                INSERT INTO dvh_fam_bt.fam_bt_utbet_det
                (
                  pk_bt_utbet_det
                 ,klassekode
                 ,delytelse_id
                 ,utbetalt_pr_mnd
                 ,lastet_dato
                 ,fk_bt_person
                 ,fk_bt_utbetaling
                 ,behandlings_id
                 ,kafka_offset
                )
                VALUES
                (
                  dvh_fambt_kafka.hibernate_sequence.NEXTVAL
                  --v_pk_bt_utbet_det
                 ,rec_utbet_det.klassekode
                 ,rec_utbet_det.delytelse_id
                 ,rec_utbet_det.utbetalt_pr_mnd
                 ,rec_utbet_det.lastet_dato
                 ,v_pk_person_utb
                 ,v_pk_bt_utbetaling
                 ,rec_utbet_det.behandlings_id
                 ,rec_utbet_det.kafka_offset
                );
                EXCEPTION
                  WHEN OTHERS THEN
                    l_error_melding := SQLCODE || ' ' || sqlerrm;
                    INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
                    VALUES(NULL, rec_utbet_det.behandlings_id, l_error_melding, v_storedate, 'FAM_BT_UTPAKKING_OFFSET4');
                    COMMIT;
                    p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
                END;
              EXCEPTION
                WHEN OTHERS THEN
                  l_error_melding := SQLCODE || ' ' || sqlerrm;
                  INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
                  VALUES(NULL, rec_fag.fagsak_id, l_error_melding, v_storedate, 'FAM_BT_UTPAKKING_OFFSET5');
                  COMMIT;
                  p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
              END;
            END LOOP;--Utbetalingsdetaljer
          EXCEPTION
            WHEN OTHERS THEN
              l_error_melding := SQLCODE || ' ' || sqlerrm;
              INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
              VALUES(NULL, rec_fag.fagsak_id, l_error_melding, v_storedate, 'FAM_BT_UTPAKKING_OFFSET6');
              COMMIT;
              p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
          END;
        END LOOP;--Utbetalinger
      EXCEPTION
        WHEN OTHERS THEN
          l_error_melding := SQLCODE || ' ' || sqlerrm;
          INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
          VALUES(NULL, rec_fag.fagsak_id, l_error_melding, v_storedate, 'FAM_BT_UTPAKKING_OFFSET7');
          COMMIT;
          p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
      END;
    END LOOP;--Fagsak
    COMMIT;
    IF l_error_melding IS NOT NULL THEN
      INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(ID, error_msg, opprettet_tid, kilde)
      VALUES(NULL, l_error_melding, v_storedate, 'FAM_BT_UTPAKKING_OFFSET8');
      COMMIT;
      p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      l_error_melding := SQLCODE || ' ' || sqlerrm;
      INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
      VALUES(NULL, NULL, l_error_melding, v_storedate, 'FAM_BT_UTPAKKING_OFFSET9');
      COMMIT;
      p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
  END fam_bt_utpakking_offset;

  PROCEDURE fam_bt2_utpakking_offset(p_in_offset IN NUMBER, p_error_melding OUT VARCHAR2) AS
  --v_person_ident VARCHAR2(20);
  v_fk_person1_soker NUMBER;
  v_fk_person1_utb NUMBER;
  v_pk_bt_utbetaling NUMBER;
  v_pk_bt_utbet_det NUMBER;
  v_pk_person_soker NUMBER;
  v_pk_person_utb NUMBER;
  v_pk_fagsak NUMBER;
  v_pk_bt_kompetanse_perioder NUMBER;
  v_pk_bt_kompetanse_barn NUMBER;
  v_storedate DATE := sysdate;
  l_error_melding VARCHAR2(4000);

  CURSOR cur_bt_fagsak(p_offset NUMBER) IS
    WITH jdata AS (
      SELECT kafka_offset
            ,melding AS doc
            ,pk_bt_meta_data
      FROM dvh_fam_bt.fam_bt_meta_data
      WHERE kafka_offset = p_offset
      AND kafka_topic = 'teamfamilie.aapen-barnetrygd-vedtak-v2'
      --WHERE kafka_offset=110--Test!!!
    )
    SELECT T.behandling_opprinnelse
          ,T.behandling_type
          ,T.fagsak_id
          ,T.behandlings_id
          ,CAST(to_timestamp_tz(T.tidspunkt_vedtak, 'FXYYYY-MM-DD"T"HH24:MI:SS.FXFF3TZH:TZM') AT TIME ZONE 'Europe/Belgrade' AS TIMESTAMP) AS tidspunkt_vedtak
          ,CASE
           WHEN T.enslig_forsørger = 'false' THEN '0'
           ELSE '1'
           END AS enslig_forsørger
          ,T.kategori
          ,T.underkategori
          ,'BT' AS kildesystem
          ,current_timestamp AS lastet_dato
          ,T.funksjonell_id
          ,T.behandlingÅrsak
          ,T.person_ident
          ,T.rolle
          ,T.statsborgerskap
          ,T.annenpart_bostedsland
          ,T.annenpart_personident
          ,T.annenpart_statsborgerskap
          ,T.bostedsland
          ,T.delingsprosent_omsorg
          ,T.delingsprosent_ytelse
          ,T.primærland
          ,T.sekundærland
          ,pk_bt_meta_data
          ,kafka_offset
    FROM jdata
        ,JSON_TABLE
        (
          doc, '$'
          COLUMNS (
          behandling_opprinnelse    VARCHAR2 PATH '$.behandlingOpprinnelse'
         ,behandling_type           VARCHAR2 PATH '$.behandlingTypeV2'
         ,fagsak_id                 VARCHAR2 PATH '$.fagsakId'
         ,behandlings_id            VARCHAR2 PATH '$.behandlingsId'
         ,tidspunkt_vedtak          VARCHAR2 PATH '$.tidspunktVedtak'
         ,enslig_forsørger          VARCHAR2 PATH '$.ensligForsørger'
         ,kategori                  VARCHAR2 PATH '$.kategoriV2'
         ,underkategori             VARCHAR2 PATH '$.underkategoriV2'
         ,funksjonell_id            VARCHAR2 PATH '$.funksjonellId'
         ,person_ident              VARCHAR2 PATH '$.personV2[*].personIdent'
         ,rolle                     VARCHAR2 PATH '$.personV2[*].rolle'
         ,statsborgerskap           VARCHAR2 PATH '$.personV2[*].statsborgerskap[*]'
         ,annenpart_bostedsland     VARCHAR2 PATH '$.personV2[*].annenpartBostedsland'
         ,annenpart_personident     VARCHAR2 PATH '$.personV2[*].annenpartPersonident'
         ,annenpart_statsborgerskap VARCHAR2 PATH '$.personV2[*].annenpartStatsborgerskap'
         ,bostedsland               VARCHAR2 PATH '$.personV2[*].bostedsland'
         ,delingsprosent_omsorg     VARCHAR2 PATH '$.personV2[*].delingsprosentOmsorg'
         ,delingsprosent_ytelse     VARCHAR2 PATH '$.personV2[*].delingsprosentYtelse'
         ,primærland                VARCHAR2 PATH '$.personV2[*].primærland'
         ,sekundærland              VARCHAR2 PATH '$.personV2[*].sekundærland'
         ,behandlingÅrsak           VARCHAR2 PATH '$.behandlingÅrsakV2'
         )
        ) T;

 ---------------------------------------------
  CURSOR cur_bt_kompetanse_perioder(p_offset NUMBER) IS
    WITH jdata as (
      SELECT kafka_offset
            ,melding AS doc
            ,pk_bt_meta_data
      FROM dvh_fam_bt.fam_bt_meta_data
      WHERE kafka_offset = p_offset
      AND kafka_topic = 'teamfamilie.aapen-barnetrygd-vedtak-v2'

    )
    select T.fom
          ,T.tom
          ,T.sokersaktivitet
          ,T.annenforelder_aktivitet
          ,T.annenforelder_aktivitetsland
          ,T.barnets_bostedsland
          ,T.kompetanse_Resultat
          ,current_timestamp AS lastet_dato
    from jdata
        ,JSON_TABLE
        (
         doc, '$'
         COLUMNS (
         tom                            VARCHAR2 PATH '$.kompetanseperioder[*].tom'
        ,fom                            VARCHAR2 PATH '$.kompetanseperioder[*].fom'
        ,sokersaktivitet                VARCHAR2 PATH '$.kompetanseperioder[*].sokersaktivitet'
        ,annenforelder_aktivitet        VARCHAR2 PATH '$.kompetanseperioder[*].annenForeldersAktivitet'
        ,annenforelder_aktivitetsland   VARCHAR2 PATH '$.kompetanseperioder[*].annenForeldersAktivitetsland'
        ,barnets_bostedsland               VARCHAR2 PATH '$.kompetanseperioder[*].barnetsBostedsland'
        ,kompetanse_Resultat            VARCHAR2 PATH '$.kompetanseperioder[*].resultat'

         )) T;
    --------------------------------------------------------

  CURSOR cur_bt_utbetaling(p_offset NUMBER) IS
    WITH jdata AS (
      SELECT kafka_offset
            ,melding AS doc
      FROM dvh_fam_bt.fam_bt_meta_data
      WHERE kafka_offset = p_offset
      AND kafka_topic = 'teamfamilie.aapen-barnetrygd-vedtak-v2'
      --WHERE kafka_offset=110--Test!!!
    )
    SELECT T.utbetalt_per_mnd
          ,TO_DATE(T.stønad_fom, 'YYYY-MM-DD') AS stønad_fom
          ,TO_DATE(T.stønad_tom, 'YYYY-MM-DD') AS stønad_tom
          ,T.hjemmel
          ,current_timestamp AS lastet_dato
          ,behandlings_id
          ,kafka_offset
    FROM jdata
        ,JSON_TABLE
        (
         doc, '$'
         COLUMNS (
         behandlings_id     VARCHAR2 PATH '$.behandlingsId',
         NESTED             PATH '$.utbetalingsperioderV2[*]'
         COLUMNS (
         utbetalt_per_mnd   VARCHAR2 PATH '$.utbetaltPerMnd'
        ,stønad_fom         VARCHAR2 PATH '$.stønadFom'
        ,stønad_tom         VARCHAR2 PATH '$.stønadTom'
        ,hjemmel            VARCHAR2 PATH '$.hjemmel'
        ))
        ) T;

  CURSOR cur_bt_utbetalings_detaljer(p_offset NUMBER, p_fom DATE, p_tom DATE) IS
    WITH jdata AS (
      SELECT kafka_offset
            ,melding AS doc
      FROM dvh_fam_bt.fam_bt_meta_data
      WHERE kafka_offset = p_offset
      AND kafka_topic = 'teamfamilie.aapen-barnetrygd-vedtak-v2'
      --WHERE kafka_offset = 110--Test!!!
    )
    SELECT --1 AS pk_bt_utbet_det
          --,
           S.hjemmel
          ,S.utbetaltpermnd
          ,S.stønadfom
          ,S.stønadtom
          ,S.klassekode
          ,S.delytelse_id
          ,S.utbetalt_pr_mnd
          ,S.stonad_fom
          ,S.personident
          ,S.rolle
          ,S.statsborgerskap
          ,S.bostedsland
         -- ,S.primærland
         -- ,S.sekundærland
         -- ,S.delingsprosentomsorg delingsprosent_omsorg
          ,S.delingsprosentytelse delingsprosent_ytelse
        --  ,S.annenpartpersonident annenpart_personident
         -- ,S.annenpartstatsborgerskap annenpart_statsborgerskap
         -- ,S.annenpartbostedsland annenpart_bostedsland
          ,current_timestamp AS lastet_dato
          ,behandlings_id
          ,kafka_offset
    FROM jdata
        ,JSON_TABLE
         (
          doc, '$'
          COLUMNS (
          behandlings_id            VARCHAR2 PATH '$.behandlingsId',
          NESTED                    PATH '$.utbetalingsperioderV2[*]'
          COLUMNS (
          hjemmel                   VARCHAR2 PATH '$.hjemmel'
         ,utbetaltpermnd            VARCHAR2 PATH '$.utbetaltPerMnd'
         ,stønadfom                 VARCHAR2 PATH '$.stønadFom'
         ,stønadtom                 VARCHAR2 PATH '$.stønadTom'
         ,joined_on                 VARCHAR2 PATH '$.joined_on'
         ,NESTED                    PATH '$.utbetalingsDetaljer[*]'
          COLUMNS (
          klassekode                VARCHAR2 PATH '$.klassekode'
         ,delytelse_id              VARCHAR2 PATH '$.delytelseId'
         ,utbetalt_pr_mnd           VARCHAR2 PATH '$..utbetaltPrMnd'
         ,stonad_fom                VARCHAR2 PATH '$.stonad_fom'
         ,personident               VARCHAR2 PATH '$.personV2[*].personIdent'
         ,rolle                     VARCHAR2 PATH '$.personV2[*].rolle'
         ,statsborgerskap           VARCHAR2 PATH '$.personV2[*].statsborgerskap[*]'
         ,bostedsland               VARCHAR2 PATH '$.personV2[*].bostedsland'
         --,primærland                VARCHAR2 PATH '$.personV2[*].primærland'
         --,sekundærland              VARCHAR2 PATH '$.personV2[*].sekundærland'
         --,delingsprosentomsorg      VARCHAR2 PATH '$.personV2[*].delingsprosentOmsorg'
         ,delingsprosentytelse      VARCHAR2 PATH '$.personV2[*].delingsprosentYtelse'
        -- ,annenpartpersonident      VARCHAR2 PATH '$.personV2[*].annenpartPersonident'
        -- ,annenpartstatsborgerskap  VARCHAR2 PATH '$.personV2[*].annenpartStatsborgerskap'
        -- ,annenpartbostedsland      VARCHAR2 PATH '$.personV2[*].annenpartBostedsland'
          )))
         ) S
    WHERE TO_DATE(S.stønadfom,'YYYY-MM-DD') = p_fom
    AND TO_DATE(S.stønadtom,'YYYY-MM-DD') = p_tom
    --WHERE TO_DATE(S.stønadfom,'YYYY-MM-DD') >= TO_DATE('2020-03-01','YYYY-MM-DD')--Test!!!
    --AND TO_DATE(S.stønadtom,'YYYY-MM-DD') <= TO_DATE('2032-01-31','YYYY-MM-DD')--Test!!!
    ;

  BEGIN
    -- For alle fagsaker
    FOR rec_fag IN cur_bt_fagsak(p_in_offset) LOOP
      BEGIN
        --dbms_output.put_line(rec_fag.fagsak_id);
        v_pk_person_soker := NULL;
        v_fk_person1_soker := -1;
        SELECT dvh_fambt_kafka.hibernate_sequence.NEXTVAL INTO v_pk_person_soker FROM dual;

        --Hent fk_person1
        BEGIN
          SELECT DISTINCT person_67_vasket.fk_person1 as ak_person1
          INTO v_fk_person1_soker
          FROM dt_person.dvh_person_ident_off_id_ikke_skjermet person_67_vasket
          WHERE person_67_vasket.off_id = rec_fag.person_ident
          AND rec_fag.tidspunkt_vedtak BETWEEN person_67_vasket.gyldig_fra_dato AND person_67_vasket.gyldig_til_dato;
        EXCEPTION
          WHEN OTHERS THEN
            v_fk_person1_soker := -1;
            l_error_melding := SQLCODE || ' ' || sqlerrm;
            INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
            VALUES(NULL, rec_fag.fagsak_id, l_error_melding, v_storedate, 'FAM_BT_UTPAKKING_OFFSET1');
            COMMIT;
            p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
        END;
        -- Insert søker into person tabell
        INSERT INTO dvh_fam_bt.fam_bt_person
        (
          pk_bt_person
         --,annenpart_bostedsland
         --,annenpart_personident
         --,annenpart_statsborgerskap
         ,bostedsland
         --,delingsprosent_omsorg
         ,delingsprosent_ytelse
         ,person_ident
         --,primærland
         ,rolle
         --,sekundærland
         ,fk_person1
         ,lastet_dato
         ,behandlings_id
         ,kafka_offset
        )
        VALUES
        (
          v_pk_person_soker
         --,rec_fag.annenpart_bostedsland
         --,rec_fag.annenpart_personident
         --,rec_fag.annenpart_statsborgerskap
         ,rec_fag.bostedsland
         --,rec_fag.delingsprosent_omsorg
         ,rec_fag.delingsprosent_ytelse
         ,rec_fag.person_ident
         --,rec_fag.primærland
         ,rec_fag.rolle
         --,rec_fag.sekundærland
         ,v_fk_person1_soker
         ,rec_fag.lastet_dato
         ,rec_fag.behandlings_id
         ,rec_fag.kafka_offset
        );

        -- Insert into FAGSAK tabellen
        v_pk_fagsak := NULL;
        SELECT dvh_fambt_kafka.hibernate_sequence.NEXTVAL INTO v_pk_fagsak FROM dual;
        INSERT INTO dvh_fam_bt.fam_bt_fagsak
        (
           pk_bt_fagsak
          ,fk_bt_person
          ,fk_bt_meta_data
          ,behandling_opprinnelse
          ,behandling_type
          ,fagsak_id
          ,behandlings_id
          ,tidspunkt_vedtak
          ,enslig_forsørger
          ,kategori
          ,underkategori
          ,kildesystem
          ,lastet_dato
          ,funksjonell_id
          ,behandling_Årsak
          ,kafka_offset
        )
        VALUES
        (
          v_pk_fagsak
         ,v_pk_person_soker
         ,rec_fag.pk_bt_meta_data
         ,rec_fag.behandling_opprinnelse
         ,rec_fag.behandling_type
         ,rec_fag.fagsak_id
         ,rec_fag.behandlings_id
         ,rec_fag.tidspunkt_vedtak
         ,rec_fag.enslig_forsørger
         ,rec_fag.kategori
         ,rec_fag.underkategori
         ,rec_fag.kildesystem
         ,rec_fag.lastet_dato
         ,rec_fag.funksjonell_id
         ,rec_fag.behandlingÅrsak
         ,rec_fag.kafka_offset
        );
   --------------------------------------


        -- For alle Kompetanse/insert into fam_bt_kompetanse_perioder
        FOR rec_kompetanse IN cur_bt_kompetanse_perioder(p_in_offset) LOOP
          BEGIN
            v_pk_bt_kompetanse_perioder := NULL;
            SELECT dvh_fambt_kafka.hibernate_sequence.NEXTVAL INTO v_pk_bt_kompetanse_perioder FROM dual;
            INSERT INTO dvh_fam_bt.fam_bt_kompetanse_perioder
            (
              pk_bt_kompetanse_perioder
             ,fom
             ,tom
             ,sokersaktivitet
             ,annenforelder_aktivitet
             ,annenforelder_aktivitetsland
             ,kompetanse_Resultat
             ,barnets_bostedsland
             ,fk_bt_fagsak
             ,lastet_dato

            )
            VALUES
            (
              v_pk_bt_kompetanse_perioder
             ,rec_kompetanse.fom
             ,rec_kompetanse.tom
             ,rec_kompetanse.sokersaktivitet
             ,rec_kompetanse.annenforelder_aktivitet
             ,rec_kompetanse.annenforelder_aktivitetsland
             ,rec_kompetanse.kompetanse_Resultat
             ,rec_kompetanse.barnets_bostedsland
             ,v_pk_fagsak
             ,rec_kompetanse.lastet_dato
            );
       END;
        -- Insert into fam_bt_kompetanse_barn
        v_pk_bt_kompetanse_barn := NULL;
        SELECT dvh_fambt_kafka.hibernate_sequence.NEXTVAL INTO v_pk_bt_kompetanse_barn FROM dual;
        INSERT INTO dvh_fam_bt.fam_bt_kompetanse_barn
        (
             pk_bt_kompetanse_barn
             ,fk_bt_kompetanse_perioder
             ,fk_person1
        )
        VALUES
        (
              v_pk_bt_kompetanse_barn
             ,v_pk_bt_kompetanse_perioder
             ,v_fk_person1_soker
        );
        END LOOP;
        -----------------------------




        -- For alle utbetalingsperioder
        FOR rec_utbetaling IN cur_bt_utbetaling(p_in_offset) LOOP
          BEGIN
            --dbms_output.put_line('Hallo z:'||rec_utbetaling.stønad_fom||','||rec_utbetaling.stønad_tom);
            v_pk_bt_utbetaling := NULL;
            SELECT dvh_fambt_kafka.hibernate_sequence.NEXTVAL INTO v_pk_bt_utbetaling FROM dual;
            --v_pk_bt_utbetaling:=rec_utbetaling.PK_BT_UTBETALING;
            INSERT INTO dvh_fam_bt.fam_bt_utbetaling
            (
              pk_bt_utbetaling
             ,utbetalt_per_mnd
             ,stønad_fom
             ,stønad_tom
             ,hjemmel
             ,lastet_dato
             ,fk_bt_fagsak
             ,behandlings_id
             ,kafka_offset
            )
            VALUES
            (
              v_pk_bt_utbetaling
             ,rec_utbetaling.utbetalt_per_mnd
             ,rec_utbetaling.stønad_fom
             ,rec_utbetaling.stønad_tom
             ,rec_utbetaling.hjemmel
             ,rec_utbetaling.lastet_dato
             ,v_pk_fagsak
             ,rec_utbetaling.behandlings_id
             ,rec_utbetaling.kafka_offset
            );

            -- For alle utbetalingsdetaljer for aktuell tidsperiode
            FOR rec_utbet_det IN cur_bt_utbetalings_detaljer(p_in_offset, rec_utbetaling.stønad_fom, rec_utbetaling.stønad_tom) LOOP
              BEGIN
                --dbms_output.put_line(rec_utbet_det.personident||','||rec_utbet_det.stønadfom||'YY'||to_char(rec_utbet_det.utbetalt_pr_mnd));
                --Hent fk_person1
                v_fk_person1_utb := -1;
                BEGIN
                  SELECT DISTINCT person_67_vasket.fk_person1 as ak_person1
                  INTO v_fk_person1_utb
                  FROM dt_person.dvh_person_ident_off_id_ikke_skjermet person_67_vasket
                  WHERE person_67_vasket.off_id = rec_utbet_det.personident
                  AND rec_fag.tidspunkt_vedtak BETWEEN person_67_vasket.gyldig_fra_dato AND person_67_vasket.gyldig_til_dato;
                EXCEPTION
                  WHEN OTHERS THEN
                    v_fk_person1_utb := -1;
                    l_error_melding := SQLCODE || ' ' || sqlerrm;
                    INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
                    VALUES(NULL, rec_utbet_det.behandlings_id, l_error_melding, v_storedate, 'FAM_BT_UTPAKKING_OFFSET2');
                    COMMIT;
                    p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
                END;

                v_pk_person_utb := NULL;
                SELECT dvh_fambt_kafka.hibernate_sequence.NEXTVAL INTO v_pk_person_utb FROM dual;
                --dbms_output.put_line(v_pk_person_utb);
                BEGIN
                INSERT INTO dvh_fam_bt.fam_bt_person
                (
                  pk_bt_person
                 --,annenpart_bostedsland
                 --,annenpart_personident
                 --,annenpart_statsborgerskap
                 ,bostedsland
                 --,delingsprosent_omsorg
                 ,delingsprosent_ytelse
                 ,person_ident
                 --,primærland
                 ,rolle
                 --,sekundærland
                 ,fk_person1
                 ,lastet_dato
                 ,behandlings_id
                 ,kafka_offset
                )
                VALUES
                (
                  --dvh_fam_fp.hibernate_sequence_test.NEXTVAL
                  v_pk_person_utb
                 --,rec_utbet_det.annenpart_bostedsland
                 --,rec_utbet_det.annenpart_personident
                 --,rec_utbet_det.annenpart_statsborgerskap
                 ,rec_utbet_det.bostedsland
                 --,rec_utbet_det.delingsprosent_omsorg
                 ,rec_utbet_det.delingsprosent_ytelse
                 ,rec_utbet_det.personident
                 --,rec_utbet_det.primærland
                 ,rec_utbet_det.rolle
                 --,rec_utbet_det.sekundærland
                 -- ,rec_utbet_det.FK_PERSON1
                 ,v_fk_person1_utb
                 ,rec_utbet_det.lastet_dato
                 ,rec_utbet_det.behandlings_id
                 ,rec_utbet_det.kafka_offset
                );
                EXCEPTION
                  WHEN OTHERS THEN
                    dbms_output.put_line(v_pk_person_utb);
                    l_error_melding := SQLCODE || ' ' || sqlerrm;
                    INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
                    VALUES(NULL, rec_utbet_det.behandlings_id, l_error_melding, v_storedate, 'FAM_BT_UTPAKKING_OFFSET3');
                    COMMIT;
                    p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
                END;

                BEGIN
                INSERT INTO dvh_fam_bt.fam_bt_utbet_det
                (
                  pk_bt_utbet_det
                 ,klassekode
                 ,delytelse_id
                 ,utbetalt_pr_mnd
                 ,lastet_dato
                 ,fk_bt_person
                 ,fk_bt_utbetaling
                 ,behandlings_id
                 ,kafka_offset
                )
                VALUES
                (
                  dvh_fambt_kafka.hibernate_sequence.NEXTVAL
                  --v_pk_bt_utbet_det
                 ,rec_utbet_det.klassekode
                 ,rec_utbet_det.delytelse_id
                 ,rec_utbet_det.utbetalt_pr_mnd
                 ,rec_utbet_det.lastet_dato
                 ,v_pk_person_utb
                 ,v_pk_bt_utbetaling
                 ,rec_utbet_det.behandlings_id
                 ,rec_utbet_det.kafka_offset
                );
                EXCEPTION
                  WHEN OTHERS THEN
                    l_error_melding := SQLCODE || ' ' || sqlerrm;
                    INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
                    VALUES(NULL, rec_utbet_det.behandlings_id, l_error_melding, v_storedate, 'FAM_BT_UTPAKKING_OFFSET4');
                    COMMIT;
                    p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
                END;
              EXCEPTION
                WHEN OTHERS THEN
                  l_error_melding := SQLCODE || ' ' || sqlerrm;
                  INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
                  VALUES(NULL, rec_fag.fagsak_id, l_error_melding, v_storedate, 'FAM_BT_UTPAKKING_OFFSET5');
                  COMMIT;
                  p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
              END;
            END LOOP;--Utbetalingsdetaljer
          EXCEPTION
            WHEN OTHERS THEN
              l_error_melding := SQLCODE || ' ' || sqlerrm;
              INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
              VALUES(NULL, rec_fag.fagsak_id, l_error_melding, v_storedate, 'FAM_BT_UTPAKKING_OFFSET6');
              COMMIT;
              p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
          END;
        END LOOP;--Utbetalinger
      EXCEPTION
        WHEN OTHERS THEN
          l_error_melding := SQLCODE || ' ' || sqlerrm;
          INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
          VALUES(NULL, rec_fag.fagsak_id, l_error_melding, v_storedate, 'FAM_BT_UTPAKKING_OFFSET7');
          COMMIT;
          p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
      END;
    END LOOP;--Fagsak
    COMMIT;
    IF l_error_melding IS NOT NULL THEN
      INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(ID, error_msg, opprettet_tid, kilde)
      VALUES(NULL, l_error_melding, v_storedate, 'FAM_BT_UTPAKKING_OFFSET8');
      COMMIT;
      p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      l_error_melding := SQLCODE || ' ' || sqlerrm;
      INSERT INTO dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, ID, error_msg, opprettet_tid, kilde)
      VALUES(NULL, NULL, l_error_melding, v_storedate, 'FAM_BT_UTPAKKING_OFFSET9');
      COMMIT;
      p_error_melding := substr(p_error_melding || l_error_melding, 1, 1000);
  END fam_bt2_utpakking_offset;

END fam_bt;
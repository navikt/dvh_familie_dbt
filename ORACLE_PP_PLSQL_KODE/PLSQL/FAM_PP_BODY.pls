create or replace PACKAGE BODY FAM_PP AS

procedure fam_pp_slett_offset(p_in_offset in varchar2, p_out_error out varchar2) as
  v_error_melding varchar2(4000);
  cursor cur_fagsak is
    select pk_pp_fagsak
    from dvh_fam_pp.fam_pp_fagsak
    where kafka_offset = p_in_offset;
  cursor cur_diagnose(p_fk_fagsak number) is
    select pk_pp_diagnose
    from dvh_fam_pp.fam_pp_diagnose
    where fk_pp_fagsak = p_fk_fagsak;
  cursor cur_perioder(p_fk_fagsak number) is
    select pk_pp_perioder
    from DVH_FAM_PP.fam_pp_perioder
    where fk_pp_fagsak = p_fk_fagsak;
  cursor cur_periode_aarsak(p_fk_perioder number) is
    select pk_pp_periode_aarsak
    from DVH_FAM_PP.fam_pp_periode_aarsak
    where fk_pp_perioder = p_fk_perioder;
  cursor cur_periode_inngangsvilkaar(p_fk_perioder number) is
    select pk_pp_periode_inngangsvilkaar
    from DVH_FAM_PP.fam_pp_periode_inngangsvilkaar
    where fk_pp_perioder = p_fk_perioder;
  cursor cur_periode_utbet_grader(p_fk_perioder number) is
    select pk_pp_periode_utbet_grader
    from DVH_FAM_PP.fam_pp_periode_utbet_grader
    where fk_pp_perioder = p_fk_perioder;
  cursor cur_periode_inngangsvilkaar_detaljertUtfall(p_fk_periode_inngangsvilkaar number) is
    select pk_vilkaar_detaljert_utfall
    from dvh_fam_pp.fam_vilkaar_detaljer_utfall
    where fk_pp_periode_inngangsvilkaar = p_fk_periode_inngangsvilkaar;
begin
  for rec_fagsak in cur_fagsak loop
    v_error_melding := null;
    savepoint do_delete;

    for rec_perioder in cur_perioder(rec_fagsak.pk_pp_fagsak) loop
      for rec_periode_aarsak in cur_periode_aarsak(rec_perioder.pk_pp_perioder) loop
        begin
          delete from dvh_fam_pp.fam_pp_periode_aarsak
          where pk_pp_periode_aarsak = rec_periode_aarsak.pk_pp_periode_aarsak;
        exception
          when others then
            v_error_melding := substr(v_error_melding || sqlcode || ' ' || sqlerrm, 1, 1000);
        end;
      end loop;--periode_aarsak
      for rec_periode_inngangsvilkaar in cur_periode_inngangsvilkaar(rec_perioder.pk_pp_perioder) loop
        for rec_periode_inngangsvilkaar_detUtfall in cur_periode_inngangsvilkaar_detaljertUtfall(rec_periode_inngangsvilkaar.pk_pp_periode_inngangsvilkaar) loop
			begin
			  delete from dvh_fam_pp.fam_vilkaar_detaljer_utfall
			  where pk_vilkaar_detaljert_utfall = rec_periode_inngangsvilkaar_detUtfall.pk_vilkaar_detaljert_utfall;
			exception
              when others then
               v_error_melding := substr(v_error_melding || sqlcode || ' ' || sqlerrm, 1, 1000);
			end;
        end loop;--inngangsvilkaar_detUtfall
        if v_error_melding is not null then
			rollback to do_delete; continue;
			insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
			values(null, null, v_error_melding, sysdate, 'FAM_PP_SLETT_OFFSET:øvrige periode tabeller');
			commit;
			p_out_error := substr(p_out_error || v_error_melding, 1, 1000);
			exit;
		else
			begin
			  delete from dvh_fam_pp.fam_pp_periode_inngangsvilkaar
              where pk_pp_periode_inngangsvilkaar = rec_periode_inngangsvilkaar.pk_pp_periode_inngangsvilkaar;
			exception
			  when others then
				rollback to do_delete; continue;
				v_error_melding := substr(sqlcode || ' ' || sqlerrm, 1, 1000);
				insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
				values(null, null, v_error_melding, sysdate, 'FAM_PP_SLETT_OFFSET:periode tabellen');
				commit;
				p_out_error := substr(p_out_error || v_error_melding, 1, 1000);
				exit;
			end;
		end if;
	  end loop; -- periode_inngangsvilkaar
      for rec_periode_utbet_grader in cur_periode_utbet_grader(rec_perioder.pk_pp_perioder) loop
        begin
          delete from dvh_fam_pp.fam_pp_periode_utbet_grader
          where pk_pp_periode_utbet_grader = rec_periode_utbet_grader.pk_pp_periode_utbet_grader;
        exception
          when others then
            v_error_melding := substr(v_error_melding || sqlcode || ' ' || sqlerrm, 1, 1000);
        end;
      end loop;--periode_utbet_grader

      if v_error_melding is not null then
        rollback to do_delete; continue;
        insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
        values(null, null, v_error_melding, sysdate, 'FAM_PP_SLETT_OFFSET:øvrige periode tabeller');
        commit;
        p_out_error := substr(p_out_error || v_error_melding, 1, 1000);
        exit;
      else
        begin
          delete from dvh_fam_pp.fam_pp_perioder
          where pk_pp_perioder = rec_perioder.pk_pp_perioder;
        exception
          when others then
            rollback to do_delete; continue;
            v_error_melding := substr(sqlcode || ' ' || sqlerrm, 1, 1000);
            insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
            values(null, null, v_error_melding, sysdate, 'FAM_PP_SLETT_OFFSET:periode tabellen');
            commit;
            p_out_error := substr(p_out_error || v_error_melding, 1, 1000);
            exit;
        end;
      end if;
    end loop;--perioder

    if v_error_melding is null then
      for rec_diagnose in cur_diagnose(rec_fagsak.pk_pp_fagsak) loop
        begin
          delete from DVH_FAM_PP.fam_pp_diagnose
          where pk_pp_diagnose = rec_diagnose.pk_pp_diagnose;
        exception
          when others then
            rollback to do_delete; continue;
            v_error_melding := substr(sqlcode || ' ' || sqlerrm, 1, 1000);
            insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
            values(null, null, v_error_melding, sysdate, 'FAM_PP_SLETT_OFFSET:diagnose');
            commit;
            p_out_error := substr(p_out_error || v_error_melding, 1, 1000);
            exit;
        end;
      end loop;--diagnose
    end if;--periode feiler ikke
    if v_error_melding is null then
      begin
        delete from DVH_FAM_PP.fam_pp_fagsak
        where pk_pp_fagsak = rec_fagsak.pk_pp_fagsak;
        commit;--commit for alle tabeller
      exception
        when others then
          rollback to do_delete; continue;
          v_error_melding := substr(sqlcode || ' ' || sqlerrm, 1, 1000);
          insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
          values(null, null, v_error_melding, sysdate, 'FAM_PP_SLETT_OFFSET:fagsak');
          commit;
          p_out_error := substr(p_out_error || v_error_melding, 1, 1000);
      end;
    end if;--både periode og diagnose feiler ikke
  end loop;--fagsak
exception
  when others then
    v_error_melding := substr(sqlcode || ' ' || sqlerrm, 1, 1000);
    insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
    values(null, null, v_error_melding, sysdate, 'FAM_PP_SLETT_OFFSET');
    commit;
    p_out_error := substr(p_out_error || v_error_melding, 1, 1000);
end fam_pp_slett_offset;

procedure fam_pp_slett_kode67(p_out_antall_slettet out number, p_out_error out varchar2) as
  v_antall number := 0;
  v_error_melding varchar2(4000);

  cursor cur_fagsak is
    select distinct kafka_offset
    from dvh_fam_pp.fam_pp_fagsak
    where saksnummer in (select distinct saksnummer
                         from dvh_fam_pp.fam_pp_fagsak
                         where fk_person1_mottaker = -1
                         or (fk_person1_pleietrengende = -1 and ytelse_type!='OMP'));
    --and saksnummer = '1DMCGV0';--TEST!!!
begin
  for rec_fagsak in cur_fagsak loop
    dvh_fam_pp.fam_pp.fam_pp_slett_offset(rec_fagsak.kafka_offset, v_error_melding);
    p_out_error := substr(p_out_error || v_error_melding, 1, 1000);
    if v_error_melding is null then
      v_antall := v_antall + 1;
    end if;
  end loop;
  p_out_antall_slettet := v_antall;
exception
  when others then
    v_error_melding := substr(sqlcode || ' ' || sqlerrm, 1, 1000);
    insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
    values(null, null, v_error_melding, sysdate, 'FAM_PP_SLETT_KODE67');
    commit;
    p_out_error := substr(p_out_error || v_error_melding, 1, 1000);
    p_out_antall_slettet := 0;
end fam_pp_slett_kode67;

function fam_pp_fk_person1(p_in_gyldig_dato in date, p_in_personident in varchar2) return number as
  l_fk_person1 number;
begin
  select max(fk_person1) keep
        (dense_rank first order by gyldig_fra_dato desc) as fk_person1
  into l_fk_person1
  from dt_person.dvh_person_ident_off_id_ikke_skjermet
  where off_id = p_in_personident
  and p_in_gyldig_dato between gyldig_fra_dato and gyldig_til_dato
  group by off_id;
  return l_fk_person1;
exception
  when others then
    return -1;
end fam_pp_fk_person1;

procedure fam_pp_utpakking_offset(p_in_offset in number, p_out_error out varchar2) as
  cursor cur_fagsak(p_offset in number) is
    with jdata as (
      select fam_pp_meta_data.kafka_topic
            ,fam_pp_meta_data.kafka_offset
            ,fam_pp_meta_data.kafka_partition
            ,fam_pp_meta_data.pk_pp_meta_data
            ,fam_pp_meta_data.melding as doc
      from dvh_fam_pp.fam_pp_meta_data
      left join dvh_fam_pp.fam_pp_fagsak
      on fam_pp_meta_data.kafka_offset = fam_pp_fagsak.kafka_offset
      where fam_pp_meta_data.kafka_offset = p_offset
      --and fam_pp_fagsak.kafka_offset is null--TEST!!!
    )
    select t.behandlings_id, t.pleietrengende, t.saksnummer
          ,t.soker, t.utbetalingsreferanse, t.ytelse_type
          ,cast(to_timestamp_tz(t.vedtaks_tidspunkt,'yyyy-mm-dd"T"hh24:mi:ss.ff+tzh:tzm')
                at time zone 'europe/belgrade' as timestamp) as vedtaks_tidspunkt
          ,t.forrige_behandlings_id
          ,kafka_topic, kafka_offset, kafka_partition, pk_pp_meta_data
    from jdata
        ,json_table
        (
          doc, '$'
          columns (
          behandlings_id         varchar2 path '$.behandlingUuid'
         ,pleietrengende         varchar2 path '$.pleietrengende'
         ,saksnummer             varchar2 path '$.saksnummer'
         ,soker                  varchar2 path '$.søker'
         ,utbetalingsreferanse   varchar2 path '$.utbetalingsreferanse'
         ,ytelse_type            varchar2 path '$.ytelseType'
         ,vedtaks_tidspunkt      varchar2 path '$.vedtakstidspunkt'
         ,forrige_behandlings_id varchar2 path '$.forrigeBehandlingUuid'
         )
        ) t;

  cursor cur_diagnose(p_offset in number) is
    with jdata as (
      select melding as doc
      from dvh_fam_pp.fam_pp_meta_data
      where kafka_offset = p_offset
    )
    select t.kode, t.type
    from jdata
        ,json_table
        (
         doc, '$'
         columns (
         nested path '$.diagnosekoder[*]' columns (
         kode varchar2 path '$.kode'
        ,type varchar2 path '$.type'
         )
        )
        ) t;

  cursor cur_perioder(p_offset in number) is
    with jdata as (
      select melding as doc
      from dvh_fam_pp.fam_pp_meta_data
      where kafka_offset = p_offset
    )
    select t.beredskap, to_number(t.brutto_beregningsgrunnlag) as brutto_beregningsgrunnlag
          ,to_date(t.dato_fom,'yyyy-mm-dd') as dato_fom, to_date(t.dato_tom,'yyyy-mm-dd') as dato_tom
          ,t.gmt_andre_sokers_tilsyn, t.gmt_etablert_tilsyn, t.gmt_overse_etablert_tilsyn_aarsak
          ,t.gmt_tilgjengelig_for_soker, t.nattevaak, t.oppgitt_tilsyn, t.pleiebehov, t.sokers_tapte_timer
          ,t.utfall, t.uttaksgrad, t.sokers_tapte_arbeidstid
    from jdata
        ,json_table
        (
          doc, '$'
          columns (
          nested path '$.perioder[*]' columns (
          beredskap                         varchar2 path '$.beredskap'
         ,brutto_beregningsgrunnlag         varchar2 path '$.bruttoBeregningsgrunnlag'
         ,dato_fom                          varchar2 path '$.fom'
         ,dato_tom                          varchar2 path '$.tom'
         ,gmt_andre_sokers_tilsyn           varchar2 path '$.graderingMotTilsyn.andreSøkeresTilsyn'
         ,gmt_etablert_tilsyn               varchar2 path '$.graderingMotTilsyn.etablertTilsyn'
         ,gmt_overse_etablert_tilsyn_aarsak varchar2 path '$.graderingMotTilsyn.overseEtablertTilsynÅrsak'
         ,gmt_tilgjengelig_for_soker        varchar2 path '$.graderingMotTilsyn.tilgjengeligForSøker'
         ,nattevaak                         varchar2 path '$.nattevåk'
         ,oppgitt_tilsyn                    varchar2 path '$.oppgittTilsyn'
         ,pleiebehov                        varchar2 path '$.pleiebehov'
         ,sokers_tapte_timer                varchar2 path '$.søkersTapteTimer'
         ,utfall                            varchar2 path '$.utfall'
         ,uttaksgrad                        varchar2 path '$.uttaksgrad'
         ,sokers_tapte_arbeidstid           varchar2 path '$.søkersTapteArbeidstid'
           )
        )
        ) t;

  cursor cur_periode_inngangsvilkaar(p_offset in number) is
    with jdata as (
      select melding as doc
      from dvh_fam_pp.fam_pp_meta_data
      where kafka_offset = p_offset
    )
    select t.*
    from jdata
        ,json_table (
         doc, '$'
         columns (
         nested path '$.perioder.inngangsvilkår[*]' columns (
         utfall  varchar2 path '$.utfall'
        ,vilkaar varchar2 path '$.vilkår'
         )
        )
        ) t;

  cursor cur_periode_utbet_grader(p_offset in number) is
    with jdata as (
      select melding as doc
      from dvh_fam_pp.fam_pp_meta_data
      where kafka_offset = p_offset
    )
    select t.*
          ,null as delytelse_id_direkte
          ,null as delytelse_id_refusjon
    from jdata
        ,json_table (
         doc, '$'
         columns (
         nested path '$.perioder.utbetalingsgrader[*]' columns (
         arbeidsforhold_aktorid varchar2 path '$.arbeidsforhold.aktørId'
        ,arbeidsforhold_id      varchar2 path '$.arbeidsforhold.arbeidsforholdId'
        ,arbeidsforhold_orgnr   varchar2 path '$.arbeidsforhold.organisasjonsnummer'
        ,arbeidsforhold_type    varchar2 path '$.arbeidsforhold.type'
        ,dagsats                varchar2 path '$.dagsats'
        ,faktisk_arbeidstid     varchar2 path '$.faktiskArbeidstid'
        ,normal_arbeidstid      varchar2 path '$.normalArbeidstid'
        ,utbetalingsgrad        varchar2 path '$.utbetalingsgrad'
        ,bruker_er_mottaker     varchar2 path '$.brukerErMottaker'
         )
        )
        ) t;

  cursor cur_periode_aarsak(p_offset in number) is
    with jdata as (
      select melding as doc
      from dvh_fam_pp.fam_pp_meta_data
      where kafka_offset = p_offset
    )
    select t.*
    from jdata
        ,json_table (
         doc, '$'
         columns (
         nested path '$.perioder.årsaker[*]' columns (
         aarsak  varchar2 path '$[0]'
         )
        )
        ) t;

  v_pk_pp_fagsak number;
  v_pk_pp_perioder number;

  v_fk_person1_soker number;
  v_fk_person1_pleietrengende number;

  v_error_melding varchar2(4000);
  v_commit number := 0;
  v_feil_kilde_navn varchar2(100) := null;
begin
  for rec_fagsak in cur_fagsak(p_in_offset) loop
    begin
      savepoint do_insert;
      v_pk_pp_fagsak := -1;
      v_fk_person1_soker := -1;
      v_fk_person1_pleietrengende := -1;
      select dvh_fampp_kafka.hibernate_sequence.nextval into v_pk_pp_fagsak from dual;
      --Hent fk_person1
      v_fk_person1_soker := fam_pp_fk_person1(rec_fagsak.vedtaks_tidspunkt, rec_fagsak.soker);
      v_fk_person1_pleietrengende := fam_pp_fk_person1(rec_fagsak.vedtaks_tidspunkt, rec_fagsak.pleietrengende);
      insert into dvh_fam_pp.fam_pp_fagsak
      (
        pk_pp_fagsak, behandlings_id, fk_person1_mottaker, fk_person1_pleietrengende
       ,kafka_offset, kafka_partition, kafka_topic, lastet_dato
       ,pleietrengende, saksnummer, soker, utbetalingsreferanse
       ,ytelse_type, fk_pp_metadata, vedtaks_tidspunkt, forrige_behandlings_id
      )
      values
      (
        v_pk_pp_fagsak, rec_fagsak.behandlings_id, v_fk_person1_soker, v_fk_person1_pleietrengende
       ,rec_fagsak.kafka_offset, rec_fagsak.kafka_partition, rec_fagsak.kafka_topic, sysdate
       ,rec_fagsak.pleietrengende, rec_fagsak.saksnummer, rec_fagsak.soker, rec_fagsak.utbetalingsreferanse
       ,rec_fagsak.ytelse_type, rec_fagsak.pk_pp_meta_data, rec_fagsak.vedtaks_tidspunkt
       ,rec_fagsak.forrige_behandlings_id
      );

      --Diagnose
      for rec_diagnose in cur_diagnose(rec_fagsak.kafka_offset) loop
        begin
          insert into dvh_fam_pp.fam_pp_diagnose(kode, type, fk_pp_fagsak, lastet_dato)
          values(rec_diagnose.kode, rec_diagnose.type, v_pk_pp_fagsak, sysdate);
        exception
          when others then
            v_error_melding := substr(sqlcode||sqlerrm,1,1000);
            v_feil_kilde_navn := 'FAM_PP_DIAGNOSE';
            p_out_error := substr(v_error_melding||' '||v_feil_kilde_navn, 1, 1000);
            rollback to do_insert; continue;
            insert into dvh_fam_fp.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
            values(null, v_error_melding, sysdate, v_feil_kilde_navn);
            commit;
            exit;--Ut av diagnose
        end;
      end loop;--Diagnose

      --Perioder
      for rec_perioder in cur_perioder(rec_fagsak.kafka_offset) loop
        begin
          v_pk_pp_perioder := -1;
          select dvh_fampp_kafka.hibernate_sequence.nextval into v_pk_pp_perioder from dual;
          insert into dvh_fam_pp.fam_pp_perioder
          (
            pk_pp_perioder, beredskap, brutto_beregningsgrunnlag, dato_fom, dato_tom
           ,gmt_andre_sokers_tilsyn, gmt_etablert_tilsyn, gmt_overse_etablert_tilsyn_aarsak
           ,gmt_tilgjengelig_for_soker, nattevaak, oppgitt_tilsyn, pleiebehov
           ,sokers_tapte_timer, utfall, uttaksgrad, fk_pp_fagsak, sokers_tapte_arbeidstid
           ,lastet_dato
          )
          values
          (
            v_pk_pp_perioder, rec_perioder.beredskap, rec_perioder.brutto_beregningsgrunnlag
           ,rec_perioder.dato_fom, rec_perioder.dato_tom
           ,rec_perioder.gmt_andre_sokers_tilsyn, rec_perioder.gmt_etablert_tilsyn
           ,rec_perioder.gmt_overse_etablert_tilsyn_aarsak, rec_perioder.gmt_tilgjengelig_for_soker
           ,rec_perioder.nattevaak, rec_perioder.oppgitt_tilsyn, rec_perioder.pleiebehov
          ,rec_perioder.sokers_tapte_timer, rec_perioder.utfall, rec_perioder.uttaksgrad
          ,v_pk_pp_fagsak, rec_perioder.sokers_tapte_arbeidstid
          ,sysdate
          );

          --Inngangsvilkaar
          for rec_vilkaar in cur_periode_inngangsvilkaar(rec_fagsak.kafka_offset) loop
            begin
              insert into dvh_fam_pp.fam_pp_periode_inngangsvilkaar(utfall, vilkaar, fk_pp_perioder, lastet_dato)
              values(rec_vilkaar.utfall, rec_vilkaar.vilkaar, v_pk_pp_perioder, sysdate);
            exception
              when others then
                v_error_melding := substr(sqlcode||sqlerrm,1,1000);
                v_feil_kilde_navn := 'FAM_PP_PERIODE_INNGANGSVILKAAR';
                p_out_error := substr(v_error_melding||' '||v_feil_kilde_navn, 1, 1000);
                rollback to do_insert; continue;
                insert into dvh_fam_fp.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
                values(null, v_error_melding, sysdate, v_feil_kilde_navn);
                commit;
                exit;--Ut av inngangsvilkår
            end;
          end loop;--Inngangsvilkaar

          --Utbet_grader
          for rec_utbet in cur_periode_utbet_grader(rec_fagsak.kafka_offset) loop
            begin
              insert into dvh_fam_pp.fam_pp_periode_utbet_grader
              (
                arbeidsforhold_aktorid, arbeidsforhold_id, arbeidsforhold_orgnr
               ,arbeidsforhold_type, dagsats, delytelse_id_direkte, delytelse_id_refusjon
               ,faktisk_arbeidstid, normal_arbeidstid, utbetalingsgrad
               ,bruker_er_mottaker, fk_pp_perioder, lastet_dato
              )
              values
              (
                rec_utbet.arbeidsforhold_aktorid, rec_utbet.arbeidsforhold_id, rec_utbet.arbeidsforhold_orgnr
               ,rec_utbet.arbeidsforhold_type, rec_utbet.dagsats, rec_utbet.delytelse_id_direkte, rec_utbet.delytelse_id_refusjon
               ,rec_utbet.faktisk_arbeidstid, rec_utbet.normal_arbeidstid, rec_utbet.utbetalingsgrad
               ,rec_utbet.bruker_er_mottaker, v_pk_pp_perioder, sysdate
              );
            exception
              when others then
                v_error_melding := substr(sqlcode||sqlerrm,1,1000);
                v_feil_kilde_navn := 'FAM_PP_PERIODE_UTBET_GRADER';
                p_out_error := substr(v_error_melding||' '||v_feil_kilde_navn, 1, 1000);
                rollback to do_insert; continue;
                insert into dvh_fam_fp.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
                values(null, v_error_melding, sysdate, v_feil_kilde_navn);
                commit;
                exit;--Ut av utbet_grader
            end;
          end loop;--Utbet_grader

          --Aarsak
          for rec_aarsak in cur_periode_aarsak(rec_fagsak.kafka_offset) loop
            begin
              insert into dvh_fam_pp.fam_pp_periode_aarsak(aarsak, fk_pp_perioder, lastet_dato)
              values(rec_aarsak.aarsak, v_pk_pp_perioder, sysdate);
            exception
              when others then
                v_error_melding := substr(sqlcode||sqlerrm,1,1000);
                v_feil_kilde_navn := 'FAM_PP_PERIODE_AARSAK';
                p_out_error := substr(v_error_melding||' '||v_feil_kilde_navn, 1, 1000);
                rollback to do_insert; continue;
                insert into dvh_fam_fp.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
                values(null, v_error_melding, sysdate, v_feil_kilde_navn);
                commit;
                exit;--Ut av årsak
            end;
          end loop; --Aarsak
        exception
          when others then
            v_error_melding := substr(sqlcode||sqlerrm,1,1000);
            v_feil_kilde_navn := 'FAM_PP_PERIODER';
            p_out_error := substr(v_error_melding||' '||v_feil_kilde_navn, 1, 1000);
            rollback to do_insert; continue;
            insert into dvh_fam_fp.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
            values(null, v_error_melding, sysdate, v_feil_kilde_navn);
            commit;
            exit;--Ut av perioder
        end;
      end loop;--Perioder

      v_commit := v_commit + 1;
      if v_commit >= 10000 then
        commit;
        v_commit := 0;
      end if;
    exception
      when others then
        v_error_melding := substr(sqlcode||sqlerrm,1,1000);
        v_feil_kilde_navn := 'FAM_PP_FAGSAK';
        p_out_error := substr(v_error_melding||' '||v_feil_kilde_navn, 1, 1000);
        rollback to do_insert; continue;
        insert into dvh_fam_fp.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
        values(null, v_error_melding, sysdate, v_feil_kilde_navn);
        commit;
        --Gå til neste fagsak
    end;
  end loop;--Fagsak
  commit;
exception
  when others then
    v_error_melding := substr(sqlcode||sqlerrm,1,1000);
    insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
    values(null, null, v_error_melding, sysdate, 'FAM_PP_UTPAKKING');
    commit;
    p_out_error := substr(p_out_error || v_error_melding, 1, 1000);
end fam_pp_utpakking_offset;



procedure fam_pp_utpakking_offset_2(p_in_offset in number, p_out_error out varchar2) as
  cursor cur_fagsak(p_offset in number) is
    with jdata as (
      select fam_pp_meta_data.kafka_topic
            ,fam_pp_meta_data.kafka_offset
            ,fam_pp_meta_data.kafka_partition
            ,fam_pp_meta_data.pk_pp_meta_data
            ,fam_pp_meta_data.melding as doc
      from dvh_fam_pp.fam_pp_meta_data
      left join dvh_fam_pp.fam_pp_fagsak
      on fam_pp_meta_data.kafka_offset = fam_pp_fagsak.kafka_offset
      where fam_pp_meta_data.kafka_offset = p_offset
      --and fam_pp_fagsak.kafka_offset is null--TEST!!!
    )
    select t.behandlings_id, t.pleietrengende, t.saksnummer
          ,t.soker, t.utbetalingsreferanse, t.ytelse_type
          ,cast(to_timestamp_tz(t.vedtaks_tidspunkt,'yyyy-mm-dd"T"hh24:mi:ss.ff+tzh:tzm')
                at time zone 'europe/belgrade' as timestamp) as vedtaks_tidspunkt
          ,t.forrige_behandlings_id
          ,kafka_topic, kafka_offset, kafka_partition, pk_pp_meta_data
    from jdata
        ,json_table
        (
          doc, '$'
          columns (
          behandlings_id         varchar2 path '$.behandlingUuid'
         ,pleietrengende         varchar2 path '$.pleietrengende'
         ,saksnummer             varchar2 path '$.saksnummer'
         ,soker                  varchar2 path '$.søker'
         ,utbetalingsreferanse   varchar2 path '$.utbetalingsreferanse'
         ,ytelse_type            varchar2 path '$.ytelseType'
         ,vedtaks_tidspunkt      varchar2 path '$.vedtakstidspunkt'
         ,forrige_behandlings_id varchar2 path '$.forrigeBehandlingUuid'
         )
        ) t;

  cursor cur_diagnose(p_offset in number) is
    with jdata as (
      select melding as doc
      from dvh_fam_pp.fam_pp_meta_data
      where kafka_offset = p_offset
    )
    select t.kode, t.type
    from jdata
        ,json_table
        (
         doc, '$'
         columns (
         nested path '$.diagnosekoder[*]' columns (
         kode varchar2 path '$.kode'
        ,type varchar2 path '$.type'
         )
        )
        ) t
        where t.kode is not null;

  cursor cur_perioder(p_offset in number) is
    with jdata as (
      select melding as doc
      from dvh_fam_pp.fam_pp_meta_data
      where kafka_offset = p_offset
    )
    select t.beredskap, to_number(t.brutto_beregningsgrunnlag,'999999999.99') as brutto_beregningsgrunnlag
          ,to_date(t.dato_fom,'yyyy-mm-dd') as dato_fom, to_date(t.dato_tom,'yyyy-mm-dd') as dato_tom
          ,t.gmt_andre_sokers_tilsyn, t.gmt_etablert_tilsyn, t.gmt_overse_etablert_tilsyn_aarsak
          ,t.gmt_tilgjengelig_for_soker, t.nattevaak, t.oppgitt_tilsyn, t.pleiebehov, t.sokers_tapte_timer
          ,t.utfall, t.uttaksgrad, t.sokers_tapte_arbeidstid
    from jdata
        ,json_table
        (
          doc, '$'
          columns (
          nested path '$.perioder[*]' columns (
          beredskap                         varchar2 path '$.beredskap'
         ,brutto_beregningsgrunnlag         varchar2 path '$.bruttoBeregningsgrunnlag'
         ,dato_fom                          varchar2 path '$.fom'
         ,dato_tom                          varchar2 path '$.tom'
         ,gmt_andre_sokers_tilsyn           varchar2 path '$.graderingMotTilsyn.andreSøkeresTilsyn'
         ,gmt_etablert_tilsyn               varchar2 path '$.graderingMotTilsyn.etablertTilsyn'
         ,gmt_overse_etablert_tilsyn_aarsak varchar2 path '$.graderingMotTilsyn.overseEtablertTilsynÅrsak'
         ,gmt_tilgjengelig_for_soker        varchar2 path '$.graderingMotTilsyn.tilgjengeligForSøker'
         ,nattevaak                         varchar2 path '$.nattevåk'
         ,oppgitt_tilsyn                    varchar2 path '$.oppgittTilsyn'
         ,pleiebehov                        varchar2 path '$.pleiebehov'
         ,sokers_tapte_timer                varchar2 path '$.søkersTapteTimer'
         ,utfall                            varchar2 path '$.utfall'
         ,uttaksgrad                        varchar2 path '$.uttaksgrad'
         ,sokers_tapte_arbeidstid           varchar2 path '$.søkersTapteArbeidstid'
           )
        )
        ) t;

  cursor cur_periode_inngangsvilkaar(p_offset in number, p_fom date, p_tom date) is
    with jdata as (
      select melding as doc
      from dvh_fam_pp.fam_pp_meta_data
      where kafka_offset = p_offset
    )
    select t.*
    from jdata
        ,json_table (
         doc, '$'
         columns (
		 nested path '$.perioder[*]' columns (
           dato_fom         date path '$.fom'
          ,dato_tom         date path '$.tom'
        , nested path '$.inngangsvilkår[*]' columns (
         utfall  varchar2 path '$.utfall'
        ,vilkaar varchar2 path '$.vilkår'
         )
         )
		)
        ) t
      --where dato_fom=to_date(p_fom, 'dd.mm.yyyy') and dato_tom=to_date(p_tom, 'dd.mm.yyyy');
      where dato_fom = p_fom and dato_tom = p_tom
      ;


  cursor cur_periode_inngangsvilkaar_detaljertUtfall(p_offset in number, p_fom date, p_tom date, p_utfall string, p_vilkaar string) is
    with jdata as (
      select melding as doc
      from dvh_fam_pp.fam_pp_meta_data
      where kafka_offset = p_offset
    )
    select t.*
    from jdata
        ,json_table(
        doc, '$'
         columns (
		 nested path '$.perioder[*]' columns (
           dato_fom         date path '$.fom'
          ,dato_tom         date path '$.tom'
        , nested path '$.inngangsvilkår[*]' columns (
         inn_utfall  varchar2 path '$.utfall'
        ,inn_vilkaar varchar2 path '$.vilkår',
        nested path '$.detaljertUtfall[*]' columns (
        gjelderKravstiller         varchar2 path '$.gjelderKravstiller',
        gjelderAktivitetType       varchar2 path '$.gjelderAktivitetType',
        gjelderOrganisasjonsnummer varchar2 path '$.gjelderOrganisasjonsnummer',
        gjelderAktorId             varchar2 path '$.gjelderAktørId',
        gjelderArbeidsforholdId    varchar2 path '$.gjelderArbeidsforholdId',
        utfall   varchar2 path '$.utfall'
        )
        )
        )
        )
        )
        t
        --WHERE
      WHERE DATO_FOM = p_fom AND DATO_TOM = p_tom AND INN_UTFALL=p_utfall AND INN_VILKAAR=p_vilkaar
      AND  t.utfall IS NOT NULL
        ;






  cursor cur_periode_utbet_grader(p_offset in number) is
    with jdata as (
      select melding as doc
      from dvh_fam_pp.fam_pp_meta_data
      where kafka_offset = p_offset
    )
    select t.*
          ,null as delytelse_id_direkte
          ,null as delytelse_id_refusjon
    from jdata
        ,json_table (
         doc, '$'
         columns (
         nested path '$.perioder.utbetalingsgrader[*]' columns (
         arbeidsforhold_aktorid varchar2 path '$.arbeidsforhold.aktørId'
        ,arbeidsforhold_id      varchar2 path '$.arbeidsforhold.arbeidsforholdId'
        ,arbeidsforhold_orgnr   varchar2 path '$.arbeidsforhold.organisasjonsnummer'
        ,arbeidsforhold_type    varchar2 path '$.arbeidsforhold.type'
        ,dagsats                varchar2 path '$.dagsats'
        ,faktisk_arbeidstid     varchar2 path '$.faktiskArbeidstid'
        ,normal_arbeidstid      varchar2 path '$.normalArbeidstid'
        ,utbetalingsgrad        varchar2 path '$.utbetalingsgrad'
        ,bruker_er_mottaker     varchar2 path '$.brukerErMottaker'
         )
        )
        ) t;

  cursor cur_periode_aarsak(p_offset in number) is
    with jdata as (
      select melding as doc
      from dvh_fam_pp.fam_pp_meta_data
      where kafka_offset = p_offset
    )
    select t.*
    from jdata
        ,json_table (
         doc, '$'
         columns (
         nested path '$.perioder.årsaker[*]' columns (
         aarsak  varchar2 path '$[0]'
         )
        )
        ) t;

  v_pk_pp_fagsak number;
  v_pk_pp_perioder number;
  v_pk_pp_periode_inngangsvilkaar number;

  v_fk_person1_soker number;
  v_fk_person1_pleietrengende number;

  v_error_melding varchar2(4000);
  v_commit number := 0;
  v_feil_kilde_navn varchar2(100) := null;
begin
  for rec_fagsak in cur_fagsak(p_in_offset) loop
    begin
      savepoint do_insert;
      v_pk_pp_fagsak := -1;
      v_fk_person1_soker := -1;
      v_fk_person1_pleietrengende := -1;
      select dvh_fampp_kafka.hibernate_sequence.nextval into v_pk_pp_fagsak from dual;
      --Hent fk_person1
      v_fk_person1_soker := fam_pp_fk_person1(rec_fagsak.vedtaks_tidspunkt, rec_fagsak.soker);
      v_fk_person1_pleietrengende := fam_pp_fk_person1(rec_fagsak.vedtaks_tidspunkt, rec_fagsak.pleietrengende);
        insert into dvh_fam_pp.fam_pp_fagsak
      (
        pk_pp_fagsak, behandlings_id, fk_person1_mottaker, fk_person1_pleietrengende
       ,kafka_offset, kafka_partition, kafka_topic, lastet_dato
       ,pleietrengende, saksnummer, soker, utbetalingsreferanse
       ,ytelse_type, fk_pp_metadata, vedtaks_tidspunkt, forrige_behandlings_id
      )
      values
      (
        v_pk_pp_fagsak, rec_fagsak.behandlings_id, v_fk_person1_soker, v_fk_person1_pleietrengende
       ,rec_fagsak.kafka_offset, rec_fagsak.kafka_partition, rec_fagsak.kafka_topic, sysdate
       ,rec_fagsak.pleietrengende, rec_fagsak.saksnummer, rec_fagsak.soker, rec_fagsak.utbetalingsreferanse
       ,rec_fagsak.ytelse_type, rec_fagsak.pk_pp_meta_data, rec_fagsak.vedtaks_tidspunkt
       ,rec_fagsak.forrige_behandlings_id
      );

      --Diagnose
      --if rec_fagsak.ytelse_type != 'OMP' then

      for rec_diagnose in cur_diagnose(rec_fagsak.kafka_offset) loop
        begin
          insert into dvh_fam_pp.fam_pp_diagnose(kode, type, fk_pp_fagsak, lastet_dato)
          values(rec_diagnose.kode, rec_diagnose.type, v_pk_pp_fagsak, sysdate);
        exception
          when others then
            v_error_melding := substr(sqlcode||sqlerrm,1,1000);
            v_feil_kilde_navn := 'FAM_PP_DIAGNOSE';
            p_out_error := substr(v_error_melding||' '||v_feil_kilde_navn, 1, 1000);
            rollback to do_insert; continue;
            insert into dvh_fam_fp.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
            values(null, v_error_melding, sysdate, v_feil_kilde_navn);
            commit;
            exit;--Ut av diagnose
        end;
      end loop;--Diagnose
     -- end if;

      --Perioder
      for rec_perioder in cur_perioder(rec_fagsak.kafka_offset) loop
        begin
          v_pk_pp_perioder := -1;
          select dvh_fampp_kafka.hibernate_sequence.nextval into v_pk_pp_perioder from dual;
          insert into dvh_fam_pp.fam_pp_perioder
          (
            pk_pp_perioder, beredskap, brutto_beregningsgrunnlag, dato_fom, dato_tom
           ,gmt_andre_sokers_tilsyn, gmt_etablert_tilsyn, gmt_overse_etablert_tilsyn_aarsak
           ,gmt_tilgjengelig_for_soker, nattevaak, oppgitt_tilsyn, pleiebehov
           ,sokers_tapte_timer, utfall, uttaksgrad, fk_pp_fagsak, sokers_tapte_arbeidstid
           ,lastet_dato
          )
          values
          (
            v_pk_pp_perioder, rec_perioder.beredskap, rec_perioder.brutto_beregningsgrunnlag
           ,rec_perioder.dato_fom, rec_perioder.dato_tom
           ,rec_perioder.gmt_andre_sokers_tilsyn, rec_perioder.gmt_etablert_tilsyn
           ,rec_perioder.gmt_overse_etablert_tilsyn_aarsak, rec_perioder.gmt_tilgjengelig_for_soker
           ,rec_perioder.nattevaak, rec_perioder.oppgitt_tilsyn, rec_perioder.pleiebehov
          ,rec_perioder.sokers_tapte_timer, rec_perioder.utfall, rec_perioder.uttaksgrad
          ,v_pk_pp_fagsak, rec_perioder.sokers_tapte_arbeidstid
          ,sysdate
          );

          --Inngangsvilkaar
          for rec_vilkaar in cur_periode_inngangsvilkaar(rec_fagsak.kafka_offset, rec_perioder.dato_fom, rec_perioder.dato_tom) loop
            begin
              v_pk_pp_periode_inngangsvilkaar:= -1;
              select dvh_fampp_kafka.hibernate_sequence.nextval into v_pk_pp_periode_inngangsvilkaar from dual;
              insert into dvh_fam_pp.fam_pp_periode_inngangsvilkaar
              (
              pk_pp_periode_inngangsvilkaar,utfall, vilkaar, fk_pp_perioder, lastet_dato--,fom,tom
              )
              values
              (
              v_pk_pp_periode_inngangsvilkaar,rec_vilkaar.utfall, rec_vilkaar.vilkaar, v_pk_pp_perioder, sysdate
              --,rec_perioder.dato_fom, rec_perioder.dato_tom
              );



              if rec_fagsak.ytelse_type = 'OMP' then
              for rec_detUtfall in cur_periode_inngangsvilkaar_detaljertUtfall(rec_fagsak.kafka_offset
              ,rec_perioder.dato_fom, rec_perioder.dato_tom,rec_vilkaar.utfall, rec_vilkaar.vilkaar) loop
                begin
                 insert into dvh_fam_pp.fam_vilkaar_detaljer_utfall
                 (
                    gjelder_kravstiller,gjelder_aktivitet_type,gjelder_organisasjonsnummer,gjelder_aktor_id
                    ,gjelder_arbeidsforhold_id,det_utfall,lastet_dato,fk_pp_periode_inngangsvilkaar
                 )
                 values
                 (
                    rec_detUtfall.gjelderKravstiller, rec_detUtfall.gjelderAktivitetType, rec_detUtfall.gjelderOrganisasjonsnummer, rec_detUtfall.gjelderAktorId
                    ,rec_detUtfall.gjelderArbeidsforholdId, rec_detUtfall.utfall, sysdate, v_pk_pp_periode_inngangsvilkaar
                 );
            exception
              when others then
                v_error_melding := substr(sqlcode||sqlerrm,1,1000);
                v_feil_kilde_navn := 'FAM_VILKAAR_DETALJER_UTFALL';
                p_out_error := substr(v_error_melding||' '||v_feil_kilde_navn, 1, 1000);
                rollback to do_insert; continue;
                insert into dvh_fam_fp.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
                values(null, v_error_melding, sysdate, v_feil_kilde_navn);
                commit;
                exit;--DetaljertUtfall
                end;
              end loop;--DetaljertUtfall
              end if;

            exception
              when others then
                v_error_melding := substr(sqlcode||sqlerrm,1,1000);
                v_feil_kilde_navn := 'FAM_PP_PERIODE_INNGANGSVILKAAR';
                p_out_error := substr(v_error_melding||' '||v_feil_kilde_navn, 1, 1000);
                rollback to do_insert; continue;
                insert into dvh_fam_fp.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
                values(null, v_error_melding, sysdate, v_feil_kilde_navn);
                commit;
                exit;--Ut av inngangsvilkår
            end;
          end loop;--Inngangsvilkaar

          --detaljertUtfall

          --Utbet_grader
          for rec_utbet in cur_periode_utbet_grader(rec_fagsak.kafka_offset) loop
            begin
              insert into dvh_fam_pp.fam_pp_periode_utbet_grader
              (
                arbeidsforhold_aktorid, arbeidsforhold_id, arbeidsforhold_orgnr
               ,arbeidsforhold_type, dagsats, delytelse_id_direkte, delytelse_id_refusjon
               ,faktisk_arbeidstid, normal_arbeidstid, utbetalingsgrad
               ,bruker_er_mottaker, fk_pp_perioder, lastet_dato
              )
              values
              (
                rec_utbet.arbeidsforhold_aktorid, rec_utbet.arbeidsforhold_id, rec_utbet.arbeidsforhold_orgnr
               ,rec_utbet.arbeidsforhold_type, rec_utbet.dagsats, rec_utbet.delytelse_id_direkte, rec_utbet.delytelse_id_refusjon
               ,rec_utbet.faktisk_arbeidstid, rec_utbet.normal_arbeidstid, rec_utbet.utbetalingsgrad
               ,rec_utbet.bruker_er_mottaker, v_pk_pp_perioder, sysdate
              );
            exception
              when others then
                v_error_melding := substr(sqlcode||sqlerrm,1,1000);
                v_feil_kilde_navn := 'FAM_PP_PERIODE_UTBET_GRADER';
                p_out_error := substr(v_error_melding||' '||v_feil_kilde_navn, 1, 1000);
                rollback to do_insert; continue;
                insert into dvh_fam_fp.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
                values(null, v_error_melding, sysdate, v_feil_kilde_navn);
                commit;
                exit;--Ut av utbet_grader
            end;
          end loop;--Utbet_grader

          --Aarsak
          for rec_aarsak in cur_periode_aarsak(rec_fagsak.kafka_offset) loop
            begin
              insert into dvh_fam_pp.fam_pp_periode_aarsak(aarsak, fk_pp_perioder, lastet_dato)
              values(rec_aarsak.aarsak, v_pk_pp_perioder, sysdate);
            exception
              when others then
                v_error_melding := substr(sqlcode||sqlerrm,1,1000);
                v_feil_kilde_navn := 'FAM_PP_PERIODE_AARSAK';
                p_out_error := substr(v_error_melding||' '||v_feil_kilde_navn, 1, 1000);
                rollback to do_insert; continue;
                insert into dvh_fam_fp.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
                values(null, v_error_melding, sysdate, v_feil_kilde_navn);
                commit;
                exit;--Ut av årsak
            end;
          end loop; --Aarsak


        exception
          when others then
            v_error_melding := substr(sqlcode||sqlerrm,1,1000);
            v_feil_kilde_navn := 'FAM_PP_PERIODER';
            p_out_error := substr(v_error_melding||' '||v_feil_kilde_navn, 1, 1000);
            rollback to do_insert; continue;
            insert into dvh_fam_fp.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
            values(null, v_error_melding, sysdate, v_feil_kilde_navn);
            commit;
            exit;--Ut av perioder
        end;
      end loop;--Perioder

      v_commit := v_commit + 1;
      if v_commit >= 10000 then
        commit;
        v_commit := 0;
      end if;
    exception
      when others then
        v_error_melding := substr(sqlcode||sqlerrm,1,1000);
        v_feil_kilde_navn := 'FAM_PP_FAGSAK';
        p_out_error := substr(v_error_melding||' '||v_feil_kilde_navn, 1, 1000);
        rollback to do_insert; continue;
        insert into dvh_fam_fp.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
        values(null, v_error_melding, sysdate, v_feil_kilde_navn);
        commit;
        --Gå til neste fagsak
    end;
  end loop;--Fagsak
  commit;
exception
  when others then
    v_error_melding := substr(sqlcode||sqlerrm,1,1000);
    insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
    values(null, null, v_error_melding, sysdate, 'FAM_PP_UTPAKKING');
    commit;
    p_out_error := substr(p_out_error || v_error_melding, 1, 1000);
end fam_pp_utpakking_offset_2;

procedure fam_pp_utpakking_offset_test(p_in_offset in number, p_out_error out varchar2) as
  cursor cur_fagsak(p_offset in number) is
    with jdata as (
      select fam_pp_meta_data.kafka_topic
            ,fam_pp_meta_data.kafka_offset
            ,fam_pp_meta_data.kafka_partition
            ,fam_pp_meta_data.pk_pp_meta_data
            ,fam_pp_meta_data.melding as doc
      from dvh_fam_pp.fam_pp_meta_data
      left join dvh_fam_pp.fam_pp_fagsak
      on fam_pp_meta_data.kafka_offset = fam_pp_fagsak.kafka_offset
      where fam_pp_meta_data.kafka_offset = p_offset
      and fam_pp_fagsak.kafka_offset is null--TEST!!!
    )
    select t.behandlings_id, t.pleietrengende, t.saksnummer
          ,t.soker, t.utbetalingsreferanse, t.ytelse_type
          ,cast(to_timestamp_tz(t.vedtaks_tidspunkt,'yyyy-mm-dd"T"hh24:mi:ss.ff+tzh:tzm')
                at time zone 'europe/belgrade' as timestamp) as vedtaks_tidspunkt
          ,t.forrige_behandlings_id
          ,kafka_topic, kafka_offset, kafka_partition, pk_pp_meta_data
    from jdata
        ,json_table
        (
          doc, '$'
          columns (
          behandlings_id         varchar2 path '$.behandlingUuid'
         ,pleietrengende         varchar2 path '$.pleietrengende'
         ,saksnummer             varchar2 path '$.saksnummer'
         ,soker                  varchar2 path '$.søker'
         ,utbetalingsreferanse   varchar2 path '$.utbetalingsreferanse'
         ,ytelse_type            varchar2 path '$.ytelseType'
         ,vedtaks_tidspunkt      varchar2 path '$.vedtakstidspunkt'
         ,forrige_behandlings_id varchar2 path '$.forrigeBehandlingUuid'
         )
        ) t;

  cursor cur_diagnose(p_offset in number) is
    with jdata as (
      select melding as doc
      from dvh_fam_pp.fam_pp_meta_data
      where kafka_offset = p_offset
    )
    select t.kode, t.type
    from jdata
        ,json_table
        (
         doc, '$'
         columns (
         nested path '$.diagnosekoder[*]' columns (
         kode varchar2 path '$.kode'
        ,type varchar2 path '$.type'
         )
        )
        ) t;

  cursor cur_perioder(p_offset in number) is
    with jdata as (
      select melding as doc
      from dvh_fam_pp.fam_pp_meta_data
      where kafka_offset = p_offset
    )
    select t.beredskap, to_number(t.brutto_beregningsgrunnlag) as brutto_beregningsgrunnlag
          ,to_date(t.dato_fom,'yyyy-mm-dd') as dato_fom, to_date(t.dato_tom,'yyyy-mm-dd') as dato_tom
          ,t.gmt_andre_sokers_tilsyn, t.gmt_etablert_tilsyn, t.gmt_overse_etablert_tilsyn_aarsak
          ,t.gmt_tilgjengelig_for_soker, t.nattevaak, t.oppgitt_tilsyn, t.pleiebehov, t.sokers_tapte_timer
          ,t.utfall, t.uttaksgrad, t.sokers_tapte_arbeidstid
    from jdata
        ,json_table
        (
          doc, '$'
          columns (
          nested path '$.perioder[*]' columns (
          beredskap                         varchar2 path '$.beredskap'
         ,brutto_beregningsgrunnlag         varchar2 path '$.bruttoBeregningsgrunnlag'
         ,dato_fom                          varchar2 path '$.fom'
         ,dato_tom                          varchar2 path '$.tom'
         ,gmt_andre_sokers_tilsyn           varchar2 path '$.graderingMotTilsyn.andreSøkeresTilsyn'
         ,gmt_etablert_tilsyn               varchar2 path '$.graderingMotTilsyn.etablertTilsyn'
         ,gmt_overse_etablert_tilsyn_aarsak varchar2 path '$.graderingMotTilsyn.overseEtablertTilsynårsak'
         ,gmt_tilgjengelig_for_soker        varchar2 path '$.graderingMotTilsyn.tilgjengeligForSøker'
         ,nattevaak                         varchar2 path '$.nattevåk'
         ,oppgitt_tilsyn                    varchar2 path '$.oppgittTilsyn'
         ,pleiebehov                        varchar2 path '$.pleiebehov'
         ,sokers_tapte_timer                varchar2 path '$.søkersTapteTimer'
         ,utfall                            varchar2 path '$.utfall'
         ,uttaksgrad                        varchar2 path '$.uttaksgrad'
         ,sokers_tapte_arbeidstid           varchar2 path '$.søkersTapteArbeidstid'
           )
        )
        ) t;

  cursor cur_periode_inngangsvilkaar(p_offset in number) is
    with jdata as (
      select melding as doc
      from dvh_fam_pp.fam_pp_meta_data
      where kafka_offset = p_offset
    )
    select t.*
    from jdata
        ,json_table (
         doc, '$'
         columns (
         nested path '$.perioder.inngangsvilkår[*]' columns (
         utfall  varchar2 path '$.utfall'
        ,vilkaar varchar2 path '$.vilkår'
         )
        )
        ) t;

  cursor cur_periode_utbet_grader(p_offset in number) is
    with jdata as (
      select melding as doc
      from dvh_fam_pp.fam_pp_meta_data
      where kafka_offset = p_offset
    )
    select t.*
          ,null as delytelse_id_direkte
          ,null as delytelse_id_refusjon
    from jdata
        ,json_table (
         doc, '$'
         columns (
         nested path '$.perioder.utbetalingsgrader[*]' columns (
         arbeidsforhold_aktorid varchar2 path '$.arbeidsforhold.aktørId'
        ,arbeidsforhold_id      varchar2 path '$.arbeidsforhold.arbeidsforholdId'
        ,arbeidsforhold_orgnr   varchar2 path '$.arbeidsforhold.organisasjonsnummer'
        ,arbeidsforhold_type    varchar2 path '$.arbeidsforhold.type'
        ,dagsats                varchar2 path '$.dagsats'
        ,faktisk_arbeidstid     varchar2 path '$.faktiskArbeidstid'
        ,normal_arbeidstid      varchar2 path '$.normalArbeidstid'
        ,utbetalingsgrad        varchar2 path '$.utbetalingsgrad'
        ,bruker_er_mottaker     varchar2 path '$.brukerErMottaker'
         )
        )
        ) t;

  cursor cur_periode_aarsak(p_offset in number) is
    with jdata as (
      select melding as doc
      from dvh_fam_pp.fam_pp_meta_data
      where kafka_offset = p_offset
    )
    select t.*
    from jdata
        ,json_table (
         doc, '$'
         columns (
         nested path '$.perioder.årsaker[*]' columns (
         aarsak  varchar2 path '$[0]'
         )
        )
        ) t;

  v_pk_pp_fagsak number;
  v_pk_pp_perioder number;

  v_fk_person1_soker number;
  v_fk_person1_pleietrengende number;

  v_error_melding varchar2(4000);
  v_commit number := 0;
  v_feil_kilde_navn varchar2(100) := null;
begin
  for rec_fagsak in cur_fagsak(p_in_offset) loop
    begin
      savepoint do_insert;
      v_pk_pp_fagsak := -1;
      v_fk_person1_soker := -1;
      v_fk_person1_pleietrengende := -1;
      select dvh_fampp_kafka.hibernate_sequence.nextval into v_pk_pp_fagsak from dual;
      --Hent fk_person1
      v_fk_person1_soker := fam_pp_fk_person1(rec_fagsak.vedtaks_tidspunkt, rec_fagsak.soker);
      v_fk_person1_pleietrengende := fam_pp_fk_person1(rec_fagsak.vedtaks_tidspunkt, rec_fagsak.pleietrengende);
      insert into dvh_fam_pp.fam_pp_fagsak
      (
        pk_pp_fagsak, behandlings_id, fk_person1_mottaker, fk_person1_pleietrengende
       ,kafka_offset, kafka_partition, kafka_topic, lastet_dato
       ,pleietrengende, saksnummer, soker, utbetalingsreferanse
       ,ytelse_type, fk_pp_metadata, vedtaks_tidspunkt, forrige_behandlings_id
      )
      values
      (
        v_pk_pp_fagsak, rec_fagsak.behandlings_id, v_fk_person1_soker, v_fk_person1_pleietrengende
       ,rec_fagsak.kafka_offset, rec_fagsak.kafka_partition, rec_fagsak.kafka_topic, sysdate
       ,rec_fagsak.pleietrengende, rec_fagsak.saksnummer, rec_fagsak.soker, rec_fagsak.utbetalingsreferanse
       ,rec_fagsak.ytelse_type, rec_fagsak.pk_pp_meta_data, rec_fagsak.vedtaks_tidspunkt
       ,rec_fagsak.forrige_behandlings_id
      );

      --Diagnose
      for rec_diagnose in cur_diagnose(rec_fagsak.kafka_offset) loop
        begin
          insert into dvh_fam_pp.fam_pp_diagnose(kode, type, fk_pp_fagsak, lastet_dato)
          values(rec_diagnose.kode, rec_diagnose.type, v_pk_pp_fagsak, sysdate);
        exception
          when others then
            v_error_melding := substr(sqlcode||sqlerrm,1,1000);
            v_feil_kilde_navn := 'FAM_PP_DIAGNOSE';
            p_out_error := substr(v_error_melding||' '||v_feil_kilde_navn, 1, 1000);
            rollback to do_insert; continue;
            insert into fk_sensitiv.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
            values(null, v_error_melding, sysdate, v_feil_kilde_navn);
            commit;
            exit;--Ut av diagnose
        end;
      end loop;--Diagnose

      --Perioder
      for rec_perioder in cur_perioder(rec_fagsak.kafka_offset) loop
        begin
          v_pk_pp_perioder := -1;
          select dvh_fampp_kafka.hibernate_sequence.nextval into v_pk_pp_perioder from dual;
          insert into dvh_fam_pp.fam_pp_perioder
          (
            pk_pp_perioder, beredskap, brutto_beregningsgrunnlag, dato_fom, dato_tom
           ,gmt_andre_sokers_tilsyn, gmt_etablert_tilsyn, gmt_overse_etablert_tilsyn_aarsak
           ,gmt_tilgjengelig_for_soker, nattevaak, oppgitt_tilsyn, pleiebehov
           ,sokers_tapte_timer, utfall, uttaksgrad, fk_pp_fagsak, sokers_tapte_arbeidstid
           ,lastet_dato
          )
          values
          (
            v_pk_pp_perioder, rec_perioder.beredskap, rec_perioder.brutto_beregningsgrunnlag
           ,rec_perioder.dato_fom, rec_perioder.dato_tom
           ,rec_perioder.gmt_andre_sokers_tilsyn, rec_perioder.gmt_etablert_tilsyn
           ,rec_perioder.gmt_overse_etablert_tilsyn_aarsak, rec_perioder.gmt_tilgjengelig_for_soker
           ,rec_perioder.nattevaak, rec_perioder.oppgitt_tilsyn, rec_perioder.pleiebehov
          ,rec_perioder.sokers_tapte_timer, rec_perioder.utfall, rec_perioder.uttaksgrad
          ,v_pk_pp_fagsak, rec_perioder.sokers_tapte_arbeidstid
          ,sysdate
          );

          --Inngangsvilkaar
          for rec_vilkaar in cur_periode_inngangsvilkaar(rec_fagsak.kafka_offset) loop
            begin
              insert into dvh_fam_pp.fam_pp_periode_inngangsvilkaar(utfall, vilkaar, fk_pp_perioder, lastet_dato)
              values(rec_vilkaar.utfall, rec_vilkaar.vilkaar, v_pk_pp_perioder, sysdate);
            exception
              when others then
                v_error_melding := substr(sqlcode||sqlerrm,1,1000);
                v_feil_kilde_navn := 'FAM_PP_PERIODE_INNGANGSVILKAAR';
                p_out_error := substr(v_error_melding||' '||v_feil_kilde_navn, 1, 1000);
                rollback to do_insert; continue;
                insert into fk_sensitiv.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
                values(null, v_error_melding, sysdate, v_feil_kilde_navn);
                commit;
                exit;--Ut av inngangsvilk¿r
            end;
          end loop;--Inngangsvilkaar

          --Utbet_grader
          for rec_utbet in cur_periode_utbet_grader(rec_fagsak.kafka_offset) loop
            begin
              insert into dvh_fam_pp.fam_pp_periode_utbet_grader
              (
                arbeidsforhold_aktorid, arbeidsforhold_id, arbeidsforhold_orgnr
               ,arbeidsforhold_type, dagsats, delytelse_id_direkte, delytelse_id_refusjon
               ,faktisk_arbeidstid, normal_arbeidstid, utbetalingsgrad
               ,bruker_er_mottaker, fk_pp_perioder, lastet_dato
              )
              values
              (
                rec_utbet.arbeidsforhold_aktorid, rec_utbet.arbeidsforhold_id, rec_utbet.arbeidsforhold_orgnr
               ,rec_utbet.arbeidsforhold_type, rec_utbet.dagsats, rec_utbet.delytelse_id_direkte, rec_utbet.delytelse_id_refusjon
               ,rec_utbet.faktisk_arbeidstid, rec_utbet.normal_arbeidstid, rec_utbet.utbetalingsgrad
               ,rec_utbet.bruker_er_mottaker, v_pk_pp_perioder, sysdate
              );
            exception
              when others then
                v_error_melding := substr(sqlcode||sqlerrm,1,1000);
                v_feil_kilde_navn := 'FAM_PP_PERIODE_UTBET_GRADER';
                p_out_error := substr(v_error_melding||' '||v_feil_kilde_navn, 1, 1000);
                rollback to do_insert; continue;
                insert into fk_sensitiv.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
                values(null, v_error_melding, sysdate, v_feil_kilde_navn);
                commit;
                exit;--Ut av utbet_grader
            end;
          end loop;--Utbet_grader

          --Aarsak
          for rec_aarsak in cur_periode_aarsak(rec_fagsak.kafka_offset) loop
            begin
              insert into dvh_fam_pp.fam_pp_periode_aarsak(aarsak, fk_pp_perioder, lastet_dato)
              values(rec_aarsak.aarsak, v_pk_pp_perioder, sysdate);
            exception
              when others then
                v_error_melding := substr(sqlcode||sqlerrm,1,1000);
                v_feil_kilde_navn := 'FAM_PP_PERIODE_AARSAK';
                p_out_error := substr(v_error_melding||' '||v_feil_kilde_navn, 1, 1000);
                rollback to do_insert; continue;
                insert into fk_sensitiv.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
                values(null, v_error_melding, sysdate, v_feil_kilde_navn);
                commit;
                exit;--Ut av ¿rsak
            end;
          end loop; --Aarsak
        exception
          when others then
            v_error_melding := substr(sqlcode||sqlerrm,1,1000);
            v_feil_kilde_navn := 'FAM_PP_PERIODER';
            p_out_error := substr(v_error_melding||' '||v_feil_kilde_navn, 1, 1000);
            rollback to do_insert; continue;
            insert into fk_sensitiv.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
            values(null, v_error_melding, sysdate, v_feil_kilde_navn);
            commit;
            exit;--Ut av perioder
        end;
      end loop;--Perioder

      v_commit := v_commit + 1;
      if v_commit >= 10000 then
        commit;
        v_commit := 0;
      end if;
    exception
      when others then
        v_error_melding := substr(sqlcode||sqlerrm,1,1000);
        v_feil_kilde_navn := 'FAM_PP_FAGSAK';
        p_out_error := substr(v_error_melding||' '||v_feil_kilde_navn, 1, 1000);
        rollback to do_insert; continue;
        insert into fk_sensitiv.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
        values(null, v_error_melding, sysdate, v_feil_kilde_navn);
        commit;
        --G¿ til neste fagsak
    end;
  end loop;--Fagsak
  commit;
exception
  when others then
    v_error_melding := substr(sqlcode||sqlerrm,1,1000);
    insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
    values(null, null, v_error_melding, sysdate, 'FAM_PP_UTPAKKING');
    commit;
    p_out_error := substr(p_out_error || v_error_melding, 1, 1000);
end fam_pp_utpakking_offset_test;

procedure fam_pp_stonad_vedtak_insert(p_in_vedtak_periode_yyyymm in number
                                     ,p_in_max_vedtaksperiode_yyyymm in number
                                     ,p_in_forskyvninger_dag_dd in number
                                     ,p_in_gyldig_flagg in number default 0
                                     ,p_out_error out varchar2) as
  l_error_melding varchar2(1000);
  l_commit number := 0;
  l_dato_fom date;
  l_dato_tom date;

  cursor cur_pp(p_dato_fom in date, p_dato_tom in date) is
    with versjon as
    (
      select saksnummer
            ,max(pk_pp_fagsak) keep (dense_rank first order by vedtaks_tidspunkt desc) as pk_pp_fagsak
            ,count(distinct behandlings_id) as antall_behandlings_id
            ,last_day(to_date(p_in_max_vedtaksperiode_yyyymm,'yyyymm')) + p_in_forskyvninger_dag_dd as max_vedtaksdato
      from dvh_fam_pp.fam_pp_fagsak
      where trunc(vedtaks_tidspunkt) <=
                  last_day(to_date(p_in_max_vedtaksperiode_yyyymm,'yyyymm')) + p_in_forskyvninger_dag_dd
      and vedtaks_tidspunkt <= sysdate
      group by saksnummer
    ),
    tid as
    (
      select dato, pk_dim_tid
      from dt_kodeverk.dim_tid
      where dim_nivaa = 1
      and gyldig_flagg = 1
      and dato between p_dato_fom and p_dato_tom
    ),
    fagsak as
    (
      select versjon.antall_behandlings_id, versjon.max_vedtaksdato
            ,fagsak.pk_pp_fagsak, fagsak.behandlings_id, fagsak.forrige_behandlings_id
            ,fagsak.fk_person1_mottaker, fagsak.fk_person1_pleietrengende
            ,fagsak.saksnummer, fagsak.utbetalingsreferanse, fagsak.ytelse_type
            ,fagsak.vedtaks_tidspunkt as vedtaks_dato
            --,perioder.dato_fom, perioder.dato_tom
            ,perioder.dato_fom as min_dato_fom, perioder.dato_tom as max_dato_tom
            ,perioder.utbet_fom, perioder.utbet_tom
            ,perioder.brutto_beregningsgrunnlag
            ,perioder.oppgitt_tilsyn, perioder.pleiebehov, perioder.sokers_tapte_timer
            ,perioder.sokers_tapte_arbeidstid, perioder.utfall, perioder.uttaksgrad
            ,perioder.antall_dager
            , perioder.pk_pp_perioder fk_pp_perioder
            ,utbet.dagsats
            ,utbet.dagsats * perioder.antall_dager as belop
            ,sum(dagsats) over (partition by fagsak.saksnummer, perioder.dato_fom, perioder.dato_tom) as dagsats_total
            ,utbet.arbeidsforhold_aktorid, utbet.arbeidsforhold_id,utbet.arbeidsforhold_orgnr
            ,utbet.arbeidsforhold_type, utbet.normal_arbeidstid, utbet.faktisk_arbeidstid
            ,utbet.utbetalingsgrad, utbet.bruker_er_mottaker
            ,diagnose.diagnose_kode
            ,diagnose.diagnose_type
            ,diagnose.pk_pp_diagnose
            ,diagnose.antall_diagnose
            ,relasjoner.relasjon
            ,relasjoner.pk_pp_relasjoner
      from versjon
      join dvh_fam_pp.fam_pp_fagsak fagsak
      on versjon.pk_pp_fagsak = fagsak.pk_pp_fagsak

      join
      (
        select perioder.pk_pp_perioder, perioder.fk_pp_fagsak, perioder.dato_fom, perioder.dato_tom
              ,perioder.brutto_beregningsgrunnlag
              ,perioder.oppgitt_tilsyn, perioder.pleiebehov, perioder.sokers_tapte_timer
              ,perioder.sokers_tapte_arbeidstid, perioder.utfall, perioder.uttaksgrad
              ,count(distinct tid.dato) as antall_dager
              ,min(tid.dato) as utbet_fom, max(tid.dato) as utbet_tom
        from dvh_fam_pp.fam_pp_perioder perioder
        join tid
        on tid.dato between perioder.dato_fom and perioder.dato_tom

        group by perioder.pk_pp_perioder, perioder.fk_pp_fagsak, perioder.dato_fom, perioder.dato_tom
                ,perioder.brutto_beregningsgrunnlag
                ,perioder.oppgitt_tilsyn, perioder.pleiebehov, perioder.sokers_tapte_timer
                ,perioder.sokers_tapte_arbeidstid, perioder.utfall, perioder.uttaksgrad
      ) perioder
      on perioder.fk_pp_fagsak = fagsak.pk_pp_fagsak

      left join dvh_fam_pp.fam_pp_periode_utbet_grader utbet
      on utbet.fk_pp_perioder = perioder.pk_pp_perioder
      and utbet.dagsats > 0

      left join
      (
        select fam_pp_relasjoner.fk_pp_fagsak
              ,max(fam_pp_relasjoner.kode) as relasjon
              ,max(fam_pp_relasjoner.pk_pp_relasjoner) as pk_pp_relasjoner
        from dvh_fam_pp.fam_pp_relasjoner
        join tid
        on tid.dato between fam_pp_relasjoner.dato_fom and fam_pp_relasjoner.dato_tom
        group by fam_pp_relasjoner.fk_pp_fagsak
      ) relasjoner
      on relasjoner.fk_pp_fagsak = fagsak.pk_pp_fagsak

      left join
      (
        select diagnose.fk_pp_fagsak
              ,max(diagnose.kode) keep (dense_rank first order by diagnose.pk_pp_diagnose desc) as diagnose_kode
              ,max(diagnose.type) keep (dense_rank first order by diagnose.pk_pp_diagnose desc) as diagnose_type
              ,max(pk_pp_diagnose) keep (dense_rank first order by diagnose.pk_pp_diagnose desc) as pk_pp_diagnose
              ,count(distinct diagnose.kode) as antall_diagnose
        from dvh_fam_pp.fam_pp_diagnose diagnose
        group by diagnose.fk_pp_fagsak
      ) diagnose
      on diagnose.fk_pp_fagsak = fagsak.pk_pp_fagsak
    ),
    vektet as
    (
      select fagsak.*
            ,antall_dager*uttaksgrad/100 as forbrukte_dager
            ,case when dagsats_total != 0 then dagsats/dagsats_total
                  else 0
             end vekt
      from fagsak
    ),
    dim as
    (
      select vektet.*
            ,vekt*forbrukte_dager as forbrukte_dager_vektet
            ,to_char(dim_person.fodt_dato, 'yyyy') as fodsel_aar_mottaker
            ,to_char(dim_person.fodt_dato, 'mm') as fodsel_mnd_mottaker
            ,to_char(pleietrengende.fodt_dato, 'yyyy') as fodsel_aar_pleietrengende
            ,to_char(pleietrengende.fodt_dato, 'mm') as fodsel_mnd_pleietrengende
            ,dim_person.pk_dim_person, dim_person.bosted_kommune_nr
            ,dim_geografi.pk_dim_geografi, dim_geografi.bydel_kommune_nr
      from vektet

      left join dt_person.dim_person
      on vektet.fk_person1_mottaker = dim_person.fk_person1
      and vektet.vedtaks_dato between dim_person.gyldig_fra_dato and dim_person.gyldig_til_dato

      left join dt_person.dim_person pleietrengende
      on vektet.fk_person1_pleietrengende = pleietrengende.fk_person1
      and vektet.vedtaks_dato between pleietrengende.gyldig_fra_dato and pleietrengende.gyldig_til_dato

      left join dt_kodeverk.dim_geografi
      on dim_person.fk_dim_geografi_bosted = dim_geografi.pk_dim_geografi
    )
    select dim.*
    from dim
    order by saksnummer, utbet_fom, utbet_tom;
begin
  --fam_pp_diagnose_dim_oppdater(l_error_melding);
  l_dato_fom := to_date(p_in_vedtak_periode_yyyymm||'01','yyyymmdd');
  l_dato_tom := last_day(to_date(p_in_vedtak_periode_yyyymm, 'yyyymm'));
  dbms_output.put_line(l_dato_fom||'-'||l_dato_tom);--TEST!!!

  -- Slett vedtak for aktuell periode
  begin
    delete from dvh_fam_pp.fam_pp_stonad
    where kildesystem = 'PP_VEDTAK'
    and periode = p_in_vedtak_periode_yyyymm
    and gyldig_flagg = p_in_gyldig_flagg;
    commit;
  exception
    when others then
      l_error_melding := substr(sqlcode || ' ' || sqlerrm, 1, 1000);
      insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
      values(null, null, l_error_melding, sysdate, 'FAM_PP_STONAD_VEDTAK_INSERT1');
      commit;
      p_out_error := l_error_melding;
      l_error_melding := null;
  end;

  for rec_pp in cur_pp(l_dato_fom, l_dato_tom) loop
    begin
      insert into dvh_fam_pp.fam_pp_stonad
      (fk_pp_fagsak, behandlings_id, forrige_behandlings_id, fk_person1_mottaker, fk_person1_pleietrengende
      ,saksnummer, utbetalingsreferanse, ytelse_type, vedtaks_dato, antall_behandlings_id, max_vedtaksdato
      ,min_dato_fom, max_dato_tom, brutto_beregningsgrunnlag, dagsats_total, oppgitt_tilsyn
      ,pleiebehov, sokers_tapte_timer, sokers_tapte_arbeidstid, utfall, uttaksgrad
      ,utbet_fom, utbet_tom, dagsats, antall_dager, belop, arbeidsforhold_aktorid
      ,arbeidsforhold_id, arbeidsforhold_orgnr, arbeidsforhold_type, normal_arbeidstid
      ,faktisk_arbeidstid, utbetalingsgrad, bruker_er_mottaker, diagnose_kode, diagnose_type, statistikk_fom
      ,statistikk_tom, forbrukte_dager, gyldig_flagg, kildesystem, periode, lastet_dato
      ,vekt, forbrukte_dager_vektet
      ,fk_dim_person, fk_dim_geografi, bosted_kommune_nr, bydel_kommune_nr
      ,fk_pp_diagnose, antall_diagnose, relasjon, fk_pp_relasjoner
      ,fodsel_aar_mottaker, fodsel_mnd_mottaker
      ,fodsel_aar_pleietrengende, fodsel_mnd_pleietrengende, fk_pp_perioder
      )
      values
      (rec_pp.pk_pp_fagsak, rec_pp.behandlings_id, rec_pp.forrige_behandlings_id, rec_pp.fk_person1_mottaker
      ,rec_pp.fk_person1_pleietrengende, rec_pp.saksnummer, rec_pp.utbetalingsreferanse, rec_pp.ytelse_type
      ,rec_pp.vedtaks_dato, rec_pp.antall_behandlings_id, rec_pp.max_vedtaksdato
      ,rec_pp.min_dato_fom, rec_pp.max_dato_tom, rec_pp.brutto_beregningsgrunnlag, rec_pp.dagsats_total
      ,rec_pp.oppgitt_tilsyn, rec_pp.pleiebehov, rec_pp.sokers_tapte_timer, rec_pp.sokers_tapte_arbeidstid
      ,rec_pp.utfall, rec_pp.uttaksgrad, rec_pp.utbet_fom, rec_pp.utbet_tom, rec_pp.dagsats, rec_pp.antall_dager
      ,rec_pp.belop, rec_pp.arbeidsforhold_aktorid, rec_pp.arbeidsforhold_id, rec_pp.arbeidsforhold_orgnr
      ,rec_pp.arbeidsforhold_type, rec_pp.normal_arbeidstid, rec_pp.faktisk_arbeidstid, rec_pp.utbetalingsgrad
      ,rec_pp.bruker_er_mottaker, rec_pp.diagnose_kode, rec_pp.diagnose_type, l_dato_fom
      ,l_dato_tom, rec_pp.forbrukte_dager, p_in_gyldig_flagg, 'PP_VEDTAK', p_in_vedtak_periode_yyyymm, sysdate
      ,rec_pp.vekt, rec_pp.forbrukte_dager_vektet
      ,rec_pp.pk_dim_person, rec_pp.pk_dim_geografi, rec_pp.bosted_kommune_nr, rec_pp.bydel_kommune_nr
      ,rec_pp.pk_pp_diagnose, rec_pp.antall_diagnose, rec_pp.relasjon, rec_pp.pk_pp_relasjoner
      ,rec_pp.fodsel_aar_mottaker, rec_pp.fodsel_mnd_mottaker
      ,rec_pp.fodsel_aar_pleietrengende, rec_pp.fodsel_mnd_pleietrengende, rec_pp.fk_pp_perioder);
      l_commit := l_commit + 1;
    exception
      when others then
        l_error_melding := substr(sqlcode || ' ' || sqlerrm, 1, 1000);
        insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
        values(null, rec_pp.pk_pp_fagsak, l_error_melding, sysdate, 'FAM_PP_STONAD_VEDTAK_INSERT2');
        l_commit := l_commit + 1;--Gå videre til neste rekord
        p_out_error := substr(p_out_error || l_error_melding, 1, 1000);
        l_error_melding := null;
    end;
    if l_commit >= 100000 then
        commit;
        l_commit := 0;
    end if;
  end loop;
  commit;
  if l_error_melding is not null then
    insert into dvh_fam_fp.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
    values(null, l_error_melding, sysdate, 'FAM_PP_STONAD_VEDTAK_INSERT3');
    commit;
    p_out_error := substr(p_out_error || l_error_melding, 1, 1000);
  end if;
exception
  when others then
    l_error_melding := substr(sqlcode || ' ' || sqlerrm, 1, 1000);
    insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
    values(null, null, l_error_melding, sysdate, 'FAM_PP_STONAD_VEDTAK_INSERT4');
    commit;
    p_out_error := substr(p_out_error || l_error_melding, 1, 1000);
end fam_pp_stonad_vedtak_insert;

procedure fam_pp_stonad_vedtak_insert_bck(p_in_vedtak_periode_yyyymm in number
                                     ,p_in_max_vedtaksperiode_yyyymm in number
                                     ,p_in_forskyvninger_dag_dd in number
                                     ,p_in_gyldig_flagg in number default 0
                                     ,p_out_error out varchar2) as
  l_error_melding varchar2(1000);
  l_commit number := 0;
  l_dato_fom date;
  l_dato_tom date;

  cursor cur_pp(p_dato_fom in date, p_dato_tom in date) is
    with versjon as
    (
      select saksnummer
            ,max(pk_pp_fagsak) keep (dense_rank first order by vedtaks_tidspunkt desc) as pk_pp_fagsak
            ,count(distinct behandlings_id) as antall_behandlings_id
            ,last_day(to_date(p_in_max_vedtaksperiode_yyyymm,'yyyymm')) + p_in_forskyvninger_dag_dd as max_vedtaksdato
      from dvh_fam_pp.fam_pp_fagsak
      where trunc(vedtaks_tidspunkt) <=
                  last_day(to_date(p_in_max_vedtaksperiode_yyyymm,'yyyymm')) + p_in_forskyvninger_dag_dd
      and vedtaks_tidspunkt <= sysdate
      group by saksnummer
    ),
    fagsak as
    (
      select fagsak.pk_pp_fagsak, fagsak.behandlings_id, fagsak.forrige_behandlings_id
            ,fagsak.fk_person1_mottaker, fagsak.fk_person1_pleietrengende
            ,fagsak.saksnummer, fagsak.utbetalingsreferanse, fagsak.ytelse_type
            ,fagsak.vedtaks_tidspunkt as vedtaks_dato
            ,versjon.antall_behandlings_id, versjon.max_vedtaksdato
            ,min(perioder.dato_fom) as min_dato_fom, max(perioder.dato_tom) as max_dato_tom
            ,perioder.brutto_beregningsgrunnlag
            ,perioder.oppgitt_tilsyn, perioder.pleiebehov, perioder.sokers_tapte_timer
            ,perioder.sokers_tapte_arbeidstid, perioder.utfall, perioder.uttaksgrad
            ,min(dim_tid.dato) as utbet_fom, max(dim_tid.dato) as utbet_tom
            ,utbet.dagsats , count(distinct dim_tid.dato) as antall_dager
            ,utbet.dagsats * count(distinct dim_tid.dato) as belop
            ,utbet.arbeidsforhold_aktorid, utbet.arbeidsforhold_id,utbet.arbeidsforhold_orgnr
            ,utbet.arbeidsforhold_type, utbet.normal_arbeidstid, utbet.faktisk_arbeidstid
            ,utbet.utbetalingsgrad, utbet.bruker_er_mottaker
            ,max(diagnose.kode) keep (dense_rank first order by diagnose.pk_pp_diagnose desc) as diagnose_kode
            ,max(diagnose.type) keep (dense_rank first order by diagnose.pk_pp_diagnose desc) as diagnose_type
            ,max(pk_pp_diagnose) as pk_pp_diagnose
            ,count(distinct diagnose.kode) as antall_diagnose
      from versjon

      join dvh_fam_pp.fam_pp_fagsak fagsak
      on versjon.pk_pp_fagsak = fagsak.pk_pp_fagsak

      join dvh_fam_pp.fam_pp_perioder perioder
      on perioder.fk_pp_fagsak = fagsak.pk_pp_fagsak

      join dt_kodeverk.dim_tid
      on dim_tid.dim_nivaa = 1
      and dim_tid.gyldig_flagg = 1
      and dim_tid.dato between perioder.dato_fom and perioder.dato_tom
      and dim_tid.dato between p_dato_fom and p_dato_tom

      left join dvh_fam_pp.fam_pp_diagnose diagnose
      on diagnose.fk_pp_fagsak = fagsak.pk_pp_fagsak
      /*
      left join dvh_fam_pp.fam_pp_periode_inngangsvilkaar inngangsvilkaar
      on inngangsvilkaar.fk_pp_perioder = perioder.pk_pp_perioder

      left join dvh_fam_pp.fam_pp_periode_aarsak aarsak
      on aarsak.fk_pp_perioder = perioder.pk_pp_perioder*/

      left join dvh_fam_pp.fam_pp_periode_utbet_grader utbet
      on utbet.fk_pp_perioder = perioder.pk_pp_perioder

      group by fagsak.pk_pp_fagsak, fagsak.behandlings_id, fagsak.forrige_behandlings_id
              ,fagsak.fk_person1_mottaker, fagsak.fk_person1_pleietrengende
              ,fagsak.saksnummer, fagsak.utbetalingsreferanse, fagsak.ytelse_type
              ,fagsak.vedtaks_tidspunkt
              ,versjon.antall_behandlings_id, versjon.max_vedtaksdato
              ,perioder.brutto_beregningsgrunnlag, perioder.oppgitt_tilsyn, perioder.pleiebehov, perioder.sokers_tapte_timer
              ,perioder.sokers_tapte_arbeidstid, perioder.utfall, perioder.uttaksgrad
              ,utbet.dagsats, utbet.arbeidsforhold_aktorid, utbet.arbeidsforhold_id,utbet.arbeidsforhold_orgnr
              ,utbet.arbeidsforhold_type, utbet.normal_arbeidstid, utbet.faktisk_arbeidstid
              ,utbet.utbetalingsgrad, utbet.bruker_er_mottaker
    ),
    vektet_1 as
    (
      select fagsak.*
            ,antall_dager*uttaksgrad/100 as forbrukte_dager
            ,sum(dagsats) over (partition by saksnummer,utbet_fom,utbet_tom) as dagsats_total
      from fagsak
    ),
    vektet as
    (
      select vektet_1.*
            ,case when dagsats_total != 0 then dagsats/dagsats_total
                  else 0
             end vekt
      from vektet_1
    ),
    dim as
    (
      select vektet.*
            ,vekt*forbrukte_dager as forbrukte_dager_vektet
            ,dim_person.pk_dim_person, dim_person.bosted_kommune_nr
            ,dim_geografi.pk_dim_geografi, dim_geografi.bydel_kommune_nr
      from vektet

      left join dt_person.dim_person
      on vektet.fk_person1_mottaker = dim_person.fk_person1
      and vektet.vedtaks_dato between dim_person.gyldig_fra_dato and dim_person.gyldig_til_dato

      left join dt_kodeverk.dim_geografi
      on dim_person.fk_dim_geografi_bosted = dim_geografi.pk_dim_geografi
    )
    select dim.*
    from dim
    order by saksnummer, utbet_fom, utbet_tom;
begin
  l_dato_fom := to_date(p_in_vedtak_periode_yyyymm||'01','yyyymmdd');
  l_dato_tom := last_day(to_date(p_in_vedtak_periode_yyyymm, 'yyyymm'));
  dbms_output.put_line(l_dato_fom||'-'||l_dato_tom);--TEST!!!

  -- Slett vedtak for aktuell periode
  begin
    delete from dvh_fam_pp.fam_pp_stonad
    where kildesystem = 'PP_VEDTAK'
    and periode = p_in_vedtak_periode_yyyymm
    and gyldig_flagg = p_in_gyldig_flagg;
    commit;
  exception
    when others then
      l_error_melding := substr(sqlcode || ' ' || sqlerrm, 1, 1000);
      insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
      values(null, null, l_error_melding, sysdate, 'FAM_PP_STONAD_VEDTAK_INSERT1');
      commit;
      p_out_error := l_error_melding;
      l_error_melding := null;
  end;

  for rec_pp in cur_pp(l_dato_fom, l_dato_tom) loop
    begin
      insert into dvh_fam_pp.fam_pp_stonad
      (fk_pp_fagsak, behandlings_id, forrige_behandlings_id, fk_person1_mottaker, fk_person1_pleietrengende
      ,saksnummer, utbetalingsreferanse, ytelse_type, vedtaks_dato, antall_behandlings_id, max_vedtaksdato
      ,min_dato_fom, max_dato_tom, brutto_beregningsgrunnlag, dagsats_total, oppgitt_tilsyn
      ,pleiebehov, sokers_tapte_timer, sokers_tapte_arbeidstid, utfall, uttaksgrad
      ,utbet_fom, utbet_tom, dagsats, antall_dager, belop, arbeidsforhold_aktorid
      ,arbeidsforhold_id, arbeidsforhold_orgnr, arbeidsforhold_type, normal_arbeidstid
      ,faktisk_arbeidstid, utbetalingsgrad, bruker_er_mottaker, diagnose_kode, diagnose_type, statistikk_fom
      ,statistikk_tom, forbrukte_dager, gyldig_flagg, kildesystem, periode, lastet_dato
      ,vekt, forbrukte_dager_vektet
      ,fk_dim_person, fk_dim_geografi, bosted_kommune_nr, bydel_kommune_nr
      ,fk_pp_diagnose, antall_diagnose
      )
      values
      (rec_pp.pk_pp_fagsak, rec_pp.behandlings_id, rec_pp.forrige_behandlings_id, rec_pp.fk_person1_mottaker
      ,rec_pp.fk_person1_pleietrengende, rec_pp.saksnummer, rec_pp.utbetalingsreferanse, rec_pp.ytelse_type
      ,rec_pp.vedtaks_dato, rec_pp.antall_behandlings_id, rec_pp.max_vedtaksdato
      ,rec_pp.min_dato_fom, rec_pp.max_dato_tom, rec_pp.brutto_beregningsgrunnlag, rec_pp.dagsats_total
      ,rec_pp.oppgitt_tilsyn, rec_pp.pleiebehov, rec_pp.sokers_tapte_timer, rec_pp.sokers_tapte_arbeidstid
      ,rec_pp.utfall, rec_pp.uttaksgrad, rec_pp.utbet_fom, rec_pp.utbet_tom, rec_pp.dagsats, rec_pp.antall_dager
      ,rec_pp.belop, rec_pp.arbeidsforhold_aktorid, rec_pp.arbeidsforhold_id, rec_pp.arbeidsforhold_orgnr
      ,rec_pp.arbeidsforhold_type, rec_pp.normal_arbeidstid, rec_pp.faktisk_arbeidstid, rec_pp.utbetalingsgrad
      ,rec_pp.bruker_er_mottaker, rec_pp.diagnose_kode, rec_pp.diagnose_type, l_dato_fom
      ,l_dato_tom, rec_pp.forbrukte_dager, p_in_gyldig_flagg, 'PP_VEDTAK', p_in_vedtak_periode_yyyymm, sysdate
      ,rec_pp.vekt, rec_pp.forbrukte_dager_vektet
      ,rec_pp.pk_dim_person, rec_pp.pk_dim_geografi, rec_pp.bosted_kommune_nr, rec_pp.bydel_kommune_nr
      ,rec_pp.pk_pp_diagnose, rec_pp.antall_diagnose);
      l_commit := l_commit + 1;
    exception
      when others then
        l_error_melding := substr(sqlcode || ' ' || sqlerrm, 1, 1000);
        insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
        values(null, rec_pp.pk_pp_fagsak, l_error_melding, sysdate, 'FAM_PP_STONAD_VEDTAK_INSERT2');
        l_commit := l_commit + 1;--Gå videre til neste rekord
        p_out_error := substr(p_out_error || l_error_melding, 1, 1000);
        l_error_melding := null;
    end;
    if l_commit >= 100000 then
        commit;
        l_commit := 0;
    end if;
  end loop;
  commit;
  if l_error_melding is not null then
    insert into dvh_fam_fp.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
    values(null, l_error_melding, sysdate, 'FAM_PP_STONAD_VEDTAK_INSERT3');
    commit;
    p_out_error := substr(p_out_error || l_error_melding, 1, 1000);
  end if;
exception
  when others then
    l_error_melding := substr(sqlcode || ' ' || sqlerrm, 1, 1000);
    insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
    values(null, null, l_error_melding, sysdate, 'FAM_PP_STONAD_VEDTAK_INSERT4');
    commit;
    p_out_error := substr(p_out_error || l_error_melding, 1, 1000);
end fam_pp_stonad_vedtak_insert_bck;

procedure fam_pp_stonad_vedtak_ur_insert(p_in_vedtak_periode_yyyymm in number
                                        ,p_in_gyldig_flagg in number default 0
                                        ,p_out_error out varchar2) as
  l_error_melding varchar2(1000);
  l_commit number := 0;
  l_dato_fom date;
  l_dato_tom date;

  cursor cur_pp(p_dato_fom in date, p_dato_tom in date) is
    with ur1 as
    (
      select ur_sign.*
            --,sum(dagsats) over (partition by henvisning, dato_utbet_fom, dato_utbet_tom, ur_sign) as dagsats_total
      from
      (select pk_ur_utbetaling, delytelse_id, henvisning, posteringsdato, hovedkontonr
             ,underkontonr, underkonto_navn, gjelder_mottaker
             ,fk_person1, status, dato_utbet_fom, dato_utbet_tom
             ,belop_sign, belop, sats as dagsats, antall, type_mottaker
             ,fagsystem_id, substr(mottaker_fs,3,9) as mottaker_fs, valuteringsdato, mottaker_utbetaling
             ,(case when belop_sign >= 0 then 1 else -1 end) as ur_sign
       from dvh_fam_pp.fam_pp_ur_utbetaling
       where posteringsdato between p_dato_fom and p_dato_tom
       and length(henvisning) > 15--Kun vedtak
      ) ur_sign
    ),
    ur2 as
    (
      select ur1.*
      from ur1
      join
      (select fagsystem_id, delytelse_id
       from ur1
       group by fagsystem_id, delytelse_id
       having sum(belop_sign) != 0
      ) ur1_1
      on ur1.fagsystem_id = ur1_1.fagsystem_id
      and ur1.delytelse_id = ur1_1.delytelse_id
    ),
    ur as
    (
      select ur2.*
      from ur2
      join
      (select fagsystem_id, dato_utbet_fom, dato_utbet_tom
       from ur2
       group by fagsystem_id, dato_utbet_fom, dato_utbet_tom
       having sum(belop_sign) != 0
      ) ur2_1
      on ur2.fagsystem_id = ur2_1.fagsystem_id
      and ur2.dato_utbet_fom = ur2_1.dato_utbet_fom
      and ur2.dato_utbet_tom = ur2_1.dato_utbet_tom
    ),
    fagsak_ur1 as
    (
      select ur.pk_ur_utbetaling, ur.delytelse_id, ur.henvisning, ur.posteringsdato, ur.hovedkontonr
            ,ur.underkontonr, ur.underkonto_navn, ur.gjelder_mottaker
            ,ur.fk_person1, ur.status, ur.dato_utbet_fom, ur.dato_utbet_tom
            ,ur.belop_sign, ur.belop, ur.dagsats, ur.antall, ur.type_mottaker
            ,ur.fagsystem_id, ur.mottaker_fs, ur.valuteringsdato, ur.mottaker_utbetaling
            --,ur.dagsats_total
            ,ur.ur_sign
            ,fagsak.pk_pp_fagsak, fagsak.behandlings_id, fagsak.forrige_behandlings_id
            ,fagsak.fk_person1_mottaker, fagsak.fk_person1_pleietrengende
            ,fagsak.saksnummer, fagsak.utbetalingsreferanse, fagsak.ytelse_type
            ,fagsak.vedtaks_tidspunkt as vedtaks_dato
            --,dim_tid.dato
            ,max(perioder.pk_pp_perioder) as pk_pp_perioder, max(perioder.dato_fom) as dato_fom
            ,max(perioder.dato_tom) as dato_tom
            ,max(perioder.brutto_beregningsgrunnlag) as brutto_beregningsgrunnlag
            ,max(perioder.oppgitt_tilsyn) as oppgitt_tilsyn
            ,max(perioder.pleiebehov) as pleiebehov, max(perioder.sokers_tapte_timer) as sokers_tapte_timer
            ,max(perioder.sokers_tapte_arbeidstid) as sokers_tapte_arbeidstid
            ,max(perioder.utfall) as utfall, max(perioder.uttaksgrad) as uttaksgrad
            ,count(distinct dim_tid.dato) as antall_dager
            ,max(relasjoner.kode) as relasjon
            ,max(pk_pp_relasjoner) as pk_pp_relasjoner
            ,max(utbet.dagsats_total) as dagsats_total_utbet
      from ur
      left join dvh_fam_pp.fam_pp_fagsak fagsak
      on ur.henvisning = fagsak.utbetalingsreferanse

      join dt_kodeverk.dim_tid
      on dim_tid.dato between ur.dato_utbet_fom and ur.dato_utbet_tom
      and dim_tid.dag_i_uke<6

      left join dvh_fam_pp.fam_pp_perioder perioder
      on perioder.fk_pp_fagsak = fagsak.pk_pp_fagsak
      and dim_tid.dato between perioder.dato_fom and perioder.dato_tom
      and dim_tid.dag_i_uke<6

      left join dvh_fam_pp.fam_pp_relasjoner relasjoner
      on relasjoner.fk_pp_fagsak = fagsak.pk_pp_fagsak
      and dim_tid.dato between relasjoner.dato_fom and relasjoner.dato_tom

      left join
      (
        select fk_pp_perioder, sum(dagsats) as dagsats_total
        from dvh_fam_pp.fam_pp_periode_utbet_grader
        where dagsats > 0
        group by fk_pp_perioder
      ) utbet
      on utbet.fk_pp_perioder = perioder.pk_pp_perioder

      group by ur.pk_ur_utbetaling, ur.delytelse_id, ur.henvisning, ur.posteringsdato, ur.hovedkontonr
            ,ur.underkontonr, ur.underkonto_navn, ur.gjelder_mottaker
            ,ur.fk_person1, ur.status, ur.dato_utbet_fom, ur.dato_utbet_tom
            ,ur.belop_sign, ur.belop, ur.dagsats, ur.antall, ur.type_mottaker
            ,ur.fagsystem_id, ur.mottaker_fs, ur.valuteringsdato, ur.mottaker_utbetaling
            --,ur.dagsats_total
            ,ur.ur_sign
            ,fagsak.pk_pp_fagsak, fagsak.behandlings_id, fagsak.forrige_behandlings_id
            ,fagsak.fk_person1_mottaker, fagsak.fk_person1_pleietrengende
            ,fagsak.saksnummer, fagsak.utbetalingsreferanse, fagsak.ytelse_type
            ,fagsak.vedtaks_tidspunkt
    ),
    fagsak_ur2 as
    (
      select fagsak_ur.pk_ur_utbetaling
            --,utbet.dagsats_utbet
            ,utbet.arbeidsforhold_aktorid, utbet.arbeidsforhold_id
            ,utbet.arbeidsforhold_type, utbet.normal_arbeidstid, utbet.faktisk_arbeidstid
            ,utbet.utbetalingsgrad, utbet.bruker_er_mottaker
            --,utbet.dagsats_total_utbet
            ,utbet.orgenhet
      from fagsak_ur1 fagsak_ur
      left join
      (
        select fk_pp_perioder, dagsats
              ,max(arbeidsforhold_aktorid) as arbeidsforhold_aktorid
              ,max(arbeidsforhold_id) as arbeidsforhold_id
              ,max(arbeidsforhold_type) as arbeidsforhold_type
              ,max(normal_arbeidstid) as normal_arbeidstid, max(faktisk_arbeidstid) as faktisk_arbeidstid
              ,max(utbetalingsgrad) as utbetalingsgrad, max(bruker_er_mottaker) as bruker_er_mottaker
              --,sum(dagsats) over (partition by fk_pp_perioder) as dagsats_total_utbet
              ,max(dim_ia_orgenhet.orgnrkn) as orgenhet
        from dvh_fam_pp.fam_pp_periode_utbet_grader utbet
        join dt_p.dim_ia_orgenhet
        on utbet.arbeidsforhold_orgnr = dim_ia_orgenhet.orgnr
        where bruker_er_mottaker = '0'
        and dagsats > 0
        group by utbet.fk_pp_perioder, utbet.dagsats
      ) utbet
      on utbet.fk_pp_perioder = fagsak_ur.pk_pp_perioder
      and fagsak_ur.mottaker_fs = utbet.orgenhet
      and utbet.dagsats = fagsak_ur.dagsats
      where fagsak_ur.type_mottaker = 'ORG'

      union all
      select fagsak_ur.pk_ur_utbetaling
            --,utbet.dagsats_utbet
            ,utbet.arbeidsforhold_aktorid, utbet.arbeidsforhold_id
            ,utbet.arbeidsforhold_type, utbet.normal_arbeidstid, utbet.faktisk_arbeidstid
            ,utbet.utbetalingsgrad, utbet.bruker_er_mottaker
            --,utbet.dagsats_total_utbet
            ,null as orgenhet
      from fagsak_ur1 fagsak_ur
      left join
      (
        select fk_pp_perioder--, sum(dagsats) as dagsats_utbet
              ,max(arbeidsforhold_aktorid) as arbeidsforhold_aktorid, max(arbeidsforhold_id) as arbeidsforhold_id
              ,max(arbeidsforhold_orgnr) as arbeidsforhold_orgnr, max(arbeidsforhold_type) as arbeidsforhold_type
              ,max(normal_arbeidstid) as normal_arbeidstid, max(faktisk_arbeidstid) as faktisk_arbeidstid
              ,max(utbetalingsgrad) as utbetalingsgrad, max(bruker_er_mottaker) as bruker_er_mottaker
              --,sum(dagsats) as dagsats_total_utbet
        from dvh_fam_pp.fam_pp_periode_utbet_grader
        where bruker_er_mottaker = '1'
        and dagsats > 0
        group by fk_pp_perioder
      ) utbet
      on utbet.fk_pp_perioder = fagsak_ur.pk_pp_perioder
      where fagsak_ur.type_mottaker = 'FNR'
    ),
    fagsak_ur as
    (
      select fagsak_ur.*
            --,fagsak_ur2.dagsats
            ,fagsak_ur2.arbeidsforhold_aktorid, fagsak_ur2.arbeidsforhold_id
            ,fagsak_ur2.arbeidsforhold_type, fagsak_ur2.normal_arbeidstid, fagsak_ur2.faktisk_arbeidstid
            ,fagsak_ur2.utbetalingsgrad, fagsak_ur2.bruker_er_mottaker--, fagsak_ur2.dagsats_total
            ,diagnose.diagnose_kode
            ,diagnose.diagnose_type
            ,diagnose.pk_pp_diagnose
            ,diagnose.antall_diagnose, diagnose.fk_dim_diagnose
      from fagsak_ur1 fagsak_ur
      join fagsak_ur2
      on fagsak_ur.pk_ur_utbetaling = fagsak_ur2.pk_ur_utbetaling

      left join
      (
        select diagnose.fk_pp_fagsak
              ,max(diagnose.kode) keep (dense_rank first order by diagnose.pk_pp_diagnose desc) as diagnose_kode
              ,max(diagnose.type) keep (dense_rank first order by diagnose.pk_pp_diagnose desc) as diagnose_type
              ,max(pk_pp_diagnose) keep (dense_rank first order by diagnose.pk_pp_diagnose desc) as pk_pp_diagnose
              ,max(fk_dim_diagnose) keep (dense_rank first order by diagnose.pk_pp_diagnose desc) as fk_dim_diagnose
              ,count(distinct diagnose.kode) as antall_diagnose
        from dvh_fam_pp.fam_pp_diagnose diagnose
        group by diagnose.fk_pp_fagsak
      ) diagnose
      on diagnose.fk_pp_fagsak = fagsak_ur.pk_pp_fagsak
    ),
    vektet as
    (
      select fagsak_ur.*
            ,antall_dager*uttaksgrad/100*ur_sign as forbrukte_dager
            /*,case when dagsats_total != 0 then dagsats/dagsats_total
                  else 0
             end vekt*/
            ,case when dagsats_total_utbet != 0 then dagsats/dagsats_total_utbet
                  else 0
             end vekt
      from fagsak_ur
    ),
    dim as
    (
      select vektet.*
            ,vekt*forbrukte_dager as forbrukte_dager_vektet
            ,dim_person.pk_dim_person, dim_person.bosted_kommune_nr
            ,to_char(dim_person.fodt_dato, 'yyyy') as fodsel_aar_mottaker
            ,to_char(dim_person.fodt_dato, 'mm') as fodsel_mnd_mottaker
            ,to_char(pleietrengende.fodt_dato, 'yyyy') as fodsel_aar_pleietrengende
            ,to_char(pleietrengende.fodt_dato, 'mm') as fodsel_mnd_pleietrengende
            ,dim_geografi.pk_dim_geografi, dim_geografi.bydel_kommune_nr
      from vektet

      left join dt_person.dim_person
      on vektet.fk_person1_mottaker = dim_person.fk_person1
      and vektet.posteringsdato between dim_person.gyldig_fra_dato and dim_person.gyldig_til_dato

      left join dt_kodeverk.dim_geografi
      on dim_person.fk_dim_geografi_bosted = dim_geografi.pk_dim_geografi

      left join dt_person.dim_person pleietrengende
      on vektet.fk_person1_pleietrengende = pleietrengende.fk_person1
      and vektet.posteringsdato between pleietrengende.gyldig_fra_dato and pleietrengende.gyldig_til_dato
    )
    select *
    from dim
    --where fk_person1 = 1821426387
    order by saksnummer, dato_utbet_fom, dato_utbet_tom;
begin
  --fam_pp_diagnose_dim_oppdater(l_error_melding);
  l_dato_fom := to_date(p_in_vedtak_periode_yyyymm||'01','yyyymmdd');
  l_dato_tom := last_day(to_date(p_in_vedtak_periode_yyyymm, 'yyyymm'));
  dbms_output.put_line(l_dato_fom||'-'||l_dato_tom);--TEST!!!

  -- Slett vedtak for aktuell periode
  begin
    delete from dvh_fam_pp.fam_pp_stonad
    where kildesystem = 'PP_VEDTAK_UR'
    and periode = p_in_vedtak_periode_yyyymm
    and gyldig_flagg = p_in_gyldig_flagg;
    commit;
  exception
    when others then
      l_error_melding := substr(sqlcode || ' ' || sqlerrm, 1, 1000);
      insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
      values(null, null, l_error_melding, sysdate, 'FAM_PP_STONAD_VEDTAK_UR_INSERT1');
      commit;
      p_out_error := l_error_melding;
      l_error_melding := null;
  end;

  for rec_pp in cur_pp(l_dato_fom, l_dato_tom) loop
    begin
      insert into dvh_fam_pp.fam_pp_stonad
      (fk_pp_fagsak, behandlings_id, forrige_behandlings_id, fk_person1_mottaker, fk_person1_pleietrengende
      ,saksnummer, utbetalingsreferanse, ytelse_type, vedtaks_dato, antall_behandlings_id, max_vedtaksdato
      ,min_dato_fom, max_dato_tom, brutto_beregningsgrunnlag, dagsats_total, oppgitt_tilsyn
      ,pleiebehov, sokers_tapte_timer, sokers_tapte_arbeidstid, utfall, uttaksgrad
      ,utbet_fom, utbet_tom, dagsats, antall_dager, belop, arbeidsforhold_aktorid
      ,arbeidsforhold_id, arbeidsforhold_orgnr, arbeidsforhold_type, normal_arbeidstid
      ,faktisk_arbeidstid, utbetalingsgrad, bruker_er_mottaker, diagnose_kode, diagnose_type, statistikk_fom
      ,statistikk_tom, forbrukte_dager, gyldig_flagg, kildesystem, periode, lastet_dato
      ,vekt, forbrukte_dager_vektet
      ,fk_dim_person, fk_dim_geografi, bosted_kommune_nr, bydel_kommune_nr
      ,fk_pp_diagnose, antall_diagnose
      ,relasjon, fk_ur_utbetaling, fk_pp_perioder, fk_pp_relasjoner
      ,posteringsdato_ur, fk_dim_diagnose
      ,fodsel_aar_mottaker, fodsel_mnd_mottaker
      ,fodsel_aar_pleietrengende, fodsel_mnd_pleietrengende
      )
      values
      (rec_pp.pk_pp_fagsak, rec_pp.behandlings_id, rec_pp.forrige_behandlings_id, rec_pp.gjelder_mottaker
      ,rec_pp.fk_person1_pleietrengende, rec_pp.saksnummer, rec_pp.henvisning, rec_pp.ytelse_type
      ,rec_pp.vedtaks_dato, null, null
      ,rec_pp.dato_utbet_fom, rec_pp.dato_utbet_tom, rec_pp.brutto_beregningsgrunnlag, rec_pp.dagsats_total_utbet
      ,rec_pp.oppgitt_tilsyn, rec_pp.pleiebehov, rec_pp.sokers_tapte_timer, rec_pp.sokers_tapte_arbeidstid
      ,rec_pp.utfall, rec_pp.uttaksgrad, rec_pp.dato_utbet_fom, rec_pp.dato_utbet_tom, rec_pp.dagsats, rec_pp.antall_dager
      ,rec_pp.belop, rec_pp.arbeidsforhold_aktorid, rec_pp.arbeidsforhold_id, rec_pp.mottaker_fs
      ,rec_pp.arbeidsforhold_type, rec_pp.normal_arbeidstid, rec_pp.faktisk_arbeidstid, rec_pp.utbetalingsgrad
      ,rec_pp.bruker_er_mottaker, rec_pp.diagnose_kode, rec_pp.diagnose_type, l_dato_fom
      ,l_dato_tom, rec_pp.forbrukte_dager, p_in_gyldig_flagg, 'PP_VEDTAK_UR', p_in_vedtak_periode_yyyymm, sysdate
      ,rec_pp.vekt, rec_pp.forbrukte_dager_vektet
      ,rec_pp.pk_dim_person, rec_pp.pk_dim_geografi, rec_pp.bosted_kommune_nr, rec_pp.bydel_kommune_nr
      ,rec_pp.pk_pp_diagnose, rec_pp.antall_diagnose
      ,rec_pp.relasjon, rec_pp.pk_ur_utbetaling, rec_pp.pk_pp_perioder, rec_pp.pk_pp_relasjoner
      ,rec_pp.posteringsdato, rec_pp.fk_dim_diagnose
      ,rec_pp.fodsel_aar_mottaker, rec_pp.fodsel_mnd_mottaker
      ,rec_pp.fodsel_aar_pleietrengende, rec_pp.fodsel_mnd_pleietrengende);
      l_commit := l_commit + 1;
    exception
      when others then
        l_error_melding := substr(sqlcode || ' ' || sqlerrm, 1, 1000);
        insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
        values(null, rec_pp.pk_pp_fagsak, l_error_melding, sysdate, 'FAM_PP_STONAD_VEDTAK_UR_INSERT2');
        l_commit := l_commit + 1;--Gå videre til neste rekord
        p_out_error := substr(p_out_error || l_error_melding, 1, 1000);
        l_error_melding := null;
    end;
    if l_commit >= 100000 then
        commit;
        l_commit := 0;
    end if;
  end loop;
  commit;
  if l_error_melding is not null then
    insert into dvh_fam_fp.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
    values(null, l_error_melding, sysdate, 'FAM_PP_STONAD_VEDTAK_UR_INSERT3');
    commit;
    p_out_error := substr(p_out_error || l_error_melding, 1, 1000);
  end if;
exception
  when others then
    l_error_melding := substr(sqlcode || ' ' || sqlerrm, 1, 1000);
    insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
    values(null, null, l_error_melding, sysdate, 'FAM_PP_STONAD_VEDTAK_UR_INSERT4');
    commit;
    p_out_error := substr(p_out_error || l_error_melding, 1, 1000);
end fam_pp_stonad_vedtak_ur_insert;

procedure fam_pp_diagnose_dim_oppdater(p_out_error out varchar2) as
  v_error_melding varchar2(4000);

begin
  merge into dvh_fam_pp.fam_pp_diagnose
  using
  (
    select pk_pp_diagnose, max(pk_dim_diagnose) as pk_dim_diagnose
    from
    (
      select fam_pp_diagnose.pk_pp_diagnose
            ,dim_diagnose.pk_dim_diagnose, dim_diagnose.gyldig_fra_dato, dim_diagnose.gyldig_til_dato
            ,fam_pp_fagsak.vedtaks_tidspunkt
      from dvh_fam_pp.fam_pp_diagnose
      join dvh_fam_pp.fam_pp_fagsak
      on fam_pp_diagnose.fk_pp_fagsak = fam_pp_fagsak.pk_pp_fagsak
      join dt_p.dim_diagnose
      on fam_pp_diagnose.kode = dim_diagnose.diagnose_kode
      and fam_pp_diagnose.type = upper(dim_diagnose.diagnose_tabell)
      and fam_pp_fagsak.vedtaks_tidspunkt between dim_diagnose.gyldig_fra_dato and dim_diagnose.gyldig_til_dato
    )
    group by pk_pp_diagnose
  ) dim
  on (fam_pp_diagnose.pk_pp_diagnose = dim.pk_pp_diagnose)
  when matched then
    update set fam_pp_diagnose.fk_dim_diagnose = dim.pk_dim_diagnose
    where fam_pp_diagnose.fk_dim_diagnose is null;

  commit;
exception
  when others then
    v_error_melding := substr(sqlcode || ' ' || sqlerrm, 1, 1000);
    p_out_error := v_error_melding;
    insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
    values(null, null, v_error_melding, sysdate, 'FAM_PP_DIAGNOSE_DIM_OPPDATER');
    commit;
end fam_pp_diagnose_dim_oppdater;

procedure fam_pp_stonad_siste_diagnose_patching(p_in_vedtak_periode_yyyymm in number
                                     ,p_in_kildesystem in varchar2
                                     ,p_in_gyldig_flagg in number default 0
                                     ,p_out_error out varchar2) as
  l_error_melding varchar2(1000);
Begin
    merge into FAM_PP_STONAD stonad
    using (
      SELECT
  saksnummer,
  behandlings_id,
  vedtaks_tidspunkt,
  max(fk_dim_diagnose) SISTE_FK_DIM_DIAGNOSE,
  kode siste_diagnose_kode,
  type siste_diagnose_type
FROM
  (
    SELECT
      saksnummer,
      behandlings_id,
      vedtaks_tidspunkt,
      LAST_VALUE(fk_dim_diagnose IGNORE NULLS) OVER ( ORDER BY saksnummer, vedtaks_tidspunkt) fk_dim_diagnose,
      type,
      LAST_VALUE(kode IGNORE NULLS) OVER ( ORDER BY saksnummer, vedtaks_tidspunkt) kode
    FROM
      (
        SELECT
          *
        FROM
          (
            SELECT
              saksnummer,
              vedtaks_tidspunkt,
              NULL kode,
              behandlings_id,
              NULL fk_dim_diagnose,
              type,
              ROW_NUMBER() OVER ( PARTITION BY saksnummer, vedtaks_tidspunkt ORDER BY vedtaks_tidspunkt DESC) sorted_id
            FROM
              (
                SELECT
                  s.*
                FROM
                  slett_1 s
                WHERE
                  s.kode IN (
                    SELECT
                      b.kode
                    FROM
                      slett_1 b
                    WHERE
                      s.saksnummer = b.saksnummer AND s.vedtaks_tidspunkt > b.vedtaks_tidspunkt
                  )
              ) A
          ) WHERE sorted_id = 1
        UNION
        SELECT
          *
        FROM
          (
            SELECT
              saksnummer,
              vedtaks_tidspunkt,
              kode,
              behandlings_id,
              fk_dim_diagnose,
              type,
              ROW_NUMBER() OVER ( PARTITION BY saksnummer, vedtaks_tidspunkt ORDER BY vedtaks_tidspunkt DESC) sorted_id
            FROM
              (
                SELECT
                  s.*
                FROM
                  slett_1 s
                WHERE
                  s.kode NOT IN (
                    SELECT
                      b.kode
                    FROM
                      slett_1 b
                    WHERE
                      s.saksnummer = b.saksnummer AND s.vedtaks_tidspunkt > b.vedtaks_tidspunkt
                  )
              ) A
          ) WHERE sorted_id = 1
      )
  )
GROUP BY saksnummer, behandlings_id, vedtaks_tidspunkt, kode, fk_dim_diagnose, type
    ) siste
    on (stonad.behandlings_id = siste.behandlings_id)
    when matched then
        update set stonad.siste_diagnose_kode = siste.siste_diagnose_kode,
                   stonad.siste_diagnose_type = siste.siste_diagnose_type,
                   stonad.siste_fk_dim_diagnose = siste.siste_fk_dim_diagnose
                WHERE
                stonad.siste_diagnose_kode != siste.siste_diagnose_kode
                OR stonad.siste_diagnose_type != siste.siste_diagnose_type AND
                stonad.gyldig_flagg = p_in_gyldig_flagg
                AND stonad.kildesystem = p_in_kildesystem
                AND stonad.periode = p_in_vedtak_periode_yyyymm;
    COMMIT;
    EXCEPTION
    WHEN OTHERS THEN
      l_error_melding := substr(sqlcode || ' ' || sqlerrm, 1, 1000);
      insert into dvh_fam_fp.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
      values(null, null, l_error_melding, sysdate, 'FAM_PP_STONAD_siste_diagnose');
      commit;
      p_out_error := l_error_melding;
      l_error_melding := null;
    END;

END FAM_PP;
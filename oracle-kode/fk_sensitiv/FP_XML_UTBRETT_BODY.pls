create or replace PACKAGE BODY                                                                                                                                                                                                                                                                                                                                                                         FP_XML_UTBRETT AS

  --****************************************************************************************************
  -- NAME:     SLETT_KODE67_FAGSAK
  -- PURPOSE:  Slett hele fagsak som har søker av kode67.
  --
  -- REVISION:
  -- Version  Date           Author                  Description
  -- 0.1      31.03.2020     Helen Rong              Slett hele fagsak som har søker av kode67. Sletting
  --                                                 kjøres på alle tabellene for blant annet FP,
  --                                                 ES og SVP.
  --****************************************************************************************************
  procedure SLETT_KODE67_FAGSAK(dummy in varchar2, p_error_melding out varchar2) as
    cursor cur_fp_fagsak(p_inn_lastet_dato date) is
      select distinct fam_fp_personopplysninger.fagsak_id
      from fk_sensitiv.fam_fp_personopplysninger
      left outer join dt_person.dvh_person_ident_aktor_ikke_skjermet person_67_vasket
      on fam_fp_personopplysninger.aktoer_id = person_67_vasket.aktor_id
      and trunc(sysdate, 'dd') between person_67_vasket.gyldig_fra_dato and person_67_vasket.gyldig_til_dato
      where fam_fp_personopplysninger.aktoer_id is not null
      and trunc(fam_fp_personopplysninger.lastet_dato, 'dd') = p_inn_lastet_dato
      and person_67_vasket.aktor_id is null;
    cursor cur_svp_fagsak(p_inn_lastet_dato date) is
      select distinct fam_sp_personopplysninger.fagsak_id
      from fk_sensitiv.fam_sp_personopplysninger
      left outer join dt_person.dvh_person_ident_aktor_ikke_skjermet person_67_vasket
      on fam_sp_personopplysninger.aktoer_id = person_67_vasket.aktor_id
      and trunc(sysdate, 'dd') between person_67_vasket.gyldig_fra_dato and person_67_vasket.gyldig_til_dato
      where fam_sp_personopplysninger.aktoer_id is not null
      and trunc(fam_sp_personopplysninger.lastet_dato, 'dd') = p_inn_lastet_dato
      and person_67_vasket.aktor_id is null;
    cursor cur_es_fagsak(p_inn_lastet_dato date) is
      select distinct fp_engangsstonad_dvh.fagsak_id
      from fk_sensitiv.fp_engangsstonad_dvh
      left outer join dt_person.dvh_person_ident_aktor_ikke_skjermet person_67_vasket
      on fp_engangsstonad_dvh.aktoerid = person_67_vasket.aktor_id
      and trunc(sysdate, 'dd') between person_67_vasket.gyldig_fra_dato and person_67_vasket.gyldig_til_dato
      where fp_engangsstonad_dvh.aktoerid is not null
      and trunc(fp_engangsstonad_dvh.lastet_dato, 'dd') = p_inn_lastet_dato
      and person_67_vasket.aktor_id is null;      
    cursor cur_trans(p_inn_fagsak_id number) is
      select distinct fam_fp_vedtak_utbetaling.trans_id
      from fk_sensitiv.fam_fp_vedtak_utbetaling
      where fagsak_id = p_inn_fagsak_id;

      l_max_lastet_dato date := null;
  begin
    --Hent max lastet_dato for å kun behandle nyligste fagsak
    select trunc(max(lastet_dato), 'dd')
    into l_max_lastet_dato
    from fk_sensitiv.fam_fp_vedtak_utbetaling;
    --ES
    for rec_es_fagsak in cur_es_fagsak(l_max_lastet_dato) loop
      for rec_es_trans in cur_trans(rec_es_fagsak.fagsak_id) loop
        begin
          SLETT_TRANS_ID('ES', rec_es_trans.trans_id, p_error_melding);
        exception
          when others then
            insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
            values(null, rec_es_trans.trans_id, p_error_melding, sysdate, 'SLETT_KODE67_FAGSAK:ES');
            commit;
            exit;
        end;
      end loop;
    end loop;
    --FP
    for rec_fp_fagsak in cur_fp_fagsak(l_max_lastet_dato) loop
      for rec_fp_trans in cur_trans(rec_fp_fagsak.fagsak_id) loop
        begin
          SLETT_TRANS_ID('FP', rec_fp_trans.trans_id, p_error_melding);
        exception
          when others then
            insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
            values(null, rec_fp_trans.trans_id, p_error_melding, sysdate, 'SLETT_KODE67_FAGSAK:FP');
            commit;
            exit;
        end;
      end loop;
    end loop;
    --SVP
    for rec_svp_fagsak in cur_svp_fagsak(l_max_lastet_dato) loop
      for rec_svp_trans in cur_trans(rec_svp_fagsak.fagsak_id) loop
        begin
          SLETT_TRANS_ID('SVP', rec_svp_trans.trans_id, p_error_melding);
        exception
          when others then
            insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
            values(null, rec_svp_trans.trans_id, p_error_melding, sysdate, 'SLETT_KODE67_FAGSAK:SVP');
            commit;
            exit;
        end;
      end loop;
    end loop;
  exception
    when others then
      if p_error_melding is null then
        p_error_melding := sqlcode || ' ' || sqlerrm;
      end if;
      insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
      values(null, null, p_error_melding, sysdate, 'SLETT_KODE67_FAGSAK');
      commit;
  end SLETT_KODE67_FAGSAK;

  --****************************************************************************************************
  -- NAME:     SLETT_TRANS_ID
  -- PURPOSE:  Slett en spesifikk trans av vedtak.
  --
  -- REVISION:
  -- Version  Date           Author                  Description
  -- 0.1      13.02.2020     Helen Rong              Slett en spesifikk trans av vedtak fra alle tilsvarende
  --                                                 basis tabeller, for blant annet
  --                                                 FP, ES og SVP.
  --****************************************************************************************************
  procedure SLETT_TRANS_ID(p_inn_kilde in varchar2, p_inn_trans_id in number, p_out_error_melding out varchar2) as
    cursor cur_es_tabeller is
      select tabeller.owner || '.' || tabeller.table_name as tabell
      from all_tables tabeller
      join all_tab_columns kolonner
      on tabeller.owner = kolonner.owner
      and tabeller.table_name = kolonner.table_name
      and kolonner.column_name = 'TRANS_ID'
      where tabeller.owner = 'FK_SENSITIV'
      and tabeller.table_name  IN ('FP_ENGANGSSTONAD_DVH','FAM_FP_VEDTAK_UTBETALING');

    cursor cur_fp_tabeller is
      select tabeller.owner || '.' || tabeller.table_name as tabell
      from all_tables tabeller
      join all_tab_columns kolonner
      on tabeller.owner = kolonner.owner
      and tabeller.table_name = kolonner.table_name
      and kolonner.column_name = 'TRANS_ID'
      where tabeller.owner = 'FK_SENSITIV'
      and tabeller.table_name like 'FAM_FP%'
      and tabeller.table_name not like 'FAM_%HIST%';

    cursor cur_svp_tabeller is
      select tabeller.owner || '.' || tabeller.table_name as tabell
      from all_tables tabeller
      join all_tab_columns kolonner
      on tabeller.owner = kolonner.owner
      and tabeller.table_name = kolonner.table_name
      and kolonner.column_name = 'TRANS_ID'
      where tabeller.owner = 'FK_SENSITIV'
      and (tabeller.table_name like 'FAM_SP%' OR tabeller.table_name = 'FAM_FP_VEDTAK_UTBETALING')
      and tabeller.table_name not like 'FAM_%HIST%';
  begin
    if p_inn_kilde = 'ES' then
      for rec_es_tabeller in cur_es_tabeller loop
        begin
          --dbms_output.put_line('delete from ' || rec_es_tabeller.tabell || ' where trans_id = ' || p_inn_trans_id);--Test
          execute immediate 'delete from ' || rec_es_tabeller.tabell || ' where trans_id = ' || p_inn_trans_id;
        exception
          when others then
            rollback;        
            p_out_error_melding := sqlcode || ' ' || sqlerrm;
            insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
            values(null, p_inn_trans_id, p_out_error_melding, sysdate, 'SLETT_TRANS_ID:ES');
            commit;
            exit;
        end;
      end loop;
      commit;
    end if;

    if p_inn_kilde = 'FP' then
      for rec_fp_tabeller in cur_fp_tabeller loop
        begin
          --dbms_output.put_line('delete from ' || rec_fp_tabeller.tabell || ' where trans_id = ' || p_inn_trans_id);--Test
          execute immediate 'delete from ' || rec_fp_tabeller.tabell || ' where trans_id = ' || p_inn_trans_id; 
        exception
          when others then
            rollback;
            p_out_error_melding := sqlcode || ' ' || sqlerrm;
            insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
            values(null, p_inn_trans_id, p_out_error_melding, sysdate, 'SLETT_TRANS_ID:FP');
            commit;
            exit;
        end;
      end loop;
      commit;
    end if;

    if p_inn_kilde = 'SVP' then
      for rec_svp_tabeller in cur_svp_tabeller loop
        begin
          --dbms_output.put_line('delete from ' || rec_svp_tabeller.tabell || ' where trans_id = ' || p_inn_trans_id);--Test
          execute immediate 'delete from ' || rec_svp_tabeller.tabell || ' where trans_id = ' || p_inn_trans_id;
        exception
          when others then
            rollback;
            p_out_error_melding := sqlcode || ' ' || sqlerrm;
            insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
            values(null, p_inn_trans_id, p_out_error_melding, sysdate, 'SLETT_TRANS_ID:SVP');
            commit;
            exit;
        end;
      end loop;
      commit;
    end if;
    
    --Blanke ut xml_clob i hist tabell
    if p_out_error_melding is null then
      begin
        update fk_sensitiv.fam_fp_vedtak_utbetaling_hist
        set xml_clob = null
        where trans_id = p_inn_trans_id;
        commit;
      exception
        when others then
          rollback;
          p_out_error_melding := sqlcode || ' ' || sqlerrm;
          insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
          values(null, p_inn_trans_id, p_out_error_melding, sysdate, 'SLETT_TRANS_ID:HIST');
          commit;
      end;
    end if;
  end SLETT_TRANS_ID;

  --****************************************************************************************************
  -- NAME:     SLETT_GAMLE_VEDTAK
  -- PURPOSE:  Slett gamle versjoner av vedtak.
  --
  -- REVISION:
  -- Version  Date           Author                  Description
  -- 0.1      12.02.2020     Helen Rong              Slett gamle versjoner av vedtak fra alle tilsvarende
  --                                                 basis tabeller, for blant annet
  --                                                 FP, ES og SVP.
  --****************************************************************************************************
  procedure SLETT_GAMLE_VEDTAK(p_inn_kilde in varchar2, p_out_error_melding out varchar2) as
    cursor cur_es_vedtak is
      select es.trans_id, es.versjon
      from fk_sensitiv.fam_fp_vedtak_utbetaling fam
      join fk_sensitiv.fp_engangsstonad_dvh es
      on fam.trans_id = es.trans_id
      and fam.versjon != es.versjon
      where fam.fagsak_type = 'ES';--Engangsstønad

    cursor cur_fp_vedtak is
      select fp.trans_id, fp.versjon
      from fk_sensitiv.fam_fp_vedtak_utbetaling fam
      join fk_sensitiv.fam_fp_fagsak fp
      on fam.trans_id = fp.trans_id
      and fam.versjon != fp.versjon
      where fam.fagsak_type = 'FP';--Foreldrepenger

    cursor cur_svp_vedtak is
      select sp.trans_id, sp.versjon
      from fk_sensitiv.fam_fp_vedtak_utbetaling fam
      join fk_sensitiv.fam_sp_fagsak sp
      on fam.trans_id = sp.trans_id
      and fam.versjon != sp.versjon
      where fam.fagsak_type = 'SVP';--Svangerskapspenger
  begin
    if p_inn_kilde = 'ES' then
      for rec_es_vedtak in cur_es_vedtak loop
        begin
          SLETT_TRANS_ID(p_inn_kilde, rec_es_vedtak.trans_id, p_out_error_melding);
          if p_out_error_melding is not null then
            exit;
          end if;
        exception
          when others then
            p_out_error_melding := sqlcode || ' ' || sqlerrm;          
            insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
            values(null, rec_es_vedtak.trans_id, p_out_error_melding, sysdate, 'SLETT_GAMLE_VEDTAK:ES');
            commit;
            exit;
        end;
      end loop;
    end if;

    if p_inn_kilde = 'FP' then
      for rec_fp_vedtak in cur_fp_vedtak loop
        begin
          SLETT_TRANS_ID(p_inn_kilde, rec_fp_vedtak.trans_id, p_out_error_melding);
          if p_out_error_melding is not null then
            exit;
          end if;
        exception
          when others then
            p_out_error_melding := sqlcode || ' ' || sqlerrm;          
            insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
            values(null, rec_fp_vedtak.trans_id, p_out_error_melding, sysdate, 'SLETT_GAMLE_VEDTAK:FP');
            commit;
            exit;
        end;
      end loop;
    end if;

    if p_inn_kilde = 'SVP' then
      for rec_svp_vedtak in cur_svp_vedtak loop
        begin
          SLETT_TRANS_ID(p_inn_kilde, rec_svp_vedtak.trans_id, p_out_error_melding);
          if p_out_error_melding is not null then
            exit;
          end if;
        exception
          when others then
            p_out_error_melding := sqlcode || ' ' || sqlerrm;          
            insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
            values(null, rec_svp_vedtak.trans_id, p_out_error_melding, sysdate, 'SLETT_GAMLE_VEDTAK:SVP');
            commit;
            exit;
        end;
      end loop;
    end if;
  end SLETT_GAMLE_VEDTAK;  

  --****************************************************************************************************
  -- NAME:     FP_ENGANGSSTONAD_XML_UTBRETT_PROC
  -- PURPOSE:  Parse xml som ligger i forkammer.
  --
  -- REVISION:
  -- Version  Date           Author                  Description
  -- 0.1      29.10.2018     Helen Rong              Initial
  -- 0.2      19.12.2018     Helen Rong              Fjernet tidsbegrense for å laste inn alle data
  --                                                 som ikke eksisterer i fk_sensitiv.FP_ENGANGSSTONAD,
  --                                                 fra delta tabellen fk_sensitiv.lagret_vedtak.
  --                                                 Det er historisk prosedyre som ikke kjøres lenger.
  --****************************************************************************************************
  procedure FP_ENGANGSSTONAD_XML_UTBRETT(dummy in varchar2, p_error_melding out varchar2) as
    --l_max_opprettet_tid date;
    l_error_melding varchar2(1000);
    cursor cur_vedtak_engangsstonad is
      select lv.id
      from fk_sensitiv.lagret_vedtak lv
      left outer join fk_sensitiv.FP_ENGANGSSTONAD fe
      on lv.id = fe.id
      --where lv.opprettet_tid > p_opprettet_tid
      where lv.opprettet_tid < to_date('20.10.2018','dd.mm.yyyy')--Tidspunktet for skift av de 2 xml-versjoner fra kilde
      and  fe.id is null;
  BEGIN
    /*
    --Hente siste lastdato fra datakjerne
    begin
      select nvl(max(opprettet_tid),to_date('01.01.1900','dd.mm.yyyy'))
      into l_max_opprettet_tid
      from fk_sensitiv.FP_ENGANGSSTONAD;
    exception
      when others then
        p_error_melding := sqlcode || ' ' || sqlerrm;
        raise;
    end;
    */--Fjernet tidsbegrense for cursor cur_vedtak_engangsstonad

    --Parse xml en og en.
    --Ikke stopp om det feiler tilfeldigvis
    for rec_vedtak_engangsstonad in cur_vedtak_engangsstonad loop
      begin
        insert into fk_sensitiv.FP_ENGANGSSTONAD(id,
                                                 fagsak_id,
                                                 behandling_id,
                                                 behandlendeEnhet,
                                                 termindato,
                                                 boad_postnummer,
                                                 boad_land,
                                                 post_postnummer,
                                                 post_land,
                                                 antallbarn,
                                                 norskIdent,
                                                 anpa_norskIdent,
                                                 fagsakId,
                                                 behandlingsTemaKode,
                                                 behandlingsTema,
                                                 behandlingsresultat,
                                                 vedtaksdato,
                                                 beloep,
                                                 xml_ns_versjon,
                                                 lagret_vedtak_type,
                                                 versjon,
                                                 opprettet_av,
                                                 opprettet_tid,
                                                 endret_av,
                                                 endret_tid,
                                                 kl_lagret_vedtak_type,
                                                 kildesystem,
                                                 lastet_dato)
       select  id
              ,fagsak_id
              ,behandling_id
              ,cast(extractValue(xmltype(xml_clob), '/*/behandlendeEnhet') as varchar2(120)) as behandlendeEnhet--Sohaib 01.10.2018
              ,to_date(substr(extractValue(xmltype(xml_clob), '/*/personOpplysninger/terminbekreftelse/termindato'), 1, 10), 'yyyy-mm-dd') as termindato--Sohaib 01.10.2018
              ,cast(extractValue(xmltype(xml_clob), '/*/personOpplysninger/adresse[addresseType/@kode="BOAD"]/postnummer') as varchar2(22)) as boad_postnummer--Sohaib 01.10.2018
              ,cast(extractValue(xmltype(xml_clob), '/*/personOpplysninger/adresse[addresseType/@kode="BOAD"]/land') as varchar2(22)) as boad_land--Sohaib 01.10.2018
              ,cast(extractValue(xmltype(xml_clob), '/*/personOpplysninger/adresse[addresseType/@kode="POST"]/postnummer') as varchar2(22)) as post_postnummer
              ,cast(extractValue(xmltype(xml_clob), '/*/personOpplysninger/adresse[addresseType/@kode="POST"]/land') as varchar2(22)) as post_land
              ,cast(extractValue(xmltype(xml_clob), '/*/behandlingsresultat/beregningsresultat/beregningsgrunnlag/antallBarn') as number) as antallbarn --Sohaib 01.10.2018
              ,cast(extractValue(xmltype(xml_clob), '/*/personOpplysninger/bruker/norskIdent') as varchar2(22)) as norskIdent
              ,cast(extractValue(xmltype(xml_clob), '/*/personOpplysninger/familierelasjoner/familierelasjon[relasjon/@kode="ANPA"]/tilPerson/norskIdent') as varchar2(22)) as anpa_norskIdent --Sohaib 01.10.2018
              ,cast(extractValue(xmltype(xml_clob), '/*/fagsakId') as varchar2(19)) as fagsakId
              ,cast(extractValue(xmltype(xml_clob), '/*/behandlingsTema/@kode') as varchar2(110)) as behandlingsTemaKode
              ,cast(extractValue(xmltype(xml_clob), '/*/behandlingsTema') as varchar2(300)) as behandlingsTema
              ,cast(extractValue(xmltype(xml_clob), '/*/behandlingsresultat/behandlingsresultat') as varchar2(120)) as behandlingsresultat
              ,to_date(substr(extractValue(xmltype(xml_clob), '/*/vedtaksdato'), 1, 10), 'yyyy-mm-dd') as vedtaksdato
              ,cast(to_number(nvl(extractValue(xmltype(xml_clob), '/*/behandlingsresultat/beregningsresultat/tilkjentYtelse/beloep'), '0'), '999999.9') as decimal(7,1)) as beloep
              --,xmltype(xml_clob).getNamespace() as xml_ns_versjon
              ,(select xmltype(xml_clob).getNamespace() from fk_sensitiv.lagret_vedtak where id = rec_vedtak_engangsstonad.id) as xml_ns_versjon--Funksjonen over fungerte ikke
              ,lagret_vedtak_type
              ,versjon
              ,opprettet_av
              ,opprettet_tid
              ,endret_av
              ,endret_tid
              ,kl_lagret_vedtak_type
              ,kildesystem
              ,lastet_dato
        from fk_sensitiv.lagret_vedtak
        where id = rec_vedtak_engangsstonad.id;
        --dbms_output.put_line(l_create); -- Debug
        commit;
      exception
        when others then
          l_error_melding := sqlcode || ' ' || sqlerrm;
          insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
          values(null, rec_vedtak_engangsstonad.id, l_error_melding, sysdate, 'FP_ENGANGSSTONAD_XML_UTBRETT');
          commit;--Fortsett med neste rad
      end;
    end loop;
  exception
    when others then
      rollback;
      p_error_melding := sqlcode || ' ' || sqlerrm;
  END FP_ENGANGSSTONAD_XML_UTBRETT;

  --****************************************************************************************************
  -- NAME:     ENGANGSSTONAD_DVH_XML_UTBRETT
  -- PURPOSE:  Parse xml som ligger i forkammer.
  --
  -- REVISION:
  -- Version  Date           Author                  Description
  -- 0.1      03.12.2018     Helen Rong              Initial
  -- 0.2      08.01.2019     Helen Rong              Fjernet tidsbegrense for å laste inn alle data
  --                                                 som ikke eksisterer i fk_sensitiv.FP_ENGANGSSTONAD_DVH,
  --                                                 fra tabellen fk_sensitiv.hist_vedtak_utbetaling_dvh.
  -- 0.3      16.05.2019     Helen Rong              Endret source tabellen til fam_fp_vedtak_utbetaling.
  --****************************************************************************************************
  procedure ENGANGSSTONAD_DVH_XML_UTBRETT(dummy in varchar2, p_error_melding out varchar2) as
    l_commit number := 0;
    l_feil_trans_id number;
    l_error_melding varchar2(1000);
    cursor cur_vedtak_engangsstonad is
      select vud.trans_id
      from fk_sensitiv.fam_fp_vedtak_utbetaling vud
      left outer join fk_sensitiv.FP_ENGANGSSTONAD_DVH fed
      on vud.trans_id = fed.trans_id
      where vud.fagsak_type = 'ES'--Engangsstønad
      --and vud.trans_tid > p_trans_tid
      --and vud.trans_tid < to_date('20.10.2018','dd.mm.yyyy')--Tidspunktet for skift av de 2 xml-versjoner fra kilde
      and fed.trans_id is null;
  BEGIN
    /*
    --Hente siste lastdato fra datakjerne
    begin
      select nvl(max(trans_tid),to_date('01.01.1900','dd.mm.yyyy'))
      into l_max_trans_tid
      from fk_sensitiv.FP_ENGANGSSTONAD_DVH;
    exception
      when others then
        p_error_melding := sqlcode || ' ' || sqlerrm;
        raise;
    end;
    */
    --Slett den gamle versjonen av vedtak før den siste versjonen blir lastet inn.
    SLETT_GAMLE_VEDTAK('ES',l_error_melding);
    if l_error_melding is null then
      --Parse xml en og en.
      --Ikke stopp om det feiler tilfeldigvis
      for rec_vedtak_engangsstonad in cur_vedtak_engangsstonad loop
        begin
          insert into fk_sensitiv.FP_ENGANGSSTONAD_DVH(trans_id,
                                                       trans_tid,
                                                       fagsak_id,
                                                       behandling_id,
                                                       vedtak_id,
                                                       behandling_type,
                                                       soeknad_type,
                                                       fagsak_type,
                                                       vedtak_dato,
                                                       funksjonell_tid,
                                                       endret_av,
                                                       aktoerid,
                                                       kjonn,
                                                       statsborgerstatus,
                                                       personstatus,
                                                       region,
                                                       sivilstand,
                                                       foedselsdato,
                                                       foedsel_foedselsdato,
                                                       foedsel_antallbarn,
                                                       fagsakid,
                                                       behandlingstemakode,
                                                       behandlingstema,
                                                       behandlendeenhet,
                                                       vedtaksresultat,
                                                       vedtaksdato,
                                                       beregning_beloep,
                                                       termindato,
                                                       attribute2,--adressetype_kode
                                                       boad_postnummer,
                                                       boad_land,
                                                       post_postnummer,
                                                       post_land,
                                                       beregning_antallbarn,
                                                       anpa_aktoerid,
                                                       ekte_aktoerid,
                                                       vilkaargrunn_sokerskjoenn,
                                                       vilkaargrunn_antallbarn,
                                                       vilkaargrunn_termindato,
                                                       vilkaargrunn_foedseldato,
                                                       vilkaargrunn_soekersrolle,
                                                       vilkaargrunn_soeknadsdato,
                                                       vilkaarmedlem_personstatus,
                                                       vilkaarmedlem_erbrukermedlem,
                                                       vilkaarmedlem_erbrukerbosatt,
                                                       vilkaarmedlem_harbrukeropphold,
                                                       vilkaarmedlem_harbrukerlovopph,
                                                       vilkaarmedlem_erbrukernordisk,
                                                       vilkaarmedlem_erbrukerborgereu,
                                                       oppdragsid,
                                                       linje_id,
                                                       fagsystem_id,
                                                       delytelse_id,
                                                       attribute1,--delytelse_id tilhører en annen namespace
                                                       --xml_ns_versjon,
                                                       kildesystem,
                                                       lastet_dato)
          SELECT t.trans_id,
                 t.trans_tid,
                 t.fagsak_id,
                 t.behandling_id,
                 t.vedtak_id,
                 t.behandling_type,
                 t.soeknad_type,
                 t.fagsak_type,
                 t.vedtak_dato,
                 t.funksjonell_tid,
                 t.endret_av,
                 q.aktoerid,--har null verdi
                 q.kjonn,
                 q.statsborgerstatus,
                 q.personstatus,
                 q.region,
                 q.sivilstand,
                 to_date(substr(q.foedselsdato,1,10), 'yyyy-mm-dd'),
                 to_date(substr(q.foedsel_foedselsdato,1,10), 'yyyy-mm-dd'),
                 q.foedsel_antallbarn,
                 q.fagsakid,
                 q.behandlingstemakode,
                 q.behandlingstema,
                 q.behandlendeenhet,
                 q.vedtaksresultat,
                 to_date(substr(q.vedtaksdato,1,10), 'yyyy-mm-dd'),
                 cast(to_number(nvl(q.beregning_beloep, 0), '999999.9') as decimal(7,1)),
                 to_date(substr(q.termindato,1,10), 'yyyy-mm-dd'),
                 q.adressetype_kode,
                 q.boad_postnummer,--Trans_id 1953 har 2 bostedaddresse--har null verdi
                 q.boad_land,--har null verdi
                 q.post_postnummer,--har null verdi
                 q.post_land,--har null verdi
                 q.beregning_antallbarn,--har null verdi
                 q.anpa_aktoerid,--har null verdi
                 q.ekte_aktoerid,
                 q.vilkaargrunn_sokerskjoenn,
                 q.vilkaargrunn_antallbarn,
                 to_date(substr(q.vilkaargrunn_termindato,1,10), 'yyyy-mm-dd'),
                 to_date(substr(q.vilkaargrunn_foedseldato,1,10), 'yyyy-mm-dd'),
                 q.vilkaargrunn_soekersrolle,
                 to_date(substr(q.vilkaargrunn_soeknadsdato,1,10), 'yyyy-mm-dd'),
                 q.vilkaarmedlem_personstatus,
                 q.vilkaarmedlem_erbrukermedlem,
                 q.vilkaarmedlem_erbrukerbosatt,
                 q.vilkaarmedlem_harbrukeropphold,
                 q.vilkaarmedlem_harbrukerlovopph,
                 q.vilkaarmedlem_erbrukernordisk,
                 q.vilkaarmedlem_erbrukerborgereu,
                 q.oppdragsid,
                 q.linje_id,
                 q.fagsystem_id,
                 q.delytelse_id,
                 q.delytelse_id2,
                 --(select xml_clob.getNamespace() from fk_sensitiv.fam_fp_vedtak_utbetaling where trans_id = rec_vedtak_engangsstonad.trans_id) as xml_ns_versjon,
                 t.kildesystem,
                 t.lastet_dato
          FROM  FK_SENSITIV.fam_fp_vedtak_utbetaling t
          LEFT JOIN XMLTABLE
          (
               XMLNamespaces(
                        'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'       AS "ns2" --2
                       ,'urn:no:nav:vedtak:felles:xml:felles:v2'                             AS "ns3" --3
                       ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'          AS "ns4" --4
                       ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'   AS "ns5" --5 
                       ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'   AS "ns6" --6
                       ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'       AS "ns7" --7
                       ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'            AS "ns8" --8
                       ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'         AS "ns9" --9
                       ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'         AS "ns10"--10 
                       ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                   AS "ns11"--11
                       ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                   AS "ns12"--12
                       ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'       AS "ns13"--13 
                       ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'       AS "ns14"--14
                       ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                             AS "ns15"--15 
                       ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                     AS "ns16"--16
                       ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                    AS "ns17"--17
                       ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'              AS "ns18"--18
               )
          ,'/ns15:vedtak'
          PASSING t.XML_CLOB
          COLUMNS

           AKTOERID                                   VARCHAR2(20)  PATH './ns15:personOpplysninger/ns5:PersonopplysningerDvhEngangsstoenad/ns5:bruker/ns4:aktoerId'
          ,KJONN                                      VARCHAR2(10)  PATH './ns15:personOpplysninger/ns5:PersonopplysningerDvhEngangsstoenad/ns5:bruker/ns4:kjoenn'
          ,STATSBORGERSTATUS                          VARCHAR2(100) PATH './ns15:personOpplysninger/ns5:PersonopplysningerDvhEngangsstoenad/ns5:bruker/ns4:statsborgerskap'
          ,PERSONSTATUS                               VARCHAR2(20)  PATH './ns15:personOpplysninger/ns5:PersonopplysningerDvhEngangsstoenad/ns5:bruker/ns4:personstatus'
          ,REGION                                     VARCHAR2(20)  PATH './ns15:personOpplysninger/ns5:PersonopplysningerDvhEngangsstoenad/ns5:bruker/ns4:region'
          ,SIVILSTAND                                 VARCHAR2(20)  PATH './ns15:personOpplysninger/ns5:PersonopplysningerDvhEngangsstoenad/ns5:bruker/ns4:sivilstand'
          ,FOEDSELSDATO                               VARCHAR2(10)  PATH './ns15:personOpplysninger/ns5:PersonopplysningerDvhEngangsstoenad/ns5:bruker/ns4:foedselsdato'
          ,FOEDSEL_FOEDSELSDATO                       VARCHAR2(10)  PATH './ns15:personOpplysninger/ns5:PersonopplysningerDvhEngangsstoenad/ns5:foedsel/ns4:foedselsdato'
          ,FOEDSEL_ANTALLBARN                         NUMBER        PATH './ns15:personOpplysninger/ns5:PersonopplysningerDvhEngangsstoenad/ns5:foedsel/ns4:antallBarn'
          ,FAGSAKID                                   VARCHAR2(19)  PATH './ns15:fagsakId'
          --,FAGSAKTYPE                                 VARCHAR2(100) PATH './ns15:fagsakType'
          ,BEHANDLINGSTEMAKODE                        VARCHAR2(110) PATH './ns15:behandlingsTema/@kode'
          ,BEHANDLINGSTEMA                            VARCHAR2(300) PATH './ns15:behandlingsTema'
          ,BEHANDLENDEENHET                           VARCHAR2(120) PATH './ns15:behandlendeEnhet'    
          ,VEDTAKSRESULTAT                            VARCHAR2(120) PATH './ns15:vedtaksresultat'
          ,VEDTAKSDATO                                VARCHAR2(10)  PATH './ns15:vedtaksdato'
          ,BEREGNING_BELOEP                           VARCHAR2(19)  PATH './ns15:behandlingsresultat/ns15:beregningsresultat/ns15:tilkjentYtelse/ns12:ytelseEngangsstoenad/ns12:beloep'
          ,TERMINDATO                                 VARCHAR2(10)  PATH './ns15:personOpplysninger/ns5:PersonopplysningerDvhEngangsstoenad/ns5:terminbekreftelse/ns5:termindato'
          ,ADRESSETYPE_KODE                           VARCHAR2(110) PATH './ns15:personOpplysninger/ns5:PersonopplysningerDvhEngangsstoenad/ns5:adresse[1]/ns5:addresseType/@kode'
          ,BOAD_POSTNUMMER                            VARCHAR2(22)  PATH './ns15:personOpplysninger/ns5:PersonopplysningerDvhEngangsstoenad/ns5:adresse[(ns5:addresseType/@kode="BOSTEDSADRESSE")]/ns5:postnummer'
          ,BOAD_LAND                                  VARCHAR2(22)  PATH './ns15:personOpplysninger/ns5:PersonopplysningerDvhEngangsstoenad/ns5:adresse[(ns5:addresseType/@kode="BOSTEDSADRESSE")]/ns5:land'
          ,POST_POSTNUMMER                            VARCHAR2(22)  PATH './ns15:personOpplysninger/ns5:PersonopplysningerDvhEngangsstoenad/ns5:adresse[(ns5:addresseType/@kode="POSTADRESSE")]/ns5:postnummer'--Trenger test
          ,POST_LAND                                  VARCHAR2(22)  PATH './ns15:personOpplysninger/ns5:PersonopplysningerDvhEngangsstoenad/ns5:adresse[(ns5:addresseType/@kode="POSTADRESSE")]/ns5:land'--Trenger test
          ,BEREGNING_ANTALLBARN                       NUMBER        PATH './ns15:behandlingsresultat/ns15:beregningsresultat/ns15:beregningsgrunnlag/ns13:BeregningsgrunnlagEngangsstoenad/ns13:antallBarn'
          ,ANPA_AKTOERID                              VARCHAR2(20)  PATH './ns15:personOpplysninger/ns5:PersonopplysningerDvhEngangsstoenad/ns5:familierelasjoner/ns5:familierelasjon[ns4:relasjon/@kode="ANPA"]/ns5:tilPerson/ns4:aktoerId'
          ,EKTE_AKTOERID                              VARCHAR2(20)  PATH './ns15:personOpplysninger/ns5:PersonopplysningerDvhEngangsstoenad/ns5:familierelasjoner/ns5:familierelasjon[ns4:relasjon/@kode="EKTE"]/ns5:tilPerson/ns4:aktoerId'
          ,VILKAARGRUNN_SOKERSKJOENN                  VARCHAR2(10)  PATH './ns15:behandlingsresultat/ns15:vurderteVilkaar/ns15:vilkaar[(@vurdert="AUTOMATISK")][(ns15:type/@kode="FP_VK_1")]/ns15:vilkaarsgrunnlag/ns8:vilkaarsgrunnlag/ns8:sokersKjoenn'
          ,VILKAARGRUNN_ANTALLBARN                    NUMBER        PATH './ns15:behandlingsresultat/ns15:vurderteVilkaar/ns15:vilkaar[(@vurdert="AUTOMATISK")][(ns15:type/@kode="FP_VK_1")]/ns15:vilkaarsgrunnlag/ns8:vilkaarsgrunnlag/ns8:antallBarn'
          ,VILKAARGRUNN_TERMINDATO                    VARCHAR2(10)  PATH './ns15:behandlingsresultat/ns15:vurderteVilkaar/ns15:vilkaar[(@vurdert="AUTOMATISK")][(ns15:type/@kode="FP_VK_1")]/ns15:vilkaarsgrunnlag/ns8:vilkaarsgrunnlag/ns8:termindato'
          ,VILKAARGRUNN_FOEDSELDATO                   VARCHAR2(10)  PATH './ns15:behandlingsresultat/ns15:vurderteVilkaar/ns15:vilkaar[(@vurdert="AUTOMATISK")][(ns15:type/@kode="FP_VK_1")]/ns15:vilkaarsgrunnlag/ns8:vilkaarsgrunnlag/ns8:foedselsdatoBarn'
          ,VILKAARGRUNN_SOEKERSROLLE                  VARCHAR2(100) PATH './ns15:behandlingsresultat/ns15:vurderteVilkaar/ns15:vilkaar[(@vurdert="AUTOMATISK")][(ns15:type/@kode="FP_VK_1")]/ns15:vilkaarsgrunnlag/ns8:vilkaarsgrunnlag/ns8:soekersRolle'
          ,VILKAARGRUNN_SOEKNADSDATO                  VARCHAR2(10)  PATH './ns15:behandlingsresultat/ns15:vurderteVilkaar/ns15:vilkaar[(@vurdert="AUTOMATISK")][(ns15:type/@kode="FP_VK_1")]/ns15:vilkaarsgrunnlag/ns8:vilkaarsgrunnlag/ns8:soeknadsdato'
          ,VILKAARMEDLEM_PERSONSTATUS                 VARCHAR2(20)  PATH './ns15:behandlingsresultat/ns15:vurderteVilkaar/ns15:vilkaar[(@vurdert="AUTOMATISK")][(ns15:type/@kode="FP_VK_2")]/ns15:vilkaarsgrunnlag/ns8:vilkaarsgrunnlag/ns8:personstatus'
          ,VILKAARMEDLEM_ERBRUKERMEDLEM               VARCHAR2(10)  PATH './ns15:behandlingsresultat/ns15:vurderteVilkaar/ns15:vilkaar[(@vurdert="AUTOMATISK")][(ns15:type/@kode="FP_VK_2")]/ns15:vilkaarsgrunnlag/ns8:vilkaarsgrunnlag/ns8:erBrukerMedlem'
          ,VILKAARMEDLEM_ERBRUKERBOSATT               VARCHAR2(10)  PATH './ns15:behandlingsresultat/ns15:vurderteVilkaar/ns15:vilkaar[(@vurdert="AUTOMATISK")][(ns15:type/@kode="FP_VK_2")]/ns15:vilkaarsgrunnlag/ns8:vilkaarsgrunnlag/ns8:erBrukerBosatt'
          ,VILKAARMEDLEM_HARBRUKEROPPHOLD             VARCHAR2(10)  PATH './ns15:behandlingsresultat/ns15:vurderteVilkaar/ns15:vilkaar[(@vurdert="AUTOMATISK")][(ns15:type/@kode="FP_VK_2")]/ns15:vilkaarsgrunnlag/ns8:vilkaarsgrunnlag/ns8:harBrukerOppholdsrett'
          ,VILKAARMEDLEM_HARBRUKERLOVOPPH             VARCHAR2(10)  PATH './ns15:behandlingsresultat/ns15:vurderteVilkaar/ns15:vilkaar[(@vurdert="AUTOMATISK")][(ns15:type/@kode="FP_VK_2")]/ns15:vilkaarsgrunnlag/ns8:vilkaarsgrunnlag/ns8:harBrukerLovligOppholdINorge'
          ,VILKAARMEDLEM_ERBRUKERNORDISK              VARCHAR2(10)  PATH './ns15:behandlingsresultat/ns15:vurderteVilkaar/ns15:vilkaar[(@vurdert="AUTOMATISK")][(ns15:type/@kode="FP_VK_2")]/ns15:vilkaarsgrunnlag/ns8:vilkaarsgrunnlag/ns8:erBrukerNordiskstatsborger'
          ,VILKAARMEDLEM_ERBRUKERBORGEREU             VARCHAR2(10)  PATH './ns15:behandlingsresultat/ns15:vurderteVilkaar/ns15:vilkaar[(@vurdert="AUTOMATISK")][(ns15:type/@kode="FP_VK_2")]/ns15:vilkaarsgrunnlag/ns8:vilkaarsgrunnlag/ns8:erBrukerBorgerAvEUEOS'
          ,OPPDRAGSID                                 VARCHAR2(30)  PATH './ns15:oppdrag/ns16:oppdragId'
          ,LINJE_ID                                   VARCHAR2(30)  PATH './ns15:oppdrag/ns18:linjeId'
          ,FAGSYSTEM_ID                               VARCHAR2(30)  PATH './ns15:oppdrag/ns16:fagsystemId'    
          ,DELYTELSE_ID                               VARCHAR2(30)  PATH './ns15:oppdrag/ns18:delytelseId'
          ,DELYTELSE_ID2                              VARCHAR2(30)  PATH './ns15:oppdrag/ns16:delytelseId'
          ) q
          ON 1 = 1
          where t.trans_id = rec_vedtak_engangsstonad.trans_id;
         --dbms_output.put_line(l_create); -- Debug
         l_commit := l_commit + 1;
         if l_commit >= 10000 then
          commit;
          l_commit := 0;
         end if;
        exception
          when others then
            --Fortsett med neste rad
            l_feil_trans_id := rec_vedtak_engangsstonad.trans_id;
            l_error_melding := sqlcode || ' ' || sqlerrm;
            insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
            values(null, rec_vedtak_engangsstonad.trans_id, l_error_melding, sysdate, 'ENGANGSSTONAD_DVH_XML_UTBRETT');
        end;
      end loop;
      if l_error_melding is not null then
        insert into fk_sensitiv.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
        values(l_feil_trans_id, l_error_melding, sysdate, 'PL/SQL: FP_ENGANGSSTONAD_DVH');
      end if;
      commit;--commit til slutt
    end if;
  exception
    when others then
      rollback;
      p_error_melding := sqlcode || ' ' || sqlerrm;
  END ENGANGSSTONAD_DVH_XML_UTBRETT;

  --****************************************************************************************************
  -- NAME:     FP_DVH_XML_UTBRETT
  -- PURPOSE:  Parse xml som ligger i forkammer.
  --
  -- REVISION:
  -- Version  Date           Author                  Description
  -- 0.1      23.05.2019     Helen Rong              Utpakke FP xml som ligger i felles tabellen
  --                                                 fk_sensitiv.fam_fp_vedtak_utbetaling.
  --****************************************************************************************************
  procedure FP_DVH_XML_UTBRETT(dummy in varchar2, p_error_melding out varchar2) as
    l_commit number := 0;
    l_error_melding varchar2(1000) := null;
    l_feil_kilde_navn varchar2(50) := null;
    l_feil_trans_id number;
    cursor cur_vedtak_fp is
      select vud.trans_id
      from fk_sensitiv.fam_fp_vedtak_utbetaling vud
      left outer join fk_sensitiv.fam_fp_fagsak fed
      on vud.trans_id = fed.trans_id
      where vud.fagsak_type = 'FP'--Foreldrepenger
      --and rownum <= 1000--Test!!!
      and fed.trans_id is null;
  BEGIN
    --Slett den gamle versjonen av vedtak før den siste versjonen blir lastet inn.
    SLETT_GAMLE_VEDTAK('FP',l_error_melding);
    if l_error_melding is null then
      --Parse xml en og en.
      --Ikke stopp om det feiler tilfeldigvis
      for rec_vedtak_fp in cur_vedtak_fp loop
        begin
          if l_error_melding is not null then
            insert into fk_sensitiv.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
            values(l_feil_trans_id, l_error_melding, sysdate, l_feil_kilde_navn);
            l_commit := l_commit +1;
            l_error_melding := null;
            l_feil_kilde_navn := null;
            l_feil_trans_id := null;
          end if;
          savepoint do_insert;
          --Insert utbrettede data
          --FP_FAGSAK
          begin
            INSERT INTO FK_SENSITIV.FAM_FP_FAGSAK
            (
               TRANS_ID	               
              ,TRANS_TID	              
              ,VEDTAK_ID	              
              ,FUNKSJONELL_TID         
              ,FAGSAK_ID               
              ,BEHANDLINGS_ID          
              ,FAGSAKANNENFORELDER_ID  
              ,FAGSAK_TYPE             
              ,TEMA                    
              ,TEMA_KODEVERK           
              ,BEHANDLINGSTEMA         
              ,BEHANDLINGSTEMA_KODEVERK
              ,SOEKNADSDATO            
              ,VEDTAKSDATO             
              ,BEHANDLENDEENHET        
              ,VEDTAKSRESULTAT         
              ,BEHANDLINGSRESULTAT     
              ,BEHANDLINGSTYPE         
              ,KILDESYSTEM             
              ,LASTET_DATO             
            )
            SELECT   t.TRANS_ID
                    ,t.TRANS_TID
                    ,t.VEDTAK_ID
                    ,t.FUNKSJONELL_TID
                    ,q.FAGSAK_ID
                    ,q.BEHANDLINGS_ID
                    ,q.FAGSAKANNENFORELDER_ID
                    ,q.FAGSAK_TYPE
                    ,q.TEMA
                    ,q.TEMA_KODEVERK
                    ,q.BEHANDLINGSTEMA
                    ,q.BEHANDLINGSTEMA_KODEVERK 
                    ,to_date(q.SOEKNADSDATO,'YYYY-MM-DD')
                    ,to_date(q.VEDTAKSDATO,'YYYY-MM-DD')
                    ,q.BEHANDLENDEENHET
                    ,q.VEDTAKSRESULTAT
                    ,q.BEHANDLINGSRESULTAT
                    ,q.BEHANDLINGSTYPE
                    ,t.kildesystem
                    ,t.lastet_dato
            FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t,
            XMLTABLE
            (
               XMLNamespaces
               (                    
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19
               )
               ,'/ns19:vedtak'
               PASSING t.XML_CLOB
               COLUMNS            
                 FAGSAK_ID                   NUMBER(19,0)       PATH './ns19:fagsakId'
                ,BEHANDLINGS_ID              NUMBER(19,0)       PATH './ns19:behandlingsresultat/ns19:behandlingsId'
                ,FAGSAKANNENFORELDER_ID      VARCHAR2(100 CHAR) PATH './ns19:fagsakAnnenForelderId' 
                ,FAGSAK_TYPE                 VARCHAR2(100 CHAR) PATH './ns19:fagsakType'  
                ,TEMA                        VARCHAR2(100 CHAR) PATH './ns19:tema/@kode'
                ,TEMA_KODEVERK               VARCHAR2(100 CHAR) PATH './ns19:tema/@kodeverk' 
                ,BEHANDLINGSTEMA             VARCHAR2(100 CHAR) PATH './ns19:behandlingsTema/@kode'
                ,BEHANDLINGSTEMA_KODEVERK    VARCHAR2(100 CHAR) PATH './ns19:behandlingsTema/@kodeverk'
                ,SOEKNADSDATO                VARCHAR2(100 CHAR) PATH './ns19:soeknadsdato'
                ,VEDTAKSDATO                 VARCHAR2(100 CHAR) PATH './ns19:vedtaksdato' 
                ,BEHANDLENDEENHET            VARCHAR2(100 CHAR) PATH './ns19:behandlendeEnhet' 
                ,VEDTAKSRESULTAT             VARCHAR2(100 CHAR) PATH './ns19:vedtaksresultat' 
                ,BEHANDLINGSRESULTAT         VARCHAR2(100 CHAR) PATH './ns19:behandlingsresultat/ns19:behandlingsresultat/@kode'
                ,BEHANDLINGSTYPE             VARCHAR2(100 CHAR) PATH './ns19:behandlingsresultat/ns19:behandlingstype'       
            ) q
            where t.trans_id = rec_vedtak_fp.trans_id;
          exception
            when others then
              l_error_melding := sqlerrm;
              l_feil_kilde_navn := 'PL/SQL: FAM_FP_FAGSAK';
              l_feil_trans_id := rec_vedtak_fp.trans_id;
              rollback to do_insert; continue;
          end;

          --FAM_FP_ADRESSE
          begin
            INSERT INTO  FK_SENSITIV.FAM_FP_ADRESSE
            (
               TRANS_ID	      
              ,TRANS_TID
              ,VEDTAK_ID	     
              ,FUNKSJONELL_TID
              ,FAGSAK_ID      
              ,BEHANDLINGS_ID 
              ,ADRESSE_TYPE   
              ,POSTNUMMER     
              ,LAND           
              ,KILDESYSTEM    
              ,LASTET_DATO    
            )
            SELECT   t.TRANS_ID
                    ,t.TRANS_TID
                    ,t.VEDTAK_ID
                    ,t.FUNKSJONELL_TID
                    ,q.FAGSAK_ID
                    ,q.BEHANDLINGS_ID
                    ,b.ADRESSE_TYPE
                    ,b.POSTNUMMER
                    ,b.LAND
                    ,t.kildesystem
                    ,t.lastet_dato
            FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t,     
            XMLTABLE
            (
              XMLNamespaces
              (                                    
                 'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19                                                  
              )
              ,'/ns19:vedtak'
              PASSING t.XML_CLOB
              COLUMNS             
                FAGSAK_ID               NUMBER(19,0)  PATH './ns19:fagsakId' 
               ,BEHANDLINGS_ID          NUMBER(19,0)  PATH './ns19:behandlingsresultat/ns19:behandlingsId'
               ,ADRESSE                 XMLTYPE       PATH './ns19:personOpplysninger/ns4:PersonopplysningerDvhForeldrepenger/ns4:adresse'        
            ) q,
            XMLTABLE
            (
              XMLNamespaces
              (                               
                 'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19    
              )
              ,'ns4:adresse'
              PASSING q.ADRESSE
              COLUMNS   
                ADRESSE_TYPE            VARCHAR2(100) PATH 'ns4:adressetype/@kode'
               ,POSTNUMMER              VARCHAR2(100) PATH 'ns4:postnummer'
               ,LAND                    VARCHAR2(100) PATH 'ns4:land'
            ) b
            where t.trans_id = rec_vedtak_fp.trans_id;
          exception
            when others then
               l_error_melding := sqlerrm;
               l_feil_kilde_navn := 'PL/SQL: FAM_FP_ADRESSE';
               l_feil_trans_id := rec_vedtak_fp.trans_id;
               rollback to do_insert; continue;
          end;

          --FAM_FP_PERSONOPPLYSNINGER
          begin
            INSERT INTO FK_SENSITIV.FAM_FP_PERSONOPPLYSNINGER
            (
               TRANS_ID	              
              ,TRANS_TID	             
              ,VEDTAK_ID	             
              ,FUNKSJONELL_TID        
              ,FAGSAK_ID              
              ,BEHANDLINGS_ID         
              ,AKTOER_ID              
              ,REGION                 
              ,KJONN                  
              ,STATSBORGERSKAP        
              ,SIVILSTAND             
              ,FOEDSELSDATO           
              ,PERSONSTATUS           
              ,ANNENFORELDER_AKTOER_ID
              ,KILDESYSTEM            
              ,LASTET_DATO             
            )
            SELECT   t.TRANS_ID
                    ,t.TRANS_TID
                    ,t.VEDTAK_ID
                    ,t.FUNKSJONELL_TID
                    ,q.FAGSAK_ID
                    ,q.BEHANDLINGS_ID
                    ,q.AKTOER_ID
                    ,q.REGION
                    ,q.KJONN
                    ,q.STATSBORGERSKAP
                    ,q.SIVILSTAND
                    ,to_date(q.FOEDSELSDATO,'YYYY-MM-DD')
                    ,q.PERSONSTATUS        
                    ,q.ANNENFORELDER_AKTOER_ID    
                    ,t.kildesystem
                    ,t.lastet_dato
            FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t,          
            XMLTABLE
            (
              XMLNamespaces
              (                  
                 'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19
               )
               ,'/ns19:vedtak'
               PASSING t.XML_CLOB
               COLUMNS        
                FAGSAK_ID                  NUMBER(19,0)       PATH './ns19:fagsakId' 
               ,BEHANDLINGS_ID             NUMBER(19,0)       PATH './ns19:behandlingsresultat/ns19:behandlingsId'
               ,AKTOER_ID                  VARCHAR2(50 CHAR)  PATH './ns19:personOpplysninger/ns4:PersonopplysningerDvhForeldrepenger/ns4:bruker/ns7:aktoerId'
               ,REGION                     VARCHAR2(100 CHAR) PATH './ns19:personOpplysninger/ns4:PersonopplysningerDvhForeldrepenger/ns4:bruker/ns7:region'
               ,KJONN                      VARCHAR2(100 CHAR) PATH './ns19:personOpplysninger/ns4:PersonopplysningerDvhForeldrepenger/ns4:bruker/ns7:kjoenn'
               ,STATSBORGERSKAP            VARCHAR2(100 CHAR) PATH './ns19:personOpplysninger/ns4:PersonopplysningerDvhForeldrepenger/ns4:bruker/ns7:statsborgerskap/@kode'
               ,SIVILSTAND                 VARCHAR2(100 CHAR) PATH './ns19:personOpplysninger/ns4:PersonopplysningerDvhForeldrepenger/ns4:bruker/ns7:sivilstand/@kode'
               ,FOEDSELSDATO               VARCHAR2(200 CHAR) PATH './ns19:personOpplysninger/ns4:PersonopplysningerDvhForeldrepenger/ns4:bruker/ns7:foedselsdato'
               ,PERSONSTATUS               VARCHAR2(100 CHAR) PATH './ns19:personOpplysninger/ns4:PersonopplysningerDvhForeldrepenger/ns4:bruker/ns7:personstatus/@kode'
               ,ANNENFORELDER_AKTOER_ID    VARCHAR2(50 CHAR)  PATH './ns19:personOpplysninger/ns4:PersonopplysningerDvhForeldrepenger/ns4:annenForelder/ns4:aktoerId'    
            ) q
            where t.trans_id = rec_vedtak_fp.trans_id;
          exception
            when others then
              l_error_melding := sqlerrm;
              l_feil_kilde_navn := 'PL/SQL: FAM_FP_PERSONOPPLYSNINGER';
              l_feil_trans_id := rec_vedtak_fp.trans_id;
              rollback to do_insert; continue;
          end;

          --FAM_FP_FAMILIEHENDELSE
          begin
            INSERT INTO FK_SENSITIV.FAM_FP_FAMILIEHENDELSE
            (
               TRANS_ID	      
              ,TRANS_TID	     
              ,VEDTAK_ID	     
              ,FUNKSJONELL_TID
              ,FAGSAK_ID      
              ,BEHANDLINGS_ID 
              ,RELASJON       
              ,TIL_AKTOER_ID  
              ,KJOENN         
              ,STATSBORGERSKAP
              ,PERSONSTATUS   
              ,REGION         
              ,SIVILSTAND     
              ,FOEDSELSDATO   
              ,KILDESYSTEM    
              ,LASTET_DATO    
            )
            SELECT
                 t.TRANS_ID
                ,t.TRANS_TID
                ,t.VEDTAK_ID
                ,t.FUNKSJONELL_TID
                ,q.FAGSAK_ID
                ,q.BEHANDLINGS_ID
                ,a.RELASJON               
                ,a.TIL_AKTOER_ID        
                ,a.KJOENN                 
                ,a.STATSBORGERSKAP       
                ,a.PERSONSTATUS          
                ,a.REGION               
                ,a.SIVILSTAND     
                ,to_date(a.FOEDSELSDATO,'YYYY-MM-DD')
                ,t.kildesystem
                ,t.lastet_dato
            FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t,       
            XMLTABLE
            (
               XMLNamespaces
               (                   
                  'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                 ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
               ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19  
               )
               ,'/ns19:vedtak'
               PASSING t.XML_CLOB
               COLUMNS        
               FAGSAK_ID                    NUMBER(19,0) PATH './ns19:fagsakId'
              ,BEHANDLINGS_ID               NUMBER(19,0) PATH './ns19:behandlingsresultat/ns19:behandlingsId'
              ,FAMILIERELASJON              XMLTYPE      PATH  './ns19:personOpplysninger/ns4:PersonopplysningerDvhForeldrepenger/ns4:familierelasjoner/ns4:familierelasjon'
            ) q,       
            XMLTABLE
            (
              XMLNamespaces
              (                               
                  'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                 ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                 ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19 
              )
              ,'ns4:familierelasjon'
              PASSING q.FAMILIERELASJON
              COLUMNS      
               RELASJON               VARCHAR2(100 CHAR) PATH 'ns7:relasjon/@kode'
              ,TIL_AKTOER_ID          VARCHAR2(100 CHAR) PATH 'ns4:tilPerson/ns7:aktoerId'
              ,KJOENN                 VARCHAR2(100 CHAR) PATH 'ns4:tilPerson/ns7:kjoenn'
              ,STATSBORGERSKAP        VARCHAR2(100 CHAR) PATH 'ns4:tilPerson/ns7:statsborgerskap/@kode'
              ,PERSONSTATUS           VARCHAR2(100 CHAR) PATH 'ns4:tilPerson/ns7:personstatus/@kode'
              ,REGION                 VARCHAR2(100 CHAR) PATH 'ns4:tilPerson/ns7:region'
              ,SIVILSTAND             VARCHAR2(100 CHAR) PATH 'ns4:tilPerson/ns7:sivilstand/@kode'
              ,FOEDSELSDATO           VARCHAR2(100 CHAR) PATH 'ns4:tilPerson/ns7:foedselsdato'
            ) a
            where t.trans_id = rec_vedtak_fp.trans_id;
          exception
            when others then
              l_error_melding := sqlerrm;
              l_feil_kilde_navn := 'PL/SQL: FAM_FP_FAMILIEHENDELSE';
              l_feil_trans_id := rec_vedtak_fp.trans_id;
              rollback to do_insert; continue;
          end;

          --FAM_FP_UTTAK_FP_KONTOER
          begin
            INSERT INTO  FK_SENSITIV.FAM_FP_UTTAK_FP_KONTOER
            (
               TRANS_ID	                
              ,TRANS_TID	               
              ,VEDTAK_ID	               
              ,FUNKSJONELL_TID          
              ,FAGSAK_ID                
              ,BEHANDLINGS_ID           
              ,FOERSTE_LOVLIGE_UTTAKSDAG
              ,STOENADSKONTOTYPE        
              ,MAX_DAGER                
              ,KILDESYSTEM              
              ,LASTET_DATO              
            )
            SELECT 
                    t.TRANS_ID
                   ,t.TRANS_TID
                   ,t.VEDTAK_ID
                   ,t.FUNKSJONELL_TID
                   ,q.FAGSAK_ID
                   ,q.BEHANDLINGS_ID
                   ,to_date(a.FOERSTE_LOVLIGE_UTTAKSDAG,'YYYY-MM-DD')
                   ,b.STOENADSKONTOTYPE      
                   ,b.MAX_DAGER
                   ,t.kildesystem
                   ,t.lastet_dato
            FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t,
            XMLTABLE
            (
               XMLNamespaces
               (                   
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19 
               )
               ,'/ns19:vedtak'
               PASSING t.XML_CLOB
               COLUMNS
                 FAGSAK_ID         NUMBER(19,0)  PATH './ns19:fagsakId'
                ,BEHANDLINGS_ID    NUMBER(19,0)  PATH './ns19:behandlingsresultat/ns19:behandlingsId'
                ,UTTAK             XMLTYPE       PATH  './ns19:behandlingsresultat/ns19:beregningsresultat/ns19:uttak'
                ,STOENADSKONTOER   XMLTYPE       PATH  './ns19:behandlingsresultat/ns19:beregningsresultat/ns19:uttak/ns16:uttak/ns16:stoenadskontoer'
            ) q,
            XMLTABLE
            (
               XMLNamespaces
               (
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19     
               )
               ,'ns19:uttak' PASSING q.UTTAK
               COLUMNS      
                 FOERSTE_LOVLIGE_UTTAKSDAG       VARCHAR2(200) PATH 'ns16:uttak/ns16:foersteLovligeUttaksdag'
            ) a,
            XMLTABLE
            (
               XMLNamespaces
               (
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19      
               )
               ,'ns16:stoenadskontoer'
               PASSING q.STOENADSKONTOER
               COLUMNS      
                  STOENADSKONTOTYPE      VARCHAR2(100 CHAR) PATH 'ns16:stoenadskontotype'
                 ,MAX_DAGER              NUMBER(3,0)        PATH 'ns16:maxdager'
            ) b
            where t.trans_id = rec_vedtak_fp.trans_id;
          exception
            when others then
              l_error_melding := sqlerrm;
              l_feil_kilde_navn := 'PL/SQL: FAM_FP_UTTAK_FP_KONTOER';
              l_feil_trans_id := rec_vedtak_fp.trans_id;
              rollback to do_insert; continue;
          end;

          --FAM_FP_UTTAK_RES_PER_AKTIV
          begin
            INSERT INTO FK_SENSITIV.FAM_FP_UTTAK_RES_PER_AKTIV
            (
               TRANS_ID
              ,TRANS_TID
              ,VEDTAK_ID
              ,FUNKSJONELL_TID
              ,FAGSAK_ID
              ,BEHANDLINGS_ID
              ,FOM
              ,TOM
              ,SAMTIDIG_UTTAK
              ,UTTAK_UTSETTELSE_TYPE
              ,GRADERING_INNVILGET
              ,TREKKONTO              
              ,TREKKDAGER             
              ,VIRKSOMHET              
              ,ARBEIDSTIDSPROSENT   
              ,UTBETALINGSPROSENT 
              ,UTTAK_ARBEID_TYPE
              ,GRADERING
              ,GRADERINGSDAGER
              ,PERIODE_RESULTAT_TYPE
              ,PERIODE_RESULTAT_AARSAK
              ,kildesystem
              ,lastet_dato
            )
            SELECT
                 t.TRANS_ID
                ,t.TRANS_TID
                ,t.VEDTAK_ID
                ,t.FUNKSJONELL_TID
                ,a.FAGSAK_ID
                ,a.BEHANDLINGS_ID
                ,to_date(b.FOM,'YYYY-MM-DD')
                ,to_date(b.TOM,'YYYY-MM-DD')
                ,b.SAMTIDIG_UTTAK
                ,b.UTTAK_UTSETTELSE_TYPE
                ,b.GRADERING_INNVILGET
                ,d.TREKKONTO              
                ,d.TREKKDAGER
                ,d.VIRKSOMHET              
                ,d.ARBEIDSTIDSPROSENT
                ,d.UTBETALINGSPROSENT
                ,d.UTTAK_ARBEID_TYPE
                ,d.GRADERING
                ,d.GRADERINGSDAGER
                ,b.PERIODE_RESULTAT_TYPE
                ,b.PERIODE_RESULTAT_AARSAK
                ,t.kildesystem
                ,t.lastet_dato
            FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t,
            XMLTABLE
            (
               XMLNamespaces
               (
                              'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                             ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8 
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13  
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19    
               )
               ,'/ns19:vedtak'
               PASSING t.XML_CLOB
               COLUMNS 
                   FAGSAK_ID                 NUMBER(19,0)  PATH 'ns19:fagsakId'
                  ,BEHANDLINGS_ID            NUMBER(19,0)  PATH 'ns19:behandlingsresultat/ns19:behandlingsId'
                  ,UTTAKSRESULTATPERIODER   XMLTYPE        PATH 'ns19:behandlingsresultat/ns19:beregningsresultat/ns19:uttak/ns16:uttak/ns16:uttaksresultatPerioder'
            ) a,
            XMLTABLE
            (
               XMLNamespaces
               (
                              'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                             ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8 
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13  
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                             ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19     
               )
               ,'ns16:uttaksresultatPerioder'
               PASSING a.UTTAKSRESULTATPERIODER
               COLUMNS   
                 GRADERING_INNVILGET       VARCHAR2(50 CHAR)  PATH 'ns16:graderingInnvilget'
                ,PERIODE_RESULTAT_TYPE     VARCHAR2(100 CHAR) PATH 'ns16:periodeResultatType/@kode'
                ,PERIODE_RESULTAT_AARSAK   VARCHAR2(100 CHAR) PATH 'ns16:perioderesultataarsak/@kode'   
                ,FOM                       VARCHAR2(100 CHAR) PATH 'ns16:periode/ns6:fom'
                ,TOM                       VARCHAR2(100 CHAR) PATH 'ns16:periode/ns6:tom'
                ,SAMTIDIG_UTTAK            VARCHAR2(50 CHAR)  PATH 'ns16:samtidiguttak'
                ,UTTAK_UTSETTELSE_TYPE     VARCHAR2(100 CHAR) PATH 'ns16:uttakUtsettelseType/@kode'
                ,PERIODEAKTIVITETER        XMLTYPE            PATH 'ns16:uttaksresultatPeriodeAktiviteter'
            ) b,
            XMLTABLE
            (
               XMLNamespaces
               (                  
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8 
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19  
               ) ,'ns16:uttaksresultatPeriodeAktiviteter'
               PASSING b.PERIODEAKTIVITETER
               COLUMNS     
                 TREKKONTO              VARCHAR2(100 CHAR)  PATH 'ns16:trekkkonto/@kode' 
                ,TREKKDAGER             NUMBER(3,0)         PATH 'ns16:trekkdager'
                ,VIRKSOMHET             VARCHAR2(100 CHAR)  PATH 'ns16:virksomhet'
                ,ARBEIDSTIDSPROSENT     NUMBER(5,2)         PATH 'ns16:arbeidstidsprosent'
                ,UTBETALINGSPROSENT     NUMBER(5,2)         PATH 'ns16:utbetalingsprosent'
                ,UTTAK_ARBEID_TYPE      VARCHAR2(100 CHAR)  PATH 'ns16:uttakarbeidtype/@kode'
                ,GRADERING              VARCHAR2(10)        PATH 'ns16:gradering'
                ,GRADERINGSDAGER        NUMBER(3,0)         PATH 'ns16:graderingsdager'
            ) d
            where t.trans_id = rec_vedtak_fp.trans_id;
          exception
            when others then
              l_error_melding := sqlerrm;
              l_feil_kilde_navn := 'PL/SQL: FAM_FP_UTTAK_RES_PER_AKTIV';
              l_feil_trans_id := rec_vedtak_fp.trans_id;
              rollback to do_insert; continue;
          end;

          --FAM_FP_DELYTELSEID
          begin
            INSERT INTO FK_SENSITIV.FAM_FP_DELYTELSEID
            (
               TRANS_ID	        
              ,TRANS_TID	       
              ,VEDTAK_ID	       
              ,FUNKSJONELL_TID  
              ,FAGSAK_ID        
              ,BEHANDLINGS_ID   
              ,DATO_VEDTAK_FOM  
              ,DATO_VEDTAK_TOM  
              ,LINJE_ID         
              ,DELYTELSE_ID     
              ,REF_DELYTELSE_ID 
              ,UTBETALES_TIL_ID 
              ,REFUNDERES_ID    
              ,KODE_STATUS_LINJE
              ,DATO_STATUS_FOM  
              ,OPPDRAG_ID       
              ,FAGSYSTEM_ID     
              ,KILDESYSTEM      
              ,LASTET_DATO
            )
            SELECT        
                 t.TRANS_ID
                ,t.TRANS_TID
                ,t.VEDTAK_ID
                ,t.FUNKSJONELL_TID
                ,b.FAGSAK_ID
                ,b.BEHANDLINGS_ID
                ,to_date(a.DATO_VEDTAK_FOM,'YYYY-MM-DD')
                ,to_date(a.DATO_VEDTAK_TOM,'YYYY-MM-DD')
                ,a.LINJE_ID
                ,a.DELYTELSE_ID
                ,a.REF_DELYTELSE_ID
                ,a.UTBETALES_TIL_ID    
                ,a.REFUNDERES_ID
                ,a.KODE_STATUS_LINJE 
                ,to_date(a.DATO_STATUS_FOM,'YYYY-MM-DD')
                ,c.OPPDRAG_ID
                ,c.FAGSYSTEM_ID
                ,t.kildesystem
                ,t.lastet_dato
            FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t,            
            XMLTABLE
            (
               XMLNamespaces
               (
                           'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19   
               )
               ,'/ns19:vedtak'
               PASSING t.XML_CLOB
               COLUMNS
                 FAGSAK_ID                 NUMBER(19,0)  PATH 'ns19:fagsakId'
                ,BEHANDLINGS_ID            NUMBER(19,0)  PATH 'ns19:behandlingsresultat/ns19:behandlingsId'
                ,UTTAKSRESULTATPERIODER   XMLTYPE        PATH 'ns19:oppdrag'
            ) b,       
            XMLTABLE
            (
               XMLNamespaces
               (                    
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19

               )
               ,'ns19:oppdrag'
               PASSING b.UTTAKSRESULTATPERIODER
               COLUMNS      
                 OPPDRAG_ID          VARCHAR2(200) PATH 'ns13:oppdragId'
                ,FAGSYSTEM_ID        VARCHAR2(200) PATH 'ns13:fagsystemId'
                ,OPPDRAGSLINJE       XMLTYPE       PATH 'ns15:oppdragslinje'
            ) c,   
            XMLTABLE
            (
               XMLNamespaces
               (                     
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19

               )
               ,'ns15:oppdragslinje'
               PASSING c.OPPDRAGSLINJE
               COLUMNS
                 DATO_VEDTAK_FOM          VARCHAR2(100 CHAR) PATH  'ns15:periode/ns6:fom'
                ,DATO_VEDTAK_TOM          VARCHAR2(100 CHAR) PATH  'ns15:periode/ns6:tom'
                ,LINJE_ID                 NUMBER(19,0)       PATH  'ns15:linjeId'
                ,DELYTELSE_ID             NUMBER(19,0)       PATH  'ns15:delytelseId' 
                ,REF_DELYTELSE_ID         NUMBER(19,0)       PATH  'ns15:ref_delytelse_id'
                ,UTBETALES_TIL_ID         VARCHAR2(20 CHAR)  PATH  'ns15:utbetales_til_id' 
                ,REFUNDERES_ID            VARCHAR2(20 CHAR)  PATH  'ns15:refunderes_id'
                ,KODE_STATUS_LINJE        VARCHAR2(10 CHAR)  PATH  'ns15:kode_status_linje' 
                ,DATO_STATUS_FOM          VARCHAR2(100 CHAR) PATH  'ns15:status_fom'      
            ) a
            where t.trans_id = rec_vedtak_fp.trans_id;
          exception
            when others then
              l_error_melding := sqlerrm;
              l_feil_kilde_navn := 'PL/SQL: FAM_FP_DELYTELSEID';
              l_feil_trans_id := rec_vedtak_fp.trans_id;
              rollback to do_insert; continue;
          end;

          --FAM_FP_BEREG_GRUNNLAGPERIODE
          begin
            INSERT INTO FK_SENSITIV.FAM_FP_BEREG_GRUNNLAGPERIODE
            (
               TRANS_ID	          
              ,TRANS_TID	         
              ,VEDTAK_ID	         
              ,FUNKSJONELL_TID    
              ,FAGSAK_ID          
              ,BEHANDLINGS_ID     
              ,BG_PERIODE_FOM     
              ,BRUTTO_PR_AAR      
              ,AVKORTET_PR_AAR    
              ,REDUSERT_PR_AAR    
              ,DAGSATS            
              ,DEKNINGSGRAD       
              ,SKJAERINGSTIDSPUNKT
              ,KILDESYSTEM        
              ,LASTET_DATO
            )
            SELECT   t.TRANS_ID
                    ,t.TRANS_TID
                    ,t.VEDTAK_ID
                    ,t.FUNKSJONELL_TID
                    ,b.FAGSAK_ID
                    ,b.BEHANDLINGS_ID
                    ,to_date(q.BG_PERIODE_FOM,'YYYY-MM-DD')
                    ,q.BRUTTO_PR_AAR
                    ,q.AVKORTET_PR_AAR
                    ,q.REDUSERT_PR_AAR
                    ,q.DAGSATS
                    ,d.DEKNINGSGRAD
                    ,to_date(d.SKJAERINGSTIDSPUNKT,'YYYY-MM-DD')
                    ,t.kildesystem
                    ,t.lastet_dato
            FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t,
            XMLTABLE
            (
               XMLNamespaces
               (
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19   
               )
               ,'/ns19:vedtak'
               PASSING t.XML_CLOB
               COLUMNS
                 FAGSAK_ID                 NUMBER(19,0)  PATH 'ns19:fagsakId'
                ,BEHANDLINGS_ID            NUMBER(19,0)  PATH 'ns19:behandlingsresultat/ns19:behandlingsId'
                ,BEREGNINGSGRUNNLAG        XMLTYPE       PATH 'ns19:behandlingsresultat/ns19:beregningsresultat/ns19:beregningsgrunnlag/ns18:beregningsgrunnlag'
            ) b,
            XMLTABLE
            (
                  XMLNamespaces
                  (       
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19
                  )
                  ,'ns18:beregningsgrunnlag'
                  PASSING b.BEREGNINGSGRUNNLAG
                  COLUMNS
                    DEKNINGSGRAD                   NUMBER(19,0)       PATH 'ns18:dekningsgrad',
                    SKJAERINGSTIDSPUNKT            VARCHAR2(100 CHAR) PATH 'ns18:skjaeringstidspunkt'  
                   ,BEREGNINGSGRUNNLAGPERIODE      XMLTYPE            PATH 'ns18:beregningsgrunnlagPeriode'
            ) d,
            XMLTABLE
            (
               XMLNamespaces
               (
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19
               )
               ,'ns18:beregningsgrunnlagPeriode'
               PASSING d.BEREGNINGSGRUNNLAGPERIODE
               COLUMNS
                    BG_PERIODE_FOM      VARCHAR2(100 CHAR)  PATH 'ns18:periode/ns6:fom',
                    BRUTTO_PR_AAR       NUMBER(19,2)        PATH 'ns18:brutto',
                    AVKORTET_PR_AAR     NUMBER(19,2)        PATH 'ns18:avkortet',
                    REDUSERT_PR_AAR     NUMBER(19,2)        PATH 'ns18:redusert',
                    DAGSATS             VARCHAR2(100 CHAR)  PATH 'ns18:dagsats'

            ) q
            where t.trans_id = rec_vedtak_fp.trans_id;
          exception
            when others then
              l_error_melding := sqlerrm;
              l_feil_kilde_navn := 'PL/SQL: FAM_FP_BEREG_GRUNNLAGPERIODE';
              l_feil_trans_id := rec_vedtak_fp.trans_id;
              rollback to do_insert; continue;
          end;

          --FAM_FP_TILKJENTYTELSE
          begin
            INSERT INTO FK_SENSITIV.FAM_FP_TILKJENTYTELSE
            (
               TRANS_ID
              ,TRANS_TID
              ,VEDTAK_ID
              ,FUNKSJONELL_TID
              ,FAGSAK_ID
              ,BEHANDLINGS_ID
              ,BRUKERERMOTTAKER
              ,FOM
              ,TOM
              ,ORGNR
              ,ORGNAVN
              ,ARBEIDSFORHOLD_ID
              ,AKTIVITETSSTATUS
              ,INNTEKTSKATEGORI
              ,DAGSATS
              ,STILLINGSPROSENT
              ,UTBETALINGSGRAD
              ,KILDESYSTEM
              ,LASTET_DATO
            )
            SELECT 
                     t.TRANS_ID
                    ,t.TRANS_TID
                    ,t.VEDTAK_ID
                    ,t.FUNKSJONELL_TID
                    ,b.FAGSAK_ID
                    ,b.BEHANDLINGS_ID
                    ,q.BRUKERERMOTTAKER
                    ,to_date(q.FOM,'YYYY-MM-DD')
                    ,to_date(q.TOM,'YYYY-MM-DD')
                    ,q.ORGNR                     
                    ,q.ORGNAVN                   
                    ,q.ARBEIDSFORHOLD_ID          
                    ,q.AKTIVITETSSTATUS          
                    ,q.INNTEKTSKATEGORI          
                    ,q.DAGSATS
                    ,q.STILLINGSPROSENT
                    ,q.UTBETALINGSGRAD
                    ,t.kildesystem
                    ,t.lastet_dato
            FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t,
            XMLTABLE
            (
               XMLNamespaces
               (                       
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19   
               )
               ,'/ns19:vedtak'
               PASSING t.XML_CLOB
               COLUMNS        
                 FAGSAK_ID                 NUMBER(19,0)  PATH 'ns19:fagsakId'
                ,BEHANDLINGS_ID            NUMBER(19,0)  PATH 'ns19:behandlingsresultat/ns19:behandlingsId'
                ,BEREGNINGSRESULTAT        XMLTYPE       PATH 'ns19:behandlingsresultat/ns19:beregningsresultat/ns19:tilkjentYtelse/ns12:YtelseForeldrepenger/ns12:beregningsresultat'
            ) b,
            XMLTABLE
            (
               XMLNamespaces
               (
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19
               )
               ,'ns12:beregningsresultat'
               PASSING b.BEREGNINGSRESULTAT
               COLUMNS            
                     BRUKERERMOTTAKER         VARCHAR2(100 CHAR) PATH 'ns12:brukerErMottaker'
                    ,FOM                      VARCHAR2(100 CHAR) PATH 'ns12:periode/ns6:fom'
                    ,TOM                      VARCHAR2(100 CHAR) PATH 'ns12:periode/ns6:tom'
                    ,ORGNR                    VARCHAR2(100 CHAR) PATH 'ns12:virksomhet/ns12:orgnr'   
                    ,ORGNAVN                  VARCHAR2(100 CHAR) PATH 'ns12:virksomhet/ns12:navn'  
                    ,ARBEIDSFORHOLD_ID        VARCHAR2(100 CHAR) PATH 'ns12:virksomhet/ns12:arbeidsforholdid'
                    ,AKTIVITETSSTATUS         VARCHAR2(100 CHAR) PATH 'ns12:aktivitetstatus/@kode'
                    ,INNTEKTSKATEGORI         VARCHAR2(100 CHAR) PATH 'ns12:inntektskategori/@kode'
                    ,DAGSATS                  VARCHAR2(100 CHAR) PATH 'ns12:dagsats'
                    ,STILLINGSPROSENT         NUMBER(5,2)        PATH 'ns12:stillingsprosent'
                    ,UTBETALINGSGRAD          VARCHAR2(100 CHAR) PATH 'ns12:utbetalingsgrad'

            ) q
            where t.trans_id = rec_vedtak_fp.trans_id;
          exception
            when others then
              l_error_melding := sqlerrm;
              l_feil_kilde_navn := 'PL/SQL: FAM_FP_TILKJENTYTELSE';
              l_feil_trans_id := rec_vedtak_fp.trans_id;
              rollback to do_insert; continue;
          end;

          --FAM_FP_FODSELTERMIN
          begin
            INSERT INTO FK_SENSITIV.FAM_FP_FODSELTERMIN
            (
               TRANS_ID
              ,TRANS_TID
              ,VEDTAK_ID
              ,FUNKSJONELL_TID
              ,FAGSAK_ID
              ,BEHANDLINGS_ID
              ,ANTALL_BARN_FOEDSEL
              ,FOEDSELSDATO
              ,TERMINDATO
              ,UTSTEDT_DATO
              ,ANTALL_BARN_TERMIN
              ,FOEDSELSDATO_ADOPSJON
              ,EREKTEFELLES_BARN
              ,KILDESYSTEM
              ,LASTET_DATO
            )
            SELECT               
                     t.TRANS_ID
                    ,t.TRANS_TID
                    ,t.VEDTAK_ID
                    ,t.FUNKSJONELL_TID
                    ,q.FAGSAK_ID
                    ,q.BEHANDLINGS_ID
                    ,q.ANTALL_BARN_FOEDSEL
                    ,to_date(q.FOEDSELSDATO,'YYYY-MM-DD')
                    ,to_date(q.TERMINDATO,'YYYY-MM-DD')
                    ,to_date(q.UTSTEDTDATO,'YYYY-MM-DD')
                    ,q.ANTALL_BARN_TERMIN
                    ,to_date(a.FOEDSELSDATO_ADOPSJON,'YYYY-MM-DD')
                    ,q.EREKTEFELLES_BARN
                    ,t.kildesystem
                    ,t.lastet_dato
            FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t
            left join       
            XMLTABLE
            (
               XMLNamespaces
               (                    
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8 
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19
               )
               ,'/ns19:vedtak'
               PASSING t.XML_CLOB
               COLUMNS            
                 FAGSAK_ID                    NUMBER(19,0)         PATH './ns19:fagsakId'
                ,BEHANDLINGS_ID               NUMBER(19,0)         PATH './ns19:behandlingsresultat/ns19:behandlingsId'
                ,ANTALL_BARN_FOEDSEL          NUMBER(19,0)         PATH './ns19:personOpplysninger/ns4:PersonopplysningerDvhForeldrepenger/ns4:familiehendelse/ns4:foedsel/ns7:antallBarn'
                ,FOEDSELSDATO                 VARCHAR2(100 CHAR)   PATH './ns19:personOpplysninger/ns4:PersonopplysningerDvhForeldrepenger/ns4:familiehendelse/ns4:foedsel/ns7:foedselsdato'
                ,TERMINDATO                   VARCHAR2(100 CHAR)   PATH './ns19:personOpplysninger/ns4:PersonopplysningerDvhForeldrepenger/ns4:familiehendelse/ns4:terminbekreftelse/ns4:termindato'
                ,UTSTEDTDATO                  VARCHAR2(100 CHAR)   PATH './ns19:personOpplysninger/ns4:PersonopplysningerDvhForeldrepenger/ns4:familiehendelse/ns4:terminbekreftelse/ns4:utstedtDato'
                ,ANTALL_BARN_TERMIN           NUMBER(19,0)         PATH './ns19:personOpplysninger/ns4:PersonopplysningerDvhForeldrepenger/ns4:familiehendelse/ns4:terminbekreftelse/ns4:antallBarn'
                ,EREKTEFELLES_BARN            VARCHAR2(100 CHAR)   PATH './ns19:personOpplysninger/ns4:PersonopplysningerDvhForeldrepenger/ns4:familiehendelse/ns4:adopsjon/ns4:erEktefellesBarn'
                ,ADOPSBARN                    XMLTYPE              PATH 'ns19:personOpplysninger/ns4:PersonopplysningerDvhForeldrepenger/ns4:familiehendelse/ns4:adopsjon/ns4:adopsjonsbarn'
                ,FOEDSEL                      XMLTYPE              PATH 'ns19:personOpplysninger/ns4:PersonopplysningerDvhForeldrepenger/ns4:familiehendelse/ns4:foedsel'
            ) q
            on 1 = 1
            left join
            xmltable
            (
               XMLNamespaces
               (                         
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8 
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19  
               )
               ,'ns4:adopsjonsbarn'
               PASSING q.ADOPSBARN
               COLUMNS        
                FOEDSELSDATO_ADOPSJON        VARCHAR2(100 CHAR)   PATH 'ns4:foedselsdato'

            ) a
            on 1 = 1
            where t.trans_id = rec_vedtak_fp.trans_id;
          exception
            when others then
              l_error_melding := sqlerrm;
              l_feil_kilde_navn := 'PL/SQL: FAM_FP_FODSELTERMIN';
              l_feil_trans_id := rec_vedtak_fp.trans_id;
              rollback to do_insert; continue;
          end;

          --FAM_FP_VILKAAR
          begin
            INSERT INTO FK_SENSITIV.FAM_FP_VILKAAR
            (
               TRANS_ID
              ,TRANS_TID	                    
              ,VEDTAK_ID	                    
              ,FUNKSJONELL_TID               
              ,FAGSAK_ID                     
              ,BEHANDLINGS_ID                
              ,EKTEFELLES_BARN               
              ,SOEKERS_KJOEN                 
              ,MANN_ADOPTERER_ALENE          
              ,OMSORGS_OVERTAKELSESDATO      
              ,PERSON_STATUS                 
              ,ER_BRUKER_MEDLEM              
              ,ER_BRUKER_BOSATT              
              ,HAR_OPPHOLDSRETT              
              ,HAR_LOVLIGOPPHOLD_I_NORGE     
              ,ER_NORDISK_STATSBORGER        
              ,ER_BORGER_AV_EU_EOS           
              ,PLIKTIG_ELLER_FRIVILLIG_MEDLEM
              ,ELEKTRONISK_SOEKNAD           
              ,SKJAERINGS_TIDSPUNKT          
              ,SOEKNAD_MOTTAT_DATO           
              ,BEHANDLINGS_DATO              
              ,FOM                           
              ,TOM                           
              ,MAKS_MELLOM_LIGGENDE_PERIODE  
              ,MIN_MELLOM_LIGGENDE_PERIODE   
              ,MIN_STEANTALLDAGER_FORVENT    
              ,MIN_STEANTALLDAGER_GODKJENT   
              ,MIN_STEANTALLMANEDER_GODKJENT 
              ,MIN_STEINNTEKT                
              ,PERIODE_ANTATT_GODKJENT       
              ,KILDESYSTEM                   
              ,LASTET_DATO
            )
            SELECT   t.TRANS_ID
                    ,t.TRANS_TID
                    ,t.VEDTAK_ID
                    ,t.FUNKSJONELL_TID
                    ,b.FAGSAK_ID
                    ,b.BEHANDLINGS_ID
                    ,q.EKTEFELLES_BARN
                    ,q.SOEKERS_KJOEN
                    ,q.MANN_ADOPTERER_ALENE
                    ,to_date(q.OMSORGS_OVERTAKELSESDATO,'YYYY-MM-DD')
                    ,q.PERSON_STATUS                        
                    ,q.ER_BRUKER_MEDLEM                        
                    ,q.ER_BRUKER_BOSATT                       
                    ,q.HAR_OPPHOLDSRETT                 
                    ,q.HAR_LOVLIGOPPHOLD_I_NORGE          
                    ,q.ER_NORDISK_STATSBORGER            
                    ,q.ER_BORGER_AV_EU_EOS                
                    ,q.PLIKTIG_ELLER_FRIVILLIG_MEDLEM               
                    ,q.ELEKTRONISK_SOEKNAD   
                    ,to_date(q.SKJAERINGS_TIDSPUNKT,'YYYY-MM-DD')
                    ,to_date(q.SOEKNAD_MOTTAT_DATO,'YYYY-MM-DD')
                   ,to_date(q.BEHANDLINGS_DATO,'YYYY-MM-DD')
                   ,to_date(q.FOM,'YYYY-MM-DD')
                   ,to_date(q.TOM,'YYYY-MM-DD')
                   ,q.MAKS_MELLOM_LIGGENDE_PERIODE    
                   ,q.MIN_MELLOM_LIGGENDE_PERIODE     
                   ,q.MIN_STEANTALLDAGER_FORVENT    
                   ,q.MIN_STEANTALLDAGER_GODKJENT    
                   ,q.MIN_STEANTALLMANEDER_GODKJENT  
                   ,q.MIN_STEINNTEKT                
                   ,q.PERIODE_ANTATT_GODKJENT
                   ,t.kildesystem
                   ,t.lastet_dato
            FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t,
            XMLTABLE
            (
               XMLNamespaces
               (                         
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19   
               )
               ,'/ns19:vedtak'
               PASSING t.XML_CLOB
               COLUMNS            
                 FAGSAK_ID                 NUMBER(19,0)  PATH 'ns19:fagsakId'
                ,BEHANDLINGS_ID            NUMBER(19,0)  PATH 'ns19:behandlingsresultat/ns19:behandlingsId'
                ,VILKAARSGRUNNLAG          XMLTYPE       PATH './ns19:behandlingsresultat/ns19:vurderteVilkaar/ns19:vilkaar/ns19:vilkaarsgrunnlag/ns8:vilkaarsgrunnlag'
            ) b,
            XMLTABLE
            (
               XMLNamespaces
               (                     
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19
               )
               ,'ns8:vilkaarsgrunnlag'
               PASSING b.VILKAARSGRUNNLAG  
               COLUMNS      
                    EKTEFELLES_BARN                  VARCHAR2(100 CHAR) PATH 'ns10:ektefellesBarn',
                    SOEKERS_KJOEN                    VARCHAR2(100 CHAR) PATH 'ns10:soekersKjoenn/@kode',
                    MANN_ADOPTERER_ALENE             VARCHAR2(100 CHAR) PATH 'ns10:mannAdoptererAlene',
                    OMSORGS_OVERTAKELSESDATO         VARCHAR2(100 CHAR) PATH 'ns10:omsorgsovertakelsesdato',
                    PERSON_STATUS                    VARCHAR2(100 CHAR) PATH 'ns8:personstatus',
                    ER_BRUKER_MEDLEM                 VARCHAR2(100 CHAR) PATH 'ns8:erBrukerMedlem',
                    ER_BRUKER_BOSATT                 VARCHAR2(100 CHAR) PATH 'ns8:erBrukerBosatt',
                    HAR_OPPHOLDSRETT                 VARCHAR2(100 CHAR) PATH 'ns8:harBrukerOppholdsrett',
                    HAR_LOVLIGOPPHOLD_I_NORGE        VARCHAR2(100 CHAR) PATH 'ns8:harBrukerLovligOppholdINorge',
                    ER_NORDISK_STATSBORGER           VARCHAR2(100 CHAR) PATH 'ns8:erBrukerNordiskstatsborger',
                    ER_BORGER_AV_EU_EOS              VARCHAR2(100 CHAR) PATH 'ns8:erBrukerBorgerAvEUEOS',
                    PLIKTIG_ELLER_FRIVILLIG_MEDLEM   VARCHAR2(100 CHAR) PATH 'ns8:erBrukerPliktigEllerFrivilligMedlem',
                    ELEKTRONISK_SOEKNAD              VARCHAR2(100 CHAR) PATH 'ns10:elektroniskSoeknad',
                    SKJAERINGS_TIDSPUNKT             VARCHAR2(100 CHAR) PATH 'ns10:skjaeringstidspunkt',
                    SOEKNAD_MOTTAT_DATO              VARCHAR2(100 CHAR) PATH 'ns10:soeknadMottatDato',
                    BEHANDLINGS_DATO                 VARCHAR2(100 CHAR) PATH 'ns10:behandlingsDato',
                    FOM                              VARCHAR2(100 CHAR) PATH 'ns10:opptjeningperiode/ns6:fom',
                    TOM                              VARCHAR2(100 CHAR) PATH 'ns10:opptjeningperiode/ns6:tom',
                    MAKS_MELLOM_LIGGENDE_PERIODE     VARCHAR2(100 CHAR) PATH 'ns10:maksMellomliggendePeriodeForArbeidsforhold',
                    MIN_MELLOM_LIGGENDE_PERIODE      VARCHAR2(100 CHAR) PATH 'ns10:minForegaaendeForMellomliggendePeriodeForArbeidsforhold',
                    MIN_STEANTALLDAGER_FORVENT       VARCHAR2(100 CHAR) PATH 'ns10:minsteAntallDagerForVent',
                    MIN_STEANTALLDAGER_GODKJENT      VARCHAR2(100 CHAR) PATH 'ns10:minsteAntallDagerGodkjent',
                    MIN_STEANTALLMANEDER_GODKJENT    VARCHAR2(100 CHAR) PATH 'ns10:minsteAntallMånederGodkjent',
                    MIN_STEINNTEKT                   VARCHAR2(100 CHAR) PATH 'ns10:minsteInntekt',
                    PERIODE_ANTATT_GODKJENT          VARCHAR2(100 CHAR) PATH 'ns10:periodeAntattGodkjentForBehandlingstidspunkt'

            ) q
            where t.trans_id = rec_vedtak_fp.trans_id;
          exception
            when others then
              l_error_melding := sqlerrm;
              l_feil_kilde_navn := 'PL/SQL: FAM_FP_VILKAAR';
              l_feil_trans_id := rec_vedtak_fp.trans_id;
              rollback to do_insert; continue;
          end;

          --FAM_FP_UTTAK_FORDELINGSPER
          begin
            INSERT INTO FK_SENSITIV.FAM_FP_UTTAK_FORDELINGSPER
            (
               TRANS_ID	      
              ,TRANS_TID	     
              ,VEDTAK_ID	     
              ,FUNKSJONELL_TID
              ,FAGSAK_ID      
              ,BEHANDLINGS_ID 
              ,FOM            
              ,TOM            
              ,PERIODE_TYPE   
              ,MORS_AKTIVITET 
              ,KILDESYSTEM    
              ,LASTET_DATO
            )
            SELECT   t.TRANS_ID
                    ,t.TRANS_TID
                    ,t.VEDTAK_ID
                    ,t.FUNKSJONELL_TID
                    ,b.FAGSAK_ID
                    ,b.BEHANDLINGS_ID 
                    ,to_date(q.FOM,'YYYY-MM-DD')
                    ,to_date(q.TOM,'YYYY-MM-DD')
                    ,q.PERIODE_TYPE                
                    ,q.MORS_AKTIVITET
                    ,t.kildesystem
                    ,t.lastet_dato
            FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t,          
            XMLTABLE
            (
               XMLNamespaces
               (                          
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19   
               )
               ,'/ns19:vedtak'
               PASSING t.XML_CLOB
               COLUMNS        
                 FAGSAK_ID                 NUMBER(19,0)  PATH 'ns19:fagsakId'
                ,BEHANDLINGS_ID            NUMBER(19,0)  PATH 'ns19:behandlingsresultat/ns19:behandlingsId'
                ,FORDELINGPERIODE          XMLTYPE       PATH 'ns19:behandlingsresultat/ns19:beregningsresultat/ns19:uttak/ns16:uttak/ns16:fordelingPerioder/ns16:fordelingPeriode'
            ) b,
            XMLTABLE
            (
               XMLNamespaces
               (                     
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19
               )
               ,'ns16:fordelingPeriode'
               PASSING b.FORDELINGPERIODE
               COLUMNS            
                    FOM                         VARCHAR2(100 CHAR) PATH 'ns16:periode/ns6:fom',
                    TOM                         VARCHAR2(100 CHAR) PATH 'ns16:periode/ns6:tom',
                    PERIODE_TYPE                VARCHAR2(100 CHAR) PATH 'ns16:periodetype/@kode',
                    MORS_AKTIVITET              VARCHAR2(100 CHAR) PATH 'ns16:morsAktivitet/@kode'

            ) q
            where t.trans_id = rec_vedtak_fp.trans_id;
          exception
            when others then
              l_error_melding := sqlerrm;
              l_feil_kilde_navn := 'PL/SQL: FAM_FP_UTTAK_FORDELINGSPER';
              l_feil_trans_id := rec_vedtak_fp.trans_id;
              rollback to do_insert; continue;
          end;

          --FAM_FP_BEREGNINGSGRUNNLAG
          begin
            INSERT INTO FK_SENSITIV.FAM_FP_BEREGNINGSGRUNNLAG
            (
               TRANS_ID	                    
              ,TRANS_TID	                   
              ,VEDTAK_ID	                   
              ,FUNKSJONELL_TID              
              ,FAGSAK_ID                    
              ,BEHANDLINGS_ID               
              ,FOM                          
              ,TOM                          
              ,BRUTTO                       
              ,AVKORTET                     
              ,REDUSERT                     
              ,DAGSATS                      
              ,DEKNINGSGRAD                 
              ,SKJARINGSTIDSPUNKT           
              ,STATUS_OG_ANDEL_FOM          
              ,STATUS_OG_ANDEL_TOM          
              ,AKTIVITET_STATUS             
              ,VIRKSOMHETSNUMMER            
              ,STATUS_OG_ANDEL_BRUTTO       
              ,STATUS_OG_ANDEL_AVKORTET     
              ,STATUS_OG_ANDEL_REDUSERT     
              ,STATUS_OG_ANDEL_BEREGNET     
              ,STATUS_OG_ANDEL_INNTEKTSKAT  
              ,NATURALYTELSEBORTFALL        
              ,TIL_STOETENDEYTELSE_TYPE     
              ,TIL_STOETENDEYTELSE          
              ,AVKORTET_BRUKERS_ANDEL       
              ,REDUSERT_BRUKERS_ANDEL       
              ,DAGSATS_BRUKER               
              ,DAGSATS_ARBEIDSGIVER         
              ,REFUSJON_MAKSIMAL            
              ,REFUSJON_AVKORTET            
              ,REFUSJON_REDUSERT            
              ,KILDESYSTEM                  
              ,LASTET_DATO
            )
            SELECT   t.TRANS_ID
                    ,t.TRANS_TID
                    ,t.VEDTAK_ID
                    ,t.FUNKSJONELL_TID
                    ,b.FAGSAK_ID
                    ,b.BEHANDLINGS_ID
                    ,to_date(q.FOM,'YYYY-MM-DD')
                    ,to_date(q.TOM,'YYYY-MM-DD')
                    ,q.BRUTTO
                    ,q.AVKORTET
                    ,q.REDUSERT
                    ,q.DAGSATS
                    ,d.DEKNINGSGRAD
                    ,to_date(d.SKJAERINGSTIDSPUNKT,'YYYY-MM-DD')
                    ,to_date(c.STATUS_OG_ANDEL_FOM,'YYYY-MM-DD')
                    ,to_date(c.STATUS_OG_ANDEL_TOM,'YYYY-MM-DD')   
                    ,c.AKTIVITET_STATUS
                    ,c.VIRKSOMHETSNUMMER
                    ,c.STATUS_OG_ANDEL_BRUTTO
                    ,c.STATUS_OG_ANDEL_AVKORTET
                    ,c.STATUS_OG_ANDEL_REDUSERT
                    ,c.STATUS_OG_ANDEL_BEREGNET
                    ,c.STATUS_OG_ANDEL_INNTEKTSKAT     
                    ,c.NATURALYTELSEBORTFALL
                    ,c.TIL_STOETENDEYTELSE_TYPE
                    ,c.TIL_STOETENDEYTELSE
                    ,c.AVKORTET_BRUKERS_ANDEL
                    ,c.REDUSERT_BRUKERS_ANDEL
                    ,c.DAGSATS_BRUKER
                    ,c.DAGSATS_ARBEIDSGIVER
                    ,c.REFUSJON_MAKSIMAL
                    ,c.REFUSJON_AVKORTET
                    ,c.REFUSJON_REDUSERT
                    ,t.kildesystem
                    ,t.lastet_dato
            FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t,
            XMLTABLE
            (
               XMLNamespaces
               (                          
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19   
               )
               ,'/ns19:vedtak'
               PASSING t.XML_CLOB
               COLUMNS        
                 FAGSAK_ID                 NUMBER(19,0)  PATH 'ns19:fagsakId'
                ,BEHANDLINGS_ID            NUMBER(19,0)  PATH 'ns19:behandlingsresultat/ns19:behandlingsId'
                ,BEREGNINGSGRUNNLAG        XMLTYPE       PATH 'ns19:behandlingsresultat/ns19:beregningsresultat/ns19:beregningsgrunnlag/ns18:beregningsgrunnlag'

            ) b,
            XMLTABLE
            (
               XMLNamespaces
               (                     
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19
               )
               ,'ns18:beregningsgrunnlag'
               PASSING b.BEREGNINGSGRUNNLAG
               COLUMNS   
                    DEKNINGSGRAD                   NUMBER(3,0)        PATH 'ns18:dekningsgrad',
                    SKJAERINGSTIDSPUNKT            VARCHAR2(100 CHAR) PATH 'ns18:skjaeringstidspunkt'  
                   ,BEREGNINGSGRUNNLAGPERIODE      XMLTYPE            PATH 'ns18:beregningsgrunnlagPeriode'
            ) d,
            XMLTABLE
            (
               XMLNamespaces
               (                     
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19
               )
               ,'ns18:beregningsgrunnlagPeriode'
               PASSING d.BEREGNINGSGRUNNLAGPERIODE
               COLUMNS            
                    FOM                   VARCHAR2(100 CHAR) PATH 'ns18:periode/ns6:fom',
                    TOM                   VARCHAR2(100 CHAR) PATH 'ns18:periode/ns6:tom',
                    BRUTTO                NUMBER(19,2)       PATH 'ns18:brutto',
                    AVKORTET              NUMBER(19,2)       PATH 'ns18:avkortet',
                    REDUSERT              NUMBER(19,2)       PATH 'ns18:redusert',
                    DAGSATS               VARCHAR2(100 CHAR) PATH 'ns18:dagsats',
                    xyz XMLTYPE           PATH 'ns18:beregningsgrunnlagPrStatusOgAndel'

            ) q,
            XMLTABLE
            (
               XMLNamespaces
               (                     
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19
               )
               ,'ns18:beregningsgrunnlagPrStatusOgAndel'
               PASSING q.xyz
               COLUMNS            
                    STATUS_OG_ANDEL_FOM         VARCHAR2(100 CHAR) PATH 'ns18:periode/ns6:fom',
                    STATUS_OG_ANDEL_TOM         VARCHAR2(100 CHAR) PATH 'ns18:periode/ns6:tom',
                    AKTIVITET_STATUS            VARCHAR2(100 CHAR) PATH 'ns18:aktivitetstatus/@kode',
                    VIRKSOMHETSNUMMER           VARCHAR2(100 CHAR) PATH 'ns18:virksomhetsnummer',
                    STATUS_OG_ANDEL_BRUTTO      NUMBER(19,2)       PATH 'ns18:brutto', 
                    STATUS_OG_ANDEL_AVKORTET    NUMBER(19,2)       PATH 'ns18:avkortet', 
                    STATUS_OG_ANDEL_REDUSERT    NUMBER(19,2)       PATH 'ns18:redusert',    
                    STATUS_OG_ANDEL_BEREGNET    NUMBER(19,2)       PATH 'ns18:beregnet', 
                    STATUS_OG_ANDEL_INNTEKTSKAT VARCHAR2(100 CHAR) PATH 'ns18:inntektskategori/@kode',       
                    NATURALYTELSEBORTFALL       NUMBER(19,2)       PATH 'ns18:naturalytelseBortfall',
                    TIL_STOETENDEYTELSE_TYPE    VARCHAR2(100 CHAR) PATH 'ns18:tilstoetendeYtelseType',
                    TIL_STOETENDEYTELSE         NUMBER(19,2)       PATH 'ns18:tilstoetendeYtelse',
                    AVKORTET_BRUKERS_ANDEL      NUMBER(19,2)       PATH 'ns18:avkortetBrukersAndel',
                    REDUSERT_BRUKERS_ANDEL      NUMBER(19,2)       PATH 'ns18:redusertBrukersAndel',
                    DAGSATS_BRUKER              VARCHAR2(100 CHAR) PATH 'ns18:dagsatsBruker',
                    DAGSATS_ARBEIDSGIVER        VARCHAR2(100 CHAR) PATH 'ns18:dagsatsArbeidsgiver',               
                    REFUSJON_MAKSIMAL           NUMBER(19,2)       PATH 'ns18:refusjonTilArbeidsgiver/ns18:maksimal', 
                    REFUSJON_AVKORTET           NUMBER(19,2)       PATH 'ns18:refusjonTilArbeidsgiver/ns18:avkortet', 
                    REFUSJON_REDUSERT           NUMBER(19,2)       PATH 'ns18:refusjonTilArbeidsgiver/ns18:redusert'              
            ) c
            where t.trans_id = rec_vedtak_fp.trans_id;
          exception
            when others then
              l_error_melding := sqlerrm;
              l_feil_kilde_navn := 'PL/SQL: FAM_FP_BEREGNINGSGRUNNLAG';
              l_feil_trans_id := rec_vedtak_fp.trans_id;
              rollback to do_insert; continue;
          end;

          --FAM_FP_DOKUMENTASJONSPERIODER
          begin
            INSERT INTO FK_SENSITIV.FAM_FP_DOKUMENTASJONSPERIODER(
                   TRANS_ID
                  ,TRANS_TID
                  ,VEDTAK_ID
                  ,FUNKSJONELL_TID
                  ,FAGSAK_ID
                  ,BEHANDLINGS_ID
                  ,FOM
                  ,TOM
                  ,DOKUMENTASJON_TYPE
                  ,kildesystem
                  ,lastet_dato)
            SELECT 
              t.TRANS_ID
             ,t.TRANS_TID
             ,t.VEDTAK_ID
             ,t.FUNKSJONELL_TID
             ,q.FAGSAK_ID
             ,q.BEHANDLINGS_ID
             ,to_date(d.FOM,'YYYY-MM-DD')
             ,to_date(d.TOM,'YYYY-MM-DD')
             ,d.DOKUMENTASJON_TYPE
             ,t.kildesystem
             ,t.lastet_dato
            FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t,
            XMLTABLE
            (
               XMLNamespaces
               (       
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19
               )
               ,'/ns19:vedtak'
               PASSING t.XML_CLOB
               COLUMNS            
                 FAGSAK_ID                    NUMBER(19,0)  PATH './ns19:fagsakId'    
                ,BEHANDLINGS_ID               NUMBER(19,0)  PATH './ns19:behandlingsresultat/ns19:behandlingsId'
                ,DOKUMENTASJONPERIODE         XMLTYPE       PATH './ns19:personOpplysninger/ns4:PersonopplysningerDvhForeldrepenger/ns4:dokumentasjonsperioder/ns4:dokumentasjonperiode'
            ) q,
            XMLTABLE
            (
               XMLNamespaces
               (       
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19
               )
               ,'ns4:dokumentasjonperiode'
               PASSING q.DOKUMENTASJONPERIODE
               COLUMNS 
                        FOM                          VARCHAR2(100 CHAR) PATH 'ns4:periode/ns6:fom'
                       ,TOM                          VARCHAR2(100 CHAR) PATH 'ns4:periode/ns6:tom'
                       ,DOKUMENTASJON_TYPE           VARCHAR2(100 CHAR) PATH 'ns4:dokumentasjontype/@kode' 
            ) d
            where t.trans_id = rec_vedtak_fp.trans_id;
          exception
            when others then
              l_error_melding := sqlerrm;
              l_feil_kilde_navn := 'PL/SQL: FAM_FP_DOKUMENTASJONSPERIODER';
              l_feil_trans_id := rec_vedtak_fp.trans_id;
              rollback to do_insert; continue;
          end;

          --FAM_FP_AKTIVITETSTATUS
          begin
            INSERT INTO  FK_SENSITIV.FAM_FP_AKTIVITETSTATUS
            (
               TRANS_ID	       
              ,TRANS_TID	      
              ,VEDTAK_ID	      
              ,FUNKSJONELL_TID 
              ,FAGSAK_ID       
              ,BEHANDLINGS_ID  
              ,AKTIVITET_STATUS
              ,HJEMMEL         
              ,KILDESYSTEM     
              ,LASTET_DATO
            )
            SELECT   t.TRANS_ID
                    ,t.TRANS_TID
                    ,t.VEDTAK_ID
                    ,t.FUNKSJONELL_TID
                    ,q.FAGSAK_ID
                    ,q.BEHANDLINGS_ID
                    ,b.AKTIVITET_STATUS                  
                    ,b.HJEMMEL
                    ,t.kildesystem
                    ,t.lastet_dato
            FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t,       
            XMLTABLE
            (
               XMLNamespaces
               (
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19

               )
               ,'/ns19:vedtak'
               PASSING t.XML_CLOB
               COLUMNS       
                 FAGSAK_ID               NUMBER(19,0)  PATH './ns19:fagsakId' 
                ,BEHANDLINGS_ID          NUMBER(19,0)  PATH './ns19:behandlingsresultat/ns19:behandlingsId'
                ,AKTIVITETSTATUSER       XMLTYPE       PATH './ns19:behandlingsresultat/ns19:beregningsresultat/ns19:beregningsgrunnlag/ns18:beregningsgrunnlag/ns18:aktivitetstatuser'

            ) q,
            XMLTABLE
            (
               XMLNamespaces
               (       
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'      AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'      AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'             AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'               AS "ns8" --8  
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'            AS "ns10"--10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                      AS "ns11"--11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                      AS "ns12"--12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                        AS "ns13"--13   
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                 AS "ns14"--14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                 AS "ns15"--15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                       AS "ns16"--16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'          AS "ns17"--17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'          AS "ns18"--18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                AS "ns19"--19    
               )
               ,'ns18:aktivitetstatuser'
               PASSING q.AKTIVITETSTATUSER
               COLUMNS       
                    AKTIVITET_STATUS           VARCHAR2(100 CHAR) PATH 'ns18:aktivitetStatus/@kode',
                    HJEMMEL                    VARCHAR2(100 CHAR) PATH 'ns18:hjemmel/@kode'               
            ) b
            where t.trans_id = rec_vedtak_fp.trans_id;
          exception
            when others then
              l_error_melding := sqlerrm;
              l_feil_kilde_navn := 'PL/SQL: FAM_FP_AKTIVITETSTATUS';
              l_feil_trans_id := rec_vedtak_fp.trans_id;
              rollback to do_insert; continue;
          end;

          l_commit := l_commit + 1;--Ett commit inntil alle tabellene har fått inn data lyktes
          if l_commit >= 10000 then
            commit;
            l_commit := 0;
          end if;
         --dbms_output.put_line(l_create); -- Debug

        exception
          when others then
            --Fortsett med neste rad
            l_error_melding := sqlcode || ' ' || sqlerrm;          
            insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
            values(null, rec_vedtak_fp.trans_id, l_error_melding, sysdate, 'FP_DVH_XML_UTBRETT');
        end;
      end loop;
      if l_error_melding is not null then
        insert into fk_sensitiv.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
        values(l_feil_trans_id, l_error_melding, sysdate, l_feil_kilde_navn);
      end if;
      commit;--commit til slutt
    end if;
  exception
    when others then
      rollback;
      p_error_melding := sqlcode || ' ' || sqlerrm;
  END FP_DVH_XML_UTBRETT;

  --****************************************************************************************************
  -- NAME:     SP_DVH_XML_UTBRETT
  -- PURPOSE:  Parse xml som ligger i forkammer.
  --
  -- REVISION:
  -- Version  Date           Author                                 Description
  -- 0.1      23.10.2019     Helen Rong og Sohaib Khan              Utpakke SP xml som ligger i felles tabellen
  --                                                                 fk_sensitiv.fam_fp_vedtak_utbetaling.
  --****************************************************************************************************
  procedure SP_DVH_XML_UTBRETT(dummy in varchar2, p_error_melding out varchar2) as
    l_commit number := 0;
    l_error_melding varchar2(1000) := null;
    l_feil_kilde_navn varchar2(50) := null;
    l_feil_trans_id number;
    cursor cur_vedtak_sp is
      select vud.trans_id
      from fk_sensitiv.fam_fp_vedtak_utbetaling vud
      left outer join fk_sensitiv.fam_sp_fagsak fed
      on vud.trans_id = fed.trans_id
      where vud.fagsak_type = 'SVP'--Svangeskapspenger
      --and rownum <= 1000--Test!!!
      and fed.trans_id is null;
  BEGIN
    --Slett den gamle versjonen av vedtak før den siste versjonen blir lastet inn.
    SLETT_GAMLE_VEDTAK('SVP',l_error_melding);
    if l_error_melding is null then
      --Parse xml en og en.
      --Ikke stopp om det feiler tilfeldigvis
      for rec_vedtak_sp in cur_vedtak_sp loop
        begin
          if l_error_melding is not null then
            insert into fk_sensitiv.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
            values(l_feil_trans_id, l_error_melding, sysdate, l_feil_kilde_navn);
            l_commit := l_commit +1;
            l_error_melding := null;
            l_feil_kilde_navn := null;
            l_feil_trans_id := null;
          end if;
          savepoint do_insert;
          --Insert utbrettede data
          --SP_FAGSAK
          begin
            INSERT INTO FK_SENSITIV.FAM_SP_FAGSAK
            (
              TRANS_ID	               
             ,TRANS_TID	              
             ,VEDTAK_ID	              
             ,FUNKSJONELL_TID         
             ,FAGSAK_ID               
             ,BEHANDLINGS_ID          
             ,FAGSAKANNENFORELDER_ID  
             ,FAGSAK_TYPE             
             ,TEMA                    
             ,TEMA_KODEVERK           
             ,BEHANDLINGSTEMA         
             ,BEHANDLINGSTEMA_KODEVERK
             ,SOEKNADSDATO            
             ,VEDTAKSDATO             
             ,BEHANDLENDEENHET        
             ,VEDTAKSRESULTAT         
             ,BEHANDLINGSRESULTAT     
             ,BEHANDLINGSTYPE         
             ,KILDESYSTEM             
             ,LASTET_DATO
             ,VERSJON
            )
            SELECT
              t.TRANS_ID	                
             ,t.TRANS_TID	               
             ,t.VEDTAK_ID	                
             ,t.FUNKSJONELL_TID              
             ,q.FAGSAK_ID                   
             ,q.BEHANDLINGS_ID               
             ,q.FAGSAKANNENFORELDER_ID             
             ,q.FAGSAK_TYPE                  
             ,q.TEMA                       
             ,q.TEMA_KODEVERK               
             ,q.BEHANDLINGSTEMA             
             ,q.BEHANDLINGSTEMA_KODEVERK    
             ,to_date(q.SOEKNADSDATO,'YYYY-MM-DD') SOEKNADSDATO                
             ,to_date(q.VEDTAKSDATO,'YYYY-MM-DD')  VEDTAKSDATO                 
             ,q.BEHANDLENDEENHET            
             ,q.VEDTAKSRESULTAT             
             ,q.BEHANDLINGSRESULTAT         
             ,q.BEHANDLINGSTYPE            
             ,t.KILDESYSTEM                
             ,t.LASTET_DATO
             ,t.VERSJON
            FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t,  
                 XMLTABLE
                 (
                  XMLNamespaces(
                               'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                              ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                              ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                              ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                              ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                              ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                              ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                              ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                              ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                              ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                              ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                              ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                              ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                              ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                              ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                              ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                              ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                              ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                              ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                              ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                              ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                              ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                              ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24
                    )
                    ,'/ns13:vedtak'
                    PASSING t.XML_CLOB
                    COLUMNS     
                    FAGSAK_ID                 NUMBER (19,0) PATH './ns13:fagsakId'  
                   ,BEHANDLINGS_ID             NUMBER(19,0) PATH './ns13:behandlingsresultat/ns13:behandlingsId'  
                   ,FAGSAKANNENFORELDER_ID    VARCHAR2(200) PATH './ns13:fagsakAnnenForelderId'  --Finnes ikke i XML         
                   ,FAGSAK_TYPE                  VARCHAR2 (100 CHAR)  PATH './ns13:fagsakType' 
                   ,TEMA                       VARCHAR2(200) PATH './ns13:tema' 
                   ,TEMA_KODEVERK               VARCHAR2(200) PATH './ns13:tema/@kodeverk' 
                   ,BEHANDLINGSTEMA             VARCHAR2(200) PATH './ns13:behandlingsTema'
                   ,BEHANDLINGSTEMA_KODEVERK    VARCHAR2(200) PATH './ns13:behandlingsTema/@kodeverk'
                   ,SOEKNADSDATO                VARCHAR2(200) PATH './ns13:soeknadsdato'
                   ,VEDTAKSDATO                 VARCHAR2(200) PATH './ns13:vedtaksdato' 
                   ,BEHANDLENDEENHET            VARCHAR2(200) PATH './ns13:behandlendeEnhet' 
                   ,VEDTAKSRESULTAT             VARCHAR2(200) PATH './ns13:vedtaksresultat' 
                   ,BEHANDLINGSRESULTAT         VARCHAR2(200) PATH './ns13:behandlingsresultat/ns13:behandlingsresultat'
                   ,BEHANDLINGSTYPE            VARCHAR2(200) PATH './ns13:behandlingsresultat/ns13:behandlingstype'
                 ) q
            where t.trans_id = rec_vedtak_sp.trans_id;
          exception
            when others then
              l_error_melding := sqlerrm;
              l_feil_kilde_navn := 'PL/SQL: FAM_SP_FAGSAK';
              l_feil_trans_id := rec_vedtak_sp.trans_id;
              rollback to do_insert; continue;
          end;

          --FAM_SP_ADRESSE
          begin
          INSERT INTO FK_SENSITIV.FAM_SP_ADRESSE 
          (         
            TRANS_ID	      
           ,TRANS_TID
           ,VEDTAK_ID	     
           ,FUNKSJONELL_TID
           ,FAGSAK_ID      
           ,BEHANDLINGS_ID 
           ,ADRESSE_TYPE   
           ,POSTNUMMER     
           ,LAND           
           ,KILDESYSTEM    
           ,LASTET_DATO 
          )
          SELECT
            t.TRANS_ID	      
           ,t.TRANS_TID
           ,t.VEDTAK_ID	     
           ,t.FUNKSJONELL_TID
           ,q.FAGSAK_ID      
           ,q.BEHANDLINGS_ID 
           ,b.ADRESSE_TYPE   
           ,b.POSTNUMMER     
           ,b.LAND           
           ,t.KILDESYSTEM    
           ,t.LASTET_DATO
          FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t
          LEFT JOIN XMLTABLE
          (
            XMLNamespaces(
                          'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                         ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24
                         )
            ,'/ns13:vedtak'
            PASSING t.XML_CLOB
            COLUMNS
            FAGSAK_ID               NUMBER (19,0) PATH './ns13:fagsakId' 
           ,BEHANDLINGS_ID          NUMBER(19,0) PATH './ns13:behandlingsresultat/ns13:behandlingsId'
          ) q
          ON ( 1 = 1 )
          LEFT JOIN XMLTABLE
          (
            XMLNamespaces(
                          'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                         ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                         ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24  
                         )
            ,'/ns13:vedtak/ns13:personOpplysninger/ns6:PersonopplysningerDvhForeldrepenger/ns6:adresse'
            PASSING t.XML_CLOB
            COLUMNS
            ADRESSE_TYPE            VARCHAR2(200) PATH 'ns6:adressetype'    
           ,POSTNUMMER             VARCHAR2(200) PATH 'ns6:postnummer'     
           ,LAND                   VARCHAR2(200) PATH 'ns6:land'           
          ) b
          ON ( 1 = 1 )
          where t.trans_id = rec_vedtak_sp.trans_id;
          exception
            when others then
               l_error_melding := sqlerrm;
               l_feil_kilde_navn := 'PL/SQL: FAM_SP_ADRESSE';
               l_feil_trans_id := rec_vedtak_sp.trans_id;
               rollback to do_insert; continue;
          end;

          --FAM_SP_PERSONOPPLYSNINGER
          begin
          INSERT INTO FK_SENSITIV.FAM_SP_PERSONOPPLYSNINGER
          (
            TRANS_ID	              
           ,TRANS_TID	             
           ,VEDTAK_ID	             
           ,FUNKSJONELL_TID        
           ,FAGSAK_ID              
           ,BEHANDLINGS_ID         
           ,AKTOER_ID              
           ,REGION                 
           ,KJONN                  
           ,STATSBORGERSKAP        
           ,SIVILSTAND             
           ,FOEDSELSDATO           
           ,PERSONSTATUS           
           ,ANNENFORELDER_AKTOER_ID
           ,KILDESYSTEM            
           ,LASTET_DATO
          )
          SELECT
            t.TRANS_ID
           ,t.TRANS_TID
           ,t.VEDTAK_ID
           ,t.FUNKSJONELL_TID 
           ,q.FAGSAK_ID
           ,q.BEHANDLINGS_ID
           ,q.AKTOER_ID
           ,q.REGION
           ,q.KJONN
           ,q.STATSBORGERSKAP
           ,q.SIVILSTAND
           ,to_date(q.FOEDSELSDATO,'YYYY-MM-DD')  FOEDSELSDATO
           ,q.PERSONSTATUS
           ,'N/A' ANNENFORELDER_AKTOER_ID
           ,t.KILDESYSTEM                
           ,t.LASTET_DATO
          FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t
          LEFT JOIN XMLTABLE
          (
            XMLNamespaces(
                          'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                          ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                          ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                          ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                          ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                          ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                          ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                          ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                          ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                          ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                          ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                          ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                          ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                          ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                          ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                          ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                          ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                          ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                          ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                          ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                          ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                          ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                          ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24
                          )
            ,'/ns13:vedtak'
            PASSING t.XML_CLOB
            COLUMNS
            FAGSAK_ID               NUMBER (19,0) PATH './ns13:fagsakId' 
           ,BEHANDLINGS_ID          NUMBER(19,0)  PATH './ns13:behandlingsresultat/ns13:behandlingsId'
          ,AKTOER_ID               VARCHAR2(200) PATH './ns13:personOpplysninger/ns6:PersonopplysningerDvhForeldrepenger/ns6:bruker/ns4:aktoerId'
          ,KJONN                  VARCHAR2(200) PATH './ns13:personOpplysninger/ns6:PersonopplysningerDvhForeldrepenger/ns6:bruker/ns4:kjoenn'
          ,STATSBORGERSKAP   VARCHAR2(200) PATH './ns13:personOpplysninger/ns6:PersonopplysningerDvhForeldrepenger/ns6:bruker/ns4:statsborgerskap/@kode'
          ,SIVILSTAND         VARCHAR2(200) PATH './ns13:personOpplysninger/ns6:PersonopplysningerDvhForeldrepenger/ns6:bruker/ns4:sivilstand/@kode'
          ,FOEDSELSDATO           VARCHAR2(200) PATH './ns13:personOpplysninger/ns6:PersonopplysningerDvhForeldrepenger/ns6:bruker/ns4:foedselsdato'
          ,PERSONSTATUS           VARCHAR2(200) PATH './ns13:personOpplysninger/ns6:PersonopplysningerDvhForeldrepenger/ns6:bruker/ns4:personstatus/@kode'
          ,REGION                 VARCHAR2(200) PATH './ns13:personOpplysninger/ns6:PersonopplysningerDvhForeldrepenger/ns6:bruker/ns4:region'
          ) q
          ON ( 1 = 1 )
          where t.trans_id = rec_vedtak_sp.trans_id;
          exception
            when others then
              l_error_melding := sqlerrm;
              l_feil_kilde_navn := 'PL/SQL: FAM_SP_PERSONOPPLYSNINGER';
              l_feil_trans_id := rec_vedtak_sp.trans_id;
              rollback to do_insert; continue;
          end;

          --FAM_SP_BEREGNINGSGRUNNLAG
          begin
            INSERT INTO FK_SENSITIV.FAM_SP_BEREGNINGSGRUNNLAG
            (
              TRANS_ID	                    
              ,TRANS_TID	                   
              ,VEDTAK_ID	                   
              ,FUNKSJONELL_TID              
              ,FAGSAK_ID                    
              ,BEHANDLINGS_ID               
              ,FOM                          
              ,TOM                          
              ,BRUTTO                       
              ,AVKORTET                     
              ,REDUSERT                     
              ,DAGSATS                      
              ,DEKNINGSGRAD                 
              ,SKJARINGSTIDSPUNKT           
              ,STATUS_OG_ANDEL_FOM          
              ,STATUS_OG_ANDEL_TOM          
              ,AKTIVITET_STATUS             
              ,VIRKSOMHETSNUMMER            
              ,STATUS_OG_ANDEL_BRUTTO       
              ,STATUS_OG_ANDEL_AVKORTET     
              ,STATUS_OG_ANDEL_REDUSERT     
              ,STATUS_OG_ANDEL_BEREGNET     
              ,STATUS_OG_ANDEL_INNTEKTSKAT  
              ,NATURALYTELSEBORTFALL        
              ,TIL_STOETENDEYTELSE_TYPE     
              ,TIL_STOETENDEYTELSE          
              ,AVKORTET_BRUKERS_ANDEL       
              ,REDUSERT_BRUKERS_ANDEL       
              ,DAGSATS_BRUKER               
              ,DAGSATS_ARBEIDSGIVER         
              ,REFUSJON_MAKSIMAL            
              ,REFUSJON_AVKORTET            
              ,REFUSJON_REDUSERT            
              ,KILDESYSTEM                  
              ,LASTET_DATO
            )
            SELECT 
              t.TRANS_ID
             ,t.TRANS_TID
             ,t.VEDTAK_ID
             ,t.FUNKSJONELL_TID 
             ,b.FAGSAK_ID
             ,b.BEHANDLINGS_ID
             ,to_date(q.FOM,'YYYY-MM-DD') FOM
             ,to_date(q.TOM,'YYYY-MM-DD') TOM
             ,q.BRUTTO
             ,q.AVKORTET
             ,q.REDUSERT
             ,q.DAGSATS
             ,d.DEKNINGSGRAD
             ,to_date(d.SKJAERINGSTIDSPUNKT,'YYYY-MM-DD') SKJARINGSTIDSPUNKT   
             ,to_date(c.STATUS_OG_ANDEL_FOM,'YYYY-MM-DD') STATUS_OG_ANDEL_FOM
             ,to_date(c.STATUS_OG_ANDEL_FOM,'YYYY-MM-DD') STATUS_OG_ANDEL_TOM
             ,c.AKTIVITET_STATUS
             ,c.VIRKSOMHETSNUMMER
             ,c.STATUS_OG_ANDEL_BRUTTO     
             ,c.STATUS_OG_ANDEL_AVKORTET   
             ,c.STATUS_OG_ANDEL_REDUSERT      
             ,c.STATUS_OG_ANDEL_BEREGNET   
             ,c.STATUS_OG_ANDEL_INNTEKTSKAT   
             ,c.NATURALYTELSEBORTFALL
             ,c.TIL_STOETENDEYTELSE_TYPE
             ,c.TIL_STOETENDEYTELSE
             ,c.AVKORTET_BRUKERS_ANDEL
             ,c.REDUSERT_BRUKERS_ANDEL
             ,c.DAGSATS_BRUKER
             ,c.DAGSATS_ARBEIDSGIVER   
             ,c.REFUSJON_MAKSIMAL
             ,c.REFUSJON_AVKORTET
             ,c.REFUSJON_REDUSERT
             ,t.KILDESYSTEM                  
             ,t.LASTET_DATO
            FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t,
            XMLTABLE
            (
              XMLNamespaces(
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                            ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24
                            )
              ,'/ns13:vedtak'
              PASSING t.XML_CLOB
              COLUMNS
              FAGSAK_ID                 NUMBER (19,0) PATH 'ns13:fagsakId'
             ,BEHANDLINGS_ID            NUMBER(19,0)  PATH 'ns13:behandlingsresultat/ns13:behandlingsId'
             ,BEREGNINGSGRUNNLAG   XMLTYPE       PATH 'ns13:behandlingsresultat/ns13:beregningsresultat/ns13:beregningsgrunnlag/ns20:beregningsgrunnlagSvangerskapspenger'
            ) b,
            XMLTABLE
            (
              XMLNamespaces(
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24  
                          )
              ,'ns20:beregningsgrunnlagSvangerskapspenger'
              PASSING b.BEREGNINGSGRUNNLAG
              COLUMNS
              DEKNINGSGRAD                   VARCHAR2(200) PATH 'ns20:dekningsgrad',
              SKJAERINGSTIDSPUNKT            VARCHAR2(200) PATH 'ns20:skjaeringstidspunkt'  
             ,BEREGNINGSGRUNNLAGPERIODE   XMLTYPE       PATH 'ns20:beregningsgrunnlagPeriode'
            ) d,
            XMLTABLE
            (
              XMLNamespaces(
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24  
                           )
              ,'ns20:beregningsgrunnlagPeriode'
              PASSING d.BEREGNINGSGRUNNLAGPERIODE
              COLUMNS
              FOM                   VARCHAR2(200) PATH 'ns20:periode/ns3:fom',
              TOM                   VARCHAR2(200) PATH 'ns20:periode/ns3:tom',
              BRUTTO                VARCHAR2(200) PATH 'ns20:brutto',
              AVKORTET              VARCHAR2(200) PATH 'ns20:avkortet',
              REDUSERT              VARCHAR2(200) PATH 'ns20:redusert',
              DAGSATS               VARCHAR2(200) PATH 'ns20:dagsats',
              xyz XMLTYPE       PATH 'ns20:beregningsgrunnlagPrStatusOgAndel'
            ) q,
            XMLTABLE
            (
              XMLNamespaces(
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24   
                          )
              ,'ns20:beregningsgrunnlagPrStatusOgAndel'
              PASSING q.xyz
              COLUMNS
              STATUS_OG_ANDEL_FOM     VARCHAR2(200) PATH 'ns20:periode/ns3:fom',
              STATUS_OG_ANDEL_TOM    VARCHAR2(200) PATH 'ns20:periode/ns3:tom',
              AKTIVITET_STATUS               VARCHAR2(200) PATH 'ns20:aktivitetstatus/@kode',
              VIRKSOMHETSNUMMER             VARCHAR2(200) PATH 'ns20:virksomhetsnummer',
              STATUS_OG_ANDEL_BRUTTO      VARCHAR2(200) PATH 'ns20:brutto', 
              STATUS_OG_ANDEL_AVKORTET    VARCHAR2(200) PATH 'ns20:avkortet', 
              STATUS_OG_ANDEL_REDUSERT    VARCHAR2(200) PATH 'ns20:redusert',    
              STATUS_OG_ANDEL_BEREGNET    VARCHAR2(200) PATH 'ns20:beregnet', 
              STATUS_OG_ANDEL_INNTEKTSKAT   VARCHAR2(200) PATH 'ns20:inntektskategori/@kode',    
              NATURALYTELSEBORTFALL    VARCHAR2(200) PATH 'ns20:naturalytelseBortfall',                  --Finnes ikke i XML
              TIL_STOETENDEYTELSE_TYPE   VARCHAR2(200) PATH 'ns20:tilstoetendeYtelseType',
              TIL_STOETENDEYTELSE       VARCHAR2(200) PATH 'ns20:tilstoetendeYtelse',
              AVKORTET_BRUKERS_ANDEL     VARCHAR2(200) PATH 'ns20:avkortetBrukersAndel',
              REDUSERT_BRUKERS_ANDEL     VARCHAR2(200) PATH 'ns20:redusertBrukersAndel',
              DAGSATS_BRUKER            VARCHAR2(200) PATH 'ns20:dagsatsBruker',
              DAGSATS_ARBEIDSGIVER      VARCHAR2(200) PATH 'ns20:dagsatsArbeidsgiver',    
              REFUSJON_MAKSIMAL         VARCHAR2(200) PATH 'ns20:refusjonTilArbeidsgiver/ns20:maksimal', 
              REFUSJON_AVKORTET         VARCHAR2(200) PATH 'ns20:refusjonTilArbeidsgiver/ns20:avkortet', 
              REFUSJON_REDUSERT         VARCHAR2(200) PATH 'ns20:refusjonTilArbeidsgiver/ns20:redusert'
            ) c
            where t.trans_id = rec_vedtak_sp.trans_id;
          exception
            when others then
              l_error_melding := sqlerrm;
              l_feil_kilde_navn := 'PL/SQL: FAM_SP_BEREGNINGSGRUNNLAG';
              l_feil_trans_id := rec_vedtak_sp.trans_id;
              rollback to do_insert; continue;
          end;

          --FAM_SP_TILKJENTYTELSE
          begin
            INSERT INTO FK_SENSITIV.FAM_SP_TILKJENTYTELSE
            (
              TRANS_ID
              ,TRANS_TID
              ,VEDTAK_ID
              ,FUNKSJONELL_TID
              ,FAGSAK_ID
              ,BEHANDLINGS_ID
              ,BRUKERERMOTTAKER
              ,FOM
              ,TOM
              ,ORGNR
              ,ORGNAVN
              ,ARBEIDSFORHOLD_ID
              ,AKTIVITETSSTATUS
              ,INNTEKTSKATEGORI
              ,DAGSATS
              ,STILLINGSPROSENT
              ,UTBETALINGSGRAD
              ,KILDESYSTEM
              ,LASTET_DATO
            )
            SELECT 
              t.TRANS_ID
             ,t.TRANS_TID
             ,t.VEDTAK_ID
             ,t.FUNKSJONELL_TID
             ,b.FAGSAK_ID
             ,b.BEHANDLINGS_ID   
             ,q.BRUKERERMOTTAKER
             ,to_date(q.FOM,'YYYY-MM-DD') FOM
             ,to_date(q.TOM,'YYYY-MM-DD') TOM 
             ,q.ORGNR                     
             ,q.ORGNAVN                   
             ,q.ARBEIDSFORHOLD_ID          
             ,q.AKTIVITETSSTATUS          
             ,q.INNTEKTSKATEGORI          
             ,q.DAGSATS                   
             ,q.STILLINGSPROSENT         
             ,q.UTBETALINGSGRAD  
             ,t.KILDESYSTEM
             ,t.LASTET_DATO
            FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t,
            XMLTABLE
            (
              XMLNamespaces(
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24   
                          )
              ,'/ns13:vedtak'
              PASSING t.XML_CLOB
              COLUMNS
              FAGSAK_ID                 NUMBER (19,0) PATH 'ns13:fagsakId'
             ,BEHANDLINGS_ID            NUMBER(19,0)  PATH 'ns13:behandlingsresultat/ns13:behandlingsId'
             ,BEREGNINGSRESULTAT       XMLTYPE       PATH 'ns13:behandlingsresultat/ns13:beregningsresultat/ns13:tilkjentYtelse/ns22:YtelseSvangerskapspenger/ns22:beregningsresultat'
            ) b,
            XMLTABLE
            (
              XMLNamespaces(
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24   
                          )
              ,'ns22:beregningsresultat'
              PASSING b.BEREGNINGSRESULTAT
              COLUMNS
              BRUKERERMOTTAKER           VARCHAR2(200) PATH 'ns22:brukerErMottaker'
             ,FOM                       VARCHAR2(200) PATH 'ns22:periode/ns3:fom'
             ,TOM                       VARCHAR2(200) PATH 'ns22:periode/ns3:tom'
             ,ORGNR                     VARCHAR2(200) PATH 'ns22:virksomhet/ns13:orgnr'                --Finnes ikke i XML
             ,ORGNAVN                   VARCHAR2(200) PATH 'ns22:virksomhet/ns13:navn'                 --Finnes ikke i XML
             ,ARBEIDSFORHOLD_ID          VARCHAR2(200) PATH 'ns22:virksomhet/ns13:arbeidsforholdid'     --Finnes ikke i XML
             ,AKTIVITETSSTATUS          VARCHAR2(200) PATH 'ns22:aktivitetstatus/@kode'
             ,INNTEKTSKATEGORI          VARCHAR2(200) PATH 'ns22:inntektskategori/@kode'
             ,DAGSATS                   VARCHAR2(200) PATH 'ns22:dagsats'
             ,STILLINGSPROSENT          VARCHAR2(200) PATH 'ns22:stillingsprosent'
             ,UTBETALINGSGRAD           VARCHAR2(200) PATH 'ns22:utbetalingsgrad'
            ) q
            where t.trans_id = rec_vedtak_sp.trans_id;
          exception
            when others then
              l_error_melding := sqlerrm;
              l_feil_kilde_navn := 'PL/SQL: FAM_SP_TILKJENTYTELSE';
              l_feil_trans_id := rec_vedtak_sp.trans_id;
              rollback to do_insert; continue;
          end;

          --FAM_SP_DELYTELSEID
          begin
            INSERT INTO FK_SENSITIV.FAM_SP_DELYTELSEID
            (
              TRANS_ID	        
             ,TRANS_TID	       
             ,VEDTAK_ID	       
             ,FUNKSJONELL_TID  
             ,FAGSAK_ID        
             ,BEHANDLINGS_ID   
             ,DATO_VEDTAK_FOM  
             ,DATO_VEDTAK_TOM  
             ,LINJE_ID         
             ,DELYTELSE_ID     
             ,REF_DELYTELSE_ID 
             ,UTBETALES_TIL_ID 
             ,REFUNDERES_ID    
             ,KODE_STATUS_LINJE
             ,DATO_STATUS_FOM  
             ,OPPDRAG_ID       
             ,FAGSYSTEM_ID     
             ,KILDESYSTEM      
             ,LASTET_DATO
            )
            SELECT
              t.TRANS_ID
             ,t.TRANS_TID
             ,t.VEDTAK_ID
             ,t.FUNKSJONELL_TID 
             ,b.FAGSAK_ID
             ,b.BEHANDLINGS_ID
             ,to_date(a.DATO_VEDTAK_FOM,'YYYY-MM-DD')  DATO_VEDTAK_FOM  
             ,to_date(a.DATO_VEDTAK_TOM,'YYYY-MM-DD')  DATO_VEDTAK_TOM 
             ,a.LINJE_ID
             ,a.DELYTELSE_ID
             ,a.REF_DELYTELSE_ID
             ,a.UTBETALES_TIL_ID    
             ,a.REFUNDERES_ID
             ,a.KODE_STATUS_LINJE  
             ,to_date(a.DATO_STATUS_FOM,'YYYY-MM-DD')  DATO_STATUS_FOM    
             ,c.OPPDRAG_ID
             ,c.FAGSYSTEM_ID
             ,t.KILDESYSTEM      
             ,t.LASTET_DATO
            FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t,
            XMLTABLE
            (
              XMLNamespaces(
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24  
                          )
              ,'/ns13:vedtak'
              PASSING t.XML_CLOB
              COLUMNS      
              FAGSAK_ID                 NUMBER (19,0) PATH 'ns13:fagsakId'
             ,BEHANDLINGS_ID            NUMBER(19,0)  PATH 'ns13:behandlingsresultat/ns13:behandlingsId'
             ,UTTAKSRESULTATPERIODER   XMLTYPE       PATH 'ns13:oppdrag'
            ) b,
            XMLTABLE
            (
              XMLNamespaces(
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24 
                          )
              ,'ns13:oppdrag'
             PASSING b.UTTAKSRESULTATPERIODER
             COLUMNS
             OPPDRAG_ID          VARCHAR2(200) PATH 'ns14:oppdragId'
            ,FAGSYSTEM_ID        VARCHAR2(200) PATH 'ns14:fagsystemId'
            ,OPPDRAGSLINJE      XMLTYPE       PATH 'ns15:oppdragslinje'
            ) c,   
            XMLTABLE
            (
              XMLNamespaces(
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24 
                          )
              ,'ns15:oppdragslinje'
              PASSING c.OPPDRAGSLINJE
              COLUMNS
              DATO_VEDTAK_FOM                   VARCHAR2(200) PATH  'ns15:periode/ns3:fom'
             ,DATO_VEDTAK_TOM                   VARCHAR2(200) PATH  'ns15:periode/ns3:tom'
             ,LINJE_ID               VARCHAR2(200) PATH  'ns15:linjeId'
             ,DELYTELSE_ID           VARCHAR2(200) PATH  'ns15:delytelseId' 
             ,REF_DELYTELSE_ID        VARCHAR2(200) PATH  'ns15:ref_delytelse_id'
             ,UTBETALES_TIL_ID      VARCHAR2(200) PATH  'ns15:utbetales_til_id' 
             ,REFUNDERES_ID         VARCHAR2(200) PATH  'ns15:refunderes_id'
             ,KODE_STATUS_LINJE     VARCHAR2(200) PATH  'ns15:kode_status_linje' 
             ,DATO_STATUS_FOM            VARCHAR2(200) PATH  'ns15:status_fom'  
            ) a
            where t.trans_id = rec_vedtak_sp.trans_id;
          exception
            when others then
              l_error_melding := sqlerrm;
              l_feil_kilde_navn := 'PL/SQL: FAM_SP_DELYTELSEID';
              l_feil_trans_id := rec_vedtak_sp.trans_id;
              rollback to do_insert; continue;
          end;


          --FAM_SP_VILKAAR
          begin
            INSERT INTO FK_SENSITIV.FAM_SP_VILKAAR 
            (
              TRANS_ID
             ,TRANS_TID	                    
             ,VEDTAK_ID	                    
             ,FUNKSJONELL_TID               
             ,FAGSAK_ID                     
             ,BEHANDLINGS_ID                
             ,EKTEFELLES_BARN               
             ,SOEKERS_KJOEN                 
             ,MANN_ADOPTERER_ALENE          
             ,OMSORGS_OVERTAKELSESDATO      
             ,PERSON_STATUS                 
             ,ER_BRUKER_MEDLEM              
             ,ER_BRUKER_BOSATT              
             ,HAR_OPPHOLDSRETT              
             ,HAR_LOVLIGOPPHOLD_I_NORGE     
             ,ER_NORDISK_STATSBORGER        
             ,ER_BORGER_AV_EU_EOS           
             ,PLIKTIG_ELLER_FRIVILLIG_MEDLEM
             ,ELEKTRONISK_SOEKNAD           
             ,SKJAERINGS_TIDSPUNKT          
             ,SOEKNAD_MOTTAT_DATO           
             ,BEHANDLINGS_DATO              
             ,FOM                           
             ,TOM                           
             ,MAKS_MELLOM_LIGGENDE_PERIODE  
             ,MIN_MELLOM_LIGGENDE_PERIODE   
             ,MIN_STEANTALLDAGER_FORVENT    
             ,MIN_STEANTALLDAGER_GODKJENT   
             ,MIN_STEANTALLMANEDER_GODKJENT 
             ,MIN_STEINNTEKT                
             ,PERIODE_ANTATT_GODKJENT       
             ,KILDESYSTEM                   
             ,LASTET_DATO
            )
            SELECT
              t.TRANS_ID
             ,t.TRANS_TID
             ,t.VEDTAK_ID
             ,t.FUNKSJONELL_TID 
             ,b.FAGSAK_ID
             ,b.BEHANDLINGS_ID
             ,q.EKTEFELLES_BARN
             ,q.SOEKERS_KJOEN
             ,q.MANN_ADOPTERER_ALENE
             ,to_date(q.OMSORGS_OVERTAKELSESDATO,'YYYY-MM-DD') OMSORGS_OVERTAKELSESDATO    
             ,q.PERSON_STATUS                        
             ,q.ER_BRUKER_MEDLEM                        
             ,q.ER_BRUKER_BOSATT                       
             ,q.HAR_OPPHOLDSRETT                 
             ,q.HAR_LOVLIGOPPHOLD_I_NORGE          
             ,q.ER_NORDISK_STATSBORGER            
             ,q.ER_BORGER_AV_EU_EOS                
             ,q.PLIKTIG_ELLER_FRIVILLIG_MEDLEM          
             ,q.ELEKTRONISK_SOEKNAD   
             ,to_date(q.SKJAERINGS_TIDSPUNKT,'YYYY-MM-DD') SKJAERINGS_TIDSPUNKT    
             ,to_date(q.SOEKNAD_MOTTAT_DATO,'YYYY-MM-DD')   SOEKNAD_MOTTAT_DATO           
             ,to_date(q.BEHANDLINGS_DATO,'YYYY-MM-DD')  BEHANDLINGS_DATO              
             ,to_date(q.FOM,'YYYY-MM-DD')  FOM    
             ,to_date(q.TOM,'YYYY-MM-DD')  TOM                           
             ,q.MAKS_MELLOM_LIGGENDE_PERIODE    
             ,q.MIN_MELLOM_LIGGENDE_PERIODE     
             ,q.MIN_STEANTALLDAGER_FORVENT    
             ,q.MIN_STEANTALLDAGER_GODKJENT    
             ,q.MIN_STEANTALLMANEDER_GODKJENT  
             ,q.MIN_STEINNTEKT                
             ,q.PERIODE_ANTATT_GODKJENT 
             ,t.KILDESYSTEM                   
             ,t.LASTET_DATO
            FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t,
            XMLTABLE
            (
              XMLNamespaces(
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24   
                          )
              ,'/ns13:vedtak'
              PASSING t.XML_CLOB
              COLUMNS
              FAGSAK_ID                 NUMBER (19,0) PATH 'ns13:fagsakId'
             ,BEHANDLINGS_ID            NUMBER(19,0)  PATH 'ns13:behandlingsresultat/ns13:behandlingsId'
             ,VILKAARSGRUNNLAG         XMLTYPE       PATH './ns13:behandlingsresultat/ns13:vurderteVilkaar/ns13:vilkaar/ns13:vilkaarsgrunnlag/ns11:vilkaarsgrunnlag'
            ) b,
            XMLTABLE
            (
              XMLNamespaces(
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24 
                          )
              ,'ns11:vilkaarsgrunnlag'
              PASSING b.VILKAARSGRUNNLAG  
              COLUMNS
              EKTEFELLES_BARN              VARCHAR2(200) PATH 'ns10:ektefellesBarn',               --Finnes ikke i XML
              SOEKERS_KJOEN                VARCHAR2(200) PATH 'ns10:soekersKjoenn/@kode',          --Finnes ikke i XML
              MANN_ADOPTERER_ALENE          VARCHAR2(200) PATH 'ns10:mannAdoptererAlene',           --Finnes ikke i XML
              OMSORGS_OVERTAKELSESDATO     VARCHAR2(200) PATH 'ns10:omsorgsovertakelsesdato',     --Finnes ikke i XML
              PERSON_STATUS                          VARCHAR2(200) PATH 'ns11:personstatus',
              ER_BRUKER_MEDLEM                        VARCHAR2(200) PATH 'ns11:erBrukerMedlem',
              ER_BRUKER_BOSATT                        VARCHAR2(200) PATH 'ns11:erBrukerBosatt',
              HAR_OPPHOLDSRETT                 VARCHAR2(200) PATH 'ns11:harBrukerOppholdsrett',
              HAR_LOVLIGOPPHOLD_I_NORGE          VARCHAR2(200) PATH 'ns11:harBrukerLovligOppholdINorge',
              ER_NORDISK_STATSBORGER            VARCHAR2(200) PATH 'ns11:erBrukerNordiskstatsborger',
              ER_BORGER_AV_EU_EOS                 VARCHAR2(200) PATH 'ns11:erBrukerBorgerAvEUEOS',
              PLIKTIG_ELLER_FRIVILLIG_MEDLEM           VARCHAR2(200) PATH 'ns11:erBrukerPliktigEllerFrivilligMedlem',
              ELEKTRONISK_SOEKNAD                    VARCHAR2(200) PATH 'ns10:elektroniskSoeknad',
              SKJAERINGS_TIDSPUNKT                   VARCHAR2(200) PATH 'ns10:skjaeringstidspunkt',
              SOEKNAD_MOTTAT_DATO                     VARCHAR2(200) PATH 'ns10:soeknadMottatDato',
              BEHANDLINGS_DATO              VARCHAR2(200) PATH 'ns10:behandlingsDato',
              FOM                          VARCHAR2(200) PATH 'ns10:opptjeningperiode/ns3:fom',
              TOM                          VARCHAR2(200) PATH 'ns10:opptjeningperiode/ns3:tom',
              MAKS_MELLOM_LIGGENDE_PERIODE    VARCHAR2(200) PATH 'ns10:maksMellomliggendePeriodeForArbeidsforhold',
              MIN_MELLOM_LIGGENDE_PERIODE     VARCHAR2(200) PATH 'ns10:minForegaaendeForMellomliggendePeriodeForArbeidsforhold',
              MIN_STEANTALLDAGER_FORVENT     VARCHAR2(200) PATH 'ns10:minsteAntallDagerForVent',
              MIN_STEANTALLDAGER_GODKJENT    VARCHAR2(200) PATH 'ns10:minsteAntallDagerGodkjent',
              MIN_STEANTALLMANEDER_GODKJENT  VARCHAR2(200) PATH 'ns10:minsteAntallMånederGodkjent',
              MIN_STEINNTEKT                VARCHAR2(200) PATH 'ns10:minsteInntekt',
              PERIODE_ANTATT_GODKJENT        VARCHAR2(200) PATH 'ns10:periodeAntattGodkjentForBehandlingstidspunkt'
            ) q
            where t.trans_id = rec_vedtak_sp.trans_id;
          exception
            when others then
              l_error_melding := sqlerrm;
              l_feil_kilde_navn := 'PL/SQL: FAM_SP_VILKAAR';
              l_feil_trans_id := rec_vedtak_sp.trans_id;
              rollback to do_insert; continue;
          end;

          --FAM_SP_BEREG_GRUNNLAGPERIODE
          begin
            INSERT INTO FK_SENSITIV.FAM_SP_BEREG_GRUNNLAGPERIODE
            (
              TRANS_ID	          
             ,TRANS_TID	         
             ,VEDTAK_ID	         
             ,FUNKSJONELL_TID    
             ,FAGSAK_ID          
             ,BEHANDLINGS_ID     
             ,BG_PERIODE_FOM
             ,BRUTTO_PR_AAR      
             ,AVKORTET_PR_AAR 
             ,REDUSERT_PR_AAR
             ,DAGSATS  
             ,DEKNINGSGRAD       
             ,SKJAERINGSTIDSPUNKT
             ,KILDESYSTEM        
             ,LASTET_DATO            
            )
            SELECT 
              t.TRANS_ID
             ,t.TRANS_TID
             ,t.VEDTAK_ID
             ,t.FUNKSJONELL_TID 
             ,b.FAGSAK_ID
             ,b.BEHANDLINGS_ID        
             ,to_date(q.BG_PERIODE_FOM,'YYYY-MM-DD') BG_PERIODE_FOM
             ,q.BRUTTO_PR_AAR
             ,q.AVKORTET_PR_AAR
             ,q.REDUSERT_PR_AAR
             ,q.DAGSATS
             ,d.DEKNINGSGRAD
             ,to_date(d.SKJAERINGSTIDSPUNKT,'YYYY-MM-DD') SKJAERINGSTIDSPUNKT
             ,t.KILDESYSTEM                   
             ,t.LASTET_DATO
            FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t,
            XMLTABLE
            (
              XMLNamespaces(
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24    
                          )
              ,'/ns13:vedtak'
              PASSING t.XML_CLOB
              COLUMNS       
              FAGSAK_ID                 NUMBER (19,0) PATH 'ns13:fagsakId'
             ,BEHANDLINGS_ID            NUMBER(19,0)  PATH 'ns13:behandlingsresultat/ns13:behandlingsId'
             ,BEREGNINGSGRUNNLAG   XMLTYPE       PATH 'ns13:behandlingsresultat/ns13:beregningsresultat/ns13:beregningsgrunnlag/ns20:beregningsgrunnlagSvangerskapspenger'
            ) b,
            XMLTABLE
            (
              XMLNamespaces(
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24 
                          )
              ,'ns20:beregningsgrunnlagSvangerskapspenger'
              PASSING b.BEREGNINGSGRUNNLAG
              COLUMNS
              DEKNINGSGRAD                   VARCHAR2(200) PATH 'ns20:dekningsgrad',
              SKJAERINGSTIDSPUNKT            VARCHAR2(200) PATH 'ns20:skjaeringstidspunkt'  
             ,BEREGNINGSGRUNNLAGPERIODE   XMLTYPE       PATH 'ns20:beregningsgrunnlagPeriode'
            ) d,
            XMLTABLE
            (
              XMLNamespaces(
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24 
                           )
              ,'ns20:beregningsgrunnlagPeriode'
              PASSING d.BEREGNINGSGRUNNLAGPERIODE
              COLUMNS
              BG_PERIODE_FOM                   VARCHAR2(200) PATH 'ns20:periode/ns3:fom',
              BRUTTO_PR_AAR                VARCHAR2(200) PATH 'ns20:brutto',
              AVKORTET_PR_AAR              VARCHAR2(200) PATH 'ns20:avkortet',
              REDUSERT_PR_AAR              VARCHAR2(200) PATH 'ns20:redusert',
              DAGSATS               VARCHAR2(200) PATH 'ns20:dagsats'
            ) q
            where t.trans_id = rec_vedtak_sp.trans_id;
          exception
            when others then
              l_error_melding := sqlerrm;
              l_feil_kilde_navn := 'PL/SQL: FAM_SP_BEREG_GRUNNLAGPERIODE';
              l_feil_trans_id := rec_vedtak_sp.trans_id;
              rollback to do_insert; continue;
          end;
          --FAM_SP_FODSELTERMIN
          begin
            INSERT INTO FK_SENSITIV.FAM_SP_FODSELTERMIN
            (
               TRANS_ID
              ,TRANS_TID
              ,VEDTAK_ID
              ,FUNKSJONELL_TID
              ,FAGSAK_ID
              ,BEHANDLINGS_ID
              ,ANTALL_BARN_FOEDSEL
              ,FOEDSELSDATO
              ,TERMINDATO
              ,UTSTEDT_DATO
              ,ANTALL_BARN_TERMIN
              ,FOEDSELSDATO_ADOPSJON
              ,EREKTEFELLES_BARN
              ,KILDESYSTEM
              ,LASTET_DATO
            )
            SELECT               
                     t.TRANS_ID
                    ,t.TRANS_TID
                    ,t.VEDTAK_ID
                    ,t.FUNKSJONELL_TID
                    ,q.FAGSAK_ID
                    ,q.BEHANDLINGS_ID
                    ,q.ANTALL_BARN_FOEDSEL
                    ,to_date(q.FOEDSELSDATO,'YYYY-MM-DD')
                    ,to_date(q.TERMINDATO,'YYYY-MM-DD')
                    ,to_date(q.UTSTEDTDATO,'YYYY-MM-DD')
                    ,q.ANTALL_BARN_TERMIN
                     ,null
                    ,q.EREKTEFELLES_BARN
                    ,t.kildesystem
                    ,t.lastet_dato
            FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t

            left join       
            XMLTABLE
            (
               XMLNamespaces
               (                    
                   'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                  ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                  ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                  ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                  ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                  ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                  ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                  ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                  ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                  ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                  ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                  ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                  ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                  ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                  ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                  ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                  ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                  ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                  ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                  ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                  ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                  ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                  ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24 
               )
               ,'/ns13:vedtak'
               PASSING t.XML_CLOB
               COLUMNS            
                 FAGSAK_ID                    NUMBER(19,0)         PATH './ns13:fagsakId'
                ,BEHANDLINGS_ID               NUMBER(19,0)         PATH './ns13:behandlingsresultat/ns13:behandlingsId'
                ,ANTALL_BARN_FOEDSEL          NUMBER(19,0)         PATH './ns13:personOpplysninger/ns6:PersonopplysningerDvhForeldrepenger/ns6:familiehendelse/ns6:foedsel/ns4:antallBarn'
                ,FOEDSELSDATO                 VARCHAR2(100 CHAR)   PATH './ns13:personOpplysninger/ns6:PersonopplysningerDvhForeldrepenger/ns6:familiehendelse/ns6:foedsel/ns4:foedselsdato'
                ,TERMINDATO                   VARCHAR2(100 CHAR)   PATH './ns13:personOpplysninger/ns6:PersonopplysningerDvhForeldrepenger/ns6:familiehendelse/ns6:terminbekreftelse/ns6:termindato'
                ,UTSTEDTDATO                  VARCHAR2(100 CHAR)   PATH './ns13:personOpplysninger/ns6:PersonopplysningerDvhForeldrepenger/ns6:familiehendelse/ns6:terminbekreftelse/ns6:utstedtDato'
                ,ANTALL_BARN_TERMIN           NUMBER(19,0)         PATH './ns13:personOpplysninger/ns6:PersonopplysningerDvhForeldrepenger/ns6:familiehendelse/ns6:terminbekreftelse/ns6:antallBarn'
                ,EREKTEFELLES_BARN            VARCHAR2(100 CHAR)   PATH './ns13:personOpplysninger/ns6:PersonopplysningerDvhForeldrepenger/ns6:familiehendelse/ns6:adopsjon/ns6:erEktefellesBarn'
                ,ADOPSBARN                    XMLTYPE              PATH 'ns13:personOpplysninger/ns6:PersonopplysningerDvhForeldrepenger/ns6:familiehendelse/ns6:adopsjon/ns6:adopsjonsbarn'
                ,FOEDSEL                      XMLTYPE              PATH 'ns13:personOpplysninger/ns6:PersonopplysningerDvhForeldrepenger/ns6:familiehendelse/ns6:foedsel'
            ) q
            on 1 = 1
            where t.trans_id = rec_vedtak_sp.trans_id;
          exception
            when others then
              l_error_melding := sqlerrm;
              l_feil_kilde_navn := 'PL/SQL: FAM_SP_FODSELTERMIN';
              l_feil_trans_id := rec_vedtak_sp.trans_id;
              rollback to do_insert; continue;
          end;

          ----------------------------------------------
           --FAM_SP_INNTEKTER
          begin
            INSERT INTO  FK_SENSITIV.FAM_SP_INNTEKTER
            SELECT
              t.TRANS_ID
             ,t.TRANS_TID
             ,t.VEDTAK_ID
             ,q.FAGSAK_ID
             ,q.BEHANDLINGS_ID
             ,a.MOTTAKER               
             ,a.ARBEIDSGIVER        
             ,to_date(b.FOM,'YYYY-MM-DD')  FOM              
             ,to_date(b.TOM,'YYYY-MM-DD')  TOM   
             ,b.BELOEP          
             ,b.YTELSETYPE
            FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t,
            XMLTABLE
            (
              XMLNamespaces(
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24   
                          )
              ,'/ns13:vedtak'
              PASSING t.XML_CLOB
              COLUMNS
              FAGSAK_ID                    NUMBER (19,0) PATH './ns13:fagsakId'
             ,BEHANDLINGS_ID               NUMBER (19,0) PATH './ns13:behandlingsresultat/ns13:behandlingsId'
             ,INNTEKT XMLTYPE PATH  './ns13:personOpplysninger/ns6:PersonopplysningerDvhForeldrepenger/ns6:inntekter/ns6:inntekt'
             ,INNTEKTSPOSTER XMLTYPE PATH  './ns13:personOpplysninger/ns6:PersonopplysningerDvhForeldrepenger/ns6:inntekter/ns6:inntekt/ns6:inntektsposter'
            ) q,
            XMLTABLE
            (
              XMLNamespaces(
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24  
                          )
              ,'ns6:inntekt' PASSING q.INNTEKT
              COLUMNS      
              MOTTAKER                    VARCHAR2(200) PATH 'ns6:mottaker'
             ,ARBEIDSGIVER                VARCHAR2(200) PATH 'ns6:arbeidsgiver'
             ,INNTEKTSPOSTER XMLTYPE PATH  '/ns6:inntektsposter'
            ) a,
            XMLTABLE
            (
              XMLNamespaces(
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24  
                          )
              ,'ns6:inntektsposter' PASSING q.INNTEKTSPOSTER
              COLUMNS      
              FOM                         VARCHAR2(200) PATH 'ns6:periode/ns3:fom'
             ,TOM                         VARCHAR2(200) PATH 'ns6:periode/ns3:tom'
             ,BELOEP                      VARCHAR2(200) PATH 'ns6:beloep'
             ,YTELSETYPE                  VARCHAR2(200) PATH 'ns6:ytelsetype'
            ) b
            where t.trans_id = rec_vedtak_sp.trans_id;
          exception
            when others then
              l_error_melding := sqlerrm;
              l_feil_kilde_navn := 'PL/SQL: FAM_SP_INNTEKTER';
              l_feil_trans_id := rec_vedtak_sp.trans_id;
              rollback to do_insert; continue;
          end;
          ----------------------------------------------
          begin
            INSERT INTO  FK_SENSITIV.FAM_SP_UTTAK_TILRETTELEGGING
            (
              TRANS_ID	          
             ,TRANS_TID	         
             ,VEDTAK_ID	         
             ,FUNKSJONELL_TID    
             ,FAGSAK_ID          
             ,BEHANDLINGS_ID                
             ,BEHOVFORTILRETTELEGGINGFOM
             ,SLUTTEARBEIDFOM
             ,ARBEIDTYPE
             ,TIDLIGEREBEHANDLING
             ,VIRKSOMHET
             ,ARBEIDSFORHOLDID
             ,ERVIRKSOMHET
             ,MOTTATTTIDSPUNKT
             ,KILDESYSTEM        
             ,LASTET_DATO           
            )
            SELECT
              t.TRANS_ID
             ,t.TRANS_TID
             ,t.VEDTAK_ID
             ,t.FUNKSJONELL_TID 
             ,q.FAGSAK_ID
             ,q.BEHANDLINGS_ID
             ,to_date(a.BEHOVFORTILRETTELEGGINGFOM,'YYYY-MM-DD') BEHOVFORTILRETTELEGGINGFOM 
             ,to_date(a.SLUTTEARBEIDFOM,'YYYY-MM-DD') SLUTTEARBEIDFOM
             ,a.ARBEIDTYPE
             ,a.TIDLIGEREBEHANDLING
             ,a.VIRKSOMHET
             ,a.ARBEIDSFORHOLDID
             ,a.ERVIRKSOMHET
             ,to_date(a.MOTTATTTIDSPUNKT,'YYYY-MM-DD') MOTTATTTIDSPUNKT
             ,t.KILDESYSTEM
             ,t.LASTET_DATO
            FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t,
            XMLTABLE
            (
              XMLNamespaces(
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                            ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                            ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24
                            )
              ,'/ns13:vedtak'
              PASSING t.XML_CLOB
              COLUMNS 
              FAGSAK_ID                    NUMBER (19,0) PATH './ns13:fagsakId'
             ,BEHANDLINGS_ID               NUMBER (19,0) PATH './ns13:behandlingsresultat/ns13:behandlingsId'
             ,TILRETTELEGGING XMLTYPE PATH  './ns13:behandlingsresultat/ns13:beregningsresultat/ns13:uttak/ns24:uttak/ns24:tilrettelegging'
            ) q,
            XMLTABLE
            (
              XMLNamespaces(
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24  
                          )
              ,'ns24:tilrettelegging' PASSING q.TILRETTELEGGING
              COLUMNS 
              BEHOVFORTILRETTELEGGINGFOM  VARCHAR2(200) PATH 'ns24:behovForTilretteleggingFom'
             ,SLUTTEARBEIDFOM             VARCHAR2(200) PATH 'ns24:slutteArbeidFom'
             ,ARBEIDTYPE                  VARCHAR2(200) PATH 'ns24:arbeidtype/@kode'
             ,TIDLIGEREBEHANDLING         VARCHAR2(200) PATH 'ns24:kopiertFraTidligereBehandling'
             ,VIRKSOMHET                  VARCHAR2(200) PATH 'ns24:virksomhet'
             ,ARBEIDSFORHOLDID            VARCHAR2(200) PATH 'ns24:arbeidsforholdid'
             ,ERVIRKSOMHET                VARCHAR2(200) PATH 'ns24:erVirksomhet'
             ,MOTTATTTIDSPUNKT            VARCHAR2(200) PATH 'ns24:mottattTidspunkt'
            ) a
            where t.trans_id = rec_vedtak_sp.trans_id;
          exception
            when others then
              l_error_melding := sqlerrm;
              l_feil_kilde_navn := 'PL/SQL: FAM_SP_UTTAK_TILRETTELEGGING';
              l_feil_trans_id := rec_vedtak_sp.trans_id;
              rollback to do_insert; continue;
          end;
          ----------------------------------------------
          begin
            INSERT INTO FK_SENSITIV.FAM_SP_UTTAK_RES_PER
            (
              TRANS_ID	          
             ,TRANS_TID	         
             ,VEDTAK_ID	         
             ,FUNKSJONELL_TID    
             ,FAGSAK_ID          
             ,BEHANDLINGS_ID
             ,FOERSTEUTTAKSDATO
             ,SISTEUTTAKSDATO
             ,FOM
             ,TOM
             ,PERIODERESULTATTYPE
             ,PERIODERESULTATAARSAK
             ,VIRKSOMHET
             ,ARBEIDSFORHOLDID
             ,KILDESYSTEM        
             ,LASTET_DATO            
            )
            SELECT
              t.TRANS_ID
             ,t.TRANS_TID
             ,t.VEDTAK_ID
             ,t.FUNKSJONELL_TID
             ,a.FAGSAKID
             ,a.BEHANDLINGSID
             ,to_date(a.FOERSTEUTTAKSDATO,'YYYY-MM-DD') FOERSTEUTTAKSDATO
             ,to_date(a.SISTEUTTAKSDATO,'YYYY-MM-DD')  SISTEUTTAKSDATO
             ,to_date(c.FOM,'YYYY-MM-DD')  FOM
             ,to_date(c.TOM,'YYYY-MM-DD')  TOM
             ,c.PERIODERESULTATTYPE
             ,c.PERIODERESULTATAARSAK
             ,b.VIRKSOMHET
             ,b.ARBEIDSFORHOLDID
             ,t.KILDESYSTEM
             ,t.LASTET_DATO
            FROM FK_SENSITIV.fam_fp_vedtak_utbetaling t,
            XMLTABLE
            (
              XMLNamespaces(
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24  
                          )
              ,'/ns13:vedtak'
              PASSING t.XML_CLOB
              COLUMNS      
              FAGSAKID                 NUMBER (19,0) PATH './ns13:fagsakId'
             ,BEHANDLINGSID            NUMBER(19,0)  PATH './ns13:behandlingsresultat/ns13:behandlingsId'
             ,FOERSTEUTTAKSDATO        VARCHAR2(200) PATH './ns13:behandlingsresultat/ns13:beregningsresultat/ns13:uttak/ns24:uttak/ns24:foersteUttaksdato'
             ,SISTEUTTAKSDATO          VARCHAR2(200) PATH './ns13:behandlingsresultat/ns13:beregningsresultat/ns13:uttak/ns24:uttak/ns24:sisteUttaksdato'
             ,UTTAKSRESULTATARBEIDSFORHOLD   XMLTYPE       PATH './ns13:behandlingsresultat/ns13:beregningsresultat/ns13:uttak/ns24:uttak/ns24:uttaksResultatArbeidsforhold'
             ,UTTAKSRESULTATPERIODER         XMLTYPE       PATH './ns13:behandlingsresultat/ns13:beregningsresultat/ns13:uttak/ns24:uttak/ns24:uttaksResultatArbeidsforhold/ns24:uttaksresultatPerioder'
            ) a,
            XMLTABLE
            (
              XMLNamespaces(
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24    
                          )
              ,'ns24:uttaksResultatArbeidsforhold'
              PASSING a.UTTAKSRESULTATARBEIDSFORHOLD
              COLUMNS
              VIRKSOMHET                      VARCHAR2(200) PATH 'ns24:virksomhet'
             ,ARBEIDSFORHOLDID    VARCHAR2(200) PATH 'ns24:arbeidsforholdid'
            ) b,
            XMLTABLE
            (
              XMLNamespaces(
                            'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:fp:v2'                          AS "ns2" --2
                           ,'urn:no:nav:vedtak:felles:xml:felles:v2'                                                AS "ns3" --3
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:v2'                             AS "ns4" --4
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:es:v2'                          AS "ns5" --5
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:fp:v2'                      AS "ns6" --6
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:dvh:es:v2'                      AS "ns7" --7
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:personopplysninger:svp:v2'                         AS "ns8" --8
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:es:v2'                            AS "ns9" --9
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:svp:v2'                           AS "ns10" --10
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:v2'                               AS "ns11" --11
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:vilkaarsgrunnlag:fp:v2'                            AS "ns12" --12
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:v2'                                                AS "ns13" --13
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:v2'                                        AS "ns14" --14
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:fp:v2'                                 AS "ns15" --15
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:oppdrag:dvh:es:v2'                                 AS "ns16" --16
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:fp:v2'                                       AS "ns17" --17
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:es:v2'                          AS "ns18" --18
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:fp:v2'                          AS "ns19" --19
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:beregningsgrunnlag:svp:v2'                         AS "ns20" --20
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:fp:v2'                                      AS "ns21" --21
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:svp:v2'                                     AS "ns22" --22
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:ytelse:es:v2'                                      AS "ns23" --23
                           ,'urn:no:nav:vedtak:felles:xml:vedtak:uttak:svp:v2'                                      AS "ns24" --24     
                          )
              ,'ns24:uttaksresultatPerioder'
              PASSING a.UTTAKSRESULTATPERIODER
              COLUMNS
              FOM                      VARCHAR2(200) PATH 'ns24:periode/ns3:fom'
             ,TOM                      VARCHAR2(200) PATH 'ns24:periode/ns3:tom'
             ,PERIODERESULTATTYPE      VARCHAR2(200) PATH 'ns24:periodeResultatType/@kode'
             ,PERIODERESULTATAARSAK    VARCHAR2(200) PATH 'ns24:perioderesultataarsak/@kode'
            ) c
            where t.trans_id = rec_vedtak_sp.trans_id;
          exception
            when others then
              l_error_melding := sqlerrm;
              l_feil_kilde_navn := 'PL/SQL: FAM_SP_UTTAK_RES_PER';
              l_feil_trans_id := rec_vedtak_sp.trans_id;
              rollback to do_insert; continue;
          end;
          ----------------------------------------------


          l_commit := l_commit + 1;--Ett commit inntil alle tabellene har fått inn data lyktes
          if l_commit >= 10000 then
            commit;
            l_commit := 0;
          end if;
         --dbms_output.put_line(l_create); -- Debug

        exception
          when others then
            --Fortsett med neste rad
            l_error_melding := sqlcode || ' ' || sqlerrm;          
            insert into fk_sensitiv.fp_xml_utbrett_error(min_lastet_dato, id, error_msg, opprettet_tid, kilde)
            values(null, rec_vedtak_sp.trans_id, l_error_melding, sysdate, 'SP_DVH_XML_UTBRETT');
        end;
      end loop;
      if l_error_melding is not null then
        insert into fk_sensitiv.fp_xml_utbrett_error(id, error_msg, opprettet_tid, kilde)
        values(l_feil_trans_id, l_error_melding, sysdate, l_feil_kilde_navn);
      end if;
      commit;--commit til slutt
    end if;
  exception
    when others then
      rollback;
      p_error_melding := sqlcode || ' ' || sqlerrm;
  END SP_DVH_XML_UTBRETT;  

END FP_XML_UTBRETT;
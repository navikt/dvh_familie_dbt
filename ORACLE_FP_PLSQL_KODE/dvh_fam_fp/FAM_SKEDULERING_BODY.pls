create or replace package body            fam_skedulering as

procedure fam_fp_skedulering(p_out_error out varchar2) as
  p_in_vedtak_tom varchar2(6);
  p_in_rapport_dato varchar2(6);
  p_in_forskyvninger number := 1;
  p_in_gyldig_flagg number := 1;
  p_in_periode_type varchar2(2);
  v_error varchar2(4000);
begin  
  --Kjør 2. hver måned
  if to_char(sysdate, 'dd') = '02' then
    if to_char(sysdate, 'mm') = '01' then
      p_in_periode_type := 'A';
      p_in_vedtak_tom := to_char(sysdate, 'yyyy')-1 || '12';
      p_in_rapport_dato := to_char(sysdate, 'yyyy')-1 || '12';    
      dvh_fam_fp.fam_fp.fam_fp_statistikk_aar(p_in_vedtak_tom => p_in_vedtak_tom,
                                              p_in_rapport_dato => p_in_rapport_dato,
                                              p_in_forskyvninger => p_in_forskyvninger,
                                              p_in_gyldig_flagg => p_in_gyldig_flagg,
                                              p_in_periode_type => p_in_periode_type,
                                              p_out_error => v_error);                                            
      p_out_error := substr(p_out_error || v_error, 1, 1000);
      insert into dvh_fam_fp.fam_skedulering_logg
      (kilde, periode_type, statistikk_periode, max_vedtaksdato
     ,forskyvnings_dager, gyldig_flagg, lastet_dato, lastet_av, kjort_error)
      values
      ('FP', p_in_periode_type, p_in_vedtak_tom, p_in_rapport_dato, p_in_forskyvninger, p_in_gyldig_flagg
      ,sysdate, sys_context('USERENV','SESSION_USER'), v_error);
      commit;    
    end if;
    
    p_in_periode_type := 'M';
    p_in_vedtak_tom := to_char(add_months(sysdate,-1), 'yyyymm');
    p_in_rapport_dato := to_char(add_months(sysdate,-1), 'yyyymm');
    dvh_fam_fp.fam_fp.fam_fp_statistikk_maaned(p_in_vedtak_tom => p_in_vedtak_tom,
                                               p_in_rapport_dato => p_in_rapport_dato,
                                               p_in_forskyvninger => p_in_forskyvninger,
                                               p_in_gyldig_flagg => p_in_gyldig_flagg,
                                               p_in_periode_type => p_in_periode_type,
                                               p_out_error => v_error);
    p_out_error := substr(p_out_error || v_error, 1, 1000);
    insert into dvh_fam_fp.fam_skedulering_logg
    (kilde, periode_type, statistikk_periode, max_vedtaksdato
    ,forskyvnings_dager, gyldig_flagg, lastet_dato, lastet_av, kjort_error)
    values
    ('FP', p_in_periode_type, p_in_vedtak_tom, p_in_rapport_dato, p_in_forskyvninger, p_in_gyldig_flagg
    ,sysdate, sys_context('USERENV','SESSION_USER'), v_error);
    commit;
    
    if to_char(sysdate, 'mm') = '07' or to_char(sysdate, 'mm') = '01' then
      p_in_periode_type := 'H';
      p_in_vedtak_tom := to_char(add_months(sysdate,-1), 'yyyymm');
      p_in_rapport_dato := to_char(add_months(sysdate,-1), 'yyyymm');
      dvh_fam_fp.fam_fp.fam_fp_statistikk_halvaar(p_in_vedtak_tom => p_in_vedtak_tom,
                                                  p_in_rapport_dato => p_in_rapport_dato,
                                                  p_in_forskyvninger => p_in_forskyvninger,
                                                  p_in_gyldig_flagg => p_in_gyldig_flagg,
                                                  p_in_periode_type => p_in_periode_type,
                                                  p_out_error => v_error);
      p_out_error := substr(p_out_error || v_error, 1, 1000);
      insert into dvh_fam_fp.fam_skedulering_logg
      (kilde, periode_type, statistikk_periode, max_vedtaksdato
      ,forskyvnings_dager, gyldig_flagg, lastet_dato, lastet_av, kjort_error)
      values
      ('FP', p_in_periode_type, p_in_vedtak_tom, p_in_rapport_dato, p_in_forskyvninger, p_in_gyldig_flagg
      ,sysdate, sys_context('USERENV','SESSION_USER'), v_error);
      commit;
    end if;
    
    if to_char(sysdate, 'mm') = '04' or to_char(sysdate, 'mm') = '07' or to_char(sysdate, 'mm') = '10'
       or to_char(sysdate, 'mm') = '01' then
      p_in_periode_type := 'K';
      p_in_vedtak_tom := to_char(add_months(sysdate,-1), 'yyyymm');
      p_in_rapport_dato := to_char(add_months(sysdate,-1), 'yyyymm');
      dvh_fam_fp.fam_fp.fam_fp_statistikk_kvartal(p_in_vedtak_tom => p_in_vedtak_tom,
                                                  p_in_rapport_dato => p_in_rapport_dato,
                                                  p_in_forskyvninger => p_in_forskyvninger,
                                                  p_in_gyldig_flagg => p_in_gyldig_flagg,
                                                  p_in_periode_type => p_in_periode_type,
                                                  p_out_error => v_error);
      p_out_error := substr(p_out_error || v_error, 1, 1000);
      insert into dvh_fam_fp.fam_skedulering_logg
      (kilde, periode_type, statistikk_periode, max_vedtaksdato
      ,forskyvnings_dager, gyldig_flagg, lastet_dato, lastet_av, kjort_error)
      values
      ('FP', p_in_periode_type, p_in_vedtak_tom, p_in_rapport_dato, p_in_forskyvninger, p_in_gyldig_flagg
      ,sysdate, sys_context('USERENV','SESSION_USER'), v_error);
      commit;
    end if;
    
    if to_char(sysdate, 'mm') = '10' then
      p_in_periode_type := 'S';
      p_in_vedtak_tom := to_char(add_months(sysdate,-1), 'yyyymm');
      p_in_rapport_dato := to_char(add_months(sysdate,-1), 'yyyymm');
      dvh_fam_fp.fam_fp.fam_fp_statistikk_s(p_in_vedtak_tom => p_in_vedtak_tom,
                                            p_in_rapport_dato => p_in_rapport_dato,
                                            p_in_forskyvninger => p_in_forskyvninger,
                                            p_in_gyldig_flagg => p_in_gyldig_flagg,
                                            p_in_periode_type => p_in_periode_type,
                                            p_out_error => v_error);
      p_out_error := substr(p_out_error || v_error, 1, 1000);
      insert into dvh_fam_fp.fam_skedulering_logg
      (kilde, periode_type, statistikk_periode, max_vedtaksdato
      ,forskyvnings_dager, gyldig_flagg, lastet_dato, lastet_av, kjort_error)
      values
      ('FP', p_in_periode_type, p_in_vedtak_tom, p_in_rapport_dato, p_in_forskyvninger, p_in_gyldig_flagg
      ,sysdate, sys_context('USERENV','SESSION_USER'), v_error);
      commit;
    end if;
  end if;
exception
  when others then
    p_out_error := substr('FAM_FP_SKEDULERING: ' || sqlcode || sqlerrm, 1, 1000);
    insert into dvh_fam_fp.fam_skedulering_logg
    (kilde, periode_type, statistikk_periode, max_vedtaksdato
   ,forskyvnings_dager, gyldig_flagg, lastet_dato, lastet_av, kjort_error)
    values
    ('FAM_FP_SKEDULERING', null, null, null, null, null
    ,sysdate, sys_context('USERENV','SESSION_USER'), p_out_error);
    commit;
end fam_fp_skedulering;

procedure fam_bt_skedulering(p_out_error out varchar2) as
  p_in_period varchar2(6) := to_char(add_months(sysdate,-1), 'yyyymm');
  p_in_gyldig_flagg number := 1;
  v_error varchar2(4000);
  
begin
  --Kjør daglig
  dvh_fam_bt.fam_bt.fam_bt_infotrygd_mottaker_update(p_in_period => p_in_period,
                                                     p_error_melding => v_error);
  p_out_error := substr(p_out_error || v_error, 1, 1000);
  insert into dvh_fam_fp.fam_skedulering_logg
  (kilde, periode_type, statistikk_periode, max_vedtaksdato
  ,forskyvnings_dager, gyldig_flagg, lastet_dato, lastet_av, kjort_error)
  values
  ('BT_INFOTRYGD_MOTTAKER_UPDATE', 'M', p_in_period, p_in_period, null, null
  ,sysdate, sys_context('USERENV','SESSION_USER'), v_error);
  commit;
    
  --Kjør 4. hver måned
  if to_char(sysdate, 'dd') = '02' then
    dvh_fam_bt.fam_bt.fam_bt_barn_insert(p_in_period => p_in_period,
                                         p_in_gyldig_flagg => p_in_gyldig_flagg,
                                         p_error_melding => v_error);                                       
    p_out_error := substr(p_out_error || v_error, 1, 1000);
    insert into dvh_fam_fp.fam_skedulering_logg
    (kilde, periode_type, statistikk_periode, max_vedtaksdato
    ,forskyvnings_dager, gyldig_flagg, lastet_dato, lastet_av, kjort_error)
    values
    ('BT_BARN', 'M', p_in_period, p_in_period, null, null
    ,sysdate, sys_context('USERENV','SESSION_USER'), v_error);
    commit;
    
    dvh_fam_bt.fam_bt.fam_bt_mottaker_insert(p_in_period => p_in_period,
                                             p_in_gyldig_flagg => p_in_gyldig_flagg,
                                             p_error_melding => v_error);
    p_out_error := substr(p_out_error || v_error, 1, 1000);
    insert into dvh_fam_fp.fam_skedulering_logg
    (kilde, periode_type, statistikk_periode, max_vedtaksdato
    ,forskyvnings_dager, gyldig_flagg, lastet_dato, lastet_av, kjort_error)
    values
    ('BT_MOTTAKER', 'M', p_in_period, p_in_period, null, null
    ,sysdate, sys_context('USERENV','SESSION_USER'), v_error);
    commit;
  end if;
exception
  when others then
    p_out_error := substr('FAM_BT_SKEDULERING: ' || sqlcode || sqlerrm, 1, 1000);
    insert into dvh_fam_fp.fam_skedulering_logg
    (kilde, periode_type, statistikk_periode, max_vedtaksdato
    ,forskyvnings_dager, gyldig_flagg, lastet_dato, lastet_av, kjort_error)
    values
    ('FAM_BT_SKEDULERING', null, null, null, null, null
    ,sysdate, sys_context('USERENV','SESSION_USER'), p_out_error);
    commit;
end fam_bt_skedulering;

procedure fam_ef_skedulering(p_out_error out varchar2) as
  p_in_period varchar2(6) := to_char(add_months(sysdate,-1), 'yyyymm');
  p_in_vedtak_periode_yyyymm number := to_number(to_char(add_months(sysdate,-1), 'yyyymm'));
  p_in_max_vedtaksperiode_yyyymm number := to_number(to_char(add_months(sysdate,-1), 'yyyymm'));
  p_in_forskyvninger_dag_dd number := 1;
  p_in_gyldig_flagg number := 1;
  v_error varchar2(4000);
begin
  --Kjør 4. hver måned
  if to_char(sysdate, 'dd') = '03' then
    dvh_fam_ef.fam_ef.fam_ef_patch_infotrygd_arena(p_in_periode_yyyymm => to_number(p_in_period),
                                                   p_out_error => v_error);
    p_out_error := substr(p_out_error || v_error, 1, 1000);
    insert into dvh_fam_fp.fam_skedulering_logg
    (kilde, periode_type, statistikk_periode, max_vedtaksdato
    ,forskyvnings_dager, gyldig_flagg, lastet_dato, lastet_av, kjort_error)
    values
    ('EF_PATCH_INFOTRYGD_ARENA', 'M', p_in_period, null, null, null
    ,sysdate, sys_context('USERENV','SESSION_USER'), v_error);
    commit;
  
    dvh_fam_ef.fam_ef.fam_ef_stonad_insert(p_in_period => p_in_period,
                                           p_in_gyldig_flagg => p_in_gyldig_flagg,
                                           p_out_error => v_error);
    p_out_error := substr(p_out_error || v_error, 1, 1000);
    insert into dvh_fam_fp.fam_skedulering_logg
    (kilde, periode_type, statistikk_periode, max_vedtaksdato
    ,forskyvnings_dager, gyldig_flagg, lastet_dato, lastet_av, kjort_error)
    values
    ('EF', 'M', p_in_period, p_in_period, null, p_in_gyldig_flagg
    ,sysdate, sys_context('USERENV','SESSION_USER'), v_error);
    commit;
  
    dvh_fam_ef.fam_ef.fam_ef_stonad_vedtak_insert(p_in_vedtak_periode_yyyymm => p_in_vedtak_periode_yyyymm,
                                                  p_in_max_vedtaksperiode_yyyymm => p_in_max_vedtaksperiode_yyyymm,
                                                  p_in_forskyvninger_dag_dd => p_in_forskyvninger_dag_dd,
                                                  p_in_gyldig_flagg => p_in_gyldig_flagg,
                                                  p_out_error => v_error) ;  
    p_out_error := substr(p_out_error || v_error, 1, 1000);
    insert into dvh_fam_fp.fam_skedulering_logg
    (kilde, periode_type, statistikk_periode, max_vedtaksdato
    ,forskyvnings_dager, gyldig_flagg, lastet_dato, lastet_av, kjort_error)
    values
    ('EF_VEDTAK', 'M', p_in_vedtak_periode_yyyymm, p_in_max_vedtaksperiode_yyyymm, p_in_forskyvninger_dag_dd, p_in_gyldig_flagg
    ,sysdate, sys_context('USERENV','SESSION_USER'), v_error);
    commit;
  end if;
exception
  when others then
    p_out_error := substr('FAM_EF_SKEDULERING: ' || sqlcode || sqlerrm, 1, 1000);
    insert into dvh_fam_fp.fam_skedulering_logg
    (kilde, periode_type, statistikk_periode, max_vedtaksdato
    ,forskyvnings_dager, gyldig_flagg, lastet_dato, lastet_av, kjort_error)
    values
    ('FAM_EF_SKEDULERING', null, null, null, null, null
    ,sysdate, sys_context('USERENV','SESSION_USER'), p_out_error);
    commit;
end fam_ef_skedulering;

procedure fam_pp_skedulering(p_out_error out varchar2) as
  p_in_vedtak_periode_yyyymm number := to_number(to_char(add_months(sysdate,-1), 'yyyymm'));
  p_in_max_vedtaksperiode_yyyymm number := to_number(to_char(add_months(sysdate,-1), 'yyyymm'));
  p_in_forskyvninger_dag_dd number := 1;
  p_in_gyldig_flagg number := 1;
  v_antall_slettet number := 0;
  v_error varchar2(4000);
begin
  --Kjør daglig
  dvh_fam_pp.fam_pp.fam_pp_slett_kode67(p_out_antall_slettet => v_antall_slettet,
                                        p_out_error => v_error);
  p_out_error := substr(p_out_error || v_error, 1, 1000);
  insert into dvh_fam_fp.fam_skedulering_logg
  (kilde, periode_type, statistikk_periode, max_vedtaksdato
  ,forskyvnings_dager, gyldig_flagg, lastet_dato, lastet_av, kjort_error, kommentar)
  values
  ('PP_SLETT_KODE67', 'D', null, null, null, null
  ,sysdate, sys_context('USERENV','SESSION_USER'), v_error, 'Antall kode67 slettet: ' || v_antall_slettet);
  commit;
  
  dvh_fam_pp.fam_pp.fam_pp_diagnose_dim_oppdater(p_out_error => v_error);
  p_out_error := substr(p_out_error || v_error, 1, 1000);
  insert into dvh_fam_fp.fam_skedulering_logg
  (kilde, periode_type, statistikk_periode, max_vedtaksdato
  ,forskyvnings_dager, gyldig_flagg, lastet_dato, lastet_av, kjort_error, kommentar)
  values
  ('PP_DIAGNOSE_DIM', 'D', null, null, null, null
  ,sysdate, sys_context('USERENV','SESSION_USER'), v_error, null);
  commit;
  
  --Kjør 3. hver måned
  if to_char(sysdate, 'dd') = '04' then
    dvh_fam_pp.fam_pp.fam_pp_stonad_vedtak_insert(p_in_vedtak_periode_yyyymm => p_in_vedtak_periode_yyyymm,
                                                  p_in_max_vedtaksperiode_yyyymm => p_in_max_vedtaksperiode_yyyymm,
                                                  p_in_forskyvninger_dag_dd => p_in_forskyvninger_dag_dd,
                                                  p_in_gyldig_flagg => p_in_gyldig_flagg,
                                                  p_out_error => v_error);
    p_out_error := substr(p_out_error || v_error, 1, 1000);
    insert into dvh_fam_fp.fam_skedulering_logg
    (kilde, periode_type, statistikk_periode, max_vedtaksdato
    ,forskyvnings_dager, gyldig_flagg, lastet_dato, lastet_av, kjort_error)
    values
    ('PP_VEDTAK', 'M', p_in_vedtak_periode_yyyymm, p_in_max_vedtaksperiode_yyyymm, p_in_forskyvninger_dag_dd, p_in_gyldig_flagg
    ,sysdate, sys_context('USERENV','SESSION_USER'), v_error);
    commit;

    dvh_fam_pp.fam_pp.fam_pp_stonad_vedtak_ur_insert(p_in_vedtak_periode_yyyymm => p_in_vedtak_periode_yyyymm,
                                                     p_in_gyldig_flagg => p_in_gyldig_flagg,
                                                     p_out_error => v_error);
    p_out_error := substr(p_out_error || v_error, 1, 1000);
    insert into dvh_fam_fp.fam_skedulering_logg
    (kilde, periode_type, statistikk_periode, max_vedtaksdato
    ,forskyvnings_dager, gyldig_flagg, lastet_dato, lastet_av, kjort_error)
    values
    ('PP_VEDTAK_UR', 'M', p_in_vedtak_periode_yyyymm, null, null, p_in_gyldig_flagg
    ,sysdate, sys_context('USERENV','SESSION_USER'), v_error);
    commit;
  end if;
exception
  when others then
    p_out_error := substr('FAM_PP_SKEDULERING: ' || sqlcode || sqlerrm, 1, 1000);
    insert into dvh_fam_fp.fam_skedulering_logg
    (kilde, periode_type, statistikk_periode, max_vedtaksdato
    ,forskyvnings_dager, gyldig_flagg, lastet_dato, lastet_av, kjort_error)
    values
    ('FAM_PP_SKEDULERING', null, null, null, null, null
    ,sysdate, sys_context('USERENV','SESSION_USER'), p_out_error);
    commit;
end fam_pp_skedulering;

procedure fam_skedulering(p_in_dummy in varchar2, p_out_error out varchar2) as
  v_error varchar2(4000);
begin
  fam_fp_skedulering(v_error);
  p_out_error := substr(p_out_error || v_error, 1, 1000);
  
  fam_bt_skedulering(v_error);
  p_out_error := substr(p_out_error || v_error, 1, 1000);
  
  fam_ef_skedulering(v_error);
  p_out_error := substr(p_out_error || v_error, 1, 1000);
  
  fam_pp_skedulering(v_error);
  p_out_error := substr(p_out_error || v_error, 1, 1000);
end fam_skedulering;

end fam_skedulering;
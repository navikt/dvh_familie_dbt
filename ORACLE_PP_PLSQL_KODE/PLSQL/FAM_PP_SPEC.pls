create or replace PACKAGE FAM_PP authid current_user AS

  procedure fam_pp_slett_offset(p_in_offset in varchar2, p_out_error out varchar2);
  procedure fam_pp_slett_kode67(p_out_antall_slettet out number, p_out_error out varchar2);
  procedure fam_pp_utpakking_offset(p_in_offset in number, p_out_error out varchar2);
  procedure fam_pp_utpakking_offset_test(p_in_offset in number, p_out_error out varchar2);
  procedure fam_pp_utpakking_offset_2(p_in_offset in number, p_out_error out varchar2);
  procedure fam_pp_stonad_vedtak_insert(p_in_vedtak_periode_yyyymm in number
                                     ,p_in_max_vedtaksperiode_yyyymm in number
                                     ,p_in_forskyvninger_dag_dd in number
                                     ,p_in_gyldig_flagg in number default 0
                                     ,p_out_error out varchar2);
  procedure fam_pp_stonad_vedtak_insert_bck(p_in_vedtak_periode_yyyymm in number
                                     ,p_in_max_vedtaksperiode_yyyymm in number
                                     ,p_in_forskyvninger_dag_dd in number
                                     ,p_in_gyldig_flagg in number default 0
                                     ,p_out_error out varchar2);
  procedure fam_pp_stonad_vedtak_ur_insert(p_in_vedtak_periode_yyyymm in number
                                        ,p_in_gyldig_flagg in number default 0
                                        ,p_out_error out varchar2);
  procedure fam_pp_diagnose_dim_oppdater(p_out_error out varchar2);
  procedure fam_pp_stonad_siste_diagnose_patching(p_in_vedtak_periode_yyyymm in number
                                     ,p_in_kildesystem in varchar2
                                     ,p_in_gyldig_flagg in number default 0
                                     ,p_out_error out varchar2);

END FAM_PP;
create or replace package fam_ef authid current_user as
  procedure fam_ef_utpakking_offset(p_in_offset in number, p_error_melding out varchar2);
  procedure fam_ef_stonad_insert(p_in_period in varchar2
                                ,p_in_gyldig_flagg in number default 0
                                ,p_out_error out varchar2);
  procedure fam_ef_stonad_vedtak_insert(p_in_vedtak_periode_yyyymm in number
                                       ,p_in_max_vedtaksperiode_yyyymm in number
                                       ,p_in_forskyvninger_dag_dd in number
                                       ,p_in_gyldig_flagg in number default 0
                                       ,p_out_error out varchar2);
  procedure fam_ef_patch_infotrygd_arena(p_in_periode_yyyymm in number ,p_out_error out varchar2);
  procedure fam_ef_patch_migrering_vedtak(p_in_periode_yyyymm in number ,p_out_error out varchar2);
end fam_ef;
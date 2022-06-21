create or replace PACKAGE                                                                                                                                                                                     FAM_FP authid current_user AS 
  procedure fam_fp_statistikk_aar(p_in_vedtak_tom in varchar2, p_in_rapport_dato in varchar2, p_in_forskyvninger in number
                                     ,p_in_gyldig_flagg in number default 0
                                     ,p_in_periode_type in varchar2 default 'A'
                                     ,p_out_error out varchar2);
  procedure fam_fp_statistikk_s(p_in_vedtak_tom in varchar2, p_in_rapport_dato in varchar2, p_in_forskyvninger in number
                                     ,p_in_gyldig_flagg in number default 0
                                     ,p_in_periode_type in varchar2 default 'S'
                                     ,p_out_error out varchar2);
  procedure fam_fp_statistikk_halvaar(p_in_vedtak_tom in varchar2, p_in_rapport_dato in varchar2, p_in_forskyvninger in number
                                     ,p_in_gyldig_flagg in number default 0
                                     ,p_in_periode_type in varchar2 default 'H'
                                     ,p_out_error out varchar2);
  procedure fam_fp_statistikk_kvartal(p_in_vedtak_tom in varchar2, p_in_rapport_dato in varchar2, p_in_forskyvninger in number
                                     ,p_in_gyldig_flagg in number default 0
                                     ,p_in_periode_type in varchar2 default 'K'
                                     ,p_out_error out varchar2);
  procedure fam_fp_statistikk_maaned(p_in_vedtak_tom in varchar2, p_in_rapport_dato in varchar2, p_in_forskyvninger in number
                                     ,p_in_gyldig_flagg in number default 0
                                     ,p_in_periode_type in varchar2 default 'M'
                                     ,p_out_error out varchar2);
END FAM_FP;
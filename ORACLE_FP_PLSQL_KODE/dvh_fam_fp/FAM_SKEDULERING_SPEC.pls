create or replace package fam_skedulering authid current_user as 

  procedure fam_skedulering(p_in_dummy in varchar2, p_out_error out varchar2);
  procedure fam_pp_skedulering(p_out_error out varchar2);
  procedure fam_bt_skedulering(p_out_error out varchar2);
  procedure fam_ef_skedulering(p_out_error out varchar2);
  procedure fam_fp_skedulering(p_out_error out varchar2);

end fam_skedulering;
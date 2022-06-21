create or replace PACKAGE                                                                                                                                   fam_bt AUTHID CURRENT_USER AS
  PROCEDURE fam_bt_infotrygd_mottaker_update(p_in_period IN VARCHAR2, p_error_melding OUT VARCHAR2);
  PROCEDURE fam_bt_mottaker_insert(p_in_period IN VARCHAR2
                                  ,p_in_gyldig_flagg in number default 0
                                  ,p_error_melding OUT VARCHAR2);
  PROCEDURE fam_bt_barn_insert(p_in_period IN VARCHAR2
                              ,p_in_gyldig_flagg in number default 0
                              ,p_error_melding OUT VARCHAR2);
  PROCEDURE fam_bt_barn_insert_bck(p_in_period IN VARCHAR2, p_error_melding OUT VARCHAR2);
  --PROCEDURE fam_bt_mottaker_insert(p_in_period IN VARCHAR2, p_error_melding OUT VARCHAR2);
  PROCEDURE fam_bt_slett_offset(p_in_offset IN VARCHAR2, p_error_melding OUT VARCHAR2);
  PROCEDURE fam_bt_utpakking_offset(p_in_offset IN NUMBER, p_error_melding OUT VARCHAR2);
END fam_bt;
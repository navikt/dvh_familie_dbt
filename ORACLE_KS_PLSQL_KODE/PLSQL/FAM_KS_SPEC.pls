create or replace PACKAGE fam_ks AS
PROCEDURE fam_ks_mottaker_insert(p_in_period IN VARCHAR2
                                  ,p_in_gyldig_flagg in number default 0
                                  ,p_error_melding OUT VARCHAR2);
PROCEDURE fam_ks_barn_insert(p_in_period IN VARCHAR2
                                  ,p_in_gyldig_flagg in number default 0
                                  ,p_error_melding OUT VARCHAR2);

END fam_ks;
create or replace PACKAGE                                                                                                                                     FP_XML_UTBRETT authid current_user AS
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
  procedure SLETT_KODE67_FAGSAK(dummy in varchar2, p_error_melding out varchar2);

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
  procedure SLETT_TRANS_ID(p_inn_kilde in varchar2, p_inn_trans_id in number, p_out_error_melding out varchar2);

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
  procedure SLETT_GAMLE_VEDTAK(p_inn_kilde in varchar2, p_out_error_melding out varchar2);

  --****************************************************************************************************
  -- NAME:     ENGANGSSTONAD_DVH_XML_UTBRETT
  -- PURPOSE:  Parse xml som ligger i forkammer.
  --
  -- REVISION:
  -- Version  Date           Author                  Description
  -- 0.1      03.12.2018     Helen Rong              Initial
  --****************************************************************************************************  
  procedure ENGANGSSTONAD_DVH_XML_UTBRETT(dummy in varchar2, p_error_melding out varchar2);

  --****************************************************************************************************
  -- NAME:     FP_DVH_XML_UTBRETT
  -- PURPOSE:  Parse xml som ligger i forkammer.
  --
  -- REVISION:
  -- Version  Date           Author                  Description
  -- 0.1      23.05.2019     Helen Rong              Utpakke FP xml som ligger i felles tabellen
  --                                                 fk_sensitiv.fam_fp_vedtak_utbetaling.
  --****************************************************************************************************
  procedure FP_DVH_XML_UTBRETT(dummy in varchar2, p_error_melding out varchar2);


  --****************************************************************************************************
  -- NAME:     FP_ENGANGSSTONAD_XML_UTBRETT_PROC
  -- PURPOSE:  Parse xml som ligger i forkammer.
  --
  -- REVISION:
  -- Version  Date           Author                  Description
  -- 0.1      29.10.2018     Helen Rong              Initial
  --****************************************************************************************************
  procedure FP_ENGANGSSTONAD_XML_UTBRETT(dummy in varchar2, p_error_melding out varchar2);

  --****************************************************************************************************
  -- NAME:     SP_DVH_XML_UTBRETT
  -- PURPOSE:  Parse xml som ligger i forkammer.
  --
  -- REVISION:
  -- Version  Date           Author                  Description
  -- 0.1      23.10.2019     Sohaib Khan             Utpakke SP xml som ligger i felles tabellen
  --                                                 fk_sensitiv.fam_fp_vedtak_utbetaling.
  --****************************************************************************************************
  procedure SP_DVH_XML_UTBRETT(dummy in varchar2, p_error_melding out varchar2);

END FP_XML_UTBRETT;
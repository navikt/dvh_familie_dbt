select
    kjonn_besk
   ,alder_gruppe_besk
   ,statistikkvariabel
   ,aar
   ,aar_kvartal
   ,kvartal
   ,kvartal_besk
   ,px_data
from dvh_fam_bt.agg_bt_px_mottaker_kjonn_alder
order by aar_kvartal, sortering_kjonn, sortering_alder_gruppe